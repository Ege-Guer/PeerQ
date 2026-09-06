"""Unit tests for peerq.queue."""

import asyncio

import pytest

from peerq.queue import (
    BackpressurePriorityQueue,
    CreditFlowController,
    QueueEmpty,
    QueueFull,
    QueuePolicy,
)


def test_priority_and_fifo_order() -> None:
    q: BackpressurePriorityQueue[str] = BackpressurePriorityQueue()
    # Enqueue mixed priorities
    q.put_nowait("p0-first", priority=0)
    q.put_nowait("p10-first", priority=10)
    q.put_nowait("p5-first", priority=5)
    q.put_nowait("p10-second", priority=10)
    q.put_nowait("p0-second", priority=0)
    q.put_nowait("p5-second", priority=5)

    assert q.qsize() == 6
    assert not q.empty()

    # Highest priority (10) first, in FIFO order
    pri, item = q.get_nowait()
    assert pri == 10 and item == "p10-first"
    pri, item = q.get_nowait()
    assert pri == 10 and item == "p10-second"

    # Next priority (5), in FIFO order
    pri, item = q.get_nowait()
    assert pri == 5 and item == "p5-first"
    pri, item = q.get_nowait()
    assert pri == 5 and item == "p5-second"

    # Lowest priority (0), in FIFO order
    pri, item = q.get_nowait()
    assert pri == 0 and item == "p0-first"
    pri, item = q.get_nowait()
    assert pri == 0 and item == "p0-second"

    assert q.empty()
    with pytest.raises(QueueEmpty):
        q.get_nowait()


def test_reject_on_full() -> None:
    q: BackpressurePriorityQueue[str] = BackpressurePriorityQueue(
        maxsize=2, policy=QueuePolicy.REJECT_ON_FULL
    )
    q.put_nowait("item-1", priority=1)
    q.put_nowait("item-2", priority=1)
    assert q.full()

    with pytest.raises(QueueFull):
        q.put_nowait("item-3", priority=1)


@pytest.mark.asyncio
async def test_block_on_full() -> None:
    q: BackpressurePriorityQueue[str] = BackpressurePriorityQueue(
        maxsize=1, policy=QueuePolicy.BLOCK_ON_FULL
    )
    await q.put("item-1", priority=1)
    assert q.full()

    put_finished = False

    async def producer() -> None:
        nonlocal put_finished
        await q.put("item-2", priority=1)
        put_finished = True

    task = asyncio.create_task(producer())
    await asyncio.sleep(0.01)
    assert not put_finished

    # Dequeue item-1 to unblock producer
    pri, item = await q.get()
    assert item == "item-1"
    await asyncio.sleep(0.01)
    assert put_finished

    _, item2 = await q.get()
    assert item2 == "item-2"
    await task


def test_peek() -> None:
    q: BackpressurePriorityQueue[str] = BackpressurePriorityQueue()
    assert q.peek() is None

    q.put_nowait("item-1", priority=5)
    assert q.peek() == (5, "item-1")
    # Peek should not dequeue
    assert q.qsize() == 1


@pytest.mark.asyncio
async def test_credit_flow_controller() -> None:
    fc = CreditFlowController(initial_credits=2)
    fc.init_peer("peer-a", credits=2)

    assert fc.get_credits("peer-a") == 2
    assert fc.try_acquire("peer-a", 1) is True
    assert fc.try_acquire("peer-a", 1) is True
    assert fc.try_acquire("peer-a", 1) is False  # Exhausted

    acquired = False

    async def waiter() -> None:
        nonlocal acquired
        await fc.acquire("peer-a", 1)
        acquired = True

    task = asyncio.create_task(waiter())
    await asyncio.sleep(0.01)
    assert not acquired

    # Replenish credit
    fc.replenish("peer-a", 1)
    await asyncio.sleep(0.01)
    assert acquired
    await task


@pytest.mark.asyncio
async def test_credit_waiter_cancellation() -> None:
    fc = CreditFlowController(initial_credits=0)
    fc.init_peer("peer-b", credits=0)

    task = asyncio.create_task(fc.acquire("peer-b", 1))
    await asyncio.sleep(0.01)

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    # Replenish now: should not crash with cancelled future
    fc.replenish("peer-b", 2)
    assert fc.get_credits("peer-b") == 2


@pytest.mark.asyncio
async def test_queue_reject_on_full_async_put() -> None:
    q: BackpressurePriorityQueue[str] = BackpressurePriorityQueue(
        maxsize=1, policy=QueuePolicy.REJECT_ON_FULL
    )
    await q.put("item-1", priority=1)
    with pytest.raises(QueueFull):
        await q.put("item-2", priority=1)


def test_credit_flow_unknown_peer() -> None:
    fc = CreditFlowController(initial_credits=5)
    # Unknown peer gets initial_credits
    assert fc.get_credits("unknown-peer") == 5

    # Replenishing an unknown peer initializes it
    fc.replenish("another-unknown", 3)
    assert fc.get_credits("another-unknown") == 8
