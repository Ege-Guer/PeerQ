"""Unit tests for peerq.clock."""

import asyncio

import pytest

from peerq.clock import Clock, RealClock, SimClock


def test_clock_protocols() -> None:
    real = RealClock()
    sim = SimClock()
    assert isinstance(real, Clock)
    assert isinstance(sim, Clock)


@pytest.mark.asyncio
async def test_real_clock_now_and_sleep() -> None:
    clock = RealClock()
    t0 = clock.now()
    await clock.sleep(0.01)
    t1 = clock.now()
    assert t1 >= t0
    # Zero or negative sleep should return without error
    await clock.sleep(0)
    await clock.sleep(-1)


@pytest.mark.asyncio
async def test_sim_clock_virtual_progression() -> None:
    clock = SimClock(initial_time=100.0)
    assert clock.now() == 100.0

    wakeups: list[str] = []

    async def sleeper(name: str, delay: float) -> None:
        await clock.sleep(delay)
        wakeups.append(name)

    t1 = asyncio.create_task(sleeper("task1", 10.0))
    t2 = asyncio.create_task(sleeper("task2", 20.0))
    t3 = asyncio.create_task(sleeper("task3", 5.0))

    # Give tasks a chance to register
    await asyncio.sleep(0)

    assert clock.has_pending_sleepers()
    assert clock.next_deadline() == 105.0

    # Advance to 104.0: none should wake
    woken = clock.advance_to(104.0)
    assert woken == 0
    assert wakeups == []

    # Advance by 2.0 (to 106.0): task3 should wake
    woken = clock.advance(2.0)
    assert woken == 1
    # Give event loop a cycle to run the completed task callback
    await asyncio.sleep(0)
    assert wakeups == ["task3"]

    # Step to next deadline (110.0 for task1)
    next_time = clock.step()
    assert next_time == 110.0
    await asyncio.sleep(0)
    assert wakeups == ["task3", "task1"]

    # Advance past task2 (to 130.0)
    woken = clock.advance_to(130.0)
    assert woken == 1
    await asyncio.sleep(0)
    assert wakeups == ["task3", "task1", "task2"]
    assert not clock.has_pending_sleepers()
    assert clock.step() is None

    await t1
    await t2
    await t3


@pytest.mark.asyncio
async def test_sim_clock_cancellation() -> None:
    clock = SimClock()

    async def sleeper() -> None:
        await clock.sleep(50.0)

    task = asyncio.create_task(sleeper())
    await asyncio.sleep(0)
    assert clock.has_pending_sleepers()

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    # Advance time: cancelled sleeper does not cause error
    woken = clock.advance(100.0)
    assert woken == 0


@pytest.mark.asyncio
async def test_sim_clock_negative_advance_raises() -> None:
    clock = SimClock(10.0)
    with pytest.raises(ValueError, match="Cannot advance backwards"):
        clock.advance(-1.0)

    with pytest.raises(ValueError, match="Cannot advance backwards"):
        clock.advance_to(5.0)


@pytest.mark.asyncio
async def test_sim_clock_zero_delay_sleep() -> None:
    clock = SimClock(10.0)
    executed = False

    async def job() -> None:
        nonlocal executed
        await clock.sleep(0)
        executed = True

    task = asyncio.create_task(job())
    await task
    assert executed
    assert clock.now() == 10.0


@pytest.mark.asyncio
async def test_sim_clock_cancelled_sleepers_purged_on_heap_inspection() -> None:
    clock = SimClock(10.0)

    async def sleeper() -> None:
        await clock.sleep(100.0)

    task = asyncio.create_task(sleeper())
    await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    # Top of heap is cancelled future: has_pending_sleepers pops it
    assert not clock.has_pending_sleepers()

    # Repeat for next_deadline
    task2 = asyncio.create_task(sleeper())
    await asyncio.sleep(0)
    task2.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task2
    assert clock.next_deadline() is None

    # Repeat for step
    task3 = asyncio.create_task(sleeper())
    await asyncio.sleep(0)
    task3.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task3
    assert clock.step() is None
