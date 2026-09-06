"""Unit tests for peerq.node (PeerNode coordinator)."""

import random
from pathlib import Path

import pytest

from peerq.clock import SimClock
from peerq.consensus import TaskState
from peerq.node import PeerNode
from peerq.transport import InMemoryTransport, SimNetwork
from peerq.wal import WriteAheadLog


@pytest.mark.asyncio
async def test_node_single_task_execution() -> None:
    clock = SimClock(0.0)
    net = SimNetwork(clock)
    t1 = InMemoryTransport("node-1", net)
    rng = random.Random(42)

    async def sample_handler(payload: bytes) -> bytes:
        return payload.upper()

    node = PeerNode(
        node_id="node-1",
        clock=clock,
        transport=t1,
        rng=rng,
        peers=[],
        handler=sample_handler,
    )

    await node.start()
    await node.submit_task("t1", b"hello world")

    # Step clock to allow worker loop to run and execute
    clock.advance(0.1)
    await clock.sleep(0)  # cooperative yield

    rec = node.get_task("t1")
    assert rec is not None
    assert rec.state == TaskState.DONE
    assert rec.result == b"HELLO WORLD"
    assert node.metrics.get_counter("completed") == 1

    await node.stop()


@pytest.mark.asyncio
async def test_node_multi_peer_gossip() -> None:
    clock = SimClock(0.0)
    net = SimNetwork(clock)
    t1 = InMemoryTransport("node-1", net)
    t2 = InMemoryTransport("node-2", net)
    rng1 = random.Random(101)
    rng2 = random.Random(102)

    async def handler_2(payload: bytes) -> bytes:
        return payload + b"-processed-by-node-2"

    # Node 1 submits without handler, Node 2 processes with handler
    node1 = PeerNode("node-1", clock, t1, rng1, peers=["node-2"], handler=None)
    node2 = PeerNode("node-2", clock, t2, rng2, peers=["node-1"], handler=handler_2)

    await node1.start()
    await node2.start()

    await node1.submit_task("task-x", b"raw-data")

    # Advance virtual time through gossip intervals
    for _ in range(10):
        clock.advance(0.5)
        await clock.sleep(0)

    # Node 2 should have received, claimed, and executed task-x
    rec2 = node2.get_task("task-x")
    assert rec2 is not None
    assert rec2.state == TaskState.DONE
    assert rec2.result == b"raw-data-processed-by-node-2"

    # Advance more to gossip result back to Node 1
    for _ in range(10):
        clock.advance(0.5)
        await clock.sleep(0)

    rec1 = node1.get_task("task-x")
    assert rec1 is not None
    assert rec1.state == TaskState.DONE
    assert rec1.result == b"raw-data-processed-by-node-2"

    await node1.stop()
    await node2.stop()


@pytest.mark.asyncio
async def test_node_crash_and_lease_reclaim() -> None:
    clock = SimClock(0.0)
    net = SimNetwork(clock)
    t1 = InMemoryTransport("node-1", net)
    t2 = InMemoryTransport("node-2", net)
    rng1 = random.Random(201)
    rng2 = random.Random(202)

    # Handler on Node 1 hangs/dies mid-task
    async def hanging_handler(payload: bytes) -> bytes:
        await clock.sleep(100.0)
        return payload

    async def successful_handler(payload: bytes) -> bytes:
        return payload + b"-recovered"

    node1 = PeerNode(
        "node-1",
        clock,
        t1,
        rng1,
        peers=["node-2"],
        handler=hanging_handler,
        lease_duration=2.0,
        heartbeat_interval=0.5,
        reclaim_interval=0.5,
    )
    node2 = PeerNode(
        "node-2",
        clock,
        t2,
        rng2,
        peers=["node-1"],
        handler=successful_handler,
        lease_duration=2.0,
        heartbeat_interval=0.5,
        reclaim_interval=0.5,
    )

    await node1.start()
    await node2.start()

    # Exchange heartbeats initially so failure detector knows nodes
    for _ in range(10):
        clock.advance(0.5)
        await clock.sleep(0)

    # Node 1 submits and claims task
    await node1.submit_task("t-reclaim", b"crash-data")
    clock.advance(0.1)
    await clock.sleep(0)

    rec1 = node1.get_task("t-reclaim")
    assert rec1 is not None
    assert rec1.state in (TaskState.CLAIMED, TaskState.RUNNING)
    assert rec1.claimed_by == "node-1"

    # Gossip the claim to node 2
    clock.advance(0.5)
    await clock.sleep(0)

    # Now Node 1 crashes hard: stopped completely
    await node1.stop()

    # Advance time: heartbeats from node-1 stop, suspicion crosses threshold, lease expires
    for _ in range(25):
        clock.advance(0.5)
        await clock.sleep(0)

    # Node 2 must have reclaimed and completed the task!
    rec2 = node2.get_task("t-reclaim")
    assert rec2 is not None
    assert rec2.state == TaskState.DONE
    assert rec2.result == b"crash-data-recovered"
    assert rec2.claimed_by == "node-2"
    assert rec2.fence_token.epoch >= 2
    await node2.stop()


@pytest.mark.asyncio
async def test_node_wal_crash_recovery(tmp_path: Path) -> None:
    clock = SimClock(0.0)
    net = SimNetwork(clock)
    t1 = InMemoryTransport("node-1", net)
    rng = random.Random(42)
    wal_path = tmp_path / "node1.wal"

    async def upper_handler(payload: bytes) -> bytes:
        return payload.upper()

    wal1 = WriteAheadLog(wal_path)
    node1 = PeerNode(
        "node-1",
        clock,
        t1,
        rng,
        peers=[],
        handler=upper_handler,
        wal=wal1,
    )

    await node1.start()
    await node1.submit_task("t-persist", b"durable-data")
    clock.advance(0.1)
    await clock.sleep(0)

    task_rec = node1.get_task("t-persist")
    assert task_rec is not None
    assert task_rec.state == TaskState.DONE
    assert task_rec.result == b"DURABLE-DATA"

    await node1.stop()

    # Now simulate a crash and reboot: new transport, new node instance, re-opening same WAL
    t1_reboot = InMemoryTransport("node-1", net)
    wal_reboot = WriteAheadLog(wal_path)
    node1_reboot = PeerNode(
        "node-1",
        clock,
        t1_reboot,
        rng,
        peers=[],
        handler=upper_handler,
        wal=wal_reboot,
    )

    # Before start, memory is empty
    assert node1_reboot.get_task("t-persist") is None

    # On start, state is reconstructed from WAL
    await node1_reboot.start()
    recovered_task = node1_reboot.get_task("t-persist")
    assert recovered_task is not None
    assert recovered_task.task_id == "t-persist"
    assert recovered_task.state == TaskState.DONE
    assert recovered_task.result == b"DURABLE-DATA"
    assert recovered_task.vector_clock.get("node-1") >= 1

    await node1_reboot.stop()
