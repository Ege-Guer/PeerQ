"""Unit tests for peerq.node (PeerNode coordinator)."""

import random
from pathlib import Path

import pytest

from peerq.clock import SimClock
from peerq.consensus import TaskState
from peerq.crypto import Ed25519KeyPair, PeerKeyRing
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
    identities = {nid: Ed25519KeyPair.generate() for nid in ("node-1", "node-2")}
    keyring = PeerKeyRing()
    for nid, identity in identities.items():
        keyring.add_peer(nid, identity.public_key)

    async def handler_2(payload: bytes) -> bytes:
        return payload + b"-processed-by-node-2"

    # Node 1 submits without handler, Node 2 processes with handler
    node1 = PeerNode(
        "node-1",
        clock,
        t1,
        rng1,
        peers=["node-2"],
        handler=None,
        identity=identities["node-1"],
        keyring=keyring,
    )
    node2 = PeerNode(
        "node-2",
        clock,
        t2,
        rng2,
        peers=["node-1"],
        handler=handler_2,
        identity=identities["node-2"],
        keyring=keyring,
    )

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
    identities = {nid: Ed25519KeyPair.generate() for nid in ("node-1", "node-2")}
    keyring = PeerKeyRing()
    for nid, identity in identities.items():
        keyring.add_peer(nid, identity.public_key)

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
        identity=identities["node-1"],
        keyring=keyring,
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
        identity=identities["node-2"],
        keyring=keyring,
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
    identity = Ed25519KeyPair.generate()
    keyring = PeerKeyRing()
    keyring.add_peer("node-1", identity.public_key)
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
        identity=identity,
        keyring=keyring,
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
        identity=identity,
        keyring=keyring,
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


@pytest.mark.asyncio
async def test_node_handler_exception_records_failure() -> None:
    clock = SimClock(0.0)
    net = SimNetwork(clock)
    t1 = InMemoryTransport("n1", net)
    rng = random.Random(42)

    async def failing_handler(payload: bytes) -> bytes:
        raise RuntimeError("simulated task computation failure")

    node = PeerNode("n1", clock, t1, rng, peers=[], handler=failing_handler)
    await node.start()
    await node.submit_task("fail-task", b"bad_input")

    clock.advance(0.1)
    await clock.sleep(0)

    task = node.get_task("fail-task")
    assert task is not None
    assert task.state == TaskState.FAILED
    assert "simulated task computation failure" in (task.error or "")
    assert node.metrics.get_counter("failed") == 1

    await node.stop()


@pytest.mark.asyncio
async def test_node_commit_rejected_on_lease_expiry() -> None:
    clock = SimClock(0.0)
    net = SimNetwork(clock)
    t1 = InMemoryTransport("n1", net)
    rng = random.Random(42)

    async def slow_handler(payload: bytes) -> bytes:
        # Sleep virtual time beyond the 1.0s lease duration
        await clock.sleep(2.0)
        return b"late_result"

    node = PeerNode("n1", clock, t1, rng, peers=[], handler=slow_handler, lease_duration=1.0)
    await node.start()
    await node.submit_task("slow-task", b"payload")

    # Step clock to allow task to be claimed and start running
    clock.advance(0.1)
    await clock.sleep(0)
    task = node.get_task("slow-task")
    assert task is not None
    assert task.state == TaskState.RUNNING

    # Now advance clock past lease expiry: worker finishes late
    clock.advance(2.5)
    await clock.sleep(0)

    # Stale commit should be rejected by fencing check!
    assert node.metrics.get_counter("rejected") == 1
    assert node.metrics.get_counter("completed") == 0

    await node.stop()


@pytest.mark.asyncio
async def test_node_queue_full_rejection() -> None:
    from peerq.queue import QueueFull

    clock = SimClock(0.0)
    net = SimNetwork(clock)
    t1 = InMemoryTransport("n1", net)
    rng = random.Random(42)

    # Worker handler doesn't consume immediately
    async def blocking_handler(payload: bytes) -> bytes:
        await clock.sleep(10.0)
        return payload

    node = PeerNode(
        "n1",
        clock,
        t1,
        rng,
        peers=[],
        handler=blocking_handler,
        max_queue_size=1,
    )
    await node.start()

    # First task fills queue
    await node.submit_task("t1", b"1")

    # Second task should raise QueueFull
    with pytest.raises(QueueFull):
        await node.submit_task("t2", b"2")

    await node.stop()


@pytest.mark.asyncio
async def test_node_payload_lease_duration() -> None:
    clock = SimClock(0.0)
    net = SimNetwork(clock)
    t1 = InMemoryTransport("n1", net)
    rng = random.Random(42)

    # Task takes 10 seconds to execute, which would exceed default 5s lease
    async def slow_handler(payload: bytes) -> bytes:
        await clock.sleep(10.0)
        return b"done-slow"

    node = PeerNode(
        "n1",
        clock,
        t1,
        rng,
        peers=[],
        handler=slow_handler,
        lease_duration=5.0,
    )
    await node.start()

    # Payload explicitly sets lease_duration to 30.0s
    payload = b'{"task": "long_quant", "lease_duration": 30.0}'
    await node.submit_task("t-long", payload)

    # Step clock to allow claim and execution
    clock.advance(0.1)
    await clock.sleep(0)

    # Verify that the task was claimed with the 30.0s lease duration
    rec = node.get_task("t-long")
    assert rec is not None
    assert rec.lease_expiry >= 30.0

    # Advance virtual clock by 10s to let handler finish
    clock.advance(10.0)
    await clock.sleep(0)

    rec_finished = node.get_task("t-long")
    assert rec_finished is not None
    assert rec_finished.state == TaskState.DONE
    assert rec_finished.result == b"done-slow"
    assert node.metrics.get_counter("rejected") == 0

    await node.stop()


def test_node_env_lease_duration(monkeypatch: pytest.MonkeyPatch) -> None:
    clock = SimClock(0.0)
    net = SimNetwork(clock)
    t1 = InMemoryTransport("n1", net)
    rng = random.Random(42)

    monkeypatch.setenv("PEERQ_LEASE_DURATION", "45.0")
    node = PeerNode("n1", clock, t1, rng, peers=[])
    assert node.lease_duration == 45.0


@pytest.mark.asyncio
async def test_node_reclaim_without_handler_resets_to_pending() -> None:
    clock = SimClock(0.0)
    net = SimNetwork(clock)
    t1 = InMemoryTransport("node-client", net)
    t2 = InMemoryTransport("node-worker", net)
    rng1 = random.Random(301)
    rng2 = random.Random(302)
    identities = {nid: Ed25519KeyPair.generate() for nid in ("node-client", "node-worker")}
    keyring = PeerKeyRing()
    for nid, identity in identities.items():
        keyring.add_peer(nid, identity.public_key)

    async def hanging_worker(payload: bytes) -> bytes:
        await clock.sleep(100.0)
        return payload

    # Client has no handler (like PC), Worker has hanging handler (like Mac)
    client = PeerNode(
        "node-client",
        clock,
        t1,
        rng1,
        peers=["node-worker"],
        handler=None,
        lease_duration=2.0,
        heartbeat_interval=0.5,
        reclaim_interval=0.5,
        identity=identities["node-client"],
        keyring=keyring,
    )
    worker = PeerNode(
        "node-worker",
        clock,
        t2,
        rng2,
        peers=["node-client"],
        handler=hanging_worker,
        lease_duration=2.0,
        heartbeat_interval=0.5,
        reclaim_interval=0.5,
        identity=identities["node-worker"],
        keyring=keyring,
    )

    await client.start()
    await worker.start()

    for _ in range(10):
        clock.advance(0.5)
        await clock.sleep(0)

    # Worker submits and claims task
    await worker.submit_task("t-orphan", b"data")
    clock.advance(0.1)
    await clock.sleep(0)

    rec = worker.get_task("t-orphan")
    assert rec is not None
    assert rec.claimed_by == "node-worker"

    # Gossip claim to client
    clock.advance(0.5)
    await clock.sleep(0)

    # Worker crashes hard
    await worker.stop()

    # Advance time: worker heartbeats stop, lease expires, client reclaims
    for _ in range(25):
        clock.advance(0.5)
        await clock.sleep(0)

    # Client (without handler) must have reclaimed and reset task to PENDING with claimed_by=None
    rec_client = client.get_task("t-orphan")
    assert rec_client is not None
    assert rec_client.state == TaskState.PENDING
    assert rec_client.claimed_by is None
    assert rec_client.lease_expiry == 0.0

    await client.stop()
