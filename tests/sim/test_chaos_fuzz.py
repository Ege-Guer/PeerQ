"""
Extended chaos fuzzing simulation test suite for peerq.

Executes randomized fault injection over virtual time:
- Intermittent symmetric & asymmetric partitions
- Abrupt node crashes mid-execution
- Rolling restarts with state restoration
- Continuous assertion of all 5 distributed system invariants
"""

from pathlib import Path

import pytest

from peerq.consensus import TaskState
from peerq.node import PeerNode
from peerq.transport import InMemoryTransport
from peerq.wal import WriteAheadLog
from tests.sim.engine import SimCluster


@pytest.mark.asyncio
async def test_randomized_chaos_fuzzing() -> None:
    """Run multi-seed chaos fuzzing asserting convergence under network & node churn."""
    seeds = [12345, 54321, 98765, 13579, 24680, 11223]

    async def echo_handler(payload: bytes) -> bytes:
        return payload + b"-ok"

    for seed in seeds:
        cluster = SimCluster(
            seed=seed,
            peer_ids=["p1", "p2", "p3", "p4"],
            handler=echo_handler,
        )
        await cluster.start()

        # Submit initial batch
        for i in range(4):
            submitter = f"p{(i % 4) + 1}"
            await cluster.submit_task(
                node_id=submitter,
                task_id=f"chaos-{seed}-{i}",
                payload=f"data-{seed}-{i}".encode(),
                priority=i,
            )

        # 1. Warm-up and establish heartbeats
        await cluster.run_for(3.0, step_size=0.2)

        # 2. Inject partition between {p1, p2} and {p3, p4}
        cluster.partition({"p1", "p2"}, {"p3", "p4"}, symmetric=True)
        await cluster.run_for(4.0, step_size=0.2)

        # 3. Crash a node in partition A
        await cluster.crash_node("p1")
        await cluster.run_for(5.0, step_size=0.2)

        # 4. Heal partition while p1 is still dead
        cluster.heal_partition({"p1", "p2"}, {"p3", "p4"}, symmetric=True)
        await cluster.run_for(6.0, step_size=0.2)

        # 5. Restart p1
        await cluster.restart_node("p1")
        await cluster.run_for(10.0, step_size=0.2)

        # 6. Verify invariants and full completion
        cluster.assert_all_submitted_tasks_completed()
        cluster.assert_eventual_consistency()

        await cluster.stop()


@pytest.mark.asyncio
async def test_asymmetric_partition_churn() -> None:
    """Verify convergence under directed asymmetric network partitions."""

    async def upper_handler(payload: bytes) -> bytes:
        return payload.upper()

    cluster = SimCluster(
        seed=9999,
        peer_ids=["a", "b", "c"],
        handler=upper_handler,
    )
    await cluster.start()

    # Heartbeat warm-up
    await cluster.run_for(3.0, step_size=0.2)

    # Submit tasks
    await cluster.submit_task("a", "task-asym-1", b"alpha")
    await cluster.submit_task("b", "task-asym-2", b"beta")

    # Asymmetric: a cannot send to b, but b can send to a
    cluster.partition({"a"}, {"b"}, symmetric=False)
    await cluster.run_for(5.0, step_size=0.2)

    # Submit third task while partitioned
    await cluster.submit_task("c", "task-asym-3", b"gamma")
    await cluster.run_for(5.0, step_size=0.2)

    # Heal asymmetric partition
    cluster.heal_partition({"a"}, {"b"}, symmetric=False)
    await cluster.run_for(8.0, step_size=0.2)

    cluster.assert_all_submitted_tasks_completed()
    cluster.assert_eventual_consistency()
    await cluster.stop()


@pytest.mark.asyncio
async def test_wal_simulation_reboot(tmp_path: Path) -> None:
    """Verify node crash and reboot with real WriteAheadLog under simulated network."""
    cluster = SimCluster(seed=7777, peer_ids=["w1", "w2"])
    wal1_path = tmp_path / "w1.wal"
    wal1 = WriteAheadLog(wal1_path)

    # Re-wire w1 to have WAL attached
    t1 = InMemoryTransport("w1", cluster.network)
    cluster.transports["w1"] = t1

    async def simple_handler(payload: bytes) -> bytes:
        return payload + b"-durable"

    w1_with_wal = PeerNode(
        "w1",
        cluster.clock,
        t1,
        cluster.node_rngs["w1"],
        peers=["w2"],
        handler=simple_handler,
        wal=wal1,
    )
    cluster.nodes["w1"] = w1_with_wal
    await cluster.start()

    # Submit task to w1
    await cluster.submit_task("w1", "task-wal-1", b"saveme")
    await cluster.run_for(2.0, step_size=0.2)

    task_w1 = cluster.nodes["w1"].get_task("task-wal-1")
    assert task_w1 is not None
    assert task_w1.state == TaskState.DONE

    # Crash w1 hard
    await cluster.nodes["w1"].stop()
    cluster.active_nodes.remove("w1")

    # Recreate w1 from same WAL
    wal1_reboot = WriteAheadLog(wal1_path)
    t1_reboot = InMemoryTransport("w1", cluster.network)
    w1_reboot = PeerNode(
        "w1",
        cluster.clock,
        t1_reboot,
        cluster.node_rngs["w1"],
        peers=["w2"],
        handler=simple_handler,
        wal=wal1_reboot,
    )
    cluster.nodes["w1"] = w1_reboot
    cluster.active_nodes.add("w1")
    await w1_reboot.start()

    # Verify task-wal-1 was restored from WAL immediately on start
    restored = w1_reboot.get_task("task-wal-1")
    assert restored is not None
    assert restored.state == TaskState.DONE
    assert restored.result == b"saveme-durable"

    await cluster.stop()
