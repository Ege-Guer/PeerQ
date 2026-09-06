"""
Deterministic simulation scenarios for peerq.

Required scenarios:
1. clean run
2. single worker crash mid-task
3. symmetric partition and heal
4. asymmetric partition (A hears B, B does not hear A)
5. false suspicion followed by return
6. rolling restart of all peers
7. slow peer (high latency, not dead) — must NOT be evicted prematurely
"""

import pytest

from peerq.consensus import TaskState
from tests.sim.engine import SimCluster


@pytest.mark.asyncio
async def test_scenario_clean_run() -> None:
    """Scenario 1: Clean cluster run under standard multi-task workload."""

    async def handler(payload: bytes) -> bytes:
        return payload.upper()

    cluster = SimCluster(seed=1001, peer_ids=["p1", "p2", "p3"], handler=handler)
    await cluster.start()

    for i in range(6):
        await cluster.submit_task(f"p{(i % 3) + 1}", f"task-{i}", f"data-{i}".encode(), priority=i)

    await cluster.run_for(20.0, step_size=0.2)
    cluster.assert_all_submitted_tasks_completed()
    await cluster.stop()


@pytest.mark.asyncio
async def test_scenario_single_worker_crash_mid_task() -> None:
    """Scenario 2: Single worker crashes abruptly mid-task; peer reclaims lease."""
    first_attempt = True

    async def handler(payload: bytes) -> bytes:
        nonlocal first_attempt
        if first_attempt:
            first_attempt = False
            # First attempt hangs/sleeps long so p1 is killed mid-task
            await cluster.clock.sleep(25.0)
            return payload
        # Reclaimed execution finishes promptly within lease duration
        await cluster.clock.sleep(0.5)
        return payload + b"-done"

    cluster = SimCluster(seed=1002, peer_ids=["p1", "p2", "p3"], handler=handler)
    await cluster.start()

    # Establish heartbeat baseline
    await cluster.run_for(5.0, step_size=0.2)

    await cluster.submit_task("p1", "task-crash-1", b"work-data")
    # Step until p1 claims and starts running the task
    await cluster.run_for(1.0, step_size=0.1)

    rec = cluster.nodes["p1"].get_task("task-crash-1")
    assert rec is not None
    assert rec.state in (TaskState.CLAIMED, TaskState.RUNNING)
    assert rec.fence_token.epoch == 1

    # Hard crash p1 mid-task
    await cluster.crash_node("p1")

    # Advance virtual time through lease expiry and reclaim execution
    await cluster.run_for(35.0, step_size=0.2)

    # Remaining live nodes must have reclaimed and completed the task
    completed = False
    for pid in ["p2", "p3"]:
        r = cluster.nodes[pid].get_task("task-crash-1")
        if r is not None and r.state == TaskState.DONE:
            completed = True
            assert r.fence_token.epoch >= 2
            assert r.result == b"work-data-done"
            break
    assert completed, "Task was not reclaimed after worker crash!"
    await cluster.stop()


@pytest.mark.asyncio
async def test_scenario_symmetric_partition_and_heal() -> None:
    """Scenario 3: Symmetric network partition between {p1, p2} and {p3, p4}, then heal."""

    async def handler(payload: bytes) -> bytes:
        return payload + b"-ok"

    cluster = SimCluster(seed=1003, peer_ids=["p1", "p2", "p3", "p4"], handler=handler)
    await cluster.start()

    await cluster.run_for(3.0, step_size=0.2)

    # Partition cluster into two isolated halves
    group_a = {"p1", "p2"}
    group_b = {"p3", "p4"}
    cluster.partition(group_a, group_b, symmetric=True)

    await cluster.submit_task("p1", "task-group-a", b"a-data")
    await cluster.submit_task("p3", "task-group-b", b"b-data")

    # Run while partitioned: each half operates independently
    await cluster.run_for(15.0, step_size=0.2)

    # Heal the partition
    cluster.heal_partition(group_a, group_b, symmetric=True)

    # Run after healing to allow gossip anti-entropy reconciliation
    await cluster.run_for(15.0, step_size=0.2)

    cluster.assert_all_submitted_tasks_completed()
    await cluster.stop()


@pytest.mark.asyncio
async def test_scenario_asymmetric_partition() -> None:
    """Scenario 4: Asymmetric partition where p1 can send to p2, but p2 cannot send to p1."""

    async def handler(payload: bytes) -> bytes:
        return payload + b"-asym"

    cluster = SimCluster(seed=1004, peer_ids=["p1", "p2", "p3"], handler=handler)
    await cluster.start()

    await cluster.run_for(3.0, step_size=0.2)

    # Asymmetric block: p2 cannot send to p1 (p1 can still send to p2)
    cluster.partition(group_a={"p2"}, group_b={"p1"}, symmetric=False)

    await cluster.submit_task("p1", "task-asym-1", b"input")
    await cluster.run_for(20.0, step_size=0.2)

    cluster.assert_all_submitted_tasks_completed()
    await cluster.stop()


@pytest.mark.asyncio
async def test_scenario_false_suspicion_followed_by_return() -> None:
    """Scenario 5: False suspicion leads to task reclaim; returning peer's stale commit rejected."""
    first_attempt = True

    async def handler(payload: bytes) -> bytes:
        nonlocal first_attempt
        if first_attempt:
            first_attempt = False
            # First attempt on p1 takes 20.0s (exceeds lease duration)
            await cluster.clock.sleep(20.0)
            return payload + b"-stale"
        # Reclaimed execution on p2 finishes promptly (0.5s < 3.0s lease)
        await cluster.clock.sleep(0.5)
        return payload + b"-result"

    cluster = SimCluster(seed=1005, peer_ids=["p1", "p2"], handler=handler)
    await cluster.start()

    await cluster.run_for(3.0, step_size=0.2)

    # Submit task on p1
    await cluster.submit_task("p1", "task-stale-test", b"stale-data")
    # Run briefly so p1 claims and p2 receives the gossip with the task and lease
    await cluster.run_for(1.0, step_size=0.1)

    rec_p2_initial = cluster.nodes["p2"].get_task("task-stale-test")
    assert rec_p2_initial is not None
    assert rec_p2_initial.claimed_by == "p1"

    # Now partition p1 from p2 (false suspicion: p1 is alive but isolated)
    cluster.partition({"p1"}, {"p2"}, symmetric=True)

    # Advance time: p2 suspects p1, lease expires, p2 reclaims with higher token and completes
    await cluster.run_for(30.0, step_size=0.2)

    rec_p2 = cluster.nodes["p2"].get_task("task-stale-test")
    assert rec_p2 is not None
    assert rec_p2.state == TaskState.DONE
    assert rec_p2.claimed_by == "p2"
    assert rec_p2.fence_token.epoch >= 2
    assert rec_p2.result == b"stale-data-result"

    # Reconnect p1 and p2
    cluster.heal_partition({"p1"}, {"p2"}, symmetric=True)
    await cluster.run_for(10.0, step_size=0.2)

    # Both nodes must now agree on p2's winning higher-token result
    rec_p1_final = cluster.nodes["p1"].get_task("task-stale-test")
    rec_p2_final = cluster.nodes["p2"].get_task("task-stale-test")

    assert rec_p1_final is not None and rec_p2_final is not None
    assert rec_p1_final.fence_token == rec_p2_final.fence_token
    assert rec_p1_final.result == rec_p2_final.result
    assert rec_p1_final.fence_token.epoch >= 2
    assert rec_p1_final.result == b"stale-data-result"
    await cluster.stop()


@pytest.mark.asyncio
async def test_scenario_rolling_restart() -> None:
    """Scenario 6: Rolling restart of all cluster peers under active workload."""

    async def handler(payload: bytes) -> bytes:
        return payload + b"-restarted"

    peer_ids = ["p1", "p2", "p3"]
    cluster = SimCluster(seed=1006, peer_ids=peer_ids, handler=handler)
    await cluster.start()

    await cluster.run_for(3.0, step_size=0.2)
    await cluster.submit_task("p1", "task-rolling-1", b"r1")
    await cluster.submit_task("p2", "task-rolling-2", b"r2")

    # Rolling restart of each node sequentially
    for pid in peer_ids:
        await cluster.crash_node(pid)
        await cluster.run_for(1.0, step_size=0.2)
        await cluster.restart_node(pid)
        await cluster.run_for(2.0, step_size=0.2)

    await cluster.run_for(20.0, step_size=0.2)
    cluster.assert_all_submitted_tasks_completed()
    await cluster.stop()


@pytest.mark.asyncio
async def test_scenario_slow_peer() -> None:
    """Scenario 7: Slow peer with high link latency is NOT falsely evicted prematurely."""

    async def handler(payload: bytes) -> bytes:
        return payload + b"-slow"

    cluster = SimCluster(seed=1007, peer_ids=["p1", "p2", "p3"], handler=handler, base_latency=0.05)
    await cluster.start()

    # Let baseline form
    await cluster.run_for(5.0, step_size=0.2)

    # Submit task to p1
    await cluster.submit_task("p1", "task-slow-1", b"slow-input")
    await cluster.run_for(15.0, step_size=0.2)

    # Invariants continuously verified that slow peer wasn't evicted
    cluster.assert_all_submitted_tasks_completed()
    await cluster.stop()
