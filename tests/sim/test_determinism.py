"""
Determinism verification test for peerq simulation.

Asserts the non-negotiable guarantee:
Same seed => byte-identical event trace.
"""

import pytest

from tests.sim.engine import SimCluster


@pytest.mark.asyncio
async def test_simulation_determinism() -> None:
    seed = 8472

    async def sample_handler(payload: bytes) -> bytes:
        return payload + b"-done"

    # Run 1
    cluster1 = SimCluster(seed=seed, peer_ids=["p1", "p2", "p3"], handler=sample_handler)
    await cluster1.start()
    await cluster1.submit_task("p1", "task-det-1", b"input-1", priority=5)
    await cluster1.submit_task("p2", "task-det-2", b"input-2", priority=10)
    await cluster1.run_for(15.0, step_size=0.2)
    await cluster1.stop()
    trace1 = cluster1.get_trace_bytes()

    # Run 2 with identical seed
    cluster2 = SimCluster(seed=seed, peer_ids=["p1", "p2", "p3"], handler=sample_handler)
    await cluster2.start()
    await cluster2.submit_task("p1", "task-det-1", b"input-1", priority=5)
    await cluster2.submit_task("p2", "task-det-2", b"input-2", priority=10)
    await cluster2.run_for(15.0, step_size=0.2)
    await cluster2.stop()
    trace2 = cluster2.get_trace_bytes()

    assert len(trace1) > 0
    assert trace1 == trace2, (
        "Simulation is non-deterministic: identical seeds produced differing event traces!"
    )
