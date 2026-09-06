"""
End-to-End Demo: Multi-Peer Leaderless Task Mesh.

Demonstrates:
- Initializing a 3-peer mesh.
- Submitting prioritized tasks to different peers.
- Autonomous gossip replication and execution by peer workers.
- Real-time task completion tracking and telemetry reporting.
"""

from __future__ import annotations

import asyncio
import random
import time

from peerq.clock import RealClock
from peerq.consensus import TaskRecord, TaskState
from peerq.node import PeerNode
from peerq.transport import InMemoryTransport, SimNetwork


async def task_handler(payload: bytes) -> bytes:
    """Simulate realistic CPU/IO task workload."""
    text = payload.decode("utf-8")
    await asyncio.sleep(0.01)  # Real time delay
    return f"PROCESSED[{text.upper()}]".encode("utf-8")


def _is_task_done(rec: TaskRecord | None) -> bool:
    return rec is not None and rec.state == TaskState.DONE


async def main() -> None:
    print("=" * 70)
    print("peerq: Leaderless Distributed Task Mesh Demo")
    print("=" * 70)

    clock = RealClock()
    net = SimNetwork(clock)
    peer_ids = ["node-alpha", "node-beta", "node-gamma"]

    print(f"\n1. Initializing 3 equal peers: {peer_ids}...")
    nodes: list[PeerNode] = []
    for pid in peer_ids:
        transport = InMemoryTransport(pid, net)
        rng = random.Random(hash(pid))
        node = PeerNode(
            node_id=pid,
            clock=clock,
            transport=transport,
            rng=rng,
            peers=peer_ids,
            handler=task_handler,
            lease_duration=2.0,
            heartbeat_interval=0.5,
            gossip_interval=0.2,
        )
        nodes.append(node)
        await node.start()

    print("2. Submitting 12 prioritized tasks across peers...")
    task_ids: list[str] = []
    for i in range(12):
        tid = f"job-{i:02d}"
        task_ids.append(tid)
        priority = (i % 3) * 5  # priorities 0, 5, 10
        submitter = nodes[i % len(nodes)]
        payload = f"item-{i}".encode("utf-8")
        await submitter.submit_task(tid, payload, priority=priority)
        print(f"   [Submitted] {tid} -> {submitter.node_id} (priority={priority})")

    print("\n3. Processing tasks collaboratively across mesh...")
    t_start = time.perf_counter()

    # Poll until all tasks complete
    while True:
        completed_count = 0
        for tid in task_ids:
            done = any(_is_task_done(n.get_task(tid)) for n in nodes)
            if done:
                completed_count += 1

        if completed_count >= len(task_ids):
            break
        await asyncio.sleep(0.02)

    elapsed = time.perf_counter() - t_start
    print(f"\nAll {len(task_ids)} tasks completed in {elapsed:.3f} seconds!")

    print("\n4. Final Task Execution Summary:")
    print("-" * 70)
    for tid in task_ids:
        # Get record from alpha
        rec = nodes[0].get_task(tid)
        if rec is None:
            continue
        result_str = rec.result.decode("utf-8") if rec.result else "None"
        print(
            f"   {tid:8s} | Worker: {rec.claimed_by:12s} | "
            f"FenceToken: ({rec.fence_token.epoch}, {rec.fence_token.peer_id}) | "
            f"Result: {result_str}"
        )
    print("-" * 70)

    print("\n5. Local Node Telemetry Snapshots:")
    for n in nodes:
        snap = n.metrics.snapshot()
        print(
            f"   [{n.node_id:10s}] Enqueued: {snap.counters['enqueued']:2d} | "
            f"Claimed: {snap.counters['claimed']:2d} | "
            f"Completed: {snap.counters['completed']:2d}"
        )

    print("\nShutting down mesh...")
    for n in nodes:
        await n.stop()

    print("Demo finished successfully.\n")


if __name__ == "__main__":
    asyncio.run(main())
