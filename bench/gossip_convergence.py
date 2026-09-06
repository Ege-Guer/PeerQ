"""
PeerQ Gossip Anti-Entropy Convergence Benchmark.

Measures:
1. Anti-entropy convergence latency across 3, 5, and 10 node mesh topologies.
2. Rounds to full cluster state consensus under distributed concurrent task ingestion.
3. Message volume and byte overhead during gossip synchronization.

Honesty rule: Commit raw unedited output to bench/results/convergence_raw.txt.
"""

from __future__ import annotations

import asyncio
import platform
import random
import sys
import time
from pathlib import Path

# Ensure project root is on sys.path
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from peerq.clock import Clock, SimClock  # noqa: E402
from peerq.node import PeerNode  # noqa: E402
from peerq.transport import InMemoryTransport, Message, SimNetwork  # noqa: E402


class BenchSimNetwork(SimNetwork):
    """Simulation network tracking message and byte counts for benchmarking."""

    def __init__(self, clock: Clock, rng: random.Random | None = None) -> None:
        super().__init__(clock, rng)
        self.messages_routed: int = 0
        self.bytes_routed: int = 0

    async def route_message(self, source: str, target: str, msg: Message) -> None:
        self.messages_routed += 1
        self.bytes_routed += len(msg.to_bytes())
        await super().route_message(source, target, msg)


async def measure_convergence_for_topology(
    node_count: int,
    task_count: int,
    gossip_interval: float = 0.5,
    seed: int = 42,
) -> dict[str, float]:
    clock = SimClock(100.0)
    rng = random.Random(seed)
    net = BenchSimNetwork(clock, rng=rng)

    node_ids = [f"peer-{i}" for i in range(node_count)]
    nodes: dict[str, PeerNode] = {}

    for nid in node_ids:
        transport = InMemoryTransport(nid, net)
        peers = [p for p in node_ids if p != nid]
        node = PeerNode(
            node_id=nid,
            clock=clock,
            transport=transport,
            rng=random.Random(rng.randint(0, 1_000_000)),
            peers=peers,
            gossip_interval=gossip_interval,
        )
        nodes[nid] = node

    # Start all nodes
    for node in nodes.values():
        await node.start()

    # Distribute tasks across nodes
    for i in range(task_count):
        chosen_node = nodes[node_ids[i % node_count]]
        await chosen_node.submit_task(
            f"bench-conv-task-{i}",
            f"task-payload-{i}".encode(),
        )

    # Measure rounds and clock time until 100% convergence
    t_start = clock.now()
    wall_start = time.perf_counter()
    rounds = 0
    max_rounds = 200

    def is_converged() -> bool:
        first_tasks = nodes[node_ids[0]].all_tasks()
        if len(first_tasks) != task_count:
            return False
        # Check all nodes have exactly the same tasks in same states
        for nid in node_ids[1:]:
            peer_tasks = nodes[nid].all_tasks()
            if len(peer_tasks) != task_count:
                return False
            for tid, t_record in first_tasks.items():
                if tid not in peer_tasks:
                    return False
                if peer_tasks[tid].state != t_record.state:
                    return False
        return True

    while not is_converged() and rounds < max_rounds:
        rounds += 1
        clock.advance(gossip_interval)
        await clock.sleep(0)

    t_converged = clock.now() - t_start
    wall_elapsed = time.perf_counter() - wall_start
    assert is_converged(), f"Topology with {node_count} nodes failed to converge!"

    # Clean up
    for node in nodes.values():
        await node.stop()

    return {
        "node_count": float(node_count),
        "task_count": float(task_count),
        "virtual_time_sec": t_converged,
        "wall_time_sec": wall_elapsed,
        "rounds": float(rounds),
        "messages_sent": float(net.messages_routed),
        "bytes_sent": float(net.bytes_routed),
    }


def run_gossip_benchmarks() -> None:
    print("=" * 65)
    print("PEERQ GOSSIP ANTI-ENTROPY CONVERGENCE BENCHMARK")
    print("=" * 65)
    print(f"Timestamp:       {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}")
    print(f"Platform:        {platform.platform()}")
    print(f"Processor:       {platform.processor() or platform.machine()}")
    print(f"Python:          {sys.version.split()[0]} ({platform.python_implementation()})")
    print(f"Command line:    {' '.join(sys.argv)}")
    print("=" * 65)

    test_topologies = [(3, 30), (5, 50), (10, 100)]

    for node_count, task_count in test_topologies:
        res = asyncio.run(
            measure_convergence_for_topology(
                node_count=node_count,
                task_count=task_count,
                gossip_interval=0.5,
                seed=42 + node_count,
            )
        )
        print(f"Mesh Topology: {node_count} Nodes | {task_count} Distributed Tasks")
        print(f"   Convergence Virtual Time: {res['virtual_time_sec']:.2f} s")
        print(f"   Convergence Wall Time:    {res['wall_time_sec'] * 1000.0:.2f} ms")
        print(f"   Rounds to Convergence:    {int(res['rounds'])}")
        print(f"   Total Messages Exchanged: {int(res['messages_sent']):,}")
        print(f"   Total Volume Exchanged:   {res['bytes_sent'] / 1024.0:,.1f} KB")
        per_task_ms = (res["virtual_time_sec"] / task_count) * 1000.0
        print(f"   Per-Task Convergence:     {per_task_ms:.2f} ms/task")
        print()

    print("=" * 65)
    print("PEERQ CONVERGENCE BENCHMARK COMPLETED SUCCESSFULLY")
    print("=" * 65)


if __name__ == "__main__":
    run_gossip_benchmarks()
