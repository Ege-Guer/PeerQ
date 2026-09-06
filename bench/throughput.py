"""
Benchmark: End-to-end task mesh throughput.

Measures tasks/sec under credit-based flow control and reject-on-full policies.
Outputs system hardware, Python version, and exact measurements.
"""

from __future__ import annotations

import asyncio
import platform
import random
import sys
import time

from peerq.clock import RealClock
from peerq.node import PeerNode
from peerq.transport import InMemoryTransport, SimNetwork


def print_environment_header(bench_name: str) -> None:
    print("=" * 80)
    print(f"PEERQ BENCHMARK: {bench_name}")
    print(f"Timestamp:   {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}")
    print(f"Python:      {sys.version.split()[0]} ({platform.python_implementation()})")
    print(f"Platform:    {platform.system()} {platform.release()} ({platform.machine()})")
    print(f"Processor:   {platform.processor()}")
    print(f"Command:     {sys.executable} {' '.join(sys.argv)}")
    print("=" * 80)


async def run_throughput_benchmark(
    num_tasks: int = 5000,
    num_peers: int = 3,
) -> None:
    print_environment_header("Throughput Benchmark")

    clock = RealClock()
    net = SimNetwork(clock)
    peer_ids = [f"peer-{i}" for i in range(num_peers)]

    async def fast_handler(payload: bytes) -> bytes:
        return payload.upper()

    nodes: list[PeerNode] = []
    for pid in peer_ids:
        transport = InMemoryTransport(pid, net)
        rng = random.Random(42)
        node = PeerNode(
            node_id=pid,
            clock=clock,
            transport=transport,
            rng=rng,
            peers=peer_ids,
            handler=fast_handler,
            max_queue_size=10000,
            initial_credits=500,
        )
        nodes.append(node)
        await node.start()

    print(f"Cluster initialized with {num_peers} peers.")
    print(f"Submitting and processing {num_tasks} tasks...")

    t_start = time.perf_counter()

    # Submit tasks distributed across peers
    for i in range(num_tasks):
        target = nodes[i % num_peers]
        await target.submit_task(f"task-{i}", f"payload-{i}".encode())

    # Wait until all tasks are completed
    while True:
        completed = sum(n.metrics.get_counter("completed") for n in nodes)
        if completed >= num_tasks:
            break
        await asyncio.sleep(0.001)

    t_end = time.perf_counter()
    duration = t_end - t_start
    throughput = num_tasks / duration

    for node in nodes:
        await node.stop()

    print("\nResults:")
    print(f"  Total Tasks:       {num_tasks:,}")
    print(f"  Elapsed Time:      {duration:.4f} s")
    print(f"  Throughput:        {throughput:,.2f} tasks/sec")
    print(f"  Avg Latency/Task:  {(duration / num_tasks) * 1e6:.2f} µs")
    print("=" * 80)


if __name__ == "__main__":
    asyncio.run(run_throughput_benchmark())
