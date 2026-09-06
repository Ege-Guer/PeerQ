"""
Benchmark: End-to-end task turnaround latency.

Measures p50, p95, p99, p99.9 task execution latency using LogLinearHistogram.
Outputs system hardware, Python version, and percentile distribution.
"""

from __future__ import annotations

import asyncio
import platform
import random
import sys
import time

from peerq.clock import RealClock
from peerq.metrics import LogLinearHistogram
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


async def run_latency_benchmark(num_tasks: int = 3000) -> None:
    print_environment_header("Task Turnaround Latency Benchmark")

    clock = RealClock()
    net = SimNetwork(clock)
    hist = LogLinearHistogram(sub_bucket_bits=7)

    async def benchmark_handler(payload: bytes) -> bytes:
        return payload.upper()

    t1 = InMemoryTransport("p1", net)
    t2 = InMemoryTransport("p2", net)
    n1 = PeerNode("p1", clock, t1, random.Random(1), peers=["p2"], handler=benchmark_handler)
    n2 = PeerNode("p2", clock, t2, random.Random(2), peers=["p1"], handler=benchmark_handler)

    await n1.start()
    await n2.start()

    print(f"Executing {num_tasks} round-trip task executions...")

    for i in range(num_tasks):
        t0 = time.perf_counter()
        await n1.submit_task(f"t-lat-{i}", f"data-{i}".encode())

        # Wait until task completes
        while True:
            rec = n1.get_task(f"t-lat-{i}")
            if rec is not None and rec.state.is_terminal:
                break
            await asyncio.sleep(0.0001)

        t1_time = time.perf_counter()
        latency_us = (t1_time - t0) * 1e6
        hist.record(latency_us)

    await n1.stop()
    await n2.stop()

    print("\nTurnaround Latency Results (microseconds):")
    print(f"  Samples:      {hist.count:,}")
    print(f"  Min:          {hist.min:,.1f} µs")
    print(f"  Mean:         {hist.mean:,.1f} µs")
    print(f"  p50 (Median): {hist.quantile(0.50):,.1f} µs")
    print(f"  p90:          {hist.quantile(0.90):,.1f} µs")
    print(f"  p95:          {hist.quantile(0.95):,.1f} µs")
    print(f"  p99:          {hist.quantile(0.99):,.1f} µs")
    print(f"  p99.9:        {hist.quantile(0.999):,.1f} µs")
    print(f"  Max:          {hist.max:,.1f} µs")
    print(f"  Relative Error Bound: <= {hist.relative_error_bound * 100:.3f}%")
    print("=" * 80)


if __name__ == "__main__":
    asyncio.run(run_latency_benchmark())
