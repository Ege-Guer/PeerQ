"""
Benchmark: Instrumentation & Telemetry Overhead.

Measures the CPU overhead of LogLinearHistogram, MetricsCollector,
and VectorClock operations versus raw uninstrumented baseline.
Outputs exact hardware, Python version, and nanosecond per operation.
"""

from __future__ import annotations

import platform
import sys
import time

from peerq.consensus import VectorClock
from peerq.metrics import LogLinearHistogram, MetricsCollector


def print_environment_header(bench_name: str) -> None:
    print("=" * 80)
    print(f"PEERQ BENCHMARK: {bench_name}")
    print(f"Timestamp:   {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}")
    print(f"Python:      {sys.version.split()[0]} ({platform.python_implementation()})")
    print(f"Platform:    {platform.system()} {platform.release()} ({platform.machine()})")
    print(f"Processor:   {platform.processor()}")
    print(f"Command:     {sys.executable} {' '.join(sys.argv)}")
    print("=" * 80)


def run_overhead_benchmark(iterations: int = 500_000) -> None:
    print_environment_header("Telemetry & Instrumentation Overhead")
    print(f"Running {iterations:,} iterations per benchmark component...\n")

    # 1. Baseline empty loop
    t0 = time.perf_counter_ns()
    val = 0
    for i in range(iterations):
        val += i
    t1 = time.perf_counter_ns()
    baseline_ns = (t1 - t0) / iterations

    # 2. Histogram record overhead
    hist = LogLinearHistogram(sub_bucket_bits=7)
    sample_val = 1450.0
    t0 = time.perf_counter_ns()
    for _ in range(iterations):
        hist.record(sample_val)
    t1 = time.perf_counter_ns()
    hist_record_ns = ((t1 - t0) / iterations) - baseline_ns

    # 3. MetricsCollector increment overhead
    collector = MetricsCollector()
    t0 = time.perf_counter_ns()
    for _ in range(iterations):
        collector.increment("completed")
    t1 = time.perf_counter_ns()
    counter_inc_ns = ((t1 - t0) / iterations) - baseline_ns

    # 4. VectorClock increment overhead
    vc = VectorClock({"p1": 1, "p2": 5, "p3": 12})
    t0 = time.perf_counter_ns()
    for _ in range(iterations):
        vc = vc.increment("p1")
    t1 = time.perf_counter_ns()
    vc_inc_ns = ((t1 - t0) / iterations) - baseline_ns

    print("Measured Instrumentation Overhead (per operation):")
    print(f"  LogLinearHistogram.record():  {max(0.0, hist_record_ns):.2f} ns")
    print(f"  MetricsCollector.increment(): {max(0.0, counter_inc_ns):.2f} ns")
    print(f"  VectorClock.increment():      {max(0.0, vc_inc_ns):.2f} ns")
    print(f"  Baseline loop overhead:       {baseline_ns:.2f} ns")
    print("=" * 80)


if __name__ == "__main__":
    run_overhead_benchmark()
