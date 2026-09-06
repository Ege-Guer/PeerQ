"""Unit tests for peerq.metrics."""

import pytest

from peerq.metrics import LogLinearHistogram, MetricsCollector


def test_histogram_basic_stats() -> None:
    hist = LogLinearHistogram(sub_bucket_bits=7)
    assert hist.count == 0
    assert hist.min == 0.0
    assert hist.max == 0.0
    assert hist.mean == 0.0
    assert hist.quantile(0.50) == 0.0

    values = [10.0, 20.0, 30.0, 40.0, 50.0]
    for v in values:
        hist.record(v)

    assert hist.count == 5
    assert hist.min == 10.0
    assert hist.max == 50.0
    assert hist.mean == 30.0

    # Quantiles should be within expected range
    assert 10.0 <= hist.quantile(0.50) <= 50.0
    assert hist.quantile(0.0) == 10.0
    assert hist.quantile(1.0) == 50.0


def test_histogram_negative_value_raises() -> None:
    hist = LogLinearHistogram()
    with pytest.raises(ValueError, match="non-negative"):
        hist.record(-1.0)


def test_histogram_error_bound_constant() -> None:
    hist = LogLinearHistogram(sub_bucket_bits=7)
    # 1 / 128 = 0.0078125 ≈ 0.78%
    assert hist.relative_error_bound == pytest.approx(1.0 / 128)


def test_metrics_collector_counters() -> None:
    metrics = MetricsCollector()
    for name in MetricsCollector.COUNTER_NAMES:
        assert metrics.get_counter(name) == 0

    metrics.increment("enqueued", 10)
    metrics.increment("claimed", 8)
    metrics.increment("completed", 6)
    metrics.increment("failed", 1)
    metrics.increment("reclaimed", 1)
    metrics.increment("rejected", 2)

    assert metrics.get_counter("enqueued") == 10
    assert metrics.get_counter("claimed") == 8
    assert metrics.get_counter("completed") == 6
    assert metrics.get_counter("failed") == 1
    assert metrics.get_counter("reclaimed") == 1
    assert metrics.get_counter("rejected") == 2

    # Latencies
    metrics.record_latency("task_latency", 1500.0)
    metrics.record_latency("task_latency", 2500.0)

    snap = metrics.snapshot()
    assert snap.counters["enqueued"] == 10
    assert snap.latencies["task_latency"]["count"] == 2.0
    assert snap.latencies["task_latency"]["min"] == 1500.0
    assert snap.latencies["task_latency"]["max"] == 2500.0
