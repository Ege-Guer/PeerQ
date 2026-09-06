"""Unit tests for peerq.exporter (Prometheus text exporter)."""

from peerq.exporter import (
    CONTENT_TYPE_PROMETHEUS,
    format_prometheus_text,
)
from peerq.metrics import MetricsCollector


def test_content_type_constant() -> None:
    assert CONTENT_TYPE_PROMETHEUS == "text/plain; version=0.0.4; charset=utf-8"


def test_format_prometheus_empty() -> None:
    collector = MetricsCollector()
    output = format_prometheus_text(collector)

    assert output.endswith("\n")
    assert "# HELP peerq_tasks_enqueued_total" in output
    assert "# TYPE peerq_tasks_enqueued_total counter" in output
    assert "peerq_tasks_enqueued_total 0" in output

    assert "# HELP peerq_task_execution_latency_seconds" in output
    assert "# TYPE peerq_task_execution_latency_seconds summary" in output
    assert 'peerq_task_execution_latency_seconds{quantile="0.5"} 0.0' in output
    assert "peerq_task_execution_latency_seconds_count 0" in output


def test_format_prometheus_with_values() -> None:
    collector = MetricsCollector()
    collector.increment("enqueued", 10)
    collector.increment("completed", 7)
    collector.increment("failed", 3)

    # Record latency: 1000us = 0.001s, 2000us = 0.002s
    collector.record_latency("task_latency", 1000.0)
    collector.record_latency("task_latency", 2000.0)

    output = format_prometheus_text(collector, node_id="worker-1")

    assert 'peerq_tasks_enqueued_total{node="worker-1"} 10' in output
    assert 'peerq_tasks_completed_total{node="worker-1"} 7' in output
    assert 'peerq_tasks_failed_total{node="worker-1"} 3' in output

    assert 'peerq_task_execution_latency_seconds{node="worker-1",quantile="0.5"}' in output
    assert 'peerq_task_execution_latency_seconds_count{node="worker-1"} 2' in output
    assert 'peerq_task_execution_latency_seconds_sum{node="worker-1"}' in output
