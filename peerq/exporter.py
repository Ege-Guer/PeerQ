"""
Prometheus & OpenMetrics exposition exporter for peerq.

Translates internal MetricsCollector counters and LogLinearHistogram
quantiles into Prometheus text exposition format (version 0.0.4).
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from peerq.metrics import MetricsCollector

CONTENT_TYPE_PROMETHEUS: str = "text/plain; version=0.0.4; charset=utf-8"

COUNTER_METRIC_NAMES: dict[str, tuple[str, str]] = {
    "enqueued": ("peerq_tasks_enqueued_total", "Total tasks admitted into local queue"),
    "claimed": ("peerq_tasks_claimed_total", "Total tasks claimed by workers"),
    "completed": ("peerq_tasks_completed_total", "Total tasks successfully completed"),
    "failed": ("peerq_tasks_failed_total", "Total tasks that encountered errors"),
    "reclaimed": ("peerq_tasks_reclaimed_total", "Total tasks reclaimed after lease expiry"),
    "rejected": ("peerq_tasks_rejected_total", "Total stale task commits rejected by fencing"),
}

HISTOGRAM_METRIC_NAMES: dict[str, tuple[str, str]] = {
    "task_latency": ("peerq_task_execution_latency_seconds", "Task handler execution latency"),
    "claim_latency": ("peerq_claim_latency_seconds", "Task claim scheduling latency"),
    "gossip_latency": ("peerq_gossip_latency_seconds", "Gossip anti-entropy propagation latency"),
}


def format_prometheus_text(collector: MetricsCollector, node_id: str = "") -> str:
    """
    Render metrics in standard Prometheus text exposition format.
    """
    lines: list[str] = []
    labels = f'{{node="{node_id}"}}' if node_id else ""
    label_prefix = f'node="{node_id}",' if node_id else ""

    # 1. Operational Counters
    for counter_key, (metric_name, help_text) in COUNTER_METRIC_NAMES.items():
        val = collector.get_counter(counter_key)
        lines.append(f"# HELP {metric_name} {help_text}")
        lines.append(f"# TYPE {metric_name} counter")
        lines.append(f"{metric_name}{labels} {val}")

    # 2. Latency Histograms (as summaries with quantiles)
    for hist_key, (metric_name, help_text) in HISTOGRAM_METRIC_NAMES.items():
        hist = collector.get_histogram(hist_key)
        if hist is None:
            continue

        lines.append(f"# HELP {metric_name} {help_text}")
        lines.append(f"# TYPE {metric_name} summary")

        if hist.count == 0:
            lines.append(f'{metric_name}{{{label_prefix}quantile="0.5"}} 0.0')
            lines.append(f'{metric_name}{{{label_prefix}quantile="0.95"}} 0.0')
            lines.append(f'{metric_name}{{{label_prefix}quantile="0.99"}} 0.0')
            lines.append(f"{metric_name}_count{labels} 0")
            lines.append(f"{metric_name}_sum{labels} 0.0")
        else:
            # Latency values are recorded in microseconds, convert to seconds
            p50_sec = hist.quantile(0.50) / 1e6
            p95_sec = hist.quantile(0.95) / 1e6
            p99_sec = hist.quantile(0.99) / 1e6
            sum_sec = (hist.mean * hist.count) / 1e6

            lines.append(f'{metric_name}{{{label_prefix}quantile="0.5"}} {p50_sec:.6f}')
            lines.append(f'{metric_name}{{{label_prefix}quantile="0.95"}} {p95_sec:.6f}')
            lines.append(f'{metric_name}{{{label_prefix}quantile="0.99"}} {p99_sec:.6f}')
            lines.append(f"{metric_name}_count{labels} {hist.count}")
            lines.append(f"{metric_name}_sum{labels} {sum_sec:.6f}")

    lines.append("")  # trailing newline required by Prometheus exposition standard
    return "\n".join(lines)
