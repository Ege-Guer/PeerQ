"""
Prometheus & OpenMetrics exposition exporter for peerq.

Translates internal MetricsCollector counters and LogLinearHistogram
quantiles into Prometheus text exposition format (version 0.0.4).
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from peerq.consensus import TaskState
from peerq.transport import HttpServer

if TYPE_CHECKING:
    from peerq.metrics import MetricsCollector
    from peerq.node import PeerNode

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


def format_cluster_status(node: PeerNode) -> dict[str, Any]:
    """
    Format internal node and cluster state into a JSON-serializable dictionary.
    Includes topology (known, live, suspected peers), queue depth, task counts by state,
    active task leases, and vector clock state.
    """
    now = node.clock.now()
    tasks = node.all_tasks()

    live_peers: list[str] = []
    suspected_peers: list[str] = []
    for peer in node.peers:
        if node.failure_detector.is_suspected(peer, timestamp=now):
            suspected_peers.append(peer)
        else:
            live_peers.append(peer)

    by_state: dict[str, int] = {state.value: 0 for state in TaskState}
    active_leases: list[dict[str, Any]] = []

    for t in tasks.values():
        by_state[t.state.value] = by_state.get(t.state.value, 0) + 1
        if t.state in (TaskState.CLAIMED, TaskState.RUNNING):
            active_leases.append(
                {
                    "task_id": t.task_id,
                    "claimed_by": t.claimed_by,
                    "fence_epoch": t.fence_token.epoch,
                    "fence_peer": t.fence_token.peer_id,
                    "lease_expiry": t.lease_expiry,
                }
            )

    credits: dict[str, int] = {}
    for peer in node.peers:
        credits[peer] = node.flow_controller.get_credits(peer)

    return {
        "node_id": node.node_id,
        "cluster_topology": {
            "known_peers": list(node.peers),
            "live_peers": live_peers,
            "suspected_peers": suspected_peers,
        },
        "queue": {
            "depth": node._queue.qsize(),
            "peer_credits": credits,
        },
        "tasks": {
            "total": len(tasks),
            "by_state": by_state,
            "active_leases": active_leases,
        },
        "vector_clock": node._vector_clock.to_dict(),
    }


class StatusServer:
    """
    Lightweight HTTP dashboard and status endpoint for peerq.
    Pure asyncio HTTP without third-party frameworks.

    Endpoints:
    - /metrics: Prometheus text exposition format (counters + latency summaries)
    - /status: JSON payload describing topology, task states, leases, and queues
    - /healthz: Liveness check returning 200 OK
    """

    def __init__(self, node: PeerNode, host: str = "127.0.0.1", port: int = 9102) -> None:
        self.node = node
        self.host = host
        self.port = port
        self._server = HttpServer(host, port, self._handle_request)

    def _handle_request(self, method: str, path: str) -> tuple[int, str, bytes]:
        if method != "GET":
            return 405, "text/plain", b"Method Not Allowed\n"

        if path == "/metrics":
            text = format_prometheus_text(self.node.metrics, self.node.node_id)
            return 200, CONTENT_TYPE_PROMETHEUS, text.encode("utf-8")

        if path == "/status":
            data = format_cluster_status(self.node)
            body = json.dumps(data, indent=2, sort_keys=True).encode("utf-8")
            return 200, "application/json", body

        if path == "/healthz":
            return 200, "text/plain", b"OK\n"

        return 404, "text/plain", b"Not Found\n"

    async def start(self) -> None:
        """Start the status HTTP server."""
        await self._server.start()
        self.port = self._server.port

    async def stop(self) -> None:
        """Stop the status HTTP server."""
        await self._server.stop()
