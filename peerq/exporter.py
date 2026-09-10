"""
Prometheus & OpenMetrics exposition exporter for peerq.

Translates internal MetricsCollector counters and LogLinearHistogram
quantiles into Prometheus text exposition format (version 0.0.4).
"""

from __future__ import annotations

import html
import json
from typing import TYPE_CHECKING, Any

from peerq.consensus import TaskState
from peerq.security import SecurityConfig
from peerq.transport import HttpServer

if TYPE_CHECKING:
    from peerq.metrics import MetricsCollector
    from peerq.node import PeerNode

CONTENT_TYPE_PROMETHEUS: str = "text/plain; version=0.0.4; charset=utf-8"
CONTENT_TYPE_HTML: str = "text/html; charset=utf-8"

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


def _format_quantile(hist: Any, q: float) -> str:
    if hist is None or hist.count == 0:
        return "N/A"
    return f"{hist.quantile(q) / 1000.0:.2f} ms"


def get_dashboard_html(node: PeerNode) -> str:
    """
    Render a responsive, self-contained HTML dashboard for the PeerQ node.
    Displays live cluster topology, task distribution, active leases,
    vector clocks, flow control credits, and operational latencies.
    """
    status = format_cluster_status(node)
    node_id = html.escape(str(node.node_id))
    now = node.clock.now()

    topology = status["cluster_topology"]
    known_peers = [html.escape(str(p)) for p in topology["known_peers"]]
    live_peers = [html.escape(str(p)) for p in topology["live_peers"]]
    suspected_peers = [html.escape(str(p)) for p in topology["suspected_peers"]]

    queue_depth = status["queue"]["depth"]
    peer_credits = status["queue"]["peer_credits"]

    tasks = status["tasks"]
    total_tasks = tasks["total"]
    by_state = tasks["by_state"]
    active_leases = tasks["active_leases"]
    vclock = status["vector_clock"]

    # Prometheus counters
    enqueued = node.metrics.get_counter("enqueued")
    claimed = node.metrics.get_counter("claimed")
    completed = node.metrics.get_counter("completed")
    failed = node.metrics.get_counter("failed")
    reclaimed = node.metrics.get_counter("reclaimed")
    rejected = node.metrics.get_counter("rejected")

    # Latencies
    task_hist = node.metrics.get_histogram("task_latency")
    task_p50 = _format_quantile(task_hist, 0.50)
    task_p99 = _format_quantile(task_hist, 0.99)

    gossip_hist = node.metrics.get_histogram("gossip_latency")
    gossip_p50 = _format_quantile(gossip_hist, 0.50)
    gossip_p99 = _format_quantile(gossip_hist, 0.99)

    # Format peers HTML
    if known_peers:
        peer_badges: list[str] = []
        for p in known_peers:
            if p in live_peers:
                peer_badges.append(
                    f'<span class="badge badge-success">'
                    f'<span class="dot dot-green"></span>{p} (live)</span>'
                )
            elif p in suspected_peers:
                peer_badges.append(
                    f'<span class="badge badge-warning">'
                    f'<span class="dot dot-amber"></span>{p} (suspected)</span>'
                )
            else:
                peer_badges.append(f'<span class="badge badge-neutral">{p}</span>')
        peers_html = " ".join(peer_badges)
    else:
        peers_html = '<span class="text-muted">Single node (standalone mode)</span>'

    # Format leases HTML table
    if active_leases:
        lease_rows: list[str] = []
        for lease in active_leases:
            tid = html.escape(str(lease["task_id"]))
            cby = html.escape(str(lease["claimed_by"]))
            fepoch = lease["fence_epoch"]
            exp = lease["lease_expiry"]
            rem = max(0.0, exp - now)
            lease_rows.append(
                f"<tr><td><code>{tid}</code></td><td><code>{cby}</code></td>"
                f"<td>{fepoch}</td><td>{exp:.2f}s</td><td>{rem:.2f}s</td></tr>"
            )
        leases_table = (
            "<table><thead><tr><th>Task ID</th><th>Claimed By</th><th>Fence Epoch</th>"
            "<th>Lease Expiry</th><th>Remaining</th></tr></thead><tbody>"
            + "".join(lease_rows)
            + "</tbody></table>"
        )
    else:
        leases_table = '<p class="text-muted">No active leases currently held.</p>'

    # Format vector clock HTML
    if vclock:
        vc_badges = [
            f'<span class="badge badge-neutral"><code>{html.escape(k)}</code>: {v}</span>'
            for k, v in sorted(vclock.items())
        ]
        vclock_html = " ".join(vc_badges)
    else:
        vclock_html = '<span class="text-muted">Empty</span>'

    # Format flow credits HTML
    if peer_credits:
        credit_badges = [
            f'<span class="badge badge-neutral"><code>{html.escape(k)}</code>: {v} credits</span>'
            for k, v in sorted(peer_credits.items())
        ]
        credits_html = " ".join(credit_badges)
    else:
        credits_html = '<span class="text-muted">No peer connections</span>'

    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <meta http-equiv="refresh" content="3">
  <title>PeerQ — Node {node_id}</title>
  <style>
    :root {{
      --bg: #0b0f19;
      --card-bg: #111827;
      --card-border: #1f2937;
      --text: #f3f4f6;
      --text-muted: #9ca3af;
      --primary: #3b82f6;
      --success: #10b981;
      --warning: #f59e0b;
      --danger: #ef4444;
      --code-bg: #1e293b;
    }}
    * {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body {{
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, sans-serif;
      background-color: var(--bg);
      color: var(--text);
      line-height: 1.5;
      padding: 1.5rem;
    }}
    header {{
      display: flex;
      justify-content: space-between;
      align-items: center;
      padding-bottom: 1.5rem;
      border-bottom: 1px solid var(--card-border);
      margin-bottom: 1.5rem;
      flex-wrap: wrap;
      gap: 1rem;
    }}
    .brand {{
      display: flex;
      align-items: center;
      gap: 0.75rem;
    }}
    .brand h1 {{
      font-size: 1.5rem;
      font-weight: 700;
      letter-spacing: -0.025em;
    }}
    .node-tag {{
      background: var(--code-bg);
      padding: 0.25rem 0.6rem;
      border-radius: 6px;
      font-family: ui-monospace, monospace;
      font-size: 0.9rem;
      border: 1px solid var(--card-border);
    }}
    .nav-links {{
      display: flex;
      gap: 0.75rem;
    }}
    .nav-links a {{
      color: var(--primary);
      text-decoration: none;
      font-size: 0.875rem;
      background: var(--card-bg);
      padding: 0.35rem 0.75rem;
      border-radius: 6px;
      border: 1px solid var(--card-border);
      transition: background 0.15s;
    }}
    .nav-links a:hover {{
      background: var(--card-border);
    }}
    .grid-4 {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
      gap: 1rem;
      margin-bottom: 1.5rem;
    }}
    .stat-card {{
      background: var(--card-bg);
      border: 1px solid var(--card-border);
      border-radius: 8px;
      padding: 1rem 1.25rem;
    }}
    .stat-card .label {{
      font-size: 0.8rem;
      color: var(--text-muted);
      text-transform: uppercase;
      letter-spacing: 0.05em;
      margin-bottom: 0.25rem;
    }}
    .stat-card .value {{
      font-size: 1.75rem;
      font-weight: 700;
    }}
    .grid-2 {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(400px, 1fr));
      gap: 1.5rem;
      margin-bottom: 1.5rem;
    }}
    .panel {{
      background: var(--card-bg);
      border: 1px solid var(--card-border);
      border-radius: 8px;
      padding: 1.25rem;
    }}
    .panel h2 {{
      font-size: 1.1rem;
      font-weight: 600;
      margin-bottom: 1rem;
      border-bottom: 1px solid var(--card-border);
      padding-bottom: 0.5rem;
      display: flex;
      justify-content: space-between;
      align-items: center;
    }}
    .badge {{
      display: inline-flex;
      align-items: center;
      gap: 0.35rem;
      padding: 0.25rem 0.6rem;
      border-radius: 9999px;
      font-size: 0.8rem;
      font-weight: 500;
      margin: 0.2rem;
    }}
    .badge-success {{
      background: rgba(16, 185, 129, 0.15);
      color: #34d399;
      border: 1px solid rgba(16, 185, 129, 0.3);
    }}
    .badge-warning {{
      background: rgba(245, 158, 11, 0.15);
      color: #fbbf24;
      border: 1px solid rgba(245, 158, 11, 0.3);
    }}
    .badge-neutral {{
      background: var(--code-bg);
      color: var(--text);
      border: 1px solid var(--card-border);
    }}
    .dot {{ width: 8px; height: 8px; border-radius: 50%; display: inline-block; }}
    .dot-green {{ background-color: var(--success); }}
    .dot-amber {{ background-color: var(--warning); }}
    .state-grid {{
      display: grid;
      grid-template-columns: repeat(3, 1fr);
      gap: 0.75rem;
      margin-bottom: 1rem;
    }}
    .state-box {{
      background: var(--code-bg);
      border-radius: 6px;
      padding: 0.75rem;
      text-align: center;
    }}
    .state-box .name {{ font-size: 0.75rem; color: var(--text-muted); text-transform: uppercase; }}
    .state-box .count {{ font-size: 1.3rem; font-weight: 700; }}
    table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 0.875rem;
    }}
    th, td {{
      padding: 0.6rem 0.75rem;
      text-align: left;
      border-bottom: 1px solid var(--card-border);
    }}
    th {{ color: var(--text-muted); font-weight: 500; }}
    code {{
      font-family: ui-monospace, monospace;
      background: var(--code-bg);
      padding: 0.1rem 0.3rem;
      border-radius: 4px;
    }}
    .text-muted {{ color: var(--text-muted); font-size: 0.875rem; font-style: italic; }}
    footer {{
      margin-top: 2rem;
      padding-top: 1rem;
      border-top: 1px solid var(--card-border);
      text-align: center;
      color: var(--text-muted);
      font-size: 0.8rem;
    }}
  </style>
</head>
<body>
  <header>
    <div class="brand">
      <h1>PeerQ Mesh</h1>
      <span class="node-tag">Node: {node_id}</span>
      <span class="badge badge-success"><span class="dot dot-green"></span>Active</span>
    </div>
    <div class="nav-links">
      <a href="/status" target="_blank">JSON Status</a>
      <a href="/metrics" target="_blank">Prometheus Metrics</a>
      <a href="/healthz" target="_blank">Healthz</a>
    </div>
  </header>

  <div class="grid-4">
    <div class="stat-card">
      <div class="label">Total Tasks</div>
      <div class="value">{total_tasks}</div>
    </div>
    <div class="stat-card">
      <div class="label">Queue Depth</div>
      <div class="value">{queue_depth}</div>
    </div>
    <div class="stat-card">
      <div class="label">Active Leases</div>
      <div class="value">{len(active_leases)}</div>
    </div>
    <div class="stat-card">
      <div class="label">Live Peers</div>
      <div class="value">
        {len(live_peers)}
        <span style="font-size: 0.8rem; color: var(--text-muted);">/ {len(known_peers)}</span>
      </div>
    </div>
  </div>

  <div class="grid-2">
    <div class="panel">
      <h2>Task State Distribution</h2>
      <div class="state-grid">
        <div class="state-box">
          <div class="name">Submitted</div>
          <div class="count">{by_state.get("submitted", 0)}</div>
        </div>
        <div class="state-box">
          <div class="name">Claimed</div>
          <div class="count">{by_state.get("claimed", 0)}</div>
        </div>
        <div class="state-box">
          <div class="name">Running</div>
          <div class="count">{by_state.get("running", 0)}</div>
        </div>
        <div class="state-box">
          <div class="name">Completed</div>
          <div class="count" style="color: var(--success);">{by_state.get("completed", 0)}</div>
        </div>
        <div class="state-box">
          <div class="name">Failed</div>
          <div class="count" style="color: var(--danger);">{by_state.get("failed", 0)}</div>
        </div>
        <div class="state-box">
          <div class="name">Timed Out</div>
          <div class="count">{by_state.get("timed_out", 0)}</div>
        </div>
      </div>
      <div style="font-size: 0.85rem; color: var(--text-muted);">
        Enqueued: {enqueued} | Claimed: {claimed} | Completed: {completed} | Failed: {failed}
        | Reclaimed: {reclaimed} | Rejected: {rejected}
      </div>
    </div>

    <div class="panel">
      <h2>Cluster Topology</h2>
      <div style="margin-bottom: 1rem;">
        {peers_html}
      </div>
      <h2>Flow Control Credits</h2>
      <div>
        {credits_html}
      </div>
    </div>
  </div>

  <div class="grid-2">
    <div class="panel">
      <h2>Active Task Leases</h2>
      {leases_table}
    </div>

    <div class="panel">
      <h2>Vector Clock & Latencies</h2>
      <div style="margin-bottom: 1rem;">
        <strong>Vector Clock:</strong><br>
        {vclock_html}
      </div>
      <div>
        <strong>Execution Latencies:</strong><br>
        <table>
          <tr>
            <td>Task Latency (p50 / p99)</td>
            <td><code>{task_p50}</code> / <code>{task_p99}</code></td>
          </tr>
          <tr>
            <td>Gossip Latency (p50 / p99)</td>
            <td><code>{gossip_p50}</code> / <code>{gossip_p99}</code></td>
          </tr>
        </table>
      </div>
    </div>
  </div>

  <footer>
    PeerQ Leaderless Distributed Task Mesh &bull; Local clock: {now:.3f}s &bull; Auto-refreshing
  </footer>
</body>
</html>
"""
    return html_content


class StatusServer:
    """
    Lightweight HTTP dashboard and status endpoint for peerq.
    Pure asyncio HTTP without third-party frameworks.

    Endpoints:
    - / or /dashboard: Interactive single-page HTML web dashboard
    - /metrics: Prometheus text exposition format (counters + latency summaries)
    - /status: JSON payload describing topology, task states, leases, and queues
    - /healthz: Liveness check returning 200 OK
    """

    def __init__(
        self,
        node: PeerNode,
        host: str = "127.0.0.1",
        port: int = 9102,
        security: SecurityConfig | None = None,
        auth_token: str | None = None,
    ) -> None:
        self.node = node
        self.host = host
        self.port = port
        self._server = HttpServer(
            host,
            port,
            self._handle_request,
            security=security or node.security,
            auth_token=auth_token,
        )

    def _handle_request(self, method: str, path: str) -> tuple[int, str, bytes]:
        if method != "GET":
            return 405, "text/plain", b"Method Not Allowed\n"

        if path in ("/", "/dashboard"):
            html_page = get_dashboard_html(self.node)
            return 200, CONTENT_TYPE_HTML, html_page.encode("utf-8")

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
