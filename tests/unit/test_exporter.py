"""Unit tests for peerq.exporter (Prometheus text exporter)."""

import pytest

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


def test_format_cluster_status() -> None:
    import random

    from peerq.clock import SimClock
    from peerq.consensus import FenceToken, TaskRecord, TaskState
    from peerq.exporter import format_cluster_status
    from peerq.node import PeerNode
    from peerq.transport import InMemoryTransport, SimNetwork

    clock = SimClock(0.0)
    net = SimNetwork(clock)
    t = InMemoryTransport("n1", net)
    rng = random.Random(42)
    node = PeerNode("n1", clock, t, rng, peers=["n2", "n3"])

    # Put a task in node
    t1 = TaskRecord(
        task_id="task-1",
        state=TaskState.CLAIMED,
        payload=b"data",
        claimed_by="n1",
        fence_token=FenceToken(epoch=1, peer_id="n1"),
        lease_expiry=10.0,
    )
    node._tasks["task-1"] = t1

    status = format_cluster_status(node)
    assert status["node_id"] == "n1"
    assert status["cluster_topology"]["known_peers"] == ["n2", "n3"]
    assert status["tasks"]["total"] == 1
    assert status["tasks"]["by_state"]["claimed"] == 1
    assert len(status["tasks"]["active_leases"]) == 1
    assert status["tasks"]["active_leases"][0]["task_id"] == "task-1"
    assert status["queue"]["depth"] == 0
    assert "n2" in status["queue"]["peer_credits"]


@pytest.mark.asyncio
async def test_status_server_endpoints() -> None:
    import asyncio
    import json
    import random

    from peerq.clock import RealClock
    from peerq.exporter import StatusServer
    from peerq.node import PeerNode
    from peerq.transport import InMemoryTransport, SimNetwork

    clock = RealClock()
    net = SimNetwork(clock)
    t = InMemoryTransport("node-http", net)
    rng = random.Random(123)
    node = PeerNode("node-http", clock, t, rng, peers=["peer-x"])

    server = StatusServer(node, host="127.0.0.1", port=0)
    await server.start()
    port = server.port

    async def _http_req(method: str, path: str) -> tuple[int, dict[str, str], bytes]:
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        req = f"{method} {path} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nConnection: close\r\n\r\n"
        writer.write(req.encode("utf-8"))
        await writer.drain()

        status_line = await reader.readline()
        parts = status_line.decode("utf-8").split()
        status_code = int(parts[1])

        headers: dict[str, str] = {}
        while True:
            line = await reader.readline()
            if not line or line in (b"\r\n", b"\n"):
                break
            header_line = line.decode("utf-8").strip()
            if ":" in header_line:
                k, v = header_line.split(":", 1)
                headers[k.strip().lower()] = v.strip()

        body = await reader.read()
        writer.close()
        await writer.wait_closed()
        return status_code, headers, body

    try:
        # 1. /healthz
        code, hdrs, body = await _http_req("GET", "/healthz")
        assert code == 200
        assert body == b"OK\n"
        assert hdrs.get("content-type") == "text/plain"

        # 2. /metrics
        code, hdrs, body = await _http_req("GET", "/metrics")
        assert code == 200
        assert b"peerq_tasks_enqueued_total" in body
        assert "charset=utf-8" in hdrs.get("content-type", "")

        # 3. /status
        code, hdrs, body = await _http_req("GET", "/status")
        assert code == 200
        assert hdrs.get("content-type") == "application/json"
        data = json.loads(body.decode("utf-8"))
        assert data["node_id"] == "node-http"
        assert "cluster_topology" in data
        assert "tasks" in data

        # 4. / and /dashboard (Web UI)
        for path in ("/", "/dashboard"):
            code, hdrs, body = await _http_req("GET", path)
            assert code == 200
            assert hdrs.get("content-type") == "text/html; charset=utf-8"
            assert b"<!DOCTYPE html>" in body
            assert b"PeerQ Mesh" in body
            assert b"node-http" in body

        # 5. 404 Not Found
        code, _, _ = await _http_req("GET", "/not-found-path")
        assert code == 404

        # 6. 405 Method Not Allowed
        code, _, _ = await _http_req("POST", "/status")
        assert code == 405

        # 7. Malformed HTTP request
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(b"INVALID\r\n\r\n")
        await writer.drain()
        status_line = await reader.readline()
        parts = status_line.decode("utf-8").split()
        assert int(parts[1]) == 400
        writer.close()
        await writer.wait_closed()
    finally:
        await server.stop()


def test_get_dashboard_html_rendering() -> None:
    import random

    from peerq.clock import SimClock
    from peerq.consensus import FenceToken, TaskRecord, TaskState
    from peerq.exporter import get_dashboard_html
    from peerq.node import PeerNode
    from peerq.transport import InMemoryTransport, SimNetwork

    clock = SimClock(10.0)
    net = SimNetwork(clock)
    t = InMemoryTransport("dash-node", net)
    rng = random.Random(42)
    node = PeerNode("dash-node", clock, t, rng, peers=["peer-a", "peer-b"])

    # Simulate suspected peer
    node.failure_detector.heartbeat("peer-a", 10.0)
    # peer-b has no heartbeats, so it will be suspected

    # Add task with active lease
    t1 = TaskRecord(
        task_id="task-active-1",
        state=TaskState.CLAIMED,
        payload=b"test-payload",
        claimed_by="dash-node",
        fence_token=FenceToken(epoch=3, peer_id="dash-node"),
        lease_expiry=25.0,
    )
    node._tasks["task-active-1"] = t1

    # Record metrics & latencies
    node.metrics.increment("enqueued", 5)
    node.metrics.increment("claimed", 2)
    node.metrics.record_latency("task_latency", 4200.0)  # 4.2ms
    node.metrics.record_latency("gossip_latency", 1500.0)  # 1.5ms

    html_out = get_dashboard_html(node)
    assert "<!DOCTYPE html>" in html_out
    assert "dash-node" in html_out
    assert "task-active-1" in html_out
    assert "peer-a (live)" in html_out
    assert "peer-b (suspected)" in html_out
    assert "4.20 ms" in html_out
    assert "1.50 ms" in html_out
