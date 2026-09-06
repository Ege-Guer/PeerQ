"""Unit tests for peerq.cli."""

import asyncio
import sys
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from peerq.cli import _parse_peer_addresses, main, run_node_command, submit_task_command


def test_parse_peer_addresses_valid() -> None:
    raw = "p1=127.0.0.1:8001, p2=10.0.0.2:9000"
    parsed = _parse_peer_addresses(raw)
    assert parsed == {
        "p1": ("127.0.0.1", 8001),
        "p2": ("10.0.0.2", 9000),
    }


def test_parse_peer_addresses_empty() -> None:
    assert _parse_peer_addresses("") == {}
    assert _parse_peer_addresses("   ") == {}


def test_parse_peer_addresses_invalid() -> None:
    with pytest.raises(ValueError, match="Invalid peer format"):
        _parse_peer_addresses("invalid_string_without_equal")

    with pytest.raises(ValueError, match="Invalid peer format"):
        _parse_peer_addresses("p1=no_port_here")


@pytest.mark.asyncio
async def test_run_node_command_lifecycle(monkeypatch: pytest.MonkeyPatch) -> None:
    mock_transport = AsyncMock()
    mock_node = AsyncMock()

    with (
        patch("peerq.cli.TcpTransport", return_value=mock_transport),
        patch("peerq.cli.PeerNode", return_value=mock_node),
    ):
        task = asyncio.create_task(
            run_node_command("node-test", "127.0.0.1", 9999, "p2=127.0.0.1:9998")
        )
        await asyncio.sleep(0.02)
        task.cancel()
        await task

        mock_transport.start.assert_awaited_once()
        mock_node.start.assert_awaited_once()
        mock_node.stop.assert_awaited_once()


@pytest.mark.asyncio
async def test_submit_task_command_success() -> None:
    mock_transport = AsyncMock()
    with patch("peerq.cli.TcpTransport", return_value=mock_transport):
        await submit_task_command("127.0.0.1", 9999, "sender-1", "task-abc", "hello-data")
        mock_transport.send.assert_awaited_once()
        mock_transport.close.assert_awaited_once()


def test_main_node_invocation(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "peerq",
            "node",
            "--id",
            "n1",
            "--host",
            "127.0.0.1",
            "--port",
            "9001",
            "--peers",
            "n2=127.0.0.1:9002",
        ],
    )

    called: list[object] = []

    def fake_run(coro: object) -> None:
        called.append(coro)
        if hasattr(coro, "close"):
            coro.close()

    monkeypatch.setattr("asyncio.run", fake_run)
    main()
    assert len(called) == 1


def test_main_submit_invocation(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "peerq",
            "submit",
            "--target-host",
            "127.0.0.1",
            "--target-port",
            "9001",
            "--task-id",
            "t-1",
            "--payload",
            "foo",
        ],
    )

    called: list[object] = []

    def fake_run(coro: object) -> None:
        called.append(coro)
        if hasattr(coro, "close"):
            coro.close()

    monkeypatch.setattr("asyncio.run", fake_run)
    main()
    assert len(called) == 1


def test_main_keyboard_interrupt(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sys, "argv", ["peerq", "node", "--id", "n1", "--port", "9001"])

    def fake_run_raise(coro: object) -> None:
        if hasattr(coro, "close"):
            coro.close()
        raise KeyboardInterrupt

    monkeypatch.setattr("asyncio.run", fake_run_raise)
    with pytest.raises(SystemExit) as exc_info:
        main()
    assert exc_info.value.code == 0


@pytest.mark.asyncio
async def test_run_node_command_with_discovery_and_status(tmp_path: Path) -> None:
    from unittest.mock import MagicMock

    mock_transport = AsyncMock()
    mock_node = AsyncMock()
    mock_discovery = MagicMock()
    mock_discovery.start = AsyncMock()
    mock_discovery.stop = AsyncMock()
    mock_status_server = AsyncMock()
    mock_udp = AsyncMock()

    with (
        patch("peerq.cli.TcpTransport", return_value=mock_transport),
        patch("peerq.cli.PeerNode", return_value=mock_node),
        patch("peerq.cli.UdpBroadcastTransport", return_value=mock_udp),
        patch("peerq.cli.PeerDiscovery", return_value=mock_discovery),
        patch("peerq.cli.StatusServer", return_value=mock_status_server),
    ):
        task = asyncio.create_task(
            run_node_command(
                node_id="n1",
                host="127.0.0.1",
                port=9001,
                peers_str="",
                wal_path=str(tmp_path / "node.wal"),
                enable_discovery=True,
                discovery_port=19876,
                status_port=9102,
            )
        )
        await asyncio.sleep(0.02)
        task.cancel()
        await task

        mock_discovery.start.assert_awaited_once()
        mock_status_server.start.assert_awaited_once()
        mock_discovery.stop.assert_awaited_once()
        mock_status_server.stop.assert_awaited_once()


def test_main_status_invocation(monkeypatch: pytest.MonkeyPatch) -> None:
    from unittest.mock import MagicMock

    monkeypatch.setattr(
        sys, "argv", ["peerq", "status", "--endpoint", "http://127.0.0.1:9102/status"]
    )

    mock_resp = MagicMock()
    mock_resp.read.return_value = b'{"node_id": "test-node", "status": "ok"}'
    mock_resp.__enter__.return_value = mock_resp

    with patch("urllib.request.urlopen", return_value=mock_resp):
        main()
