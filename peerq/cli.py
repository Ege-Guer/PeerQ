"""
Command-line interface (CLI) for peerq.

Provides terminal commands for:
- Starting a mesh peer node over TCP (with optional WAL, Discovery, and HTTP Status).
- Submitting tasks to a cluster node.
- Querying node status and topology over HTTP.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import random
import sys
import urllib.request
from pathlib import Path

from peerq.clock import RealClock
from peerq.discovery import PeerDiscovery
from peerq.exporter import StatusServer
from peerq.node import PeerNode
from peerq.transport import Message, TcpTransport, UdpBroadcastTransport
from peerq.wal import WriteAheadLog


def _parse_peer_addresses(peers_str: str) -> dict[str, tuple[str, int]]:
    """Parse comma-separated 'node_id=host:port' definitions."""
    if not peers_str.strip():
        return {}
    peer_map: dict[str, tuple[str, int]] = {}
    for entry in peers_str.split(","):
        entry = entry.strip()
        if not entry:
            continue
        if "=" not in entry or ":" not in entry:
            raise ValueError(f"Invalid peer format '{entry}', expected 'node_id=host:port'")
        nid, addr = entry.split("=", 1)
        host, port_str = addr.split(":", 1)
        peer_map[nid.strip()] = (host.strip(), int(port_str.strip()))
    return peer_map


async def run_node_command(
    node_id: str,
    host: str,
    port: int,
    peers_str: str,
    wal_path: str | None = None,
    enable_discovery: bool = False,
    discovery_port: int = 19876,
    cluster_id: str = "peerq-default",
    status_port: int | None = None,
    lease_duration: float | None = None,
) -> None:
    clock = RealClock()
    peer_addresses = _parse_peer_addresses(peers_str)
    transport = TcpTransport(
        node_id=node_id,
        host=host,
        port=port,
        peer_addresses=peer_addresses,
        clock=clock,
    )
    await transport.start()
    rng = random.Random()

    wal = WriteAheadLog(Path(wal_path)) if wal_path else None

    async def default_handler(payload: bytes) -> bytes:
        return payload.upper()

    node = PeerNode(
        node_id=node_id,
        clock=clock,
        transport=transport,
        rng=rng,
        peers=list(peer_addresses.keys()),
        handler=default_handler,
        wal=wal,
        lease_duration=lease_duration,
    )
    await node.start()

    discovery: PeerDiscovery | None = None
    if enable_discovery:
        b_transport = UdpBroadcastTransport(port=discovery_port)
        await b_transport.start()
        discovery = PeerDiscovery(
            node_id=node_id,
            tcp_host=host,
            tcp_port=transport.port,
            broadcast_transport=b_transport,
            clock=clock,
            cluster_id=cluster_id,
        )
        discovery.bind_node(node)
        await discovery.start()
        print(
            f"[peerq] Dynamic discovery enabled on UDP {discovery_port} (cluster: '{cluster_id}')"
        )

    status_server: StatusServer | None = None
    if status_port is not None:
        status_server = StatusServer(node=node, host=host, port=status_port)
        await status_server.start()
        print(
            f"[peerq] HTTP Status & Prometheus metrics listening at http://{host}:{status_server.port}"
        )

    known = list(peer_addresses.keys())
    print(
        f"[peerq] Node '{node_id}' listening on {host}:{transport.port} with static peers: {known}"
    )
    print("[peerq] Press Ctrl+C to terminate.")

    try:
        while True:
            await clock.sleep(1.0)
    except (KeyboardInterrupt, asyncio.CancelledError):
        print(f"\n[peerq] Shutting down node '{node_id}'...")
    finally:
        if status_server is not None:
            await status_server.stop()
        if discovery is not None:
            await discovery.stop()
        await node.stop()


async def submit_task_command(
    target_host: str,
    target_port: int,
    sender_id: str,
    task_id: str,
    payload_str: str,
) -> None:
    clock = RealClock()
    target_map = {"target": (target_host, target_port)}
    transport = TcpTransport(
        node_id=sender_id,
        host="127.0.0.1",
        port=0,
        peer_addresses=target_map,
        clock=clock,
    )
    msg = Message(
        msg_type="gossip",
        sender_id=sender_id,
        payload={
            "tasks": {
                task_id: {
                    "task_id": task_id,
                    "state": "pending",
                    "payload": payload_str.encode().hex(),
                    "result": None,
                    "error": None,
                    "claimed_by": None,
                    "fence_token": {"epoch": 0, "peer_id": ""},
                    "lease_expiry": 0.0,
                    "vector_clock": {sender_id: 1},
                    "updated_by": sender_id,
                }
            }
        },
    )
    try:
        await transport.send("target", msg)
        print(f"[peerq] Task '{task_id}' submitted successfully to {target_host}:{target_port}")
    finally:
        await transport.close()


def query_status_command(endpoint: str) -> None:
    try:
        req = urllib.request.Request(endpoint, headers={"User-Agent": "peerq-cli"})
        with urllib.request.urlopen(req, timeout=5.0) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            print(json.dumps(data, indent=2))
    except Exception as exc:
        print(f"[peerq] Error querying status endpoint '{endpoint}': {exc}", file=sys.stderr)
        sys.exit(1)


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="peerq",
        description="Leaderless peer-to-peer async task mesh",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Command: node
    node_parser = subparsers.add_parser("node", help="Start a mesh peer node")
    node_parser.add_argument("--id", required=True, help="Unique node identifier")
    node_parser.add_argument("--host", default="127.0.0.1", help="Bind IP address")
    node_parser.add_argument("--port", type=int, required=True, help="Bind TCP port")
    node_parser.add_argument("--peers", default="", help="Comma-separated peers: 'id=h:p,id2=h:p'")
    node_parser.add_argument("--wal-path", default=None, help="Optional Write-Ahead Log path")
    node_parser.add_argument("--discovery", action="store_true", help="Enable UDP subnet discovery")
    node_parser.add_argument("--discovery-port", type=int, default=19876, help="Discovery UDP port")
    node_parser.add_argument(
        "--cluster-id", default="peerq-default", help="Discovery cluster namespace"
    )
    node_parser.add_argument(
        "--status-port", type=int, default=None, help="HTTP dashboard / metrics port"
    )
    node_parser.add_argument(
        "--lease-duration",
        type=float,
        default=None,
        help="Task lease duration in seconds (defaults to PEERQ_LEASE_DURATION or 5.0)",
    )

    # Command: submit
    submit_parser = subparsers.add_parser("submit", help="Submit task to a running node")
    submit_parser.add_argument("--target-host", default="127.0.0.1", help="Target node host")
    submit_parser.add_argument("--target-port", type=int, required=True, help="Target node port")
    submit_parser.add_argument("--task-id", required=True, help="Unique task identifier")
    submit_parser.add_argument("--payload", default="", help="Task payload data")
    submit_parser.add_argument("--sender-id", default="cli-client", help="Sender identifier")

    # Command: status
    status_parser = subparsers.add_parser(
        "status", help="Query cluster status from a node HTTP endpoint"
    )
    status_parser.add_argument(
        "--endpoint",
        default="http://127.0.0.1:9102/status",
        help="HTTP status URL (default: http://127.0.0.1:9102/status)",
    )

    args = parser.parse_args()

    if args.command == "node":
        try:
            asyncio.run(
                run_node_command(
                    node_id=args.id,
                    host=args.host,
                    port=args.port,
                    peers_str=args.peers,
                    wal_path=args.wal_path,
                    enable_discovery=args.discovery,
                    discovery_port=args.discovery_port,
                    cluster_id=args.cluster_id,
                    status_port=args.status_port,
                    lease_duration=args.lease_duration,
                )
            )
        except KeyboardInterrupt:
            sys.exit(0)
    elif args.command == "submit":
        asyncio.run(
            submit_task_command(
                args.target_host,
                args.target_port,
                args.sender_id,
                args.task_id,
                args.payload,
            )
        )
    elif args.command == "status":
        query_status_command(args.endpoint)


if __name__ == "__main__":
    main()
