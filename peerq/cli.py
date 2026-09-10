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
import ipaddress
import json
import random
import sys
import urllib.request
from pathlib import Path

from peerq.clock import RealClock
from peerq.consensus import FenceToken, TaskRecord, TaskState, VectorClock
from peerq.crypto import Ed25519KeyPair, PeerKeyRing, sign_task
from peerq.discovery import PeerDiscovery
from peerq.exporter import StatusServer
from peerq.node import PeerNode
from peerq.security import SecurityConfig
from peerq.transport import Message, TcpTransport, UdpBroadcastTransport
from peerq.wal import WriteAheadLog


def _is_loopback_host(host: str) -> bool:
    """Return whether an HTTP bind host is loopback-only."""
    if host.lower() == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def _validate_status_binding(status_host: str, status_token: str | None) -> None:
    """Reject unsafe dashboard configuration before starting node services."""
    if status_token is not None and len(status_token) < 32:
        raise ValueError("HTTP bearer token must contain at least 32 characters")
    if not _is_loopback_host(status_host) and not status_token:
        raise ValueError("remote HTTP binding requires an explicit bearer token")


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


def _parse_peer_keys(keys_str: str) -> PeerKeyRing:
    """Parse comma-separated ``node_id=public-key-hex`` trust entries."""
    keyring = PeerKeyRing()
    if not keys_str.strip():
        return keyring
    for entry in keys_str.split(","):
        entry = entry.strip()
        if not entry or "=" not in entry:
            raise ValueError(
                f"Invalid peer key format '{entry}', expected 'node_id=public_key_hex'"
            )
        peer_id, public_key = entry.split("=", 1)
        keyring.add_peer(peer_id.strip(), public_key.strip())
    return keyring


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
    private_key_hex: str | None = None,
    peer_keys_str: str = "",
    status_host: str = "127.0.0.1",
    status_token: str | None = None,
    insecure_dev_mode: bool = False,
) -> None:
    if status_port is not None:
        _validate_status_binding(status_host, status_token)
    clock = RealClock()
    peer_addresses = _parse_peer_addresses(peers_str)
    identity = (
        Ed25519KeyPair.from_hex(private_key_hex) if private_key_hex else Ed25519KeyPair.generate()
    )
    keyring = _parse_peer_keys(peer_keys_str)
    keyring.add_peer(node_id, identity.public_key)
    security = SecurityConfig(enabled=not insecure_dev_mode)
    if security.enabled:
        missing_keys = sorted(
            peer_id for peer_id in peer_addresses if not keyring.has_peer(peer_id)
        )
        if missing_keys:
            raise ValueError(
                "secure node startup requires --peer-keys for every static peer: "
                + ", ".join(missing_keys)
            )
    transport = TcpTransport(
        node_id=node_id,
        host=host,
        port=port,
        peer_addresses=peer_addresses,
        clock=clock,
        identity=identity,
        keyring=keyring,
        security=security,
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
        identity=identity,
        keyring=keyring,
        security=security,
    )
    await node.start()

    discovery: PeerDiscovery | None = None
    if enable_discovery:
        b_transport = UdpBroadcastTransport(
            port=discovery_port,
            clock=clock,
            security=security,
        )
        await b_transport.start()
        discovery = PeerDiscovery(
            node_id=node_id,
            tcp_host=host,
            tcp_port=transport.port,
            broadcast_transport=b_transport,
            clock=clock,
            cluster_id=cluster_id,
            identity=identity,
            keyring=keyring,
            security=security,
        )
        discovery.bind_node(node)
        await discovery.start()
        print(
            f"[peerq] Dynamic discovery enabled on UDP {discovery_port} (cluster: '{cluster_id}')"
        )

    status_server: StatusServer | None = None
    if status_port is not None:
        status_server = StatusServer(
            node=node,
            host=status_host,
            port=status_port,
            security=security,
            auth_token=status_token,
        )
        await status_server.start()
        print(
            f"[peerq] HTTP Status & Prometheus metrics listening at "
            f"http://{status_host}:{status_server.port}"
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
    private_key_hex: str | None = None,
    target_key_hex: str | None = None,
    target_id: str = "target",
    insecure_dev_mode: bool = False,
) -> None:
    clock = RealClock()
    identity = (
        Ed25519KeyPair.from_hex(private_key_hex) if private_key_hex else Ed25519KeyPair.generate()
    )
    keyring = PeerKeyRing()
    keyring.add_peer(sender_id, identity.public_key)
    if not insecure_dev_mode:
        if not target_key_hex:
            raise ValueError("secure submit requires --target-key-hex")
        keyring.add_peer(target_id, target_key_hex)
    security = SecurityConfig(enabled=not insecure_dev_mode)
    target_map = {target_id: (target_host, target_port)}
    transport = TcpTransport(
        node_id=sender_id,
        host="127.0.0.1",
        port=0,
        peer_addresses=target_map,
        clock=clock,
        identity=identity,
        keyring=keyring,
        security=security,
    )
    record = TaskRecord(
        task_id=task_id,
        state=TaskState.PENDING,
        payload=payload_str.encode(),
        fence_token=FenceToken(epoch=0, peer_id=""),
        vector_clock=VectorClock({sender_id: 1}),
        updated_by=sender_id,
    )
    if security.enabled:
        record = sign_task(identity, record, signer_id=sender_id)
    msg = Message(
        msg_type="gossip",
        sender_id=sender_id,
        payload={
            "tasks": {
                task_id: {
                    **record.to_dict(),
                }
            }
        },
        protocol_version=security.protocol_version,
    )
    if security.enabled:
        msg = msg.signed(identity, timestamp=clock.wall_now())
    try:
        await transport.send(target_id, msg)
        print(f"[peerq] Task '{task_id}' submitted successfully to {target_host}:{target_port}")
    finally:
        await transport.close()


def query_status_command(endpoint: str, token: str | None = None) -> None:
    try:
        headers = {"User-Agent": "peerq-cli"}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        req = urllib.request.Request(endpoint, headers=headers)
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
    node_parser.add_argument(
        "--private-key-hex", default=None, help="Ed25519 private seed (64 hex chars)"
    )
    node_parser.add_argument(
        "--peer-keys",
        default="",
        help="Comma-separated trusted keys: 'node_id=public_key_hex'",
    )
    node_parser.add_argument(
        "--status-host",
        default="127.0.0.1",
        help="Dashboard bind address; remote binds require --status-token",
    )
    node_parser.add_argument(
        "--status-token", default=None, help="Bearer token for a remote dashboard bind"
    )
    node_parser.add_argument(
        "--insecure-dev",
        action="store_true",
        help="Disable protocol authentication for isolated development only",
    )

    # Command: submit
    submit_parser = subparsers.add_parser("submit", help="Submit task to a running node")
    submit_parser.add_argument("--target-host", default="127.0.0.1", help="Target node host")
    submit_parser.add_argument("--target-port", type=int, required=True, help="Target node port")
    submit_parser.add_argument("--task-id", required=True, help="Unique task identifier")
    submit_parser.add_argument("--payload", default="", help="Task payload data")
    submit_parser.add_argument("--sender-id", default="cli-client", help="Sender identifier")
    submit_parser.add_argument(
        "--private-key-hex", default=None, help="Ed25519 private seed (64 hex chars)"
    )
    submit_parser.add_argument(
        "--target-key-hex", default=None, help="Trusted target Ed25519 public key"
    )
    submit_parser.add_argument("--target-id", default="target", help="Target node identifier")
    submit_parser.add_argument(
        "--insecure-dev",
        action="store_true",
        help="Disable protocol authentication for isolated development only",
    )

    # Command: status
    status_parser = subparsers.add_parser(
        "status", help="Query cluster status from a node HTTP endpoint"
    )
    status_parser.add_argument(
        "--endpoint",
        default="http://127.0.0.1:9102/status",
        help="HTTP status URL (default: http://127.0.0.1:9102/status)",
    )
    status_parser.add_argument(
        "--token", default=None, help="Bearer token for an authenticated endpoint"
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
                    private_key_hex=args.private_key_hex,
                    peer_keys_str=args.peer_keys,
                    status_host=args.status_host,
                    status_token=args.status_token,
                    insecure_dev_mode=args.insecure_dev,
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
                private_key_hex=args.private_key_hex,
                target_key_hex=args.target_key_hex,
                target_id=args.target_id,
                insecure_dev_mode=args.insecure_dev,
            )
        )
    elif args.command == "status":
        query_status_command(args.endpoint, token=args.token)


if __name__ == "__main__":
    main()
