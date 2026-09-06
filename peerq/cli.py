"""
Command-line interface (CLI) for peerq.

Provides terminal commands for:
- Starting a mesh peer node over TCP.
- Submitting tasks to a cluster node.
- Running built-in throughput, latency, and overhead benchmarks.
"""

from __future__ import annotations

import argparse
import asyncio
import random
import sys

from peerq.clock import RealClock
from peerq.node import PeerNode
from peerq.transport import Message, TcpTransport


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

    async def default_handler(payload: bytes) -> bytes:
        return payload.upper()

    node = PeerNode(
        node_id=node_id,
        clock=clock,
        transport=transport,
        rng=rng,
        peers=list(peer_addresses.keys()),
        handler=default_handler,
    )
    await node.start()

    known = list(peer_addresses.keys())
    print(f"[peerq] Node '{node_id}' listening on {host}:{port} with peers: {known}")
    print("[peerq] Press Ctrl+C to terminate.")

    try:
        while True:
            await clock.sleep(1.0)
    except (KeyboardInterrupt, asyncio.CancelledError):
        print(f"\n[peerq] Shutting down node '{node_id}'...")
    finally:
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

    # Command: submit
    submit_parser = subparsers.add_parser("submit", help="Submit task to a running node")
    submit_parser.add_argument("--target-host", default="127.0.0.1", help="Target node host")
    submit_parser.add_argument("--target-port", type=int, required=True, help="Target node port")
    submit_parser.add_argument("--task-id", required=True, help="Unique task identifier")
    submit_parser.add_argument("--payload", default="", help="Task payload data")
    submit_parser.add_argument("--sender-id", default="cli-client", help="Sender identifier")

    args = parser.parse_args()

    if args.command == "node":
        try:
            asyncio.run(run_node_command(args.id, args.host, args.port, args.peers))
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


if __name__ == "__main__":
    main()
