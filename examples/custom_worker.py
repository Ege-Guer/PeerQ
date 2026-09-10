"""
Custom Worker Example for PeerQ.

Demonstrates how to build an application-specific worker node with PeerQ:
1. Defines a custom task execution handler (e.g. image processing, data ETL, ML inference).
2. Starts a node listening on TCP port 9001 with UDP subnet discovery.
3. Automatically connects to peer workers on the local network.
4. Safely claims and processes tasks with at-least-once idempotency guarantees.
"""

from __future__ import annotations

import asyncio
import os
import random
import socket
import sys
from pathlib import Path

# Ensure project root is in sys.path
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from peerq import (  # noqa: E402
    Ed25519KeyPair,
    PeerDiscovery,
    PeerKeyRing,
    PeerNode,
    RealClock,
    SecurityConfig,
    TcpTransport,
    UdpBroadcastTransport,
)


async def process_task(payload: bytes) -> bytes:
    """
    Application-specific task execution logic.
    Replace this with your project's workload:
    e.g., video transcoding, model inference, database mutations.
    """
    task_text = payload.decode(errors="replace")
    print(f"\n[Worker]  Starting execution of task payload: '{task_text}'")

    # Simulate workload processing
    await asyncio.sleep(1.0)

    result = f"PROCESSED({task_text}) on host {socket.gethostname()}"
    print(f"[Worker]  Task successfully finished: '{result}'")
    return result.encode("utf-8")


async def main() -> None:
    host_id = socket.gethostname().split(".")[0].lower()
    port = int(sys.argv[1]) if len(sys.argv) > 1 else 9001
    node_id = f"worker-{host_id}-{port}"

    clock = RealClock()
    private_key_hex = os.environ.get("PEERQ_PRIVATE_KEY_HEX")
    identity = (
        Ed25519KeyPair.from_hex(private_key_hex)
        if private_key_hex
        else Ed25519KeyPair.generate()
    )
    keyring = PeerKeyRing()
    keyring.add_peer(node_id, identity.public_key)
    for entry in os.environ.get("PEERQ_PEER_KEYS", "").split(","):
        if entry.strip():
            peer_id, public_key = entry.split("=", 1)
            keyring.add_peer(peer_id.strip(), public_key.strip())
    security = SecurityConfig()
    transport = TcpTransport(
        node_id=node_id,
        host="0.0.0.0",
        port=port,
        peer_addresses={},
        clock=clock,
        identity=identity,
        keyring=keyring,
        security=security,
    )
    await transport.start()

    node = PeerNode(
        node_id=node_id,
        clock=clock,
        transport=transport,
        rng=random.Random(),
        peers=[],
        handler=process_task,
        identity=identity,
        keyring=keyring,
        security=security,
    )
    await node.start()

    # Enable zero-config UDP discovery on local network
    b_transport = UdpBroadcastTransport(port=19876, clock=clock, security=security)
    await b_transport.start()
    discovery = PeerDiscovery(
        node_id=node_id,
        tcp_host="0.0.0.0",
        tcp_port=transport.port,
        broadcast_transport=b_transport,
        clock=clock,
        identity=identity,
        keyring=keyring,
        security=security,
    )
    discovery.bind_node(node)
    await discovery.start()

    print("=" * 60)
    print(f" PeerQ Worker Node '{node_id}' Active")
    print(f" Listening on TCP 0.0.0.0:{transport.port}")
    print(" Subnet discovery listening on UDP 19876")
    print(" Press Ctrl+C to stop.")
    print("=" * 60)

    # Optional: if passed --submit, submit an example task into the mesh
    if "--submit" in sys.argv:
        await asyncio.sleep(2.0)  # Wait for initial peer discovery
        print("\n[Submitter] Ingesting distributed demo task into mesh...")
        await node.submit_task("demo-task-1", b"Compute-Dataset-Batch-42")

    try:
        while True:
            await clock.sleep(1.0)
    except (KeyboardInterrupt, asyncio.CancelledError):
        print("\nShutting down worker...")
    finally:
        await discovery.stop()
        await node.stop()


if __name__ == "__main__":
    asyncio.run(main())
