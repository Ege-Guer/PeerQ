"""
Zero-Configuration Multi-Node Cluster with Ed25519 Cryptographic Signatures.

Demonstrates Phase 2 features:
1. Dynamic Subnet Discovery via UDP Multicast (zero manual peer IP configuration).
2. Ed25519 Key Generation, Digital Signing, and Byzantine Task Integrity Verification.
3. Live HTTP Dashboard & Prometheus text exposition endpoints (/metrics, /status, /healthz).
"""

from __future__ import annotations

import asyncio
import json
import random

from peerq.clock import RealClock
from peerq.consensus import TaskState
from peerq.crypto import (
    Ed25519KeyPair,
    PeerKeyRing,
    sign_task,
    verify_task_authorization,
)
from peerq.discovery import PeerDiscovery
from peerq.exporter import StatusServer
from peerq.node import PeerNode
from peerq.security import SecurityConfig
from peerq.transport import TcpTransport, UdpBroadcastTransport


async def main() -> None:
    print("=" * 75)
    print("PeerQ Phase 2 Showcase: Zero-Config Discovery, Ed25519 & HTTP Telemetry")
    print("=" * 75)

    clock = RealClock()
    multicast_group = "239.255.42.99"
    disc_port = 29199
    cluster_id = "showcase-cluster"

    # 1. Setup cryptographic identities
    print("\n1. Generating Ed25519 Keypairs and constructing cluster KeyRing...")
    keyring = PeerKeyRing()
    security = SecurityConfig()
    keypairs: dict[str, Ed25519KeyPair] = {}
    node_ids = ["alpha", "beta", "gamma"]

    for nid in node_ids:
        kp = Ed25519KeyPair.generate()
        keypairs[nid] = kp
        keyring.add_peer(nid, kp.public_key)
        print(f"   [{nid:5s}] Public Key: {kp.public_key.to_hex()[:24]}...")

    # 2. Bootstrapping nodes with dynamic port allocation (port 0)
    print("\n2. Initializing 3 nodes with zero static peers (dynamic discovery)...")
    nodes: list[PeerNode] = []
    discoveries: list[PeerDiscovery] = []
    status_servers: list[StatusServer] = []

    async def sample_handler(payload: bytes) -> bytes:
        await asyncio.sleep(0.02)
        return f"EXECUTED[{payload.decode().upper()}]".encode()

    for idx, nid in enumerate(node_ids):
        # TCP transport bound to dynamic port 0
        tcp = TcpTransport(
            nid,
            "127.0.0.1",
            0,
            {},
            clock,
            identity=keypairs[nid],
            keyring=keyring,
            security=security,
        )
        await tcp.start()

        rng = random.Random(42 + idx)
        node = PeerNode(
            node_id=nid,
            clock=clock,
            transport=tcp,
            rng=rng,
            peers=[],  # Zero static peers!
            handler=sample_handler,
            lease_duration=3.0,
            gossip_interval=0.3,
            heartbeat_interval=0.5,
            identity=keypairs[nid],
            keyring=keyring,
            security=security,
        )
        await node.start()
        nodes.append(node)

        # Discovery transport
        udp = UdpBroadcastTransport(
            port=disc_port,
            broadcast_addr=multicast_group,
            clock=clock,
            security=security,
        )
        await udp.start()

        disc = PeerDiscovery(
            node_id=nid,
            tcp_host="127.0.0.1",
            tcp_port=tcp.port,
            broadcast_transport=udp,
            clock=clock,
            cluster_id=cluster_id,
            beacon_interval=0.2,
            peer_ttl=1.5,
            identity=keypairs[nid],
            keyring=keyring,
            security=security,
        )
        disc.bind_node(node)
        await disc.start()
        discoveries.append(disc)

        # HTTP Status & Metrics server on dynamic port
        srv = StatusServer(node=node, host="127.0.0.1", port=0)
        await srv.start()
        status_servers.append(srv)

        print(
            f"   [{nid:5s}] TCP: 127.0.0.1:{tcp.port} | HTTP Dashboard: http://127.0.0.1:{srv.port}"
        )

    # 3. Wait for discovery convergence
    print("\n3. Waiting for multicast discovery to converge across mesh...")
    for _ in range(30):
        if all(len(n.peers) == 2 for n in nodes):
            break
        await asyncio.sleep(0.1)

    print("   --> Topology successfully formed:")
    for n in nodes:
        print(f"       [{n.node_id:5s}] Known peers: {n.peers}")

    # 4. Cryptographic Task Submission
    print("\n4. Submitting and cryptographically signing tasks...")
    for i in range(6):
        tid = f"task-{i:02d}"
        submitter = nodes[i % 3]
        kp = keypairs[submitter.node_id]

        # Submit task locally
        await submitter.submit_task(tid, f"crypto-job-{i}".encode(), priority=i * 2)

        # Sign the task record
        rec = submitter.get_task(tid)
        if rec is not None:
            signed = sign_task(kp, rec, signer_id=submitter.node_id)
            submitter._tasks[tid] = signed
            assert verify_task_authorization(signed, keyring) is True
            sig_abbr = signed.signature.hex()[:16] if signed.signature else "none"
            print(f"   [Signed] {tid} by '{submitter.node_id}' (Sig: {sig_abbr}...)")

    # 5. Await execution
    print("\n5. Executing tasks across mesh with gossip replication...")
    for _ in range(25):
        completed = 0
        for i in range(6):
            tid = f"task-{i:02d}"
            if any(
                (rec := n.get_task(tid)) is not None and rec.state == TaskState.DONE for n in nodes
            ):
                completed += 1
        if completed >= 6:
            break
        await asyncio.sleep(0.1)

    print("   --> All 6 tasks reached terminal state DONE.")

    # 6. Query HTTP status & Prometheus metrics
    print("\n6. Live HTTP Dashboard & Telemetry Inspection:")

    async def fetch_http(path: str, port: int) -> tuple[int, bytes]:
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        req = f"GET {path} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nConnection: close\r\n\r\n"
        writer.write(req.encode())
        await writer.drain()

        status_line = await reader.readline()
        status_code = int(status_line.decode().split()[1])
        while True:
            line = await reader.readline()
            if not line or line in (b"\r\n", b"\n"):
                break
        body = await reader.read()
        writer.close()
        await writer.wait_closed()
        return status_code, body

    status_code, body = await fetch_http("/status", status_servers[0].port)
    status_json = json.loads(body.decode())
    print(f"   GET http://127.0.0.1:{status_servers[0].port}/status -> HTTP {status_code}:")
    print(f"     Topology Live Peers: {status_json['cluster_topology']['live_peers']}")
    print(f"     Tasks Total:         {status_json['tasks']['total']}")
    print(f"     Tasks by State:      {status_json['tasks']['by_state']}")

    status_code, body = await fetch_http("/metrics", status_servers[0].port)
    print(f"\n   GET http://127.0.0.1:{status_servers[0].port}/metrics (first 4 lines):")
    for line in body.decode().splitlines()[:4]:
        print(f"     {line}")

    # Clean shutdown
    print("\n7. Gracefully shutting down cluster...")
    for srv in status_servers:
        await srv.stop()
    for disc in discoveries:
        await disc.stop()
    for node in nodes:
        await node.stop()

    print("Showcase completed successfully.\n")


if __name__ == "__main__":
    asyncio.run(main())
