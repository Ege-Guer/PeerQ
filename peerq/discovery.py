"""
Dynamic local peer discovery for peerq.

Enables zero-configuration peer discovery on local subnets:
- Periodic UDP beacon announcements
- Automated peer directory caching and TTL expiration
- Automatic binding with PeerNode to dynamically expand cluster topology
"""

from __future__ import annotations

import asyncio
import contextlib
import json
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

from peerq.clock import Clock
from peerq.transport import BroadcastTransport, TcpTransport

if TYPE_CHECKING:
    from peerq.node import PeerNode


@dataclass(frozen=True)
class DiscoveredPeer:
    """Represents a discovered peer node on the local network."""

    node_id: str
    host: str
    port: int
    cluster_id: str
    last_seen: float


class PeerDiscovery:
    """
    Manages local subnet peer discovery using broadcast beacons.
    """

    def __init__(
        self,
        node_id: str,
        tcp_host: str,
        tcp_port: int,
        broadcast_transport: BroadcastTransport,
        clock: Clock,
        cluster_id: str = "peerq-default",
        beacon_interval: float = 1.0,
        peer_ttl: float = 3.5,
        on_peer_discovered: Callable[[DiscoveredPeer], None] | None = None,
        on_peer_lost: Callable[[str], None] | None = None,
    ) -> None:
        self.node_id = node_id
        self.tcp_host = tcp_host
        self.tcp_port = tcp_port
        self.broadcast_transport = broadcast_transport
        self.clock = clock
        self.cluster_id = cluster_id
        self.beacon_interval = beacon_interval
        self.peer_ttl = peer_ttl
        self.on_peer_discovered = on_peer_discovered
        self.on_peer_lost = on_peer_lost

        self._peers: dict[str, DiscoveredPeer] = {}
        self._running = False
        self._bg_tasks: list[asyncio.Task[None]] = []
        self._bound_node: PeerNode | None = None

    def get_active_peers(self) -> dict[str, tuple[str, int]]:
        """Return currently active discovered peers mapping node_id -> (host, port)."""
        now = self.clock.now()
        return {
            pid: (peer.host, peer.port)
            for pid, peer in self._peers.items()
            if (now - peer.last_seen) <= self.peer_ttl
        }

    def bind_node(self, node: PeerNode) -> None:
        """
        Bind discovery to a running PeerNode.

        When new peers are discovered, they are automatically added to the node's
        known peers list and transport directory.
        """
        self._bound_node = node

        def _on_discovered(peer: DiscoveredPeer) -> None:
            if self._bound_node is None or peer.node_id == self.node_id:
                return
            if peer.node_id not in self._bound_node.peers:
                self._bound_node.peers.append(peer.node_id)
                self._bound_node.flow_controller.init_peer(peer.node_id, 20)
            transport = self._bound_node.transport
            if isinstance(transport, TcpTransport):
                transport.peer_addresses[peer.node_id] = (peer.host, peer.port)

        original_discovered = self.on_peer_discovered

        def _combined_discovered(peer: DiscoveredPeer) -> None:
            _on_discovered(peer)
            if original_discovered is not None:
                original_discovered(peer)

        self.on_peer_discovered = _combined_discovered

    async def start(self) -> None:
        """Start discovery beacon broadcasting and listening."""
        if self._running:
            return
        self._running = True

        self._bg_tasks = [
            asyncio.create_task(self._announce_loop()),
            asyncio.create_task(self._listen_loop()),
            asyncio.create_task(self._ttl_loop()),
        ]

    async def stop(self) -> None:
        """Stop discovery loops and close broadcast transport."""
        self._running = False
        for t in self._bg_tasks:
            t.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await t
        self._bg_tasks.clear()
        await self.broadcast_transport.close()

    def _encode_beacon(self) -> bytes:
        payload = {
            "node_id": self.node_id,
            "host": self.tcp_host,
            "port": self.tcp_port,
            "cluster_id": self.cluster_id,
            "ts": self.clock.now(),
        }
        return json.dumps(payload, separators=(",", ":"), sort_keys=True).encode("utf-8")

    async def _announce_loop(self) -> None:
        while self._running:
            beacon_bytes = self._encode_beacon()
            with contextlib.suppress(Exception):
                await self.broadcast_transport.send_broadcast(beacon_bytes)
            await self.clock.sleep(self.beacon_interval)

    async def _listen_loop(self) -> None:
        while self._running:
            try:
                _sender_addr, raw_data = await self.broadcast_transport.recv_broadcast()
            except asyncio.CancelledError:
                break
            except Exception:
                await self.clock.sleep(0.05)
                continue

            try:
                data = json.loads(raw_data.decode("utf-8"))
                remote_id = str(data["node_id"])
                remote_host = str(data["host"])
                if remote_host in ("0.0.0.0", "", "::") and _sender_addr:
                    remote_host = _sender_addr[0]
                remote_port = int(data["port"])
                cluster_id = str(data.get("cluster_id", ""))
            except Exception:
                continue

            # Ignore own beacons and differing clusters
            if remote_id == self.node_id or cluster_id != self.cluster_id:
                continue

            now = self.clock.now()
            peer = DiscoveredPeer(
                node_id=remote_id,
                host=remote_host,
                port=remote_port,
                cluster_id=cluster_id,
                last_seen=now,
            )

            is_new = remote_id not in self._peers
            self._peers[remote_id] = peer

            if is_new and self.on_peer_discovered is not None:
                self.on_peer_discovered(peer)

    async def _ttl_loop(self) -> None:
        while self._running:
            await self.clock.sleep(self.peer_ttl / 2)
            now = self.clock.now()
            expired: list[str] = []
            for pid, peer in list(self._peers.items()):
                if now - peer.last_seen > self.peer_ttl:
                    expired.append(pid)

            for pid in expired:
                del self._peers[pid]
                if self.on_peer_lost is not None:
                    self.on_peer_lost(pid)
