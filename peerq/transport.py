"""
Transport abstraction for peerq.

Strict architectural rule:
NO module outside this file may import socket, asyncio.open_connection,
or asyncio.start_server. All network I/O flows through this protocol.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import struct
from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable

from peerq.clock import Clock


@dataclass(frozen=True)
class Message:
    """Envelope for all network messages in peerq."""

    msg_type: str
    sender_id: str
    payload: dict[str, Any]

    def to_bytes(self) -> bytes:
        data = json.dumps(
            {"msg_type": self.msg_type, "sender_id": self.sender_id, "payload": self.payload},
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        return struct.pack("!I", len(data)) + data

    @classmethod
    def from_bytes(cls, data: bytes) -> Message:
        parsed = json.loads(data.decode("utf-8"))
        return cls(
            msg_type=parsed["msg_type"],
            sender_id=parsed["sender_id"],
            payload=parsed["payload"],
        )


@runtime_checkable
class Transport(Protocol):
    """Protocol for peer network transport."""

    async def send(self, peer_id: str, msg: Message) -> None:
        """Send a message to a specific peer."""
        ...

    async def recv(self) -> tuple[str, Message]:
        """Receive the next incoming message as (sender_id, message)."""
        ...

    async def close(self) -> None:
        """Close transport and release all associated network resources."""
        ...


class InMemoryTransport:
    """
    In-memory transport implementation for simulation and fast testing.

    Messages are routed through a shared SimNetwork instance which can simulate
    delays, reordering, packet drops, and network partitions.
    """

    def __init__(self, node_id: str, network: SimNetwork) -> None:
        self.node_id = node_id
        self.network = network
        self._inbox: asyncio.Queue[tuple[str, Message]] = asyncio.Queue()
        self._closed = False
        self.network.register(node_id, self)

    async def deliver_packet(self, sender_id: str, msg: Message) -> None:
        """Internal callback used by SimNetwork to deliver message to this node's inbox."""
        if not self._closed:
            # Re-serialize/deserialize to prevent shared mutable state across nodes
            isolated_msg = Message.from_bytes(msg.to_bytes()[4:])
            await self._inbox.put((sender_id, isolated_msg))

    async def send(self, peer_id: str, msg: Message) -> None:
        if self._closed:
            raise RuntimeError(f"Cannot send on closed transport: {self.node_id}")
        await self.network.route_message(self.node_id, peer_id, msg)

    async def recv(self) -> tuple[str, Message]:
        if self._closed and self._inbox.empty():
            raise RuntimeError(f"Transport {self.node_id} is closed")
        return await self._inbox.get()

    async def close(self) -> None:
        self._closed = True
        self.network.unregister(self.node_id)


class SimNetwork:
    """
    Network environment coordinator for InMemoryTransport.

    Controls network topology, packet latency, loss rates, and partitions.
    Integrates with Clock (typically SimClock) for virtual time progression.
    """

    def __init__(self, clock: Clock) -> None:
        self.clock = clock
        self._transports: dict[str, InMemoryTransport] = {}
        # Partitions: set of (source_node, target_node) tuples blocked from communicating
        self._blocked_links: set[tuple[str, str]] = set()
        self.drop_rate: float = 0.0
        self.base_latency: float = 0.0

    def register(self, node_id: str, transport: InMemoryTransport) -> None:
        self._transports[node_id] = transport

    def unregister(self, node_id: str) -> None:
        self._transports.pop(node_id, None)

    def partition(self, group_a: set[str], group_b: set[str], symmetric: bool = True) -> None:
        """Create a network partition between two groups of nodes."""
        for a in group_a:
            for b in group_b:
                self._blocked_links.add((a, b))
                if symmetric:
                    self._blocked_links.add((b, a))

    def heal_partition(
        self, group_a: set[str], group_b: set[str], symmetric: bool = True
    ) -> None:
        """Heal a network partition between two groups of nodes."""
        for a in group_a:
            for b in group_b:
                self._blocked_links.discard((a, b))
                if symmetric:
                    self._blocked_links.discard((b, a))

    def heal_all(self) -> None:
        """Clear all active network partitions."""
        self._blocked_links.clear()

    def is_blocked(self, source: str, target: str) -> bool:
        return (source, target) in self._blocked_links

    async def route_message(self, source: str, target: str, msg: Message) -> None:
        """Route a message from source to target respecting partitions and delay."""
        if self.is_blocked(source, target):
            # Message dropped by network partition
            return

        target_transport = self._transports.get(target)
        if target_transport is None:
            # Target node offline or nonexistent
            return

        if self.base_latency > 0:
            await self.clock.sleep(self.base_latency)

        # Check partition again after sleep in case network partitioned during transit
        if self.is_blocked(source, target):
            return

        await target_transport.deliver_packet(source, msg)


class TcpTransport:
    """
    Production asyncio TCP transport using length-prefixed framing.
    """

    def __init__(
        self,
        node_id: str,
        host: str,
        port: int,
        peer_addresses: dict[str, tuple[str, int]],
        clock: Clock,
    ) -> None:
        self.node_id = node_id
        self.host = host
        self.port = port
        self.peer_addresses = dict(peer_addresses)
        self.clock = clock
        self._inbox: asyncio.Queue[tuple[str, Message]] = asyncio.Queue()
        self._server: asyncio.Server | None = None
        self._connections: dict[str, tuple[asyncio.StreamReader, asyncio.StreamWriter]] = {}
        self._incoming_writers: set[asyncio.StreamWriter] = set()
        self._closed = False

    async def start(self) -> None:
        """Start listening for incoming TCP connections."""
        self._server = await asyncio.start_server(self._handle_client, self.host, self.port)

    async def _handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        self._incoming_writers.add(writer)
        try:
            while not self._closed:
                length_bytes = await reader.readexactly(4)
                (msg_len,) = struct.unpack("!I", length_bytes)
                payload_bytes = await reader.readexactly(msg_len)
                msg = Message.from_bytes(payload_bytes)
                await self._inbox.put((msg.sender_id, msg))
        except (asyncio.IncompleteReadError, ConnectionResetError):
            pass
        finally:
            self._incoming_writers.discard(writer)
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()

    async def _get_or_connect(
        self, peer_id: str
    ) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        if peer_id in self._connections:
            reader, writer = self._connections[peer_id]
            if not writer.is_closing():
                return reader, writer

        if peer_id not in self.peer_addresses:
            raise ValueError(f"Unknown peer address for peer_id: {peer_id}")

        host, port = self.peer_addresses[peer_id]
        reader, writer = await asyncio.open_connection(host, port)
        self._connections[peer_id] = (reader, writer)
        return reader, writer

    async def send(self, peer_id: str, msg: Message) -> None:
        if self._closed:
            raise RuntimeError(f"Transport {self.node_id} is closed")
        _, writer = await self._get_or_connect(peer_id)
        writer.write(msg.to_bytes())
        await writer.drain()

    async def recv(self) -> tuple[str, Message]:
        if self._closed and self._inbox.empty():
            raise RuntimeError(f"Transport {self.node_id} is closed")
        return await self._inbox.get()

    async def close(self) -> None:
        self._closed = True

        for _, writer in self._connections.values():
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()
        self._connections.clear()

        for writer in list(self._incoming_writers):
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()
        self._incoming_writers.clear()

        if self._server is not None:
            self._server.close()
            with contextlib.suppress(Exception):
                await self._server.wait_closed()
