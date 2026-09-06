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
import random
import socket
import struct
from collections.abc import Callable
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


@runtime_checkable
class BroadcastTransport(Protocol):
    """Protocol for broadcast/multicast beacon discovery transport."""

    async def send_broadcast(self, data: bytes) -> None:
        """Broadcast data packet to the local network."""
        ...

    async def recv_broadcast(self) -> tuple[str, bytes]:
        """Receive the next incoming broadcast packet as (sender_address, data)."""
        ...

    async def close(self) -> None:
        """Close broadcast transport."""
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

    def __init__(self, clock: Clock, rng: random.Random | None = None) -> None:
        self.clock = clock
        self.rng = rng
        self._transports: dict[str, InMemoryTransport] = {}
        self._broadcast_transports: dict[str, SimBroadcastTransport] = {}
        # Partitions: set of (source_node, target_node) tuples blocked from communicating
        self._blocked_links: set[tuple[str, str]] = set()
        self.drop_rate: float = 0.0
        self.base_latency: float = 0.0

    def register(self, node_id: str, transport: InMemoryTransport) -> None:
        self._transports[node_id] = transport

    def unregister(self, node_id: str) -> None:
        self._transports.pop(node_id, None)

    def register_broadcast(self, node_id: str, transport: SimBroadcastTransport) -> None:
        self._broadcast_transports[node_id] = transport

    def unregister_broadcast(self, node_id: str) -> None:
        self._broadcast_transports.pop(node_id, None)

    async def deliver_broadcast(self, sender: str, data: bytes) -> None:
        for target, transport in list(self._broadcast_transports.items()):
            if target == sender:
                continue
            if self.is_blocked(sender, target):
                continue
            transport.deliver(sender, data)

    def partition(self, group_a: set[str], group_b: set[str], symmetric: bool = True) -> None:
        """Create a network partition between two groups of nodes."""
        for a in group_a:
            for b in group_b:
                self._blocked_links.add((a, b))
                if symmetric:
                    self._blocked_links.add((b, a))

    def heal_partition(self, group_a: set[str], group_b: set[str], symmetric: bool = True) -> None:
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

        if self.rng is not None and self.drop_rate > 0.0 and self.rng.random() < self.drop_rate:
            # Message dropped by packet loss
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
        if self._server.sockets:
            sock_name = self._server.sockets[0].getsockname()
            if isinstance(sock_name, tuple) and len(sock_name) >= 2:
                self.port = int(sock_name[1])

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


class SimBroadcastTransport:
    """In-memory simulated broadcast transport for deterministic discovery testing."""

    def __init__(self, node_id: str, network: SimNetwork) -> None:
        self.node_id = node_id
        self.network = network
        self._inbox: asyncio.Queue[tuple[str, bytes]] = asyncio.Queue()
        self.network.register_broadcast(node_id, self)

    async def send_broadcast(self, data: bytes) -> None:
        await self.network.deliver_broadcast(self.node_id, data)

    async def recv_broadcast(self) -> tuple[str, bytes]:
        return await self._inbox.get()

    def deliver(self, sender: str, data: bytes) -> None:
        self._inbox.put_nowait((sender, data))

    async def close(self) -> None:
        self.network.unregister_broadcast(self.node_id)


class _UdpProtocol(asyncio.DatagramProtocol):
    def __init__(self, inbox: asyncio.Queue[tuple[str, bytes]]) -> None:
        self.inbox = inbox

    def datagram_received(self, data: bytes, addr: tuple[str, int]) -> None:
        self.inbox.put_nowait((addr[0], data))

    def error_received(self, exc: Exception) -> None:
        pass


class UdpBroadcastTransport:
    """
    Production asyncio UDP broadcast/multicast transport.
    Supports subnet broadcast (SO_BROADCAST) and multicast groups (RFC 2365/mDNS).
    """

    def __init__(self, port: int = 19876, broadcast_addr: str = "239.255.42.99") -> None:
        self.port = port
        self.broadcast_addr = broadcast_addr
        self._sock: socket.socket | None = None
        self._inbox: asyncio.Queue[tuple[str, bytes]] = asyncio.Queue()
        self._transport: asyncio.DatagramTransport | None = None

    @staticmethod
    def _is_multicast(ip: str) -> bool:
        try:
            first_octet = int(ip.split(".")[0])
            return 224 <= first_octet <= 239
        except (ValueError, IndexError):
            return False

    async def start(self) -> None:
        loop = asyncio.get_running_loop()
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if hasattr(socket, "SO_REUSEPORT"):
            with contextlib.suppress(Exception):
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)

        if self._is_multicast(self.broadcast_addr):
            with contextlib.suppress(Exception):
                sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, 1)
            with contextlib.suppress(Exception):
                sock.setsockopt(
                    socket.IPPROTO_IP,
                    socket.IP_MULTICAST_IF,
                    socket.inet_aton("127.0.0.1"),
                )
            sock.bind(("", self.port))
            with contextlib.suppress(Exception):
                mreq = struct.pack(
                    "4s4s",
                    socket.inet_aton(self.broadcast_addr),
                    socket.inet_aton("127.0.0.1"),
                )
                sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
            with contextlib.suppress(Exception):
                mreq_any = struct.pack(
                    "4sl",
                    socket.inet_aton(self.broadcast_addr),
                    socket.INADDR_ANY,
                )
                sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq_any)
        else:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            sock.bind(("", self.port))

        sock.setblocking(False)
        self._sock = sock
        transport, _ = await loop.create_datagram_endpoint(
            lambda: _UdpProtocol(self._inbox),
            sock=sock,
        )
        self._transport = transport
        if self.port == 0:
            sock_name = sock.getsockname()
            if isinstance(sock_name, tuple) and len(sock_name) >= 2:
                self.port = int(sock_name[1])

    async def send_broadcast(self, data: bytes) -> None:
        if self._transport is not None:
            self._transport.sendto(data, (self.broadcast_addr, self.port))

    async def recv_broadcast(self) -> tuple[str, bytes]:
        return await self._inbox.get()

    async def close(self) -> None:
        if self._transport is not None:
            self._transport.close()
            self._transport = None
        if self._sock is not None:
            self._sock.close()
            self._sock = None


class HttpServer:
    """
    Lightweight standard-library HTTP server for metrics and health inspection.
    Implemented purely using asyncio.start_server and stream readers/writers.
    """

    def __init__(
        self,
        host: str,
        port: int,
        handler: Callable[[str, str], tuple[int, str, bytes]],
    ) -> None:
        self.host = host
        self.port = port
        self.handler = handler
        self._server: asyncio.Server | None = None
        self._closed = False

    async def start(self) -> None:
        """Start HTTP server listening on host and port."""
        self._server = await asyncio.start_server(self._handle_client, self.host, self.port)
        if self._server.sockets:
            sock_name = self._server.sockets[0].getsockname()
            if isinstance(sock_name, tuple) and len(sock_name) >= 2:
                self.port = int(sock_name[1])

    async def _handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        try:
            line_bytes = await reader.readline()
            if not line_bytes:
                return
            req_line = line_bytes.decode("utf-8", errors="replace").strip()
            parts = req_line.split()
            if len(parts) < 2:
                status, ctype, body = 400, "text/plain", b"Bad Request\n"
            else:
                method, path = parts[0], parts[1]
                # Drain request headers until blank line
                while True:
                    hdr = await reader.readline()
                    if not hdr or hdr in (b"\r\n", b"\n"):
                        break
                status, ctype, body = self.handler(method, path)

            status_messages = {
                200: "OK",
                400: "Bad Request",
                404: "Not Found",
                405: "Method Not Allowed",
                500: "Internal Server Error",
            }
            reason = status_messages.get(status, "Unknown")
            response_header = (
                f"HTTP/1.1 {status} {reason}\r\n"
                f"Content-Type: {ctype}\r\n"
                f"Content-Length: {len(body)}\r\n"
                f"Connection: close\r\n\r\n"
            )
            writer.write(response_header.encode("utf-8") + body)
            await writer.drain()
        except Exception:
            pass
        finally:
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()

    async def stop(self) -> None:
        """Stop HTTP server and wait for listener socket to close."""
        self._closed = True
        if self._server is not None:
            self._server.close()
            with contextlib.suppress(Exception):
                await self._server.wait_closed()
