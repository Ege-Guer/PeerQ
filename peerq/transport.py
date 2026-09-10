"""
Transport abstraction for peerq.

Strict architectural rule:
NO module outside this file may import socket, asyncio.open_connection,
or asyncio.start_server. All network I/O flows through this protocol.
"""

from __future__ import annotations

import asyncio
import contextlib
import hmac
import ipaddress
import json
import random
import secrets
import socket
import struct
from collections.abc import Callable
from dataclasses import dataclass
from math import isfinite
from typing import Any, Protocol, runtime_checkable

from peerq.clock import Clock, RealClock
from peerq.crypto import Ed25519KeyPair, PeerKeyRing
from peerq.security import (
    MAX_MESSAGE_TYPE_LENGTH,
    MAX_PEER_ID_LENGTH,
    MessageDecodeError,
    RateLimiter,
    ReplayCache,
    SecurityConfig,
    canonical_json_bytes,
    reject_json_constants,
    validate_json_tree,
)


@dataclass(frozen=True)
class Message:
    """Envelope for all network messages in peerq."""

    msg_type: str
    sender_id: str
    payload: dict[str, Any]
    timestamp: float | None = None
    nonce: str | None = None
    signature: bytes | None = None
    protocol_version: int = 2

    def _unsigned_document(self) -> dict[str, Any]:
        return {
            "msg_type": self.msg_type,
            "nonce": self.nonce,
            "payload": self.payload,
            "protocol_version": self.protocol_version,
            "sender_id": self.sender_id,
            "timestamp": self.timestamp,
        }

    def canonical_bytes(self) -> bytes:
        """Return the exact signed representation, excluding the signature."""
        return canonical_json_bytes(self._unsigned_document())

    def signed(
        self,
        key_pair: Ed25519KeyPair,
        *,
        timestamp: float,
        nonce: str | None = None,
    ) -> Message:
        """Return a cryptographically signed copy of this message."""
        if not isfinite(timestamp):
            raise ValueError("message timestamp must be finite")
        chosen_nonce = secrets.token_hex(16) if nonce is None else nonce
        if not 1 <= len(chosen_nonce) <= 128:
            raise ValueError("message nonce must contain 1-128 characters")
        signed = Message(
            msg_type=self.msg_type,
            sender_id=self.sender_id,
            payload=self.payload,
            timestamp=timestamp,
            nonce=chosen_nonce,
            protocol_version=self.protocol_version,
        )
        return Message(
            msg_type=signed.msg_type,
            sender_id=signed.sender_id,
            payload=signed.payload,
            timestamp=signed.timestamp,
            nonce=signed.nonce,
            signature=key_pair.sign(signed.canonical_bytes()),
            protocol_version=signed.protocol_version,
        )

    def verify_signature(self, keyring: PeerKeyRing) -> bool:
        """Verify this envelope against the pre-configured peer key directory."""
        if self.signature is None or self.nonce is None or self.timestamp is None:
            return False
        return keyring.verify(self.sender_id, self.signature, self.canonical_bytes())

    def to_bytes(self, max_size: int = 256 * 1024) -> bytes:
        if not self.msg_type or len(self.msg_type) > MAX_MESSAGE_TYPE_LENGTH:
            raise MessageDecodeError("message type is empty or too long")
        if not self.sender_id or len(self.sender_id) > MAX_PEER_ID_LENGTH:
            raise MessageDecodeError("sender ID is empty or too long")
        validate_json_tree(
            self.payload,
            max_depth=16,
            max_items=10_000,
            max_string_length=256 * 1024,
        )
        data = canonical_json_bytes(
            {
                **self._unsigned_document(),
                "signature": self.signature.hex() if self.signature is not None else None,
            }
        )
        if len(data) > max_size:
            raise MessageDecodeError("message exceeds the configured frame limit")
        return struct.pack("!I", len(data)) + data

    @classmethod
    def from_bytes(cls, data: bytes, security: SecurityConfig | None = None) -> Message:
        config = security or SecurityConfig()
        if not isinstance(data, bytes):
            raise MessageDecodeError("message wire data must be bytes")
        if len(data) > config.max_frame_size:
            raise MessageDecodeError("message exceeds the configured frame limit")
        try:
            parsed = json.loads(data.decode("utf-8"), parse_constant=reject_json_constants)
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
            raise MessageDecodeError(f"invalid message JSON: {exc}") from exc
        validate_json_tree(
            parsed,
            max_depth=config.max_json_depth,
            max_items=config.max_json_items,
            max_string_length=config.max_string_length,
        )
        if not isinstance(parsed, dict):
            raise MessageDecodeError("message root must be a JSON object")

        required = {"msg_type", "sender_id", "payload", "protocol_version"}
        if not required.issubset(parsed):
            raise MessageDecodeError("message is missing required fields")
        msg_type = parsed["msg_type"]
        sender_id = parsed["sender_id"]
        payload = parsed["payload"]
        protocol_version = parsed["protocol_version"]
        if (
            not isinstance(msg_type, str)
            or not msg_type
            or len(msg_type) > MAX_MESSAGE_TYPE_LENGTH
            or not isinstance(sender_id, str)
            or not sender_id
            or len(sender_id) > MAX_PEER_ID_LENGTH
            or not isinstance(payload, dict)
            or not isinstance(protocol_version, int)
            or isinstance(protocol_version, bool)
        ):
            raise MessageDecodeError("message fields have invalid types or lengths")

        timestamp = parsed.get("timestamp")
        nonce = parsed.get("nonce")
        signature_hex = parsed.get("signature")
        if timestamp is not None and (
            not isinstance(timestamp, (int, float))
            or isinstance(timestamp, bool)
            or not isfinite(float(timestamp))
        ):
            raise MessageDecodeError("message timestamp is invalid")
        if nonce is not None and (not isinstance(nonce, str) or not 1 <= len(nonce) <= 128):
            raise MessageDecodeError("message nonce is invalid")
        if signature_hex is not None and (
            not isinstance(signature_hex, str) or len(signature_hex) != 128
        ):
            raise MessageDecodeError("message signature is invalid")
        try:
            signature = bytes.fromhex(signature_hex) if signature_hex is not None else None
        except ValueError as exc:
            raise MessageDecodeError("message signature is not hexadecimal") from exc

        return cls(
            msg_type=msg_type,
            sender_id=sender_id,
            payload=payload,
            timestamp=float(timestamp) if timestamp is not None else None,
            nonce=nonce,
            signature=signature,
            protocol_version=protocol_version,
        )


@runtime_checkable
class Transport(Protocol):
    """Protocol for peer network transport."""

    async def send(self, peer_id: str, msg: Message) -> None:
        """Send a message to a specific peer."""
        ...  # pragma: no cover

    async def recv(self) -> tuple[str, Message]:
        """Receive the next incoming message as (sender_id, message)."""
        ...  # pragma: no cover

    async def close(self) -> None:
        """Close transport and release all associated network resources."""
        ...  # pragma: no cover


@runtime_checkable
class BroadcastTransport(Protocol):
    """Protocol for broadcast/multicast beacon discovery transport."""

    async def send_broadcast(self, data: bytes) -> None:
        """Broadcast data packet to the local network."""
        ...  # pragma: no cover

    async def recv_broadcast(self) -> tuple[str, bytes]:
        """Receive the next incoming broadcast packet as (sender_address, data)."""
        ...  # pragma: no cover

    async def close(self) -> None:
        """Close broadcast transport."""
        ...  # pragma: no cover


class InMemoryTransport:
    """
    In-memory transport implementation for simulation and fast testing.

    Messages are routed through a shared SimNetwork instance which can simulate
    delays, reordering, packet drops, and network partitions.
    """

    def __init__(
        self,
        node_id: str,
        network: SimNetwork,
        security: SecurityConfig | None = None,
    ) -> None:
        self.node_id = node_id
        self.network = network
        self.security = security or SecurityConfig()
        self._inbox: asyncio.Queue[tuple[str, Message]] = asyncio.Queue(
            maxsize=self.security.max_inbox_size
        )
        self._closed = False
        self.network.register(node_id, self)

    async def deliver_packet(self, sender_id: str, msg: Message) -> None:
        """Internal callback used by SimNetwork to deliver message to this node's inbox."""
        if not self._closed:
            # Re-serialize/deserialize to prevent shared mutable state across nodes
            isolated_msg = Message.from_bytes(
                msg.to_bytes(max_size=self.security.max_frame_size)[4:], self.security
            )
            if not self._inbox.full():
                self._inbox.put_nowait((sender_id, isolated_msg))

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
    """Authenticated asyncio TCP transport using bounded framed messages."""

    def __init__(
        self,
        node_id: str,
        host: str,
        port: int,
        peer_addresses: dict[str, tuple[str, int]],
        clock: Clock,
        identity: Ed25519KeyPair | None = None,
        keyring: PeerKeyRing | None = None,
        security: SecurityConfig | None = None,
    ) -> None:
        self.node_id = node_id
        self.host = host
        self.port = port
        self.peer_addresses = dict(peer_addresses)
        self.clock = clock
        self.identity = identity or Ed25519KeyPair.generate()
        self.keyring = keyring or PeerKeyRing()
        existing_key = self.keyring.get_peer_key(node_id)
        if existing_key is None:
            self.keyring.add_peer(node_id, self.identity.public_key)
        elif existing_key.to_bytes() != self.identity.public_key.to_bytes():
            raise ValueError("keyring identity does not match the transport identity")
        self.security = security or SecurityConfig()
        self._replay_cache = ReplayCache(self.security.replay_cache_size)
        self._inbox: asyncio.Queue[tuple[str, Message]] = asyncio.Queue(
            maxsize=self.security.max_inbox_size
        )
        self._server: asyncio.Server | None = None
        self._connections: dict[str, tuple[asyncio.StreamReader, asyncio.StreamWriter]] = {}
        self._incoming_writers: set[asyncio.StreamWriter] = set()
        self._closed = False

    async def start(self) -> None:
        """Start listening for incoming TCP connections."""
        self._server = await asyncio.start_server(
            self._handle_client,
            self.host,
            self.port,
            limit=self.security.max_frame_size + 4,
        )
        if self._server.sockets:
            sock_name = self._server.sockets[0].getsockname()
            if isinstance(sock_name, tuple) and len(sock_name) >= 2:
                self.port = int(sock_name[1])

    def _signed_message(self, msg_type: str, payload: dict[str, Any]) -> Message:
        message = Message(
            msg_type,
            self.node_id,
            payload,
            protocol_version=self.security.protocol_version,
        )
        return message.signed(self.identity, timestamp=self.clock.wall_now())

    def _verify_message(self, msg: Message, expected_sender: str | None = None) -> bool:
        if not self.security.enabled:
            return True
        if msg.protocol_version != self.security.protocol_version:
            return False
        if expected_sender is not None and msg.sender_id != expected_sender:
            return False
        if not self.keyring.has_peer(msg.sender_id):
            return False
        if not msg.verify_signature(self.keyring) or msg.timestamp is None or msg.nonce is None:
            return False
        now = self.clock.wall_now()
        if abs(now - msg.timestamp) > self.security.replay_window_seconds:
            return False
        return self._replay_cache.check_and_remember(
            msg.sender_id,
            msg.nonce,
            now,
            self.security.replay_window_seconds,
        )

    def _verify_local_message(self, msg: Message) -> bool:
        now = self.clock.wall_now()
        return bool(
            msg.sender_id == self.node_id
            and msg.protocol_version == self.security.protocol_version
            and msg.timestamp is not None
            and msg.nonce is not None
            and abs(now - msg.timestamp) <= self.security.replay_window_seconds
            and msg.verify_signature(self.keyring)
        )

    async def _read_frame(self, reader: asyncio.StreamReader) -> Message:
        try:
            length_bytes = await asyncio.wait_for(
                reader.readexactly(4), timeout=self.security.read_timeout_seconds
            )
            (msg_len,) = struct.unpack("!I", length_bytes)
            if msg_len <= 0 or msg_len > self.security.max_frame_size:
                raise MessageDecodeError("TCP frame exceeds the configured limit")
            payload_bytes = await asyncio.wait_for(
                reader.readexactly(msg_len), timeout=self.security.read_timeout_seconds
            )
            return Message.from_bytes(payload_bytes, self.security)
        except TimeoutError as exc:
            raise MessageDecodeError("TCP read timed out") from exc
        except asyncio.IncompleteReadError as exc:
            raise MessageDecodeError("TCP frame was truncated") from exc

    async def _write_frame(self, writer: asyncio.StreamWriter, msg: Message) -> None:
        try:
            writer.write(msg.to_bytes(max_size=self.security.max_frame_size))
            await asyncio.wait_for(writer.drain(), timeout=self.security.write_timeout_seconds)
        except TimeoutError as exc:
            raise MessageDecodeError("TCP write timed out") from exc

    async def _handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        if len(self._incoming_writers) >= self.security.max_connections:
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()
            return
        self._incoming_writers.add(writer)
        sock = writer.get_extra_info("socket")
        if sock is not None:
            with contextlib.suppress(Exception):
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        authorized_sender: str | None = None
        try:
            if self.security.enabled:
                hello = await self._read_frame(reader)
                if (
                    hello.msg_type != "hello"
                    or hello.payload.get("protocol_version") != self.security.protocol_version
                    or not self._verify_message(hello)
                ):
                    return
                authorized_sender = hello.sender_id
                await self._write_frame(
                    writer,
                    self._signed_message(
                        "hello_ack", {"protocol_version": self.security.protocol_version}
                    ),
                )

            while not self._closed:
                msg = await self._read_frame(reader)
                if self.security.enabled and (
                    msg.msg_type in {"hello", "hello_ack"}
                    or not self._verify_message(msg, expected_sender=authorized_sender)
                ):
                    return
                if not self._inbox.full():
                    self._inbox.put_nowait((msg.sender_id, msg))
        except (MessageDecodeError, ConnectionResetError, OSError):
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
        if len(self._connections) >= self.security.max_connections:
            raise ConnectionError("TCP connection limit reached")

        host, port = self.peer_addresses[peer_id]
        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(host, port, limit=self.security.max_frame_size + 4),
                timeout=self.security.connect_timeout_seconds,
            )
        except TimeoutError as exc:
            raise ConnectionError(f"Timed out connecting to peer {peer_id}") from exc
        sock = writer.get_extra_info("socket")
        if sock is not None:
            with contextlib.suppress(Exception):
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        if self.security.enabled:
            await self._write_frame(
                writer,
                self._signed_message("hello", {"protocol_version": self.security.protocol_version}),
            )
            ack = await self._read_frame(reader)
            if (
                ack.msg_type != "hello_ack"
                or ack.payload.get("protocol_version") != self.security.protocol_version
                or not self._verify_message(ack, expected_sender=peer_id)
            ):
                writer.close()
                with contextlib.suppress(Exception):
                    await writer.wait_closed()
                raise PermissionError(f"Peer {peer_id} failed authenticated handshake")
        self._connections[peer_id] = (reader, writer)
        return reader, writer

    async def send(self, peer_id: str, msg: Message) -> None:
        if self._closed:
            raise RuntimeError(f"Transport {self.node_id} is closed")
        if self.security.enabled and not self._verify_local_message(msg):
            raise PermissionError("secure transport requires a locally signed message")
        _, writer = await self._get_or_connect(peer_id)
        try:
            await self._write_frame(writer, msg)
        except Exception:
            self._connections.pop(peer_id, None)
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()
            raise

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
        self._inbox: asyncio.Queue[tuple[str, bytes]] = asyncio.Queue(maxsize=256)
        self.network.register_broadcast(node_id, self)

    async def send_broadcast(self, data: bytes) -> None:
        await self.network.deliver_broadcast(self.node_id, data)

    async def recv_broadcast(self) -> tuple[str, bytes]:
        return await self._inbox.get()

    def deliver(self, sender: str, data: bytes) -> None:
        if not self._inbox.full():
            self._inbox.put_nowait((sender, data))

    async def close(self) -> None:
        self.network.unregister_broadcast(self.node_id)


class _UdpProtocol(asyncio.DatagramProtocol):
    def __init__(
        self,
        inbox: asyncio.Queue[tuple[str, bytes]],
        max_datagram_size: int,
        rate_limiter: RateLimiter,
        clock: Clock,
    ) -> None:
        self.inbox = inbox
        self.max_datagram_size = max_datagram_size
        self.rate_limiter = rate_limiter
        self.clock = clock

    def datagram_received(self, data: bytes, addr: tuple[str, int]) -> None:
        if len(data) > self.max_datagram_size:
            return
        if not self.rate_limiter.allow(addr[0], self.clock.now()):
            return
        if not self.inbox.full():
            self.inbox.put_nowait((addr[0], data))

    def error_received(self, exc: Exception) -> None:
        pass


class UdpBroadcastTransport:
    """
    Production asyncio UDP broadcast/multicast transport.
    Supports subnet broadcast (SO_BROADCAST) and multicast groups (RFC 2365/mDNS).
    """

    def __init__(
        self,
        port: int = 19876,
        broadcast_addr: str = "239.255.42.99",
        clock: Clock | None = None,
        security: SecurityConfig | None = None,
    ) -> None:
        self.port = port
        self.broadcast_addr = broadcast_addr
        self.clock = clock or RealClock()
        self.security = security or SecurityConfig()
        self._rate_limiter = RateLimiter(
            self.security.udp_rate_limit,
            self.security.udp_rate_window_seconds,
        )
        self._sock: socket.socket | None = None
        self._inbox: asyncio.Queue[tuple[str, bytes]] = asyncio.Queue(
            maxsize=self.security.max_inbox_size
        )
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
            lambda: _UdpProtocol(
                self._inbox,
                self.security.max_udp_payload,
                self._rate_limiter,
                self.clock,
            ),
            sock=sock,
        )
        self._transport = transport
        if self.port == 0:
            sock_name = sock.getsockname()
            if isinstance(sock_name, tuple) and len(sock_name) >= 2:
                self.port = int(sock_name[1])

    async def send_broadcast(self, data: bytes) -> None:
        if len(data) > self.security.max_udp_payload:
            raise MessageDecodeError("UDP datagram exceeds the configured limit")
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
        security: SecurityConfig | None = None,
        auth_token: str | None = None,
    ) -> None:
        self.host = host
        self.port = port
        self.handler = handler
        self.security = security or SecurityConfig()
        self.auth_token = auth_token
        if not self._is_loopback_host(host) and not auth_token:
            raise ValueError("remote HTTP binding requires an explicit bearer token")
        if auth_token is not None and len(auth_token) < 32:
            raise ValueError("HTTP bearer token must contain at least 32 characters")
        self._server: asyncio.Server | None = None
        self._closed = False

    @staticmethod
    def _is_loopback_host(host: str) -> bool:
        if host.lower() == "localhost":
            return True
        try:
            return ipaddress.ip_address(host).is_loopback
        except ValueError:
            return False

    async def start(self) -> None:
        """Start HTTP server listening on host and port."""
        self._server = await asyncio.start_server(
            self._handle_client,
            self.host,
            self.port,
            limit=self.security.max_http_headers,
        )
        if self._server.sockets:
            sock_name = self._server.sockets[0].getsockname()
            if isinstance(sock_name, tuple) and len(sock_name) >= 2:
                self.port = int(sock_name[1])

    async def _handle_client(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        status = 400
        ctype = "text/plain; charset=utf-8"
        body = b"Bad Request\n"
        try:
            line_bytes = await asyncio.wait_for(
                reader.readline(), timeout=self.security.read_timeout_seconds
            )
            if not line_bytes:
                return
            if len(line_bytes) > self.security.max_http_headers:
                status, body = 431, b"Request Header Fields Too Large\n"
                await self._write_response(writer, status, ctype, body)
                return
            req_line = line_bytes.decode("utf-8", errors="replace").strip()
            parts = req_line.split()
            if len(parts) != 3 or len(parts[1]) > 2048:
                await self._write_response(writer, status, ctype, body)
                return

            method, path = parts[0], parts[1]
            headers: dict[str, str] = {}
            header_bytes = len(line_bytes)
            while True:
                hdr = await asyncio.wait_for(
                    reader.readline(), timeout=self.security.read_timeout_seconds
                )
                header_bytes += len(hdr)
                if header_bytes > self.security.max_http_headers:
                    status, body = 431, b"Request Header Fields Too Large\n"
                    await self._write_response(writer, status, ctype, body)
                    return
                if not hdr or hdr in (b"\r\n", b"\n"):
                    break
                if b":" not in hdr:
                    await self._write_response(writer, status, ctype, body)
                    return
                key, value = hdr.decode("utf-8", errors="replace").split(":", 1)
                headers[key.strip().lower()] = value.strip()

            content_length = headers.get("content-length", "0")
            try:
                body_length = int(content_length)
            except ValueError:
                body_length = -1
            if self.auth_token is not None and not hmac.compare_digest(
                headers.get("authorization", ""), f"Bearer {self.auth_token}"
            ):
                status, body = 401, b"Unauthorized\n"
            elif body_length < 0:
                status, body = 400, b"Invalid Content-Length\n"
            elif body_length > self.security.max_http_body:
                status, body = 413, b"Request body too large\n"
            elif body_length:
                # Status endpoints are read-only and do not accept request bodies.
                await asyncio.wait_for(
                    reader.readexactly(body_length), timeout=self.security.read_timeout_seconds
                )
                status, body = 400, b"Request body is not accepted\n"
            else:
                status, ctype, body = self.handler(method, path)

            if status == 200 and len(body) > self.security.max_http_body:
                status, ctype, body = 500, "text/plain; charset=utf-8", b"Response too large\n"

            await self._write_response(writer, status, ctype, body)
        except TimeoutError:
            await self._write_response(writer, 408, ctype, b"Request Timeout\n")
        except (asyncio.IncompleteReadError, UnicodeError, ValueError, OSError):
            # Malformed or incomplete clients are isolated to their connection.
            with contextlib.suppress(Exception):
                await self._write_response(writer, 400, ctype, body)
        except Exception:
            # Do not expose parser or handler internals to an untrusted client.
            with contextlib.suppress(Exception):
                await self._write_response(writer, 500, ctype, b"Internal Server Error\n")
        finally:
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()

    async def _write_response(
        self, writer: asyncio.StreamWriter, status: int, ctype: str, body: bytes
    ) -> None:
        status_messages = {
            200: "OK",
            400: "Bad Request",
            401: "Unauthorized",
            404: "Not Found",
            405: "Method Not Allowed",
            408: "Request Timeout",
            413: "Payload Too Large",
            431: "Request Header Fields Too Large",
            500: "Internal Server Error",
        }
        reason = status_messages.get(status, "Unknown")
        auth_header = "WWW-Authenticate: Bearer realm=peerq\r\n" if status == 401 else ""
        response_header = (
            f"HTTP/1.1 {status} {reason}\r\n"
            f"Content-Type: {ctype}\r\n"
            f"Content-Length: {len(body)}\r\n"
            "Cache-Control: no-store\r\n"
            "X-Content-Type-Options: nosniff\r\n"
            "Content-Security-Policy: default-src 'self'; frame-ancestors 'none'\r\n"
            f"{auth_header}"
            "Connection: close\r\n\r\n"
        )
        writer.write(response_header.encode("utf-8") + body)
        await asyncio.wait_for(writer.drain(), timeout=self.security.write_timeout_seconds)

    async def stop(self) -> None:
        """Stop HTTP server and wait for listener socket to close."""
        self._closed = True
        if self._server is not None:
            self._server.close()
            with contextlib.suppress(Exception):
                await self._server.wait_closed()
