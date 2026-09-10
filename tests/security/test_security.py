"""Deterministic regression tests for PeerQ's authenticated wire protocols."""

from __future__ import annotations

import asyncio
import copy
import json
import math
import random
import struct
from dataclasses import replace
from pathlib import Path

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from peerq.clock import SimClock
from peerq.consensus import FenceToken, TaskRecord, TaskState, VectorClock
from peerq.crypto import Ed25519KeyPair, PeerKeyRing, sign_task
from peerq.discovery import PeerDiscovery
from peerq.node import PeerNode
from peerq.queue import QueueFull
from peerq.security import (
    MessageDecodeError,
    RateLimiter,
    ReplayCache,
    SecurityConfig,
    canonical_json_bytes,
    validate_json_tree,
)
from peerq.transport import (
    HttpServer,
    InMemoryTransport,
    Message,
    SimBroadcastTransport,
    SimNetwork,
    TcpTransport,
    _UdpProtocol,
)
from peerq.wal import (
    FRAME_HEADER_STRUCT,
    RECORD_TASK,
    WAL_MAGIC,
    WalCorruptError,
    WalError,
    WriteAheadLog,
)


def _keyring(identities: dict[str, Ed25519KeyPair]) -> PeerKeyRing:
    ring = PeerKeyRing()
    for peer_id, identity in identities.items():
        ring.add_peer(peer_id, identity.public_key)
    return ring


def _signed_message(
    identity: Ed25519KeyPair,
    sender_id: str,
    *,
    timestamp: float = 100.0,
    nonce: str = "nonce-1",
) -> Message:
    return Message(
        "heartbeat",
        sender_id,
        {"ts": timestamp},
        protocol_version=2,
    ).signed(identity, timestamp=timestamp, nonce=nonce)


def _task(task_id: str = "task-1") -> TaskRecord:
    return TaskRecord(
        task_id=task_id,
        state=TaskState.PENDING,
        payload=b"payload",
        fence_token=FenceToken(epoch=0, peer_id=""),
        vector_clock=VectorClock({"peer-b": 1}),
        updated_by="peer-b",
    )


def test_signed_message_roundtrip_and_tamper_rejection() -> None:
    identity = Ed25519KeyPair.from_private_bytes(bytes(range(32)))
    ring = _keyring({"peer-b": identity})
    config = SecurityConfig()
    message = _signed_message(identity, "peer-b")

    restored = Message.from_bytes(message.to_bytes()[4:], config)
    assert restored.verify_signature(ring)

    tampered = replace(restored, payload={"ts": 101.0})
    assert not tampered.verify_signature(ring)


def test_unknown_and_unauthorized_peer_are_rejected() -> None:
    trusted = Ed25519KeyPair.from_private_bytes(bytes(range(32)))
    unknown = Ed25519KeyPair.from_private_bytes(bytes(range(1, 33)))
    ring = _keyring({"peer-b": trusted})
    message = _signed_message(unknown, "peer-c")
    assert not message.verify_signature(ring)

    clock = SimClock(100.0)
    network = SimNetwork(clock)
    transport = InMemoryTransport("peer-a", network)
    node = PeerNode(
        "peer-a",
        clock,
        transport,
        random.Random(1),
        peers=["peer-b"],
        identity=Ed25519KeyPair.from_private_bytes(bytes([3]) * 32),
        keyring=ring,
    )
    assert not node._authenticate_message("peer-c", message)


def test_authorized_submitter_does_not_need_static_peer_address() -> None:
    own = Ed25519KeyPair.from_private_bytes(bytes([3]) * 32)
    client = Ed25519KeyPair.from_private_bytes(bytes([4]) * 32)
    ring = _keyring({"peer-a": own, "cli-client": client})
    clock = SimClock(100.0)
    network = SimNetwork(clock)
    transport = InMemoryTransport("peer-a", network)
    node = PeerNode(
        "peer-a",
        clock,
        transport,
        random.Random(1),
        peers=[],
        identity=own,
        keyring=ring,
    )
    message = _signed_message(client, "cli-client")
    assert node._authenticate_message("cli-client", message)


def test_message_replay_and_expiry_are_rejected() -> None:
    sender = Ed25519KeyPair.from_private_bytes(bytes(range(32)))
    own = Ed25519KeyPair.from_private_bytes(bytes([9]) * 32)
    ring = _keyring({"peer-a": own, "peer-b": sender})
    clock = SimClock(100.0)
    network = SimNetwork(clock)
    transport = InMemoryTransport("peer-a", network)
    node = PeerNode(
        "peer-a", clock, transport, random.Random(2), ["peer-b"], identity=own, keyring=ring
    )

    fresh = _signed_message(sender, "peer-b", timestamp=100.0, nonce="fresh")
    assert node._authenticate_message("peer-b", fresh)
    assert not node._authenticate_message("peer-b", fresh)

    expired = _signed_message(sender, "peer-b", timestamp=0.0, nonce="expired")
    assert not node._authenticate_message("peer-b", expired)


@pytest.mark.asyncio
async def test_unsigned_gossip_cannot_enter_a_secure_node() -> None:
    clock = SimClock(100.0)
    network = SimNetwork(clock)
    transport = InMemoryTransport("peer-a", network)
    sender = Ed25519KeyPair.from_private_bytes(bytes(range(32)))
    own = Ed25519KeyPair.from_private_bytes(bytes([9]) * 32)
    ring = _keyring({"peer-a": own, "peer-b": sender})
    node = PeerNode(
        "peer-a",
        clock,
        transport,
        random.Random(3),
        peers=["peer-b"],
        identity=own,
        keyring=ring,
    )
    await node.start()
    try:
        unsigned = Message(
            "gossip",
            "peer-b",
            {"tasks": {_task().task_id: _task().to_dict()}},
            protocol_version=2,
        )
        await transport.deliver_packet("peer-b", unsigned)
        await asyncio.sleep(0)
        assert node.get_task("task-1") is None

        valid_record = sign_task(sender, _task(), signer_id="peer-b")
        signed = Message(
            "gossip",
            "peer-b",
            {"tasks": {"task-1": valid_record.to_dict()}},
            protocol_version=2,
        ).signed(sender, timestamp=100.0, nonce="valid")
        await transport.deliver_packet("peer-b", signed)
        await asyncio.sleep(0)
        assert node.get_task("task-1") is not None
    finally:
        await node.stop()


@pytest.mark.asyncio
async def test_signed_discovery_requires_trust_and_freshness() -> None:
    clock = SimClock(100.0)
    network = SimNetwork(clock)
    b1 = SimBroadcastTransport("peer-a", network)
    b2 = SimBroadcastTransport("peer-b", network)
    a = Ed25519KeyPair.from_private_bytes(bytes([1]) * 32)
    b = Ed25519KeyPair.from_private_bytes(bytes([2]) * 32)
    ring_a = _keyring({"peer-a": a, "peer-b": b})
    ring_b = _keyring({"peer-b": b})
    d1 = PeerDiscovery("peer-a", "127.0.0.1", 8001, b1, clock, keyring=ring_a, identity=a)
    d2 = PeerDiscovery("peer-b", "127.0.0.1", 8002, b2, clock, keyring=ring_b, identity=b)
    await d1.start()
    await d2.start()
    try:
        beacon = d2._encode_beacon()
        await b2.send_broadcast(beacon)
        await asyncio.sleep(0)
        assert d1.get_active_peers()["peer-b"] == ("127.0.0.1", 8002)

        # The exact same datagram is a replay and must not produce a second event.
        before = dict(d1.get_active_peers())
        b1.deliver("127.0.0.1", beacon)
        await asyncio.sleep(0)
        assert d1.get_active_peers() == before

        stale = Message(
            "discovery",
            "peer-b",
            {
                "cluster_id": "peerq-default",
                "host": "127.0.0.1",
                "node_id": "peer-b",
                "port": 8002,
            },
            protocol_version=2,
        ).signed(b, timestamp=0.0, nonce="stale")
        b1.deliver("127.0.0.1", stale.to_bytes()[4:])
        await asyncio.sleep(0)
        assert d1.get_active_peers() == before
    finally:
        await d1.stop()
        await d2.stop()


def test_untrusted_bytes_are_bounded_and_decoder_is_total() -> None:
    config = SecurityConfig(max_frame_size=32)
    with pytest.raises(MessageDecodeError):
        Message.from_bytes(b"x" * 33, config)
    with pytest.raises(MessageDecodeError):
        Message.from_bytes(
            b'{"msg_type":"x","sender_id":"y","payload":{},"protocol_version":2,"timestamp":NaN}',
            SecurityConfig(),
        )


@settings(max_examples=60, derandomize=True)
@given(st.binary(max_size=1024))
def test_arbitrary_wire_bytes_never_escape_as_unexpected_decoder_error(data: bytes) -> None:
    try:
        Message.from_bytes(data, SecurityConfig(max_frame_size=2048))
    except MessageDecodeError:
        return
    except Exception as exc:  # pragma: no cover - assertion documents the boundary
        pytest.fail(f"unexpected parser exception for untrusted bytes: {exc!r}")


def test_udp_size_and_rate_limits_are_deterministic() -> None:
    clock = SimClock(100.0)
    queue: asyncio.Queue[tuple[str, bytes]] = asyncio.Queue(maxsize=2)
    protocol = _UdpProtocol(queue, 4, RateLimiter(1, 1.0), clock)
    protocol.datagram_received(b"12345", ("127.0.0.1", 1))
    protocol.datagram_received(b"1234", ("127.0.0.1", 1))
    protocol.datagram_received(b"5678", ("127.0.0.1", 1))
    assert queue.qsize() == 1
    assert queue.get_nowait()[1] == b"1234"


@pytest.mark.asyncio
async def test_http_remote_binding_requires_token_and_enforces_auth() -> None:
    token = "t" * 32
    server = HttpServer(
        "127.0.0.1", 0, lambda _method, _path: (200, "text/plain", b"ok"), auth_token=token
    )
    await server.start()
    try:
        reader, writer = await asyncio.open_connection("127.0.0.1", server.port)
        writer.write(b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        await writer.drain()
        response = await reader.read()
        writer.close()
        await writer.wait_closed()
        assert response.startswith(b"HTTP/1.1 401 Unauthorized")

        reader, writer = await asyncio.open_connection("127.0.0.1", server.port)
        writer.write(
            f"GET / HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {token}\r\n\r\n".encode()
        )
        await writer.drain()
        response = await reader.read()
        writer.close()
        await writer.wait_closed()
        assert response.startswith(b"HTTP/1.1 200 OK")
    finally:
        await server.stop()

    with pytest.raises(ValueError, match="remote HTTP binding"):
        HttpServer("0.0.0.0", 0, lambda _method, _path: (200, "text/plain", b"ok"))


@pytest.mark.asyncio
async def test_tcp_frame_limit_and_wal_payload_limit(tmp_path: Path) -> None:
    config = SecurityConfig(max_frame_size=8)
    transport = TcpTransport("node-a", "127.0.0.1", 0, {}, SimClock(100.0), security=config)
    reader = asyncio.StreamReader()
    reader.feed_data(struct.pack("!I", 9))
    reader.feed_eof()
    with pytest.raises(MessageDecodeError, match="frame"):
        await transport._read_frame(reader)

    wal = WriteAheadLog(tmp_path / "bounded.wal", max_record_bytes=32)
    try:
        with pytest.raises(WalError, match="size limit"):
            wal.append(1, {"data": "x" * 20, "more": "y" * 20})
    finally:
        wal.close()


@pytest.mark.asyncio
async def test_secure_wal_recovery_rejects_unsigned_legacy_record(tmp_path: Path) -> None:
    clock = SimClock(100.0)
    network = SimNetwork(clock)
    path = tmp_path / "legacy.wal"
    wal = WriteAheadLog(path)
    wal.append_task(_task())
    wal.close()

    identity = Ed25519KeyPair.from_private_bytes(bytes([7]) * 32)
    ring = _keyring({"peer-a": identity})
    transport = InMemoryTransport("peer-a", network)
    node = PeerNode(
        "peer-a",
        clock,
        transport,
        random.Random(4),
        peers=[],
        identity=identity,
        keyring=ring,
        wal=WriteAheadLog(path),
    )
    # Recovery completes without promoting the unsigned historical record.
    node._recover_from_wal()
    assert node.get_task("task-1") is None
    node.wal.close() if node.wal is not None else None
    await node.transport.close()


@pytest.mark.parametrize(
    "kwargs",
    [
        {"max_frame_size": 0},
        {"max_udp_payload": 0},
        {"max_credit_amount": 0},
        {"replay_window_seconds": 0.0},
        {"max_lease_duration_seconds": math.inf},
    ],
)
def test_security_config_rejects_unbounded_values(kwargs: dict[str, object]) -> None:
    with pytest.raises(ValueError):
        SecurityConfig(**kwargs)  # type: ignore[arg-type]


def test_json_limits_and_bounded_caches() -> None:
    with pytest.raises(MessageDecodeError):
        canonical_json_bytes({"bad": object()})
    with pytest.raises(MessageDecodeError):
        validate_json_tree("long", max_depth=2, max_items=2, max_string_length=2)
    with pytest.raises(MessageDecodeError):
        validate_json_tree([[[]]], max_depth=1, max_items=10, max_string_length=10)
    with pytest.raises(MessageDecodeError):
        validate_json_tree([1, 2, 3], max_depth=3, max_items=2, max_string_length=10)
    with pytest.raises(MessageDecodeError):
        validate_json_tree(float("inf"), max_depth=3, max_items=2, max_string_length=10)
    with pytest.raises(MessageDecodeError):
        validate_json_tree(object(), max_depth=3, max_items=2, max_string_length=10)

    cache = ReplayCache(max_entries=2)
    assert cache.check_and_remember("a", "1", 0.0, 10.0)
    assert cache.check_and_remember("b", "2", 0.0, 10.0)
    assert not cache.check_and_remember("a", "1", 0.0, 10.0)
    assert cache.check_and_remember("c", "3", 0.0, 10.0)
    assert len(cache) == 2
    assert cache.check_and_remember("a", "1", 20.0, 10.0)

    limiter = RateLimiter(1, 1.0, max_sources=1)
    assert limiter.allow("a", 0.0)
    assert not limiter.allow("a", 0.1)
    assert limiter.allow("a", 2.0)
    assert limiter.allow("b", 2.0)


@pytest.mark.parametrize(
    "mutator",
    [
        None,
        lambda d: d.update(task_id=""),
        lambda d: d.update(state=1),
        lambda d: d.update(state="unknown"),
        lambda d: d.update(payload="not-hex"),
        lambda d: d.update(result="not-hex"),
        lambda d: d.update(error="x" * 5000),
        lambda d: d.update(claimed_by=1),
        lambda d: d.update(fence_token=None),
        lambda d: d.update(fence_token={"epoch": -1, "peer_id": ""}),
        lambda d: d.update(vector_clock={"p": True}),
        lambda d: d.update(lease_expiry=float("inf")),
        lambda d: d.update(signature="g" * 128),
    ],
)
def test_task_record_parser_rejects_malformed_fields(mutator: object) -> None:
    raw = _task().to_dict()
    if mutator is None:
        with pytest.raises(ValueError):
            TaskRecord.from_dict([])  # type: ignore[arg-type]
        return
    candidate = copy.deepcopy(raw)
    mutator(candidate)  # type: ignore[operator]
    with pytest.raises(ValueError):
        TaskRecord.from_dict(candidate)


@pytest.mark.asyncio
async def test_discovery_rejects_invalid_signed_beacons_and_untrusted_binding() -> None:
    clock = SimClock(100.0)
    network = SimNetwork(clock)
    b1 = SimBroadcastTransport("peer-a", network)
    remote = Ed25519KeyPair.from_private_bytes(bytes([2]) * 32)
    own = Ed25519KeyPair.from_private_bytes(bytes([1]) * 32)
    ring = _keyring({"peer-a": own, "peer-b": remote})
    discovery = PeerDiscovery("peer-a", "127.0.0.1", 8001, b1, clock, identity=own, keyring=ring)

    def beacon(payload: dict[str, object], *, nonce: str) -> bytes:
        return (
            Message("discovery", "peer-b", payload, protocol_version=2)
            .signed(remote, timestamp=100.0, nonce=nonce)
            .to_bytes()[4:]
        )

    await discovery.start()
    try:
        payload: dict[str, object] = {
            "cluster_id": "peerq-default",
            "host": "127.0.0.1",
            "node_id": "peer-b",
            "port": 8002,
        }
        invalid_payloads = [
            {**payload, "node_id": "other"},
            {**payload, "host": "bad host"},
            {**payload, "port": 0},
            {**payload, "cluster_id": "other-cluster"},
        ]
        for index, invalid in enumerate(invalid_payloads):
            b1.deliver("127.0.0.1", beacon(invalid, nonce=f"bad-{index}"))
        tampered = bytearray(beacon(payload, nonce="tampered"))
        tampered[-1] ^= 1
        b1.deliver("127.0.0.1", bytes(tampered))
        await asyncio.sleep(0)
        assert discovery.get_active_peers() == {}

        node_transport = InMemoryTransport("peer-a", network)
        node = PeerNode(
            "peer-a",
            clock,
            node_transport,
            random.Random(5),
            peers=[],
            identity=own,
            keyring=_keyring({"peer-a": own}),
        )
        discovery.bind_node(node)
        b1.deliver("127.0.0.1", beacon(payload, nonce="untrusted-node-binding"))
        await asyncio.sleep(0)
        assert node.peers == []
        await node.stop()
    finally:
        await discovery.stop()


@pytest.mark.asyncio
async def test_secure_node_rejects_bad_shapes_and_credit_amounts() -> None:
    clock = SimClock(100.0)
    network = SimNetwork(clock)
    transport = InMemoryTransport("peer-a", network)
    sender = Ed25519KeyPair.from_private_bytes(bytes([2]) * 32)
    own = Ed25519KeyPair.from_private_bytes(bytes([1]) * 32)
    ring = _keyring({"peer-a": own, "peer-b": sender})
    node = PeerNode(
        "peer-a",
        clock,
        transport,
        random.Random(6),
        ["peer-b"],
        identity=own,
        keyring=ring,
    )
    await node.start()
    try:
        for index, payload in enumerate(
            [
                {"tasks": []},
                {"tasks": {"task-1": {"invalid": True}}},
                {"amount": -1},
                {"amount": True},
                {"amount": 100_001},
            ]
        ):
            msg_type = "credit" if "amount" in payload else "gossip"
            message = Message(msg_type, "peer-b", payload, protocol_version=2).signed(
                sender, timestamp=100.0, nonce=f"bad-node-{index}"
            )
            await transport.deliver_packet("peer-b", message)
        unknown_type = Message("unknown", "peer-b", {}, protocol_version=2).signed(
            sender, timestamp=100.0, nonce="unknown-type"
        )
        await transport.deliver_packet("peer-b", unknown_type)
        await asyncio.sleep(0)
        assert node.all_tasks() == {}
    finally:
        await node.stop()


@pytest.mark.parametrize(
    "document",
    [
        [],
        {},
        {"msg_type": "x"},
        {"msg_type": 1, "sender_id": "p", "payload": {}, "protocol_version": 2},
        {"msg_type": "x", "sender_id": "p", "payload": [], "protocol_version": 2},
        {"msg_type": "x", "sender_id": "p", "payload": {}, "protocol_version": True},
        {
            "msg_type": "x",
            "sender_id": "p",
            "payload": {},
            "protocol_version": 2,
            "timestamp": "bad",
        },
        {
            "msg_type": "x",
            "sender_id": "p",
            "payload": {},
            "protocol_version": 2,
            "nonce": "",
        },
        {
            "msg_type": "x",
            "sender_id": "p",
            "payload": {},
            "protocol_version": 2,
            "signature": "00",
        },
        {
            "msg_type": "x",
            "sender_id": "p",
            "payload": {},
            "protocol_version": 2,
            "signature": "g" * 128,
        },
    ],
)
def test_message_decoder_rejects_invalid_documents(document: object) -> None:
    raw = json.dumps(document).encode("utf-8")
    with pytest.raises(MessageDecodeError):
        Message.from_bytes(raw)


def test_message_encoder_rejects_invalid_local_messages() -> None:
    identity = Ed25519KeyPair.from_private_bytes(bytes([8]) * 32)
    with pytest.raises(ValueError):
        Message("x", "p", {}).signed(identity, timestamp=float("inf"))
    with pytest.raises(ValueError):
        Message("x", "p", {}).signed(identity, timestamp=1.0, nonce="")
    with pytest.raises(MessageDecodeError):
        Message("", "p", {}).to_bytes()
    with pytest.raises(MessageDecodeError):
        Message("x", "", {}).to_bytes()
    with pytest.raises(MessageDecodeError):
        Message("x", "p", {"x": "y"}).to_bytes(max_size=1)


def test_secure_transport_verification_rejects_wrong_context() -> None:
    sender = Ed25519KeyPair.from_private_bytes(bytes([2]) * 32)
    own = Ed25519KeyPair.from_private_bytes(bytes([1]) * 32)
    ring = _keyring({"peer-a": own, "peer-b": sender})
    transport = TcpTransport(
        "peer-a", "127.0.0.1", 0, {}, SimClock(100.0), keyring=ring, identity=own
    )
    message = _signed_message(sender, "peer-b", timestamp=100.0, nonce="context")
    assert transport._verify_message(message, expected_sender="peer-b")
    assert not transport._verify_message(message, expected_sender="peer-c")
    assert not transport._verify_message(replace(message, protocol_version=1))
    assert not transport._verify_message(replace(message, sender_id="missing"))
    assert not transport._verify_local_message(message)


@pytest.mark.asyncio
async def test_tcp_rejects_unsigned_handshake_and_cleans_up() -> None:
    identity = Ed25519KeyPair.from_private_bytes(bytes([1]) * 32)
    ring = _keyring({"server": identity})
    transport = TcpTransport(
        "server",
        "127.0.0.1",
        0,
        {},
        SimClock(100.0),
        identity=identity,
        keyring=ring,
        security=SecurityConfig(read_timeout_seconds=0.5),
    )
    await transport.start()
    try:
        reader, writer = await asyncio.open_connection("127.0.0.1", transport.port)
        unsigned = Message(
            "hello", "unknown", {"protocol_version": 2}, protocol_version=2
        ).to_bytes()
        writer.write(unsigned)
        await writer.drain()
        assert await reader.read() == b""
        writer.close()
        await writer.wait_closed()
    finally:
        await transport.close()


@pytest.mark.asyncio
async def test_http_parser_limits_and_method_errors() -> None:
    token = "t" * 32

    async def request(server: HttpServer, raw: bytes) -> bytes:
        reader, writer = await asyncio.open_connection("127.0.0.1", server.port)
        writer.write(raw)
        await writer.drain()
        response = await reader.read()
        writer.close()
        await writer.wait_closed()
        return response

    server = HttpServer(
        "127.0.0.1",
        0,
        lambda method, _path: (
            (200, "text/plain", b"ok")
            if method == "GET"
            else (405, "text/plain", b"Method Not Allowed\n")
        ),
        auth_token=token,
        security=SecurityConfig(max_http_body=4, max_http_headers=128),
    )
    await server.start()
    try:
        auth = f"Authorization: Bearer {token}\r\n"
        response = await request(
            server,
            (f"POST / HTTP/1.1\r\nHost: x\r\n{auth}Content-Length: nope\r\n\r\n").encode(),
        )
        assert response.startswith(b"HTTP/1.1 400 Bad Request")
        response = await request(
            server,
            (f"POST / HTTP/1.1\r\nHost: x\r\n{auth}Content-Length: 5\r\n\r\n").encode(),
        )
        assert response.startswith(b"HTTP/1.1 413 Payload Too Large")
        response = await request(
            server,
            (f"POST / HTTP/1.1\r\nHost: x\r\n{auth}Content-Length: 1\r\n\r\nx").encode(),
        )
        assert response.startswith(b"HTTP/1.1 400 Bad Request")
        response = await request(
            server,
            (f"POST / HTTP/1.1\r\nHost: x\r\n{auth}\r\n").encode(),
        )
        assert response.startswith(b"HTTP/1.1 405 Method Not Allowed")
        response = await request(
            server,
            (f"GET / HTTP/1.1\r\nHost: x\r\n{auth}broken-header\r\n\r\n").encode(),
        )
        assert response.startswith(b"HTTP/1.1 400 Bad Request")
    finally:
        await server.stop()


@pytest.mark.asyncio
async def test_node_admission_and_recovery_bounds(tmp_path: Path) -> None:
    clock = SimClock(100.0)
    network = SimNetwork(clock)
    transport = InMemoryTransport("peer-a", network)
    identity = Ed25519KeyPair.from_private_bytes(bytes([1]) * 32)
    ring = _keyring({"peer-a": identity})

    with pytest.raises(ValueError):
        PeerNode("", clock, transport, random.Random(1), [], identity=identity, keyring=ring)
    with pytest.raises(ValueError):
        PeerNode(
            "peer-a",
            clock,
            transport,
            random.Random(1),
            [""],
            identity=identity,
            keyring=ring,
        )
    node = PeerNode(
        "peer-a",
        clock,
        transport,
        random.Random(1),
        [],
        identity=identity,
        keyring=ring,
        max_tasks=1,
    )
    with pytest.raises(ValueError):
        await node.submit_task("", b"x")
    with pytest.raises(ValueError):
        await node.submit_task("too-large", b"x" * (64 * 1024 + 1))
    await node.submit_task("one", b"x")
    with pytest.raises(QueueFull):
        await node.submit_task("two", b"x")
    await node.stop()

    path = tmp_path / "malformed.wal"
    wal = WriteAheadLog(path)
    wal.append(1, {})
    wal.append(2, {"bad": "clock"})
    wal.append(3, {"tasks": [], "vector_clock": []})
    wal.close()
    recovery_transport = InMemoryTransport("peer-a", network)
    recovered = PeerNode(
        "peer-a",
        clock,
        recovery_transport,
        random.Random(2),
        [],
        identity=identity,
        keyring=ring,
        wal=WriteAheadLog(path),
    )
    recovered._recover_from_wal()
    assert recovered.all_tasks() == {}
    recovered.wal.close() if recovered.wal is not None else None
    await recovered.transport.close()


def test_wal_rejects_unknown_types_nonfinite_data_and_oversized_replay(tmp_path: Path) -> None:
    path = tmp_path / "wal-input.wal"
    wal = WriteAheadLog(path, max_record_bytes=32)
    with pytest.raises(WalError, match="Unknown WAL record type"):
        wal.append(99, {})
    with pytest.raises(WalError):
        wal.append(RECORD_TASK, {"bad": object()})
    with pytest.raises(WalError):
        wal.append(RECORD_TASK, {"bad": float("nan")})
    wal.close()

    path.write_bytes(WAL_MAGIC + FRAME_HEADER_STRUCT.pack(0, 0, 99))
    with pytest.raises(WalCorruptError, match="Unknown WAL record type"):
        list(WriteAheadLog(path, max_record_bytes=32).replay(strict=True))

    path.write_bytes(WAL_MAGIC + FRAME_HEADER_STRUCT.pack(33, 0, RECORD_TASK))
    replay_wal = WriteAheadLog(path, max_record_bytes=32)
    with pytest.raises(WalCorruptError, match="exceeds"):
        list(replay_wal.replay(strict=True))
    replay_wal.close()

    checkpoint_path = tmp_path / "checkpoint-limit.wal"
    checkpoint_wal = WriteAheadLog(checkpoint_path, max_record_bytes=32)
    with pytest.raises(WalError, match="checkpoint"):
        checkpoint_wal.checkpoint({"task": _task()}, VectorClock({"peer-a": 1}))
    checkpoint_wal.close()
