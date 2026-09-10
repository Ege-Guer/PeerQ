import asyncio

import pytest

from peerq.clock import RealClock, SimClock
from peerq.crypto import Ed25519KeyPair, PeerKeyRing
from peerq.security import SecurityConfig
from peerq.transport import InMemoryTransport, Message, SimNetwork, TcpTransport


def test_message_serialization() -> None:
    msg = Message(
        msg_type="gossip",
        sender_id="peer-1",
        payload={"task_id": "t1", "epoch": 42, "items": [1, 2, 3]},
    )
    raw = msg.to_bytes()
    assert len(raw) > 4
    restored = Message.from_bytes(raw[4:])
    assert restored.msg_type == msg.msg_type
    assert restored.sender_id == msg.sender_id
    assert restored.payload == msg.payload


@pytest.mark.asyncio
async def test_in_memory_transport_delivery() -> None:
    clock = SimClock()
    net = SimNetwork(clock)

    t1 = InMemoryTransport("node-1", net)
    t2 = InMemoryTransport("node-2", net)

    msg = Message("ping", "node-1", {"seq": 1})
    await t1.send("node-2", msg)

    sender, received = await t2.recv()
    assert sender == "node-1"
    assert received.msg_type == "ping"
    assert received.payload == {"seq": 1}

    await t1.close()
    await t2.close()


@pytest.mark.asyncio
async def test_in_memory_transport_partitions() -> None:
    clock = SimClock()
    net = SimNetwork(clock)

    t1 = InMemoryTransport("node-1", net)
    t2 = InMemoryTransport("node-2", net)

    # Symmetric partition
    net.partition({"node-1"}, {"node-2"}, symmetric=True)
    assert net.is_blocked("node-1", "node-2")
    assert net.is_blocked("node-2", "node-1")

    await t1.send("node-2", Message("test", "node-1", {}))
    # Should not arrive
    assert t2._inbox.empty()

    # Heal partition
    net.heal_partition({"node-1"}, {"node-2"}, symmetric=True)
    assert not net.is_blocked("node-1", "node-2")

    await t1.send("node-2", Message("test2", "node-1", {}))
    _, received = await t2.recv()
    assert received.msg_type == "test2"

    # Asymmetric partition (1 cannot send to 2, but 2 can send to 1)
    net.partition({"node-1"}, {"node-2"}, symmetric=False)
    assert net.is_blocked("node-1", "node-2")
    assert not net.is_blocked("node-2", "node-1")

    await t1.send("node-2", Message("blocked", "node-1", {}))
    assert t2._inbox.empty()

    await t2.send("node-1", Message("allowed", "node-2", {}))
    _, received_at_1 = await t1.recv()
    assert received_at_1.msg_type == "allowed"

    await t1.close()
    await t2.close()


@pytest.mark.asyncio
async def test_tcp_transport_loopback() -> None:
    clock = RealClock()
    # Find free ports
    port1 = 18001
    port2 = 18002

    peer_map = {
        "peer-1": ("127.0.0.1", port1),
        "peer-2": ("127.0.0.1", port2),
    }
    identities = {"peer-1": Ed25519KeyPair.generate(), "peer-2": Ed25519KeyPair.generate()}
    keyring = PeerKeyRing()
    for peer_id, identity in identities.items():
        keyring.add_peer(peer_id, identity.public_key)

    t1 = TcpTransport("peer-1", "127.0.0.1", port1, peer_map, clock, identities["peer-1"], keyring)
    t2 = TcpTransport("peer-2", "127.0.0.1", port2, peer_map, clock, identities["peer-2"], keyring)

    await t1.start()
    await t2.start()

    try:
        msg = Message("greet", "peer-1", {"hello": "world"}).signed(
            identities["peer-1"], timestamp=clock.wall_now()
        )
        await t1.send("peer-2", msg)

        sender, received = await t2.recv()
        assert sender == "peer-1"
        assert received.msg_type == "greet"
        assert received.payload == {"hello": "world"}

        # Reply back
        reply = Message("ack", "peer-2", {"ok": True}).signed(
            identities["peer-2"], timestamp=clock.wall_now()
        )
        await t2.send("peer-1", reply)
        reply_sender, reply_received = await t1.recv()
        assert reply_sender == "peer-2"
        assert reply_received.payload == {"ok": True}
    finally:
        await t1.close()
        await t2.close()


@pytest.mark.asyncio
async def test_in_memory_transport_packet_drop_and_latency() -> None:
    import random

    clock = SimClock(0.0)
    rng = random.Random(42)
    net = SimNetwork(clock, rng=rng)
    net.drop_rate = 1.0
    t1 = InMemoryTransport("n1", net)
    t2 = InMemoryTransport("n2", net)

    # 100% drop probability -> packet never arrives
    await t1.send("n2", Message("ping", "n1", {}))
    assert t2._inbox.empty()

    # Reset drop, add latency
    net.drop_rate = 0.0
    net.base_latency = 1.5
    send_task = asyncio.create_task(t1.send("n2", Message("ping2", "n1", {})))
    await asyncio.sleep(0)

    # Not yet arrived before latency advances
    assert t2._inbox.empty()
    clock.advance(1.6)
    await clock.sleep(0)
    await send_task
    assert not t2._inbox.empty()
    _, msg = await t2.recv()
    assert msg.msg_type == "ping2"

    # Send to unregistered peer -> dropped safely
    await t1.send("unregistered_node", Message("ping3", "n1", {}))

    await t1.close()
    await t2.close()


@pytest.mark.asyncio
async def test_tcp_transport_errors() -> None:
    clock = RealClock()
    t = TcpTransport("p1", "127.0.0.1", 18099, {}, clock, security=SecurityConfig(enabled=False))

    # Sending to unregistered peer -> ValueError
    with pytest.raises(ValueError, match="Unknown peer address"):
        await t.send("p2", Message("test", "p1", {}))

    await t.close()

    # Sending or receiving on closed transport -> RuntimeError
    with pytest.raises(RuntimeError, match="is closed"):
        await t.send("p2", Message("test", "p1", {}))

    with pytest.raises(RuntimeError, match="is closed"):
        await t.recv()


def test_udp_broadcast_transport_multicast_detection() -> None:
    from peerq.transport import UdpBroadcastTransport

    assert UdpBroadcastTransport._is_multicast("239.255.42.99") is True
    assert UdpBroadcastTransport._is_multicast("224.0.0.1") is True
    assert UdpBroadcastTransport._is_multicast("127.0.0.1") is False
    assert UdpBroadcastTransport._is_multicast("192.168.1.255") is False
    assert UdpBroadcastTransport._is_multicast("invalid-ip") is False
