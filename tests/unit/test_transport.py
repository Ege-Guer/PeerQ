"""Unit tests for peerq.transport."""

import pytest

from peerq.clock import RealClock, SimClock
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

    t1 = TcpTransport("peer-1", "127.0.0.1", port1, peer_map, clock)
    t2 = TcpTransport("peer-2", "127.0.0.1", port2, peer_map, clock)

    await t1.start()
    await t2.start()

    try:
        msg = Message("greet", "peer-1", {"hello": "world"})
        await t1.send("peer-2", msg)

        sender, received = await t2.recv()
        assert sender == "peer-1"
        assert received.msg_type == "greet"
        assert received.payload == {"hello": "world"}

        # Reply back
        reply = Message("ack", "peer-2", {"ok": True})
        await t2.send("peer-1", reply)
        reply_sender, reply_received = await t1.recv()
        assert reply_sender == "peer-2"
        assert reply_received.payload == {"ok": True}
    finally:
        await t1.close()
        await t2.close()
