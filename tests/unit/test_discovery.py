"""
Unit tests for peerq.discovery (Dynamic peer discovery).
"""

import random

import pytest

from peerq.clock import SimClock
from peerq.discovery import DiscoveredPeer, PeerDiscovery
from peerq.node import PeerNode
from peerq.transport import InMemoryTransport, SimBroadcastTransport, SimNetwork


@pytest.mark.asyncio
async def test_discovery_simulated_mesh() -> None:
    clock = SimClock(0.0)
    net = SimNetwork(clock)

    b1 = SimBroadcastTransport("n1", net)
    b2 = SimBroadcastTransport("n2", net)
    b3 = SimBroadcastTransport("n3", net)

    discovered_n1: list[DiscoveredPeer] = []
    d1 = PeerDiscovery(
        "n1",
        "10.0.0.1",
        8001,
        b1,
        clock,
        beacon_interval=0.5,
        peer_ttl=2.0,
        on_peer_discovered=discovered_n1.append,
    )
    d2 = PeerDiscovery(
        "n2",
        "10.0.0.2",
        8002,
        b2,
        clock,
        beacon_interval=0.5,
        peer_ttl=2.0,
    )
    d3 = PeerDiscovery(
        "n3",
        "10.0.0.3",
        8003,
        b3,
        clock,
        beacon_interval=0.5,
        peer_ttl=2.0,
    )

    await d1.start()
    await d2.start()
    await d3.start()

    # Advance clock to trigger beacon broadcast and reception
    clock.advance(0.6)
    await clock.sleep(0)

    # d1 should have discovered n2 and n3
    peers1 = d1.get_active_peers()
    assert "n2" in peers1
    assert peers1["n2"] == ("10.0.0.2", 8002)
    assert "n3" in peers1
    assert peers1["n3"] == ("10.0.0.3", 8003)
    assert len(discovered_n1) == 2

    # Stop all
    await d1.stop()
    await d2.stop()
    await d3.stop()


@pytest.mark.asyncio
async def test_discovery_ttl_expiration() -> None:
    clock = SimClock(0.0)
    net = SimNetwork(clock)

    b1 = SimBroadcastTransport("n1", net)
    b2 = SimBroadcastTransport("n2", net)

    lost_peers: list[str] = []
    d1 = PeerDiscovery(
        "n1",
        "127.0.0.1",
        9001,
        b1,
        clock,
        beacon_interval=1.0,
        peer_ttl=2.0,
        on_peer_lost=lost_peers.append,
    )
    d2 = PeerDiscovery("n2", "127.0.0.1", 9002, b2, clock, beacon_interval=1.0, peer_ttl=2.0)

    await d1.start()
    await d2.start()

    # Discover each other
    clock.advance(1.1)
    await clock.sleep(0)
    assert "n2" in d1.get_active_peers()

    # Now n2 stops broadcasting (crashed or left subnet)
    await d2.stop()

    # Advance time beyond TTL
    clock.advance(2.5)
    await clock.sleep(0)

    # n2 should be expired and purged
    assert "n2" not in d1.get_active_peers()
    assert "n2" in lost_peers

    await d1.stop()


@pytest.mark.asyncio
async def test_discovery_cluster_isolation() -> None:
    clock = SimClock(0.0)
    net = SimNetwork(clock)

    b1 = SimBroadcastTransport("n1", net)
    b2 = SimBroadcastTransport("n2", net)

    d1 = PeerDiscovery("n1", "127.0.0.1", 9001, b1, clock, cluster_id="cluster-prod")
    d2 = PeerDiscovery("n2", "127.0.0.1", 9002, b2, clock, cluster_id="cluster-staging")

    await d1.start()
    await d2.start()

    clock.advance(1.5)
    await clock.sleep(0)

    # Different clusters must ignore each other
    assert d1.get_active_peers() == {}
    assert d2.get_active_peers() == {}

    await d1.stop()
    await d2.stop()


@pytest.mark.asyncio
async def test_discovery_bind_node() -> None:
    clock = SimClock(0.0)
    net = SimNetwork(clock)

    b1 = SimBroadcastTransport("n1", net)
    b2 = SimBroadcastTransport("n2", net)

    t1 = InMemoryTransport("n1", net)
    rng = random.Random(42)
    node = PeerNode("n1", clock, t1, rng, peers=[])

    d1 = PeerDiscovery("n1", "127.0.0.1", 9001, b1, clock, beacon_interval=0.5)
    d2 = PeerDiscovery("n2", "127.0.0.1", 9002, b2, clock, beacon_interval=0.5)

    d1.bind_node(node)

    assert "n2" not in node.peers

    await d1.start()
    await d2.start()

    clock.advance(0.6)
    await clock.sleep(0)

    # Node peers directory should have dynamically incorporated n2
    assert "n2" in node.peers
    assert node.flow_controller.get_credits("n2") == 20

    await d1.stop()
    await d2.stop()
    await node.stop()


@pytest.mark.asyncio
async def test_discovery_malformed_beacon_ignored() -> None:
    clock = SimClock(0.0)
    net = SimNetwork(clock)

    b1 = SimBroadcastTransport("n1", net)
    d1 = PeerDiscovery("n1", "127.0.0.1", 9001, b1, clock)
    await d1.start()

    # Deliver garbage packet
    b1.deliver("attacker", b"NOT_A_VALID_JSON_BEACON_123")
    clock.advance(0.1)
    await clock.sleep(0)

    assert d1.get_active_peers() == {}
    await d1.stop()


@pytest.mark.asyncio
async def test_discovery_double_start_and_own_beacon() -> None:
    clock = SimClock(0.0)
    net = SimNetwork(clock)
    b1 = SimBroadcastTransport("n1", net)

    discovered: list[DiscoveredPeer] = []
    d1 = PeerDiscovery(
        "n1",
        "127.0.0.1",
        9001,
        b1,
        clock,
        on_peer_discovered=discovered.append,
    )
    await d1.start()
    # Idempotent double start
    await d1.start()

    # Deliver beacon from self
    b1.deliver("n1", d1._encode_beacon())
    clock.advance(0.1)
    await clock.sleep(0)

    assert d1.get_active_peers() == {}
    assert len(discovered) == 0

    await d1.stop()


@pytest.mark.asyncio
async def test_discovery_bind_node_with_tcp_transport_and_callback() -> None:
    clock = SimClock(0.0)
    net = SimNetwork(clock)
    b1 = SimBroadcastTransport("n1", net)
    b2 = SimBroadcastTransport("n2", net)

    from peerq.transport import TcpTransport

    tcp_t1 = TcpTransport("n1", "127.0.0.1", 9001, {}, clock)
    rng = random.Random(42)
    node = PeerNode("n1", clock, tcp_t1, rng, peers=[])

    discovered: list[DiscoveredPeer] = []
    d1 = PeerDiscovery(
        "n1",
        "127.0.0.1",
        9001,
        b1,
        clock,
        beacon_interval=0.5,
        on_peer_discovered=discovered.append,
    )
    d2 = PeerDiscovery("n2", "127.0.0.1", 9002, b2, clock, beacon_interval=0.5)

    d1.bind_node(node)
    await d1.start()
    await d2.start()

    clock.advance(0.6)
    await clock.sleep(0)

    assert "n2" in node.peers
    assert "n2" in tcp_t1.peer_addresses
    assert tcp_t1.peer_addresses["n2"] == ("127.0.0.1", 9002)
    assert len(discovered) == 1

    await d1.stop()
    await d2.stop()
    await node.stop()


@pytest.mark.asyncio
async def test_discovery_malformed_partial_json() -> None:
    clock = SimClock(0.0)
    net = SimNetwork(clock)
    b1 = SimBroadcastTransport("n1", net)
    d1 = PeerDiscovery("n1", "127.0.0.1", 9001, b1, clock)
    await d1.start()

    # JSON missing required 'port' or 'host'
    b1.deliver("remote", b'{"node_id": "remote"}')
    clock.advance(0.1)
    await clock.sleep(0)

    assert d1.get_active_peers() == {}
    await d1.stop()


@pytest.mark.asyncio
async def test_discovery_sender_addr_formats() -> None:
    clock = SimClock(0.0)
    net = SimNetwork(clock)
    b1 = SimBroadcastTransport("n1", net)
    d1 = PeerDiscovery("n1", "127.0.0.1", 9001, b1, clock)
    await d1.start()

    # Sender as string IP
    b1.deliver("192.168.1.100", b'{"node_id": "n2", "host": "0.0.0.0", "port": 9002, "cluster_id": "peerq-default"}')
    # Sender as tuple (ip, port)
    b1._inbox.put_nowait((("192.168.1.200", 9003), b'{"node_id": "n3", "host": "0.0.0.0", "port": 9003, "cluster_id": "peerq-default"}'))

    clock.advance(0.1)
    await clock.sleep(0)

    peers = d1.get_active_peers()
    assert peers.get("n2") == ("192.168.1.100", 9002)
    assert peers.get("n3") == ("192.168.1.200", 9003)

    await d1.stop()

