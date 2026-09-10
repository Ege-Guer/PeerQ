"""
Integration test for real UDP broadcast/multicast discovery.
Verifies that UdpBroadcastTransport discovers peers across local sockets.
"""

import asyncio

import pytest

from peerq.clock import RealClock
from peerq.crypto import Ed25519KeyPair, PeerKeyRing
from peerq.discovery import DiscoveredPeer, PeerDiscovery
from peerq.transport import UdpBroadcastTransport


@pytest.mark.asyncio
async def test_udp_discovery_loopback() -> None:
    clock = RealClock()
    identities = {
        "node-udp-1": Ed25519KeyPair.generate(),
        "node-udp-2": Ed25519KeyPair.generate(),
    }
    keyring = PeerKeyRing()
    for node_id, identity in identities.items():
        keyring.add_peer(node_id, identity.public_key)
    # Test port with SO_REUSEPORT on macOS
    test_port = 28971
    group = "239.255.42.99"

    t1 = UdpBroadcastTransport(port=test_port, broadcast_addr=group)
    t2 = UdpBroadcastTransport(port=test_port, broadcast_addr=group)

    await t1.start()
    await t2.start()

    discovered_by_d1: list[DiscoveredPeer] = []
    discovered_by_d2: list[DiscoveredPeer] = []

    d1 = PeerDiscovery(
        node_id="node-udp-1",
        tcp_host="127.0.0.1",
        tcp_port=10001,
        broadcast_transport=t1,
        clock=clock,
        cluster_id="test-udp-cluster",
        beacon_interval=0.1,
        peer_ttl=1.0,
        on_peer_discovered=discovered_by_d1.append,
        identity=identities["node-udp-1"],
        keyring=keyring,
    )

    d2 = PeerDiscovery(
        node_id="node-udp-2",
        tcp_host="127.0.0.1",
        tcp_port=10002,
        broadcast_transport=t2,
        clock=clock,
        cluster_id="test-udp-cluster",
        beacon_interval=0.1,
        peer_ttl=1.0,
        on_peer_discovered=discovered_by_d2.append,
        identity=identities["node-udp-2"],
        keyring=keyring,
    )

    await d1.start()
    await d2.start()

    try:
        # Wait for beacons to be exchanged (up to 1.5s)
        for _ in range(20):
            if "node-udp-2" in d1.get_active_peers() and "node-udp-1" in d2.get_active_peers():
                break
            await asyncio.sleep(0.1)

        peers_1 = d1.get_active_peers()
        peers_2 = d2.get_active_peers()
        assert "node-udp-2" in peers_1
        assert peers_1["node-udp-2"] == ("127.0.0.1", 10002)
        assert "node-udp-1" in peers_2
        assert peers_2["node-udp-1"] == ("127.0.0.1", 10001)
    finally:
        await d1.stop()
        await d2.stop()
