# ADR 0007: Zero-Configuration Local Peer Discovery via UDP Multicast and Broadcast

## Status
Accepted

## Context
Prior to v0.2.0, cluster bootstrapping required operator manual configuration: every node had to be explicitly provided the IP addresses and TCP ports of all peer nodes via CLI flags or static configuration files. In dynamic environments (e.g. local Kubernetes pods, ad-hoc developer meshes, edge IoT clusters), static addressing is fragile and introduces operational toil.

A modern peer-to-peer library must support zero-configuration discovery on local subnets while upholding:
1. **Architectural Isolation**: All socket management and datagram endpoints must remain strictly inside `peerq.transport` without leaking raw socket operations into application logic.
2. **Deterministic Simulation Testing**: Discovery must be fully testable in virtual time via `SimClock` and in-memory simulated networks without requiring live OS networking.
3. **Multi-Platform Support**: Must work seamlessly across macOS, Linux, and Windows, including multi-instance loopback scenarios on developer workstations.

## Decision
We implement `peerq.discovery.PeerDiscovery` backed by the `BroadcastTransport` abstraction in `peerq.transport`:

1. **Protocol Separation**:
   - `BroadcastTransport` protocol defines `send_broadcast(data: bytes)` and `recv_broadcast() -> (str, bytes)`.
   - `SimBroadcastTransport` provides deterministic simulated broadcast over `SimNetwork`.
   - `UdpBroadcastTransport` provides real OS networking using UDP datagrams.

2. **Multicast and Subnet Broadcast Hybrid**:
   - Uses site-local administrative multicast (`239.255.42.99`, RFC 2365) and standard IPv4 broadcast (`255.255.255.255`).
   - Sockets enable `SO_REUSEADDR` and `SO_REUSEPORT` (BSD/macOS/Linux) and `IP_MULTICAST_LOOP`, allowing multiple local nodes or tests to bind to the same discovery port simultaneously.

3. **Authenticated Beacon Framing & Topology Lifecycle**:
   - Nodes periodically broadcast bounded protocol-v2 JSON envelopes signed with
     Ed25519. The payload contains `node_id`, `host`, `port`, `cluster_id`, and
     the advertised public key for diagnostics.
   - A beacon is accepted only when its signer is already present in the local
     `PeerKeyRing`, its signature and protocol version are valid, its timestamp
     is fresh, and its nonce has not been replayed. The advertised public key
     is never trusted automatically.
   - Different cluster IDs (`cluster_id`) are partitioned and ignored, preventing cross-environment leakage between production and test nodes on shared subnets.
   - Nodes maintain a sliding TTL window (`peer_ttl`). If a peer stops broadcasting (e.g. crash or network detachment), it is automatically purged from the active directory.

4. **Dynamic PeerNode Binding**:
   - `discovery.bind_node(node)` dynamically introduces newly discovered peers into `node.peers`, registers their TCP addresses in `TcpTransport.peer_addresses`, and allocates initial flow control credits in `CreditFlowController`.

## Rejected Alternatives
- **External Coordinator (Consul, Etcd, ZooKeeper)**:
  Rejected. Contradicts the fundamental core design tenet of `peerq` as an autonomous leaderless mesh with zero external daemon dependencies.
- **mDNS / Bonjour (Zeroconf / Avahi / pyzeroconf)**:
  Rejected as mandatory dependency. Full mDNS protocol stacks require DNS-SD record parsing, PTR/SRV/TXT records, and significant dependency overhead. Simple lightweight UDP beacons satisfy 100% of the mesh requirements with zero third-party dependencies.

## Consequences
- **Positive**: Zero-config bootstrap: nodes on the same subnet find each other automatically.
- **Positive**: Complete architectural isolation: zero socket calls outside `peerq.transport`.
- **Positive**: 100% deterministic testing via `SimBroadcastTransport`.
- **Trade-off**: Subnet broadcast/multicast is limited to local Layer 2 broadcast domains. WAN or cross-VPC peering still uses explicit TCP addresses.
- **Trade-off**: Discovery is zero-address-configuration, not zero-trust-configuration:
  operators still provision peer public keys out of band.
