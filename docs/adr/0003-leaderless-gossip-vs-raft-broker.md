# ADR 0003: Leaderless Gossip Mesh vs. Raft-Replicated Broker

## Status
Accepted

## Context
When architecting distributed coordination, two primary archetypes exist:
1. **Strong Consensus with Leader (e.g. Raft / Multi-Paxos)**:
   A dedicated elected leader orders all transitions into a linear log. All writes must pass through the leader and achieve quorum replication before committing.
2. **Leaderless Peer-to-Peer Mesh (e.g. Dynamo / Gossip / CRDTs)**:
   All nodes are equal peers. Any peer can accept tasks, process work, and gossip state asynchronously.

We needed to determine whether to build `peerq` around a Raft broker or a leaderless gossip topology.

## Decision
We chose a **leaderless peer-to-peer gossip architecture** with CRDT join-semilattices:
1. Every peer is equal. There is no central point of failure, no coordinator bottleneck, and no split-brain broker partition where minority partitions become completely read-only.
2. Under network partitions, both sides of the partition can continue executing locally available tasks. When the partition heals, gossip anti-entropy reconciles state deterministically via monotonic join-semilattice merges.
3. This is the only architecture in which **vector clocks and fencing tokens are the foundational primitive** rather than decorative overhead.

## Rejected Alternatives
- **Raft / ZooKeeper Replicated Broker**:
  - Requires maintaining a strict quorum ($N/2 + 1$). Under network partitions, a minority partition (e.g. 2 nodes out of 5) becomes completely disabled and cannot progress any work.
  - Creates a single leader bottleneck through which all enqueue and claim throughput must serialize.
  - Raft eliminates the need for vector clocks entirely (since the Raft log index provides a linear wall-time total order), violating our goal of building a true decentralized mesh.
- **External Dependency (Redis / RabbitMQ / etcd)**:
  - Introducing an external broker defeats the zero-dependency, self-contained mesh requirement.

## Consequences
- **Positive**: High availability. Work continues progressing locally even under asymmetric links or severe network partitions.
- **Positive**: Zero external operational dependencies. A `peerq` cluster is formed simply by connecting Python async processes.
- **Trade-off**: Eventual consistency. Task state takes multiple gossip rounds ($O(\log N)$) to converge across the mesh rather than being immediately visible everywhere.
