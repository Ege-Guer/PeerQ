# ADR 0001: Division of Labour Between Leases with Fencing Tokens and Vector Clocks

## Status
Accepted

## Context
In `peerq`, tasks are replicated across equal peers without a centralized coordinator or broker. When multiple peers execute and replicate task states across an asynchronous network subject to arbitrary delays and partitions, two distinct coordination problems arise:
1. **State Replication Ordering & Concurrency Detection**: Determining whether state update $S_1$ causally preceded, succeeded, or is concurrent with state update $S_2$.
2. **Mutual Exclusion & Stale-Write Protection (The Zombie Peer Problem)**: Preventing a worker whose execution took longer than expected (due to an un-monitored GC pause, network partition, or transient freeze) from committing a stale result after another peer has already reclaimed and completed the task.

A naive design might attempt to use Vector Clocks for both mutual exclusion and replication, or conversely, rely purely on lease timestamps.

## Decision
We decouple state replication from lease execution protection into two distinct primitives:
1. **Vector Clocks for Causal State Replication**:
   Every `TaskRecord` carries a `VectorClock`. Vector clocks track causal history ($BEFORE, AFTER, EQUAL, CONCURRENT$) across gossip anti-entropy exchanges. Concurrent states are resolved deterministically through a join-semilattice CRDT total order without wall-clock timestamps.
2. **Leases with Monotonically Increasing Fencing Tokens for Execution Safety**:
   Claiming a task grants a time-bounded lease accompanied by a strictly monotonic `FenceToken = (epoch: int, peer_id: str)`. When a lease expires and another peer reclaims the task, the reclaimer increments the epoch ($epoch_{new} = epoch_{old} + 1$).
   A worker verifying its commit checks:
   - Its lease must not have expired (`clock.now() <= lease_expiry`).
   - The token must not have been superseded by a higher fencing token.
   - Any remote peer receiving a state commit verifies that the incoming `FenceToken` is greater than or equal to any previously accepted token for that task.

## Rejected Alternatives
- **Vector Clocks Alone for Lease Exclusion**:
  Vector clocks capture causality between messages that have already been sent and received. They cannot prevent a disconnected zombie worker from performing work and attempting to write. If worker A is partitioned, worker B increments its clock and completes the task. When A returns with a concurrent vector clock, vector clocks alone cannot distinguish which worker holds the legitimate active lease without an explicit sequencing token.
- **Wall-Clock Last-Write-Wins (LWW)**:
  NTP clock skew and leap seconds make physical timestamps fundamentally unreliable for ordering distributed operations. Two independent nodes claiming a task cannot rely on physical clock comparisons to avoid split-brain execution.

## Consequences
- **Positive**: Strict safety against zombie writes. Reclaim under false suspicion is provably safe because the resurrected peer's stale token is rejected.
- **Positive**: Deterministic CRDT state convergence without centralized sequencers.
- **Trade-off**: Requires task handlers to verify lease validity and mandates that task durations remain smaller than lease renewal intervals.
