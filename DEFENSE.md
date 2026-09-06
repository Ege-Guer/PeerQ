# Distributed Systems Architecture Defense

*Internal engineering defense document. Addresses the 10 hardest adversarial questions about the `peerq` architecture.*

---

### 1. Why build a leaderless mesh when Raft, Redis, or RabbitMQ already exist?
**Answer**: Central broker architectures (Redis/RabbitMQ) introduce a single point of failure, operational complexity, and a single throughput bottleneck. Raft-replicated brokers require strict quorums ($N/2 + 1$), meaning any minority partition becomes completely paralyzed. `peerq` is designed for environments where peers are co-located in an application cluster, work must continue locally under partial partitions, and zero external infrastructure dependencies (no external daemon, database, or coordinator) are permitted.

### 2. How do you generate a monotonic fencing token without a central sequencer?
**Answer**: By defining `FenceToken = (epoch: int, peer_id: str)` with a lexicographical total order. When a peer claims or reclaims a task, it computes `epoch = max(known_epochs) + 1` and attaches its unique `peer_id`. Because `(epoch_A, peer_A) != (epoch_B, peer_B)` for any distinct peers, tokens are strictly monotonic and unique across the entire distributed system without needing a coordinator.

### 3. What happens if two partitioned peers reclaim the same task concurrently?
**Answer**: Both peers will increment the epoch counter and attempt execution. Because peer IDs break ties in `FenceToken`, one token strictly dominates the other ($token_1 > token_2$). When the partition heals and gossip exchanges state, the CRDT join-semilattice deterministically accepts the result with the higher fencing token and discards the lower one. Because task handlers are required to be idempotent, the duplicated execution is harmless to the state lattice.

### 4. How do you prevent a thundering herd where multiple peers simultaneously reclaim a dead peer's tasks?
**Answer**: Through deterministic **Rendezvous Hashing** (Highest Random Weight). For any task `T` and the current set of live peers `P`, every node computes `select_reclaimer(T, P) = argmax_{p in P} hash(T, p)`. Exactly one surviving peer is elected the primary reclaimer. Other live peers will not attempt to reclaim unless the primary reclaimer itself is suspected. Furthermore, claims are immediately broadcast via gossip with an active lease.

### 5. Why use vector clocks if task ownership conflicts are resolved by a fencing token lattice?
**Answer**: Division of labour (see ADR 0001). Vector clocks track causal dependency between gossip anti-entropy exchanges (knowing whether node A has observed node B's updates). Fencing tokens track execution authorization and prevent resurrected zombie workers from committing stale results. Vector clocks tell us *when* updates happened in logical time; fencing tokens tell us *who* had valid authorization to commit.

### 6. What happens if a node experiences a stop-the-world pause longer than its lease duration?
**Answer**: The pause causes heartbeat silence, so the mesh accrues suspicion $\phi$ and the lease expires. Another peer reclaims the task with an incremented fencing token. When the paused node resumes, its execution finishes, but its commit is rejected because:
1. `clock.now() > lease_expiry` triggers an immediate local reject.
2. Even if local time were manipulated, when the paused node attempts to gossip its result, any remote peer rejects the result because the token is lower than the already accepted successor token. The zombie write is neutralized.

### 7. In a leaderless gossip protocol, what guarantees that a pending task is ever picked up?
**Answer**: Liveness guarantee under assumption of at least one live worker:
When a task is submitted as `PENDING`, it is stored in the local CRDT table and enqueued in the local priority queue. If the submitter has no worker handler, its periodic gossip loop selects peers uniformly at random. In a connected graph, gossip spreads exponentially ($O(\log N)$ rounds). Any peer receiving a `PENDING` task enqueues it to its local priority queue. As long as at least one peer in the component has an active worker loop, the task is dequeued and claimed.

### 8. How does credit flow control prevent queue starvation when multiple senders target the same peer?
**Answer**: Each sender maintains a dedicated send credit window per receiver (modeled on HTTP/2 stream flow control). A receiver grants initial credits (e.g. 20) and replenishes credits only when tasks complete. If senders exceed the receiver's capacity, their local `acquire()` blocks on an `asyncio.Future` FIFO queue, applying true upstream backpressure rather than dropping packets or blowing memory.

### 9. Why fit a normal distribution in φ-accrual when network inter-arrivals are heavy-tailed?
**Answer**: Known theoretical approximation. Hayashibara et al. (2004) proved that while raw internet delays exhibit heavy tails, inter-arrival times over short sliding windows (e.g. 100-1000 samples) under steady periodic heartbeats are closely approximated by a Gaussian distribution. Furthermore, the variance floor $\sigma \ge \sigma_{floor}$ prevents the normal distribution's tail from shrinking to zero on low-jitter links.

### 10. What is the single biggest weakness in this entire architecture?
**Answer**: **In-memory state replication overhead under high task volumes.** Because every task is represented as a CRDT `TaskRecord` in memory and gossiped between peers, running millions of tiny, short-lived tasks creates high GC churn and network gossip overhead. For millions of ephemeral sub-millisecond messages, a centralized broker (e.g. Kafka or Redis) is superior. `peerq` is optimized for coarse-to-medium granularity asynchronous jobs (10 ms to 10 s) where node autonomy and zero infrastructure dependencies matter more than millions of events per second.
