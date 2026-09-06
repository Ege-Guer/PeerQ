# `peerq` Architecture & System Design Document

## 1. Architectural Philosophy

`peerq` is an async, leaderless distributed task mesh in Python 3.11+. Every node is an equal peer:
- **No Broker**: No central Redis, RabbitMQ, or message broker.
- **No Coordinator**: No ZooKeeper, etcd, or Consul.
- **No Single Leader**: No Raft or Paxos leader bottleneck.
- **Single-Threaded Asyncio**: Pure asynchronous event loop with zero threads and zero multiprocessing.

---

## 2. Fundamental Protocols & Injected Abstractions

To achieve 100% deterministic simulation testing, three strict architectural constraints are enforced by AST test inspection (`tests/unit/test_architecture.py`):

1. **Clock Isolation (`peerq.clock`)**:
   - `Clock` protocol with `now() -> float` and `async sleep(delay: float) -> None`.
   - `RealClock`: Wraps `time.monotonic()` and `asyncio.sleep()`.
   - `SimClock`: Manually stepped virtual time driving simulated event queues.
2. **Transport Isolation (`peerq.transport`)**:
   - `Transport` protocol with `async send(peer_id, msg) -> None` and `async recv() -> tuple[str, Message]`.
   - `TcpTransport`: Length-prefixed binary framing over asyncio TCP sockets.
   - `InMemoryTransport`: In-process router managed by `SimNetwork` capable of injecting configurable latency, jitter, drops, and symmetric/asymmetric partitions.
3. **Randomness Isolation**:
   - No module invokes global `random.*`. All stochastic choices inject a seeded `random.Random` instance.

---

## 3. Data Structures & Consensus Lattice

### 3.1 Vector Clocks
A `VectorClock` maps `peer_id -> int`. Counters are monotonically non-decreasing. Clocks are compared pairwise:
- $A \le B \iff \forall k: A[k] \le B[k]$
- $A < B \implies$ `ClockComparison.BEFORE`
- $A > B \implies$ `ClockComparison.AFTER`
- $A == B \implies$ `ClockComparison.EQUAL`
- Otherwise: `ClockComparison.CONCURRENT`

### 3.2 Fencing Tokens
`FenceToken = (epoch: int, peer_id: str)`
Ordered lexicographically by `(epoch, peer_id)`. When claiming or reclaiming an expired lease, the claiming peer computes:
$$epoch_{new} = epoch_{known} + 1$$
This produces a strictly increasing, totally ordered sequence without central sequencers.

### 3.3 Task Lifecycle State Machine
```
   [PENDING]
       │
       ▼ (claim with FenceToken)
   [CLAIMED]
       │
       ▼ (handler starts)
   [RUNNING] ──────────┐ (lease expired & suspected)
    /     \            │
   ▼       ▼           ▼
[DONE]  [FAILED]   [CLAIMED] (reclaimed with epoch + 1)
```

### 3.4 CRDT Join-Semilattice Merge
When two records for the same task are merged via gossip, the winner is determined by a strict total-order key:
$$\text{Key}(R) = (\text{terminal\_flag}, \text{fence\_token}, \text{state\_rank}, \text{updated\_by}, \text{claimed\_by}, \dots)$$
The merged record adopts the winning state attributes and component-wise merges vector clocks:
$$VC_{merged} = VC_1 \sqcup VC_2 = \max(VC_1, VC_2)$$
Hypothesis property tests prove this merge is strictly **commutative, associative, and idempotent**.

---

## 4. Failure Detection (φ-Accrual)

Based on Hayashibara et al. (2004). Tracks sliding window of heartbeat inter-arrivals $\Delta t$. Fits normal distribution ($\mu, \sigma$):
$$P_{later}(t) = \frac{1}{2} \operatorname{erfc}\left(\frac{t - \mu}{\sigma \sqrt{2}}\right)$$
$$\phi = -\log_{10}(P_{later}(t))$$
- **Variance Floor**: $\sigma \ge \sigma_{floor}$ prevents zero division or hyper-sensitivity on low-jitter local links.
- **Cold Start**: Returns $\phi = 0.0$ when sample count $< N_{min}$.
- **Unknown Peer**: Returns $\phi = \infty$.

---

## 5. Flow Control & Backpressure

- `BackpressurePriorityQueue`: `heapq` priority queue with monotonic sequence numbers guaranteeing strict FIFO ordering within equal priority.
- Two selectable backpressure policies:
  1. `REJECT_ON_FULL`: Raises `QueueFull` when capacity reached.
  2. `CREDIT_BASED`: HTTP/2-style credit windows. Senders consume credits; receivers replenish credits upon task completion.

---

## 6. Telemetry & Metrics

- `LogLinearHistogram`: HDR-histogram principle. Decomposes values into base-2 octaves with $2^B$ linear sub-buckets per octave ($B=7 \implies 128$ sub-buckets).
- Relative error bound:
  $$\text{Relative Error} \le \frac{1}{2^B} = 0.78125\%$$
- Bounded memory ($O(1)$ allocations) and $O(1)$ record time.
