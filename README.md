# peerq

`peerq` is a leaderless, peer-to-peer asynchronous task mesh in Python 3.11+. Nodes operate as equal peers without brokers, coordinators, or external infrastructure (no Redis, RabbitMQ, ZooKeeper, or etcd). Peer state is synchronized via gossip anti-entropy using Conflict-Free Replicated Data Types (CRDT join-semilattices) and vector clocks, peer health is monitored dynamically via φ-accrual failure detection, and task ownership is protected by time-bounded leases with monotonically increasing fencing tokens. Delivery semantics are strictly **at-least-once**; all task handlers must be idempotent.

---

## Task Lifecycle & Failure Recovery

```
Peer A (Submitter/Worker)         Peer B (Peer/Reclaimer)
      │                                     │
      ├─────── submit_task("t1") ───────────┤
      │        (state=PENDING, epoch=0)     │
      │                                     │
      ├─────── claim("t1", epoch=1) ────────┤  (Gossip claim broadcast)
      │        (state=CLAIMED, lease=3.0s)  │
      ▼                                     │
 [RUNNING: Worker executes]                 │
      │                                     │
   *CRASH* (Peer A abruptly dies)           │
      x                                     │
      x (Heartbeats cease)                  │
      x                                     ▼
      x                               [φ-Accrual suspends A]
      x                               (φ >= 8.0, lease expired)
      x                                     │
      x                                     ├─────── reclaim("t1", epoch=2)
      x                                     │        (state=CLAIMED, epoch=2)
      x                                     ▼
      x                                [RUNNING: Peer B executes]
      x                                     │
      x                                [DONE: Result committed]
      x                                     │
(Peer A resurrects with stale epoch=1)      │
      │                                     │
      ├── gossip commit("t1", epoch=1) ────►│
      │                                     │
      │◄── REJECT: Stale token (1 < 2) ─────┤ (Fencing token protects mesh)
      ▼                                     ▼
```

---

## Guarantee Matrix

| Failure Mode | What is Guaranteed | What is NOT Guaranteed |
| :--- | :--- | :--- |
| **Worker Crash mid-task** | Lease expires; task is automatically reclaimed by surviving peers with an incremented fencing token. No task is dropped. | Exactly-once execution is not guaranteed; partial work performed before the crash cannot be rolled back by the mesh. |
| **Full-Cluster Crash** | Clean deterministic restart and state recovery when Write-Ahead Log (`peerq.wal`) is enabled. | In pure in-memory mode (`wal=None`), tasks are lost if all nodes crash simultaneously. |
| **Symmetric Partition** | Both partitions continue processing local workloads independently. On heal, CRDT join-semilattice merges states deterministically. | Partitions cannot observe each other's state in real time. Concurrent claims resolve to the higher fencing token upon healing. |
| **Asymmetric Partition (A hears B, B does not hear A)** | Asymmetric failure detection adapts via one-way φ-accrual suspicion; gossip propagates along directed reachable paths ($A \to C \to B$). | Direct point-to-point acknowledgment from the blind node is delayed until multi-hop gossip propagates. |

---

## Benchmarks & Reproduction

All benchmark numbers are measured directly from committed reproducible scripts. Raw outputs, hardware specifications, and execution timestamps are committed under [`bench/results/`](bench/results/).

### Environment
- **Platform**: Darwin 25.5.0 (macOS, arm64)
- **Processor**: Apple Silicon (arm)
- **Python**: 3.12.4 (CPython)
- **Concurrency Model**: Single-threaded `asyncio` event loop

### Measured Results

| Metric | Measured Value | Methodology & Script | Raw Output Reference |
| :--- | :--- | :--- | :--- |
| **Throughput** | **6,933.91 tasks/sec** | 5,000 tasks across 3 peers, in-memory transport | [`bench/results/throughput_raw.txt`](bench/results/throughput_raw.txt) |
| **Average Latency** | **144.22 µs** | End-to-end task turnaround time | [`bench/results/throughput_raw.txt`](bench/results/throughput_raw.txt) |
| **Median Latency (p50)** | **147.5 µs** | High-resolution HDR histogram (3,000 tasks) | [`bench/results/latency_raw.txt`](bench/results/latency_raw.txt) |
| **Tail Latency (p95)** | **178.5 µs** | 95th percentile turnaround time | [`bench/results/latency_raw.txt`](bench/results/latency_raw.txt) |
| **Tail Latency (p99)** | **333.0 µs** | 99th percentile turnaround time | [`bench/results/latency_raw.txt`](bench/results/latency_raw.txt) |
| **Max Tail Latency (p99.9)** | **614.0 µs** | 99.9th percentile turnaround time | [`bench/results/latency_raw.txt`](bench/results/latency_raw.txt) |
| **Telemetry Overhead** | **603.42 ns / op** | `LogLinearHistogram.record()` CPU delta | [`bench/results/overhead_raw.txt`](bench/results/overhead_raw.txt) |
| **Counter Overhead** | **94.84 ns / op** | `MetricsCollector.increment()` CPU delta | [`bench/results/overhead_raw.txt`](bench/results/overhead_raw.txt) |
| **Vector Clock Overhead** | **744.36 ns / op** | Immutable `VectorClock.increment()` CPU delta | [`bench/results/overhead_raw.txt`](bench/results/overhead_raw.txt) |

### Reproducing Locally
```bash
# Run throughput benchmark
python bench/throughput.py

# Run latency quantile benchmark
python bench/latency.py

# Run telemetry overhead benchmark
python bench/overhead.py
```

---

## When NOT to Use This

`peerq` is an opinionated, leaderless distributed systems primitive. In many production architectures, conventional centralized systems are the correct choice:

- **Use Redis / Celery / BullMQ** when you already operate a managed Redis cluster, require simple push/pop queue semantics, and want centralized operational monitoring. Redis is simpler, faster for trivial workloads, and avoids distributed state replication overhead.
- **Use RabbitMQ / Apache Kafka** when your system requires millions of sub-millisecond messages per second, partitioned stream persistence, complex fan-out exchanges, or consumer groups. `peerq` is an async task mesh, not an event streaming log.
- **Use Temporal / Cadence** when your workflows require multi-day state machines, complex human-in-the-loop approvals, distributed saga rollbacks, and persistent event-sourced execution histories. `peerq` executes tasks with short-to-medium leases (seconds to minutes), not long-lived multi-day orchestrations.
- **Do not use `peerq`** if your tasks cannot be made idempotent. Because network partitions and worker crashes trigger lease reclaims, handlers must tolerate duplicate executions safely.

---

## Architectural Decision Records (ADRs)

Key architectural decisions and trade-offs are documented under [`docs/adr/`](docs/adr/):
- [ADR 0001: Division of Labour Between Leases with Fencing Tokens and Vector Clocks](docs/adr/0001-leases-fencing-tokens-vs-vector-clocks.md)
- [ADR 0002: At-Least-Once Delivery Semantics and the Fallacy of Exactly-Once](docs/adr/0002-at-least-once-vs-exactly-once.md)
- [ADR 0003: Leaderless Gossip Mesh vs. Raft-Replicated Broker](docs/adr/0003-leaderless-gossip-vs-raft-broker.md)
- [ADR 0004: Adaptive φ-Accrual vs. Fixed-Timeout Failure Detection](docs/adr/0004-phi-accrual-vs-fixed-timeout.md)
- [ADR 0005: Write-Ahead Log (WAL) and Crash-Recovery State Replay](docs/adr/0005-write-ahead-log-and-crash-recovery.md)

---

## Verification & Quality Gates

The test suite runs in under 5 seconds and enforces strict type and architecture checks:

```bash
# Quality Gates
ruff check .
ruff format --check .
mypy --strict peerq tests bench examples

# Test Suite with Coverage (>= 90% floor)
pytest --cov=peerq --cov-report=term-missing tests/

# Deterministic Simulation Suite
pytest tests/sim/ --durations=10
```

---

## Quickstart & Runnable Demo

```bash
python examples/demo.py
```
