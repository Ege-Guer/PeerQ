# peerq

[![CI](https://github.com/Ege-Guer/PeerQ/actions/workflows/ci.yml/badge.svg)](https://github.com/Ege-Guer/PeerQ/actions/workflows/ci.yml)

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
| **WAL Append (Buffered)** | **60,560 records/sec** (22.9 MB/s) | 30,000 serialized & CRC32-framed TaskRecords | [`bench/results/wal_throughput_raw.txt`](bench/results/wal_throughput_raw.txt) |
| **WAL Append (fsync)** | **16,686 fsyncs/sec** (0.060 ms/op) | Synchronous disk barrier per record | [`bench/results/wal_throughput_raw.txt`](bench/results/wal_throughput_raw.txt) |
| **WAL Replay & Verify** | **218,024 records/sec** (82.6 MB/s) | Sequential replay with 32-bit CRC validation | [`bench/results/wal_throughput_raw.txt`](bench/results/wal_throughput_raw.txt) |
| **Ed25519 Keygen** | **6,712.5 keys/sec** | 5,000 cryptographic keypair generations | [`bench/results/crypto_throughput_raw.txt`](bench/results/crypto_throughput_raw.txt) |
| **Task Signing** | **5,979.6 signs/sec** | 10,000 Ed25519 task digital signatures | [`bench/results/crypto_throughput_raw.txt`](bench/results/crypto_throughput_raw.txt) |
| **Signature Verification** | **2,951.0 verifications/sec** | 10,000 Ed25519 authentications via PeerKeyRing | [`bench/results/crypto_throughput_raw.txt`](bench/results/crypto_throughput_raw.txt) |
| **Byzantine Rejection** | **10.44M rejections/sec** (100% caught) | Tampered payload detection before state merge | [`bench/results/crypto_throughput_raw.txt`](bench/results/crypto_throughput_raw.txt) |
| **Gossip Convergence (3 peers)** | **1.50 s** (3 rounds, 3.05 ms wall) | 30 distributed tasks across 3-node mesh | [`bench/results/convergence_raw.txt`](bench/results/convergence_raw.txt) |
| **Gossip Convergence (10 peers)** | **4.00 s** (8 rounds, 106.28 ms wall) | 100 distributed tasks across 10-node mesh | [`bench/results/convergence_raw.txt`](bench/results/convergence_raw.txt) |

### Reproducing Locally
```bash
# Run throughput benchmark
python bench/throughput.py

# Run latency quantile benchmark
python bench/latency.py

# Run telemetry overhead benchmark
python bench/overhead.py

# Run Write-Ahead Log (WAL) throughput benchmark
python bench/wal_throughput.py

# Run Ed25519 cryptographic throughput benchmark
python bench/crypto_throughput.py

# Run gossip anti-entropy convergence benchmark
python bench/gossip_convergence.py
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
- [ADR 0006: Ed25519 Digital Signatures and Byzantine Fault Resistance](docs/adr/0006-ed25519-signatures-and-byzantine-fault-tolerance.md)
- [ADR 0007: Zero-Configuration Local Peer Discovery via UDP Multicast and Broadcast](docs/adr/0007-zero-configuration-peer-discovery.md)

---

## Quick Start: Python SDK (For Applications & AI Agents)

To integrate `peerq` into your project or invoke it from an autonomous AI agent, install the package and define your application-specific task execution handler:

```python
import asyncio
import random
import socket
from peerq import (
    Ed25519KeyPair,
    PeerDiscovery,
    PeerKeyRing,
    PeerNode,
    RealClock,
    SecurityConfig,
    TcpTransport,
    UdpBroadcastTransport,
)


# 1. Define your project-specific task execution logic
async def my_task_handler(payload: bytes) -> bytes:
    task_str = payload.decode(errors="replace")
    print(f"Executing task: {task_str}")
    # Perform computation, ML inference, image rendering, or database work
    return f"COMPLETED({task_str})".encode()


async def main():
    my_id = socket.gethostname().split(".")[0].lower()
    clock = RealClock()
    identity = Ed25519KeyPair.generate()
    keyring = PeerKeyRing()
    keyring.add_peer(my_id, identity.public_key)
    security = SecurityConfig()
    # Add every expected peer's public key to this ring before enabling discovery.

    # 2. Bind TCP transport to all interfaces (0.0.0.0) for network reachability
    transport = TcpTransport(
        node_id=my_id,
        host="0.0.0.0",
        port=9001,
        peer_addresses={},
        clock=clock,
        identity=identity,
        keyring=keyring,
        security=security,
    )
    await transport.start()

    # 3. Initialize mesh node with your task handler
    node = PeerNode(
        node_id=my_id,
        clock=clock,
        transport=transport,
        rng=random.Random(),
        peers=[],
        handler=my_task_handler,
        identity=identity,
        keyring=keyring,
        security=security,
    )
    await node.start()

    # 4. Enable zero-config UDP discovery (auto-pairs with other nodes on LAN/WiFi)
    b_transport = UdpBroadcastTransport(port=19876, clock=clock, security=security)
    await b_transport.start()
    discovery = PeerDiscovery(
        node_id=my_id,
        tcp_host="0.0.0.0",
        tcp_port=transport.port,
        broadcast_transport=b_transport,
        clock=clock,
        identity=identity,
        keyring=keyring,
        security=security,
    )
    discovery.bind_node(node)
    await discovery.start()

    print(f"Node '{my_id}' is live and participating in the mesh!")

    # 5. Ingest a task into the mesh (will be claimed and executed across available peers)
    await node.submit_task(task_id="task-101", payload=b"Process-Image-Batch-A")

    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
```

See [`examples/custom_worker.py`](examples/custom_worker.py) for a complete standalone implementation.

---

## Multi-Device / Local Network (LAN) Setup

To connect nodes across different physical machines (e.g. your **MacBook** and your **PC** on the same WiFi or LAN):

1. **Install PeerQ on both machines**:
   ```bash
   git clone https://github.com/Ege-Guer/PeerQ.git
   cd PeerQ
   pip install -e .
   ```

2. **Provision identities before starting**: generate one Ed25519 private seed per
   node and exchange only the corresponding public keys through a trusted channel.
   Discovery does not trust an unknown public key from the network.

3. **Start a node on Machine 1 (MacBook)**:
   ```bash
   peerq node --id macbook --host 0.0.0.0 --port 9001 --discovery \
     --private-key-hex <macbook-private-seed> \
     --peer-keys pc-worker=<pc-worker-public-key> --status-port 9102
   ```

4. **Start a node on Machine 2 (PC / Linux)**:
   ```bash
   peerq node --id pc-worker --host 0.0.0.0 --port 9001 --discovery \
     --private-key-hex <pc-worker-private-seed> \
     --peer-keys macbook=<macbook-public-key> --status-port 9102
   ```

> [!TIP]
> - **Zero-Config Pairing**: The nodes will automatically find each other via UDP multicast beacon on port 19876 and establish direct TCP mesh links.
> - **Firewall Note**: If your operating system (macOS Application Firewall or Windows Defender) displays a network prompt, select **Allow / Erlauben** for Python to accept incoming traffic on TCP 9001 and UDP 19876.
> - **Web Dashboard**: The dashboard binds to loopback by default. To expose it remotely, use `--status-host 0.0.0.0 --status-token <random-token-of-at-least-32-chars>` and protect the connection with TLS or a trusted network boundary.

---

## Command-Line Interface (CLI)

The `peerq` CLI provides native commands for production nodes and task ingestion:

```bash
# 1. Start a node with authenticated UDP discovery and local-only status
peerq node --id node-1 --port 9001 --discovery --status-port 9102 \
  --private-key-hex <private-seed> --peer-keys node-2=<public-key>

# 2. Start a peer node on the same machine/LAN (finds node-1 automatically)
peerq node --id node-2 --port 9002 --discovery --status-port 9103 \
  --private-key-hex <private-seed> --peer-keys node-1=<public-key>

# 3. Query cluster topology, queue depth, and health over HTTP
peerq status --endpoint http://127.0.0.1:9102/status

# 4. Ingest a signed task into the running mesh. Provision the submitter's
#    public key on node-1 with --peer-keys cli-client=<client-public-key>.
peerq submit --target-id node-1 --target-port 9001 \
  --target-key-hex <node-1-public-key> --private-key-hex <client-private-seed> \
  --task-id task-100 --payload "process-dataset"

# Explicitly isolated unsigned development mode (never use for a shared network)
peerq node --id dev-node --port 9001 --insecure-dev
```

---

## Verification & Quality Gates

The test suite runs in under 8 seconds and enforces strict type, architecture, and invariant checks:

```bash
# Quality Gates
ruff check .
ruff format --check .
mypy --strict peerq tests bench examples scripts

# Test Suite with Coverage (>= 90% floor)
pytest --cov=peerq --cov-report=term-missing tests/

# Deterministic Simulation Suite
pytest tests/sim/ --durations=10
```

---

## Interactive Web Dashboard

PeerQ includes a lightweight, real-time single-page web UI built directly into the node runtime:
- Live cluster topology & node health status (green/amber peer status pills)
- Task state breakdown (Submitted, Claimed, Running, Completed, Failed, Timed Out)
- Real-time active task leases with fence epoch and remaining lease TTL
- Vector clock logical sequence counters and peer credit flow meters
- Embedded Prometheus counters and p50/p99 execution latency summaries
- Accessible at `http://127.0.0.1:9102/` (or `/dashboard`)

---

## Container Deployment (Docker & Compose)

Spin up an isolated 3-node container example with UDP discovery and local WAL
persistence. Secure discovery still requires operator-provisioned peer keys;
unknown beacons are rejected by default.

```bash
# Build and boot the 3-node container skeleton
docker compose up --build

# The status listener is loopback-only inside each container by default. Secure
# discovery also rejects unknown beacons until the operator provisions each
# node's Ed25519 private key and the matching --peer-keys entries. This keeps
# the example fail-closed; see docs/SECURITY.md for the provisioning contract.

# Query node-2 dashboard from inside its container
docker compose exec node-2 python -c "import urllib.request; print(urllib.request.urlopen('http://127.0.0.1:9102/status').read().decode())"
```

The published host ports are not dashboard access while the services bind to
container loopback. For intentional remote dashboard access, configure a
32-character-or-longer bearer token and an explicit non-loopback
`--status-host`; protect that connection with TLS or a trusted network
boundary. Do not use `--insecure-dev` outside an isolated development test.

---

## Runnable Demos

```bash
# In-memory mesh demonstration (task execution, gossip, lease fencing)
python examples/demo.py

# Custom worker with application task handler and subnet discovery
python examples/custom_worker.py

# Zero-configuration cluster (UDP multicast discovery, Ed25519 signatures, HTTP dashboard)
python examples/zero_config_cluster.py
```
