# `peerq` Known Limitations & Boundaries

In accordance with our non-negotiable honesty rules, this document explicitly records what `peerq` does NOT provide.

---

## 1. Optional Durability via Write-Ahead Log (`peerq.wal`)
`peerq` provides crash-recovery durability through an append-only binary Write-Ahead Log (`peerq.wal.WriteAheadLog`, see [ADR 0005](docs/adr/0005-write-ahead-log-and-crash-recovery.md)).
- **With WAL enabled**: Nodes journal state transitions (task admissions, worker claims, result commits, and vector clocks) to disk framed by 32-bit CRC checksums. On reboot, nodes replay the journal to reconstruct state and re-enqueue pending tasks. Unclean crash boundaries with partial torn writes are gracefully isolated.
- **Without WAL (default)**: If initialized without a WAL (`wal=None`), nodes operate in pure in-memory mode for transient workloads (e.g. cache warming, ephemeral scrapers), where process termination resets local state.


## 2. Strictly At-Least-Once Delivery
- `peerq` guarantees at-least-once execution. It does **NOT** provide exactly-once execution.
- If a peer's execution exceeds its lease duration or is falsely suspected by a partitioned network, another peer will reclaim and execute the task.
- While the fencing token guarantees that only one peer's result will be accepted by the mesh, any external side effects (database writes, HTTP requests, emails) performed inside the user handler will have executed multiple times. All handlers **must** be idempotent.

## 3. Cooperative Cluster Scale (N <= 50)
- `peerq` uses full-mesh heartbeat monitoring and randomized gossip. Every peer maintains a failure detector history for all other known peers.
- This design operates efficiently for small-to-medium clusters (3 to 50 nodes). It is **not** designed for thousands of nodes. Scaling to massive node counts would require hierarchical gossip overlays (e.g. SWIM / Plumtree).

## 4. Authenticated Runtime with Explicit Trust Bootstrap
- Normal protocol-v2 runtime traffic is authenticated with Ed25519. Unknown,
  unsigned, stale, replayed, or tampered messages are rejected before state
  merge.
- This does not make the system fully Byzantine-safe: trust is anchored in the
  operator-provisioned public-key ring, and a compromised authorized private
  key or host remains in scope.
- The old unsigned mode is retained only as an explicit development/test escape
  hatch (`SecurityConfig(enabled=False)` or `--insecure-dev`). It is not a
  secure deployment mode.

## 5. Task Payload Limits
- Designed for task descriptors, job arguments, and references (typically < 64 KB, hard upper limit ~1 MB).
- Large binary artifacts, video streams, or large datasets should be stored in object storage (S3/GCS/MinIO) with only the reference URI passed in the task payload.

## 6. Handler Execution vs. Lease Expiry
- If a task handler takes longer than `lease_duration` without an explicit lease extension, its lease will expire.
- When the lease expires, the task becomes reclaimable by other peers. When the original worker finally finishes, its result will be rejected by its own lease expiry check. Tasks must be sized to complete well within the configured lease window.
