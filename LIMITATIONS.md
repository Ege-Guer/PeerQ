# `peerq` Known Limitations & Boundaries

In accordance with our non-negotiable honesty rules, this document explicitly records what `peerq` does NOT provide.

---

## 1. No Persistent Disk Storage (In-Memory Only in v1)
`peerq` v1 is an in-memory async task mesh. Task states, vector clocks, and queues are maintained in process memory.
- **Consequence**: If the entire cluster is stopped or crashes simultaneously, all in-flight and pending tasks are lost.
- **Scope**: Designed for transient task distribution, cache-warming, distributed web crawling, and background jobs where tasks can be re-submitted if the cluster restarts. Persistent durability requires an external durable storage layer or a future disk WAL ADR.

## 2. Strictly At-Least-Once Delivery
- `peerq` guarantees at-least-once execution. It does **NOT** provide exactly-once execution.
- If a peer's execution exceeds its lease duration or is falsely suspected by a partitioned network, another peer will reclaim and execute the task.
- While the fencing token guarantees that only one peer's result will be accepted by the mesh, any external side effects (database writes, HTTP requests, emails) performed inside the user handler will have executed multiple times. All handlers **must** be idempotent.

## 3. Cooperative Cluster Scale (N <= 50)
- `peerq` uses full-mesh heartbeat monitoring and randomized gossip. Every peer maintains a failure detector history for all other known peers.
- This design operates efficiently for small-to-medium clusters (3 to 50 nodes). It is **not** designed for thousands of nodes. Scaling to massive node counts would require hierarchical gossip overlays (e.g. SWIM / Plumtree).

## 4. Crash-Fault-Tolerant Only (Non-Byzantine)
- Assumes nodes are honest and crash-stop or crash-recovery.
- Does not defend against Byzantine (malicious or compromised) peers that forge vector clocks or manipulate epoch counters.

## 5. Task Payload Limits
- Designed for task descriptors, job arguments, and references (typically < 64 KB, hard upper limit ~1 MB).
- Large binary artifacts, video streams, or large datasets should be stored in object storage (S3/GCS/MinIO) with only the reference URI passed in the task payload.

## 6. Handler Execution vs. Lease Expiry
- If a task handler takes longer than `lease_duration` without an explicit lease extension, its lease will expire.
- When the lease expires, the task becomes reclaimable by other peers. When the original worker finally finishes, its result will be rejected by its own lease expiry check. Tasks must be sized to complete well within the configured lease window.
