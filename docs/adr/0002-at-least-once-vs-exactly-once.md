# ADR 0002: At-Least-Once Delivery Semantics and the Fallacy of Exactly-Once

## Status
Accepted

## Context
Distributed queues frequently advertise "exactly-once delivery". In distributed systems theory (formalized by the Two Generals Problem and the FLP Impossibility Theorem), guaranteed exactly-once delivery across unreliable networks without shared transactional side effects is impossible.

If a worker completes processing but crashes before acknowledging or gossiping the result, another peer must reclaim and execute the task to prevent silent message loss. If the original worker did indeed perform side effects (such as sending an email or charging a payment card), those side effects have occurred, even if the queue re-dispatches the task.

## Decision
`peerq` explicitly guarantees **at-least-once delivery**. We make this guarantee transparent and non-negotiable across all interfaces and documentation:
1. Every task handler must be **idempotent**.
2. If a peer executing a task experiences a network partition or transient failure exceeding its lease duration, the task will be reclaimed and executed by another peer.
3. We explicitly reject marketing terms such as "effectively-once" or "exactly-once delivery".

## Rejected Alternatives
- **Claiming "Exactly-Once Delivery"**:
  Discredited marketing terminology. Any system claiming exactly-once without two-phase commit across both message broker and external storage is silently masking duplicate execution windows.
- **At-Most-Once Delivery**:
  Accepting task loss on peer failure was deemed unacceptable for a reliable task execution mesh.

## Consequences
- **Positive**: Complete architectural honesty. Users are explicitly guided to write idempotent handlers (e.g. using idempotency keys, conditional database writes, or deduplication tables).
- **Trade-off**: In the event of network partitions or premature false suspicion, a task may execute more than once. The fencing token ensures only one result is accepted by the mesh, but external side-effects executed inside the handler must handle repetition.
