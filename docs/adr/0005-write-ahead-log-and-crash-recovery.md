# ADR 0005: Write-Ahead Log (WAL) and Crash-Recovery State Replay

## Status
Accepted

## Context
Prior to v0.2.0, `peerq` was purely in-memory. If a node process terminated or experienced an operating system crash, all local task records, claimed states, and vector clocks were erased. While tasks could theoretically be re-discovered via gossip from surviving peers, a simultaneous restart or a cluster partition during node restart risked state amnesia or redundant re-execution of completed tasks.

An open-source distributed task mesh requires durable state persistence with explicit guarantees:
1. **Crash Durability**: Tasks and vector clocks must survive process crashes and reboot cycles.
2. **Data Integrity & Torn-Write Protection**: Unclean terminations mid-write must not corrupt historical log entries or prevent log recovery.
3. **Zero External Dependencies**: The storage engine must be pure Python standard library without C extensions (e.g. SQLite, RocksDB) or native build steps.
4. **Log Compaction**: Unbounded append logs must support checkpointing to prevent disk exhaustion.

## Decision
We implement a dedicated, append-only binary Write-Ahead Log (`peerq.wal.WriteAheadLog`) with the following technical specification:

1. **Header & Frame Layout**:
   - Magic Header (8 bytes): `b"PQWL\x01\x00\x00\x00"` (identifies file format, version 1, and alignment padding).
   - Binary Frame Header (9 bytes):
     ```
     +-----------------------+--------------------+-------------------+
     | Payload Length uint32 | CRC32 uint32       | Record Type uint8 |
     | (4 bytes, big-endian) | (4 bytes, IEEE)    | (1 byte)          |
     +-----------------------+--------------------+-------------------+
     ```
   - Payload: Deterministic JSON-serialized UTF-8 bytes.

2. **Integrity & Torn-Write Tolerant Replay**:
   - Every frame computes CRC32 checksum over `record_type + payload_bytes`.
   - During recovery replay (`replay(strict=False)`), any partial write at the end of the file (fewer than 9 bytes header or fewer than `payload_len` bytes) is treated as an in-flight write interrupted by a crash. The replay halts gracefully at the clean boundary, recovering all verified historical records.
   - Any corruption prior to EOF raises a strict `WalCorruptError`.

3. **Atomic Checkpointing & Compaction**:
   - Nodes periodically or on demand compact the log via `wal.checkpoint(tasks, vector_clock)`.
   - Checkpointing writes an atomic snapshot record (`RECORD_CHECKPOINT`) to a `.tmp` file and executes an atomic POSIX filesystem rename (`tmp_path.replace(wal_path)`), preserving crash consistency.

4. **Integration with `PeerNode`**:
   - On initialization, `PeerNode` accepts an optional `wal: WriteAheadLog`.
   - On `start()`, the node replays the WAL into `_tasks` and `_vector_clock`, re-enqueueing any pending tasks that were in-flight.
   - On state mutations (`submit_task`, `_broadcast_task_update`, and gossip merge), entries are appended to the WAL.

## Rejected Alternatives
- **SQLite / Embedded Relational Database**:
  Rejected. Introduces heavy multi-process locking overhead, complex connection management in async event loops, and external SQL semantics for what is fundamentally an append-only time-ordered journal.
- **Unframed JSON Lines (`.jsonl`)**:
  Rejected. JSON Lines have no framing headers, no checksums, and cannot distinguish a valid incomplete JSON string from a torn disk write without fragile heuristic parsing.
- **Synchronous fsync on Every Operation**:
  Rejected as mandatory default. Synchronous disk flushes reduce throughput by 100x to 1000x due to disk spindle/SSD sync latency. Configurable via `sync_on_write=True` for high-durability environments.

## Consequences
- **Positive**: Nodes reboot cleanly after crash-stop faults with zero task amnesia.
- **Positive**: Complete data integrity verified by 32-bit CRC checksums.
- **Positive**: Zero external dependencies, pure Python standard library.
- **Trade-off**: Disk I/O overhead on task mutations; mitigated by OS write buffering and atomic snapshot compaction.
