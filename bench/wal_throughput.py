"""
WAL Throughput and Replay Benchmark.

Measures:
1. Buffered WAL append throughput (records/sec and MB/sec).
2. Synchronous fsync append throughput (fsyncs/sec).
3. Checksum-verified sequential log replay throughput.

Honesty rule: Commit raw unedited output to bench/results/wal_throughput_raw.txt.
"""

from __future__ import annotations

import platform
import sys
import tempfile
import time
from pathlib import Path

# Ensure project root is on sys.path
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from peerq.consensus import FenceToken, TaskRecord, TaskState, VectorClock  # noqa: E402
from peerq.wal import WriteAheadLog  # noqa: E402


def make_record(i: int) -> TaskRecord:
    return TaskRecord(
        task_id=f"bench-task-{i}",
        state=TaskState.DONE,
        payload=b'{"action":"compute","matrix":[1.2,3.4,5.6,7.8]}',
        result=b'{"status":"ok","value":42.0}',
        error=None,
        claimed_by="node-bench",
        fence_token=FenceToken(epoch=1, peer_id="node-bench"),
        lease_expiry=0.0,
        vector_clock=VectorClock({"node-bench": i + 1}),
        updated_by="node-bench",
    )


def run_wal_benchmarks() -> None:
    print("=" * 65)
    print("PEERQ WRITE-AHEAD LOG (WAL) THROUGHPUT BENCHMARK")
    print("=" * 65)
    print(f"Timestamp:       {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}")
    print(f"Platform:        {platform.platform()}")
    print(f"Processor:       {platform.processor() or platform.machine()}")
    print(f"Python:          {sys.version.split()[0]} ({platform.python_implementation()})")
    print(f"Command line:    {' '.join(sys.argv)}")
    print("=" * 65)

    with tempfile.TemporaryDirectory() as tmpdir:
        wal_path = Path(tmpdir) / "bench.wal"

        # 1. Buffered WAL Append
        count_buffered = 30_000
        wal = WriteAheadLog(wal_path, sync_on_write=False)
        print(f"\nPhase 1: Appending {count_buffered:,} records (buffered OS cache)...")

        t0 = time.perf_counter()
        for i in range(count_buffered):
            rec = make_record(i)
            wal.append_task(rec)
        wal.sync()
        t1 = time.perf_counter()

        elapsed_buffered = t1 - t0
        file_size_bytes = wal_path.stat().st_size
        file_size_mb = file_size_bytes / (1024 * 1024)
        throughput_buffered = count_buffered / elapsed_buffered
        mb_per_sec = file_size_mb / elapsed_buffered

        print(f"  Total records:     {count_buffered:,}")
        print(f"  Elapsed time:      {elapsed_buffered:.4f} s")
        print(f"  Log file size:     {file_size_mb:.2f} MB ({file_size_bytes:,} bytes)")
        print(f"  Append Throughput: {throughput_buffered:,.1f} records/sec")
        print(f"  Write Bandwidth:   {mb_per_sec:,.2f} MB/sec")

        # 2. Synchronous fsync Append
        count_sync = 500
        print(f"\nPhase 2: Appending {count_sync:,} records (strict os.fsync on every record)...")

        t0_sync = time.perf_counter()
        for i in range(count_sync):
            rec = make_record(count_buffered + i)
            wal.append_task(rec, sync=True)
        t1_sync = time.perf_counter()

        elapsed_sync = t1_sync - t0_sync
        throughput_sync = count_sync / elapsed_sync
        avg_fsync_ms = (elapsed_sync / count_sync) * 1000.0

        print(f"  Total records:     {count_sync:,}")
        print(f"  Elapsed time:      {elapsed_sync:.4f} s")
        print(f"  Sync Throughput:   {throughput_sync:,.1f} fsyncs/sec")
        print(f"  Average fsync:     {avg_fsync_ms:.3f} ms/record")

        wal.close()

        # 3. Sequential Log Replay with CRC32 verification
        total_records = count_buffered + count_sync
        print(f"\nPhase 3: Sequential replay & CRC32 verification ({total_records:,} records)...")

        wal_replay = WriteAheadLog(wal_path)
        t0_replay = time.perf_counter()
        replayed_count = 0
        for _record in wal_replay.replay(strict=True):
            replayed_count += 1
        t1_replay = time.perf_counter()

        elapsed_replay = t1_replay - t0_replay
        throughput_replay = replayed_count / elapsed_replay
        replay_mb_sec = (wal_path.stat().st_size / (1024 * 1024)) / elapsed_replay

        print(f"  Verified records:  {replayed_count:,}")
        print(f"  Elapsed time:      {elapsed_replay:.4f} s")
        print(f"  Replay Throughput: {throughput_replay:,.1f} records/sec")
        print(f"  Replay Bandwidth:  {replay_mb_sec:,.2f} MB/sec")
        wal_replay.close()

    print("\n" + "=" * 65)
    print("WAL BENCHMARK COMPLETE")
    print("=" * 65)


if __name__ == "__main__":
    run_wal_benchmarks()
