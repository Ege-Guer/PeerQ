"""
Unit tests for peerq.wal (Durable Write-Ahead Log).
"""

from pathlib import Path

import pytest

from peerq.consensus import FenceToken, TaskRecord, TaskState, VectorClock
from peerq.wal import (
    RECORD_CHECKPOINT,
    RECORD_CLOCK,
    RECORD_TASK,
    WalCorruptError,
    WalError,
    WriteAheadLog,
)


def _sample_task(task_id: str = "t1", state: str = "pending") -> TaskRecord:
    return TaskRecord(
        task_id=task_id,
        state=TaskState(state),
        payload=b"test-payload",
        result=None,
        error=None,
        claimed_by=None,
        fence_token=FenceToken(epoch=1, peer_id="p1"),
        lease_expiry=100.0,
        vector_clock=VectorClock({"p1": 1}),
        updated_by="p1",
    )


def test_wal_create_and_append(tmp_path: Path) -> None:
    wal_path = tmp_path / "test.wal"
    wal = WriteAheadLog(wal_path)

    t1 = _sample_task("task-1")
    clock = VectorClock({"p1": 5, "p2": 3})

    off1 = wal.append_task(t1)
    assert off1 >= 8  # Past 8-byte magic header

    off2 = wal.append_clock(clock)
    assert off2 > off1

    wal.close()

    # Replay
    records = list(wal.replay())
    assert len(records) == 2
    assert records[0].record_type == RECORD_TASK
    assert records[0].payload["task_id"] == "task-1"
    assert records[1].record_type == RECORD_CLOCK
    assert records[1].payload == {"p1": 5, "p2": 3}


def test_wal_invalid_header(tmp_path: Path) -> None:
    bad_path = tmp_path / "bad.wal"
    bad_path.write_bytes(b"NOT_A_VALID_WAL_HEADER")

    with pytest.raises(WalCorruptError, match="Invalid WAL magic header"):
        WriteAheadLog(bad_path)


def test_wal_unsupported_version(tmp_path: Path) -> None:
    bad_ver = tmp_path / "bad_ver.wal"
    bad_ver.write_bytes(b"PQWL\x02\x00\x00\x00")

    with pytest.raises(WalCorruptError, match="Unsupported WAL version"):
        WriteAheadLog(bad_ver)


def test_wal_checksum_corruption_detected(tmp_path: Path) -> None:
    wal_path = tmp_path / "corrupt.wal"
    wal = WriteAheadLog(wal_path)
    wal.append_task(_sample_task("t1"))
    wal.close()

    raw = bytearray(wal_path.read_bytes())
    # Corrupt a byte in the payload area
    raw[-1] ^= 0xFF
    wal_path.write_bytes(bytes(raw))

    with pytest.raises(WalCorruptError, match="Checksum mismatch"):
        list(wal.replay())


def test_wal_torn_write_recovery(tmp_path: Path) -> None:
    wal_path = tmp_path / "torn.wal"
    wal = WriteAheadLog(wal_path)
    wal.append_task(_sample_task("t1"))
    wal.append_task(_sample_task("t2"))
    wal.close()

    raw = wal_path.read_bytes()
    # Truncate the file 5 bytes into the second record
    truncated = raw[:-5]
    wal_path.write_bytes(truncated)

    # Non-strict replay should gracefully stop at clean boundary
    replayed = list(wal.replay(strict=False))
    assert len(replayed) == 1
    assert replayed[0].payload["task_id"] == "t1"

    # Strict replay should raise WalCorruptError
    with pytest.raises(WalCorruptError):
        list(wal.replay(strict=True))


def test_wal_checkpoint_and_compaction(tmp_path: Path) -> None:
    wal_path = tmp_path / "compact.wal"
    wal = WriteAheadLog(wal_path)

    t1 = _sample_task("t1", state="done")
    t2 = _sample_task("t2", state="pending")
    vc = VectorClock({"p1": 10})

    wal.append_task(t1)
    wal.append_task(t2)
    wal.append_clock(vc)

    # Compact
    wal.checkpoint({"t1": t1, "t2": t2}, vc)
    wal.close()

    records = list(wal.replay())
    assert len(records) == 1
    assert records[0].record_type == RECORD_CHECKPOINT
    assert "t1" in records[0].payload["tasks"]
    assert "t2" in records[0].payload["tasks"]
    assert records[0].payload["vector_clock"] == {"p1": 10}


def test_wal_context_manager_and_sync(tmp_path: Path) -> None:
    wal_path = tmp_path / "sync.wal"
    with WriteAheadLog(wal_path, sync_on_write=True) as wal:
        wal.append_task(_sample_task("t_sync"), sync=True)
        wal.sync()

    assert wal_path.exists()
    assert wal_path.stat().st_size > 8


def test_wal_closed_file_error(tmp_path: Path) -> None:
    wal_path = tmp_path / "closed.wal"
    wal = WriteAheadLog(wal_path)
    wal.close()

    with pytest.raises(WalError, match="closed"):
        wal.append(RECORD_TASK, {"dummy": 1})


def test_wal_record_repr() -> None:
    from peerq.wal import WalRecord

    r = WalRecord(offset=12, record_type=RECORD_TASK, payload={"foo": "bar"})
    assert "offset=12" in repr(r)
    assert f"type={RECORD_TASK}" in repr(r)


def test_wal_replay_nonexistent_file(tmp_path: Path) -> None:
    wal = WriteAheadLog(tmp_path / "created.wal")
    wal.close()
    wal.path = tmp_path / "never_existed.wal"
    assert list(wal.replay()) == []


def test_wal_replay_truncated_header(tmp_path: Path) -> None:
    wal_path = tmp_path / "trunc_header.wal"
    wal_path.write_bytes(b"PQW")  # only 3 bytes

    # Create dummy WriteAheadLog instance without calling _open on bad file
    wal = WriteAheadLog.__new__(WriteAheadLog)
    wal.path = wal_path

    # Non-strict: returns empty
    assert list(wal.replay(strict=False)) == []

    # Strict: raises WalCorruptError
    with pytest.raises(WalCorruptError, match="Truncated WAL header"):
        list(wal.replay(strict=True))


def test_wal_replay_invalid_magic(tmp_path: Path) -> None:
    wal_path = tmp_path / "bad_magic.wal"
    wal_path.write_bytes(b"BAD_MAGIC_HEADER")

    wal = WriteAheadLog.__new__(WriteAheadLog)
    wal.path = wal_path

    with pytest.raises(WalCorruptError, match="Invalid WAL header"):
        list(wal.replay(strict=False))


def test_wal_replay_torn_frame_header_strict(tmp_path: Path) -> None:
    wal_path = tmp_path / "torn_hdr.wal"
    wal = WriteAheadLog(wal_path)
    wal.append_task(_sample_task("t1"))
    wal.close()

    # Append 3 extra bytes (less than FRAME_HEADER_LEN=9)
    with open(wal_path, "ab") as f:
        f.write(b"123")

    with pytest.raises(WalCorruptError, match="Torn frame header"):
        list(wal.replay(strict=True))


@pytest.mark.asyncio
async def test_node_recover_from_checkpoint(tmp_path: Path) -> None:
    from peerq.clock import SimClock
    from peerq.node import PeerNode
    from peerq.security import SecurityConfig
    from peerq.transport import InMemoryTransport, SimNetwork

    clock = SimClock(0.0)
    net = SimNetwork(clock)
    t = InMemoryTransport("n1", net)
    import random

    rng = random.Random(42)

    wal_path = tmp_path / "checkpoint_node.wal"
    wal = WriteAheadLog(wal_path)

    # Populate WAL with checkpoint and task updates
    t1 = _sample_task("t1", state="pending")
    t2 = _sample_task("t2", state="done")
    vc = VectorClock({"n1": 2, "n2": 1})
    wal.checkpoint({"t1": t1, "t2": t2}, vc)

    # Append a newer update to t1
    t1_updated = TaskRecord(
        task_id="t1",
        state=TaskState.DONE,
        payload=b"new-payload",
        result=b"new-result",
        error=None,
        claimed_by="n1",
        fence_token=FenceToken(epoch=2, peer_id="n1"),
        lease_expiry=0.0,
        vector_clock=VectorClock({"n1": 5}),
        updated_by="n1",
    )
    wal.append_task(t1_updated)
    wal.close()

    wal_for_node = WriteAheadLog(wal_path)
    # This fixture intentionally exercises the pre-signature WAL format.
    # Secure nodes must reject these legacy records; the secure rejection path
    # is covered separately in the security regression tests.
    node = PeerNode(
        "n1",
        clock,
        t,
        rng,
        peers=[],
        wal=wal_for_node,
        security=SecurityConfig(enabled=False),
    )

    await node.start()
    task1 = node.get_task("t1")
    assert task1 is not None
    assert task1.result == b"new-result"
    task2 = node.get_task("t2")
    assert task2 is not None
    assert task2.state == TaskState.DONE
    await node.stop()
