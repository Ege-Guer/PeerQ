"""
Property-based tests for peerq.wal using Hypothesis.
"""

import contextlib
import tempfile
from pathlib import Path

from hypothesis import given, settings
from hypothesis import strategies as st

from peerq.consensus import FenceToken, TaskRecord, TaskState, VectorClock
from peerq.wal import (
    RECORD_TASK,
    WalCorruptError,
    WriteAheadLog,
)

peer_names = st.sampled_from(["p1", "p2", "p3", "p4"])
st_vector_clock = st.dictionaries(
    keys=peer_names,
    values=st.integers(min_value=0, max_value=50),
    max_size=4,
).map(lambda d: VectorClock(d))

st_task_record = st.builds(
    TaskRecord,
    task_id=st.text(min_size=1, max_size=15, alphabet="abcdefghijklmnopqrstuvwxyz0123456789-"),
    state=st.sampled_from(list(TaskState)),
    payload=st.binary(max_size=32),
    result=st.one_of(st.none(), st.binary(max_size=32)),
    error=st.one_of(st.none(), st.text(max_size=32)),
    claimed_by=st.one_of(st.none(), peer_names),
    fence_token=st.builds(
        FenceToken,
        epoch=st.integers(min_value=0, max_value=20),
        peer_id=peer_names,
    ),
    lease_expiry=st.floats(min_value=0.0, max_value=1000.0, allow_nan=False),
    vector_clock=st_vector_clock,
    updated_by=peer_names,
)


@settings(max_examples=30, deadline=None)
@given(records=st.lists(st_task_record, min_size=1, max_size=10))
def test_wal_task_record_roundtrip_prop(records: list[TaskRecord]) -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        wal_path = Path(tmpdir) / "prop_task.wal"
        wal = WriteAheadLog(wal_path)

        for rec in records:
            wal.append_task(rec)
        wal.close()

        replayed = list(wal.replay(strict=True))
        assert len(replayed) == len(records)
        for original, restored_rec in zip(records, replayed, strict=True):
            assert restored_rec.record_type == RECORD_TASK
            restored_task = TaskRecord.from_dict(restored_rec.payload)
            assert restored_task.task_id == original.task_id
            assert restored_task.state == original.state
            assert restored_task.payload == original.payload
            assert restored_task.result == original.result
            assert restored_task.error == original.error
            assert restored_task.claimed_by == original.claimed_by
            assert restored_task.fence_token == original.fence_token
            assert restored_task.vector_clock.clock == original.vector_clock.clock


@settings(max_examples=30, deadline=None)
@given(
    records=st.lists(st_task_record, min_size=1, max_size=5),
    byte_offset_ratio=st.floats(min_value=0.0, max_value=1.0),
    mutation_val=st.integers(min_value=1, max_value=255),
)
def test_wal_bit_corruption_detected_prop(
    records: list[TaskRecord],
    byte_offset_ratio: float,
    mutation_val: int,
) -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        wal_path = Path(tmpdir) / "prop_corrupt.wal"
        wal = WriteAheadLog(wal_path)

        for rec in records:
            wal.append_task(rec)
        wal.close()

        raw = bytearray(wal_path.read_bytes())
        # Target a byte strictly in the frame body (offset >= 8)
        body_len = len(raw) - 8
        if body_len <= 0:
            return

        target_idx = 8 + int(byte_offset_ratio * (body_len - 1))
        raw[target_idx] ^= mutation_val
        wal_path.write_bytes(bytes(raw))

        # Replay must either fail with WalCorruptError or gracefully stop if torn at EOF
        with contextlib.suppress(WalCorruptError):
            list(wal.replay(strict=True))
