"""Unit tests for peerq.consensus."""

import pytest

from peerq.consensus import (
    ClockComparison,
    FenceToken,
    TaskRecord,
    TaskState,
    VectorClock,
    merge_records,
)


def test_vector_clock_compare() -> None:
    vc0 = VectorClock()
    vc1 = vc0.increment("a")
    vc2 = vc1.increment("b")
    vc3 = vc1.increment("c")

    # vc0 < vc1 < vc2
    assert vc0.compare(vc1) == ClockComparison.BEFORE
    assert vc1.compare(vc0) == ClockComparison.AFTER
    assert vc1.compare(vc1) == ClockComparison.EQUAL
    assert vc1.compare(vc2) == ClockComparison.BEFORE
    assert vc2.compare(vc1) == ClockComparison.AFTER

    # vc2 and vc3 branched from vc1 -> CONCURRENT
    assert vc2.compare(vc3) == ClockComparison.CONCURRENT
    assert vc3.compare(vc2) == ClockComparison.CONCURRENT

    # Merging resolves concurrency
    vc_merged = vc2.merge(vc3)
    assert vc2.compare(vc_merged) == ClockComparison.BEFORE
    assert vc3.compare(vc_merged) == ClockComparison.BEFORE


def test_fence_token_ordering() -> None:
    t1 = FenceToken(epoch=1, peer_id="node-a")
    t2 = FenceToken(epoch=1, peer_id="node-b")
    t3 = FenceToken(epoch=2, peer_id="node-a")

    assert t1 < t2  # Same epoch, node-a < node-b
    assert t2 < t3  # epoch 1 < epoch 2
    assert t1 < t3

    successor = t1.next_for("node-c")
    assert successor == FenceToken(epoch=2, peer_id="node-c")
    assert successor > t1


def test_task_record_dict_roundtrip() -> None:
    rec = TaskRecord(
        task_id="task-42",
        state=TaskState.RUNNING,
        payload=b"input-bytes",
        result=None,
        error=None,
        claimed_by="node-1",
        fence_token=FenceToken(epoch=3, peer_id="node-1"),
        lease_expiry=123.456,
        vector_clock=VectorClock({"node-1": 3, "node-2": 1}),
        updated_by="node-1",
    )
    data = rec.to_dict()
    restored = TaskRecord.from_dict(data)

    assert restored.task_id == rec.task_id
    assert restored.state == rec.state
    assert restored.payload == rec.payload
    assert restored.result == rec.result
    assert restored.claimed_by == rec.claimed_by
    assert restored.fence_token == rec.fence_token
    assert restored.lease_expiry == rec.lease_expiry
    assert restored.vector_clock.clock == rec.vector_clock.clock
    assert restored.updated_by == rec.updated_by


def test_merge_records_fencing_token_dominance() -> None:
    # Record from older lease holder that resurrects and tries to commit
    stale_rec = TaskRecord(
        task_id="task-1",
        state=TaskState.RUNNING,
        payload=b"payload",
        fence_token=FenceToken(epoch=1, peer_id="node-1"),
        vector_clock=VectorClock({"node-1": 10}),
        updated_by="node-1",
    )

    # Newer record from peer that legitimately reclaimed the lease
    fresh_rec = TaskRecord(
        task_id="task-1",
        state=TaskState.CLAIMED,
        payload=b"payload",
        fence_token=FenceToken(epoch=2, peer_id="node-2"),
        vector_clock=VectorClock({"node-2": 1}),
        updated_by="node-2",
    )

    merged = merge_records(stale_rec, fresh_rec)
    # Higher fencing token must dominate
    assert merged.fence_token == FenceToken(epoch=2, peer_id="node-2")
    assert merged.state == TaskState.CLAIMED
    assert merged.vector_clock.get("node-1") == 10
    assert merged.vector_clock.get("node-2") == 1


def test_merge_records_terminal_precedence() -> None:
    running_rec = TaskRecord(
        task_id="task-2",
        state=TaskState.RUNNING,
        payload=b"payload",
        fence_token=FenceToken(epoch=1, peer_id="node-1"),
        updated_by="node-1",
    )

    done_rec = TaskRecord(
        task_id="task-2",
        state=TaskState.DONE,
        payload=b"payload",
        result=b"output",
        fence_token=FenceToken(epoch=1, peer_id="node-1"),
        updated_by="node-1",
    )

    merged = merge_records(running_rec, done_rec)
    assert merged.state == TaskState.DONE
    assert merged.result == b"output"


def test_merge_different_tasks_raises() -> None:
    r1 = TaskRecord(task_id="t1", state=TaskState.PENDING, payload=b"")
    r2 = TaskRecord(task_id="t2", state=TaskState.PENDING, payload=b"")
    with pytest.raises(ValueError, match="Cannot merge records for different tasks"):
        merge_records(r1, r2)
