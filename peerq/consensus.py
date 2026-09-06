"""
Consensus module: Vector clocks, task ownership state, and CRDT join-semilattice.

Guarantees:
- VectorClock with increment, merge, and compare (BEFORE, AFTER, EQUAL, CONCURRENT).
- TaskRecord tracking lifecycle: PENDING -> CLAIMED -> RUNNING -> DONE / FAILED.
- Monotonically increasing FenceToken protecting against resurrected stale leases.
- Deterministic conflict resolution for CONCURRENT states forming a join-semilattice:
  merge is strictly commutative, associative, and idempotent.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from enum import Enum
from typing import Any


class ClockComparison(Enum):
    BEFORE = "BEFORE"
    AFTER = "AFTER"
    EQUAL = "EQUAL"
    CONCURRENT = "CONCURRENT"


@dataclass(frozen=True)
class VectorClock:
    """
    Vector clock for causality tracking in a leaderless peer mesh.

    Immutable: all operations return new VectorClock instances.
    Normalized: zero-valued peer counters are pruned so {} == {'p1': 0}.
    """

    clock: dict[str, int]

    def __init__(self, clock: dict[str, int] | None = None) -> None:
        filtered = {k: v for k, v in (clock or {}).items() if v > 0}
        object.__setattr__(self, "clock", filtered)

    def get(self, peer_id: str) -> int:
        return self.clock.get(peer_id, 0)

    def increment(self, peer_id: str) -> VectorClock:
        """Return a new VectorClock with peer_id's counter incremented by 1."""
        new_clock = dict(self.clock)
        new_clock[peer_id] = new_clock.get(peer_id, 0) + 1
        return VectorClock(new_clock)

    def merge(self, other: VectorClock) -> VectorClock:
        """
        Component-wise maximum of two vector clocks.
        Commutative, associative, and idempotent.
        """
        all_keys = set(self.clock.keys()) | set(other.clock.keys())
        merged = {k: max(self.get(k), other.get(k)) for k in all_keys}
        return VectorClock(merged)

    def compare(self, other: VectorClock) -> ClockComparison:
        """
        Compare this clock with another clock.
        Returns:
            BEFORE if self < other (self causally preceded other)
            AFTER if self > other (self causally succeeded other)
            EQUAL if self == other
            CONCURRENT if neither dominates (concurrent updates)
        """
        all_keys = set(self.clock.keys()) | set(other.clock.keys())
        self_leq_other = all(self.get(k) <= other.get(k) for k in all_keys)
        other_leq_self = all(other.get(k) <= self.get(k) for k in all_keys)

        if self_leq_other and other_leq_self:
            return ClockComparison.EQUAL
        if self_leq_other and not other_leq_self:
            return ClockComparison.BEFORE
        if other_leq_self and not self_leq_other:
            return ClockComparison.AFTER
        return ClockComparison.CONCURRENT

    def to_dict(self) -> dict[str, int]:
        """Return raw mapping of non-zero peer counters."""
        return dict(self.clock)


class TaskState(Enum):
    PENDING = "pending"
    CLAIMED = "claimed"
    RUNNING = "running"
    DONE = "done"
    FAILED = "failed"

    @property
    def is_terminal(self) -> bool:
        return self in (TaskState.DONE, TaskState.FAILED)

    @property
    def lattice_rank(self) -> int:
        if self == TaskState.PENDING:
            return 0
        if self == TaskState.CLAIMED:
            return 1
        if self == TaskState.RUNNING:
            return 2
        if self == TaskState.FAILED:
            return 3
        return 4  # DONE has highest lattice rank


@dataclass(frozen=True, order=True)
class FenceToken:
    """
    Monotonically ordered fencing token.

    Ordered by (epoch, peer_id). Guarantees strict total ordering across all peers
    without requiring a centralized sequencer.
    """

    epoch: int
    peer_id: str

    def next_for(self, peer_id: str) -> FenceToken:
        """Generate a successor token for a claiming peer."""
        return FenceToken(epoch=self.epoch + 1, peer_id=peer_id)


@dataclass(frozen=True)
class TaskRecord:
    """
    Replicated task state record.

    Carries vector clock for causal tracking and a FenceToken for lease enforcement.
    """

    task_id: str
    state: TaskState
    payload: bytes
    result: bytes | None = None
    error: str | None = None
    claimed_by: str | None = None
    fence_token: FenceToken = FenceToken(epoch=0, peer_id="")
    lease_expiry: float = 0.0
    vector_clock: VectorClock = VectorClock()
    updated_by: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "task_id": self.task_id,
            "state": self.state.value,
            "payload": self.payload.hex(),
            "result": self.result.hex() if self.result is not None else None,
            "error": self.error,
            "claimed_by": self.claimed_by,
            "fence_token": {"epoch": self.fence_token.epoch, "peer_id": self.fence_token.peer_id},
            "lease_expiry": self.lease_expiry,
            "vector_clock": self.vector_clock.clock,
            "updated_by": self.updated_by,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> TaskRecord:
        ft = data["fence_token"]
        return cls(
            task_id=data["task_id"],
            state=TaskState(data["state"]),
            payload=bytes.fromhex(data["payload"]),
            result=bytes.fromhex(data["result"]) if data["result"] is not None else None,
            error=data["error"],
            claimed_by=data["claimed_by"],
            fence_token=FenceToken(epoch=ft["epoch"], peer_id=ft["peer_id"]),
            lease_expiry=float(data["lease_expiry"]),
            vector_clock=VectorClock(data["vector_clock"]),
            updated_by=data["updated_by"],
        )


def _record_lattice_key(
    r: TaskRecord,
) -> tuple[
    int,
    FenceToken,
    int,
    str,
    tuple[int, str],
    tuple[int, bytes],
    tuple[int, str],
    bytes,
    float,
]:
    """
    Strict total ordering key for TaskRecords.

    Order precedence:
    1. Terminal precedence (DONE/FAILED cannot be demoted back to PENDING/CLAIMED/RUNNING)
    2. Higher FenceToken strictly dominates lower FenceToken
    3. State lattice rank (PENDING < CLAIMED < RUNNING < FAILED < DONE)
    4. Deterministic tiebreakers covering all non-clock attributes so distinct
       records produce distinct keys without collision.
    """
    terminal_flag = 1 if r.state.is_terminal else 0
    claimed_tuple = (0, "") if r.claimed_by is None else (1, r.claimed_by)
    result_tuple = (0, b"") if r.result is None else (1, r.result)
    error_tuple = (0, "") if r.error is None else (1, r.error)

    return (
        terminal_flag,
        r.fence_token,
        r.state.lattice_rank,
        r.updated_by,
        claimed_tuple,
        result_tuple,
        error_tuple,
        r.payload,
        r.lease_expiry,
    )


def merge_records(r1: TaskRecord, r2: TaskRecord) -> TaskRecord:
    """
    CRDT join-semilattice merge for replicated task records.

    Guarantees:
    - Commutative: merge(r1, r2) == merge(r2, r1)
    - Associative: merge(r1, merge(r2, r3)) == merge(merge(r1, r2), r3)
    - Idempotent: merge(r, r) == r
    """
    if r1.task_id != r2.task_id:
        raise ValueError(f"Cannot merge records for different tasks: {r1.task_id} vs {r2.task_id}")

    # Determine winning state record via strict lattice key
    k1 = _record_lattice_key(r1)
    k2 = _record_lattice_key(r2)

    winner = r1 if k1 >= k2 else r2

    # Vector clocks always merge via component-wise upper bound
    merged_vc = r1.vector_clock.merge(r2.vector_clock)

    return replace(winner, vector_clock=merged_vc)
