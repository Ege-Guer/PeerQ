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
from math import isfinite
from typing import TYPE_CHECKING, Any

from peerq.security import (
    MAX_ERROR_LENGTH,
    MAX_PEER_ID_LENGTH,
    MAX_TASK_ID_LENGTH,
    MAX_TASK_PAYLOAD,
    MAX_TASK_RESULT,
)

if TYPE_CHECKING:
    from peerq.crypto import PeerKeyRing


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
    signature: bytes | None = None
    signer_id: str | None = None

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
            "signature": self.signature.hex() if self.signature is not None else None,
            "signer_id": self.signer_id,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> TaskRecord:
        if not isinstance(data, dict):
            raise ValueError("task record must be a JSON object")
        task_id = data.get("task_id")
        if not isinstance(task_id, str) or not task_id or len(task_id) > MAX_TASK_ID_LENGTH:
            raise ValueError("task_id is empty or exceeds the configured limit")
        state_raw = data.get("state")
        if not isinstance(state_raw, str):
            raise ValueError("task state must be a string")
        try:
            state = TaskState(state_raw)
        except ValueError as exc:
            raise ValueError("unknown task state") from exc

        payload_hex = data.get("payload")
        if not isinstance(payload_hex, str) or len(payload_hex) > MAX_TASK_PAYLOAD * 2:
            raise ValueError("task payload exceeds the configured limit")
        try:
            payload = bytes.fromhex(payload_hex)
        except ValueError as exc:
            raise ValueError("task payload is not valid hexadecimal") from exc

        result_hex = data.get("result")
        if result_hex is not None and (
            not isinstance(result_hex, str) or len(result_hex) > MAX_TASK_RESULT * 2
        ):
            raise ValueError("task result exceeds the configured limit")
        try:
            result = bytes.fromhex(result_hex) if result_hex is not None else None
        except ValueError as exc:
            raise ValueError("task result is not valid hexadecimal") from exc

        error = data.get("error")
        if error is not None and (not isinstance(error, str) or len(error) > MAX_ERROR_LENGTH):
            raise ValueError("task error exceeds the configured limit")
        claimed_by = data.get("claimed_by")
        updated_by = data.get("updated_by")
        signer_id = data.get("signer_id")
        for label, value in (
            ("claimed_by", claimed_by),
            ("updated_by", updated_by),
            ("signer_id", signer_id),
        ):
            if value is not None and (
                not isinstance(value, str) or not value or len(value) > MAX_PEER_ID_LENGTH
            ):
                raise ValueError(f"{label} is invalid")

        ft = data.get("fence_token")
        if not isinstance(ft, dict):
            raise ValueError("fence_token must be an object")
        epoch = ft.get("epoch")
        fence_peer = ft.get("peer_id")
        if (
            not isinstance(epoch, int)
            or isinstance(epoch, bool)
            or epoch < 0
            or not isinstance(fence_peer, str)
            or len(fence_peer) > MAX_PEER_ID_LENGTH
        ):
            raise ValueError("fence_token is invalid")

        vector_clock = data.get("vector_clock")
        if not isinstance(vector_clock, dict) or len(vector_clock) > 128:
            raise ValueError("vector_clock is invalid or too large")
        for peer, counter in vector_clock.items():
            if (
                not isinstance(peer, str)
                or not peer
                or len(peer) > MAX_PEER_ID_LENGTH
                or not isinstance(counter, int)
                or isinstance(counter, bool)
                or counter < 0
            ):
                raise ValueError("vector_clock contains an invalid entry")

        lease_expiry = data.get("lease_expiry")
        if (
            not isinstance(lease_expiry, (int, float))
            or isinstance(lease_expiry, bool)
            or not isfinite(float(lease_expiry))
        ):
            raise ValueError("lease_expiry is invalid")
        sig_hex = data.get("signature")
        if sig_hex is not None and (not isinstance(sig_hex, str) or len(sig_hex) != 128):
            raise ValueError("signature is invalid")
        try:
            signature = bytes.fromhex(sig_hex) if sig_hex is not None else None
        except ValueError as exc:
            raise ValueError("signature is not valid hexadecimal") from exc
        return cls(
            task_id=task_id,
            state=state,
            payload=payload,
            result=result,
            error=error,
            claimed_by=claimed_by,
            fence_token=FenceToken(epoch=epoch, peer_id=fence_peer),
            lease_expiry=float(lease_expiry),
            vector_clock=VectorClock(vector_clock),
            updated_by=updated_by or "",
            signature=signature,
            signer_id=signer_id,
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
    tuple[int, bytes],
    tuple[int, str],
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
    sig_tuple = (0, b"") if r.signature is None else (1, r.signature)
    signer_tuple = (0, "") if r.signer_id is None else (1, r.signer_id)

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
        sig_tuple,
        signer_tuple,
    )


def merge_records(
    r1: TaskRecord,
    r2: TaskRecord,
    keyring: PeerKeyRing | None = None,
) -> TaskRecord:
    """
    CRDT join-semilattice merge for replicated task records.

    Guarantees:
    - Commutative: merge(r1, r2) == merge(r2, r1)
    - Associative: merge(r1, merge(r2, r3)) == merge(merge(r1, r2), r3)
    - Idempotent: merge(r, r) == r
    - Byzantine resistance: If keyring is supplied, untrusted or forged records
      are rejected in favor of verified authorized records.
    """
    if r1.task_id != r2.task_id:
        raise ValueError(f"Cannot merge records for different tasks: {r1.task_id} vs {r2.task_id}")

    if keyring is not None:
        from peerq.crypto import verify_task_authorization

        valid1 = verify_task_authorization(r1, keyring)
        valid2 = verify_task_authorization(r2, keyring)
        if valid1 and not valid2:
            return r1
        if valid2 and not valid1:
            return r2
        if not valid1 and not valid2:
            raise ValueError(
                f"Neither record is cryptographically authorized for task {r1.task_id}"
            )

    # Determine winning state record via strict lattice key
    k1 = _record_lattice_key(r1)
    k2 = _record_lattice_key(r2)

    winner = r1 if k1 >= k2 else r2

    # Vector clocks always merge via component-wise upper bound
    merged_vc = r1.vector_clock.merge(r2.vector_clock)

    return replace(winner, vector_clock=merged_vc)
