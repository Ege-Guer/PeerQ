"""Unit tests for peerq.failure (φ-accrual failure detector)."""

from peerq.clock import SimClock
from peerq.consensus import FenceToken, TaskRecord, TaskState, merge_records
from peerq.failure import PhiAccrualDetector


def test_unknown_peer_returns_infinity() -> None:
    clock = SimClock(100.0)
    detector = PhiAccrualDetector(clock)
    assert detector.phi("nonexistent-node") == float("inf")
    assert detector.is_suspected("nonexistent-node", threshold=8.0)


def test_cold_start_returns_zero() -> None:
    clock = SimClock(0.0)
    detector = PhiAccrualDetector(clock, min_samples=5)

    # Send 3 heartbeats (fewer than min_samples=5 intervals)
    for _ in range(4):
        detector.heartbeat("node-1")
        clock.advance(1.0)

    # 4 heartbeats generate 3 intervals < min_samples
    assert detector.phi("node-1") == 0.0
    assert not detector.is_suspected("node-1")


def test_steady_heartbeats_and_accrual() -> None:
    clock = SimClock(0.0)
    detector = PhiAccrualDetector(clock, min_samples=5, variance_floor=0.01)

    # Establish steady 1.0 second heartbeat baseline
    for _ in range(20):
        detector.heartbeat("node-1")
        clock.advance(1.0)

    # Exactly at expected interval, phi should be low (~0.3)
    phi_expected = detector.phi("node-1")
    assert phi_expected < 1.0
    assert not detector.is_suspected("node-1")

    # If 2.5 seconds pass without heartbeat, suspicion climbs
    clock.advance(1.5)
    phi_delayed = detector.phi("node-1")
    assert phi_delayed > phi_expected

    # If 8 seconds pass without heartbeat, suspicion crosses threshold
    clock.advance(5.5)
    assert detector.is_suspected("node-1", threshold=8.0)


def test_variance_floor_on_zero_jitter() -> None:
    clock = SimClock(0.0)
    detector = PhiAccrualDetector(clock, min_samples=5, variance_floor=0.001)

    # Perfectly zero jitter: exactly 1.0s every time
    for _ in range(10):
        detector.heartbeat("node-perfect")
        clock.advance(1.0)

    # Must not raise ZeroDivisionError and must give stable phi
    phi_val = detector.phi("node-perfect")
    assert phi_val != float("inf")
    assert phi_val < 2.0


def test_safe_reclaim_under_false_suspicion() -> None:
    """
    Simulation test: Peer A is falsely suspected due to network delay,
    its task lease expires, Peer B reclaims the task with a newer fencing token,
    and Peer A later returns and attempts to commit with its stale lease.

    Invariant: Peer A's stale commit MUST be rejected by the fencing token.
    """
    clock = SimClock(0.0)
    detector = PhiAccrualDetector(clock, min_samples=5)

    for _ in range(10):
        detector.heartbeat("node-a")
        clock.advance(1.0)

    # Node A claims task with lease until t=20.0, FenceToken(1, 'node-a')
    task_id = "task-reclaim-test"
    token_a = FenceToken(epoch=1, peer_id="node-a")

    # Node A runs the task
    record_a_running = TaskRecord(
        task_id=task_id,
        state=TaskState.RUNNING,
        payload=b"work",
        claimed_by="node-a",
        fence_token=token_a,
        lease_expiry=20.0,
        updated_by="node-a",
    )

    # Node A experiences temporary hiccup/partition. Time advances past lease expiry to t=25.0
    clock.advance(15.0)  # clock now at 25.0
    assert detector.is_suspected("node-a", threshold=8.0)
    assert clock.now() > record_a_running.lease_expiry

    # Node B detects suspicion + expired lease, reclaims with successor token
    token_b = token_a.next_for("node-b")
    assert token_b == FenceToken(epoch=2, peer_id="node-b")
    assert token_b > token_a

    # Node B finishes task and produces result
    record_b_done = TaskRecord(
        task_id=task_id,
        state=TaskState.DONE,
        payload=b"work",
        result=b"valid-output-from-node-b",
        claimed_by="node-b",
        fence_token=token_b,
        updated_by="node-b",
    )

    # Node A returns ("resurrected") and attempts to commit stale result
    record_a_stale_done = TaskRecord(
        task_id=task_id,
        state=TaskState.DONE,
        payload=b"work",
        result=b"stale-output-from-node-a",
        claimed_by="node-a",
        fence_token=token_a,
        updated_by="node-a",
    )

    # Merge: Node B's higher fencing token MUST beat Node A's stale commit
    final_merged = merge_records(record_a_stale_done, record_b_done)
    assert final_merged.fence_token == token_b
    assert final_merged.claimed_by == "node-b"
    assert final_merged.result == b"valid-output-from-node-b"
