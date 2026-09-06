"""Property-based tests for peerq.queue using Hypothesis."""

from hypothesis import given
from hypothesis import strategies as st

from peerq.queue import BackpressurePriorityQueue


@given(
    items=st.lists(
        st.tuples(
            st.integers(min_value=-1000, max_value=1000),  # priority
            st.integers(min_value=0, max_value=100000),  # item id
        ),
        min_size=1,
        max_size=100,
    )
)
def test_priority_and_fifo_invariants(items: list[tuple[int, int]]) -> None:
    """
    Hypothesis property:
    1. Highest priority is always popped first.
    2. Within equal priority, items are popped in strict FIFO order of insertion.
    """
    q: BackpressurePriorityQueue[int] = BackpressurePriorityQueue()

    # Track insertion order per priority
    expected_fifo: dict[int, list[int]] = {}
    for pri, val in items:
        q.put_nowait(val, priority=pri)
        expected_fifo.setdefault(pri, []).append(val)

    dequeued: list[tuple[int, int]] = []
    while not q.empty():
        pri, val = q.get_nowait()
        dequeued.append((pri, val))

    assert len(dequeued) == len(items)

    # Invariant 1: Non-increasing priority
    for i in range(len(dequeued) - 1):
        assert dequeued[i][0] >= dequeued[i + 1][0], (
            f"Priority inversion: {dequeued[i][0]} popped before {dequeued[i + 1][0]}"
        )

    # Invariant 2: Strict FIFO within same priority
    dequeued_by_pri: dict[int, list[int]] = {}
    for pri, val in dequeued:
        dequeued_by_pri.setdefault(pri, []).append(val)

    for pri, expected_vals in expected_fifo.items():
        actual_vals = dequeued_by_pri[pri]
        assert actual_vals == expected_vals, (
            f"FIFO violation for priority {pri}: expected {expected_vals}, got {actual_vals}"
        )
