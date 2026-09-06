"""Property-based tests for peerq.consensus using Hypothesis."""

from hypothesis import given
from hypothesis import strategies as st

from peerq.consensus import (
    ClockComparison,
    FenceToken,
    TaskRecord,
    TaskState,
    VectorClock,
    merge_records,
)

# Strategy for generating arbitrary VectorClocks
peer_names = st.sampled_from(["p1", "p2", "p3", "p4", "p5"])
st_clock_dict = st.dictionaries(
    keys=peer_names,
    values=st.integers(min_value=0, max_value=50),
    max_size=5,
)
st_vector_clock = st_clock_dict.map(lambda d: VectorClock(d))


@given(vc1=st_vector_clock, vc2=st_vector_clock)
def test_vector_clock_commutativity(vc1: VectorClock, vc2: VectorClock) -> None:
    """vc1.merge(vc2) == vc2.merge(vc1)"""
    m1 = vc1.merge(vc2)
    m2 = vc2.merge(vc1)
    assert m1.clock == m2.clock


@given(vc1=st_vector_clock, vc2=st_vector_clock, vc3=st_vector_clock)
def test_vector_clock_associativity(vc1: VectorClock, vc2: VectorClock, vc3: VectorClock) -> None:
    """(vc1.merge(vc2)).merge(vc3) == vc1.merge(vc2.merge(vc3))"""
    m1 = (vc1.merge(vc2)).merge(vc3)
    m2 = vc1.merge(vc2.merge(vc3))
    assert m1.clock == m2.clock


@given(vc=st_vector_clock)
def test_vector_clock_idempotence(vc: VectorClock) -> None:
    """vc.merge(vc) == vc"""
    assert vc.merge(vc).clock == vc.clock


@given(vc1=st_vector_clock, vc2=st_vector_clock)
def test_vector_clock_upper_bound(vc1: VectorClock, vc2: VectorClock) -> None:
    """Merged clock is an upper bound of both inputs."""
    merged = vc1.merge(vc2)
    comp1 = vc1.compare(merged)
    comp2 = vc2.compare(merged)
    assert comp1 in (ClockComparison.BEFORE, ClockComparison.EQUAL)
    assert comp2 in (ClockComparison.BEFORE, ClockComparison.EQUAL)


@given(vc1=st_vector_clock, vc2=st_vector_clock)
def test_vector_clock_compare_consistency(vc1: VectorClock, vc2: VectorClock) -> None:
    """Comparison symmetry: vc1 < vc2 <=> vc2 > vc1; vc1 == vc2 <=> vc2 == vc1."""
    c1 = vc1.compare(vc2)
    c2 = vc2.compare(vc1)
    if c1 == ClockComparison.EQUAL:
        assert c2 == ClockComparison.EQUAL
    elif c1 == ClockComparison.BEFORE:
        assert c2 == ClockComparison.AFTER
    elif c1 == ClockComparison.AFTER:
        assert c2 == ClockComparison.BEFORE
    elif c1 == ClockComparison.CONCURRENT:
        assert c2 == ClockComparison.CONCURRENT


# Strategy for generating TaskRecords
st_task_record = st.builds(
    TaskRecord,
    task_id=st.just("task-fuzz"),
    state=st.sampled_from(list(TaskState)),
    payload=st.binary(max_size=10),
    result=st.one_of(st.none(), st.binary(max_size=10)),
    error=st.one_of(st.none(), st.text(max_size=10)),
    claimed_by=st.one_of(st.none(), peer_names),
    fence_token=st.builds(
        FenceToken,
        epoch=st.integers(min_value=0, max_value=10),
        peer_id=peer_names,
    ),
    lease_expiry=st.floats(min_value=0.0, max_value=1000.0),
    vector_clock=st_vector_clock,
    updated_by=peer_names,
)


@given(r1=st_task_record, r2=st_task_record)
def test_merge_records_commutativity(r1: TaskRecord, r2: TaskRecord) -> None:
    """merge_records(r1, r2) == merge_records(r2, r1)"""
    m1 = merge_records(r1, r2)
    m2 = merge_records(r2, r1)
    assert m1.state == m2.state
    assert m1.fence_token == m2.fence_token
    assert m1.claimed_by == m2.claimed_by
    assert m1.result == m2.result
    assert m1.error == m2.error
    assert m1.vector_clock.clock == m2.vector_clock.clock


@given(r1=st_task_record, r2=st_task_record, r3=st_task_record)
def test_merge_records_associativity(r1: TaskRecord, r2: TaskRecord, r3: TaskRecord) -> None:
    """merge(r1, merge(r2, r3)) == merge(merge(r1, r2), r3)"""
    m1 = merge_records(r1, merge_records(r2, r3))
    m2 = merge_records(merge_records(r1, r2), r3)
    assert m1.state == m2.state
    assert m1.fence_token == m2.fence_token
    assert m1.claimed_by == m2.claimed_by
    assert m1.result == m2.result
    assert m1.error == m2.error
    assert m1.vector_clock.clock == m2.vector_clock.clock


@given(r=st_task_record)
def test_merge_records_idempotency(r: TaskRecord) -> None:
    """merge_records(r, r) == r"""
    merged = merge_records(r, r)
    assert merged.state == r.state
    assert merged.fence_token == r.fence_token
    assert merged.claimed_by == r.claimed_by
    assert merged.result == r.result
    assert merged.error == r.error
    assert merged.vector_clock.clock == r.vector_clock.clock
