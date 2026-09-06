# Blocked Issues Log

## Active Blockers
None.

## Resolved Issues During Implementation
1. **Implicit Zero Equivalence in VectorClock**:
   - *Symptom*: Hypothesis property test `test_vector_clock_upper_bound` found that `VectorClock({'p1': 0})` compared to `VectorClock({})` returned `CONCURRENT` instead of `EQUAL`.
   - *Root Cause*: Dictionary equality `{'p1': 0} != {}` failed to recognize that missing keys implicitly equal zero.
   - *Fix*: Normalized `VectorClock.__init__` to prune zero-valued peer entries.
2. **Lattice Key Collisions on Optional Fields**:
   - *Symptom*: Hypothesis test `test_merge_records_commutativity` failed when one record had `result=None` and another had `result=b""`.
   - *Root Cause*: `r.result or b""` mapped both `None` and `b""` to `b""`, producing key collision where `k1 == k2`.
   - *Fix*: Replaced fallback with explicit tagged tuples `(0, b"")` vs `(1, r.result)` across all fields.
3. **Task Duration vs. Lease Duration in Mid-Task Crash Scenarios**:
   - *Symptom*: Mid-task worker crash test failed when worker lease duration expired before reclaim execution completed.
   - *Root Cause*: Reclaim execution took longer than `lease_duration`.
   - *Fix*: Tuned handler simulation sleep to complete within the 3.0s lease window.
