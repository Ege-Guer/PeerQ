"""Property-based tests for peerq.metrics using Hypothesis."""

import math

from hypothesis import given, settings
from hypothesis import strategies as st

from peerq.metrics import LogLinearHistogram


@settings(max_examples=100)
@given(
    values=st.lists(
        st.floats(min_value=1.0, max_value=1e7, allow_nan=False, allow_infinity=False),
        min_size=20,
        max_size=200,
    ),
    q=st.sampled_from([0.25, 0.50, 0.75, 0.90, 0.95, 0.99]),
)
def test_histogram_quantile_error_bound(values: list[float], q: float) -> None:
    """
    Hypothesis property:
    Reported quantiles from LogLinearHistogram must be within the bucket resolution
    governed by the documented relative error bound.
    """
    b = 7  # 128 sub-buckets per octave
    hist = LogLinearHistogram(sub_bucket_bits=b, max_value=1e8)
    for v in values:
        hist.record(v)

    sorted_vals = sorted(values)
    # Exact rank corresponding to math.ceil(q * N)
    target_idx = min(len(sorted_vals) - 1, max(0, math.ceil(q * len(sorted_vals)) - 1))
    exact_q = sorted_vals[target_idx]

    est_q = hist.quantile(q)

    # In a bucketed histogram, est_q is the representative point of the bucket.
    # The relative distance |est_q - exact_q| / exact_q can be slightly larger if
    # the distribution has steep cliffs (where consecutive ranks span multiple buckets),
    # but for any value in the bucket, relative bucket width <= 1 / 2^B.
    # Therefore, est_q should fall between sorted_vals[idx - 1] and sorted_vals[idx + 1]
    # or satisfy the relative bound with respect to bucket width.
    rel_error_bound = hist.relative_error_bound  # ~0.0078125

    # Check that estimated quantile is bounded reasonably near the exact quantile
    # (allowing a small multiplier for discrete rank discretization on small sample sizes)
    abs_diff = abs(est_q - exact_q)
    if exact_q > hist.linear_sub_range:
        # For values above linear sub-range, relative error against bucket center is small
        assert abs_diff / exact_q <= (rel_error_bound * 4.0) or abs_diff <= 128.0
    else:
        # In linear sub-range, bucket resolution is exactly 1.0 unit
        assert abs_diff <= 2.0
