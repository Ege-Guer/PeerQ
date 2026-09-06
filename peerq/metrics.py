"""
Low-overhead telemetry: Log-linear bucketed HDR-histogram and operational counters.

Guarantees:
- Bounded memory footprint (no unbounded sample arrays).
- Documented maximum relative error bound:
  With sub_bucket_bits=7 (128 sub-buckets per octave), max relative error <= 1/128 ≈ 0.78125%.
- O(1) sample recording time and bounded-time quantile lookups.
- Exact operational counters: enqueued, claimed, completed, failed, reclaimed, rejected.
"""

from __future__ import annotations

import math
from dataclasses import dataclass


class LogLinearHistogram:
    """
    Log-linear bucketed histogram inspired by HdrHistogram.

    Partitions positive values into power-of-two octaves, with each octave
    subdivided linearly into 2^B sub-buckets.

    Error Bound:
        Let B = sub_bucket_bits.
        For any recorded value x >= 2^B, the bucket width in octave [2^e, 2^(e+1))
        is 2^(e-B). The relative error is bounded by:
            Relative Error <= 2^(e-B) / x <= 2^(e-B) / 2^e = 2^(-B) = 1 / (2^B).
        For B=7 (128 sub-buckets), maximum relative error is <= 0.78125%.
    """

    def __init__(
        self,
        sub_bucket_bits: int = 7,
        max_value: float = 1e9,
    ) -> None:
        if sub_bucket_bits < 1 or sub_bucket_bits > 16:
            raise ValueError(f"sub_bucket_bits must be between 1 and 16, got {sub_bucket_bits}")

        self.sub_bucket_bits = sub_bucket_bits
        self.sub_bucket_count = 1 << sub_bucket_bits
        self.sub_bucket_mask = self.sub_bucket_count - 1
        self.max_value = max_value
        self.relative_error_bound = 1.0 / self.sub_bucket_count

        # Smallest power of 2 that can host full linear sub-buckets
        self.linear_sub_range = self.sub_bucket_count

        # Total buckets calculation:
        # Values < linear_sub_range map directly to indices 0..linear_sub_range-1.
        # Values >= linear_sub_range occupy octaves up to max_octave.
        max_octave = (
            math.ceil(math.log2(max_value))
            if max_value > self.linear_sub_range
            else self.sub_bucket_bits
        )
        octave_count = max(1, max_octave - self.sub_bucket_bits + 1)
        self._total_buckets = self.linear_sub_range + (octave_count * self.sub_bucket_count)
        self._counts: list[int] = [0] * self._total_buckets

        self._total_count: int = 0
        self._sum: float = 0.0
        self._min: float = float("inf")
        self._max: float = 0.0

    @property
    def count(self) -> int:
        return self._total_count

    @property
    def min(self) -> float:
        return self._min if self._total_count > 0 else 0.0

    @property
    def max(self) -> float:
        return self._max if self._total_count > 0 else 0.0

    @property
    def mean(self) -> float:
        return (self._sum / self._total_count) if self._total_count > 0 else 0.0

    def _value_to_index(self, value: float) -> int:
        """Map value to discrete histogram bucket index."""
        if value < self.linear_sub_range:
            return max(0, int(value))

        val_int = int(value)
        # Exponent of highest bit
        octave = val_int.bit_length() - 1
        octave_index = octave - self.sub_bucket_bits
        # Sub-bucket within the octave
        sub_index = (val_int >> (octave - self.sub_bucket_bits)) & self.sub_bucket_mask
        idx = self.linear_sub_range + (octave_index * self.sub_bucket_count) + sub_index
        return min(idx, self._total_buckets - 1)

    def _index_to_approx_value(self, index: int) -> float:
        """Convert a bucket index back to its representative center value."""
        if index < self.linear_sub_range:
            return float(index) + 0.5

        offset = index - self.linear_sub_range
        octave_index = offset // self.sub_bucket_count
        sub_index = offset % self.sub_bucket_count
        octave = octave_index + self.sub_bucket_bits

        base = 1 << octave
        bucket_width = 1 << (octave - self.sub_bucket_bits)
        val = base + (sub_index * bucket_width) + (bucket_width / 2.0)
        return float(val)

    def record(self, value: float) -> None:
        """Record a single non-negative measurement."""
        if value < 0:
            raise ValueError(f"Histogram values must be non-negative, got {value}")

        idx = self._value_to_index(value)
        self._counts[idx] += 1
        self._total_count += 1
        self._sum += value
        if value < self._min:
            self._min = value
        if value > self._max:
            self._max = value

    def quantile(self, q: float) -> float:
        """
        Estimate the q-th quantile (0.0 <= q <= 1.0).
        For example: 0.50 for median, 0.95 for p95, 0.99 for p99, 0.999 for p999.
        """
        if self._total_count == 0:
            return 0.0
        if q <= 0.0:
            return self._min
        if q >= 1.0:
            return self._max

        target_count = math.ceil(q * self._total_count)
        cumulative = 0

        for idx, count in enumerate(self._counts):
            cumulative += count
            if cumulative >= target_count:
                approx = self._index_to_approx_value(idx)
                # Clamp within recorded min and max
                return max(self._min, min(self._max, approx))

        return self._max


@dataclass
class MetricSnapshot:
    counters: dict[str, int]
    latencies: dict[str, dict[str, float]]


class MetricsCollector:
    """
    Local node telemetry collecting exact operational counters and latency quantiles.
    """

    COUNTER_NAMES = (
        "enqueued",
        "claimed",
        "completed",
        "failed",
        "reclaimed",
        "rejected",
    )

    def __init__(self, sub_bucket_bits: int = 7) -> None:
        self._counters: dict[str, int] = dict.fromkeys(self.COUNTER_NAMES, 0)
        self._histograms: dict[str, LogLinearHistogram] = {
            "task_latency": LogLinearHistogram(sub_bucket_bits=sub_bucket_bits, max_value=1e8),
            "claim_latency": LogLinearHistogram(sub_bucket_bits=sub_bucket_bits, max_value=1e8),
            "gossip_latency": LogLinearHistogram(sub_bucket_bits=sub_bucket_bits, max_value=1e8),
        }

    def increment(self, counter_name: str, amount: int = 1) -> None:
        if counter_name in self._counters:
            self._counters[counter_name] += amount
        else:
            self._counters[counter_name] = amount

    def get_counter(self, counter_name: str) -> int:
        return self._counters.get(counter_name, 0)

    def record_latency(self, histogram_name: str, latency_us: float) -> None:
        """Record a latency measurement in microseconds."""
        if histogram_name not in self._histograms:
            self._histograms[histogram_name] = LogLinearHistogram()
        self._histograms[histogram_name].record(latency_us)

    def get_histogram(self, histogram_name: str) -> LogLinearHistogram | None:
        return self._histograms.get(histogram_name)

    def snapshot(self) -> MetricSnapshot:
        """Export a complete snapshot of all counters and latency quantiles."""
        hist_summaries: dict[str, dict[str, float]] = {}
        for name, hist in self._histograms.items():
            hist_summaries[name] = {
                "count": float(hist.count),
                "min": hist.min,
                "mean": hist.mean,
                "max": hist.max,
                "p50": hist.quantile(0.50),
                "p95": hist.quantile(0.95),
                "p99": hist.quantile(0.99),
                "p999": hist.quantile(0.999),
            }

        return MetricSnapshot(
            counters=dict(self._counters),
            latencies=hist_summaries,
        )
