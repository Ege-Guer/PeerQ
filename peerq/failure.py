"""
φ-accrual failure detector based on Hayashibara et al. (2004).

Key implementation details:
- Maintains a sliding window of heartbeat inter-arrival intervals per peer.
- Computes mean (μ) and standard deviation (σ) to fit a normal distribution.
- Suspicion metric: φ = -log10(P(t_later >= t_now - t_last)).
- Real-world gotchas handled explicitly:
  1. Cold start: Before window reaches min_samples, returns φ = 0.0 to prevent
     premature false accusations before an arrival baseline is established.
  2. Variance floor (variance_floor): Prevents division by zero and extreme
     hyper-sensitivity on zero-jitter or localhost links where σ ≈ 0.
  3. Unknown / never-heard-from peers: Returns float('inf') indicating complete absence.
  4. Numerical stability: Clamps probability floor to avoid log10(0) MathDomainError.
- Time is strictly injected via the Clock protocol.
"""

from __future__ import annotations

import math
from collections import deque

from peerq.clock import Clock


class PhiAccrualDetector:
    """
    Adaptive φ-accrual failure detector.

    Instead of binary up/down states, outputs a continuous suspicion level φ.
    Higher φ indicates higher probability that the monitored peer has crashed.
    Typical threshold: φ >= 8.0 (error probability 10^-8) or φ >= 12.0 for aggressive.
    """

    def __init__(
        self,
        clock: Clock,
        window_size: int = 1000,
        min_samples: int = 5,
        variance_floor: float = 0.0001,
    ) -> None:
        self.clock = clock
        self.window_size = window_size
        self.min_samples = min_samples
        self.variance_floor = variance_floor

        # Per-peer heartbeat history:
        # peer_id -> deque of inter-arrival intervals
        self._intervals: dict[str, deque[float]] = {}
        # peer_id -> timestamp of most recent heartbeat
        self._last_heartbeat: dict[str, float] = {}

    def heartbeat(self, peer_id: str, timestamp: float | None = None) -> None:
        """
        Record the arrival of a heartbeat from peer_id.
        Uses clock.now() if timestamp is not explicitly provided.
        """
        now = timestamp if timestamp is not None else self.clock.now()

        if peer_id in self._last_heartbeat:
            delta = now - self._last_heartbeat[peer_id]
            if delta > 0:
                if peer_id not in self._intervals:
                    self._intervals[peer_id] = deque(maxlen=self.window_size)
                self._intervals[peer_id].append(delta)

        self._last_heartbeat[peer_id] = now

    def is_known(self, peer_id: str) -> bool:
        """Check if peer has ever been registered with a heartbeat."""
        return peer_id in self._last_heartbeat

    def phi(self, peer_id: str, timestamp: float | None = None) -> float:
        """
        Calculate current suspicion level φ for peer_id.
        Returns:
            float('inf') if peer was never heard from.
            0.0 if in cold start (fewer than min_samples intervals recorded).
            Continuous φ value >= 0.0 otherwise.
        """
        if peer_id not in self._last_heartbeat:
            # Gotcha 3: Peer has never been heard from -> maximum suspicion
            return float("inf")

        intervals = self._intervals.get(peer_id)
        if intervals is None or len(intervals) < self.min_samples:
            # Gotcha 1: Cold start before baseline inter-arrival window fills
            return 0.0

        now = timestamp if timestamp is not None else self.clock.now()
        time_since_last = now - self._last_heartbeat[peer_id]

        if time_since_last <= 0:
            return 0.0

        # Fit normal distribution to sliding window intervals
        n = len(intervals)
        mean = sum(intervals) / n
        variance = sum((x - mean) ** 2 for x in intervals) / n

        # Gotcha 2: Variance floor prevents hyper-sensitivity on low-jitter links
        std_dev = math.sqrt(max(variance, self.variance_floor))

        # Compute P(t >= time_since_last) using complementary error function
        # P_later = 0.5 * erfc((t - mean) / (std_dev * sqrt(2)))
        y = (time_since_last - mean) / (std_dev * math.sqrt(2.0))
        p_later = 0.5 * math.erfc(y)

        # Gotcha 4: Numerical stability floor to avoid log10(0)
        p_later = max(p_later, 1e-100)

        phi_val = -math.log10(p_later)
        return max(0.0, phi_val)

    def is_suspected(
        self,
        peer_id: str,
        threshold: float = 8.0,
        timestamp: float | None = None,
    ) -> bool:
        """Return True if peer suspicion level crosses the configured threshold."""
        return self.phi(peer_id, timestamp=timestamp) >= threshold
