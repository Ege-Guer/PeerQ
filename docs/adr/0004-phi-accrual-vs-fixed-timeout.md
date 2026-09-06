# ADR 0004: Adaptive φ-Accrual vs. Fixed-Timeout Failure Detection

## Status
Accepted

## Context
Failure detection in distributed networks must balance two contradictory goals:
1. **Speed (Detection Time)**: Quickly detecting dead nodes to reclaim orphaned tasks.
2. **Accuracy (Safety)**: Avoiding false accusations caused by transient network congestion, GC pauses, or temporary load spikes.

Traditional heartbeat systems use a fixed timeout (e.g. "declare dead if no heartbeat in 5 seconds").

## Decision
We implemented the **φ-accrual failure detector** based on Hayashibara et al. (2004):
1. Instead of binary up/down statuses, the detector outputs a continuous suspicion level $\phi = -\log_{10}(P(\text{later than now}))$.
2. It dynamically fits a normal distribution over a sliding window of observed heartbeat inter-arrival times ($\mu, \sigma$).
3. Real-world edge cases are handled explicitly:
   - **Cold start protection**: Suspicion remains 0.0 before $N_{min}$ intervals are recorded.
   - **Variance floor**: Enforces $\sigma \ge \sigma_{floor}$ to avoid hypersensitivity on low-jitter or localhost links.
   - **Unknown peers**: Explicitly returns $\infty$.

## Rejected Alternatives
- **Static Fixed Timeout (e.g. $T = 5.0\text{s}$)**:
  - Fragile across varying network environments. A 5-second timeout on a cross-region cloud link causes rampant false suspicions during transient packet delays, while being unnecessarily slow on a low-latency 10GbE local network.
- **Simple Ping-Pong Heartbeats**:
  - Point-to-point synchronous polling incurs excessive lock contention and fails to capture variance over time.

## Consequences
- **Positive**: Seamless adaptation to fluctuating network jitter without manual recalibration.
- **Positive**: Tunable trade-off between sensitivity and false alarm rate via a single threshold parameter ($\phi \ge 8.0$ vs $\phi \ge 12.0$).
- **Trade-off**: Requires maintaining a sliding window of historical float intervals per monitored peer.
