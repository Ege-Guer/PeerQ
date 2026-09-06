"""
peerq: A leaderless, peer-to-peer async task mesh in Python.

Guarantees:
- Leaderless architecture: no broker, coordinator, or external dependencies.
- At-least-once task delivery with idempotent handlers.
- Monotonically fenced leases preventing stale execution commits.
- Deterministic conflict resolution with vector clocks and CRDT lattice.
- Adaptive failure detection via φ-accrual.
"""

from __future__ import annotations

__version__ = "0.1.0"
