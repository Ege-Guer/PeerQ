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

from peerq.clock import Clock, RealClock, SimClock
from peerq.consensus import (
    ClockComparison,
    FenceToken,
    TaskRecord,
    TaskState,
    VectorClock,
    merge_records,
)
from peerq.exporter import format_prometheus_text
from peerq.failure import PhiAccrualDetector
from peerq.metrics import LogLinearHistogram, MetricsCollector
from peerq.node import PeerNode
from peerq.queue import BackpressurePriorityQueue, CreditFlowController, QueueFull
from peerq.transport import InMemoryTransport, Message, TcpTransport, Transport
from peerq.wal import (
    RECORD_CHECKPOINT,
    RECORD_CLOCK,
    RECORD_TASK,
    WalCorruptError,
    WalError,
    WalRecord,
    WriteAheadLog,
)

__version__ = "0.2.0"

__all__ = [
    "Clock",
    "RealClock",
    "SimClock",
    "ClockComparison",
    "FenceToken",
    "TaskRecord",
    "TaskState",
    "VectorClock",
    "merge_records",
    "format_prometheus_text",
    "PhiAccrualDetector",
    "LogLinearHistogram",
    "MetricsCollector",
    "PeerNode",
    "BackpressurePriorityQueue",
    "CreditFlowController",
    "QueueFull",
    "InMemoryTransport",
    "Message",
    "TcpTransport",
    "Transport",
    "WriteAheadLog",
    "WalRecord",
    "WalError",
    "WalCorruptError",
    "RECORD_TASK",
    "RECORD_CLOCK",
    "RECORD_CHECKPOINT",
]
