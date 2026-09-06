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
from peerq.crypto import (
    Ed25519KeyPair,
    Ed25519PublicKeyWrapper,
    PeerKeyRing,
    sign_task,
    verify_task_authorization,
    verify_task_signature,
)
from peerq.discovery import DiscoveredPeer, PeerDiscovery
from peerq.exporter import StatusServer, format_cluster_status, format_prometheus_text
from peerq.failure import PhiAccrualDetector
from peerq.metrics import LogLinearHistogram, MetricsCollector
from peerq.node import PeerNode
from peerq.queue import BackpressurePriorityQueue, CreditFlowController, QueueFull
from peerq.transport import (
    HttpServer,
    InMemoryTransport,
    Message,
    TcpTransport,
    Transport,
    UdpBroadcastTransport,
)
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
    "UdpBroadcastTransport",
    "WriteAheadLog",
    "WalRecord",
    "WalError",
    "WalCorruptError",
    "RECORD_TASK",
    "RECORD_CLOCK",
    "RECORD_CHECKPOINT",
    "PeerDiscovery",
    "DiscoveredPeer",
    "Ed25519KeyPair",
    "Ed25519PublicKeyWrapper",
    "PeerKeyRing",
    "sign_task",
    "verify_task_signature",
    "verify_task_authorization",
    "HttpServer",
    "StatusServer",
    "format_cluster_status",
]
