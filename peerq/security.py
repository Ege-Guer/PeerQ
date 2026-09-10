"""Shared security limits and bounded state for the PeerQ wire protocols.

The defaults in this module are deliberately conservative.  They are part of
the protocol contract: callers may raise or lower them explicitly, but a
normal node never runs without authentication, freshness checks, or resource
bounds.
"""

from __future__ import annotations

import json
from collections import OrderedDict, deque
from dataclasses import dataclass
from math import isfinite
from typing import Any


class SecurityError(Exception):
    """Base exception for rejected or unsafe protocol input."""


class MessageDecodeError(SecurityError):
    """Raised when a wire message is malformed or exceeds a configured limit."""


PROTOCOL_VERSION = 2
MAX_FRAME_SIZE = 256 * 1024
MAX_DISCOVERY_DATAGRAM = 4 * 1024
MAX_TASK_PAYLOAD = 64 * 1024
MAX_TASK_RESULT = 64 * 1024
MAX_TASK_ID_LENGTH = 128
MAX_PEER_ID_LENGTH = 128
MAX_MESSAGE_TYPE_LENGTH = 32
MAX_ERROR_LENGTH = 4 * 1024


@dataclass(frozen=True)
class SecurityConfig:
    """Configurable security policy with secure defaults.

    ``enabled=False`` is an explicit development/test escape hatch.  It is
    never selected implicitly by the CLI or by any production transport.
    """

    enabled: bool = True
    protocol_version: int = PROTOCOL_VERSION
    max_frame_size: int = MAX_FRAME_SIZE
    max_udp_payload: int = MAX_DISCOVERY_DATAGRAM
    max_http_headers: int = 8 * 1024
    max_http_body: int = 64 * 1024
    max_tasks: int = 10_000
    max_peers: int = 128
    max_gossip_tasks: int = 128
    max_credit_amount: int = 100_000
    max_lease_duration_seconds: float = 3_600.0
    replay_cache_size: int = 4096
    replay_window_seconds: float = 30.0
    max_connections: int = 64
    max_inbox_size: int = 1024
    read_timeout_seconds: float = 10.0
    write_timeout_seconds: float = 10.0
    connect_timeout_seconds: float = 10.0
    udp_rate_limit: int = 100
    udp_rate_window_seconds: float = 1.0
    max_json_depth: int = 16
    max_json_items: int = 10_000
    max_string_length: int = 256 * 1024

    def __post_init__(self) -> None:
        positive_ints = (
            "protocol_version",
            "max_frame_size",
            "max_udp_payload",
            "max_http_headers",
            "max_http_body",
            "max_tasks",
            "max_peers",
            "max_gossip_tasks",
            "max_credit_amount",
            "replay_cache_size",
            "max_connections",
            "max_inbox_size",
            "udp_rate_limit",
            "max_json_depth",
            "max_json_items",
            "max_string_length",
        )
        for field_name in positive_ints:
            if getattr(self, field_name) <= 0:
                raise ValueError(f"{field_name} must be positive")

        positive_floats = (
            "replay_window_seconds",
            "read_timeout_seconds",
            "write_timeout_seconds",
            "connect_timeout_seconds",
            "udp_rate_window_seconds",
            "max_lease_duration_seconds",
        )
        for field_name in positive_floats:
            value = float(getattr(self, field_name))
            if not isfinite(value) or value <= 0.0:
                raise ValueError(f"{field_name} must be finite and positive")


def canonical_json_bytes(value: Any) -> bytes:
    """Serialize a protocol value deterministically and reject non-finite numbers."""

    try:
        return json.dumps(
            value,
            separators=(",", ":"),
            sort_keys=True,
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise MessageDecodeError(f"value is not safely JSON serializable: {exc}") from exc


def reject_json_constants(value: str) -> None:
    """Reject NaN/Infinity accepted by Python's permissive JSON decoder."""

    raise ValueError(f"non-finite JSON constant is not permitted: {value}")


def validate_json_tree(
    value: Any,
    *,
    max_depth: int,
    max_items: int,
    max_string_length: int,
) -> None:
    """Validate parsed JSON without allowing unbounded nested structures."""

    stack: list[tuple[Any, int]] = [(value, 0)]
    item_count = 0
    while stack:
        current, depth = stack.pop()
        if depth > max_depth:
            raise MessageDecodeError("JSON nesting exceeds the configured limit")

        if isinstance(current, str):
            if len(current.encode("utf-8")) > max_string_length:
                raise MessageDecodeError("JSON string exceeds the configured limit")
        elif isinstance(current, (int, float)):
            if isinstance(current, float) and not isfinite(current):
                raise MessageDecodeError("non-finite JSON number is not permitted")
        elif isinstance(current, dict):
            item_count += len(current)
            if item_count > max_items:
                raise MessageDecodeError("JSON object contains too many items")
            stack.extend((key, depth + 1) for key in current)
            stack.extend((child, depth + 1) for child in current.values())
        elif isinstance(current, list):
            item_count += len(current)
            if item_count > max_items:
                raise MessageDecodeError("JSON array contains too many items")
            stack.extend((child, depth + 1) for child in current)
        elif current is not None and not isinstance(current, bool):
            raise MessageDecodeError(f"unsupported JSON value type: {type(current).__name__}")


class ReplayCache:
    """Bounded, deterministic nonce cache used by messages and discovery."""

    def __init__(self, max_entries: int = 4096) -> None:
        if max_entries <= 0:
            raise ValueError("max_entries must be positive")
        self.max_entries = max_entries
        self._entries: OrderedDict[tuple[str, str], float] = OrderedDict()

    def check_and_remember(self, sender_id: str, nonce: str, now: float, window: float) -> bool:
        """Return False for a replay and remember a fresh nonce otherwise."""

        cutoff = now - window
        while self._entries:
            _, timestamp = next(iter(self._entries.items()))
            if timestamp >= cutoff:
                break
            self._entries.popitem(last=False)

        key = (sender_id, nonce)
        if key in self._entries:
            return False

        self._entries[key] = now
        while len(self._entries) > self.max_entries:
            self._entries.popitem(last=False)
        return True

    def __len__(self) -> int:
        return len(self._entries)


class RateLimiter:
    """Small bounded fixed-window limiter for untrusted datagram sources."""

    def __init__(self, max_events: int, window_seconds: float, max_sources: int = 1024) -> None:
        if max_events <= 0 or window_seconds <= 0 or max_sources <= 0:
            raise ValueError("rate limiter values must be positive")
        self.max_events = max_events
        self.window_seconds = window_seconds
        self.max_sources = max_sources
        self._events: OrderedDict[str, deque[float]] = OrderedDict()

    def allow(self, source: str, now: float) -> bool:
        events = self._events.get(source)
        if events is None:
            if len(self._events) >= self.max_sources:
                self._events.popitem(last=False)
            events = deque()
            self._events[source] = events
        else:
            self._events.move_to_end(source)

        cutoff = now - self.window_seconds
        while events and events[0] < cutoff:
            events.popleft()
        if len(events) >= self.max_events:
            return False
        events.append(now)
        return True
