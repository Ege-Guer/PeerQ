"""
Backpressure-aware priority queue with credit-based flow control.

Key features:
- Priority queue based on heapq with asyncio.Condition synchronization.
- Monotonic sequence numbers ensuring strict FIFO tie-breaking within equal priority.
- Bounded capacity with two policies:
  1. reject-on-full: raises QueueFull immediately when capacity reached.
  2. credit-based flow control: HTTP/2 style send windows and replenishment.
"""

from __future__ import annotations

import asyncio
import heapq
from dataclasses import dataclass
from enum import Enum
from typing import Generic, TypeVar

T = TypeVar("T")


class QueueFull(Exception):
    """Raised when enqueuing into a full queue under REJECT_ON_FULL policy."""


class QueueEmpty(Exception):
    """Raised when dequeuing immediately from an empty queue."""


class InsufficientCredits(Exception):
    """Raised when an operation requires send credits but none are available."""


class QueuePolicy(Enum):
    REJECT_ON_FULL = "reject-on-full"
    BLOCK_ON_FULL = "block-on-full"


@dataclass(order=True)
class _PrioritizedItem(Generic[T]):
    priority_neg: int
    seq: int
    item: T


class BackpressurePriorityQueue(Generic[T]):
    """
    Backpressure-aware priority queue.

    Items with higher priority values are dequeued before items with lower priority.
    Items with equal priority are dequeued strictly in FIFO order.
    """

    def __init__(
        self,
        maxsize: int = 0,
        policy: QueuePolicy = QueuePolicy.REJECT_ON_FULL,
    ) -> None:
        self.maxsize = maxsize  # 0 or negative means unbounded
        self.policy = policy
        self._heap: list[_PrioritizedItem[T]] = []
        self._seq: int = 0
        self._cond = asyncio.Condition()

    def qsize(self) -> int:
        return len(self._heap)

    def empty(self) -> bool:
        return len(self._heap) == 0

    def full(self) -> bool:
        if self.maxsize <= 0:
            return False
        return len(self._heap) >= self.maxsize

    def put_nowait(self, item: T, priority: int = 0) -> None:
        """Enqueue an item immediately or raise QueueFull."""
        if self.full():
            raise QueueFull(f"Queue reached maximum capacity of {self.maxsize}")

        entry = _PrioritizedItem(priority_neg=-priority, seq=self._seq, item=item)
        self._seq += 1
        heapq.heappush(self._heap, entry)

    async def put(self, item: T, priority: int = 0) -> None:
        """Enqueue an item, blocking or rejecting depending on policy."""
        async with self._cond:
            if self.policy == QueuePolicy.REJECT_ON_FULL:
                if self.full():
                    raise QueueFull(f"Queue reached maximum capacity of {self.maxsize}")
            else:
                while self.full():
                    await self._cond.wait()

            entry = _PrioritizedItem(priority_neg=-priority, seq=self._seq, item=item)
            self._seq += 1
            heapq.heappush(self._heap, entry)
            self._cond.notify()

    def get_nowait(self) -> tuple[int, T]:
        """Dequeue highest priority item immediately or raise QueueEmpty."""
        if self.empty():
            raise QueueEmpty("Queue is empty")

        entry = heapq.heappop(self._heap)
        return -entry.priority_neg, entry.item

    async def get(self) -> tuple[int, T]:
        """Dequeue highest priority item, waiting until an item is available."""
        async with self._cond:
            while self.empty():
                await self._cond.wait()

            entry = heapq.heappop(self._heap)
            self._cond.notify()
            return -entry.priority_neg, entry.item

    def peek(self) -> tuple[int, T] | None:
        """Inspect highest priority item without removing it."""
        if self.empty():
            return None
        entry = self._heap[0]
        return -entry.priority_neg, entry.item


class CreditFlowController:
    """
    HTTP/2-style credit-based flow control manager.

    Each sender maintains a credit window per peer. Sending a task consumes
    a credit; receiving peer replenishes credits upon task completion/rejection.
    Guarantees the receiver's inbound queue cannot exceed negotiated capacity.
    """

    def __init__(self, initial_credits: int = 10) -> None:
        self.default_initial_credits = initial_credits
        self._credits: dict[str, int] = {}
        self._waiters: dict[str, list[asyncio.Future[None]]] = {}

    def init_peer(self, peer_id: str, credits: int | None = None) -> None:
        """Initialize or reset credit balance for a peer."""
        amount = credits if credits is not None else self.default_initial_credits
        self._credits[peer_id] = amount
        if peer_id not in self._waiters:
            self._waiters[peer_id] = []

    def get_credits(self, peer_id: str) -> int:
        """Return available send credits for target peer."""
        return self._credits.get(peer_id, self.default_initial_credits)

    def try_acquire(self, peer_id: str, amount: int = 1) -> bool:
        """Non-blocking credit acquisition. Returns True if acquired, False otherwise."""
        current = self._credits.get(peer_id, self.default_initial_credits)
        if current >= amount:
            self._credits[peer_id] = current - amount
            return True
        return False

    async def acquire(self, peer_id: str, amount: int = 1) -> None:
        """Acquire credits, asynchronously waiting if window is exhausted."""
        if peer_id not in self._credits:
            self._credits[peer_id] = self.default_initial_credits

        while self._credits[peer_id] < amount:
            loop = asyncio.get_running_loop()
            future: asyncio.Future[None] = loop.create_future()
            self._waiters.setdefault(peer_id, []).append(future)
            try:
                await future
            except asyncio.CancelledError:
                if peer_id in self._waiters and future in self._waiters[peer_id]:
                    self._waiters[peer_id].remove(future)
                raise

        self._credits[peer_id] -= amount

    def replenish(self, peer_id: str, amount: int = 1) -> None:
        """Return credits to the peer send window and wake pending senders."""
        if amount <= 0:
            return

        current = self._credits.get(peer_id, self.default_initial_credits)
        self._credits[peer_id] = current + amount

        waiters = self._waiters.get(peer_id, [])
        while waiters and self._credits[peer_id] > 0:
            fut = waiters.pop(0)
            if not fut.cancelled() and not fut.done():
                fut.set_result(None)
