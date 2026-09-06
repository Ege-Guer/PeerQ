"""
Clock abstraction for peerq.

Strict architectural rule:
NO module outside this file may import time, datetime, or call asyncio.sleep.
All components must receive an injected Clock implementation.
"""

from __future__ import annotations

import asyncio
import heapq
import time
from typing import Protocol, runtime_checkable


@runtime_checkable
class Clock(Protocol):
    """Protocol for abstract time and sleep."""

    def now(self) -> float:
        """Return the current monotonic timestamp in seconds."""
        ...

    async def sleep(self, delay: float) -> None:
        """Asynchronously suspend execution for the specified duration in seconds."""
        ...


class RealClock:
    """Real wall/monotonic clock for production and live network deployments."""

    def now(self) -> float:
        return time.monotonic()

    async def sleep(self, delay: float) -> None:
        if delay > 0:
            await asyncio.sleep(delay)
        else:
            await asyncio.sleep(0)


class SimClock:
    """
    Virtual deterministic clock for simulation testing.

    Time does not pass unless explicitly stepped or advanced.
    Sleeping coroutines are registered in a min-heap and woken in strict
    chronological order when virtual time advances.
    """

    def __init__(self, initial_time: float = 0.0) -> None:
        self._now: float = initial_time
        self._sleepers: list[tuple[float, int, asyncio.Future[None]]] = []
        self._counter: int = 0

    def now(self) -> float:
        return self._now

    async def sleep(self, delay: float) -> None:
        if delay <= 0:
            # Yield control cooperatively to the event loop
            loop = asyncio.get_running_loop()
            future: asyncio.Future[None] = loop.create_future()
            loop.call_soon(future.set_result, None)
            await future
            return

        loop = asyncio.get_running_loop()
        future = loop.create_future()
        deadline = self._now + delay
        entry = (deadline, self._counter, future)
        self._counter += 1
        heapq.heappush(self._sleepers, entry)

        try:
            await future
        except asyncio.CancelledError:
            # Future was cancelled by caller; heap entry will be discarded on pop
            raise

    def has_pending_sleepers(self) -> bool:
        """Check if any coroutines are currently awaiting sleep."""
        # Clean up already cancelled futures from top of heap
        while self._sleepers and self._sleepers[0][2].cancelled():
            heapq.heappop(self._sleepers)
        return len(self._sleepers) > 0

    def next_deadline(self) -> float | None:
        """Return the deadline of the earliest sleeper, or None if no sleepers."""
        while self._sleepers and self._sleepers[0][2].cancelled():
            heapq.heappop(self._sleepers)
        return self._sleepers[0][0] if self._sleepers else None

    def advance(self, duration: float) -> int:
        """
        Advance virtual time by duration, waking up any sleepers whose deadline has arrived.
        Returns the number of sleepers woken.
        """
        if duration < 0:
            raise ValueError(f"Cannot advance backwards in time: {duration}")
        return self.advance_to(self._now + duration)

    def advance_to(self, target_time: float) -> int:
        """
        Advance virtual time to target_time, waking up sleepers in order.
        Returns the number of sleepers woken.
        """
        if target_time < self._now:
            raise ValueError(f"Cannot advance backwards: {target_time} < {self._now}")

        woken = 0
        while self._sleepers:
            deadline, _, future = self._sleepers[0]
            if deadline > target_time:
                break
            heapq.heappop(self._sleepers)
            if not future.cancelled() and not future.done():
                self._now = deadline
                future.set_result(None)
                woken += 1

        self._now = target_time
        return woken

    def step(self) -> float | None:
        """
        Advance virtual time to the earliest scheduled deadline and wake its sleeper(s).
        Returns the new virtual timestamp, or None if no sleepers were waiting.
        """
        while self._sleepers and self._sleepers[0][2].cancelled():
            heapq.heappop(self._sleepers)

        if not self._sleepers:
            return None

        earliest = self._sleepers[0][0]
        self.advance_to(earliest)
        return self._now
