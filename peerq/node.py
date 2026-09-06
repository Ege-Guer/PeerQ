"""
Leaderless PeerNode coordinator.

Integrates:
- BackpressurePriorityQueue for local task scheduling.
- CRDT TaskRecords and VectorClocks for conflict-free state replication.
- Fencing tokens and lease expiry checks for stale-lease prevention.
- PhiAccrualDetector for adaptive failure detection.
- LogLinearHistogram & MetricsCollector for telemetry.
- CreditFlowController for backpressure send windows.
- Rendezvous hashing for deterministic lease reclaim selection.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import random
from collections.abc import Awaitable, Callable
from typing import Any

from peerq.clock import Clock
from peerq.consensus import (
    FenceToken,
    TaskRecord,
    TaskState,
    VectorClock,
    merge_records,
)
from peerq.failure import PhiAccrualDetector
from peerq.metrics import MetricsCollector
from peerq.queue import BackpressurePriorityQueue, CreditFlowController, QueueFull
from peerq.transport import Message, Transport
from peerq.wal import (
    RECORD_CHECKPOINT,
    RECORD_CLOCK,
    RECORD_TASK,
    WriteAheadLog,
)

TaskHandler = Callable[[bytes], Awaitable[bytes]]


def _rendezvous_reclaimer(task_id: str, candidate_peers: list[str]) -> str | None:
    """Deterministically elect primary reclaimer for a task among live candidates."""
    if not candidate_peers:
        return None

    def _score(p: str) -> int:
        digest = hashlib.sha256(f"{task_id}:{p}".encode()).digest()
        return int.from_bytes(digest[:8], "big")

    return max(candidate_peers, key=_score)


class PeerNode:
    """
    Leaderless mesh node. All nodes are equal peers.
    """

    def __init__(
        self,
        node_id: str,
        clock: Clock,
        transport: Transport,
        rng: random.Random,
        peers: list[str],
        handler: TaskHandler | None = None,
        lease_duration: float = 5.0,
        heartbeat_interval: float = 1.0,
        gossip_interval: float = 0.5,
        reclaim_interval: float = 1.0,
        max_queue_size: int = 1000,
        initial_credits: int = 20,
        wal: WriteAheadLog | None = None,
    ) -> None:
        self.node_id = node_id
        self.clock = clock
        self.transport = transport
        self.rng = rng
        self.peers = [p for p in peers if p != node_id]
        self.handler = handler
        self.lease_duration = lease_duration
        self.heartbeat_interval = heartbeat_interval
        self.gossip_interval = gossip_interval
        self.reclaim_interval = reclaim_interval
        self.wal = wal

        # Local state
        self._tasks: dict[str, TaskRecord] = {}
        self._queue: BackpressurePriorityQueue[str] = BackpressurePriorityQueue(
            maxsize=max_queue_size
        )
        self._queued_task_ids: set[str] = set()
        self._vector_clock = VectorClock({self.node_id: 0})

        # Subsystems
        self.failure_detector = PhiAccrualDetector(clock=self.clock)
        self.metrics = MetricsCollector()
        self.flow_controller = CreditFlowController(initial_credits=initial_credits)
        for p in self.peers:
            self.flow_controller.init_peer(p, initial_credits)

        # Background coroutine tasks
        self._running = False
        self._bg_tasks: list[asyncio.Task[None]] = []

    def get_task(self, task_id: str) -> TaskRecord | None:
        return self._tasks.get(task_id)

    def all_tasks(self) -> dict[str, TaskRecord]:
        return dict(self._tasks)

    def _recover_from_wal(self) -> None:
        """Reconstruct local task state and vector clock from durable WAL."""
        if self.wal is None:
            return
        for rec in self.wal.replay():
            if rec.record_type == RECORD_TASK:
                task = TaskRecord.from_dict(rec.payload)
                if task.task_id not in self._tasks:
                    self._tasks[task.task_id] = task
                else:
                    self._tasks[task.task_id] = merge_records(self._tasks[task.task_id], task)
            elif rec.record_type == RECORD_CLOCK:
                self._vector_clock = self._vector_clock.merge(VectorClock(rec.payload))
            elif rec.record_type == RECORD_CHECKPOINT:
                raw_tasks: dict[str, Any] = rec.payload.get("tasks", {})
                for tid, tdict in raw_tasks.items():
                    snap_task = TaskRecord.from_dict(tdict)
                    if tid not in self._tasks:
                        self._tasks[tid] = snap_task
                    else:
                        self._tasks[tid] = merge_records(self._tasks[tid], snap_task)
                self._vector_clock = self._vector_clock.merge(
                    VectorClock(rec.payload.get("vector_clock", {}))
                )

        # Re-enqueue any recovered tasks that remain pending
        for tid, task in self._tasks.items():
            if task.state == TaskState.PENDING and tid not in self._queued_task_ids:
                with contextlib.suppress(QueueFull):
                    self._queue.put_nowait(tid, priority=0)
                    self._queued_task_ids.add(tid)

    async def submit_task(self, task_id: str, payload: bytes, priority: int = 0) -> None:
        """Submit a new task into the mesh from this peer."""
        self._vector_clock = self._vector_clock.increment(self.node_id)
        record = TaskRecord(
            task_id=task_id,
            state=TaskState.PENDING,
            payload=payload,
            fence_token=FenceToken(epoch=0, peer_id=""),
            vector_clock=self._vector_clock,
            updated_by=self.node_id,
        )
        self._tasks[task_id] = record
        self.metrics.increment("enqueued")
        if self.wal is not None:
            self.wal.append_task(record)
            self.wal.append_clock(self._vector_clock)

        try:
            await self._queue.put(task_id, priority=priority)
            self._queued_task_ids.add(task_id)
        except QueueFull:
            self.metrics.increment("rejected")
            raise

    async def start(self) -> None:
        """Start node background loops."""
        if self._running:
            return
        self._running = True

        if self.wal is not None:
            self._recover_from_wal()

        self._bg_tasks = [
            asyncio.create_task(self._recv_loop()),
            asyncio.create_task(self._heartbeat_loop()),
            asyncio.create_task(self._gossip_loop()),
            asyncio.create_task(self._reclaim_loop()),
        ]
        if self.handler is not None:
            self._bg_tasks.append(asyncio.create_task(self._worker_loop()))

    async def stop(self) -> None:
        """Stop node background loops and close transport."""
        self._running = False
        for t in self._bg_tasks:
            t.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await t
        self._bg_tasks.clear()
        if self.wal is not None:
            self.wal.close()
        await self.transport.close()

    async def _recv_loop(self) -> None:
        while self._running:
            try:
                sender, msg = await self.transport.recv()
            except Exception:
                if not self._running:
                    break
                await self.clock.sleep(0.01)
                continue

            if msg.msg_type == "heartbeat":
                self.failure_detector.heartbeat(sender)

            elif msg.msg_type == "gossip":
                raw_tasks: dict[str, Any] = msg.payload.get("tasks", {})
                for task_id, raw_rec in raw_tasks.items():
                    remote_rec = TaskRecord.from_dict(raw_rec)
                    if task_id not in self._tasks:
                        self._tasks[task_id] = remote_rec
                        if (
                            remote_rec.state == TaskState.PENDING
                            and task_id not in self._queued_task_ids
                        ):
                            with contextlib.suppress(QueueFull):
                                self._queue.put_nowait(task_id, priority=0)
                                self._queued_task_ids.add(task_id)
                    else:
                        local_rec = self._tasks[task_id]
                        merged = merge_records(local_rec, remote_rec)
                        self._tasks[task_id] = merged
                        if (
                            merged.state == TaskState.PENDING
                            and task_id not in self._queued_task_ids
                        ):
                            with contextlib.suppress(QueueFull):
                                self._queue.put_nowait(task_id, priority=0)
                                self._queued_task_ids.add(task_id)

                    self._vector_clock = self._vector_clock.merge(remote_rec.vector_clock)
                    if self.wal is not None:
                        self.wal.append_task(self._tasks[task_id])
                        self.wal.append_clock(self._vector_clock)

            elif msg.msg_type == "credit":
                amount = int(msg.payload.get("amount", 1))
                self.flow_controller.replenish(sender, amount)

    async def _heartbeat_loop(self) -> None:
        while self._running:
            hb_msg = Message("heartbeat", self.node_id, {"ts": self.clock.now()})
            for peer in self.peers:
                with contextlib.suppress(Exception):
                    await self.transport.send(peer, hb_msg)
            await self.clock.sleep(self.heartbeat_interval)

    async def _gossip_loop(self) -> None:
        while self._running:
            await self.clock.sleep(self.gossip_interval)
            if not self.peers or not self._tasks:
                continue

            partner = self.rng.choice(self.peers)
            payload = {"tasks": {tid: rec.to_dict() for tid, rec in self._tasks.items()}}
            msg = Message("gossip", self.node_id, payload)
            with contextlib.suppress(Exception):
                await self.transport.send(partner, msg)

    async def _broadcast_task_update(self, task_id: str, record: TaskRecord) -> None:
        """Immediately broadcast a task state change to all peers."""
        if self.wal is not None:
            self.wal.append_task(record)
            self.wal.append_clock(self._vector_clock)
        msg = Message("gossip", self.node_id, {"tasks": {task_id: record.to_dict()}})
        for peer in self.peers:
            with contextlib.suppress(Exception):
                await self.transport.send(peer, msg)

    async def _worker_loop(self) -> None:
        assert self.handler is not None
        while self._running:
            try:
                pri, task_id = await self._queue.get()
                self._queued_task_ids.discard(task_id)
            except asyncio.CancelledError:
                break

            record = self._tasks.get(task_id)
            if record is None or record.state.is_terminal:
                continue

            now = self.clock.now()
            holder = record.claimed_by or ""
            is_active_claim = (
                record.state in (TaskState.CLAIMED, TaskState.RUNNING)
                and record.claimed_by != self.node_id
                and record.lease_expiry > now
                and not self.failure_detector.is_suspected(holder)
            )
            if is_active_claim:
                continue

            # Claim the task
            new_token = record.fence_token.next_for(self.node_id)
            self._vector_clock = self._vector_clock.increment(self.node_id)
            lease_exp = self.clock.now() + self.lease_duration

            claimed_record = TaskRecord(
                task_id=task_id,
                state=TaskState.CLAIMED,
                payload=record.payload,
                claimed_by=self.node_id,
                fence_token=new_token,
                lease_expiry=lease_exp,
                vector_clock=self._vector_clock,
                updated_by=self.node_id,
            )
            self._tasks[task_id] = claimed_record
            self.metrics.increment("claimed")

            # Broadcast claim
            await self._broadcast_task_update(task_id, claimed_record)

            # Transition to RUNNING
            running_record = TaskRecord(
                task_id=task_id,
                state=TaskState.RUNNING,
                payload=record.payload,
                claimed_by=self.node_id,
                fence_token=new_token,
                lease_expiry=lease_exp,
                vector_clock=self._vector_clock,
                updated_by=self.node_id,
            )
            self._tasks[task_id] = running_record

            # Execute handler
            try:
                result = await self.handler(record.payload)
                error = None
                new_state = TaskState.DONE
            except Exception as exc:
                result = None
                error = str(exc)
                new_state = TaskState.FAILED

            # Crucial fencing token & lease expiry verification:
            # If execution exceeded lease_expiry, or if our token was superseded,
            # we must NOT commit a stale result!
            current_time = self.clock.now()
            current = self._tasks.get(task_id)
            if current_time > lease_exp:
                # Lease expired during execution: do not commit!
                self.metrics.increment("rejected")
                continue

            if current is not None and current.fence_token > new_token:
                # Superseded by higher fencing token: do not commit!
                self.metrics.increment("rejected")
                continue

            # Commit result
            self._vector_clock = self._vector_clock.increment(self.node_id)
            committed_record = TaskRecord(
                task_id=task_id,
                state=new_state,
                payload=record.payload,
                result=result,
                error=error,
                claimed_by=self.node_id,
                fence_token=new_token,
                lease_expiry=0.0,
                vector_clock=self._vector_clock,
                updated_by=self.node_id,
            )
            self._tasks[task_id] = committed_record
            await self._broadcast_task_update(task_id, committed_record)

            if new_state == TaskState.DONE:
                self.metrics.increment("completed")
            else:
                self.metrics.increment("failed")

            # Replenish credits back to submitter if remote
            if record.updated_by and record.updated_by in self.peers:
                credit_msg = Message("credit", self.node_id, {"amount": 1})
                with contextlib.suppress(Exception):
                    await self.transport.send(record.updated_by, credit_msg)

    async def _reclaim_loop(self) -> None:
        """Periodic audit of task leases with rendezvous reclaimer election."""
        while self._running:
            await self.clock.sleep(self.reclaim_interval)
            now = self.clock.now()

            # Determine currently live peers according to failure detector
            live_peers = [self.node_id] + [
                p for p in self.peers if not self.failure_detector.is_suspected(p)
            ]

            for task_id, record in list(self._tasks.items()):
                if (
                    record.state in (TaskState.CLAIMED, TaskState.RUNNING)
                    and record.claimed_by != self.node_id
                ):
                    lease_expired = record.lease_expiry <= now
                    holder = record.claimed_by or ""
                    holder_suspected = self.failure_detector.is_suspected(holder)

                    if lease_expired and holder_suspected:
                        # Elect deterministic reclaimer among live peers
                        elected = _rendezvous_reclaimer(task_id, live_peers)
                        if elected == self.node_id:
                            self.metrics.increment("reclaimed")
                            if task_id not in self._queued_task_ids:
                                with contextlib.suppress(QueueFull):
                                    self._queue.put_nowait(task_id, priority=10)
                                    self._queued_task_ids.add(task_id)
