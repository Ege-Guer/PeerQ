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
import json
import os
import random
from collections.abc import Awaitable, Callable
from math import isfinite
from typing import Any

from peerq.clock import Clock
from peerq.consensus import (
    FenceToken,
    TaskRecord,
    TaskState,
    VectorClock,
    merge_records,
)
from peerq.crypto import (
    Ed25519KeyPair,
    PeerKeyRing,
    sign_task,
    verify_task_authorization,
)
from peerq.failure import PhiAccrualDetector
from peerq.metrics import MetricsCollector
from peerq.queue import BackpressurePriorityQueue, CreditFlowController, QueueFull
from peerq.security import (
    MAX_PEER_ID_LENGTH,
    MAX_TASK_ID_LENGTH,
    MAX_TASK_PAYLOAD,
    ReplayCache,
    SecurityConfig,
)
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
        lease_duration: float | None = 5.0,
        heartbeat_interval: float = 1.0,
        gossip_interval: float = 0.5,
        reclaim_interval: float = 1.0,
        max_queue_size: int = 1000,
        initial_credits: int = 20,
        wal: WriteAheadLog | None = None,
        identity: Ed25519KeyPair | None = None,
        keyring: PeerKeyRing | None = None,
        security: SecurityConfig | None = None,
        max_tasks: int = 10_000,
    ) -> None:
        self.node_id = node_id
        self.clock = clock
        self.transport = transport
        self.rng = rng
        if not node_id or len(node_id) > MAX_PEER_ID_LENGTH:
            raise ValueError("node_id must be a non-empty bounded string")
        self.peers = list(dict.fromkeys(p for p in peers if p != node_id))
        if any(not p or len(p) > MAX_PEER_ID_LENGTH for p in self.peers):
            raise ValueError("peer IDs must be non-empty bounded strings")
        self.handler = handler
        if lease_duration is None or lease_duration == 5.0:
            env_lease = os.environ.get("PEERQ_LEASE_DURATION")
            if env_lease:
                with contextlib.suppress(ValueError):
                    lease_duration = float(env_lease)
        if lease_duration is None:
            lease_duration = 5.0
        self.lease_duration = lease_duration
        self.heartbeat_interval = heartbeat_interval
        self.gossip_interval = gossip_interval
        self.reclaim_interval = reclaim_interval
        self.wal = wal
        self.identity = identity or Ed25519KeyPair.generate()
        self.keyring = keyring or PeerKeyRing()
        existing_key = self.keyring.get_peer_key(node_id)
        if existing_key is None:
            self.keyring.add_peer(node_id, self.identity.public_key)
        elif existing_key.to_bytes() != self.identity.public_key.to_bytes():
            raise ValueError("keyring identity does not match the node identity")
        self.security = security or SecurityConfig()
        self.max_tasks = max_tasks
        if max_tasks <= 0:
            raise ValueError("max_tasks must be positive")
        self._message_replay = ReplayCache(self.security.replay_cache_size)

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

    def _seal_record(self, record: TaskRecord) -> TaskRecord:
        if not self.security.enabled:
            return record
        return sign_task(self.identity, record, signer_id=self.node_id)

    def _signed_message(self, msg_type: str, payload: dict[str, Any]) -> Message:
        message = Message(
            msg_type,
            self.node_id,
            payload,
            protocol_version=self.security.protocol_version,
        )
        return message.signed(self.identity, timestamp=self.clock.wall_now())

    def _authenticate_message(self, sender: str, msg: Message) -> bool:
        if not self.security.enabled:
            return True
        if sender != msg.sender_id:
            return False
        if msg.protocol_version != self.security.protocol_version:
            return False
        if msg.msg_type not in {"heartbeat", "gossip", "credit"}:
            return False
        if not self.keyring.has_peer(sender) or not msg.verify_signature(self.keyring):
            return False
        if msg.timestamp is None or msg.nonce is None:
            return False
        now = self.clock.wall_now()
        if abs(now - msg.timestamp) > self.security.replay_window_seconds:
            return False
        return self._message_replay.check_and_remember(
            sender,
            msg.nonce,
            now,
            self.security.replay_window_seconds,
        )

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
                try:
                    task = TaskRecord.from_dict(rec.payload)
                except (TypeError, ValueError):
                    # A bounded WAL can still contain an old or malformed record.
                    # Recovery is fail-closed for task state rather than promoting
                    # unverified bytes into the live CRDT.
                    continue
                if self.security.enabled and not verify_task_authorization(task, self.keyring):
                    continue
                if task.task_id not in self._tasks and len(self._tasks) >= self.max_tasks:
                    continue
                if task.task_id not in self._tasks:
                    self._tasks[task.task_id] = task
                else:
                    with contextlib.suppress(ValueError):
                        self._tasks[task.task_id] = merge_records(
                            self._tasks[task.task_id],
                            task,
                            keyring=self.keyring if self.security.enabled else None,
                        )
            elif rec.record_type == RECORD_CLOCK:
                try:
                    self._vector_clock = self._vector_clock.merge(VectorClock(rec.payload))
                except (TypeError, ValueError):
                    continue
            elif rec.record_type == RECORD_CHECKPOINT:
                raw_tasks: dict[str, Any] = rec.payload.get("tasks", {})
                if not isinstance(raw_tasks, dict):
                    continue
                for tid, tdict in raw_tasks.items():
                    if (not isinstance(tid, str) or len(self._tasks) >= self.max_tasks) and (
                        tid not in self._tasks
                    ):
                        continue
                    try:
                        snap_task = TaskRecord.from_dict(tdict)
                    except (TypeError, ValueError):
                        continue
                    if self.security.enabled and not verify_task_authorization(
                        snap_task, self.keyring
                    ):
                        continue
                    if tid not in self._tasks:
                        self._tasks[tid] = snap_task
                    else:
                        with contextlib.suppress(ValueError):
                            self._tasks[tid] = merge_records(
                                self._tasks[tid],
                                snap_task,
                                keyring=self.keyring if self.security.enabled else None,
                            )
                try:
                    self._vector_clock = self._vector_clock.merge(
                        VectorClock(rec.payload.get("vector_clock", {}))
                    )
                except (TypeError, ValueError):
                    continue

        # Re-enqueue any recovered tasks that remain pending
        for tid, task in self._tasks.items():
            if task.state == TaskState.PENDING and tid not in self._queued_task_ids:
                with contextlib.suppress(QueueFull):
                    self._queue.put_nowait(tid, priority=0)
                    self._queued_task_ids.add(tid)

    async def submit_task(self, task_id: str, payload: bytes, priority: int = 0) -> None:
        """Submit a new task into the mesh from this peer."""
        if not isinstance(task_id, str) or not task_id or len(task_id) > MAX_TASK_ID_LENGTH:
            raise ValueError("task_id must be a non-empty bounded string")
        if not isinstance(payload, bytes) or len(payload) > MAX_TASK_PAYLOAD:
            raise ValueError("task payload exceeds the configured limit")
        if task_id not in self._tasks and len(self._tasks) >= self.max_tasks:
            self.metrics.increment("rejected")
            raise QueueFull("task store is full")
        self._vector_clock = self._vector_clock.increment(self.node_id)
        record = self._seal_record(
            TaskRecord(
                task_id=task_id,
                state=TaskState.PENDING,
                payload=payload,
                fence_token=FenceToken(epoch=0, peer_id=""),
                vector_clock=self._vector_clock,
                updated_by=self.node_id,
            )
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

            if not self._authenticate_message(sender, msg):
                self.metrics.increment("rejected")
                continue

            if msg.msg_type == "heartbeat":
                self.failure_detector.heartbeat(sender)

            elif msg.msg_type == "gossip":
                raw_tasks = msg.payload.get("tasks", {})
                if not isinstance(raw_tasks, dict) or len(raw_tasks) > (
                    self.security.max_gossip_tasks
                ):
                    self.metrics.increment("rejected")
                    continue
                for task_id, raw_rec in raw_tasks.items():
                    if not isinstance(task_id, str):
                        self.metrics.increment("rejected")
                        continue
                    try:
                        remote_rec = TaskRecord.from_dict(raw_rec)
                    except (TypeError, ValueError):
                        self.metrics.increment("rejected")
                        continue
                    if remote_rec.task_id != task_id:
                        self.metrics.increment("rejected")
                        continue
                    if self.security.enabled and (
                        remote_rec.signer_id != sender
                        or not verify_task_authorization(remote_rec, self.keyring)
                    ):
                        self.metrics.increment("rejected")
                        continue
                    if task_id not in self._tasks and len(self._tasks) >= self.max_tasks:
                        self.metrics.increment("rejected")
                        continue
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
                        try:
                            merged = merge_records(
                                local_rec,
                                remote_rec,
                                keyring=self.keyring if self.security.enabled else None,
                            )
                        except ValueError:
                            self.metrics.increment("rejected")
                            continue
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
                amount = msg.payload.get("amount", 1)
                if (
                    not isinstance(amount, int)
                    or isinstance(amount, bool)
                    or amount < 0
                    or amount > self.security.max_credit_amount
                ):
                    self.metrics.increment("rejected")
                    continue
                self.flow_controller.replenish(sender, amount)

    async def _heartbeat_loop(self) -> None:
        while self._running:
            hb_msg = self._signed_message("heartbeat", {"ts": self.clock.now()})
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
            task_items = sorted(self._tasks.items())[: self.security.max_gossip_tasks]
            payload = {"tasks": {tid: rec.to_dict() for tid, rec in task_items}}
            msg = self._signed_message("gossip", payload)
            with contextlib.suppress(Exception):
                await self.transport.send(partner, msg)

    async def _broadcast_task_update(self, task_id: str, record: TaskRecord) -> None:
        """Immediately broadcast a task state change to all peers."""
        record = self._seal_record(record)
        self._tasks[task_id] = record
        if self.wal is not None:
            self.wal.append_task(record)
            self.wal.append_clock(self._vector_clock)
        msg = self._signed_message("gossip", {"tasks": {task_id: record.to_dict()}})
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
            task_lease_duration = self.lease_duration
            if isinstance(record.payload, bytes):
                with contextlib.suppress(Exception):
                    payload_obj = json.loads(record.payload.decode("utf-8"))
                    if isinstance(payload_obj, dict) and "lease_duration" in payload_obj:
                        task_lease_duration = float(payload_obj["lease_duration"])
            elif isinstance(record.payload, dict) and "lease_duration" in record.payload:
                with contextlib.suppress(Exception):
                    task_lease_duration = float(record.payload["lease_duration"])
            if (
                not isfinite(task_lease_duration)
                or task_lease_duration <= 0.0
                or task_lease_duration > self.security.max_lease_duration_seconds
            ):
                task_lease_duration = self.lease_duration
            lease_exp = self.clock.now() + task_lease_duration

            claimed_record = self._seal_record(
                TaskRecord(
                    task_id=task_id,
                    state=TaskState.CLAIMED,
                    payload=record.payload,
                    claimed_by=self.node_id,
                    fence_token=new_token,
                    lease_expiry=lease_exp,
                    vector_clock=self._vector_clock,
                    updated_by=self.node_id,
                )
            )
            self._tasks[task_id] = claimed_record
            self.metrics.increment("claimed")

            # Broadcast claim
            await self._broadcast_task_update(task_id, claimed_record)

            # Transition to RUNNING
            running_record = self._seal_record(
                TaskRecord(
                    task_id=task_id,
                    state=TaskState.RUNNING,
                    payload=record.payload,
                    claimed_by=self.node_id,
                    fence_token=new_token,
                    lease_expiry=lease_exp,
                    vector_clock=self._vector_clock,
                    updated_by=self.node_id,
                )
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
            committed_record = self._seal_record(
                TaskRecord(
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
            )
            self._tasks[task_id] = committed_record
            await self._broadcast_task_update(task_id, committed_record)

            if new_state == TaskState.DONE:
                self.metrics.increment("completed")
            else:
                self.metrics.increment("failed")

            # Replenish credits back to submitter if remote
            if record.updated_by and record.updated_by in self.peers:
                credit_msg = self._signed_message("credit", {"amount": 1})
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
                            if self.handler is not None:
                                if task_id not in self._queued_task_ids:
                                    with contextlib.suppress(QueueFull):
                                        self._queue.put_nowait(task_id, priority=10)
                                        self._queued_task_ids.add(task_id)
                            else:
                                # Reclaimer without local worker: release task back to PENDING
                                self._vector_clock = self._vector_clock.increment(self.node_id)
                                new_token = record.fence_token.next_for(self.node_id)
                                reset_record = self._seal_record(
                                    TaskRecord(
                                        task_id=task_id,
                                        state=TaskState.PENDING,
                                        payload=record.payload,
                                        claimed_by=None,
                                        fence_token=new_token,
                                        lease_expiry=0.0,
                                        vector_clock=self._vector_clock,
                                        updated_by=self.node_id,
                                    )
                                )
                                self._tasks[task_id] = reset_record
                                await self._broadcast_task_update(task_id, reset_record)
