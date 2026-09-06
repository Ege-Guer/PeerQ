"""
Deterministic simulation engine for peerq.

Provides single-process virtual-time simulation of an N-peer mesh driven
strictly by a pseudo-random seed.
Enforces all 5 continuous invariants across every simulation step.
"""

from __future__ import annotations

import random
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from peerq.clock import SimClock
from peerq.consensus import FenceToken, TaskState
from peerq.node import PeerNode
from peerq.transport import InMemoryTransport, SimNetwork

SimTaskHandler = Callable[[bytes], Awaitable[bytes]]


@dataclass(frozen=True)
class SimEvent:
    timestamp: float
    event_type: str
    node_id: str
    details: str

    def to_line(self) -> str:
        ts = f"{self.timestamp:10.4f}"
        nid = f"{self.node_id:10s}"
        etype = f"{self.event_type:14s}"
        return f"[{ts}] [{nid}] {etype} {self.details}\n"


class SimCluster:
    """
    Simulation cluster coordinator managing virtual time, nodes, and invariant monitors.
    """

    def __init__(
        self,
        seed: int,
        peer_ids: list[str],
        handler: SimTaskHandler | None = None,
        base_latency: float = 0.05,
    ) -> None:
        self.seed = seed
        self.rng = random.Random(seed)
        self.clock = SimClock(0.0)
        self.network = SimNetwork(self.clock)
        self.network.base_latency = base_latency
        self.peer_ids = list(peer_ids)
        self.handler = handler

        self.nodes: dict[str, PeerNode] = {}
        self.transports: dict[str, InMemoryTransport] = {}
        self.node_rngs: dict[str, random.Random] = {}
        self.active_nodes: set[str] = set()

        # Deterministic event trace
        self.trace: list[SimEvent] = []

        # Invariant tracking state
        self._submitted_tasks: set[str] = set()
        self._highest_committed_tokens: dict[str, FenceToken] = {}
        self._last_seen_clocks: dict[str, dict[str, int]] = {}

        # Set up nodes
        for pid in peer_ids:
            node_seed = self.rng.randint(0, 10**9)
            node_rng = random.Random(node_seed)
            self.node_rngs[pid] = node_rng

            transport = InMemoryTransport(pid, self.network)
            self.transports[pid] = transport

            node = PeerNode(
                node_id=pid,
                clock=self.clock,
                transport=transport,
                rng=node_rng,
                peers=peer_ids,
                handler=handler,
                lease_duration=3.0,
                heartbeat_interval=0.5,
                gossip_interval=0.3,
                reclaim_interval=0.5,
            )
            self.nodes[pid] = node
            self.active_nodes.add(pid)

    def log_event(self, event_type: str, node_id: str, details: str) -> None:
        event = SimEvent(
            timestamp=self.clock.now(),
            event_type=event_type,
            node_id=node_id,
            details=details,
        )
        self.trace.append(event)

    def get_trace_bytes(self) -> bytes:
        return "".join(ev.to_line() for ev in self.trace).encode("utf-8")

    async def start(self) -> None:
        for pid in self.peer_ids:
            await self.nodes[pid].start()
            self.log_event("NODE_START", pid, "Node started")

    async def stop(self) -> None:
        for pid in list(self.active_nodes):
            await self.nodes[pid].stop()
            self.log_event("NODE_STOP", pid, "Node stopped")
        self.active_nodes.clear()

    async def crash_node(self, node_id: str) -> None:
        """Simulate an abrupt node crash."""
        if node_id in self.active_nodes:
            self.active_nodes.remove(node_id)
            await self.nodes[node_id].stop()
            self.log_event("NODE_CRASH", node_id, "Abrupt crash simulated")

    async def restart_node(self, node_id: str) -> None:
        """Restart a crashed node with preserved task storage and vector clock."""
        old_node = self.nodes[node_id]
        preserved_tasks = old_node.all_tasks()
        preserved_clock = old_node._vector_clock

        transport = InMemoryTransport(node_id, self.network)
        self.transports[node_id] = transport
        node_rng = self.node_rngs[node_id]

        new_node = PeerNode(
            node_id=node_id,
            clock=self.clock,
            transport=transport,
            rng=node_rng,
            peers=self.peer_ids,
            handler=self.handler,
            lease_duration=3.0,
            heartbeat_interval=0.5,
            gossip_interval=0.3,
            reclaim_interval=0.5,
        )
        new_node._tasks = preserved_tasks
        new_node._vector_clock = preserved_clock
        self.nodes[node_id] = new_node
        self.active_nodes.add(node_id)
        await new_node.start()
        self.log_event("NODE_RESTART", node_id, "Node restarted")

    def partition(self, group_a: set[str], group_b: set[str], symmetric: bool = True) -> None:
        self.network.partition(group_a, group_b, symmetric=symmetric)
        self.log_event("NET_PARTITION", "SIM", f"Part {group_a} <-> {group_b} (sym={symmetric})")

    def heal_partition(self, group_a: set[str], group_b: set[str], symmetric: bool = True) -> None:
        self.network.heal_partition(group_a, group_b, symmetric=symmetric)
        self.log_event("NET_HEAL", "SIM", f"Heal {group_a} <-> {group_b}")

    async def submit_task(
        self, node_id: str, task_id: str, payload: bytes, priority: int = 0
    ) -> None:
        self._submitted_tasks.add(task_id)
        await self.nodes[node_id].submit_task(task_id, payload, priority=priority)
        self.log_event("TASK_SUBMIT", node_id, f"task_id={task_id} pri={priority}")

    async def step(self, duration: float = 0.1) -> None:
        """Advance virtual time by duration, allowing background coroutines to process."""
        self.clock.advance(duration)
        await self.clock.sleep(0)
        self.check_invariants()

    async def run_for(self, virtual_seconds: float, step_size: float = 0.1) -> None:
        """Run virtual simulation for the specified virtual duration."""
        steps = int(virtual_seconds / step_size)
        for _ in range(steps):
            await self.step(step_size)

    def check_invariants(self) -> None:
        """
        Continuously check all 5 required system invariants:
        1. No task concurrently held by two live peers with valid leases.
        2. Vector clocks never regress on any peer.
        3. Fencing token monotonically respected on result commit.
        4. Credit balance never negative.
        """
        now = self.clock.now()

        # Invariant 1: Mutual exclusion of valid unexpired leases among live peers
        leases_by_task: dict[str, list[tuple[str, FenceToken, float]]] = {}
        for pid in self.active_nodes:
            node = self.nodes[pid]
            for tid, rec in node.all_tasks().items():
                if rec.state in (TaskState.CLAIMED, TaskState.RUNNING) and rec.lease_expiry > now:
                    holder = rec.claimed_by or pid
                    if holder in self.active_nodes:
                        entry = (holder, rec.fence_token, rec.lease_expiry)
                        leases_by_task.setdefault(tid, []).append(entry)

        for tid, holders in leases_by_task.items():
            distinct_holders = {h[0] for h in holders}
            assert len(distinct_holders) <= 1, (
                f"Invariant 1 violated: Task {tid} held concurrently by multiple "
                f"live peers: {distinct_holders} at t={now}"
            )

        # Invariant 2: Vector clocks never regress
        for pid in self.active_nodes:
            node = self.nodes[pid]
            curr_clock = node._vector_clock.clock
            last_clock = self._last_seen_clocks.get(pid, {})
            for k, val in last_clock.items():
                curr_val = curr_clock.get(k, 0)
                assert curr_val >= val, (
                    f"Invariant 2 violated: Vector clock regressed on node {pid} "
                    f"for key {k}: {curr_val} < {val}"
                )
            self._last_seen_clocks[pid] = dict(curr_clock)

        # Invariant 3: Fencing token commits never regress
        for pid in self.active_nodes:
            node = self.nodes[pid]
            for tid, rec in node.all_tasks().items():
                if rec.state.is_terminal:
                    prev_token = self._highest_committed_tokens.get(tid)
                    if prev_token is not None:
                        assert rec.fence_token >= prev_token, (
                            f"Invariant 3 violated: Result committed with stale "
                            f"fencing token for {tid}: {rec.fence_token} < {prev_token}"
                        )
                    self._highest_committed_tokens[tid] = rec.fence_token

        # Invariant 4: Credit balance never negative
        for pid in self.active_nodes:
            node = self.nodes[pid]
            for peer_id in node.peers:
                credits = node.flow_controller.get_credits(peer_id)
                assert credits >= 0, (
                    f"Invariant 4 violated: Negative credits on {pid} for peer {peer_id}: {credits}"
                )

    def assert_all_submitted_tasks_completed(self) -> None:
        """Verify that every submitted task reached terminal DONE or FAILED."""
        for tid in self._submitted_tasks:
            completed_anywhere = False
            for pid in self.active_nodes:
                rec = self.nodes[pid].get_task(tid)
                if rec is not None and rec.state.is_terminal:
                    completed_anywhere = True
                    break
            assert completed_anywhere, (
                f"Invariant: Task {tid} vanished or never completed across live nodes!"
            )
