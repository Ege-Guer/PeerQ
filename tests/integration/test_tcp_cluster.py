"""
Integration tests for peerq over real operating system TCP sockets.

Verifies end-to-end multi-peer coordination:
- TCP connection pooling & length-prefixed message framing
- Distributed task submission, worker execution, and gossip convergence
- Real crash recovery with WriteAheadLog persistence over loopback networking
"""

import asyncio
import random
from pathlib import Path

import pytest

from peerq.clock import RealClock
from peerq.consensus import TaskState
from peerq.node import PeerNode
from peerq.transport import TcpTransport
from peerq.wal import WriteAheadLog


@pytest.mark.asyncio
async def test_tcp_cluster_task_execution_and_gossip() -> None:
    clock = RealClock()

    # 1. Allocate 3 transports on dynamic port 0
    t1 = TcpTransport("n1", "127.0.0.1", 0, {}, clock)
    t2 = TcpTransport("n2", "127.0.0.1", 0, {}, clock)
    t3 = TcpTransport("n3", "127.0.0.1", 0, {}, clock)

    await t1.start()
    await t2.start()
    await t3.start()

    # 2. Interconnect peer directory
    cluster_addrs = {
        "n1": ("127.0.0.1", t1.port),
        "n2": ("127.0.0.1", t2.port),
        "n3": ("127.0.0.1", t3.port),
    }
    t1.peer_addresses = {k: v for k, v in cluster_addrs.items() if k != "n1"}
    t2.peer_addresses = {k: v for k, v in cluster_addrs.items() if k != "n2"}
    t3.peer_addresses = {k: v for k, v in cluster_addrs.items() if k != "n3"}

    # 3. Create nodes; n2 is the designated worker
    async def uppercase_handler(payload: bytes) -> bytes:
        return payload.upper()

    rng1 = random.Random(1)
    rng2 = random.Random(2)
    rng3 = random.Random(3)

    n1 = PeerNode(
        "n1",
        clock,
        t1,
        rng1,
        peers=["n2", "n3"],
        gossip_interval=0.1,
        heartbeat_interval=0.2,
    )
    n2 = PeerNode(
        "n2",
        clock,
        t2,
        rng2,
        peers=["n1", "n3"],
        handler=uppercase_handler,
        gossip_interval=0.1,
        heartbeat_interval=0.2,
    )
    n3 = PeerNode(
        "n3",
        clock,
        t3,
        rng3,
        peers=["n1", "n2"],
        gossip_interval=0.1,
        heartbeat_interval=0.2,
    )

    await n1.start()
    await n2.start()
    await n3.start()

    try:
        # Submit task on n1
        await n1.submit_task("task-tcp-e2e", b"distributed-task", priority=5)

        # Allow real asyncio network traffic to exchange heartbeats, claims, and results
        for _ in range(30):
            await asyncio.sleep(0.1)
            rec1 = n1.get_task("task-tcp-e2e")
            rec2 = n2.get_task("task-tcp-e2e")
            rec3 = n3.get_task("task-tcp-e2e")
            if (
                rec1 is not None
                and rec1.state == TaskState.DONE
                and rec2 is not None
                and rec2.state == TaskState.DONE
                and rec3 is not None
                and rec3.state == TaskState.DONE
            ):
                break

        final_rec1 = n1.get_task("task-tcp-e2e")
        final_rec2 = n2.get_task("task-tcp-e2e")
        final_rec3 = n3.get_task("task-tcp-e2e")

        assert final_rec1 is not None
        assert final_rec1.state == TaskState.DONE
        assert final_rec1.result == b"DISTRIBUTED-TASK"

        assert final_rec2 is not None
        assert final_rec2.state == TaskState.DONE
        assert final_rec2.result == b"DISTRIBUTED-TASK"

        assert final_rec3 is not None
        assert final_rec3.state == TaskState.DONE
        assert final_rec3.result == b"DISTRIBUTED-TASK"

    finally:
        await n1.stop()
        await n2.stop()
        await n3.stop()


@pytest.mark.asyncio
async def test_tcp_node_wal_crash_recovery(tmp_path: Path) -> None:
    clock = RealClock()
    wal_file = tmp_path / "tcp_node.wal"

    t = TcpTransport("node-tcp", "127.0.0.1", 0, {}, clock)
    await t.start()

    async def double_handler(payload: bytes) -> bytes:
        return payload + payload

    wal = WriteAheadLog(wal_file)
    rng = random.Random(42)
    node = PeerNode("node-tcp", clock, t, rng, peers=[], handler=double_handler, wal=wal)

    await node.start()
    await node.submit_task("t-tcp-wal", b"persist-over-tcp")

    # Wait for execution
    for _ in range(15):
        await asyncio.sleep(0.05)
        rec = node.get_task("t-tcp-wal")
        if rec is not None and rec.state == TaskState.DONE:
            break

    final_task = node.get_task("t-tcp-wal")
    assert final_task is not None
    assert final_task.state == TaskState.DONE
    assert final_task.result == b"persist-over-tcppersist-over-tcp"

    # Stop node (closes WAL and transport)
    await node.stop()

    # Re-open WAL in fresh instance
    wal_reboot = WriteAheadLog(wal_file)
    t_reboot = TcpTransport("node-tcp", "127.0.0.1", 0, {}, clock)
    await t_reboot.start()

    reboot_node = PeerNode(
        "node-tcp", clock, t_reboot, rng, peers=[], handler=double_handler, wal=wal_reboot
    )
    assert reboot_node.get_task("t-tcp-wal") is None

    await reboot_node.start()
    recovered_task = reboot_node.get_task("t-tcp-wal")
    assert recovered_task is not None
    assert recovered_task.state == TaskState.DONE
    assert recovered_task.result == b"persist-over-tcppersist-over-tcp"

    await reboot_node.stop()
