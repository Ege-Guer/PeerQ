"""
PeerQ Ed25519 Cryptographic & Task Integrity Benchmark.

Measures:
1. Ed25519 keypair generation throughput (keys/sec).
2. Canonical deterministic serialization throughput (records/sec and MB/sec).
3. Digital signature generation throughput (sign ops/sec).
4. Digital signature verification throughput (verify ops/sec).
5. Byzantine tamper detection & rejection throughput (rejections/sec).

Honesty rule: Commit raw unedited output to bench/results/crypto_throughput_raw.txt.
"""

from __future__ import annotations

import platform
import sys
import time
from pathlib import Path

# Ensure project root is on sys.path
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from peerq.consensus import FenceToken, TaskRecord, TaskState, VectorClock  # noqa: E402
from peerq.crypto import (  # noqa: E402
    Ed25519KeyPair,
    PeerKeyRing,
    canonical_task_bytes,
    sign_task,
    verify_task_signature,
)


def make_bench_record(i: int) -> TaskRecord:
    return TaskRecord(
        task_id=f"crypto-bench-task-{i}",
        state=TaskState.DONE,
        payload=b'{"operation":"aggregate","metrics":[98.2, 99.1, 99.9],"batch":1024}',
        result=b'{"status":"completed","records_processed":1024,"duration_ms":12.4}',
        error=None,
        claimed_by="node-signer",
        fence_token=FenceToken(epoch=1, peer_id="node-signer"),
        lease_expiry=100.0,
        vector_clock=VectorClock({"node-signer": i + 1}),
        updated_by="node-signer",
    )


def run_crypto_benchmarks() -> None:
    print("=" * 65)
    print("PEERQ ED25519 CRYPTOGRAPHIC & INTEGRITY BENCHMARK")
    print("=" * 65)
    print(f"Timestamp:       {time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime())}")
    print(f"Platform:        {platform.platform()}")
    print(f"Processor:       {platform.processor() or platform.machine()}")
    print(f"Python:          {sys.version.split()[0]} ({platform.python_implementation()})")
    print(f"Command line:    {' '.join(sys.argv)}")
    print("=" * 65)

    # 1. Keypair Generation
    keygen_iterations = 5_000
    t0 = time.perf_counter()
    for _ in range(keygen_iterations):
        _ = Ed25519KeyPair.generate()
    t_keygen = time.perf_counter() - t0
    keygen_rate = keygen_iterations / t_keygen

    print(f"1. Ed25519 Keypair Generation ({keygen_iterations:,} iterations):")
    print(f"   Elapsed:      {t_keygen:.4f} s")
    print(f"   Throughput:   {keygen_rate:,.1f} keys/sec")
    print()

    # 2. Canonical Deterministic Serialization
    keypair = Ed25519KeyPair.generate()
    sample_records = [make_bench_record(i) for i in range(10_000)]
    t0 = time.perf_counter()
    serialized_bytes = [canonical_task_bytes(r) for r in sample_records]
    t_ser = time.perf_counter() - t0
    ser_rate = len(sample_records) / t_ser
    total_bytes = sum(len(b) for b in serialized_bytes)
    ser_mb = (total_bytes / (1024 * 1024)) / t_ser

    print(f"2. Canonical Task Serialization ({len(sample_records):,} records):")
    print(f"   Elapsed:      {t_ser:.4f} s")
    print(f"   Throughput:   {ser_rate:,.1f} records/sec ({ser_mb:,.2f} MB/sec)")
    print(f"   Payload avg:  {total_bytes / len(sample_records):.1f} bytes/record")
    print()

    # 3. Digital Signature Generation
    t0 = time.perf_counter()
    signed_records = [sign_task(keypair, r, "node-signer") for r in sample_records]
    t_sign = time.perf_counter() - t0
    sign_rate = len(signed_records) / t_sign

    print(f"3. Ed25519 Task Signing ({len(signed_records):,} signatures):")
    print(f"   Elapsed:      {t_sign:.4f} s")
    print(f"   Throughput:   {sign_rate:,.1f} signs/sec")
    print()

    # 4. Digital Signature Verification
    keyring = PeerKeyRing()
    keyring.add_peer("node-signer", keypair.public_key)

    t0 = time.perf_counter()
    for r in signed_records:
        ok = verify_task_signature(r, keyring)
        assert ok, f"Verification failed unexpectedly for task {r.task_id}"
    t_verify = time.perf_counter() - t0
    verify_rate = len(signed_records) / t_verify

    print(f"4. Ed25519 Signature Verification ({len(signed_records):,} validations):")
    print(f"   Elapsed:      {t_verify:.4f} s")
    print(f"   Throughput:   {verify_rate:,.1f} verifications/sec")
    print()

    # 5. Byzantine Tamper Detection & Rejection
    # Create records with modified payload but original signature
    tampered_records = [
        TaskRecord(
            task_id=r.task_id,
            state=r.state,
            payload=b"MALICIOUS_TAMPERED_PAYLOAD",
            result=r.result,
            error=r.error,
            claimed_by=r.claimed_by,
            fence_token=r.fence_token,
            lease_expiry=r.lease_expiry,
            vector_clock=r.vector_clock,
            signature=r.signature,
            updated_by=r.updated_by,
        )
        for r in signed_records
    ]

    t0 = time.perf_counter()
    rejected_count = 0
    for tr in tampered_records:
        if not verify_task_signature(tr, keyring):
            rejected_count += 1
    t_reject = time.perf_counter() - t0
    assert rejected_count == len(tampered_records)
    reject_rate = len(tampered_records) / t_reject

    print(f"5. Byzantine Tamper Detection ({len(tampered_records):,} forged tasks):")
    print(f"   Elapsed:      {t_reject:.4f} s")
    print(f"   Throughput:   {reject_rate:,.1f} rejections/sec (100.0% rejected)")
    print()
    print("=" * 65)
    print("PEERQ CRYPTOGRAPHIC BENCHMARK COMPLETED SUCCESSFULLY")
    print("=" * 65)


if __name__ == "__main__":
    run_crypto_benchmarks()
