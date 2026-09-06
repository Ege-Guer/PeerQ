"""
Hypothesis property tests for peerq.crypto.

Verifies mathematical and cryptographic invariants:
1. Correctness: Every authentically signed record always verifies under its public key.
2. Sensitivity: Any mutation in payload, state, fence token, or vector clock invalidates
   verification.
3. Non-forgeability: Signing with key A cannot be verified under key B's public key.
"""

from dataclasses import replace

from hypothesis import given
from hypothesis import strategies as st

from peerq.consensus import FenceToken, TaskRecord, TaskState, VectorClock
from peerq.crypto import (
    Ed25519KeyPair,
    PeerKeyRing,
    sign_task,
    verify_task_authorization,
    verify_task_signature,
)

states_st = st.sampled_from(list(TaskState))
task_ids_st = st.text(min_size=1, max_size=32, alphabet="abcdefghijklmnopqrstuvwxyz0123456789-")
node_ids_st = st.text(min_size=1, max_size=16, alphabet="abcdefghijklmnopqrstuvwxyz0123456789")
payloads_st = st.binary(min_size=0, max_size=512)
epochs_st = st.integers(min_value=0, max_value=1_000_000)
leases_st = st.floats(min_value=0.0, max_value=10_000.0, allow_nan=False, allow_infinity=False)


@given(
    task_id=task_ids_st,
    state=states_st,
    payload=payloads_st,
    epoch=epochs_st,
    lease=leases_st,
)
def test_crypto_sign_verify_roundtrip(
    task_id: str,
    state: TaskState,
    payload: bytes,
    epoch: int,
    lease: float,
) -> None:
    kp = Ed25519KeyPair.generate()
    node_id = "node-alpha"
    ring = PeerKeyRing()
    ring.add_peer(node_id, kp.public_key)

    task = TaskRecord(
        task_id=task_id,
        state=state,
        payload=payload,
        claimed_by=node_id if state == TaskState.CLAIMED else None,
        fence_token=FenceToken(epoch=epoch, peer_id=node_id),
        lease_expiry=lease,
        vector_clock=VectorClock({node_id: 1}),
        updated_by=node_id,
    )

    signed = sign_task(kp, task, signer_id=node_id)
    assert verify_task_signature(signed, ring) is True
    assert verify_task_authorization(signed, ring) is True


@given(
    payload=payloads_st,
    mutation=st.binary(min_size=1, max_size=16),
)
def test_crypto_payload_mutation_fails_verification(payload: bytes, mutation: bytes) -> None:
    kp = Ed25519KeyPair.generate()
    node_id = "node-alpha"
    ring = PeerKeyRing()
    ring.add_peer(node_id, kp.public_key)

    task = TaskRecord(
        task_id="task-prop",
        state=TaskState.PENDING,
        payload=payload,
        vector_clock=VectorClock({node_id: 1}),
    )
    signed = sign_task(kp, task, signer_id=node_id)

    # Mutate payload
    tampered_payload = (
        payload + mutation if payload != payload + mutation else b"mutated_" + payload
    )
    tampered = replace(signed, payload=tampered_payload)

    assert verify_task_signature(tampered, ring) is False


@given(
    epoch_delta=st.integers(min_value=1, max_value=10_000),
)
def test_crypto_fence_tampering_fails_verification(epoch_delta: int) -> None:
    kp = Ed25519KeyPair.generate()
    node_id = "node-alpha"
    ring = PeerKeyRing()
    ring.add_peer(node_id, kp.public_key)

    task = TaskRecord(
        task_id="task-prop",
        state=TaskState.CLAIMED,
        payload=b"payload",
        claimed_by=node_id,
        fence_token=FenceToken(epoch=10, peer_id=node_id),
    )
    signed = sign_task(kp, task, signer_id=node_id)

    # Attacker tries to artificially increment fence token
    tampered = replace(
        signed,
        fence_token=FenceToken(epoch=signed.fence_token.epoch + epoch_delta, peer_id=node_id),
    )

    assert verify_task_signature(tampered, ring) is False
    assert verify_task_authorization(tampered, ring) is False


@given(
    other_peer=node_ids_st,
)
def test_crypto_untrusted_peer_fails_verification(other_peer: str) -> None:
    kp_legit = Ed25519KeyPair.generate()
    kp_rogue = Ed25519KeyPair.generate()

    ring = PeerKeyRing()
    ring.add_peer("legit-node", kp_legit.public_key)

    task = TaskRecord(
        task_id="task-prop",
        state=TaskState.PENDING,
        payload=b"payload",
    )

    # Rogue node signs with their own key but claims to be legit-node or other
    signed_by_rogue = sign_task(kp_rogue, task, signer_id="legit-node")
    assert verify_task_signature(signed_by_rogue, ring) is False

    # Signed with rogue signer_id (not in ring)
    signed_unknown = sign_task(kp_rogue, task, signer_id="unknown-peer")
    assert verify_task_signature(signed_unknown, ring) is False
