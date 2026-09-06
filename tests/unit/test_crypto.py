"""
Unit tests for peerq.crypto (Ed25519 digital signatures and integrity verification).
"""

import pytest

from peerq.consensus import FenceToken, TaskRecord, TaskState, VectorClock, merge_records
from peerq.crypto import (
    Ed25519KeyPair,
    Ed25519PublicKeyWrapper,
    InvalidKeyError,
    PeerKeyRing,
    sign_task,
    verify_task_authorization,
    verify_task_signature,
)


def test_keypair_generation_and_export() -> None:
    kp = Ed25519KeyPair.generate()
    priv_bytes = kp.to_private_bytes()
    assert len(priv_bytes) == 32

    pub = kp.public_key
    pub_bytes = pub.to_bytes()
    assert len(pub_bytes) == 32
    assert pub.to_hex() == pub_bytes.hex()

    # Reconstitute from bytes
    kp2 = Ed25519KeyPair.from_private_bytes(priv_bytes)
    assert kp2.public_key.to_bytes() == pub_bytes

    pub2 = Ed25519PublicKeyWrapper.from_bytes(pub_bytes)
    assert pub2.to_bytes() == pub_bytes

    pub3 = Ed25519PublicKeyWrapper.from_hex(pub.to_hex())
    assert pub3.to_bytes() == pub_bytes


def test_invalid_key_length() -> None:
    with pytest.raises(InvalidKeyError):
        Ed25519KeyPair.from_private_bytes(b"short")

    with pytest.raises(InvalidKeyError):
        Ed25519PublicKeyWrapper.from_bytes(b"short")

    with pytest.raises(InvalidKeyError):
        Ed25519PublicKeyWrapper.from_hex("invalid-hex-value")


def test_raw_data_signing_and_verification() -> None:
    kp1 = Ed25519KeyPair.generate()
    kp2 = Ed25519KeyPair.generate()

    msg = b"PeerQ distributed transaction"
    sig = kp1.sign(msg)
    assert len(sig) == 64

    # Valid verification
    assert kp1.verify(sig, msg) is True
    assert kp1.public_key.verify(sig, msg) is True

    # Tampered message
    assert kp1.verify(sig, b"Tampered transaction") is False

    # Wrong key
    assert kp2.verify(sig, msg) is False

    # Invalid signature length
    assert kp1.public_key.verify(b"bad_sig", msg) is False


def test_peer_keyring_registration_and_revocation() -> None:
    ring = PeerKeyRing()
    kp = Ed25519KeyPair.generate()

    assert ring.has_peer("node-1") is False

    # Add via wrapper
    ring.add_peer("node-1", kp.public_key)
    assert ring.has_peer("node-1") is True
    assert ring.get_peer_key("node-1") == kp.public_key

    # Verification via ring
    msg = b"hello ring"
    sig = kp.sign(msg)
    assert ring.verify("node-1", sig, msg) is True
    assert ring.verify("unknown-node", sig, msg) is False

    # Add via bytes and hex
    kp2 = Ed25519KeyPair.generate()
    ring.add_peer("node-2", kp2.public_key.to_bytes())
    assert ring.verify("node-2", kp2.sign(msg), msg) is True

    kp3 = Ed25519KeyPair.generate()
    ring.add_peer("node-3", kp3.public_key.to_hex())
    assert ring.verify("node-3", kp3.sign(msg), msg) is True

    with pytest.raises(TypeError):
        ring.add_peer("node-invalid", 12345)  # type: ignore[arg-type]

    # Revoke peer
    ring.remove_peer("node-1")
    assert ring.has_peer("node-1") is False
    assert ring.verify("node-1", sig, msg) is False


def test_task_signing_and_verification() -> None:
    ring = PeerKeyRing()
    kp1 = Ed25519KeyPair.generate()
    ring.add_peer("node-1", kp1.public_key)

    task = TaskRecord(
        task_id="task-42",
        state=TaskState.PENDING,
        payload=b"do_work",
        vector_clock=VectorClock({"node-1": 1}),
        updated_by="node-1",
    )

    signed = sign_task(kp1, task, signer_id="node-1")
    assert signed.signature is not None
    assert signed.signer_id == "node-1"

    # Should verify correctly
    assert verify_task_signature(signed, ring) is True
    assert verify_task_authorization(signed, ring) is True

    # Unsigned task should fail
    assert verify_task_signature(task, ring) is False


def test_byzantine_spoofing_prevention() -> None:
    ring = PeerKeyRing()
    kp_legit = Ed25519KeyPair.generate()
    kp_attacker = Ed25519KeyPair.generate()

    ring.add_peer("node-legit", kp_legit.public_key)
    ring.add_peer("node-attacker", kp_attacker.public_key)

    # 1. Attacker signs a task pretending to be node-legit
    task = TaskRecord(
        task_id="task-100",
        state=TaskState.CLAIMED,
        payload=b"sensitive_data",
        claimed_by="node-legit",
        fence_token=FenceToken(epoch=10, peer_id="node-legit"),
    )

    # If signed by attacker's key but signer_id="node-legit", signature fails because
    # key in ring for node-legit does not match attacker's private key
    forged_1 = sign_task(kp_attacker, task, signer_id="node-legit")
    assert verify_task_signature(forged_1, ring) is False
    assert verify_task_authorization(forged_1, ring) is False

    # 2. Attacker signs with their own key (signer_id="node-attacker") but tries
    # to spoof lease ownership of node-legit (claimed_by="node-legit")
    forged_2 = sign_task(kp_attacker, task, signer_id="node-attacker")
    # Signature is valid cryptographically for attacker
    assert verify_task_signature(forged_2, ring) is True
    # But authorization MUST fail because claimed_by != signer_id
    assert verify_task_authorization(forged_2, ring) is False

    # 3. Attacker tries to forge fence token peer_id
    tampered_fence = TaskRecord(
        task_id="task-100",
        state=TaskState.CLAIMED,
        payload=b"sensitive_data",
        claimed_by="node-attacker",
        fence_token=FenceToken(epoch=10, peer_id="node-legit"),  # Spoofed fence peer
    )
    forged_3 = sign_task(kp_attacker, tampered_fence, signer_id="node-attacker")
    assert verify_task_authorization(forged_3, ring) is False


def test_crdt_merge_with_byzantine_defense() -> None:
    ring = PeerKeyRing()
    kp1 = Ed25519KeyPair.generate()
    ring.add_peer("node-1", kp1.public_key)

    legit_task = TaskRecord(
        task_id="task-merge",
        state=TaskState.CLAIMED,
        payload=b"task_body",
        claimed_by="node-1",
        fence_token=FenceToken(epoch=1, peer_id="node-1"),
        vector_clock=VectorClock({"node-1": 1}),
    )
    legit_signed = sign_task(kp1, legit_task, signer_id="node-1")

    # Attacker crafts a fake DONE record with higher fence token and unverified signature
    fake_attacker_task = TaskRecord(
        task_id="task-merge",
        state=TaskState.DONE,
        payload=b"task_body",
        claimed_by="node-1",
        fence_token=FenceToken(epoch=999, peer_id="node-1"),
        signature=b"\x00" * 64,
        signer_id="untrusted-node",
    )

    # In standard merge with keyring, the forged record is rejected and authentic record wins!
    merged = merge_records(legit_signed, fake_attacker_task, keyring=ring)
    assert merged.state == TaskState.CLAIMED
    assert merged.fence_token.epoch == 1

    # Commutative: reverse order must produce identical result
    merged_rev = merge_records(fake_attacker_task, legit_signed, keyring=ring)
    assert merged_rev.state == TaskState.CLAIMED
    assert merged_rev.fence_token.epoch == 1

    # If neither record is authorized, raise ValueError
    with pytest.raises(ValueError, match="Neither record is cryptographically authorized"):
        merge_records(fake_attacker_task, fake_attacker_task, keyring=ring)
