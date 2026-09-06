"""
Cryptographic task signature and integrity verification for peerq.

Provides:
- Ed25519 keypair generation, public key export/import
- Canonical TaskRecord deterministic serialization for digital signatures
- PeerKeyRing for authorized peer identity management
- Byzantine resistance: prevents spoofing of task states, leases, and fencing tokens
"""

from __future__ import annotations

import json
from dataclasses import dataclass, replace
from typing import TYPE_CHECKING, Any

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ed25519

if TYPE_CHECKING:
    from peerq.consensus import TaskRecord


class CryptoError(Exception):
    """Base exception for peerq cryptographic operations."""


class InvalidKeyError(CryptoError):
    """Raised when an Ed25519 key cannot be parsed or loaded."""


@dataclass(frozen=True)
class Ed25519PublicKeyWrapper:
    """Immutable wrapper around an Ed25519 public key."""

    _key: ed25519.Ed25519PublicKey

    @classmethod
    def from_bytes(cls, data: bytes) -> Ed25519PublicKeyWrapper:
        if len(data) != 32:
            raise InvalidKeyError(f"Ed25519 public key must be 32 bytes, got {len(data)}")
        try:
            key = ed25519.Ed25519PublicKey.from_public_bytes(data)
            return cls(key)
        except Exception as e:
            raise InvalidKeyError(f"Failed to load public key: {e}") from e

    @classmethod
    def from_hex(cls, hex_str: str) -> Ed25519PublicKeyWrapper:
        try:
            raw = bytes.fromhex(hex_str)
        except ValueError as e:
            raise InvalidKeyError(f"Invalid hex string for public key: {e}") from e
        return cls.from_bytes(raw)

    def to_bytes(self) -> bytes:
        return self._key.public_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PublicFormat.Raw,
        )

    def to_hex(self) -> str:
        return self.to_bytes().hex()

    def verify(self, signature: bytes, data: bytes) -> bool:
        """Verify an Ed25519 signature over data. Returns True if valid, False otherwise."""
        if len(signature) != 64:
            return False
        try:
            self._key.verify(signature, data)
            return True
        except InvalidSignature:
            return False


@dataclass(frozen=True)
class Ed25519KeyPair:
    """Immutable Ed25519 key pair with signing capability."""

    _private_key: ed25519.Ed25519PrivateKey

    @classmethod
    def generate(cls) -> Ed25519KeyPair:
        """Generate a new random Ed25519 key pair."""
        return cls(ed25519.Ed25519PrivateKey.generate())

    @classmethod
    def from_private_bytes(cls, data: bytes) -> Ed25519KeyPair:
        """Load private key from 32-byte raw seed."""
        if len(data) != 32:
            raise InvalidKeyError(f"Ed25519 private key seed must be 32 bytes, got {len(data)}")
        try:
            priv = ed25519.Ed25519PrivateKey.from_private_bytes(data)
            return cls(priv)
        except Exception as e:
            raise InvalidKeyError(f"Failed to load private key: {e}") from e

    def to_private_bytes(self) -> bytes:
        """Export raw 32-byte private key seed."""
        return self._private_key.private_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PrivateFormat.Raw,
            encryption_algorithm=serialization.NoEncryption(),
        )

    @property
    def public_key(self) -> Ed25519PublicKeyWrapper:
        """Return corresponding public key wrapper."""
        return Ed25519PublicKeyWrapper(self._private_key.public_key())

    def sign(self, data: bytes) -> bytes:
        """Produce 64-byte Ed25519 signature over data."""
        return self._private_key.sign(data)

    def verify(self, signature: bytes, data: bytes) -> bool:
        """Verify signature using this keypair's public key."""
        return self.public_key.verify(signature, data)


class PeerKeyRing:
    """
    Registry of authorized peers and their Ed25519 public keys.
    """

    def __init__(self) -> None:
        self._keys: dict[str, Ed25519PublicKeyWrapper] = {}

    def add_peer(self, peer_id: str, public_key: Ed25519PublicKeyWrapper | bytes | str) -> None:
        """Register an authorized peer ID and its public key."""
        if isinstance(public_key, Ed25519PublicKeyWrapper):
            self._keys[peer_id] = public_key
        elif isinstance(public_key, bytes):
            self._keys[peer_id] = Ed25519PublicKeyWrapper.from_bytes(public_key)
        elif isinstance(public_key, str):
            self._keys[peer_id] = Ed25519PublicKeyWrapper.from_hex(public_key)
        else:
            raise TypeError(f"Unsupported public key type: {type(public_key)}")

    def remove_peer(self, peer_id: str) -> None:
        """Revoke a peer's authorized key."""
        self._keys.pop(peer_id, None)

    def has_peer(self, peer_id: str) -> bool:
        return peer_id in self._keys

    def get_peer_key(self, peer_id: str) -> Ed25519PublicKeyWrapper | None:
        return self._keys.get(peer_id)

    def verify(self, peer_id: str, signature: bytes, data: bytes) -> bool:
        """Verify signature for a given peer ID."""
        key = self._keys.get(peer_id)
        if key is None:
            return False
        return key.verify(signature, data)


def canonical_task_bytes(task: TaskRecord) -> bytes:
    """
    Deterministically serialize task state attributes for digital signing.
    Excludes the signature field itself.
    """
    doc: dict[str, Any] = {
        "claimed_by": task.claimed_by,
        "error": task.error,
        "fence_epoch": task.fence_token.epoch,
        "fence_peer": task.fence_token.peer_id,
        "lease_expiry": f"{task.lease_expiry:.6f}",
        "payload": task.payload.hex(),
        "result": task.result.hex() if task.result is not None else None,
        "signer_id": task.signer_id,
        "state": task.state.value,
        "task_id": task.task_id,
        "updated_by": task.updated_by,
        "vector_clock": sorted(task.vector_clock.clock.items()),
    }
    return json.dumps(doc, sort_keys=True, separators=(",", ":")).encode("utf-8")


def sign_task(key_pair: Ed25519KeyPair, task: TaskRecord, signer_id: str) -> TaskRecord:
    """
    Sign a TaskRecord with an Ed25519 key pair, setting signer_id and signature.
    """
    staged = replace(task, signer_id=signer_id, signature=None)
    data = canonical_task_bytes(staged)
    sig = key_pair.sign(data)
    return replace(staged, signature=sig)


def verify_task_signature(task: TaskRecord, key_ring: PeerKeyRing) -> bool:
    """
    Verify whether a TaskRecord carries a valid Ed25519 signature from an authorized peer.
    """
    if task.signature is None or task.signer_id is None:
        return False
    staged = replace(task, signature=None)
    data = canonical_task_bytes(staged)
    return key_ring.verify(task.signer_id, task.signature, data)


def verify_task_authorization(task: TaskRecord, key_ring: PeerKeyRing) -> bool:
    """
    Full Byzantine verification of a TaskRecord:
    1. Signature integrity matches canonical content.
    2. Signer is authorized in key_ring.
    3. Fencing token ownership: If claimed_by is set, signer_id MUST match claimed_by
       and fence_token.peer_id MUST match claimed_by (prevents spoofing another peer's lease).
    """
    if not verify_task_signature(task, key_ring):
        return False

    if task.claimed_by is not None:
        if task.signer_id != task.claimed_by:
            return False
        if task.fence_token.peer_id != task.claimed_by:
            return False

    return True
