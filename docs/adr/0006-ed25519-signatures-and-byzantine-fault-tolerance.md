# ADR 0006: Ed25519 Digital Signatures and Byzantine Fault Resistance

## Status
Accepted — runtime enforcement is protocol-v2 secure by default.

## Context
In a decentralized, leaderless gossip mesh, all participating nodes propagate state updates via epidemic anti-entropy. In an untrusted, heterogeneous, or multi-tenant network environment, any faulty, compromised, or adversarial peer could:
1. **Spoof Task Leases**: Announce a lease claim for a task while masquerading as another worker node.
2. **Fabricate Fencing Tokens**: Artificially increment fencing token epochs to revoke legitimate leases held by active peers.
3. **Poison Task Payloads or Terminal States**: Overwrite task results or transition tasks to `DONE` or `FAILED` with forged computation outputs.

To prevent Byzantine nodes from disrupting mesh operations, the library must provide cryptographic non-repudiation, tamper-evident state replication, and strict identity-bound authorization.

## Decision
We introduce cryptographic task integrity in `peerq.crypto` using Edwards-curve Digital Signature Algorithm (Ed25519, RFC 8032) and bind cryptographic identities directly to CRDT lattice conflict resolution:

1. **Ed25519 Asymmetric Cryptography**:
   - High-performance, constant-time Edwards-curve signatures (64 bytes signature, 32 bytes public key).
   - `Ed25519KeyPair` provides signing capability from 32-byte raw seeds.
   - `Ed25519PublicKeyWrapper` and `PeerKeyRing` maintain the cluster's trusted peer public key directory.

2. **Deterministic Canonical Serialization**:
   - `canonical_task_bytes(task)` deterministically serializes all operational attributes (`task_id`, `state`, `payload`, `result`, `error`, `claimed_by`, `fence_token`, `lease_expiry`, `vector_clock`, `updated_by`, `signer_id`) with sorted keys and normalized delimiters.
   - Excludes the `signature` field itself, preventing circularity.

3. **Byzantine Fencing Authorization**:
   - `verify_task_authorization(task, keyring)` enforces strict lease invariants:
     - The signature MUST be valid against the signer's registered public key.
     - If `claimed_by` is set, `task.signer_id` MUST equal `claimed_by`.
     - `task.fence_token.peer_id` MUST equal `claimed_by`.
   - An attacker cannot claim a lease under another node's identity or increment another peer's fencing token without possessing that peer's private key.

4. **Runtime envelope and CRDT enforcement**:
   - TCP handshakes and all runtime gossip/control messages carry an Ed25519
     envelope with a protocol version, timestamp, nonce, and bounded replay
     check. Unsigned or unknown-peer traffic is rejected before it reaches the
     node receive loop.
   - `merge_records(r1, r2, keyring=keyring)` incorporates authorization into the semilattice:
   - `merge_records(r1, r2, keyring=keyring)` incorporates authorization into the semilattice:
     - An unauthorized or tampered record is rejected immediately, even if it presents a higher fencing token epoch or terminal `DONE` state.
     - The legitimate authorized record is preserved.
     - When both records are authentically signed, standard join-semilattice resolution applies deterministically.

## Rejected Alternatives
- **RSA / ECDSA (secp256r1)**:
  Rejected. RSA produces large signatures (256-512 bytes) and slow key generation. ECDSA is vulnerable to nonce leakage (leading to private key recovery) if RNG lacks entropy. Ed25519 is deterministic (RFC 8032), immune to nonce reuse, and significantly faster.
- **HMAC Shared Secret**:
  Rejected. Symmetric HMAC requires all peers to share identical secret keys. If any single peer is compromised, the entire mesh's integrity is compromised, and non-repudiation is impossible.

## Consequences
- **Positive**: Byzantine nodes cannot spoof leases, forge fencing tokens, or alter task state without private keys.
- **Positive**: Seamless integration with CRDT join-semilattice merge.
- **Positive**: Fast constant-time cryptographic verification with minimal CPU overhead.
- **Trade-off**: Requires `cryptography` dependency for RFC 8032 curve operations.
- **Trade-off**: Public-key bootstrap, rotation, and revocation are explicit
  operational inputs; the protocol does not use trust-on-first-use.
- **Trade-off**: Protocol-v1 unsigned traffic is available only through an
  explicit development/test configuration and is not a production fallback.
