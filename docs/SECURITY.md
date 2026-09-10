# PeerQ Security Model

## Scope and status

PeerQ's normal runtime mode is authenticated and bounded. The wire protocol is
version 2. TCP gossip, UDP discovery, task records, and the HTTP status server
must be treated as untrusted-input boundaries. This document describes the
implemented controls; it is not a claim of immunity against a compromised
host or a stolen private key.

## Threat model

The protocol assumes that an attacker may:

- connect to the TCP listener and send arbitrary frames;
- send arbitrary UDP datagrams, including forged, replayed, or oversized beacons;
- spoof a claimed peer identifier, alter task state, or replay a valid message;
- open many connections, fill queues, send malformed JSON, or hold a read/write
  operation open;
- reach the HTTP dashboard when an operator explicitly exposes it remotely.

The trust boundary is the operator-provisioned `PeerKeyRing`. A public key in a
discovery packet is informational; it is never accepted as authorization by
itself. Key distribution, rotation, revocation, and host compromise remain
operational responsibilities.

The Ed25519 implementation uses `cryptography>=50.0.1`, the current patched
dependency floor used by the release checks. CI also runs `pip-audit`; a clean
audit must be evaluated in the project dependency environment, not against
unrelated packages installed by a development workstation.

## Runtime protocol

- Ed25519 signatures are required by default for TCP handshakes, gossip,
  heartbeats, credits, and discovery beacons.
- A message is accepted only when its protocol version, sender ID, signature,
  timestamp, nonce, and configured peer authorization are valid.
- Nonces are kept in a bounded replay cache and timestamps must fall inside the
  configured freshness window. Replaying the same valid envelope is rejected.
- Task records are separately signed. A claimed/running/terminal record must be
  authorized by the signer and its signer must match the lease holder.
- Protocol version 1 or unsigned legacy traffic is not silently upgraded. The
  only compatibility escape hatch is an explicit `SecurityConfig(enabled=False)`
  or the CLI's `--insecure-dev` option for isolated development.

## Discovery

Discovery uses signed protocol-v2 envelopes over the existing UDP transport.
Cluster IDs, bounded host/port fields, freshness, replay protection, and an
authorized keyring entry are all checked before a peer is added to a node or
TCP address directory. An unknown beacon is ignored; there is no automatic
trust-on-first-use behavior.

## Resource and parser bounds

The defaults are intentionally finite: TCP frames, UDP datagrams, HTTP headers
and bodies, task payloads/results, gossip batch size, JSON nesting/items,
connection counts, inbox queues, TCP timeouts, and UDP source rate are bounded.
Malformed input is rejected per connection/datagram and is not allowed to
terminate the node process. Limits can be tightened for a deployment through
`SecurityConfig`; raising them increases the resource budget and must be
reviewed as an operational change.

## HTTP dashboard

The dashboard binds to loopback by default. A non-loopback bind requires an
explicit bearer token of at least 32 characters. Responses include no-store,
MIME-sniffing, and restrictive content-security headers. The dashboard is
read-only, but it still exposes operational state and should be placed behind
TLS/network access control when used remotely; PeerQ does not implement TLS.

## WAL durability and legacy data

WAL frames have a bounded payload, a record-type allowlist, CRC32 integrity
checks, strict JSON handling, and torn-tail recovery. `sync_on_write=True`
adds an `fsync` barrier to each append and syncs directory metadata after an
atomic checkpoint replacement. The default `sync_on_write=False` is buffered
and therefore does not promise survival of an OS or storage failure before a
flush; applications needing that guarantee must opt in.

Secure nodes fail closed during recovery: unsigned or unauthorized historical
task records are skipped rather than promoted into authenticated live state.
The WAL file is not rewritten or deleted by this behavior.

## Verification

Security regressions live in `tests/security/` and cover signature tampering,
unknown/unauthorized peers, replay and freshness, signed discovery, parser
fuzzing, UDP/TCP/HTTP limits, WAL limits, and legacy-record rejection. The
full project test suite, coverage, Ruff, mypy, wheel build, and Docker Compose
configuration are release gates.

## Remaining risks

PeerQ does not provide certificate authorities, automatic key rotation, TLS,
encrypted task payloads, or protection against a peer whose private key or
host is compromised. Operators must provision the same trusted public keys to
all participating nodes, rotate/revoke them out of band, and keep the HTTP
dashboard on loopback or behind an authenticated, encrypted network path.
