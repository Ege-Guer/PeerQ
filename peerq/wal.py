"""
Durable Write-Ahead Log (WAL) for peerq.

Provides append-only persistence with:
- Binary frame headers with CRC32 data integrity verification.
- Graceful torn-write recovery at crash boundaries.
- Replay mechanism restoring TaskRecord lattices and VectorClocks.
- Atomic checkpointing and log compaction.
"""

from __future__ import annotations

import json
import os
import struct
import zlib
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from peerq.consensus import TaskRecord, VectorClock
from peerq.security import (
    MAX_FRAME_SIZE,
    MessageDecodeError,
    reject_json_constants,
    validate_json_tree,
)

# Magic 4 bytes + 1 byte version (1) + 3 reserved padding bytes = 8 bytes
WAL_MAGIC: bytes = b"PQWL\x01\x00\x00\x00"
WAL_HEADER_LEN: int = len(WAL_MAGIC)

# Frame layout: [payload_len (uint32)][crc32 (uint32)][record_type (uint8)] = 9 bytes
FRAME_HEADER_STRUCT: struct.Struct = struct.Struct(">IIB")
FRAME_HEADER_LEN: int = FRAME_HEADER_STRUCT.size

RECORD_TASK: int = 1
RECORD_CLOCK: int = 2
RECORD_CHECKPOINT: int = 3


class WalError(Exception):
    """Base error for WAL operations."""


class WalCorruptError(WalError):
    """Raised when log data is corrupted or invalid."""


class WalRecord:
    """An individual record read from a write-ahead log."""

    def __init__(self, offset: int, record_type: int, payload: dict[str, Any]) -> None:
        self.offset = offset
        self.record_type = record_type
        self.payload = payload

    def __repr__(self) -> str:
        return f"WalRecord(offset={self.offset}, type={self.record_type})"


class WriteAheadLog:
    """
    Append-only write-ahead log supporting sync writes and recovery replay.

    Thread-safe within single-threaded asyncio event loops.
    """

    def __init__(
        self,
        path: Path | str,
        *,
        sync_on_write: bool = False,
        max_record_bytes: int = MAX_FRAME_SIZE,
    ) -> None:
        self.path: Path = Path(path).resolve()
        self.sync_on_write: bool = sync_on_write
        self.max_record_bytes = max_record_bytes
        if max_record_bytes <= 0:
            raise ValueError("max_record_bytes must be positive")
        self._file: Any | None = None
        self._open()

    def _open(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if not self.path.exists() or self.path.stat().st_size == 0:
            with open(self.path, "wb") as f:
                f.write(WAL_MAGIC)
                f.flush()
                if self.sync_on_write:
                    os.fsync(f.fileno())

        self._file = open(self.path, "a+b")  # noqa: SIM115
        self._verify_header()

    def _verify_header(self) -> None:
        if self._file is None:
            raise WalError("WAL file is not open")
        self._file.seek(0)
        hdr = self._file.read(WAL_HEADER_LEN)
        if len(hdr) < WAL_HEADER_LEN or hdr[:4] != b"PQWL":
            raise WalCorruptError(f"Invalid WAL magic header in {self.path}")
        version = hdr[4]
        if version != 1:
            raise WalCorruptError(f"Unsupported WAL version {version} in {self.path}")
        self._file.seek(0, os.SEEK_END)

    def append(
        self,
        record_type: int,
        payload: dict[str, Any],
        *,
        sync: bool | None = None,
    ) -> int:
        """
        Append a record to the WAL.

        Returns the byte offset of the record start.
        """
        if self._file is None:
            raise WalError("WAL file is closed")
        if record_type not in {RECORD_TASK, RECORD_CLOCK, RECORD_CHECKPOINT}:
            raise WalError(f"Unknown WAL record type: {record_type}")

        try:
            validate_json_tree(
                payload,
                max_depth=16,
                max_items=10_000,
                max_string_length=self.max_record_bytes,
            )
            payload_bytes = json.dumps(
                payload,
                separators=(",", ":"),
                sort_keys=True,
                allow_nan=False,
            ).encode("utf-8")
        except (MessageDecodeError, TypeError, ValueError) as exc:
            raise WalError(f"WAL payload is not safely serializable: {exc}") from exc
        payload_len = len(payload_bytes)
        if payload_len > self.max_record_bytes:
            raise WalError("WAL record exceeds the configured size limit")

        # Compute CRC32 over record_type (1 byte) + payload_bytes
        crc_data = bytes([record_type]) + payload_bytes
        crc = zlib.crc32(crc_data) & 0xFFFFFFFF

        header = FRAME_HEADER_STRUCT.pack(payload_len, crc, record_type)

        offset = self._file.tell()
        self._file.write(header)
        self._file.write(payload_bytes)
        self._file.flush()

        do_sync = self.sync_on_write if sync is None else sync
        if do_sync:
            os.fsync(self._file.fileno())

        return int(offset)

    def append_task(self, record: TaskRecord, *, sync: bool | None = None) -> int:
        """Convenience method to log a TaskRecord update."""
        return self.append(RECORD_TASK, record.to_dict(), sync=sync)

    def append_clock(self, clock: VectorClock, *, sync: bool | None = None) -> int:
        """Convenience method to log a VectorClock update."""
        return self.append(RECORD_CLOCK, clock.to_dict(), sync=sync)

    def replay(self, *, strict: bool = False) -> Iterator[WalRecord]:
        """
        Replay all records from the log sequentially.

        If `strict=False`, torn writes at the tail of the log are gracefully tolerated
        (discarded), representing incomplete in-flight writes during an unclean crash.
        """
        if not self.path.exists():
            return

        with open(self.path, "rb") as f:
            hdr = f.read(WAL_HEADER_LEN)
            if len(hdr) < WAL_HEADER_LEN:
                if strict:
                    raise WalCorruptError("Truncated WAL header")
                return

            if hdr[:4] != b"PQWL" or hdr[4] != 1:
                raise WalCorruptError("Invalid WAL header during replay")

            offset = WAL_HEADER_LEN
            while True:
                f.seek(offset)
                hdr_bytes = f.read(FRAME_HEADER_LEN)
                if not hdr_bytes:
                    break  # Clean EOF

                if len(hdr_bytes) < FRAME_HEADER_LEN:
                    if strict:
                        msg = f"Torn frame header at offset {offset}: got {len(hdr_bytes)} bytes"
                        raise WalCorruptError(msg)
                    break  # Torn write at tail; safely stop

                payload_len, expected_crc, record_type = FRAME_HEADER_STRUCT.unpack(hdr_bytes)
                if record_type not in {RECORD_TASK, RECORD_CLOCK, RECORD_CHECKPOINT}:
                    raise WalCorruptError(
                        f"Unknown WAL record type {record_type} at offset {offset}"
                    )
                if payload_len > self.max_record_bytes:
                    raise WalCorruptError(
                        f"WAL payload exceeds the configured limit at offset {offset}"
                    )
                payload_bytes = f.read(payload_len)

                if len(payload_bytes) < payload_len:
                    if strict:
                        msg = f"Torn payload at offset {offset}: expected {payload_len} bytes"
                        raise WalCorruptError(msg)
                    break  # Torn payload write at tail; safely stop

                crc_data = bytes([record_type]) + payload_bytes
                actual_crc = zlib.crc32(crc_data) & 0xFFFFFFFF
                if actual_crc != expected_crc:
                    msg = (
                        f"Checksum mismatch at offset {offset}: "
                        f"expected {expected_crc:#010x}, got {actual_crc:#010x}"
                    )
                    raise WalCorruptError(msg)

                try:
                    payload = json.loads(
                        payload_bytes.decode("utf-8"), parse_constant=reject_json_constants
                    )
                    validate_json_tree(
                        payload,
                        max_depth=16,
                        max_items=10_000,
                        max_string_length=self.max_record_bytes,
                    )
                except Exception as exc:
                    raise WalCorruptError(
                        f"Corrupt JSON payload at offset {offset}: {exc}"
                    ) from exc
                if not isinstance(payload, dict):
                    raise WalCorruptError(f"WAL payload is not an object at offset {offset}")

                yield WalRecord(offset=offset, record_type=record_type, payload=payload)
                offset += FRAME_HEADER_LEN + payload_len

    def checkpoint(
        self,
        tasks: dict[str, TaskRecord],
        vector_clock: VectorClock,
    ) -> None:
        """
        Compact the log by writing a single atomic checkpoint and truncating older history.
        """
        tmp_path = self.path.with_suffix(".tmp")
        payload = {
            "tasks": {k: v.to_dict() for k, v in tasks.items()},
            "vector_clock": vector_clock.to_dict(),
        }
        try:
            validate_json_tree(
                payload,
                max_depth=16,
                max_items=10_000,
                max_string_length=self.max_record_bytes,
            )
            payload_bytes = json.dumps(
                payload,
                separators=(",", ":"),
                sort_keys=True,
                allow_nan=False,
            ).encode("utf-8")
        except (MessageDecodeError, TypeError, ValueError) as exc:
            raise WalError(f"WAL checkpoint is not safely serializable: {exc}") from exc
        payload_len = len(payload_bytes)
        if payload_len > self.max_record_bytes:
            raise WalError("WAL checkpoint exceeds the configured size limit")

        self.close()
        with open(tmp_path, "wb") as f:
            f.write(WAL_MAGIC)

            # Write checkpoint record
            crc_data = bytes([RECORD_CHECKPOINT]) + payload_bytes
            crc = zlib.crc32(crc_data) & 0xFFFFFFFF

            header = FRAME_HEADER_STRUCT.pack(payload_len, crc, RECORD_CHECKPOINT)
            f.write(header)
            f.write(payload_bytes)
            f.flush()
            if self.sync_on_write:
                os.fsync(f.fileno())

        # Atomic replacement
        tmp_path.replace(self.path)
        # Directory metadata is part of the atomic replacement contract.  Some
        # platforms do not permit opening a directory for fsync; in that case
        # the file itself is still durably flushed when sync_on_write is true.
        if self.sync_on_write:
            try:
                dir_fd = os.open(str(self.path.parent), os.O_RDONLY)
                try:
                    os.fsync(dir_fd)
                finally:
                    os.close(dir_fd)
            except OSError:
                pass
        self._open()

    def sync(self) -> None:
        """Force write buffers to durable disk storage."""
        if self._file is not None:
            self._file.flush()
            os.fsync(self._file.fileno())

    def close(self) -> None:
        """Close the underlying log file."""
        if self._file is not None:
            self._file.flush()
            self._file.close()
            self._file = None

    def __enter__(self) -> WriteAheadLog:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.close()
