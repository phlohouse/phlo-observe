"""JSONL drain: one canonical JSON object per line, with size rotation."""

from __future__ import annotations

import os
import threading
from collections.abc import Sequence
from pathlib import Path

from observe_core.drains.base import CanonicalEvent


class JsonlDrain:
    """Append canonical events to a rotating JSONL file.

    Rotation is size-based: when the active file would exceed ``max_bytes`` it
    is renamed to ``<path>.1`` and older backups shift up, bounded by
    ``backup_count``. Writes are line-oriented UTF-8; ``fsync`` is optional for
    stronger durability of critical use cases.
    """

    name = "jsonl"
    is_remote = False

    def __init__(
        self,
        path: Path,
        *,
        max_bytes: int = 64 * 1024 * 1024,
        backup_count: int = 5,
        fsync: bool = False,
    ) -> None:
        self.path = path
        self.max_bytes = max_bytes
        self.backup_count = backup_count
        self.fsync = fsync
        path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = path.open("ab")
        self._size = path.stat().st_size if path.exists() else 0
        self._lock = threading.Lock()

    def emit_batch(self, events: Sequence[CanonicalEvent]) -> None:
        """Append each event as one JSON line."""
        with self._lock:
            for item in events:
                self._write(item.payload)

    def emit_raw(self, payloads: Sequence[bytes]) -> None:
        """Append pre-serialized payloads (spool replay)."""
        with self._lock:
            for payload in payloads:
                self._write(payload)

    def _write(self, payload: bytes) -> None:
        line = payload if payload.endswith(b"\n") else payload + b"\n"
        if self._size + len(line) > self.max_bytes:
            self._rotate()
        self._fh.write(line)
        self._size += len(line)
        if self.fsync:
            self._fh.flush()
            os.fsync(self._fh.fileno())

    def _rotate(self) -> None:
        self._fh.close()
        for i in range(self.backup_count - 1, 0, -1):
            older = self.path.with_name(f"{self.path.name}.{i}")
            newer = self.path.with_name(f"{self.path.name}.{i + 1}")
            if older.exists():
                if i + 1 > self.backup_count:
                    older.unlink(missing_ok=True)
                else:
                    older.replace(newer)
        self.path.replace(self.path.with_name(f"{self.path.name}.1"))
        self._fh = self.path.open("ab")
        self._size = 0

    def flush(self) -> None:
        """Flush buffered writes to the OS."""
        with self._lock:
            self._fh.flush()
            if self.fsync:
                os.fsync(self._fh.fileno())

    def close(self) -> None:
        """Flush and close the file handle."""
        with self._lock:
            try:
                self._fh.flush()
            finally:
                self._fh.close()
