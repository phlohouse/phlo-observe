"""In-memory drain for tests and embedding."""

from __future__ import annotations

import threading
from collections.abc import Sequence

from observe_core.drains.base import CanonicalEvent


class MemoryDrain:
    """Stores canonical events in a list. Intended for tests and notebooks."""

    name = "memory"
    is_remote = False

    def __init__(self) -> None:
        self.events: list[CanonicalEvent] = []
        self.raw_payloads: list[bytes] = []
        self.flushes = 0
        self.closed = False
        self._lock = threading.Lock()

    def emit_batch(self, events: Sequence[CanonicalEvent]) -> None:
        """Append the batch to the in-memory store."""
        with self._lock:
            self.events.extend(events)

    def emit_raw(self, payloads: Sequence[bytes]) -> None:
        """Append raw payloads (spool replay)."""
        with self._lock:
            self.raw_payloads.extend(payloads)

    def flush(self) -> None:
        """Record a flush."""
        with self._lock:
            self.flushes += 1

    def close(self) -> None:
        """Mark closed."""
        self.closed = True

    def clear(self) -> None:
        """Drop all captured events."""
        with self._lock:
            self.events.clear()
            self.raw_payloads.clear()
