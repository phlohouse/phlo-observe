"""Globally unique, sortable event identifiers.

V1 uses UUIDv7 (RFC 9562): a 48-bit Unix-millisecond timestamp followed by a
monotonic per-process counter and random bits. IDs generated within the same
process are strictly monotonic, which makes them safe as a cursor tie-breaker.
"""

from __future__ import annotations

import secrets
import threading
import time
import uuid

_last_ms = -1
_counter = 0
_lock = threading.Lock()


def uuid7() -> uuid.UUID:
    """Generate a process-monotonic UUIDv7.

    The 12-bit ``rand_a`` field doubles as a same-millisecond sequence counter,
    so IDs sort strictly by creation order within one process.
    """
    global _last_ms, _counter  # noqa: PLW0603 - process-monotonic state by design
    rand_b = secrets.randbits(62)
    with _lock:
        ts_ms = time.time_ns() // 1_000_000
        if ts_ms <= _last_ms:
            _counter = (_counter + 1) & 0xFFF
            # Keep the timestamp clamped at the last claimed millisecond so IDs
            # never regress; on counter wrap, claim the next millisecond.
            ts_ms = _last_ms if _counter else _last_ms + 1
        else:
            _counter = secrets.randbits(12)
        _last_ms = ts_ms
        value = (
            ((ts_ms & 0xFFFFFFFFFFFF) << 80)
            | (0x7 << 76)
            | (_counter << 64)
            | (0b10 << 62)
            | rand_b
        )
        return uuid.UUID(int=value)


def new_event_id() -> str:
    """Return a new canonical event ID string."""
    return str(uuid7())
