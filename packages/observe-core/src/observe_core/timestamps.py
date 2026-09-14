"""Canonical timestamp handling.

All emitted timestamps are UTC RFC3339 with a ``Z`` suffix and millisecond
precision, matching the examples in the V1 specification.
"""

from __future__ import annotations

import datetime as dt
import time

UTC = dt.UTC


def utcnow() -> dt.datetime:
    """Return the current aware UTC time."""
    return dt.datetime.now(UTC)


def ensure_utc(value: dt.datetime) -> dt.datetime:
    """Return an aware UTC datetime. Naive values are assumed to be UTC."""
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def format_rfc3339(value: dt.datetime) -> str:
    """Format a datetime as ``2026-09-14T21:15:10.120Z``."""
    return ensure_utc(value).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def parse_rfc3339(value: str) -> dt.datetime:
    """Parse an RFC3339 timestamp into an aware UTC datetime."""
    text = value.strip()
    if text.endswith(("Z", "z")):
        text = text[:-1] + "+00:00"
    return ensure_utc(dt.datetime.fromisoformat(text))


def monotonic_ms() -> float:
    """Monotonic clock in milliseconds for duration measurement."""
    return time.monotonic() * 1000.0
