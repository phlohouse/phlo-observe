"""Internal telemetry health counters.

Exposed via :func:`observe_core.stats` / :func:`observe_core.get_stats` so
applications and tests can observe the observer-path itself: drops, spool
writes, drain failures, truncation.
"""

from __future__ import annotations

import threading
from typing import Any


class TelemetryStats:
    """Thread-safe counters for the emission pipeline."""

    _FIELDS = (
        "enqueued",
        "emitted_events",
        "emitted_batches",
        "dropped_debug",
        "dropped_telemetry",
        "dropped_sampled",
        "dropped_oversized",
        "dropped_disabled",
        "dropped_closed",
        "truncated_events",
        "spooled_events",
        "spool_dropped_oldest",
        "spool_errors",
        "spool_quarantined",
        "spool_replayed_events",
        "drain_errors",
        "worker_errors",
        "flushes",
    )

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counts = dict.fromkeys(self._FIELDS, 0)

    def incr(self, field: str, amount: int = 1) -> None:
        """Increment a counter (unknown names are ignored defensively)."""
        with self._lock:
            if field in self._counts:
                self._counts[field] += amount

    def snapshot(self) -> dict[str, Any]:
        """Return a consistent copy of all counters plus queue depth."""
        with self._lock:
            return dict(self._counts)
