"""Bounded tail sampling (spec §7.7).

Buffers canonical events per run until the run ends, then keeps the run when
it is interesting: failed, or slower than ``tail_min_duration_ms``. Boring
runs are dropped after the final decision instead of paying volume up front.

Safety bounds — when a limit is hit the buffer degrades to pass-through
mode for that run rather than growing unbounded:

- ``max_runs``: concurrent run buffers (new runs pass through);
- ``max_run_events``: per-run event count (the run's buffer releases as one
  bounded chunk, then buffering resumes so the run drains in chunks);
- ``max_age_seconds``: orphaned runs without a terminal event are flushed
  when polled.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from observe_core.models import Outcome

if TYPE_CHECKING:
    from observe_core.backends import CanonicalEvent


@dataclass
class _RunBuffer:
    events: list[CanonicalEvent] = field(default_factory=list)
    updated_at: float = field(default_factory=time.monotonic)


class TailSampler:
    """Bounded per-run buffer with keep/drop-at-end decisions."""

    def __init__(
        self,
        *,
        min_duration_ms: float,
        max_runs: int,
        max_run_events: int = 10_000,
        max_age_seconds: float = 3_600.0,
        stats: Any | None = None,
    ) -> None:
        self.min_duration_ms = min_duration_ms
        self.max_runs = max_runs
        self.max_run_events = max_run_events
        self.max_age_seconds = max_age_seconds
        self._stats = stats
        self._buffers: dict[str, _RunBuffer] = {}
        self._lock = threading.Lock()

    def _incr(self, counter: str) -> None:
        if self._stats is not None:
            self._stats.incr(counter)

    def _release(self, run_id: str, emit: Callable[[CanonicalEvent], Any]) -> None:
        buffer = self._buffers.pop(run_id, None)
        if buffer is None:
            return
        for event in buffer.events:
            emit(event)

    def process(self, event: CanonicalEvent, emit: Callable[[CanonicalEvent], Any]) -> None:
        """Buffer or forward an event; ``emit`` receives events to deliver."""
        correlation = event.data.get("correlation") or {}
        run_id = correlation.get("run_id")
        if not run_id:
            emit(event)
            return
        name = str(event.data.get("event") or "")
        terminal = name.endswith((".completed", ".failed", ".cancelled"))
        with self._lock:
            buffer = self._buffers.get(run_id)
            if buffer is None:
                if len(self._buffers) >= self.max_runs:
                    # Bound hit: pass through rather than growing unbounded.
                    emit(event)
                    return
                buffer = _RunBuffer()
                self._buffers[run_id] = buffer
            buffer.updated_at = time.monotonic()
            buffer.events.append(event)
            self._incr("tail_buffered")
            if len(buffer.events) > self.max_run_events:
                # Per-run bound hit: release everything buffered (the current
                # event included — it was appended above), then start a fresh
                # buffer so the run keeps draining in bounded chunks.
                self._release(run_id, emit)
                self._incr("tail_released")
                return
            if not terminal:
                return
            if self._interesting(buffer):
                self._release(run_id, emit)
                self._incr("tail_flushed")
            else:
                # Boring run: drop the buffered events instead of paying
                # the volume (spec §7.7).
                self._buffers.pop(run_id, None)
                self._incr("tail_reduced")

    def _interesting(self, buffer: _RunBuffer) -> bool:
        """Keep runs that failed or ran longer than the threshold."""
        last = buffer.events[-1].data if buffer.events else {}
        if last.get("outcome") == Outcome.FAILURE.value:
            return True
        duration = last.get("duration_ms")
        return isinstance(duration, int | float) and duration >= self.min_duration_ms

    def flush_expired(self, emit: Callable[[CanonicalEvent], Any]) -> int:
        """Flush run buffers idle longer than ``max_age_seconds``.

        Returns the number of runs flushed. Called on a periodic cadence by
        the runtime so orphaned runs (crashed producers) are not lost.
        """
        now = time.monotonic()
        with self._lock:
            stale = [
                run_id
                for run_id, buffer in self._buffers.items()
                if now - buffer.updated_at > self.max_age_seconds
            ]
            for run_id in stale:
                self._release(run_id, emit)
                self._incr("tail_flushed")
        return len(stale)

    def flush_all(self, emit: Callable[[CanonicalEvent], Any]) -> int:
        """Flush every buffered run (shutdown drain)."""
        with self._lock:
            run_ids = list(self._buffers)
            count = len(run_ids)
            for run_id in run_ids:
                self._release(run_id, emit)
            self._incr("tail_flushed")
            return count

    def buffered_runs(self) -> int:
        """Number of run buffers currently held."""
        return len(self._buffers)
