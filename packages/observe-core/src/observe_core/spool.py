"""Local spool for ``critical`` events.

A directory of append-only JSONL segment files. Events are spooled when the
queue is full or a remote drain cannot accept them, and replayed oldest-first.
Segments are deleted only after confirmed remote acceptance; corrupt segments
are quarantined with a ``.corrupt`` suffix rather than crashing the process.

Capacity policy is explicit: when ``max_bytes`` is exceeded the spool either
evicts the oldest sealed segment (``drop_oldest``, the default — bounded disk)
or refuses new writes (``drop_newest``). Either way the failure is counted and
diagnosed; disk growth is always bounded.
"""

from __future__ import annotations

import contextlib
import itertools
import logging
import sys
import threading
import time
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from observe_core.drains.base import Drain
    from observe_core.stats import TelemetryStats

_log = logging.getLogger("observe_core.spool")

_SEGMENT_PREFIX = "seg-"
_SEGMENT_SUFFIX = ".jsonl"
_CORRUPT_SUFFIX = ".corrupt"


class Spool:
    """Append-only JSONL segment spool with bounded size."""

    def __init__(
        self,
        directory: Path,
        *,
        max_bytes: int = 1024 * 1024 * 1024,
        segment_max_bytes: int = 32 * 1024 * 1024,
        on_full: str = "drop_oldest",
        stats: TelemetryStats | None = None,
    ) -> None:
        self.directory = directory
        self.max_bytes = max_bytes
        self.segment_max_bytes = segment_max_bytes
        self.on_full = on_full
        self._stats = stats
        self._seq = itertools.count()
        self._current: Path | None = None
        self._current_size = 0
        self._lock = threading.Lock()
        directory.mkdir(parents=True, exist_ok=True)

    # -- write path ---------------------------------------------------------

    def append(self, payload: bytes) -> bool:
        """Append one canonical event line. Returns False when capacity refused it."""
        line = payload if payload.endswith(b"\n") else payload + b"\n"
        with self._lock:
            self._rotate_if_needed(len(line))
            if self._current is None:
                self._open_segment()
            current = self._current
            if current is None:  # pragma: no cover - _open_segment always sets it
                return False
            while self._total_size() + len(line) > self.max_bytes:
                if self.on_full == "drop_oldest" and self._evict_oldest():
                    if self._stats:
                        self._stats.incr("spool_dropped_oldest")
                    continue
                if self._stats:
                    self._stats.incr("spool_errors")
                _diag(f"phlo-observe spool full at {self.directory}; dropping critical event")
                return False
            try:
                with current.open("ab") as fh:
                    fh.write(line)
                self._current_size += len(line)
                return True
            except OSError as exc:
                if self._stats:
                    self._stats.incr("spool_errors")
                _diag(f"phlo-observe spool write failed: {exc}")
                return False

    def _open_segment(self) -> None:
        name = f"{_SEGMENT_PREFIX}{time.time_ns()}-{next(self._seq)}{_SEGMENT_SUFFIX}"
        self._current = self.directory / name
        self._current_size = 0

    def _rotate_if_needed(self, incoming: int) -> None:
        if self._current is None:
            return
        if self._current_size + incoming > self.segment_max_bytes:
            self._current = None

    def _segments(self) -> list[Path]:
        return sorted(
            self.directory.glob(f"{_SEGMENT_PREFIX}*{_SEGMENT_SUFFIX}"),
            key=lambda p: p.name,
        )

    def _total_size(self) -> int:
        try:
            return sum(p.stat().st_size for p in self._segments())
        except OSError:
            return 0

    def _evict_oldest(self) -> bool:
        segments = self._segments()
        if self._current is not None and segments and segments[0] == self._current:
            if len(segments) > 1:
                target = segments[1]
            else:
                return False
        elif segments:
            target = segments[0]
        else:
            return False
        try:
            target.unlink()
            return True
        except OSError:
            return False

    # -- replay -------------------------------------------------------------

    def pending_bytes(self) -> int:
        """Total bytes currently held in the spool."""
        return self._total_size()

    def pending_segments(self) -> int:
        """Number of spool segments on disk."""
        return len(self._segments())

    def replay(self, drain: Drain, *, max_events: int | None = None) -> int:
        """Replay oldest segments through ``drain``. Returns events replayed.

        A segment is deleted only after the drain accepts its whole batch.
        Segments containing undecodable lines are quarantined to ``.corrupt``.
        """
        replayed = 0
        for segment in self._segments():
            if max_events is not None and replayed >= max_events:
                break
            if segment == self._current:
                continue
            try:
                lines = [line for line in segment.read_bytes().splitlines() if line.strip()]
            except OSError:
                continue
            if any(not _looks_like_json(line) for line in lines):
                self._quarantine(segment)
                continue
            try:
                drain.emit_raw(lines)
            except Exception:
                break
            with contextlib.suppress(OSError):
                segment.unlink()
            replayed += len(lines)
            if self._stats:
                self._stats.incr("spool_replayed_events", len(lines))
        return replayed

    def _quarantine(self, segment: Path) -> None:
        with contextlib.suppress(OSError):
            segment.rename(segment.with_suffix(_CORRUPT_SUFFIX))
        if self._stats:
            self._stats.incr("spool_errors")
        _log.warning("quarantined corrupt spool segment %s", segment)


def _looks_like_json(line: bytes) -> bool:
    return line[:1] == b"{" and line[-1:] == b"}"


def _diag(message: str) -> None:
    """Minimal stderr diagnostic that never raises."""
    with contextlib.suppress(Exception):
        sys.stderr.write(f"[observe-core] {message}\n")
