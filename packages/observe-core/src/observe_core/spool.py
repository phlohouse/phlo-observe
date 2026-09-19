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
import hashlib
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
_DEAD_SUFFIX = ".dead"


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
        _capacity_root: Path | None = None,
        _active_paths: set[Path] | None = None,
        _capacity_lock: threading.Lock | None = None,
    ) -> None:
        self.directory = directory
        self.max_bytes = max_bytes
        self.segment_max_bytes = segment_max_bytes
        self.on_full = on_full
        self._stats = stats
        # Destination spools live below one configured directory.  Accounting
        # against the root keeps fan-out bounded by the configured budget.
        self._capacity_root = _capacity_root or directory
        self._active_paths = _active_paths if _active_paths is not None else set()
        self._seq = itertools.count()
        self._current: Path | None = None
        self._current_size = 0
        self._lock = _capacity_lock or threading.Lock()
        self._replay_lock = threading.Lock()
        directory.mkdir(parents=True, exist_ok=True)

    def destination(self, identity: str) -> Spool:
        """Return the durable spool for one destination identity."""
        digest = hashlib.sha256(identity.encode("utf-8")).hexdigest()[:24]
        return Spool(
            self.directory / ("dest-" + digest),
            max_bytes=self.max_bytes,
            segment_max_bytes=self.segment_max_bytes,
            on_full=self.on_full,
            stats=self._stats,
            _capacity_root=self._capacity_root,
            _active_paths=self._active_paths,
            _capacity_lock=self._lock,
        )

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
        self._active_paths.add(self._current)
        self._current_size = 0

    def _rotate_if_needed(self, incoming: int) -> None:
        if self._current is None:
            return
        if self._current_size + incoming > self.segment_max_bytes:
            self._active_paths.discard(self._current)
            self._current = None

    def _segments(self) -> list[Path]:
        """Return this spool's segments, including destination children.

        The recursive view keeps existing diagnostics able to inspect the
        configured spool root. Replay internals use ``_segments_local`` so a
        legacy root replay never guesses which destination owns a child.
        """
        return sorted(
            (
                p
                for p in self.directory.rglob(f"{_SEGMENT_PREFIX}*{_SEGMENT_SUFFIX}")
                if p.is_file()
            ),
            key=lambda p: p.name,
        )

    def _segments_local(self) -> list[Path]:
        return sorted(
            self.directory.glob(f"{_SEGMENT_PREFIX}*{_SEGMENT_SUFFIX}"),
            key=lambda p: p.name,
        )

    def _all_files(self) -> list[Path]:
        """Every file this spool owns, including quarantined segments."""
        return sorted(self.directory.glob(f"{_SEGMENT_PREFIX}*"), key=lambda p: p.name)

    def _total_size(self) -> int:
        # Quarantined segments still occupy disk: they must count toward
        # max_bytes or a poison-payload flood grows the directory unboundedly.
        try:
            return sum(
                p.stat().st_size
                for p in self._capacity_root.rglob(f"{_SEGMENT_PREFIX}*")
                if p.is_file()
            )
        except OSError:
            return 0

    def _evict_oldest(self) -> bool:
        # Oldest pending segment first (never the active one); when nothing
        # pending can go, evict the oldest quarantined file — dead/corrupt
        # segments still count toward the disk budget.
        all_segments = sorted(
            (p for p in self._capacity_root.rglob(f"{_SEGMENT_PREFIX}*") if p.is_file()),
            key=lambda p: p.name,
        )
        pending = [
            p for p in all_segments if p.suffix == _SEGMENT_SUFFIX and p not in self._active_paths
        ]
        candidates = pending or [p for p in all_segments if p not in self._active_paths]
        if not candidates:
            return False
        try:
            candidates[0].unlink()
            return True
        except OSError:
            return False

    # -- replay -------------------------------------------------------------

    def pending_bytes(self) -> int:
        """Total bytes currently held in the spool."""
        return self._total_size()

    def pending_segments(self) -> int:
        """Number of spool segments on disk."""
        if self.directory == self._capacity_root:
            return len(self._segments())
        return len(self._segments_local())

    def replay(self, drain: Drain, *, max_events: int | None = None) -> int:
        """Replay oldest segments through ``drain``. Returns events replayed.

        The active segment is sealed first so its records are replayable now
        rather than stranded until a size rotation. A segment is deleted only
        after the drain accepts its whole batch. Undecodable segments are
        quarantined to ``.corrupt``; segments the drain permanently rejects
        are quarantined to ``.dead`` so one poison segment cannot block all
        later critical events forever. Transient drain failures stop the
        replay and keep the segment for the next interval.
        """
        from observe_core.drains.base import PermanentDrainFailure  # noqa: PLC0415

        if not self._replay_lock.acquire(blocking=False):
            return 0  # another worker is already replaying
        try:
            with self._lock:
                # Seal the open segment: no writer touches it again, so its
                # complete records can be replayed and the file removed.
                if self._current is not None:
                    self._active_paths.discard(self._current)
                self._current = None
                self._current_size = 0
            replayed = 0
            for segment in self._segments_local():
                if max_events is not None and replayed >= max_events:
                    break
                with self._lock:
                    # A racing append can have opened a new current segment
                    # between sealing and the glob; it is still being written
                    # (a partial trailing line would look "corrupt") and must
                    # never be renamed or deleted under a writer.
                    if segment == self._current:
                        continue
                try:
                    lines = [line for line in segment.read_bytes().splitlines() if line.strip()]
                except OSError:
                    continue
                if any(not _looks_like_json(line) for line in lines):
                    self._quarantine(segment, _CORRUPT_SUFFIX, "corrupt")
                    continue
                try:
                    drain.emit_raw(lines)
                except PermanentDrainFailure:
                    self._quarantine(segment, _DEAD_SUFFIX, "permanently rejected")
                    continue
                except Exception:
                    break  # transient: keep for the next replay interval
                with self._lock, contextlib.suppress(OSError):
                    segment.unlink()
                replayed += len(lines)
                if self._stats:
                    self._stats.incr("spool_replayed_events", len(lines))
            return replayed
        finally:
            self._replay_lock.release()

    def _quarantine(self, segment: Path, suffix: str, reason: str) -> None:
        with self._lock, contextlib.suppress(OSError):
            segment.rename(segment.with_suffix(suffix))
        if self._stats:
            self._stats.incr("spool_quarantined")
        _log.warning("quarantined %s spool segment %s", reason, segment)


def _looks_like_json(line: bytes) -> bool:
    return line[:1] == b"{" and line[-1:] == b"}"


def _diag(message: str) -> None:
    """Minimal stderr diagnostic that never raises."""
    with contextlib.suppress(Exception):
        sys.stderr.write(f"[observe-core] {message}\n")
