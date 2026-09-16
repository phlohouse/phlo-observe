"""Aggregation output for high-frequency signals (spec §7.11).

Metrics are event-shaped too: instead of emitting thousands of identical
``metric`` events per minute, ``MetricAggregator`` accumulates values keyed by
(metric name, dimensions, entities, correlation, tags) and flushes a summary
event on a cadence. The aggregated event is a normal envelope
(``category="metric"``, ``delivery="telemetry"``) carrying the statistical
summary in attributes, so it flows through the same drains, spool, and
ingestion path as everything else.
"""

from __future__ import annotations

import math
import random
import threading
import time
from dataclasses import dataclass, field
from typing import Any

_COUNTS = ("count", "sum", "min", "max")

_rng = random.Random()  # noqa: S311 — reservoir sampling, not security
"""Reservoir sampling source; percentiles stay unbiased past the cap."""


def _freeze(mapping: dict[str, Any] | None) -> tuple[tuple[str, str], ...] | None:
    """Canonical key form for a dict section; None when absent."""
    if not mapping:
        return None
    frozen = tuple(sorted((str(k), str(v)) for k, v in mapping.items() if v is not None))
    return frozen or None


@dataclass
class _Bucket:
    """One series: exact count/sum/min/max plus a bounded value reservoir."""

    values: list[float] = field(default_factory=list)
    seen: int = 0
    total: float = 0.0
    minimum: float = math.inf
    maximum: float = -math.inf
    dimensions: dict[str, str] | None = None
    correlation: dict[str, str] | None = None
    entities: dict[str, str] | None = None
    tags: dict[str, str] | None = None

    def add(self, value: float, cap: int) -> None:
        self.seen += 1
        self.total += value
        self.minimum = min(self.minimum, value)
        self.maximum = max(self.maximum, value)
        if len(self.values) < cap:
            self.values.append(value)
        else:
            # Reservoir sample: bounded memory, unbiased percentiles.
            slot = _rng.randrange(self.seen)
            if slot < cap:
                self.values[slot] = value

    def summary(self) -> dict[str, Any]:
        if self.seen == 0:
            return {}
        values = sorted(self.values)
        return {
            "count": self.seen,
            "sum": self.total,
            "min": self.minimum,
            "max": self.maximum,
            "mean": self.total / self.seen,
            "p50": _percentile(values, 0.50),
            "p95": _percentile(values, 0.95),
            "p99": _percentile(values, 0.99),
        }


def _percentile(sorted_values: list[float], p: float) -> float:
    if not sorted_values:
        return 0.0
    index = max(0, min(len(sorted_values) - 1, math.ceil(p * len(sorted_values)) - 1))
    return sorted_values[index]


_SeriesKey = tuple[
    str,
    tuple[tuple[str, str], ...] | None,
    tuple[tuple[str, str], ...] | None,
    tuple[tuple[str, str], ...] | None,
    tuple[tuple[str, str], ...] | None,
]


class MetricAggregator:
    """Thread-safe in-memory metric accumulator.

    ``record(name, value, ...)`` adds a sample; ``flush()`` returns and clears
    ready summary payloads keyed ``(name, dimensions, entities, correlation,
    tags)`` so the caller can emit them as events with their correlation and
    entity declarations intact (baselines need the entity to key on).
    """

    def __init__(self, *, max_series: int = 10_000, max_samples: int = 10_000) -> None:
        self._buckets: dict[_SeriesKey, _Bucket] = {}
        self._max_series = max_series
        self._max_samples = max_samples
        self._lock = threading.Lock()
        self._last_flush = time.monotonic()

    def record(
        self,
        name: str,
        value: float,
        dimensions: dict[str, str] | None = None,
        *,
        correlation: dict[str, Any] | None = None,
        entities: dict[str, Any] | None = None,
        tags: dict[str, Any] | None = None,
    ) -> bool:
        """Add a sample; False when the series bound rejected it."""
        key: _SeriesKey = (
            name,
            _freeze(dimensions),
            _freeze(entities),
            _freeze(correlation),
            _freeze(tags),
        )
        with self._lock:
            bucket = self._buckets.get(key)
            if bucket is None:
                if len(self._buckets) >= self._max_series:
                    return False
                bucket = _Bucket(
                    dimensions=dict(dimensions) if dimensions else None,
                    correlation={k: str(v) for k, v in correlation.items() if v is not None}
                    if correlation
                    else None,
                    entities={k: str(v) for k, v in entities.items()} if entities else None,
                    tags={k: str(v) for k, v in tags.items()} if tags else None,
                )
                self._buckets[key] = bucket
            bucket.add(value, self._max_samples)
            return True

    def due(self, flush_after_seconds: float) -> bool:
        """True when the accumulator has sat unflushed past the cadence."""
        return time.monotonic() - self._last_flush >= flush_after_seconds

    def flush(self) -> list[dict[str, Any]]:
        """Drain all series into summary payloads with their series context."""
        with self._lock:
            ready = []
            for (_name, _dims, _ents, _corr, _tags), bucket in self._buckets.items():
                payload: dict[str, Any] = {
                    "metric": _name,
                    "dimensions": dict(bucket.dimensions or {}),
                    **bucket.summary(),
                }
                if bucket.correlation:
                    payload["correlation"] = dict(bucket.correlation)
                if bucket.entities:
                    payload["entities"] = dict(bucket.entities)
                if bucket.tags:
                    payload["tags"] = dict(bucket.tags)
                ready.append(payload)
            self._buckets.clear()
            self._last_flush = time.monotonic()
        return ready

    def series_count(self) -> int:
        """Number of series currently accumulating."""
        return len(self._buckets)
