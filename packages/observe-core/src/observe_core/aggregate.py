"""Aggregation output for high-frequency signals (spec §7.11).

Metrics are event-shaped too: instead of emitting thousands of identical
``metric`` events per minute, ``MetricAggregator`` accumulates values keyed by
(metric name, dimensions) and flushes a summary event on a cadence. The
aggregated event is a normal envelope (``category="metric"``,
``delivery="telemetry"``) carrying the statistical summary in attributes, so
it flows through the same drains, spool, and ingestion path as everything
else.
"""

from __future__ import annotations

import math
import threading
from dataclasses import dataclass, field
from typing import Any

_COUNTS = ("count", "sum", "min", "max")


@dataclass
class _Bucket:
    values: list[float] = field(default_factory=list)
    total: float = 0.0
    minimum: float = math.inf
    maximum: float = -math.inf

    def add(self, value: float) -> None:
        self.values.append(value)
        self.total += value
        self.minimum = min(self.minimum, value)
        self.maximum = max(self.maximum, value)

    def summary(self) -> dict[str, Any]:
        count = len(self.values)
        if count == 0:
            return {}
        values = sorted(self.values)
        return {
            "count": count,
            "sum": self.total,
            "min": self.minimum,
            "max": self.maximum,
            "mean": self.total / count,
            "p50": _percentile(values, 0.50),
            "p95": _percentile(values, 0.95),
            "p99": _percentile(values, 0.99),
        }


def _percentile(sorted_values: list[float], p: float) -> float:
    if not sorted_values:
        return 0.0
    index = max(0, min(len(sorted_values) - 1, math.ceil(p * len(sorted_values)) - 1))
    return sorted_values[index]


class MetricAggregator:
    """Thread-safe in-memory metric accumulator.

    ``record(name, value, dimensions=...)`` adds a sample; ``flush()`` returns
    and clears ready summary attribute dicts keyed ``(name, dimensions)`` so
    the caller can emit them as events.
    """

    def __init__(self, *, max_series: int = 10_000) -> None:
        self._buckets: dict[tuple[str, tuple[tuple[str, str], ...]], _Bucket] = {}
        self._max_series = max_series
        self._lock = threading.Lock()

    def record(self, name: str, value: float, dimensions: dict[str, str] | None = None) -> bool:
        """Add a sample; False when the series bound rejected it."""
        key = (name, tuple(sorted((dimensions or {}).items())))
        with self._lock:
            bucket = self._buckets.get(key)
            if bucket is None:
                if len(self._buckets) >= self._max_series:
                    return False
                bucket = _Bucket()
                self._buckets[key] = bucket
            bucket.add(value)
            return True

    def flush(self) -> list[dict[str, Any]]:
        """Drain all series into summary attribute payloads."""
        with self._lock:
            ready = [
                {
                    "metric": name,
                    "dimensions": dict(dimensions),
                    **bucket.summary(),
                }
                for (name, dimensions), bucket in self._buckets.items()
            ]
            self._buckets.clear()
        return ready

    def series_count(self) -> int:
        """Number of series currently accumulating."""
        return len(self._buckets)
