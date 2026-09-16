"""Rolling baselines per (entity, metric) — spec §16.

Statistics stay deliberately simple and inspectable: rolling median, MAD,
percentile bands and a running mean over a bounded sample window. No ML —
the spec's graduation criteria (§16.3) gate that explicitly.

Baselines are fed by ``metric.*`` events and by completed run durations.
Partition-aware keys come from the metric event's ``partition`` tag when
present.
"""

from __future__ import annotations

import statistics
from typing import Any

from observe_core.timestamps import utcnow
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from phlo_observer.models import Baseline
from phlo_observer.state_engine import _EPOCH, _parse_key, event_key, event_producer

MAX_SAMPLES = 200
"""Bounded rolling window per (entity, metric) — enough for stable medians."""


def compute(samples: list[float]) -> dict[str, float | None]:
    """Median/MAD/percentiles/mean for a sample window."""
    if not samples:
        return {"median": None, "mad": None, "mean": None, "p10": None, "p90": None}
    ordered = sorted(samples)
    median = statistics.median(ordered)
    mad = statistics.median([abs(x - median) for x in ordered])
    n = len(ordered)

    def pct(p: float) -> float:
        idx = min(n - 1, max(0, round(p * (n - 1))))
        return ordered[idx]

    return {
        "median": median,
        "mad": mad,
        "mean": sum(ordered) / n,
        "p10": pct(0.10),
        "p90": pct(0.90),
    }


def metric_entity(event: dict[str, Any]) -> str | None:
    """Entity a metric observation belongs to (asset, run, table, ...)."""
    entities = event.get("entities") or {}
    for role in ("asset", "table", "model", "run", "service"):
        if entities.get(role):
            return entities[role]
    corr = event.get("correlation") or {}
    if corr.get("asset_key"):
        return f"asset://{corr['asset_key']}"
    if corr.get("run_id"):
        # Same producer fallback chain as event_entities() so the baseline
        # entity id matches the entity-registry id exactly.
        return f"run://{event_producer(event)}/{corr['run_id']}"
    return None


def observations_of(event: dict[str, Any]) -> list[tuple[str, str, float]]:
    """``(entity_id, metric_name, value)`` triples one event contributes.

    ``metric.*`` events carry ``attributes.metric``/``attributes.value``;
    completed ``pipeline.run`` events contribute ``run.duration_ms``.
    A ``partition`` tag or correlation partition keys the baseline so
    partitioned assets don't share one statistic (spec §16.1).
    """
    attrs = event.get("attributes") or {}
    tags = event.get("tags") or {}
    corr = event.get("correlation") or {}
    partition = tags.get("partition") or corr.get("partition_key") or ""
    entity = metric_entity(event)
    out: list[tuple[str, str, float]] = []
    name = event.get("event") or ""

    def _key(base: str) -> str:
        return f"{base}|{partition}" if partition else base

    if name.startswith("metric.") and attrs.get("metric") is not None:
        # metric.recorded carries a single ``value``; metric.summary is an
        # aggregate whose ``mean`` is the best per-sample representative.
        raw_value = attrs.get("value")
        if raw_value is None:
            raw_value = attrs.get("mean")
        if raw_value is None:
            return out
        try:
            value = float(raw_value)
        except (TypeError, ValueError):
            return out
        if entity:
            out.append((entity, _key(str(attrs["metric"])), value))
    elif name in ("pipeline.run", "dlt.pipeline.run") and event.get("duration_ms"):
        # Run durations belong to the pipeline's history, not the one-off
        # run entity: a per-run baseline can never reach the warm-up
        # threshold, so duration-regression could never fire.
        job = corr.get("job_id") or corr.get("pipeline")
        if job:
            entity = f"job://{event_producer(event)}/{job}"
        if entity:
            out.append((entity, _key("run.duration_ms"), float(event["duration_ms"])))
    elif attrs.get("rows_written") is not None or attrs.get("row_count") is not None:
        raw = attrs.get("rows_written", attrs.get("row_count"))
        try:
            value = float(raw)
        except (TypeError, ValueError):
            return out
        if entity:
            out.append((entity, _key("rows"), value))
    return out


def _entries(row: Baseline | None) -> list[tuple[Any, str, float]]:
    """A row's samples as ``(observed_at, event_id, value)``, observed-sorted.

    Samples are stored as ``[iso, event_id, value]`` so the window keeps the
    newest *observed* samples regardless of arrival order — incremental
    ingest and rebuild converge on the same retained set. Plain-float
    samples written before the keyed format anchor at the epoch (they are
    always older than anything newly folded).
    """
    out: list[tuple[Any, str, float]] = []
    for item in (row.samples if row is not None else []) or []:
        raw: Any = item
        if isinstance(raw, (list, tuple)) and len(raw) >= 3:
            key = _parse_key(raw[:2])
            if key is not None:
                out.append((key[0], key[1], float(raw[2])))
                continue
        try:
            out.append((_EPOCH, "", float(raw)))
        except (TypeError, ValueError):
            continue
    out.sort(key=lambda entry: (entry[0], entry[1]))
    return out


def window_before(row: Baseline | None, key: tuple[Any, str]) -> list[float]:
    """Retained sample values preceding ``key`` in observed order.

    Insight rules evaluate an event against the baseline *as of* the
    event's position, so a late arrival judges itself against the same
    window a rebuild would have seen. Bounded by ``MAX_SAMPLES``: an event
    whose position predates the retained window sees the oldest retained
    prefix (a documented bounded-history limitation).
    """
    return [value for at, eid, value in _entries(row) if (at, eid) < key]


async def update_baselines(
    session: AsyncSession,
    event: dict[str, Any],
    *,
    rows: dict[tuple[str, str], Baseline] | None = None,
) -> int:
    """Fold one event's metric observations into rolling baselines.

    Samples insert at their ``(observed_at, event_id)`` position and the
    window keeps the newest ``MAX_SAMPLES`` by observed order, so folding a
    late event lands it in place rather than at the tail — the retained set
    matches a rebuild's.

    ``rows`` optionally supplies the batch's preloaded (entity, metric)
    -> Baseline map; newly created rows are registered into it so later
    events in the same batch see them without another query.
    """
    updated = 0
    key = event_key(event)
    for entity_id, metric, value in observations_of(event):
        if rows is None:
            row = (
                await session.execute(
                    select(Baseline).where(
                        Baseline.entity_id == entity_id, Baseline.metric == metric
                    )
                )
            ).scalar_one_or_none()
        else:
            row = rows.get((entity_id, metric))
        entries = _entries(row)
        inserted = not any((at, eid) == key for at, eid, _ in entries)
        if inserted:
            entries.append((key[0], key[1], value))
            entries.sort(key=lambda entry: (entry[0], entry[1]))
            if len(entries) > MAX_SAMPLES:
                del entries[: len(entries) - MAX_SAMPLES]
        stats = compute([value for _, _, value in entries])
        if row is None:
            row = Baseline(entity_id=entity_id, metric=metric, samples=[])
            session.add(row)
            if rows is not None:
                rows[(entity_id, metric)] = row
        row.samples = [[at.isoformat(), eid, v] for at, eid, v in entries]
        if inserted:
            row.count = (row.count or 0) + 1
        row.median = stats["median"]
        row.mad = stats["mad"]
        row.mean = stats["mean"]
        row.p10 = stats["p10"]
        row.p90 = stats["p90"]
        row.updated_at = utcnow()
        updated += 1
    return updated
