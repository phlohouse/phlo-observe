"""Reusable realistic Phlo telemetry workloads for hardening tests.

Scenarios model what Phlo actually emits: Dagster runs with steps, assets,
partitions and checks; DLT loads; dbt invocations; Pandera checks; WAP
branch lifecycles; Nessie/Iceberg commits; Trino queries. All timestamps
are simulated (``observed_at``) so histories compress into milliseconds of
test time while keeping realistic temporal relationships.

Every generator is deterministic given ``seed``: the same seed always
produces the same scenario, which keeps failure modes reproducible.
"""

from __future__ import annotations

import datetime as dt
import random
import uuid
from typing import Any

T0 = dt.datetime(2026, 1, 5, 6, 0, 0, tzinfo=dt.UTC)
"""Simulated clock origin (a Monday, before any freshness baseline)."""


def _iso(t: dt.datetime) -> str:
    return t.isoformat().replace("+00:00", "Z")


def _event(
    name: str,
    t: dt.datetime,
    *,
    category: str = "pipeline",
    outcome: str | None = "success",
    severity: str = "info",
    service: str = "phlo",
    producer: str = "phlo",
    env: str = "prod",
    correlation: dict[str, Any] | None = None,
    entities: dict[str, str] | None = None,
    attributes: dict[str, Any] | None = None,
    error: dict[str, Any] | None = None,
    tags: dict[str, Any] | None = None,
    started_at: dt.datetime | None = None,
    ended_at: dt.datetime | None = None,
    duration_ms: float | None = None,
    delivery: str = "telemetry",
    event_id: str | None = None,
) -> dict[str, Any]:
    """One canonical V2 envelope for the scenario language."""
    ev: dict[str, Any] = {
        "schema_version": "2.0",
        "event_id": event_id or str(uuid.uuid4()),
        "event": name,
        "category": category,
        "outcome": outcome,
        "severity": severity,
        "delivery": delivery,
        "observed_at": _iso(t),
        "service": {"name": service, "version": "2.4.1", "environment": env},
        "source": {"producer": producer},
        "correlation": correlation or {},
        "entities": entities or {},
        "attributes": attributes or {},
    }
    if error:
        ev["error"] = error
        ev["severity"] = "error"
    if tags:
        ev["tags"] = tags
    if started_at:
        ev["started_at"] = _iso(started_at)
    if ended_at:
        ev["ended_at"] = _iso(ended_at)
    if duration_ms is not None:
        ev["duration_ms"] = duration_ms
    return ev


# -- per-producer scenario generators ---------------------------------------


def dagster_run(
    run_id: str,
    t0: dt.datetime,
    *,
    job: str = "daily_etl",
    assets: list[str] | None = None,
    partitions: list[str | None] | None = None,
    outcome: str = "success",
    fail_step: int | None = None,
    steps: int = 4,
    check_fails: set[int] | None = None,
    duration_ms: float = 120_000,
    retry_of: str | None = None,
    env: str = "prod",
) -> list[dict[str, Any]]:
    """One Dagster-style run: start, steps, materializations, checks, end."""
    assets = assets or []
    partitions = partitions or [None] * len(assets)
    check_fails = check_fails or set()
    events: list[dict[str, Any]] = []
    t = t0
    started = t
    run_ent = f"run://dagster/{run_id}"
    base_entities = {"run": run_ent, "service": "service://dagster-daemon"}
    corr = {"run_id": run_id, "job_id": job}

    events.append(
        _event(
            "pipeline.run",
            t,
            outcome="unknown",
            service="dagster-daemon",
            producer="dagster",
            env=env,
            correlation=corr,
            entities=base_entities,
            attributes={"job": job, "trigger": "schedule"},
            started_at=started,
        )
    )
    for i in range(steps):
        t += dt.timedelta(milliseconds=duration_ms / (steps + 1))
        failed = fail_step == i
        events.append(
            _event(
                "pipeline.step",
                t,
                outcome="failure" if failed else "success",
                service="dagster-daemon",
                producer="dagster",
                env=env,
                correlation=corr,
                entities=base_entities,
                attributes={"step": f"step_{i}", "job": job},
                started_at=started,
                ended_at=t,
                error=(
                    {"exception_type": "StepError", "message": f"step_{i} raised"}
                    if failed
                    else None
                ),
            )
        )
    for i, asset in enumerate(assets):
        part = partitions[i] if i < len(partitions) else None
        t += dt.timedelta(seconds=2)
        ok = outcome != "failure" or i < len(assets) - 1
        asset_corr = {**corr, "asset_key": asset}
        if part:
            asset_corr["partition_key"] = part
        events.append(
            _event(
                "asset.materialize",
                t,
                category="data",
                outcome="success" if ok else "failure",
                service="dagster-daemon",
                producer="dagster",
                env=env,
                correlation=asset_corr,
                entities={**base_entities, "asset": f"asset://{asset}"},
                attributes={
                    "asset_key": asset,
                    "partition": part,
                    "row_count": 50_000,
                    "freshness_sla_seconds": 86_400,
                },
                tags={"partition": part} if part else None,
            )
        )
        events.append(
            _event(
                "quality.check",
                t + dt.timedelta(seconds=1),
                category="quality",
                outcome="failure" if i in check_fails else "success",
                service="dagster-daemon",
                producer="dagster",
                env=env,
                correlation=asset_corr,
                entities={**base_entities, "asset": f"asset://{asset}"},
                attributes={"check": "not_null", "asset_key": asset},
                error=(
                    {"exception_type": "CheckError", "message": "nulls in key column"}
                    if i in check_fails
                    else None
                ),
            )
        )
    t += dt.timedelta(seconds=5)
    attrs: dict[str, Any] = {"job": job, "trigger": "schedule"}
    if retry_of:
        attrs["retry_of"] = retry_of
    events.append(
        _event(
            "pipeline.run",
            t,
            outcome=outcome,
            service="dagster-daemon",
            producer="dagster",
            env=env,
            correlation=corr,
            entities=base_entities,
            attributes=attrs,
            started_at=started,
            ended_at=t,
            duration_ms=(t - started).total_seconds() * 1000,
            error=(
                {"exception_type": "RunFailure", "message": f"{job} failed"}
                if outcome == "failure"
                else None
            ),
        )
    )
    return events


def dlt_load(
    run_id: str,
    t0: dt.datetime,
    *,
    pipeline: str = "stripe_ingest",
    dataset: str = "raw_stripe",
    tables: list[str] | None = None,
    outcome: str = "success",
    rows: int = 12_000,
) -> list[dict[str, Any]]:
    """One DLT load: run + per-table load events with row counts."""
    tables = tables or ["charges", "customers"]
    run_ent = f"run://dlt/{run_id}"
    corr = {"run_id": run_id, "pipeline": pipeline}
    events = [
        _event(
            "dlt.pipeline.run",
            t0,
            outcome="unknown",
            producer="dlt",
            correlation=corr,
            entities={"run": run_ent, "service": "service://dlt-worker"},
            attributes={"pipeline": pipeline, "dataset": dataset},
        )
    ]
    t = t0
    for tbl in tables:
        t += dt.timedelta(seconds=10)
        events.append(
            _event(
                "dlt.load",
                t,
                category="data",
                outcome=outcome,
                producer="dlt",
                correlation={**corr, "table": f"{dataset}.{tbl}"},
                entities={
                    "run": run_ent,
                    "service": "service://dlt-worker",
                    "table": f"table://{dataset}.{tbl}",
                    "source": f"source://{pipeline}",
                },
                attributes={"table": tbl, "rows_written": rows},
            )
        )
    t += dt.timedelta(seconds=3)
    events.append(
        _event(
            "dlt.pipeline.run",
            t,
            outcome=outcome,
            producer="dlt",
            correlation=corr,
            entities={"run": run_ent, "service": "service://dlt-worker"},
            started_at=t0,
            ended_at=t,
            duration_ms=(t - t0).total_seconds() * 1000,
            error=(
                {"exception_type": "LoadError", "message": "schema inference failed"}
                if outcome == "failure"
                else None
            ),
        )
    )
    return events


def dbt_invocation(
    invocation_id: str,
    t0: dt.datetime,
    *,
    models: list[str] | None = None,
    test_failures: set[str] | None = None,
    outcome: str = "success",
) -> list[dict[str, Any]]:
    """One dbt run: invocation + model.execute + test.execute events."""
    models = models or ["stg_orders", "fct_revenue"]
    test_failures = test_failures or set()
    run_ent = f"run://dbt/{invocation_id}"
    corr = {"invocation_id": invocation_id, "run_id": f"dbt-{invocation_id}"}
    events = [
        _event(
            "dbt.invocation",
            t0,
            outcome="unknown",
            producer="dbt",
            correlation=corr,
            entities={"run": run_ent, "service": "service://dbt-runner"},
            attributes={"dbt_version": "1.9.0", "command": "build"},
        )
    ]
    t = t0
    for model in models:
        t += dt.timedelta(seconds=20)
        failed = model in test_failures
        events.append(
            _event(
                "dbt.model.execute",
                t,
                category="data",
                outcome="failure" if failed else "success",
                producer="dbt",
                correlation=corr,
                entities={
                    "run": run_ent,
                    "service": "service://dbt-runner",
                    "model": f"model://dbt/{model}",
                },
                attributes={"model": model, "rows_affected": 30_000},
            )
        )
        events.append(
            _event(
                "dbt.test.execute",
                t + dt.timedelta(seconds=2),
                category="quality",
                outcome="failure" if failed else "success",
                producer="dbt",
                correlation=corr,
                entities={
                    "run": run_ent,
                    "service": "service://dbt-runner",
                    "model": f"model://dbt/{model}",
                },
                attributes={"test": f"not_null_{model}"},
                error=(
                    {"exception_type": "TestFailure", "message": f"{model} test failed"}
                    if failed
                    else None
                ),
            )
        )
    t += dt.timedelta(seconds=4)
    events.append(
        _event(
            "pipeline.run",
            t,
            outcome="failure" if test_failures else outcome,
            producer="dbt",
            correlation=corr,
            entities={"run": run_ent, "service": "service://dbt-runner"},
            started_at=t0,
            ended_at=t,
            duration_ms=(t - t0).total_seconds() * 1000,
        )
    )
    return events


def wap_branch_lifecycle(
    run_id: str,
    branch: str,
    t0: dt.datetime,
    *,
    table: str = "lake.sales",
    outcome: str = "promoted",
    commits: int = 2,
    base_branch: str = "main",
) -> list[dict[str, Any]]:
    """WAP branch lifecycle: create → commits → validate → promote/reject → cleanup.

    ``outcome`` is "promoted" or "rejected".
    """
    run_ent = f"run://dagster/{run_id}"
    branch_ent = f"branch://nessie/{branch}"
    table_ent = f"table://{table}"
    events = [
        _event(
            "wap.branch.create",
            t0,
            category="storage",
            producer="phlo",
            correlation={"run_id": run_id, "branch": branch},
            entities={"run": run_ent, "branch": branch_ent, "table": table_ent},
            attributes={"base_branch": base_branch},
        )
    ]
    t = t0
    for i in range(commits):
        t += dt.timedelta(seconds=30)
        snap = f"snap-{branch}-{i}"
        events.append(
            _event(
                "iceberg.commit",
                t,
                category="storage",
                producer="phlo",
                correlation={
                    "run_id": run_id,
                    "branch": branch,
                    "table": table,
                    "snapshot_id": snap,
                },
                entities={
                    "run": run_ent,
                    "branch": branch_ent,
                    "table": table_ent,
                    "iceberg": f"iceberg://{table}",
                    "snapshot": f"snapshot://{table}/{snap}",
                },
                attributes={"snapshot_id": snap, "added_rows": 8_000},
            )
        )
    t += dt.timedelta(seconds=10)
    events.append(
        _event(
            "wap.validate",
            t,
            category="quality",
            outcome="success" if outcome == "promoted" else "failure",
            producer="phlo",
            correlation={"run_id": run_id, "branch": branch},
            entities={"run": run_ent, "branch": branch_ent, "table": table_ent},
            attributes={"checks_passed": 4 if outcome == "promoted" else 2},
            error=(
                {"exception_type": "ValidationError", "message": "2 checks failed"}
                if outcome == "rejected"
                else None
            ),
        )
    )
    t += dt.timedelta(seconds=5)
    if outcome == "promoted":
        events.append(
            _event(
                "wap.promote",
                t,
                category="storage",
                outcome="success",
                producer="phlo",
                correlation={"run_id": run_id, "branch": branch},
                entities={"run": run_ent, "branch": branch_ent, "table": table_ent},
                attributes={"target": base_branch},
            )
        )
    else:
        events.append(
            _event(
                "wap.reject",
                t,
                category="storage",
                outcome="success",
                producer="phlo",
                correlation={"run_id": run_id, "branch": branch},
                entities={"run": run_ent, "branch": branch_ent, "table": table_ent},
                attributes={"reason": "validation_failed"},
            )
        )
    t += dt.timedelta(seconds=2)
    events.append(
        _event(
            "wap.cleanup",
            t,
            category="storage",
            outcome="success",
            producer="phlo",
            correlation={"run_id": run_id, "branch": branch},
            entities={"run": run_ent, "branch": branch_ent},
        )
    )
    return events


def trino_queries(
    t0: dt.datetime,
    *,
    count: int,
    catalog: str = "lake",
    run_id: str | None = None,
    rng: random.Random,
) -> list[dict[str, Any]]:
    """Trino query telemetry: scans with rows/bytes/duration attributes."""
    events = []
    for i in range(count):
        t = t0 + dt.timedelta(seconds=i * 7)
        corr = {"run_id": run_id} if run_id else {}
        events.append(
            _event(
                "trino.query",
                t,
                category="infrastructure",
                outcome="success",
                service="trino-coordinator",
                producer="trino",
                correlation=corr,
                entities={"service": "service://trino-coordinator"},
                attributes={
                    "query_hash": f"q{rng.randint(0, 50)}",
                    "scanned_rows": rng.randint(10_000, 2_000_000),
                    "scanned_bytes": rng.randint(1 << 20, 1 << 30),
                    "catalog": catalog,
                },
                duration_ms=rng.randint(200, 30_000),
            )
        )
    return events


def metric_samples(
    entity_role: str,
    entity_id: str,
    metric: str,
    values: list[float],
    t0: dt.datetime,
    *,
    step: dt.timedelta = dt.timedelta(hours=6),
    partition: str | None = None,
    producer: str = "phlo",
) -> list[dict[str, Any]]:
    """``metric.<name>`` observations feeding the rolling baseline."""
    events = []
    tags = {"partition": partition} if partition else None
    corr = {"partition_key": partition} if partition else {}
    for i, v in enumerate(values):
        events.append(
            _event(
                f"metric.{metric}",
                t0 + i * step,
                category="metric",
                outcome="success",
                producer=producer,
                correlation=corr,
                entities={entity_role: entity_id},
                attributes={"metric": metric, "value": v},
                tags=tags,
            )
        )
    return events


# -- scenario composition -----------------------------------------------------


def mixed_history(
    *,
    seed: int = 7,
    days: int = 7,
    daily_runs: int = 3,
    assets: list[str] | None = None,
    failure_rate: float = 0.15,
) -> list[dict[str, Any]]:
    """A week of interleaved multi-producer telemetry in observed-time order.

    Includes scheduled Dagster runs (some failing, some retried), DLT loads,
    dbt invocations, WAP branch lifecycles, Trino queries and metric samples.
    """
    rng = random.Random(seed)
    assets = assets or ["analytics.daily_orders", "analytics.user_sessions"]
    events: list[dict[str, Any]] = []
    for day in range(days):
        day_start = T0 + dt.timedelta(days=day)
        for r in range(daily_runs):
            run_id = f"daily-etl-d{day}-r{r}"
            fail = rng.random() < failure_rate
            events += dagster_run(
                run_id,
                day_start + dt.timedelta(hours=r * 6, minutes=rng.randint(0, 30)),
                assets=assets,
                partitions=[f"2026-01-{5 + day:02d}"] * len(assets),
                outcome="failure" if fail else "success",
                fail_step=rng.randint(0, 3) if fail else None,
                check_fails={0} if fail and rng.random() < 0.5 else set(),
                duration_ms=rng.uniform(90_000, 150_000),
            )
            if fail:
                events += dagster_run(
                    f"{run_id}-retry",
                    day_start + dt.timedelta(hours=r * 6 + 1),
                    assets=assets,
                    outcome="success",
                    retry_of=run_id,
                )
        events += dlt_load(f"dlt-d{day}", day_start + dt.timedelta(hours=2))
        events += dbt_invocation(
            f"inv-d{day}",
            day_start + dt.timedelta(hours=4),
            test_failures={"fct_revenue"} if rng.random() < 0.1 else set(),
        )
        branch = f"wap/sales-d{day}"
        events += wap_branch_lifecycle(
            f"wap-run-d{day}",
            branch,
            day_start + dt.timedelta(hours=8),
            outcome="rejected" if rng.random() < 0.2 else "promoted",
        )
        events += trino_queries(day_start + dt.timedelta(hours=10), count=4, rng=rng)
        events += metric_samples(
            "asset",
            f"asset://{assets[0]}",
            "rows",
            [rng.uniform(45_000, 55_000) for _ in range(2)],
            day_start + dt.timedelta(hours=7),
            partition=f"2026-01-{5 + day:02d}",
        )
    events.sort(key=lambda e: (e["observed_at"], e["event_id"]))
    return events


# -- arrival-order injectors --------------------------------------------------


def inject_out_of_order(
    events: list[dict[str, Any]], *, seed: int = 0, window: int = 6
) -> list[dict[str, Any]]:
    """Shuffle arrivals inside sliding windows: near-miss reordering."""
    rng = random.Random(seed)
    out = list(events)
    for i in range(0, len(out), window):
        rng.shuffle(out[i : i + window])
    return out


def inject_late(
    events: list[dict[str, Any]], *, seed: int = 0, fraction: float = 0.05
) -> list[dict[str, Any]]:
    """Move a fraction of events to the end: they arrive long after observed."""
    rng = random.Random(seed)
    late_idx = set(rng.sample(range(len(events)), int(len(events) * fraction)))
    on_time = [e for i, e in enumerate(events) if i not in late_idx]
    on_time.extend(e for i, e in enumerate(events) if i in late_idx)
    return on_time


def inject_duplicates(
    events: list[dict[str, Any]], *, seed: int = 0, fraction: float = 0.05
) -> list[dict[str, Any]]:
    """Re-send a fraction of events (same event_id) as transport duplicates."""
    rng = random.Random(seed)
    dup_idx = set(rng.sample(range(len(events)), int(len(events) * fraction)))
    out: list[dict[str, Any]] = []
    for i, e in enumerate(events):
        out.append(e)
        if i in dup_idx:
            out.append(dict(e))
    return out


def batches(events: list[dict[str, Any]], size: int = 200) -> list[list[dict[str, Any]]]:
    """Split an arrival-ordered event list into ingest batches."""
    return [events[i : i + size] for i in range(0, len(events), size)]


async def post_all(client: Any, events: list[dict[str, Any]], batch: int = 200) -> dict[str, int]:
    """POST events in batches; return aggregate accepted/rejected counts."""
    accepted = rejected = 0
    for chunk in batches(events, batch):
        resp = await client.post("/v1/events", json=chunk)
        assert resp.status_code == 202, resp.text[:300]
        body = resp.json()
        accepted += body["accepted"]
        rejected += body.get("rejected", 0)
    return {"accepted": accepted, "rejected": rejected}
