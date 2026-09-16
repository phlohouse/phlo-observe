"""Insight-rule quality on controlled histories (hardening item 7).

Each scenario is built to answer one question: does the deterministic rule
set produce useful signal — one alert per real problem, no warm-up noise,
partition-aware baselines, auto-resolution — rather than maximum volume?
"""

from __future__ import annotations

import datetime as dt
import uuid
from typing import Any

import pytest
import workloads as wl
from httpx import AsyncClient
from phlo_observer.models import Baseline, Incident, Insight
from sqlalchemy import select

pytestmark = pytest.mark.asyncio

_UID = uuid.uuid4().hex[:6]


async def _insights(client: AsyncClient) -> list[dict[str, Any]]:
    resp = await client.get("/v2/insights", params={"limit": 500})
    assert resp.status_code == 200
    return resp.json()["items"] if isinstance(resp.json(), dict) else resp.json()


async def _db_insights(session_factory: Any) -> list[Any]:
    async with session_factory() as session:
        return list((await session.execute(select(Insight))).scalars())


async def _db_incidents(session_factory: Any) -> list[Any]:
    async with session_factory() as session:
        return list((await session.execute(select(Incident))).scalars())


def _metric_run_events(
    entity: str,
    values: list[float],
    t0: dt.datetime,
    metric: str = "rows",
    partition: str | None = None,
) -> list[dict[str, Any]]:
    return wl.metric_samples("asset", entity, metric, values, t0, partition=partition)


async def test_duration_regression_fires_once(client: AsyncClient, session_factory: Any) -> None:
    """6 normal durations then a 3x outlier -> exactly one regression insight."""
    t0 = wl.T0
    events: list[dict[str, Any]] = []
    # Six baseline runs then the slow one.
    for i in range(6):
        events += wl.dagster_run(f"reg-ok-{i}", t0 + dt.timedelta(hours=i), duration_ms=100_000)
    events += wl.dagster_run("reg-slow", t0 + dt.timedelta(hours=7), duration_ms=320_000)
    await client.post("/v1/events", json=events)
    rows = await _db_insights(session_factory)
    regressions = [i for i in rows if i.rule_id == "duration-regression"]
    assert len(regressions) == 1
    assert "3.2x" in regressions[0].attributes["summary"] or "regressed" in regressions[0].title


async def test_warmup_suppresses_early_outliers(client: AsyncClient, session_factory: Any) -> None:
    """Below MIN_BASELINE_SAMPLES, even a 10x outlier must not alert."""
    events: list[dict[str, Any]] = []
    for i in range(3):
        events += wl.dagster_run(f"warm-{i}", wl.T0 + dt.timedelta(hours=i), duration_ms=50_000)
    events += wl.dagster_run("warm-outlier", wl.T0 + dt.timedelta(hours=4), duration_ms=600_000)
    await client.post("/v1/events", json=events)
    rows = await _db_insights(session_factory)
    assert not [i for i in rows if i.rule_id == "duration-regression"]


async def test_volume_spike(client: AsyncClient, session_factory: Any) -> None:
    """Row volume >3x rolling median -> one volume-spike insight."""
    entity = f"asset://analytics.vol_{_UID}"
    values = [50_000] * 7 + [200_000]
    await client.post("/v1/events", json=_metric_run_events(entity, values, wl.T0))
    rows = await _db_insights(session_factory)
    spikes = [i for i in rows if i.rule_id == "volume-spike" and i.entity_id == entity]
    assert len(spikes) == 1


async def test_partition_baselines_isolated(client: AsyncClient, session_factory: Any) -> None:
    """A spike on partition A must not judge partition B's fresh history."""
    entity = f"asset://analytics.part_{_UID}"
    events = _metric_run_events(entity, [50_000] * 7 + [200_000], wl.T0, partition="2026-01-01")
    # Partition B only has its own short history — never compares to A's spike.
    events += _metric_run_events(
        entity, [10_000] * 3, wl.T0 + dt.timedelta(days=2), partition="2026-01-02"
    )
    await client.post("/v1/events", json=events)
    rows = await _db_insights(session_factory)
    spikes = [i for i in rows if i.rule_id == "volume-spike"]
    assert len(spikes) == 1
    # The insight names the partitioned baseline that fired.
    assert "2026-01-01" in spikes[0].title
    assert spikes[0].entity_id == entity
    # Partition B's own baseline stayed short — it was judged only on its
    # own history and never inherited partition A's samples.
    async with session_factory() as session:
        part_b = await session.scalar(
            select(Baseline).where(
                Baseline.entity_id == entity, Baseline.metric == "rows|2026-01-02"
            )
        )
    assert part_b is not None
    assert part_b.count == 3


async def test_quality_failure_then_resolve(client: AsyncClient, session_factory: Any) -> None:
    """Failed check opens an insight; a later success resolves it."""
    asset = f"asset://analytics.q_{_UID}"
    fail = wl._event(
        "quality.check",
        wl.T0,
        category="quality",
        outcome="failure",
        entities={"asset": asset},
        error={"exception_type": "CheckError", "message": "not_null violated"},
    )
    ok = wl._event(
        "quality.check",
        wl.T0 + dt.timedelta(hours=1),
        category="quality",
        outcome="success",
        entities={"asset": asset},
    )
    await client.post("/v1/events", json=[fail, ok])
    rows = await _db_insights(session_factory)
    quality = [i for i in rows if i.rule_id == "quality-failure"]
    assert len(quality) == 1
    assert quality[0].state == "resolved"


async def test_dbt_test_failure_opens_quality_insight(
    client: AsyncClient, session_factory: Any
) -> None:
    """Regression: ``dbt.test.execute`` failures feed the quality-failure rule."""
    model = f"model://dbt/stg_q_{_UID}"
    fail = wl._event(
        "dbt.test.execute",
        wl.T0,
        category="quality",
        outcome="failure",
        producer="dbt",
        entities={"model": model, "run": "run://dbt/inv-q"},
        error={"exception_type": "TestFailure", "message": "not_null failed"},
    )
    ok = wl._event(
        "dbt.test.execute",
        wl.T0 + dt.timedelta(hours=1),
        category="quality",
        outcome="success",
        producer="dbt",
        entities={"model": model, "run": "run://dbt/inv-q"},
    )
    await client.post("/v1/events", json=[fail, ok])
    rows = await _db_insights(session_factory)
    quality = [i for i in rows if i.rule_id == "quality-failure" and i.entity_id == model]
    assert len(quality) == 1
    assert quality[0].state == "resolved"


async def test_quality_validate_failure_opens_insight(
    client: AsyncClient, session_factory: Any
) -> None:
    """Regression: ``quality.validate`` failures also feed the rule."""
    asset = f"asset://analytics.val_{_UID}"
    fail = wl._event(
        "quality.validate",
        wl.T0,
        category="quality",
        outcome="failure",
        entities={"asset": asset},
    )
    await client.post("/v1/events", json=[fail])
    rows = await _db_insights(session_factory)
    assert any(i.rule_id == "quality-failure" and i.entity_id == asset for i in rows)


async def test_metric_summary_feeds_baseline(client: AsyncClient, session_factory: Any) -> None:
    """Regression: aggregated ``metric.summary`` means reach the baseline."""
    from phlo_observer.models import Baseline

    asset = f"asset://analytics.ms_{_UID}"
    for i, mean in enumerate([100.0, 120.0, 110.0]):
        await client.post(
            "/v1/events",
            json=[
                wl._event(
                    "metric.summary",
                    wl.T0 + dt.timedelta(hours=i),
                    category="metric",
                    entities={"asset": asset},
                    attributes={
                        "metric": "rows",
                        "count": 10,
                        "sum": mean * 10,
                        "mean": mean,
                        "min": mean - 5,
                        "max": mean + 5,
                    },
                )
            ],
        )
    async with session_factory() as session:
        row = (
            await session.execute(
                select(Baseline).where(Baseline.entity_id == asset, Baseline.metric == "rows")
            )
        ).scalar_one_or_none()
    assert row is not None, "metric.summary mean must feed the rows baseline"
    assert row.count == 3
    assert row.mean == pytest.approx(110.0)


async def test_run_failure_opens_incident(client: AsyncClient, session_factory: Any) -> None:
    """A critical run-failure insight opens an incident holding the run."""
    events = wl.dagster_run(f"crit-{uuid.uuid4().hex[:8]}", wl.T0, outcome="failure", fail_step=0)
    await client.post("/v1/events", json=events)
    incidents = await _db_incidents(session_factory)
    assert len(incidents) == 1
    assert incidents[0].state == "open"
    assert incidents[0].severity == "critical"
    assert any(str(e).startswith("run://") for e in incidents[0].entities)


async def test_retry_failure_marks_recurring(client: AsyncClient, session_factory: Any) -> None:
    """A run failing with retry_of set is classified recurring_failure."""
    events = wl.dagster_run("orig-fail", wl.T0, outcome="failure", fail_step=0)
    events += wl.dagster_run(
        "retry-fail",
        wl.T0 + dt.timedelta(hours=1),
        outcome="failure",
        fail_step=0,
        retry_of="orig-fail",
    )
    await client.post("/v1/events", json=events)
    rows = await _db_insights(session_factory)
    recurring = [i for i in rows if i.attributes.get("kind") == "recurring_failure"]
    assert len(recurring) == 1


async def test_repeat_failure_dedupes_not_duplicates(
    client: AsyncClient, session_factory: Any
) -> None:
    """Same failure signature twice -> one insight, evidence appended."""
    asset = f"asset://analytics.dedup_{_UID}"
    events = [
        wl._event(
            "quality.check",
            wl.T0 + dt.timedelta(hours=i),
            category="quality",
            outcome="failure",
            entities={"asset": asset},
            error={"exception_type": "CheckError", "message": "same violation"},
        )
        for i in range(3)
    ]
    await client.post("/v1/events", json=events)
    rows = await _db_insights(session_factory)
    quality = [i for i in rows if i.rule_id == "quality-failure"]
    assert len(quality) == 1
    assert len(quality[0].evidence_event_ids) == 3


async def test_mixed_history_signal_is_bounded(client: AsyncClient, session_factory: Any) -> None:
    """A week of mostly-healthy telemetry produces few insights, not noise."""
    events = wl.mixed_history(seed=21, days=7, daily_runs=3, failure_rate=0.05)
    await client.post("/v1/events", json=events)
    rows = await _db_insights(session_factory)
    # 7 days * ~6 runs/day at 5% failure + dedup: single-digit alert count.
    assert len(rows) <= 15, f"noisy: {len(rows)} insights on a healthy week"
    # And every insight carries evidence back to canonical events.
    for i in rows:
        assert i.evidence_event_ids, f"insight {i.insight_id} has no evidence"


async def test_insight_lifecycle_transition(client: AsyncClient, session_factory: Any) -> None:
    """open -> acknowledged -> resolved via the transition endpoint."""
    events = wl.dagster_run(
        f"lifecycle-{uuid.uuid4().hex[:8]}", wl.T0, outcome="failure", fail_step=0
    )
    await client.post("/v1/events", json=events)
    rows = await _db_insights(session_factory)
    insight = rows[0]
    resp = await client.post(
        f"/v2/insights/{insight.insight_id}/transition",
        json={"state": "acknowledged"},
    )
    assert resp.status_code == 200
    resp = await client.post(
        f"/v2/insights/{insight.insight_id}/transition",
        json={"state": "resolved"},
    )
    assert resp.status_code == 200
    rows = await _db_insights(session_factory)
    assert rows[0].state == "resolved"


async def test_stale_signal_never_joins_or_rewinds_incident(
    client: AsyncClient, session_factory: Any
) -> None:
    """An insight signaled >30min before an incident's last signal is not
    related to it — the one-sided window let it attach and rewind the
    incident's freshness anchor backwards."""
    from phlo_observer import incidents as inc_mod

    last = wl.T0 + dt.timedelta(hours=5)
    incident = Incident(
        incident_id=uuid.uuid4(),
        title="prior incident",
        state="open",
        severity="critical",
        entities=["run://phlo/stale"],
        insight_ids=[],
        timeline={},
        impact={},
        attributes={"last_signal_at": last.isoformat()},
        updated_at=wl.T0,
    )
    stale = Insight(
        insight_id=uuid.uuid4(),
        rule_id="run-failure",
        rule_version=1,
        title="stale failure",
        severity="critical",
        state="open",
        entity_id="run://phlo/stale",
        evidence_event_ids=[],
        dedupe_key="k-stale",
        attributes={"observed_at": (last - dt.timedelta(hours=2)).isoformat()},
        created_at=wl.T0,
        updated_at=wl.T0,
    )
    async with session_factory() as session:
        result = await inc_mod.group_insight(session, stale, open_incidents=[incident])
    # The stale insight may open its own incident (critical), but the
    # existing incident is untouched: no attach, no rewound anchor.
    assert result is not incident
    assert incident.attributes["last_signal_at"] == last.isoformat()
    assert str(stale.insight_id) not in (incident.insight_ids or [])
    if result is not None:
        assert result.attributes["last_signal_at"] == (last - dt.timedelta(hours=2)).isoformat()


async def test_attach_never_rewinds_last_signal(session_factory: Any) -> None:
    """An in-window insight whose signal predates the incident's last
    signal attaches without moving ``last_signal_at`` backwards."""
    from phlo_observer import incidents as inc_mod

    last = wl.T0 + dt.timedelta(hours=5)
    incident = Incident(
        incident_id=uuid.uuid4(),
        title="inc",
        state="open",
        severity="error",
        entities=["asset://a"],
        insight_ids=[],
        timeline={},
        impact={},
        attributes={"last_signal_at": last.isoformat()},
        updated_at=wl.T0,
    )
    older = Insight(
        insight_id=uuid.uuid4(),
        rule_id="quality-failure",
        rule_version=1,
        title="older",
        severity="error",
        state="open",
        entity_id="asset://a",
        evidence_event_ids=[],
        dedupe_key="k-older",
        attributes={"observed_at": (last - dt.timedelta(minutes=10)).isoformat()},
        created_at=wl.T0,
        updated_at=wl.T0,
    )
    async with session_factory() as session:
        result = await inc_mod.group_insight(session, older, open_incidents=[incident])
    assert result is incident
    assert str(older.insight_id) in (incident.insight_ids or [])
    # In-window but older: attached, anchor untouched.
    assert incident.attributes["last_signal_at"] == last.isoformat()


async def test_refresh_advances_last_observed_at(session_factory: Any) -> None:
    """Repeat findings move ``last_observed_at`` forward — grouping keys on
    the newest signal — and an out-of-order repeat cannot rewind it."""
    from phlo_observer import insights as ins

    entity = f"asset://analytics.touch_{_UID}"
    finding = ins.Finding(
        rule_id="quality-failure",
        kind="quality_degradation",
        severity="warning",
        title="t",
        summary="s",
        entity_id=entity,
        evidence_event_ids=["e1"],
    )

    def _check(at: dt.datetime) -> dict[str, Any]:
        return wl._event(
            "quality.check",
            at,
            category="quality",
            outcome="failure",
            entities={"asset": entity},
            error={"exception_type": "CheckError", "message": "same"},
        )

    dedupe: dict[str, Any] = {}
    async with session_factory() as session, session.begin():
        created = await ins.record_findings(
            session, _check(wl.T0), [finding], open_by_dedupe=dedupe
        )
        insight = created[0]
        assert "last_observed_at" not in (insight.attributes or {})

        later = wl.T0 + dt.timedelta(hours=2)
        again = await ins.record_findings(session, _check(later), [finding], open_by_dedupe=dedupe)
        assert again[0] is insight
        assert insight.attributes["last_observed_at"] == later.isoformat()

        # An older repeat finding must not move the anchor backwards.
        await ins.record_findings(
            session, _check(wl.T0 + dt.timedelta(hours=1)), [finding], open_by_dedupe=dedupe
        )
        assert insight.attributes["last_observed_at"] == later.isoformat()
