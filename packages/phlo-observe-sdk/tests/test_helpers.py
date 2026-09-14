"""Helper emit functions and WAP critical delivery."""

from __future__ import annotations

from observe_core import flush
from observe_core.drains.memory import MemoryDrain
from observe_core.runtime import Runtime
from phlo_observe import asset_materialize, pipeline_run, quality_validate
from phlo_observe.integrations.wap import (
    wap_branch_create,
    wap_cleanup,
    wap_promote,
    wap_reject,
    wap_validate,
)


def _data(drain: MemoryDrain) -> list[dict]:
    flush(2.0)
    return [e.data for e in drain.events]


def test_pipeline_run_attributes(captured: tuple[Runtime, MemoryDrain]):
    _, drain = captured
    with pipeline_run(job="daily_ingestion", run_id="R1", attempt=1, trigger="schedule") as evt:
        evt.set(assets=14)
    (ev,) = _data(drain)
    assert ev["event"] == "pipeline.run"
    assert ev["category"] == "pipeline"
    assert ev["outcome"] == "success"
    assert ev["correlation"]["run_id"] == "R1"
    assert ev["correlation"]["job_id"] == "daily_ingestion"
    attrs = ev["attributes"]
    assert attrs["job"] == "daily_ingestion"
    assert attrs["attempt"] == 1
    assert attrs["trigger"] == "schedule"
    assert attrs["assets"] == 14


def test_asset_materialize_attributes(captured: tuple[Runtime, MemoryDrain]):
    _, drain = captured
    with asset_materialize(
        asset_key="silver.samples",
        partition_key="2026-09-14",
        rows_in=14291,
        rows_out=14277,
        bytes_written=5_832_031,
    ):
        pass
    (ev,) = _data(drain)
    assert ev["event"] == "asset.materialize"
    assert ev["correlation"]["asset_key"] == "silver.samples"
    assert ev["correlation"]["partition_key"] == "2026-09-14"
    assert ev["attributes"]["rows_in"] == 14291
    assert ev["attributes"]["rows_out"] == 14277


def test_quality_validate_failure(captured: tuple[Runtime, MemoryDrain]):
    _, drain = captured
    try:
        with quality_validate(
            suite="silver_samples", checks_total=12, checks_passed=11, checks_failed=1
        ):
            raise RuntimeError("check failed")
    except RuntimeError:
        pass
    (ev,) = _data(drain)
    assert ev["event"] == "quality.validate"
    assert ev["outcome"] == "failure"
    assert ev["attributes"]["checks_failed"] == 1


def test_wap_promote_is_critical(captured: tuple[Runtime, MemoryDrain]):
    _, drain = captured
    with wap_promote(branch="run/01J", target_branch="main", attributes={"commit_id": "abc"}):
        pass
    (ev,) = _data(drain)
    assert ev["event"] == "wap.promote"
    assert ev["delivery"] == "critical"
    assert ev["correlation"]["branch"] == "run/01J"
    assert ev["attributes"]["target"] == "main"
    assert ev["attributes"]["commit_id"] == "abc"


def test_wap_reject_is_critical(captured: tuple[Runtime, MemoryDrain]):
    _, drain = captured
    with wap_reject(branch="run/01J"):
        pass
    (ev,) = _data(drain)
    assert ev["event"] == "wap.reject"
    assert ev["delivery"] == "critical"


def test_wap_lifecycle_events(captured: tuple[Runtime, MemoryDrain]):
    _, drain = captured
    with wap_branch_create(branch="run/01J", base_branch="main"):
        pass
    with wap_validate(branch="run/01J"):
        pass
    with wap_cleanup(branch="run/01J"):
        pass
    names = [e["event"] for e in _data(drain)]
    assert names == ["wap.branch.create", "wap.validate", "wap.cleanup"]
    assert all(e["delivery"] == "telemetry" for e in _data(drain))
