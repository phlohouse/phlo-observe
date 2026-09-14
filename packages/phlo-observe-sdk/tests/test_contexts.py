"""Domain context tests: binding, composition, isolation."""

from __future__ import annotations

from observe_core import event, flush
from observe_core.drains.memory import MemoryDrain
from observe_core.runtime import Runtime
from phlo_observe import asset_context, phlo_run_context, table_context, wap_context


def _data(drain: MemoryDrain) -> list[dict]:
    flush(2.0)
    return [e.data for e in drain.events]


def test_run_context_binds_identifiers(captured: tuple[Runtime, MemoryDrain]):
    _, drain = captured
    with phlo_run_context(run_id="R1", job="daily_ingestion", attempt=2):
        event("pipeline.step")
    (ev,) = _data(drain)
    assert ev["correlation"]["run_id"] == "R1"
    assert ev["correlation"]["job_id"] == "daily_ingestion"
    assert ev["correlation"]["extra"]["attempt"] == 2


def test_nested_contexts_compose(captured: tuple[Runtime, MemoryDrain]):
    _, drain = captured
    with (
        phlo_run_context(run_id="R1", job="daily"),
        asset_context(asset_key="silver.samples", partition_key="2026-09-14"),
        table_context(table="silver.samples", catalog="nessie", snapshot_id="81293"),
    ):
        event("asset.materialize")
    (ev,) = _data(drain)
    corr = ev["correlation"]
    assert corr["run_id"] == "R1"
    assert corr["asset_key"] == "silver.samples"
    assert corr["partition_key"] == "2026-09-14"
    assert corr["table"] == "silver.samples"
    assert corr["snapshot_id"] == "81293"
    assert corr["extra"]["catalog"] == "nessie"


def test_wap_context(captured: tuple[Runtime, MemoryDrain]):
    _, drain = captured
    with wap_context(branch="run/01J", base_branch="main", table="silver.samples"):
        event("wap.validate")
    (ev,) = _data(drain)
    assert ev["correlation"]["branch"] == "run/01J"
    assert ev["correlation"]["table"] == "silver.samples"
    assert ev["correlation"]["extra"]["base_branch"] == "main"


def test_context_reverts_after_block(captured: tuple[Runtime, MemoryDrain]):
    _, drain = captured
    with phlo_run_context(run_id="R1"):
        pass
    event("pipeline.step")
    (ev,) = _data(drain)
    assert ev["correlation"]["run_id"] is None
