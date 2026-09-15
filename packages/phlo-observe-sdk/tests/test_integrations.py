"""Integration tests: dagster duck-typing, dbt parsing, trino, pandera, dlt."""

from __future__ import annotations

import logging
from typing import ClassVar

from observe_core import flush
from observe_core.drains.memory import MemoryDrain
from observe_core.runtime import Runtime
from phlo_observe.integrations import dagster, dbt, dlt, trino
from phlo_observe.integrations.pandera import pandera_attributes


def _data(drain: MemoryDrain) -> list[dict]:
    flush(2.0)
    return [e.data for e in drain.events]


# -- dagster -----------------------------------------------------------------


class _FakeAssetKey:
    path = ("silver", "samples")


class _FakeDagsterContext:
    run_id = "dagster-run-1"
    job_name = "daily_ingestion"
    partition_key = "2026-09-14"
    retry_number = 1
    asset_key = _FakeAssetKey()


def test_dagster_run_scope_binds(captured: tuple[Runtime, MemoryDrain]):
    _, drain = captured
    with dagster.dagster_run_scope(_FakeDagsterContext()):
        from observe_core import event

        event("pipeline.step")
    (ev,) = _data(drain)
    corr = ev["correlation"]
    assert corr["run_id"] == "dagster-run-1"
    assert corr["job_id"] == "daily_ingestion"
    assert corr["partition_key"] == "2026-09-14"
    assert corr["asset_key"] == "silver.samples"
    assert corr["extra"]["retry_number"] == 1
    # Ambient producer: server-derived entity ids land in the dagster
    # namespace (run://dagster/...), matching declared entities.
    assert ev["source"]["producer"] == "dagster"


def test_dagster_emit_materialization(captured: tuple[Runtime, MemoryDrain]):
    _, drain = captured
    with dagster.emit_materialization(_FakeDagsterContext(), rows=100):
        pass
    (ev,) = _data(drain)
    assert ev["event"] == "asset.materialize"
    assert ev["correlation"]["asset_key"] == "silver.samples"
    assert ev["attributes"]["rows_out"] == 100
    assert ev["source"]["producer"] == "dagster"


def test_dagster_asset_check(captured: tuple[Runtime, MemoryDrain]):
    _, drain = captured
    dagster.emit_asset_check(_FakeDagsterContext(), check_name="no_nulls", passed=False)
    (ev,) = _data(drain)
    assert ev["event"] == "quality.check"
    assert ev["outcome"] == "failure"
    assert ev["severity"] == "error"
    assert ev["attributes"]["check_name"] == "no_nulls"


# -- dbt ----------------------------------------------------------------------

RUN_RESULTS = {
    "metadata": {"invocation_id": "inv-123", "dbt_version": "1.8.0"},
    "elapsed_time": 12.4,
    "args": {"which": "run"},
    "results": [
        {
            "unique_id": "model.phlo.silver_samples",
            "status": "success",
            "execution_time": 1.2,
            "relation_name": "silver.samples",
            "adapter_response": {"rows_affected": 14277},
        },
        {
            "unique_id": "test.phlo.not_null_samples_id",
            "status": "fail",
            "execution_time": 0.3,
            "failures": 8,
            "depends_on": {"nodes": ["model.phlo.silver_samples"]},
        },
    ],
}


def test_dbt_run_results_events(captured: tuple[Runtime, MemoryDrain]):
    _, drain = captured
    n = dbt.emit_run_results(RUN_RESULTS)
    assert n == 3
    events = _data(drain)
    invocation = events[0]
    assert invocation["event"] == "dbt.invocation"
    assert invocation["correlation"]["invocation_id"] == "inv-123"
    assert invocation["attributes"]["dbt_version"] == "1.8.0"
    # The invocation is the run's terminal event: run_id correlation gives the
    # observer a real run projection, outcome reflects the worst result.
    assert invocation["correlation"]["run_id"] == "inv-123"
    assert invocation["outcome"] == "failure"  # a result failed
    assert invocation["duration_ms"] == 12_400.0
    assert invocation["entities"]["run"] == "run://dbt/inv-123"

    model = events[1]
    assert model["event"] == "dbt.model.execute"
    assert model["outcome"] == "success"
    assert model["attributes"]["rows_affected"] == 14277
    assert model["source"]["producer"] == "dbt"
    assert model["entities"]["run"] == "run://dbt/inv-123"
    assert model["entities"]["model"] == "model://dbt/silver_samples"
    assert model["correlation"]["run_id"] == "inv-123"

    test_ev = events[2]
    assert test_ev["event"] == "dbt.test.execute"
    assert test_ev["outcome"] == "failure"
    assert test_ev["severity"] == "error"
    assert test_ev["attributes"]["failures"] == 8
    # depends_on.nodes links the test to the model it checks.
    assert test_ev["entities"]["model"] == "model://dbt/silver_samples"
    assert test_ev["entities"]["run"] == "run://dbt/inv-123"


def test_dbt_invocation_success_when_all_pass(captured: tuple[Runtime, MemoryDrain]):
    """A clean run_results document yields a successful invocation outcome."""
    _, drain = captured
    doc = {
        "metadata": {"invocation_id": "inv-ok"},
        "elapsed_time": 1.0,
        "results": [{"unique_id": "model.p.m1", "status": "success"}],
    }
    dbt.emit_run_results(doc)
    events = _data(drain)
    assert events[0]["outcome"] == "success"
    assert events[0]["duration_ms"] == 1000.0


def test_dbt_manifest_metadata():
    manifest = {
        "metadata": {"dbt_version": "1.8.0", "project_name": "phlo", "adapter_type": "trino"}
    }
    meta = dbt.manifest_metadata(manifest)
    assert meta["project_name"] == "phlo"
    assert meta["adapter_type"] == "trino"


# -- trino ---------------------------------------------------------------------


def test_trino_query_hash_no_sql_by_default(captured: tuple[Runtime, MemoryDrain]):
    _, drain = captured
    sql = "SELECT * FROM silver.samples WHERE id = 'secret-123' AND n > 42"
    with trino.trino_query(query_id="q1", sql=sql, catalog="iceberg"):
        pass
    (ev,) = _data(drain)
    assert ev["event"] == "trino.query"
    assert ev["attributes"]["query_class"] == "select"
    assert ev["attributes"]["query_hash"]
    assert ev["source"]["producer"] == "trino"
    assert "sql" not in ev["attributes"]


def test_trino_include_sql_sanitized(captured: tuple[Runtime, MemoryDrain]):
    _, drain = captured
    sql = "SELECT * FROM t WHERE name = 'alice' AND n > 42 -- comment"
    with trino.trino_query(sql=sql, include_sql=True):
        pass
    (ev,) = _data(drain)
    emitted_sql = ev["attributes"]["sql"]
    assert "alice" not in emitted_sql
    assert "42" not in emitted_sql
    assert "comment" not in emitted_sql
    assert "?" in emitted_sql


def test_trino_query_hash_stable():
    h1 = trino.query_hash("SELECT * FROM t WHERE id = 'x'")
    h2 = trino.query_hash("SELECT * FROM t WHERE id = 'x'")
    assert h1 == h2
    assert len(h1) == 16


# -- pandera -------------------------------------------------------------------


class _FakeSeries:
    def __init__(self, values):
        self._values = values

    def tolist(self):
        return self._values


class _FakeFailureCases:
    def __init__(self, rows):
        self._rows = rows

    def __getitem__(self, name):
        return _FakeSeries([r[name] for r in self._rows])

    def __len__(self):
        return len(self._rows)


class _FakeSchemaError:
    schema = type("S", (), {"name": "silver_samples"})()
    failure_cases = _FakeFailureCases(
        [
            {"column": "id", "check": "not_null", "failure_case": None},
            {"column": "id", "check": "not_null", "failure_case": None},
        ]
    )
    schema_errors: ClassVar[list] = []


def test_pandera_attributes_from_schema_error():
    attrs = pandera_attributes(_FakeSchemaError())
    assert attrs["suite"] == "silver_samples"
    assert attrs["rows_failed"] == 2
    assert attrs["failing_columns"] == ["id"]
    assert attrs["failure_codes"] == ["not_null"]
    assert "sample_failures" not in attrs


def test_pandera_samples_opt_in():
    attrs = pandera_attributes(_FakeSchemaError(), include_samples=True)
    assert attrs["sample_failures"] == ["None", "None"]


# -- dlt -----------------------------------------------------------------------


class _FakePipeline:
    pipeline_name = "github_issues"
    destination_name = "iceberg"
    dataset_name = "bronze"


def test_dlt_pipeline_run(captured: tuple[Runtime, MemoryDrain]):
    _, drain = captured
    with dlt.dlt_pipeline_run(_FakePipeline()):
        pass
    (ev,) = _data(drain)
    assert ev["event"] == "dlt.pipeline.run"
    assert ev["correlation"]["pipeline"] == "github_issues"
    assert ev["attributes"]["destination"] == "iceberg"
    assert ev["source"]["producer"] == "dlt"


def test_dlt_load_info_attributes():
    load_info = type(
        "LI",
        (),
        {
            "pipeline": _FakePipeline(),
            "dataset_name": "bronze",
            "load_ids": ["l1"],
            "load_packages": [],
        },
    )()
    attrs = dlt.load_info_attributes(load_info)
    assert attrs["pipeline_name"] == "github_issues"
    assert attrs["dataset_name"] == "bronze"


# -- logging bridge ------------------------------------------------------------


def test_configure_logging_no_duplicate_handlers():
    from phlo_observe import configure_logging

    logger = logging.getLogger("phlo.test.bridge")
    configure_logging(logger=logger)
    count = len(logger.handlers)
    configure_logging(logger=logger)
    assert len(logger.handlers) == count
