# Integrations

## Sending events from application code

```python
from phlo_observe import pipeline_run, asset_materialize, quality_validate

with pipeline_run(run_id="dagster:abc123", job_name="nightly_etl"):
    with asset_materialize(asset_key="mart.fct_orders"):
        ...
    with quality_validate(asset_key="mart.fct_orders", check_name="not_null"):
        ...
```

Or emit canonical events straight to the observer over HTTP:

```python
from observe_core.config import ObserveConfig
from observe_core.runtime import configure

configure(
    ObserveConfig(
        service_name="my-service",
        drains=[
            {
                "type": "http",
                "endpoint": "http://observer:8080/v1/events",
                "api_key": "<ingest-token>",
            }
        ],
    )
)
```

## Dagster

- Client-side: `phlo_observe.integrations.dagster.observe_dagster_run()`
  attaches Phlo run/asset context to a Dagster run.
- Server-side: POST Dagster event records to `/v1/ingest/dagster`. Run start/
  success/failure/cancel, step start/success/failure, asset materializations
  and asset check results are normalized; other engine events are skipped.

## dbt

- Client-side: `phlo_observe.integrations.dbt.run_results_events()` maps a
  parsed `run_results.json` to canonical event payloads.
- Server-side: POST the document to `/v1/ingest/dbt`, or
  `{"run_results": ..., "manifest": ...}` to `/v1/ingest/dbt/artifacts`
  (manifest supplies project/adapter metadata).

## Trino

`phlo_observe.integrations.trino` sanitizes query text (literals stripped,
hashed) so queries are identifiable without exfiltrating data. Client-side
only; the observer receives the resulting canonical events.

## Iceberg / Nessie / WAP

`phlo_observe.integrations.wap` + `iceberg` emit `wap.create`, `wap.merge`,
`wap.promote`, and `table.commit` events correlated on `branch` and
`snapshot_id`.

## DLT / Pandera

`integrations.dlt` wraps pipeline run events; `integrations.pandera` maps
schema-validation failures to `quality.check` events with structured errors.

## OTel Collector

Enable the `otel` compose profile to run a collector that forwards to
`POST /v1/ingest/otlp`. See `examples/otel-collector-config.yaml`.
