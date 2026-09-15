# Integrations

## Sending events from application code

```python
from phlo_observe import pipeline_run, asset_materialize, quality_validate

with pipeline_run(job="nightly_etl", run_id="dagster:abc123"):
    with asset_materialize(asset_key="mart.fct_orders"):
        ...
    with quality_validate(suite="mart", checks_total=12, checks_passed=12):
        ...
```

Or emit canonical events straight to the observer over HTTP:

```python
from observe_core.config import HttpDrainConfig, ObserveSettings
from observe_core.runtime import configure

configure(
    ObserveSettings(
        service_name="my-service",
        drains=[
            HttpDrainConfig(
                endpoint="http://observer:8080/v1/events",
                api_key="<ingest-token>",
            )
        ],
    )
)
```

## Dagster

- Client-side: `phlo_observe.integrations.dagster.dagster_run_scope()`
  attaches Phlo run/asset context to a Dagster run; `dagster_step()`,
  `emit_materialization()` and `emit_asset_check()` cover steps, assets and
  checks.
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

`phlo_observe.integrations.wap` + `iceberg` emit `wap.branch.create`,
`wap.promote`, `wap.reject`, and `iceberg.commit` / `nessie.commit` events
correlated on `branch` and `snapshot_id`.

## DLT / Pandera

`integrations.dlt` wraps pipeline run events; `integrations.pandera` maps
schema-validation failures to `quality.validate` events with structured
errors.

## OTel Collector

Enable the `otel` compose profile to run a collector that forwards to
`POST /v1/ingest/otlp` (OTLP/HTTP JSON; each log record normalizes to one
canonical event). See `examples/otel-collector-config.yaml`.

### OTLP attribute mapping

Both the `observe-core` OTLP drain and the observer's OTLP forwarder encode a
canonical event as an OTel log record: the event name is the record `body`,
`severity` maps to `severityNumber`/`severityText`, and the rest of the
envelope is carried as `observe.*` record attributes (shared encoder:
`observe_core.otlp_mapping`):

| OTLP attribute | Canonical field |
| --- | --- |
| `observe.event_id`, `observe.event`, `observe.schema_version` | envelope identity |
| `observe.category`, `observe.outcome`, `observe.severity`, `observe.delivery` | classification |
| `observe.started_at`, `observe.ended_at`, `observe.duration_ms`, `observe.observed_at` | timing |
| `observe.correlation.<key>` | every canonical correlation key |
| `observe.correlation.extra` | non-canonical correlation (JSON) |
| `observe.trace_id`, `observe.span_id` | trace-join shortcuts |
| `observe.service`, `observe.error`, `observe.source`, `observe.attributes` | structured sections (JSON) |

The `/v1/ingest/otlp` adapter restores these attributes: a record carrying
`observe.event_id` re-enters as the original canonical event — same
`event_id` (so re-ingest dedupes), full correlation (`run_id`, trace/span
and friends), timing, service, error and attributes. Records without
`observe.*` attributes are normalized generically: `traceId`/`spanId` map to
correlation, `event.name` becomes the event name, and remaining attributes
land under `attributes` (resource attributes under `resource.*`).

## Ingestion headers

All `POST /v1/ingest/*` endpoints accept:

- `X-Source-Version` — producer version, stored on `raw_events.source_version`
  (defaults to the adapter's own version when absent);
- `X-Run-Id`, `X-Trace-Id`, `X-Request-Id`, `X-Asset-Key` — transport-level
  correlation passed to adapters via `RawPayload.metadata`; the generic
  adapter merges them over body-level keys so generic events still join runs.
