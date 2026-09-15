# phlo-observe

A coherent observability system for Phlo and related Python applications.

V1 is intentionally split into three components:

- **`observe-core`** — generic wide-event observability, context propagation, structured errors, buffering, redaction, sampling and drains.
- **`phlo-observe-sdk`** — Phlo-specific contexts and integrations for Dagster, dbt, DLT, Pandera, WAP, Iceberg/Nessie and Trino.
- **`phlo-observer`** — central ingestion, normalization, correlation, persistence and query service for Observatory and downstream telemetry systems.

The implementation contract is in [`docs/V1_SPEC.md`](docs/V1_SPEC.md).

## First event in under five minutes

`observe-core` needs nothing but Python 3.12+ — no database, no server:

```bash
pip install observe-core
```

```python
from observe_core import configure, observe

configure(service_name="demo")

with observe("demo.work") as evt:
    evt.set(rows_loaded=1000)
```

```text
2025-01-01T00:00:00.000Z INFO     demo.work                        success 0ms service=demo
```

One wide event per operation, printed by the console drain by default. Add an
`HttpDrainConfig` to point the same call at the observer below.

## Quickstart

```bash
mise install          # pinned Python + uv
uv sync --all-packages

docker compose up -d postgres
export PHLO_OBSERVER_DATABASE_URL=postgresql+asyncpg://phlo:phlo@localhost:5432/phlo_observer

phlo-observer migrate && phlo-observer serve
# ingest a canonical event
curl -X POST localhost:8080/v1/events -H 'content-type: application/json' -d "{
  \"schema_version\":\"1.0\",\"event\":\"pipeline.run\",\"category\":\"pipeline\",
  \"event_id\":\"$(uuidgen)\",\"outcome\":\"success\",\"severity\":\"info\",
  \"delivery\":\"telemetry\",\"observed_at\":\"2025-01-01T00:00:00Z\",
  \"service\":{\"name\":\"demo\"},\"correlation\":{\"run_id\":\"demo-1\"},
  \"attributes\":{},\"error\":null,\"source\":{\"producer\":\"demo\"}
}"
```

Instrumented apps emit through `observe-core` (see `examples/`); the observer
normalizes, correlates and serves `GET /v1/events`, `/v1/runs/{id}/timeline`,
`/health/live`, `/health/ready`, `/metrics`. Auth via `PHLO_OBSERVER_INGEST_TOKENS` /
`PHLO_OBSERVER_READ_TOKENS` (bearer or `X-API-Key`).

## Repository layout

- `packages/observe-core` — generic library (context, queue, drains, spool)
- `packages/phlo-observe-sdk` — Phlo contexts + integrations
- `services/phlo-observer` — FastAPI + Postgres service + Alembic migrations
- `schemas/` — canonical JSON Schemas
- `tests/{contract,integration,performance}` — cross-component suites
- `docs/` — architecture, event model, configuration, deployment, troubleshooting

## Status

V1 implemented: core pipeline, SDK integrations, and the observer service are
tested (250+ tests incl. real-Postgres + live-HTTP end-to-end) and CI-gated.

## Audit versus observability

`phlo-observe` is an operational observability system. It may record decisions
such as WAP promotion for visibility, but V1 is not by itself a validated
authoritative electronic audit trail for GxP records.

If regulated workflows later depend on it as a system of record, separate
requirements for immutability, identity, validation, retention, review,
electronic signatures and change control are required.
