# phlo-observe

A coherent observability system for Phlo and related Python applications.

V1 is intentionally split into three components:

- **`observe-core`** — generic wide-event observability, context propagation, structured errors, buffering, redaction, sampling and drains.
- **`phlo-observe-sdk`** — Phlo-specific contexts and integrations for Dagster, dbt, DLT, Pandera, WAP, Iceberg/Nessie and Trino.
- **`phlo-observer`** — central ingestion, normalization, correlation, persistence and query service for Observatory and downstream telemetry systems.

The implementation contract is in [`docs/V1_SPEC.md`](docs/V1_SPEC.md).

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
`/healthz`, `/readyz`, `/metrics`. Auth via `PHLO_OBSERVER_INGEST_TOKENS` /
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
tested (202 tests incl. real-Postgres + live-HTTP end-to-end) and CI-gated.
