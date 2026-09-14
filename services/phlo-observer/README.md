# phlo-observer

Central ingestion, normalization, correlation, persistence and query service
for phlo-observe. Receives canonical events and external telemetry, preserves
raw source payloads, derives run projections and timelines, and serves queries.

## Quickstart

```bash
docker compose up postgres -d          # or point at any Postgres 14+
export PHLO_OBSERVER_DATABASE_URL=postgresql+asyncpg://phlo:phlo@localhost:5432/phlo_observer
export PHLO_OBSERVER_INGEST_TOKENS=dev-ingest-token
export PHLO_OBSERVER_READ_TOKENS=dev-read-token

phlo-observer migrate                  # apply schema
phlo-observer check                    # verify config + database
phlo-observer serve                    # http://localhost:8080
```

Without any tokens configured the service runs in dev mode: all endpoints are
unauthenticated. Set `PHLO_OBSERVER_AUTH_OPTIONAL_DEV=false` to hard-fail at
startup instead.

## API surface

| Route | Auth | Purpose |
|---|---|---|
| `POST /v1/events` | ingest | Canonical events: object or array, gzip ok |
| `POST /v1/ingest/dagster` | ingest | Dagster run/step/asset events |
| `POST /v1/ingest/dbt` | ingest | `run_results.json` documents |
| `POST /v1/ingest/dbt/artifacts` | ingest | `{"run_results": ..., "manifest": ...}` bundle |
| `POST /v1/ingest/generic` | ingest | Arbitrary JSON wrapped as `external.source_event` |
| `POST /v1/ingest/otlp` | ingest | Collector-forwarded OTLP JSON |
| `GET /v1/events` | read | Filter + cursor pagination |
| `GET /v1/events/{id}` | read | Single event |
| `GET /v1/runs`, `GET /v1/runs/{id}`, `GET /v1/runs/{id}/timeline` | read | Run projections |
| `GET /healthz`, `GET /readyz` | none | Liveness / readiness (DB + schema) |
| `GET /metrics` | read unless `metrics_public` | Prometheus exposition |

### Ingestion response

```json
{"accepted": 98, "rejected": 2, "duplicates": 1, "errors": [
  {"index": 14, "code": "SCHEMA_INVALID", "message": "event: Field required"}
]}
```

`202` when at least one event is stored or deduplicated; `422` when nothing
could be accepted; `413` when the body or batch exceeds limits.

### Idempotency

`event_id` is the dedup key. An identical resubmission is counted as a
duplicate and does not create a second row. A resubmission with a different
payload is rejected as `INTEGRITY_CONFLICT` — the stored row is never
overwritten.

## Development

```bash
uv sync --all-packages
uv run pytest services/phlo-observer/tests          # needs Postgres; see below
PHLO_OBSERVER_TEST_DATABASE_URL=postgresql+asyncpg://phlo:phlo@localhost:5433/phlo_test
```

Tests skip cleanly when Postgres is unreachable. Migrations live in
`migrations/` (Alembic, async engine); `phlo-observer migrate` runs them.

See `../../docs/` for architecture, configuration, deployment and
troubleshooting references.
