# Troubleshooting

## Events not appearing

1. `GET /readyz` — is the database reachable and migrated?
2. `GET /metrics` — check `phlo_observer_ingest_events_total{status="rejected"}`
   and `phlo_observer_normalization_total{status="error"}`.
3. Inspect `raw_events` rows with `normalization_status='failed'` — the
   `normalization_error` column says why the adapter rejected the payload.

## `422` on `/v1/events`

The response body lists per-item `errors` with `index` and `code`
(`SCHEMA_INVALID`, `INTEGRITY_CONFLICT`, `BATCH_TOO_LARGE`). Conflicts mean an
`event_id` was resubmitted with different content — investigate the producer;
the original row was preserved.

## `413`

Body exceeds `PHLO_OBSERVER_MAX_BODY_BYTES` (compressed size counts), or the
batch exceeds `PHLO_OBSERVER_MAX_BATCH_EVENTS`. Producers should batch under
the limit — the observe-core HTTP drain already splits on 413.

## Duplicate events

`duplicates` in the ingest response counts identical resubmissions — normal
for retries. `phlo_observer_duplicate_events_total` tracks it over time.

## Runs stuck in `unknown`/`running`

Runs only reach a terminal status when a `pipeline.run` event with a terminal
outcome arrives. Late events still join the run and update counters, but a run
whose terminal event was never sent stays `running` — check the producer's
flush path (queue depth, drain errors, `OBSERVE_TELEMETRY_REQUIRED`).

## Database schema mismatch

`/readyz` reports `schema_version` from `alembic_version`. If it differs from
the packaged head, run `phlo-observer migrate`. If the table is missing
entirely, `schema_version` is `null`.

## High ingestion latency

Watch `phlo_observer_persist_duration_seconds` and Postgres. Each ingest
request does raw-store + normalize + upsert in one transaction; concurrent
ingestion of the same `event_id` serializes on the primary key — that is the
dedup mechanism working, not a bug.
