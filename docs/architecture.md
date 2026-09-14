# Architecture

## Component diagram

```text
instrumented apps ─┐
  observe-core     │  canonical events (POST /v1/events)
dagster webhooks ──┼── POST /v1/ingest/dagster
dbt artifacts ─────┤   POST /v1/ingest/dbt[/artifacts]
OTel collector ────┤   POST /v1/ingest/otlp
other sources ─────┘   POST /v1/ingest/generic
                          │
                   phlo-observer (FastAPI)
                          │ raw payload -> raw_events
                          │ adapter -> canonical events
                          │ correlate -> runs projection
                          ▼
                     PostgreSQL ──> /v1/events, /v1/runs, /v1/runs/{id}/timeline
                          │
                     optional OTLP forward
```

## Application event lifecycle

```text
observe()/event() call
  -> EventBuilder merges ambient context
  -> normalize attribute values
  -> enrichers
  -> redact sensitive keys
  -> limits check (max size)
  -> delivery-class sampling
  -> bounded queue  (never blocks the caller by default)
  -> background worker batches
  -> drains: console / JSONL / HTTP(observer) / OTLP
  -> critical events also append to local spool; replayed on next flush
```

## External telemetry ingestion

```text
source payload
  -> store raw_events row (payload, sha256, adapter, expires_at)
  -> adapter.normalize() -> canonical dicts + per-item errors
  -> persist_events() (idempotent on event_id, conflict detection)
  -> link trace->run, update run projection
  -> optional OTLP forward (fire-and-forget)
```

## Correlation precedence

1. explicit `correlation.run_id` (confidence 1.0)
2. `trace_id` — inherits `run_id` from a sibling event already correlated
3. `invocation_id` — recorded; joined only when the producer also sends run_id

Ambiguous events remain uncorrelated; they are never guessed into a run.

## Deployment

```text
internet -> LB -> phlo-observer (n>=1, stateless) -> PostgreSQL
                              \-> /metrics scraped by Prometheus
                              \-> optional: OTel Collector profile ships logs
```

Each replica runs an hourly retention sweep; sweeps are idempotent deletes so
overlapping runs are safe.
