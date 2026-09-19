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
  -> EventBuilder merges operation + ambient context
  -> normalize attribute values
  -> enrichers
  -> redact sensitive keys
  -> limits check (max size)
  -> delivery-class sampling
  -> bounded queue  (never blocks the caller by default)
  -> background worker batches
  -> drains: console / JSONL / HTTP(observer) / OTLP
  -> critical events also append to local spool; replayed on next flush

Projection rebuilds preserve insight and incident identities by matching the
complete producer and membership timelines under the shared projection lock.
Operator lifecycle decisions are journaled separately from derived rows. If a
manual history can map to more than one regenerated episode (or vice versa),
the rebuild fails and rolls back rather than guessing; capped or expired
evidence is retained conservatively for operator review.
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

## Run correlation flow

```text
event arrives
   │ correlation.run_id present?
   ├─ yes ─> correlate to run (method: explicit_run_id, confidence 1.0)
   └─ no
      │ trace_id already carries a run_id on an earlier sibling event?
      ├─ yes ─> inherit that run_id (method: trace_id)
      └─ no  ─> stays uncorrelated
                 (invocation_id is recorded for observability but never
                  joins a run on its own — no proximity guessing)

correlated events -> upsert run projection row
                     (first seen_at / last seen_at / counts / outcome rollup)
```

Correlation precedence:

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

## Audit versus observability

`phlo-observe` is an operational observability system. It may record decisions
such as WAP promotion for visibility, but V1 is not by itself a validated
authoritative electronic audit trail for GxP records.

If regulated workflows later depend on it as a system of record, separate
requirements for immutability, identity, validation, retention, review,
electronic signatures and change control are required.
