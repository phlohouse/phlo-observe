# Event model

Every event is a canonical envelope (`schemas/event-envelope-v1.schema.json`):

```json
{
  "schema_version": "1.0",
  "event_id": "uuidv7",
  "event": "asset.materialize",
  "category": "data",
  "outcome": "success",
  "severity": "info",
  "delivery": "telemetry",
  "started_at": null,
  "ended_at": null,
  "duration_ms": null,
  "observed_at": "2025-01-01T00:00:00.000Z",
  "service": {"name": "...", "version": "...", "environment": "..."},
  "correlation": {"run_id": "...", "trace_id": "...", "asset_key": "..."},
  "attributes": {},
  "error": {"message": "...", "exception_type": "...", "code": "..."},
  "source": {"producer": "...", "adapter": "..."},
  "entities": {"run": "run://dagster/01J...", "asset": "asset://a/b"},
  "tags": {"partition": "2026-01-01"},
  "contract": {"name": "orders-contract", "version": 2}
}
```

`entities`, `tags` and `contract` are the V2 sections (schema 2.0);
they survive SDK emission, adapters, OTLP round-trips and persistence.

## Field semantics

- `event_id` — UUIDv7, generated client-side, monotonic per process.
- `observed_at` — when the event happened at the producer.
- `received_at` — when the observer stored it (server-side only). This is
  the retention clock: producer clock skew cannot expire an event early
  or keep it past its window.
- `started_at`/`ended_at`/`duration_ms` — set for operations; null for
  instantaneous events.
- `outcome` — `success | failure | partial | cancelled | timeout | unknown`.
- `severity` — `debug | info | warn | error | critical`.
- `delivery` — `debug` (droppable), `telemetry` (normal), `critical`
  (never sampled; eligible for local spool + replay).

## Naming

`domain.action` — `pipeline.run`, `pipeline.step`, `asset.materialize`,
`quality.validate`, `quality.check`, `wap.branch.create`, `wap.validate`,
`wap.promote`, `wap.reject`, `dbt.model.execute`, `dbt.test.execute`,
`iceberg.commit`, `nessie.commit`, `external.source_event`.

## Errors

`error` carries `code`, `message`, `why`, `fix`, `exception_type`,
`retryable`, `stacktrace`, `details` — a structured shape designed for both
humans and tooling. Application exception *objects* are never serialized;
only their type and message.

## Redaction

Attribute keys matching secret patterns (`*password*`, `*token*`, `*secret*`,
`*key*`, `authorization`, ...) are replaced recursively before any drain or
the spool sees the event. Configure additional keys with `OBSERVE_REDACT_KEYS`.

## Ordering

The observer orders timelines by `observed_at` with `event_id` (UUIDv7) as the
stable tie-breaker, so events produced in the same millisecond still sort
deterministically.
