# phlo-observe

A coherent observability system for Phlo and related Python applications.

- **`observe-core`** — generic wide-event library
- **`phlo-observe`** — Phlo SDK (Dagster, dbt, DLT, Pandera, WAP, Iceberg/Nessie, Trino)
- **`phlo-observer`** — ingestion, normalization, correlation and query service

Start with the [architecture](architecture.md) overview or the
[V1 specification](V1_SPEC.md).

`phlo-observe` is an operational observability system. It may record decisions
such as WAP promotion for visibility, but V1 is not by itself a validated
authoritative electronic audit trail for GxP records — see
[architecture](architecture.md#audit-versus-observability).

The V2 evolution is implemented: see the [V2 specification](V2_SPEC.md) for
the design and [V2 features](v2.md) for the operator-facing surface — state
engine, relationship graph, baselines and insights, incidents, the `/v2`
query API, the typed `observe-query` client, SSE stream, alerting webhooks,
quarantine/replay, and archive/restore.
