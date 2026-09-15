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
