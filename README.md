# phlo-observe

A coherent observability system for Phlo and related Python applications.

V1 is intentionally split into three components:

- **`observe-core`** — generic wide-event observability, context propagation, structured errors, buffering, redaction, sampling and drains.
- **`phlo-observe-sdk`** — Phlo-specific contexts and integrations for Dagster, dbt, DLT, Pandera, WAP, Iceberg/Nessie and Trino.
- **`phlo-observer`** — central ingestion, normalization, correlation, persistence and query service for Observatory and downstream telemetry systems.

The implementation contract is in [`docs/V1_SPEC.md`](docs/V1_SPEC.md).

## Status

Pre-implementation specification phase.
