# Performance

`tests/performance/` measures overhead rather than assuming Python is fast
enough. Each benchmark asserts the V1 target from the spec; the measured
baselines below were recorded on an Apple-silicon macOS developer machine
(Python 3.12, PostgreSQL 16 in Docker, `asyncpg`) with `-s` output.

## Reproduce

```bash
docker compose up -d postgres
uv run pytest tests/performance -s
```

Observer benchmarks use `PHLO_OBSERVER_TEST_DATABASE_URL` (default
`postgresql+asyncpg://phlo:phlo@localhost:5432/phlo_observer_test`) and skip
when Postgres is unreachable.

## Measured baselines vs spec targets

| Benchmark | Spec target | Measured |
|---|---|---|
| `observe()` bookkeeping (empty block) | median < 100µs | median ~51µs, p95 ~71µs |
| Event emit, 10 scalar attributes | median < 100µs | median ~45µs |
| Event emit, nested attributes | median < 100µs | median ~61µs |
| Enqueue, normal event | p95 < 1ms; sustain >= 10,000/s | p95 ~50µs, ~22,500/s |
| Finalize + serialize | regression floor >= 2,000/s | ~26,300 events/s |
| Batch HTTP drain (mocked transport) | regression floor >= 1,000/s | ~1.6M events/s |
| Observer canonical ingest | sustain >= 1,000/s to PostgreSQL | ~9,270 events/s |
| Observer ingest, one shared run | regression floor >= 500/s | ~1,430 events/s |
| Run timeline query, 10,000 events | p95 < 500ms | p50 ~352ms, p95 ~379ms |

Numbers vary with hardware, Postgres placement, and batch shape — treat them
as baselines, not guarantees. The asserts encode the spec targets (plus a
deliberately loose floor where the spec has no target) so a real regression
fails CI; the printed values exist so the table can be refreshed per release.

## Notes

- The application path performs no remote I/O: `observe()` overhead covers
  build/normalize/redact/enqueue only; drain I/O happens on worker threads.
- "One shared run" ingest is slower because every event takes the run-row
  lock — that is the intended cost of correct projection counters, and the
  floor guards it rather than the 1,000/s spec target.
- If a target is missed, profile before reaching for Rust (spec §84): the
  expected culprits are database/network configuration, not Python CPU.
