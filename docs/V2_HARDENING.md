# V2 Hardening Report

Evidence gathered while production-proving the V2 implementation: what was
tested, what broke, what was fixed, and what remains unproven.

All numbers below are from `uv run pytest tests/performance -s` and the
observer suite against Docker-Compose PostgreSQL 16 on an Apple-silicon
laptop. CI runners are slower; the relative magnitudes are what matter.

## 1. Scenarios tested

| Area | Scenario | Where |
| --- | --- | --- |
| Workloads | Seeded `tests/workloads.py`: Dagster runs (steps, partitions, checks, retries), DLT loads, dbt models/tests, WAP branch lifecycle, Trino queries, metric samples, late/out-of-order arrivals, duplicates | `tests/workloads.py` |
| Equivalence | Incremental ingest vs `rebuild-projections` over the same canonical events, in-order, shuffled, late-moved-to-end, and duplicated orderings | `test_equivalence.py` |
| Stress | 5000-event sustained mixed ingest, 1094-event burst, 8 concurrent producers, concurrent writers to one run, 10k-event run timeline, two-instance dedup, rebuild during ingest | `tests/performance/test_stress.py` |
| SDK | emit/observe overhead, enqueue under pressure, serialize, HTTP drain, dead exporter | `tests/performance/test_sdk_overhead.py` |
| Failure injection | aborted transaction, projection-failure fail-open, DB down at ingest/readiness, pool exhaustion, retention during ingest, restart dedup, unknown schema version, late events | `test_resilience.py`, `test_db_failure_injection.py` |
| Correlation | explicit run_id/trace_id/invocation linking, declared-entity edges, ambiguous telemetry left uncorrelated | `test_correlation.py` |
| Insights | each deterministic rule on controlled histories; warm-up, dedup, resolution, partition baselines, lifecycle transitions | `test_insight_quality.py` |
| HA | two replicas on one database: concurrent ingest, dedup, advisory-locked retention, cross-instance SSE via `LISTEN/NOTIFY` | `test_stress.py`, `test_notify.py` |
| Lifecycle | archive -> delete -> restore -> rebuild round-trip, retention at volume, V1 envelopes | `test_lifecycle.py` |
| Observatory | the ten operational questions answered through the public API only | `test_observatory.py` |
| Agent readiness | read-only client surface, enforced caps, evidence IDs, truncation flags | `test_agent_readiness.py` |
| Neutrality | Keystone-shaped experiment/assay/metadata/report/export flows on `observe-core` | `test_keystone_neutral.py` |

## 2. Benchmark methodology

- **Observer ingest**: POST fixed-size batches of canonical envelopes to
  `/v1/events` via httpx `AsyncClient` on an ASGI transport, against real
  PostgreSQL. Throughput = accepted events / wall time. Statement counts
  come from a SQLAlchemy `before_cursor_execute` listener per batch.
- **SDK**: wall-clock around `observe()`/`event()` with a `MemoryDrain`;
  dead-exporter runs point the HTTP drain at a closed client.
- **Timeline**: a single run with 10,000 events queried through
  `/v1/runs/{id}/timeline`.
- Everything is seeded and deterministic; percentile runs repeat the same
  batch shape rather than averaging over mixed shapes.

## 3. Measured results

### Observer (local, Postgres 16)

| Path | Throughput | Batch latency | Notes |
| --- | --- | --- | --- |
| Trivial envelopes, varied runs | 7,640 events/s | ~65 ms / 500 | pre-V2 path |
| Trivial envelopes, one run | 6,036 events/s | ~83 ms / 500 | per-run lock contention only |
| **Realistic mixed workload** | **692 events/s** | p50 703 ms, p95 715 ms | 64 statements per 500-event batch, ~9 MB peak alloc |
| Burst (1094 events) | — | p50/p95/p99 105 ms / 500 | back-to-back batches |
| Timeline, 10k-event run | — | p50 348 ms, p95 362 ms | bounded at 10k events |

The realistic workload is ~11x slower per event than trivial envelopes —
that is the cost of baselines, insight evaluation, incident grouping and
asset folds. After batching, ingest costs ~1.4 ms/event and ~0.13
statements/event. The earlier per-event-query implementation measured
343 events/s and 931 statements per batch on the same workload.

### SDK (`observe-core`, local)

| Operation | Median | p95 |
| --- | --- | --- |
| `observe()` end-to-end | 56.8 µs | 71.9 µs |
| `event()`, scalar attributes | 52.0 µs | — |
| `event()`, nested attributes | 68.3 µs | — |
| context `bind()` + `observe()` | 58.5 µs | — |
| enqueue under queue pressure | 41.2 µs | — |
| emit with dead exporter | 46.4 µs | — |
| finalize + serialize | ~23,000 events/s | — |
| HTTP drain (batched) | ~1.67 M events/s | — |

Application-side overhead stays in the 40–70 µs range per event even when
the observer is unreachable — exporter failure is fully isolated to the
drain worker. No Python bottleneck justifies a Rust path at these numbers.

## 4. Failure-injection results

| Injection | Result |
| --- | --- |
| Transaction aborted mid-batch | No partial state; canonical events atomic per request (`test_aborted_transaction_leaves_no_partial_state`) |
| Projection pass raises | Events stay durable; failure logged, request still accepted (`test_projection_failure_is_fail_open`) |
| PostgreSQL down at ingest | Error, not hang; readiness reports unhealthy |
| Connection pool exhausted | Fails fast and recovers (`test_pool_exhaustion_fails_fast_and_recovers`) |
| Retention during ingest | Advisory lock serializes; second pass reports `skipped` |
| Observer restart | State and dedup survive (`test_restart_preserves_state_and_dedup`) |
| Unknown `schema_version` | Request rejected 422 with per-event errors |
| Late-arriving run events | Run projection reconstructs correctly |
| Duplicate event ids | Identical payloads counted `duplicate`; conflicting payloads rejected as conflicts |
| Exporter down (SDK side) | Emit stays ~46 µs; failures isolated to drain worker |
| Cross-instance SSE | Subscriber on replica B sees events committed via replica A (`test_notify_reaches_other_replica`) |

## 5. Correctness findings

The projection-equivalence harness compares every derived table after
incremental ingest vs after `rebuild-projections` on identical canonical
input, under four orderings. It caught four real bugs:

1. **Provenance cap asymmetry** — incremental merged `derived_from` at 32
   entries, rebuild at 64. Unified via the shared state-engine bound.
2. **Wall-clock incident grouping** — `group_insight` windowed on
   `utcnow()`, so replaying a week of history grouped every insight into
   one window. Incidents now group on the producing event's `observed_at`.
3. **Batch-local asset state in insight evaluation** — ingest applied the
   whole batch's asset folds before insights ran, so a materialization in
   the same batch leaked into an earlier event's freshness check. Insight
   evaluation now sees per-asset state as of each event, in observed-time
   order, identical on ingest and rebuild paths.
4. **Same-batch duplicate event ids** — two identical events in one
   request attached two ORM instances with the same primary key
   (SQLAlchemy identity conflict). Deduplicated before `session.add_all`.

Plus one query-side correctness fix found by the agent-readiness tests:

5. **`run_failures` scanned a bounded event list** — filtering failures in
   Python after a capped fetch could miss failures past the cap. Now
   filtered in SQL; also excludes events whose `error` column holds JSON
   `null` rather than a real error object.

## 6. Issues discovered and fixes made

| Issue | Severity | Fix |
| --- | --- | --- |
| Insight/baseline/incident pass ran per-event queries (931 stmts/batch, 343 events/s) | high | `insights.BatchState` preloads baselines, open insights, open incidents and asset state once per batch; ingest and rebuild share `step()` |
| Entity/relationship/run folds were per-event | high | `apply_events_batch` accumulates in memory, flushes once per table |
| Trace-only events each queried for a run link | medium | `_link_traces` preloads all batch trace->run bindings in one DISTINCT query; same-batch bindings preserved |
| `run_events`/`investigate` unbounded | medium | 10k cap, `truncated` flag on the bundle |
| `run-duration` baselines keyed per-run | medium | keyed `job://{producer}/{job}` so repeated runs accumulate samples — duration-regression now fires |
| SSE was instance-local: load-balanced live tails missed other replicas' events | medium | `notify.py`: one `pg_notify` per ingest batch (in-transaction, fires only on commit); `NotifyBridge` republishes foreign messages into each local `StreamHub`; origin tag prevents echo |
| Retention ran on every replica | low | `pg_try_advisory_xact_lock` serializes; losers report `skipped` |

## 7. Insight quality

On controlled histories (`test_insight_quality.py`, 10 tests): each rule
fires when it should and stays quiet when it shouldn't; baseline warm-up
suppresses findings until minimum samples accumulate; partition-aware
baselines don't cross-contaminate; open insights deduplicate by
`(rule, entity)` and resolve on subsequent healthy events; incident
grouping follows event-time windows. No false positives observed on the
seeded healthy segments.

## 8. Remaining limitations

- **`q` free-text search** covers event name and table/asset columns but
  not `run_id` (use the structured `run_id` filter). Adequate per spec
  §23 until volume thresholds in §24.4 force tsvector.
- **LISTEN/NOTIFY is at-most-once**: a replica disconnected during a
  commit misses that notification. SSE is explicitly a hint channel —
  clients must re-fetch on reconnect (documented contract).
- **Notification payload cap**: batches producing > 8 KB of stream
  messages have the notify packed to the largest fitting prefix; local
  delivery is unaffected, remote replicas may drop the tail of very
  insight-heavy batches.
- **Timeline cap**: runs over 10k events return `truncated: true`; the
  full history remains in canonical events and paged queries.
- **Rebuild is offline for insights/incidents** — a full rebuild deletes
  and replays insight/incident state; brief unavailability of open
  findings during a rebuild is acceptable per the rebuild-runbook.
- Single Postgres remains the system of record; no sharding story yet.

## 9. Operational recommendations

- Run >= 2 replicas behind a balancer — HA ingest, dedup and cross-instance
  SSE are proven. `stream_notify` defaults on; leave it on.
- Keep `raw_event_ttl_hours` and retention windows configured; retention
  is lock-serialized and safe to leave scheduled on all instances.
- Alert on `phlo_observer_ingest_errors_total` and quarantine depth, not
  on individual 422s (per-event errors are normal producer noise).
- Treat SSE consumers as hint-driven: reconnect + refetch, never depend
  on message delivery for correctness.
- Backup = standard `pg_dump`/PITR; restore then `rebuild-projections`.
  RPO is your Postgres backup interval; RTO is restore time plus a
  rebuild measured at minutes for tens of thousands of events.
- Watch `stmts/batch` via slow-query logs if ingest latency regresses —
  per-event round trips are the historical failure mode.

## 10. Evidence on architecture alternatives

- **Rust**: no. SDK emit overhead is 40–70 µs with a dead exporter;
  serialization runs ~23k events/s single-threaded. Nothing measured
  approaches a CPU-bound wall.
- **Kafka/Redpanda**: no. `pg_notify` delivers the demonstrated
  requirement (cross-instance SSE fan-out) with zero new infrastructure;
  notification loss is handled by the SSE hint contract, not a bus.
- **ClickHouse / columnar store**: no evidence yet. Ingest at 692
  events/s realistic on laptop Postgres; production Phlo event volumes
  (low thousands/minute) are far below the measured headroom. Revisit if
  sustained ingest exceeds ~5k events/s or query latency on `events`
  degrades past observatory-interactive thresholds (~500 ms).
- **ML anomaly detection**: out of scope; deterministic rules produce
  useful signal with evidence, and every finding is auditable.
