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
| LISTEN backend killed server-side | Bridge detects the termination, backs off, re-listens; subsequent notifications still fan out (`test_notify_reconnects_after_connection_drop`) |

## 5. Correctness findings

The projection-equivalence harness compares every derived table after
incremental ingest vs after `rebuild-projections` on identical canonical
input, under four orderings — including `fold_state` bookkeeping, so a
reducer that agrees on current values but would diverge on the next late
event is still caught. It caught four real bugs (and the order-convergence
pass in §6c added the reducers that make the claim hold for late events
across request boundaries):

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

## 6a. Second-pass review: findings and fixes

A follow-up review of the shipped V2 surface found correctness, data-loss
and security gaps the first-pass suite did not cover. Every item below
was reproduced before fixing and now has a regression test.

### SDK (`observe-core`)

| Issue | Severity | Fix | Test |
| --- | --- | --- | --- |
| `TailSampler` emitted the bound-crossing event twice (buffered, then again directly) | data correctness | Bound hit emits the buffered chunk once and drops the event; buffering resumes for the next chunk | `test_tail_bound_event_is_not_duplicated` |
| `Runtime.flush()`/`shutdown()` never drained the metric aggregator — pending summaries died silently; `_Bucket.values` grew unbounded | data loss / memory | `flush()`/`shutdown()` call `emit_metric_summaries()`; samples use a bounded reservoir; `metric()` honors `flush_after_seconds` and falls back to immediate emission when the series cap rejects | `test_runtime_flush_drains_metric_summaries`, `test_aggregator_reservoir_is_bounded`, `test_metric_cadence_auto_flushes` |
| `WorkerBackend.emit` reported `spooled=True` even when `spool_event` failed | correctness | The spool result propagates; failed critical spooling reports dropped | `test_critical_spool_failure_reports_dropped` |
| Queue eviction scanned the whole queue and could evict flush/stop sentinels | robustness | Eviction scan bounded at 256 entries; control sentinels are never evicted | `test_eviction_scan_is_bounded`, `test_eviction_never_takes_sentinels` |
| `event_to_otlp_attributes` dropped `entities`/`tags`/`contract` — OTLP round-trips lost all V2 sections | data loss | Sections encode as `observe.entities`/`observe.tags`/`observe.contract` JSON attributes and decode symmetrically | `test_otlp_mapping_encodes_v2_sections`, `test_v2_sections_roundtrip_through_otlp` |
| `metric()` ignored ambient/operation correlation, entities and tags — summaries could not reach per-entity baselines | correctness | Series key includes correlation/entities/tags; summaries carry them through | `test_metric_summary_carries_correlation`, `test_aggregator_series_context_round_trips` |
| No ambient/explicit producer plumbing — server-derived ids used `service.name`, so SDK `run://dagster/<id>` and derived `run://<service>/<id>` split one run into two entities | correctness | `observe()`/`event()`/`metric()` take `producer=`; `bind_context(producer=...)` binds it ambiently; `_finalize` merges it into `source.producer` | `test_ambient_producer_namespaces_derived_ids`, `test_explicit_producer_wins_over_ambient` |

### Observer

| Issue | Severity | Fix | Test |
| --- | --- | --- | --- |
| `run_failures` ANDed a JSON-null check onto the outcome branch, dropping `outcome=failure` events with no error payload | correctness | `outcome == 'failure' OR error is a real object` — error-less failures are returned; JSON-null errors on successes stay excluded | `test_v2_failures_includes_errorless_failures`, `test_v2_failures_excludes_json_null_error` |
| `run_impact`/`get_run_v2`/`query_events` interpolated caller input into `LIKE` patterns unescaped | security / correctness | `_like_escape` escapes `\`, `%`, `_`; escaped `escape="\\"` on every caller-driven LIKE | `test_v2_impact_like_metachars_escaped`, `test_q_like_metachars_match_literally` |
| `run_changes` scanned a 30-day window into memory with no LIMIT | resource bound | Change-event filter moved into SQL; `_MAX_CHANGES` cap with a `truncated` flag | `test_v2_changes_reports_truncation_flag` |
| Ambiguous trace→run binding resolved nondeterministically (DISTINCT, no ORDER BY) | correctness | Ordered by `(trace_id, run_id)`; smallest run_id wins deterministically across replicas | `test_ambiguous_trace_smallest_run_wins`, `test_ambiguous_trace_stable_across_batches` |
| `rebuild_projections(batch_size)` was a dead parameter — one unbounded SELECT over all events | resource bound | Real keyset pagination on `(observed_at, event_id)` matching incremental fold order | `test_equivalence` suite |
| Scoped `--run` rebuild deleted and rewrote shared `Asset` rows from only the selected run's events, regressing `last_materialized_at` | data loss | Touched assets are re-derived from all events referencing them; edges merge `source_event_ids`/max confidence instead of deleting other runs' contributions | `test_scoped_rebuild_preserves_shared_asset`, `test_scoped_rebuild_merges_shared_edges` |
| `envelope_for` dropped `entities`/`tags`/`contract` — pushed source telemetry could not express V2 sections | data loss | All three pass through envelope construction; dbt adapter wires them | golden `dbt_run_results`/`dbt_artifacts_bundle` |
| `BatchState.load` selected every open insight and open incident per batch | resource bound | Scoped to entities the batch touches (`entity_id IN batch`, `jsonb_exists_any` on incident entities) | `test_batch_state_load_scopes_to_batch_entities`, `test_batch_state_load_includes_touched_open_rows` |
| Quality-failure rule missed `quality.validate` and `dbt.test.execute`; `dbt.invocation` never terminated runs | correctness | `_QUALITY_EVENTS` covers all three; `dbt.invocation` joins `_TERMINAL_RUN_EVENTS` | `test_dbt_test_failure_opens_quality_insight`, `test_quality_validate_failure_opens_insight`, `test_dbt_invocation_is_terminal` |
| Baselines used only `source.producer` for entity ids — diverged from `event_entities`' adapter/service-name fallbacks; `metric.summary` never fed baselines | correctness | Shared `event_producer` fallback chain; `observations_of` folds `metric.summary` `mean` | `test_metric_summary_feeds_baseline` |
| Retention deleted canonical events on producer-supplied `observed_at` — skewed producers could expire events early or pin them forever | correctness | Retention keys on `received_at` (server clock); `ix_events_received_at` index + migration 0005 | `test_retention_uses_received_at_not_observed_at` |
| `v2_event_provenance` always returned `"edge": []` | correctness | Relationship scan by `source_event_ids` containment reports contributing edges | `test_provenance_reports_edges` |
| Insight/incident transitions accepted any→any; reopening left stale `resolved_at` | correctness | Enumerated state machines (`expired` terminal, same-state idempotent, illegal → 409); leaving `resolved` clears `resolved_at` | `test_insight_illegal_transition_conflicts`, `test_incident_reopen_clears_resolved_at` |
| `admin_token_set` fell back to read tokens — a read credential silently had admin powers | security | No fallback: admin requires `ADMIN_TOKENS` once any token exists; `AUTH_OPTIONAL_DEV=false` requires all three scopes | `test_admin_requires_admin_token_not_read`, `test_strict_mode_requires_admin_tokens` |
| `archive` used OFFSET pagination over a live table; `reprocess` read unbounded and oversold its scope (it does not rebuild runs/insights/baselines) | correctness | Both keyset-paginate on `(received_at, event_id)`; docstrings state the real scope | `test_archive_restore_roundtrip`, `test_reprocess_window_runs` |

### phlo-observe-sdk / dbt normalization

| Issue | Severity | Fix | Test |
| --- | --- | --- | --- |
| `emit_run_results` set `invocation_id` but no `run_id` — pushed dbt telemetry produced no run row | data correctness | Invocation carries `run_id` + `run://dbt/<invocation_id>` entity, worst-result outcome, `elapsed_time` → `duration_ms`; per-result `execution_time` → `duration_ms`; tests link to models via `depends_on.nodes`; failing tests get error severity | `test_dbt_run_results_events`, `test_dbt_invocation_success_when_all_pass` |
| Integrations never stamped producer — derived run/branch entity ids fell back to `service.name` namespaces | correctness | dagster/dlt/trino/wap/iceberg/nessie scopes and emit calls pass `producer=`; generic helpers accept `run_id`/`asset_key`/`producer` | `test_integrations` producer assertions |

## 6b. Third-pass review: findings and fixes

A further review pass found six defects the second-pass suite still
missed; each now has a regression test or strengthened assertion.

| Issue | Severity | Fix | Test |
| --- | --- | --- | --- |
| `NotifyBridge` parked on `stop.wait()` after LISTEN was established — a dropped asyncpg connection never woke it, so the documented reconnect-on-drop path was unreachable and cross-instance SSE silently stayed dead until process restart | correctness / HA | The connection's termination listener races the stop event; a drop raises into the existing bounded-backoff loop. The bridge also names its backend `phlo-observer-notify-*` so it is visible/killable in `pg_stat_activity` | `test_notify_reconnects_after_connection_drop` |
| Incident grouping window was one-sided (`signal - last > 30min`): a stale insight (late event, replay) always attached to a current same-entity incident, and `_attach` rewound `last_signal_at` backwards — made worse because a dedupe-refreshed insight's `observed_at` stayed frozen at first detection | correctness | Symmetric `abs()` window; attach takes `max(signal, last_signal)`; refresh records `last_observed_at` (newest producing event, never rewound) so repeat findings carry a live signal | `test_stale_signal_never_joins_or_rewinds_incident`, `test_attach_never_rewinds_last_signal`, `test_refresh_advances_last_observed_at` |
| `_link_traces` in-batch resolution kept the *first* payload-order claimant while the DB path deliberately picks the smallest run_id — the same canonical events could bind differently by batch composition | correctness | Smallest run_id wins across in-batch claimants and is merged with committed rows — payload order no longer matters | `test_ambiguous_trace_smallest_run_wins` (both orders) |
| `test_partition_baselines_isolated` ended `assert (... or True)` — the partition-attribution check could never fail | test quality | Volume-spike insights now name the partitioned metric key in the title; the test asserts it and that partition B's baseline stayed untouched | `test_partition_baselines_isolated` |
| `workloads.dbt_invocation` diverged from the real SDK shape (`run_id` prefixed `dbt-`, invocation first with `outcome="unknown"`, synthetic `pipeline.run`) — every workload-driven dbt run produced a `runs` row no entity/edge could join back to | test fidelity | `run_id` is the bare invocation id and the terminal `dbt.invocation` lands last with the worst-result outcome and elapsed duration — the same shape `emit_run_results` emits | exercised by every `mixed_history` consumer |
| `WorkerBackend` eviction scanned via get/re-put, rotating the first 256 queued items to the tail on every eviction and racing a sentinel loss under concurrent producers | correctness | The scan runs in place on the queue's deque under its own mutex — survivors keep position, no pull/re-put race | `test_eviction_never_takes_sentinels` (order asserted), `test_eviction_scan_is_bounded` |

## 6c. Order convergence: incremental == rebuild under late events

A dedicated review pass attacked the core invariant directly: incremental
ingest folds events in *arrival* order (sorted within each request only),
while `rebuild-projections` folds in global `(observed_at, event_id)`
order. Any reducer whose writes are last-folded-wins diverges when an
event arrives late relative to its observed position — and two did,
verifiably:

- `apply_asset_event` set `status = "failing"` unconditionally on failure
  but only escalated to `healthy`/`recovering` on success — a late
  failure folded after a later-observed success left the asset `failing`
  while rebuild reported `recovering` (the semantically correct answer:
  the newest *observed* outcome was success).
- Branch entity attributes merged last-writer-wins by fold position — a
  late `wap.branch.create` regressed a `promoted` branch back to `open`.

The same mechanism reached baselines (last-200 window shifted by
arrival), insight verdicts (evaluated against the baseline at arrival
time, not at the event's observed position), insight resolution (a
finding folded after its own resolver duplicated or stayed open), and
incident grouping (windowed on wall-clock arrival).

The fix makes every order-sensitive reducer **event-time aware** by
keying each write on the event's canonical position
`(observed_at, event_id)`:

| Component | Mechanism |
| --- | --- |
| Runs, entities, assets, edges | `fold_state` JSONB column persists per-field write keys (`field_at`), derived-id keys (`derived_keys`) and run-start bookkeeping; a write only lands when the event's position exceeds the recorded key — last-*observed* wins, not last-arrived |
| Asset status | Derived from the newest materialization outcome by event position plus a bounded `materialize_times`/`last_failure` history, so `failing`/`recovering`/`healthy` converge regardless of fold order |
| Baselines | `samples` stores `[observed_at, event_id, value]` triples sorted by position; the last-200 window is the newest *observed* samples and duplicates never inflate `count` |
| Insights | A dedupe key's history is a chain of rows partitioned at resolver positions (`open_from_key`/`resolved_key`); findings attach to the interval their position falls in, resolvers repartition resolved rows retroactively, and a resolver folded before its finding still resolves it via a canonical-event scan |
| Incidents | Membership records `{iid, entity, key:[observed_at,event_id]}`; grouping evaluates membership as-of the signal's position, and a late critical finding that replay would have made the creator absorbs the incident (earliest member wins title/start) |
| Evaluation as-of | Insights evaluate against baseline/asset state reconstructed at the event's position, not at arrival — a late metric event is judged against what was known when it happened |

Migration `0006` adds the four `fold_state` columns and rewrites legacy
`baseline.samples` into keyed triples (anchored at `updated_at`, order
preserved). The equivalence snapshot now compares `fold_state` too, so a
run that happens to agree on current values but would diverge on the
next late event is still caught.

| Issue | Severity | Fix | Test |
| --- | --- | --- | --- |
| Asset status regressed on late failures (`failing` vs rebuild's `recovering`) | correctness | Event-position-keyed status derivation | `test_order_convergence` suite |
| Late `wap.branch.create` regressed `state` to `open` after promote | correctness | Per-field `field_at` gating on entity merges | `test_order_convergence` suite |
| Baseline last-200 window shifted by arrival order | correctness | Keyed samples sorted by event position | `test_order_convergence` suite |
| Insight evaluated against arrival-time baseline | correctness | As-of baseline prefix at the event's position | `test_equivalence` + `test_order_convergence` |
| Late finding duplicated/stayed open around an already-folded resolver | correctness | Resolver-partitioned insight chain with retroactive repartition | `test_order_convergence` suite |
| Late critical finding spawned a duplicate incident | correctness | Position-aware membership; earliest member absorbs | `test_order_convergence` suite |
| Partial token config failed open on ingest/read (only admin failed closed) | security | Once any token exists, every unset surface denies | `test_auth` partial-config tests |
| `_paged_events` accumulated full history in memory | resource bound | Pages fold incrementally; no full-history list | `test_equivalence` suite |
| `reprocess` held one transaction across the window | operability | Per-page transactions via keyset pagination | `test_reprocess_paginates_past_batch_size` |
| `NotifyBridge` reconnect-looped forever on non-asyncpg DSNs | robustness | Refuses to start on URLs asyncpg cannot LISTEN on | `test_bridge_disables_cleanly_on_non_asyncpg_dsn` |
| Investigation bundle reported `truncated` at exactly the cap | correctness | Over-fetch by one; `truncated` only when events were dropped | `test_v2_investigate_truncated_only_beyond_cap` |

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
  not `run_id` (use the structured `run_id` filter). Metacharacters
  (`%`, `_`, `\`) match literally. Adequate per spec §23 until volume
  thresholds in §24.4 force tsvector.
- **LISTEN/NOTIFY is at-most-once**: a replica disconnected during a
  commit misses that notification — the bridge detects the drop and
  re-listens (bounded backoff, proven via `pg_terminate_backend`), but
  nothing replays what was missed. SSE is explicitly a hint channel —
  clients must re-fetch on reconnect (documented contract).
- **Notification payload cap**: batches producing > 7 KB of stream
  messages collapse to a single `refresh` hint; local delivery is
  unaffected, remote replicas get a refetch signal instead of the
  message list.
- **Timeline cap**: runs over 10k events return `truncated: true`; the
  full history remains in canonical events and paged queries.
- **Rebuild is offline for insights/incidents** — a full rebuild deletes
  and replays insight/incident state; brief unavailability of open
  findings during a rebuild is acceptable per the rebuild-runbook. The
  scoped `--run` rebuild is online-safe: it merges entity/edge
  contributions and re-derives touched assets from their full history.
- **`reprocess` does not rebuild runs/insights/baselines** — it re-folds
  a `received_at` window into entity/asset/edge state only; use
  `rebuild-projections` for derived-state changes.
- Single Postgres remains the system of record; no sharding story yet.

## 9. Operational recommendations

- Run >= 2 replicas behind a balancer — HA ingest, dedup and cross-instance
  SSE are proven. `stream_notify` defaults on; leave it on.
- Configure all three token scopes (`INGEST_TOKENS`, `READ_TOKENS`,
  `ADMIN_TOKENS`) outside dev: once any token exists every unset scope
  fails closed, and `AUTH_OPTIONAL_DEV=false` refuses to boot without
  all three.
- Keep `raw_event_ttl_hours` and retention windows configured; retention
  is lock-serialized and safe to leave scheduled on all instances. Event
  expiry follows `received_at`, so producer clock skew cannot pin or
  prematurely drop rows.
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
