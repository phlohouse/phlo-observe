# phlo-observe V2 Specification

**Status:** Proposed future architecture

**Audience:** Phlo maintainers, platform engineers, junior developers implementing scoped work packages, reviewers, and future contributors.

**Relationship to V1:** V2 is an evolution of the V1 system. It must not invalidate V1's core contracts unless a migration is explicitly described here. V1 remains the implementation baseline and production proving ground.

---

# 1. Purpose

V1 establishes a coherent, Python-first observability system for Phlo:

1. `observe-core` — generic wide-event SDK and runtime;
2. `phlo-observe-sdk` — Phlo-specific context, semantics, and framework integrations;
3. `phlo-observer` — central ingestion, normalization, correlation, persistence, and query service.

V2 should not simply add more integrations or UI fields. Its purpose is to make observability **operationally intelligent, horizontally scalable, fleet-wide, agent-consumable, and capable of learning from the telemetry it already has**.

The central V2 idea is:

> V1 records and correlates what happened. V2 should understand relationships, derive useful state, detect abnormal behaviour, support automated investigation, and remain dependable at materially higher scale.

V2 is therefore about five major advances:

- richer semantic correlation;
- derived and adaptive telemetry;
- platform-wide observability ingestion;
- agent- and automation-facing observability;
- scale, resilience, and performance hardening.

---

# 2. Non-goals

V2 must **not** become any of the following:

- a generic replacement for OpenTelemetry;
- a generic APM vendor;
- a generic metrics database;
- a generic SIEM;
- a log-storage product;
- a tracing backend;
- a replacement for Dagster's own execution model;
- a replacement for dbt artifacts;
- a replacement for Prometheus, Loki, Tempo, or equivalent backends;
- a compliance audit-trail system by default;
- an LLM chatbot bolted onto logs;
- a reason to rewrite working Python code in Rust without profiling evidence.

Phlo Observe should remain opinionated around **data platform operations and the Phlo execution model**.

---

# 3. V2 entry criteria

V2 work should begin only after V1 has demonstrated all of the following in real use.

## 3.1 Minimum production evidence

Before V2 implementation starts, V1 should have:

- at least 90 days of continuous use in at least one real Phlo deployment;
- telemetry from at least Dagster, dbt, DLT, WAP/promotion logic, and one infrastructure source;
- at least 1 million persisted canonical events, unless deployment volume is genuinely lower;
- at least 100 complete multi-stage pipeline runs visible end-to-end;
- known event-volume distributions per source;
- measured p50, p95, and p99 observer ingest latency;
- measured SDK overhead under realistic workload;
- measured storage growth per day;
- measured query latency for common Observatory views;
- a documented list of the 20 most common investigation queries performed by users;
- a documented list of V1 correlation failures or ambiguities;
- a documented list of any telemetry users ignore because it is too noisy or low-value.

## 3.2 Required V1 quality state

V2 must not be used to avoid finishing V1 reliability work.

Before V2 begins:

- V1 critical-event delivery must be reliable;
- schema migration processes must be established;
- event versioning must be proven;
- retention jobs must operate reliably;
- redaction must be validated;
- the observer API must have stable authentication;
- CI must include compatibility tests across supported package versions;
- SDK failures must never fail a data pipeline by default;
- event ingestion must be idempotent.

## 3.3 Evidence-driven optional work

The following V2 items are conditional and must not be implemented merely because they appear in this document:

- Rust runtime or Rust event processing;
- Kafka/Redpanda or another durable event bus;
- ClickHouse or another dedicated analytical event store;
- eBPF-based infrastructure observation;
- automatic anomaly detection models;
- LLM-powered incident summaries.

Each has explicit graduation criteria later in this specification.

---

# 4. V2 design principles

## 4.1 Preserve wide events

The wide-event model remains the primary application-facing abstraction.

Do not regress to many small log lines merely because the backend becomes more capable.

## 4.2 Raw source data remains preserved

Normalization must remain additive.

For an observed source event:

```json
{
  "source": "dagster",
  "source_version": "...",
  "raw": {},
  "canonical": {}
}
```

V2 may derive additional representations but must not discard the original payload unless retention policy explicitly allows it.

## 4.3 Correlation is explicit before it is inferred

Prefer stable identifiers:

- run IDs;
- trace IDs;
- span IDs;
- asset keys;
- partition keys;
- branch IDs;
- snapshot IDs;
- dbt invocation IDs;
- table identifiers.

Inference is allowed only when explicit correlation is unavailable, and inferred relationships must carry confidence and provenance.

## 4.4 Derived telemetry is reproducible

If Phlo Observe derives a run state, incident, anomaly, lineage edge, or diagnosis, it must be possible to identify:

- the source events used;
- the derivation rule or model version;
- the timestamp of derivation;
- whether the result is deterministic or probabilistic.

## 4.5 No invisible magic

Automated correlation, anomaly detection, and agent-generated explanations must expose why they reached a conclusion.

## 4.6 Observability cannot become a pipeline dependency

Loss or degradation of Phlo Observe must not normally prevent pipeline execution.

Exceptions may exist for explicitly configured policy gates, but they must be opt-in and fail according to documented policy.

## 4.7 Build for agents and humans from the same data model

Do not create a second simplified API for AI agents that bypasses the canonical model.

Agents should consume structured projections of the same evidence shown to people.

---

# 5. V2 system architecture

V2 retains the three V1 components and adds two logical layers.

```text
                         SOURCE SYSTEMS

 Dagster   dbt   DLT   Trino   Nessie   Iceberg   MinIO   Postgres
    │       │     │      │        │        │         │        │
    └───────┴─────┴──────┴────────┴────────┴─────────┴────────┘
                            │
                            ▼
                    ┌───────────────┐
                    │ phlo-observer │
                    │ ingestion     │
                    └───────┬───────┘
                            │
                normalize / correlate
                            │
                            ▼
                  ┌───────────────────┐
                  │ canonical events  │
                  └─────────┬─────────┘
                            │
              ┌─────────────┴─────────────┐
              ▼                           ▼
      ┌───────────────┐           ┌────────────────┐
      │ state engine  │           │ analytics      │
      │ V2            │           │ projections V2 │
      └───────┬───────┘           └────────┬───────┘
              │                            │
              └─────────────┬──────────────┘
                            ▼
                    ┌───────────────┐
                    │ query / agent │
                    │ interfaces    │
                    └───────┬───────┘
                            │
              ┌─────────────┼─────────────┐
              ▼             ▼             ▼
        Observatory      automation      agents
```

The two new V2 logical layers are:

1. **State Engine** — derives current operational state and relationships from canonical events.
2. **Insight Layer** — derives anomalies, incidents, comparisons, recommendations, summaries, and investigation context.

These may initially live inside `phlo-observer` as modules. They should not become separate deployable services until scaling or ownership requires it.

---

# 6. Repository layout

V2 should evolve toward this layout:

```text
phlo-observe/
├── packages/
│   ├── observe-core/
│   ├── phlo-observe-sdk/
│   ├── observe-protocol/
│   ├── observe-query/
│   └── observe-agent/
│
├── services/
│   └── phlo-observer/
│       ├── app/
│       │   ├── api/
│       │   ├── ingest/
│       │   ├── normalize/
│       │   ├── correlate/
│       │   ├── state/
│       │   ├── insight/
│       │   ├── policy/
│       │   ├── storage/
│       │   └── workers/
│       └── tests/
│
├── schemas/
│   ├── canonical/
│   ├── source/
│   └── derived/
│
├── docs/
│   ├── V1_SPEC.md
│   ├── V2_SPEC.md
│   ├── protocol/
│   ├── integrations/
│   └── operations/
│
└── benchmarks/
```

Do not split packages solely to make the repository look modular. A package must have an independently meaningful public API or deployment lifecycle.

---

# 7. Component 1: observe-core V2

The generic SDK remains Python-first.

## 7.1 V2 goals

V2 extends `observe-core` with:

- formal schema registration;
- explicit event contracts;
- richer context propagation;
- pluggable runtime backends;
- dynamic sampling policy;
- local health telemetry;
- source-side aggregation where beneficial;
- optional native acceleration behind the same API.

## 7.2 Event contract registry

Applications should be able to register typed contracts for important event families.

Example:

```python
from observe import EventContract, field


class AssetMaterialized(EventContract):
    name = "asset.materialized"
    version = 2

    asset: str
    rows: int
    duration_ms: float
    partition: str | None = None
```

The event API remains ergonomic:

```python
with observe("asset.materialized") as event:
    event.asset = "silver.samples"
    event.rows = 12_401
```

Contracts must support:

- required fields;
- optional fields;
- primitive and nested types;
- semantic descriptions;
- sensitive-field markers;
- cardinality hints;
- version metadata;
- deprecation metadata;
- examples.

Contract validation must be configurable:

- `off`;
- `warn`;
- `strict`.

Production default: `warn`.

Strict event validation must not unexpectedly fail application work unless explicitly enabled.

## 7.3 Schema registry

The SDK must expose registered event schemas to the observer.

Required behaviour:

- schemas have stable IDs;
- schema version increments are explicit;
- breaking and non-breaking changes are distinguishable;
- observer can reject unknown incompatible schema versions only when configured;
- schemas can be exported as JSON Schema;
- schema hashes are reproducible.

## 7.4 Context propagation V2

V1 `contextvars` remain supported.

V2 adds formal propagation for:

- subprocesses;
- multiprocessing;
- task queues;
- shell-out execution;
- dbt subprocess invocation;
- container-to-container calls where Phlo controls both sides.

Define a compact context envelope:

```json
{
  "trace_id": "...",
  "run_id": "...",
  "asset": "silver.samples",
  "partition": "2026-09-14",
  "branch": "run-abc123"
}
```

Transport methods may include:

- environment variable;
- W3C baggage where appropriate;
- OTLP resource/span attributes;
- explicit command-line handoff for controlled child processes.

Never place secrets or large payloads into propagated context.

## 7.5 Runtime backend interface

The Python public API must not depend directly on a specific implementation of queueing or export.

Define:

```python
class RuntimeBackend(Protocol):
    def emit(self, event: EventEnvelope) -> EmitResult: ...
    def flush(self, timeout: float | None = None) -> FlushResult: ...
    def health(self) -> RuntimeHealth: ...
```

V2 required backends:

- Python background-worker backend;
- synchronous test backend;
- in-memory capture backend.

Optional backend:

- native Rust backend, only if graduation criteria are met.

## 7.6 Dynamic sampling

V1 static sampling becomes policy-based.

Sampling decisions may consider:

- event name;
- severity;
- outcome;
- duration;
- source;
- environment;
- current queue pressure;
- recent error rate;
- run ID.

Rules:

- failures are retained by default;
- critical events are never sampled unless explicitly configured;
- a run should not become uninterpretable due to inconsistent random sampling;
- where possible, sample at run/trace level rather than independently per event;
- sampling decisions must be recorded.

## 7.7 Tail sampling

V2 should support retaining successful operations only if something later makes the run interesting.

Example:

- retain lightweight in-memory summaries during execution;
- if run succeeds normally, emit reduced telemetry;
- if run fails or exceeds latency threshold, preserve richer event detail.

Tail sampling is optional and must have bounded memory.

## 7.8 Local SDK health

Applications must be able to expose SDK health without emitting recursive telemetry.

Health fields:

- queue depth;
- queue capacity;
- events enqueued;
- events emitted;
- events dropped;
- critical events spooled;
- spool bytes;
- last successful export;
- last export error;
- runtime backend;
- exporter endpoint.

## 7.9 Source-side aggregation

For high-frequency repetitive telemetry, the SDK may aggregate before emission.

Example:

```text
100,000 row-level debug events
```

should generally become something like:

```json
{
  "event": "validation.summary",
  "rows_checked": 100000,
  "failed": 14,
  "failure_types": {
    "null_sample_id": 9,
    "invalid_date": 5
  }
}
```

The SDK must not silently aggregate events whose individual identity is required.

---

# 8. Conditional native runtime

Rust remains **conditional** in V2.

## 8.1 Graduation criteria

A native runtime may be introduced if at least one of these is measured in production:

- SDK telemetry overhead exceeds 2% CPU for representative workloads;
- serialization/export worker becomes a sustained bottleneck;
- queue drain cannot keep up with required event rate on allocated resources;
- memory pressure from Python event transport is operationally meaningful;
- a standalone low-footprint agent is required on hosts where Python is undesirable.

## 8.2 Scope if implemented

Rust may own:

- queueing;
- batch serialization;
- compression;
- disk spool;
- OTLP transport;
- retry/backoff;
- checksum and framing;
- local socket receiver.

Rust must **not** initially own:

- Python event-building semantics;
- Phlo domain correlation;
- schema business logic;
- anomaly detection;
- database persistence;
- Observatory APIs.

## 8.3 Python API compatibility

The following must remain unchanged regardless of backend:

```python
with observe("asset.materialized") as event:
    event["asset"] = "silver.samples"
```

Users must not need to care whether the runtime is Python or native.

---

# 9. Component 2: phlo-observe-sdk V2

## 9.1 Goal

The Phlo SDK becomes a semantic layer rather than just a collection of enrichers.

It should understand and standardize concepts that recur across Phlo:

- run;
- asset;
- dataset;
- table;
- partition;
- environment;
- WAP branch;
- promotion;
- snapshot;
- transformation;
- quality check;
- ingestion source;
- lineage edge;
- deployment;
- service.

## 9.2 Canonical domain identifiers

V2 must standardize identifiers.

Examples:

```text
asset://silver/samples
iceberg://catalog/schema/table
branch://nessie/run-abc123
run://dagster/01J...
model://dbt/silver_samples
source://dlt/labware_samples
```

Identifiers must be:

- deterministic where possible;
- parseable;
- namespaced;
- independent of UI labels;
- stable across observer restarts.

## 9.3 Framework adapters V2

V2 should deepen adapters rather than merely add source count.

### Dagster

Capture and correlate:

- run lifecycle;
- step lifecycle;
- asset materializations;
- asset checks;
- partitions;
- retries;
- sensors;
- schedules;
- run tags;
- code location;
- deployment version;
- parent/backfill relationships.

### dbt

Capture:

- invocation;
- model execution;
- test execution;
- source freshness;
- compiled relation;
- timing;
- status;
- adapter response;
- manifest identity;
- lineage from artifacts.

### DLT

Capture:

- pipeline;
- source;
- resource;
- load package;
- rows extracted;
- rows normalized;
- rows loaded;
- retries;
- schema evolution;
- destination table mapping.

### Pandera / quality

Capture:

- check suite;
- checks executed;
- failures;
- affected row counts;
- sampled failure details;
- severity;
- policy consequence.

### Nessie / WAP

Capture:

- branch creation;
- source reference;
- commits;
- validation state;
- promotion attempt;
- merge result;
- conflict;
- cleanup;
- branch lifetime.

### Iceberg

Capture:

- table identifier;
- snapshot creation;
- parent snapshot;
- operation type;
- rows added/deleted where available;
- files added/deleted;
- schema ID;
- partition spec ID;
- commit metadata.

### Trino

Capture:

- query ID;
- query type;
- user/service identity where safe;
- source catalog/schema;
- referenced tables;
- execution state;
- queued time;
- CPU time;
- wall time;
- scanned bytes;
- output rows;
- failure category.

---

# 10. External telemetry ingestion

V2 expands `phlo-observer` into a proper fleet telemetry consumer.

## 10.1 Source classes

Support four classes of source:

1. **native SDK** — Phlo-owned code using `observe-core`;
2. **OTel-native** — services already exporting OTLP;
3. **API-polled** — systems queried for state or history;
4. **log/event-adapted** — systems whose structured logs or event streams are transformed.

## 10.2 Source adapter contract

Every source adapter must implement:

```python
class SourceAdapter(Protocol):
    source_name: str
    source_version: str

    async def ingest(self, payload: RawPayload) -> list[CanonicalEvent]: ...
```

Adapters must declare:

- accepted source versions;
- event types produced;
- correlation identifiers extracted;
- expected cardinality;
- sensitive fields;
- idempotency key strategy;
- failure behaviour.

## 10.3 Adapter conformance fixtures

Each adapter requires captured fixtures from realistic source payloads.

Tests must prove:

- deterministic normalization;
- source-version compatibility;
- no accidental loss of important raw fields;
- safe handling of unknown fields;
- stable correlation IDs.

---

# 11. Component 3: phlo-observer V2

`phlo-observer` remains the central service but gains a formal pipeline.

```text
receive
  ↓
validate envelope
  ↓
deduplicate
  ↓
normalize
  ↓
redact
  ↓
correlate
  ↓
persist canonical + raw
  ↓
update derived state
  ↓
run insight rules
  ↓
publish updates
```

Each stage must be independently testable.

## 11.1 Ingestion API

V1 endpoints remain compatible.

V2 adds bulk ingestion optimized for batches.

Example:

```http
POST /v2/events:batch
```

Response should support partial acknowledgement:

```json
{
  "accepted": 997,
  "duplicate": 2,
  "rejected": 1,
  "errors": [
    {
      "index": 412,
      "code": "SCHEMA_INVALID",
      "message": "..."
    }
  ]
}
```

## 11.2 Ordering

Global ordering is not required.

Ordering guarantees:

- preserve event timestamp;
- preserve source sequence where supplied;
- store ingest timestamp separately;
- state derivation must tolerate late arrivals;
- derived state may be recomputed when late evidence materially changes interpretation.

## 11.3 Idempotency

Every event must have a deterministic idempotency identity when possible.

Preferred hierarchy:

1. source event ID;
2. source sequence + source instance;
3. deterministic content-derived ID;
4. generated event ID for genuinely unique events.

Duplicate ingestion must not create duplicate canonical state transitions.

---

# 12. State Engine

The State Engine is the main conceptual addition in V2.

V1 stores event history. V2 should be able to answer:

> What is the current state of this run, asset, branch, table, or service, and why?

## 12.1 Derived entities

Maintain projections for:

- runs;
- assets;
- WAP branches;
- tables;
- snapshots;
- models;
- quality suites;
- services;
- deployments;
- incidents.

## 12.2 State transitions

Example run state machine:

```text
queued
  ↓
running
  ├──► succeeded
  ├──► failed
  ├──► cancelled
  └──► degraded
```

A Phlo pipeline run may also have semantic phases:

```text
ingest → write → transform → validate → promote → cleanup
```

These phases are projections derived from source events, not necessarily direct source statuses.

## 12.3 State provenance

Every derived field must optionally expose provenance.

Example:

```json
{
  "status": "failed",
  "provenance": {
    "derived_from": ["evt_123", "evt_456"],
    "rule": "run-state-v2",
    "rule_version": 3,
    "derived_at": "..."
  }
}
```

## 12.4 Rebuildability

All projections must be rebuildable from canonical events.

Do not make projections the only copy of important information.

Required command:

```bash
phlo-observer rebuild-projections
```

Support scoped rebuild:

```bash
phlo-observer rebuild-projections --run 01J...
```

---

# 13. Correlation Engine V2

## 13.1 Explicit relationship graph

V2 introduces a relationship graph between observed entities.

Example:

```text
Dagster run
   │
   ├── executes dbt invocation
   │      └── materializes model
   │             └── writes Iceberg snapshot
   │
   └── owns WAP branch
          └── promoted to main
```

Store relationships as typed edges.

Required fields:

- `from_entity`;
- `to_entity`;
- `relationship_type`;
- `source_event_ids`;
- `confidence`;
- `method`;
- `created_at`.

## 13.2 Relationship types

Initial V2 vocabulary:

- `started_by`;
- `part_of`;
- `executes`;
- `reads_from`;
- `writes_to`;
- `produces`;
- `validates`;
- `promotes`;
- `supersedes`;
- `depends_on`;
- `triggered_by`;
- `deployed_as`.

Do not allow arbitrary free-text relationship types in canonical storage.

## 13.3 Inferred relationships

Inference may use:

- temporal proximity;
- shared run ID;
- shared branch;
- shared table;
- trace/span relationship;
- source-specific metadata;
- known orchestration structure.

Every inferred edge must have:

```json
{
  "method": "inferred",
  "confidence": 0.92,
  "rule": "dbt-iceberg-table-time-window-v1"
}
```

Never present inferred relationships as guaranteed facts.

---

# 14. Lineage V2

Lineage becomes a first-class derived projection.

## 14.1 Dataset lineage

Represent:

- source system → bronze table;
- bronze → silver;
- silver → gold;
- model → model;
- table → report/export where known.

## 14.2 Runtime lineage

Static lineage is not enough.

V2 should distinguish:

- declared lineage;
- compiled lineage;
- observed runtime lineage.

Example:

```json
{
  "edge": "bronze.samples -> silver.samples",
  "kind": "runtime",
  "run_id": "...",
  "observed_at": "..."
}
```

## 14.3 Column lineage

Column-level lineage is explicitly **optional V2.1+**.

Do not implement it unless:

- source tooling provides reliable metadata; or
- there is a demonstrated user need.

---

# 15. Insight Layer

The Insight Layer turns event history into operationally useful findings.

V2 insight categories:

- anomaly;
- regression;
- recurring failure;
- bottleneck;
- freshness issue;
- quality degradation;
- unusual resource usage;
- likely root cause;
- change correlation;
- recommendation.

## 15.1 Rule-based insights first

Initial V2 insight detection must be deterministic rules where possible.

Examples:

- duration > 2x rolling median;
- row count differs > configured threshold;
- same quality check failed in three consecutive runs;
- promotion latency increasing for seven runs;
- query scanned bytes increased > 300% after deployment;
- retries increased after code version change.

## 15.2 Insight schema

```json
{
  "insight_id": "...",
  "kind": "regression",
  "severity": "warning",
  "entity": "asset://silver/samples",
  "title": "Materialization duration increased",
  "summary": "p95 duration is 2.4x the previous 14-day baseline",
  "evidence": ["evt_...", "evt_..."],
  "rule": "duration-regression",
  "rule_version": 2,
  "detected_at": "...",
  "status": "open"
}
```

## 15.3 Insight lifecycle

Insights may be:

- open;
- acknowledged;
- resolved;
- suppressed;
- expired.

A new event should be able to resolve an existing insight automatically where appropriate.

---

# 16. Baselines and anomaly detection

## 16.1 Baseline service

Maintain rolling baselines for useful measures:

- run duration;
- step duration;
- rows in/out;
- data volume;
- query CPU;
- scanned bytes;
- validation failure rate;
- promotion time;
- retry count.

Baselines must be partition-aware where appropriate.

## 16.2 Statistical methods

Start simple:

- rolling median;
- median absolute deviation;
- percentile bands;
- EWMA;
- seasonal comparison if sufficient data exists.

Do not begin with opaque ML anomaly models.

## 16.3 Machine-learning graduation criteria

ML anomaly models become eligible only when:

- deterministic/statistical methods demonstrably produce too many false positives or miss meaningful anomalies;
- sufficient historical data exists;
- model behaviour can be evaluated against labelled or reviewed incidents;
- inference cost is justified.

---

# 17. Incident model

V2 should group related failures and insights into operational incidents.

Example:

```text
Incident INC-1042

Root symptom:
  silver.samples materialization failed

Related evidence:
  dbt model failed
  Trino query OOM
  scan volume +410%
  deployment changed 18 minutes earlier

Affected:
  silver.samples
  gold.batch_summary
  dashboard.batch_release
```

## 17.1 Incident creation

Incidents may be:

- manually created;
- rule-created;
- agent-suggested but human-confirmed;
- automatically created for configured critical failures.

## 17.2 Incident grouping

Grouping signals:

- same run;
- same root error code;
- same deployment;
- same affected asset;
- same service;
- tight time window;
- causal relationship graph.

---

# 18. Change intelligence

Observability is much more useful if Phlo knows what changed.

V2 should ingest change metadata from:

- Phlo deployment version;
- Git commit SHA;
- container image digest;
- dbt manifest version/hash;
- schema changes;
- Iceberg schema ID changes;
- Nessie commits;
- configuration version;
- feature flags where used.

Then allow questions such as:

- what changed before failures started?;
- did performance regress after deployment?;
- which assets changed behaviour after schema revision?;
- did query cost increase after a model change?.

Change events are canonical events, not a separate hidden database.

---

# 19. Agent-facing observability

V2 should be designed so an AI agent can investigate safely and reliably.

The agent layer is **not** a raw SQL connection and is **not** a raw log dump.

## 19.1 observe-query package

Provide a typed query client.

Example:

```python
from observe_query import ObserverClient

client = ObserverClient(...)

run = client.run("01J...")
failures = run.failures()
changes = run.recent_changes()
lineage = run.affected_downstream()
```

## 19.2 Agent tools

Expose bounded tools such as:

```text
get_run
get_run_timeline
get_event
search_events
get_asset_health
get_asset_history
get_failures
get_incident
get_related_changes
get_lineage
compare_runs
explain_relationship
```

Tools should return structured JSON suitable for both human UI and agent consumption.

## 19.3 Evidence-first answers

Any agent-generated conclusion must reference evidence IDs.

Example:

```json
{
  "conclusion": "The run likely failed because the Trino query exceeded memory after scan volume increased.",
  "confidence": 0.84,
  "evidence": [
    "evt_query_failed_123",
    "evt_scan_regression_456",
    "evt_deploy_789"
  ]
}
```

## 19.4 Read-only by default

V2 agent access is read-only by default.

Any action such as:

- retry run;
- cancel run;
- promote branch;
- suppress alert;
- change policy;

must use a separate privileged action interface with explicit authorization and audit.

---

# 20. Automated investigation

V2 should support deterministic investigation bundles before introducing generative summaries.

For a failed run, automatically gather:

- run metadata;
- failed stage;
- structured error;
- preceding warnings;
- relevant query failures;
- quality failures;
- code/deployment changes;
- affected assets;
- recent comparable runs;
- baseline deviations;
- downstream impact.

Output:

```json
{
  "run_id": "...",
  "failure": {},
  "timeline": [],
  "related_changes": [],
  "comparisons": [],
  "impact": [],
  "candidate_causes": []
}
```

This structured bundle is the preferred input to any LLM summary.

---

# 21. LLM-assisted diagnosis

LLM usage is optional V2 functionality.

## 21.1 Allowed responsibilities

An LLM may:

- summarize incident evidence;
- explain structured errors in plain language;
- compare two runs;
- suggest investigation steps;
- rank candidate causes already supported by evidence;
- generate a handoff summary.

## 21.2 Disallowed responsibilities by default

An LLM must not autonomously:

- mark regulated work compliant;
- alter raw telemetry;
- delete evidence;
- promote WAP branches;
- rerun production workflows;
- change retention;
- change security policies;
- invent causal links not represented as hypotheses.

## 21.3 Prompt grounding

Only provide bounded, relevant evidence to the model.

Do not dump unrestricted logs or database contents.

## 21.4 Model provenance

Persist:

- model identifier;
- prompt template version;
- evidence IDs supplied;
- generated output;
- timestamp;
- human feedback if given.

---

# 22. Query API V2

Add APIs designed around operational questions rather than storage tables.

Required endpoints should include equivalents of:

```text
GET /v2/runs/{id}
GET /v2/runs/{id}/timeline
GET /v2/runs/{id}/failures
GET /v2/runs/{id}/changes
GET /v2/runs/{id}/impact
GET /v2/assets/{id}
GET /v2/assets/{id}/health
GET /v2/assets/{id}/history
GET /v2/assets/{id}/lineage
GET /v2/incidents
GET /v2/incidents/{id}
GET /v2/insights
GET /v2/events/{id}/provenance
POST /v2/query/compare-runs
```

Do not expose database schema directly as the API design.

---

# 23. Search V2

Support structured and text search.

Searchable fields:

- event name;
- service;
- run ID;
- asset;
- table;
- model;
- branch;
- snapshot;
- error code;
- error message;
- deployment;
- source;
- tags;
- incident.

Full-text search should be implemented using PostgreSQL first.

Do not introduce Elasticsearch/OpenSearch merely for convenience.

---

# 24. Storage architecture V2

## 24.1 Default storage remains PostgreSQL

PostgreSQL remains the default until measured evidence proves otherwise.

Separate logical storage concerns:

- canonical events;
- raw source payloads;
- entity projections;
- relationship edges;
- insights;
- incidents;
- baselines;
- agent analyses.

## 24.2 Table families

Suggested tables:

```text
observe_events
observe_raw_events
observe_entities
observe_relationships
observe_runs
observe_assets
observe_incidents
observe_insights
observe_baselines
observe_schema_registry
observe_agent_analyses
observe_ingest_failures
```

## 24.3 Partitioning

Use PostgreSQL time partitioning when event volume justifies it.

Prefer monthly or weekly partitions depending on volume.

Do not partition low-volume tables unnecessarily.

## 24.4 Event-store graduation criteria

A dedicated analytical store such as ClickHouse may be evaluated if:

- retained canonical events exceed approximately 500 million rows; or
- common event analytics cannot meet p95 latency objectives despite reasonable PostgreSQL tuning; or
- storage cost becomes materially problematic; or
- sustained ingest throughput exceeds comfortable PostgreSQL capacity.

Migration must preserve PostgreSQL as the source for transactional observer state unless there is a separate decision to change that architecture.

Likely split if needed:

```text
PostgreSQL
  entities / runs / incidents / state

ClickHouse
  high-volume event analytics
```

---

# 25. Event bus graduation

V2 does not require Kafka/Redpanda by default.

Introduce a durable bus only when at least one is true:

- observer must absorb bursts significantly above database ingest capacity;
- multiple independent consumers require replay;
- ingestion and processing need independent scaling;
- downtime tolerance requires durable decoupling beyond local spool;
- event throughput makes direct ingestion operationally difficult.

If introduced:

```text
sources → ingest gateway → durable bus → processors → stores
```

The bus must not become the canonical schema definition.

---

# 26. Real-time update stream

Observatory should not poll every view aggressively.

V2 observer should provide an update stream via one of:

- Server-Sent Events preferred initially;
- WebSocket if bidirectional capability becomes necessary.

Use cases:

- run state changed;
- event arrived;
- insight opened;
- incident updated;
- quality state changed;
- WAP promotion completed.

The update stream is a notification channel, not the canonical data API.

Clients should refetch authoritative state after receiving a notification.

---

# 27. Observatory contract

Phlo Observe must support the Observatory without embedding presentation logic.

V2 should enable these views cleanly:

## 27.1 Fleet overview

Show:

- active runs;
- failures;
- degraded assets;
- open incidents;
- unusual changes;
- platform services with health problems.

## 27.2 Run view

Show:

- semantic phases;
- timeline;
- source events;
- quality decisions;
- WAP state;
- snapshots;
- queries;
- errors;
- changes;
- downstream impact;
- comparison to baseline.

## 27.3 Asset view

Show:

- current health;
- latest materialization;
- freshness;
- quality trend;
- row-count trend;
- duration trend;
- lineage;
- recent incidents;
- related changes.

## 27.4 Incident view

Show:

- impact;
- timeline;
- evidence;
- suspected causes;
- related changes;
- affected lineage;
- comparable historical failures;
- human notes;
- agent summary if enabled.

---

# 28. Policy Engine

V2 may introduce observable operational policies.

Examples:

```yaml
policies:
  - name: block-promotion-on-critical-quality-failure
    when:
      event: quality.completed
      severity: critical
      outcome: failed
    action:
      emit_decision: deny_promotion
```

Policy decisions must themselves be events.

A policy engine may advise or gate actions, but gating must be explicitly configured.

Never silently convert an observability rule into an execution-control rule.

---

# 29. Data quality observability

V2 should distinguish infrastructure health from data health.

Track per asset:

- freshness;
- completeness;
- validity;
- volume;
- schema drift;
- distribution shifts where useful;
- failed records;
- rule severity.

Do not attempt to become a full standalone data-quality product.

Phlo Observe stores and correlates outcomes from Pandera/dbt/other checks and may derive trends.

---

# 30. Cost observability

V2 should support approximate cost/efficiency signals where available.

Examples:

- Trino CPU time;
- bytes scanned;
- object-store IO;
- compute duration;
- query concurrency;
- retries;
- storage growth.

The initial goal is **relative efficiency**, not perfect financial chargeback.

Useful outputs:

- cost regression after model change;
- top expensive queries;
- top expensive assets;
- bytes scanned per output row;
- repeated failed work.

---

# 31. SLOs and reliability objectives

V2 introduces explicit SLO modelling.

Potential SLOs:

- pipeline success rate;
- asset freshness;
- data quality pass rate;
- observer ingest availability;
- observer query latency;
- critical-event delivery.

Example:

```yaml
slo:
  name: silver-samples-freshness
  entity: asset://silver/samples
  target: 99.5
  window: 30d
  condition:
    freshness_minutes_lte: 90
```

Track error budgets where useful.

Do not impose SLO concepts on every asset.

---

# 32. Alerting V2

Alerts should derive from insights/incidents rather than raw event matches where possible.

Required features:

- deduplication;
- suppression;
- cooldown;
- routing;
- severity;
- acknowledgement;
- resolution;
- maintenance windows;
- run-aware grouping.

Destinations may include:

- email;
- Slack/Teams through external integration;
- webhook;
- Observatory notification centre.

Core observer must expose webhooks rather than hard-code every vendor.

---

# 33. Security V2

## 33.1 Authentication

Support service-to-service authentication and user authentication separately.

Possible mechanisms:

- bearer service tokens;
- OIDC for users;
- mTLS where deployment requires it.

## 33.2 Authorization

Introduce scoped permissions:

```text
observe:ingest
observe:read
observe:admin
observe:policy
observe:agent
observe:action
```

## 33.3 Row/entity restrictions

If Phlo becomes multi-tenant or environment-separated, access control must be enforceable by:

- tenant;
- environment;
- project;
- service.

Do not rely solely on UI filtering.

---

# 34. Privacy and redaction V2

V2 expands redaction into classification-aware handling.

Fields may be classified as:

- public;
- internal;
- confidential;
- sensitive;
- prohibited.

Schema contracts may mark classification.

Prohibited fields must be removed before persistence.

Sensitive values may be:

- removed;
- hashed;
- tokenized;
- stored only in restricted raw payload storage.

Redaction rules must be versioned and testable.

---

# 35. GxP and regulated context

Phlo Observe remains an observability system unless explicitly validated for another purpose.

V2 must preserve the distinction between:

- operational telemetry;
- authoritative source data;
- audit trail;
- validation evidence;
- quality decisions.

If regulated workflows consume observer-derived information, documentation must identify whether the data is:

- informational only;
- supporting evidence;
- decision-driving.

Decision-driving usage may require additional validation and controls.

Do not imply that immutable-looking telemetry is automatically a compliant audit trail.

---

# 36. Retention V2

Retention can vary by data class.

Example defaults:

```text
raw debug telemetry       14 days
canonical normal events   90 days
critical events           1 year
run summaries             2 years
incidents                  2 years
schema registry           indefinite
```

Actual defaults should remain configurable.

Derived projections may outlive raw event detail if they remain reproducible from retained canonical evidence.

---

# 37. Cold archive

V2 may archive historical event data to object storage.

Archive format preference:

- Parquet;
- partitioned by date and source/event family;
- immutable objects;
- manifest/index metadata in PostgreSQL.

Archive should support:

```bash
phlo-observer archive --before 2026-01-01
phlo-observer restore --run 01J...
```

Restoration should not create duplicate events.

---

# 38. High availability

V2 should support multiple observer instances.

Requirements:

- stateless HTTP ingest nodes where practical;
- shared database;
- safe concurrent workers;
- idempotent processing;
- leader election only for jobs that genuinely require a singleton;
- no local-only critical state except bounded spool/cache.

Scheduled jobs should use database-backed locking or equivalent.

---

# 39. Backpressure

The system must have explicit backpressure policies.

At SDK:

- bounded queue;
- drop/sample low-priority telemetry;
- spool critical events.

At observer:

- request size limits;
- batch limits;
- bounded worker concurrency;
- database pool limits;
- overload response codes;
- retry hints.

Never allow observability load to exhaust resources required for the data platform itself.

---

# 40. Disaster recovery

V2 operations documentation must define:

- database backup;
- restore procedure;
- RPO;
- RTO;
- schema migration rollback;
- spool recovery;
- archive restoration;
- reprocessing of canonical events;
- projection rebuild.

A restore test should be performed periodically in CI or staging where practical.

---

# 41. Performance targets

Targets should be validated against hardware profile, but V2 design goals are:

## SDK

- p95 event-finalization overhead below 250 µs excluding user field construction;
- non-blocking enqueue under normal queue conditions;
- telemetry CPU overhead below 1% for normal Phlo workloads;
- bounded memory under exporter outage.

## Observer ingest

On a modest single-node deployment:

- sustained 5,000 canonical events/sec;
- bursts of 20,000 events/sec for 30 seconds without loss of critical telemetry;
- p95 accepted-ingest latency below 100 ms for normal batches.

## Query

For hot 30-day data:

- run detail p95 < 300 ms;
- run timeline p95 < 500 ms;
- asset health p95 < 300 ms;
- common search p95 < 1 s;
- lineage neighborhood p95 < 1 s.

Do not optimize synthetic throughput at the expense of understandable architecture.

---

# 42. Benchmarks

Create reproducible benchmarks for:

- SDK event creation;
- queue overhead;
- serialization;
- batch export;
- observer normalization;
- correlation;
- PostgreSQL insert throughput;
- timeline query;
- relationship traversal;
- projection rebuild.

Benchmark datasets should include realistic Phlo event shapes, not trivial tiny dictionaries.

---

# 43. Compatibility

V2 observer must ingest V1 event envelopes for the supported migration period.

Minimum policy:

- V2 SDK emits V2 envelopes;
- observer accepts V1 and V2;
- migration tooling identifies remaining V1 producers;
- V1 support may only be removed in V3 or an explicitly announced major break.

---

# 44. Schema evolution

Event schema changes fall into:

## Non-breaking

- add optional field;
- expand enum safely;
- add metadata.

## Breaking

- rename/remove field;
- change meaning;
- change type incompatibly;
- change identifier semantics.

Breaking changes require a new major event schema version.

Observer must preserve schema version per event.

---

# 45. Migrations

Database migration tooling must support:

- forward migration;
- migration status;
- safe retry;
- large-table migration strategies;
- compatibility windows between app and database versions.

Avoid migrations that rewrite the entire events table synchronously.

---

# 46. Self-observability

Phlo Observe must observe itself without creating infinite recursion.

Expose Prometheus/OpenTelemetry metrics for:

- ingest requests;
- events accepted;
- events rejected;
- duplicates;
- normalization failures;
- correlation failures;
- projection lag;
- insight evaluation lag;
- database latency;
- worker queue depth;
- API latency;
- archive jobs;
- event age at ingest.

Observer internal failures should be visible independently of observer event storage.

---

# 47. Failure quarantine

Malformed or unsupported events should be quarantined rather than silently discarded.

Store:

- source;
- reason;
- received time;
- safe representation of payload;
- retryable flag;
- adapter version.

Provide admin APIs/CLI to:

- inspect;
- replay;
- dismiss;
- export.

---

# 48. Developer experience

V2 should remain simple to use.

Golden path:

```python
from observe import observe
from phlo_observe import phlo_context

with phlo_context(run_id=run_id, asset=asset):
    with observe("asset.materialize") as event:
        event["rows_in"] = len(source)
        result = transform(source)
        event["rows_out"] = len(result)
```

A developer must not need to understand:

- OTLP protobuf;
- PostgreSQL schema;
- exporter retries;
- observer internals;
- correlation graph internals.

---

# 49. CLI V2

Required CLI families:

```text
phlo-observe doctor
phlo-observe schemas list
phlo-observe schemas validate
phlo-observe events tail
phlo-observe run show
phlo-observe run compare
phlo-observe incident show
phlo-observe replay
phlo-observe benchmark
```

Observer administration:

```text
phlo-observer migrate
phlo-observer rebuild-projections
phlo-observer reprocess
phlo-observer archive
phlo-observer restore
phlo-observer health
```

---

# 50. Configuration V2

Use one documented configuration hierarchy:

1. explicit code config;
2. environment variables;
3. configuration file;
4. defaults.

All settings must have:

- name;
- type;
- default;
- description;
- security implications;
- restart requirement.

Unknown configuration keys should fail validation in the observer service.

SDK unknown keys should warn or fail during startup depending on mode.

---

# 51. Testing strategy

V2 requires more than unit tests.

## 51.1 Unit tests

Cover:

- schema validation;
- event IDs;
- redaction;
- sampling;
- correlation rules;
- relationship inference;
- projection reducers;
- insight rules;
- baselines;
- authorization.

## 51.2 Contract tests

Each source adapter requires contract fixtures.

Each API endpoint requires request/response contract tests.

Each event schema requires round-trip serialization tests.

## 51.3 Property tests

Use property-based tests where valuable for:

- idempotency;
- event ordering;
- deduplication;
- projection rebuild equivalence;
- schema compatibility.

## 51.4 Integration tests

Test real supported versions of:

- PostgreSQL;
- OpenTelemetry Collector;
- Dagster;
- dbt;
- Trino where practical;
- Nessie where practical.

## 51.5 Chaos/failure tests

Test:

- observer unavailable;
- database unavailable;
- slow database;
- duplicate delivery;
- out-of-order events;
- corrupted payload;
- exporter timeout;
- process crash during spool write;
- disk full;
- projection worker restart.

## 51.6 Load tests

Load test realistic event mixes and batch sizes.

Do not benchmark only happy-path homogeneous events.

---

# 52. Release strategy

V2 should release packages independently only if needed.

Suggested compatibility labels:

```text
observe-core        2.x
phlo-observe-sdk    2.x
phlo-observer       2.x
observe-query       1.x
observe-agent       1.x
```

The observer should publish a compatibility matrix.

---

# 53. V2 implementation phases

V2 should be implemented in ordered phases.

## Phase 0 — V1 evidence review

Deliverables:

- production metrics report;
- correlation failure report;
- user investigation query inventory;
- event-volume profile;
- storage profile;
- performance profile;
- V2 scope confirmation.

Do not proceed without this review.

## Phase 1 — Protocol and schema hardening

Implement:

- formal schema registry;
- V2 event envelope;
- compatibility layer;
- contract validation;
- canonical identifiers;
- source adapter contracts.

Exit criteria:

- V1 events still ingest;
- V2 events validate;
- schemas are queryable;
- compatibility tests pass.

## Phase 2 — State Engine

Implement:

- entity registry;
- run projections;
- asset projections;
- branch projections;
- rebuild command;
- provenance.

Exit criteria:

- projections reproduce expected state from fixtures;
- full rebuild produces same state as incremental processing.

## Phase 3 — Relationship graph and lineage

Implement:

- typed relationships;
- explicit correlation;
- confidence model;
- lineage projection;
- provenance API.

Exit criteria:

- a representative Phlo run displays end-to-end relationship chain.

## Phase 4 — Insight engine

Implement:

- baselines;
- rule engine;
- insight lifecycle;
- regression detection;
- recurring-failure detection.

Exit criteria:

- known synthetic regressions are detected;
- false-positive behaviour is reviewed.

## Phase 5 — Incident model

Implement:

- incident entity;
- grouping;
- impact analysis;
- change correlation;
- notification hooks.

Exit criteria:

- failed pipeline creates a coherent incident bundle.

## Phase 6 — Query and agent interfaces

Implement:

- `observe-query`;
- structured investigation endpoints;
- agent-safe tool surface;
- evidence references;
- permissions.

Exit criteria:

- an automated investigation can answer common failure questions without direct DB access.

## Phase 7 — Operational hardening

Implement:

- HA support;
- partitioning if needed;
- archive;
- quarantine/replay;
- SLOs;
- self-observability;
- disaster-recovery runbooks.

## Phase 8 — Conditional scale work

Evaluate with evidence:

- native Rust runtime;
- event bus;
- ClickHouse;
- advanced anomaly models.

Only adopt components that pass documented graduation criteria.

---

# 54. Junior-developer work package pattern

Every V2 implementation issue should contain:

1. problem statement;
2. exact package/module location;
3. public API to add/change;
4. database changes;
5. example input;
6. expected output;
7. edge cases;
8. tests required;
9. documentation required;
10. performance expectations;
11. compatibility expectations;
12. explicit non-goals;
13. acceptance checklist.

A junior developer should not be asked to invent event semantics, persistence strategy, or API shape inside a task.

---

# 55. Definition of done for any V2 feature

A feature is complete only when:

- implementation exists;
- type checking passes;
- linting passes;
- unit tests pass;
- integration tests exist where appropriate;
- migration exists where needed;
- API/schema documentation is updated;
- failure behaviour is tested;
- observability of the feature itself exists;
- compatibility impact is documented;
- security/redaction impact is reviewed;
- benchmarks exist if feature affects hot paths.

---

# 56. V2 acceptance criteria

V2 can be considered complete only when all mandatory items below are met.

## Protocol and SDK

- [ ] V2 envelope is versioned and documented.
- [ ] V1 envelope remains ingestible.
- [ ] schema registry is implemented.
- [ ] typed event contracts work.
- [ ] context propagates through supported subprocess boundaries.
- [ ] dynamic sampling is available.
- [ ] SDK health is observable.

## Phlo semantics

- [ ] canonical entity identifiers are implemented.
- [ ] Dagster, dbt, DLT, quality, WAP/Nessie, Iceberg, and Trino adapters satisfy V2 contracts.
- [ ] source fixtures exist.
- [ ] source-version compatibility is tested.

## Observer

- [ ] batch ingestion supports partial acknowledgement.
- [ ] deduplication is robust.
- [ ] late/out-of-order events are handled.
- [ ] quarantine/replay exists.
- [ ] projection rebuild exists.

## State and correlation

- [ ] run state projection is implemented.
- [ ] asset state projection is implemented.
- [ ] WAP branch state projection is implemented.
- [ ] relationship graph exists.
- [ ] inferred edges carry confidence and provenance.
- [ ] runtime lineage is queryable.

## Insights

- [ ] rolling baselines exist.
- [ ] deterministic regression detection exists.
- [ ] recurring failure detection exists.
- [ ] insight lifecycle is implemented.
- [ ] incident grouping is implemented.
- [ ] change correlation is implemented.

## Query/agent

- [ ] typed query client exists.
- [ ] structured run investigation endpoint exists.
- [ ] asset-health endpoint exists.
- [ ] compare-runs capability exists.
- [ ] agent tools are read-only by default.
- [ ] agent conclusions can reference evidence IDs.

## Operations

- [ ] horizontal observer deployment is supported.
- [ ] backup and restore are documented and tested.
- [ ] archive/restore exists if retention volume justifies it.
- [ ] self-observability metrics exist.
- [ ] load tests meet agreed production targets.
- [ ] security scopes are implemented.
- [ ] retention policies are enforced.

---

# 57. Explicit V2 decisions

Unless production evidence forces reconsideration, the following decisions should be treated as fixed during V2 implementation.

1. **Wide events remain the primary developer-facing abstraction.**
2. **Python remains the primary SDK language.**
3. **A Rust runtime is optional and evidence-driven.**
4. **OpenTelemetry remains the interoperability standard; Phlo does not invent a competing wire ecosystem.**
5. **PostgreSQL remains the default operational store.**
6. **A dedicated event analytics store is conditional on demonstrated scale need.**
7. **A durable event bus is conditional, not foundational.**
8. **Canonical source events remain immutable after ingestion.**
9. **Derived state must be rebuildable.**
10. **Raw source payloads and canonical representations remain distinguishable.**
11. **Inferred relationships must expose confidence and provenance.**
12. **Rule/statistical anomaly detection precedes opaque machine-learning methods.**
13. **Agent outputs must be evidence-grounded.**
14. **Agent access is read-only by default.**
15. **Observability remains distinct from authoritative audit trail unless separately validated.**
16. **Observer outages must not normally stop Phlo workloads.**
17. **The Observatory consumes observer APIs; presentation logic does not live inside observer internals.**

---

# 58. Likely V3 boundary

The following should generally be considered beyond V2 unless required sooner by real deployment needs:

- full multi-tenant SaaS operation;
- globally distributed event ingestion;
- cross-region active-active observer clusters;
- autonomous remediation without human approval;
- generalized enterprise SIEM functionality;
- full column-level lineage across arbitrary SQL engines;
- generic infrastructure eBPF platform;
- custom time-series database;
- custom tracing backend;
- observability marketplace/plugin ecosystem.

V2 should leave clean extension points for these without attempting to pre-build them.

---

# 59. Summary architecture

The intended V2 outcome is:

```text
                          PHLO WORKLOADS

              native events         external telemetry
                    │                        │
                    └───────────┬────────────┘
                                ▼
                         phlo-observer
                                │
                   validate / dedupe / redact
                                │
                             normalize
                                │
                            correlate
                                │
                    ┌───────────┴───────────┐
                    ▼                       ▼
             canonical events        raw evidence
                    │
                    ▼
                state engine
                    │
         ┌──────────┼───────────┐
         ▼          ▼           ▼
      entities   lineage    relationships
         │          │           │
         └──────────┼───────────┘
                    ▼
               insight engine
                    │
         ┌──────────┼───────────┐
         ▼          ▼           ▼
      baselines   insights   incidents
                    │
                    ▼
             investigation layer
                    │
         ┌──────────┼──────────────┐
         ▼          ▼              ▼
    Observatory   automation   agent tools
```

The result should be a system that does not merely answer **"what logs were emitted?"**.

It should answer:

- What happened?
- What is happening now?
- Which systems and data products were involved?
- What changed?
- Is this behaviour unusual?
- What is affected downstream?
- Have we seen this before?
- What evidence supports the likely cause?
- What should a person investigate next?

That is the V2 boundary: **from coherent observability to operational understanding**, while keeping the implementation auditable, evidence-driven, and simple enough to operate.
