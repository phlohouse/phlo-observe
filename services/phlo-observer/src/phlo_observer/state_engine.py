"""Rebuildable state engine (spec §12).

Pure reducers over canonical event dicts. The same functions drive both the
incremental ingest path (one event at a time) and ``rebuild-projections``
(the whole event history in ``observed_at`` order), which is what makes the
two paths equivalent (spec §12.4).

Projection state is held in plain dicts so reducers stay storage-agnostic;
``projections.py`` maps them onto ORM rows. Every reducer records the event
ids it consumed in ``state["derived_from"]`` so projections can expose
provenance (spec §12.3) without a second bookkeeping pass.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Any

RUN_RULE = "run-state-v2"
RUN_RULE_VERSION = 1
ENTITY_RULE = "entity-registry-v2"
ENTITY_RULE_VERSION = 1
ASSET_RULE = "asset-state-v2"
ASSET_RULE_VERSION = 1
EDGE_RULE = "edge-inference-v2"
EDGE_RULE_VERSION = 1

_MAX_DERIVED_FROM = 64
_MAX_SUMMARY_ASSETS = 1000
_MAX_EDGE_SOURCES = 32

# Run terminal signal: outcome=unknown means "the run started".
_TERMINAL_RUN_EVENTS = {"pipeline.run", "dlt.pipeline.run"}

_STATUS_PRECEDENCE = {
    "unknown": 0,
    "queued": 0,
    "running": 1,
    "degraded": 2,
    "partial": 2,
    "cancelled": 3,
    "success": 4,
    "succeeded": 4,
    "failure": 5,
    "failed": 5,
}

# Event-name verb → edge type for entity role pairs (spec §13.1).
_VERB_EDGES = {
    "read": "reads_from",
    "scan": "reads_from",
    "query": "reads_from",
    "extract": "reads_from",
    "write": "writes_to",
    "load": "writes_to",
    "commit": "writes_to",
    "materialize": "produces",
    "materialized": "produces",
    "produce": "produces",
    "execute": "produces",
    "validate": "validates",
    "check": "validates",
    "test": "validates",
    "promote": "promotes",
    "publish": "promotes",
}

# Correlation/entity role → canonical entity namespace.
_ROLE_NAMESPACE = {
    "run": "run",
    "asset": "asset",
    "table": "table",
    "iceberg": "iceberg",
    "branch": "branch",
    "model": "model",
    "source": "source",
    "snapshot": "snapshot",
    "service": "service",
    "deployment": "deployment",
    "quality_suite": "quality",
    "incident": "incident",
}


def _verb_of(event_name: str) -> str:
    return event_name.rsplit(".", 1)[-1]


def event_producer(event: dict[str, Any]) -> str:
    """Best-effort producer identity for namespacing (dagster, dlt, ...)."""
    source = event.get("source") or {}
    return (
        source.get("producer")
        or source.get("adapter")
        or (event.get("service") or {}).get("name")
        or "phlo"
    )


def event_entities(event: dict[str, Any]) -> dict[str, str]:
    """Canonical entity identifiers by role for one event.

    V2 envelopes carry ``entities`` explicitly; for V1 envelopes and for
    producers that only set correlation fields, the same identifiers are
    derived from ``correlation`` so both schema generations land in the
    same registry (spec §9.2, §43).
    """
    corr = event.get("correlation") or {}
    service = event.get("service") or {}
    producer = event_producer(event)
    derived: dict[str, str] = {}
    if corr.get("run_id"):
        derived["run"] = f"run://{producer}/{corr['run_id']}"
    if corr.get("asset_key"):
        derived["asset"] = f"asset://{corr['asset_key']}"
    if corr.get("table"):
        derived["table"] = f"table://{corr['table']}"
    if corr.get("branch"):
        derived["branch"] = f"branch://{producer}/{corr['branch']}"
    if corr.get("snapshot_id"):
        table = corr.get("table") or "unknown"
        derived["snapshot"] = f"snapshot://{table}/{corr['snapshot_id']}"
    if service.get("name"):
        derived["service"] = f"service://{service['name']}"
    derived.update(event.get("entities") or {})
    return derived


def _record(state: dict[str, Any], event: dict[str, Any]) -> None:
    refs = state.setdefault("derived_from", [])
    eid = str(event.get("event_id") or "")
    if eid and len(refs) < _MAX_DERIVED_FROM and eid not in refs:
        refs.append(eid)


def provenance(
    state: dict[str, Any], rule: str, rule_version: int, derived_at: Any
) -> dict[str, Any]:
    """The §12.3 provenance block attached to a projection row."""
    return {
        "derived_from": list(state.get("derived_from") or []),
        "rule": rule,
        "rule_version": rule_version,
        "derived_at": derived_at.isoformat() if hasattr(derived_at, "isoformat") else derived_at,
    }


# -- run projection ---------------------------------------------------------


def new_run_state(run_id: str) -> dict[str, Any]:
    """Empty reducer state for one run."""
    return {
        "run_id": run_id,
        "job_name": None,
        "service_name": None,
        "environment": None,
        "status": "unknown",
        "started_at": None,
        "ended_at": None,
        "duration_ms": None,
        "trigger": None,
        "event_count": 0,
        "error_count": 0,
        "warning_count": 0,
        "branch": None,
        "asset_keys": set(),
        "derived_from": [],
    }


def _event_dt(event: dict[str, Any]) -> Any:
    return event.get("observed_at") or event.get("started_at")


def apply_run_event(state: dict[str, Any], event: dict[str, Any]) -> None:
    """Fold one canonical event into run reducer state.

    Order-tolerant: ``started_at`` keeps the earliest observed start,
    ``ended_at`` the latest terminal end, and status transitions take the
    maximum precedence, so replay order does not change the outcome.
    """
    corr = event.get("correlation") or {}
    service = event.get("service") or {}
    attrs = event.get("attributes") or {}
    severity = event.get("severity")
    outcome = event.get("outcome")

    _record(state, event)
    state["event_count"] += 1
    if event.get("error") is not None or severity in ("error", "critical"):
        state["error_count"] += 1
    elif severity == "warn":
        state["warning_count"] += 1

    asset_key = corr.get("asset_key")
    if asset_key and len(state["asset_keys"]) < _MAX_SUMMARY_ASSETS:
        state["asset_keys"].add(asset_key)

    for key, value in (
        ("branch", corr.get("branch")),
        ("job_name", corr.get("job_id")),
        ("service_name", service.get("name")),
        ("environment", service.get("environment")),
    ):
        if value and not state[key]:
            state[key] = value

    if event.get("event") in _TERMINAL_RUN_EVENTS:
        new_status = "running" if outcome in (None, "unknown") else outcome
        if _STATUS_PRECEDENCE.get(new_status, 0) >= _STATUS_PRECEDENCE.get(state["status"], 0):
            state["status"] = new_status
        if event.get("started_at") and (
            state["started_at"] is None or event["started_at"] < state["started_at"]
        ):
            state["started_at"] = event["started_at"]
        if outcome not in (None, "unknown"):
            end = event.get("ended_at") or event.get("observed_at")
            if end and (state["ended_at"] is None or end > state["ended_at"]):
                state["ended_at"] = end
        if event.get("duration_ms"):
            state["duration_ms"] = event["duration_ms"]
        if attrs.get("trigger"):
            state["trigger"] = attrs["trigger"]
    elif state["status"] == "unknown" and state["started_at"] is None:
        state["status"] = "running"
        state["started_at"] = event.get("started_at") or event.get("observed_at")

    if state["started_at"] is None:
        state["started_at"] = event.get("started_at") or event.get("observed_at")
    elif event.get("started_at") and event["started_at"] < state["started_at"]:
        state["started_at"] = event["started_at"]


# -- entity registry --------------------------------------------------------


def _branch_attributes(event: dict[str, Any]) -> dict[str, Any]:
    """WAP/Nessie lifecycle fields for a branch entity (spec §11, §9.2).

    Branch state lives in ``Entity.attributes``: create/validate/promote/
    reject/cleanup verbs update it, and ``iceberg.commit`` records the last
    commit observed on the branch. Last-write-wins per field; replay order
    matters only within a branch's own history, which is chronological.
    """
    name = str(event.get("event") or "")
    attrs = event.get("attributes") or {}
    outcome = event.get("outcome")
    raw_observed = event.get("observed_at")
    observed = raw_observed.isoformat() if hasattr(raw_observed, "isoformat") else raw_observed
    if name in ("wap.branch.create", "nessie.branch.create"):
        out: dict[str, Any] = {"state": "open", "created_at": observed}
        if attrs.get("base_branch"):
            out["base_branch"] = attrs["base_branch"]
        return out
    if name in ("wap.validate", "nessie.validate") or name.endswith(".validate"):
        return {"validation": outcome, "validated_at": observed}
    if name.endswith(".promote"):
        return {
            "state": "promoted" if outcome == "success" else "promotion_failed",
            "promoted_at": observed,
            "target": attrs.get("target"),
        }
    if name.endswith(".reject"):
        return {"state": "rejected", "rejected_at": observed}
    if name.endswith(".cleanup"):
        return {"state": "cleaned", "cleaned_at": observed}
    if name.endswith(".commit"):
        out = {"last_commit_at": observed}
        if attrs.get("snapshot_id"):
            out["last_snapshot_id"] = attrs["snapshot_id"]
        return out
    return {}


def entity_rows(event: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Entities touched by one event: ``entity_id -> partial row state``."""
    entities: dict[str, dict[str, Any]] = {}
    for role, eid in event_entities(event).items():
        kind = _ROLE_NAMESPACE.get(role, role)
        entities.setdefault(
            eid,
            {
                "entity_id": eid,
                "kind": kind,
                "display_name": eid.split("://", 1)[-1],
                "derived_from": [],
            },
        )
        if role == "branch":
            updates = _branch_attributes(event)
            if updates:
                entities[eid]["attributes"] = updates
    return entities


# -- relationship edges ------------------------------------------------------


@dataclass(frozen=True)
class Edge:
    """One inferred/explicit relationship to upsert (spec §13)."""

    from_entity: str
    to_entity: str
    relationship_type: str
    method: str = "explicit"
    confidence: float = 1.0


def edges_of(event: dict[str, Any]) -> list[Edge]:
    """Derive typed relationship edges from one event's entities.

    Edges are ``explicit`` when both endpoints came straight from envelope
    fields (confidence 1.0). The verb table supplies the edge type; unknown
    verbs still register a ``part_of`` edge between an event's run and its
    primary entity so the graph stays connected without guessing semantics.
    """
    entities = event_entities(event)
    edges: list[Edge] = []
    run = entities.get("run")
    service = entities.get("service")
    verb = _verb_of(str(event.get("event") or ""))
    edge_type = _VERB_EDGES.get(verb)

    if run and service:
        edges.append(Edge(service, run, "executes"))
    for role in ("asset", "table", "iceberg", "model", "snapshot"):
        target = entities.get(role)
        if run and target:
            edges.append(Edge(run, target, edge_type or "part_of"))
    if run and entities.get("branch"):
        edges.append(Edge(run, entities["branch"], "writes_to"))
    if entities.get("source") and entities.get("asset"):
        edges.append(Edge(entities["source"], entities["asset"], "produces"))
    if run and entities.get("deployment"):
        edges.append(Edge(run, entities["deployment"], "deployed_as"))
    return edges


def merge_edge_sources(existing: list[str], event_id: str) -> list[str]:
    """Bounded provenance for an edge: keep the earliest distinct sources."""
    if event_id in existing or len(existing) >= _MAX_EDGE_SOURCES:
        return existing
    return [*existing, event_id]


# -- asset projection ---------------------------------------------------------


def new_asset_state(entity_id: str, asset_key: str) -> dict[str, Any]:
    """Empty reducer state for one asset."""
    return {
        "entity_id": entity_id,
        "asset_key": asset_key,
        "status": "unknown",
        "last_materialized_at": None,
        "last_event_at": None,
        "freshness_sla_seconds": None,
        "attributes": {},
        "derived_from": [],
    }


def apply_asset_event(state: dict[str, Any], event: dict[str, Any]) -> None:
    """Fold one event into asset reducer state."""
    _record(state, event)
    observed = _event_dt(event)
    if observed and (state["last_event_at"] is None or observed > state["last_event_at"]):
        state["last_event_at"] = observed
    verb = _verb_of(str(event.get("event") or ""))
    if verb in ("materialize", "materialized", "produce", "write", "load", "execute"):
        outcome = event.get("outcome")
        if (
            outcome == "success"
            and observed
            and (state["last_materialized_at"] is None or observed > state["last_materialized_at"])
        ):
            state["last_materialized_at"] = observed
        if outcome == "failure":
            state["status"] = "failing"
        elif outcome == "success" and state["status"] in ("unknown", "failing"):
            state["status"] = "healthy" if state["status"] == "unknown" else "recovering"
    sla = (event.get("attributes") or {}).get("freshness_sla_seconds")
    if sla and not state["freshness_sla_seconds"]:
        state["freshness_sla_seconds"] = sla


def dedupe_key(rule_id: str, entity_id: str | None, event: dict[str, Any]) -> str:
    """Stable insight dedupe key (spec §16.2 lifecycle)."""
    basis = json.dumps(
        [rule_id, entity_id, event.get("event"), event_producer(event)],
        separators=(",", ":"),
    )
    return hashlib.sha256(basis.encode()).hexdigest()[:32]


@dataclass
class DerivedBatch:
    """Everything one event contributes to the projection tables."""

    run_id: str | None = None
    entities: dict[str, dict[str, Any]] = field(default_factory=dict)
    edges: list[Edge] = field(default_factory=list)
    asset_entity_id: str | None = None


def derive(event: dict[str, Any]) -> DerivedBatch:
    """One event's full projection contribution (incremental path)."""
    corr = event.get("correlation") or {}
    batch = DerivedBatch(
        run_id=corr.get("run_id"),
        entities=entity_rows(event),
        edges=edges_of(event),
    )
    asset_eid = event_entities(event).get("asset")
    if asset_eid:
        batch.asset_entity_id = asset_eid
    return batch
