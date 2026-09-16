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

import bisect
import datetime as dt
import hashlib
import json
from dataclasses import dataclass, field
from typing import Any

from observe_core.timestamps import parse_rfc3339

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
# ``dbt.invocation`` is terminal too: the run_results document is written at
# the end of the invocation and carries the invocation's own outcome/duration.
_TERMINAL_RUN_EVENTS = {"pipeline.run", "dlt.pipeline.run", "dbt.invocation"}

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
    """Record provenance for one contributing event, earliest-observed wins.

    ``derived_keys`` holds the ``(observed_at, event_id)`` positions of the
    retained sources — the window keeps the earliest *observed* events, so
    late arrivals fold into place and incremental/rebuild provenance sets
    converge.
    """
    eid = str(event.get("event_id") or "")
    if not eid:
        return
    keys = state.setdefault("derived_keys", [])
    _insert_sorted(keys, event_key(event), _MAX_DERIVED_FROM, keep="earliest")
    state["derived_from"] = [k[1] for k in keys]


def parse_keys(raw: Any) -> list[tuple[Any, str]]:
    """A stored ``[[iso, eid], ...]`` key list, parsed and sorted."""
    keys = [k for k in (_parse_key(entry) for entry in raw or []) if k is not None]
    keys.sort()
    return keys


def _derived_keys_from(state_or_fold: dict[str, Any], derived_from: list[str]) -> list[tuple]:
    """``derived_keys`` for a row written before keyed provenance existed."""
    stored = parse_keys(state_or_fold.get("derived_keys"))
    if stored:
        return stored
    return [(_EPOCH, str(eid)) for eid in derived_from or []]


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


# -- event-time ordering ------------------------------------------------------
#
# Incremental ingest folds events in arrival order across batches; a rebuild
# folds the whole history in observed order. Any reducer state whose value
# depends on *which* event wrote it (status fields, lifecycle attributes,
# durations, baselines) must therefore be keyed on the event's fold-order
# position — ``(observed_at, event_id)`` — rather than on fold position, or
# late and out-of-order events leave incremental state diverged from a
# rebuild. ``field_at``/``attr_at``/``materialize_times`` and friends carry
# that bookkeeping and round-trip through ``fold_state`` columns.

_EPOCH = dt.datetime(1970, 1, 1, tzinfo=dt.UTC)


def _as_dt(value: Any) -> Any:
    """Normalize an event timestamp to a tz-aware datetime (None-tolerant).

    Canonical rows carry datetimes; reducer unit tests and V1 envelopes may
    carry RFC3339 strings. Keys and comparisons must never mix the two —
    ``str < datetime`` raises, and ``str.replace(tzinfo=...)`` is not the
    datetime method.
    """
    if value is None:
        return None
    if isinstance(value, str):
        try:
            return parse_rfc3339(value)
        except ValueError:
            return None
    if getattr(value, "tzinfo", None) is None:
        return value.replace(tzinfo=dt.UTC)
    return value


def event_key(event: dict[str, Any]) -> tuple[Any, str]:
    """Canonical fold-order position of one event: (observed_at, event_id)."""
    observed = _as_dt(_event_dt(event))
    if observed is None:
        observed = _EPOCH
    return (observed, str(event.get("event_id") or ""))


def _dump_key(key: tuple[Any, str]) -> list[Any]:
    at, eid = key
    return [at.isoformat() if hasattr(at, "isoformat") else at, eid]


def _parse_key(raw: Any) -> tuple[Any, str] | None:
    """Inverse of ``_dump_key``; tolerant of malformed stored keys."""
    if not isinstance(raw, (list, tuple)) or len(raw) < 2:
        return None
    at = _as_dt(raw[0])
    if at is None:
        return None
    return (at, str(raw[1]))


def _dump_field_at(field_at: dict[str, tuple[Any, str]]) -> dict[str, list[Any]]:
    return {name: _dump_key(key) for name, key in field_at.items()}


def _parse_field_at(raw: Any) -> dict[str, tuple[Any, str]]:
    out: dict[str, tuple[Any, str]] = {}
    for name, key in (raw or {}).items():
        parsed = _parse_key(key)
        if parsed is not None:
            out[str(name)] = parsed
    return out


def _insert_sorted(items: list[tuple], key: tuple, cap: int, *, keep: str = "newest") -> None:
    """Insert ``key`` into a sorted list, bounded to ``cap`` entries.

    ``keep="newest"`` retains the largest keys (materialization times);
    ``keep="earliest"`` retains the smallest (provenance windows).
    """
    if key in items:
        return
    bisect.insort(items, key)
    if len(items) > cap:
        if keep == "earliest":
            del items[cap:]
        else:
            del items[: len(items) - cap]


def _set_latest(state: dict[str, Any], field_name: str, value: Any, key: tuple[Any, str]) -> None:
    """Write ``value`` when the event is the newest writer of the field.

    The write wins when the event's ``(observed_at, event_id)`` key is at or
    after the key that last wrote the field — a late event can fill a field
    but never overwrite a newer observation, so arrival order cannot regress
    projected state.
    """
    if value in (None, ""):
        return
    at_map = state.setdefault("field_at", {})
    current = at_map.get(field_name)
    if current is None or key >= current:
        state[field_name] = value
        at_map[field_name] = key


def merge_attributes(state: dict[str, Any], updates: dict[str, Any], key: tuple[Any, str]) -> None:
    """Merge per-event attribute updates, newest observation wins per field.

    ``state["attr_at"]`` records the ``(observed_at, event_id)`` key that last
    wrote each field, so a late ``wap.branch.create`` cannot rewind ``state``
    past a promotion it predates — and a rebuild in observed order converges
    to the same attributes.
    """
    at_map = state.setdefault("attr_at", {})
    merged = dict(state.get("attributes") or {})
    for name, value in updates.items():
        current = at_map.get(name)
        if current is None or key >= current:
            merged[name] = value
            at_map[name] = key
    state["attributes"] = merged


def gated_attr_merge(
    target_attrs: dict[str, Any],
    target_at: dict[str, tuple[Any, str]],
    new_attrs: dict[str, Any],
    new_at: dict[str, tuple[Any, str]],
    *,
    legacy_gate: tuple[Any, str] | None = None,
) -> None:
    """Merge accumulated attributes into an existing row, per-field gated.

    ``legacy_gate`` substitutes for missing ``target_at`` entries on rows
    written before field keys existed: a field already present is treated as
    set at the row's last-seen timestamp so a late event cannot regress it.
    """
    for name, value in new_attrs.items():
        current = target_at.get(name)
        if current is None and name in (target_attrs or {}):
            current = legacy_gate
        new_key = new_at.get(name)
        if new_key is None:
            continue
        if current is None or new_key >= current:
            target_attrs[name] = value
            target_at[name] = new_key


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
        "derived_keys": [],
        "field_at": {},
        "min_started": None,
        "first_key": None,
        "first_fallback": None,
    }


def run_fold_state(state: dict[str, Any]) -> dict[str, Any]:
    """Serializable bookkeeping for ``Run.fold_state``."""
    return {
        "field_at": _dump_field_at(state.get("field_at") or {}),
        "derived_keys": [_dump_key(k) for k in state.get("derived_keys") or []],
        "run_start": {
            "min_started": (state["min_started"].isoformat() if state.get("min_started") else None),
            "first": _dump_key(state["first_key"]) if state.get("first_key") else None,
            "first_fallback": (
                state["first_fallback"].isoformat() if state.get("first_fallback") else None
            ),
        },
    }


def _event_dt(event: dict[str, Any]) -> Any:
    return event.get("observed_at") or event.get("started_at")


def apply_run_event(state: dict[str, Any], event: dict[str, Any]) -> None:
    """Fold one canonical event into run reducer state.

    Order-tolerant: status takes maximum precedence, ``ended_at`` the
    latest terminal end, and value fields (``duration_ms``, ``trigger``,
    metadata) are won by the newest observation. ``started_at`` reproduces
    the replay result exactly — the earliest-observed event contributes its
    ``started_at`` or, lacking one, its ``observed_at``, then every event's
    ``started_at`` can only lower it — so arrival order never changes it.
    """
    corr = event.get("correlation") or {}
    service = event.get("service") or {}
    attrs = event.get("attributes") or {}
    severity = event.get("severity")
    outcome = event.get("outcome")
    key = event_key(event)
    started = _as_dt(event.get("started_at"))
    ended = _as_dt(event.get("ended_at"))
    observed = _as_dt(event.get("observed_at"))

    _record(state, event)
    state["event_count"] += 1
    if event.get("error") is not None or severity in ("error", "critical"):
        state["error_count"] += 1
    elif severity == "warn":
        state["warning_count"] += 1

    asset_key = corr.get("asset_key")
    if asset_key and len(state["asset_keys"]) < _MAX_SUMMARY_ASSETS:
        state["asset_keys"].add(asset_key)

    for field_name, value in (
        ("branch", corr.get("branch")),
        ("job_name", corr.get("job_id")),
        ("service_name", service.get("name")),
        ("environment", service.get("environment")),
    ):
        _set_latest(state, field_name, value, key)

    if event.get("event") in _TERMINAL_RUN_EVENTS:
        new_status = "running" if outcome in (None, "unknown") else outcome
        if _STATUS_PRECEDENCE.get(new_status, 0) >= _STATUS_PRECEDENCE.get(state["status"], 0):
            state["status"] = new_status
        if outcome not in (None, "unknown"):
            end = ended or observed
            if end and (state["ended_at"] is None or end > state["ended_at"]):
                state["ended_at"] = end
        _set_latest(state, "duration_ms", event.get("duration_ms"), key)
        _set_latest(state, "trigger", attrs.get("trigger"), key)
    elif _STATUS_PRECEDENCE.get(state["status"], 0) < _STATUS_PRECEDENCE["running"]:
        # A non-terminal event is evidence the run exists: replay marks the
        # run running until a terminal event supersedes it.
        state["status"] = "running"

    # started_at = min(first-observed event's started-or-observed, every
    # event's started_at). Track both pieces so a late event that precedes
    # the previously-earliest event still produces the replay value.
    if started is not None and (state["min_started"] is None or started < state["min_started"]):
        state["min_started"] = started
    if state["first_key"] is None or key < state["first_key"]:
        state["first_key"] = key
        state["first_fallback"] = observed if started is None else None
    candidates = [c for c in (state["min_started"], state["first_fallback"]) if c is not None]
    state["started_at"] = min(candidates) if candidates else None


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


# -- asset projection ---------------------------------------------------------


_MATERIALIZE_VERBS = ("materialize", "materialized", "produce", "write", "load", "execute")
_MAX_MATERIALIZE_TIMES = 64
"""Retained successful-materialization keys per asset — bounds the history a
late event's freshness check can look back across."""


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
        "derived_keys": [],
        "materialize_times": [],
        "last_failure": None,
        "field_at": {},
    }


def _asset_status(state: dict[str, Any]) -> str:
    """Status as a pure function of the newest materialization outcomes.

    ``materialize_times``/``last_failure`` hold the newest success/failure
    ``(observed_at, event_id)`` keys; comparing them decides the status the
    *latest observed* outcome implies, independent of fold order — a failure
    arriving late after a newer success leaves the asset ``recovering``, not
    ``failing``.
    """
    times = state.get("materialize_times") or []
    last_success = times[-1] if times else None
    last_failure = state.get("last_failure")
    if last_failure is None:
        return "healthy" if last_success else "unknown"
    if last_success is None or last_failure >= last_success:
        return "failing"
    return "recovering"


def materialized_before(state: dict[str, Any], key: tuple[Any, str]) -> Any:
    """Newest successful materialization observed strictly before ``key``.

    Freshness rules evaluate against the asset's history *as of* the
    checking event, not as of now — a late event must see only the
    materializations that precede its position in observed order.
    """
    times = state.get("materialize_times") or []
    i = bisect.bisect_left(times, key)
    return times[i - 1][0] if i else None


def apply_asset_event(state: dict[str, Any], event: dict[str, Any]) -> None:
    """Fold one event into asset reducer state (order-insensitive)."""
    _record(state, event)
    observed = _as_dt(_event_dt(event))
    key = event_key(event)
    if observed and (state["last_event_at"] is None or observed > state["last_event_at"]):
        state["last_event_at"] = observed
    verb = _verb_of(str(event.get("event") or ""))
    if verb in _MATERIALIZE_VERBS and observed is not None:
        outcome = event.get("outcome")
        if outcome == "success":
            _insert_sorted(state["materialize_times"], key, _MAX_MATERIALIZE_TIMES)
            if state["last_materialized_at"] is None or observed > state["last_materialized_at"]:
                state["last_materialized_at"] = observed
        elif outcome == "failure":
            if state["last_failure"] is None or key > state["last_failure"]:
                state["last_failure"] = key
        state["status"] = _asset_status(state)
    sla = (event.get("attributes") or {}).get("freshness_sla_seconds")
    _set_latest(state, "freshness_sla_seconds", sla, key)


def asset_fold_state(state: dict[str, Any]) -> dict[str, Any]:
    """Serializable bookkeeping for ``Asset.fold_state``."""
    return {
        "materialize_times": [_dump_key(k) for k in state.get("materialize_times") or []],
        "last_failure": (_dump_key(state["last_failure"]) if state.get("last_failure") else None),
        "field_at": _dump_field_at(state.get("field_at") or {}),
        "derived_keys": [_dump_key(k) for k in state.get("derived_keys") or []],
    }


def asset_state_from_row(row: Any) -> dict[str, Any]:
    """Reducer state seeded from a stored ``Asset`` row (incremental paths).

    Rows written before fold bookkeeping existed get conservative defaults:
    a ``failing`` row's failure is anchored at its last event, a
    ``recovering`` row's at the epoch, and a recorded SLA gates at the last
    event — so a late event cannot silently regress pre-migration state.
    """
    fs = row.fold_state or {}
    materialize_times = [
        key for k in fs.get("materialize_times") or [] if (key := _parse_key(k)) is not None
    ]
    if not materialize_times and row.last_materialized_at is not None:
        materialize_times = [(row.last_materialized_at, "")]
    last_failure = _parse_key(fs.get("last_failure"))
    if last_failure is None and row.status in ("failing", "recovering"):
        anchor = (
            (row.last_event_at or row.last_materialized_at or _EPOCH)
            if row.status == "failing"
            else _EPOCH
        )
        last_failure = (anchor, "")
    field_at = _parse_field_at(fs.get("field_at"))
    if row.freshness_sla_seconds is not None and "freshness_sla_seconds" not in field_at:
        field_at["freshness_sla_seconds"] = (row.last_event_at or _EPOCH, "")
    state = new_asset_state(row.entity_id, row.asset_key)
    state.update(
        {
            "status": row.status,
            "last_materialized_at": row.last_materialized_at,
            "last_event_at": row.last_event_at,
            "freshness_sla_seconds": row.freshness_sla_seconds,
            "attributes": dict(row.attributes or {}),
            "derived_from": list((row.provenance or {}).get("derived_from") or []),
            "derived_keys": _derived_keys_from(
                fs, (row.provenance or {}).get("derived_from") or []
            ),
            "materialize_times": materialize_times,
            "last_failure": last_failure,
            "field_at": field_at,
        }
    )
    return state


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
