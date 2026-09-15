"""Bounded agent tools over the observer API (spec §19.2).

Each tool is a thin, read-only function returning structured JSON suitable
for both a UI and an LLM agent. ``AGENT_TOOLS`` is the dispatch table
mapping a tool name to its callable so an agent harness can expose them as
function-calls without hand-wiring each one.
"""

from __future__ import annotations

from typing import Any

from observe_query.client import ObserverClient


def get_run(client: ObserverClient, run_id: str) -> dict[str, Any]:
    """Run projection and touched entities."""
    return client.get_run(run_id)


def get_run_timeline(client: ObserverClient, run_id: str) -> dict[str, Any]:
    """Run projection plus phase-grouped events."""
    return client.run_timeline(run_id)


def get_event(client: ObserverClient, event_id: str) -> dict[str, Any]:
    """One canonical event by id."""
    return client.get_event(event_id)


def search_events(client: ObserverClient, **filters: Any) -> dict[str, Any]:
    """Structured search over canonical event fields."""
    return client.search_events(**filters)


def get_asset_health(client: ObserverClient, entity_id: str) -> dict[str, Any]:
    """Asset status, freshness and open insights."""
    return client.asset_health(entity_id)


def get_asset_history(client: ObserverClient, entity_id: str, limit: int = 200) -> dict[str, Any]:
    """Ordered event history for an asset."""
    return client.asset_history(entity_id, limit=limit)


def get_failures(client: ObserverClient, run_id: str) -> dict[str, Any]:
    """Failure events for a run with evidence IDs."""
    return client.run_failures(run_id)


def get_incident(client: ObserverClient, incident_id: str) -> dict[str, Any]:
    """One incident with its grouped insights."""
    return client.get_incident(incident_id)


def get_related_changes(
    client: ObserverClient, run_id: str, window_hours: float = 24.0
) -> dict[str, Any]:
    """Change events in the window before a run."""
    return client.run_changes(run_id, window_hours=window_hours)


def get_lineage(client: ObserverClient, entity_id: str) -> dict[str, Any]:
    """Upstream/downstream relationship edges for an entity."""
    return client.asset_lineage(entity_id)


def compare_runs(client: ObserverClient, run_a: str, run_b: str) -> dict[str, Any]:
    """Side-by-side run comparison."""
    return client.compare_runs(run_a, run_b)


def explain_relationship(client: ObserverClient, entity_id: str) -> dict[str, Any]:
    """Relationship edges for an entity with method/confidence/evidence."""
    return client.asset_lineage(entity_id)


AGENT_TOOLS: dict[str, Any] = {
    "get_run": get_run,
    "get_run_timeline": get_run_timeline,
    "get_event": get_event,
    "search_events": search_events,
    "get_asset_health": get_asset_health,
    "get_asset_history": get_asset_history,
    "get_failures": get_failures,
    "get_incident": get_incident,
    "get_related_changes": get_related_changes,
    "get_lineage": get_lineage,
    "compare_runs": compare_runs,
    "explain_relationship": explain_relationship,
}
"""Spec §19.2 tool surface: name -> callable(client, **kwargs)."""


def call_tool(client: ObserverClient, name: str, **kwargs: Any) -> dict[str, Any]:
    """Dispatch a named agent tool."""
    fn = AGENT_TOOLS.get(name)
    if fn is None:
        raise KeyError(f"unknown agent tool: {name}")
    return fn(client, **kwargs)
