"""Typed read-only client for the phlo-observer V2 API (spec §19.1, §19.4).

The client only exposes read endpoints — agent access is read-only by
default; privileged actions live behind a separate authorized surface.
"""

from __future__ import annotations

from typing import Any

import httpx


class ObserverError(RuntimeError):
    """The observer returned an error response."""


class ObserverClient:
    """Synchronous read client for ``/v1`` and ``/v2`` endpoints.

    ``base_url`` points at the observer root (``http://host:8080``);
    ``token`` is a read-scope token when the deployment requires auth.
    """

    def __init__(
        self,
        base_url: str,
        *,
        token: str | None = None,
        timeout: float = 30.0,
        client: httpx.Client | None = None,
    ) -> None:
        headers = {"Authorization": f"Bearer {token}"} if token else {}
        self._http = client or httpx.Client(
            base_url=base_url.rstrip("/"), timeout=timeout, headers=headers
        )

    def close(self) -> None:
        """Release the underlying HTTP connection pool."""
        self._http.close()

    def __enter__(self) -> ObserverClient:
        """Context-manager entry returning the client."""
        return self

    def __exit__(self, *exc: object) -> None:
        """Close the HTTP pool on context exit."""
        self.close()

    def _get(self, path: str, **params: Any) -> dict[str, Any]:
        resp = self._http.get(path, params={k: v for k, v in params.items() if v is not None})
        if resp.status_code == 404:
            raise ObserverError(f"not found: {path}")
        if resp.status_code >= 400:
            raise ObserverError(f"{path}: HTTP {resp.status_code}: {resp.text[:200]}")
        return resp.json()

    def _post(self, path: str, body: dict[str, Any]) -> dict[str, Any]:
        resp = self._http.post(path, json=body)
        if resp.status_code >= 400:
            raise ObserverError(f"{path}: HTTP {resp.status_code}: {resp.text[:200]}")
        return resp.json()

    # -- runs ---------------------------------------------------------------

    def run(self, run_id: str) -> Run:
        """A run handle for chained lookups."""
        return Run(self, run_id)

    def get_run(self, run_id: str) -> dict[str, Any]:
        """Run projection plus touched entities."""
        return self._get(f"/v2/runs/{run_id}")

    def run_timeline(self, run_id: str) -> dict[str, Any]:
        """Run projection plus phase-grouped events."""
        return self._get(f"/v2/runs/{run_id}/timeline")

    def run_failures(self, run_id: str) -> dict[str, Any]:
        """Failed/error events for the run, with evidence IDs."""
        return self._get(f"/v2/runs/{run_id}/failures")

    def run_changes(self, run_id: str, *, window_hours: float = 24.0) -> dict[str, Any]:
        """Change events in the window before the run."""
        return self._get(f"/v2/runs/{run_id}/changes", window_hours=window_hours)

    def run_impact(self, run_id: str) -> dict[str, Any]:
        """Downstream entities reachable through this run's edges."""
        return self._get(f"/v2/runs/{run_id}/impact")

    def investigate(self, run_id: str) -> dict[str, Any]:
        """Deterministic investigation bundle for a run (spec §20)."""
        return self._get(f"/v2/runs/{run_id}/investigate")

    def compare_runs(self, run_a: str, run_b: str) -> dict[str, Any]:
        """Side-by-side run comparison."""
        return self._post("/v2/query/compare-runs", {"run_a": run_a, "run_b": run_b})

    # -- assets ---------------------------------------------------------------

    def get_asset(self, entity_id: str) -> dict[str, Any]:
        """Asset projection by canonical entity id or bare key."""
        return self._get(f"/v2/assets/{entity_id}")

    def asset_health(self, entity_id: str) -> dict[str, Any]:
        """Status, freshness SLA state and open insights."""
        return self._get(f"/v2/assets/{entity_id}/health")

    def asset_history(self, entity_id: str, *, limit: int = 200) -> dict[str, Any]:
        """Ordered event history for an asset."""
        return self._get(f"/v2/assets/{entity_id}/history", limit=limit)

    def asset_lineage(self, entity_id: str) -> dict[str, Any]:
        """Upstream/downstream relationship edges."""
        return self._get(f"/v2/assets/{entity_id}/lineage")

    # -- events ---------------------------------------------------------------

    def get_event(self, event_id: str) -> dict[str, Any]:
        """One canonical event."""
        return self._get(f"/v1/events/{event_id}")

    def event_provenance(self, event_id: str) -> dict[str, Any]:
        """Which projections an event contributed to."""
        return self._get(f"/v2/events/{event_id}/provenance")

    def search_events(self, **filters: Any) -> dict[str, Any]:
        """Structured event search over /v1/events filters."""
        return self._get("/v1/events", **filters)

    # -- insights / incidents ---------------------------------------------------

    def insights(
        self, *, state: str | None = None, entity: str | None = None, limit: int = 100
    ) -> list[dict[str, Any]]:
        """Insights, newest first."""
        return self._get("/v2/insights", state=state, entity=entity, limit=limit)["items"]

    def incidents(self, *, state: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        """Incidents, most recently updated first."""
        return self._get("/v2/incidents", state=state, limit=limit)["items"]

    def get_incident(self, incident_id: str) -> dict[str, Any]:
        """One incident with its grouped insights."""
        return self._get(f"/v2/incidents/{incident_id}")


class Run:
    """A bound run handle — ``client.run("x").failures()``."""

    def __init__(self, client: ObserverClient, run_id: str) -> None:
        self._client = client
        self.run_id = run_id

    def get(self) -> dict[str, Any]:
        """The run projection."""
        return self._client.get_run(self.run_id)

    def timeline(self) -> dict[str, Any]:
        """Phase-grouped events."""
        return self._client.run_timeline(self.run_id)

    def failures(self) -> dict[str, Any]:
        """Failure events with evidence IDs."""
        return self._client.run_failures(self.run_id)

    def recent_changes(self, *, window_hours: float = 24.0) -> dict[str, Any]:
        """Changes in the window preceding this run."""
        return self._client.run_changes(self.run_id, window_hours=window_hours)

    def affected_downstream(self) -> dict[str, Any]:
        """Downstream entities impacted via edges."""
        return self._client.run_impact(self.run_id)

    def investigate(self) -> dict[str, Any]:
        """Full investigation bundle."""
        return self._client.investigate(self.run_id)

    def compare(self, other_run_id: str) -> dict[str, Any]:
        """Compare this run to another."""
        return self._client.compare_runs(self.run_id, other_run_id)
