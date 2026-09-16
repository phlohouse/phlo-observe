"""V2 query API endpoints (spec §20, §22)."""

from __future__ import annotations

import uuid
from typing import Any

import pytest


def _event(
    *,
    event: str = "pipeline.run",
    outcome: str = "success",
    run_id: str | None = "run-v2",
    asset_key: str | None = None,
    observed_at: str = "2025-01-01T00:00:00Z",
    duration_ms: float | None = None,
    error: dict[str, Any] | None = None,
    attributes: dict[str, Any] | None = None,
    category: str = "pipeline",
) -> dict[str, Any]:
    corr: dict[str, Any] = {}
    if run_id:
        corr["run_id"] = run_id
    if asset_key:
        corr["asset_key"] = asset_key
    body: dict[str, Any] = {
        "schema_version": "2.0",
        "event_id": str(uuid.uuid4()),
        "event": event,
        "category": category,
        "outcome": outcome,
        "severity": "info",
        "delivery": "telemetry",
        "observed_at": observed_at,
        "service": {"name": "svc"},
        "correlation": corr,
        "attributes": attributes or {},
        "source": {"producer": "dagster"},
    }
    if duration_ms is not None:
        body["duration_ms"] = duration_ms
    if error:
        body["error"] = error
    return body


async def _post(client: Any, events: list[dict[str, Any]]) -> None:
    resp = await client.post("/v1/events", json=events)
    assert resp.status_code == 202, resp.text


@pytest.mark.asyncio
class TestRunEndpoints:
    async def test_v2_run(self, client: Any) -> None:
        await _post(client, [_event(run_id="v2-run", outcome="success")])
        resp = await client.get("/v2/runs/v2-run")
        assert resp.status_code == 200
        body = resp.json()
        assert body["run_id"] == "v2-run"
        assert body["status"] == "success"
        assert body["provenance"]["rule"] == "run-state-v2"

    async def test_v2_run_404(self, client: Any) -> None:
        resp = await client.get("/v2/runs/nonexistent")
        assert resp.status_code == 404

    async def test_v2_failures(self, client: Any) -> None:
        await _post(
            client,
            [
                _event(
                    run_id="fail-run",
                    outcome="failure",
                    error={"message": "boom"},
                ),
                _event(
                    run_id="fail-run",
                    event="pipeline.step",
                    outcome="success",
                ),
            ],
        )
        resp = await client.get("/v2/runs/fail-run/failures")
        assert resp.status_code == 200
        body = resp.json()
        assert len(body["failures"]) == 1
        assert body["failures"][0]["error"]["message"] == "boom"
        assert len(body["evidence"]) == 1

    async def test_v2_changes(self, client: Any) -> None:
        await _post(
            client,
            [
                _event(
                    event="deployment.deploy",
                    run_id=None,
                    category="infrastructure",
                    observed_at="2025-01-01T00:00:00Z",
                ),
                _event(
                    run_id="chg-run",
                    outcome="failure",
                    observed_at="2025-01-01T01:00:00Z",
                ),
            ],
        )
        resp = await client.get("/v2/runs/chg-run/changes")
        assert resp.status_code == 200
        body = resp.json()
        assert len(body["changes"]) == 1
        assert body["changes"][0]["event"] == "deployment.deploy"

    async def test_v2_impact(self, client: Any) -> None:
        await _post(
            client,
            [
                _event(
                    event="asset.materialize",
                    run_id="imp-run",
                    asset_key="gold/out",
                    outcome="success",
                )
            ],
        )
        resp = await client.get("/v2/runs/imp-run/impact")
        assert resp.status_code == 200
        body = resp.json()
        assert "asset://gold/out" in (body["impact"].get("produces") or [])

    async def test_v2_investigate(self, client: Any) -> None:
        await _post(
            client,
            [
                _event(
                    run_id="inv-run",
                    outcome="failure",
                    error={"message": "disk full"},
                )
            ],
        )
        resp = await client.get("/v2/runs/inv-run/investigate")
        assert resp.status_code == 200
        body = resp.json()
        assert body["run_id"] == "inv-run"
        assert body["failure"]["count"] == 1
        assert body["candidate_causes"]
        assert body["evidence"]

    async def test_v2_investigate_truncated_only_beyond_cap(
        self, client: Any, monkeypatch: Any
    ) -> None:
        """Regression: a run with exactly the cap of events is not truncated.

        The bundle over-fetches by one so ``truncated`` is true only when an
        event was actually left out — previously a run at exactly the cap
        reported a false positive.
        """
        import phlo_observer.query_v2 as q2

        monkeypatch.setattr(q2, "_MAX_RUN_EVENTS", 3)
        await _post(client, [_event(run_id="cap-run") for _ in range(3)])
        body = (await client.get("/v2/runs/cap-run/investigate")).json()
        assert body["truncated"] is False
        assert len(body["evidence"]) == 3

        await _post(client, [_event(run_id="cap-run")])
        body = (await client.get("/v2/runs/cap-run/investigate")).json()
        assert body["truncated"] is True
        assert len(body["evidence"]) == 3

    async def test_v2_compare_runs(self, client: Any) -> None:
        await _post(
            client,
            [
                _event(run_id="cmp-a", outcome="success", duration_ms=1000.0),
                _event(run_id="cmp-b", outcome="failure", duration_ms=2500.0),
            ],
        )
        resp = await client.post(
            "/v2/query/compare-runs", json={"run_a": "cmp-a", "run_b": "cmp-b"}
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["duration_delta_ms"] == 1500.0
        assert body["status_changed"] is True

    async def test_v2_failures_includes_errorless_failures(self, client: Any) -> None:
        """Regression: a failure without an error payload is still a failure."""
        await _post(
            client,
            [
                _event(run_id="nofail-err", outcome="failure"),  # no error body
                _event(run_id="nofail-err", event="pipeline.step", outcome="success"),
            ],
        )
        resp = await client.get("/v2/runs/nofail-err/failures")
        assert resp.status_code == 200
        body = resp.json()
        assert len(body["failures"]) == 1
        assert body["failures"][0]["outcome"] == "failure"

    async def test_v2_failures_excludes_json_null_error(self, client: Any) -> None:
        """A success event carrying an explicit JSON null error is not a failure."""
        ev = _event(run_id="nullerr", outcome="success")
        ev["error"] = None  # serialized as JSON null, not absent
        await _post(client, [ev])
        resp = await client.get("/v2/runs/nullerr/failures")
        assert resp.status_code == 200
        assert resp.json()["failures"] == []

    async def test_v2_impact_like_metachars_escaped(self, client: Any) -> None:
        """Regression: run_id LIKE metacharacters must match literally.

        ``a_b`` must not see edges belonging to ``axb`` — the underscore is
        data, not a wildcard.
        """
        await _post(
            client,
            [
                _event(
                    event="asset.materialize",
                    run_id="axb",
                    asset_key="other/asset",
                    outcome="success",
                ),
                _event(
                    event="asset.materialize",
                    run_id="a_b",
                    asset_key="own/asset",
                    outcome="success",
                ),
            ],
        )
        resp = await client.get("/v2/runs/a_b/impact")
        assert resp.status_code == 200
        impacted = [e for edges in resp.json()["impact"].values() for e in edges]
        assert "asset://own/asset" in impacted
        assert "asset://other/asset" not in impacted

    async def test_v2_changes_reports_truncation_flag(self, client: Any) -> None:
        """run_changes answers from a bounded SQL-filtered scan and reports
        truncation instead of silently dropping rows."""
        await _post(
            client,
            [
                _event(
                    event="deployment.deploy",
                    run_id=None,
                    category="infrastructure",
                    observed_at="2025-01-01T00:00:00Z",
                ),
                _event(run_id="trunc-run", observed_at="2025-01-01T01:00:00Z"),
            ],
        )
        resp = await client.get("/v2/runs/trunc-run/changes")
        assert resp.status_code == 200
        body = resp.json()
        assert body["truncated"] is False
        assert len(body["changes"]) == 1


@pytest.mark.asyncio
class TestAssetEndpoints:
    async def test_v2_asset(self, client: Any) -> None:
        await _post(
            client,
            [
                _event(
                    event="asset.materialized",
                    asset_key="a/b",
                    outcome="success",
                )
            ],
        )
        resp = await client.get("/v2/assets/asset://a/b")
        assert resp.status_code == 200
        body = resp.json()
        assert body["asset_key"] == "a/b"
        assert body["status"] == "healthy"

    async def test_v2_asset_bare_key(self, client: Any) -> None:
        await _post(
            client,
            [_event(event="asset.materialized", asset_key="c/d", outcome="success")],
        )
        resp = await client.get("/v2/assets/c/d")
        assert resp.status_code == 200
        assert resp.json()["entity_id"] == "asset://c/d"

    async def test_v2_asset_health(self, client: Any) -> None:
        import datetime as dt

        recent = dt.datetime.now(dt.UTC).isoformat().replace("+00:00", "Z")
        await _post(
            client,
            [
                _event(
                    event="asset.materialized",
                    asset_key="h/one",
                    outcome="success",
                    attributes={"freshness_sla_seconds": 86400},
                    observed_at=recent,
                )
            ],
        )
        resp = await client.get("/v2/assets/h/one/health")
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "healthy"
        assert body["freshness"]["breached"] is False

    async def test_v2_asset_history(self, client: Any) -> None:
        await _post(
            client,
            [
                _event(
                    event="asset.materialized",
                    asset_key="hist/x",
                    outcome="success",
                    observed_at="2025-01-01T00:00:00Z",
                ),
                _event(
                    event="quality.check",
                    asset_key="hist/x",
                    outcome="success",
                    observed_at="2025-01-01T00:05:00Z",
                ),
            ],
        )
        resp = await client.get("/v2/assets/hist/x/history")
        assert resp.status_code == 200
        assert len(resp.json()["history"]) == 2

    async def test_v2_asset_lineage(self, client: Any) -> None:
        await _post(
            client,
            [
                _event(
                    event="asset.materialize",
                    run_id="lin-run",
                    asset_key="lin/y",
                    outcome="success",
                )
            ],
        )
        resp = await client.get("/v2/assets/lin/y/lineage")
        assert resp.status_code == 200
        body = resp.json()
        assert any(
            e["type"] == "produces" and e["from"].endswith("lin-run") for e in body["upstream"]
        )


@pytest.mark.asyncio
class TestInsightIncidentEndpoints:
    async def test_v2_insights_list(self, client: Any) -> None:
        await _post(
            client,
            [_event(run_id="ins-run", outcome="failure", error={"message": "x"})],
        )
        resp = await client.get("/v2/insights")
        assert resp.status_code == 200
        items = resp.json()["items"]
        assert any(i["rule"] == "run-failure" for i in items)

    async def test_v2_insights_state_filter(self, client: Any) -> None:
        await _post(
            client,
            [_event(run_id="ins2", outcome="failure", error={"message": "x"})],
        )
        resp = await client.get("/v2/insights?state=open")
        assert all(i["state"] == "open" for i in resp.json()["items"])
        resp = await client.get("/v2/insights?state=resolved")
        assert all(i["state"] == "resolved" for i in resp.json()["items"])

    async def test_v2_incidents(self, client: Any) -> None:
        await _post(
            client,
            [_event(run_id="inc-run", outcome="failure", error={"message": "x"})],
        )
        resp = await client.get("/v2/incidents")
        assert resp.status_code == 200
        items = resp.json()["items"]
        assert len(items) == 1
        iid = items[0]["incident_id"]
        detail = await client.get(f"/v2/incidents/{iid}")
        assert detail.status_code == 200
        assert detail.json()["insights"]

    async def test_v2_event_provenance(self, client: Any) -> None:
        event = _event(run_id="prov-run", asset_key="p/a", outcome="success")
        await _post(client, [event])
        resp = await client.get(f"/v2/events/{event['event_id']}/provenance")
        assert resp.status_code == 200
        body = resp.json()
        assert "prov-run" in body["projections"]["run"]

    async def test_v2_event_provenance_404(self, client: Any) -> None:
        resp = await client.get(f"/v2/events/{uuid.uuid4()}/provenance")
        assert resp.status_code == 404
