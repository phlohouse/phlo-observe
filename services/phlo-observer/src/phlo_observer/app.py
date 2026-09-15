"""FastAPI application factory for phlo-observer.

Routes:

- ``POST /v1/events``                 canonical ingestion (single or array, gzip ok)
- ``POST /v1/ingest/{dagster,dbt,generic}``  source-specific ingestion
- ``POST /v1/ingest/dbt/artifacts``   run_results + manifest document
- ``POST /v1/ingest/otlp``            OTLP/HTTP JSON logs -> one canonical event per logRecord
- ``GET  /v1/events``                 filtered, cursor-paginated query
- ``GET  /v1/events/{event_id}``      single event
- ``GET  /v1/runs`` / ``/v1/runs/{id}`` / ``/v1/runs/{id}/timeline``
- ``GET  /v1/assets/{key}/events`` / ``/v1/branches/{b}/events`` / ``/v1/tables/{t}/events``
- ``GET  /health/live`` / ``/health/ready`` (aliases: ``/healthz`` / ``/readyz``)
- ``GET  /metrics``
"""

from __future__ import annotations

import asyncio
import contextlib
import datetime as dt
import gzip
import io
import logging
import time
from collections.abc import AsyncIterator
from typing import Any

import observe_core
from fastapi import Depends, FastAPI, HTTPException, Query, Request, Response, status
from fastapi.responses import JSONResponse
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from phlo_observer import __version__, query_v2
from phlo_observer.adapters import ADAPTERS, AdapterError, RawPayload
from phlo_observer.adapters.base import NormalizedBatch, record_normalization
from phlo_observer.auth import require_ingest_token, require_read_token
from phlo_observer.db import check_database, make_engine, make_sessionmaker
from phlo_observer.forward import forward_events
from phlo_observer.metrics import (
    HTTP_DURATION,
    HTTP_REQUESTS,
    INGEST_BATCHES,
    INGEST_EVENTS,
    NORMALIZATION_DURATION,
    PERSIST_DURATION,
    QUEUE_DEPTH,
)
from phlo_observer.models import Entity, Run
from phlo_observer.retention import retention_loop
from phlo_observer.settings import ObserverSettings, load_settings
from phlo_observer.store import (
    DEFAULT_PAGE_SIZE,
    InvalidQuery,
    _fmt,
    persist_events,
    probe_events_table,
    query_events,
    query_runs,
    store_raw,
)
from phlo_observer.timeline import event_by_id, run_timeline

logger = logging.getLogger("phlo_observer")

_UNAUTHENTICATED = {
    "/healthz",
    "/readyz",
    "/health/live",
    "/health/ready",
    "/metrics",
    "/docs",
    "/openapi.json",
}


def create_app(settings: ObserverSettings | None = None) -> FastAPI:
    """Build the application. ``settings`` defaults to environment config."""
    if settings is None:
        # load_settings resolves *_FILE secret variants; plain ObserverSettings()
        # would silently ignore them and leave the API unauthenticated.
        settings = load_settings()
    settings.require_tokens()

    self_observe_configs = settings.self_observe_drain_configs()

    @contextlib.asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        if self_observe_configs:
            app.state.observe_runtime = observe_core.configure(
                service_name="phlo-observer",
                service_version=__version__,
                drains=self_observe_configs,
            )
            app.state.self_observe = True
            observe_core.event(
                "observer.start",
                category="observer",
                attributes={"version": __version__},
            )
        if settings.run_migrations:
            await asyncio.to_thread(_run_migrations, settings)
        engine = make_engine(settings)
        factory = make_sessionmaker(engine)
        app.state.engine = engine
        app.state.session_factory = factory
        stop = asyncio.Event()
        retention_task = asyncio.create_task(
            retention_loop(
                factory,
                settings,
                interval_seconds=settings.retention_interval_s,
                stop=stop,
                self_observe=app.state.self_observe,
            )
        )
        app.state.retention_task = retention_task
        try:
            yield
        finally:
            stop.set()
            retention_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await retention_task
            # Bound in-flight OTLP forwards: give them a moment, then cancel.
            pending = [t for t in app.state.forward_tasks if not t.done()]
            if pending:
                _, still_pending = await asyncio.wait(pending, timeout=2.0)
                for task in still_pending:
                    task.cancel()
            await engine.dispose()
            if app.state.self_observe:
                observe_core.shutdown()

    app = FastAPI(
        title="phlo-observer",
        version=__version__,
        lifespan=lifespan,
        docs_url="/docs" if settings.docs_enabled else None,
        openapi_url="/openapi.json" if settings.docs_enabled else None,
    )
    app.state.settings = settings
    # Lifespan flips this on when internal drains are configured; tests and
    # embedders that bypass lifespan still get a defined value.
    app.state.self_observe = False
    app.state.observe_runtime = None
    app.state.forward_tasks = set()

    @app.exception_handler(InvalidQuery)
    async def invalid_query_handler(request: Request, exc: InvalidQuery) -> JSONResponse:
        """Malformed cursors/filters are client errors, never 500s."""
        return JSONResponse({"detail": str(exc)}, status_code=status.HTTP_400_BAD_REQUEST)

    @app.middleware("http")
    async def metrics_middleware(request: Request, call_next: Any) -> Response:
        """Count HTTP traffic and enforce the configured body limit."""
        if request.url.path not in _UNAUTHENTICATED:
            length = request.headers.get("content-length")
            if length:
                try:
                    declared = int(length)
                except ValueError:
                    declared = -1
                if declared < 0 or declared > settings.max_body_bytes:
                    return JSONResponse(
                        {"detail": "request body too large"},
                        status_code=status.HTTP_413_CONTENT_TOO_LARGE,
                    )
        start = time.perf_counter()
        response = await call_next(request)
        if settings.metrics_enabled:
            route = request.scope.get("route")
            # Unmatched requests must not leak raw paths into a label —
            # arbitrary 404 URLs are attacker-controllable cardinality.
            path = getattr(route, "path", "unmatched")
            HTTP_REQUESTS.labels(route=path, status=str(response.status_code)).inc()
            HTTP_DURATION.labels(route=path).observe(time.perf_counter() - start)
        return response

    async def _body(request: Request) -> bytes:
        """Read the request body with hard bounds on wire and decompressed size.

        Chunked bodies and gzip bombs cannot bypass the configured limit.
        """
        chunks: list[bytes] = []
        wire_bytes = 0
        async for chunk in request.stream():
            wire_bytes += len(chunk)
            if wire_bytes > settings.max_body_bytes:
                raise _http_error(413, "request body too large")
            chunks.append(chunk)
        body = b"".join(chunks)
        content_encoding = (request.headers.get("content-encoding") or "").lower()
        if "gzip" in (token.strip() for token in content_encoding.split(",")):
            try:
                with gzip.GzipFile(fileobj=io.BytesIO(body)) as gz:
                    body = gz.read(settings.max_body_bytes + 1)
            except (OSError, EOFError) as exc:
                raise _http_error(400, f"invalid gzip body: {exc}") from exc
        if len(body) > settings.max_body_bytes:
            raise _http_error(413, "request body too large")
        return body

    def get_session_factory(request: Request) -> async_sessionmaker[AsyncSession]:
        """FastAPI dependency: the session factory created at startup."""
        return request.app.state.session_factory

    async def _ingest(
        request: Request,
        *,
        producer: str,
        source_kind: str,
        adapter_name: str | None,
    ) -> JSONResponse:
        """Shared ingestion pipeline: read -> raw store -> normalize -> persist."""
        body = await _body(request)
        adapter = ADAPTERS[adapter_name or "canonical"]
        source_version = request.headers.get("x-source-version") or adapter.version
        async with app.state.session_factory() as session, session.begin():
            raw = await store_raw(
                session,
                producer=producer,
                source_kind=source_kind,
                body=body,
                content_type=request.headers.get("content-type", "application/json"),
                adapter=adapter_name,
                source_version=source_version,
                keep_payload=adapter.keep_payload,
                max_payload_bytes=settings.max_raw_payload_bytes,
                retention_days=settings.raw_retention_days,
            )
            batch = _normalize(adapter_name, body, source_version, request)
            events, errors = batch.events, batch.errors
            if len(events) > settings.max_batch_events:
                raw.normalization_status = "rejected"
                raw.normalization_error = "batch too large"
                INGEST_BATCHES.labels(status="rejected").inc()
                return JSONResponse(
                    {
                        "accepted": 0,
                        "rejected": len(events),
                        "errors": [
                            {
                                "index": -1,
                                "code": "BATCH_TOO_LARGE",
                                "message": f"batch exceeds {settings.max_batch_events} events",
                            }
                        ],
                    },
                    status_code=status.HTTP_413_CONTENT_TOO_LARGE,
                )
            start = time.perf_counter()
            result = await persist_events(
                session, events, raw_event_id=raw.id, source_indices=batch.indices
            )
            PERSIST_DURATION.observe(time.perf_counter() - start)
            if settings.otlp_endpoint and result.event_ids:
                accepted_ids = set(result.event_ids)
                forwarded = [e for e in events if e.get("event_id") in accepted_ids]
                if forwarded:
                    # Fire-and-forget: forwarding must never delay ingestion.
                    task = asyncio.create_task(forward_events(forwarded, settings.otlp_endpoint))
                    app.state.forward_tasks.add(task)
                    task.add_done_callback(app.state.forward_tasks.discard)
            result.errors = errors + result.errors
            result.rejected += len(errors)
            # Reflect persist-stage rejections too, not just adapter errors:
            # a payload that normalized cleanly but stored nothing must not
            # be recorded as "ok".
            if not result.errors:
                raw.normalization_status = "ok"
            else:
                raw.normalization_status = (
                    "partial" if (result.accepted or result.duplicates) else "failed"
                )
                raw.normalization_error = result.errors[0]["message"]
        if result.rejected:
            batch_status = "partial" if (result.accepted or result.duplicates) else "rejected"
        else:
            batch_status = "accepted"
        INGEST_BATCHES.labels(status=batch_status).inc()
        INGEST_EVENTS.labels(producer=producer, status="accepted").inc(result.accepted)
        INGEST_EVENTS.labels(producer=producer, status="rejected").inc(result.rejected)
        if app.state.self_observe:
            observe_core.event(
                "observer.ingest",
                category="observer",
                attributes={
                    "producer": producer,
                    "source_kind": source_kind,
                    "accepted": result.accepted,
                    "rejected": result.rejected,
                    "duplicates": result.duplicates,
                },
            )
        code = status.HTTP_202_ACCEPTED
        if result.accepted == 0 and result.rejected:
            code = status.HTTP_422_UNPROCESSABLE_CONTENT
        return JSONResponse(
            {
                "accepted": result.accepted,
                "rejected": result.rejected,
                "duplicates": result.duplicates,
                "errors": result.errors,
            },
            status_code=code,
        )

    def _normalize(
        adapter_name: str | None,
        body: bytes,
        source_version: str | None = None,
        request: Request | None = None,
    ) -> NormalizedBatch:
        """Run the adapter; collect per-item failures as structured errors."""
        adapter = ADAPTERS[adapter_name or "canonical"]
        metadata: dict[str, Any] = {}
        if request is not None:
            # Transport-level correlation headers flow into RawPayload.metadata
            # so adapters (e.g. generic) can join events to runs/traces even
            # when the producer body lacks the keys.
            for header, key in (
                ("x-run-id", "run_id"),
                ("x-trace-id", "trace_id"),
                ("x-request-id", "request_id"),
                ("x-asset-key", "asset_key"),
            ):
                value = request.headers.get(header)
                if value:
                    metadata[key] = value
        try:
            start = time.perf_counter()
            batch = adapter.normalize(
                RawPayload(
                    producer=adapter.name,
                    source_kind=adapter.name,
                    body=body,
                    source_version=source_version,
                    keep_payload=adapter.keep_payload,
                    metadata=metadata,
                )
            )
            NORMALIZATION_DURATION.observe(time.perf_counter() - start)
            record_normalization(adapter.name, "success" if not batch.errors else "partial")
            return batch
        except AdapterError as exc:
            record_normalization(adapter.name, "error")
            return NormalizedBatch(errors=[{"index": -1, "code": exc.code, "message": str(exc)}])
        except Exception as exc:
            # Spec §35: a malformed source event must never crash the observer.
            logger.exception("adapter %s raised unexpectedly", adapter.name)
            record_normalization(adapter.name, "error")
            return NormalizedBatch(
                errors=[
                    {
                        "index": -1,
                        "code": "NORMALIZATION_FAILED",
                        "message": f"{adapter.name} adapter failed: {exc}",
                    }
                ]
            )

    # -- ingestion ----------------------------------------------------------

    @app.post(
        "/v1/events",
        status_code=status.HTTP_202_ACCEPTED,
        dependencies=[Depends(require_ingest_token)],
    )
    async def ingest_events(request: Request) -> Response:
        """Canonical ingestion: single event object or array."""
        return await _ingest(
            request, producer="canonical", source_kind="envelope", adapter_name="canonical"
        )

    @app.post(
        "/v1/ingest/dagster",
        status_code=status.HTTP_202_ACCEPTED,
        dependencies=[Depends(require_ingest_token)],
    )
    async def ingest_dagster(request: Request) -> Response:
        """Dagster event payloads."""
        return await _ingest(
            request, producer="dagster", source_kind="events", adapter_name="dagster"
        )

    @app.post(
        "/v1/ingest/dbt",
        status_code=status.HTTP_202_ACCEPTED,
        dependencies=[Depends(require_ingest_token)],
    )
    async def ingest_dbt(request: Request) -> Response:
        """Dbt run_results JSON documents."""
        return await _ingest(request, producer="dbt", source_kind="run_results", adapter_name="dbt")

    @app.post(
        "/v1/ingest/dbt/artifacts",
        status_code=status.HTTP_202_ACCEPTED,
        dependencies=[Depends(require_ingest_token)],
    )
    async def ingest_dbt_artifacts(request: Request) -> Response:
        """Dbt artifacts bundle: ``{"run_results": ..., "manifest": ...}``."""
        return await _ingest(request, producer="dbt", source_kind="artifacts", adapter_name="dbt")

    @app.post(
        "/v1/ingest/generic",
        status_code=status.HTTP_202_ACCEPTED,
        dependencies=[Depends(require_ingest_token)],
    )
    async def ingest_generic(request: Request) -> Response:
        """Last-resort generic JSON ingestion."""
        return await _ingest(
            request, producer="generic", source_kind="json", adapter_name="generic"
        )

    @app.post(
        "/v1/ingest/otlp",
        status_code=status.HTTP_202_ACCEPTED,
        dependencies=[Depends(require_ingest_token)],
    )
    async def ingest_otlp(request: Request) -> Response:
        """OTLP/HTTP JSON ingestion (collector-forwarded logs)."""
        return await _ingest(request, producer="otlp", source_kind="logs", adapter_name="otlp")

    # -- queries -------------------------------------------------------------

    @app.get("/v1/events", dependencies=[Depends(require_read_token)])
    async def list_events(
        session_factory: async_sessionmaker[AsyncSession] = Depends(get_session_factory),
        event: str | None = None,
        category: str | None = None,
        outcome: str | None = None,
        severity: str | None = None,
        service: str | None = None,
        environment: str | None = None,
        run_id: str | None = None,
        asset_key: str | None = None,
        partition_key: str | None = None,
        branch: str | None = None,
        table: str | None = None,
        trace_id: str | None = None,
        since: str | None = None,
        until: str | None = None,
        cursor: str | None = None,
        limit: int = Query(DEFAULT_PAGE_SIZE, ge=1, le=1000),
    ) -> dict[str, Any]:
        """Filter events; paginate with the returned ``next_cursor``."""
        filters = {
            k: v
            for k, v in {
                "event": event,
                "category": category,
                "outcome": outcome,
                "severity": severity,
                "service": service,
                "environment": environment,
                "run_id": run_id,
                "asset_key": asset_key,
                "partition_key": partition_key,
                "branch": branch,
                "table": table,
                "trace_id": trace_id,
                "since": since,
                "until": until,
            }.items()
            if v is not None
        }
        async with session_factory() as session:
            page = await query_events(session, filters=filters, cursor=cursor, limit=limit)
            return {
                "items": [_event_json(row) for row in page.items],
                "next_cursor": page.next_cursor,
            }

    @app.get("/v1/events/{event_id}", dependencies=[Depends(require_read_token)])
    async def get_event(event_id: str, request: Request) -> dict[str, Any]:
        """Fetch one event by ID."""
        async with request.app.state.session_factory() as session:
            row = await event_by_id(session, event_id)
            if row is None:
                raise _http_error(404, "event not found")
            return _event_json(row)

    @app.get("/v1/runs", dependencies=[Depends(require_read_token)])
    async def list_runs(
        request: Request,
        status_filter: str | None = Query(None, alias="status"),
        job_name: str | None = None,
        cursor: str | None = None,
        limit: int = Query(DEFAULT_PAGE_SIZE, ge=1, le=1000),
    ) -> dict[str, Any]:
        """List run projections."""
        async with request.app.state.session_factory() as session:
            runs, next_cursor = await query_runs(
                session, status=status_filter, job_name=job_name, cursor=cursor, limit=limit
            )
            return {"items": [_run_json(run) for run in runs], "next_cursor": next_cursor}

    @app.get("/v1/runs/{run_id}", dependencies=[Depends(require_read_token)])
    async def get_run(run_id: str, request: Request) -> dict[str, Any]:
        """One run projection."""
        async with request.app.state.session_factory() as session:
            timeline = await run_timeline(session, run_id)
            if timeline is None:
                raise _http_error(404, "run not found")
            return timeline["run"]

    @app.get("/v1/runs/{run_id}/timeline", dependencies=[Depends(require_read_token)])
    async def get_run_timeline(run_id: str, request: Request) -> dict[str, Any]:
        """Run projection plus phase-grouped events."""
        async with request.app.state.session_factory() as session:
            timeline = await run_timeline(session, run_id)
            if timeline is None:
                raise _http_error(404, "run not found")
            return timeline

    async def _scoped_events(
        request: Request,
        key: str,
        value: str,
        since: str | None,
        until: str | None,
        cursor: str | None,
        limit: int,
    ) -> dict[str, Any]:
        """Shared handler for asset/branch/table-scoped event queries."""
        if not value:
            raise _http_error(400, f"empty {key} path parameter")
        filters: dict[str, Any] = {key: value}
        if since is not None:
            filters["since"] = since
        if until is not None:
            filters["until"] = until
        async with request.app.state.session_factory() as session:
            page = await query_events(session, filters=filters, cursor=cursor, limit=limit)
            return {
                "items": [_event_json(row) for row in page.items],
                "next_cursor": page.next_cursor,
            }

    @app.get("/v1/assets/{asset_key:path}/events", dependencies=[Depends(require_read_token)])
    async def asset_events(
        asset_key: str,
        request: Request,
        since: str | None = None,
        until: str | None = None,
        cursor: str | None = None,
        limit: int = Query(DEFAULT_PAGE_SIZE, ge=1, le=1000),
    ) -> dict[str, Any]:
        """Events correlated to one asset key."""
        return await _scoped_events(request, "asset_key", asset_key, since, until, cursor, limit)

    @app.get("/v1/branches/{branch:path}/events", dependencies=[Depends(require_read_token)])
    async def branch_events(
        branch: str,
        request: Request,
        since: str | None = None,
        until: str | None = None,
        cursor: str | None = None,
        limit: int = Query(DEFAULT_PAGE_SIZE, ge=1, le=1000),
    ) -> dict[str, Any]:
        """Events correlated to one branch."""
        return await _scoped_events(request, "branch", branch, since, until, cursor, limit)

    @app.get("/v1/tables/{table:path}/events", dependencies=[Depends(require_read_token)])
    async def table_events(
        table: str,
        request: Request,
        since: str | None = None,
        until: str | None = None,
        cursor: str | None = None,
        limit: int = Query(DEFAULT_PAGE_SIZE, ge=1, le=1000),
    ) -> dict[str, Any]:
        """Events correlated to one table name."""
        return await _scoped_events(request, "table", table, since, until, cursor, limit)

    # -- V2 operational queries (spec §22) ------------------------------------

    @app.get("/v2/runs/{run_id}", dependencies=[Depends(require_read_token)])
    async def v2_get_run(run_id: str, request: Request) -> dict[str, Any]:
        """Run projection plus its entity links."""
        async with request.app.state.session_factory() as session:
            result = await query_v2.get_run_v2(session, run_id)
            if result is None:
                raise _http_error(404, "run not found")
            return result

    @app.get("/v2/runs/{run_id}/timeline", dependencies=[Depends(require_read_token)])
    async def v2_run_timeline(run_id: str, request: Request) -> dict[str, Any]:
        """Run projection plus ordered events (same payload as v1 timeline)."""
        async with request.app.state.session_factory() as session:
            timeline = await run_timeline(session, run_id)
            if timeline is None:
                raise _http_error(404, "run not found")
            return timeline

    @app.get("/v2/runs/{run_id}/failures", dependencies=[Depends(require_read_token)])
    async def v2_run_failures(run_id: str, request: Request) -> dict[str, Any]:
        """Failed/error events for one run with evidence IDs."""
        async with request.app.state.session_factory() as session:
            result = await query_v2.run_failures(session, run_id)
            if result is None:
                raise _http_error(404, "run not found")
            return result

    @app.get("/v2/runs/{run_id}/changes", dependencies=[Depends(require_read_token)])
    async def v2_run_changes(
        run_id: str,
        request: Request,
        window_hours: float = Query(24.0, ge=0.1, le=24 * 30),
    ) -> dict[str, Any]:
        """Change events preceding the run (spec §18)."""
        async with request.app.state.session_factory() as session:
            result = await query_v2.run_changes(
                session, run_id, window=dt.timedelta(hours=window_hours)
            )
            if result is None:
                raise _http_error(404, "run not found")
            return result

    @app.get("/v2/runs/{run_id}/impact", dependencies=[Depends(require_read_token)])
    async def v2_run_impact(run_id: str, request: Request) -> dict[str, Any]:
        """Downstream entities reached through this run's edges."""
        async with request.app.state.session_factory() as session:
            result = await query_v2.run_impact(session, run_id)
            if result is None:
                raise _http_error(404, "run not found")
            return result

    @app.get("/v2/runs/{run_id}/investigate", dependencies=[Depends(require_read_token)])
    async def v2_run_investigate(run_id: str, request: Request) -> dict[str, Any]:
        """Deterministic investigation bundle (spec §20)."""
        async with request.app.state.session_factory() as session:
            result = await query_v2.investigation_bundle(session, run_id)
            if result is None:
                raise _http_error(404, "run not found")
            return result

    @app.get("/v2/assets/{entity_id:path}/health", dependencies=[Depends(require_read_token)])
    async def v2_asset_health(entity_id: str, request: Request) -> dict[str, Any]:
        """Asset health: status, freshness SLA state, open insights."""
        if "://" not in entity_id:
            entity_id = f"asset://{entity_id}"
        async with request.app.state.session_factory() as session:
            result = await query_v2.asset_health(session, entity_id)
            if result is None:
                raise _http_error(404, "asset not found")
            return result

    @app.get("/v2/assets/{entity_id:path}/history", dependencies=[Depends(require_read_token)])
    async def v2_asset_history(
        entity_id: str,
        request: Request,
        limit: int = Query(200, ge=1, le=1000),
    ) -> dict[str, Any]:
        """Ordered event history for one asset."""
        if "://" not in entity_id:
            entity_id = f"asset://{entity_id}"
        async with request.app.state.session_factory() as session:
            result = await query_v2.asset_history(session, entity_id, limit=limit)
            if result is None:
                raise _http_error(404, "asset not found")
            return result

    @app.get("/v2/assets/{entity_id:path}/lineage", dependencies=[Depends(require_read_token)])
    async def v2_asset_lineage(entity_id: str, request: Request) -> dict[str, Any]:
        """Upstream/downstream relationship edges (spec §14)."""
        if "://" not in entity_id:
            entity_id = f"asset://{entity_id}"
        async with request.app.state.session_factory() as session:
            result = await query_v2.asset_lineage(session, entity_id)
            if result is None:
                raise _http_error(404, "asset not found")
            return result

    # Registered last: the greedy :path converter must not swallow the
    # /health, /history and /lineage sub-resource routes above.
    @app.get("/v2/assets/{entity_id:path}", dependencies=[Depends(require_read_token)])
    async def v2_get_asset(entity_id: str, request: Request) -> dict[str, Any]:
        """Asset projection by canonical entity id (``asset://a/b``)."""
        if "://" not in entity_id:
            entity_id = f"asset://{entity_id}"
        async with request.app.state.session_factory() as session:
            result = await query_v2.get_asset_v2(session, entity_id)
            if result is None:
                raise _http_error(404, "asset not found")
            return result

    @app.get("/v2/insights", dependencies=[Depends(require_read_token)])
    async def v2_list_insights(
        request: Request,
        state: str | None = None,
        entity: str | None = None,
        limit: int = Query(100, ge=1, le=1000),
    ) -> dict[str, Any]:
        """Insights with optional state/entity filters."""
        async with request.app.state.session_factory() as session:
            return {
                "items": await query_v2.list_insights(
                    session, state=state, entity=entity, limit=limit
                )
            }

    @app.get("/v2/incidents", dependencies=[Depends(require_read_token)])
    async def v2_list_incidents(
        request: Request,
        state: str | None = None,
        limit: int = Query(100, ge=1, le=1000),
    ) -> dict[str, Any]:
        """Incidents with optional state filter."""
        async with request.app.state.session_factory() as session:
            return {"items": await query_v2.list_incidents(session, state=state, limit=limit)}

    @app.get("/v2/incidents/{incident_id}", dependencies=[Depends(require_read_token)])
    async def v2_get_incident(incident_id: str, request: Request) -> dict[str, Any]:
        """One incident with its grouped insights."""
        async with request.app.state.session_factory() as session:
            result = await query_v2.get_incident(session, incident_id)
            if result is None:
                raise _http_error(404, "incident not found")
            return result

    @app.get("/v2/events/{event_id}/provenance", dependencies=[Depends(require_read_token)])
    async def v2_event_provenance(event_id: str, request: Request) -> dict[str, Any]:
        """Which projections this event contributed to (spec §12.3)."""
        async with request.app.state.session_factory() as session:
            row = await event_by_id(session, event_id)
            if row is None:
                raise _http_error(404, "event not found")
            eid = str(row.event_id)
            contributing: dict[str, list[str]] = {"run": [], "entity": [], "edge": []}
            if row.run_id:
                run = await session.get(Run, row.run_id)
                if run and eid in ((run.provenance or {}).get("derived_from") or []):
                    contributing["run"].append(row.run_id)
            entities = (
                await session.execute(
                    select(Entity.entity_id).where(
                        Entity.provenance["derived_from"].cast(JSONB).contains([eid])
                    )
                )
            ).scalars()
            contributing["entity"] = list(entities)
            return {"event_id": eid, "projections": contributing}

    @app.post("/v2/query/compare-runs", dependencies=[Depends(require_read_token)])
    async def v2_compare_runs(request: Request) -> dict[str, Any]:
        """Compare two runs: ``{"run_a": ..., "run_b": ...}``."""
        body = await request.json()
        run_a, run_b = body.get("run_a"), body.get("run_b")
        if not run_a or not run_b:
            raise _http_error(400, "run_a and run_b are required")
        async with request.app.state.session_factory() as session:
            result = await query_v2.compare_runs(session, run_a, run_b)
            if result is None:
                raise _http_error(404, "one or both runs not found")
            return result

    # -- health / metrics ----------------------------------------------------

    @app.get("/health/live")
    @app.get("/healthz", include_in_schema=False)
    async def healthz() -> dict[str, str]:
        """Liveness: process is up."""
        return {"status": "ok", "version": __version__}

    @app.get("/health/ready")
    @app.get("/readyz", include_in_schema=False)
    async def readyz(request: Request) -> Response:
        """Readiness: database reachable, schema-compatible, workers alive."""
        ok = await check_database(request.app.state.session_factory)
        if not ok:
            return JSONResponse({"status": "not_ready", "database": "unreachable"}, status_code=503)
        retention_task = getattr(request.app.state, "retention_task", None)
        if retention_task is not None and retention_task.done():
            return JSONResponse(
                {"status": "not_ready", "retention_worker": "stopped"}, status_code=503
            )
        observe_runtime = getattr(request.app.state, "observe_runtime", None)
        if observe_runtime is not None and not observe_runtime.workers_alive():
            return JSONResponse(
                {"status": "not_ready", "observe_worker": "stopped"}, status_code=503
            )
        async with request.app.state.session_factory() as session:
            try:
                version = (
                    await session.execute(text("select version_num from alembic_version"))
                ).scalar()
            except Exception:
                # missing alembic_version (unmigrated schema) aborts the txn
                await session.rollback()
                version = None
            try:
                await probe_events_table(session)
            except Exception:
                # Database reachable but the events schema is absent or
                # incompatible: report not-ready rather than a 500.
                await session.rollback()
                return JSONResponse(
                    {"status": "not_ready", "database": "schema_incompatible"},
                    status_code=503,
                )
            return JSONResponse(
                {
                    "status": "ready",
                    "database": "ok",
                    "schema_version": version,
                }
            )

    metrics_deps = [] if settings.metrics_public else [Depends(require_read_token)]

    @app.get("/metrics", dependencies=metrics_deps)
    async def metrics_endpoint(request: Request) -> Response:
        """Prometheus exposition."""
        if not settings.metrics_enabled:
            raise _http_error(404, "metrics disabled")
        # The observer's internal queue is the observe-core emission queue
        # used for self-observation; report its depth (0 when disabled).
        runtime = getattr(request.app.state, "observe_runtime", None)
        QUEUE_DEPTH.set(runtime.queue_depth() if runtime is not None else 0)
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    return app


def _run_migrations(settings: ObserverSettings) -> None:
    """Apply Alembic migrations to head (startup path, sync thread)."""
    from alembic import command  # noqa: PLC0415 - keep alembic off the hot path

    from phlo_observer.cli import _alembic_config  # noqa: PLC0415

    cfg = _alembic_config(settings.database_url)
    command.upgrade(cfg, "head")
    logger.info("database migrated to head")


def _http_error(code: int, message: str) -> HTTPException:
    return HTTPException(status_code=code, detail=message)


def _event_json(row: Any) -> dict[str, Any]:
    return {
        "event_id": str(row.event_id),
        "schema_version": row.schema_version,
        "event": row.event,
        "category": row.category,
        "outcome": row.outcome,
        "severity": row.severity,
        "delivery": row.delivery,
        "started_at": _fmt(row.started_at),
        "ended_at": _fmt(row.ended_at),
        "duration_ms": row.duration_ms,
        "observed_at": _fmt(row.observed_at),
        "received_at": _fmt(row.received_at),
        "service": {
            "name": row.service_name,
            "version": row.service_version,
            "environment": row.environment,
        },
        "correlation": {
            "trace_id": row.trace_id,
            "span_id": row.span_id,
            "run_id": row.run_id,
            "job_id": row.job_id,
            "invocation_id": row.invocation_id,
            "asset_key": row.asset_key,
            "partition_key": row.partition_key,
            "branch": row.branch,
            "table": row.table_name,
            "snapshot_id": row.snapshot_id,
            "pipeline": row.pipeline,
        },
        "correlation_method": row.correlation_method,
        "attributes": row.attributes,
        "error": row.error,
        "source": row.source,
        # The canonical envelope as received: correlation.extra and any other
        # envelope extensions live only here.
        "payload": row.payload,
    }


def _run_json(run: Any) -> dict[str, Any]:
    return {
        "run_id": run.run_id,
        "status": run.status,
        "job_name": run.job_name,
        "service_name": run.service_name,
        "environment": run.environment,
        "branch": run.branch,
        "trigger": run.trigger,
        "started_at": _fmt(run.started_at),
        "ended_at": _fmt(run.ended_at),
        "duration_ms": run.duration_ms,
        "event_count": run.event_count,
        "error_count": run.error_count,
        "warning_count": run.warning_count,
        "asset_count": run.asset_count,
        "summary": run.summary,
        "updated_at": _fmt(run.updated_at),
    }


# Type aliases used only for dependency wiring clarity.
SessionFactory = async_sessionmaker[AsyncSession]
