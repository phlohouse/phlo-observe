"""``phlo-observe`` CLI: SDK diagnostics plus observer queries (spec §37).

Covers the V2 operator surface: ``doctor`` for local SDK health,
``schemas``/``events``/``run``/``incident``/``replay`` against the observer
API, and ``benchmark`` for ingest/query latency checks (§28).
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from typing import Any


def _print(data: Any) -> None:
    print(json.dumps(data, indent=2, default=str))


def _client() -> Any:
    """Build an ObserverClient from OBSERVER_URL / OBSERVE_READ_TOKEN env."""
    from observe_query import ObserverClient

    base = os.environ.get("OBSERVER_URL") or os.environ.get("PHLO_OBSERVER_URL")
    if not base:
        print("error: set OBSERVER_URL to the observer base URL", file=sys.stderr)
        sys.exit(2)
    token = os.environ.get("OBSERVE_READ_TOKEN") or os.environ.get("OBSERVER_TOKEN")
    return ObserverClient(base, token=token)


def _doctor() -> int:
    """Local SDK health: config resolution, runtime stats, drain ping."""
    from observe_core import configure, get_stats, shutdown

    try:
        rt = configure()
    except Exception as exc:
        _print({"status": "unhealthy", "error": str(exc)})
        return 1
    try:
        rt.health()  # increments health_checks counter
        stats = get_stats()
        _print({"status": "ok", "stats": stats})
        return 0
    finally:
        shutdown(timeout=2.0)


def _schemas_validate(path: str) -> int:
    """Validate a local JSON envelope file against the v2 schema."""
    from pathlib import Path

    from observe_core.models import EventEnvelope

    try:
        with Path(path).open() as fh:
            data = json.load(fh)
    except (OSError, json.JSONDecodeError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    items = data if isinstance(data, list) else [data]
    errors = 0
    for i, item in enumerate(items):
        try:
            EventEnvelope.model_validate(item)
        except Exception as exc:
            errors += 1
            print(f"[{i}] {exc}", file=sys.stderr)
    _print({"checked": len(items), "errors": errors})
    return 1 if errors else 0


def _benchmark(base: str, token: str | None, events: int) -> int:
    """Ingest + query latency check against a live observer (spec §28)."""
    import uuid

    import httpx

    headers = {"Authorization": f"Bearer {token}"} if token else {}
    run_id = f"bench-{uuid.uuid4().hex[:12]}"
    now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    batch = [
        {
            "schema_version": "2.0",
            "event_id": f"evt-{run_id}-{i}",
            "name": "run.heartbeat",
            "occurred_at": now,
            "status": "ok",
            "correlation": {"run_id": run_id},
            "attributes": {"seq": i},
        }
        for i in range(events)
    ]
    with httpx.Client(base_url=base.rstrip("/"), headers=headers, timeout=60.0) as http:
        t0 = time.perf_counter()
        resp = http.post("/v1/events", json={"events": batch})
        ingest_ms = (time.perf_counter() - t0) * 1000
        ingest_ok = resp.status_code < 400
        t0 = time.perf_counter()
        q = http.get("/v1/events", params={"run_id": run_id, "limit": 1})
        query_ms = (time.perf_counter() - t0) * 1000
    _print(
        {
            "events": events,
            "ingest_ms": round(ingest_ms, 1),
            "ingest_status": resp.status_code,
            "events_per_sec": round(events / (ingest_ms / 1000), 1) if ingest_ok else 0,
            "query_ms": round(query_ms, 1),
            "query_status": q.status_code,
        }
    )
    return 0 if ingest_ok and q.status_code < 400 else 1


def main(argv: list[str] | None = None) -> int:
    """Entry point for the ``phlo-observe`` command."""
    parser = argparse.ArgumentParser(prog="phlo-observe", description="phlo-observe operator CLI")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("doctor", help="check local SDK configuration and runtime health")

    schemas_p = sub.add_parser("schemas", help="schema registry operations")
    schemas_sub = schemas_p.add_subparsers(dest="schemas_command", required=True)
    schemas_sub.add_parser("list", help="list registered contract schemas")
    val_p = schemas_sub.add_parser("validate", help="validate a local envelope JSON file")
    val_p.add_argument("file", help="path to a JSON event or list of events")

    events_p = sub.add_parser("events", help="event operations")
    events_sub = events_p.add_subparsers(dest="events_command", required=True)
    tail_p = events_sub.add_parser("tail", help="print newest events, optionally following")
    tail_p.add_argument("--limit", type=int, default=20)
    tail_p.add_argument("--follow", action="store_true", help="poll for new events")
    tail_p.add_argument("--interval", type=float, default=2.0, help="follow poll seconds")

    run_p = sub.add_parser("run", help="run queries")
    run_sub = run_p.add_subparsers(dest="run_command", required=True)
    show_p = run_sub.add_parser("show", help="show a run projection")
    show_p.add_argument("run_id")
    cmp_p = run_sub.add_parser("compare", help="compare two runs")
    cmp_p.add_argument("run_a")
    cmp_p.add_argument("run_b")

    inc_p = sub.add_parser("incident", help="incident queries")
    inc_sub = inc_p.add_subparsers(dest="incident_command", required=True)
    inc_show = inc_sub.add_parser("show", help="show an incident")
    inc_show.add_argument("incident_id")

    replay_p = sub.add_parser("replay", help="replay a quarantined ingest failure")
    replay_p.add_argument("failure_id")

    bench_p = sub.add_parser("benchmark", help="measure ingest/query latency")
    bench_p.add_argument("--events", type=int, default=200)

    args = parser.parse_args(argv)

    if args.command == "doctor":
        return _doctor()

    if args.command == "schemas":
        if args.schemas_command == "list":
            with _client() as c:
                _print(c._get("/v2/schemas"))
            return 0
        return _schemas_validate(args.file)

    if args.command == "events":
        with _client() as c:
            while True:
                data = c._get(
                    "/v1/events",
                    limit=args.limit,
                    order="desc",
                )
                _print(data)
                if not args.follow:
                    return 0
                time.sleep(args.interval)

    if args.command == "run":
        with _client() as c:
            if args.run_command == "show":
                _print(c.get_run(args.run_id))
            else:
                _print(c.compare_runs(args.run_a, args.run_b))
        return 0

    if args.command == "incident":
        with _client() as c:
            _print(c.get_incident(args.incident_id))
        return 0

    if args.command == "replay":
        base = os.environ.get("OBSERVER_URL") or os.environ.get("PHLO_OBSERVER_URL")
        if not base:
            print("error: set OBSERVER_URL", file=sys.stderr)
            return 2
        token = os.environ.get("OBSERVE_ADMIN_TOKEN") or os.environ.get("OBSERVE_READ_TOKEN")
        import httpx

        with httpx.Client(
            base_url=base.rstrip("/"),
            headers={"Authorization": f"Bearer {token}"} if token else {},
            timeout=30.0,
        ) as http:
            resp = http.post(f"/v2/admin/quarantine/{args.failure_id}/replay")
            _print(resp.json())
            return 0 if resp.status_code < 400 else 1

    if args.command == "benchmark":
        base = os.environ.get("OBSERVER_URL") or os.environ.get("PHLO_OBSERVER_URL")
        if not base:
            print("error: set OBSERVER_URL", file=sys.stderr)
            return 2
        token = os.environ.get("OBSERVE_INGEST_TOKEN") or os.environ.get("OBSERVE_READ_TOKEN")
        return _benchmark(base, token, args.events)

    return 1


if __name__ == "__main__":
    sys.exit(main())
