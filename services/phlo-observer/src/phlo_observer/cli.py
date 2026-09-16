"""phlo-observer command line interface."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Annotated

import typer

if TYPE_CHECKING:
    from alembic.config import Config

    from phlo_observer.settings import ObserverSettings

app = typer.Typer(help="phlo-observer ingestion and query service")


def _settings() -> ObserverSettings:
    """load_settings(); config errors exit cleanly for operators (spec §73)."""
    from phlo_observer.settings import load_settings

    try:
        return load_settings()
    except ValueError as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=1) from exc


@app.command()
def serve(
    host: str | None = typer.Option(None, help="Bind host"),
    port: int | None = typer.Option(None, help="Bind port"),
) -> None:
    """Run the observer HTTP service."""
    import uvicorn

    settings = _settings()
    try:
        settings.require_tokens()
        settings.self_observe_drain_configs()  # validate early: bad values fail fast
    except ValueError as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=1) from exc
    uvicorn.run(
        "phlo_observer.app:create_app",
        factory=True,
        host=host or settings.host,
        port=port or settings.port,
        log_level=settings.log_level.lower(),
    )


@app.command()
def migrate(revision: str = typer.Argument("head", help="Alembic target revision")) -> None:
    """Run database migrations (default: upgrade to head)."""
    from alembic import command

    try:
        cfg = _alembic_config()
    except ValueError as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=1) from exc
    command.upgrade(cfg, revision)
    typer.echo(f"migrated to {revision}")


@app.command()
def downgrade(
    revision: str = typer.Argument("-1", help="Alembic revision to downgrade to"),
) -> None:
    """Downgrade the database schema."""
    from alembic import command

    try:
        cfg = _alembic_config()
    except ValueError as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=1) from exc
    command.downgrade(cfg, revision)


@app.command()
def check() -> None:
    """Verify configuration and database connectivity."""
    import asyncio

    from phlo_observer.db import check_database, make_engine, make_sessionmaker

    settings = _settings()
    try:
        settings.require_tokens()
    except ValueError as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=1) from exc
    if not settings.ingest_token_set:
        typer.echo("warning: no ingest tokens configured (dev mode)")
    if not settings.read_token_set:
        typer.echo("warning: no read tokens configured (dev mode)")

    async def _check() -> bool:
        engine = make_engine(settings)
        try:
            return await check_database(make_sessionmaker(engine))
        finally:
            await engine.dispose()

    ok = asyncio.run(_check())
    typer.echo("database: ok" if ok else "database: unreachable")
    raise typer.Exit(code=0 if ok else 1)


@app.command()
def config() -> None:
    """Print the effective configuration (secrets redacted)."""
    import json

    settings = _settings()
    data = settings.model_dump(mode="json")
    for key in ("ingest_tokens", "read_tokens", "database_url"):
        if data.get(key):
            data[key] = "***"
    typer.echo(json.dumps(data, indent=2, sort_keys=True))


@app.command(name="rebuild-projections")
def rebuild_projections_cmd(
    run: Annotated[
        str | None,
        typer.Option("--run", help="Rebuild only projections for this run_id"),
    ] = None,
) -> None:
    """Rebuild derived projections from canonical events (spec §12.4).

    Without ``--run`` every projection (runs, entities, relationships,
    assets, baselines, insights, incidents) is dropped and recomputed from
    the full event history. With ``--run`` only that run's scope is
    rebuilt: the run row is replaced, entities/edges merge into existing
    state, and touched assets are re-derived from their full event history
    so other runs' contributions survive. Insights, incidents and baselines
    are cross-run derived state and are only rebuilt unscoped.
    """
    import asyncio

    from phlo_observer.db import make_engine, make_sessionmaker
    from phlo_observer.projections import rebuild_projections

    settings = _settings()

    async def _rebuild() -> dict[str, int]:
        engine = make_engine(settings)
        try:
            factory = make_sessionmaker(engine)
            async with factory() as session, session.begin():
                return await rebuild_projections(session, run_id=run)
        finally:
            await engine.dispose()

    counts = asyncio.run(_rebuild())
    scope = f"run {run}" if run else "all events"
    typer.echo(
        f"rebuilt projections for {scope}: "
        f"{counts['events']} events -> {counts['runs']} runs, "
        f"{counts['entities']} entities, {counts['edges']} edges, "
        f"{counts['assets']} assets"
    )


@app.command()
def archive(
    out: Annotated[Path, typer.Argument(help="Output file (JSONL of canonical events)")],
    since: Annotated[
        str | None, typer.Option(help="Only events received after this ISO timestamp")
    ] = None,
    until: Annotated[
        str | None, typer.Option(help="Only events received before this ISO timestamp")
    ] = None,
    batch: Annotated[int, typer.Option(help="Read batch size")] = 1000,
) -> None:
    """Export canonical events to a JSONL archive (spec §24.3).

    Events are the source of truth: the archive contains the normalized
    envelope payload per row, so ``restore`` can re-ingest it losslessly
    and ``rebuild-projections`` recomputes all derived state.
    """
    import asyncio
    import json
    from datetime import datetime

    from sqlalchemy import select

    from phlo_observer.db import make_engine, make_sessionmaker
    from phlo_observer.models import Event

    settings = _settings()
    lo = datetime.fromisoformat(since) if since else None
    hi = datetime.fromisoformat(until) if until else None

    async def _dump() -> int:
        engine = make_engine(settings)
        written = 0
        try:
            factory = make_sessionmaker(engine)
            with out.open("w") as fh:
                async with factory() as session:
                    # Keyset pagination on (received_at, event_id): OFFSET
                    # rescans/skip rows when ingestion lands mid-export.
                    last_ts = lo
                    last_id = None
                    while True:
                        stmt = select(Event).order_by(Event.received_at, Event.event_id)
                        if lo:
                            stmt = stmt.where(Event.received_at >= lo)
                        if hi:
                            stmt = stmt.where(Event.received_at <= hi)
                        if last_id is not None:
                            stmt = stmt.where(
                                (Event.received_at > last_ts)
                                | ((Event.received_at == last_ts) & (Event.event_id > last_id))
                            )
                        rows = (await session.execute(stmt.limit(batch))).scalars().all()
                        if not rows:
                            break
                        for row in rows:
                            fh.write(json.dumps(row.payload) + "\n")
                        written += len(rows)
                        last_ts, last_id = rows[-1].received_at, rows[-1].event_id
                        if len(rows) < batch:
                            break
            return written
        finally:
            await engine.dispose()

    n = asyncio.run(_dump())
    typer.echo(f"archived {n} events to {out}")


@app.command()
def restore(
    src: Annotated[Path, typer.Argument(help="JSONL archive written by `archive`")],
    batch: Annotated[int, typer.Option(help="Insert batch size")] = 500,
) -> None:
    """Re-ingest an event archive through the canonical persist path."""
    import asyncio
    import json

    from phlo_observer.db import make_engine, make_sessionmaker
    from phlo_observer.store import persist_events

    settings = _settings()

    async def _load() -> dict[str, int]:
        engine = make_engine(settings)
        accepted = rejected = 0
        try:
            factory = make_sessionmaker(engine)
            async with factory() as session:
                with src.open() as fh:
                    buf: list[dict] = []
                    for line in fh:
                        line = line.strip()
                        if line:
                            buf.append(json.loads(line))
                        if len(buf) >= batch:
                            async with session.begin():
                                r = await persist_events(session, buf)
                            accepted += r.accepted
                            rejected += r.rejected
                            buf.clear()
                    if buf:
                        async with session.begin():
                            r = await persist_events(session, buf)
                        accepted += r.accepted
                        rejected += r.rejected
            return {"accepted": accepted, "rejected": rejected}
        finally:
            await engine.dispose()

    result = asyncio.run(_load())
    typer.echo(f"restored {result['accepted']} events ({result['rejected']} rejected)")


@app.command()
def reprocess(
    since: Annotated[str, typer.Option(help="Reprocess events received after this ISO timestamp")],
    until: Annotated[
        str | None, typer.Option(help="Reprocess events received before this ISO timestamp")
    ] = None,
    batch: Annotated[int, typer.Option(help="Read batch size")] = 1000,
) -> None:
    """Re-fold events in a received-at window into entity/asset/edge state.

    This is a surgical tool for picking up reducer fixes on a window of
    history: matching events are replayed through ``apply_event`` in
    received-at order, merging into existing projections. It does NOT
    re-derive run rows, insights, or baselines (run counters would
    double-count) — use ``rebuild-projections`` for a from-scratch rebuild
    of all derived state.

    Each page commits before the next read: a large window never holds one
    transaction's locks across the whole replay.
    """
    import asyncio
    from datetime import datetime

    from sqlalchemy import select

    from phlo_observer.db import make_engine, make_sessionmaker
    from phlo_observer.models import Event
    from phlo_observer.projections import apply_events_batch

    settings = _settings()
    lo = datetime.fromisoformat(since)
    hi = datetime.fromisoformat(until) if until else None

    async def _run() -> int:
        engine = make_engine(settings)
        processed = 0
        try:
            factory = make_sessionmaker(engine)
            last_ts = lo
            last_id = None
            while True:
                async with factory() as session, session.begin():
                    stmt = select(Event).where(Event.received_at >= lo)
                    if hi:
                        stmt = stmt.where(Event.received_at <= hi)
                    if last_id is not None:
                        stmt = stmt.where(
                            (Event.received_at > last_ts)
                            | ((Event.received_at == last_ts) & (Event.event_id > last_id))
                        )
                    stmt = stmt.order_by(Event.received_at, Event.event_id).limit(batch)
                    rows = list((await session.execute(stmt)).scalars().all())
                    if rows:
                        await apply_events_batch(session, rows)
                if not rows:
                    break
                processed += len(rows)
                last_ts, last_id = rows[-1].received_at, rows[-1].event_id
                if len(rows) < batch:
                    break
            return processed
        finally:
            await engine.dispose()

    n = asyncio.run(_run())
    typer.echo(f"reprocessed {n} events")


@app.command(name="replay-spool")
def replay_spool(
    spool_dir: Annotated[Path, typer.Argument(help="Spool directory to replay")],
    endpoint: Annotated[
        str | None,
        typer.Option(help="Ingest endpoint; default http://<host>:<port>/v1/events"),
    ] = None,
    token: Annotated[
        str | None, typer.Option(help="Ingest token; default: first configured")
    ] = None,
) -> None:
    """Replay a local critical-event spool into an observer ingest API.

    Operational recovery for events spooled by an observe-core client while
    the observer was unreachable. Segments are deleted only after the target
    accepts them; corrupt segments are quarantined as ``*.corrupt``.
    """
    from observe_core.drains.http import HttpDrain
    from observe_core.spool import Spool

    settings = _settings()
    host = settings.host if settings.host not in ("0.0.0.0", "::") else "127.0.0.1"  # noqa: S104
    url = endpoint or f"http://{host}:{settings.port}/v1/events"
    ingest_token = token or (settings.ingest_tokens[0] if settings.ingest_tokens else None)
    if ingest_token is None:
        typer.echo(
            "warning: no ingest token supplied or configured; "
            "the target will reject the replay unless it runs in dev mode",
            err=True,
        )
    drain = HttpDrain(url, token=ingest_token)
    try:
        replayed = Spool(spool_dir).replay(drain)
    finally:
        drain.close()
    typer.echo(f"replayed {replayed} events to {url}")


def _alembic_config(database_url: str | None = None) -> Config:
    """Alembic Config pointing at the packaged migrations.

    Resolution order: ``PHLO_OBSERVER_MIGRATIONS_DIR``, then the source-tree
    layout (``services/phlo-observer/migrations``), then the Docker image
    layout (``/app/migrations``).
    """
    import os
    from pathlib import Path

    from alembic.config import Config

    from phlo_observer.settings import load_settings

    candidates = [
        Path(os.environ["PHLO_OBSERVER_MIGRATIONS_DIR"])
        if os.environ.get("PHLO_OBSERVER_MIGRATIONS_DIR")
        else None,
        Path(__file__).resolve().parent.parent.parent / "migrations",
        Path("/app/migrations"),
    ]
    alembic_dir = next(
        (c for c in candidates if c is not None and (c / "alembic.ini").exists()),
        None,
    )
    if alembic_dir is None:
        raise FileNotFoundError("alembic.ini not found; set PHLO_OBSERVER_MIGRATIONS_DIR")
    cfg = Config(str(alembic_dir / "alembic.ini"))
    cfg.set_main_option("script_location", str(alembic_dir))
    cfg.set_main_option("sqlalchemy.url", database_url or load_settings().database_url)
    return cfg


def main() -> None:
    """Entry point."""
    app()


if __name__ == "__main__":
    main()
