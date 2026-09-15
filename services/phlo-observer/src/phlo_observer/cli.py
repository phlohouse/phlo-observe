"""phlo-observer command line interface."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Annotated

import typer

if TYPE_CHECKING:
    from alembic.config import Config

app = typer.Typer(help="phlo-observer ingestion and query service")


@app.command()
def serve(
    host: str | None = typer.Option(None, help="Bind host"),
    port: int | None = typer.Option(None, help="Bind port"),
) -> None:
    """Run the observer HTTP service."""
    import uvicorn

    from phlo_observer.settings import load_settings

    settings = load_settings()
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

    cfg = _alembic_config()
    command.upgrade(cfg, revision)
    typer.echo(f"migrated to {revision}")


@app.command()
def downgrade(
    revision: str = typer.Argument("-1", help="Alembic revision to downgrade to"),
) -> None:
    """Downgrade the database schema."""
    from alembic import command

    command.downgrade(_alembic_config(), revision)


@app.command()
def check() -> None:
    """Verify configuration and database connectivity."""
    import asyncio

    from phlo_observer.db import check_database, make_engine, make_sessionmaker
    from phlo_observer.settings import load_settings

    settings = load_settings()
    try:
        settings.require_tokens()
    except ValueError as exc:
        typer.echo(f"error: {exc}")
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

    from phlo_observer.settings import load_settings

    settings = load_settings()
    data = settings.model_dump(mode="json")
    for key in ("ingest_tokens", "read_tokens", "database_url"):
        if data.get(key):
            data[key] = "***"
    typer.echo(json.dumps(data, indent=2, sort_keys=True))


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

    from phlo_observer.settings import load_settings

    settings = load_settings()
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
