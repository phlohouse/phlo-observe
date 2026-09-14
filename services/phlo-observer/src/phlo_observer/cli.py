"""phlo-observer command line interface."""

from __future__ import annotations

from typing import TYPE_CHECKING

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


def _alembic_config() -> Config:
    """Alembic Config pointing at the packaged migrations."""
    from pathlib import Path

    from alembic.config import Config

    from phlo_observer.settings import load_settings

    alembic_dir = Path(__file__).resolve().parent.parent.parent / "migrations"
    cfg = Config(str(alembic_dir / "alembic.ini"))
    cfg.set_main_option("script_location", str(alembic_dir))
    cfg.set_main_option("sqlalchemy.url", load_settings().database_url)
    return cfg


def main() -> None:
    """Entry point."""
    app()


if __name__ == "__main__":
    main()
