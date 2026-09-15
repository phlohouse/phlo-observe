# Contributing to phlo-observe

## Environment setup

Toolchain is managed by [mise](https://mise.jdx.dev) and [uv](https://docs.astral.sh/uv/):

```bash
mise install          # installs pinned python + uv
uv sync --all-packages
prek install          # git pre-commit hooks (ruff, ty, lock check)
```

## Layout

- `packages/observe-core` — generic event library (`observe_core`)
- `packages/phlo-observe-sdk` — Phlo SDK (`phlo_observe`)
- `services/phlo-observer` — ingestion/query service (`phlo_observer`)
- `schemas/` — canonical JSON Schemas
- `tests/` — cross-component contract, integration and performance tests

## Commands

```bash
uv run ruff check          # lint
uv run ruff format         # format
uv run ty check            # type check
uv run pytest              # full test suite
uv build --all-packages    # build wheels
```

Observer integration tests need PostgreSQL. `docker compose up -d postgres`
provisions `phlo_observer_test` alongside the dev database (via
`docker/postgres/initdb`, first init only — `docker compose down -v` recreates
it). Tests target it by default:

```bash
docker compose up -d postgres
uv run pytest services/phlo-observer/tests tests/integration
```

Point `PHLO_OBSERVER_TEST_DATABASE_URL` elsewhere to use a different instance.
Tests skip cleanly when Postgres is unreachable.

## Code standards

- Python 3.12+, fully typed; `ty` must pass.
- Ruff lint + format, Google-style docstrings on public API.
- `observe-core` must stay generic: no Phlo/Dagster/dbt imports.
- Secrets are redacted before any drain; never log tokens.
- One event per operation; debug logs are a separate signal.

## Extending the system

- **New drain**: implement `observe_core.drains.base.Drain` (`emit_batch`, `flush`,
  `close`), register a `DrainConfig` variant, add tests for failure behaviour.
- **New adapter**: implement `phlo_observer.adapters.base.SourceAdapter`
  (`can_handle`, `normalize`), register it in the `ADAPTERS` dict in
  `phlo_observer/adapters/__init__.py`, add fixture payloads + golden event
  tests.
- **New event name**: add a constant to `phlo_observe/events.py` (SDK) and to
  `EVENT_NAMES`, or emit an `application.*`/`other` category event from core
  directly. Names are lowercase, dot-delimited, never carry IDs or status
  suffixes.

## Commits

Small, reviewable commits. No attribution trailers. CI must stay green.
