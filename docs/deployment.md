# Deployment

## Docker image

`services/phlo-observer/Dockerfile` — multi-stage:

- builder: pinned `python:3.12-slim` + pinned `uv`, `uv sync --frozen --no-dev`
- runtime: same pinned base, non-root user `observer` (uid 1000), no compiler,
  read-only-rootfs friendly (no runtime writes outside the DB)
- labels: `org.opencontainers.image.{title,source,revision,version}` —
  pass `--build-arg VCS_REF=$(git rev-parse HEAD)` at build time
- `HEALTHCHECK` hits `/healthz`; `SIGTERM` stops uvicorn gracefully via the
  ASGI lifespan (retention task cancelled, engine disposed)

## Compose

```bash
docker compose up -d postgres phlo-observer        # core stack
docker compose --profile otel up -d                 # + otel-collector
```

The observer container needs `PHLO_OBSERVER_DATABASE_URL` and tokens; see
`.env.example`. Migrations run on demand: `phlo-observer migrate` (compose
service can run it as a one-off task before deploys).

## Kubernetes notes

- Liveness probe: `GET /healthz`; readiness: `GET /readyz` (verifies DB).
- Secrets: mount token files and use `PHLO_OBSERVER_*_FILE`.
- Rollouts: migrations must reach `head` before new pods serve traffic —
  `/readyz` reports `schema_version` so mismatches are visible.
- `readOnlyRootFilesystem: true` is supported; mount `/tmp` as emptyDir if a
  tmpdir is needed.

## Retention

In-process sweep runs hourly (`PHLO_OBSERVER_RETENTION_INTERVAL_S`):

- `raw_events` rows past `expires_at` (default 14d)
- `events` older than `PHLO_OBSERVER_EVENT_RETENTION_DAYS` (90d)
- `runs` older than `PHLO_OBSERVER_RUN_RETENTION_DAYS` (365d)

For large installs, prefer an external cronjob calling `run_retention_once`
or `DELETE ... WHERE expires_at <= now()` directly.

## Observability of the observer

`/metrics` exposes `phlo_observer_*` Prometheus metrics (ingest, normalize,
persist, correlate, HTTP, export, retention). Keep it behind the read token
(default) unless `PHLO_OBSERVER_METRICS_PUBLIC=true`.
