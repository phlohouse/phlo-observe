# Configuration reference

All three components are configured by environment variables. Every secret
also accepts a `_FILE` variant pointing at a mounted secret file.

## phlo-observer (`PHLO_OBSERVER_*`)

| Variable | Default | Purpose |
|---|---|---|
| `HOST` | `0.0.0.0` | Bind address |
| `PORT` | `8080` | Bind port |
| `DATABASE_URL` | — | `postgresql+asyncpg://...` (required in production) |
| `DB_POOL_SIZE` / `DB_POOL_MAX_OVERFLOW` | `10` / `10` | Connection pool |
| `INGEST_TOKENS` | — | Comma-separated write tokens; empty = dev mode |
| `READ_TOKENS` | — | Comma-separated query tokens; empty = dev mode |
| `AUTH_OPTIONAL_DEV` | `true` | `false` hard-fails startup without tokens |
| `RAW_RETENTION_DAYS` | `14` | `raw_events.expires_at` sweep |
| `EVENT_RETENTION_DAYS` | `90` | normalized event TTL |
| `RUN_RETENTION_DAYS` | `365` | run projection TTL |
| `RETENTION_INTERVAL_S` | `3600` | in-process sweep cadence |
| `MAX_BODY_BYTES` | `10485760` | compressed or plain request limit |
| `MAX_BATCH_EVENTS` | `1000` | events per request |
| `OTLP_ENDPOINT` | — | optional HTTP endpoint to forward normalized events |
| `LOG_LEVEL` | `INFO` | uvicorn/app log level |
| `METRICS_ENABLED` | `true` | expose `/metrics` |
| `METRICS_PUBLIC` | `false` | `true` skips read-token check on `/metrics` |

## observe-core (`OBSERVE_*`)

| Variable | Default | Purpose |
|---|---|---|
| `SERVICE_NAME` | — | `service.name` on every event |
| `SERVICE_VERSION` / `ENVIRONMENT` | — | service fields |
| `DRAINS` | `console` | shorthand list or JSON array of drain configs |
| `HTTP_ENDPOINT` / `HTTP_API_KEY` | — | observer drain target + token |
| `QUEUE_CAPACITY` | `10000` | bounded event queue |
| `QUEUE_FULL_POLICY` | `drop_oldest` | `drop_oldest` / `drop_newest` / `block` |
| `SAMPLING_DEBUG_RATE` / `SAMPLING_TELEMETRY_RATE` | `1.0` / `1.0` | 0–1 |
| `REDACT_KEYS` | built-in | extra attribute keys to redact |
| `SPOOL_DIR` | — | critical-event local spool directory |
| `OTLP_ENDPOINT` | — | OTLP drain endpoint |
| `TELEMETRY_REQUIRED` | `false` | `true` = fail-closed emission |

## phlo-observe SDK (`PHLO_*`)

| Variable | Purpose |
|---|---|
| `PHLO_PROJECT` / `PHLO_DEPLOYMENT` | default project + environment tags |
| `PHLO_RUN_ID` | ambient run correlation when no context is active |

See `examples/` for end-to-end configurations, including the OTel Collector
profile in `docker-compose.yml`.
