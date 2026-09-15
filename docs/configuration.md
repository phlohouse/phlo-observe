# Configuration reference

All three components are configured by environment variables or typed
settings objects. phlo-observer secrets (`DATABASE_URL`, `INGEST_TOKENS`,
`READ_TOKENS`) additionally accept a `_FILE` variant pointing at a mounted
secret file, e.g. `PHLO_OBSERVER_INGEST_TOKENS_FILE=/run/secrets/tokens`.

## phlo-observer (`PHLO_OBSERVER_*`)

| Variable | Default | Purpose |
|---|---|---|
| `HOST` | `0.0.0.0` | Bind address |
| `PORT` | `8080` | Bind port |
| `LOG_LEVEL` | `INFO` | uvicorn/app log level |
| `DATABASE_URL` | local dev DSN | `postgresql+asyncpg://...`; `_FILE` supported |
| `DB_POOL_SIZE` / `DB_POOL_MAX_OVERFLOW` | `10` / `10` | Connection pool |
| `INGEST_TOKENS` | — | Comma-separated write tokens; empty = dev mode; `_FILE` supported |
| `READ_TOKENS` | — | Comma-separated query tokens; empty = dev mode; `_FILE` supported |
| `ADMIN_TOKENS` | — | Comma-separated admin tokens (transitions, quarantine); never inherits read tokens; `_FILE` supported |
| `AUTH_OPTIONAL_DEV` | `true` | `false` hard-fails startup unless all three token sets are configured |
| `RAW_RETENTION_DAYS` | `14` | `raw_events.expires_at` sweep |
| `EVENT_RETENTION_DAYS` | `90` | normalized event TTL |
| `RUN_RETENTION_DAYS` | `365` | run projection TTL |
| `RETENTION_INTERVAL_S` | `3600` | in-process sweep cadence |
| `MAX_BODY_BYTES` | `10485760` | request limit, wire and decompressed size |
| `MAX_BATCH_EVENTS` | `1000` | events per request |
| `MAX_RAW_PAYLOAD_BYTES` | `262144` | larger raw bodies are retained as a SHA-256 digest only |
| `OTLP_ENDPOINT` | — | optional OTLP/HTTP logs destination for accepted events; a bare collector base (`http://host:4318`) gets `/v1/logs` appended |
| `METRICS_ENABLED` | `true` | expose `/metrics` |
| `METRICS_PUBLIC` | `false` | `true` skips the read-token check on `/metrics` |
| `DOCS_ENABLED` | `true` | serve `/docs` + `/openapi.json`; disable for hardened deployments |
| `SELF_OBSERVE_DRAINS` | `console` | internal observe-core drains: `console,jsonl,otlp,memory`; `http` is rejected (self-ingest loop guard) |
| `RUN_MIGRATIONS` | `false` | apply Alembic migrations to head at startup |

Security notes:

- Tokens are compared with `secrets.compare_digest` and never logged.
- With no tokens configured the API is fully open — set
  `AUTH_OPTIONAL_DEV=false` anywhere outside local dev so startup fails
  instead.
- Once any token is configured, admin endpoints require `ADMIN_TOKENS`:
  an unset admin list fails closed (denies everyone) rather than falling
  back to read tokens.
- Canonical-event retention keys on `received_at` (the observer's clock),
  not producer-supplied `observed_at` — clock-skewed producers cannot
  expire events early or keep them forever.
- `config` and `serve` resolve `_FILE` variants; the app factory path
  (`create_app()` with no args, e.g. uvicorn `--factory`) uses
  `load_settings()` so `_FILE` works there too.

## observe-core (`OBSERVE_*`)

| Variable | Default | Purpose |
|---|---|---|
| `ENABLED` | `true` | master switch; `false` = near-zero-cost no-op |
| `SERVICE_NAME` | `unknown` | `service.name` on every event |
| `SERVICE_VERSION` / `ENVIRONMENT` / `INSTANCE_ID` / `HOST` | — / `development` / — / — | service fields |
| `QUEUE_CAPACITY` | `10000` | bounded event queue |
| `DROP_POLICY` | `drop_newest` | `drop_newest` / `drop_oldest` — queue pressure policy for non-critical events |
| `BATCH_SIZE` / `FLUSH_INTERVAL_MS` | `100` / `1000` | worker batching |
| `WORKER_COUNT` / `SHUTDOWN_TIMEOUT_S` | `1` / `5.0` | drain workers; bounded shutdown |
| `DRAINS` | `console` | shorthand list (`console,http,jsonl,otlp,memory`) or JSON array of drain configs. JSON form exposes per-drain fields such as `{"type": "http", "endpoint": ..., "spool_on_failure": false}` — `spool_on_failure` (default `true`) controls whether a rejected remote write spools critical events |
| `HTTP_ENDPOINT` | — | observer ingest URL (required when `DRAINS` includes `http`) |
| `HTTP_TOKEN` | — | observer token → `Authorization: Bearer` |
| `HTTP_API_KEY` | — | observer token → `X-API-Key` (equivalent to `HTTP_TOKEN`) |
| `JSONL_PATH` | `observe-events.jsonl` | file target for the `jsonl` shorthand drain |
| `OTLP_ENDPOINT` | — | collector endpoint for the `otlp` shorthand drain |
| `SPOOL_ENABLED` | `true` | local critical-event spool |
| `SPOOL_DIR` | `~/.local/state/phlo-observe/spool` | spool directory (`$XDG_STATE_HOME` aware) |
| `SPOOL_MAX_BYTES` / `SPOOL_SEGMENT_MAX_BYTES` | `1 GiB` / `32 MiB` | bounded spool |
| `SPOOL_ON_FULL` | `drop_oldest` | `drop_oldest` / `drop_newest` |
| `SPOOL_REPLAY_INTERVAL_S` | `30` | how often the worker replays the spool to remote drains |
| `CAPTURE_STACKTRACE` | `true` | include tracebacks on error events; `observe(capture_stacktrace=...)` overrides per operation |
| `MAX_EVENT_BYTES` / `MAX_DEPTH` | `262144` / `8` | event size cap + normalization depth |
| `REDACT_KEYS` | — | extra exact key names to redact (case-insensitive; comma-separated or JSON list) |
| `REDACT_KEY_PATTERNS` / `REDACT_PATHS` / `REDACT_VALUE_PATTERNS` | — | regex key rules, dotted paths, value regexes (comma-separated or JSON list) |
| `REDACTION_ENABLED` | `true` | master switch for redaction |
| `SAMPLING_DEBUG_RATE` | `1.0` dev / `0.1` production | fraction of `debug` events kept |
| `SAMPLING_TELEMETRY_RATE` | `1.0` | `critical` events are never sampled |
| `TELEMETRY_REQUIRED` | `false` | `true` = fail-closed emission (`TelemetryError`) |
| `FAIL_FAST` | `false` | dev aid: raise on event-construction errors |

Default redacted key *patterns* match at segment boundaries
(`_ - . /` or camelCase): `password passwd secret token api_key apikey
authorization cookie set-cookie client_secret access_key private_key
credentials` — so `access_token`, `refresh-token`, `secretKey`,
`x-api-key`, `aws_access_key_id` are all covered by default.

## phlo-observe SDK

`configure_phlo(...)` accepts `service_name`, `environment`, `drains`, and
`observer_endpoint`/`token`/`api_key`; the endpoint and credentials also
fall back to the `OBSERVE_HTTP_*` variables above. Everything else is
observe-core `OBSERVE_*` configuration — the SDK adds no env vars of its
own.

See `examples/` for end-to-end configurations, including the OTel Collector
profile in `docker-compose.yml`.
