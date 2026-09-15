# Examples

Runnable examples live in [`examples/`](https://github.com/phlohouse/phlo-observe/tree/main/examples)
at the repository root.

| Example | What it shows |
|---|---|
| `examples/basic-python` | Minimal end-to-end: `configure_phlo`-style setup, `pipeline_run` / `asset_materialize` / `quality_validate` emitted to a local observer |
| `examples/keystone-style-app` | A generic application using **observe-core only** — no Phlo SDK — emitting a correlated WAP story (branch create → load → validate → promote) |
| `examples/dagster` | Dagster job instrumentation through the SDK helpers |
| `examples/dbt` | Shipping `run_results.json` to `POST /v1/ingest/dbt/artifacts` |
| `examples/dlt` | DLT pipeline instrumentation |
| `examples/otel-collector-config.yaml` | OTel Collector receiving OTLP logs and forwarding them to `POST /v1/ingest/otlp` (JSON encoding) |

Every example expects a local stack:

```bash
docker compose up -d postgres phlo-observer
export OBSERVE_HTTP_ENDPOINT=http://localhost:8080/v1/events
export OBSERVE_HTTP_API_KEY=dev-ingest-token
uv run python examples/basic-python/main.py
```

Then inspect the correlated run:

```bash
curl -s -H "X-API-Key: dev-read-token" \
  "http://localhost:8080/v1/runs/example-run-1/timeline"
```
