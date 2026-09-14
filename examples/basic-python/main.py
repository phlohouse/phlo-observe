"""Minimal end-to-end example: emit events to a local phlo-observer.

Usage:
    docker compose up -d postgres phlo-observer
    OBSERVE_HTTP_ENDPOINT=http://localhost:8080/v1/events \
    OBSERVE_HTTP_API_KEY=dev-ingest-token \
        uv run python examples/basic-python/main.py
"""

from __future__ import annotations

import time

from observe_core.config import HttpDrainConfig, ObserveSettings
from observe_core.runtime import configure, flush, shutdown
from phlo_observe import asset_materialize, pipeline_run, quality_validate


def main() -> None:
    """Emit a small pipeline story: run -> materialize -> validate."""
    import os

    configure(
        ObserveSettings(
            service_name="example-basic",
            environment="dev",
            drains=[
                HttpDrainConfig(
                    endpoint=os.environ.get(
                        "OBSERVE_HTTP_ENDPOINT", "http://localhost:8080/v1/events"
                    ),
                    api_key=os.environ.get("OBSERVE_HTTP_API_KEY"),
                )
            ],
        )
    )
    try:
        with pipeline_run(job="example_job", run_id="example-run-1", trigger="manual"):
            time.sleep(0.05)
            with asset_materialize(asset_key="mart.fct_orders", rows_out=10_000):
                time.sleep(0.05)
            with quality_validate(suite="mart", checks_total=3, checks_passed=3):
                pass
        flush(5.0)
        print("emitted 3 events; query them at GET /v1/events?run_id=example-run-1")
    finally:
        shutdown()


if __name__ == "__main__":
    main()
