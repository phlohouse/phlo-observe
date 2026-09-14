"""Dagster example: a job instrumented with phlo-observe helpers.

The SDK helpers emit canonical events through observe-core; the configured
drains (usually the observer HTTP endpoint) carry them to phlo-observer.
"""

from __future__ import annotations

import os

from observe_core.config import HttpDrainConfig, ObserveSettings
from observe_core.runtime import configure, shutdown
from phlo_observe import asset_materialize, pipeline_run, quality_validate

configure(
    ObserveSettings(
        service_name="example-dagster",
        environment=os.environ.get("DAGSTER_DEPLOYMENT", "dev"),
        drains=[
            HttpDrainConfig(
                endpoint=os.environ.get("OBSERVE_HTTP_ENDPOINT", "http://localhost:8080/v1/events"),
                api_key=os.environ.get("OBSERVE_HTTP_API_KEY"),
            )
        ],
    )
)


def run_etl(run_id: str) -> None:
    """Stand-in for a Dagster job body — SDK helpers wrap real work."""
    with pipeline_run(job="nightly_etl", run_id=run_id, trigger="schedule"):
        for asset in ("staging.orders", "mart.fct_orders"):
            with asset_materialize(asset_key=asset):
                pass  # real materialization work
        with quality_validate(suite="mart", checks_total=12, checks_passed=12):
            pass
    shutdown()
