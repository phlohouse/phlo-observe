"""DLT example: wrap a dlt pipeline run with phlo-observe instrumentation."""

from __future__ import annotations

import os

from observe_core.config import HttpDrainConfig, ObserveSettings
from observe_core.runtime import configure, shutdown
from phlo_observe import pipeline_run

configure(
    ObserveSettings(
        service_name="example-dlt",
        environment="dev",
        drains=[
            HttpDrainConfig(
                endpoint=os.environ.get("OBSERVE_HTTP_ENDPOINT", "http://localhost:8080/v1/events"),
                api_key=os.environ.get("OBSERVE_HTTP_API_KEY"),
            )
        ],
    )
)


def run_pipeline(pipeline_name: str, dataset: str) -> None:
    """Emit pipeline.run for a dlt load; wrap real `pipeline.run()` inside."""
    with pipeline_run(job=pipeline_name, attributes={"dlt.dataset": dataset}):
        # import dlt; pipeline = dlt.pipeline(...); pipeline.run(source)
        pass
    shutdown()
