"""Keystone-style example: a generic app instrumented with observe-core only.

No Phlo SDK is required (spec §98): a lab-automation-style WAP story — load a
plate to an audit branch, validate, promote — emitted directly through
``observe()``/``event()`` with explicit correlation. The observer timeline
shows the phases correlated on one run.
"""

from __future__ import annotations

import os
import time

from observe_core import bind_context, configure, event, flush, observe, shutdown
from observe_core.config import HttpDrainConfig, ObserveSettings


def main() -> None:
    """Emit a WAP promotion story using only the generic core API."""
    configure(
        ObserveSettings(
            service_name="example-keystone",
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
        branch = "run/assay-1042"
        with (
            bind_context(run_id="assay-1042", job_id="plate_qc"),
            observe("pipeline.run", category="pipeline", attributes={"job": "plate_qc"}),
        ):
            event(
                "wap.branch.create",
                category="wap",
                attributes={"branch": branch, "base_branch": "main"},
            )
            with observe(
                "ingestion.load",
                category="data",
                attributes={"plate": "P-1042", "rows_out": 96},
                correlation={"asset_key": "lab.plate_p1042", "branch": branch},
            ):
                time.sleep(0.02)
            with observe(
                "quality.validate",
                category="quality",
                attributes={"checks_total": 4, "checks_passed": 4, "suite": "plate_qc"},
            ):
                pass
            event(
                "wap.promote",
                category="wap",
                delivery="critical",
                attributes={"branch": branch, "target": "main", "snapshots_promoted": 3},
                correlation={"branch": branch},
            )
        flush(5.0)
        print("emitted WAP story; see GET /v1/runs/assay-1042/timeline")
    finally:
        shutdown()


if __name__ == "__main__":
    main()
