"""Keystone-style example: full story with WAP, validation, and promotion.

Simulates a write-audit-publish flow: materialize to a WAP branch, validate,
then promote — the observer timeline shows the phases correlated on one run.
"""

from __future__ import annotations

import os
import time

from observe_core.config import HttpDrainConfig, ObserveSettings
from observe_core.runtime import configure, flush, shutdown
from phlo_observe import asset_materialize, pipeline_run, quality_validate, wap_promote


def main() -> None:
    """Emit a WAP promotion story."""
    configure(
        ObserveSettings(
            service_name="example-wap",
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
        with pipeline_run(job="wap_publish", run_id="wap-run-1", trigger="manual"):
            branch = "wap/audit-orders"
            with asset_materialize(
                asset_key="mart.fct_orders",
                attributes={"wap.branch": branch, "wap.branch_create": True},
            ):
                time.sleep(0.02)
            with quality_validate(suite="mart", checks_total=5, checks_passed=5):
                pass
            with wap_promote(
                branch=branch,
                target="main",
                attributes={"snapshots_promoted": 3},
            ):
                pass
        flush(5.0)
        print("emitted WAP story; see GET /v1/runs/wap-run-1/timeline")
    finally:
        shutdown()


if __name__ == "__main__":
    main()
