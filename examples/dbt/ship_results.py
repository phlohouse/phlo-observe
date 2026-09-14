"""dbt example: ship run_results.json to phlo-observer after a dbt run.

Run after `dbt build`:
    uv run python examples/dbt/ship_results.py target/run_results.json
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path


def main() -> None:
    """POST a run_results document to the observer's dbt adapter."""
    import urllib.request

    path = Path(sys.argv[1] if len(sys.argv) > 1 else "target/run_results.json")
    body = json.dumps({"run_results": json.loads(path.read_text())})
    req = urllib.request.Request(
        os.environ.get("PHLO_OBSERVER_URL", "http://localhost:8080") + "/v1/ingest/dbt/artifacts",
        data=body.encode(),
        headers={
            "Content-Type": "application/json",
            "Authorization": "Bearer "
            + os.environ.get("PHLO_OBSERVER_INGEST_TOKEN", "dev-ingest-token"),
        },
    )
    resp = urllib.request.urlopen(req, timeout=10)
    print(resp.status, resp.read().decode())


if __name__ == "__main__":
    main()
