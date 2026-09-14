"""Optional forwarding of normalized events to an OTLP/HTTP endpoint.

Fire-and-forget with a short timeout: forwarding must never delay or fail
ingestion. Outcomes are counted on ``phlo_observer_export_total``.
"""

from __future__ import annotations

import logging
from typing import Any

import httpx

from phlo_observer import metrics

logger = logging.getLogger("phlo_observer.forward")

_TIMEOUT = httpx.Timeout(5.0)


async def forward_events(events: list[dict[str, Any]], endpoint: str) -> None:
    """POST canonical event dicts to ``endpoint``; failures are logged+counted."""
    if not events:
        return
    try:
        async with httpx.AsyncClient(timeout=_TIMEOUT) as client:
            resp = await client.post(endpoint, json={"events": events})
        ok = resp.status_code < 400
        metrics.EXPORT.labels(destination="otlp", status="success" if ok else "error").inc(
            len(events)
        )
        if not ok:
            logger.warning("OTLP forward returned %s", resp.status_code)
    except Exception:
        metrics.EXPORT.labels(destination="otlp", status="error").inc(len(events))
        logger.exception("OTLP forward to %s failed", endpoint)
