"""Alerting webhooks — spec §26/§34.

Fire-and-forget POSTs to configured webhook URLs when insights or
incidents are created. Delivery is best-effort and bounded: failures are
logged and counted, never allowed to delay or fail ingestion.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

import httpx

logger = logging.getLogger("phlo_observer.alerts")

_TIMEOUT = httpx.Timeout(5.0)
_HOLDER: dict[str, httpx.AsyncClient] = {}


def _http() -> httpx.AsyncClient:
    """Lazily created shared client for webhook delivery."""
    client = _HOLDER.get("client")
    if client is None:
        client = httpx.AsyncClient(timeout=_TIMEOUT)
        _HOLDER["client"] = client
    return client


async def notify(
    urls: list[str],
    kind: str,
    payload: dict[str, Any],
    *,
    tasks: set[asyncio.Task[None]],
) -> None:
    """Schedule webhook POSTs; tracked so shutdown can drain them."""
    for url in urls:
        task = asyncio.create_task(_post(url, kind, payload))
        tasks.add(task)
        task.add_done_callback(tasks.discard)


async def _post(url: str, kind: str, payload: dict[str, Any]) -> None:
    try:
        resp = await _http().post(url, json={"kind": kind, **payload}, timeout=_TIMEOUT)
        if resp.status_code >= 400:
            logger.warning("alert webhook %s returned %s", url, resp.status_code)
    except httpx.HTTPError as exc:
        logger.warning("alert webhook %s failed: %s", url, exc)
