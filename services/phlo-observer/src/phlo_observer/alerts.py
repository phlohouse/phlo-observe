"""Alerting webhooks — spec §26/§32/§34.

Fire-and-forget POSTs to configured webhook URLs when insights or
incidents are created. Delivery is best-effort and bounded: failures are
logged and counted, never allowed to delay or fail ingestion. A per-key
cooldown suppresses repeat alerts for the same condition (§32 dedup /
cooldown) — insight-level dedup already prevents most repeats, this covers
insights that resolve and re-open.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

import httpx

logger = logging.getLogger("phlo_observer.alerts")

_TIMEOUT = httpx.Timeout(5.0)
_HOLDER: dict[str, httpx.AsyncClient] = {}
_last_sent: dict[str, float] = {}
_MAX_KEYS = 10_000
DEFAULT_COOLDOWN_S = 300.0


def _http() -> httpx.AsyncClient:
    """Lazily created shared client for webhook delivery."""
    client = _HOLDER.get("client")
    if client is None:
        client = httpx.AsyncClient(timeout=_TIMEOUT)
        _HOLDER["client"] = client
    return client


def _cooldown_ok(key: str, cooldown_s: float) -> bool:
    """True when ``key`` hasn't alerted within the cooldown window."""
    now = time.monotonic()
    last = _last_sent.get(key)
    if last is not None and now - last < cooldown_s:
        return False
    if len(_last_sent) >= _MAX_KEYS:
        _last_sent.clear()  # bound memory; a flush is a graceful reset
    _last_sent[key] = now
    return True


async def notify(
    urls: list[str],
    kind: str,
    payload: dict[str, Any],
    *,
    tasks: set[asyncio.Task[None]],
    cooldown_key: str | None = None,
    cooldown_s: float = DEFAULT_COOLDOWN_S,
) -> bool:
    """Schedule webhook POSTs; tracked so shutdown can drain them.

    Returns False when the alert was suppressed by cooldown. ``cooldown_key``
    should be a stable identity for the condition (insight dedupe key or
    incident id); when omitted every call sends.
    """
    if cooldown_key is not None and not _cooldown_ok(f"{kind}:{cooldown_key}", cooldown_s):
        return False
    for url in urls:
        task = asyncio.create_task(_post(url, kind, payload))
        tasks.add(task)
        task.add_done_callback(tasks.discard)
    return True


async def _post(url: str, kind: str, payload: dict[str, Any]) -> None:
    try:
        resp = await _http().post(url, json={"kind": kind, **payload}, timeout=_TIMEOUT)
        if resp.status_code >= 400:
            logger.warning("alert webhook %s returned %s", url, resp.status_code)
    except httpx.HTTPError as exc:
        logger.warning("alert webhook %s failed: %s", url, exc)
