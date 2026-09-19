"""Real-time update stream — spec §26.

Server-Sent Events fan-out: projection changes publish lightweight
notifications; clients refetch authoritative state after a notification
(the stream is a notification channel, not the data API).

Each process owns a local hub. PostgreSQL LISTEN/NOTIFY forwards committed
changes from other replicas; local publishers likewise wait for commit.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

logger = logging.getLogger("phlo_observer.stream")

MAX_SUBSCRIBERS = 128
"""Bounded subscriber set: notifications are best-effort, never a leak."""

_queue_size = 256


class StreamHub:
    """In-process SSE broadcaster with bounded per-subscriber queues."""

    def __init__(self) -> None:
        self._subscribers: set[asyncio.Queue[dict[str, Any]]] = set()

    def publish(self, kind: str, data: dict[str, Any]) -> int:
        """Offer a notification to every subscriber; drop on backpressure.

        Returns the number of subscribers the notification reached. A full
        subscriber queue drops the notification rather than blocking the
        ingest path — clients refetch on the next notification.
        """
        delivered = 0
        for queue in self._subscribers:
            try:
                queue.put_nowait({"kind": kind, "data": data})
            except asyncio.QueueFull:
                continue
            delivered += 1
        return delivered

    def subscribe(self) -> asyncio.Queue[dict[str, Any]]:
        """Register a subscriber queue; raise when at capacity."""
        if len(self._subscribers) >= MAX_SUBSCRIBERS:
            raise RuntimeError("too many stream subscribers")
        queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=_queue_size)
        self._subscribers.add(queue)
        return queue

    def unsubscribe(self, queue: asyncio.Queue[dict[str, Any]]) -> None:
        """Remove a subscriber queue."""
        self._subscribers.discard(queue)


def sse_encode(message: dict[str, Any]) -> bytes:
    """Encode one notification as an SSE frame."""
    return f"event: {message['kind']}\ndata: {json.dumps(message['data'])}\n\n".encode()
