"""Cross-instance stream fan-out over PostgreSQL LISTEN/NOTIFY.

The SSE hub is in-process: under a load-balanced multi-replica deployment
a subscriber sees only the notifications its own instance produced. The
observer already hard-depends on Postgres, so notifications cross
instances through ``pg_notify`` — the smallest shared bus that fits the
existing architecture (no Kafka, no new infrastructure).

Publish side: ``persist_events`` emits one ``pg_notify`` per ingest batch
inside the transaction, so a notification implies committed state.
Listen side: each instance holds a dedicated asyncpg connection and
republishes every other instance's messages into its local hub. Payloads
are capped; an oversized batch degrades to a ``refresh`` hint — the SSE
contract is "something changed, refetch", never the data itself.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import uuid
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger("phlo_observer.notify")

CHANNEL = "phlo_observer_stream"
"""Postgres NOTIFY channel carrying stream messages between instances."""

_MAX_PAYLOAD_BYTES = 7000
"""Stay under Postgres's 8000-byte NOTIFY payload limit with headroom."""


def pack_notify(instance_id: str, messages: list[dict[str, Any]]) -> str | None:
    """Pack a batch's stream messages into one NOTIFY payload.

    Returns ``None`` when there is nothing to send; an oversized batch
    collapses to a single ``refresh`` hint rather than failing ingest.
    """
    if not messages:
        return None
    payload = json.dumps({"origin": instance_id, "messages": messages}, default=str)
    if len(payload.encode()) <= _MAX_PAYLOAD_BYTES:
        return payload
    return json.dumps({"origin": instance_id, "messages": [{"kind": "refresh", "data": {}}]})


async def emit_notify(session: AsyncSession, payload: str) -> None:
    """Queue one NOTIFY; Postgres delivers it on COMMIT, never on rollback."""
    await session.execute(
        text("SELECT pg_notify(:channel, :payload)"),
        {
            "channel": CHANNEL,
            "payload": payload,
        },
    )


class NotifyBridge:
    """LISTEN loop republishing other instances' messages into the local hub.

    Dedicated connection (not the SQLAlchemy pool) so LISTEN sits open for
    the app's lifetime. Reconnects with bounded backoff on drops; a dead
    bridge degrades SSE to instance-local only — ingest is unaffected.
    """

    def __init__(self, database_url: str, hub: Any, *, instance_id: str) -> None:
        dsn: str | None = None
        for prefix in ("postgresql+asyncpg://", "postgres+asyncpg://"):
            if database_url.startswith(prefix):
                dsn = "postgresql://" + database_url[len(prefix) :]
                break
        if dsn is None and database_url.startswith(("postgresql://", "postgres://")):
            dsn = database_url
        self._dsn = dsn
        self._hub = hub
        self._instance_id = instance_id
        self._stop = asyncio.Event()
        self._task: asyncio.Task[None] | None = None

    def start(self) -> None:
        """Spawn the listener task.

        A non-asyncpg DSN cannot LISTEN: the bridge disables cleanly (one
        warning at startup) instead of reconnect-looping forever on a URL
        asyncpg can never parse.
        """
        if self._dsn is None:
            logger.warning(
                "notify bridge disabled: database_url is not an asyncpg "
                "Postgres DSN; SSE stays instance-local"
            )
            return
        self._task = asyncio.create_task(self._run(), name="phlo-notify-bridge")

    async def stop(self) -> None:
        """Stop listening and release the connection."""
        self._stop.set()
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task

    async def _run(self) -> None:
        import asyncpg  # noqa: PLC0415 - optional at runtime

        backoff = 0.5
        while not self._stop.is_set():
            conn = None
            try:
                conn = await asyncpg.connect(
                    self._dsn,
                    # Identifiable in pg_stat_activity so operators (and
                    # tests) can see/terminate the LISTEN backend directly.
                    server_settings={
                        "application_name": f"phlo-observer-notify-{self._instance_id}"
                    },
                )
                # A dropped connection cannot raise through a parked wait —
                # the termination listener converts it into an event we race
                # against shutdown, so the backoff path below is reachable.
                dropped = asyncio.Event()
                conn.add_termination_listener(lambda _conn, d=dropped: d.set())
                await conn.add_listener(CHANNEL, self._on_notify)
                backoff = 0.5
                stop_wait = asyncio.create_task(self._stop.wait())
                drop_wait = asyncio.create_task(dropped.wait())
                try:
                    await asyncio.wait({stop_wait, drop_wait}, return_when=asyncio.FIRST_COMPLETED)
                finally:
                    stop_wait.cancel()
                    drop_wait.cancel()
                if dropped.is_set() and not self._stop.is_set():
                    raise ConnectionError("notify listener connection terminated")
            except asyncio.CancelledError:
                raise
            except Exception:
                if not self._stop.is_set():
                    logger.warning("notify bridge dropped; reconnecting", exc_info=True)
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 15.0)
            finally:
                if conn is not None:
                    with contextlib.suppress(Exception):
                        await conn.close()

    def _on_notify(self, conn: Any, pid: int, channel: str, payload: str) -> None:
        """Republish foreign messages into the local hub; skip our own."""
        try:
            frame = json.loads(payload)
            if frame.get("origin") == self._instance_id:
                return
            for message in frame.get("messages") or []:
                self._hub.publish(message.get("kind", "refresh"), message.get("data") or {})
        except (ValueError, AttributeError):
            logger.warning("malformed notify payload on %s", channel)


def new_instance_id() -> str:
    """Unique identity for this observer process, used to skip own NOTIFYs."""
    return uuid.uuid4().hex[:12]
