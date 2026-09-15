"""Drain protocol shared by all export destinations."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable

from observe_core.models import Delivery


@dataclass(frozen=True, slots=True)
class CanonicalEvent:
    """A finalized event as seen by drains.

    ``data`` is the canonical envelope dict *after* normalization and
    redaction; ``payload`` is its serialized JSON form. Drains only ever see
    this post-redaction view.
    """

    data: dict[str, Any]
    payload: bytes
    delivery: Delivery

    @property
    def event(self) -> str:
        """Canonical event name."""
        return str(self.data["event"])

    @property
    def outcome(self) -> str:
        """Canonical outcome."""
        return str(self.data["outcome"])

    @property
    def severity(self) -> str:
        """Canonical severity."""
        return str(self.data["severity"])


class DrainFailure(Exception):
    """Raised when a drain cannot accept a batch after its own retries."""


class PermanentDrainFailure(DrainFailure):
    """Raised when the destination rejected the payload itself.

    Unlike a transient :class:`DrainFailure` (network errors, 429, 5xx),
    retrying a permanent rejection can never succeed. Spool replay treats it
    as a poison segment and quarantines it instead of blocking the head of
    the spool forever.
    """


@runtime_checkable
class Drain(Protocol):
    """Export destination for canonical events.

    Implementations perform their own retry policy; raising
    :class:`DrainFailure` from ``emit_batch`` tells the runtime the batch was
    rejected so critical events can be spooled.

    ``is_remote`` marks drains that talk to a network service; the runtime
    replays the local spool only to remote drains.
    """

    name: str
    is_remote: bool = False

    def emit_batch(self, events: Sequence[CanonicalEvent]) -> None:
        """Emit a batch of canonical events. Raise DrainFailure on rejection."""
        ...

    def emit_raw(self, payloads: Sequence[bytes]) -> None:
        """Emit pre-serialized canonical payloads (spool replay)."""
        ...

    def flush(self) -> None:
        """Flush any internal buffers."""
        ...

    def close(self) -> None:
        """Release resources."""
        ...
