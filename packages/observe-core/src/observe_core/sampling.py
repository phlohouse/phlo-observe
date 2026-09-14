"""Deterministic sampling.

Sampling runs after the delivery class is known and before enqueue. The
decision is stable per ``(event name, correlation key)`` so related events are
kept or dropped together instead of producing confusing partial histories.
"""

from __future__ import annotations

import hashlib

from observe_core.models import Delivery


class Sampler:
    """Per-delivery-class deterministic sampler."""

    def __init__(self, *, debug_rate: float, telemetry_rate: float) -> None:
        self._rates = {
            Delivery.DEBUG: max(0.0, min(1.0, debug_rate)),
            Delivery.TELEMETRY: max(0.0, min(1.0, telemetry_rate)),
            Delivery.CRITICAL: 1.0,
        }

    def should_keep(self, delivery: Delivery, event_name: str, sample_key: str | None) -> bool:
        """Return True when the event survives sampling.

        ``sample_key`` is the strongest available correlation identifier
        (run_id, trace_id, ...). Without one, the event ID is used, which is
        still deterministic but uncorrelated.
        """
        rate = self._rates[delivery]
        if rate >= 1.0:
            return True
        if rate <= 0.0:
            return False
        digest = hashlib.sha256(f"{event_name}|{sample_key or ''}".encode()).digest()
        fraction = int.from_bytes(digest[:8]) / 2**64
        return fraction < rate
