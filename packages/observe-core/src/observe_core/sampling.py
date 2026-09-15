"""Deterministic sampling: static rates plus V2 policy rules (spec §7.6).

Sampling runs after the delivery class is known and before enqueue. The
decision is stable per ``(event name, correlation key)`` so related events are
kept or dropped together instead of producing confusing partial histories.

V2 adds ``SamplingPolicy``: an ordered rule list evaluated before the base
delivery-class rates. Rules may match on event name, severity, outcome,
duration, service, environment and live queue pressure. Failures are retained
by default; ``critical`` events are never sampled unless a rule explicitly
opts in; decisions are recorded for observability.
"""

from __future__ import annotations

import hashlib
import re
import threading
from collections import deque
from dataclasses import dataclass
from typing import Any

from observe_core.models import Delivery, Outcome, Severity

_SAMPLE_LEVEL_KEYS = ("run_id", "trace_id", "invocation_id", "request_id")


def deterministic_fraction(*parts: str) -> float:
    """Stable fraction in [0, 1) for the given key parts."""
    digest = hashlib.sha256("|".join(parts).encode()).digest()
    return int.from_bytes(digest[:8]) / 2**64


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
        return deterministic_fraction(event_name, sample_key or "") < rate


@dataclass(frozen=True)
class SamplingRule:
    """One policy rule (spec §7.6).

    Matchers — all optional, all must match when present:

    - ``event``: glob-style name pattern (``asset.*`` or exact);
    - ``severity``: minimum severity (``warn`` matches warn and above);
    - ``outcome``: exact outcome;
    - ``min_duration_ms``: only events at least this slow;
    - ``environment`` / ``service``: exact match;
    - ``queue_pressure``: only when queue depth / capacity >= this fraction.

    Action — exactly one:

    - ``rate``: keep this fraction (applied at run/trace level via the
      deterministic sample key, so a run's events stay together);
    - ``keep``: unconditional keep/drop.
    """

    name: str
    event: str | None = None
    severity: Severity | None = None
    outcome: Outcome | None = None
    min_duration_ms: float | None = None
    environment: str | None = None
    service: str | None = None
    queue_pressure: float | None = None
    rate: float | None = None
    keep: bool | None = None
    allow_critical: bool = False
    """Explicitly permit sampling ``critical`` events (off by default)."""

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SamplingRule:
        """Parse a rule from a settings dict (``ObserveSettings.sampling_policy``)."""
        if not isinstance(data, dict) or "name" not in data:
            raise ValueError(f"sampling rule requires a 'name': {data!r}")
        return cls(
            name=str(data["name"]),
            event=data.get("event"),
            severity=Severity(data["severity"]) if data.get("severity") else None,
            outcome=Outcome(data["outcome"]) if data.get("outcome") else None,
            min_duration_ms=data.get("min_duration_ms"),
            environment=data.get("environment"),
            service=data.get("service"),
            queue_pressure=data.get("queue_pressure"),
            rate=data.get("rate"),
            keep=data.get("keep"),
        )


@dataclass
class SamplingContext:
    """Everything a policy may consider (spec §7.6)."""

    event: str
    delivery: Delivery
    severity: Severity
    outcome: Outcome
    duration_ms: float | None
    service: str | None
    environment: str | None
    sample_key: str | None
    queue_depth: int = 0
    queue_capacity: int = 0
    recent_error_rate: float = 0.0


@dataclass(frozen=True)
class SamplingDecision:
    """A recorded sampling decision (spec §7.6: decisions must be recorded)."""

    keep: bool
    rule: str | None
    """Rule name that decided, or None for the base rate."""
    reason: str


class PolicySampler:
    """Ordered-rule sampler layered over the base delivery-class rates.

    Evaluation order:

    1. failures are always kept (``outcome=failure``), unless a rule named
       ``drop_failures`` exists — an explicit opt-out;
    2. ``critical`` events are kept unless a matching rule sets
       ``allow_critical``;
    3. the first matching rule applies (``keep`` or run-level ``rate``);
    4. otherwise the base per-delivery rates apply.
    """

    def __init__(
        self,
        base: Sampler,
        rules: list[SamplingRule] | None = None,
        *,
        decision_log_size: int = 1_000,
    ) -> None:
        self.base = base
        self.rules = list(rules or ())
        self._decisions: deque[SamplingDecision] = deque(maxlen=decision_log_size)
        self._error_window: deque[bool] = deque(maxlen=512)
        self._lock = threading.Lock()

    @classmethod
    def from_settings(cls, base: Sampler, policy_dicts: list[dict[str, Any]]) -> PolicySampler:
        """Build a policy sampler from ``ObserveSettings.sampling_policy``."""
        return cls(base, [SamplingRule.from_dict(d) for d in policy_dicts])

    def note_outcome(self, outcome: Outcome) -> None:
        """Track recent failures so rules can consider the error rate."""
        with self._lock:
            self._error_window.append(outcome == Outcome.FAILURE)

    def recent_error_rate(self) -> float:
        """Fraction of recent events that failed (rolling window)."""
        with self._lock:
            if not self._error_window:
                return 0.0
            return sum(self._error_window) / len(self._error_window)

    def decide(self, ctx: SamplingContext) -> SamplingDecision:
        """Evaluate policy + base rates; the decision is recorded."""
        decision = self._evaluate(ctx)
        with self._lock:
            self._decisions.append(decision)
        return decision

    def _evaluate(self, ctx: SamplingContext) -> SamplingDecision:
        drop_failures = any(rule.name == "drop_failures" for rule in self.rules)
        if ctx.outcome == Outcome.FAILURE and not drop_failures:
            return SamplingDecision(True, None, "failures retained by default")
        if ctx.delivery == Delivery.CRITICAL:
            matched = next(
                (rule for rule in self.rules if _matches(rule, ctx)),
                None,
            )
            if matched is None or not matched.allow_critical:
                return SamplingDecision(True, None, "critical events are never sampled")
        for rule in self.rules:
            if rule.name == "drop_failures":
                continue
            if not _matches(rule, ctx):
                continue
            if rule.keep is not None:
                return SamplingDecision(rule.keep, rule.name, "rule keep/drop")
            if rule.rate is not None:
                rate = max(0.0, min(1.0, rule.rate))
                # Sample at run/trace level: the whole correlated set shares
                # the decision instead of fragmenting (spec §7.6).
                fraction = deterministic_fraction(ctx.event, ctx.sample_key or "", rule.name)
                return SamplingDecision(fraction < rate, rule.name, f"rule rate={rate}")
        keep = self.base.should_keep(ctx.delivery, ctx.event, ctx.sample_key)
        return SamplingDecision(keep, None, "base rate")

    def recent_decisions(self, limit: int = 100) -> list[SamplingDecision]:
        """Newest-first decision log for diagnostics."""
        with self._lock:
            return list(reversed(self._decisions))[:limit]


_SEVERITY_ORDER = {
    Severity.TRACE: 10,
    Severity.DEBUG: 20,
    Severity.INFO: 30,
    Severity.WARN: 40,
    Severity.ERROR: 50,
    Severity.CRITICAL: 60,
}


def _matches(rule: SamplingRule, ctx: SamplingContext) -> bool:
    if rule.event is not None:
        pattern = re.escape(rule.event).replace(r"\*", ".*")
        if not re.fullmatch(pattern, ctx.event):
            return False
    if rule.severity is not None and _SEVERITY_ORDER[ctx.severity] < _SEVERITY_ORDER[rule.severity]:
        return False
    if rule.outcome is not None and ctx.outcome != rule.outcome:
        return False
    if rule.min_duration_ms is not None and (
        ctx.duration_ms is None or ctx.duration_ms < rule.min_duration_ms
    ):
        return False
    if rule.environment is not None and ctx.environment != rule.environment:
        return False
    if rule.service is not None and ctx.service != rule.service:
        return False
    if rule.queue_pressure is not None:
        if ctx.queue_capacity <= 0:
            return False
        if ctx.queue_depth / ctx.queue_capacity < rule.queue_pressure:
            return False
    return True
