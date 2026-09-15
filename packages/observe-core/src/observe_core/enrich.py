"""Enricher extension point.

Enrichers mutate the in-flight event *before* redaction and emission. They are
how integrations (Phlo SDK, a future Keystone package) attach domain context
without the application passing identifiers to every call.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

if TYPE_CHECKING:
    from observe_core.builder import EventBuilder


@runtime_checkable
class Enricher(Protocol):
    """Mutates a mutable event during finalization."""

    def enrich(self, event: EventBuilder) -> None:
        """Add or modify fields on the event being finalized."""
        ...


class MutableEvent(Protocol):
    """The mutation surface enrichers are allowed to use."""

    def set(self, **attributes: Any) -> None:
        """Set multiple attribute values at once."""
        ...

    def set_attribute(self, key: str, value: Any) -> None:
        """Set one attribute."""
        ...

    def set_correlation(self, **values: Any) -> None:
        """Set correlation identifiers."""
        ...

    def set_entity(self, role: str, identifier: object) -> None:
        """Record a canonical entity identifier (V2 envelope)."""
        ...

    def set_tag(self, key: str, value: object) -> None:
        """Attach a searchable label (V2 envelope)."""
        ...

    def annotate(self, note: str) -> None:
        """Attach a human-readable note."""
        ...
