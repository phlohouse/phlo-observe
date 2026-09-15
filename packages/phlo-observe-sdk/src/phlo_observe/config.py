"""Phlo-flavoured runtime configuration.

``configure_phlo`` is the SDK quick-start entry point (spec §72)::

    from phlo_observe import configure_phlo, observe

    configure_phlo(service_name="example")

    with observe("asset.materialize") as evt:
        ...

It delegates to :func:`observe_core.configure` and additionally wires an
HTTP drain to a phlo-observer ingest endpoint when ``observer_endpoint`` or
``OBSERVE_HTTP_ENDPOINT`` is set. Credentials come from ``token``/
``api_key`` or the ``OBSERVE_HTTP_TOKEN`` / ``OBSERVE_HTTP_API_KEY``
environment variables.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any

from observe_core import configure
from observe_core.config import HttpDrainConfig, ObserveSettings

if TYPE_CHECKING:
    from observe_core.enrich import Enricher
    from observe_core.runtime import Runtime


def _drain_endpoint(drain: Any) -> str | None:
    """HTTP endpoint of a drain config, whether model or dict-shaped."""
    if isinstance(drain, HttpDrainConfig):
        return drain.endpoint
    if isinstance(drain, dict) and drain.get("type") == "http":
        return drain.get("endpoint")
    return None


def configure_phlo(
    settings: ObserveSettings | None = None,
    *,
    observer_endpoint: str | None = None,
    token: str | None = None,
    api_key: str | None = None,
    enrichers: list[Enricher] | None = None,
    **overrides: Any,
) -> Runtime:
    """Configure the global observe-core runtime with Phlo defaults.

    ``overrides`` are forwarded to :class:`observe_core.ObserveSettings`
    (``service_name``, ``environment``, ``drains``, ...). When an observer
    endpoint is known — via ``observer_endpoint`` or ``OBSERVE_HTTP_ENDPOINT``
    — an HTTP drain is appended unless one is already configured for it.
    """
    if settings is not None:
        # Fold the settings into overrides so the merged result is fully
        # re-validated by ObserveSettings (model_copy(update=...) would not be).
        merged = settings.model_dump(mode="python")
        merged.update(overrides)
        overrides, settings = merged, None
    drains: list[Any] | None = overrides.pop("drains", None)
    endpoint = observer_endpoint or os.environ.get("OBSERVE_HTTP_ENDPOINT")
    if endpoint is not None and not any(_drain_endpoint(d) == endpoint for d in drains or []):
        drains = [
            *(drains or []),
            HttpDrainConfig(
                endpoint=endpoint,
                token=token or os.environ.get("OBSERVE_HTTP_TOKEN"),
                api_key=api_key or os.environ.get("OBSERVE_HTTP_API_KEY"),
            ),
        ]
    if drains is not None:
        overrides["drains"] = drains
    return configure(enrichers=enrichers, **overrides)
