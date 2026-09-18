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

from phlo_observe.events import DBT_INVOCATION, DLT_PIPELINE_RUN, PIPELINE_RUN

if TYPE_CHECKING:
    from observe_core.enrich import Enricher
    from observe_core.runtime import Runtime

PHLO_TAIL_TERMINAL_EVENTS = (PIPELINE_RUN, DLT_PIPELINE_RUN, DBT_INVOCATION)
"""Phlo's run-boundary event names (spec §7.7): the ``observe`` wrapper emits
``pipeline.run`` on scope exit and the dlt/dbt integrations emit their
invocation events on run completion. None carry a ``.completed``-style
suffix, so tail sampling needs them registered explicitly — without them a
tail-sampled run buffers until an age/size bound flushes it.
"""


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

    ``tail_terminal_events`` is unioned with Phlo's run-boundary names
    (``pipeline.run``, ``dlt.pipeline.run``, ``dbt.invocation``) so tail
    sampling closes Phlo run buffers at the run's last event.
    """
    if settings is not None:
        # Fold the settings into overrides so the merged result is fully
        # re-validated by ObserveSettings (model_copy(update=...) would not be).
        merged = settings.model_dump(mode="python")
        merged.update(overrides)
        overrides, settings = merged, None
    overrides["tail_terminal_events"] = sorted(
        {*PHLO_TAIL_TERMINAL_EVENTS, *(overrides.get("tail_terminal_events") or [])}
    )
    drains: list[Any] | None = overrides.pop("drains", None)
    if isinstance(drains, str):
        # Accept the same "console,jsonl" shorthand ObserveSettings does, so
        # a caller filtering OBSERVE_DRAINS can pass the remainder verbatim.
        drains = list(ObserveSettings(drains=drains).drains)
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
