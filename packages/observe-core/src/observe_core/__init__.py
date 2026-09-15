"""observe-core: generic wide-event observability for Python applications.

Quick start::

    from observe_core import configure, observe

    configure(service_name="my-app")

    with observe("demo.work", category="application") as evt:
        evt.set(rows=100)
"""

from observe_core.builder import EventBuilder
from observe_core.config import (
    ConsoleDrainConfig,
    HttpDrainConfig,
    JsonlDrainConfig,
    ObserveSettings,
    OtlpDrainConfig,
)
from observe_core.context import (
    BoundContext,
    bind_context,
    bind_context_token,
    clear_context,
    get_context,
    propagate,
)
from observe_core.emit import event
from observe_core.enrich import Enricher, MutableEvent
from observe_core.errors import ObservedError
from observe_core.models import (
    Category,
    Correlation,
    Delivery,
    ErrorInfo,
    EventEnvelope,
    Outcome,
    ServiceInfo,
    Severity,
    SourceInfo,
)
from observe_core.operation import observe
from observe_core.redaction import REDACTED, sanitize_url
from observe_core.runtime import (
    TelemetryError,
    add_enricher,
    configure,
    flush,
    get_runtime,
    get_stats,
    shutdown,
)
from observe_core.timestamps import format_rfc3339, utcnow

__version__ = "0.0.0"

__all__ = [
    "REDACTED",
    "BoundContext",
    "Category",
    "ConsoleDrainConfig",
    "Correlation",
    "Delivery",
    "Enricher",
    "ErrorInfo",
    "EventBuilder",
    "EventEnvelope",
    "HttpDrainConfig",
    "JsonlDrainConfig",
    "MutableEvent",
    "ObserveSettings",
    "ObservedError",
    "OtlpDrainConfig",
    "Outcome",
    "ServiceInfo",
    "Severity",
    "SourceInfo",
    "TelemetryError",
    "add_enricher",
    "bind_context",
    "bind_context_token",
    "clear_context",
    "configure",
    "event",
    "flush",
    "format_rfc3339",
    "get_context",
    "get_runtime",
    "get_stats",
    "observe",
    "propagate",
    "sanitize_url",
    "shutdown",
    "utcnow",
]
