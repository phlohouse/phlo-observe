"""Adapter registry: producer name -> adapter instance."""

from __future__ import annotations

from phlo_observer.adapters.base import (
    AdapterError,
    NormalizedBatch,
    RawPayload,
    SourceAdapter,
    envelope_for,
)
from phlo_observer.adapters.canonical import CanonicalAdapter
from phlo_observer.adapters.dagster import DagsterAdapter
from phlo_observer.adapters.dbt import DbtAdapter
from phlo_observer.adapters.generic import GenericAdapter

ADAPTERS: dict[str, SourceAdapter] = {
    "canonical": CanonicalAdapter(),
    "dagster": DagsterAdapter(),
    "dbt": DbtAdapter(),
    "generic": GenericAdapter(),
}
"""Adapters keyed by the ``/v1/ingest/{name}`` route segment."""

__all__ = [
    "ADAPTERS",
    "AdapterError",
    "CanonicalAdapter",
    "DagsterAdapter",
    "DbtAdapter",
    "GenericAdapter",
    "NormalizedBatch",
    "RawPayload",
    "SourceAdapter",
    "envelope_for",
]
