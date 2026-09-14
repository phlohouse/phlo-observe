"""Drain implementations and the shared drain protocol."""

from observe_core.drains.base import CanonicalEvent, Drain, DrainFailure
from observe_core.drains.console import ConsoleDrain
from observe_core.drains.http import HttpDrain
from observe_core.drains.jsonl import JsonlDrain
from observe_core.drains.otlp import OtlpDrain

__all__ = [
    "CanonicalEvent",
    "ConsoleDrain",
    "Drain",
    "DrainFailure",
    "HttpDrain",
    "JsonlDrain",
    "OtlpDrain",
]
