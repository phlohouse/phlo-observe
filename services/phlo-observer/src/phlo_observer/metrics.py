"""Prometheus metrics for the observer.

Metric names follow spec §45 exactly; labels stay low-cardinality.
"""

from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram

INGEST_EVENTS = Counter(
    "phlo_observer_ingest_events_total",
    "Events accepted/rejected at ingestion",
    ["producer", "status"],
)
INGEST_BATCHES = Counter(
    "phlo_observer_ingest_batches_total",
    "Ingestion batches by outcome",
    ["status"],
)
NORMALIZATION = Counter(
    "phlo_observer_normalization_total",
    "Normalization attempts",
    ["adapter", "status"],
)
NORMALIZATION_DURATION = Histogram(
    "phlo_observer_normalization_duration_seconds",
    "Normalization duration",
)
PERSIST_DURATION = Histogram(
    "phlo_observer_persist_duration_seconds",
    "Event persistence duration",
)
EVENTS_STORED = Counter(
    "phlo_observer_events_stored_total",
    "Normalized events persisted",
)
DUPLICATE_EVENTS = Counter(
    "phlo_observer_duplicate_events_total",
    "Duplicate event IDs received",
)
CORRELATION = Counter(
    "phlo_observer_correlation_total",
    "Correlation decisions by method",
    ["method"],
)
HTTP_REQUESTS = Counter(
    "phlo_observer_http_requests_total",
    "HTTP requests",
    ["route", "status"],
)
HTTP_DURATION = Histogram(
    "phlo_observer_http_request_duration_seconds",
    "HTTP request duration",
    ["route"],
)
EXPORT = Counter(
    "phlo_observer_export_total",
    "Forwarded events",
    ["destination", "status"],
)
QUEUE_DEPTH = Gauge(
    "phlo_observer_queue_depth",
    "Internal ingestion queue depth",
)
