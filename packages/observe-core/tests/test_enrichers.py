"""Enricher extension point tests."""

from __future__ import annotations

from observe_core import add_enricher, configure, event, flush, shutdown


class _TeamEnricher:
    def enrich(self, evt) -> None:
        evt.set(team="phlo")
        evt.set_correlation(pipeline="daily-elt")


class _BrokenEnricher:
    def enrich(self, evt) -> None:
        raise RuntimeError("enricher exploded")


def test_enricher_mutates_events():
    rt = configure(
        service_name="svc",
        drains=[{"type": "memory"}],
        spool_enabled=False,
        enrichers=[_TeamEnricher()],
    )
    try:
        drain = rt.drains[0]
        event("pipeline.step")
        flush(2.0)
        (ev,) = [e.data for e in drain.events]
        assert ev["attributes"]["team"] == "phlo"
        assert ev["correlation"]["pipeline"] == "daily-elt"
    finally:
        shutdown(2.0)


def test_broken_enricher_does_not_kill_emit():
    rt = configure(
        service_name="svc",
        drains=[{"type": "memory"}],
        spool_enabled=False,
        enrichers=[_BrokenEnricher()],
    )
    try:
        drain = rt.drains[0]
        event("pipeline.step")
        flush(2.0)
        assert len(drain.events) == 1
        assert rt.stats.snapshot()["worker_errors"] >= 1
    finally:
        shutdown(2.0)


def test_add_enricher_applies_to_future_runtime():
    add_enricher(_TeamEnricher())
    try:
        rt = configure(service_name="svc", drains=[{"type": "memory"}], spool_enabled=False)
        drain = rt.drains[0]
        event("pipeline.step")
        flush(2.0)
        assert drain.events[0].data["attributes"]["team"] == "phlo"
    finally:
        shutdown(2.0)
