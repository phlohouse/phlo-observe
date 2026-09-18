"""Drain tests: console, JSONL, memory. HTTP/OTLP get dedicated files."""

from __future__ import annotations

import io
import json
from pathlib import Path

import pytest
from observe_core import add_drain, configure, event, flush, shutdown
from observe_core.config import HttpDrainConfig
from observe_core.drains.base import CanonicalEvent
from observe_core.drains.console import ConsoleDrain
from observe_core.drains.jsonl import JsonlDrain
from observe_core.drains.memory import MemoryDrain
from observe_core.models import Delivery, EventEnvelope
from observe_core.serialization import dumps
from observe_core.timestamps import utcnow
from pydantic import ValidationError


def _env(**overrides) -> EventEnvelope:
    base = {
        "event_id": "01JTEST0000000000000000000",
        "event": "asset.materialize",
        "category": "data",
        "outcome": "success",
        "severity": "info",
        "delivery": "telemetry",
        "observed_at": utcnow(),
        "service": {"name": "svc"},
    }
    base.update(overrides)
    return EventEnvelope(**base)


def _canon(env: EventEnvelope) -> CanonicalEvent:
    data = env.to_canonical_dict()
    return CanonicalEvent(data=data, payload=dumps(data), delivery=env.delivery)


class TestConsoleDrain:
    def test_summary_line(self):
        stream = io.StringIO()
        ConsoleDrain(stream=stream, color="never").emit_batch([_canon(_env())])
        out = stream.getvalue()
        assert "asset.materialize" in out
        assert "success" in out

    def test_emit_raw_writes_json_lines(self):
        stream = io.StringIO()
        drain = ConsoleDrain(stream=stream, color="never")
        drain.emit_raw([_canon(_env()).payload])
        parsed = json.loads(stream.getvalue().strip())
        assert parsed["event"] == "asset.materialize"

    def test_error_details_in_summary(self):
        stream = io.StringIO()
        env = _env(
            outcome="failure",
            severity="error",
            error={
                "code": "NULL_VIOLATION",
                "message": "nulls found",
                "why": "col x",
                "fix": "fix it",
                "exception_type": "ObservedError",
                "retryable": False,
                "stacktrace": None,
                "details": {},
            },
        )
        ConsoleDrain(stream=stream, color="never").emit_batch([_canon(env)])
        out = stream.getvalue()
        assert "NULL_VIOLATION" in out
        assert "col x" in out


class TestJsonlDrain:
    def test_writes_valid_lines(self, tmp_path: Path):
        path = tmp_path / "events.jsonl"
        drain = JsonlDrain(path)
        drain.emit_batch([_canon(_env()), _canon(_env())])
        drain.close()
        lines = path.read_text().strip().split("\n")
        assert len(lines) == 2
        for line in lines:
            assert json.loads(line)["event"] == "asset.materialize"

    def test_size_rotation_and_retention(self, tmp_path: Path):
        path = tmp_path / "events.jsonl"
        drain = JsonlDrain(path, max_bytes=400, backup_count=2)
        for _ in range(10):
            drain.emit_batch([_canon(_env())])
        drain.close()
        rotated = sorted(tmp_path.glob("events.jsonl*"))
        assert len(rotated) > 1  # rotation occurred
        assert len(rotated) <= 3  # active + 2 backups
        assert (tmp_path / "events.jsonl.1").exists()

    def test_emit_raw(self, tmp_path: Path):
        path = tmp_path / "events.jsonl"
        drain = JsonlDrain(path)
        drain.emit_raw([_canon(_env()).payload])
        drain.close()
        assert json.loads(path.read_text().strip())["event"] == "asset.materialize"


class TestMemoryDrain:
    def test_captures_and_clear(self):
        drain = MemoryDrain()
        drain.emit_batch([_canon(_env(delivery="critical"))])
        assert len(drain.events) == 1
        assert drain.events[0].data["event"] == "asset.materialize"
        assert drain.events[0].delivery == Delivery.CRITICAL
        drain.emit_raw([b'{"a": 1}'])
        assert drain.raw_payloads == [b'{"a": 1}']
        drain.clear()
        assert drain.events == []
        assert drain.raw_payloads == []


class TestDrainConfigValidation:
    def test_http_drain_requires_endpoint(self):
        with pytest.raises(ValidationError):
            HttpDrainConfig(type="http")


class _RecordingDrain:
    """Minimal consumer-provided drain: records every batch it receives."""

    name = "recording"
    is_remote = False

    def __init__(self):
        self.batches: list[list[CanonicalEvent]] = []
        self.raw: list[bytes] = []
        self.closed = False

    def emit_batch(self, events):
        self.batches.append(list(events))

    def emit_raw(self, payloads):
        self.raw.extend(payloads)

    def flush(self):
        pass

    def close(self):
        self.closed = True


class TestCustomDrainRegistration:
    """The runtime's programmatic extension point for consumer drains."""

    def test_runtime_add_drain_receives_events(self):
        rt = configure(service_name="svc", drains=[{"type": "memory"}], spool_enabled=False)
        try:
            custom = _RecordingDrain()
            rt.add_drain(custom)
            event("pipeline.step")
            flush(2.0)
            assert [e.event for batch in custom.batches for e in batch] == ["pipeline.step"]
        finally:
            shutdown(2.0)

    def test_add_drain_applies_to_current_runtime(self):
        rt = configure(service_name="svc", drains=[{"type": "memory"}], spool_enabled=False)
        custom = _RecordingDrain()
        add_drain(custom)
        try:
            event("pipeline.step")
            flush(2.0)
            assert custom.batches
            assert rt.drains[-1] is custom
        finally:
            shutdown(2.0)

    def test_add_drain_before_configure_attaches_to_next_runtime(self):
        custom = _RecordingDrain()
        add_drain(custom)
        try:
            rt = configure(service_name="svc", drains=[{"type": "memory"}], spool_enabled=False)
            event("pipeline.step")
            flush(2.0)
            assert rt.drains[-1] is custom
            assert [e.event for batch in custom.batches for e in batch] == ["pipeline.step"]
        finally:
            shutdown(2.0)

    def test_registered_drain_flushed_and_closed_with_runtime(self):
        rt = configure(service_name="svc", drains=[{"type": "memory"}], spool_enabled=False)
        custom = _RecordingDrain()
        rt.add_drain(custom)
        shutdown(2.0)
        assert custom.closed

    def test_add_drain_rejects_non_drain(self):
        rt = configure(service_name="svc", drains=[{"type": "memory"}], spool_enabled=False)
        try:
            with pytest.raises(TypeError, match="Drain protocol"):
                rt.add_drain(object())
            with pytest.raises(TypeError, match="Drain protocol"):
                add_drain(object())
        finally:
            shutdown(2.0)
