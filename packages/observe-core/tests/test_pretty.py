"""Golden tests for the generic pretty renderer.

All fixtures use synthetic event names (``job.*``, ``record.*``,
``validation.*``) — observe-core must stay application-neutral, so the tests
deliberately avoid any real producer vocabulary.
"""

from __future__ import annotations

import io
import json

import pytest
from observe_core import ContextField, EventPresentation, Field, PrettyRenderer
from observe_core.drains.base import CanonicalEvent
from observe_core.models import Delivery
from observe_core.pretty import (
    format_bytes,
    format_duration_ms,
    format_float,
    format_identifier,
    format_integer,
    format_percent,
    format_timestamp,
)

RULES = {
    "job.started": EventPresentation(
        label="Job started",
        fields=[Field("attributes.worker", label="Worker")],
    ),
    "record.processed": EventPresentation(
        label="Load",
        fields=[
            Field("attributes.table"),
            Field("attributes.rows", label="Rows", format="integer"),
            Field("attributes.bytes", label="Size", format="bytes", visibility="secondary"),
        ],
    ),
    "validation.completed": EventPresentation(label="Validate", visibility="secondary"),
    "internal.tick": EventPresentation(visibility="hidden"),
}
CONTEXT = [
    ContextField("correlation.run_id", label="Run", format="identifier", head=21),
    ContextField("attributes.region", label="Region"),
]


def renderer(**kwargs) -> PrettyRenderer:
    kwargs.setdefault("color", "never")
    kwargs.setdefault("symbols", "unicode")
    kwargs.setdefault("stream", io.StringIO())
    kwargs.setdefault("rules", RULES)
    kwargs.setdefault("context", CONTEXT)
    return PrettyRenderer(**kwargs)


def ev(name, outcome="success", severity="info", **kw):
    d = {"event": name, "outcome": outcome, "severity": severity}
    d.update(kw)
    return d


# -- status vocabulary ---------------------------------------------------------


def test_success_glyph() -> None:
    out = renderer().render(ev("job.started", attributes={"worker": "w-3"}))
    assert out == "✓ Job started  Worker: w-3"


def test_failure_glyph_and_outcome_word() -> None:
    out = renderer().render(ev("job.failed", outcome="failure", severity="error"))
    assert out.startswith("✕ job.failed  failure")


def test_warning_via_severity() -> None:
    """A successful outcome carrying warn severity renders the warning glyph."""
    out = renderer().render(ev("job.started", severity="warn", attributes={"worker": "w"}))
    assert out.startswith("! Job started")


def test_running_glyph_for_inflight() -> None:
    out = renderer().render(
        ev(
            "job.started",
            outcome="unknown",
            started_at="2026-09-17T21:00:00Z",
            attributes={"worker": "w"},
        )
    )
    assert out == "→ Job started  Worker: w  running"


def test_info_glyph_for_instantaneous_unknown() -> None:
    out = renderer().render(ev("job.started", outcome="unknown"))
    assert out.startswith("• Job started")


# -- structured errors -----------------------------------------------------------


def test_structured_error_block() -> None:
    out = renderer().render(
        ev(
            "job.failed",
            outcome="failure",
            severity="error",
            error={
                "message": "Validation failed",
                "why": "83 records were invalid",
                "fix": "Correct the source data",
            },
        )
    )
    assert out == (
        "✕ job.failed  failure\n"
        "    Validation failed\n"
        "    83 records were invalid\n"
        "    Fix: Correct the source data"
    )


def test_verbose_adds_error_metadata() -> None:
    out = renderer(mode="verbose").render(
        ev(
            "job.failed",
            outcome="failure",
            severity="error",
            error={
                "message": "boom",
                "code": "SCHEMA_MISMATCH",
                "exception_type": "ValueError",
                "retryable": True,
            },
        )
    )
    assert "    boom" in out
    assert "code: SCHEMA_MISMATCH" in out
    assert "exception: ValueError" in out
    assert "retryable: yes" in out


# -- formatting -----------------------------------------------------------------


def test_format_duration() -> None:
    assert format_duration_ms(723) == "723ms"
    assert format_duration_ms(8466.257) == "8.47s"
    assert format_duration_ms(90500) == "1m 30s"
    assert format_duration_ms(3600000) == "1h"


def test_format_numeric() -> None:
    assert format_integer(12481) == "12,481"
    assert format_integer(0) == "0"
    assert format_float(8466.257) == "8,466.257"
    assert format_float(2.5) == "2.5"


def test_format_bytes() -> None:
    assert format_bytes(512) == "512 B"
    assert format_bytes(19608371) == "18.7 MB"
    assert format_bytes(2048) == "2.0 KB"


def test_format_bytes_rounded_boundary_advances_unit() -> None:
    """A value that would round up to 1024 advances to the next unit instead."""
    assert format_bytes(1023) == "1023 B"
    assert format_bytes(1023.6) == "1.0 KB"
    assert format_bytes(1048575) == "1.0 MB"
    assert format_bytes(1048576) == "1.0 MB"
    assert format_bytes(1073741823) == "1.0 GB"


def test_format_percent_and_timestamp() -> None:
    assert format_percent(0.942) == "94.2%"
    assert format_timestamp("2026-09-17T21:51:23+00:00") == "21:51:23"


def test_identifier_shortening() -> None:
    ident = "pipeline-run-2f6b1f8dfa254fb0b7132656cb9d8fc9"
    assert format_identifier(ident, head=21) == "pipeline-run-2f6b1f8d…"
    assert format_identifier(ident, head=21, ellipsis="...") == "pipeline-run-2f6b1f8d..."
    assert format_identifier("short-id", head=21) == "short-id"


def test_duration_appended_when_not_configured() -> None:
    out = renderer().render(ev("job.started", duration_ms=8466.26, attributes={"worker": "w"}))
    assert out == "✓ Job started  Worker: w  8.47s"


def test_configured_fields_render_labels() -> None:
    out = renderer().render(
        ev("record.processed", attributes={"table": "raw.stores", "rows": 12481}, duration_ms=723)
    )
    assert out == "✓ Load  table: raw.stores  Rows: 12,481  723ms"


# -- visibility ------------------------------------------------------------------


def test_secondary_hidden_in_pretty() -> None:
    out = renderer().render(ev("validation.completed"))
    assert out == ""


def test_secondary_shown_in_verbose_indented() -> None:
    out = renderer(mode="verbose").render(ev("validation.completed"))
    assert out == "  ✓ Validate"


def test_hidden_never_renders() -> None:
    assert renderer(mode="verbose").render(ev("internal.tick")) == ""


def test_failure_escalates_hidden_to_primary() -> None:
    out = renderer().render(ev("internal.tick", outcome="failure", severity="error"))
    assert out.startswith("✕ internal.tick")


def test_warn_escalates_hidden_to_secondary() -> None:
    assert renderer().render(ev("internal.tick", severity="warn")) == ""
    out = renderer(mode="verbose").render(ev("internal.tick", severity="warn"))
    assert out == "  ! internal.tick"


def test_secondary_fields_only_in_verbose() -> None:
    event = ev("record.processed", attributes={"table": "t", "rows": 1, "bytes": 2048})
    assert "Size" not in renderer().render(event)
    assert "Size: 2.0 KB" in renderer(mode="verbose").render(event)


# -- fallback ---------------------------------------------------------------------


def test_unknown_event_fallback() -> None:
    out = renderer().render(ev("some.unknown.event", duration_ms=842))
    assert out == "✓ some.unknown.event  success  842ms"


def test_missing_configured_fields_silently_skipped() -> None:
    out = renderer().render(ev("record.processed", attributes={"table": "t"}))
    assert out == "✓ Load  table: t"


def test_none_field_values_omitted() -> None:
    """``None`` is canonically absent — it must not render as "null"."""
    out = renderer().render(ev("record.processed", attributes={"table": "t", "rows": None}))
    assert "Rows" not in out
    assert "null" not in out
    assert out == "✓ Load  table: t"


def test_envelope_none_fields_render_nothing() -> None:
    """Fixed envelope keys arrive present-as-None in canonical dicts."""
    from observe_core import EventEnvelope, ServiceInfo

    rules = {
        "job.done": EventPresentation(
            label="Job",
            fields=[
                Field("service.version", label="Version"),
                Field("duration_ms", format="duration"),
            ],
        )
    }
    r = PrettyRenderer(rules=rules, color="never", stream=io.StringIO())
    env = EventEnvelope(
        event_id="e1",
        event="job.done",
        outcome="success",
        observed_at="2026-09-17T21:51:23+00:00",
        service=ServiceInfo(name="svc"),
    )
    assert r.render(env) == "✓ Job"


# -- timestamps --------------------------------------------------------------------


def test_timestamps_off_by_default() -> None:
    out = renderer().render(
        ev("job.started", observed_at="2026-09-17T21:51:23.456Z", attributes={"worker": "w"})
    )
    assert out == "✓ Job started  Worker: w"


def test_timestamp_column_from_observed_at() -> None:
    out = renderer(timestamps=True).render(
        ev("job.started", observed_at="2026-09-17T21:51:23.456Z", attributes={"worker": "w"})
    )
    assert out == "21:51:23.456 ✓ Job started  Worker: w"


def test_timestamp_column_falls_back_to_started_then_ended() -> None:
    r = renderer(timestamps=True)
    started_only = r.render(
        ev("job.started", started_at="2026-09-17T21:00:00.100Z", attributes={"worker": "w"})
    )
    ended_only = r.render(ev("job.started", ended_at="2026-09-17T22:00:00.900Z"))
    assert started_only.startswith("21:00:00.100 ✓")
    assert ended_only.startswith("22:00:00.900 ✓")


def test_timestamp_column_observed_at_wins_over_started_at() -> None:
    out = renderer(timestamps=True).render(
        ev(
            "job.started",
            observed_at="2026-09-17T21:51:23.000Z",
            started_at="2026-09-17T21:00:00.000Z",
        )
    )
    assert out.startswith("21:51:23.000 ✓")


def test_timestamp_column_absent_when_event_has_no_time() -> None:
    out = renderer(timestamps=True).render(ev("job.started", attributes={"worker": "w"}))
    assert out == "✓ Job started  Worker: w"


def test_timestamp_column_unparseable_value_renders_no_column() -> None:
    out = renderer(timestamps=True).render(ev("job.started", observed_at="not-a-time"))
    assert out == "✓ Job started"


def test_timestamp_column_applies_to_custom_formatter_lines() -> None:
    rules = {
        "job.started": EventPresentation(
            label="Job", formatter=lambda data: ["custom body", "more"]
        )
    }
    out = renderer(rules=rules, timestamps=True).render(
        ev("job.started", observed_at="2026-09-17T21:51:23.001Z")
    )
    assert out.splitlines()[0] == "21:51:23.001 ✓ Job  custom body"


def test_timestamps_rejects_non_bool() -> None:
    with pytest.raises(TypeError):
        renderer(timestamps="yes")


# -- context -----------------------------------------------------------------------


def test_context_header_and_suppression() -> None:
    events = [
        ev("job.started", correlation={"run_id": "run-aaa"}, attributes={"worker": "w"}),
        ev("job.started", correlation={"run_id": "run-aaa"}, attributes={"worker": "w2"}),
    ]
    out = renderer().render_many(events)
    assert out.count("Run: run-aaa") == 1
    assert out.splitlines()[0] == "── Run: run-aaa"


def test_context_header_repeats_on_change() -> None:
    events = [
        ev("job.started", correlation={"run_id": "run-aaa"}),
        ev("job.started", correlation={"run_id": "run-bbb"}),
    ]
    out = renderer().render_many(events)
    lines = out.splitlines()
    assert "── Run: run-aaa" in lines[0]
    assert "── Run: run-bbb" in " ".join(lines[1:])


def test_context_missing_field_omitted() -> None:
    out = renderer().render_many([ev("job.started", correlation={"run_id": "run-aaa"})])
    assert "Region" not in out.splitlines()[0]


def test_no_context_configured_no_header() -> None:
    r = PrettyRenderer(color="never", stream=io.StringIO())
    out = r.render_many([ev("a.b"), ev("a.b")])
    assert "──" not in out


def test_envelope_without_context_values_emits_no_header() -> None:
    """A real envelope carries absent correlation keys as ``None``."""
    from observe_core import EventEnvelope, ServiceInfo

    env = EventEnvelope(
        event_id="e1",
        event="job.started",
        outcome="success",
        observed_at="2026-09-17T21:51:23+00:00",
        service=ServiceInfo(name="svc"),
        attributes={"worker": "w-9"},
    )
    out = renderer().render_many([env])
    assert "null" not in out
    assert "──" not in out
    assert out == "✓ Job started  Worker: w-9"


def test_context_grouping_none_equals_missing() -> None:
    """A dict without the key and an envelope carrying ``None`` share a group."""
    from observe_core import EventEnvelope, ServiceInfo

    env = EventEnvelope(
        event_id="e1",
        event="job.started",
        outcome="success",
        observed_at="2026-09-17T21:51:23+00:00",
        service=ServiceInfo(name="svc"),
        attributes={"worker": "w-1"},
    )
    d = ev("job.started", attributes={"worker": "w-2"})
    out = renderer().render_many([env, d])
    assert out == "✓ Job started  Worker: w-1\n✓ Job started  Worker: w-2"


def test_contextless_event_does_not_join_previous_group() -> None:
    """An event with no context values must not render under a run header."""
    events = [
        ev("job.started", correlation={"run_id": "r1"}),
        ev("some.other"),
        ev("job.started", correlation={"run_id": "r2"}),
    ]
    assert renderer().render_many(events) == (
        "── Run: r1\n✓ Job started\n\n✓ some.other  success\n\n── Run: r2\n✓ Job started"
    )


def test_context_reemits_header_after_ungrouped_event() -> None:
    """The headerless group is a real group: returning to a context repeats its header."""
    events = [
        ev("job.started", correlation={"run_id": "r1"}),
        ev("some.other"),
        ev("job.started", correlation={"run_id": "r1"}),
    ]
    out = renderer().render_many(events)
    assert out.count("── Run: r1") == 2


def test_ungrouped_events_before_first_header() -> None:
    events = [ev("some.other"), ev("job.started", correlation={"run_id": "r1"})]
    assert renderer().render_many(events) == "✓ some.other  success\n\n── Run: r1\n✓ Job started"


def test_hidden_events_do_not_emit_context_headers() -> None:
    """Suppressed events must not emit a group header or switch context."""
    events = [
        ev("job.started", correlation={"run_id": "run-a"}, attributes={"worker": "w"}),
        ev("internal.tick", correlation={"run_id": "run-b"}),
    ]
    out = renderer().render_many(events)
    assert "run-b" not in out
    assert out.splitlines() == ["── Run: run-a", "✓ Job started  Worker: w"]


def test_suppressed_secondary_events_do_not_emit_headers() -> None:
    events = [
        ev("job.started", correlation={"run_id": "run-a"}),
        ev("validation.completed", correlation={"run_id": "run-b"}),
    ]
    out = renderer().render_many(events)
    assert "run-b" not in out
    # ...but in verbose mode the same event renders and does switch the group.
    out = renderer(mode="verbose").render_many(events)
    assert "run-b" in out


def test_all_hidden_events_render_nothing() -> None:
    out = renderer().render_many([ev("internal.tick", correlation={"run_id": "run-b"})])
    assert out == ""


def test_secondary_context_field_verbose_only() -> None:
    """Secondary context joins the group and its header only in verbose mode."""
    ctx = [
        ContextField("correlation.run_id", label="Run"),
        ContextField("attributes.region", label="Region", visibility="secondary"),
    ]
    events = [
        ev(
            "job.started",
            correlation={"run_id": "r1"},
            attributes={"worker": "w", "region": "us"},
        ),
        ev(
            "job.started",
            correlation={"run_id": "r1"},
            attributes={"worker": "w2", "region": "eu"},
        ),
    ]
    # Pretty: region is invisible, so it neither renders nor splits the group.
    pretty = PrettyRenderer(rules=RULES, context=ctx, color="never", stream=io.StringIO())
    assert pretty.render_many(events) == (
        "── Run: r1\n✓ Job started  Worker: w\n✓ Job started  Worker: w2"
    )
    # Verbose: region joins the group key — the differing value splits it.
    verbose = PrettyRenderer(
        rules=RULES, context=ctx, mode="verbose", color="never", stream=io.StringIO()
    )
    assert verbose.render_many(events) == (
        "── Run: r1   Region: us\n"
        "✓ Job started  Worker: w\n"
        "\n"
        "── Run: r1   Region: eu\n"
        "✓ Job started  Worker: w2"
    )


def test_hidden_context_field_ignored() -> None:
    """Hidden context never renders and never splits groups."""
    ctx = [
        ContextField("correlation.run_id", label="Run"),
        ContextField("attributes.token", label="Token", visibility="hidden"),
    ]
    events = [
        ev(
            "job.started",
            correlation={"run_id": "r1"},
            attributes={"worker": "w", "token": "aaa"},
        ),
        ev(
            "job.started",
            correlation={"run_id": "r1"},
            attributes={"worker": "w2", "token": "bbb"},
        ),
    ]
    r = PrettyRenderer(
        rules=RULES, context=ctx, mode="verbose", color="never", stream=io.StringIO()
    )
    assert r.render_many(events) == (
        "── Run: r1\n✓ Job started  Worker: w\n✓ Job started  Worker: w2"
    )


# -- terminal behaviour -------------------------------------------------------------


def test_ascii_fallback() -> None:
    r = renderer(symbols="ascii")
    assert r.render(ev("job.started", attributes={"worker": "w"})) == "v Job started  Worker: w"
    assert r.render(ev("x.y", outcome="failure", severity="error")) == "x x.y  failure"


def test_ascii_ellipsis() -> None:
    r = renderer(symbols="ascii")
    out = r.render_many(
        [
            ev(
                "job.started",
                correlation={"run_id": "pipeline-run-2f6b1f8dfa254fb0b7132656cb9d8fc9"},
            )
        ]
    )
    assert "pipeline-run-2f6b1f8d..." in out


def test_no_color_env(monkeypatch) -> None:
    monkeypatch.setenv("NO_COLOR", "1")
    stream = io.StringIO()
    monkeypatch.setattr(stream, "isatty", lambda: True, raising=False)
    r = PrettyRenderer(color="auto", stream=stream)
    assert not r._color


def test_color_on_tty(monkeypatch) -> None:
    stream = io.StringIO()
    monkeypatch.setattr(stream, "isatty", lambda: True, raising=False)
    monkeypatch.delenv("NO_COLOR", raising=False)
    monkeypatch.delenv("CI", raising=False)
    r = PrettyRenderer(color="auto", stream=stream)
    assert r._color
    out = r.render(ev("job.started"))
    assert "\x1b[32m✓\x1b[0m" in out


def test_non_tty_no_color() -> None:
    r = PrettyRenderer(color="auto", stream=io.StringIO())
    assert not r._color


def test_ascii_encoding_stream_falls_back() -> None:
    class AsciiStream(io.StringIO):
        encoding = "ascii"

    r = PrettyRenderer(symbols="auto", color="never", stream=AsciiStream())
    assert not r._unicode
    assert r.render(ev("job.started")).startswith("v")


def test_unicode_symbols() -> None:
    r = renderer(symbols="unicode")
    assert "✓" in r.render(ev("job.started"))


# -- custom formatter ----------------------------------------------------------------


def test_custom_formatter_escape_hatch() -> None:
    rules = {
        "job.summary": EventPresentation(
            label="Summary",
            formatter=lambda e: [f"processed {e['attributes']['n']} things", "extra detail"],
        )
    }
    r = PrettyRenderer(rules=rules, color="never", stream=io.StringIO())
    out = r.render(ev("job.summary", attributes={"n": 5}))
    assert out == "✓ Summary  processed 5 things\n    extra detail"


def test_custom_formatter_secondary_indent() -> None:
    """Secondary continuation lines align with secondary error detail."""
    rules = {
        "job.summary": EventPresentation(
            label="Summary",
            visibility="secondary",
            formatter=lambda e: ["first line", "second line"],
        )
    }
    r = PrettyRenderer(rules=rules, mode="verbose", color="never", stream=io.StringIO())
    out = r.render(ev("job.summary"))
    assert out == "  ✓ Summary  first line\n      second line"


def test_custom_formatter_none_falls_back() -> None:
    rules = {
        "job.started": EventPresentation(
            label="Job", fields=[Field("attributes.worker")], formatter=lambda e: None
        )
    }
    r = PrettyRenderer(rules=rules, color="never", stream=io.StringIO())
    out = r.render(ev("job.started", attributes={"worker": "w-1"}))
    assert out == "✓ Job  worker: w-1"


# -- malformed input -----------------------------------------------------------------


def test_malformed_events_render_safely() -> None:
    r = renderer()
    assert "unrenderable" in r.render(None)
    assert "unrenderable" in r.render(42)
    assert "unrenderable" in r.render("nonsense")


def test_invalid_presentation_config_rejected() -> None:
    """Misconfiguration fails at construction, not midway through a render."""
    with pytest.raises(TypeError, match="EventPresentation"):
        PrettyRenderer(rules={"a.b": {"label": "X"}}, color="never", stream=io.StringIO())
    with pytest.raises(TypeError, match="EventPresentation"):
        PrettyRenderer(rules={1: EventPresentation()}, color="never", stream=io.StringIO())
    with pytest.raises(TypeError, match="ContextField"):
        PrettyRenderer(context=["correlation.run_id"], color="never", stream=io.StringIO())
    with pytest.raises(TypeError, match="Field"):
        EventPresentation(fields=["attributes.x"])
    with pytest.raises(TypeError, match="callable"):
        EventPresentation(formatter="not a function")
    with pytest.raises(TypeError, match="path"):
        Field(42)
    with pytest.raises(TypeError, match="head"):
        Field("a.b", head="x")
    with pytest.raises(TypeError, match="label"):
        EventPresentation(label=42)
    with pytest.raises(TypeError, match="label"):
        Field("a.b", label=42)
    with pytest.raises(TypeError, match="label"):
        ContextField("a.b", label=42)


def test_partial_event_dict() -> None:
    r = renderer()
    out = r.render({"event": "a.b"})
    assert "a.b" in out


def test_envelope_and_canonical_event_inputs() -> None:
    from observe_core import EventEnvelope, ServiceInfo

    env = EventEnvelope(
        event_id="e1",
        event="job.started",
        outcome="success",
        observed_at="2026-09-17T21:51:23+00:00",
        service=ServiceInfo(name="svc"),
        attributes={"worker": "w-9"},
    )
    r = renderer()
    assert r.render(env) == "✓ Job started  Worker: w-9"
    ce = CanonicalEvent(data=env.to_canonical_dict(), payload=b"{}", delivery=Delivery.TELEMETRY)
    assert r.render(ce) == "✓ Job started  Worker: w-9"


def test_write_many_uses_stream() -> None:
    stream = io.StringIO()
    r = PrettyRenderer(color="never", stream=stream)
    r.write_many([ev("a.b"), ev("c.d")])
    assert stream.getvalue() == "✓ a.b  success\n✓ c.d  success\n"


def test_write_event_tracks_context_across_calls() -> None:
    """Incremental writes share group state: one header per context switch,
    matching what render_many produces for the same sequence."""
    stream = io.StringIO()
    r = PrettyRenderer(
        rules=RULES, context=CONTEXT, color="never", symbols="unicode", stream=stream
    )
    events = [
        ev("job.started", correlation={"run_id": "r1"}),
        ev("job.started", correlation={"run_id": "r1"}),
        ev("job.started", correlation={"run_id": "r2"}),
    ]
    for event in events:
        r.write_event(event)
    assert stream.getvalue() == (
        "── Run: r1\n✓ Job started\n✓ Job started\n\n── Run: r2\n✓ Job started\n"
    )
    assert stream.getvalue() == r.render_many(events) + "\n"


def test_write_event_suppressed_events_do_not_emit_headers() -> None:
    stream = io.StringIO()
    r = PrettyRenderer(
        rules=RULES, context=CONTEXT, color="never", symbols="unicode", stream=stream
    )
    r.write_event(ev("job.started", correlation={"run_id": "run-a"}, attributes={"worker": "w"}))
    r.write_event(ev("internal.tick", correlation={"run_id": "run-b"}))
    r.write_event(ev("job.started", correlation={"run_id": "run-a"}))
    assert stream.getvalue() == "── Run: run-a\n✓ Job started  Worker: w\n✓ Job started\n"


def test_write_event_unrenderable_does_not_break_grouping() -> None:
    stream = io.StringIO()
    r = PrettyRenderer(
        rules=RULES, context=CONTEXT, color="never", symbols="unicode", stream=stream
    )
    r.write_event(ev("job.started", correlation={"run_id": "r1"}))
    r.write_event(None)
    r.write_event(ev("job.started", correlation={"run_id": "r1"}))
    assert stream.getvalue() == (
        "── Run: r1\n✓ Job started\n• <unrenderable event>\n✓ Job started\n"
    )


def test_write_event_no_context_configured() -> None:
    stream = io.StringIO()
    r = PrettyRenderer(color="never", stream=stream)
    r.write_event(ev("a.b"))
    r.write_event(ev("c.d"))
    assert stream.getvalue() == "✓ a.b  success\n✓ c.d  success\n"


def test_write_event_verbose_renders_secondary_incrementally() -> None:
    """Secondary events suppressed in pretty mode render on the incremental
    path in verbose mode — visibility applies identically to write_event."""
    stream = io.StringIO()
    r = PrettyRenderer(rules=RULES, context=CONTEXT, mode="verbose", color="never", stream=stream)
    r.write_event(ev("job.started", correlation={"run_id": "r1"}))
    r.write_event(ev("validation.completed", correlation={"run_id": "r1"}))
    assert stream.getvalue() == "── Run: r1\n✓ Job started\n  ✓ Validate\n"


def test_write_event_applies_timestamps() -> None:
    stream = io.StringIO()
    r = PrettyRenderer(color="never", timestamps=True, stream=stream)
    r.write_event(ev("a.b", observed_at="2026-09-17T21:51:23.456Z"))
    r.write_event(ev("c.d"))  # no usable time: no column, same as render()
    assert stream.getvalue() == "21:51:23.456 ✓ a.b  success\n✓ c.d  success\n"


# -- serialization unchanged --------------------------------------------------------


def test_json_serialization_unchanged() -> None:
    """Rendering is additive: canonical JSON is never touched."""
    from observe_core import EventEnvelope, ServiceInfo

    env = EventEnvelope(
        event_id="e1",
        event="job.started",
        outcome="success",
        observed_at="2026-09-17T21:51:23+00:00",
        service=ServiceInfo(name="svc"),
        attributes={"worker": "w-9"},
    )
    before = env.to_json_bytes()
    renderer().render(env)
    assert env.to_json_bytes() == before
    assert json.loads(before)["event"] == "job.started"


# -- robustness -------------------------------------------------------------------


def test_nonfinite_durations_render_safely() -> None:
    """NaN/inf durations are malformed values, not renderer crashes."""
    assert format_duration_ms(float("nan")) == "nan"
    assert format_duration_ms(float("inf")) == "inf"
    assert format_duration_ms(float("-inf")) == "-inf"
    out = renderer().render(ev("job.started", duration_ms=float("nan"), attributes={"worker": "w"}))
    assert out == "✓ Job started  Worker: w  nan"


def test_nonfinite_numeric_formats() -> None:
    assert format_bytes(float("inf")) == "inf"
    assert format_bytes(float("nan")) == "nan"
    assert format_integer(float("inf")) == "inf"
    assert format_integer(float("nan")) == "nan"
    assert format_percent(float("nan")) == "nan"


def test_duration_boundary_rounding() -> None:
    """Rounding must not emit "1000ms" or "60.0s"."""
    assert format_duration_ms(999.4) == "999ms"
    assert format_duration_ms(999.5) == "1s"
    assert format_duration_ms(59949) == "59.9s"
    assert format_duration_ms(59999) == "1m"


def test_oversized_numeric_formats_render_safely() -> None:
    """``float()`` raises ``OverflowError`` on unrepresentable inputs — that
    must degrade to ``str(value)``, never escape ``render()``."""
    from fractions import Fraction

    huge = 10**400
    for value in (huge, Fraction(10**400, 3)):
        assert format_float(value) == str(value)
        assert format_duration_ms(value) == str(value)
        assert format_bytes(value) == str(value)
        assert format_percent(value) == str(value)
        assert isinstance(format_integer(value), str)
    assert format_integer(huge).startswith("10,000")
    out = renderer().render(ev("job.started", duration_ms=huge, attributes={"worker": "w"}))
    assert out.endswith(str(huge))
    r = PrettyRenderer(
        rules={
            "job.started": EventPresentation(
                label="Job", fields=[Field("attributes.n", format="bytes")]
            )
        },
        color="never",
        stream=io.StringIO(),
    )
    assert r.render(ev("job.started", attributes={"n": huge})).endswith(str(huge))


# -- terminal safety -----------------------------------------------------------------


def test_control_characters_escaped() -> None:
    """ANSI/control characters in event data must never reach the terminal raw."""
    out = renderer().render(
        ev("job.started", attributes={"worker": "w-\x1b[31mevil\x1b[0m\nline2"})
    )
    assert "\x1b" not in out
    assert "\n" not in out
    assert "w-\\x1b[31mevil\\x1b[0m\\nline2" in out


def test_error_text_sanitized() -> None:
    out = renderer().render(
        ev(
            "x.y",
            outcome="failure",
            error={"message": "bad\x1b[31m inject\nnext", "fix": "check\tthe\tdata"},
        )
    )
    assert "\x1b" not in out
    # The embedded newline is escaped, not turned into an extra output line.
    assert "bad\\x1b[31m inject\\nnext" in out
    assert "Fix: check\\tthe\\tdata" in out
    assert out.count("\n") == 2


def test_event_name_sanitized() -> None:
    out = renderer().render({"event": "bad.\x1bname"})
    assert "\x1b" not in out
    assert "bad.\\x1bname" in out


def test_surrogate_and_line_separators_escaped() -> None:
    out = renderer().render(ev("job.started", attributes={"worker": "w\ud800x\u2028y"}))
    assert "\\xd800" in out
    assert "\\x2028" in out
    out.encode("utf-8")  # must not raise


def test_context_value_sanitized() -> None:
    out = renderer().render_many([ev("job.started", correlation={"run_id": "r\x1bbad\nid"})])
    assert "\x1b" not in out
    assert out.count("\n") == 1  # header line + event line only
    assert "r\\x1bbad\\nid" in out


def test_unicode_text_passes_through() -> None:
    """Sanitization must not mangle legitimate non-ASCII text."""
    out = renderer().render(ev("job.started", attributes={"worker": "héllo wörld — 日本語"}))
    assert "héllo wörld — 日本語" in out


def test_formatter_output_sanitized() -> None:
    rules = {
        "a.b": EventPresentation(label="S", formatter=lambda e: "one\ttwo\x1b[7m"),
    }
    r = PrettyRenderer(rules=rules, color="never", stream=io.StringIO())
    out = r.render(ev("a.b"))
    assert "\x1b" not in out
    assert "one\\ttwo\\x1b[7m" in out


def test_formatter_invalid_return_type_raises() -> None:
    """A formatter must return str / sequence of lines / None — fail clearly."""
    rules = {"a.b": EventPresentation(formatter=lambda e: 42)}
    r = PrettyRenderer(rules=rules, color="never", stream=io.StringIO())
    with pytest.raises(TypeError, match="expected str"):
        r.render(ev("a.b"))
    rules = {"a.b": EventPresentation(formatter=lambda e: b"bytes")}
    r = PrettyRenderer(rules=rules, color="never", stream=io.StringIO())
    with pytest.raises(TypeError, match="expected str"):
        r.render(ev("a.b"))


def test_formatter_mapping_return_raises() -> None:
    """A Mapping is iterable but is not a sequence of lines — fail clearly."""
    rules = {"a.b": EventPresentation(formatter=lambda e: {"x": 1, "y": 2})}
    r = PrettyRenderer(rules=rules, color="never", stream=io.StringIO())
    with pytest.raises(TypeError, match="expected str"):
        r.render(ev("a.b"))


@pytest.mark.parametrize("content", ["", [], ()])
def test_formatter_empty_return_renders_status_line(content) -> None:
    """Empty formatter output yields the bare status line — the renderer owns
    visibility, so an event is never silently suppressed by its callback."""
    rules = {"a.b": EventPresentation(label="Done", formatter=lambda e: content)}
    r = PrettyRenderer(rules=rules, color="never", stream=io.StringIO())
    assert r.render(ev("a.b")) == "✓ Done"


def test_ci_env_variants(monkeypatch) -> None:
    stream = io.StringIO()
    monkeypatch.setattr(stream, "isatty", lambda: True, raising=False)
    monkeypatch.delenv("NO_COLOR", raising=False)
    monkeypatch.setenv("CI", "true")
    assert not PrettyRenderer(color="auto", stream=stream)._color
    # An explicit false value must not be mistaken for a CI environment.
    monkeypatch.setenv("CI", "false")
    assert PrettyRenderer(color="auto", stream=stream)._color
    monkeypatch.setenv("CI", "")
    assert PrettyRenderer(color="auto", stream=stream)._color


def test_secondary_line_fully_dimmed_with_color() -> None:
    """The glyph's colour reset must not cancel the dim span mid-line."""
    r = PrettyRenderer(
        rules=RULES, mode="verbose", color="always", symbols="unicode", stream=io.StringIO()
    )
    out = r.render(ev("validation.completed"))
    assert out == "\x1b[2m  \x1b[32m✓\x1b[2m Validate\x1b[0m"


def test_verbose_error_stacktrace_and_details() -> None:
    out = renderer(mode="verbose").render(
        ev(
            "job.failed",
            outcome="failure",
            severity="error",
            error={
                "message": "boom",
                "stacktrace": 'Traceback (most recent call last):\n  File "x.py", line 1',
                "details": {"attempt": 3, "host": "w-1"},
            },
        )
    )
    assert "    Traceback (most recent call last):" in out
    # The frame's own indent is preserved under the error indent.
    assert '      File "x.py", line 1' in out
    assert "details: attempt=3, host=w-1" in out


def test_format_helpers_top_level_exports() -> None:
    """Callbacks are told to reuse the format helpers — they import like the rest."""
    import observe_core

    for name in (
        "format_bytes",
        "format_duration_ms",
        "format_float",
        "format_identifier",
        "format_integer",
        "format_percent",
        "format_timestamp",
        "format_value",
        "PresentationFormatter",
    ):
        assert name in observe_core.__all__
        assert callable(getattr(observe_core, name)) or name == "PresentationFormatter"
