"""Human-readable rendering of canonical events, driven by consumer config.

``PrettyRenderer`` turns canonical event envelopes into scannable text.
It is deliberately application-neutral: observe-core owns layout, status
vocabulary, field formatting and terminal behaviour; the *consumer* owns
what an event means — its display label, which fields matter, and how much
of it is worth showing.

::

    renderer = PrettyRenderer(
        rules={
            "job.started": EventPresentation(
                label="Job started",
                fields=[Field("attributes.worker", label="Worker")],
            ),
        },
        context=[ContextField("correlation.run_id", label="Run", format="identifier")],
    )
    print(renderer.render_many(events))

An event with no configured rule still renders safely::

    ✓ some.unknown.event  success  842ms

The machine representation is untouched: this renderer is an additional
presentation option alongside the canonical JSON/JSONL serialization,
never a replacement for it.
"""

from __future__ import annotations

import math
import os
import sys
import unicodedata
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from dataclasses import field as dataclass_field
from datetime import datetime
from enum import StrEnum
from typing import Any, TextIO

from observe_core.drains.base import CanonicalEvent
from observe_core.models import EventEnvelope

__all__ = [
    "ContextField",
    "EventPresentation",
    "Field",
    "FieldFormat",
    "PresentationFormatter",
    "PrettyRenderer",
    "Visibility",
    "format_bytes",
    "format_duration_ms",
    "format_float",
    "format_identifier",
    "format_integer",
    "format_percent",
    "format_timestamp",
    "format_value",
]


class Visibility(StrEnum):
    """How prominent an event (or field) is in rendered output.

    ``PRIMARY`` renders in both modes; ``SECONDARY`` renders only in verbose
    mode; ``HIDDEN`` is never rendered. Failures and warnings escalate
    visibility generically — see :meth:`PrettyRenderer._effective_visibility`.
    """

    PRIMARY = "primary"
    SECONDARY = "secondary"
    HIDDEN = "hidden"


class FieldFormat(StrEnum):
    """Generic value formats a consumer can request for a field."""

    STRING = "string"
    INTEGER = "integer"
    FLOAT = "float"
    DURATION = "duration"
    BYTES = "bytes"
    PERCENT = "percent"
    BOOLEAN = "boolean"
    TIMESTAMP = "timestamp"
    IDENTIFIER = "identifier"


# ``str | Sequence[str] | None``: the content a custom formatter contributes to
# the event's line(s). ``None`` means "fall back to declarative rendering".
PresentationFormatter = Callable[[Mapping[str, Any]], "str | Sequence[str] | None"]


@dataclass(frozen=True, slots=True)
class Field:
    """One display field on an event, addressed by a dot-delimited path.

    ``path`` is resolved against the canonical event dict
    (``attributes.rows``, ``correlation.run_id``, ``duration_ms``,
    ``error.message``...). A path that is missing or ``None`` renders
    nothing — configured fields are best-effort, never required.

    ``head`` is only used by the ``identifier`` format: the number of
    leading characters kept before an ellipsis. ``visibility`` mirrors
    :class:`Visibility`: ``secondary`` fields render only in verbose
    mode, ``hidden`` fields never render.

    ``suppress`` is an optional predicate applied to the resolved value:
    when it returns true the field renders nothing. It lets a consumer
    declare degenerate values — placeholder identifiers, sentinel names,
    a zero where "no measurement" reads better than ``0`` — without a
    full custom formatter. The predicate sees the raw resolved value,
    before formatting.
    """

    path: str
    label: str | None = None
    format: FieldFormat | str = FieldFormat.STRING
    visibility: Visibility | str = Visibility.PRIMARY
    head: int | None = None
    suppress: Callable[[Any], bool] | None = None

    def __post_init__(self) -> None:
        """Coerce string enum inputs into their enum members."""
        _validate_path_and_head(self.path, self.head)
        _validate_label(self.label)
        object.__setattr__(self, "format", FieldFormat(self.format))
        object.__setattr__(self, "visibility", Visibility(self.visibility))
        if self.suppress is not None and not callable(self.suppress):
            raise TypeError(
                f"suppress must be callable or None, got {type(self.suppress).__name__}"
            )

    @property
    def display_label(self) -> str:
        """Configured label, else the leaf segment of the path."""
        return self.label if self.label is not None else self.path.rpartition(".")[2]


@dataclass(frozen=True, slots=True)
class ContextField:
    """A run-scoped value rendered once in a group header, not per event.

    The renderer treats it as opaque: a path, a label, a format. It does not
    know what the value means — only that events sharing its value belong to
    the same rendered group.

    ``visibility`` mirrors :class:`Visibility`: ``secondary`` fields join the
    group (and its header) only in verbose mode; ``hidden`` fields are
    ignored entirely — a field that cannot render must not split groups on
    values nobody can see.
    """

    path: str
    label: str | None = None
    format: FieldFormat | str = FieldFormat.STRING
    head: int | None = None
    visibility: Visibility | str = Visibility.PRIMARY

    def __post_init__(self) -> None:
        """Coerce the string enum inputs into their enum members."""
        _validate_path_and_head(self.path, self.head)
        _validate_label(self.label)
        object.__setattr__(self, "format", FieldFormat(self.format))
        object.__setattr__(self, "visibility", Visibility(self.visibility))

    @property
    def display_label(self) -> str:
        """Configured label, else the leaf segment of the path."""
        return self.label if self.label is not None else self.path.rpartition(".")[2]


@dataclass(frozen=True, slots=True)
class EventPresentation:
    """How one event type should look to a human.

    ``formatter`` is the escape hatch for content that simple field
    configuration cannot express. It receives the canonical event dict and
    returns replacement content — a string or a sequence of lines. The
    renderer still owns the status glyph, visibility, indentation and
    terminal behaviour; returning ``None`` falls back to the declarative
    path, an empty return renders the bare status line, and any other
    return type raises ``TypeError``. Custom formatters are the exception,
    not the normal path.
    """

    label: str | None = None
    visibility: Visibility | str = Visibility.PRIMARY
    fields: Sequence[Field] = dataclass_field(default_factory=tuple)
    formatter: PresentationFormatter | None = None

    def __post_init__(self) -> None:
        """Coerce string enum inputs, freeze the field sequence, validate."""
        _validate_label(self.label)
        object.__setattr__(self, "visibility", Visibility(self.visibility))
        object.__setattr__(self, "fields", tuple(self.fields))
        for fld in self.fields:
            if not isinstance(fld, Field):
                raise TypeError(f"fields must contain Field instances, got {type(fld).__name__}")
        if self.formatter is not None and not callable(self.formatter):
            raise TypeError(
                f"formatter must be callable or None, got {type(self.formatter).__name__}"
            )


def _validate_path_and_head(path: Any, head: Any) -> None:
    """Fail at construction on field options that would crash at render time."""
    if not isinstance(path, str):
        raise TypeError(f"path must be a str, got {type(path).__name__}")
    if head is not None and not isinstance(head, int):
        raise TypeError(f"head must be an int or None, got {type(head).__name__}")


def _validate_label(label: Any) -> None:
    """Fail at construction on a non-string label."""
    if label is not None and not isinstance(label, str):
        raise TypeError(f"label must be a str or None, got {type(label).__name__}")


# ---------------------------------------------------------------------------
# Status vocabulary and terminal capabilities
# ---------------------------------------------------------------------------

_UNICODE_GLYPHS: dict[str, str] = {
    "success": "✓",
    "failure": "✕",
    "warning": "!",
    "info": "•",
    "running": "→",
    "partial": "◐",
    "cancelled": "⊘",
}
_ASCII_GLYPHS: dict[str, str] = {
    "success": "v",
    "failure": "x",
    "warning": "!",
    "info": "*",
    "running": ">",
    "partial": "~",
    "cancelled": "o",
}

_COLORS: dict[str, str] = {
    "success": "\x1b[32m",
    "failure": "\x1b[31m",
    "warning": "\x1b[33m",
}
_RESET = "\x1b[0m"
_DIM = "\x1b[2m"

_MISSING = object()
"""Sentinel for path resolution misses (distinguishable from ``None``)."""


def _stream_can_unicode(stream: TextIO | None) -> bool:
    """Whether the stream's encoding can represent the Unicode glyph set."""
    encoding = getattr(stream, "encoding", None) or "utf-8"
    try:
        "✓✕•→◐⊘…─".encode(encoding)
        return True
    except (UnicodeEncodeError, LookupError):
        return False


def _want_color(stream: TextIO | None, color: str) -> bool:
    """Resolve the color policy: TTY + not NO_COLOR + not CI when ``auto``."""
    if color == "always":
        return True
    if color == "never":
        return False
    if os.environ.get("NO_COLOR") is not None:
        return False
    ci = os.environ.get("CI", "")
    if ci.lower() not in ("", "0", "false"):
        return False
    return bool(stream is not None and getattr(stream, "isatty", lambda: False)())


_CONTROL_ESCAPES = {"\n": "\\n", "\r": "\\r", "\t": "\\t"}
"""Readable escapes for the common control characters."""

_ESCAPED_CATEGORIES = frozenset({"Cc", "Cs", "Zl", "Zp"})
"""Unicode categories escaped by :func:`_sanitize`.

``Cc`` covers C0/C1 controls and DEL — ESC above all, which would inject
arbitrary terminal sequences. ``Cs`` covers lone surrogates, which raise
``UnicodeEncodeError`` on most stream encodings. ``Zl``/``Zp`` are line
and paragraph separators that would break the one-line-per-event layout.
Format characters (``Cf``) are deliberately left alone: they include
legitimate joiners and marks (emoji ZWJ sequences, Indic shaping), and
stripping them mangles real text.
"""


def _sanitize(text: str) -> str:
    r"""Make ``text`` safe to write to a terminal.

    Event data is untrusted as terminal output: an ESC or C1 control
    character embedded in an attribute value or error message would
    otherwise inject ANSI sequences into the user's terminal, and a raw
    newline would break the line-oriented layout. Control characters
    render as ``\n``-style or ``\xNN`` escapes, all pure ASCII.
    """
    out: list[str] = []
    append = out.append
    for c in text:
        escape = _CONTROL_ESCAPES.get(c)
        if escape is not None:
            append(escape)
        elif unicodedata.category(c) in _ESCAPED_CATEGORIES:
            append(f"\\x{ord(c):02x}")
        else:
            append(c)
    return "".join(out)


# ---------------------------------------------------------------------------
# Generic formatters (exported so custom presentation callbacks can reuse them)
# ---------------------------------------------------------------------------


def format_integer(value: Any) -> str:
    """``12481`` -> ``"12,481"``."""
    try:
        return f"{int(value):,}"
    except (TypeError, ValueError, OverflowError):
        return str(value)


def format_float(value: Any) -> str:
    """Up to three decimals, thousands separators, trailing zeros trimmed."""
    try:
        return f"{float(value):,.3f}".rstrip("0").rstrip(".")
    except (TypeError, ValueError, OverflowError):
        return str(value)


def format_duration_ms(value: Any) -> str:
    """Milliseconds as a compact duration.

    ``723`` -> ``"723ms"``, ``8466.3`` -> ``"8.47s"``, ``90500`` -> ``"1m 30s"``.
    Non-numeric, non-finite and float-unconvertible inputs render as
    ``str(value)``.
    """
    try:
        ms = float(value)
    except (TypeError, ValueError, OverflowError):
        return str(value)
    if not math.isfinite(ms):
        return str(value)
    if ms < 1000:
        # ``:.0f`` rounding must not produce a literal "1000ms".
        return "1s" if ms >= 999.5 else f"{ms:.0f}ms"
    seconds = ms / 1000.0
    if seconds < 59.95:
        # Below the point where ``:.3g`` would round up to "60.0s".
        return f"{seconds:.3g}s"
    minutes, sec = divmod(round(seconds), 60)
    if minutes < 60:
        return f"{minutes}m {sec}s" if sec else f"{minutes}m"
    hours, minutes = divmod(minutes, 60)
    return f"{hours}h {minutes}m" if minutes else f"{hours}h"


def format_bytes(value: Any) -> str:
    """Byte counts with 1024-based units: ``19608371`` -> ``"18.7 MB"``."""
    try:
        n = float(value)
    except (TypeError, ValueError, OverflowError):
        return str(value)
    if not math.isfinite(n):
        return str(value)
    # Rounded boundaries advance to the next unit: 1023.6 B renders
    # "1.0 KB", not "1024 B"; a byte under a MiB renders "1.0 MB".
    if round(n) < 1024:
        return f"{n:.0f} B"
    for unit in ("KB", "MB", "GB", "TB", "PB"):
        n /= 1024.0
        if round(n, 1) < 1024 or unit == "PB":
            return f"{n:.1f} {unit}"
    return f"{n:.1f} PB"


def format_percent(value: Any) -> str:
    """Fractions (0..1) as percentages: ``0.942`` -> ``"94.2%"``."""
    try:
        pct = float(value) * 100
    except (TypeError, ValueError, OverflowError):
        return str(value)
    return f"{pct:.4g}%" if math.isfinite(pct) else str(value)


def format_timestamp(value: Any) -> str:
    """RFC3339 string or datetime -> ``"HH:MM:SS"``."""
    if isinstance(value, datetime):
        return value.strftime("%H:%M:%S")
    if isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return parsed.strftime("%H:%M:%S")
        except ValueError:
            return value
    return str(value)


def _format_timestamp_ms(value: Any) -> str:
    """RFC3339 string or datetime -> ``"HH:MM:SS.mmm"``; ``""`` when unusable."""
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return ""
    else:
        return ""
    return f"{parsed:%H:%M:%S}.{parsed.microsecond // 1000:03d}"


def format_identifier(value: Any, head: int = 16, *, ellipsis: str = "…") -> str:
    """Shorten long identifiers: ``"pipeline-run-2f6b1f8dfa…"`` -> prefix + ``…``.

    ``head`` is the number of leading characters kept. Values at or below
    ``head`` are returned whole.
    """
    text = str(value)
    head = max(head, 0)
    return text if len(text) <= head else text[:head] + ellipsis


def format_value(
    value: Any, fmt: FieldFormat | str, *, head: int | None = None, ellipsis: str = "…"
) -> str:
    """Dispatch a value through a generic format. ``None`` renders ``"null"``."""
    fmt = FieldFormat(fmt)
    if value is None:
        return "null"
    if fmt is FieldFormat.INTEGER:
        return format_integer(value)
    if fmt is FieldFormat.FLOAT:
        return format_float(value)
    if fmt is FieldFormat.DURATION:
        return format_duration_ms(value)
    if fmt is FieldFormat.BYTES:
        return format_bytes(value)
    if fmt is FieldFormat.PERCENT:
        return format_percent(value)
    if fmt is FieldFormat.BOOLEAN:
        return "yes" if bool(value) else "no"
    if fmt is FieldFormat.TIMESTAMP:
        return format_timestamp(value)
    if fmt is FieldFormat.IDENTIFIER:
        return format_identifier(value, head if head is not None else 16, ellipsis=ellipsis)
    return str(value)


# ---------------------------------------------------------------------------
# Renderer
# ---------------------------------------------------------------------------


def _resolve(data: Mapping[str, Any], path: str) -> Any:
    """Walk a dot-delimited path over nested mappings; ``_MISSING`` on absence."""
    node: Any = data
    for segment in path.split("."):
        if not isinstance(node, Mapping) or segment not in node:
            return _MISSING
        node = node[segment]
    return node


class PrettyRenderer:
    """Render canonical events as human-readable lines.

    Parameters:

    - ``rules`` — ``{event_name: EventPresentation}``. Unregistered events
      render via the generic fallback; registration is never required.
    - ``context`` — ``ContextField`` sequence. Values identical across a run
      of events render once as a group header instead of per event.
      ``secondary`` context fields join the group only in verbose mode;
      ``hidden`` ones are ignored entirely.
    - ``mode`` — ``"pretty"`` shows primary events and fields; ``"verbose"``
      adds secondary ones plus extra error detail.
    - ``color`` — ``"auto"`` (TTY, ``NO_COLOR`` and ``CI`` aware),
      ``"always"``, ``"never"``.
    - ``symbols`` — ``"auto"`` (Unicode when the stream's encoding supports
      it, ASCII otherwise), ``"unicode"``, ``"ascii"``.
    - ``timestamps`` — prefix each event line with the event's own time as
      ``HH:MM:SS.mmm``, read from the canonical ``observed_at`` (falling
      back to ``started_at`` then ``ended_at``). Off by default.
    - ``stream`` — used for TTY/encoding detection and as the target of
      :meth:`write`/:meth:`write_many`. Defaults to ``sys.stdout``.
    """

    def __init__(
        self,
        rules: Mapping[str, EventPresentation] | None = None,
        *,
        context: Sequence[ContextField] | None = None,
        mode: str = "pretty",
        color: str = "auto",
        symbols: str = "auto",
        timestamps: bool = False,
        stream: TextIO | None = None,
    ) -> None:
        if mode not in ("pretty", "verbose"):
            raise ValueError(f"mode must be 'pretty' or 'verbose', got {mode!r}")
        if color not in ("auto", "always", "never"):
            raise ValueError(f"color must be 'auto', 'always' or 'never', got {color!r}")
        if symbols not in ("auto", "unicode", "ascii"):
            raise ValueError(f"symbols must be 'auto', 'unicode' or 'ascii', got {symbols!r}")
        if not isinstance(timestamps, bool):
            raise TypeError(f"timestamps must be a bool, got {type(timestamps).__name__}")
        self._rules = dict(rules or {})
        for name, presentation in self._rules.items():
            if not isinstance(name, str) or not isinstance(presentation, EventPresentation):
                raise TypeError(
                    "rules must map event names (str) to EventPresentation, "
                    f"got {type(name).__name__}: {type(presentation).__name__}"
                )
        self._context = tuple(context or ())
        for cf in self._context:
            if not isinstance(cf, ContextField):
                raise TypeError(
                    f"context must contain ContextField instances, got {type(cf).__name__}"
                )
        self._mode = mode
        self._timestamps = timestamps
        self._stream = stream if stream is not None else sys.stdout
        self._color = _want_color(self._stream, color)
        if symbols == "auto":
            self._unicode = _stream_can_unicode(self._stream)
        else:
            self._unicode = symbols == "unicode"
        self._glyphs = _UNICODE_GLYPHS if self._unicode else _ASCII_GLYPHS
        self._ellipsis = "…" if self._unicode else "..."
        # Context invisible in this mode is dropped, not merely hidden: a
        # field that cannot render must not split groups on unseen values.
        self._visible_context = tuple(
            cf
            for cf in self._context
            if cf.visibility != Visibility.HIDDEN
            and (self._mode == "verbose" or cf.visibility != Visibility.SECONDARY)
        )
        # Incremental-write state for :meth:`write_event`: the group last
        # written and whether anything has been written yet (a blank line
        # separates groups, but never precedes the first line of output).
        self._stream_context: tuple[Any, ...] | None = None
        self._stream_emitted = False

    # -- public API ---------------------------------------------------------

    def render(self, event: Any) -> str:
        """Render one event to a string (one or more lines, no trailing newline).

        Returns ``""`` when the event's visibility suppresses it. Safe on any
        input: envelopes, :class:`CanonicalEvent` objects, plain dicts,
        partial dicts and outright malformed values all render something
        rather than raising.
        """
        data = self._normalize(event)
        if data is None:
            return f"{self._glyphs['info']} <unrenderable event>"
        return "\n".join(self._render_event_lines(data))

    def render_many(self, events: Sequence[Any]) -> str:
        """Render a sequence, inserting a context header when context values change.

        Group boundaries are separated by a blank line. Events with no
        context values form a headerless group of their own — separated
        like any other group so they never read as part of a headed one.
        """
        lines: list[str] = []
        current_context: tuple[Any, ...] | None = None
        for event in events:
            data = self._normalize(event)
            if data is None:
                lines.append(f"{self._glyphs['info']} <unrenderable event>")
                continue
            event_lines = self._render_event_lines(data)
            if not event_lines:
                # Suppressed events render nothing — and must not emit a
                # group header or claim a context switch either.
                continue
            group = self._context_key(data)
            if self._visible_context and group != current_context:
                # Every boundary breaks the visual grouping — including
                # transitions into the headerless group, which has no
                # header of its own to mark the change.
                if lines:
                    lines.append("")
                header = self._context_header(data)
                if header:
                    lines.append(header)
                current_context = group
            lines.extend(event_lines)
        return "\n".join(lines)

    def write(self, event: Any) -> None:
        """Render one event to the configured stream."""
        text = self.render(event)
        if text:
            self._stream.write(text + "\n")
        self._stream.flush()

    def write_many(self, events: Sequence[Any]) -> None:
        """Render a sequence to the configured stream."""
        text = self.render_many(events)
        if text:
            self._stream.write(text + "\n")
        self._stream.flush()

    def write_event(self, event: Any) -> None:
        """Render one event to the configured stream, tracking context across calls.

        Successive calls share group state — the context header is written
        only when the context values change, and a blank line separates
        groups — so incremental consumers (drains, tail-followers) get the
        same grouping :meth:`render_many` gives a complete sequence without
        re-emitting a header per batch. :meth:`write` and :meth:`write_many`
        are stateless and do not participate in this tracking.
        """
        data = self._normalize(event)
        if data is None:
            self._stream.write(f"{self._glyphs['info']} <unrenderable event>\n")
            self._stream_emitted = True
            self._stream.flush()
            return
        event_lines = self._render_event_lines(data)
        if not event_lines:
            # Suppressed events write nothing and must not emit a header or
            # claim a context switch.
            return
        out: list[str] = []
        group = self._context_key(data)
        if self._visible_context and group != self._stream_context:
            if self._stream_emitted:
                out.append("")
            header = self._context_header(data)
            if header:
                out.append(header)
            self._stream_context = group
        out.extend(event_lines)
        self._stream.write("\n".join(out) + "\n")
        self._stream_emitted = True
        self._stream.flush()

    # -- event normalization ------------------------------------------------

    @staticmethod
    def _normalize(event: Any) -> dict[str, Any] | None:
        """Coerce an envelope / CanonicalEvent / mapping into a canonical dict."""
        if isinstance(event, Mapping):
            return dict(event)
        if isinstance(event, EventEnvelope):
            return event.to_canonical_dict()
        if isinstance(event, CanonicalEvent):
            return dict(event.data)
        data = getattr(event, "data", None)
        if isinstance(data, Mapping):
            return dict(data)
        return None

    # -- status and visibility -----------------------------------------------

    @staticmethod
    def _status(data: Mapping[str, Any]) -> str:
        """Map canonical outcome/severity to the small status vocabulary."""
        outcome = str(data.get("outcome") or "unknown")
        severity = str(data.get("severity") or "info")
        has_error = isinstance(data.get("error"), Mapping)
        if outcome == "failure":
            return "failure"
        if outcome == "cancelled":
            return "cancelled"
        if outcome == "partial":
            return "partial"
        if severity in ("warn", "error", "critical") or has_error:
            return "warning"
        if outcome == "success":
            return "success"
        # outcome unknown/other: in-flight if it has a start but no end.
        if data.get("started_at") is not None and data.get("ended_at") is None:
            return "running"
        return "info"

    @staticmethod
    def _effective_visibility(
        data: Mapping[str, Any], presentation: EventPresentation | None
    ) -> Visibility:
        """Generic escalation policy for failures and warnings.

        Failures are always primary; warnings lift hidden events to
        secondary so they surface in verbose output.
        """
        base = (
            Visibility(presentation.visibility) if presentation is not None else Visibility.PRIMARY
        )
        outcome = str(data.get("outcome") or "unknown")
        severity = str(data.get("severity") or "info")
        if (
            outcome == "failure"
            or severity in ("error", "critical")
            or isinstance(data.get("error"), Mapping)
        ):
            return Visibility.PRIMARY
        if severity == "warn" and base == Visibility.HIDDEN:
            return Visibility.SECONDARY
        return base

    # -- line construction ----------------------------------------------------

    def _render_event_lines(self, data: Mapping[str, Any]) -> list[str]:
        """Build the rendered lines for one normalized event."""
        presentation = self._rules.get(str(data.get("event") or ""))
        visibility = self._effective_visibility(data, presentation)
        if visibility == Visibility.HIDDEN:
            return []
        if visibility == Visibility.SECONDARY and self._mode != "verbose":
            return []

        status = self._status(data)
        secondary = visibility == Visibility.SECONDARY
        glyph = self._colored_glyph(status, dim=secondary)
        label = _sanitize(
            (presentation.label if presentation else None) or str(data.get("event") or "event")
        )

        if presentation is not None and presentation.formatter is not None:
            content = presentation.formatter(data)
            if content is not None:
                if isinstance(content, str):
                    custom_lines = [content]
                elif isinstance(content, Iterable) and not isinstance(
                    content, (bytes, bytearray, Mapping)
                ):
                    custom_lines = list(content)
                else:
                    raise TypeError(
                        f"formatter for {data.get('event')!r} returned "
                        f"{type(content).__name__}; expected str, a sequence "
                        "of lines, or None"
                    )
                return self._with_status_line(
                    glyph,
                    label,
                    [_sanitize(str(c)) for c in custom_lines],
                    data,
                    secondary=secondary,
                )

        ts = self._timestamp_column(data)
        head = f"{ts} {glyph} {label}" if ts else f"{glyph} {label}"
        parts: list[str] = []
        if presentation is None:
            # Fallback: no configured fields — show the outcome word explicitly.
            parts.append(self._outcome_word(data, status))
        else:
            for fld in presentation.fields:
                if fld.visibility == Visibility.SECONDARY and self._mode != "verbose":
                    continue
                if fld.visibility == Visibility.HIDDEN:
                    continue
                value = _resolve(data, fld.path)
                if value is _MISSING or value is None:
                    continue
                if fld.suppress is not None and fld.suppress(value):
                    continue
                rendered = format_value(value, fld.format, head=fld.head, ellipsis=self._ellipsis)
                parts.append(_sanitize(f"{fld.display_label}: {rendered}"))
            if status in ("partial", "cancelled", "running", "info"):
                parts.append(self._outcome_word(data, status))

        configured_paths = {f.path for f in presentation.fields} if presentation else set()
        duration = data.get("duration_ms")
        if duration is not None and "duration_ms" not in configured_paths:
            parts.append(_sanitize(format_duration_ms(duration)))

        body = "  ".join(p for p in parts if p)
        line = f"{head}  {body}" if body else head
        if secondary:
            line = f"  {line}"
        lines = [line]
        lines.extend(self._error_lines(data, secondary=secondary))
        if secondary:
            lines = [self._dim(line) for line in lines]
        return lines

    def _with_status_line(
        self,
        glyph: str,
        label: str,
        content: list[str],
        data: Mapping[str, Any],
        *,
        secondary: bool,
    ) -> list[str]:
        """Wrap custom-formatter content in the standard status layout.

        Empty content still renders the bare status line — suppression is
        the job of visibility, not of the content callback.
        """
        if not content:
            content = [""]
        first, *rest = content
        ts = self._timestamp_column(data)
        head = f"{ts} {glyph} {label}" if ts else f"{glyph} {label}"
        line = f"{head}  {first}" if first else head
        if secondary:
            line = f"  {line}"
        indent = "      " if secondary else "    "
        lines = [line, *(f"{indent}{r}" for r in rest)]
        lines.extend(self._error_lines(data, secondary=secondary))
        if secondary:
            lines = [self._dim(line) for line in lines]
        return lines

    @staticmethod
    def _outcome_word(data: Mapping[str, Any], status: str) -> str:
        """Human word for the outcome when the glyph alone is ambiguous."""
        if status == "running":
            return "running"
        outcome = str(data.get("outcome") or "")
        return _sanitize(outcome if outcome not in ("", "unknown") else "unknown")

    def _timestamp_column(self, data: Mapping[str, Any]) -> str:
        """Leading ``HH:MM:SS.mmm`` column for the event's own time, or ``""``.

        Reads the canonical envelope timestamps — ``observed_at``, then
        ``started_at``, then ``ended_at`` — so no consumer configuration is
        needed. Events without a usable timestamp simply render no column.
        """
        if not self._timestamps:
            return ""
        raw = data.get("observed_at") or data.get("started_at") or data.get("ended_at")
        text = _format_timestamp_ms(raw)
        if not text:
            return ""
        return self._dim(_sanitize(text))

    def _error_lines(self, data: Mapping[str, Any], *, secondary: bool) -> list[str]:
        """Render the structured error model generically, indented under the event."""
        error = data.get("error")
        if not isinstance(error, Mapping):
            return []
        lines: list[str] = []
        indent = "      " if secondary else "    "
        if error.get("message"):
            lines.append(f"{indent}{_sanitize(str(error['message']))}")
        if error.get("why"):
            lines.append(f"{indent}{_sanitize(str(error['why']))}")
        if error.get("fix"):
            lines.append(f"{indent}Fix: {_sanitize(str(error['fix']))}")
        if self._mode == "verbose":
            extras = []
            if error.get("code"):
                extras.append(f"code: {_sanitize(str(error['code']))}")
            if error.get("exception_type"):
                extras.append(f"exception: {_sanitize(str(error['exception_type']))}")
            if error.get("retryable"):
                extras.append("retryable: yes")
            if extras:
                lines.append(f"{indent}{'  '.join(extras)}")
            if error.get("stacktrace"):
                lines.extend(
                    f"{indent}{_sanitize(line)}" for line in str(error["stacktrace"]).splitlines()
                )
            details = error.get("details")
            if isinstance(details, Mapping) and details:
                items = ", ".join(
                    f"{_sanitize(str(k))}={_sanitize(str(v))}" for k, v in details.items()
                )
                lines.append(f"{indent}details: {items}")
            elif details:
                lines.append(f"{indent}details: {_sanitize(str(details))}")
        return lines

    # -- context headers --------------------------------------------------------

    def _context_key(self, data: Mapping[str, Any]) -> tuple[Any, ...]:
        """The tuple of context values identifying the group this event belongs to.

        A missing path and a ``None`` value are equivalent: canonical
        envelopes carry ``None`` where sparse dicts carry nothing, and both
        should land an event in the same group.
        """
        return tuple(
            None if (v := _resolve(data, cf.path)) is _MISSING else v
            for cf in self._visible_context
        )

    def _context_header(self, data: Mapping[str, Any]) -> str:
        parts = []
        for cf in self._visible_context:
            value = _resolve(data, cf.path)
            if value is _MISSING or value is None:
                continue
            rendered = format_value(value, cf.format, head=cf.head, ellipsis=self._ellipsis)
            parts.append(_sanitize(f"{cf.display_label}: {rendered}"))
        if not parts:
            return ""
        prefix = "── " if self._unicode else "-- "
        return self._dim(prefix + "   ".join(parts))

    # -- terminal decoration -----------------------------------------------------

    def _colored_glyph(self, status: str, *, dim: bool = False) -> str:
        glyph = self._glyphs.get(status, self._glyphs["info"])
        if not self._color:
            return glyph
        # On dimmed (secondary) lines the glyph ends with DIM rather than
        # RESET so it does not cancel the enclosing dim span.
        trailer = _DIM if dim else _RESET
        return f"{_COLORS.get(status, _DIM)}{glyph}{trailer}"

    def _dim(self, line: str) -> str:
        return f"{_DIM}{line}{_RESET}" if self._color else line
