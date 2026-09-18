# Pretty rendering

Canonical events are designed for machines first: JSON envelopes that
serialize, transport and store identically everywhere. `PrettyRenderer`
adds a second, optional presentation: human-readable lines built from the
same envelopes.

```text
canonical event
      │
      ├── JSON/JSONL serialization   (unchanged)
      │
      └── PrettyRenderer             (this page)
                ▲
                │ presentation config
                │
        consuming application
```

The renderer is deliberately application-neutral. observe-core owns the
mechanics — layout, the status vocabulary, field formatting, terminal
behaviour — while the consuming application supplies what each event
*means*: its label, which fields matter, and how visible it should be.
There are no event names baked into observe-core; an application you have
never heard of can drive the same renderer with its own mappings.

## Basic usage

```python
from observe_core import PrettyRenderer, EventPresentation, Field

renderer = PrettyRenderer(
    rules={
        "job.started": EventPresentation(
            label="Job started",
            fields=[Field("attributes.worker", label="Worker")],
        ),
        "record.processed": EventPresentation(
            label="Load",
            fields=[
                Field("attributes.destination"),
                Field("attributes.rows", label="Rows", format="integer"),
            ],
        ),
    },
)

print(renderer.render_many(events))
```

```text
✓ Job started  Worker: w-3
✓ Load  destination: raw.events  Rows: 12,481  8.47s
```

`render()` accepts an `EventEnvelope`, a `CanonicalEvent`, or a plain dict
(shaped like a canonical envelope). `render_many()` renders a sequence and
adds context headers (below). `write()`/`write_many()` send the same output
to the configured stream.

### The fallback

An event with no registered rule still renders — registration is never
required:

```text
✓ some.unknown.event  success  842ms
```

The fallback shows the event name, the outcome word, and the duration when
present. This is what makes the renderer forwards-compatible: new event
types degrade gracefully instead of disappearing or exploding.

## Presentation rules

`EventPresentation` describes how one event type should look:

```python
EventPresentation(
    label="Load",  # headline text (defaults to the event name)
    visibility="primary",  # primary | secondary | hidden
    fields=[Field(...)],  # which values to show
    formatter=None,  # optional escape hatch (below)
)
```

## Fields

`Field` pulls one value out of the envelope by dot-delimited path, resolved
against the canonical event dict:

```python
Field("attributes.table")  # → "table: raw.events"
Field("attributes.rows", label="Rows")  # → "Rows: 12,481"
Field("duration_ms", format="duration")  # → "duration_ms: 8.47s"
Field("correlation.run_id", format="identifier")  # → "run_id: pipeline-run-2f6…"
Field("error.retryable", format="boolean")  # → "retryable: yes"
```

A field whose path is absent or null renders nothing — configured fields
are best-effort, never required. Canonical envelopes carry `None` where a
sparse dict carries nothing; both count as absent. `duration_ms` is
appended automatically when present and not already listed as a field.

## Field formats

Generic formats, usable on any value:

| `format` | input | output |
|---|---|---|
| `"string"` (default) | anything | `str(value)` |
| `"integer"` | `12481` | `12,481` |
| `"float"` | `8466.257` | `8,466.257` |
| `"duration"` | `8466.257` (ms) | `8.47s`; `723` → `723ms`; `90500` → `1m 30s` |
| `"bytes"` | `19608371` | `18.7 MB` |
| `"percent"` | `0.942` (fraction) | `94.2%` |
| `"boolean"` | `True` | `yes` |
| `"timestamp"` | RFC3339 / datetime | `21:51:23` |
| `"identifier"` | `pipeline-run-2f6b1f8dfa…` | `pipeline-run-2f6…` |

`identifier` shortens long opaque strings to a configurable prefix —
`Field(..., format="identifier", head=21)` keeps 21 characters then an
ellipsis (`…` in Unicode mode, `...` in ASCII). The renderer does not know
what the identifier identifies; it only knows how to shorten one.

## Visibility

Every event (and field) has one of three levels:

- `primary` — rendered in both modes
- `secondary` — rendered only in `mode="verbose"`, indented and dimmed
- `hidden` — never rendered

Failures and warnings escalate generically: a failure or an event carrying
`severity` `error`/`critical` (or a structured `error` object) always
renders primary, whatever its configured visibility; a `warn` lifts a
hidden event to secondary. Applications never need to wire per-event
"show on failure" logic.

```python
renderer = PrettyRenderer(rules=RULES, mode="verbose")
```

## Status

Outcome and severity map onto a small vocabulary:

| meaning | unicode | ascii |
|---|---|---|
| success | `✓` | `v` |
| failure | `✕` | `x` |
| warning | `!` | `!` |
| informational | `•` | `*` |
| in-flight (started, not ended) | `→` | `>` |
| partial | `◐` | `~` |
| cancelled | `⊘` | `o` |

The mapping is mechanical: `failure` → `✕`; warn+ severity or a present
`error` object → `!`; `success` → `✓`; unknown outcome with `started_at`
but no `ended_at` → `→`; otherwise `•`.

## Structured errors

Events carrying the canonical `error` object render its fields indented
under the event line — no application interpretation needed:

```python
{
    "error": {
        "message": "Validation failed",
        "why": "83 records were invalid",
        "fix": "Correct the source data",
    }
}
```

```text
✕ job.failed  failure
    Validation failed
    83 records were invalid
    Fix: Correct the source data
```

Verbose mode additionally shows `code`, `exception_type`, `retryable`, the
`stacktrace` (one indented line per frame line) and a compact `details`
line (`key=value` pairs).

## Context

`ContextField` marks a value as group-level: it renders once in a header
when its value changes, instead of repeating on every event.

```python
renderer = PrettyRenderer(
    rules=RULES,
    context=[
        ContextField("correlation.run_id", label="Run", format="identifier", head=21),
        ContextField("attributes.region", label="Region"),
    ],
)
```

```text
── Run: pipeline-run-2f6b1f8d…   Region: us-east-1
✓ Job started  Worker: w-3
✓ Load  destination: raw.events  Rows: 12,481

── Run: other-run-999   Region: eu-west-1
✓ job.completed  1.2s
```

observe-core does not know what a run or a region is — only that these
configured paths identify the group an event belongs to and have these
display labels. Absent or null context values are omitted from the
header; an event whose context values are all absent joins the default,
headerless group. Every group boundary is separated by a blank line —
including boundaries with the headerless group, so ungrouped events
never read as part of a headed group. Events suppressed by visibility
never emit headers.

A `ContextField` also accepts `visibility`: `secondary` context joins the
group and its header only in verbose mode, and `hidden` context is
ignored entirely — a field that cannot render never splits groups on
values nobody can see.

```python
ContextField("attributes.tenant", label="Tenant", visibility="secondary")
```

## Custom formatters

When declarative fields cannot express a layout, `formatter` supplies the
event's content. It receives the canonical event dict and returns a string
or a sequence of lines; the renderer still owns the glyph, indentation,
visibility and terminal behaviour. Returning `None` falls back to the
declarative path; an empty string or sequence renders the bare status
line (a callback cannot suppress an event — that is visibility's job);
any other return type raises `TypeError`.

```python
EventPresentation(
    label="Summary",
    formatter=lambda e: [
        f"processed {e['attributes']['n']} records",
        f"rejected {e['attributes']['bad']}",
    ],
)
```

```text
✓ Summary  processed 12,481 records
    rejected 83
```

The generic format functions (`format_duration_ms`, `format_bytes`,
`format_identifier`, …) are importable from `observe_core` so callbacks
can reuse them. Returned lines pass through the same terminal-safety
escaping as declarative output. Custom formatters are the escape hatch —
most events should stay declarative.

## Terminal behaviour

Output is restrained by default — no framework, no layout engine.

- `symbols="auto"` uses Unicode glyphs when the stream's encoding can
  represent them, ASCII otherwise (or force with `"unicode"`/`"ascii"`).
- `color="auto"` emits minimal ANSI colour only on a TTY, and honours
  `NO_COLOR` and `CI`. `"always"`/`"never"` override.
- Redirected output just gets plain text — detection is via the stream's
  `isatty()` and `encoding`.
- Event data is untrusted terminal input: control characters in values,
  labels and error text (ESC, newlines, C1 controls, unencodable
  surrogates, Unicode line separators) are escaped (`\x1b`, `\n`, …)
  rather than written raw, so event content cannot inject terminal
  sequences and each event stays on its own line(s).

## Supplying application mappings

The intended shape is one presentation module per application — a dict of
rules plus a context list — passed wholesale to `PrettyRenderer`:

```python
# my_app/observe_presentation.py
from observe_core import ContextField, EventPresentation, Field

PRESENTATION = {
    "job.started": EventPresentation(label="Job started", fields=[...]),
    "record.processed": EventPresentation(label="Load", fields=[...]),
    "validation.completed": EventPresentation(label="Validate", visibility="secondary"),
}
CONTEXT = [ContextField("correlation.run_id", label="Run", format="identifier")]

# anywhere in the app
renderer = PrettyRenderer(rules=PRESENTATION, context=CONTEXT)
```

Nothing about the application needs to live in observe-core — the renderer
mechanics are generic, the event semantics are yours. The canonical
JSON/JSONL serialization is untouched: pretty output is an additional
presentation option, not a replacement for the machine format.
