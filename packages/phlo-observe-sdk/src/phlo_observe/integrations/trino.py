"""Client-side Trino query observability.

Captures query identity, class, timing, processed volumes and failure codes.
SQL text is opt-in — it may contain sensitive literal values — while a stable
query hash is always recorded. When SQL is captured it is sanitized (literal
strings and numbers masked) unless ``sanitize=False`` is explicit.
"""

from __future__ import annotations

import hashlib
import re
from typing import Any

from observe_core import observe
from observe_core.identifiers import service_id
from observe_core.models import Category

from phlo_observe import events as E
from phlo_observe.attributes import TrinoQueryAttributes

_STRING_LITERAL = re.compile(r"'(?:[^'\\]|\\.|'')*'")
_NUMERIC_LITERAL = re.compile(r"\b\d+(\.\d+)?\b")
_LINE_COMMENT = re.compile(r"--[^\n]*")
_BLOCK_COMMENT = re.compile(r"/\*.*?\*/", re.DOTALL)
_WHITESPACE = re.compile(r"\s+")


def sanitize_sql(sql: str) -> str:
    """Mask literal values and strip comments from a SQL statement.

    Result is suitable for telemetry: structure preserved, values removed.
    """
    text = _BLOCK_COMMENT.sub(" ", sql)
    text = _LINE_COMMENT.sub(" ", text)
    text = _STRING_LITERAL.sub("'?'", text)
    text = _NUMERIC_LITERAL.sub("?", text)
    return _WHITESPACE.sub(" ", text).strip()


def query_hash(sql: str) -> str:
    """Stable fingerprint of the sanitized statement (SHA-256, 16 hex chars)."""
    return hashlib.sha256(sanitize_sql(sql).encode()).hexdigest()[:16]


def query_class(sql: str) -> str:
    """Best-effort query classification from its leading keyword."""
    word = _WHITESPACE.sub(" ", sql.strip()).split(" ", 1)[0].lower() if sql.strip() else ""
    return word or "unknown"


def trino_query(
    *,
    query_id: str | None = None,
    sql: str | None = None,
    catalog: str | None = None,
    schema: str | None = None,
    include_sql: bool = False,
    sanitize: bool = True,
    attributes: dict[str, Any] | None = None,
    correlation: dict[str, Any] | None = None,
) -> observe:
    """Wrap a client-side Trino query. Emits ``trino.query`` on exit.

    ``include_sql`` must be set explicitly to record statement text; when set,
    the statement is sanitized unless ``sanitize=False``.
    """
    model = TrinoQueryAttributes(
        query_id=query_id,
        catalog=catalog,
        schema=schema,
    )
    attrs = model.attrs()
    if sql is not None:
        attrs["query_class"] = query_class(sql)
        attrs["query_hash"] = query_hash(sql)
        if include_sql:
            attrs["sql"] = sanitize_sql(sql) if sanitize else sql
    if attributes:
        attrs.update(attributes)
    return observe(
        E.TRINO_QUERY,
        category=Category.QUERY,
        attributes=attrs,
        correlation=correlation,
        entities={"service": service_id("trino")},
        producer="trino",
    )
