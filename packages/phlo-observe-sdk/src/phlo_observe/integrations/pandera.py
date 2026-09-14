"""Pandera data-quality integration.

Translates pandera validation outcomes into canonical ``quality.validate``
attributes. Duck-typed — works without the ``pandera`` extra installed,
against any object exposing ``failure_cases``/``schema``-style attributes.

Failing sample values are capped and are only included when
``include_samples=True``; in production keep them off, since they may
contain sensitive row content.
"""

from __future__ import annotations

import contextlib
from typing import Any

from observe_core.models import SourceInfo

from phlo_observe.attributes import QualityValidateAttributes

DEFAULT_MAX_SAMPLE_VALUES = 5


def _attr(obj: Any, *names: str) -> Any:
    for name in names:
        current: Any = obj
        for part in name.split("."):
            current = getattr(current, part, None)
            if current is None:
                break
        if current is not None:
            return current
    return None


def pandera_attributes(
    exc_or_result: Any,
    *,
    suite: str | None = None,
    include_samples: bool = False,
    max_sample_values: int = DEFAULT_MAX_SAMPLE_VALUES,
) -> dict[str, Any]:
    """Build ``quality.validate`` attributes from a pandera error or report.

    Accepts ``SchemaError``, ``SchemaErrors`` (lazy), or a summary dict.
    """
    schema = _attr(exc_or_result, "schema")
    schema_name = suite or _attr(schema, "name") or _attr(exc_or_result, "schema_name")

    failure_cases = _attr(exc_or_result, "failure_cases")
    rows_failed: int | None = None
    failing_columns: list[str] = []
    failure_codes: set[str] = set()
    samples: list[Any] = []

    if failure_cases is not None:
        with contextlib.suppress(TypeError):
            rows_failed = len(failure_cases)
        failing_columns.extend(
            str(col) for col in _tabular_column(failure_cases, "column") or [] if col is not None
        )
        failure_codes.update(
            str(code) for code in _tabular_column(failure_cases, "check") or [] if code is not None
        )
        if include_samples and rows_failed:
            sample_values = _tabular_column(failure_cases, "failure_case") or []
            samples = [str(v) for v in sample_values[:max_sample_values]]

    # Lazy SchemaErrors collect per-check errors in .schema_errors
    schema_errors = _attr(exc_or_result, "schema_errors") or []
    checks_failed = (
        len(schema_errors) if schema_errors else (1 if failure_cases is not None else None)
    )
    lazy = bool(schema_errors)

    model = QualityValidateAttributes(
        suite=schema_name,
        checks_failed=checks_failed,
        rows_failed=rows_failed,
        failing_columns=sorted(set(failing_columns)) or None,
        failure_codes=sorted(failure_codes) or None,
        lazy=lazy or None,
    )
    attrs = model.attrs()
    if samples:
        attrs["sample_failures"] = samples
    return attrs


def _tabular_column(frame: Any, name: str) -> list[Any] | None:
    """Pull one column out of a dataframe-like object without importing pandas."""
    try:
        if hasattr(frame, "getcolumn"):  # polars / arrow
            series = frame.getcolumn(name)
        elif hasattr(frame, "__getitem__"):
            series = frame[name]
        else:
            return None
    except Exception:
        return None
    if hasattr(series, "tolist"):
        return list(series.tolist())
    if hasattr(series, "to_list"):
        return list(series.to_list())
    try:
        return list(series)
    except TypeError:
        return None


def pandera_source() -> SourceInfo:
    """Canonical source marker for pandera-derived events."""
    return SourceInfo(producer="pandera", kind="validation")
