"""DLT (dlt) pipeline integration.

Uses the public ``LoadInfo`` object that ``pipeline.run()`` returns —
no undocumented internals. Works without the ``dlt`` extra by duck-typing.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from observe_core import bind_context, observe
from observe_core.identifiers import source_id
from observe_core.models import Category

from phlo_observe import events as E


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


def load_info_attributes(load_info: Any) -> dict[str, Any]:
    """Extract canonical attributes from a dlt ``LoadInfo`` object."""
    attrs: dict[str, Any] = {}
    for key, names in {
        "pipeline_name": ("pipeline.pipeline_name", "pipeline_name"),
        "destination": ("destination_type", "destination_name"),
        "dataset_name": ("dataset_name",),
        "load_id": ("load_id", "load_ids"),
        "loads_ids": ("loads_ids",),
        "started_at": ("started_at",),
        "finished_at": ("finished_at",),
    }.items():
        value = _attr(load_info, *names)
        if value is None:
            continue
        if hasattr(value, "isoformat"):
            value = value.isoformat()
        attrs[key] = value
    # package/job details when exposed
    packages = _attr(load_info, "load_packages") or []
    if packages:
        rows = 0
        tables = set()
        for package in packages:
            jobs = _attr(package, "jobs", "completed_jobs") or {}
            for job_list in jobs.values() if isinstance(jobs, dict) else []:
                for job in job_list or []:
                    table = _attr(job, "table_name", "table")
                    if table:
                        tables.add(str(table))
                        row_count = _attr(job, "row_count", "rows")
                        if isinstance(row_count, int):
                            rows += row_count
        if tables:
            attrs["tables"] = sorted(tables)
        if rows:
            attrs["rows_loaded"] = rows
    return attrs


@contextmanager
def dlt_pipeline_scope(pipeline: Any) -> Iterator[None]:
    """Bind a dlt pipeline identity for contained events."""
    values = {
        "pipeline": _attr(pipeline, "pipeline_name"),
        "dlt_destination": _attr(pipeline, "destination.name", "destination_name"),
        "dlt_dataset": _attr(pipeline, "dataset_name"),
        "producer": "dlt",
    }
    with bind_context(**{k: v for k, v in values.items() if v is not None}):
        yield


def dlt_pipeline_run(pipeline: Any, *, attributes: dict[str, Any] | None = None) -> observe:
    """Wrap ``pipeline.run(...)``. Emits ``dlt.pipeline.run`` on exit.

    Set the returned ``LoadInfo`` via ``evt.set(**load_info_attributes(info))``
    inside the block, or let the caller inspect it.
    """
    attrs: dict[str, Any] = {
        "pipeline_name": _attr(pipeline, "pipeline_name"),
        "destination": _attr(pipeline, "destination.name", "destination_name"),
        "dataset_name": _attr(pipeline, "dataset_name"),
    }
    attrs.update(attributes or {})
    pipeline_name = _attr(pipeline, "pipeline_name")
    return observe(
        E.DLT_PIPELINE_RUN,
        category=Category.PIPELINE,
        attributes={k: v for k, v in attrs.items() if v is not None},
        correlation={"pipeline": pipeline_name},
        entities={"source": source_id("dlt", pipeline_name)} if pipeline_name else None,
        producer="dlt",
    )
