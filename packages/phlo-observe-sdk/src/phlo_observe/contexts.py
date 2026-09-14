"""Phlo domain contexts.

Each helper binds canonical correlation keys (``run_id``, ``asset_key``,
``branch``, ``table``, ``snapshot_id``...) plus domain-specific values in
``correlation.extra``. They compose through ``contextvars``: nested contexts
merge and inner values win, identical to ``observe_core.bind_context``.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from observe_core import bind_context


@contextmanager
def phlo_run_context(
    *,
    run_id: str,
    job: str | None = None,
    attempt: int | None = None,
    pipeline: str | None = None,
    **extra: Any,
) -> Iterator[None]:
    """Bind a pipeline run: every event inside carries ``run_id``."""
    yield_values: dict[str, Any] = {"run_id": run_id, "job_id": job, "pipeline": pipeline}
    yield_values.update(extra)
    if attempt is not None:
        yield_values["attempt"] = attempt
    with bind_context(**yield_values):
        yield


@contextmanager
def asset_context(
    *,
    asset_key: str,
    partition_key: str | None = None,
    **extra: Any,
) -> Iterator[None]:
    """Bind an asset (and optionally a partition) for contained events."""
    with bind_context(asset_key=asset_key, partition_key=partition_key, **extra):
        yield


@contextmanager
def wap_context(
    *,
    branch: str,
    base_branch: str | None = None,
    target_branch: str | None = None,
    table: str | None = None,
    **extra: Any,
) -> Iterator[None]:
    """Bind a write-audit-publish branch context."""
    with bind_context(
        branch=branch,
        base_branch=base_branch,
        target_branch=target_branch,
        table=table,
        **extra,
    ):
        yield


@contextmanager
def table_context(
    *,
    table: str,
    catalog: str | None = None,
    namespace: str | None = None,
    snapshot_id: str | None = None,
    **extra: Any,
) -> Iterator[None]:
    """Bind a table/snapshot context (Iceberg/Nessie workflows)."""
    with bind_context(
        table=table,
        catalog=catalog,
        namespace=namespace,
        snapshot_id=snapshot_id,
        **extra,
    ):
        yield
