"""Dagster integration.

Duck-typed throughout — the SDK reads Dagster context objects
(``context.run_id``, ``context.job_name``, ``context.asset_key``,
``context.partition_key``, ``context.retry_number``) without importing
dagster, so this module works with ``phlo-observe`` installed bare. The
``phlo-observe[dagster]`` extra only matters if you want real Dagster types
installed alongside.

No Dagster internals are monkeypatched; everything goes through the public
context objects Dagster already provides to ops/assets/sensors/hooks.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from observe_core import bind_context, event, observe
from observe_core.identifiers import asset_id, run_id_for
from observe_core.models import Category

from phlo_observe import events as E


def _attr(obj: Any, *names: str) -> Any:
    """First non-None attribute among nested names, else None."""
    for name in names:
        current: Any = obj
        for part in name.split("."):
            current = getattr(current, part, None)
            if current is None:
                break
        if current is not None:
            return current
    return None


@contextmanager
def dagster_run_scope(context: Any, *, asset_key: str | None = None) -> Iterator[None]:
    """Bind Dagster run/job/partition identifiers for the contained block.

    Pass any Dagster execution context (``OpExecutionContext``,
    ``AssetExecutionContext``, sensor/schedule evaluation context, hook
    context). Identifiers are pulled from its public attributes.
    """
    run = _attr(context, "run", "dagster_run")
    values: dict[str, Any] = {
        "run_id": _attr(context, "run_id", "run.run_id", "dagster_run.run_id"),
        "job_id": _attr(context, "job_name", "dagster_run.job_name"),
        "job_name": _attr(context, "job_name", "dagster_run.job_name"),
        "partition_key": _attr(context, "partition_key", "run.partition_key"),
        "retry_number": _attr(context, "retry_number"),
        "asset_key": asset_key or _dagster_asset_key(context),
        "dagster_run_tags": _attr(run, "tags") if run else None,
    }
    with bind_context(**{k: v for k, v in values.items() if v is not None}):
        yield


def _dagster_asset_key(context: Any) -> str | None:
    key = _attr(context, "asset_key", "asset_key_for_output")
    if key is None or callable(key):
        # ``asset_key_for_output`` is a bound method needing an output name —
        # it cannot be called here, and str() of it would produce garbage.
        return None
    # AssetKey has a .path tuple; strings/others str() cleanly.
    path = getattr(key, "path", None)
    if path is not None:
        return ".".join(str(p) for p in path)
    return str(key)


def dagster_step(context: Any, name: str = E.PIPELINE_STEP, **kw: Any) -> observe:
    """Wrap an op/asset step. Emits ``pipeline.step`` with Dagster correlation."""
    op_name = _attr(context, "op.name", "node.name", "op_def.name")
    attrs = {"dagster_op": op_name} if op_name else {}
    attrs.update(kw.pop("attributes", None) or {})
    return observe(name, category=Category.PIPELINE, attributes=attrs, **kw)


def emit_materialization(
    context: Any,
    *,
    asset_key: str | None = None,
    rows: int | None = None,
    bytes_written: int | None = None,
    **attributes: Any,
) -> observe:
    """Wrap an asset materialization inside a Dagster step."""
    key = asset_key or _dagster_asset_key(context)
    run_id = _attr(context, "run_id", "run.run_id")
    attrs = {"rows_out": rows, "bytes_written": bytes_written, **attributes}
    entities: dict[str, Any] = {}
    if run_id:
        entities["run"] = run_id_for("dagster", run_id)
    if key:
        entities["asset"] = asset_id(key)
    return observe(
        E.ASSET_MATERIALIZE,
        category=Category.DATA,
        attributes={k: v for k, v in attrs.items() if v is not None},
        correlation={
            "run_id": run_id,
            "asset_key": key,
            "partition_key": _attr(context, "partition_key", "run.partition_key"),
        },
        entities=entities,
    )


def emit_asset_check(
    context: Any,
    *,
    check_name: str,
    passed: bool,
    severity: str | None = None,
    **attributes: Any,
) -> None:
    """Emit a ``quality.check`` event for a Dagster asset check result."""
    run_id = _attr(context, "run_id", "run.run_id")
    key = _dagster_asset_key(context)
    entities: dict[str, Any] = {}
    if run_id:
        entities["run"] = run_id_for("dagster", run_id)
    if key:
        entities["asset"] = asset_id(key)
    event(
        E.QUALITY_CHECK,
        category="quality",
        outcome="success" if passed else "failure",
        severity=severity or ("info" if passed else "error"),
        attributes={
            "check_name": check_name,
            "passed": passed,
            "dagster_op": _attr(context, "op.name", "node.name"),
            **attributes,
        },
        correlation={
            "run_id": run_id,
            "asset_key": key,
        },
        entities=entities,
    )
