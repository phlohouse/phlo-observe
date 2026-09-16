"""Phlo operation helpers: preconfigured ``observe()`` blocks.

Each helper returns an :class:`observe_core.observe` context manager with the
canonical event name, category and correlation already wired, so call sites
stay one line::

    with pipeline_run(job="daily_ingestion", run_id=run_id, attempt=1) as evt:
        ...
        evt.set(assets=14)
"""

from __future__ import annotations

from typing import Any

from observe_core import observe
from observe_core.identifiers import branch_id
from observe_core.models import Category

from phlo_observe import events as E
from phlo_observe.attributes import (
    AssetMaterializeAttributes,
    PhloAttributes,
    PipelineRunAttributes,
    QualityValidateAttributes,
    WapPromoteAttributes,
)


def _merge_attrs(model: PhloAttributes, extra: dict[str, Any] | None) -> dict[str, Any]:
    attrs = model.attrs()
    if extra:
        attrs.update(extra)
    return attrs


def pipeline_run(
    *,
    job: str,
    run_id: str | None = None,
    attempt: int | None = None,
    trigger: str | None = None,
    assets: int | None = None,
    producer: str | None = None,
    attributes: dict[str, Any] | None = None,
) -> observe:
    """Wrap a pipeline/job run. Emits ``pipeline.run`` on exit."""
    attrs = _merge_attrs(
        PipelineRunAttributes(job=job, attempt=attempt, trigger=trigger, assets=assets),
        attributes,
    )
    return observe(
        E.PIPELINE_RUN,
        category=Category.PIPELINE,
        attributes=attrs,
        correlation={"run_id": run_id, "job_id": job},
        producer=producer,
    )


def asset_materialize(
    *,
    asset_key: str,
    partition_key: str | None = None,
    run_id: str | None = None,
    rows_in: int | None = None,
    rows_out: int | None = None,
    bytes_written: int | None = None,
    producer: str | None = None,
    attributes: dict[str, Any] | None = None,
) -> observe:
    """Wrap an asset materialization. Emits ``asset.materialize`` on exit."""
    attrs = _merge_attrs(
        AssetMaterializeAttributes(
            asset_key=asset_key,
            partition_key=partition_key,
            rows_in=rows_in,
            rows_out=rows_out,
            bytes_written=bytes_written,
        ),
        attributes,
    )
    return observe(
        E.ASSET_MATERIALIZE,
        category=Category.DATA,
        attributes=attrs,
        correlation={
            "asset_key": asset_key,
            "partition_key": partition_key,
            "run_id": run_id,
        },
        producer=producer,
    )


def quality_validate(
    *,
    suite: str | None = None,
    checks_total: int | None = None,
    checks_passed: int | None = None,
    checks_failed: int | None = None,
    rows_checked: int | None = None,
    rows_failed: int | None = None,
    run_id: str | None = None,
    asset_key: str | None = None,
    producer: str | None = None,
    attributes: dict[str, Any] | None = None,
) -> observe:
    """Wrap a validation run. Emits ``quality.validate`` on exit."""
    attrs = _merge_attrs(
        QualityValidateAttributes(
            suite=suite,
            checks_total=checks_total,
            checks_passed=checks_passed,
            checks_failed=checks_failed,
            rows_checked=rows_checked,
            rows_failed=rows_failed,
        ),
        attributes,
    )
    return observe(
        E.QUALITY_VALIDATE,
        category=Category.QUALITY,
        attributes=attrs,
        correlation={"run_id": run_id, "asset_key": asset_key},
        producer=producer,
    )


def wap_promote(
    *,
    branch: str,
    target: str = "main",
    commit_id: str | None = None,
    checks_passed: int | None = None,
    run_id: str | None = None,
    attributes: dict[str, Any] | None = None,
) -> observe:
    """Wrap a WAP promote decision. Emits ``wap.promote`` with ``critical`` delivery."""
    attrs = _merge_attrs(
        WapPromoteAttributes(
            branch=branch, target=target, commit_id=commit_id, checks_passed=checks_passed
        ),
        attributes,
    )
    return observe(
        E.WAP_PROMOTE,
        category=Category.WAP,
        delivery="critical",
        attributes=attrs,
        correlation={"branch": branch, "run_id": run_id},
        # WAP branches live in the Nessie namespace, matching the richer
        # helpers in ``integrations.wap``.
        entities={"branch": branch_id("nessie", branch)},
        producer="wap",
    )
