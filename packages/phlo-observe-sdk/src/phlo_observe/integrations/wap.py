"""Write-audit-publish (WAP) helpers.

``wap.promote`` and ``wap.reject`` default to ``critical`` delivery: they are
consequential platform decisions that must not be silently dropped.
"""

from __future__ import annotations

from typing import Any

from observe_core import observe
from observe_core.identifiers import branch_id, snapshot_id_for, table_id
from observe_core.models import Category

from phlo_observe import events as E


def _wap_op(
    name: str,
    *,
    branch: str,
    delivery: str | None = None,
    base_branch: str | None = None,
    target_branch: str | None = None,
    table: str | None = None,
    snapshot_id: str | None = None,
    attributes: dict[str, Any] | None = None,
) -> observe:
    attrs: dict[str, Any] = {"branch": branch}
    if base_branch is not None:
        attrs["base_branch"] = base_branch
    if target_branch is not None:
        attrs["target"] = target_branch
    if table is not None:
        attrs["table"] = table
    if snapshot_id is not None:
        attrs["snapshot_id"] = snapshot_id
    if attributes:
        attrs.update(attributes)
    correlation: dict[str, Any] = {"branch": branch, "table": table, "snapshot_id": snapshot_id}
    entities: dict[str, Any] = {"branch": branch_id("nessie", branch)}
    if table:
        entities["table"] = table_id(table)
    if snapshot_id:
        entities["snapshot"] = snapshot_id_for(table or "unknown", snapshot_id)
    return observe(
        name,
        category=Category.WAP,
        delivery=delivery,
        attributes=attrs,
        correlation=correlation,
        entities=entities,
        producer="wap",
    )


def wap_branch_create(*, branch: str, base_branch: str | None = None, **kw: Any) -> observe:
    """Emit ``wap.branch.create`` around branch creation."""
    return _wap_op(E.WAP_BRANCH_CREATE, branch=branch, base_branch=base_branch, **kw)


def wap_validate(*, branch: str, **kw: Any) -> observe:
    """Emit ``wap.validate`` around WAP branch validation."""
    return _wap_op(E.WAP_VALIDATE, branch=branch, **kw)


def wap_promote(*, branch: str, target_branch: str = "main", **kw: Any) -> observe:
    """Emit ``wap.promote`` (critical) around the promote decision."""
    return _wap_op(
        E.WAP_PROMOTE, branch=branch, delivery="critical", target_branch=target_branch, **kw
    )


def wap_reject(*, branch: str, target_branch: str = "main", **kw: Any) -> observe:
    """Emit ``wap.reject`` (critical) around the reject decision."""
    return _wap_op(
        E.WAP_REJECT, branch=branch, delivery="critical", target_branch=target_branch, **kw
    )


def wap_cleanup(*, branch: str, **kw: Any) -> observe:
    """Emit ``wap.cleanup`` around branch cleanup."""
    return _wap_op(E.WAP_CLEANUP, branch=branch, **kw)
