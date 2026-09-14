"""Iceberg/Nessie operation helpers.

Helpers capture the identifiers a caller already has — snapshot IDs, commit
hashes, branch names — without requiring table scans.
"""

from __future__ import annotations

from typing import Any

from observe_core import observe
from observe_core.models import Category

from phlo_observe import events as E
from phlo_observe.attributes import IcebergCommitAttributes


def iceberg_commit(
    *,
    table: str,
    catalog: str | None = None,
    namespace: str | None = None,
    branch: str | None = None,
    operation: str | None = None,
    snapshot_id_before: str | None = None,
    snapshot_id_after: str | None = None,
    commit_hash: str | None = None,
    schema_id: int | None = None,
    files_added: int | None = None,
    files_removed: int | None = None,
    rows_added: int | None = None,
    rows_removed: int | None = None,
    attributes: dict[str, Any] | None = None,
) -> observe:
    """Wrap an Iceberg table operation. Emits ``iceberg.commit`` on exit."""
    model = IcebergCommitAttributes(
        catalog=catalog,
        namespace=namespace,
        table=table,
        branch=branch,
        operation=operation,
        snapshot_id_before=snapshot_id_before,
        snapshot_id_after=snapshot_id_after,
        commit_hash=commit_hash,
        schema_id=schema_id,
        files_added=files_added,
        files_removed=files_removed,
        rows_added=rows_added,
        rows_removed=rows_removed,
    )
    attrs = model.attrs()
    if attributes:
        attrs.update(attributes)
    return observe(
        E.ICEBERG_COMMIT,
        category=Category.STORAGE,
        attributes=attrs,
        correlation={
            "table": table,
            "branch": branch,
            "snapshot_id": snapshot_id_after or snapshot_id_before,
        },
    )


def nessie_branch_create(*, branch: str, base_branch: str | None = None, **kw: Any) -> observe:
    """Emit ``nessie.branch.create`` around Nessie reference creation."""
    attrs = {"branch": branch, "base_branch": base_branch, **kw}
    return observe(
        E.NESSIE_BRANCH_CREATE,
        category=Category.STORAGE,
        attributes={k: v for k, v in attrs.items() if v is not None},
        correlation={"branch": branch},
    )
