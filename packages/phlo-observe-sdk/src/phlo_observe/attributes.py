"""Typed attribute payloads for common Phlo events.

Each model serializes to the ``attributes`` object of a canonical event via
:meth:`attrs`. All fields optional beyond the identifiers so partial data is
still emittable.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class PhloAttributes(BaseModel):
    """Base class: serialize to event attributes, dropping unset fields."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    def attrs(self) -> dict[str, Any]:
        """Return the attribute dict with unset fields omitted."""
        return self.model_dump(mode="json", by_alias=True, exclude_none=True)


class PipelineRunAttributes(PhloAttributes):
    """Attributes for ``pipeline.run``."""

    job: str
    attempt: int | None = None
    trigger: str | None = None
    assets: int | None = None


class AssetMaterializeAttributes(PhloAttributes):
    """Attributes for ``asset.materialize``."""

    asset_key: str
    partition_key: str | None = None
    rows_in: int | None = None
    rows_out: int | None = None
    bytes_written: int | None = None


class QualityValidateAttributes(PhloAttributes):
    """Attributes for ``quality.validate``."""

    suite: str | None = None
    checks_total: int | None = None
    checks_passed: int | None = None
    checks_failed: int | None = None
    rows_checked: int | None = None
    rows_failed: int | None = None
    failing_columns: list[str] | None = None
    failure_codes: list[str] | None = None
    lazy: bool | None = None


class WapPromoteAttributes(PhloAttributes):
    """Attributes for ``wap.promote``."""

    branch: str
    target: str = "main"
    commit_id: str | None = None
    checks_passed: int | None = None


class WapBranchAttributes(PhloAttributes):
    """Attributes for ``wap.branch.create`` / ``wap.cleanup``."""

    branch: str
    base_branch: str | None = None
    table: str | None = None
    snapshot_id: str | None = None


class IcebergCommitAttributes(PhloAttributes):
    """Attributes for ``iceberg.commit``."""

    catalog: str | None = None
    namespace: str | None = None
    table: str | None = None
    branch: str | None = None
    operation: str | None = None
    snapshot_id_before: str | None = None
    snapshot_id_after: str | None = None
    commit_hash: str | None = None
    schema_id: int | None = None
    files_added: int | None = None
    files_removed: int | None = None
    rows_added: int | None = None
    rows_removed: int | None = None


class TrinoQueryAttributes(PhloAttributes):
    """Attributes for ``trino.query``."""

    query_id: str | None = None
    catalog: str | None = None
    schema_: str | None = Field(default=None, alias="schema")
    query_class: str | None = None
    state: str | None = None
    rows_processed: int | None = None
    bytes_processed: int | None = None
    query_hash: str | None = None
    sql: str | None = None
    failure_code: str | None = None
    failure_type: str | None = None
