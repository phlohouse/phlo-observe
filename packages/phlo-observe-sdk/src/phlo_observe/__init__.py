"""phlo-observe: Phlo-specific contexts and integrations on top of observe-core.

Quick start::

    from phlo_observe import phlo_run_context, pipeline_run

    with phlo_run_context(run_id="01J...", job="daily_ingestion", attempt=1):
        with pipeline_run(job="daily_ingestion") as evt:
            ...
"""

from phlo_observe import events
from phlo_observe.attributes import (
    AssetMaterializeAttributes,
    IcebergCommitAttributes,
    PipelineRunAttributes,
    QualityValidateAttributes,
    TrinoQueryAttributes,
    WapBranchAttributes,
    WapPromoteAttributes,
)
from phlo_observe.contexts import (
    asset_context,
    phlo_run_context,
    table_context,
    wap_context,
)
from phlo_observe.helpers import (
    asset_materialize,
    pipeline_run,
    quality_validate,
)
from phlo_observe.logging_bridge import configure_logging

__version__ = "1.0.0"

__all__ = [
    "AssetMaterializeAttributes",
    "IcebergCommitAttributes",
    "PipelineRunAttributes",
    "QualityValidateAttributes",
    "TrinoQueryAttributes",
    "WapBranchAttributes",
    "WapPromoteAttributes",
    "asset_context",
    "asset_materialize",
    "configure_logging",
    "events",
    "phlo_run_context",
    "pipeline_run",
    "quality_validate",
    "table_context",
    "wap_context",
]
