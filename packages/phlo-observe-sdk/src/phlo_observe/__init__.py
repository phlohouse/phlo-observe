"""phlo-observe: Phlo-specific contexts and integrations on top of observe-core.

Quick start::

    from phlo_observe import configure_phlo, observe

    configure_phlo(service_name="example")

    with observe("asset.materialize") as evt:
        ...
"""

from observe_core import (
    bind_context,
    clear_context,
    event,
    flush,
    get_context,
    observe,
    shutdown,
)
from observe_core.config import ObserveSettings

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
from phlo_observe.config import configure_phlo
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
    wap_promote,
)
from phlo_observe.logging_bridge import configure_logging

__version__ = "0.2.2"

__all__ = [
    "AssetMaterializeAttributes",
    "IcebergCommitAttributes",
    "ObserveSettings",
    "PipelineRunAttributes",
    "QualityValidateAttributes",
    "TrinoQueryAttributes",
    "WapBranchAttributes",
    "WapPromoteAttributes",
    "asset_context",
    "asset_materialize",
    "bind_context",
    "clear_context",
    "configure_logging",
    "configure_phlo",
    "event",
    "events",
    "flush",
    "get_context",
    "observe",
    "phlo_run_context",
    "pipeline_run",
    "quality_validate",
    "shutdown",
    "table_context",
    "wap_context",
    "wap_promote",
]
