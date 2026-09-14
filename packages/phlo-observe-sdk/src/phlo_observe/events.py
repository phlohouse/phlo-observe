"""Canonical Phlo event-name registry.

Names follow the V1 vocabulary in ``docs/V1_SPEC.md`` §8: lowercase,
dot-delimited, no embedded IDs, no status suffixes. Import these constants
instead of spelling names by hand.
"""

from __future__ import annotations

APPLICATION_START = "application.start"
APPLICATION_STOP = "application.stop"
APPLICATION_LOG = "application.log"

PIPELINE_RUN = "pipeline.run"
PIPELINE_STEP = "pipeline.step"

ASSET_MATERIALIZE = "asset.materialize"

INGESTION_EXTRACT = "ingestion.extract"
INGESTION_LOAD = "ingestion.load"

TRANSFORM_EXECUTE = "transform.execute"

QUALITY_VALIDATE = "quality.validate"
QUALITY_CHECK = "quality.check"

WAP_BRANCH_CREATE = "wap.branch.create"
WAP_VALIDATE = "wap.validate"
WAP_PROMOTE = "wap.promote"
WAP_REJECT = "wap.reject"
WAP_CLEANUP = "wap.cleanup"

DBT_INVOCATION = "dbt.invocation"
DBT_MODEL_EXECUTE = "dbt.model.execute"
DBT_TEST_EXECUTE = "dbt.test.execute"

DLT_PIPELINE_RUN = "dlt.pipeline.run"

ICEBERG_COMMIT = "iceberg.commit"
ICEBERG_SNAPSHOT_CREATE = "iceberg.snapshot.create"

NESSIE_COMMIT = "nessie.commit"
NESSIE_BRANCH_CREATE = "nessie.branch.create"

TRINO_QUERY = "trino.query"

OBSERVER_INGEST = "observer.ingest"
OBSERVER_NORMALIZE = "observer.normalize"
OBSERVER_CORRELATE = "observer.correlate"
OBSERVER_EXPORT = "observer.export"

EVENT_NAMES: frozenset[str] = frozenset(
    name for name, value in list(globals().items()) if name.isupper() and isinstance(value, str)
)
"""Every registered canonical event name."""
