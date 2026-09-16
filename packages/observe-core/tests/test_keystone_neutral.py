"""Keystone compatibility (hardening item 12).

observe-core must express a non-Phlo domain — Keystone-style lab/ELN
operations — using only the generic envelope. If a Keystone concept needs
a Phlo-specific field to be representable, the core model is not neutral.
"""

from __future__ import annotations

from typing import Any

from observe_core import event


def _emitted(captured: Any) -> list[dict[str, Any]]:
    captured[0].flush(timeout=5.0)
    return [e.data for e in captured[1].events]


def test_keystone_operations_emit_cleanly(captured: Any) -> None:
    """experiment/assay/metadata/report/export lifecycles map onto the
    generic envelope with only domain attributes and correlation keys."""
    event(
        "experiment.process",
        category="application",
        outcome="success",
        correlation={"experiment_id": "EXP-1042"},
        entities={"experiment": "experiment://EXP-1042", "service": "service://keystone"},
        attributes={"plate_count": 4, "instrument": "liquid-handler-2"},
    )
    event(
        "assay.process",
        category="data",
        outcome="failure",
        severity="error",
        correlation={"experiment_id": "EXP-1042", "run_id": "assay-run-77"},
        entities={"assay": "assay://ELISA-9", "experiment": "experiment://EXP-1042"},
        attributes={"analyte": "IL-6"},
        error={"exception_type": "ReaderError", "message": "plate read failed"},
    )
    event(
        "metadata.validate",
        category="quality",
        outcome="success",
        correlation={"experiment_id": "EXP-1042"},
        attributes={"schema": "experiment-metadata/v3", "fields_checked": 42},
    )
    event(
        "pipeline.step",
        category="pipeline",
        outcome="success",
        correlation={"run_id": "assay-run-77", "job_id": "normalize"},
        attributes={"step": "background-subtract"},
        tags={"environment": "staging"},
    )
    event(
        "report.generate",
        category="application",
        outcome="success",
        correlation={"experiment_id": "EXP-1042", "request_id": "req-991"},
        attributes={"format": "pdf", "pages": 12},
    )
    event(
        "export.generate",
        category="data",
        outcome="success",
        correlation={"experiment_id": "EXP-1042"},
        attributes={"destination": "s3://keystone-exports/EXP-1042.zip"},
    )

    emitted = _emitted(captured)
    assert [e["event"] for e in emitted] == [
        "experiment.process",
        "assay.process",
        "metadata.validate",
        "pipeline.step",
        "report.generate",
        "export.generate",
    ]
    assay = emitted[1]
    assert assay["outcome"] == "failure"
    assert assay["correlation"]["experiment_id"] == "EXP-1042"
    assert assay["entities"]["assay"] == "assay://ELISA-9"
    assert emitted[3]["correlation"]["run_id"] == "assay-run-77"


def test_no_phlo_concepts_leak_into_core() -> None:
    """The generic package must not special-case Phlo vocabulary."""
    import inspect

    import observe_core.models as models

    src = inspect.getsource(models)
    # Phlo-specific nouns must not appear as model fields/constants.
    for leak in ("dagster", "dbt", "iceberg", "nessie", "trino", "pandera"):
        assert leak not in src.lower(), f"{leak} leaked into observe-core models"
