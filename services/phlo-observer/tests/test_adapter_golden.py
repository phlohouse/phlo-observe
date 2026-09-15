"""Adapter fixture + golden tests (spec §81).

Each checked-in fixture payload under ``fixtures/`` is normalized by its
adapter and compared against the checked-in golden canonical events under
``golden/``. Volatile fields (``event_id``, ``observed_at``) are scrubbed
before comparison; their presence is asserted separately.

Regenerate goldens after an intentional mapping change with::

    PHLO_OBSERVER_WRITE_GOLDENS=1 pytest tests/test_adapter_golden.py
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest
from phlo_observer.adapters import ADAPTERS, RawPayload

FIXTURES = Path(__file__).parent / "fixtures"
GOLDENS = Path(__file__).parent / "golden"

# fixture filename -> (adapter name, golden filename)
CASES = {
    "canonical_events.json": ("canonical", "canonical_events.golden.json"),
    "dagster_events.json": ("dagster", "dagster_events.golden.json"),
    "dbt_run_results.json": ("dbt", "dbt_run_results.golden.json"),
    "dbt_artifacts_bundle.json": ("dbt", "dbt_artifacts_bundle.golden.json"),
    "otlp_logs.json": ("otlp", "otlp_logs.golden.json"),
    "otlp_observe_core.json": ("otlp", "otlp_observe_core.golden.json"),
    "generic_event.json": ("generic", "generic_event.golden.json"),
}


def _scrub(events: list[dict]) -> list[dict]:
    """Replace volatile fields with stable placeholders for comparison."""
    out = []
    for event in events:
        scrubbed = dict(event)
        scrubbed["event_id"] = "<event-id>"
        scrubbed["observed_at"] = "<observed-at>"
        out.append(scrubbed)
    return out


@pytest.mark.parametrize("fixture_name", sorted(CASES))
def test_adapter_fixture_golden(fixture_name: str) -> None:
    adapter_name, golden_name = CASES[fixture_name]
    adapter = ADAPTERS[adapter_name]
    body = (FIXTURES / fixture_name).read_bytes()
    payload = RawPayload(producer=adapter.name, source_kind=adapter.name, body=body)
    batch = adapter.normalize(payload)
    assert not batch.errors, f"unexpected normalization errors: {batch.errors}"
    assert batch.events, f"{adapter_name} produced no events for {fixture_name}"
    for event in batch.events:
        assert event["event_id"], "event_id must be generated"
        assert event["observed_at"], "observed_at must be generated"

    actual = _scrub(batch.events)
    golden_path = GOLDENS / golden_name
    if os.environ.get("PHLO_OBSERVER_WRITE_GOLDENS"):
        golden_path.write_text(json.dumps(actual, indent=2, sort_keys=True) + "\n")
        pytest.skip("golden regenerated")
    assert golden_path.exists(), f"missing golden {golden_name}"
    expected = json.loads(golden_path.read_text())
    assert actual == expected
