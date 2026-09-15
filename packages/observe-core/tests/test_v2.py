"""V2 Phase 1: contracts, schema registry, identifiers, propagation, backends."""

from __future__ import annotations

import pytest
from observe_core import (
    EventContract,
    bind_context,
    clear_contracts,
    event,
    field,
    flush,
    flush_metrics,
    metric,
    shutdown,
)
from observe_core.backends import CaptureBackend
from observe_core.contracts import (
    ContractViolation,
    all_contracts,
    get_contract,
    validate_event_attributes,
)
from observe_core.drains.memory import MemoryDrain
from observe_core.identifiers import (
    asset_id,
    entity_id,
    iceberg_id,
    is_entity_id,
    parse_entity_id,
    run_id_for,
)
from observe_core.models import Outcome
from observe_core.propagation import (
    CONTEXT_ARG,
    CONTEXT_ENV_VAR,
    bind_from_argv,
    bind_from_env,
    child_env,
    context_arg,
    decode_context,
    encode_context,
)
from observe_core.sampling import PolicySampler, Sampler, SamplingContext, SamplingRule
from observe_core.schema_registry import diff_contracts, export_schemas
from observe_core.tail import TailSampler


@pytest.fixture(autouse=True)
def _clean_contracts():
    clear_contracts()
    yield
    clear_contracts()


def _define_asset_contract() -> None:
    class AssetMaterialized(EventContract):
        name = "asset.materialized"
        version = 1
        description = "An asset was materialized."

        asset: str = field(description="Canonical asset key")
        rows: int = field(default=0, cardinality="bounded")
        credential: str | None = field(default=None, sensitive=True)


# -- contracts ---------------------------------------------------------------


def test_contract_registers_on_definition():
    _define_asset_contract()
    spec = get_contract("asset.materialized")
    assert spec is not None
    assert spec.schema_id == "asset.materialized/v1"
    assert len(spec.schema_hash) == 64
    assert spec.fields["asset"].required is True
    assert spec.fields["rows"].required is False
    assert spec.sensitive_fields == frozenset({"credential"})


def test_contract_requires_name_and_version():
    with pytest.raises(TypeError, match=r"name.*version"):

        class Bad(EventContract):
            name = "bad.event"


def test_contract_validate_attributes():
    _define_asset_contract()
    spec = get_contract("asset.materialized")
    assert spec.validate_attributes({"asset": "x", "rows": 5}) == []
    violations = spec.validate_attributes({"rows": "nope"})
    assert any("missing required field 'asset'" in v for v in violations)
    assert any("expected int" in v for v in violations)


def test_validate_event_attributes_modes():
    _define_asset_contract()
    assert validate_event_attributes("asset.materialized", {}, mode="off") == []
    assert validate_event_attributes("asset.materialized", {}, mode="warn")
    with pytest.raises(ContractViolation):
        validate_event_attributes("asset.materialized", {}, mode="strict")


def test_contract_json_schema_export():
    _define_asset_contract()
    schema = get_contract("asset.materialized").json_schema()
    assert schema["$id"].endswith("asset.materialized/v1")
    assert schema["properties"]["asset"]["type"] == "string"
    assert schema["properties"]["credential"]["x-sensitive"] is True
    assert "asset" in schema["required"]


def test_conflicting_re_registration_rejected():
    _define_asset_contract()
    with pytest.raises(ValueError, match="already registered"):
        # Same name+version with different fields must not silently re-register.

        class ClashAgain(EventContract):
            name = "asset.materialized"
            version = 1
            another: int


def test_new_version_coexists():
    _define_asset_contract()

    class V2(EventContract):
        name = "asset.materialized"
        version = 2
        asset: str

    assert get_contract("asset.materialized").version == 2
    assert get_contract("asset.materialized", 1).version == 1
    assert len([c for c in all_contracts() if c.name == "asset.materialized"]) == 2


# -- schema registry -----------------------------------------------------------


def test_export_schemas_deterministic():
    _define_asset_contract()
    doc = export_schemas()
    ids = [entry["schema_id"] for entry in doc["schemas"]]
    assert ids == sorted(ids)
    assert doc["registry_version"] == 1


def test_diff_contracts_classifies_changes():
    class Old(EventContract):
        name = "diff.test"
        version = 1
        keep: str
        gone: str
        widen: int
        tighten: str | None = None

    class New(EventContract):
        name = "diff.test"
        version = 2
        keep: str
        widen: float
        tighten: str
        extra: str | None = None

    diff = diff_contracts(get_contract("diff.test", 1), get_contract("diff.test", 2))
    assert "removed field 'gone'" in diff["breaking"]
    assert "added optional field 'extra'" in diff["non_breaking"]
    assert "field 'widen' widened type" in diff["non_breaking"]
    assert any("became required" in b for b in diff["breaking"])
    assert diff["compatible"] is False


def test_diff_optional_addition_compatible():
    class A(EventContract):
        name = "diff.ok"
        version = 1
        keep: str

    class B(EventContract):
        name = "diff.ok"
        version = 2
        keep: str
        extra: str | None = None

    diff = diff_contracts(get_contract("diff.ok", 1), get_contract("diff.ok", 2))
    assert diff["compatible"] is True
    assert diff["non_breaking"]


# -- identifiers ----------------------------------------------------------------


def test_entity_id_parse_and_build():
    ident = entity_id("asset", "silver", "samples")
    assert str(ident) == "asset://silver/samples"
    parsed = parse_entity_id("iceberg://cat/sch/tbl")
    assert parsed.namespace == "iceberg"
    assert parsed.parts == ("cat", "sch", "tbl")
    assert parsed.kind.value == "iceberg"
    assert is_entity_id("asset://x")
    assert not is_entity_id("not-a-uri")
    with pytest.raises(ValueError):
        parse_entity_id("nope")


def test_identifier_helpers():
    assert str(asset_id("silver", "samples")) == "asset://silver/samples"
    assert str(run_id_for("dagster", "01J")) == "run://dagster/01J"
    assert str(iceberg_id("cat", "sch", "tbl")) == "iceberg://cat/sch/tbl"
    with pytest.raises(ValueError):
        entity_id("BadNamespace", "x")


# -- propagation -----------------------------------------------------------------


def test_context_envelope_round_trip():
    with bind_context(run_id="r-1", trace_id="t-9", asset_key="a.b", environment="prod"):
        token = encode_context()
        decoded = decode_context(token)
        assert decoded["run_id"] == "r-1"
        assert decoded["trace_id"] == "t-9"
        assert decoded["asset_key"] == "a.b"
        assert decoded["environment"] == "prod"


def test_context_envelope_ignores_non_canonical():
    with bind_context(run_id="r-1", secret_token="do-not-propagate"):
        decoded = decode_context(encode_context())
        assert "secret_token" not in decoded


def test_decode_context_malformed_is_empty():
    assert decode_context("!!!not-base64!!!") == {}
    assert decode_context("") == {}


def test_child_env_and_bind_from_env():
    from observe_core.context import ambient_correlation

    with bind_context(run_id="r-child"):
        env = child_env({})
        assert env[CONTEXT_ENV_VAR]
    # bind_from_env is a permanent process-level bind (child-process entry).
    assert bind_from_env(env) is True
    assert ambient_correlation()["run_id"] == "r-child"


def test_bind_from_argv():
    with bind_context(run_id="r-argv"):
        token = context_arg()
        assert token
        assert bind_from_argv(["dbt", "run", CONTEXT_ARG, token]) is True
    assert not bind_from_argv(["dbt", "run"])


# -- backends --------------------------------------------------------------------


def test_sync_backend_delivers_inline(make_runtime):
    rt = make_runtime(runtime_backend="sync")
    drain = rt.drains[0]
    assert isinstance(drain, MemoryDrain)
    event("application.start")
    assert len(drain.events) == 1  # no flush needed: synchronous
    assert rt.health()["backend"] == "sync"


def test_capture_backend(make_runtime):
    rt = make_runtime(runtime_backend="capture")
    backend = rt.backend
    assert isinstance(backend, CaptureBackend)
    event("application.start")
    assert len(backend.payloads()) == 1
    backend.clear()
    assert backend.events == []


def test_backend_health_surface(make_runtime):
    rt = make_runtime()
    event("application.start")
    flush(2.0)
    health = rt.health()
    assert health["backend"] == "worker"
    assert health["workers_alive"] is True
    assert health["emitted"] >= 1
    assert "queue_capacity" in health


# -- policy sampling ---------------------------------------------------------------


def _ctx(**kw) -> SamplingContext:
    from observe_core.models import Delivery, Severity

    base = {
        "event": "app.log",
        "delivery": Delivery.TELEMETRY,
        "severity": Severity.INFO,
        "outcome": Outcome.SUCCESS,
        "duration_ms": None,
        "service": "svc",
        "environment": "test",
        "sample_key": "r-1",
    }
    base.update(kw)
    return SamplingContext(**base)


def test_policy_sampler_failures_kept():
    sampler = PolicySampler(Sampler(debug_rate=0.0, telemetry_rate=0.0))
    decision = sampler.decide(_ctx(outcome=Outcome.FAILURE))
    assert decision.keep is True


def test_policy_sampler_critical_never_sampled():
    from observe_core.models import Delivery

    sampler = PolicySampler(
        Sampler(debug_rate=0.0, telemetry_rate=0.0),
        [SamplingRule(name="all", keep=False)],
    )
    assert sampler.decide(_ctx(delivery=Delivery.CRITICAL)).keep is True


def test_policy_sampler_rule_rate_run_level():
    """A rate rule keeps/drops a whole run consistently."""
    sampler = PolicySampler(
        Sampler(debug_rate=1.0, telemetry_rate=1.0),
        [SamplingRule(name="half", event="app.*", rate=0.5)],
    )
    keep = sampler.decide(_ctx(sample_key="run-A")).keep
    assert sampler.decide(_ctx(sample_key="run-A")).keep == keep  # deterministic


def test_policy_sampler_decisions_recorded():
    sampler = PolicySampler(Sampler(debug_rate=1.0, telemetry_rate=1.0))
    sampler.decide(_ctx())
    assert len(sampler.recent_decisions()) == 1


def test_policy_sampler_via_settings(make_runtime):
    rt = make_runtime(
        sampling_policy=[{"name": "drop-app", "event": "app.*", "keep": False}],
    )
    event("app.log")
    event("other.log")
    flush(2.0)
    stats = rt.stats.snapshot()
    assert stats["dropped_sampled"] == 1
    assert stats["emitted_events"] == 1


# -- tail sampling ------------------------------------------------------------------


class _E:
    """Minimal CanonicalEvent stand-in for tail tests."""

    def __init__(self, data):
        self.data = data


def test_tail_sampler_buffers_boring_run():
    emitted = []
    tail = TailSampler(min_duration_ms=30_000, max_runs=100)
    corr = {"correlation": {"run_id": "r-1"}}
    tail.process(_E({**corr, "event": "run.step"}), emitted.append)
    tail.process(
        _E({**corr, "event": "run.completed", "duration_ms": 500, "outcome": "success"}),
        emitted.append,
    )
    assert emitted == []  # boring run dropped


def test_tail_sampler_keeps_failed_run():
    emitted = []
    tail = TailSampler(min_duration_ms=30_000, max_runs=100)
    tail.process(_E({"correlation": {"run_id": "r-1"}, "event": "run.step"}), emitted.append)
    tail.process(
        _E(
            {
                "correlation": {"run_id": "r-1"},
                "event": "run.failed",
                "outcome": Outcome.FAILURE.value,
            }
        ),
        emitted.append,
    )
    assert len(emitted) == 2


def test_tail_sampler_passes_through_without_run_id():
    emitted = []
    tail = TailSampler(min_duration_ms=30_000, max_runs=100)
    tail.process(_E({"event": "app.log"}), emitted.append)
    assert len(emitted) == 1


def test_tail_sampling_via_settings(make_runtime):
    rt = make_runtime(tail_sampling=True, tail_min_duration_ms=0)
    assert rt._tail is not None


# -- V2 envelope emission -----------------------------------------------------------


def test_emitted_envelope_is_v2(captured):
    _, drain = captured
    event("application.start")
    flush(2.0)
    (ev,) = [e.data for e in drain.events]
    assert ev["schema_version"] == "2.0"


def test_envelope_v1_setting_emits_v1(make_runtime):
    rt = make_runtime(envelope_version="1.0")
    drain = rt.drains[0]
    event("application.start")
    flush(2.0)
    (ev,) = [e.data for e in drain.events]
    assert ev["schema_version"] == "1.0"


def test_entities_and_tags_emit(captured):
    _, drain = captured
    event(
        "asset.materialized",
        entities={"subject": "asset://silver/samples"},
        tags={"team": "data"},
    )
    flush(2.0)
    (ev,) = [e.data for e in drain.events]
    assert ev["entities"] == {"subject": "asset://silver/samples"}
    assert ev["tags"] == {"team": "data"}


def test_contract_violation_warn_mode(captured):
    _define_asset_contract()
    _, drain = captured
    event("asset.materialized", attributes={"rows": "not-an-int"})
    flush(2.0)
    (ev,) = [e.data for e in drain.events]
    violations = ev["attributes"]["_observe"]["contract_violations"]
    assert any("asset" in v for v in violations)
    assert ev["contract"]["schema_id"] == "asset.materialized/v1"


def test_contract_violation_strict_raises(make_runtime):
    _define_asset_contract()
    make_runtime(contract_validation="strict")
    with pytest.raises(ContractViolation):
        event("asset.materialized", attributes={})
    shutdown(2.0)


def test_contract_sensitive_field_redacted(captured):
    _define_asset_contract()
    _, drain = captured
    event(
        "asset.materialized",
        attributes={"asset": "x", "credential": "hunter2"},
    )
    flush(2.0)
    (ev,) = [e.data for e in drain.events]
    assert ev["attributes"]["credential"] == "[REDACTED]"
    assert ev["attributes"]["asset"] == "x"


# -- aggregation ---------------------------------------------------------------------


def test_metric_aggregates_and_flushes(captured):
    _, drain = captured
    for value in (1.0, 2.0, 3.0):
        metric("rows.written", value, dimensions={"table": "t"})
    assert flush_metrics() == 1
    flush(2.0)
    (ev,) = [e.data for e in drain.events if e.data["event"] == "metric.summary"]
    assert ev["category"] == "metric"
    assert ev["attributes"]["count"] == 3
    assert ev["attributes"]["sum"] == 6.0
    assert ev["attributes"]["dimensions"] == {"table": "t"}


def test_metric_immediate_when_not_aggregating(captured):
    _, drain = captured
    metric("latency", 12.5, aggregate=False)
    flush(2.0)
    (ev,) = [e.data for e in drain.events]
    assert ev["event"] == "metric.recorded"
    assert ev["attributes"]["value"] == 12.5
