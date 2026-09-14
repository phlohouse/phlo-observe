"""phlo-observe test fixtures."""

from __future__ import annotations

from collections.abc import Callable, Iterator

import pytest
from observe_core import ObserveSettings, clear_context, configure, shutdown
from observe_core.drains.memory import MemoryDrain
from observe_core.runtime import Runtime


@pytest.fixture
def make_runtime(tmp_path) -> Iterator[Callable[..., Runtime]]:
    """Factory building a configured runtime with a memory drain by default."""

    def _make(**overrides) -> Runtime:
        overrides.setdefault("service_name", "test-service")
        overrides.setdefault("environment", "test")
        overrides.setdefault("drains", [{"type": "memory"}])
        overrides.setdefault("spool_enabled", False)
        overrides.setdefault("flush_interval_ms", 50)
        overrides.setdefault("queue_capacity", 200)
        overrides.setdefault("spool_dir", tmp_path / "spool")
        return configure(ObserveSettings(**overrides))

    yield _make
    shutdown(timeout=2.0)
    clear_context()


@pytest.fixture
def captured(make_runtime) -> tuple[Runtime, MemoryDrain]:
    """A configured runtime plus the MemoryDrain receiving its events."""
    runtime = make_runtime()
    drain = runtime.drains[0]
    assert isinstance(drain, MemoryDrain)
    return runtime, drain
