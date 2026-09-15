"""Shared fixtures for the whole test suite."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent
SCHEMA_DIR = REPO_ROOT / "schemas"

# tests/workloads.py is shared scenario tooling for every suite.
_TESTS_DIR = str(REPO_ROOT / "tests")
if _TESTS_DIR not in sys.path:
    sys.path.insert(0, _TESTS_DIR)


@pytest.fixture
def repo_root() -> Path:
    """Repository root directory."""
    return REPO_ROOT


@pytest.fixture
def schema_dir() -> Path:
    """Directory containing the canonical JSON Schemas."""
    return SCHEMA_DIR


def _load_schema(name: str) -> dict:
    return json.loads((SCHEMA_DIR / name).read_text())


@pytest.fixture
def envelope_schema() -> dict:
    """The event-envelope-v2 JSON Schema (accepts 1.x and 2.x envelopes)."""
    return _load_schema("event-envelope-v2.schema.json")


@pytest.fixture
def envelope_schema_v1() -> dict:
    """The legacy event-envelope-v1 JSON Schema (V1 payload pin)."""
    return _load_schema("event-envelope-v1.schema.json")


@pytest.fixture
def error_schema() -> dict:
    """The error-v1 JSON Schema."""
    return _load_schema("error-v1.schema.json")


@pytest.fixture
def source_schema() -> dict:
    """The source-payload-v1 JSON Schema."""
    return _load_schema("source-payload-v1.schema.json")
