"""Shared fixtures for the whole test suite."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent
SCHEMA_DIR = REPO_ROOT / "schemas"


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
    """The event-envelope-v1 JSON Schema."""
    return _load_schema("event-envelope-v1.schema.json")


@pytest.fixture
def error_schema() -> dict:
    """The error-v1 JSON Schema."""
    return _load_schema("error-v1.schema.json")


@pytest.fixture
def source_schema() -> dict:
    """The source-payload-v1 JSON Schema."""
    return _load_schema("source-payload-v1.schema.json")
