"""UUIDv7 generation tests."""

from __future__ import annotations

import uuid

from observe_core.ids import new_event_id, uuid7


def test_uuid7_is_valid_uuid():
    value = uuid7()
    assert isinstance(value, uuid.UUID)
    assert value.version == 7
    assert value.variant == uuid.RFC_4122


def test_uuid7_unique():
    ids = {uuid7() for _ in range(10_000)}
    assert len(ids) == 10_000


def test_uuid7_monotonic_within_process():
    ids = [uuid7() for _ in range(5_000)]
    assert ids == sorted(ids)


def test_new_event_id_str():
    assert isinstance(new_event_id(), str)
    uuid.UUID(new_event_id())
