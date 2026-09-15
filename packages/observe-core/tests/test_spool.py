"""Critical-event spool: append, rotate, bound, replay oldest-first."""

from __future__ import annotations

import json

from observe_core.drains.memory import MemoryDrain
from observe_core.spool import Spool
from observe_core.stats import TelemetryStats


def _payload(i: int) -> bytes:
    return json.dumps(
        {"event_id": f"01J{i:021d}", "event": "wap.promote", "delivery": "critical"}
    ).encode()


class TestWrite:
    def test_append_and_replay(self, tmp_path):
        spool = Spool(tmp_path, max_bytes=10**6)
        assert spool.append(_payload(1))
        assert spool.append(_payload(2))
        drain = MemoryDrain()
        assert Spool(tmp_path).replay(drain) == 2
        assert len(drain.raw_payloads) == 2

    def test_empty_replay(self, tmp_path):
        assert Spool(tmp_path).replay(MemoryDrain()) == 0

    def test_segment_rotation_on_size(self, tmp_path):
        spool = Spool(tmp_path, max_bytes=10**6, segment_max_bytes=120)
        for i in range(10):
            assert spool.append(_payload(i))
        assert spool.pending_segments() > 1

    def test_max_bytes_bound_drop_newest(self, tmp_path):
        spool = Spool(tmp_path, max_bytes=500, on_full="drop_newest")
        results = [spool.append(_payload(i)) for i in range(20)]
        assert not all(results)  # writes refused once full
        assert spool.pending_bytes() <= 500 + 200  # one-line overshoot at most

    def test_max_bytes_evicts_oldest_sealed_segment(self, tmp_path):
        stats = TelemetryStats()
        spool = Spool(
            tmp_path,
            max_bytes=500,
            segment_max_bytes=120,
            on_full="drop_oldest",
            stats=stats,
        )
        for i in range(20):
            spool.append(_payload(i))
        assert stats.snapshot()["spool_dropped_oldest"] > 0


class TestReplay:
    def test_oldest_first_order(self, tmp_path):
        spool = Spool(tmp_path, segment_max_bytes=110)
        for i in range(6):
            spool.append(_payload(i))
        drain = MemoryDrain()
        Spool(tmp_path).replay(drain)  # fresh instance replays disk
        ids = [json.loads(p)["event_id"] for p in drain.raw_payloads]
        assert ids == [json.loads(_payload(i))["event_id"] for i in range(6)]

    def test_replay_replays_open_segment_same_instance(self, tmp_path):
        """Regression: replay must not skip the still-open segment.

        Previously ``self._current`` was skipped, so events appended by this
        instance were stranded until a size rotation sealed the segment.
        """
        spool = Spool(tmp_path)
        assert spool.append(_payload(1))
        assert spool.append(_payload(2))
        drain = MemoryDrain()
        assert spool.replay(drain) == 2
        assert len(drain.raw_payloads) == 2
        assert sorted(tmp_path.glob("seg-*.jsonl")) == []

    def test_replay_then_append_opens_fresh_segment(self, tmp_path):
        spool = Spool(tmp_path)
        spool.append(_payload(1))
        assert spool.replay(MemoryDrain()) == 1
        assert spool.append(_payload(2))  # next append must not fail
        assert spool.replay(MemoryDrain()) == 1

    def test_replay_removes_segments(self, tmp_path):
        Spool(tmp_path).append(_payload(1))
        Spool(tmp_path).replay(MemoryDrain())
        assert sorted(tmp_path.glob("seg-*.jsonl")) == []

    def test_failed_replay_keeps_segment(self, tmp_path):
        class FailingDrain(MemoryDrain):
            def emit_raw(self, payloads):
                raise RuntimeError("observer down")

        Spool(tmp_path).append(_payload(1))
        replayed = Spool(tmp_path).replay(FailingDrain())
        assert replayed == 0
        assert len(sorted(tmp_path.glob("seg-*.jsonl"))) == 1

    def test_transient_failure_stops_replay(self, tmp_path):
        """A transient failure keeps remaining segments for the next cycle."""
        from observe_core.drains.base import DrainFailure

        calls: list[list[bytes]] = []

        class FlakyDrain(MemoryDrain):
            def emit_raw(self, payloads):
                calls.append(list(payloads))
                raise DrainFailure("503 Service Unavailable")

        spool = Spool(tmp_path, segment_max_bytes=80)
        spool.append(_payload(1))
        spool.append(_payload(2))
        replayed = spool.replay(FlakyDrain())
        assert replayed == 0
        assert len(calls) == 1  # did not keep hammering later segments
        assert len(sorted(tmp_path.glob("seg-*.jsonl"))) == 2

    def test_permanent_rejection_quarantines_to_dead(self, tmp_path):
        """A permanently rejected segment is quarantined, not retried forever."""
        from observe_core.drains.base import PermanentDrainFailure

        class RejectingDrain(MemoryDrain):
            def emit_raw(self, payloads):
                raise PermanentDrainFailure("422 rejected")

        stats = TelemetryStats()
        spool = Spool(tmp_path, stats=stats)
        spool.append(_payload(1))
        assert spool.replay(RejectingDrain()) == 0
        assert sorted(tmp_path.glob("seg-*.jsonl")) == []
        assert sorted(tmp_path.glob("*.dead"))
        assert stats.snapshot()["spool_quarantined"] == 1

    def test_poison_segment_does_not_block_later_segments(self, tmp_path):
        """Regression: a rejected head segment must not wedge the whole spool."""
        from observe_core.drains.base import PermanentDrainFailure

        class SelectiveDrain(MemoryDrain):
            def emit_raw(self, payloads):
                if payloads == [_payload(1)]:
                    raise PermanentDrainFailure("rejected")
                super().emit_raw(payloads)

        spool = Spool(tmp_path, segment_max_bytes=80)
        spool.append(_payload(1))  # poison segment
        spool.append(_payload(2))
        drain = SelectiveDrain()
        assert spool.replay(drain) == 1
        assert drain.raw_payloads == [_payload(2)]
        assert sorted(tmp_path.glob("*.dead"))
        assert sorted(tmp_path.glob("seg-*.jsonl")) == []

    def test_corrupt_segment_quarantined(self, tmp_path):
        bad = tmp_path / "seg-9999999999-0.jsonl"
        bad.write_bytes(b"\x89\xff not json\n")
        drained = Spool(tmp_path).replay(MemoryDrain())
        assert drained == 0
        assert not bad.exists()
        assert sorted(tmp_path.glob("*.corrupt"))


class TestRuntimeIntegration:
    def test_spool_replayed_to_drain(self, make_runtime, tmp_path):
        spool_dir = tmp_path / "spool"
        Spool(spool_dir).append(_payload(7))
        rt = make_runtime(spool_enabled=True, spool_dir=spool_dir)
        drain = rt.drains[0]
        assert rt.spool is not None
        rt.spool.replay(drain)
        assert any(
            json.loads(p)["event_id"] == json.loads(_payload(7))["event_id"]
            for p in drain.raw_payloads
        )
