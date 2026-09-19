"""Critical-event spool: append, rotate, bound, replay oldest-first."""

from __future__ import annotations

import json
import threading
from pathlib import Path

from observe_core.backends import DrainDelivery
from observe_core.drains.base import CanonicalEvent, DrainFailure
from observe_core.drains.memory import MemoryDrain
from observe_core.models import Delivery
from observe_core.spool import Spool
from observe_core.stats import TelemetryStats


def _payload(i: int) -> bytes:
    return json.dumps(
        {"event_id": f"01J{i:021d}", "event": "wap.promote", "delivery": "critical"}
    ).encode()


class _Remote(MemoryDrain):
    is_remote = True

    def __init__(self, endpoint: str, failing: bool = False):
        super().__init__()
        self.endpoint = endpoint
        self.failing = failing

    def emit_batch(self, events):
        if self.failing:
            raise DrainFailure("observer down")
        super().emit_batch(events)

    def emit_raw(self, payloads):
        if self.failing:
            raise DrainFailure("observer down")
        super().emit_raw(payloads)


def _event(i: int) -> CanonicalEvent:
    payload = _payload(i)
    return CanonicalEvent(json.loads(payload), payload, Delivery.CRITICAL)


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


class TestDestinationReplay:
    def test_failure_after_healthy_destination_replays_only_to_failed_destination(self, tmp_path):
        first = _Remote("https://one.example/ingest")
        second = _Remote("https://two.example/ingest", failing=True)
        delivery = DrainDelivery(
            [first, second], TelemetryStats(), Spool(tmp_path), replay_interval_s=0
        )

        delivery.deliver([_event(1)])
        assert len(first.events) == 1
        assert len(second.events) == 0
        second.failing = False
        delivery.maybe_replay()

        assert len(first.events) == 1
        assert len(second.raw_payloads) == 1

    def test_queue_overflow_spools_one_copy_for_each_destination(self, tmp_path):
        first = _Remote("https://one.example/ingest")
        second = _Remote("https://two.example/ingest")
        spool = Spool(tmp_path, max_bytes=10**6)
        delivery = DrainDelivery([first, second], TelemetryStats(), spool, replay_interval_s=0)

        assert delivery.spool_event(_event(2))
        assert len(list(tmp_path.glob("dest-*/seg-*.jsonl"))) == 2
        delivery.maybe_replay()
        assert len(first.raw_payloads) == 1
        assert len(second.raw_payloads) == 1

    def test_destination_spool_survives_restart_and_endpoint_change_isolated(self, tmp_path):
        old = _Remote("https://old.example/ingest")
        spool = Spool(tmp_path)
        delivery = DrainDelivery([old], TelemetryStats(), spool, replay_interval_s=0)
        assert delivery.spool_event(_event(3))

        replacement = _Remote("https://new.example/ingest")
        restarted = DrainDelivery(
            [replacement], TelemetryStats(), Spool(tmp_path), replay_interval_s=0
        )
        restarted.maybe_replay()
        assert replacement.raw_payloads == []
        assert list(tmp_path.glob("dest-*/seg-*.jsonl"))

    def test_shared_destination_capacity_keeps_existing_bound(self, tmp_path):
        first = _Remote("https://one.example/ingest")
        second = _Remote("https://two.example/ingest")
        spool = Spool(tmp_path, max_bytes=500, segment_max_bytes=120)
        delivery = DrainDelivery([first, second], TelemetryStats(), spool)
        for i in range(30):
            delivery.spool_event(_event(i))
        assert spool.pending_bytes() <= 500 + 200

    def test_shared_capacity_is_serialized_across_destinations(self, tmp_path):
        first = _Remote("https://one.example/ingest")
        second = _Remote("https://two.example/ingest")
        spool = Spool(tmp_path, max_bytes=200, on_full="drop_newest")
        delivery = DrainDelivery([first, second], TelemetryStats(), spool)
        barrier = threading.Barrier(3)

        def append(i):
            barrier.wait()
            delivery.spool_event(_event(i))

        threads = [threading.Thread(target=append, args=(i,)) for i in (1, 2)]
        for thread in threads:
            thread.start()
        barrier.wait()
        for thread in threads:
            thread.join()
        assert spool.pending_bytes() <= 200 + len(_payload(1)) + 1

    def test_unavailable_destination_directory_is_a_spool_failure(self, tmp_path, monkeypatch):
        spool = Spool(tmp_path)
        delivery = DrainDelivery([_Remote("https://one.example/ingest")], TelemetryStats(), spool)

        def fail_mkdir(*args, **kwargs):
            raise OSError("read-only")

        monkeypatch.setattr(Path, "mkdir", fail_mkdir)
        assert not delivery.spool_event(_event(4))
