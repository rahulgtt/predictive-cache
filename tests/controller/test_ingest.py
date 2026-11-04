"""Ingress bounds, shared rules, causal completion and local checkpoint tests."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from predictive_cache.controller.ingest import Ingestor, instant, update_key
from predictive_cache.prep.config import CleanConfig

NOW = datetime(2026, 1, 5, 12, tzinfo=UTC)


def make(**kwargs):
    return Ingestor(CleanConfig(completion_grace_s=10), allowed_volumes={"vol-1"}, **kwargs)


def test_live_stamps_receipt_strictly_and_freezes_inputs(event):
    received = []
    core = make(sink=received.append)
    rows = [event(event_id="z", handle_id="z"), event(event_id="a", handle_id="a")]
    assert core.offer(rows, NOW) == (2, 0)
    rows[1]["file_size"] = 999
    assert core.drain(NOW) == []
    first = core.drain(NOW + timedelta(microseconds=1))
    assert first[0]["open_id"] == "a"
    assert first[0]["file_size"] == 100
    assert "bytes_read" not in first[0]
    assert core.offer([event(event_id="b", handle_id="b")], NOW) == (1, 0)
    rest = core.drain(NOW + timedelta(seconds=1))
    assert [u["known_at"] for u in first + rest] == [
        NOW,
        NOW + timedelta(microseconds=1),
        NOW + timedelta(microseconds=2),
    ]
    assert first + rest == received


def test_live_source_ingest_is_diagnostic_for_time_rules(event):
    core = make()
    core.offer([event(ingest_time=NOW + timedelta(days=100))], NOW)
    assert len(core.drain(NOW + timedelta(seconds=1))) == 1
    core.offer([event(event_id="late", event_time=NOW - timedelta(days=8))], NOW)
    core.drain(NOW + timedelta(seconds=2))
    assert core.stats()["excluded_by_reason"] == {"time_invalid": 1}


def test_bounds_nonblocking_counts_and_volume(event):
    core = make(queue_max=1, max_event_bytes=4096)
    assert core.offer([event(), event(event_id="overflow")], NOW) == (1, 1)
    assert core.offer([event(volume_id="other")], NOW) == (0, 1)
    assert core.offer([event(volume_id=["bad"])], NOW) == (0, 1)
    assert core.stats()["queue_depth"] == 1
    assert core.stats()["dropped_overflow"] == 1
    core._lock.acquire()
    try:
        assert core.offer([event()], NOW) == (0, 1)
    finally:
        core._lock.release()
    assert core.stats()["dropped_busy"] == 1
    core.drain(NOW + timedelta(seconds=1))
    assert core.offer([event(event_id="big", app="x" * 4097)], NOW) == (0, 1)
    assert core.stats()["dropped_event_too_large"] == 1


def test_shared_exclusions_and_duplicate_priority(event):
    core = make()
    rows = [
        event(event_id="a", origin="warmer"),
        event(event_id="b", origin="synthetic"),
        event(event_id="c", origin="unknown"),
        event(event_id="d", result="eio"),
        event(event_id="e", operation="read", offset=-1, bytes_requested=1, bytes_read=0),
        event(event_id="f", file_id=None),
    ]
    assert core.offer(
        sorted(rows + [event(event_id="a")], key=lambda r: r["event_id"]),
        NOW,
        replay=True,
    ) == (7, 0)
    assert core.drain(NOW + timedelta(seconds=1)) == []
    assert core.stats()["excluded_by_reason"] == {
        "origin_warmer": 1,
        "origin_synthetic": 1,
        "origin_unknown": 1,
        "result_failed": 1,
        "invalid_range": 1,
        "schema_invalid": 1,
        "duplicate": 1,
    }


def test_dedup_count_cap_window_and_invalid_first_delivery(event):
    core = make(dedupe_max=1)
    core.offer([event(event_id="a"), event(event_id="b", handle_id="b")], NOW)
    core.drain(NOW + timedelta(seconds=1))
    assert core.stats()["dedupe_capacity_evictions"] == 1
    assert core.stats()["dedupe_size"] == 1
    later = NOW + timedelta(hours=7)
    core.offer([event(event_id="b", event_time=later)], later)
    core.drain(later + timedelta(seconds=1))
    assert core.stats()["dedupe_expired"] == 1
    assert core.stats()["excluded_by_reason"].get("duplicate", 0) == 0

    core = make()
    core.offer([event(file_id=None)], NOW)
    core.offer([event()], NOW)
    assert core.drain(NOW + timedelta(seconds=1)) == []
    assert core.stats()["excluded_by_reason"] == {"schema_invalid": 1, "duplicate": 1}


def test_completion_sorted_by_deadline_and_kind_at_ties(event):
    core = make()
    rows = [
        event(event_id="open-a", handle_id="a"),
        event(event_id="open-z", handle_id="z"),
        event(
            event_id="close-z",
            operation="close",
            handle_id="z",
            ingest_time=NOW + timedelta(seconds=1),
        ),
        event(
            event_id="close-a",
            operation="close",
            handle_id="a",
            ingest_time=NOW + timedelta(seconds=2),
        ),
        event(
            event_id="meta",
            operation="write",
            offset=0,
            bytes_requested=10,
            ingest_time=NOW + timedelta(seconds=11),
        ),
        event(event_id="open-b", handle_id="b", ingest_time=NOW + timedelta(seconds=11)),
        event(
            event_id="read-z",
            operation="read",
            handle_id="z",
            offset=0,
            bytes_requested=10,
            bytes_read=10,
            ingest_time=NOW + timedelta(seconds=11),
        ),
    ]
    rows.sort(key=lambda row: (row["ingest_time"], row["event_id"]))
    core.offer(rows, replay=True)
    updates = core.drain(NOW + timedelta(seconds=13))
    assert updates == sorted(updates, key=update_key)
    completed = [u for u in updates if u["kind"] == "OpenCompleted"]
    assert [u["open_id"] for u in completed] == ["z", "a"]
    assert completed[0]["bytes_read"] == 10
    tied = [u["kind"] for u in updates if u["known_at"] == NOW + timedelta(seconds=11)]
    assert tied == ["MetaChanged", "OpenObserved", "OpenCompleted"]
    assert core.stats()["late_reads_after_completion"] == 0


def test_replay_same_timestamp_across_drains_and_future_queue(event):
    core = make()
    core.offer([event()], replay=True)
    assert core.drain(NOW) == []
    core.offer(
        [
            event(
                event_id="z",
                handle_id="z",
                operation="write",
                offset=0,
                bytes_requested=1,
            )
        ],
        replay=True,
    )
    updates = core.drain(NOW + timedelta(microseconds=1))
    assert [u["kind"] for u in updates] == ["MetaChanged", "OpenObserved"]
    assert core.offer([event(event_id="zz")], replay=True) == (0, 1)
    assert core.stats()["dropped_replay_order"] == 1
    with pytest.raises(ValueError, match="backwards"):
        core.advance(NOW)


def test_tracker_guard_bounds_and_signals_loss(event):
    core = make(tracker_max_events=3)
    core.offer([event(), event(event_id="second", handle_id="second")], NOW)
    assert len(core.drain(NOW + timedelta(seconds=1))) == 1
    stats = core.stats()
    assert stats["tracker_exhausted"] is True
    assert stats["tracker_discarded_handles"] == 1
    assert stats["tracker_size"] == 0
    core.offer([event(event_id="third", handle_id="third")], NOW)
    assert core.drain(NOW + timedelta(hours=2)) == []
    assert core.stats()["dropped_tracker_capacity"] == 2


def test_consumer_error_is_visible_and_offer_still_works(event):
    def broken(update):
        raise RuntimeError("test-only receiver failure")

    core = make(sink=broken)
    core.offer([event()], NOW)
    with pytest.raises(RuntimeError):
        core.drain(NOW + timedelta(seconds=1))
    assert core.stats()["consumer_errors"] == 1
    assert core.stats()["updates_lost_consumer_error"] == 1
    assert core.stats()["last_error"] == "RuntimeError"
    assert core.offer([event(event_id="another")], NOW) == (1, 0)


def test_checkpoint_roundtrip_queue_dedupe_partial_tracker_and_counters(event):
    core = make()
    core.offer([event()], NOW)
    core.drain(NOW + timedelta(seconds=1))
    core.offer([event(event_id="close", operation="close")], NOW + timedelta(seconds=2))
    restored = make()
    restored.restore(core.state())
    assert restored.stats() == core.stats()
    future = NOW + timedelta(hours=2)
    assert restored.drain(future) == core.drain(future)
    assert restored.stats() == core.stats()
    with pytest.raises(ValueError, match="contract"):
        make(queue_max=1).restore(core.state())
    with pytest.raises(ValueError):
        instant(NOW.replace(tzinfo=None))


def test_replay_expired_duplicate_exception_is_measured(event):
    core = make()
    core.offer([event()], replay=True)
    first = core.drain(NOW + timedelta(seconds=1))
    late = NOW + timedelta(hours=7)
    core.offer([event(handle_id="new-handle", event_time=late, ingest_time=late)], replay=True)
    second = core.drain(late + timedelta(seconds=1))
    # Test-only history identifies one duplicate accepted beyond retention.
    # The live collector intentionally cannot know a forgotten ID was seen.
    measured_window_misses = len([u for u in second if u["kind"] == "OpenObserved"])
    assert len(first) == measured_window_misses == 1
    assert core.stats()["dedupe_expired"] == 1


@pytest.mark.parametrize("after_completion", [False, True])
def test_repeated_open_is_visible_failclosed_and_checkpointed(event, after_completion):
    core = make()
    core.offer([event()], NOW)
    # Streamed prefix is real and cannot be retracted if the source later
    # violates the unique-handle lifecycle contract.
    assert len(core.drain(NOW + timedelta(seconds=1))) == 1
    again = NOW + timedelta(seconds=2)
    if after_completion:
        core.drain(NOW + timedelta(hours=2))
        again = NOW + timedelta(hours=2, seconds=1)
    core.offer([event(event_id="repeated", event_time=again)], again)
    assert core.drain(again + timedelta(seconds=1)) == []
    assert core.stats()["lifecycle_conflicts"] == 1
    assert core.stats()["tracker_failed"] is True
    assert core.stats()["tracker_failure_reason"] == "lifecycle_conflict"
    assert core.stats()["tracker_size"] == 0
    restored = make()
    restored.restore(core.state())
    assert restored.stats() == core.stats()
    restored.offer([event(event_id="later", handle_id="unique", event_time=again)], again)
    assert restored.drain(again + timedelta(seconds=2)) == []
    assert restored.stats()["dropped_tracker_failed"] == 1


def test_stats_snapshot_is_safe_during_drain_and_from_receiver(event):
    import sys
    from threading import Event, Thread

    core = make()
    core.offer([event(event_id=f"ev-{i:04}", handle_id=f"handle-{i:04}") for i in range(500)], NOW)
    errors = []
    stop = Event()

    def observe():
        while not stop.is_set():
            try:
                core.stats()
            except Exception as exc:
                errors.append(exc)
                break

    old_interval = sys.getswitchinterval()
    sys.setswitchinterval(0.000001)
    reader = Thread(target=observe)
    reader.start()
    try:
        core.sink = lambda update: core.stats()  # reentrant consumer snapshot
        assert len(core.drain(NOW + timedelta(seconds=1))) == 500
    finally:
        stop.set()
        reader.join(timeout=2)
        sys.setswitchinterval(old_interval)
    assert not reader.is_alive()
    assert not errors
