"""Contract tests for cleaning, handle completion, and observed coverage."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from predictive_cache.prep.clean.handles import HandleTracker
from predictive_cache.prep.clean.rules import classify_event
from predictive_cache.prep.config import CleanConfig
from predictive_cache.prep.coverage import Coverage, build_coverage

NOW = datetime(2026, 1, 5, 12, tzinfo=UTC)


def event(**changes):
    row = {
        "schema_version": 1,
        "event_id": "ev-1",
        "event_time": NOW,
        "ingest_time": NOW,
        "volume_id": "vol-1",
        "file_id": "file-1",
        "dir_id": "dir-1",
        "operation": "open",
        "handle_id": "hdl-1",
        "offset": None,
        "bytes_requested": None,
        "bytes_read": None,
        "handle_bytes_read": None,
        "handle_duration_ms": None,
        "file_size": 100,
        "result": "ok",
        "origin": "foreground",
        "session_id": None,
        "cohort_id": None,
        "client_id": None,
        "app": None,
        "cache_outcome": None,
        "latency_ms": None,
        "effective_time": NOW,
        "known_at": NOW,
    }
    row.update(changes)
    return row


def classify(row, cfg=None):
    # Effective and known time are cleaner additions, not source columns.
    source = {k: v for k, v in row.items() if k not in {"effective_time", "known_at"}}
    return classify_event(source, source["ingest_time"], cfg or CleanConfig())


def test_each_exclusion_rule_and_first_match():
    assert classify(event(file_id=None)) == "schema_invalid"
    assert classify(event(origin="warmer")) == "origin_warmer"
    assert classify(event(origin="synthetic")) == "origin_synthetic"
    assert classify(event(origin=None)) == "origin_unknown"
    assert classify(event(origin="unknown")) == "origin_unknown"
    invalid = event(operation="read", offset=-1, bytes_requested=5, bytes_read=0, result="einval")
    assert classify(invalid) == "invalid_range"
    assert classify(event(result="enoent")) == "result_failed"
    assert (
        classify(
            event(operation="read", offset=0, bytes_requested=5, bytes_read=None, result="eio")
        )
        == "result_failed"
    )
    assert classify(event(event_time=NOW + timedelta(hours=2))) == "time_invalid"
    assert classify(event()) is None


def test_handle_tracker_grace_timeout_and_late_read():
    cfg = CleanConfig(completion_grace_s=60, handle_timeout_s=3600)
    tracker = HandleTracker(cfg)
    opened = event()
    close = event(
        event_id="ev-2",
        operation="close",
        known_at=NOW + timedelta(seconds=10),
        effective_time=NOW + timedelta(seconds=10),
        handle_bytes_read=30,
    )
    early_read = event(
        event_id="ev-3",
        operation="read",
        known_at=NOW + timedelta(seconds=50),
        effective_time=NOW + timedelta(seconds=5),
        offset=0,
        bytes_requested=20,
        bytes_read=20,
    )
    late_read = event(
        event_id="ev-4",
        operation="read",
        known_at=NOW + timedelta(seconds=71),
        effective_time=NOW + timedelta(seconds=6),
        offset=20,
        bytes_requested=10,
        bytes_read=10,
    )
    tracker.push(opened)
    tracker.push(close)
    tracker.push(early_read)
    completed = tracker.advance(NOW + timedelta(seconds=71))[0]["row"]
    tracker.push(late_read)
    assert completed["completed_known_at"] == NOW + timedelta(seconds=70)
    assert completed["bytes_read"] == 20
    assert completed["bytes_inconsistent"] is True
    assert tracker.late_reads_after_completion == 1

    timeout = HandleTracker(cfg)
    timeout.push(opened)
    assert timeout.advance(NOW + timedelta(seconds=3599)) == []
    row = timeout.advance(NOW + timedelta(seconds=3600))[0]["row"]
    assert row["close_missing"] is True
    assert row["completed_known_at"] == NOW + timedelta(seconds=3600)


def test_ranges_union_and_summary_mode():
    ranges = HandleTracker(CleanConfig())
    ranges.push(event())
    ranges.push(
        event(event_id="ev-2", operation="read", offset=0, bytes_requested=20, bytes_read=20)
    )
    ranges.push(
        event(event_id="ev-3", operation="read", offset=10, bytes_requested=20, bytes_read=20)
    )
    row = ranges.flush()[0]["row"]
    assert row["bytes_read"] == 40
    assert row["distinct_bytes_read"] == 30

    summary = HandleTracker(CleanConfig(read_mode="summary"))
    summary.push(event())
    summary.push(event(event_id="ev-4", operation="close", handle_bytes_read=42))
    row = summary.flush()[0]["row"]
    assert row["bytes_read"] == 42
    assert row["distinct_bytes_read"] is None


def test_coverage_gaps_no_counters_and_skew_edges():
    period = {"start": NOW.isoformat(), "end": (NOW + timedelta(hours=2)).isoformat()}
    parent = {
        "period": period,
        "volumes": ["vol-1"],
        "known_gaps": [
            {
                "volume_id": "vol-1",
                "start": (NOW + timedelta(minutes=50)).isoformat(),
                "end": (NOW + timedelta(minutes=70)).isoformat(),
            }
        ],
    }
    counters = [
        {
            "volume_id": "vol-1",
            "hour_start": NOW,
            "offered": 10,
            "emitted": 10,
            "dropped": 0,
            "sampled_out": 0,
        },
        {
            "volume_id": "vol-1",
            "hour_start": NOW + timedelta(hours=1),
            "offered": 10,
            "emitted": 10,
            "dropped": 0,
            "sampled_out": 0,
        },
    ]
    cfg = CleanConfig()
    rows = build_coverage(parent, counters, cfg)
    assert [r["status"] for r in rows] == ["incomplete", "incomplete"]
    coverage = Coverage(rows, period, 600, False)
    assert coverage.is_observed("vol-1", NOW, NOW + timedelta(minutes=1)) is False
    assert coverage.is_observed("vol-1", NOW - timedelta(seconds=1), NOW) is False
    missing = Coverage(build_coverage({**parent, "known_gaps": []}, [], cfg), period, 600, False)
    assert missing.is_observed("vol-1", NOW, NOW + timedelta(minutes=1)) is False
    assumed = Coverage(missing.rows.values(), period, 600, True)
    assert assumed.is_observed("vol-1", NOW, NOW + timedelta(minutes=1)) is True


def test_coverage_margin_reaches_adjacent_hour():
    period = {"start": NOW.isoformat(), "end": (NOW + timedelta(hours=2)).isoformat()}
    rows = [
        {"volume_id": "vol-1", "hour_start": NOW, "status": "complete"},
        {"volume_id": "vol-1", "hour_start": NOW + timedelta(hours=1), "status": "incomplete"},
    ]
    coverage = Coverage(rows, period, 600, False)
    assert coverage.is_observed("vol-1", NOW + timedelta(minutes=40), NOW + timedelta(minutes=49))
    assert not coverage.is_observed(
        "vol-1", NOW + timedelta(minutes=49), NOW + timedelta(minutes=51)
    )


def test_read_at_completion_boundary_is_included():
    cfg = CleanConfig(completion_grace_s=60)
    tracker = HandleTracker(cfg)
    tracker.push(event())
    tracker.push(
        event(
            event_id="ev-2",
            operation="close",
            known_at=NOW + timedelta(seconds=10),
            handle_bytes_read=5,
        )
    )
    assert tracker.advance(NOW + timedelta(seconds=70, microseconds=-1)) == []
    tracker.push(
        event(
            event_id="ev-3",
            operation="read",
            known_at=NOW + timedelta(seconds=70),
            offset=0,
            bytes_requested=5,
            bytes_read=5,
        )
    )
    row = tracker.advance(NOW + timedelta(seconds=70))[0]["row"]
    assert row["bytes_read"] == 5
    assert tracker.late_reads_after_completion == 0


def test_missing_open_null_handles_and_all_reads_failed():
    tracker = HandleTracker(CleanConfig())
    tracker.push(
        event(
            event_id="ev-r1",
            operation="read",
            handle_id=None,
            offset=0,
            bytes_requested=5,
            bytes_read=5,
        )
    )
    tracker.push(
        event(
            event_id="ev-r2",
            operation="read",
            handle_id=None,
            offset=5,
            bytes_requested=5,
            bytes_read=5,
        )
    )
    rows = [update["row"] for update in tracker.flush()]
    assert {row["open_id"] for row in rows} == {"orphan-ev-r1", "orphan-ev-r2"}
    assert all(row["open_missing"] and row["close_missing"] for row in rows)

    tracker = HandleTracker(CleanConfig(read_mode="ranges"))
    tracker.push(event())
    tracker.push(event(event_id="ev-close", operation="close", handle_bytes_read=8))
    row = tracker.flush()[0]["row"]
    assert row["bytes_read"] == 0
    assert row["consumed"] is False
    assert row["bytes_inconsistent"] is True


def test_catalog_misses_keep_rename_identity_and_count_unknown():
    from predictive_cache.prep.clean.pipeline import _catalog_misses

    initial = [{"file_id": "file-1", "created_at": NOW - timedelta(days=1)}]
    kept = [
        event(event_id="ev-rename", operation="rename", file_id="file-1"),
        event(event_id="ev-create", operation="create", file_id="file-2"),
        event(event_id="ev-replace", operation="open", file_id="file-2"),
        event(event_id="ev-unknown", operation="open", file_id="file-3"),
    ]
    assert _catalog_misses(kept, initial, NOW) == {"files": 1, "events": 1}
