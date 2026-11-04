"""Canonical READ replay fixtures, never production demand or cache evidence."""

from datetime import UTC, datetime, timedelta

import pyarrow as pa
import pytest

from predictive_cache.controller.read_memory import CoverageObservation, ReadMemory
from predictive_cache.ml.read_demand import READ_DEMAND_FEATURES
from predictive_cache.ml.snapshots import build_read_demand_feature_rows
from predictive_cache.semantics.read_demand import ReadDemandEvent

T = datetime(2026, 11, 4, 12, tzinfo=UTC)


def read(event_id="e-1", file="file-a", *, event_time=None, known_at=None, size=4096, **extra):
    data = dict(
        event_id=event_id,
        event_time=event_time or T - timedelta(minutes=10),
        known_at=known_at or T - timedelta(minutes=9),
        volume_id="vol-1",
        file_id=file,
        dir_id="dir-1" if size is not None else None,
        offset=0,
        bytes_requested=16,
        bytes_read=16 if size != 0 else 0,
        file_size=size,
        origin="foreground",
        demand_class="fixture_demand",
        origin_trust="fixture",
        coverage_status="complete",
        metadata_status="fixture" if size is not None else "missing",
        data_kind="synthetic",
    )
    data.update(extra)
    return ReadDemandEvent.model_validate(data)


def coverage(memory, *, at=T, days=7):
    start = (at - timedelta(days=days)).replace(minute=0, second=0, microsecond=0)
    hours = int((at - start).total_seconds() // 3600)
    for index in range(hours):
        hour = start + timedelta(hours=index)
        obs = CoverageObservation(
            volume_id=memory.volume_id,
            hour_start=hour,
            observed_through=hour + timedelta(hours=1),
            known_at=at,
            status="complete",
        )
        assert memory.apply_coverage(obs, available_at=at)


def checkpoint(at, *, status="complete", hour=None, through=None):
    return CoverageObservation(
        volume_id="vol-1",
        hour_start=hour or T,
        observed_through=through or at,
        known_at=at,
        status=status,
    )


def test_features_match_offline_oracle_all_sixteen_and_metadata():
    memory = ReadMemory("vol-1", verified_file_size=True)
    rows = [
        read(),
        read(
            "e-2",
            "file-b",
            event_time=T - timedelta(seconds=20),
            known_at=T - timedelta(seconds=19),
        ),
        read("e-3", "file-a", event_time=T, known_at=T, size=8192),
    ]
    for row in rows:
        assert memory.apply_read(row, available_at=T)
    coverage(memory)
    result = memory.snapshot(T)
    source = [row.model_dump() for row in rows]
    coverage_rows = [
        dict(volume_id="vol-1", hour_start=T - timedelta(hours=i), status="complete")
        for i in range(1, 169)
    ]
    candidates = [
        dict(volume_id="vol-1", file_id=c.file_id, anchor_file_id=result.anchor_file_id)
        for c in result.candidates
    ]
    oracle = build_read_demand_feature_rows(source, coverage_rows, [T], candidates)
    assert result.complete and result.anchor_file_id == "file-a"
    assert memory.contract.canonical_read_profile()
    for actual, expected in zip(result.rows.to_pylist(), oracle, strict=True):
        assert {name: actual[name] for name in READ_DEMAND_FEATURES} == {
            name: expected[name] for name in READ_DEMAND_FEATURES
        }
    assert memory.latest_read("vol-1", "file-a", T).file_size == 8192
    assert result.is_eligible("vol-1", "file-a") and not result.is_eligible("vol-2", "file-a")


def test_known_at_equality_is_visible_later_rows_cannot_backdate():
    memory = ReadMemory("vol-1")
    before = memory.snapshot(T)
    assert before.rows.num_rows == 0
    later = read(known_at=T + timedelta(seconds=1))
    assert memory.apply_read(later, available_at=T + timedelta(seconds=1))
    after = memory.snapshot(T + timedelta(seconds=1))
    assert after.rows.num_rows == 1 and before.rows.num_rows == 0
    with pytest.raises(ValueError, match="watermark"):
        memory.snapshot(T)
    exact = ReadMemory("vol-1")
    assert exact.apply_read(read(known_at=T), available_at=T)
    assert exact.snapshot(T).rows.num_rows == 1
    assert not exact.apply_read(read("new-late", known_at=T), available_at=T + timedelta(seconds=1))
    assert exact.gap


def test_future_rows_and_clock_skew_are_refused_without_entering_history():
    memory = ReadMemory("vol-1")
    assert not memory.apply_read(read(known_at=T + timedelta(seconds=1)), available_at=T)
    assert memory.gap and memory.stats()["events"] == 0
    skew = ReadMemory("vol-1")
    assert not skew.apply_read(
        read(event_time=T + timedelta(seconds=1), known_at=T), available_at=T
    )
    assert skew.stats()["excluded_by_reason"] == {"time_invalid": 1}


def test_current_hour_coverage_expires_and_is_restored_by_new_checkpoint():
    memory = ReadMemory("vol-1")
    assert memory.apply_read(read(), available_at=T)
    coverage(memory)
    assert memory.snapshot(T).complete
    t1 = T + timedelta(seconds=1)
    assert memory.apply_coverage(checkpoint(t1), available_at=t1)
    first = memory.snapshot(t1)
    assert first.complete and first.rows["read_count_5m"][0].as_py() == 0
    t2 = t1 + timedelta(seconds=1)
    second = memory.snapshot(t2)
    assert not second.complete and second.rows["read_count_5m"][0].as_py() is None
    t3 = t2 + timedelta(seconds=1)
    assert memory.apply_coverage(checkpoint(t3), available_at=t3)
    third = memory.snapshot(t3)
    assert third.complete
    assert second.rows["read_count_5m"][0].as_py() is None


def test_late_hour_coverage_is_unknown_until_available():
    memory = ReadMemory("vol-1")
    assert memory.apply_read(read(), available_at=T)
    before = memory.snapshot(T)
    assert before.rows["read_count_24h"][0].as_py() is None
    t1 = T + timedelta(seconds=1)
    coverage(memory, at=t1)
    assert memory.apply_coverage(checkpoint(t1), available_at=t1)
    after = memory.snapshot(t1)
    assert after.complete and after.rows["read_count_24h"][0].as_py() == 1
    assert before.rows["read_count_24h"][0].as_py() is None


def test_twentyfour_hours_do_not_imply_seven_day_recency():
    memory = ReadMemory("vol-1")
    assert memory.apply_read(read(), available_at=T)
    coverage(memory, days=1)
    snap = memory.snapshot(T)
    assert snap.rows["read_count_24h"][0].as_py() == 1
    assert snap.rows["secs_since_last_read"][0].as_py() is None and not snap.complete


@pytest.mark.parametrize(
    "changes,reason",
    [
        ({"origin": "warmer", "demand_class": "warmer"}, "origin_warmer"),
        ({"origin": "unknown", "demand_class": "unknown"}, "origin_unknown"),
        (
            {"data_kind": "real", "origin_trust": "trusted", "demand_class": "foreground"},
            "operation_unsupported",
        ),
        ({"origin_trust": "unknown"}, "operation_unsupported"),
        ({"volume_id": "vol-2"}, "volume"),
    ],
)
def test_nonfixture_demand_never_enters_features(changes, reason):
    memory = ReadMemory("vol-1")
    assert not memory.apply_read(read(**changes), available_at=T)
    assert memory.snapshot(T).rows.num_rows == 0
    assert memory.stats()["excluded_by_reason"] == {reason: 1}


@pytest.mark.parametrize(
    "origin,demand_class,trust",
    [
        ("synthetic", "controlled_demand", "client_asserted"),
        ("foreground", "fixture_demand", "fixture"),
    ],
)
def test_synthetic_provenance_modes_remain_explicit(origin, demand_class, trust):
    memory = ReadMemory("vol-1")
    assert memory.apply_read(
        read(origin=origin, demand_class=demand_class, origin_trust=trust), available_at=T
    )
    assert memory.stats()["events_kept"] == 1


def test_duplicate_is_idempotent_conflict_latches_gap():
    memory = ReadMemory("vol-1")
    row = read()
    assert memory.apply_read(row, available_at=T)
    memory.snapshot(T)
    assert not memory.apply_read(row, available_at=T + timedelta(seconds=1))
    assert not memory.gap
    assert not memory.apply_read(read(size=8192), available_at=T + timedelta(seconds=1))
    assert memory.gap and memory.stats()["events"] == 1
    assert memory.latest_read("vol-1", "file-a", T).file_size == 4096


@pytest.mark.parametrize("options", [{"max_events": 1}, {"max_files": 1}])
def test_retention_never_evicts_when_capacity_exhausted(options):
    memory = ReadMemory("vol-1", **options)
    assert memory.apply_read(read(), available_at=T)
    assert not memory.apply_read(read("e-2", "file-b"), available_at=T)
    assert memory.gap and memory.stats()["events"] == 1
    assert memory.latest_read("vol-1", "file-a", T) is not None


def test_coverage_duplicate_conflict_capacity_and_no_future_evidence():
    memory = ReadMemory("vol-1", max_coverage=1)
    first = checkpoint(T, hour=T - timedelta(hours=1), through=T)
    assert memory.apply_coverage(first, available_at=T)
    assert not memory.apply_coverage(first, available_at=T) and not memory.gap
    conflict = first.model_copy(update={"status": "incomplete"})
    assert not memory.apply_coverage(conflict, available_at=T) and memory.gap
    second = checkpoint(T + timedelta(seconds=1))
    assert not memory.apply_coverage(second, available_at=T + timedelta(seconds=1))
    assert memory.stats()["coverage_observations"] == 1
    future = ReadMemory("vol-1")
    assert not future.apply_coverage(second, available_at=T)
    assert future.gap and future.stats()["coverage_observations"] == 0


@pytest.mark.parametrize(
    "changes",
    [
        {"hour_start": T + timedelta(seconds=1)},
        {"observed_through": T + timedelta(hours=2)},
        {"known_at": T - timedelta(seconds=1)},
        {"volume_id": "bad/path"},
    ],
)
def test_coverage_validation_rejects_invalid_extent_or_identity(changes):
    data = checkpoint(T).model_dump()
    data.update(changes)
    with pytest.raises(ValueError):
        CoverageObservation.model_validate(data)


def test_candidate_order_sources_caps_and_known_empty_filter():
    memory = ReadMemory("vol-1", max_candidates=3, recent_cap=2, popular_cap=2, neighbor_cap=2)
    rows = [
        read("e-0", "file-d", event_time=T - timedelta(hours=2), known_at=T - timedelta(hours=2)),
        read("e-1", "file-b"),
        read(
            "e-2",
            "file-c",
            event_time=T - timedelta(seconds=30),
            known_at=T - timedelta(seconds=29),
        ),
        read("e-3", "file-a", event_time=T, known_at=T),
    ]
    for row in rows:
        assert memory.apply_read(row, available_at=T)
    edges = pa.table(
        {
            "from_file_id": ["file-a", "file-a", "file-a"],
            "to_file_id": ["file-b", "file-d", "not-seen"],
            "conf": [0.7, 0.8, 0.1],
        }
    )
    snap = memory.snapshot(T, edges)
    assert [c.file_id for c in snap.candidates] == ["file-a", "file-c", "file-d"]
    assert snap.candidates[0].candidate_sources == ("recent", "popular")
    assert snap.candidates[2].candidate_sources == ("neighbor",)
    empty = ReadMemory("vol-1")
    assert empty.apply_read(read(size=0), available_at=T)
    assert empty.snapshot(T).candidates == ()


def test_unknown_size_stays_null_and_old_metadata_is_not_evicted():
    memory = ReadMemory("vol-1")
    assert memory.apply_read(read(size=None), available_at=T)
    snap = memory.snapshot(T)
    assert snap.candidates[0].file_size is None
    assert snap.rows["file_size_bytes"][0].as_py() is None
    assert memory.latest_read("vol-1", "file-a", T).file_size is None
    older = ReadMemory("vol-1")
    old = read(event_time=T - timedelta(days=8), known_at=T - timedelta(days=8))
    assert older.apply_read(old, available_at=T)
    assert older.latest_read("vol-1", "file-a", T).file_size == 4096


def test_anchor_tie_order_and_direct_predecessor_features():
    memory = ReadMemory("vol-1")
    rows = [
        read("a", "file-a", event_time=T - timedelta(seconds=2), known_at=T - timedelta(seconds=2)),
        read("b", "file-x", event_time=T - timedelta(seconds=1), known_at=T - timedelta(seconds=1)),
        read("c", "file-b", event_time=T, known_at=T),
        read("d", "file-a", event_time=T, known_at=T),
    ]
    for row in rows:
        assert memory.apply_read(row, available_at=T)
    coverage(memory)
    snapshot = memory.snapshot(T)
    assert snapshot.anchor_file_id == "file-a"
    by_file = {row["file_id"]: row for row in snapshot.rows.to_pylist()}
    assert by_file["file-b"]["coread_from_anchor_24h"] == 0
    assert by_file["file-x"]["coread_from_anchor_24h"] == 1
    assert memory.snapshot(T + timedelta(seconds=301)).anchor_file_id is None


def test_noncanonical_rows_bad_bounds_and_missing_metadata_capability():
    memory = ReadMemory("vol-1", verified_file_size=True)
    assert not memory.apply_read(read().model_dump() | {"path": "private"}, available_at=T)
    assert memory.gap
    metadata = ReadMemory("vol-1", verified_file_size=True)
    assert not metadata.apply_read(read(dir_id=None, metadata_status="missing"), available_at=T)
    assert metadata.gap
    for options in (
        {"max_events": 0},
        {"max_candidates": 501},
        {"recent_cap": 201},
        {"verified_file_size": 1},
    ):
        with pytest.raises(ValueError):
            ReadMemory("vol-1", **options)
    bad = ReadMemory("vol-1")
    assert bad.apply_read(read("z"), available_at=T)
    assert not bad.apply_read(read("a"), available_at=T)
    assert bad.gap
