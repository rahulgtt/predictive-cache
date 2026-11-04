"""Focused read-demand semantic and package tests."""

import json
from datetime import datetime, timedelta, timezone

import pytest

from predictive_cache.ml.dummy import make_dummy_package
from predictive_cache.ml.logreg import training_features
from predictive_cache.ml.package import PackageError, load_package
from predictive_cache.ml.read_demand import (
    READ_DEMAND_FEATURE_SCHEMA_SHA256,
    READ_DEMAND_FEATURES,
    READ_DEMAND_SERVING_CONTRACT,
    READ_DEMAND_SERVING_CONTRACT_SHA256,
    ReadDemandEngine,
    feature_schema,
    feature_schema_sha256,
    runtime_available_features,
    validate_feature_names,
)
from predictive_cache.ml.snapshots import (
    SnapshotError,
    build_read_demand_feature_rows,
    verify_read_demand_manifest,
)
from predictive_cache.ml.snapshots import (
    runtime_available_features as snapshot_runtime_available_features,
)
from predictive_cache.semantics.read_demand import (
    SEMANTIC_VIEW_ID,
    SEMANTIC_VIEW_SHA256,
    ReadDemandEvent,
)


def event(event_time, file_id="f", volume_id="v", event_id=None, **changes):
    row = {
        "event_id": event_id or f"event-{file_id}-{int(event_time.timestamp())}",
        "event_time": event_time,
        "known_at": event_time,
        "volume_id": volume_id,
        "file_id": file_id,
        "dir_id": "dir",
        "offset": 0,
        "bytes_requested": 10,
        "bytes_read": 10,
        "file_size": 100,
        "origin": "foreground",
        "demand_class": "foreground",
        "origin_trust": "trusted",
        "coverage_status": "complete",
        "metadata_status": "verified",
        "data_kind": "real",
        "protocol": "NFSv4.1",
        "operation": "read",
    }
    row.update(changes)
    return row


def covered(engine, start, end, volume_id="v", status="complete"):
    bucket = start.replace(minute=0, second=0, microsecond=0)
    while bucket <= end:
        engine.update_coverage(volume_id, bucket, status)
        bucket += timedelta(hours=1)


def test_names_disjoint_and_forbidden():
    assert not set(READ_DEMAND_FEATURES) & {"opens_1h", "secs_since_last_open"}
    with pytest.raises(ValueError):
        validate_feature_names(["open_count"])


def test_canonical_shape_rejects_mismatch():
    row = event(datetime(2026, 1, 1, tzinfo=timezone.utc))
    row["operation"] = "READ"
    with pytest.raises(ValueError):
        ReadDemandEvent.model_validate(row)


def test_replay_incremental_fixed_ticks():
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    rows = [event(start), event(start + timedelta(seconds=100), "g"), event(start + timedelta(seconds=200))]
    ticks = [start, start + timedelta(seconds=250)]
    replay = ReadDemandEngine()
    covered(replay, start - timedelta(days=7), ticks[-1])
    expected = replay.replay(rows, ticks, file_id="f", volume_id="v")
    live = ReadDemandEngine()
    covered(live, start - timedelta(days=7), ticks[-1])
    actual = []
    cursor = 0
    for tick in ticks:
        while cursor < len(rows) and rows[cursor]["known_at"] <= tick:
            live.update(rows[cursor])
            cursor += 1
        actual.append(live.features(tick, file_id="f", volume_id="v"))
    assert actual == expected


def test_known_at_leakage_and_coverage_unknown():
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    engine = ReadDemandEngine()
    covered(engine, start - timedelta(days=7), start)
    engine.update(event(start, known_at=start + timedelta(minutes=5)))
    assert engine.features(start, file_id="f", volume_id="v")["read_count_5m"] == 0
    incomplete = ReadDemandEngine()
    incomplete.update(event(start))
    assert incomplete.features(start, file_id="f", volume_id="v")["read_count_5m"] is None


def test_future_known_rows_cannot_change_non_window_features():
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    engine = ReadDemandEngine()
    covered(engine, start - timedelta(days=7), start)
    engine.update(event(start - timedelta(minutes=1), file_size=100))
    expected = engine.features(start, file_id="f", volume_id="v")
    engine.update(
        event(
            start,
            event_id="future-file",
            known_at=start + timedelta(minutes=5),
            file_size=999,
        )
    )
    engine.update(
        event(
            start,
            file_id="other",
            event_id="future-volume",
            known_at=start + timedelta(minutes=5),
        )
    )
    actual = engine.features(start, file_id="f", volume_id="v")
    assert actual["secs_since_last_read"] == expected["secs_since_last_read"]
    assert actual["file_size_bytes"] == 100
    assert actual["volume_read_share_1h"] == expected["volume_read_share_1h"]


def test_feature_windows_use_independent_coverage():
    tick = datetime(2026, 1, 1, 12, tzinfo=timezone.utc)
    engine = ReadDemandEngine()
    engine.update_coverage("v", tick - timedelta(hours=1), "complete")
    engine.update(event(tick - timedelta(minutes=1)))
    features = engine.features(tick, file_id="f", volume_id="v")
    assert features["read_count_5m"] == 1
    assert features["read_count_1h"] == 1
    assert features["read_count_24h"] is None
    assert features["read_count_7d"] is None


def test_coread_direct_predecessor_only_and_null_anchor():
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    engine = ReadDemandEngine()
    covered(engine, start - timedelta(days=7), start + timedelta(minutes=10))
    for row in (event(start, "a"), event(start + timedelta(seconds=100), "x"), event(start + timedelta(seconds=200))):
        engine.update(row)
    assert engine.features(start + timedelta(seconds=200), "v", "f", "a")["coread_from_anchor_24h"] == 0
    assert engine.features(start + timedelta(seconds=200), "v", "f")["coread_from_anchor_24h"] is None


def test_label_positive_negative_and_unknown():
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)
    engine = ReadDemandEngine()
    covered(engine, start, start + timedelta(seconds=300))
    engine.update(event(start + timedelta(seconds=100)))
    # A known successful READ proves a positive immediately; only negatives
    # require the complete elapsed horizon.
    assert engine.label(start, "v", "f", as_of=start + timedelta(seconds=100)) == 1
    negative = ReadDemandEngine()
    covered(negative, start, start + timedelta(seconds=300))
    assert negative.label(start, "v", "f", as_of=start + timedelta(seconds=300)) == 0
    assert ReadDemandEngine().label(start, "v", "f", as_of=start) is None


def test_legacy_package_loads(tmp_path):
    assert load_package(make_dummy_package(tmp_path), "fv1").package_id


def test_activation_and_semantic_mismatch(tmp_path):
    package = make_dummy_package(tmp_path)
    with pytest.raises(PackageError):
        load_package(package, "fv1", expected_semantic_view_id="read-demand-v1")
    with pytest.raises(PackageError):
        load_package(package, "fv1", available_feature_names={"x"})


def test_runtime_capabilities_and_training_registry_are_explicit():
    assert "file_size_bytes" not in runtime_available_features()
    assert "file_size_bytes" in runtime_available_features(verified_file_size=True)
    schema = feature_schema()
    assert "file_size_bytes" in training_features(schema)
    manifest = {
        "semantic_view_id": "read-demand-v1",
        "runtime_capabilities": {
            "verified_file_size": False,
            "anchor_context": True,
        },
        "runtime_available_features": sorted(runtime_available_features()),
    }
    assert snapshot_runtime_available_features(manifest) == set(
        runtime_available_features()
    )
    manifest["runtime_available_features"].append("file_size_bytes")
    with pytest.raises(ValueError):
        snapshot_runtime_available_features(manifest)


def test_snapshot_builder_uses_same_incremental_engine():
    start = datetime(2026, 1, 1, 12, tzinfo=timezone.utc)
    rows = [event(start), event(start + timedelta(minutes=1), file_id="g")]
    coverage = [
        {
            "volume_id": "v",
            "hour_start": start,
            "status": "complete",
        }
    ]
    ticks = [start, start + timedelta(minutes=2)]
    candidates = [{"volume_id": "v", "file_id": "f"}]
    offline = build_read_demand_feature_rows(rows, coverage, ticks, candidates)
    live = ReadDemandEngine()
    live.update_coverage("v", start, "complete")
    actual = []
    cursor = 0
    for tick in ticks:
        while cursor < len(rows) and rows[cursor]["known_at"] <= tick:
            live.update(rows[cursor])
            cursor += 1
        actual.append(
            {
                "t": tick,
                **candidates[0],
                **live.features(tick, volume_id="v", file_id="f"),
            }
        )
    assert offline == actual


def test_coordinated_schema_and_serving_mutations_are_rejected(tmp_path):
    schema_path = tmp_path / "feature_schema.json"
    schema_path.write_text(json.dumps(feature_schema(), sort_keys=True))
    manifest = {
        "semantic_view_id": SEMANTIC_VIEW_ID,
        "semantic_view_sha256": SEMANTIC_VIEW_SHA256,
        "feature_version": "read-demand-fv1",
        "feature_schema_sha256": READ_DEMAND_FEATURE_SCHEMA_SHA256,
        "serving_contract_sha256": READ_DEMAND_SERVING_CONTRACT_SHA256,
        "serving_contract": {
            "config": READ_DEMAND_SERVING_CONTRACT,
            "sha256": READ_DEMAND_SERVING_CONTRACT_SHA256,
        },
        "runtime_capabilities": {
            "verified_file_size": False,
            "anchor_context": True,
        },
        "runtime_available_features": sorted(runtime_available_features()),
    }
    verify_read_demand_manifest(tmp_path, manifest)

    mutated_schema = feature_schema()
    mutated_schema["features"][0]["semantic"] = "coordinated-forgery"
    schema_path.write_text(json.dumps(mutated_schema, sort_keys=True))
    forged_schema_manifest = {
        **manifest,
        "feature_schema_sha256": feature_schema_sha256(mutated_schema),
    }
    with pytest.raises(SnapshotError, match="not canonical"):
        verify_read_demand_manifest(tmp_path, forged_schema_manifest)

    schema_path.write_text(json.dumps(feature_schema(), sort_keys=True))
    mutated_contract = {**READ_DEMAND_SERVING_CONTRACT, "horizon_s": 301}
    forged_contract_manifest = {
        **manifest,
        "serving_contract_sha256": feature_schema_sha256(mutated_contract),
        "serving_contract": {
            "config": mutated_contract,
            "sha256": feature_schema_sha256(mutated_contract),
        },
    }
    with pytest.raises(SnapshotError, match="not canonical"):
        verify_read_demand_manifest(tmp_path, forged_contract_manifest)
