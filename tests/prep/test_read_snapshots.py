"""Parity and end-to-end tests for the read-demand-v1 snapshot builder."""

from __future__ import annotations

import json
import math
from datetime import UTC, datetime, timedelta
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from predictive_cache.ml.read_demand import READ_DEMAND_FEATURES, ReadDemandEngine
from predictive_cache.ml.snapshots import load_split, load_universe, read_manifest
from predictive_cache.prep.read_snapshots import ReadSnapshotConfig, build_read_snapshots
from predictive_cache.semantics.read_demand import SEMANTIC_VIEW_ID, SEMANTIC_VIEW_SHA256

START = datetime(2026, 1, 5, tzinfo=UTC)
END = START + timedelta(days=5)
VOLUMES = ("vol-a", "vol-b")


def _events(seed: int = 3) -> list[dict]:
    rng = np.random.default_rng(seed)
    rows = []
    sizes = {}
    n = 0
    for volume in VOLUMES:
        files = [f"file-{volume[-1]}{i:02d}" for i in range(14)]
        for f in files:
            sizes[f] = int(rng.choice([4096, 65536, 1 << 20]))
        t = START + timedelta(minutes=5)
        while t < END - timedelta(minutes=10):
            a = files[int(rng.integers(0, 4))] if rng.random() < 0.6 else files[int(rng.integers(0, 14))]
            chain = [a]
            if a in files[:2] and rng.random() < 0.8:
                chain.append(files[int(a[-1]) + 2])  # planted co-read A -> B
            for f in chain:
                for _ in range(int(rng.integers(1, 3))):
                    offset = int(rng.choice([0, 4096])) if sizes[f] > 4096 else 0
                    nbytes = min(4096, sizes[f] - offset)
                    delay = float(rng.choice([0.5, 3.0, 30.0, -2.0, 700.0], p=[0.5, 0.3, 0.1, 0.05, 0.05]))
                    rows.append(
                        {
                            "schema_version": 1,
                            "event_id": f"ev-{n:06d}",
                            "event_time": t,
                            "known_at": t + timedelta(seconds=delay),
                            "volume_id": volume,
                            "file_id": f,
                            "dir_id": "dir-1",
                            "offset": offset,
                            "bytes_requested": nbytes,
                            "bytes_read": nbytes,
                            "file_size": sizes[f],
                            "origin": "warmer" if rng.random() < 0.03 else "foreground",
                            "demand_class": "fixture_demand",
                            "origin_trust": "fixture",
                            "coverage_status": "complete",
                            "metadata_status": "fixture",
                            "data_kind": "synthetic",
                            "protocol": "NFSv4.1",
                            "operation": "read",
                        }
                    )
                    n += 1
                    t += timedelta(seconds=int(rng.integers(5, 90)))
            t += timedelta(seconds=int(rng.integers(60, 1500)))
    for row in rows:
        if row["origin"] == "warmer":
            row["demand_class"] = "warmer"
    return rows


def _coverage() -> list[dict]:
    rows = []
    for volume in VOLUMES:
        hour = START - timedelta(days=8)
        while hour < END:
            status = "complete"
            if volume == "vol-b" and START + timedelta(days=2, hours=3) <= hour < START + timedelta(days=2, hours=5):
                status = "incomplete"
            if volume == "vol-a" and hour == START + timedelta(days=3, hours=7):
                status = "no_counters"
            rows.append({"volume_id": volume, "hour_start": hour, "status": status})
            hour += timedelta(hours=1)
    return rows


def _clean_dir(root: Path, events: list[dict], coverage: list[dict]) -> Path:
    root.mkdir(parents=True)
    for volume in VOLUMES:
        part = root / "read_events" / f"volume_id={volume}" / "part-00000.parquet"
        part.parent.mkdir(parents=True)
        pq.write_table(pa.Table.from_pylist([e for e in events if e["volume_id"] == volume]), part)
    (root / "coverage").mkdir()
    pq.write_table(pa.Table.from_pylist(coverage), root / "coverage" / "coverage.parquet")
    manifest = {
        "dataset_id": "clean-fixture",
        "data_kind": "synthetic",
        "semantic_view_id": SEMANTIC_VIEW_ID,
        "semantic_view_sha256": SEMANTIC_VIEW_SHA256,
        "period": {"start": START.isoformat(), "end": END.isoformat()},
    }
    (root / "manifest.json").write_text(json.dumps(manifest))
    return root


CFG = ReadSnapshotConfig(tick_s=3600, min_support=1, min_lift=0.0, min_negatives=2, neg_per_pos=2)


@pytest.fixture(scope="module")
def built(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("read-snapshots")
    events, coverage = _events(), _coverage()
    clean = _clean_dir(tmp / "clean", events, coverage)
    manifest = build_read_snapshots(clean, tmp / "snap", CFG)
    return tmp / "snap", manifest, events, coverage


def _engine(events: list[dict], coverage: list[dict]) -> ReadDemandEngine:
    engine = ReadDemandEngine()
    for row in coverage:
        engine.update_coverage(row["volume_id"], row["hour_start"], row["status"])
    for row in sorted(events, key=lambda r: (r["known_at"], r["event_id"])):
        engine.update(row)
    return engine


def _same(a, b) -> bool:
    if a is None or (isinstance(a, float) and math.isnan(a)):
        return b is None
    if b is None:
        return False
    return math.isclose(float(a), float(b), rel_tol=1e-9, abs_tol=1e-9)


def test_features_and_labels_match_engine(built):
    root, _, events, coverage = built
    engine = _engine(events, coverage)
    end = END
    rows = []
    for split in ("train", "val", "test"):
        rows += load_split(root, split, ("train", "val", "test")).to_pylist()
    assert len(rows) > 200
    mismatches = []
    for row in rows:
        t = row["t"]
        # The engine filters visibility itself; give it every event (as of the end).
        expected = engine.features(
            t, volume_id=row["volume_id"], file_id=row["file_id"], anchor_file_id=row["anchor_file_id"]
        )
        for name in READ_DEMAND_FEATURES:
            if not _same(row[name], expected[name]):
                mismatches.append((row["snapshot_id"], row["file_id"], name, row[name], expected[name]))
        label = engine.label(t, row["volume_id"], row["file_id"], as_of=end)
        if label != row["label"]:
            mismatches.append((row["snapshot_id"], row["file_id"], "label", row["label"], label))
    assert not mismatches, mismatches[:10]
    assert any(r["coread_from_anchor_24h"] for r in rows)
    assert any(r["secs_since_last_read"] is not None for r in rows)
    assert any("neighbor" in r["candidate_sources"] for r in rows)


def test_anchor_is_latest_visible_read_within_300s(built):
    root, _, events, _ = built
    eligible = [e for e in events if e["origin"] != "warmer"]
    for row in load_split(root, "val", ("val",)).to_pylist()[:300]:
        t = row["t"]
        visible = [
            e for e in eligible
            if e["volume_id"] == row["volume_id"] and e["known_at"] <= t
            and timedelta(0) <= t - e["event_time"] <= timedelta(seconds=300)
        ]
        latest = max(visible, key=lambda e: (e["event_time"], e["event_id"]), default=None)
        assert row["anchor_file_id"] == (latest["file_id"] if latest else None)


def test_manifest_universe_and_warmer_exclusion(built):
    root, manifest, events, _ = built
    assert read_manifest(root)["semantic_view_id"] == SEMANTIC_VIEW_ID
    universe = load_universe(root).to_pylist()
    ok = [u for u in universe if u["status"] == "ok"]
    assert ok and {u["split"] for u in ok} == {"train", "val", "test"}
    assert all(u["n_positive_in_candidates"] <= u["n_positive_files"] for u in ok)
    assert {"ok", "warmup"} <= {u["status"] for u in universe} <= {"ok", "warmup", "dropped_boundary", "dropped_period_end"}
    warmer_only = {e["file_id"] for e in events if e["origin"] == "warmer"} - {
        e["file_id"] for e in events if e["origin"] != "warmer"
    }
    files = {r["file_id"] for s in ("train", "val", "test") for r in load_split(root, s, (s,)).to_pylist()}
    assert not (warmer_only & files)
    train = load_split(root, "train").to_pylist()
    assert any(r["sample_weight"] > 1 for r in train)
    assert manifest["counts"]["train"]["rows"] == len(train)


def test_pcache_train_accepts_output(built, tmp_path):
    from predictive_cache.ml.package import load_package
    from predictive_cache.ml.train import TrainConfig, train
    from predictive_cache.semantics.read_demand import SEMANTIC_VIEW_SHA256 as SV

    root, *_ = built
    cfg = TrainConfig(models=["logreg-v1", "baseline-hotset"])
    summary = train(root, cfg, tmp_path / "out")
    entry = summary["models"]["logreg-v1"]
    from predictive_cache.ml.read_demand import (
        READ_DEMAND_FEATURE_SCHEMA_SHA256,
        READ_DEMAND_FEATURE_VERSION,
        READ_DEMAND_SERVING_CONTRACT_SHA256,
    )

    with load_package(
        Path(entry["package"]),
        READ_DEMAND_FEATURE_VERSION,
        READ_DEMAND_SERVING_CONTRACT_SHA256,
        expected_semantic_view_id=SEMANTIC_VIEW_ID,
        expected_semantic_view_sha256=SV,
        expected_feature_schema_sha256=READ_DEMAND_FEATURE_SCHEMA_SHA256,
        available_feature_names=set(READ_DEMAND_FEATURES),
    ) as package:
        rows = load_split(root, "val", ("val",))
        scores = package.score(rows)
    assert len(scores) == rows.num_rows and np.isfinite(scores).all()
