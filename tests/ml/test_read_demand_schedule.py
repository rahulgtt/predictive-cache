from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from predictive_cache.ml.cli import main as model_cli
from predictive_cache.ml.hashing import canonical_json
from predictive_cache.ml.logreg import LogRegScorer
from predictive_cache.ml.package import DEFAULT_SERVING_POLICY, write_package
from predictive_cache.ml.read_demand import (
    READ_DEMAND_FEATURE_SCHEMA_SHA256,
    READ_DEMAND_FEATURE_VERSION,
    READ_DEMAND_SERVING_CONTRACT,
    READ_DEMAND_SERVING_CONTRACT_SHA256,
    feature_schema,
    runtime_available_features,
)
from predictive_cache.ml.read_demand_schedule import (
    NoScheduleItems,
    SchedulePublishError,
    publish_read_demand_schedule,
)
from predictive_cache.semantics.read_demand import (
    SEMANTIC_VIEW_ID,
    SEMANTIC_VIEW_SHA256,
)

CUTOFF = datetime(2026, 11, 4, 8, 0, tzinfo=UTC)


def _scorer(feature: str = "read_count_24h") -> LogRegScorer:
    return LogRegScorer(
        (feature,),
        preprocess=[
            {
                "name": feature,
                "kind": "count",
                "fill": 0.0,
                "null_indicator": False,
                "mean": 0.0,
                "std": 1.0,
            }
        ],
        coef=[1.0],
        intercept=0.0,
        params={"C": 1.0, "max_iter": 100},
    )


def _rows(*, duplicate: bool = False) -> pa.Table:
    file_ids = ["file-a", "file-b", "file-c"]
    if duplicate:
        file_ids[-1] = "file-a"
    return pa.table(
        {
            "snapshot_id": [f"vol-1@{CUTOFF.isoformat()}"] * 3,
            "t": pa.array([CUTOFF] * 3, type=pa.timestamp("us", tz="UTC")),
            "split": ["val"] * 3,
            "volume_id": ["vol-1"] * 3,
            "file_id": file_ids,
            "read_count_24h": [10, 5, 1],
            "file_size_bytes": [1024, 2048, 8192],
            "label": [1, 0, 0],
            "sample_weight": [1.0, 1.0, 1.0],
        }
    )


def _snapshot(root: Path, *, duplicate: bool = False) -> Path:
    root.mkdir()
    (root / "feature_schema.json").write_text(canonical_json(feature_schema()))
    part = root / "snapshots/split=val/volume_id=vol-1/part-00000.parquet"
    part.parent.mkdir(parents=True)
    pq.write_table(_rows(duplicate=duplicate), part)
    capabilities = {"verified_file_size": True, "anchor_context": False}
    manifest = {
        "manifest_version": 1,
        "dataset_id": "snapshots-collected-pass1",
        "data_kind": "real",
        "stage": "snapshots",
        "parent": {"dataset_id": "clean-collected-pass1", "manifest_sha256": "a" * 64},
        "period": {
            "start": (CUTOFF - timedelta(days=2)).isoformat().replace("+00:00", "Z"),
            "end": (CUTOFF + timedelta(hours=1)).isoformat().replace("+00:00", "Z"),
        },
        "feature_version": READ_DEMAND_FEATURE_VERSION,
        "semantic_view_id": SEMANTIC_VIEW_ID,
        "semantic_view_sha256": SEMANTIC_VIEW_SHA256,
        "feature_schema_sha256": READ_DEMAND_FEATURE_SCHEMA_SHA256,
        "serving_contract": {
            "config": READ_DEMAND_SERVING_CONTRACT,
            "sha256": READ_DEMAND_SERVING_CONTRACT_SHA256,
        },
        "serving_contract_sha256": READ_DEMAND_SERVING_CONTRACT_SHA256,
        "runtime_capabilities": capabilities,
        "runtime_available_features": sorted(runtime_available_features(**capabilities)),
        "labels": {"target": "demand", "lead_s": 0, "horizon_s": 300},
        "splits": {
            "val_start": (CUTOFF - timedelta(hours=1)).isoformat().replace("+00:00", "Z"),
            "test_start": (CUTOFF + timedelta(minutes=30)).isoformat().replace("+00:00", "Z"),
        },
        "candidate_profile": SEMANTIC_VIEW_ID,
    }
    (root / "manifest.json").write_text(json.dumps(manifest, sort_keys=True))
    return root


def _package(
    root: Path,
    snapshot: Path,
    *,
    trained: bool | None = None,
    min_score: float | None = None,
) -> Path:
    rows = pq.ParquetFile(next((snapshot / "snapshots/split=val").rglob("*.parquet"))).read()
    policy = {
        **DEFAULT_SERVING_POLICY,
        "min_score": min_score,
        "max_items": 2,
        "max_bytes": 4096,
        "range_cap": 4096,
    }
    meta = {
        "semantic": {"semantic_view_id": SEMANTIC_VIEW_ID},
        "serving_contract": {"config": READ_DEMAND_SERVING_CONTRACT},
        "serving_policy": policy,
        "data_kind": "real",
        "data": {
            "snapshots_dataset_id": "training-snapshot",
            "snapshots_manifest_sha256": "b" * 64,
            "clean_dataset_id": "training-clean",
            "data_kind": "real",
        },
        "training": {
            "git_sha": "1" * 40,
            "config_sha256": "2" * 64,
            "selected_by": "val:recall@bytes_256MiB",
            "val_metrics": {"recall@bytes_256MiB": 0.5},
        },
    }
    if trained is not None:
        meta["trained"] = trained
    return write_package(
        _scorer(),
        feature_schema(),
        None,
        rows,
        meta,
        root,
    )


def _edit_package_policy(package: Path, **changes) -> None:
    root = package.with_suffix("")
    manifest = json.loads((root / "package.json").read_text())
    manifest["serving_policy"].update(changes)
    from predictive_cache.ml.package import seal_manifest

    seal_manifest(root, manifest)
    import zipfile

    with zipfile.ZipFile(package, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for path in sorted(root.iterdir()):
            archive.write(path, path.name)


def test_publishes_deterministic_handle_free_schedule(tmp_path):
    snapshot = _snapshot(tmp_path / "snapshot")
    package = _package(tmp_path / "package", snapshot)
    out = tmp_path / "schedule"

    first = publish_read_demand_schedule(
        snapshot,
        package,
        out,
        prediction_cutoff=CUTOFF,
        warm_start_offset_s=10,
        warm_spacing_s=5,
    )
    second = publish_read_demand_schedule(
        snapshot,
        package,
        out,
        prediction_cutoff=CUTOFF,
        warm_start_offset_s=10,
        warm_spacing_s=5,
    )

    assert first == second
    raw = json.loads(first.path.read_text())
    assert raw["package"]["trained_evidence"] == "legacy_training_metadata"
    assert raw["admitted_items"] == 2
    assert [item["file_id"] for item in raw["items"]] == ["file-a", "file-b"]
    assert [item["warm_offset_s"] for item in raw["items"]] == [10, 15]
    assert [item["logical_range"]["length"] for item in raw["items"]] == [1024, 2048]
    assert "target_fh_b64" not in first.path.read_text()
    identity = {key: value for key, value in raw.items() if key != "schedule_id"}
    expected = hashlib.sha256(canonical_json(identity).encode()).hexdigest()[:24]
    assert raw["schedule_id"] == f"schedule-{expected}"


def test_rejects_duplicate_identity_and_non_trained_package(tmp_path):
    duplicate = _snapshot(tmp_path / "duplicate", duplicate=True)
    package = _package(tmp_path / "package", duplicate)
    with pytest.raises(SchedulePublishError, match="duplicate prediction identity"):
        publish_read_demand_schedule(
            duplicate,
            package,
            tmp_path / "out",
            prediction_cutoff=CUTOFF,
        )

    snapshot = _snapshot(tmp_path / "snapshot")
    non_trained = _package(tmp_path / "non-trained", snapshot, trained=False)
    with pytest.raises(SchedulePublishError, match="not a trained"):
        publish_read_demand_schedule(
            snapshot,
            non_trained,
            tmp_path / "out-2",
            prediction_cutoff=CUTOFF,
        )


def test_zero_items_and_cli(tmp_path):
    snapshot = _snapshot(tmp_path / "snapshot")
    package = _package(tmp_path / "package", snapshot, trained=True, min_score=1.0)
    with pytest.raises(NoScheduleItems):
        publish_read_demand_schedule(
            snapshot,
            package,
            tmp_path / "out",
            prediction_cutoff=CUTOFF,
        )

    package = _package(tmp_path / "package-ok", snapshot, trained=True)
    result = CliRunner().invoke(
        model_cli,
        [
            "publish-read-demand-schedule",
            "--snapshot",
            str(snapshot),
            "--package",
            str(package),
            "--out",
            str(tmp_path / "cli-out"),
            "--prediction-cutoff",
            "2026-11-04T08:00:00Z",
        ],
    )
    assert result.exit_code == 0, result.output
    assert "schedule-" in result.output and "items=2" in result.output


def test_threshold_uses_raw_score_and_package_horizon(tmp_path):
    snapshot = _snapshot(tmp_path / "snapshot")
    package = _package(tmp_path / "package", snapshot, trained=True)
    _edit_package_policy(package, min_score=0.9999999999996)
    with pytest.raises(NoScheduleItems):
        publish_read_demand_schedule(
            snapshot,
            package,
            tmp_path / "out-threshold",
            prediction_cutoff=CUTOFF,
        )

    package = _package(tmp_path / "package-horizon", snapshot, trained=True)
    _edit_package_policy(package, horizon_s=3601)
    with pytest.raises(SchedulePublishError, match="package policy"):
        publish_read_demand_schedule(
            snapshot,
            package,
            tmp_path / "out-horizon",
            prediction_cutoff=CUTOFF,
        )
