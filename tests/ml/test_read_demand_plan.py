from __future__ import annotations

import base64
import json
import os
import subprocess
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pytest
from click.testing import CliRunner

import predictive_cache.ml.cli as ml_cli
from predictive_cache.datagen.io import canonical_json
from predictive_cache.dispatcher.core import Dispatcher, DispatcherConfig
from predictive_cache.dispatcher.schema import InvalidPlanItem, Plan
from predictive_cache.ml.baselines import write_read_demand_frequency_package
from predictive_cache.ml.cli import main as cli
from predictive_cache.ml.hashing import file_sha256
from predictive_cache.ml.package import load_package, seal_manifest
from predictive_cache.ml.read_demand import (
    READ_DEMAND_FEATURE_SCHEMA_SHA256,
    READ_DEMAND_FEATURE_VERSION,
    READ_DEMAND_SERVING_CONTRACT_SHA256,
)
from predictive_cache.ml.read_demand_plan import (
    ZERO_ITEMS_EXIT_CODE,
    NoAdmittedItems,
    PlanPublishError,
    _candidate_records,
)
from predictive_cache.ml.read_demand_plan import (
    publish_read_demand_plan as _publish_read_demand_plan,
)
from predictive_cache.ml.snapshots import (
    INFERENCE_TABLE_PATH,
    NARROW_CANDIDATE_PROFILE_ID,
    OMISSIONS_PATH,
    build_narrow_read_demand_snapshot,
    load_narrow_read_demand_snapshot,
    runtime_available_features,
)
from predictive_cache.semantics.read_demand import (
    SEMANTIC_VIEW_ID,
    SEMANTIC_VIEW_SHA256,
    ReadDemandEvent,
)

TICK = datetime(2026, 11, 4, 8, 0, tzinfo=UTC)
REPOSITORY_REVISION = "1" * 40


def publish_read_demand_plan(*args, **kwargs):
    kwargs.setdefault("expected_repository_revision", REPOSITORY_REVISION)
    kwargs.setdefault("expected_source_patch_sha256", None)
    return _publish_read_demand_plan(*args, **kwargs)


def _event(
    file_id: str,
    seconds_ago: int,
    *,
    event_id: str,
    volume_id: str = "vol-1",
    origin: str = "synthetic",
    demand_class: str = "controlled_demand",
) -> dict:
    event_time = TICK - timedelta(seconds=seconds_ago)
    return ReadDemandEvent(
        event_id=event_id,
        event_time=event_time,
        known_at=event_time,
        volume_id=volume_id,
        file_id=file_id,
        dir_id="dir-1",
        offset=0,
        bytes_requested=4096,
        bytes_read=4096,
        file_size=8192,
        origin=origin,
        demand_class=demand_class,
        origin_trust="client_asserted" if demand_class == "controlled_demand" else "fixture",
        coverage_status="complete",
        metadata_status="fixture",
        data_kind="synthetic",
    ).model_dump(mode="json")


def _fixture(
    root: Path,
    *,
    incomplete_volume: str | None = None,
    events: list[dict] | None = None,
) -> tuple[Path, Path]:
    root.mkdir(parents=True)
    source = root / "source-manifest.json"
    source_manifest = {
        "dataset_id": "source-fixture",
        "generator": {"git_sha": "source-revision-1"},
        "config_sha256": "a" * 64,
    }
    source.write_text(canonical_json(source_manifest) + "\n")

    clean = root / "clean"
    events_path = clean / "read_events/volume_id=vol-1/date=2026-11-04/part-00000.jsonl"
    coverage_path = clean / "coverage/coverage.jsonl"
    events_path.parent.mkdir(parents=True)
    coverage_path.parent.mkdir(parents=True)
    fixture_events = events or [
        _event("file-b", 60, event_id="b-1"),
        _event("file-a", 120, event_id="a-1"),
        _event("file-a", 30, event_id="a-2"),
        _event(
            "file-warmer",
            10,
            event_id="w-1",
            origin="warmer",
            demand_class="warmer",
        ),
        _event(
            "file-unknown",
            10,
            event_id="u-1",
            origin="unknown",
            demand_class="unknown",
        ),
        _event(
            "file-fixture",
            20,
            event_id="f-1",
            origin="foreground",
            demand_class="fixture_demand",
        ),
    ]
    events_path.write_text(
        "".join(canonical_json(row) + "\n" for row in fixture_events)
    )
    coverage = []
    for volume_id in sorted({row["volume_id"] for row in fixture_events}):
        for hour in range(24):
            bucket = TICK - timedelta(hours=24 - hour)
            coverage.append(
                {
                    "volume_id": volume_id,
                    "hour_start": bucket,
                    "offered": 1,
                    "emitted": 1,
                    "dropped": 0,
                    "sampled_out": 0,
                    "loss_fraction": 0.0,
                    "status": (
                        "incomplete"
                        if volume_id == incomplete_volume and hour == 0
                        else "complete"
                    ),
                }
            )
    coverage_path.write_text(
        "".join(canonical_json(row) + "\n" for row in coverage)
    )
    clean_manifest = {
        "manifest_version": 1,
        "dataset_id": "clean-fixture",
        "data_kind": "synthetic",
        "stage": "clean",
        "semantic_view_id": SEMANTIC_VIEW_ID,
        "semantic_view_sha256": SEMANTIC_VIEW_SHA256,
        "parent": {
            "dataset_id": source_manifest["dataset_id"],
            "manifest_sha256": file_sha256(source),
        },
        "files": [
            {
                "path": events_path.relative_to(clean).as_posix(),
                "rows": len(fixture_events),
                "sha256": file_sha256(events_path),
            },
            {
                "path": coverage_path.relative_to(clean).as_posix(),
                "rows": len(coverage),
                "sha256": file_sha256(coverage_path),
            },
        ],
    }
    (clean / "manifest.json").write_text(canonical_json(clean_manifest) + "\n")
    return clean, source


def _snapshot(root: Path, **fixture_kwargs) -> Path:
    clean, source = _fixture(root, **fixture_kwargs)
    output = root / "snapshot"
    build_narrow_read_demand_snapshot(clean, source, output, tick=TICK)
    return output


def _package(
    root: Path,
    snapshot: Path,
    *,
    max_items: int = 20,
    max_bytes: int = 256 * 2**20,
    range_cap: int = 16 * 2**20,
    min_score: float | None = None,
    repository_state: str = "clean",
    source_patch_sha256: str | None = None,
) -> Path:
    return write_read_demand_frequency_package(
        snapshot,
        root / "packages",
        repository_sha=REPOSITORY_REVISION,
        repository_state=repository_state,
        source_patch_sha256=source_patch_sha256,
        max_items=max_items,
        max_bytes=max_bytes,
        range_cap=range_cap,
        min_score=min_score,
    )


def _owner_target_map(
    root: Path,
    snapshot: Path,
    targets: list[dict],
) -> Path:
    root.mkdir(mode=0o700)
    os.chmod(root, 0o700)
    manifest = json.loads((snapshot / "manifest.json").read_text())
    value = {
        "schema_version": 1,
        "target_map_id": "fixture-target-map-1",
        "data_kind": "synthetic",
        "authorization": "approved-fixture-dry-run",
        "snapshot_id": manifest["dataset_id"],
        "snapshot_manifest_sha256": file_sha256(snapshot / "manifest.json"),
        "source_dataset_id": manifest["source"]["dataset_id"],
        "clean_dataset_id": manifest["clean"]["dataset_id"],
        "candidate_profile_sha256": manifest["candidate_profile_sha256"],
        "targets": targets,
    }
    path = root / "target-map.json"
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        os.write(fd, (canonical_json(value) + "\n").encode())
    finally:
        os.close(fd)
    return path


def _target(
    file_id: str,
    *,
    valid_until: datetime = TICK + timedelta(seconds=240),
    length: int = 4096,
    approved: bool = True,
) -> dict:
    return {
        "approved": approved,
        "volume_id": "vol-1",
        "file_id": file_id,
        "export_name": "fixture-export",
        "export_id": "fixture-export-id",
        "target_fh_b64": base64.urlsafe_b64encode(f"fh-{file_id}".encode())
        .rstrip(b"=")
        .decode(),
        "target_valid_until": valid_until.isoformat().replace("+00:00", "Z"),
        "offset": 0,
        "length": length,
    }


def _dispatcher_root(path: Path) -> Path:
    path.mkdir(mode=0o700)
    os.chmod(path, 0o700)
    return path


def test_snapshot_is_deterministic_narrow_and_read_only(tmp_path):
    first = _snapshot(tmp_path / "first")
    second = _snapshot(tmp_path / "second")
    first_manifest, rows = load_narrow_read_demand_snapshot(first)
    second_manifest, second_rows = load_narrow_read_demand_snapshot(second)

    assert first_manifest["dataset_id"] == second_manifest["dataset_id"]
    assert first_manifest["files"] == second_manifest["files"]
    assert rows.equals(second_rows)
    assert first_manifest["candidate_profile_id"] == NARROW_CANDIDATE_PROFILE_ID
    assert first_manifest["candidate_profile_sha256"] not in {
        SEMANTIC_VIEW_SHA256,
        READ_DEMAND_SERVING_CONTRACT_SHA256,
    }
    assert (
        first_manifest["snapshot_serving_contract_sha256"]
        != READ_DEMAND_SERVING_CONTRACT_SHA256
    )
    assert first_manifest["runtime_capabilities"] == {
        "verified_file_size": False,
        "anchor_context": False,
    }
    assert "file_size_bytes" not in rows.column_names
    assert "coread_from_anchor_24h" not in rows.column_names
    assert not any(
        token in column
        for column in rows.column_names
        for token in ("open", "close", "session", "lifecycle", "write")
    )
    assert rows.column("file_id").to_pylist() == [
        "file-a",
        "file-fixture",
        "file-b",
    ]
    assert rows.column("secs_since_last_read").to_pylist() == [30.0, 20.0, 60.0]
    assert file_sha256(first / INFERENCE_TABLE_PATH) == file_sha256(
        second / INFERENCE_TABLE_PATH
    )


def test_candidate_order_is_score_then_recency_then_file_and_volume():
    rows = pa.table(
        {
            "volume_id": ["vol-b", "vol-a", "vol-z", "vol-a"],
            "file_id": ["same", "same", "low-score", "recent"],
            "secs_since_last_read": [30.0, 30.0, 1.0, 10.0],
        }
    )

    ordered = _candidate_records(rows, [2.0, 2.0, 1.0, 2.0])

    assert [
        (row["file_id"], row["volume_id"]) for row in ordered
    ] == [
        ("recent", "vol-a"),
        ("same", "vol-a"),
        ("same", "vol-b"),
        ("low-score", "vol-z"),
    ]


def test_incomplete_coverage_is_omitted_with_reason(tmp_path):
    events = [_event("file-incomplete", 60, event_id="i-1", volume_id="vol-2")]
    snapshot = _snapshot(
        tmp_path / "input",
        incomplete_volume="vol-2",
        events=events,
    )
    _, rows = load_narrow_read_demand_snapshot(snapshot)
    omissions = [
        json.loads(line)
        for line in (snapshot / OMISSIONS_PATH).read_text().splitlines()
    ]
    assert rows.num_rows == 0
    assert omissions == [
        {
            "file_id": "file-incomplete",
            "reason": "required_coverage_incomplete",
            "required_features": ["read_count_24h"],
            "volume_id": "vol-2",
        }
    ]


def test_foreground_demand_does_not_inflate_fixture_features(tmp_path):
    events = [
        _event("file-shared", 60, event_id="controlled-1"),
        _event(
            "file-shared",
            50,
            event_id="foreground-1",
            origin="foreground",
            demand_class="foreground",
        ),
        _event(
            "file-shared",
            40,
            event_id="foreground-2",
            origin="foreground",
            demand_class="foreground",
        ),
    ]
    snapshot = _snapshot(tmp_path / "input", events=events)
    _, rows = load_narrow_read_demand_snapshot(snapshot)
    assert rows.column("file_id").to_pylist() == ["file-shared"]
    assert rows.column("read_count_24h").to_pylist() == [1]


def test_read_frequency_package_round_trip_and_identity(tmp_path):
    snapshot = _snapshot(tmp_path / "input")
    first = _package(tmp_path / "one", snapshot)
    second = _package(tmp_path / "two", snapshot)
    manifest = json.loads((first.with_suffix("") / "package.json").read_text())
    snapshot_manifest = json.loads((snapshot / "manifest.json").read_text())
    assert manifest["package_id"] == json.loads(
        (second.with_suffix("") / "package.json").read_text()
    )["package_id"]
    assert manifest["trained"] is False
    assert manifest["data_kind"] == "synthetic"
    assert manifest["training"]["repository_revision"] == REPOSITORY_REVISION
    assert manifest["training"]["repository_state"] == "clean"
    assert manifest["training"]["source_patch_sha256"] is None
    assert (
        manifest["data"]["candidate_profile_sha256"]
        == snapshot_manifest["candidate_profile_sha256"]
    )
    with load_package(
        first,
        READ_DEMAND_FEATURE_VERSION,
        READ_DEMAND_SERVING_CONTRACT_SHA256,
        expected_semantic_view_id=SEMANTIC_VIEW_ID,
        expected_semantic_view_sha256=SEMANTIC_VIEW_SHA256,
        expected_feature_schema_sha256=READ_DEMAND_FEATURE_SCHEMA_SHA256,
        available_feature_names=runtime_available_features(snapshot_manifest),
    ) as loaded:
        _, rows = load_narrow_read_demand_snapshot(snapshot)
        assert loaded.score(rows).tolist() == rows.column(
            "read_count_24h"
        ).to_pylist()
    verified = CliRunner().invoke(
        cli,
        ["verify", str(first), "--snapshot", str(snapshot)],
    )
    assert verified.exit_code == 0, verified.output
    changed_revision = write_read_demand_frequency_package(
        snapshot,
        tmp_path / "three/packages",
        repository_sha="2" * 40,
    )
    assert manifest["package_id"] != json.loads(
        (changed_revision.with_suffix("") / "package.json").read_text()
    )["package_id"]
    patched_one = _package(
        tmp_path / "patched-one",
        snapshot,
        repository_state="patched",
        source_patch_sha256="1" * 64,
    )
    patched_two = _package(
        tmp_path / "patched-two",
        snapshot,
        repository_state="patched",
        source_patch_sha256="2" * 64,
    )
    assert json.loads(
        (patched_one.with_suffix("") / "package.json").read_text()
    )["package_id"] != json.loads(
        (patched_two.with_suffix("") / "package.json").read_text()
    )["package_id"]
    with pytest.raises(ValueError, match="requires a canonical"):
        _package(tmp_path / "missing-patch", snapshot, repository_state="patched")
    with pytest.raises(ValueError, match="requires a canonical"):
        _package(
            tmp_path / "bad-patch",
            snapshot,
            repository_state="patched",
            source_patch_sha256="not-a-sha256",
        )


def test_cli_dirty_worktree_requires_sealed_patch_identity(tmp_path, monkeypatch):
    snapshot = _snapshot(tmp_path / "input")
    monkeypatch.setattr(
        ml_cli,
        "_git_source_provenance",
        lambda: (REPOSITORY_REVISION, "3" * 64),
    )
    missing = CliRunner().invoke(
        cli,
        [
            "build-read-frequency-package",
            "--snapshot",
            str(snapshot),
            "--out",
            str(tmp_path / "missing"),
        ],
    )
    assert missing.exit_code != 0
    assert "dirty worktree requires --source-patch-sha256" in missing.output
    supplied = CliRunner().invoke(
        cli,
        [
            "build-read-frequency-package",
            "--snapshot",
            str(snapshot),
            "--out",
            str(tmp_path / "supplied"),
            "--source-patch-sha256",
            "3" * 64,
        ],
    )
    assert supplied.exit_code == 0, supplied.output
    package = Path(supplied.output.strip())
    manifest = json.loads((package.with_suffix("") / "package.json").read_text())
    assert manifest["training"]["repository_state"] == "patched"
    assert manifest["training"]["source_patch_sha256"] == "3" * 64
    mismatch = CliRunner().invoke(
        cli,
        [
            "build-read-frequency-package",
            "--snapshot",
            str(snapshot),
            "--out",
            str(tmp_path / "mismatched-head"),
            "--repository-revision",
            "2" * 40,
            "--source-patch-sha256",
            "3" * 64,
        ],
    )
    assert mismatch.exit_code != 0
    assert "does not match the detected HEAD" in mismatch.output
    wrong_patch = CliRunner().invoke(
        cli,
        [
            "build-read-frequency-package",
            "--snapshot",
            str(snapshot),
            "--out",
            str(tmp_path / "wrong-patch"),
            "--source-patch-sha256",
            "4" * 64,
        ],
    )
    assert wrong_patch.exit_code != 0
    assert "does not match the canonical worktree patch" in wrong_patch.output
    with pytest.raises(ValueError, match="canonical Git object ID"):
        write_read_demand_frequency_package(
            snapshot,
            tmp_path / "bad-revision",
            repository_sha="not-a-revision",
        )


def test_git_source_provenance_includes_staged_tracked_changes(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    subprocess.run(["git", "init", "-q"], cwd=repo, check=True)
    subprocess.run(
        ["git", "config", "user.email", "fixture@example.invalid"],
        cwd=repo,
        check=True,
    )
    subprocess.run(
        ["git", "config", "user.name", "Fixture"],
        cwd=repo,
        check=True,
    )
    tracked = repo / "tracked.txt"
    tracked.write_text("base\n")
    subprocess.run(["git", "add", "tracked.txt"], cwd=repo, check=True)
    subprocess.run(
        ["git", "-c", "commit.gpgsign=false", "commit", "-qm", "base"],
        cwd=repo,
        check=True,
    )
    revision, clean_patch = ml_cli._git_source_provenance(repo)
    assert len(revision) in {40, 64}
    assert clean_patch is None

    tracked.write_text("staged change\n")
    subprocess.run(["git", "add", "tracked.txt"], cwd=repo, check=True)
    staged_revision, staged_patch = ml_cli._git_source_provenance(repo)
    assert staged_revision == revision
    assert staged_patch is not None
    assert len(staged_patch) == 64


def test_publish_applies_order_range_item_and_byte_budgets(tmp_path):
    snapshot = _snapshot(tmp_path / "input")
    package = _package(
        tmp_path / "package",
        snapshot,
        max_items=2,
        max_bytes=8192,
        range_cap=4096,
    )
    target_map = _owner_target_map(
        tmp_path / "targets",
        snapshot,
        [
            _target("file-a", length=8192),
            _target("file-b"),
            _target("file-fixture"),
        ],
    )
    dispatcher = _dispatcher_root(tmp_path / "dispatcher")
    published = publish_read_demand_plan(
        snapshot,
        package,
        target_map,
        dispatcher,
        run_id="run-fixture",
        created_at=TICK,
        deadline=TICK + timedelta(seconds=120),
    )
    raw = json.loads(published.plan_path.read_text())
    plan = Plan.model_validate(raw)
    assert not any(isinstance(item, InvalidPlanItem) for item in plan.items)
    assert [item.file_id for item in plan.items] == ["file-a", "file-fixture"]
    assert [item.length for item in plan.items] == [4096, 4096]
    assert all(item.max_bytes == item.length for item in plan.items)
    assert plan.max_bytes >= sum(item.length for item in plan.items)
    assert all(item.operation == "read" for item in plan.items)
    index = json.loads(published.index_path.read_text())
    assert index["plan_sha256"] == file_sha256(published.plan_path)
    assert index["provenance_sha256"] == file_sha256(published.provenance_path)
    provenance = json.loads(published.provenance_path.read_text())
    assert provenance["rejection_counts"]["max_items"] == 1

    marker = tmp_path / "warmer-started"
    warmer = tmp_path / "fake-warmer"
    warmer.write_text(f"#!/bin/sh\ntouch {marker}\nexit 99\n")
    os.chmod(warmer, 0o700)
    with Dispatcher(
        DispatcherConfig(
            root=dispatcher.absolute(),
            warmer_command=warmer.absolute(),
            host="192.0.2.10",
            real_reads=False,
            max_bytes_per_min=64 * 2**20,
            max_items_per_min=60,
        ),
        now=lambda: TICK,
    ) as service:
        service.run_once()
    results_dir = (
        dispatcher
        / "runs"
        / "run-fixture"
        / "plans"
        / published.plan_id
        / "results"
    )
    results = [
        json.loads(path.read_text()) for path in sorted(results_dir.glob("*.json"))
    ]
    assert len(results) == 2
    assert {
        (
            result["run_id"],
            result["plan_id"],
            result["status"],
            result["reason"],
            result["bytes_requested"],
            result["bytes_read"],
            result["bytes_read_is_lower_bound"],
        )
        for result in results
    } == {
        ("run-fixture", published.plan_id, "skipped", "dry_run", 4096, 0, False)
    }
    assert not marker.exists()


def test_publish_applies_cumulative_byte_budget(tmp_path):
    snapshot = _snapshot(tmp_path / "input")
    package = _package(
        tmp_path / "package",
        snapshot,
        max_items=3,
        max_bytes=4096,
        range_cap=4096,
    )
    target_map = _owner_target_map(
        tmp_path / "targets",
        snapshot,
        [_target("file-a"), _target("file-b"), _target("file-fixture")],
    )
    published = publish_read_demand_plan(
        snapshot,
        package,
        target_map,
        _dispatcher_root(tmp_path / "dispatcher"),
        run_id="run-byte-budget",
        created_at=TICK,
        deadline=TICK + timedelta(seconds=120),
    )
    plan = Plan.model_validate(json.loads(published.plan_path.read_text()))
    assert [item.file_id for item in plan.items] == ["file-a"]
    provenance = json.loads(published.provenance_path.read_text())
    assert provenance["rejection_counts"]["max_bytes"] == 2


@pytest.mark.parametrize(
    "binding",
    [
        "source_dataset_id",
        "snapshots_dataset_id",
        "candidate_profile_sha256",
    ],
)
def test_package_identity_mismatch_blocks_publication(tmp_path, binding):
    snapshot = _snapshot(tmp_path / "input")
    package = _package(tmp_path / "package", snapshot)
    root = package.with_suffix("")
    manifest = json.loads((root / "package.json").read_text())
    manifest["data"][binding] = "mismatch"
    seal_manifest(root, manifest)
    target_map = _owner_target_map(
        tmp_path / "targets",
        snapshot,
        [_target("file-a")],
    )
    dispatcher = _dispatcher_root(tmp_path / "dispatcher")
    with pytest.raises(PlanPublishError, match="binding mismatch"):
        publish_read_demand_plan(
            snapshot,
            root,
            target_map,
            dispatcher,
            run_id="run-mismatch",
            created_at=TICK,
            deadline=TICK + timedelta(seconds=120),
        )
    assert not (dispatcher / "plans").exists()


def test_invalid_package_source_provenance_blocks_publication(tmp_path):
    snapshot = _snapshot(tmp_path / "input")
    package = _package(tmp_path / "package", snapshot)
    root = package.with_suffix("")
    manifest = json.loads((root / "package.json").read_text())
    manifest["training"]["repository_state"] = "patched"
    manifest["training"]["source_patch_sha256"] = "not-a-sha256"
    seal_manifest(root, manifest)
    target_map = _owner_target_map(
        tmp_path / "targets",
        snapshot,
        [_target("file-a")],
    )
    dispatcher = _dispatcher_root(tmp_path / "dispatcher")
    with pytest.raises(PlanPublishError, match="explicit non-trained"):
        publish_read_demand_plan(
            snapshot,
            root,
            target_map,
            dispatcher,
            run_id="run-bad-source-provenance",
            created_at=TICK,
            deadline=TICK + timedelta(seconds=120),
        )
    assert not (dispatcher / "plans").exists()


@pytest.mark.parametrize(
    ("package_kwargs", "expected_revision", "expected_patch"),
    [
        ({}, "2" * 40, None),
        (
            {
                "repository_state": "patched",
                "source_patch_sha256": "1" * 64,
            },
            REPOSITORY_REVISION,
            "2" * 64,
        ),
    ],
)
def test_unapproved_package_source_provenance_blocks_publication(
    tmp_path,
    package_kwargs,
    expected_revision,
    expected_patch,
):
    snapshot = _snapshot(tmp_path / "input")
    package = _package(tmp_path / "package", snapshot, **package_kwargs)
    target_map = _owner_target_map(
        tmp_path / "targets",
        snapshot,
        [_target("file-a")],
    )
    dispatcher = _dispatcher_root(tmp_path / "dispatcher")
    with pytest.raises(PlanPublishError, match="source provenance mismatch"):
        publish_read_demand_plan(
            snapshot,
            package,
            target_map,
            dispatcher,
            run_id="run-unapproved-source",
            created_at=TICK,
            deadline=TICK + timedelta(seconds=120),
            expected_repository_revision=expected_revision,
            expected_source_patch_sha256=expected_patch,
        )
    assert not (dispatcher / "plans").exists()


def test_target_map_snapshot_mismatch_blocks_publication(tmp_path):
    snapshot = _snapshot(tmp_path / "input")
    package = _package(tmp_path / "package", snapshot)
    target_map = _owner_target_map(
        tmp_path / "targets",
        snapshot,
        [_target("file-a")],
    )
    value = json.loads(target_map.read_text())
    value["snapshot_id"] = "different-snapshot"
    target_map.write_text(canonical_json(value) + "\n")
    os.chmod(target_map, 0o600)
    with pytest.raises(PlanPublishError, match="target map.*mismatch"):
        publish_read_demand_plan(
            snapshot,
            package,
            target_map,
            _dispatcher_root(tmp_path / "dispatcher"),
            run_id="run-mismatch",
            created_at=TICK,
            deadline=TICK + timedelta(seconds=120),
        )


def test_zero_items_writes_rejection_without_plan_and_cli_exits_distinctly(tmp_path):
    snapshot = _snapshot(tmp_path / "input")
    package = _package(tmp_path / "package", snapshot)
    target_map = _owner_target_map(tmp_path / "targets", snapshot, [])
    dispatcher = _dispatcher_root(tmp_path / "dispatcher")
    with pytest.raises(NoAdmittedItems) as info:
        publish_read_demand_plan(
            snapshot,
            package,
            target_map,
            dispatcher,
            run_id="run-empty",
            created_at=TICK,
            deadline=TICK + timedelta(seconds=120),
        )
    assert info.value.report_path.is_file()
    assert list((dispatcher / "plans").iterdir()) == []
    result = CliRunner().invoke(
        cli,
        [
            "publish-read-demand-plan",
            "--snapshot",
            str(snapshot),
            "--package",
            str(package),
            "--target-map",
            str(target_map),
            "--dispatcher-root",
            str(_dispatcher_root(tmp_path / "dispatcher-cli")),
            "--run-id",
            "run-empty-cli",
            "--created-at",
            TICK.isoformat().replace("+00:00", "Z"),
            "--deadline",
            (TICK + timedelta(seconds=120)).isoformat().replace("+00:00", "Z"),
            "--expected-repository-revision",
            REPOSITORY_REVISION,
        ],
    )
    assert result.exit_code == ZERO_ITEMS_EXIT_CODE


@pytest.mark.parametrize(
    ("valid_until", "deadline", "reason"),
    [
        (TICK + timedelta(seconds=60), TICK + timedelta(seconds=120), "target_validity"),
        (TICK + timedelta(seconds=301), TICK + timedelta(seconds=120), "target_ttl"),
    ],
)
def test_deadline_and_ttl_fail_closed(tmp_path, valid_until, deadline, reason):
    snapshot = _snapshot(tmp_path / "input")
    package = _package(tmp_path / "package", snapshot)
    target_map = _owner_target_map(
        tmp_path / "targets",
        snapshot,
        [_target("file-a", valid_until=valid_until)],
    )
    dispatcher = _dispatcher_root(tmp_path / "dispatcher")
    with pytest.raises(NoAdmittedItems) as info:
        publish_read_demand_plan(
            snapshot,
            package,
            target_map,
            dispatcher,
            run_id="run-invalid-time",
            created_at=TICK,
            deadline=deadline,
        )
    report = json.loads(info.value.report_path.read_text())
    assert report["rejection_counts"][reason] == 1


def test_creation_must_precede_deadline(tmp_path):
    snapshot = _snapshot(tmp_path / "input")
    package = _package(tmp_path / "package", snapshot)
    target_map = _owner_target_map(
        tmp_path / "targets",
        snapshot,
        [_target("file-a")],
    )
    dispatcher = _dispatcher_root(tmp_path / "dispatcher")
    with pytest.raises(PlanPublishError, match="earlier than deadline"):
        publish_read_demand_plan(
            snapshot,
            package,
            target_map,
            dispatcher,
            run_id="run-bad-deadline",
            created_at=TICK,
            deadline=TICK,
        )
    assert not (dispatcher / "plans").exists()


@pytest.mark.parametrize(
    ("target", "reason"),
    [
        (_target("file-a", approved=False), "unauthorized_target"),
        ({**_target("file-a"), "target_fh_b64": "not+base64"}, "malformed_target"),
    ],
)
def test_target_authorization_and_shape_fail_closed(tmp_path, target, reason):
    snapshot = _snapshot(tmp_path / "input")
    package = _package(tmp_path / "package", snapshot)
    target_map = _owner_target_map(tmp_path / "targets", snapshot, [target])
    dispatcher = _dispatcher_root(tmp_path / "dispatcher")
    with pytest.raises(NoAdmittedItems) as info:
        publish_read_demand_plan(
            snapshot,
            package,
            target_map,
            dispatcher,
            run_id="run-invalid-target",
            created_at=TICK,
            deadline=TICK + timedelta(seconds=120),
        )
    report = json.loads(info.value.report_path.read_text())
    assert report["rejection_counts"][reason] == 1


def test_min_score_can_reject_every_candidate(tmp_path):
    snapshot = _snapshot(tmp_path / "input")
    package = _package(tmp_path / "package", snapshot, min_score=100.0)
    target_map = _owner_target_map(
        tmp_path / "targets",
        snapshot,
        [_target("file-a"), _target("file-b"), _target("file-fixture")],
    )
    with pytest.raises(NoAdmittedItems) as info:
        publish_read_demand_plan(
            snapshot,
            package,
            target_map,
            _dispatcher_root(tmp_path / "dispatcher"),
            run_id="run-min-score",
            created_at=TICK,
            deadline=TICK + timedelta(seconds=120),
        )
    report = json.loads(info.value.report_path.read_text())
    assert report["rejection_counts"]["below_min_score"] == 3
