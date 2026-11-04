from __future__ import annotations

import base64
import hashlib
import json
import os
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from predictive_cache.controller.pass2_runtime import (
    Pass2RuntimeConfig,
    Pass2RuntimeError,
    Pass2Scheduler,
)
from predictive_cache.dispatcher.core import Dispatcher, DispatcherConfig
from predictive_cache.dispatcher.schema import Plan
from predictive_cache.ml.hashing import canonical_json

RUN_START = datetime(2026, 11, 4, 14, 0, tzinfo=UTC)


class Clock:
    def __init__(self, value: datetime) -> None:
        self.value = value
        self.monotonic_value = 100.0

    def now(self) -> datetime:
        return self.value

    def monotonic(self) -> float:
        return self.monotonic_value

    def advance(self, seconds: float) -> None:
        self.value += timedelta(seconds=seconds)
        self.monotonic_value += seconds

    def sleep(self, seconds: float) -> None:
        self.advance(seconds)


def _seal_schedule(document: dict) -> dict:
    body = deepcopy(document)
    body.pop("schedule_id", None)
    document["schedule_id"] = (
        "schedule-"
        + hashlib.sha256(canonical_json(body).encode()).hexdigest()[:24]
    )
    return document


def _schedule_document() -> dict:
    policy = {"max_items": 1, "max_bytes": 4096, "range_cap": 4096}
    return _seal_schedule(
        {
            "schema_version": 1,
            "schedule_id": "",
            "semantic_view": {
                "id": "read-demand-v1",
                "sha256": "a" * 64,
                "feature_version": "read-demand-v1",
                "feature_schema_sha256": "b" * 64,
                "serving_contract_sha256": "c" * 64,
            },
            "snapshot": {
                "dataset_id": "snapshot-1",
                "manifest_sha256": "f" * 64,
                "files_sha256": {
                    "manifest.json": "1" * 64,
                    "feature_schema.json": "5" * 64,
                },
                "candidate_profile": "read-demand-v1",
                "runtime_capabilities": {
                    "verified_file_size": False,
                    "anchor_context": False,
                },
                "scoring_rows_sha256": "2" * 64,
                "prediction_cutoff": "2026-11-04T13:55:00Z",
            },
            "package": {
                "package_id": "package-1",
                "artifact_sha256": "3" * 64,
                "package_manifest_sha256": "4" * 64,
                "model_type": "baseline-read-frequency",
                "model_version": "baseline-read-frequency:v1",
                "trained": False,
                "trained_evidence": "explicit",
                "data_kind": "synthetic",
            },
            "policy": policy,
            "policy_sha256": hashlib.sha256(
                canonical_json(policy).encode()
            ).hexdigest(),
            "time_basis": {
                "kind": "pass2_start_relative",
                "warm_start_offset_s": 10,
                "warm_spacing_s": 0,
                "score_quantization": "12-significant-digits",
            },
            "ordering": ["score_desc", "file_id_asc", "volume_id_asc"],
            "budget": {
                "max_items": 1,
                "max_bytes": 4096,
                "range_cap_bytes": 4096,
            },
            "rejection_counts": {},
            "admitted_items": 1,
            "requested_bytes": 4096,
            "items": [
                {
                    "prediction_id": "prediction-1",
                    "rank": 1,
                    "volume_id": "vol-1",
                    "file_id": "file-1",
                    "prediction_cutoff": "2026-11-04T13:55:00Z",
                    "warm_offset_s": 10,
                    "score": 1.0,
                    "reasons": ["fixture-score"],
                    "logical_range": {
                        "offset": 0,
                        "length": 4096,
                        "unit": "bytes",
                    },
                }
            ],
        }
    )


def _write_schedule(tmp_path: Path) -> tuple[Path, dict]:
    document = _schedule_document()
    path = tmp_path / "schedule.json"
    path.write_text(canonical_json(document) + "\n")
    return path, document


def _write_target_map(
    tmp_path: Path,
    schedule_id: str,
    *,
    valid_until: datetime,
    approved: bool = True,
    file_ids: tuple[str, ...] = ("file-1",),
) -> Path:
    root = tmp_path / "targets"
    root.mkdir(mode=0o700)
    os.chmod(root, 0o700)
    handle = base64.urlsafe_b64encode(b"pass2-fixture-handle").rstrip(b"=").decode()
    document = {
        "schema_version": 1,
        "target_map_id": "target-map-1",
        "authorization": "approved-pass2-treatment",
        "schedule_id": schedule_id,
        "created_at": (RUN_START - timedelta(seconds=1))
        .isoformat()
        .replace("+00:00", "Z"),
        "targets": [
            {
                "approved": approved,
                "volume_id": "vol-1",
                "file_id": file_id,
                "export_name": "fixture-export",
                "export_id": "fixture-export-id",
                "target_fh_b64": handle,
                "target_valid_until": valid_until.isoformat().replace("+00:00", "Z"),
                "offset": 0,
                "length": 4096,
            }
            for file_id in file_ids
        ],
    }
    path = root / "target-map.json"
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        os.write(fd, (canonical_json(document) + "\n").encode())
    finally:
        os.close(fd)
    return path


def _dispatcher_root(tmp_path: Path) -> Path:
    root = tmp_path / "dispatcher"
    root.mkdir(mode=0o700)
    os.chmod(root, 0o700)
    return root


def _config(
    schedule: Path,
    target_map: Path,
    root: Path,
    *,
    publish_lead_seconds: int = 0,
    max_bytes_per_min: int = 4096,
    max_items_per_min: int = 1,
) -> Pass2RuntimeConfig:
    return Pass2RuntimeConfig(
        schedule_path=schedule,
        target_map_path=target_map,
        dispatcher_root=root,
        run_id="pass2-run-1",
        run_start=RUN_START,
        publish_lead_seconds=publish_lead_seconds,
        plan_ttl_seconds=120,
        max_bytes_per_min=max_bytes_per_min,
        max_items_per_min=max_items_per_min,
        clock_tolerance_seconds=0.01,
    )


def test_pass2_publishes_only_when_prediction_is_due_and_replay_is_safe(tmp_path):
    schedule, document = _write_schedule(tmp_path)
    target_map = _write_target_map(
        tmp_path,
        document["schedule_id"],
        valid_until=RUN_START + timedelta(seconds=180),
    )
    root = _dispatcher_root(tmp_path)
    clock = Clock(RUN_START + timedelta(seconds=9))

    with Pass2Scheduler(
        _config(schedule, target_map, root),
        now=clock.now,
        monotonic=clock.monotonic,
        sleep=clock.sleep,
    ) as scheduler:
        early = scheduler.step()
        assert early.pending_items == 1
        assert not list((root / "plans").glob("*.json"))
        clock.advance(1)
        due = scheduler.step()

    plans = list((root / "plans").glob("*.json"))
    assert due.status == "completed"
    assert due.published_plans == 1
    assert due.published_items == 1
    assert len(plans) == 1
    plan = Plan.model_validate(json.loads(plans[0].read_text()))
    assert plan.created_at == RUN_START + timedelta(seconds=10)
    assert plan.max_bytes == sum(item.length for item in plan.items)

    with Pass2Scheduler(
        _config(schedule, target_map, root),
        now=clock.now,
        monotonic=clock.monotonic,
    ) as restarted:
        replay = restarted.step()
    assert replay.status == "completed"
    assert len(list((root / "plans").glob("*.json"))) == 1


def test_pass2_skips_target_that_expires_before_plan_deadline(tmp_path):
    schedule, document = _write_schedule(tmp_path)
    target_map = _write_target_map(
        tmp_path,
        document["schedule_id"],
        valid_until=RUN_START + timedelta(seconds=100),
    )
    root = _dispatcher_root(tmp_path)
    clock = Clock(RUN_START + timedelta(seconds=10))

    with Pass2Scheduler(
        _config(schedule, target_map, root),
        now=clock.now,
        monotonic=clock.monotonic,
    ) as scheduler:
        result = scheduler.step()

    assert result.status == "completed"
    assert result.skipped_items == 1
    assert not list((root / "plans").glob("*.json"))
    audit = json.loads(next((root / "pass2-runs/pass2-run-1/audit").iterdir()).read_text())
    assert audit["reason"] == "stale_target"


def test_pass2_reports_cumulative_counts_across_due_groups(tmp_path):
    document = _schedule_document()
    document["policy"].update(max_items=2, max_bytes=8192)
    document["policy_sha256"] = hashlib.sha256(
        canonical_json(document["policy"]).encode()
    ).hexdigest()
    document["budget"].update(max_items=2, max_bytes=8192)
    document["time_basis"]["warm_spacing_s"] = 10
    second = deepcopy(document["items"][0])
    second.update(
        prediction_id="prediction-2",
        rank=2,
        file_id="file-2",
        warm_offset_s=20,
        score=0.5,
    )
    document["items"].append(second)
    document["admitted_items"] = 2
    document["requested_bytes"] = 8192
    _seal_schedule(document)
    schedule = tmp_path / "schedule.json"
    schedule.write_text(canonical_json(document) + "\n")
    target_map = _write_target_map(
        tmp_path,
        document["schedule_id"],
        valid_until=RUN_START + timedelta(seconds=240),
        file_ids=("file-1", "file-2"),
    )
    root = _dispatcher_root(tmp_path)
    clock = Clock(RUN_START + timedelta(seconds=10))

    with Pass2Scheduler(
        _config(
            schedule,
            target_map,
            root,
            max_bytes_per_min=8192,
            max_items_per_min=2,
        ),
        now=clock.now,
        monotonic=clock.monotonic,
    ) as scheduler:
        first = scheduler.step()
        clock.advance(10)
        second_result = scheduler.step()

    assert first.published_plans == 1
    assert first.published_items == 1
    assert second_result.status == "completed"
    assert second_result.published_plans == 2
    assert second_result.published_items == 2


def test_pass2_rejects_schedule_that_cannot_fit_configured_rate_window(tmp_path):
    document = _schedule_document()
    document["policy"].update(max_items=2, max_bytes=8192)
    document["policy_sha256"] = hashlib.sha256(
        canonical_json(document["policy"]).encode()
    ).hexdigest()
    document["budget"].update(max_items=2, max_bytes=8192)
    second = deepcopy(document["items"][0])
    second.update(
        prediction_id="prediction-2",
        rank=2,
        file_id="file-2",
        score=0.5,
    )
    document["items"].append(second)
    document["admitted_items"] = 2
    document["requested_bytes"] = 8192
    _seal_schedule(document)
    schedule = tmp_path / "schedule.json"
    schedule.write_text(canonical_json(document) + "\n")
    target_map = _write_target_map(
        tmp_path,
        document["schedule_id"],
        valid_until=RUN_START + timedelta(seconds=240),
        file_ids=("file-1", "file-2"),
    )
    root = _dispatcher_root(tmp_path)

    with pytest.raises(Pass2RuntimeError, match="per-minute limits"):
        Pass2Scheduler(
            _config(schedule, target_map, root),
            now=lambda: RUN_START,
            monotonic=lambda: 100.0,
        )


def test_pass2_plan_is_exactly_accepted_by_dispatcher_dry_run(tmp_path):
    schedule, document = _write_schedule(tmp_path)
    target_map = _write_target_map(
        tmp_path,
        document["schedule_id"],
        valid_until=RUN_START + timedelta(seconds=180),
    )
    root = _dispatcher_root(tmp_path)
    clock = Clock(RUN_START + timedelta(seconds=10))
    with Pass2Scheduler(
        _config(schedule, target_map, root),
        now=clock.now,
        monotonic=clock.monotonic,
    ) as scheduler:
        scheduler.step()

    warmer = tmp_path / "must-not-run"
    warmer.write_text("#!/bin/sh\nexit 99\n")
    os.chmod(warmer, 0o700)
    with Dispatcher(
        DispatcherConfig(
            root=root,
            warmer_command=warmer,
            host="192.0.2.10",
            real_reads=False,
            max_bytes_per_min=4096,
            max_items_per_min=1,
        ),
        now=clock.now,
        monotonic=clock.monotonic,
    ) as dispatcher:
        stats = dispatcher.run_once()

    results = list((root / "runs/pass2-run-1/plans").glob("*/results/*.json"))
    assert stats.claimed == 1
    assert stats.results == 1
    assert len(results) == 1
    result = json.loads(results[0].read_text())
    assert result["status"] == "skipped"
    assert result["reason"] == "dry_run"
    assert result["bytes_requested"] == 4096
    assert result["bytes_read"] == 0
    assert not list((root / "runs/pass2-run-1/plans").glob("*/tasks/*.json"))
