from __future__ import annotations

import base64
import hashlib
import json
import os
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from predictive_cache.controller.pass2_schedule import (
    ORDERING,
    SCORE_QUANTIZATION,
    ScheduleError,
    load_schedule,
)
from predictive_cache.controller.pass2_targets import (
    TargetMapError,
    load_target_map,
)
from predictive_cache.dispatcher.schema import MAX_TASK_BYTES
from predictive_cache.ml.hashing import canonical_json

NOW = datetime(2026, 11, 4, 12, 0, tzinfo=UTC)
HASH = "a" * 64


def _schedule_document() -> dict:
    policy = {"max_items": 2, "max_bytes": 12288, "range_cap": 8192}
    document = {
        "schema_version": 1,
        "schedule_id": "",
        "semantic_view": {
            "id": "read-demand-v1",
            "sha256": HASH,
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
            "prediction_cutoff": "2026-11-04T11:55:00Z",
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
            "warm_start_offset_s": 30,
            "warm_spacing_s": 10,
            "score_quantization": SCORE_QUANTIZATION,
        },
        "ordering": list(ORDERING),
        "budget": {
            "max_items": 2,
            "max_bytes": 12288,
            "range_cap_bytes": 8192,
        },
        "rejection_counts": {"below_threshold": 1},
        "admitted_items": 2,
        "requested_bytes": 12288,
        "items": [
            {
                "prediction_id": "prediction-1",
                "rank": 1,
                "volume_id": "vol-1",
                "file_id": "file-a",
                "prediction_cutoff": "2026-11-04T11:55:00Z",
                "warm_offset_s": 30,
                "score": 2.0,
                "reasons": ["read_count_24h=2"],
                "logical_range": {
                    "offset": 0,
                    "length": 8192,
                    "unit": "bytes",
                },
            },
            {
                "prediction_id": "prediction-2",
                "rank": 2,
                "volume_id": "vol-1",
                "file_id": "file-b",
                "prediction_cutoff": "2026-11-04T11:55:00Z",
                "warm_offset_s": 40,
                "score": 1.0,
                "reasons": ["read_count_24h=1"],
                "logical_range": {
                    "offset": 4096,
                    "length": 4096,
                    "unit": "bytes",
                },
            },
        ],
    }
    return _seal_schedule(document)


def _seal_schedule(document: dict) -> dict:
    body = deepcopy(document)
    body.pop("schedule_id", None)
    document["schedule_id"] = (
        "schedule-"
        + hashlib.sha256(canonical_json(body).encode()).hexdigest()[:24]
    )
    return document


def _write_schedule(path: Path, document: dict) -> Path:
    path.write_text(canonical_json(document) + "\n")
    return path


def _target(
    file_id: str,
    *,
    approved: bool = True,
    handle: str | None = None,
) -> dict:
    return {
        "approved": approved,
        "volume_id": "vol-1",
        "file_id": file_id,
        "export_name": "export-1",
        "export_id": "export-id-1",
        "target_fh_b64": handle
        or base64.urlsafe_b64encode(f"fh-{file_id}".encode())
        .rstrip(b"=")
        .decode(),
        "target_valid_until": "2026-11-04T12:05:00Z",
        "offset": 0,
        "length": 4096,
    }


def _target_map(schedule_id: str, targets: list[dict] | None = None) -> dict:
    return {
        "schema_version": 1,
        "target_map_id": "target-map-1",
        "authorization": "approved-pass2-treatment",
        "schedule_id": schedule_id,
        "created_at": "2026-11-04T11:59:00Z",
        "targets": targets if targets is not None else [_target("file-a")],
    }


def _write_target_map(root: Path, document: dict) -> Path:
    root.mkdir(mode=0o700)
    os.chmod(root, 0o700)
    path = root / "targets.json"
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        os.write(fd, (canonical_json(document) + "\n").encode())
    finally:
        os.close(fd)
    return path


def test_schedule_loads_self_hash_and_preserves_canonical_provenance(tmp_path):
    document = _schedule_document()
    path = _write_schedule(tmp_path / "schedule.json", document)

    schedule, digest = load_schedule(path)

    assert schedule.schedule_id == document["schedule_id"]
    assert schedule.model_dump() == document
    assert schedule.raw_canonical_json == canonical_json(document)
    assert digest == hashlib.sha256(path.read_bytes()).hexdigest()
    assert schedule.items[1].logical_range.length == 4096


def test_schedule_rejects_duplicate_json_keys(tmp_path):
    document = _schedule_document()
    encoded = canonical_json(document).replace(
        '"schema_version":1',
        '"schema_version":1,"schema_version":1',
        1,
    )
    path = tmp_path / "schedule.json"
    path.write_text(encoded)

    with pytest.raises(ScheduleError, match="duplicate JSON key"):
        load_schedule(path)


@pytest.mark.parametrize(
    "mutate",
    [
        lambda value: value.update(extra=True),
        lambda value: value["items"][0].update(extra=True),
        lambda value: value["time_basis"].pop("score_quantization"),
        lambda value: value["time_basis"].update(score_quantization="raw-float"),
        lambda value: value["policy"].update(target_fh_b64="sensitive"),
        lambda value: value["snapshot"]["runtime_capabilities"].update(
            endpoint="sensitive"
        ),
    ],
)
def test_schedule_rejects_nonexact_or_execution_identity_fields(tmp_path, mutate):
    document = _schedule_document()
    mutate(document)
    _seal_schedule(document)
    path = _write_schedule(tmp_path / "schedule.json", document)

    with pytest.raises(ScheduleError):
        load_schedule(path)


@pytest.mark.parametrize(
    "mutate",
    [
        lambda value: value["items"][1].update(rank=3),
        lambda value: value["items"][1].update(warm_offset_s=41),
        lambda value: value["items"].reverse(),
        lambda value: value["items"][0].update(
            prediction_cutoff="2026-11-04T11:54:00Z"
        ),
    ],
)
def test_schedule_rejects_rank_due_order_and_cutoff_violations(tmp_path, mutate):
    document = _schedule_document()
    mutate(document)
    _seal_schedule(document)
    path = _write_schedule(tmp_path / "schedule.json", document)

    with pytest.raises(ScheduleError):
        load_schedule(path)


@pytest.mark.parametrize(
    "mutate",
    [
        lambda value: value["items"][1].update(prediction_id="prediction-1"),
        lambda value: value["items"][1].update(file_id="file-a"),
    ],
)
def test_schedule_rejects_duplicate_prediction_or_logical_identity(
    tmp_path,
    mutate,
):
    document = _schedule_document()
    mutate(document)
    _seal_schedule(document)
    path = _write_schedule(tmp_path / "schedule.json", document)

    with pytest.raises(ScheduleError, match="unique"):
        load_schedule(path)


@pytest.mark.parametrize(
    "mutate",
    [
        lambda value: value["items"][0]["logical_range"].update(length=8193),
        lambda value: value.update(requested_bytes=1),
        lambda value: value["budget"].update(max_bytes=12287),
        lambda value: value.update(admitted_items=1),
        lambda value: value["budget"].update(range_cap_bytes=MAX_TASK_BYTES + 1),
        lambda value: value["items"][0]["logical_range"].update(offset=-1),
        lambda value: value["items"][0]["logical_range"].update(length=True),
        lambda value: value["items"][0].update(score=True),
    ],
)
def test_schedule_rejects_budget_range_total_and_numeric_violations(
    tmp_path,
    mutate,
):
    document = _schedule_document()
    mutate(document)
    _seal_schedule(document)
    path = _write_schedule(tmp_path / "schedule.json", document)

    with pytest.raises(ScheduleError):
        load_schedule(path)


def test_schedule_rejects_nonfinite_score(tmp_path):
    document = _schedule_document()
    document["items"][0]["score"] = float("nan")
    path = tmp_path / "schedule.json"
    path.write_text(json.dumps(document, sort_keys=True, separators=(",", ":")))

    with pytest.raises(ScheduleError, match="noncanonical JSON numbers"):
        load_schedule(path)


@pytest.mark.parametrize(
    "reasons",
    [[""], ["x" * 513], ["ok"] * 63],
)
def test_schedule_rejects_reasons_that_dispatcher_cannot_accept(tmp_path, reasons):
    document = _schedule_document()
    document["items"][0]["reasons"] = reasons
    _seal_schedule(document)
    path = _write_schedule(tmp_path / "schedule.json", document)

    with pytest.raises(ScheduleError, match="reasons"):
        load_schedule(path)


def test_schedule_rejects_policy_hash_mismatch(tmp_path):
    document = _schedule_document()
    document["policy_sha256"] = "0" * 64
    _seal_schedule(document)
    path = _write_schedule(tmp_path / "schedule.json", document)

    with pytest.raises(ScheduleError, match="policy_sha256"):
        load_schedule(path)


@pytest.mark.parametrize(
    "field,value",
    [("max_items", 1), ("max_bytes", 1), ("range_cap", 1)],
)
def test_schedule_rejects_policy_budget_mismatch(tmp_path, field, value):
    document = _schedule_document()
    document["policy"][field] = value
    document["policy_sha256"] = hashlib.sha256(
        canonical_json(document["policy"]).encode()
    ).hexdigest()
    _seal_schedule(document)
    path = _write_schedule(tmp_path / "schedule.json", document)

    with pytest.raises(ScheduleError, match="policy budget"):
        load_schedule(path)


def test_schedule_rejects_incorrect_self_hash(tmp_path):
    document = _schedule_document()
    document["requested_bytes"] = 1
    path = _write_schedule(tmp_path / "schedule.json", document)

    with pytest.raises(ScheduleError, match="schedule_id"):
        load_schedule(path)


def test_target_map_loads_owner_only_and_resolves_authorized_and_unauthorized(
    tmp_path,
):
    schedule_id = _schedule_document()["schedule_id"]
    unauthorized = _target("file-b", approved=False)
    unauthorized["target_valid_until"] = "2026-11-04T11:58:00Z"
    document = _target_map(
        schedule_id,
        [_target("file-a"), unauthorized],
    )
    path = _write_target_map(tmp_path / "private", document)

    target_map, digest = load_target_map(path, schedule_id, NOW)

    assert target_map.resolve("vol-1", "file-a").approved is True
    assert target_map.resolve("vol-1", "file-b").approved is False
    assert target_map.resolve("vol-1", "file-b").target_valid_until < NOW
    assert target_map.resolve("vol-1", "missing") is None
    assert digest == hashlib.sha256(path.read_bytes()).hexdigest()
    assert target_map.model_dump() == document


@pytest.mark.parametrize("parent_mode,file_mode", [(0o755, 0o600), (0o700, 0o644)])
def test_target_map_requires_owner_only_modes(tmp_path, parent_mode, file_mode):
    schedule_id = _schedule_document()["schedule_id"]
    path = _write_target_map(
        tmp_path / f"private-{parent_mode:o}-{file_mode:o}",
        _target_map(schedule_id),
    )
    os.chmod(path.parent, parent_mode)
    os.chmod(path, file_mode)

    with pytest.raises(TargetMapError, match="security"):
        load_target_map(path, schedule_id, NOW)


def test_target_map_rejects_symlink_and_hardlink(tmp_path):
    schedule_id = _schedule_document()["schedule_id"]
    source = _write_target_map(
        tmp_path / "source",
        _target_map(schedule_id),
    )
    symlink_root = tmp_path / "symlink"
    symlink_root.mkdir(mode=0o700)
    symlink = symlink_root / "targets.json"
    symlink.symlink_to(source)
    with pytest.raises(TargetMapError, match="security"):
        load_target_map(symlink, schedule_id, NOW)

    hardlink_root = tmp_path / "hardlink"
    hardlink_root.mkdir(mode=0o700)
    hardlink = hardlink_root / "targets.json"
    os.link(source, hardlink)
    with pytest.raises(TargetMapError, match="security"):
        load_target_map(hardlink, schedule_id, NOW)


def test_target_map_rejects_wrong_schedule_duplicate_targets_and_future_creation(
    tmp_path,
):
    schedule_id = _schedule_document()["schedule_id"]
    wrong_schedule = "schedule-" + "0" * 24
    path = _write_target_map(
        tmp_path / "wrong",
        _target_map(wrong_schedule),
    )
    with pytest.raises(TargetMapError, match="expected schedule"):
        load_target_map(path, schedule_id, NOW)

    path = _write_target_map(
        tmp_path / "duplicate",
        _target_map(schedule_id, [_target("file-a"), _target("file-a")]),
    )
    with pytest.raises(TargetMapError, match="unique"):
        load_target_map(path, schedule_id, NOW)

    future = _target_map(schedule_id)
    future["created_at"] = "2026-11-04T12:00:01Z"
    path = _write_target_map(tmp_path / "future", future)
    with pytest.raises(TargetMapError, match="future"):
        load_target_map(path, schedule_id, NOW)


def test_target_map_rejects_nonexact_fields(tmp_path):
    schedule_id = _schedule_document()["schedule_id"]
    document = _target_map(schedule_id)
    document["targets"][0]["extra"] = True
    path = _write_target_map(tmp_path / "private", document)

    with pytest.raises(TargetMapError, match="fields"):
        load_target_map(path, schedule_id, NOW)


def test_target_map_rejects_duplicate_json_keys(tmp_path):
    schedule_id = _schedule_document()["schedule_id"]
    document = _target_map(schedule_id)
    root = tmp_path / "private"
    root.mkdir(mode=0o700)
    path = root / "targets.json"
    encoded = canonical_json(document).replace(
        '"schema_version":1',
        '"schema_version":1,"schema_version":1',
        1,
    )
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        os.write(fd, encoded.encode())
    finally:
        os.close(fd)

    with pytest.raises(TargetMapError, match="duplicate JSON key"):
        load_target_map(path, schedule_id, NOW)


@pytest.mark.parametrize(
    "mutate",
    [
        lambda target: target.update(target_fh_b64="not*base64"),
        lambda target: target.update(approved=1),
        lambda target: target.update(offset=-1),
        lambda target: target.update(length=MAX_TASK_BYTES + 1),
    ],
)
def test_target_map_validates_handle_boolean_and_range_without_leaking_handle(
    tmp_path,
    mutate,
):
    schedule_id = _schedule_document()["schedule_id"]
    target = _target("file-a")
    mutate(target)
    secret_handle = target["target_fh_b64"]
    path = _write_target_map(
        tmp_path / "private",
        _target_map(schedule_id, [target]),
    )

    with pytest.raises(TargetMapError) as caught:
        load_target_map(path, schedule_id, NOW)
    assert secret_handle not in str(caught.value)


def test_target_repr_omits_raw_handle(tmp_path):
    schedule_id = _schedule_document()["schedule_id"]
    handle = base64.urlsafe_b64encode(b"highly-sensitive-handle").rstrip(b"=").decode()
    path = _write_target_map(
        tmp_path / "private",
        _target_map(schedule_id, [_target("file-a", handle=handle)]),
    )

    target_map, _ = load_target_map(path, schedule_id, NOW + timedelta(seconds=1))

    assert handle not in repr(target_map)
    assert handle not in repr(target_map.targets[0])
