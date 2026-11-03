"""Contract and acceptance tests for the controlled Chimera v2 importer."""

from __future__ import annotations

import base64
import hashlib
import json
import os
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from click.testing import CliRunner

from predictive_cache.datagen.io import canonical_json
from predictive_cache.prep.cli import main as prep_main
from predictive_cache.sources.chimera.cli import main
from predictive_cache.sources.chimera.importer import import_dataset

START = datetime(2026, 1, 5, 10, tzinfo=UTC)
END = START + timedelta(hours=5)
VOLUME_ID = "01" * 16
FILE_ID = "02" * 32
EPOCH = "11111111-2222-4333-8444-555555555555"
POD_UID = "33333333-3333-4333-8333-333333333333"
RUN_ID = "controlled-run"
EXPORT_ID = 7
EXPORT_NAME = "export-demo"


def _b64(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode().rstrip("=")


def _write(path: Path, payload: str) -> None:
    path.write_text(payload)
    path.chmod(0o600)


def _json(path: Path, value) -> None:
    _write(path, canonical_json(value) + "\n")


def _jsonl(path: Path, rows: list[dict]) -> None:
    _write(path, "".join(canonical_json(row) + "\n" for row in rows))


def _operation_id(index: int) -> str:
    return hashlib.sha256(f"offered-{index}".encode()).hexdigest()


def _tag(operation_id: str) -> str:
    return ""


def _raw_event(index: int, operation_id: str, *, sequence: int | None = None) -> dict:
    event_time = START + timedelta(minutes=index - 1, seconds=1)
    sink_time = event_time + timedelta(milliseconds=10)
    tag = _tag(operation_id)
    return {
        "schema": "nfs4_read_v2",
        "local_id": str(uuid.UUID(int=index)),
        "producer_epoch": EPOCH,
        "producer_sequence": str(sequence if sequence is not None else index),
        "event_time_ns": str(int(event_time.timestamp() * 1_000_000_000)),
        "sink_time_ns": str(int(sink_time.timestamp() * 1_000_000_000)),
        "target_created_ns": "1",
        "volume_id": VOLUME_ID,
        "file_id": FILE_ID,
        "identity_valid": True,
        "export_name_b64": _b64(EXPORT_NAME.encode()),
        "export_id": EXPORT_ID,
        "target_fh": _b64(b"opaque-handle"),
        "operation_id": hashlib.sha256(f"nfs-{index}".encode()).hexdigest(),
        "client_ref": "03" * 32,
        "session_ref": "04" * 32,
        "compound_op_index": 1,
        "slot_id": 2,
        "sequence_id": index,
        "compound_tag_b64": _b64(tag.encode()),
        "compound_tag_original_length": len(tag),
        "compound_tag_truncated": False,
        "compound_tag_id": hashlib.sha256(tag.encode()).hexdigest(),
        "origin_claim": "unknown",
        "origin_trust": "client_asserted",
        "requested_offset": "0",
        "requested_count": 10,
        "returned_bytes": 10,
        "nfs_status": 0,
        "outcome": "success",
        "eof": False,
        "file_size_valid": True,
        "file_size": "100",
    }


def _fixture(tmp_path: Path) -> dict[str, Path]:
    tmp_path.mkdir(parents=True, mode=0o700, exist_ok=True)
    paths = {
        name: tmp_path / filename
        for name, filename in {
            "event_sources": "event-sources.json",
            "observed": "observed.jsonl",
            "run_manifest": "run.json",
            "offered_ledger": "offered.jsonl",
            "fixture_catalog": "catalog.json",
            "checkpoints": "checkpoints.jsonl",
            "raw": f"collector/{POD_UID}/{EPOCH}/events.jsonl",
        }.items()
    }
    paths["raw"].parent.mkdir(parents=True)
    catalog = {
        "catalog_version": 2,
        "identity_scheme": "chimera-v2-canonical",
        "fixtures": [
            {
                "fixture_id": "fixture-1",
                "volume_id": VOLUME_ID,
                "file_id": FILE_ID,
                "dir_id": "dir-1",
                "export_id": EXPORT_ID,
                "export_name": EXPORT_NAME,
                "export_path": f"/{EXPORT_NAME}",
                "relative_path": "fixtures/file.bin",
                "size_bytes": 100,
                "sha256": "05" * 32,
                "weight": 1,
            }
        ],
    }
    catalog_hash = hashlib.sha256(canonical_json(catalog).encode()).hexdigest()
    _json(paths["fixture_catalog"], catalog)
    _json(
        paths["run_manifest"],
        {
            "manifest_version": 2,
            "run_id": RUN_ID,
            "run_ref": hashlib.sha256(RUN_ID.encode()).hexdigest()[:32],
            "data_kind": "synthetic",
            "experiment_kind": "controlled",
            "representative_production_demand": False,
            "protocol": "NFSv4.1",
            "operation": "READ",
            "warmer_enabled": False,
            "cluster_id": "cluster-1",
            "namespace": "nasuni",
            "volume_id": VOLUME_ID,
            "export_id": EXPORT_ID,
            "cluster_manifest_sha256": "06" * 32,
            "fixture_catalog_sha256": catalog_hash,
            "event_schema_sha256": "07" * 32,
            "budget_evidence_sha256": "0a" * 32,
            "seed": 32,
            "start_time": START,
            "end_time": END,
            "offered_rate_per_s": 1 / 60,
            "offered_count": 5,
            "max_concurrency": 1,
            "checkpoint_interval_s": 900,
            "checkpoint_tolerance_s": 30,
            "operation_schedule_tolerance_s": 30,
            "clock_skew_tolerance_s": 5,
            "pre_read_plus_read_guardrail_ms": 5000,
            "fixture_distribution": "seeded_unique_nonoverlapping",
            "range_distribution": "seeded_unique_nonoverlapping",
            "range_bytes": [10],
            "range_offsets": [0, 10, 20, 30, 40],
            "untagged_correlation_window_s": 120,
            "client_id": "client-1",
            "tag_mode": "empty",
            "tag_contract_status": "proposed-unverified",
            "allowed_exclusion_reasons": [],
            "software": {
                "predictive_cache_revision": "a" * 40,
                "chimera_image": "chimera:test",
                "chimera_image_sha256": "08" * 32,
                "chimera_revision": "b" * 40,
                "cufs_image": "cufs:test",
                "cufs_image_sha256": "09" * 32,
                "cufs_revision": "c" * 40,
                "collector_schema_version": "v1",
                "harness_version": 2,
            },
            "stop_rules": {
                "stop_on_any_drop": True,
                "stop_on_epoch_mismatch": True,
                "stop_on_unexplained_restart": True,
                "stop_on_sink_fault": True,
                "stop_on_parse_or_identity_mismatch": True,
                "stop_on_checksum_mismatch": True,
                "stop_on_read_error": True,
                "latency_guardrail_ms": 250,
            },
            "retention": {
                "owner": "owner",
                "cleanup_owner": "owner",
                "cleanup_action": "delete",
                "trigger": "validated_dataset_finalization",
                "delete_within_days": 7,
                "retain_until": None,
            },
            "limitations": ["synthetic"],
        },
    )
    operations = []
    raw_events = []
    observed = []
    for index in range(1, 6):
        operation_id = _operation_id(index)
        offered_at = START + timedelta(minutes=index - 1)
        offset = (index - 1) * 10
        operations.append(
            {
                "ledger_version": 1,
                "operation_id": operation_id,
                "operation_index": index - 1,
                "run_id": RUN_ID,
                "fixture_id": "fixture-1",
                "offered_at_utc": offered_at,
                "offered_at_monotonic_ns": index,
                "volume_id": VOLUME_ID,
                "file_id": FILE_ID,
                "dir_id": "dir-1",
                "export_id": EXPORT_ID,
                "relative_path": "fixtures/file.bin",
                "file_size": 100,
                "offset": offset,
                "count": 10,
                "expected_outcome": "ok",
                "client_id": "client-1",
                "tag": _tag(operation_id),
                "timing_mode": "simulated",
                "origin": "synthetic",
            }
        )
        raw_event = _raw_event(index, operation_id)
        raw_event["requested_offset"] = str(offset)
        raw_events.append(raw_event)
        observed.append(
            {
                "observed_event_version": 2,
                "operation_id": operation_id,
                "event_id": str(uuid.UUID(int=index)),
                "pod_uid": POD_UID,
                "producer_epoch": EPOCH,
                "producer_sequence": index,
                "server_operation_id": raw_event["operation_id"],
                "sink_time_ns": int(raw_event["sink_time_ns"]),
                "volume_id": VOLUME_ID,
                "file_id": FILE_ID,
                "offset": offset,
                "count": 10,
                "durable_at": offered_at + timedelta(seconds=2),
                "status": "durable",
                "exclusion_reason": None,
                "tag": _tag(operation_id),
            }
        )
    _jsonl(paths["offered_ledger"], operations)
    _jsonl(paths["raw"], raw_events)
    _jsonl(paths["observed"], observed)
    raw_data = paths["raw"].read_bytes()
    raw_hash = hashlib.sha256(raw_data).hexdigest()
    _json(
        paths["event_sources"],
        {
            "collector_file_manifest_version": 1,
            "run_id": RUN_ID,
            "files": [
                {
                    "raw_jsonl_path": paths["raw"].relative_to(tmp_path).as_posix(),
                    "sha256": raw_hash,
                    "pod_name": "collector-0",
                    "pod_uid": POD_UID,
                    "node_name": "node-1",
                    "producer_epoch": EPOCH,
                }
            ],
        },
    )
    checkpoints = []
    for index in range(21):
        captured = START + timedelta(minutes=15 * index)
        synced = sum(
            datetime.fromtimestamp(int(row["sink_time_ns"]) / 1_000_000_000, UTC) <= captured
            for row in raw_events
        )
        checkpoints.append(
            {
                "checkpoint_version": 1,
                "run_id": RUN_ID,
                "scheduled_at": captured,
                "captured_at": captured + timedelta(seconds=5),
                "pod_name": "collector-0",
                "pod_uid": POD_UID,
                "node_name": "node-1",
                "producer_epoch": EPOCH,
                "restart_count": 0,
                "restart_explained": False,
                "last_enqueued_sequence": synced,
                "last_synced_sequence": synced,
                "synced_count": synced,
                "queue_depth": 0,
                "worker_in_flight": 0,
                "delivery_clean": 1,
                "barrier_ready": 1,
                "drops": {
                    "queue": 0,
                    "encode": 0,
                    "sink": 0,
                    "rollback": 0,
                    "rotation": 0,
                },
                "active_jsonl": {
                    "path": paths["raw"].relative_to(tmp_path).as_posix(),
                    "size_bytes": len(raw_data),
                    "sha256": raw_hash,
                },
                "backup_jsonl": None,
                "collector_sink_healthy": True,
                "storage_healthy": True,
                "cluster_healthy": True,
                "volume_healthy": True,
                "export_healthy": True,
                "parse_errors": 0,
                "identity_mismatches": 0,
                "checksum_mismatches": 0,
                "foreground_read_errors": 0,
                "max_observed_latency_ms": 1.0,
            }
        )
    _jsonl(paths["checkpoints"], checkpoints)
    return paths


def _reseal_raw(paths: dict[str, Path]) -> None:
    raw_hash = hashlib.sha256(paths["raw"].read_bytes()).hexdigest()
    source = json.loads(paths["event_sources"].read_text())
    source["files"][0]["sha256"] = raw_hash
    _json(paths["event_sources"], source)
    checkpoints = [json.loads(line) for line in paths["checkpoints"].read_text().splitlines()]
    for row in checkpoints:
        row["active_jsonl"]["sha256"] = raw_hash
        row["active_jsonl"]["size_bytes"] = paths["raw"].stat().st_size
    _jsonl(paths["checkpoints"], checkpoints)


def _import(paths: dict[str, Path], out: Path, **changes):
    kwargs = {
        key: value
        for key, value in paths.items()
        if key
        in {
            "event_sources",
            "observed",
            "run_manifest",
            "offered_ledger",
            "fixture_catalog",
            "checkpoints",
        }
    }
    arguments = {
        **{f"{key}_path": value for key, value in kwargs.items()},
        "out_dir": out,
        "format": "jsonl",
        **changes,
    }
    return import_dataset(**arguments)


def _rows(root: Path, manifest: dict, prefix: str) -> list[dict]:
    return [
        json.loads(line)
        for entry in manifest["files"]
        if entry["path"].startswith(prefix)
        for line in (root / entry["path"]).read_text().splitlines()
    ]


def test_authoritative_run_is_deterministic_and_cleaner_accepts_it(tmp_path: Path):
    paths = _fixture(tmp_path)
    first, second = tmp_path / "first", tmp_path / "second"
    manifest = _import(paths, first)
    repeated = _import(paths, second)
    first_json = json.loads((first / "manifest.json").read_text())
    second_json = json.loads((second / "manifest.json").read_text())
    assert first_json == second_json
    assert manifest.files == repeated.files
    assert manifest.data_kind == "synthetic"
    assert not any(entry["path"].startswith("catalog/") for entry in first_json["files"])
    events = _rows(first, first_json, "events/")
    assert len(events) == 5
    assert {row["origin"] for row in events} == {"synthetic"}
    assert events[0]["ingest_time"] == "2026-01-05T10:00:02.000000Z"
    report = json.loads((first / "report/import.json").read_text())
    assert report["counts"] == {
        "event_sources": 1,
        "raw_events": 5,
        "durable_events": 5,
        "approved_exclusions": 0,
        "offered_operations": 5,
    }

    clean = tmp_path / "clean"
    clean_result = CliRunner().invoke(
        prep_main,
        ["clean", "--input", str(first), "--out", str(clean), "--format", "jsonl"],
    )
    assert clean_result.exit_code == 0, clean_result.output
    cleaned = json.loads((clean / "manifest.json").read_text())
    assert cleaned["counts"]["kept_events"] == 0
    assert cleaned["counts"]["excluded_by_reason"] == {"origin_synthetic": 5}
    assert cleaned["coverage"]["complete"] == 5

    result = CliRunner().invoke(
        main,
        [
            "--event-sources",
            str(paths["event_sources"]),
            "--observed",
            str(paths["observed"]),
            "--run-manifest",
            str(paths["run_manifest"]),
            "--offered-ledger",
            str(paths["offered_ledger"]),
            "--fixture-catalog",
            str(paths["fixture_catalog"]),
            "--checkpoints",
            str(paths["checkpoints"]),
            "--out",
            str(tmp_path / "cli"),
            "--format",
            "jsonl",
        ],
    )
    assert result.exit_code == 0, result.output


def test_raw_schema_and_sequence_are_strict(tmp_path: Path):
    paths = _fixture(tmp_path)
    rows = [json.loads(line) for line in paths["raw"].read_text().splitlines()]
    rows[0]["event_time_ns"] = int(rows[0]["event_time_ns"])
    _jsonl(paths["raw"], rows)
    _reseal_raw(paths)
    with pytest.raises(ValueError, match="event_time_ns"):
        _import(paths, tmp_path / "type")

    paths = _fixture(tmp_path / "sequence")
    rows = [json.loads(line) for line in paths["raw"].read_text().splitlines()]
    rows[1]["producer_sequence"] = "3"
    _jsonl(paths["raw"], rows)
    _reseal_raw(paths)
    with pytest.raises(ValueError, match="contiguous from one"):
        _import(paths, tmp_path / "sequence-out")


def test_exact_observed_and_catalog_joins_fail_closed(tmp_path: Path):
    paths = _fixture(tmp_path)
    rows = [json.loads(line) for line in paths["observed"].read_text().splitlines()]
    rows[0]["pod_uid"] = "other-pod"
    _jsonl(paths["observed"], rows)
    with pytest.raises(ValueError, match="pod_uid"):
        _import(paths, tmp_path / "pod")

    paths = _fixture(tmp_path / "catalog")
    catalog = json.loads(paths["fixture_catalog"].read_text())
    catalog["fixtures"][0]["file_id"] = "0a" * 32
    _json(paths["fixture_catalog"], catalog)
    run = json.loads(paths["run_manifest"].read_text())
    run["fixture_catalog_sha256"] = hashlib.sha256(
        canonical_json(catalog).encode()
    ).hexdigest()
    _json(paths["run_manifest"], run)
    with pytest.raises(ValueError, match="offered operation does not match"):
        _import(paths, tmp_path / "catalog-out")

    paths = _fixture(tmp_path / "range")
    raw = [json.loads(line) for line in paths["raw"].read_text().splitlines()]
    raw[0]["requested_offset"] = "1"
    _jsonl(paths["raw"], raw)
    _reseal_raw(paths)
    with pytest.raises(ValueError, match="observed range identity mismatch"):
        _import(paths, tmp_path / "range-out")


def test_durable_time_not_sink_attempt_is_ingest_time(tmp_path: Path):
    paths = _fixture(tmp_path)
    raw = [json.loads(line) for line in paths["raw"].read_text().splitlines()]
    raw[0]["sink_time_ns"] = raw[0]["event_time_ns"]
    _jsonl(paths["raw"], raw)
    observed = [json.loads(line) for line in paths["observed"].read_text().splitlines()]
    observed[0]["sink_time_ns"] = int(raw[0]["sink_time_ns"])
    _jsonl(paths["observed"], observed)
    _reseal_raw(paths)
    out = tmp_path / "out"
    _import(paths, out)
    manifest = json.loads((out / "manifest.json").read_text())
    event = _rows(out, manifest, "events/")[0]
    assert event["event_time"] == "2026-01-05T10:00:01.000000Z"
    assert event["ingest_time"] == "2026-01-05T10:00:02.000000Z"

    paths = _fixture(tmp_path / "early")
    observed = [json.loads(line) for line in paths["observed"].read_text().splitlines()]
    observed[0]["durable_at"] = START + timedelta(milliseconds=500)
    _jsonl(paths["observed"], observed)
    early_out = tmp_path / "early-out"
    _import(paths, early_out)
    early_manifest = json.loads((early_out / "manifest.json").read_text())
    early_event = _rows(early_out, early_manifest, "events/")[0]
    assert early_event["ingest_time"] == "2026-01-05T10:00:00.500000Z"


def test_tag_checkpoint_hash_and_permissions_are_enforced(tmp_path: Path):
    paths = _fixture(tmp_path)
    rows = [json.loads(line) for line in paths["raw"].read_text().splitlines()]
    rows[0]["compound_tag_id"] = "ff" * 32
    _jsonl(paths["raw"], rows)
    _reseal_raw(paths)
    with pytest.raises(ValueError, match="tag digest"):
        _import(paths, tmp_path / "tag")

    paths = _fixture(tmp_path / "drop")
    checkpoints = [json.loads(line) for line in paths["checkpoints"].read_text().splitlines()]
    checkpoints[0]["drops"]["sink"] = 1
    _jsonl(paths["checkpoints"], checkpoints)
    with pytest.raises(ValueError, match="collector drops"):
        _import(paths, tmp_path / "drop-out")

    paths = _fixture(tmp_path / "restart")
    checkpoints = [json.loads(line) for line in paths["checkpoints"].read_text().splitlines()]
    checkpoints[0]["restart_count"] = 1
    _jsonl(paths["checkpoints"], checkpoints)
    with pytest.raises(ValueError, match="before first checkpoint"):
        _import(paths, tmp_path / "restart-out")

    paths = _fixture(tmp_path / "late-checkpoint")
    checkpoints = [json.loads(line) for line in paths["checkpoints"].read_text().splitlines()]
    checkpoints[0]["captured_at"] = checkpoints[0]["scheduled_at"].replace("Z", "+00:00")
    checkpoints[0]["captured_at"] = datetime.fromisoformat(
        checkpoints[0]["captured_at"]
    ) + timedelta(seconds=31)
    _jsonl(paths["checkpoints"], checkpoints)
    with pytest.raises(ValueError, match="30-second tolerance"):
        _import(paths, tmp_path / "late-checkpoint-out")

    paths = _fixture(tmp_path / "permissions")
    paths["observed"].chmod(0o644)
    with pytest.raises(PermissionError, match="owner-only"):
        _import(paths, tmp_path / "permissions-out")


def test_existing_output_is_never_replaced(tmp_path: Path):
    paths = _fixture(tmp_path)
    out = tmp_path / "out"
    _import(paths, out)
    assert oct(os.stat(out).st_mode & 0o777) == "0o700"
    with pytest.raises(ValueError, match="atomic replacement"):
        _import(paths, out)
