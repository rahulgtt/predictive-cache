"""Offline publication of bounded READ-only plans from a narrow inference snapshot."""

from __future__ import annotations

import hashlib
import json
import math
import os
import re
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa

from predictive_cache.dispatcher.schema import (
    ID_PATTERN,
    MAX_TTL_SECONDS,
    InvalidPlanItem,
    Plan,
    PlanItem,
    format_utc_z,
    warmer_ref,
)
from predictive_cache.dispatcher.securefs import (
    atomic_write_bytes,
    ensure_child_directory,
    loads_strict_json,
    read_secure_bytes,
    require_secure_directory,
)
from predictive_cache.semantics.read_demand import (
    SEMANTIC_VIEW_ID,
    SEMANTIC_VIEW_SHA256,
)

from .hashing import canonical_json, file_sha256
from .package import LoadedPackage, PackageError, load_package
from .read_demand import (
    READ_DEMAND_FEATURE_SCHEMA_SHA256,
    READ_DEMAND_FEATURE_VERSION,
    READ_DEMAND_SERVING_CONTRACT_SHA256,
)
from .snapshots import (
    load_narrow_read_demand_snapshot,
    runtime_available_features,
)

TARGET_MAP_AUTHORIZATION = "approved-fixture-dry-run"
ZERO_ITEMS_EXIT_CODE = 3
MAX_TARGET_MAP_BYTES = 4 * 2**20


class PlanPublishError(ValueError):
    """The offline publisher cannot safely commit a plan."""


class NoAdmittedItems(PlanPublishError):
    """No candidate passed all gates; ``report_path`` contains rejection evidence."""

    def __init__(self, report_path: Path) -> None:
        super().__init__(f"zero items admitted; rejection report: {report_path}")
        self.report_path = report_path


@dataclass(frozen=True)
class PublishedPlan:
    plan_path: Path
    provenance_path: Path
    index_path: Path
    plan_id: str
    prediction_id: str
    item_count: int
    requested_bytes: int


def _encoded(value: object) -> bytes:
    return canonical_json(value).encode("utf-8") + b"\n"


def _digest_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _identifier_digest(prefix: str, value: object, length: int = 24) -> str:
    return f"{prefix}-{_digest_bytes(canonical_json(value).encode())[:length]}"


def _load_owner_target_map(path: Path) -> tuple[dict, str]:
    path = Path(path)
    parent_fd = require_secure_directory(path.parent)
    try:
        data = read_secure_bytes(
            parent_fd,
            path.name,
            max_bytes=MAX_TARGET_MAP_BYTES,
        )
    finally:
        os.close(parent_fd)
    try:
        value = loads_strict_json(data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise PlanPublishError(f"target map is not complete UTF-8 JSON: {exc}") from exc
    if not isinstance(value, dict):
        raise PlanPublishError("target map must be an object")
    required = {
        "schema_version",
        "target_map_id",
        "data_kind",
        "authorization",
        "snapshot_id",
        "snapshot_manifest_sha256",
        "source_dataset_id",
        "clean_dataset_id",
        "candidate_profile_sha256",
        "targets",
    }
    if set(value) != required:
        raise PlanPublishError("target map fields are invalid")
    if (
        value["schema_version"] != 1
        or value["data_kind"] != "synthetic"
        or value["authorization"] != TARGET_MAP_AUTHORIZATION
        or not isinstance(value["target_map_id"], str)
        or not ID_PATTERN.fullmatch(value["target_map_id"])
        or not isinstance(value["targets"], list)
    ):
        raise PlanPublishError("target map is not an approved synthetic fixture map")
    return value, _digest_bytes(data)


def _verify_package_bindings(
    package: LoadedPackage,
    snapshot_root: Path,
    snapshot: dict,
    *,
    expected_repository_revision: str,
    expected_source_patch_sha256: str | None,
) -> None:
    manifest = package.manifest
    data = manifest.get("data")
    training = manifest.get("training")
    repository_state = (
        training.get("repository_state") if isinstance(training, dict) else None
    )
    source_patch_sha256 = (
        training.get("source_patch_sha256") if isinstance(training, dict) else None
    )
    valid_source_provenance = (
        (repository_state == "clean" and source_patch_sha256 is None)
        or (
            repository_state == "patched"
            and isinstance(source_patch_sha256, str)
            and re.fullmatch(r"[0-9a-f]{64}", source_patch_sha256) is not None
        )
    )
    if re.fullmatch(
        r"(?:[0-9a-f]{40}|[0-9a-f]{64})",
        expected_repository_revision,
    ) is None:
        raise PlanPublishError("expected repository revision is not canonical")
    if expected_source_patch_sha256 is not None and re.fullmatch(
        r"[0-9a-f]{64}", expected_source_patch_sha256
    ) is None:
        raise PlanPublishError("expected source patch SHA-256 is not canonical")
    if (
        package.model_type != "baseline-read-frequency"
        or manifest.get("trained") is not False
        or manifest.get("data_kind") != "synthetic"
        or not isinstance(data, dict)
        or not isinstance(training, dict)
        or training.get("trained") is not False
        or training.get("data_kind") != "synthetic"
        or not isinstance(training.get("repository_revision"), str)
        or re.fullmatch(
            r"(?:[0-9a-f]{40}|[0-9a-f]{64})",
            training["repository_revision"],
        )
        is None
        or not valid_source_provenance
        or training.get("method") != "deterministic_read_count_24h"
    ):
        raise PlanPublishError("package is not the explicit non-trained synthetic baseline")
    expected_state = (
        "patched" if expected_source_patch_sha256 is not None else "clean"
    )
    if (
        training["repository_revision"] != expected_repository_revision
        or repository_state != expected_state
        or source_patch_sha256 != expected_source_patch_sha256
    ):
        raise PlanPublishError("package source provenance mismatch")
    expected = {
        "data_kind": "synthetic",
        "source_dataset_id": snapshot["source"]["dataset_id"],
        "source_revision": snapshot["source"]["revision"],
        "source_config_sha256": snapshot["source"]["config_sha256"],
        "source_manifest_sha256": snapshot["source"]["manifest_sha256"],
        "clean_dataset_id": snapshot["clean"]["dataset_id"],
        "clean_manifest_sha256": snapshot["clean"]["manifest_sha256"],
        "clean_table_sha256": snapshot["clean"]["table_sha256"],
        "snapshots_dataset_id": snapshot["dataset_id"],
        "snapshots_manifest_sha256": file_sha256(snapshot_root / "manifest.json"),
        "snapshot_table_sha256": {
            entry["path"]: entry["sha256"] for entry in snapshot["files"]
        },
        "candidate_profile_id": snapshot["candidate_profile_id"],
        "candidate_profile_sha256": snapshot["candidate_profile_sha256"],
        "snapshot_serving_contract_sha256": snapshot[
            "snapshot_serving_contract_sha256"
        ],
    }
    if data != expected:
        raise PlanPublishError("package source/snapshot/profile binding mismatch")


def _verify_target_map_bindings(
    target_map: dict,
    snapshot_root: Path,
    snapshot: dict,
) -> None:
    expected = {
        "snapshot_id": snapshot["dataset_id"],
        "snapshot_manifest_sha256": file_sha256(snapshot_root / "manifest.json"),
        "source_dataset_id": snapshot["source"]["dataset_id"],
        "clean_dataset_id": snapshot["clean"]["dataset_id"],
        "candidate_profile_sha256": snapshot["candidate_profile_sha256"],
    }
    if any(target_map[key] != value for key, value in expected.items()):
        raise PlanPublishError("target map source/snapshot/profile binding mismatch")


def _target_index(target_map: dict) -> tuple[dict[tuple[str, str], dict], Counter]:
    result: dict[tuple[str, str], dict] = {}
    rejected = Counter()
    fields = {
        "approved",
        "volume_id",
        "file_id",
        "export_name",
        "export_id",
        "target_fh_b64",
        "target_valid_until",
        "offset",
        "length",
    }
    for raw in target_map["targets"]:
        if not isinstance(raw, dict) or set(raw) != fields:
            rejected["malformed_target"] += 1
            continue
        volume_id, file_id = raw.get("volume_id"), raw.get("file_id")
        if not isinstance(volume_id, str) or not isinstance(file_id, str):
            rejected["malformed_target"] += 1
            continue
        key = (volume_id, file_id)
        if key in result:
            raise PlanPublishError(f"duplicate target identity: {volume_id}/{file_id}")
        result[key] = raw
    return result, rejected


def _candidate_records(rows: pa.Table, scores) -> list[dict]:
    """Apply the frozen order: score descending, recency, file ID, volume ID."""
    records = rows.to_pylist()
    if len(records) != len(scores):
        raise PlanPublishError("score count differs from snapshot row count")
    candidates = []
    for row, score in zip(records, scores, strict=True):
        numeric = float(score)
        if not math.isfinite(numeric):
            raise PlanPublishError("baseline produced a non-finite score")
        recency = row["secs_since_last_read"]
        if (
            isinstance(recency, bool)
            or not isinstance(recency, (int, float))
            or not math.isfinite(recency)
            or recency < 0
        ):
            raise PlanPublishError("snapshot row lacks a finite READ recency")
        candidates.append(
            {**row, "score": numeric, "secs_since_last_read": float(recency)}
        )
    return sorted(
        candidates,
        key=lambda row: (
            -row["score"],
            row["secs_since_last_read"],
            row["file_id"],
            row["volume_id"],
        ),
    )


def _target_item(
    target: dict,
    *,
    task_id: str,
    score: float,
    range_cap: int,
    target_map_id: str,
) -> tuple[dict | None, str | None]:
    if target["approved"] is not True:
        return None, "unauthorized_target"
    length = target["length"]
    if isinstance(length, bool) or not isinstance(length, int) or length <= 0:
        return None, "malformed_target"
    length = min(length, range_cap)
    raw = {
        "task_id": task_id,
        "volume_id": target["volume_id"],
        "file_id": target["file_id"],
        "export_name": target["export_name"],
        "export_id": target["export_id"],
        "target_fh_b64": target["target_fh_b64"],
        "target_valid_until": target["target_valid_until"],
        "offset": target["offset"],
        "length": length,
        "max_bytes": length,
        "score": score,
        "reasons": [
            f"baseline-read-frequency:read_count_24h={score:g}",
            f"fixture-target-map:{target_map_id}",
        ],
        "operation": "read",
    }
    try:
        PlanItem.model_validate(raw)
    except ValueError:
        return None, "malformed_target"
    return raw, None


def _open_publish_dirs(root: Path) -> tuple[int, dict[str, int]]:
    root = Path(root)
    root_fd = require_secure_directory(root)
    children = {}
    try:
        for name in ("plans", "provenance", "indexes", "rejections"):
            children[name] = ensure_child_directory(root_fd, name)
        return root_fd, children
    except Exception:
        for fd in children.values():
            os.close(fd)
        os.close(root_fd)
        raise


def _close_publish_dirs(root_fd: int, children: dict[str, int]) -> None:
    for fd in children.values():
        os.close(fd)
    os.close(root_fd)


def _commit_json(fd: int, name: str, value: object) -> tuple[bytes, str]:
    data = _encoded(value)
    if not atomic_write_bytes(fd, name, data, no_overwrite=True):
        raise PlanPublishError(f"committed artifact already exists: {name}")
    return data, _digest_bytes(data)


def publish_read_demand_plan(
    snapshot_root: Path,
    package_path: Path,
    target_map_path: Path,
    dispatcher_root: Path,
    *,
    run_id: str,
    created_at: datetime,
    deadline: datetime,
    expected_repository_revision: str,
    expected_source_patch_sha256: str | None,
) -> PublishedPlan:
    """Verify identities, score, budget and atomically publish one schema-v2 plan."""
    if (
        created_at.tzinfo is None
        or deadline.tzinfo is None
        or created_at.utcoffset() != UTC.utcoffset(created_at)
        or deadline.utcoffset() != UTC.utcoffset(deadline)
    ):
        raise PlanPublishError("created_at and deadline must be timezone-aware UTC")
    created_at = created_at.astimezone(UTC)
    deadline = deadline.astimezone(UTC)
    if created_at >= deadline:
        raise PlanPublishError("created_at must be earlier than deadline")
    if not isinstance(run_id, str) or not ID_PATTERN.fullmatch(run_id):
        raise PlanPublishError(f"run_id must match {ID_PATTERN.pattern}")

    snapshot_root = Path(snapshot_root)
    snapshot, rows = load_narrow_read_demand_snapshot(snapshot_root)
    target_map, target_map_sha = _load_owner_target_map(Path(target_map_path))
    _verify_target_map_bindings(target_map, snapshot_root, snapshot)
    available = runtime_available_features(snapshot)
    try:
        loaded = load_package(
            Path(package_path),
            READ_DEMAND_FEATURE_VERSION,
            READ_DEMAND_SERVING_CONTRACT_SHA256,
            expected_semantic_view_id=SEMANTIC_VIEW_ID,
            expected_semantic_view_sha256=SEMANTIC_VIEW_SHA256,
            expected_feature_schema_sha256=READ_DEMAND_FEATURE_SCHEMA_SHA256,
            available_feature_names=available,
            extract_dir=Path(dispatcher_root),
        )
    except (PackageError, OSError) as exc:
        raise PlanPublishError(f"package verification failed: {exc}") from exc

    with loaded:
        _verify_package_bindings(
            loaded,
            snapshot_root,
            snapshot,
            expected_repository_revision=expected_repository_revision,
            expected_source_patch_sha256=expected_source_patch_sha256,
        )
        policy = loaded.serving_policy
        candidates = _candidate_records(rows, loaded.score(rows))
        target_by_id, rejected = _target_index(target_map)
        prediction_identity = {
            "package_id": loaded.package_id,
            "snapshot_id": snapshot["dataset_id"],
            "snapshot_manifest_sha256": file_sha256(snapshot_root / "manifest.json"),
            "target_map_id": target_map["target_map_id"],
            "target_map_sha256": target_map_sha,
            "created_at": format_utc_z(created_at),
            "deadline": format_utc_z(deadline),
            "serving_policy": policy,
        }
        prediction_id = _identifier_digest("prediction", prediction_identity)
        plan_id = _identifier_digest(
            "plan", {"run_id": run_id, "prediction_id": prediction_id}
        )
        selected = []
        requested_bytes = 0
        for candidate in candidates:
            if policy["min_score"] is not None and candidate["score"] < policy["min_score"]:
                rejected["below_min_score"] += 1
                continue
            key = (candidate["volume_id"], candidate["file_id"])
            target = target_by_id.get(key)
            if target is None:
                rejected["missing_target"] += 1
                continue
            task_id = _identifier_digest(
                "task",
                {
                    "plan_id": plan_id,
                    "volume_id": candidate["volume_id"],
                    "file_id": candidate["file_id"],
                },
            )
            item, reason = _target_item(
                target,
                task_id=task_id,
                score=candidate["score"],
                range_cap=policy["range_cap"],
                target_map_id=target_map["target_map_id"],
            )
            if reason:
                rejected[reason] += 1
                continue
            item_valid_until = PlanItem.model_validate(item).target_valid_until
            ttl = (item_valid_until - created_at).total_seconds()
            if not created_at < deadline <= item_valid_until:
                rejected["target_validity"] += 1
                continue
            if ttl <= 0 or ttl > MAX_TTL_SECONDS:
                rejected["target_ttl"] += 1
                continue
            if len(selected) >= policy["max_items"]:
                rejected["max_items"] += 1
                continue
            if requested_bytes + item["length"] > policy["max_bytes"]:
                rejected["max_bytes"] += 1
                continue
            selected.append(item)
            requested_bytes += item["length"]

        report_base = {
            "schema_version": 1,
            "run_id": run_id,
            "prediction_id": prediction_id,
            "plan_id": plan_id,
            "source": snapshot["source"],
            "clean": snapshot["clean"],
            "snapshot": {
                "dataset_id": snapshot["dataset_id"],
                "manifest_sha256": prediction_identity["snapshot_manifest_sha256"],
                "candidate_profile_sha256": snapshot["candidate_profile_sha256"],
                "snapshot_serving_contract_sha256": snapshot[
                    "snapshot_serving_contract_sha256"
                ],
            },
            "package": {
                "package_id": loaded.package_id,
                "model_version": loaded.model_version,
                "trained": False,
                "data_kind": "synthetic",
            },
            "target_map": {
                "target_map_id": target_map["target_map_id"],
                "sha256": target_map_sha,
                "authorization": TARGET_MAP_AUTHORIZATION,
            },
            "created_at": format_utc_z(created_at),
            "deadline": format_utc_z(deadline),
            "serving_policy": policy,
            "rejection_counts": dict(sorted(rejected.items())),
        }

        root_fd, dirs = _open_publish_dirs(Path(dispatcher_root))
        try:
            if not selected:
                rejection = {
                    **report_base,
                    "status": "rejected",
                    "reason": "zero_items_admitted",
                    "admitted_items": 0,
                    "requested_bytes": 0,
                }
                name = f"{prediction_id}.json"
                _, report_sha = _commit_json(dirs["rejections"], name, rejection)
                _commit_json(
                    dirs["indexes"],
                    f"{prediction_id}-rejection.json",
                    {
                        "schema_version": 1,
                        "prediction_id": prediction_id,
                        "status": "rejected",
                        "report_path": f"rejections/{name}",
                        "report_sha256": report_sha,
                    },
                )
                raise NoAdmittedItems(Path(dispatcher_root) / "rejections" / name)

            plan_raw = {
                "schema_version": 2,
                "run_id": run_id,
                "plan_id": plan_id,
                "model_version": loaded.model_version,
                "created_at": format_utc_z(created_at),
                "deadline": format_utc_z(deadline),
                "max_bytes": policy["max_bytes"],
                "items": selected,
            }
            plan = Plan.model_validate(plan_raw)
            if any(isinstance(item, InvalidPlanItem) for item in plan.items):
                raise PlanPublishError("publisher produced an invalid dispatcher item")
            if plan.max_bytes < sum(item.length for item in plan.items):
                raise PlanPublishError("plan byte budget is smaller than selected ranges")
            provenance = {
                **report_base,
                "status": "published",
                "admitted_items": len(selected),
                "requested_bytes": requested_bytes,
                "plan": {
                    "run_id": run_id,
                    "plan_id": plan_id,
                    "model_version": loaded.model_version,
                },
                "tasks": [
                    {
                        "task_id": item["task_id"],
                        "volume_id": item["volume_id"],
                        "file_id": item["file_id"],
                        "expected_dispatcher_result": {
                            "status": "skipped",
                            "reason": "dry_run",
                            "bytes_requested": item["length"],
                            "bytes_read": 0,
                            "bytes_read_is_lower_bound": False,
                            "plan_ref": warmer_ref(plan_id),
                            "task_ref": warmer_ref(item["task_id"]),
                        },
                    }
                    for item in selected
                ],
            }
            plan_bytes = _encoded(plan_raw)
            plan_sha = _digest_bytes(plan_bytes)
            provenance_bytes = _encoded(provenance)
            provenance_sha = _digest_bytes(provenance_bytes)
            index = {
                "schema_version": 1,
                "run_id": run_id,
                "plan_id": plan_id,
                "prediction_id": prediction_id,
                "plan_path": f"plans/{plan_id}.json",
                "plan_sha256": plan_sha,
                "provenance_path": f"provenance/{plan_id}.json",
                "provenance_sha256": provenance_sha,
            }
            _commit_json(dirs["provenance"], f"{plan_id}.json", provenance)
            _commit_json(dirs["indexes"], f"{plan_id}.json", index)
            if not atomic_write_bytes(
                dirs["plans"],
                f"{plan_id}.json",
                plan_bytes,
                no_overwrite=True,
            ):
                raise PlanPublishError(f"committed plan already exists: {plan_id}")
        finally:
            _close_publish_dirs(root_fd, dirs)

    return PublishedPlan(
        plan_path=Path(dispatcher_root) / "plans" / f"{plan_id}.json",
        provenance_path=Path(dispatcher_root) / "provenance" / f"{plan_id}.json",
        index_path=Path(dispatcher_root) / "indexes" / f"{plan_id}.json",
        plan_id=plan_id,
        prediction_id=prediction_id,
        item_count=len(selected),
        requested_bytes=requested_bytes,
    )
