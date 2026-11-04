"""Publish a deterministic Pass-1 prediction schedule from one snapshot tick."""

from __future__ import annotations

import hashlib
import math
import os
import zipfile
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc

from predictive_cache.dispatcher.schema import format_utc_z
from predictive_cache.semantics.read_demand import (
    SEMANTIC_VIEW_ID,
    SEMANTIC_VIEW_SHA256,
)

from .hashing import canonical_json, file_sha256
from .package import PackageError, load_package
from .read_demand import (
    READ_DEMAND_FEATURE_SCHEMA_SHA256,
    READ_DEMAND_FEATURE_VERSION,
    READ_DEMAND_SERVING_CONTRACT_SHA256,
)
from .snapshots import (
    load_split,
    read_manifest,
    runtime_available_features,
    verify_read_demand_manifest,
)

SCHEDULE_VERSION = 1
ZERO_ITEMS_EXIT_CODE = 4
TRAINED_MODEL_TYPES = {"logreg-v1", "gbdt-v1", "gbdt-rank-v1"}
ORDERING = ["score_desc", "file_id_asc", "volume_id_asc"]


class SchedulePublishError(ValueError):
    """The snapshot/package pair cannot safely produce a frozen schedule."""


class NoScheduleItems(SchedulePublishError):
    """No prediction survived the frozen package policy."""


@dataclass(frozen=True)
class PublishedSchedule:
    path: Path
    schedule_id: str
    item_count: int
    requested_bytes: int


def _json_value(value):
    if isinstance(value, datetime):
        return format_utc_z(value)
    if isinstance(value, dict):
        return {key: _json_value(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_value(item) for item in value]
    return value


def _digest(value: object) -> str:
    return hashlib.sha256(canonical_json(_json_value(value)).encode()).hexdigest()


def _package_manifest_sha256(path: Path) -> str:
    try:
        if path.is_dir():
            return file_sha256(path / "package.json")
        with zipfile.ZipFile(path) as archive:
            return hashlib.sha256(archive.read("package.json")).hexdigest()
    except (OSError, KeyError, zipfile.BadZipFile) as exc:
        raise SchedulePublishError(f"cannot hash package manifest: {exc}") from exc


def _package_artifact_sha256(path: Path) -> str:
    if path.is_file():
        return file_sha256(path)
    return _package_manifest_sha256(path)


def _trained_evidence(manifest: dict, model_type: str) -> str:
    if model_type not in TRAINED_MODEL_TYPES:
        raise SchedulePublishError("package is not a trained read-demand model")
    if manifest.get("trained") is True:
        return "explicit"
    if manifest.get("trained") is False:
        raise SchedulePublishError("package is not a trained read-demand model")
    training = manifest.get("training")
    required = ("git_sha", "config_sha256", "selected_by", "val_metrics")
    if (
        not isinstance(training, dict)
        or training.get("trained") is False
        or any(not training.get(key) for key in required[:-1])
        or not isinstance(training.get("val_metrics"), dict)
    ):
        raise SchedulePublishError("legacy package lacks sealed training metadata")
    return "legacy_training_metadata"


def _snapshot_files(root: Path) -> dict[str, str]:
    files = [root / "manifest.json", root / "feature_schema.json"]
    files.extend(sorted((root / "snapshots" / "split=val").rglob("*.parquet")))
    if any(not path.is_file() for path in files):
        raise SchedulePublishError("snapshot validation files are incomplete")
    return {path.relative_to(root).as_posix(): file_sha256(path) for path in sorted(files)}


def _selected_rows(rows: pa.Table, cutoff: datetime) -> pa.Table:
    if "t" not in rows.column_names:
        raise SchedulePublishError("snapshot rows lack prediction time")
    selected = rows.filter(pc.equal(rows.column("t"), pa.scalar(cutoff, type=rows["t"].type)))
    if selected.num_rows == 0:
        raise SchedulePublishError("prediction cutoff is absent from the validation snapshot")
    if "split" in selected.column_names and set(selected["split"].to_pylist()) != {"val"}:
        raise SchedulePublishError("prediction schedule accepts validation rows only")
    if "sample_weight" in selected.column_names and any(
        weight != 1.0 for weight in selected["sample_weight"].to_pylist()
    ):
        raise SchedulePublishError("prediction rows are label-sampled")
    identities = list(
        zip(
            selected["volume_id"].to_pylist(),
            selected["file_id"].to_pylist(),
            strict=True,
        )
    )
    if len(identities) != len(set(identities)):
        raise SchedulePublishError("duplicate prediction identity")
    return selected


def _range_length(row: dict, policy: dict, capabilities: dict) -> tuple[int, str]:
    cap = policy["range_cap"]
    if capabilities.get("verified_file_size") is not True:
        return cap, "range_cap:unverified_file_size"
    value = row.get("file_size_bytes")
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not math.isfinite(value)
        or value <= 0
        or value != int(value)
    ):
        return cap, "range_cap:invalid_file_size"
    return min(cap, int(value)), "range_cap:verified_file_size"


def publish_read_demand_schedule(
    snapshot_root: Path,
    package_path: Path,
    out_dir: Path,
    *,
    prediction_cutoff: datetime,
    warm_start_offset_s: int = 0,
    warm_spacing_s: int = 0,
) -> PublishedSchedule:
    """Score one validation tick and publish one immutable handle-free JSON schedule."""
    if prediction_cutoff.tzinfo is None or prediction_cutoff.utcoffset() != UTC.utcoffset(
        prediction_cutoff
    ):
        raise SchedulePublishError("prediction cutoff must be timezone-aware UTC")
    prediction_cutoff = prediction_cutoff.astimezone(UTC)
    for name, value in (
        ("warm_start_offset_s", warm_start_offset_s),
        ("warm_spacing_s", warm_spacing_s),
    ):
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise SchedulePublishError(f"{name} must be a non-negative integer")

    snapshot_root = Path(snapshot_root)
    package_path = Path(package_path)
    manifest = read_manifest(snapshot_root)
    verify_read_demand_manifest(snapshot_root, manifest)
    if (
        manifest.get("stage") != "snapshots"
        or manifest.get("candidate_profile") != SEMANTIC_VIEW_ID
    ):
        raise SchedulePublishError("schedule requires a canonical read-demand snapshot")
    rows = _selected_rows(load_split(snapshot_root, "val", allowed=("val",)), prediction_cutoff)
    available = runtime_available_features(manifest)
    try:
        loaded = load_package(
            package_path,
            READ_DEMAND_FEATURE_VERSION,
            READ_DEMAND_SERVING_CONTRACT_SHA256,
            expected_semantic_view_id=SEMANTIC_VIEW_ID,
            expected_semantic_view_sha256=SEMANTIC_VIEW_SHA256,
            expected_feature_schema_sha256=READ_DEMAND_FEATURE_SCHEMA_SHA256,
            available_feature_names=available,
        )
    except (PackageError, OSError) as exc:
        raise SchedulePublishError(f"package verification failed: {exc}") from exc

    with loaded:
        trained_evidence = _trained_evidence(loaded.manifest, loaded.model_type)
        policy = loaded.serving_policy
        period_end = datetime.fromisoformat(manifest["period"]["end"].replace("Z", "+00:00"))
        if prediction_cutoff + timedelta(seconds=policy["horizon_s"]) > period_end:
            raise SchedulePublishError(
                "prediction cutoff lacks the package policy's future coverage horizon"
            )
        try:
            raw_scores = loaded.score(rows)
            explanations = loaded.scorer.explain(rows, k=2)
        except ValueError as exc:
            raise SchedulePublishError(f"scoring failed: {exc}") from exc
        records = rows.to_pylist()
        if len(raw_scores) != len(records) or len(explanations) != len(records):
            raise SchedulePublishError("score or explanation count differs from row count")
        candidates = []
        for row, raw_score, reasons in zip(records, raw_scores, explanations, strict=True):
            score = float(raw_score)
            if not math.isfinite(score):
                raise SchedulePublishError("model produced a non-finite score")
            emitted_score = float(f"{score:.12g}")
            if not isinstance(reasons, list) or any(
                not isinstance(reason, str) or not reason for reason in reasons
            ):
                raise SchedulePublishError("model produced invalid reasons")
            candidates.append(
                {
                    **row,
                    "raw_score": score,
                    "score": emitted_score,
                    "reasons": list(reasons),
                }
            )
        candidates.sort(key=lambda row: (-row["score"], row["file_id"], row["volume_id"]))

        selected = []
        requested_bytes = 0
        rejected: dict[str, int] = {}
        capabilities = manifest["runtime_capabilities"]
        for candidate in candidates:
            if policy["min_score"] is not None and candidate["raw_score"] < policy["min_score"]:
                rejected["below_min_score"] = rejected.get("below_min_score", 0) + 1
                continue
            length, range_reason = _range_length(candidate, policy, capabilities)
            if len(selected) >= policy["max_items"]:
                rejected["max_items"] = rejected.get("max_items", 0) + 1
                continue
            if requested_bytes + length > policy["max_bytes"]:
                rejected["max_bytes"] = rejected.get("max_bytes", 0) + 1
                continue
            rank = len(selected) + 1
            warm_offset = warm_start_offset_s + (rank - 1) * warm_spacing_s
            item_core = {
                "rank": rank,
                "volume_id": candidate["volume_id"],
                "file_id": candidate["file_id"],
                "prediction_cutoff": format_utc_z(prediction_cutoff),
                "warm_offset_s": warm_offset,
                "score": candidate["score"],
                "reasons": [*candidate["reasons"], range_reason],
                "logical_range": {"offset": 0, "length": length, "unit": "bytes"},
            }
            selected.append(
                {
                    "prediction_id": (
                        "prediction-"
                        + _digest(
                            {
                                "package_id": loaded.package_id,
                                "snapshot_id": manifest["dataset_id"],
                                **item_core,
                            }
                        )[:24]
                    ),
                    **item_core,
                }
            )
            requested_bytes += length

        if not selected:
            raise NoScheduleItems("zero predictions admitted")
        if len({(item["volume_id"], item["file_id"]) for item in selected}) != len(selected):
            raise SchedulePublishError("schedule contains duplicate identities")
        if requested_bytes != sum(item["logical_range"]["length"] for item in selected):
            raise SchedulePublishError("schedule byte accounting mismatch")

        scoring_columns = ["t", "volume_id", "file_id", *loaded.feature_columns]
        scoring_rows = [_json_value(row) for row in rows.select(scoring_columns).to_pylist()]
        files = _snapshot_files(snapshot_root)
        package_manifest_sha = _package_manifest_sha256(package_path)
        document = {
            "schema_version": SCHEDULE_VERSION,
            "semantic_view": {
                "id": SEMANTIC_VIEW_ID,
                "sha256": SEMANTIC_VIEW_SHA256,
                "feature_version": READ_DEMAND_FEATURE_VERSION,
                "feature_schema_sha256": READ_DEMAND_FEATURE_SCHEMA_SHA256,
                "serving_contract_sha256": READ_DEMAND_SERVING_CONTRACT_SHA256,
            },
            "snapshot": {
                "dataset_id": manifest["dataset_id"],
                "manifest_sha256": file_sha256(snapshot_root / "manifest.json"),
                "files_sha256": files,
                "candidate_profile": manifest["candidate_profile"],
                "runtime_capabilities": capabilities,
                "scoring_rows_sha256": _digest(scoring_rows),
                "prediction_cutoff": format_utc_z(prediction_cutoff),
            },
            "package": {
                "package_id": loaded.package_id,
                "artifact_sha256": _package_artifact_sha256(package_path),
                "package_manifest_sha256": package_manifest_sha,
                "model_type": loaded.model_type,
                "model_version": loaded.model_version,
                "trained": True,
                "trained_evidence": trained_evidence,
                "data_kind": loaded.manifest.get("data_kind")
                or (loaded.manifest.get("data") or {}).get("data_kind"),
            },
            "policy": policy,
            "policy_sha256": _digest(policy),
            "time_basis": {
                "kind": "pass2_start_relative",
                "warm_start_offset_s": warm_start_offset_s,
                "warm_spacing_s": warm_spacing_s,
                "score_quantization": "12-significant-digits",
            },
            "ordering": ORDERING,
            "budget": {
                "max_items": policy["max_items"],
                "max_bytes": policy["max_bytes"],
                "range_cap_bytes": policy["range_cap"],
            },
            "rejection_counts": dict(sorted(rejected.items())),
            "admitted_items": len(selected),
            "requested_bytes": requested_bytes,
            "items": selected,
        }
        schedule_id = f"schedule-{_digest(document)[:24]}"
        schedule = {"schedule_id": schedule_id, **document}
        data = (canonical_json(schedule) + "\n").encode()

    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{schedule_id}.json"
    if path.exists():
        if path.read_bytes() != data:
            raise SchedulePublishError("hash collision with different schedule bytes")
    else:
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        fd = os.open(path, flags, 0o600)
        try:
            with os.fdopen(fd, "wb", closefd=False) as stream:
                stream.write(data)
                stream.flush()
                os.fsync(stream.fileno())
        finally:
            os.close(fd)
    return PublishedSchedule(path, schedule_id, len(selected), requested_bytes)
