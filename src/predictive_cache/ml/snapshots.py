"""Read data-prep snapshot directories (docs/data-prep/spec.md §8.4-§8.6).

Training may read only the ``train`` and ``val`` splits; ``test`` is reserved
for the frozen evaluation (#58), so this loader refuses to open it unless the
caller explicitly allows it.
"""

from __future__ import annotations

import hashlib
import json
import shutil
import tempfile
from collections.abc import Iterable
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from predictive_cache.semantics.read_demand import (
    SEMANTIC_VIEW_ID,
    SEMANTIC_VIEW_SHA256,
    ReadDemandEvent,
)

from .hashing import canonical_json, file_sha256
from .read_demand import (
    READ_DEMAND_FEATURE_SCHEMA_SHA256,
    READ_DEMAND_FEATURE_VERSION,
    READ_DEMAND_SERVING_CONTRACT,
    READ_DEMAND_SERVING_CONTRACT_SHA256,
    feature_schema,
)
from .read_demand import runtime_available_features as read_demand_runtime_features

TRAINING_SPLITS = ("train", "val")
ALL_SPLITS = ("train", "val", "test")
NARROW_CANDIDATE_PROFILE_ID = "read-demand-v1-narrow-mvp"
INFERENCE_TABLE_PATH = "inference/rows.parquet"
OMISSIONS_PATH = "inference/omissions.jsonl"
CANDIDATE_PROFILE_PATH = "candidate-profile.json"
FEATURE_SCHEMA_PATH = "feature_schema.json"


class SnapshotError(ValueError):
    """The snapshot directory is missing something or violates the split rules."""


def read_manifest(root: Path) -> dict:
    path = Path(root) / "manifest.json"
    if not path.is_file():
        raise SnapshotError(f"{root} has no manifest.json")
    return json.loads(path.read_text())


def manifest_sha256(root: Path) -> str:
    return file_sha256(Path(root) / "manifest.json")


def read_feature_schema(root: Path) -> dict:
    path = Path(root) / "feature_schema.json"
    if not path.is_file():
        raise SnapshotError(f"{root} has no feature_schema.json")
    return json.loads(path.read_text())


def _json_sha256(value: object) -> str:
    return hashlib.sha256(canonical_json(value).encode()).hexdigest()


def narrow_candidate_profile(max_candidates: int) -> dict:
    if isinstance(max_candidates, bool) or not isinstance(max_candidates, int):
        raise SnapshotError("max_candidates must be an integer")
    if not 1 <= max_candidates <= 100_000:
        raise SnapshotError("max_candidates must be in 1..100000")
    return {
        "candidate_profile_id": NARROW_CANDIDATE_PROFILE_ID,
        "algorithm": "visible-eligible-read-frequency-v1",
        "enabled_sources": ["controlled_demand", "fixture_demand"],
        "data_kind": "synthetic",
        "max_candidates": max_candidates,
        "feature_requirements": ["read_count_24h"],
        "coverage_rule": "complete_required_24h_window_or_omit",
        "ordering": [
            "read_count_24h_desc",
            "secs_since_last_read_asc",
            "file_id_asc",
            "volume_id_asc",
        ],
        "visibility": "known_at_lte_tick_and_event_time_lte_tick",
        "identity": ["volume_id", "file_id"],
        "excluded_origins": ["warmer", "unknown"],
        "excluded_demand_classes": ["warmer", "unknown", "foreground"],
        "lifecycle_features": False,
        "project_candidates": False,
        "directory_candidates": False,
        "session_candidates": False,
    }


def candidate_profile_sha256(profile: dict) -> str:
    return _json_sha256(profile)


def narrow_snapshot_serving_contract(profile_sha256: str) -> dict:
    return {
        "semantic_view_id": SEMANTIC_VIEW_ID,
        "feature_version": READ_DEMAND_FEATURE_VERSION,
        "canonical_serving_contract_sha256": READ_DEMAND_SERVING_CONTRACT_SHA256,
        "candidate_profile_id": NARROW_CANDIDATE_PROFILE_ID,
        "candidate_profile_sha256": profile_sha256,
        "runtime_capabilities": {
            "verified_file_size": False,
            "anchor_context": False,
        },
        "scoring_feature": "read_count_24h",
    }


def coaccess_path(root: Path) -> Path | None:
    path = Path(root) / "artifacts" / "coaccess.parquet"
    return path if path.is_file() else None


def serving_contract(manifest: dict) -> dict:
    if manifest.get("semantic_view_id") == "read-demand-v1":
        required = ("semantic_view_sha256", "feature_schema_sha256", "serving_contract_sha256")
        if manifest.get("feature_version") != "read-demand-fv1" or any(
            not manifest.get(key) for key in required
        ):
            raise SnapshotError("read-demand-v1 manifest lacks required semantic keys")
    contract = manifest.get("serving_contract")
    if not isinstance(contract, dict) or not isinstance(contract.get("config"), dict):
        raise SnapshotError("snapshots manifest lacks serving_contract.config (data-prep §8.6)")
    return contract


def _partition_value(path: Path, key: str) -> str | None:
    for part in path.parts:
        if part.startswith(f"{key}="):
            return part.split("=", 1)[1]
    return None


def _read_parts(files: Iterable[Path], fill: dict[str, str]) -> pa.Table | None:
    tables = []
    for file in sorted(files):
        table = pq.ParquetFile(file).read()  # no hive discovery: in-file columns win
        for key in fill:
            value = _partition_value(file, key)
            if key not in table.column_names and value is not None:
                table = table.append_column(key, pa.array([value] * table.num_rows))
        tables.append(table)
    if not tables:
        return None
    return pa.concat_tables(tables, promote_options="default")


def load_split(root: Path, split: str, allowed: tuple[str, ...] = TRAINING_SPLITS) -> pa.Table:
    """Concatenate ``snapshots/split=<split>/**/*.parquet``; refuses splits outside ``allowed``."""
    if split not in ALL_SPLITS:
        raise SnapshotError(f"unknown split {split!r}")
    if split not in allowed:
        raise SnapshotError(f"reading split {split!r} is not allowed here (allowed: {allowed})")
    directory = Path(root) / "snapshots" / f"split={split}"
    table = _read_parts(directory.rglob("*.parquet"), {"split": split, "volume_id": ""})
    if table is None:
        raise SnapshotError(f"no rows for split {split!r} under {directory}")
    return table


def load_universe(root: Path) -> pa.Table:
    table = _read_parts((Path(root) / "universe").rglob("*.parquet"), {"volume_id": ""})
    if table is None:
        raise SnapshotError(f"{root} has no universe table")
    return table


def verify_read_demand_manifest(root: Path, manifest: dict) -> dict:
    """Verify all read-demand bindings against shared semantics and snapshot bytes."""
    if manifest.get("semantic_view_id") != "read-demand-v1":
        return manifest
    from predictive_cache.semantics.read_demand import SEMANTIC_VIEW_SHA256

    from .package import contract_sha256
    from .read_demand import (
        READ_DEMAND_FEATURE_SCHEMA_SHA256,
        READ_DEMAND_SERVING_CONTRACT,
        READ_DEMAND_SERVING_CONTRACT_SHA256,
        feature_schema,
    )

    if manifest.get("semantic_view_sha256") != SEMANTIC_VIEW_SHA256:
        raise SnapshotError("read-demand semantic hash mismatch")
    schema = read_feature_schema(root)
    if schema != feature_schema():
        raise SnapshotError("read-demand feature schema is not canonical")
    if manifest.get("feature_schema_sha256") != READ_DEMAND_FEATURE_SCHEMA_SHA256:
        raise SnapshotError("read-demand feature schema hash mismatch")
    contract = serving_contract(manifest)
    if contract["config"] != READ_DEMAND_SERVING_CONTRACT:
        raise SnapshotError("read-demand serving contract is not canonical")
    if (
        manifest.get("serving_contract_sha256")
        != READ_DEMAND_SERVING_CONTRACT_SHA256
        or contract_sha256(contract["config"])
        != READ_DEMAND_SERVING_CONTRACT_SHA256
    ):
        raise SnapshotError("read-demand serving contract hash mismatch")
    runtime_available_features(manifest)
    profile_id = manifest.get("candidate_profile_id")
    if profile_id is not None:
        if profile_id != NARROW_CANDIDATE_PROFILE_ID:
            raise SnapshotError("unknown read-demand candidate profile")
        profile = manifest.get("candidate_profile")
        if not isinstance(profile, dict) or profile != json.loads(
            (Path(root) / CANDIDATE_PROFILE_PATH).read_text()
        ):
            raise SnapshotError("candidate profile payload mismatch")
        profile_sha = candidate_profile_sha256(profile)
        if manifest.get("candidate_profile_sha256") != profile_sha:
            raise SnapshotError("candidate profile hash mismatch")
        narrow_contract = manifest.get("snapshot_serving_contract")
        if narrow_contract != narrow_snapshot_serving_contract(profile_sha):
            raise SnapshotError("narrow snapshot serving contract mismatch")
        if manifest.get("snapshot_serving_contract_sha256") != _json_sha256(
            narrow_contract
        ):
            raise SnapshotError("narrow snapshot serving contract hash mismatch")
    return manifest


def runtime_available_features(manifest: dict) -> set[str]:
    """Validate the explicit serving capability profile for read-demand-v1."""
    if manifest.get("semantic_view_id") != "read-demand-v1":
        return set()
    from .read_demand import runtime_available_features as expected_features

    capabilities = manifest.get("runtime_capabilities")
    claimed = manifest.get("runtime_available_features")
    if not isinstance(capabilities, dict) or not isinstance(claimed, list):
        raise SnapshotError("read-demand snapshot lacks runtime capability profile")
    expected = expected_features(
        verified_file_size=capabilities.get("verified_file_size") is True,
        anchor_context=capabilities.get("anchor_context") is True,
    )
    if set(claimed) != set(expected):
        raise SnapshotError("read-demand runtime feature profile mismatch")
    return set(claimed)


def build_read_demand_feature_rows(
    events: list[dict],
    coverage: list[dict],
    ticks: list,
    candidates: list[dict],
) -> list[dict]:
    """Build offline snapshot rows with the same engine used by live updates."""
    from .read_demand import ReadDemandEngine

    engine = ReadDemandEngine()
    for row in coverage:
        engine.update_coverage(row["volume_id"], row["hour_start"], row["status"])
    ordered = sorted(events, key=lambda row: (row["known_at"], row["event_id"]))
    cursor = 0
    result = []
    for tick in ticks:
        while cursor < len(ordered) and ordered[cursor]["known_at"] <= tick:
            engine.update(ordered[cursor])
            cursor += 1
        for candidate in candidates:
            features = engine.features(
                tick,
                volume_id=candidate["volume_id"],
                file_id=candidate["file_id"],
                anchor_file_id=candidate.get("anchor_file_id"),
            )
            result.append({"t": tick, **candidate, **features})
    return result


def _read_verified_clean_tables(clean_root: Path) -> tuple[dict, list[dict], list[dict]]:
    from predictive_cache.prep.clean.io import read_rows

    clean_root = Path(clean_root)
    manifest_path = clean_root / "manifest.json"
    try:
        manifest = json.loads(manifest_path.read_text())
    except (OSError, ValueError) as exc:
        raise SnapshotError(f"invalid clean manifest: {exc}") from exc
    if (
        manifest.get("stage") != "clean"
        or manifest.get("semantic_view_id") != SEMANTIC_VIEW_ID
        or manifest.get("semantic_view_sha256") != SEMANTIC_VIEW_SHA256
        or manifest.get("data_kind") != "synthetic"
    ):
        raise SnapshotError("narrow inference requires a synthetic read-demand-v1 clean dataset")
    files = manifest.get("files")
    if not isinstance(files, list):
        raise SnapshotError("clean manifest files must be a list")
    read_events: list[dict] = []
    coverage: list[dict] = []
    for entry in files:
        if (
            not isinstance(entry, dict)
            or not isinstance(entry.get("path"), str)
            or not isinstance(entry.get("sha256"), str)
        ):
            raise SnapshotError("clean manifest contains an invalid file entry")
        relative = Path(entry["path"])
        if relative.is_absolute() or ".." in relative.parts:
            raise SnapshotError("clean manifest contains an unsafe path")
        path = clean_root / relative
        if not path.is_file() or file_sha256(path) != entry["sha256"]:
            raise SnapshotError(f"clean artifact hash mismatch: {entry['path']}")
        if entry["path"].startswith("read_events/"):
            read_events.extend(read_rows(path))
        elif entry["path"].startswith("coverage/"):
            coverage.extend(read_rows(path))
    if not read_events:
        raise SnapshotError("clean dataset has no read_events rows")
    if not coverage:
        raise SnapshotError("clean dataset has no coverage rows")
    try:
        events = [
            ReadDemandEvent.model_validate(row).model_dump(mode="python")
            for row in read_events
        ]
    except ValueError as exc:
        raise SnapshotError(f"invalid read_events row: {exc}") from exc
    normalized_coverage = []
    for row in coverage:
        try:
            hour_start = row["hour_start"]
            if isinstance(hour_start, str):
                hour_start = datetime.fromisoformat(hour_start.replace("Z", "+00:00"))
            if (
                not isinstance(hour_start, datetime)
                or hour_start.tzinfo is None
                or hour_start.utcoffset() != UTC.utcoffset(hour_start)
                or row["status"] not in {"complete", "incomplete", "unknown", "no_counters"}
            ):
                raise ValueError
            normalized_coverage.append(
                {
                    **row,
                    "hour_start": hour_start.astimezone(UTC),
                    "status": "unknown" if row["status"] == "no_counters" else row["status"],
                }
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise SnapshotError("invalid coverage row") from exc
    return manifest, events, normalized_coverage


def _verify_source_manifest(clean_manifest: dict, source_manifest_path: Path) -> dict:
    source_manifest_path = Path(source_manifest_path)
    try:
        source = json.loads(source_manifest_path.read_text())
    except (OSError, ValueError) as exc:
        raise SnapshotError(f"invalid source manifest: {exc}") from exc
    parent = clean_manifest.get("parent") or {}
    if (
        parent.get("dataset_id") != source.get("dataset_id")
        or parent.get("manifest_sha256") != file_sha256(source_manifest_path)
    ):
        raise SnapshotError("source manifest does not match the clean dataset parent")
    revision = (source.get("generator") or {}).get("git_sha")
    config_sha = source.get("config_sha256")
    if not isinstance(revision, str) or not revision or revision == "unknown":
        raise SnapshotError("source manifest lacks generator.git_sha")
    if (
        not isinstance(config_sha, str)
        or len(config_sha) != 64
        or any(character not in "0123456789abcdef" for character in config_sha)
    ):
        raise SnapshotError("source manifest lacks config_sha256")
    return source


def _write_snapshot_table(path: Path, rows: list[dict]) -> None:
    available = sorted(
        read_demand_runtime_features(
            verified_file_size=False,
            anchor_context=False,
        )
    )
    columns = ["t", "volume_id", "file_id", *available]
    if rows:
        table = pa.Table.from_pylist(
            [{key: row.get(key) for key in columns} for row in rows]
        ).select(columns)
    else:
        table = pa.table(
            {
                "t": pa.array([], type=pa.timestamp("us", tz="UTC")),
                "volume_id": pa.array([], type=pa.string()),
                "file_id": pa.array([], type=pa.string()),
                **{
                    name: pa.array([], type=pa.float64())
                    for name in available
                },
            }
        )
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(
        table,
        path,
        compression="zstd",
        use_dictionary=False,
        version="2.6",
        coerce_timestamps="us",
        allow_truncated_timestamps=False,
    )


def build_narrow_read_demand_snapshot(
    clean_root: Path,
    source_manifest_path: Path,
    out_root: Path,
    *,
    tick: datetime,
    max_candidates: int = 100,
) -> dict:
    """Write one deterministic, inference-only READ-demand snapshot."""
    if tick.tzinfo is None or tick.utcoffset() != UTC.utcoffset(tick):
        raise SnapshotError("tick must be timezone-aware UTC")
    tick = tick.astimezone(UTC)
    clean_root = Path(clean_root)
    out_root = Path(out_root)
    if out_root.exists():
        raise SnapshotError("snapshot output already exists")
    clean_manifest, events, coverage = _read_verified_clean_tables(clean_root)
    source_manifest = _verify_source_manifest(clean_manifest, source_manifest_path)
    profile = narrow_candidate_profile(max_candidates)
    profile_sha = candidate_profile_sha256(profile)
    capabilities = {"verified_file_size": False, "anchor_context": False}
    available = sorted(read_demand_runtime_features(**capabilities))

    visible = [
        row
        for row in events
        if row["known_at"] <= tick
        and row["event_time"] <= tick
        and row["data_kind"] == "synthetic"
        and row["demand_class"] in {"controlled_demand", "fixture_demand"}
        and row["origin"] not in {"warmer", "unknown"}
    ]
    identities = sorted({(row["volume_id"], row["file_id"]) for row in visible})
    recency_by_identity = {
        identity: (
            tick
            - max(
                row["event_time"]
                for row in visible
                if (row["volume_id"], row["file_id"]) == identity
            )
        ).total_seconds()
        for identity in identities
    }
    candidates = [
        {"volume_id": volume_id, "file_id": file_id}
        for volume_id, file_id in identities
    ]
    raw_rows = build_read_demand_feature_rows(visible, coverage, [tick], candidates)
    omissions = []
    eligible = []
    for row in raw_rows:
        if row["read_count_24h"] is None:
            omissions.append(
                {
                    "volume_id": row["volume_id"],
                    "file_id": row["file_id"],
                    "reason": "required_coverage_incomplete",
                    "required_features": ["read_count_24h"],
                }
            )
            continue
        row["secs_since_last_read"] = recency_by_identity[
            (row["volume_id"], row["file_id"])
        ]
        eligible.append(row)
    eligible.sort(
        key=lambda row: (
            -row["read_count_24h"],
            recency_by_identity[(row["volume_id"], row["file_id"])],
            row["file_id"],
            row["volume_id"],
        )
    )
    selected = eligible[:max_candidates]
    for row in eligible[max_candidates:]:
        omissions.append(
            {
                "volume_id": row["volume_id"],
                "file_id": row["file_id"],
                "reason": "candidate_cap",
                "required_features": ["read_count_24h"],
            }
        )
    omissions.sort(key=lambda row: (row["reason"], row["file_id"], row["volume_id"]))

    out_root.parent.mkdir(parents=True, exist_ok=True)
    stage = Path(
        tempfile.mkdtemp(prefix=f".{out_root.name}-staging-", dir=out_root.parent)
    )
    try:
        (stage / FEATURE_SCHEMA_PATH).write_text(canonical_json(feature_schema()))
        (stage / CANDIDATE_PROFILE_PATH).write_text(canonical_json(profile))
        _write_snapshot_table(stage / INFERENCE_TABLE_PATH, selected)
        omissions_path = stage / OMISSIONS_PATH
        omissions_path.parent.mkdir(parents=True, exist_ok=True)
        omissions_path.write_text(
            "".join(canonical_json(row) + "\n" for row in omissions)
        )
        files = {
            FEATURE_SCHEMA_PATH: file_sha256(stage / FEATURE_SCHEMA_PATH),
            CANDIDATE_PROFILE_PATH: file_sha256(stage / CANDIDATE_PROFILE_PATH),
            INFERENCE_TABLE_PATH: file_sha256(stage / INFERENCE_TABLE_PATH),
            OMISSIONS_PATH: file_sha256(stage / OMISSIONS_PATH),
        }
        clean_tables = {
            entry["path"]: entry["sha256"]
            for entry in clean_manifest["files"]
            if entry["path"].startswith(("read_events/", "coverage/"))
        }
        snapshot_contract = narrow_snapshot_serving_contract(profile_sha)
        identity = {
            "semantic_view_sha256": SEMANTIC_VIEW_SHA256,
            "feature_schema_sha256": READ_DEMAND_FEATURE_SCHEMA_SHA256,
            "serving_contract_sha256": READ_DEMAND_SERVING_CONTRACT_SHA256,
            "candidate_profile_sha256": profile_sha,
            "snapshot_serving_contract_sha256": _json_sha256(snapshot_contract),
            "source_manifest_sha256": file_sha256(source_manifest_path),
            "clean_manifest_sha256": file_sha256(clean_root / "manifest.json"),
            "files": files,
            "tick": tick.isoformat(timespec="microseconds").replace("+00:00", "Z"),
        }
        dataset_id = f"read-demand-snapshot-{_json_sha256(identity)[:16]}"
        manifest = {
            "manifest_version": 1,
            "stage": "inference_snapshot",
            "dataset_id": dataset_id,
            "data_kind": "synthetic",
            "semantic_view_id": SEMANTIC_VIEW_ID,
            "semantic_view_sha256": SEMANTIC_VIEW_SHA256,
            "feature_version": READ_DEMAND_FEATURE_VERSION,
            "feature_schema_sha256": READ_DEMAND_FEATURE_SCHEMA_SHA256,
            "serving_contract": {
                "config": READ_DEMAND_SERVING_CONTRACT,
                "sha256": READ_DEMAND_SERVING_CONTRACT_SHA256,
            },
            "serving_contract_sha256": READ_DEMAND_SERVING_CONTRACT_SHA256,
            "candidate_profile_id": NARROW_CANDIDATE_PROFILE_ID,
            "candidate_profile": profile,
            "candidate_profile_sha256": profile_sha,
            "snapshot_serving_contract": snapshot_contract,
            "snapshot_serving_contract_sha256": _json_sha256(snapshot_contract),
            "runtime_capabilities": capabilities,
            "runtime_available_features": available,
            "tick": identity["tick"],
            "source": {
                "dataset_id": source_manifest["dataset_id"],
                "revision": source_manifest["generator"]["git_sha"],
                "config_sha256": source_manifest["config_sha256"],
                "manifest_sha256": identity["source_manifest_sha256"],
            },
            "clean": {
                "dataset_id": clean_manifest["dataset_id"],
                "manifest_sha256": identity["clean_manifest_sha256"],
                "table_sha256": clean_tables,
            },
            "counts": {
                "visible_identities": len(identities),
                "selected": len(selected),
                "omitted": len(omissions),
            },
            "files": [
                {"path": path, "sha256": digest}
                for path, digest in sorted(files.items())
            ],
            "created_at": identity["tick"],
        }
        (stage / "manifest.json").write_text(canonical_json(manifest) + "\n")
        stage.rename(out_root)
        return manifest
    finally:
        if stage.exists():
            shutil.rmtree(stage)


def load_narrow_read_demand_snapshot(root: Path) -> tuple[dict, pa.Table]:
    root = Path(root)
    manifest = read_manifest(root)
    verify_read_demand_manifest(root, manifest)
    if manifest.get("candidate_profile_id") != NARROW_CANDIDATE_PROFILE_ID:
        raise SnapshotError("snapshot is not the narrow MVP candidate profile")
    files = manifest.get("files")
    if not isinstance(files, list):
        raise SnapshotError("snapshot files must be a list")
    listed = {}
    for entry in files:
        if not isinstance(entry, dict) or set(entry) != {"path", "sha256"}:
            raise SnapshotError("invalid snapshot file entry")
        path = root / entry["path"]
        if not path.is_file() or file_sha256(path) != entry["sha256"]:
            raise SnapshotError(f"snapshot artifact hash mismatch: {entry['path']}")
        listed[entry["path"]] = entry["sha256"]
    if INFERENCE_TABLE_PATH not in listed:
        raise SnapshotError("snapshot lacks inference table")
    identity = {
        "semantic_view_sha256": manifest["semantic_view_sha256"],
        "feature_schema_sha256": manifest["feature_schema_sha256"],
        "serving_contract_sha256": manifest["serving_contract_sha256"],
        "candidate_profile_sha256": manifest["candidate_profile_sha256"],
        "snapshot_serving_contract_sha256": manifest[
            "snapshot_serving_contract_sha256"
        ],
        "source_manifest_sha256": manifest["source"]["manifest_sha256"],
        "clean_manifest_sha256": manifest["clean"]["manifest_sha256"],
        "files": listed,
        "tick": manifest["tick"],
    }
    if manifest.get("dataset_id") != f"read-demand-snapshot-{_json_sha256(identity)[:16]}":
        raise SnapshotError("snapshot dataset identity mismatch")
    table = pq.read_table(root / INFERENCE_TABLE_PATH)
    required = {
        "t",
        "volume_id",
        "file_id",
        *runtime_available_features(manifest),
    }
    if not required <= set(table.column_names):
        raise SnapshotError("inference table lacks required columns")
    forbidden = [
        name
        for name in table.column_names
        if any(
            token in name.lower()
            for token in ("open", "close", "session", "lifecycle", "write")
        )
    ]
    if forbidden:
        raise SnapshotError(f"inference table contains lifecycle fields: {forbidden}")
    return manifest, table
