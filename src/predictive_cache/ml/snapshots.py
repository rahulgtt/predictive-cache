"""Read data-prep snapshot directories (docs/data-prep/spec.md §8.4-§8.6).

Training may read only the ``train`` and ``val`` splits; ``test`` is reserved
for the frozen evaluation (#58), so this loader refuses to open it unless the
caller explicitly allows it.
"""

from __future__ import annotations

import json
from collections.abc import Iterable
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from .hashing import file_sha256

TRAINING_SPLITS = ("train", "val")
ALL_SPLITS = ("train", "val", "test")


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
