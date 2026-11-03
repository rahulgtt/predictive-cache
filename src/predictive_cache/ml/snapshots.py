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
