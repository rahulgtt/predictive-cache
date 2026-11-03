"""Deterministic dataset serialization; no accesses to modeled filesystem paths."""

from __future__ import annotations

import dataclasses
import hashlib
import importlib.metadata
import json
import platform
import shutil
import subprocess
import tempfile
from collections import Counter, defaultdict
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import yaml

from .schema import SCHEMAS, Manifest, rows_to_table

TABLE_PATHS = {
    "volumes": "catalog/volumes",
    "directories": "catalog/directories",
    "files": "catalog/files",
    "file_paths": "catalog/file_paths",
    "actors": "catalog/actors",
    "loss_counters": "loss/loss_counters",
    "event_truth": "truth/event_truth",
}
PARTITIONED = {"events": "events", "dropped_events": "truth/dropped_events"}
ROW_GROUP_SIZE = 65536


def json_value(value: Any) -> Any:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            raise ValueError("timestamp must be timezone-aware")
        return value.astimezone(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")
    if isinstance(value, date):
        return value.isoformat()
    if dataclasses.is_dataclass(value):
        return dataclasses.asdict(value)
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json")
    raise TypeError(f"Unsupported JSON value: {type(value).__name__}")


def canonical_json(value: Any) -> str:
    return json.dumps(
        value, default=json_value, sort_keys=True, separators=(",", ":"), allow_nan=False
    )


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def config_hash(cfg: Any) -> str:
    return hashlib.sha256(canonical_json(cfg.model_dump(mode="json")).encode()).hexdigest()


def write_table(path: Path, name: str, rows: list[dict]) -> int:
    table = rows_to_table(name, rows)
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.suffix == ".parquet":
        pq.write_table(
            table,
            path,
            compression="zstd",
            use_dictionary=False,
            row_group_size=ROW_GROUP_SIZE,
            version="2.6",
            coerce_timestamps="us",
            allow_truncated_timestamps=False,
        )
    else:
        with path.open("w", encoding="utf-8", newline="\n") as stream:
            for row in table.to_pylist():
                stream.write(canonical_json(row) + "\n")
    return table.num_rows


def _decode(value: Any, kind: pa.DataType) -> Any:
    if value is None:
        return None
    if pa.types.is_timestamp(kind):
        if not isinstance(value, str) or not value.endswith("Z"):
            raise ValueError("JSON timestamp must be UTC with Z suffix")
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.isoformat(timespec="microseconds").replace("+00:00", "Z") != value:
            raise ValueError("JSON timestamp must have six fractional digits")
        return parsed
    if pa.types.is_date(kind):
        return date.fromisoformat(value)
    if pa.types.is_list(kind):
        if not isinstance(value, list):
            raise ValueError("expected list")
        return [_decode(item, kind.value_type) for item in value]
    return value


def read_table(path: Path, name: str) -> pa.Table:
    if path.suffix == ".parquet":
        # ParquetFile avoids hive partition-column inference overriding volume_id.
        table = pq.ParquetFile(path).read()
        if not table.schema.equals(SCHEMAS[name], check_metadata=False):
            raise ValueError(f"{path}: Arrow schema mismatch for {name}")
        return rows_to_table(name, table.to_pylist())
    rows = []
    for line in path.read_text().splitlines():
        row = json.loads(line)
        if not isinstance(row, dict):
            raise ValueError(f"{path}: expected JSON object")
        for field in SCHEMAS[name]:
            if field.name in row:
                row[field.name] = _decode(row[field.name], field.type)
        rows.append(row)
    return rows_to_table(name, rows)


def _safe_destination(out: Path, overwrite: bool) -> None:
    if out.is_symlink():
        raise ValueError("output must not be a symlink")
    if not out.exists():
        return
    if not overwrite:
        raise ValueError("output already exists; choose another directory or use --overwrite")
    if not out.is_dir():
        raise ValueError("output is not a directory")
    # Only replace a complete, intact generator-owned dataset. Protect stray files.
    from .validate import validate_dataset

    validate_dataset(out)
    manifest = Manifest.model_validate_json((out / "manifest.json").read_text())
    allowed = {entry.path for entry in manifest.files} | {"manifest.json"}
    expected_dirs = {Path(p).parent for p in allowed}
    expected_dirs |= {Path("events"), Path("truth/dropped_events")}
    for parent in list(expected_dirs):
        expected_dirs.update(parent.parents)
    for path in out.rglob("*"):
        relative = path.relative_to(out)
        if path.is_symlink():
            raise ValueError("refusing to overwrite dataset containing symlinks")
        if path.is_file() and relative.as_posix() not in allowed:
            raise ValueError(f"refusing to overwrite unowned file: {relative}")
        if path.is_dir() and relative not in expected_dirs:
            raise ValueError(f"refusing to overwrite unowned directory: {relative}")


def _partition_volume(value: Any) -> str:
    if (
        not isinstance(value, str)
        or not value
        or any(
            c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_"
            for c in value
        )
    ):
        raise ValueError("unsafe volume ID for partition path")
    return value


def source_revision() -> str:
    """Use only this package's source checkout, never the caller's repository."""
    source = Path(__file__).resolve()
    root = source.parents[3]
    if not (root / ".git").exists() or not (root / "pyproject.toml").is_file():
        return "unknown"
    if (root / "src/predictive_cache/datagen/io.py").resolve() != source:
        return "unknown"
    try:
        discovered = subprocess.check_output(
            ["git", "rev-parse", "--show-toplevel"],
            cwd=root,
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
        if Path(discovered).resolve() != root:
            return "unknown"
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            cwd=root,
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except (OSError, subprocess.CalledProcessError):
        return "unknown"


def write_dataset(
    cfg: Any,
    out_dir: Path,
    tables: dict[str, list[dict]],
    patterns: list[Any],
    known_gaps: list[dict],
    *,
    overwrite: bool = False,
    report_updates: dict | None = None,
) -> Manifest:
    out = Path(out_dir).absolute()
    _safe_destination(out, overwrite)
    if set(tables) != set(TABLE_PATHS) | set(PARTITIONED):
        raise ValueError("pipeline must supply every schema table family")
    # Validate every path component before creating staging files, including
    # the empty-partition fallback that takes IDs from the volume catalog.
    for name in ("volumes", "events", "dropped_events"):
        for row in tables[name]:
            _partition_volume(row["volume_id"])
    out.parent.mkdir(parents=True, exist_ok=True)
    stage = Path(tempfile.mkdtemp(prefix=f".{out.name}-staging-", dir=out.parent))
    try:
        entries = []

        def register(relative: str, rows: int | None = None) -> None:
            entries.append({"path": relative, "rows": rows, "sha256": sha256(stage / relative)})

        suffix = cfg.format
        for name, relative in TABLE_PATHS.items():
            rows = sorted(tables[name], key=canonical_json)
            filename = f"{relative}.{suffix}"
            register(filename, write_table(stage / filename, name, rows))
        for name, root in PARTITIONED.items():
            (stage / root).mkdir(parents=True, exist_ok=True)
            grouped = defaultdict(list)
            for row in tables[name]:
                volume = _partition_volume(row["volume_id"])
                instant = row["ingest_time"]
                if not isinstance(instant, datetime) or instant.tzinfo is None:
                    raise ValueError("ingest_time must be aware datetime")
                grouped[(volume, instant.astimezone(UTC).date().isoformat())].append(row)
            # Empty removals still have a typed file for every volume.
            if not grouped:
                for volume in tables["volumes"]:
                    instant = volume["period_start"]
                    grouped[(volume["volume_id"], instant.astimezone(UTC).date().isoformat())] = []
            for (volume, day), rows in sorted(grouped.items()):
                rows.sort(
                    key=lambda row: (row["ingest_time"], row["event_id"], canonical_json(row))
                )
                filename = f"{root}/volume_id={volume}/date={day}/part-00000.{suffix}"
                register(filename, write_table(stage / filename, name, rows))
        pattern_rows = [
            dataclasses.asdict(p) if dataclasses.is_dataclass(p) else p for p in patterns
        ]
        # Pattern schema validation uses object params; Arrow representation is an internal detail.
        from .schema import validate_rows

        validate_rows("patterns", pattern_rows)
        path = stage / "truth/patterns.jsonl"
        path.write_text(
            "".join(canonical_json(row) + "\n" for row in sorted(pattern_rows, key=canonical_json))
        )
        register("truth/patterns.jsonl", len(pattern_rows))
        (stage / "config.resolved.yaml").write_text(
            yaml.safe_dump(cfg.model_dump(mode="json"), sort_keys=True)
        )
        register("config.resolved.yaml")
        report = {
            "implementation": "synthetic_workloads_events_and_faults",
            "validation_scope": "schema_and_artifact_integrity",
            "data_kind": "synthetic",
            "events": len(tables["events"]),
            "not_implemented": ["full recovery reports and preset scale/runtime targets (#19)"],
            **(report_updates or {}),
        }
        (stage / "report").mkdir()
        (stage / "report/quality.json").write_text(canonical_json(report) + "\n")
        register("report/quality.json")
        (stage / "report/summary.md").write_text(
            "# Synthetic access dataset\n\n"
            "Fictional workloads, complete read operations and configured telemetry faults.\n"
            "No modeled contents or measured latency/cache outcomes.\n"
            f"\nEvents: {len(tables['events'])}. Validation: schema/artifact integrity.\n"
            "Fault-stage counts are in quality.json. Full recovery reports and scale tuning await #19.\n"
        )
        register("report/summary.md")
        digest = config_hash(cfg)
        git_sha = source_revision()
        events = tables["events"]
        manifest_data = {
            "manifest_version": 1,
            "dataset_id": f"synth-{cfg.preset}-s{cfg.seed}-{digest[:8]}",
            "data_kind": "synthetic",
            "generator": {
                "name": "pcache-datagen",
                "version": "0.1.0",
                "git_sha": git_sha,
                "runtime": {
                    "python": platform.python_version(),
                    **{
                        name: importlib.metadata.version(name)
                        for name in ("numpy", "pyarrow", "pydantic", "pyyaml", "click")
                    },
                },
            },
            "event_schema_version": 1,
            "seed": cfg.seed,
            "preset": cfg.preset,
            "config_sha256": digest,
            "period": {
                "start": min(v["period_start"] for v in tables["volumes"]),
                "end": max(v["period_end"] for v in tables["volumes"]),
            },
            "volumes": [v["volume_id"] for v in tables["volumes"]],
            "counts": {
                "events": len(events),
                "dropped_events": len(tables["dropped_events"]),
                "files": len(tables["files"]),
                "actors": len(tables["actors"]),
                "patterns": len(pattern_rows),
                "by_operation": dict(Counter(e["operation"] for e in events)),
                "by_origin": dict(Counter(e["origin"] or "unknown" for e in events)),
                "by_fault": dict(Counter(f for e in tables["event_truth"] for f in e["faults"])),
            },
            "known_gaps": known_gaps,
            "files": sorted(entries, key=lambda item: item["path"]),
            "created_at": datetime.now(UTC),
            "limitations": [
                "synthetic only; not evidence of real-workload model quality",
                "buffered generation; preset scale/runtime targets and full recovery reports await #19",
                "P9 writes update catalog history but do not feed back into already generated P1 choices",
            ],
        }
        manifest = Manifest.model_validate(manifest_data)
        (stage / "manifest.json").write_text(
            canonical_json(manifest.model_dump(mode="python")) + "\n"
        )
        if out.exists():
            # Recheck after generation before replacing an existing output.
            _safe_destination(out, overwrite)
            backup = Path(tempfile.mkdtemp(prefix=f".{out.name}-backup-", dir=out.parent))
            backup.rmdir()
            out.rename(backup)
            try:
                stage.rename(out)
            except BaseException:
                backup.rename(out)
                raise
            shutil.rmtree(backup)
        else:
            stage.rename(out)
        return manifest
    finally:
        if stage.exists():
            shutil.rmtree(stage)
