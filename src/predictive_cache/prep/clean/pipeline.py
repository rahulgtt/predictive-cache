"""Build a versioned demand dataset from datagen-compatible events."""

from __future__ import annotations

import json
import shutil
import subprocess
import tempfile
from collections import Counter, defaultdict
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pyarrow as pa
import yaml

from predictive_cache.datagen.io import canonical_json, config_hash, read_table
from predictive_cache.datagen.schema import Manifest as SourceManifest
from predictive_cache.datagen.schema import normalize_rows
from predictive_cache.prep.clean.handles import HandleTracker
from predictive_cache.prep.clean.io import read_rows, sha256, write_rows
from predictive_cache.prep.clean.report import build_report, render_summary
from predictive_cache.prep.clean.rules import classify_event
from predictive_cache.prep.clean.schema import EXCLUDED
from predictive_cache.prep.config import CleanConfig
from predictive_cache.prep.coverage import build_coverage, utc


def _safe_volume(value) -> str:
    if (
        not isinstance(value, str)
        or not value
        or any(
            c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_"
            for c in value
        )
    ):
        return "invalid"
    return value


def _event_sort(record):
    row, path, index = record
    try:
        known = utc(row.get("ingest_time"))
        if not isinstance(known, datetime) or known.tzinfo is None:
            raise ValueError
    except (TypeError, ValueError):
        known = datetime.min.replace(tzinfo=UTC)
    return known, path, index


def _sort_instant(value):
    try:
        instant = utc(value)
        return (
            instant
            if isinstance(instant, datetime) and instant.tzinfo is not None
            else datetime.min.replace(tzinfo=UTC)
        )
    except (TypeError, ValueError):
        return datetime.min.replace(tzinfo=UTC)


def _partitioned(stage, name, rows, suffix, date_field, entries, volumes, fallback_day):
    grouped = defaultdict(list)
    for row in rows:
        instant = row.get(date_field)
        try:
            day = utc(instant).astimezone(UTC).date().isoformat()
        except (TypeError, ValueError, AttributeError):
            day = "invalid"
        grouped[(_safe_volume(row.get("volume_id")), day)].append(row)
    (stage / name).mkdir(parents=True, exist_ok=True)
    if not grouped:
        for volume in volumes:
            grouped[(_safe_volume(volume), fallback_day)] = []
    for (volume, day), group in sorted(grouped.items()):
        if name in {"events_clean", "excluded"}:
            group.sort(
                key=lambda r: (
                    _sort_instant(r.get("ingest_time")),
                    r.get("event_id") or "",
                )
            )
        elif name == "opens":
            group.sort(key=lambda r: (r["open_time"], r["open_id"]))
        else:
            group.sort(key=lambda r: (r["open_id"], r["offset"], r["known_at"]))
        relative = f"{name}/volume_id={volume}/date={day}/part-00000.{suffix}"
        count = write_rows(stage / relative, name, group)
        entries.append({"path": relative, "rows": count, "sha256": sha256(stage / relative)})


def _source_rows(source: Path, manifest: dict):
    input_rows = []
    counters = []
    catalogs = []
    for entry in manifest["files"]:
        path = entry["path"]
        if not (
            path.startswith("events/") or path.startswith("loss/") or path.startswith("catalog/")
        ):
            continue
        artifact = source / path
        if not artifact.is_file() or sha256(artifact) != entry["sha256"]:
            raise ValueError(f"missing or corrupted source artifact: {path}")
        if path.startswith("events/"):
            input_rows.extend(
                (row if isinstance(row, dict) else {}, path, index)
                for index, row in enumerate(read_rows(artifact))
            )
        elif path.startswith("loss/"):
            counters.extend(read_table(artifact, "loss_counters").to_pylist())
        else:
            catalogs.append(path)
    return input_rows, counters, catalogs


def _excluded_invalid(raw: dict, path: str, index: int) -> dict:
    """Retain representable source fields and identify malformed ones."""
    row = {}
    invalid = []
    for field in EXCLUDED:
        if field.name in {"reason", "detail"}:
            continue
        value = raw.get(field.name)
        try:
            if value is not None and pa.types.is_timestamp(field.type):
                value = utc(value)
            pa.scalar(value, type=field.type)
        except (ValueError, TypeError, pa.ArrowException):
            invalid.append(field.name)
            value = None
        row[field.name] = value
    row["reason"] = "schema_invalid"
    row["detail"] = f"{path}:{index}; invalid={','.join(invalid)}"
    return row


def _catalog_misses(kept, catalog_files, period_start):
    initial = {r["file_id"] for r in catalog_files if utc(r["created_at"]) < period_start}
    known = set(initial)
    misses = set()
    miss_events = 0
    for row in sorted(kept, key=lambda r: (r["known_at"], r["event_id"])):
        if row["operation"] == "create":
            known.add(row["file_id"])
        elif row["file_id"] not in known:
            misses.add(row["file_id"])
            miss_events += 1
    return {"files": len(misses), "events": miss_events}


def _owned_output(out: Path):
    if out.is_symlink():
        raise ValueError("output must not be a symlink")
    if not out.exists():
        return
    if not out.is_dir():
        raise ValueError("output must be a directory")
    manifest_path = out / "manifest.json"
    if not manifest_path.is_file():
        raise ValueError("refusing to overwrite directory without clean manifest")
    manifest = json.loads(manifest_path.read_text())
    if manifest.get("stage") != "clean" or manifest.get("tool", {}).get("name") != "pcache-prep":
        raise ValueError("refusing to overwrite non-clean dataset")
    allowed = {r["path"] for r in manifest.get("files", [])} | {"manifest.json"}
    for path in out.rglob("*"):
        if path.is_symlink():
            raise ValueError("refusing to overwrite dataset with symlinks")
        if path.is_file() and path.relative_to(out).as_posix() not in allowed:
            raise ValueError(f"refusing to overwrite unowned file: {path}")


def run_clean(
    input_dir: Path,
    cfg: CleanConfig,
    out_dir: Path,
    *,
    format: str = "parquet",
    overwrite: bool = False,
) -> dict:
    """Clean one source dataset, leaving a complete output or no output."""
    if format not in {"parquet", "jsonl"}:
        raise ValueError("format must be parquet or jsonl")
    source, out = Path(input_dir).resolve(), Path(out_dir).absolute()
    if source == out or out.is_relative_to(source):
        raise ValueError("output must be outside the input dataset")
    parent_path = source / "manifest.json"
    parent = SourceManifest.model_validate_json(parent_path.read_text()).model_dump(mode="python")
    if parent["event_schema_version"] != 1:
        raise ValueError("unsupported input event schema")
    records, counters, catalog_paths = _source_rows(source, parent)
    records.sort(key=_event_sort)
    seen = set()
    kept, excluded = [], []
    for raw, path, index in records:
        # Reserve the first delivery even when another field is malformed.
        # Schema validation still wins for that delivery (§4.3 rule order).
        raw_id = raw.get("event_id")
        duplicate = isinstance(raw_id, str) and raw_id in seen
        if isinstance(raw_id, str) and not duplicate:
            seen.add(raw_id)
        try:
            event = normalize_rows("events", [raw])[0]
        except (ValueError, TypeError, KeyError, OverflowError):
            excluded.append(_excluded_invalid(raw, path, index))
            continue
        if duplicate:
            excluded.append({**event, "reason": "duplicate", "detail": None})
            continue
        known = event["ingest_time"]
        reason = classify_event(event, known, cfg)
        if reason:
            excluded.append({**event, "reason": reason, "detail": None})
            continue
        kept.append({**event, "known_at": known, "effective_time": min(event["event_time"], known)})
    kept.sort(key=lambda r: (r["known_at"], r["event_id"]))
    tracker = HandleTracker(cfg)
    opens, ranges, meta = [], [], []
    for event in kept:
        opens.extend(
            update["row"]
            for update in tracker.advance(event["known_at"] - timedelta(microseconds=1))
        )
        tracker.push(event)
        operation = event["operation"]
        if operation == "read":
            ranges.append(
                {
                    "open_id": tracker.key(event),
                    "volume_id": event["volume_id"],
                    "file_id": event["file_id"],
                    "offset": event["offset"],
                    "length": event["bytes_read"],
                    "effective_time": event["effective_time"],
                    "known_at": event["known_at"],
                    "file_size": event["file_size"],
                }
            )
        elif operation in {"create", "write", "rename", "delete"}:
            meta.append(
                {
                    key: event[key]
                    for key in (
                        "event_id",
                        "volume_id",
                        "file_id",
                        "dir_id",
                        "operation",
                        "effective_time",
                        "known_at",
                        "file_size",
                    )
                }
            )
    opens.extend(update["row"] for update in tracker.flush())
    coverage = build_coverage(parent, counters, cfg)
    catalog_files = []
    for path in catalog_paths:
        if path.startswith("catalog/files."):
            catalog_files.extend(read_table(source / path, "files").to_pylist())
    catalog_misses = _catalog_misses(kept, catalog_files, utc(parent["period"]["start"]))
    reasons = Counter(row["reason"] for row in excluded)
    statuses = Counter(row["status"] for row in coverage)
    counts = {
        "input_events": len(records),
        "kept_events": len(kept),
        "excluded_by_reason": dict(sorted(reasons.items())),
        "opens": len(opens),
        "opens_open_missing": sum(r["open_missing"] for r in opens),
        "opens_close_missing": sum(r["close_missing"] for r in opens),
        "opens_bytes_inconsistent": sum(r["bytes_inconsistent"] for r in opens),
        "late_reads_after_completion": tracker.late_reads_after_completion,
        "unexpected_reads_in_summary_mode": tracker.unexpected_reads_in_summary_mode,
        "open_ranges": len(ranges),
        "meta_events": len(meta),
        "catalog_miss_files": catalog_misses["files"],
        "catalog_miss_events": catalog_misses["events"],
    }
    report = build_report(
        [r for r, _, _ in records],
        kept,
        excluded,
        opens,
        coverage,
        counts,
        cfg.coverage.skew_margin_s,
        catalog_misses,
    )
    if out.exists() and not overwrite:
        raise ValueError("output already exists; use --overwrite")
    if out.exists():
        _owned_output(out)
    out.parent.mkdir(parents=True, exist_ok=True)
    stage = Path(tempfile.mkdtemp(prefix=f".{out.name}-staging-", dir=out.parent))
    try:
        entries = []
        for name, rows, field in (
            ("events_clean", kept, "known_at"),
            ("excluded", excluded, "ingest_time"),
            ("opens", opens, "open_time"),
            ("open_ranges", ranges, "effective_time"),
        ):
            _partitioned(
                stage,
                name,
                rows,
                format,
                field,
                entries,
                parent["volumes"],
                utc(parent["period"]["start"]).date().isoformat(),
            )
        meta.sort(key=lambda r: (r["known_at"], r["event_id"]))
        for volume in sorted(parent["volumes"]):
            relative = f"meta/volume_id={_safe_volume(volume)}/part-00000.{format}"
            group = [r for r in meta if r["volume_id"] == volume]
            entries.append(
                {
                    "path": relative,
                    "rows": write_rows(stage / relative, "meta", group),
                    "sha256": sha256(stage / relative),
                }
            )
        relative = f"coverage/coverage.{format}"
        entries.append(
            {
                "path": relative,
                "rows": write_rows(stage / relative, "coverage", coverage),
                "sha256": sha256(stage / relative),
            }
        )
        for path in catalog_paths:
            destination = stage / path
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source / path, destination)
            source_entry = next(item for item in parent["files"] if item["path"] == path)
            entries.append(
                {"path": path, "rows": source_entry["rows"], "sha256": sha256(destination)}
            )
        (stage / "config.resolved.yaml").write_text(
            yaml.safe_dump({"clean": cfg.model_dump(mode="json")}, sort_keys=True)
        )
        (stage / "report").mkdir()
        (stage / "report/quality.json").write_text(canonical_json(report) + "\n")
        (stage / "report/summary.md").write_text(render_summary(report))
        for relative in ("config.resolved.yaml", "report/quality.json", "report/summary.md"):
            entries.append({"path": relative, "rows": None, "sha256": sha256(stage / relative)})
        cfg_hash = config_hash(cfg)
        try:
            revision = subprocess.check_output(
                ["git", "rev-parse", "HEAD"],
                cwd=Path(__file__).resolve().parents[4],
                text=True,
                stderr=subprocess.DEVNULL,
            ).strip()
        except (OSError, subprocess.CalledProcessError):
            revision = "unknown"
        manifest = {
            "manifest_version": 1,
            "dataset_id": f"clean-{parent['dataset_id']}-{cfg_hash[:8]}",
            "data_kind": parent["data_kind"],
            "stage": "clean",
            "parent": {"dataset_id": parent["dataset_id"], "manifest_sha256": sha256(parent_path)},
            "tool": {"name": "pcache-prep", "version": "0.1.0", "git_sha": revision},
            "config_sha256": cfg_hash,
            "period": parent["period"],
            "counts": counts,
            "coverage": {
                "hours": len(coverage),
                "complete": statuses["complete"],
                "incomplete": statuses["incomplete"],
                "no_counters": statuses["no_counters"],
                "skew_margin_s": cfg.coverage.skew_margin_s,
                "assume_complete_without_counters": cfg.coverage.assume_complete_without_counters,
            },
            "files": sorted(entries, key=lambda r: r["path"]),
            "created_at": datetime.now(UTC),
        }
        (stage / "manifest.json").write_text(canonical_json(manifest) + "\n")
        if out.exists():
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
