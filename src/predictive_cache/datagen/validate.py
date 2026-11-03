"""Artifact, schema and restored-clean semantic validation."""

from __future__ import annotations

import json
import re
from collections import Counter, defaultdict
from pathlib import Path

from .config import load_config
from .io import PARTITIONED, TABLE_PATHS, config_hash, read_table, sha256, table_rows
from .schema import Manifest
from .semantic import validate_semantics


def validate_dataset(directory: Path) -> dict:
    root = Path(directory)
    if root.is_symlink() or not root.is_dir():
        raise ValueError("dataset must be a real directory")
    manifest_path = root / "manifest.json"
    if manifest_path.is_symlink():
        raise ValueError("manifest must not be a symlink")
    manifest = Manifest.model_validate_json(manifest_path.read_text())
    cfg = load_config(path=root / "config.resolved.yaml")
    if config_hash(cfg) != manifest.config_sha256:
        raise ValueError("resolved config hash mismatch")
    suffix = cfg.format
    fixed = {f"{path}.{suffix}": name for name, path in TABLE_PATHS.items()}
    required = set(fixed) | {
        "truth/patterns.jsonl",
        "config.resolved.yaml",
        "report/quality.json",
        "report/summary.md",
    }
    seen = set()
    row_counts = {}
    tables = defaultdict(list)
    patterns = []
    partition_counts = dict.fromkeys(PARTITIONED, 0)
    for entry in manifest.files:
        if entry.path in seen:
            raise ValueError(f"duplicate manifest path: {entry.path}")
        seen.add(entry.path)
        path = root / entry.path
        if any(p.is_symlink() for p in [path, *path.parents] if p != root.parent):
            raise ValueError("dataset artifacts must not use symlinks")
        if not path.is_file() or sha256(path) != entry.sha256:
            raise ValueError(f"missing file or hash mismatch: {entry.path}")
        name = fixed.get(entry.path)
        if name is None:
            for table_name, prefix in PARTITIONED.items():
                match = re.fullmatch(
                    re.escape(prefix)
                    + r"/volume_id=([A-Za-z0-9_-]+)/date=(\d{4}-\d{2}-\d{2})/part-\d{5}\."
                    + suffix,
                    entry.path,
                )
                if match:
                    name = table_name
                    partition_counts[name] += 1
                    break
        if name:
            table = read_table(path, name)
            if table.num_rows != entry.rows:
                raise ValueError(f"row-count mismatch: {entry.path}")
            row_counts[name] = row_counts.get(name, 0) + table.num_rows
            rows = table_rows(table)
            tables[name].extend(rows)
            if name in PARTITIONED:
                order = [(row["ingest_time"], row["event_id"]) for row in rows]
                if order != sorted(order):
                    raise ValueError("partition replay order mismatch")
                if any(
                    row["volume_id"] != match[1]
                    or row["ingest_time"].date().isoformat() != match[2]
                    for row in rows
                ):
                    raise ValueError("partition identity/date mismatch")
        elif entry.path == "truth/patterns.jsonl":
            table = read_table(path, "patterns")
            if table.num_rows != entry.rows:
                raise ValueError("pattern row-count mismatch")
            row_counts["patterns"] = table.num_rows
            patterns = table_rows(table)
        elif entry.path in {"config.resolved.yaml", "report/quality.json", "report/summary.md"}:
            if entry.rows is not None:
                raise ValueError("non-tabular artifact rows must be null")
            if entry.path.endswith(".json"):
                json.loads(path.read_text())
        else:
            raise ValueError(f"unrecognized dataset artifact: {entry.path}")
    if required - seen:
        raise ValueError(f"missing required artifacts: {sorted(required - seen)}")
    if not all(partition_counts.values()):
        raise ValueError("missing events or dropped-events partition files")
    actual = {p.relative_to(root).as_posix() for p in root.rglob("*") if p.is_file()}
    if actual != seen | {"manifest.json"}:
        raise ValueError("unlisted dataset artifacts")
    for name in ("events", "dropped_events", "files", "actors", "patterns"):
        if row_counts.get(name, 0) != getattr(manifest.counts, name):
            raise ValueError(f"manifest count mismatch: {name}")
    if dict(Counter(e["operation"] for e in tables["events"])) != manifest.counts.by_operation:
        raise ValueError("manifest operation counts mismatch")
    if (
        dict(Counter(e["origin"] or "unknown" for e in tables["events"]))
        != manifest.counts.by_origin
    ):
        raise ValueError("manifest origin counts mismatch")
    if (
        dict(Counter(f for t in tables["event_truth"] for f in t["faults"]))
        != manifest.counts.by_fault
    ):
        raise ValueError("manifest fault counts mismatch")
    result = validate_semantics(
        tables, cfg, patterns, [g.model_dump() for g in manifest.known_gaps]
    )
    return {
        **result,
        "scope": "schema_artifact_and_semantic",
        "files": len(seen),
        "events": manifest.counts.events,
        "data_kind": manifest.data_kind,
    }
