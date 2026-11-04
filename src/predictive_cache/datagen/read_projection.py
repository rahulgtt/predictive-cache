"""Opt-in projection of a generic synthetic dataset into read-demand-v1."""

from __future__ import annotations

import json
import shutil
import tempfile
from pathlib import Path

from predictive_cache.datagen.io import canonical_json, read_table, sha256, table_rows
from predictive_cache.datagen.validate import validate_dataset
from predictive_cache.prep.config import CleanConfig
from predictive_cache.prep.coverage import build_coverage
from predictive_cache.semantics.read_demand import (
    SEMANTIC_VIEW_ID,
    SEMANTIC_VIEW_SHA256,
    project_datagen_read,
)


def project_dataset(source_dir: Path, out_dir: Path) -> dict:
    source = Path(source_dir).resolve()
    out = Path(out_dir).absolute()
    if source == out or out.is_relative_to(source):
        raise ValueError("projection output must be outside the source dataset")
    validate_dataset(source)
    source_manifest_path = source / "manifest.json"
    source_manifest = json.loads(source_manifest_path.read_text())
    if out.exists():
        raise ValueError("projection output already exists")

    events = []
    counters = []
    truth = {}
    for entry in source_manifest["files"]:
        path = entry["path"]
        artifact = source / path
        if path.startswith("events/"):
            events.extend(table_rows(read_table(artifact, "events")))
        elif path.startswith("loss/"):
            counters.extend(table_rows(read_table(artifact, "loss_counters")))
        elif path == "truth/event_truth.parquet" or path == "truth/event_truth.jsonl":
            truth = {
                row["event_id"]: row for row in table_rows(read_table(artifact, "event_truth"))
            }

    coverage = build_coverage(source_manifest, counters, CleanConfig())
    coverage_by_hour = {
        (row["volume_id"], row["hour_start"]): row["status"] for row in coverage
    }
    rows = []
    truth_rows = []
    for event in sorted(events, key=lambda row: (row["ingest_time"], row["event_id"])):
        if event["operation"] != "read" or event["result"] != "ok":
            continue
        event_hour = event["event_time"].replace(minute=0, second=0, microsecond=0)
        coverage_status = coverage_by_hour.get(
            (event["volume_id"], event_hour), "unknown"
        )
        if coverage_status == "no_counters":
            coverage_status = "unknown"
        projected = project_datagen_read(event, coverage_status=coverage_status)
        rows.append(projected.model_dump(mode="json"))
        planted = truth.get(event["event_id"], {})
        truth_rows.append(
            {
                "source_event_id": event["event_id"],
                "intent_id": planted.get("intent_id"),
                "cause": planted.get("cause"),
                "session_id": event.get("session_id"),
                "cohort_id": event.get("cohort_id"),
                "client_id": event.get("client_id"),
                "app": event.get("app"),
                "handle_id": event.get("handle_id"),
            }
        )

    out.parent.mkdir(parents=True, exist_ok=True)
    stage = Path(tempfile.mkdtemp(prefix=f".{out.name}-staging-", dir=out.parent))
    try:
        events_path = stage / "events/read-demand.jsonl"
        coverage_path = stage / "coverage/coverage.jsonl"
        truth_path = stage / "truth/read-intents.jsonl"
        events_path.parent.mkdir(parents=True)
        coverage_path.parent.mkdir(parents=True)
        truth_path.parent.mkdir(parents=True)
        events_path.write_text("".join(canonical_json(row) + "\n" for row in rows))
        coverage_path.write_text(
            "".join(canonical_json(row) + "\n" for row in coverage)
        )
        truth_path.write_text("".join(canonical_json(row) + "\n" for row in truth_rows))
        manifest = {
            "manifest_version": 1,
            "projection": SEMANTIC_VIEW_ID,
            "semantic_view_sha256": SEMANTIC_VIEW_SHA256,
            "source": {
                "dataset_id": source_manifest["dataset_id"],
                "manifest_sha256": sha256(source_manifest_path),
            },
            "counts": {
                "read_events": len(rows),
                "coverage_rows": len(coverage),
                "truth_rows": len(truth_rows),
            },
            "files": [
                {
                    "path": "events/read-demand.jsonl",
                    "rows": len(rows),
                    "sha256": sha256(events_path),
                },
                {
                    "path": "coverage/coverage.jsonl",
                    "rows": len(coverage),
                    "sha256": sha256(coverage_path),
                },
                {
                    "path": "truth/read-intents.jsonl",
                    "rows": len(truth_rows),
                    "sha256": sha256(truth_path),
                },
            ],
            "limitations": [
                "synthetic fixture only",
                "generator lifecycle/session fields remain truth, not observed facts",
            ],
        }
        (stage / "manifest.json").write_text(canonical_json(manifest) + "\n")
        stage.rename(out)
        return manifest
    finally:
        if stage.exists():
            shutil.rmtree(stage)
