"""The observable READ projection is deterministic and does not alter its source."""

from __future__ import annotations

import json
from pathlib import Path

from predictive_cache.datagen.config import load_config
from predictive_cache.datagen.pipeline import run
from predictive_cache.datagen.read_projection import project_dataset
from predictive_cache.datagen.validate import validate_dataset
from predictive_cache.semantics.read_demand import SEMANTIC_VIEW_ID, SEMANTIC_VIEW_SHA256


def test_projection_is_read_only_deterministic_and_truth_separated(tmp_path: Path):
    source = tmp_path / "source"
    cfg = load_config(preset="tiny")
    cfg.period.weeks = 1
    cfg.files.target_count = 30
    cfg.actors.users = 2
    run(cfg, source)
    before = (source / "manifest.json").read_bytes()
    first = project_dataset(source, tmp_path / "first")
    second = project_dataset(source, tmp_path / "second")
    assert first["projection"] == SEMANTIC_VIEW_ID
    assert first["semantic_view_sha256"] == SEMANTIC_VIEW_SHA256
    assert first["counts"]["read_events"] == first["counts"]["truth_rows"]
    assert first["counts"]["coverage_rows"] > 0
    assert [(row["path"], row["rows"], row["sha256"]) for row in first["files"]] == [
        (row["path"], row["rows"], row["sha256"]) for row in second["files"]
    ]
    rows = [
        json.loads(line)
        for line in (tmp_path / "first/events/read-demand.jsonl").read_text().splitlines()
    ]
    assert rows
    assert {row["operation"] for row in rows} == {"read"}
    assert all("handle_id" not in row and "session_id" not in row for row in rows)
    truth = [
        json.loads(line)
        for line in (tmp_path / "first/truth/read-intents.jsonl").read_text().splitlines()
    ]
    assert any(row["handle_id"] is not None for row in truth)
    coverage = [
        json.loads(line)
        for line in (tmp_path / "first/coverage/coverage.jsonl").read_text().splitlines()
    ]
    assert coverage
    status_by_hour = {
        (row["volume_id"], row["hour_start"]): row["status"] for row in coverage
    }
    assert all(
        row["coverage_status"]
        == status_by_hour[
            (
                row["volume_id"],
                row["event_time"][:13] + ":00:00.000000Z",
            )
        ]
        for row in rows
    )
    assert (source / "manifest.json").read_bytes() == before
    validate_dataset(source)
