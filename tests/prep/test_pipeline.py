"""End-to-end, deterministic cleaning of a generated fixture."""

from __future__ import annotations

import json
from pathlib import Path

from click.testing import CliRunner

from predictive_cache.datagen.config import load_config
from predictive_cache.datagen.pipeline import run as run_datagen
from predictive_cache.prep.catalog import InitialCatalog
from predictive_cache.prep.clean.pipeline import run_clean
from predictive_cache.prep.cli import main
from predictive_cache.prep.config import CleanConfig


def test_generated_dataset_partitions_lineage_and_determinism(tmp_path: Path):
    source = tmp_path / "source"
    cfg = load_config(preset="tiny")
    cfg.period.weeks = 1
    cfg.files.target_count = 60
    cfg.actors.users = 3
    run_datagen(cfg, source)
    clean_cfg = CleanConfig()
    first = run_clean(source, clean_cfg, tmp_path / "first")
    second = run_clean(source, clean_cfg, tmp_path / "second")
    assert first["parent"] == second["parent"]
    assert first["parent"]["dataset_id"].startswith("synth-tiny-")
    assert first["counts"]["input_events"] == (
        first["counts"]["kept_events"] + sum(first["counts"]["excluded_by_reason"].values())
    )
    assert first["files"] == second["files"]
    assert not (tmp_path / "first" / "truth").exists()
    assert {entry["path"].split("/")[0] for entry in first["files"]} >= {
        "events_clean",
        "excluded",
        "opens",
        "open_ranges",
        "meta",
        "coverage",
        "catalog",
        "report",
    }
    report = json.loads((tmp_path / "first" / "report/quality.json").read_text())
    assert report["counts"] == first["counts"]
    assert sum(report["coverage_hours"].values()) == first["coverage"]["hours"]

    catalog = InitialCatalog.load(tmp_path / "first")
    assert catalog is not None
    for volume in json.loads((source / "manifest.json").read_text())["volumes"]:
        files = catalog.files(volume)
        assert files.num_rows > 0
        assert "deleted_at" not in files.column_names
        assert "replaced_by_file_id" not in files.column_names
        assert all(row["created_at"] < catalog.period_start for row in files.to_pylist())

    cli = CliRunner()
    result = cli.invoke(
        main,
        ["clean", "--input", str(source), "--out", str(tmp_path / "jsonl"), "--format", "jsonl"],
    )
    assert result.exit_code == 0, result.output
    assert any(p.suffix == ".jsonl" for p in (tmp_path / "jsonl" / "events_clean").rglob("*"))
    described = cli.invoke(main, ["describe", str(tmp_path / "jsonl")])
    assert described.exit_code == 0, described.output
    assert "Cleaned dataset quality" in described.output
    assert sum(row["count"] for row in report["exclusions_by_volume_day_reason"]) == (
        first["counts"]["input_events"] - first["counts"]["kept_events"]
    )


def test_schema_invalid_delivery_is_excluded(tmp_path: Path):
    source = tmp_path / "source-jsonl"
    cfg = load_config(preset="tiny", format="jsonl")
    cfg.period.weeks = 1
    cfg.files.target_count = 30
    cfg.actors.users = 2
    run_datagen(cfg, source)
    manifest_path = source / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    entry = next(item for item in manifest["files"] if item["path"].startswith("events/"))
    event_path = source / entry["path"]
    lines = event_path.read_text().splitlines()
    first = json.loads(lines[0])
    invalid = dict(first)
    invalid["event_time"] = "not-a-timestamp"
    invalid["ingest_time"] = "2026-01-05T12:00:00"
    invalid["volume_id"] = ["bad"]
    invalid["origin"] = ["bad"]
    invalid["app"] = {"bad": 1}
    lines[0] = json.dumps(invalid, sort_keys=True, separators=(",", ":"))
    lines.append(json.dumps(first, sort_keys=True, separators=(",", ":")))
    bad_id = dict(first)
    bad_id["event_id"] = ["unhashable"]
    bad_id["ingest_time"] = ["bad"]
    lines.append(json.dumps(bad_id, sort_keys=True, separators=(",", ":")))
    event_path.write_text("\n".join(lines) + "\n")
    entry["rows"] += 2
    manifest["counts"]["events"] += 2
    from predictive_cache.prep.clean.io import sha256

    entry["sha256"] = sha256(event_path)
    manifest_path.write_text(json.dumps(manifest))
    cleaned = run_clean(source, CleanConfig(), tmp_path / "cleaned", format="jsonl")
    assert cleaned["counts"]["excluded_by_reason"]["schema_invalid"] >= 2
    excluded = [
        row
        for file in cleaned["files"]
        if file["path"].startswith("excluded/")
        for row in (
            json.loads(line)
            for line in (tmp_path / "cleaned" / file["path"]).read_text().splitlines()
        )
    ]
    assert any(row["reason"] == "schema_invalid" and row["event_time"] is None for row in excluded)
    assert any(
        row["reason"] == "duplicate" and row["event_id"] == first["event_id"] for row in excluded
    )
    assert all(
        row["event_id"] != first["event_id"]
        for file in cleaned["files"]
        if file["path"].startswith("events_clean/")
        for row in (
            json.loads(line)
            for line in (tmp_path / "cleaned" / file["path"]).read_text().splitlines()
        )
    )
