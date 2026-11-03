"""Black-box acceptance checks for the foundation's output contract."""

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
from click.testing import CliRunner

from predictive_cache.datagen.cli import main
from predictive_cache.datagen.config import load_config
from predictive_cache.datagen.io import config_hash, read_table, sha256
from predictive_cache.datagen.pipeline import run
from predictive_cache.datagen.schema import Manifest
from predictive_cache.datagen.validate import validate_dataset


def invoke_generate(out, fmt="parquet", seed=42):
    env = {**os.environ, "PYTHONHASHSEED": str(seed)}
    return subprocess.run(
        [
            sys.executable,
            "-c",
            "from predictive_cache.datagen.cli import main; main()",
            "generate",
            "--preset",
            "tiny",
            "--format",
            fmt,
            "--seed",
            str(seed),
            "--out",
            str(out),
        ],
        text=True,
        capture_output=True,
        env=env,
        check=True,
    )


@pytest.mark.parametrize("fmt", ["parquet", "jsonl"])
def test_subprocess_generation_determinism_and_cli(tmp_path, fmt):
    a, b, c = (tmp_path / name for name in ("a", "b", "c"))
    invoke_generate(a, fmt)
    invoke_generate(b, fmt)
    invoke_generate(c, fmt, seed=43)
    for folder in (a, b, c):
        result = validate_dataset(folder)
        assert result["events"] > 0
        assert (folder / "report/quality.json").exists()
        assert (folder / "truth/dropped_events").is_dir()
        assert CliRunner().invoke(main, ["validate", str(folder)]).exit_code == 0
        description = CliRunner().invoke(main, ["describe", str(folder)])
        assert description.exit_code == 0
        assert "foundation" in description.output.lower()
    ma = Manifest.model_validate_json((a / "manifest.json").read_text())
    mb = Manifest.model_validate_json((b / "manifest.json").read_text())
    mc = Manifest.model_validate_json((c / "manifest.json").read_text())
    assert {e.path: e.sha256 for e in ma.files} == {e.path: e.sha256 for e in mb.files}
    assert ma.dataset_id != mc.dataset_id
    assert next(e.sha256 for e in ma.files if e.path.startswith("events/")) != next(
        e.sha256 for e in mc.files if e.path.startswith("events/")
    )
    assert ma.data_kind == "synthetic"
    assert ma.limitations
    assert ma.config_sha256 == config_hash(load_config(path=a / "config.resolved.yaml"))
    for entry in ma.files:
        assert sha256(a / entry.path) == entry.sha256
        if entry.path.startswith("events/"):
            rows = read_table(a / entry.path, "events").to_pylist()
            assert [(r["ingest_time"], r["event_id"]) for r in rows] == sorted(
                (r["ingest_time"], r["event_id"]) for r in rows
            )
            assert all(r["cache_outcome"] is None and r["latency_ms"] is None for r in rows)
            assert all("cause" not in r and "intent_id" not in r for r in rows)
            assert all(r["event_time"].tzinfo is not None for r in rows)


def test_overwrite_protects_unrelated_files(tmp_path):
    out = tmp_path / "dataset"
    cfg = load_config(preset="tiny")
    run(cfg, out)
    with pytest.raises(ValueError, match="already exists"):
        run(cfg, out)
    run(cfg, out, overwrite=True)
    assert validate_dataset(out)["events"] > 0
    sentinel = out / "personal.txt"
    sentinel.write_text("must survive")
    with pytest.raises(ValueError):
        run(cfg, out, overwrite=True)
    assert sentinel.read_text() == "must survive"
    ordinary = tmp_path / "ordinary"
    ordinary.mkdir()
    (ordinary / "data.txt").write_text("keep")
    with pytest.raises((OSError, ValueError)):
        run(cfg, ordinary, overwrite=True)
    assert (ordinary / "data.txt").read_text() == "keep"


def test_output_symlink_rejected(tmp_path):
    target = tmp_path / "target"
    target.mkdir()
    link = tmp_path / "link"
    link.symlink_to(target, target_is_directory=True)
    with pytest.raises(ValueError, match="symlink"):
        run(load_config(preset="tiny"), link, overwrite=True)


@pytest.mark.parametrize(
    "args",
    [
        ["generate"],
        ["generate", "--out", "unused"],
        ["generate", "--preset", "tiny", "--seed", "-1", "--out", "unused"],
        ["generate", "--preset", "bad", "--out", "unused"],
    ],
)
def test_cli_errors(args):
    assert CliRunner().invoke(main, args).exit_code != 0


def test_validate_detects_schema_corruption_after_rehash(tmp_path):
    out = tmp_path / "data"
    run(load_config(preset="tiny", format="jsonl"), out)
    manifest = json.loads((out / "manifest.json").read_text())
    entry = next(e for e in manifest["files"] if e["path"].startswith("events/"))
    path = out / entry["path"]
    rows = [json.loads(line) for line in path.read_text().splitlines()]
    rows[0]["operation"] = "fabricated_operation"
    path.write_text("".join(json.dumps(r) + "\n" for r in rows))
    entry["sha256"] = sha256(path)
    (out / "manifest.json").write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="operation"):
        validate_dataset(out)
    assert CliRunner().invoke(main, ["validate", str(out)]).exit_code != 0


def test_manifest_missing_hash_count_corruption(tmp_path):
    out = tmp_path / "data"
    run(load_config(preset="tiny"), out)
    original = (out / "manifest.json").read_text()
    obj = json.loads(original)
    obj["files"][0]["sha256"] = "0" * 64
    (out / "manifest.json").write_text(json.dumps(obj))
    with pytest.raises(ValueError, match="hash mismatch"):
        validate_dataset(out)
    obj = json.loads(original)
    obj["counts"]["events"] += 1
    (out / "manifest.json").write_text(json.dumps(obj))
    with pytest.raises(ValueError, match="count mismatch"):
        validate_dataset(out)
    obj = json.loads(original)
    obj["files"] = [e for e in obj["files"] if e["path"] != "truth/patterns.jsonl"]
    (out / "manifest.json").write_text(json.dumps(obj))
    with pytest.raises(ValueError, match="missing required"):
        validate_dataset(out)


def test_manifest_cannot_escape_root(tmp_path):
    out = tmp_path / "data"
    run(load_config(preset="tiny"), out)
    obj = json.loads((out / "manifest.json").read_text())
    obj["files"][0]["path"] = "../outside"
    (out / "manifest.json").write_text(json.dumps(obj))
    with pytest.raises(ValueError, match="relative"):
        validate_dataset(out)


@pytest.mark.parametrize("table_name", ["volumes", "events", "dropped_events"])
def test_partition_ids_checked_before_any_write(tmp_path, table_name):
    from predictive_cache.datagen.io import write_dataset
    from predictive_cache.datagen.pipeline import component_rng
    from predictive_cache.datagen.stubs import build_world, catalog_rows

    cfg = load_config(preset="tiny", format="jsonl")
    tables = catalog_rows(build_world(cfg, component_rng(cfg.seed, "world")))
    tables.update(events=[], dropped_events=[], event_truth=[], loss_counters=[])
    if table_name == "volumes":
        tables["volumes"][0]["volume_id"] = "../../../../escaped"
    else:
        tables[table_name] = [{"volume_id": "../../../../escaped"}]
    out = tmp_path / "output"
    with pytest.raises(ValueError, match="unsafe volume ID"):
        write_dataset(cfg, out, tables, [], [])
    assert list(tmp_path.iterdir()) == []


def test_revision_uses_package_checkout_not_caller(tmp_path, monkeypatch):
    from predictive_cache.datagen import io

    source_root = Path(io.__file__).resolve().parents[3]
    expected = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=source_root, text=True
    ).strip()
    foreign = tmp_path / "foreign"
    foreign.mkdir()
    subprocess.run(["git", "init", "-q", str(foreign)], check=True)
    # No foreign commit is needed: the caller has a valid repo but no HEAD.
    monkeypatch.chdir(foreign)
    assert io.source_revision() == expected
    assert run(load_config(preset="tiny"), tmp_path / "data").generator.git_sha == expected


def test_revision_unknown_without_verified_source_checkout(tmp_path, monkeypatch):
    from predictive_cache.datagen import io

    installed = tmp_path / "lib/site-packages/predictive_cache/datagen/io.py"
    installed.parent.mkdir(parents=True)
    installed.write_text("# installed package")
    monkeypatch.setattr(io, "__file__", str(installed))
    assert io.source_revision() == "unknown"
