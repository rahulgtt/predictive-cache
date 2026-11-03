"""Acceptance tests for #58: pcache-eval freeze, val/test runs, ledger and reports (spec §7.4-§7.5)."""

from __future__ import annotations

import json
import shutil
import zipfile
from pathlib import Path

import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

import predictive_cache.ml.evaluate as evaluate_module
from predictive_cache.ml.eval_cli import main as eval_cli
from predictive_cache.ml.evaluate import (
    LEDGER,
    EvalConfig,
    EvalError,
    compute_freeze_id,
    freeze,
    run,
)
from predictive_cache.ml.fixtures import make_fake_snapshots
from predictive_cache.ml.train import TrainConfig, train

BASELINES = ["baseline-mru", "baseline-frequency", "baseline-hotset", "baseline-coaccess"]


@pytest.fixture(autouse=True)
def offline(monkeypatch, tmp_path):
    monkeypatch.setenv("PCACHE_TRACKING", "off")
    monkeypatch.delenv("CLEARML_API_HOST", raising=False)
    monkeypatch.delenv("CLEARML_CONFIG_FILE", raising=False)
    monkeypatch.setenv("HOME", str(tmp_path / "home"))


@pytest.fixture(scope="module")
def built(tmp_path_factory):
    """Fake snapshots + baseline packages (no scikit-learn needed)."""
    root = tmp_path_factory.mktemp("eval")
    snaps = make_fake_snapshots(
        root / "s", days=5, ticks_per_day=8, candidates_per_tick=30, split_days=(3, 4)
    )
    summary = train(snaps, TrainConfig(models=list(BASELINES)), root / "t")
    refs = sorted(entry["package"] for entry in summary["models"].values())
    return snaps, refs


@pytest.fixture
def workspace(built, tmp_path):
    """Private copies, so tampering tests can't affect each other."""
    snaps, refs = built
    s = tmp_path / "s"
    shutil.copytree(snaps, s)
    p = tmp_path / "p"
    p.mkdir()
    local = []
    for ref in refs:
        dst = p / Path(ref).name
        shutil.copyfile(ref, dst)
        local.append(str(dst))
    return s, local, tmp_path / "freeze" / "freeze.json"


@pytest.fixture
def no_test_reads(monkeypatch):
    """Fail loudly if any code path reads test rows."""
    original = evaluate_module.load_split

    def guarded(root, split, allowed=("train", "val")):
        if split == "test":
            raise AssertionError("test rows were read")
        return original(root, split, allowed)

    monkeypatch.setattr(evaluate_module, "load_split", guarded)


def frozen(workspace):
    s, refs, path = workspace
    record = freeze(s, refs, EvalConfig(), path)
    return s, refs, path, record


# --- freeze ---------------------------------------------------------------------------------


def test_freeze_record_contents(workspace):
    s, refs, path, record = frozen(workspace)
    assert json.loads(path.read_text()) == record
    assert record["freeze_id"] == compute_freeze_id(record)
    assert sorted(p["ref"] for p in record["packages"]) == sorted(refs)
    assert all(p["package_json_sha256"] and p["contract_sha256"] for p in record["packages"])
    assert record["best_baseline"]["model_type"] in BASELINES
    assert record["primary"] == "recall@bytes_256MiB"
    files = record["data"]["files"]
    assert any(k.startswith("snapshots/split=test/") for k in files)
    assert any(k.startswith("universe/") for k in files)
    assert not any("split=train" in k or "split=val" in k for k in files)
    assert record["data"]["splits"] and record["data"]["labels"]


def test_freeze_never_reads_test_rows(workspace, no_test_reads):
    frozen(workspace)


def test_freeze_needs_a_baseline_and_unique_packages(workspace, tmp_path):
    s, refs, path = workspace
    from predictive_cache.ml.dummy import make_dummy_package

    with pytest.raises(EvalError) as info:
        freeze(s, [refs[0], refs[0]], EvalConfig(), path)
    assert info.value.reason == "bad_package_set"
    # a dummy package has the wrong contract for these snapshots -> rejected by the loader
    with pytest.raises(Exception):
        freeze(s, [str(make_dummy_package(tmp_path / "d"))], EvalConfig(), path)


# --- run: test protections ----------------------------------------------------------------


def test_test_without_freeze_refused(workspace, tmp_path, no_test_reads):
    s, refs, _ = workspace
    with pytest.raises(EvalError) as info:
        run(s, "test", refs, None, EvalConfig(), tmp_path / "o")
    assert info.value.reason == "freeze_required"


def expect_refused(s, path, reason, tmp_path, refs=None, cfg=None):
    with pytest.raises(EvalError) as info:
        run(s, "test", refs, path, cfg or EvalConfig(), tmp_path / "o")
    assert info.value.reason == reason, info.value


def test_changed_package_refused(workspace, tmp_path, no_test_reads):
    s, refs, path, _ = frozen(workspace)
    victim = Path(refs[0])
    with zipfile.ZipFile(victim) as archive:
        members = {n: archive.read(n) for n in archive.namelist()}
    manifest = json.loads(members["package.json"])
    manifest["serving_policy"]["max_items"] = 1
    members["package.json"] = json.dumps(manifest).encode()
    with zipfile.ZipFile(victim, "w") as archive:
        for name, data in members.items():
            archive.writestr(name, data)
    expect_refused(s, path, "package_changed", tmp_path)


def test_different_package_set_refused(workspace, tmp_path, no_test_reads):
    s, refs, path, _ = frozen(workspace)
    expect_refused(s, path, "package_set_mismatch", tmp_path, refs=refs[:-1])


def test_changed_test_file_refused(workspace, tmp_path, no_test_reads):
    s, _, path, _ = frozen(workspace)
    part = next((s / "snapshots" / "split=test").rglob("*.parquet"))
    table = pq.ParquetFile(part).read()
    pq.write_table(table.slice(1), part)
    expect_refused(s, path, "data_changed", tmp_path)


def test_extra_test_file_refused(workspace, tmp_path, no_test_reads):
    s, _, path, _ = frozen(workspace)
    part = next((s / "snapshots" / "split=test").rglob("*.parquet"))
    shutil.copyfile(part, part.with_name("part-00001.parquet"))
    expect_refused(s, path, "data_changed", tmp_path)


def test_changed_universe_or_manifest_refused(workspace, tmp_path, no_test_reads):
    s, _, path, _ = frozen(workspace)
    manifest = json.loads((s / "manifest.json").read_text())
    manifest["splits"]["test_start"] = "2030-01-01T00:00:00Z"
    (s / "manifest.json").write_text(json.dumps(manifest))
    expect_refused(s, path, "data_changed", tmp_path)


def test_tampered_freeze_and_config_refused(workspace, tmp_path, no_test_reads):
    s, _, path, record = frozen(workspace)
    expect_refused(s, path, "config_mismatch", tmp_path, cfg=EvalConfig(bootstrap_reps=10))
    record["primary"] = "recall@top_10"
    path.write_text(json.dumps(record))
    expect_refused(s, path, "freeze_tampered", tmp_path)


def test_once_per_freeze_and_rerun_reason(workspace, tmp_path):
    s, _, path, record = frozen(workspace)
    first = run(s, "test", None, path, EvalConfig(), tmp_path / "t1")
    assert first["test_run_number"] == 1
    with pytest.raises(EvalError) as info:
        run(s, "test", None, path, EvalConfig(), tmp_path / "t2")
    assert info.value.reason == "already_evaluated"
    again = run(s, "test", None, path, EvalConfig(), tmp_path / "t3", rerun_reason="bug fix")
    assert again["test_run_number"] == 2 and again["rerun_reason"] == "bug fix"
    lines = [json.loads(x) for x in (path.parent / LEDGER).read_text().splitlines()]
    assert [x["run"] for x in lines] == [1, 2] and lines[1]["rerun_reason"] == "bug fix"
    assert all(x["freeze_id"] == record["freeze_id"] for x in lines)
    assert "rerun reason: bug fix" in (tmp_path / "t3" / "report.md").read_text()


# --- run: reports ---------------------------------------------------------------------------


def test_val_run_report_and_metrics(workspace, tmp_path, no_test_reads):
    s, refs, _ = workspace
    summary = run(s, "val", refs, None, EvalConfig(), tmp_path / "v")
    report = (tmp_path / "v" / "report.md").read_text()
    assert "plumbing / planted-pattern evidence only" in report
    assert "Recall by budget" in report
    metrics = json.loads((tmp_path / "v" / "metrics.json").read_text())
    assert metrics["split"] == "val" and metrics["data_kind"] == "fixture"
    best = summary["best_baseline"]
    for pid, entry in summary["models"].items():
        assert set(entry["curve"]) == set(EvalConfig().budgets)
        assert entry["per_volume"]
        if pid == best:
            assert entry["vs_best_baseline"] is None
        else:
            vs = entry["vs_best_baseline"]
            assert vs["ci95"][0] <= vs["ci95"][1]


def test_test_run_uses_frozen_best_baseline(workspace, tmp_path):
    s, _, path, record = frozen(workspace)
    summary = run(s, "test", None, path, EvalConfig(), tmp_path / "t")
    assert summary["best_baseline"] == record["best_baseline"]["package_id"]
    assert "lab-grade" in (tmp_path / "t" / "report.md").read_text()


def test_out_dir_must_be_fresh(workspace, tmp_path):
    s, refs, _ = workspace
    (tmp_path / "o").mkdir()
    (tmp_path / "o" / "x").write_text("x")
    with pytest.raises(FileExistsError):
        run(s, "val", refs, None, EvalConfig(), tmp_path / "o")


# --- config and CLI -------------------------------------------------------------------------


def test_config_from_spec_yaml_shape():
    cfg = EvalConfig.from_dict(
        {
            "eval": {
                "budgets": {"top_k": [5], "bytes_mib": [128]},
                "range_cap_mib": 8,
                "primary": "recall@bytes_128MiB",
                "bootstrap": {"reps": 50, "seed": 3},
            }
        }
    )
    assert cfg.budgets == ["top_5", "bytes_128MiB"] and cfg.bootstrap_reps == 50
    for bad in ({"eval": {"primary": "recall@bytes_999MiB"}}, {"eval": {"nope": 1}}):
        with pytest.raises(ValueError):
            EvalConfig.from_dict(bad)


def test_cli_freeze_run_and_refusals(workspace, tmp_path):
    s, refs, path = workspace
    runner = CliRunner()
    pk = [arg for ref in refs for arg in ("--packages", ref)]
    made = runner.invoke(eval_cli, ["freeze", "--snapshots", str(s), *pk, "--out", str(path)])
    assert made.exit_code == 0, made.output
    again = runner.invoke(eval_cli, ["freeze", "--snapshots", str(s), *pk, "--out", str(path)])
    assert again.exit_code != 0 and "never overwritten" in again.output
    nofreeze = runner.invoke(
        eval_cli,
        ["run", "--snapshots", str(s), "--split", "test", *pk, "--out", str(tmp_path / "x")],
    )
    assert nofreeze.exit_code != 0 and "freeze_required" in nofreeze.output
    ok = runner.invoke(
        eval_cli,
        [
            "run",
            "--snapshots",
            str(s),
            "--split",
            "test",
            "--freeze",
            str(path),
            "--out",
            str(tmp_path / "t"),
        ],
    )
    assert ok.exit_code == 0, ok.output
    assert "Evaluated 4 packages on test (fixture)" in ok.output
    twice = runner.invoke(
        eval_cli,
        [
            "run",
            "--snapshots",
            str(s),
            "--split",
            "test",
            "--freeze",
            str(path),
            "--out",
            str(tmp_path / "t2"),
        ],
    )
    assert twice.exit_code != 0 and "already_evaluated" in twice.output


# --- Review fixes ---------------------------------------------------------------------------


def test_untracked_test_run_honours_server_record(workspace, tmp_path, monkeypatch):
    import predictive_cache.tracking as tracking

    s, _, path, _ = frozen(workspace)
    monkeypatch.setattr(tracking, "query_prior_runs", lambda project, tags, exclude_task=None: 1)
    with pytest.raises(EvalError) as info:
        run(s, "test", None, path, EvalConfig(), tmp_path / "t")  # NullTracker, empty ledger
    assert info.value.reason == "already_evaluated"


def test_invalid_package_is_a_clean_cli_error(workspace, tmp_path):
    s, refs, path = workspace
    victim = Path(refs[0])
    with zipfile.ZipFile(victim) as archive:
        members = {n: archive.read(n) for n in archive.namelist()}
    members["model.json"] = b'{"model_type": "tampered"}'
    with zipfile.ZipFile(victim, "w") as archive:
        for name, data in members.items():
            archive.writestr(name, data)
    pk = [arg for ref in refs for arg in ("--packages", ref)]
    result = CliRunner().invoke(
        eval_cli, ["freeze", "--snapshots", str(s), *pk, "--out", str(path)]
    )
    assert result.exit_code == 1 and "package_invalid" in result.output
    assert isinstance(result.exception, SystemExit)  # handled ClickException, not a traceback


def test_val_subset_always_includes_frozen_best_baseline(workspace, tmp_path):
    s, refs, path, record = frozen(workspace)
    best = record["best_baseline"]["package_id"]
    other = next(p["ref"] for p in record["packages"] if p["package_id"] != best)
    summary = run(s, "val", [other], path, EvalConfig(), tmp_path / "v")
    assert set(summary["models"]) == {
        best,
        next(p["package_id"] for p in record["packages"] if p["ref"] == other),
    }
    assert summary["models"][best]["vs_best_baseline"] is None
