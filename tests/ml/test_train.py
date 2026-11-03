"""Acceptance tests for #56: logistic regression v1, pcache-train and tracking (spec §6, §9)."""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

sklearn = pytest.importorskip("sklearn")

from predictive_cache.ml import load_package  # noqa: E402
from predictive_cache.ml.dummy import default_feature_schema  # noqa: E402
from predictive_cache.ml.fixtures import make_fake_snapshots  # noqa: E402
from predictive_cache.ml.logreg import LogRegScorer, training_features  # noqa: E402
from predictive_cache.ml.metrics import Budget, evaluate, select  # noqa: E402
from predictive_cache.ml.snapshots import (  # noqa: E402
    SnapshotError,
    load_split,
    load_universe,
)
from predictive_cache.ml.train import ALL_MODELS, TrainConfig, train  # noqa: E402
from predictive_cache.ml.train_cli import main as train_cli  # noqa: E402
from predictive_cache.tracking import NullTracker, tracking_enabled  # noqa: E402


@pytest.fixture(autouse=True)
def no_clearml(monkeypatch, tmp_path):
    """Tests never talk to a ClearML server."""
    monkeypatch.setenv("PCACHE_TRACKING", "off")
    monkeypatch.delenv("CLEARML_API_HOST", raising=False)
    monkeypatch.delenv("CLEARML_CONFIG_FILE", raising=False)
    monkeypatch.setenv("HOME", str(tmp_path / "home"))


@pytest.fixture(scope="module")
def snaps(tmp_path_factory) -> Path:
    return make_fake_snapshots(
        tmp_path_factory.mktemp("snaps") / "s",
        days=5,
        ticks_per_day=8,
        candidates_per_tick=30,
        split_days=(3, 4),
    )


@pytest.fixture(scope="module")
def trained(snaps, tmp_path_factory):
    out = tmp_path_factory.mktemp("train") / "out"
    cfg = TrainConfig(C_grid=[0.1, 1.0])
    return out, train(snaps, cfg, out)


# --- Snapshot loading ------------------------------------------------------------------


def test_load_split_refuses_test_and_unknown(snaps):
    with pytest.raises(SnapshotError, match="not allowed"):
        load_split(snaps, "test")
    with pytest.raises(SnapshotError):
        load_split(snaps, "holdout")
    assert load_split(snaps, "test", allowed=("test",)).num_rows > 0


def test_load_split_has_split_and_volume_columns(snaps):
    val = load_split(snaps, "val")
    assert set(val.column("split").to_pylist()) == {"val"}
    assert set(val.column("volume_id").to_pylist()) == {"vol-0001", "vol-0002"}
    assert load_universe(snaps).num_rows == 2 * 5 * 8


# --- Logistic regression ---------------------------------------------------------------


def test_numpy_scores_match_sklearn(snaps):
    train_rows, val_rows = load_split(snaps, "train"), load_split(snaps, "val")
    scorer = LogRegScorer(training_features(default_feature_schema()))
    scorer.fit(train_rows, None, {"C": 1.0})
    ours = scorer.score(val_rows)
    theirs = scorer._sklearn.predict_proba(scorer.design(val_rows))[:, 1]
    assert np.max(np.abs(ours - theirs)) <= 1e-9


def test_preprocessing_statistics_come_from_train_only(snaps):
    features = training_features(default_feature_schema())
    train_rows, val_rows = load_split(snaps, "train"), load_split(snaps, "val")
    a, b = LogRegScorer(features), LogRegScorer(features)
    a.fit(train_rows, val_rows, {"C": 1.0})
    b.fit(train_rows, None, {"C": 1.0})
    assert a.preprocess == b.preprocess and np.array_equal(a.coef, b.coef)


def test_null_indicators_and_transform_kinds(snaps):
    scorer = LogRegScorer(("secs_since_last_open", "opens_1h"))
    scorer.fit(load_split(snaps, "train"), None, {"C": 1.0})
    by_name = {p["name"]: p for p in scorer.preprocess}
    assert by_name["secs_since_last_open"]["kind"] == "seconds"
    assert by_name["secs_since_last_open"]["fill"] == 604_800.0
    assert by_name["opens_1h"]["null_indicator"]  # fixture has nulls in every feature
    assert scorer.transformed_columns[0] == "isnull__secs_since_last_open"
    assert np.all(np.abs(scorer.design(load_split(snaps, "val"))) <= 10)


def test_unknown_feature_kind_fails():
    schema = default_feature_schema()
    schema["features"].append({"name": "hour_of_day_local", "dtype": "int64", "available": True})
    with pytest.raises(ValueError, match="hour_of_day_local"):
        training_features(schema)
    schema["features"][-1]["available"] = False
    assert "hour_of_day_local" not in training_features(schema)


def test_payload_round_trip_and_explain(snaps):
    scorer = LogRegScorer(("opens_1h", "coacc_max_conf"))
    scorer.fit(load_split(snaps, "train"), None, {"C": 1.0})
    again = LogRegScorer.from_payload(json.loads(json.dumps(scorer.to_payload())), {})
    val = load_split(snaps, "val")
    assert np.array_equal(again.score(val), scorer.score(val))
    reasons = scorer.explain(val.slice(0, 3))
    assert len(reasons) == 3 and all(len(r) == 2 and ":" in r[0] for r in reasons)


def test_unfitted_logreg_errors():
    with pytest.raises(ValueError):
        LogRegScorer(("opens_1h",)).to_payload()


# --- pcache-train core ------------------------------------------------------------------


def test_train_writes_packages_reports_and_manifest(trained, snaps):
    out, summary = trained
    assert set(summary["models"]) == set(ALL_MODELS)
    manifest = json.loads((out / "train_manifest.json").read_text())
    assert manifest["stage"] == "train" and manifest["data_kind"] == "fixture"
    assert manifest["parent"]["dataset_id"] == "fixture-snapshots-s7"
    assert "plumbing" in (out / "val_report.md").read_text()
    for model, entry in summary["models"].items():
        with load_package(Path(entry["package"]), "fv1") as loaded:
            assert loaded.model_type == model
            policy = loaded.serving_policy
            assert policy["lead_s"] == 0 and policy["horizon_s"] == 300
            assert policy["max_bytes"] == 256 * 2**20
            assert loaded.manifest["data"]["snapshots_dataset_id"] == "fixture-snapshots-s7"
            assert loaded.manifest["training"]["selected_by"] == "val:recall@bytes_256MiB"


def test_contract_copied_from_snapshots(trained, snaps):
    _, summary = trained
    expected = json.loads((snaps / "manifest.json").read_text())["serving_contract"]["sha256"]
    for entry in summary["models"].values():
        with load_package(Path(entry["package"]), "fv1", expected) as loaded:
            assert loaded.serving_contract_sha256 == expected


def test_C_chosen_by_val_primary(trained, snaps):
    _, summary = trained
    train_rows, val_rows = load_split(snaps, "train"), load_split(snaps, "val")
    universe = load_universe(snaps)
    budget = Budget.parse("bytes_256MiB")
    values = {}
    for C in (0.1, 1.0):
        s = LogRegScorer(training_features(default_feature_schema()))
        s.fit(train_rows, None, {"C": C})
        values[C] = evaluate(val_rows, universe, s.score(val_rows), [budget], split="val")[
            "overall"
        ]["recall@bytes_256MiB"]
    best = max(values, key=lambda c: (values[c], -c))
    assert summary["models"]["logreg-v1"]["params"]["C"] == best


def test_every_package_plans_something_under_its_min_score(trained, snaps):
    _, summary = trained
    val = load_split(snaps, "val")
    tick = val.filter(pa.compute.equal(val.column("snapshot_id"), val.column("snapshot_id")[0]))
    for entry in summary["models"].values():
        with load_package(Path(entry["package"]), "fv1") as loaded:
            min_score = loaded.serving_policy["min_score"]
            mask = select(
                tick,
                loaded.score(tick),
                Budget.parse("bytes_256MiB"),
                min_score=float("-inf") if min_score is None else min_score,
            )
            assert mask.any(), loaded.model_type


def test_training_never_reads_the_test_split(snaps, tmp_path):
    poisoned = tmp_path / "poisoned"
    import shutil

    shutil.copytree(snaps, poisoned)
    for part in (poisoned / "snapshots" / "split=test").rglob("*.parquet"):
        part.write_bytes(b"not parquet: reading this would fail")
    summary = train(
        poisoned, TrainConfig(models=["logreg-v1", "baseline-mru"], C_grid=[1.0]), tmp_path / "out"
    )
    assert set(summary["models"]) == {"logreg-v1", "baseline-mru"}


def test_threshold_never_excludes_nothing(trained):
    _, summary = trained
    assert summary["models"]["baseline-hotset"]["min_score"] is None


def test_out_dir_must_be_fresh(snaps, tmp_path):
    (tmp_path / "out").mkdir()
    (tmp_path / "out" / "x").write_text("x")
    with pytest.raises(FileExistsError):
        train(snaps, TrainConfig(models=["baseline-mru"]), tmp_path / "out")


def test_missing_contract_rejected(snaps, tmp_path):
    import shutil

    copy = tmp_path / "nocontract"
    shutil.copytree(snaps, copy)
    manifest = json.loads((copy / "manifest.json").read_text())
    del manifest["serving_contract"]
    (copy / "manifest.json").write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="serving_contract"):
        train(copy, TrainConfig(models=["baseline-mru"]), tmp_path / "out")


# --- Config ------------------------------------------------------------------------------


def test_config_from_spec_yaml_shape():
    cfg = TrainConfig.from_dict(
        {
            "train": {
                "models": ["logreg-v1", "baseline-mru"],
                "logreg": {"C_grid": [0.5], "max_iter": 100},
                "hotset": {"top_n": 50},
                "serving_policy": {
                    "max_items": 5,
                    "min_score_quantiles": [0, 0.9],
                    "min_score_keep": 0.9,
                },
            }
        }
    )
    assert cfg.models == ["logreg-v1", "baseline-mru"] and cfg.C_grid == [0.5]
    assert cfg.hotset_top_n == 50 and cfg.max_items == 5 and cfg.min_score_keep == 0.9
    for bad in (
        {"train": {"nope": 1}},
        {"train": {"models": ["lgbm"]}},
        {"train": {"primary": "precision@top_10"}},
        {"train": {"serving_policy": {"range_cap_mib": 32}}},
    ):
        with pytest.raises((ValueError, KeyError)):
            TrainConfig.from_dict(bad)


# --- CLI and tracking ---------------------------------------------------------------------


def test_cli_trains_without_clearml(snaps, tmp_path):
    result = CliRunner().invoke(
        train_cli,
        [
            "--snapshots",
            str(snaps),
            "--out",
            str(tmp_path / "out"),
            "--models",
            "baseline-mru,logreg-v1",
        ],
    )
    assert result.exit_code == 0, result.output
    assert "Trained 2 models from fixture-snapshots-s7 (fixture)" in result.output


def test_cli_input_and_queue_validation(snaps, tmp_path):
    runner = CliRunner()
    neither = runner.invoke(train_cli, ["--out", str(tmp_path / "a")])
    assert neither.exit_code != 0 and "exactly one" in neither.output
    both = runner.invoke(train_cli, ["--snapshots", str(snaps), "--snapshots-dataset", "x"])
    assert both.exit_code != 0
    queued = runner.invoke(train_cli, ["--snapshots", str(snaps), "--queue", "default"])
    assert queued.exit_code != 0 and "configure a server" in queued.output


def test_tracking_requires_explicit_server_config(monkeypatch, tmp_path):
    monkeypatch.setenv("PCACHE_TRACKING", "on")
    assert tracking_enabled() is False  # clearml may be installed, but no server configured
    monkeypatch.setenv("CLEARML_API_HOST", "http://clearml.example.internal:8008")
    assert tracking_enabled() == _clearml_installed()
    assert tracking_enabled(no_track=True) is False
    monkeypatch.setenv("PCACHE_TRACKING", "off")
    assert tracking_enabled() is False


def _clearml_installed() -> bool:
    try:
        import clearml  # noqa: F401
    except ImportError:
        return False
    return True


def test_null_tracker_is_transparent(tmp_path):
    t = NullTracker()
    assert t.connect({"a": 1}, "x") == {"a": 1}
    assert t.register_model(tmp_path / "z.zip", "m", [], {}) is None
    assert t.task_id is None


def test_fixture_is_deterministic(tmp_path):
    a = make_fake_snapshots(
        tmp_path / "a", days=3, ticks_per_day=2, candidates_per_tick=10, split_days=(1, 2)
    )
    b = make_fake_snapshots(
        tmp_path / "b", days=3, ticks_per_day=2, candidates_per_tick=10, split_days=(1, 2)
    )
    for part in sorted((a / "snapshots").rglob("*.parquet")):
        other = b / part.relative_to(a)
        assert pq.ParquetFile(part).read().equals(pq.ParquetFile(other).read())


# --- ClearML control flow with a fake tracker (no server) -------------------------------


class FakeTracker(NullTracker):
    """Records calls; ``server_inputs`` plays the values stored on the ClearML task."""

    def __init__(self, server_inputs=None):
        self.server_inputs = server_inputs
        self.connected: dict = {}
        self.failed: str | None = None
        self.closed = False
        self.queued: str | None = None
        self.models: list[str] = []
        self.task_id = "fake-task"

    def connect(self, params, name):
        self.connected[name] = dict(params)
        return dict(self.server_inputs) if self.server_inputs is not None else dict(params)

    def register_model(self, package_zip, name, tags, metadata):
        self.models.append(name)
        return f"model-{name}"

    def execute_remotely(self, queue):
        self.queued = queue
        raise SystemExit(0)  # ClearML exits the submitting process

    def fail(self, message):
        self.failed = message

    def close(self):
        self.closed = True


def use_fake(monkeypatch, fake, snaps=None):
    import predictive_cache.ml.train_cli as cli_module

    monkeypatch.setattr(cli_module.ClearMLTracker, "start", classmethod(lambda cls, *a, **k: fake))
    if snaps is not None:
        monkeypatch.setattr(cli_module, "get_dataset_copy", lambda dataset_id: snaps)


def test_agent_run_without_cli_args_uses_task_inputs(monkeypatch, snaps):
    monkeypatch.setenv("CLEARML_TASK_ID", "abc")
    fake = FakeTracker({"snapshots_dataset_id": "ds-123"})
    use_fake(monkeypatch, fake, snaps)
    result = CliRunner().invoke(train_cli, ["--models", "baseline-mru"])
    assert result.exit_code == 0, result.output
    assert fake.models == ["baseline-mru"] and fake.closed and fake.failed is None


def test_agent_run_without_dataset_fails_and_marks_task(monkeypatch):
    monkeypatch.setenv("CLEARML_TASK_ID", "abc")
    fake = FakeTracker({"snapshots_dataset_id": ""})
    use_fake(monkeypatch, fake)
    result = CliRunner().invoke(train_cli, [])
    assert result.exit_code != 0 and "snapshots_dataset_id" in result.output
    assert fake.failed and fake.closed


def test_only_dataset_id_is_sent_to_the_server(monkeypatch, snaps, tmp_path):
    monkeypatch.setenv("CLEARML_API_HOST", "http://clearml.example.internal:8008")
    monkeypatch.setenv("PCACHE_TRACKING", "on")
    fake = FakeTracker()
    use_fake(monkeypatch, fake)
    if not _clearml_installed():
        pytest.skip("clearml extra not installed")
    result = CliRunner().invoke(
        train_cli,
        ["--snapshots", str(snaps), "--out", str(tmp_path / "o"), "--models", "baseline-mru"],
    )
    assert result.exit_code == 0, result.output
    assert fake.connected["inputs"] == {"snapshots_dataset_id": ""}
    assert str(snaps) not in json.dumps(fake.connected)


def test_tracked_failure_is_marked_failed(monkeypatch, snaps, tmp_path):
    monkeypatch.setenv("CLEARML_API_HOST", "http://clearml.example.internal:8008")
    monkeypatch.setenv("PCACHE_TRACKING", "on")
    if not _clearml_installed():
        pytest.skip("clearml extra not installed")
    fake = FakeTracker()
    use_fake(monkeypatch, fake)
    out = tmp_path / "o"
    out.mkdir()
    (out / "occupied").write_text("x")  # train() refuses a non-empty out dir
    result = CliRunner().invoke(
        train_cli, ["--snapshots", str(snaps), "--out", str(out), "--models", "baseline-mru"]
    )
    assert result.exit_code != 0 and fake.failed and fake.closed


def test_queue_submits_and_needs_dataset(monkeypatch, snaps):
    monkeypatch.setenv("CLEARML_API_HOST", "http://clearml.example.internal:8008")
    monkeypatch.setenv("PCACHE_TRACKING", "on")
    if not _clearml_installed():
        pytest.skip("clearml extra not installed")
    fake = FakeTracker()
    use_fake(monkeypatch, fake)
    local = CliRunner().invoke(train_cli, ["--snapshots", str(snaps), "--queue", "default"])
    assert local.exit_code != 0 and "--snapshots-dataset" in local.output and fake.failed
    fake2 = FakeTracker()
    use_fake(monkeypatch, fake2)
    queued = CliRunner().invoke(train_cli, ["--snapshots-dataset", "ds-1", "--queue", "default"])
    assert queued.exit_code == 0 and fake2.queued == "default" and fake2.models == []
