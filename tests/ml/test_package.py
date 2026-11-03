"""Acceptance tests for #54: Scorer interface, model package and loader (docs/ml/spec.md §3, §5)."""

from __future__ import annotations

import json
import shutil
import zipfile
from pathlib import Path
from typing import ClassVar

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from predictive_cache.ml import KEY_COLUMNS, PackageError, load_package, write_package
from predictive_cache.ml.cli import main as cli
from predictive_cache.ml.dummy import (
    ConstantScorer,
    default_feature_schema,
    fixture_rows,
    make_dummy_package,
)
from predictive_cache.ml.package import (
    COACCESS_SCHEMA,
    DEFAULT_SERVING_POLICY,
    compute_package_id,
    contract_sha256,
    seal_manifest,
)
from predictive_cache.ml.scorer import (
    MissingColumnError,
    feature_matrix,
    float_matrix,
    register_scorer,
    require_columns,
    scorer_class,
)

COLUMNS = ("opens_1h", "secs_since_last_open", "coacc_max_conf")


@register_scorer
class LinearTestScorer:
    """Feature-dependent scorer so smoke checks are meaningful."""

    model_type: ClassVar[str] = "test-linear"

    def __init__(self, weights: dict[str, float]) -> None:
        self.weights = dict(weights)

    @property
    def feature_columns(self) -> tuple[str, ...]:
        return tuple(self.weights)

    def fit(self, train, val, cfg) -> None:
        return None

    def score(self, rows: pa.Table) -> np.ndarray:
        x = np.nan_to_num(float_matrix(rows, self.feature_columns), nan=0.0)
        return x @ np.array(list(self.weights.values()), dtype=np.float64)

    def explain(self, rows, k=2):
        return [list(self.feature_columns[:k]) for _ in range(rows.num_rows)]

    def to_payload(self) -> dict:
        return {"columns": list(self.weights), "weights": list(self.weights.values())}

    @classmethod
    def from_payload(cls, payload, feature_schema):
        return cls(dict(zip(payload["columns"], payload["weights"], strict=True)))


def meta(**overrides):
    base = {
        "serving_contract": {
            "config": {"feature_version": "fv1", "clean": {"read_mode": "ranges"}}
        },
        "serving_policy": dict(DEFAULT_SERVING_POLICY),
        "data": {"data_kind": "fixture"},
        "training": {"params": {}},
    }
    base.update(overrides)
    return base


def build(tmp_path: Path, scorer=None, coaccess=None, **meta_overrides) -> Path:
    scorer = scorer or LinearTestScorer({c: 0.5 + i for i, c in enumerate(COLUMNS)})
    return write_package(
        scorer,
        default_feature_schema(),
        coaccess,
        fixture_rows(12),
        meta(**meta_overrides),
        tmp_path / "out",
    )


_OPEN: list = []


def request_close(loaded) -> None:
    _OPEN.append(loaded)


@pytest.fixture(autouse=True)
def _close_loaded_packages():
    yield
    while _OPEN:
        _OPEN.pop().close()


def package_dir(zip_path: Path) -> Path:
    return zip_path.with_suffix("")


def reseal(root: Path, mutate) -> None:
    """Edit package.json, then recompute hashes and package_id so earlier checks pass."""
    manifest = json.loads((root / "package.json").read_text())
    mutate(manifest)
    seal_manifest(root, manifest)


# --- Scorer interface -------------------------------------------------------------


def test_registry_and_unknown_type():
    assert scorer_class("test-linear") is LinearTestScorer
    assert scorer_class("dummy-constant") is ConstantScorer
    with pytest.raises(KeyError):
        scorer_class("nope")


def test_register_rejects_conflicting_type():
    with pytest.raises(ValueError):
        register_scorer(type("Other", (), {"model_type": "test-linear"}))


def test_feature_matrix_orders_columns_and_ignores_extras():
    rows = fixture_rows(4).append_column("label", pa.array([1, 0, 0, 1]))
    out = feature_matrix(rows, ["opens_5m", "file_size"])
    assert out.column_names == ["opens_5m", "file_size"]
    m = float_matrix(rows, ["file_size"])
    assert m.shape == (4, 1) and m.dtype == np.float64


def test_nulls_become_nan():
    rows = fixture_rows(6)
    m = float_matrix(rows, ["opens_1h"])
    assert np.isnan(m).sum() == rows.column("opens_1h").null_count


def test_missing_feature_or_key_column_rejected():
    rows = fixture_rows(3)
    with pytest.raises(MissingColumnError):
        LinearTestScorer({"opens_1h": 1.0}).score(rows.drop(["opens_1h"]))
    with pytest.raises(MissingColumnError):
        require_columns(rows.drop(["file_id"]), [])


# --- Package round trip -------------------------------------------------------------


def test_round_trip_identical_scores(tmp_path):
    scorer = LinearTestScorer({"opens_1h": 2.0, "coacc_max_conf": -1.5})
    zip_path = build(tmp_path, scorer)
    rows = fixture_rows(30, seed=3)
    expected = scorer.score(rows)
    for target in (zip_path, package_dir(zip_path)):
        loaded = load_package(target, "fv1")
        request_close(loaded)
        assert np.array_equal(loaded.score(rows), expected)
        assert loaded.model_version == f"test-linear@{loaded.package_id}"
        assert loaded.feature_columns == ("opens_1h", "coacc_max_conf")
        assert loaded.serving_policy == DEFAULT_SERVING_POLICY
        assert loaded.coaccess().schema.names == COACCESS_SCHEMA.names


def test_scoring_accepts_key_and_extra_columns(tmp_path):
    loaded = load_package(build(tmp_path), "fv1")
    request_close(loaded)
    rows = fixture_rows(5).append_column("label", pa.array([0] * 5))
    assert loaded.score(rows).shape == (5,)
    with pytest.raises(MissingColumnError):
        loaded.score(rows.drop(["coacc_max_conf"]))


def test_coaccess_copied_byte_for_byte(tmp_path):
    src = tmp_path / "coaccess.parquet"
    pq.write_table(
        pa.table(
            {
                "from_file_id": ["a"],
                "to_file_id": ["b"],
                "support": [3],
                "n_from": [5],
                "conf": [0.6],
                "lift": [2.5],
            },
            schema=COACCESS_SCHEMA,
        ),
        src,
    )
    loaded = load_package(build(tmp_path, coaccess=src), "fv1")
    request_close(loaded)
    assert loaded.coaccess_path.read_bytes() == src.read_bytes()
    assert loaded.coaccess().num_rows == 1


def test_smoke_rows_capped_and_have_keys(tmp_path):
    root = package_dir(build(tmp_path))
    smoke = pq.read_table(root / "smoke.parquet")
    assert smoke.num_rows <= 50
    assert set(KEY_COLUMNS) <= set(smoke.column_names)
    assert "expected_score" in smoke.column_names


def test_zip_contains_only_package_files(tmp_path):
    with zipfile.ZipFile(build(tmp_path)) as archive:
        assert sorted(archive.namelist()) == sorted(
            [
                "package.json",
                "model.json",
                "feature_schema.json",
                "coaccess.parquet",
                "smoke.parquet",
            ]
        )


# --- package_id sensitivity -----------------------------------------------------------


@pytest.mark.parametrize(
    "mutate",
    [
        lambda m: m["serving_policy"].update(max_items=7),
        lambda m: m["serving_contract"].update(sha256="0" * 64),
        lambda m: m.update(feature_columns=["opens_1h"]),
        lambda m: m["data"].update(snapshots_dataset_id="x"),
    ],
)
def test_any_manifest_change_changes_package_id(tmp_path, mutate):
    manifest = json.loads((package_dir(build(tmp_path)) / "package.json").read_text())
    before = compute_package_id(manifest)
    mutate(manifest)
    assert compute_package_id(manifest) != before


def test_created_at_does_not_change_package_id(tmp_path):
    manifest = json.loads((package_dir(build(tmp_path)) / "package.json").read_text())
    before = compute_package_id(manifest)
    manifest["created_at"] = "2000-01-01T00:00:00.000000Z"
    assert compute_package_id(manifest) == before


# --- Loader rejections, one per §5 check ----------------------------------------------


def expect_reason(target, reason, feature_version="fv1", contract=None):
    with pytest.raises(PackageError) as info:
        load_package(target, feature_version, contract)
    assert info.value.reason == reason, info.value


def test_1_bad_file_hash(tmp_path):
    root = package_dir(build(tmp_path))
    (root / "model.json").write_text('{"columns": ["opens_1h"], "weights": [99.0]}\n')
    expect_reason(root, "hash_mismatch")


def test_1_missing_file(tmp_path):
    root = package_dir(build(tmp_path))
    (root / "smoke.parquet").unlink()
    expect_reason(root, "missing_file")


def test_2_wrong_package_id(tmp_path):
    root = package_dir(build(tmp_path))
    manifest = json.loads((root / "package.json").read_text())
    manifest["package_id"] = "deadbeefdeadbeef"
    (root / "package.json").write_text(json.dumps(manifest))
    expect_reason(root, "bad_package_id")


def test_2_manifest_field_edited_without_resealing(tmp_path):
    root = package_dir(build(tmp_path))
    manifest = json.loads((root / "package.json").read_text())
    manifest["serving_policy"]["max_items"] = 1
    (root / "package.json").write_text(json.dumps(manifest))
    expect_reason(root, "bad_package_id")


def test_3_unknown_version(tmp_path):
    root = package_dir(build(tmp_path))
    reseal(root, lambda m: m.update(package_version=2))
    expect_reason(root, "unknown_package_version")


def test_3_unknown_model_type(tmp_path):
    root = package_dir(build(tmp_path))
    reseal(root, lambda m: m.update(model_type="lgbm-v9"))
    expect_reason(root, "unknown_model_type")


@pytest.mark.parametrize(
    "change",
    [
        {"lead_s": 30},
        {"range_cap": 32 * 2**20},
        {"max_items": 0},
        {"target": "x"},
        {"min_score": "high"},
    ],
)
def test_3_unservable_policy(tmp_path, change):
    root = package_dir(build(tmp_path))
    reseal(root, lambda m: m["serving_policy"].update(change))
    expect_reason(root, "invalid_serving_policy")


def test_3_policy_rejected_at_write_time(tmp_path):
    with pytest.raises(PackageError) as info:
        build(tmp_path, serving_policy={**DEFAULT_SERVING_POLICY, "lead_s": 60})
    assert info.value.reason == "invalid_serving_policy"


def test_4_wrong_feature_version(tmp_path):
    expect_reason(build(tmp_path), "feature_version_mismatch", feature_version="fv2")


def test_5_contract_hash_does_not_recompute(tmp_path):
    root = package_dir(build(tmp_path))
    reseal(root, lambda m: m["serving_contract"]["config"].update(extra=1))
    expect_reason(root, "contract_hash_mismatch")


def test_5_contract_differs_from_expected(tmp_path):
    zip_path = build(tmp_path)
    with load_package(zip_path, "fv1") as first:
        good = first.serving_contract_sha256
    with load_package(zip_path, "fv1", good) as second:
        assert second.serving_contract_sha256 == good
    expect_reason(zip_path, "contract_mismatch", contract=contract_sha256({"other": True}))


def test_6_feature_column_not_in_schema(tmp_path):
    root = package_dir(build(tmp_path))
    reseal(root, lambda m: m["feature_columns"].append("not_a_feature"))
    expect_reason(root, "unknown_feature_column")


def test_6_rejected_at_write_time(tmp_path):
    with pytest.raises(PackageError) as info:
        build(tmp_path, LinearTestScorer({"not_a_feature": 1.0}))
    assert info.value.reason == "unknown_feature_column"


def test_7_smoke_mismatch(tmp_path):
    root = package_dir(build(tmp_path))
    smoke = pq.read_table(root / "smoke.parquet")
    bumped = pa.array(smoke.column("expected_score").to_numpy() + 1e-6)
    idx = smoke.column_names.index("expected_score")
    pq.write_table(smoke.set_column(idx, "expected_score", bumped), root / "smoke.parquet")
    reseal(root, lambda m: None)
    expect_reason(root, "smoke_mismatch")


def test_unexpected_member_in_zip_or_dir(tmp_path):
    zip_path = build(tmp_path)
    tampered = tmp_path / "tampered.zip"
    shutil.copyfile(zip_path, tampered)
    with zipfile.ZipFile(tampered, "a") as archive:
        archive.writestr("../evil.sh", "echo hi")
    expect_reason(tampered, "unexpected_file")
    root = package_dir(zip_path)
    (root / "notes.txt").write_text("x")
    expect_reason(root, "unexpected_file")


def test_not_a_zip(tmp_path):
    bogus = tmp_path / "bogus.zip"
    bogus.write_bytes(b"not a zip")
    expect_reason(bogus, "invalid_zip")


# --- Dummy package and CLI ------------------------------------------------------------


def test_dummy_package_loads_and_scores_constant(tmp_path):
    loaded = load_package(make_dummy_package(tmp_path, value=0.25), "fv1")
    request_close(loaded)
    assert loaded.model_type == "dummy-constant"
    assert np.all(loaded.score(fixture_rows(7)) == 0.25)
    assert loaded.manifest["data"]["data_kind"] == "fixture"
    assert loaded.coaccess().num_rows == 0


def test_cli_dummy_verify_inspect(tmp_path):
    runner = CliRunner()
    made = runner.invoke(cli, ["dummy", "--out", str(tmp_path)])
    assert made.exit_code == 0, made.output
    zip_path = made.output.strip()
    verified = runner.invoke(cli, ["verify", zip_path])
    assert verified.exit_code == 0 and verified.output.startswith("OK dummy-constant@")
    inspected = runner.invoke(cli, ["inspect", zip_path])
    assert inspected.exit_code == 0 and "feature_version: fv1" in inspected.output
    wrong = runner.invoke(cli, ["verify", zip_path, "--feature-version", "fv9"])
    assert wrong.exit_code != 0 and "feature_version_mismatch" in wrong.output


# --- Review fixes: temp cleanup, exception types, runtime dependencies ------------------


def test_zip_extraction_cleaned_on_failure_and_close(tmp_path):
    zip_path = build(tmp_path)
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    expect_failure = pytest.raises(PackageError)
    with expect_failure:
        load_package(zip_path, "fv9", extract_dir=scratch)
    assert list(scratch.iterdir()) == []
    with load_package(zip_path, "fv1", extract_dir=scratch) as loaded:
        assert loaded.owns_root and loaded.root.parent == scratch
    assert list(scratch.iterdir()) == []
    loaded_dir = load_package(package_dir(zip_path), "fv1")
    loaded_dir.close()  # directory packages are never deleted
    assert package_dir(zip_path).is_dir()


def test_corrupt_zip_member_is_package_error(tmp_path):
    zip_path = build(tmp_path)
    data = bytearray(zip_path.read_bytes())
    with zipfile.ZipFile(zip_path) as archive:
        info = archive.getinfo("model.json")
    start = info.header_offset + 30 + len(info.filename) + len(info.extra)
    data[start + 2] ^= 0xFF
    corrupt = tmp_path / "corrupt.zip"
    corrupt.write_bytes(bytes(data))
    scratch = tmp_path / "scratch"
    scratch.mkdir()
    with pytest.raises(PackageError) as info_err:
        load_package(corrupt, "fv1", extract_dir=scratch)
    assert info_err.value.reason == "invalid_zip"
    assert list(scratch.iterdir()) == []


def test_non_string_model_type_is_package_error(tmp_path):
    root = package_dir(build(tmp_path))
    reseal(root, lambda m: m.update(model_type=["x"]))
    expect_reason(root, "unknown_model_type")


def test_runtime_import_needs_only_numpy_and_pyarrow():
    import subprocess
    import sys

    code = (
        "import sys, predictive_cache.ml, predictive_cache.ml.package;"
        "bad=[m for m in ('pydantic','yaml','sklearn','clearml','click') if m in sys.modules];"
        "print(','.join(bad))"
    )
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True)
    assert out.stdout.strip() == ""


def test_canonical_json_matches_datagen_for_plain_values():
    from predictive_cache.datagen.io import canonical_json as datagen_canonical
    from predictive_cache.ml.hashing import canonical_json

    value = {"b": [1, 2.5, None, True], "a": {"z": "x", "y": {"k": 0}}}
    assert canonical_json(value) == datagen_canonical(value)
