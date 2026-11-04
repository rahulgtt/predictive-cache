"""GBDT scorer: numpy traversal, payload/package round trip, and LightGBM parity."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pyarrow as pa
import pytest

from predictive_cache.ml.gbdt import GbdtRankScorer, GbdtScorer, _walk
from predictive_cache.ml.scorer import scorer_class


def _lightgbm():
    try:
        import lightgbm
    except (ImportError, OSError):  # OSError: macOS wheel without libomp
        pytest.skip("lightgbm not loadable here")
    return lightgbm


# root: f0 <= 1.0 (missing NaN -> left); left: f1 <= 0.0 (missing Zero -> right); right leaf
TREE = {
    "feature": [0, 1, -1, -1, -1],
    "threshold": [1.0, 0.0, 0.0, 0.0, 0.0],
    "default_left": [True, False, False, False, False],
    "missing": [2, 1, 0, 0, 0],
    "left": [1, 2, -1, -1, -1],
    "right": [4, 3, -1, -1, -1],
    "value": [0.1, 0.2, -1.0, 2.0, 5.0],
}


def test_walk_follows_lightgbm_missing_rules():
    x = np.array(
        [
            [0.5, -1.0],  # left, left -> -1
            [np.nan, -1.0],  # NaN on a NaN-missing split -> default left, then left
            [0.5, 0.0],  # zero on a Zero-missing split -> default right -> 2
            [0.5, np.nan],  # NaN on a Zero-missing split counts as zero -> right -> 2
            [3.0, -1.0],  # right leaf -> 5
        ]
    )
    assert _walk(TREE, x).tolist() == [-1.0, -1.0, 2.0, 2.0, 5.0]


def test_contributions_sum_to_prediction_minus_root():
    x = np.array([[0.5, -1.0], [3.0, 1.0], [0.5, 2.0]])
    contrib = np.zeros((3, 2))
    leaf = _walk(TREE, x, contrib)
    assert np.allclose(contrib.sum(axis=1), leaf - TREE["value"][0])


def test_payload_round_trip_and_registry():
    scorer = GbdtScorer(("a", "b"), [TREE], {"rounds": 1})
    from predictive_cache.ml.scorer import KEY_COLUMNS

    base = {key: ["k", "k"] for key in KEY_COLUMNS}
    rows = pa.table({**base, "a": [0.5, 3.0], "b": [-1.0, None]})
    payload = scorer.to_payload()
    again = scorer_class("gbdt-v1").from_payload(payload, {})
    assert np.allclose(again.score(rows), scorer.score(rows))
    assert again.score(rows)[1] == pytest.approx(1 / (1 + np.exp(-5.0)))
    rank = GbdtRankScorer(("a", "b"), [TREE])
    assert rank.score(rows).tolist() == [-1.0, 5.0]
    with pytest.raises(ValueError):
        GbdtRankScorer.from_payload(payload, {})
    assert len(scorer.explain(rows, 1)[0]) == 1


@pytest.fixture(scope="module")
def snapshots(tmp_path_factory):
    from predictive_cache.ml.fixtures import make_fake_snapshots

    return make_fake_snapshots(tmp_path_factory.mktemp("gbdt") / "snap", days=10)


@pytest.mark.parametrize("cls", [GbdtScorer, GbdtRankScorer])
def test_matches_lightgbm_predict(snapshots, cls):
    _lightgbm()
    from predictive_cache.ml.logreg import training_features
    from predictive_cache.ml.snapshots import load_split, read_feature_schema

    features = training_features(read_feature_schema(snapshots))
    train, val = load_split(snapshots, "train"), load_split(snapshots, "val")
    scorer = cls(features)
    scorer.fit(train, val, {"num_leaves": 15, "rounds": 60, "min_data_in_leaf": 20})
    x = scorer._matrix(val)
    expected = scorer._booster.predict(
        x, num_iteration=scorer.params["rounds"], raw_score=cls is GbdtRankScorer
    )
    assert np.allclose(scorer.score(val), expected, rtol=1e-9, atol=1e-12)
    contrib = scorer._booster.predict(x[:20], num_iteration=scorer.params["rounds"], pred_contrib=True)
    assert np.allclose(contrib[:, :-1].sum(axis=1) + contrib[:, -1], scorer.raw(val.slice(0, 20)))


def test_train_packages_gbdt_and_loader_scores_it(snapshots, tmp_path):
    _lightgbm()
    from predictive_cache.ml.package import load_package
    from predictive_cache.ml.snapshots import load_split
    from predictive_cache.ml.train import TrainConfig, train

    cfg = TrainConfig(models=["gbdt-v1", "gbdt-rank-v1"], gbdt_num_leaves_grid=[7], gbdt_rounds=40)
    summary = train(snapshots, cfg, tmp_path / "out")
    for model in ("gbdt-v1", "gbdt-rank-v1"):
        with load_package(Path(summary["models"][model]["package"]), "fv1") as package:
            scores = package.score(load_split(snapshots, "test", ("test",)))
        assert np.isfinite(scores).all()
