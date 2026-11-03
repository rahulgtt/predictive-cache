"""Acceptance tests for #55: baseline scorers (docs/ml/spec.md §4)."""

from __future__ import annotations

from datetime import UTC, datetime

import numpy as np
import pyarrow as pa
import pytest

from predictive_cache.ml import load_package, scorer_class, write_package
from predictive_cache.ml.baselines import (
    BASELINES,
    CoaccessScorer,
    FrequencyScorer,
    HotsetScorer,
    MruScorer,
)
from predictive_cache.ml.dummy import default_feature_schema, fixture_rows
from predictive_cache.ml.metrics import Budget, select
from predictive_cache.ml.package import DEFAULT_SERVING_POLICY
from predictive_cache.ml.scorer import MissingColumnError

T = datetime(2026, 1, 12, 9, 0, tzinfo=UTC)


def rows(**columns):
    n = len(next(iter(columns.values())))
    base = {
        "volume_id": columns.pop("volume_id", ["vol-1"] * n),
        "file_id": columns.pop("file_id", [f"file-{i}" for i in range(n)]),
        "t": pa.array([T] * n, type=pa.timestamp("us", tz="UTC")),
    }
    return pa.table({**base, **columns})


def ranking(scorer, table):
    """file_ids in the order evaluation/planner would take them (score desc, file_id asc)."""
    table = table.append_column("file_size", pa.array([1] * table.num_rows, pa.int64()))
    order = []
    remaining = np.ones(table.num_rows, dtype=bool)
    scores = scorer.score(table)
    while remaining.any():
        sub = table.filter(pa.array(remaining))
        pick = select(sub, scores[remaining], Budget("top_k", 1))
        idx = np.flatnonzero(remaining)[np.flatnonzero(pick)[0]]
        order.append(table.column("file_id")[idx].as_py())
        remaining[idx] = False
    return order


def test_all_registered():
    for cls in BASELINES:
        assert scorer_class(cls.model_type) is cls
    assert {c.model_type for c in BASELINES} == {
        "baseline-mru",
        "baseline-frequency",
        "baseline-hotset",
        "baseline-coaccess",
    }


def test_mru_ranks_recent_first_and_never_opened_last():
    t = rows(secs_since_last_open=pa.array([600.0, None, 30.0, 600.0]))
    scores = MruScorer().score(t)
    assert scores.tolist() == [-600.0, -np.inf, -30.0, -600.0]
    assert ranking(MruScorer(), t) == ["file-2", "file-0", "file-3", "file-1"]


def test_frequency_formula_and_nulls():
    t = rows(
        opens_24h=pa.array([3, None, 3, 10], pa.int64()),
        opens_7d=pa.array([50, 100, 7, None], pa.int64()),
    )
    assert FrequencyScorer().score(t).tolist() == pytest.approx([3.05, 0.1, 3.007, 10.0])
    assert ranking(FrequencyScorer(), t) == ["file-3", "file-0", "file-2", "file-1"]


def test_coaccess_formula_and_nulls():
    t = rows(
        coacc_max_conf=pa.array([0.7, None, 0.7, 0.2]),
        opens_1h=pa.array([1, 50, 4, None], pa.int64()),
    )
    assert CoaccessScorer().score(t).tolist() == pytest.approx([0.701, 0.05, 0.704, 0.2])
    assert ranking(CoaccessScorer(), t) == ["file-2", "file-0", "file-3", "file-1"]


def test_missing_feature_column_rejected():
    with pytest.raises(MissingColumnError):
        FrequencyScorer().score(rows(opens_24h=pa.array([1], pa.int64())))


def test_explain_reasons():
    t = rows(secs_since_last_open=pa.array([30.0, None]))
    assert MruScorer().explain(t) == [["secs_since_last_open=30"], ["secs_since_last_open=null"]]


# --- Hot set ---------------------------------------------------------------------------


def train_rows():
    """vol-1: a×(w1+w2)=3, b×1=1, c×3 (w1 each)=3 -> tie a/c broken by id; vol-2: z=1."""
    data = [
        ("vol-1", "a", 1, 1.0),
        ("vol-1", "a", 1, 2.0),
        ("vol-1", "a", 0, 9.0),
        ("vol-1", "b", 1, 1.0),
        ("vol-1", "c", 1, 1.0),
        ("vol-1", "c", 1, 1.0),
        ("vol-1", "c", 1, 1.0),
        ("vol-1", "d", 0, 50.0),
        ("vol-2", "z", 1, 1.0),
    ]
    return pa.table(
        {
            "volume_id": [d[0] for d in data],
            "file_id": [d[1] for d in data],
            "label": pa.array([d[2] for d in data], pa.int8()),
            "sample_weight": [d[3] for d in data],
            "split": ["train"] * len(data),
        }
    )


def test_hotset_fit_weighted_ranked_per_volume():
    h = HotsetScorer()
    h.fit(train_rows(), None, {"top_n": 2})
    assert h.hotset == {"vol-1": ["a", "c"], "vol-2": ["z"]}
    t = rows(volume_id=["vol-1"] * 4 + ["vol-2"], file_id=["a", "b", "c", "d", "z"])
    assert h.score(t).tolist() == [2.0, 0.0, 1.0, 0.0, 2.0]
    assert h.explain(t)[0] == ["hotset_rank=1"] and h.explain(t)[1] == []


def test_hotset_same_file_id_in_other_volume_not_hot():
    h = HotsetScorer()
    h.fit(train_rows(), None, {"top_n": 5})
    assert h.score(rows(volume_id=["vol-2"], file_id=["a"])).tolist() == [0.0]


def test_hotset_never_uses_val_or_test():
    h = HotsetScorer()
    val = train_rows().set_column(4, "split", pa.array(["val"] * 9))
    h.fit(train_rows(), val.filter(pa.array([True] * 9)), {"top_n": 2})
    baseline = dict(h.hotset)
    h2 = HotsetScorer()
    h2.fit(train_rows(), None, {"top_n": 2})
    assert h2.hotset == baseline  # val argument has no effect
    mixed = pa.concat_tables([train_rows(), val])
    with pytest.raises(ValueError, match="train rows only"):
        HotsetScorer().fit(mixed, None, {})


def test_hotset_unfitted_errors():
    with pytest.raises(ValueError):
        HotsetScorer().score(rows(file_id=["a"]))
    with pytest.raises(ValueError):
        HotsetScorer().to_payload()


# --- Packaging --------------------------------------------------------------------------


@pytest.mark.parametrize("cls", BASELINES)
def test_each_baseline_packages_and_reloads(tmp_path, cls):
    scorer = cls()
    if cls is HotsetScorer:
        fixture = fixture_rows(12)
        train = fixture.append_column("label", pa.array([1, 0] * 6, pa.int8()))
        scorer.fit(train, None, {"top_n": 3})
    meta = {
        "serving_contract": {"config": {"feature_version": "fv1"}},
        "serving_policy": dict(DEFAULT_SERVING_POLICY),
        "data": {"data_kind": "fixture"},
    }
    zip_path = write_package(
        scorer, default_feature_schema(), None, fixture_rows(12), meta, tmp_path
    )
    with load_package(zip_path, "fv1") as loaded:
        assert loaded.model_type == cls.model_type
        check = fixture_rows(20, seed=5)
        assert np.array_equal(loaded.score(check), scorer.score(check))
