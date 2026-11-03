"""Acceptance tests for #57: evaluation metrics (docs/ml/spec.md §7.1-§7.3).

Expected values are computed by hand in the comments; see ``fixture()``.
"""

from __future__ import annotations

from datetime import UTC, datetime

import numpy as np
import pyarrow as pa
import pytest

from predictive_cache.ml.metrics import (
    MIB,
    PRIMARY_METRIC,
    RANGE_CAP,
    Budget,
    average_precision,
    bootstrap_diff,
    costs,
    default_budgets,
    evaluate,
    micro,
    reliability,
    select,
)

D1 = datetime(2026, 1, 12, 9, 0, tzinfo=UTC)
D1B = datetime(2026, 1, 12, 10, 0, tzinfo=UTC)
D2 = datetime(2026, 1, 13, 9, 0, tzinfo=UTC)
TS = pa.timestamp("us", tz="UTC")
TOP2 = Budget("top_k", 2)
B30 = Budget("bytes", 30 * MIB)


def fixture():
    """Ticks (status ok, split val unless noted):

    A vol-1 D1: 3 positives eligible/known (2 candidates), 1 unknown-at-t.
        rows f1 .9 pos 10MiB | f2 .8 neg 20MiB (cost 16) | f3 .8 pos null (cost 16) | f4 .1 neg 1MiB
        order f1, f2, f3 (tie -> file_id), f4
    B vol-1 D2: 2 positives, none among candidates. rows g1 .5 neg 5MiB
    C vol-2 D1: 0 positives. rows h1 .3 neg 2MiB
    E vol-1 D1B: 1 positive, no candidate rows at all.
    W vol-1: warmup (ignored). T vol-1: split test (ignored with split="val").
    """
    rows = [
        # snapshot_id, volume, file, label, size, score
        ("vol-1@A", "vol-1", "file-1", 1, 10 * MIB, 0.9),
        ("vol-1@A", "vol-1", "file-2", 0, 20 * MIB, 0.8),
        ("vol-1@A", "vol-1", "file-3", 1, None, 0.8),
        ("vol-1@A", "vol-1", "file-4", 0, 1 * MIB, 0.1),
        ("vol-1@B", "vol-1", "file-5", 0, 5 * MIB, 0.5),
        ("vol-2@C", "vol-2", "file-6", 0, 2 * MIB, 0.3),
    ]
    snapshots = pa.table(
        {
            "snapshot_id": [r[0] for r in rows],
            "volume_id": [r[1] for r in rows],
            "file_id": [r[2] for r in rows],
            "label": pa.array([r[3] for r in rows], type=pa.int8()),
            "file_size": pa.array([r[4] for r in rows], type=pa.int64()),
            "split": ["val"] * len(rows),
        }
    )
    scores = np.array([r[5] for r in rows])
    ticks = [
        # snapshot_id, t, split, status, n_pos, n_pos_cand, n_unknown
        ("vol-1@A", D1, "val", "ok", 3, 2, 1),
        ("vol-1@B", D2, "val", "ok", 2, 0, 0),
        ("vol-2@C", D1, "val", "ok", 0, 0, 0),
        ("vol-1@E", D1B, "val", "ok", 1, 0, 0),
        ("vol-1@W", D1, None, "warmup", 5, 5, 0),
        ("vol-1@T", D2, "test", "ok", 9, 9, 0),
    ]
    universe = pa.table(
        {
            "snapshot_id": [t[0] for t in ticks],
            "t": pa.array([t[1] for t in ticks], type=TS),
            "split": [t[2] for t in ticks],
            "status": [t[3] for t in ticks],
            "n_positive_files": [t[4] for t in ticks],
            "n_positive_in_candidates": [t[5] for t in ticks],
            "n_positive_unknown_at_t": [t[6] for t in ticks],
        }
    )
    return snapshots, universe, scores


def tick_rows(snapshots, sid):
    return snapshots.filter(pa.compute.equal(snapshots.column("snapshot_id"), sid))


# --- Budget and costs ---------------------------------------------------------------


def test_budget_parse_and_names():
    assert Budget.parse("top_25") == Budget("top_k", 25)
    assert Budget.parse("bytes_256MiB") == Budget("bytes", 256 * MIB)
    assert [b.name for b in default_budgets()] == [
        "top_10",
        "top_25",
        "top_50",
        "bytes_64MiB",
        "bytes_256MiB",
        "bytes_1024MiB",
    ]
    assert PRIMARY_METRIC == "recall@bytes_256MiB"
    for bad in ("top_0", "bytes_1GB", "k10", "bytes_0MiB"):
        with pytest.raises(ValueError):
            Budget.parse(bad)


def test_costs_cap_and_null():
    out = costs(np.array([1.0, 20 * MIB, np.nan, 0.0]))
    assert out.tolist() == [1.0, RANGE_CAP, RANGE_CAP, 0.0]


# --- Per-tick selection ---------------------------------------------------------------


def test_top_k_tie_broken_by_file_id():
    snapshots, _, scores = fixture()
    rows = tick_rows(snapshots, "vol-1@A")
    mask = select(rows, scores[:4], TOP2)
    assert rows.column("file_id").filter(pa.array(mask)).to_pylist() == ["file-1", "file-2"]


def test_byte_budget_skips_and_continues():
    snapshots, _, scores = fixture()
    rows = tick_rows(snapshots, "vol-1@A")
    # f1 10 -> 10, f2 16 -> 26, f3 16 -> 42 > 30 skip, f4 1 -> 27
    mask = select(rows, scores[:4], B30)
    assert rows.column("file_id").filter(pa.array(mask)).to_pylist() == [
        "file-1",
        "file-2",
        "file-4",
    ]


def test_min_score_applied_before_budget():
    snapshots, _, scores = fixture()
    rows = tick_rows(snapshots, "vol-1@A")
    mask = select(rows, scores[:4], B30, min_score=0.5)
    assert rows.column("file_id").filter(pa.array(mask)).to_pylist() == ["file-1", "file-2"]
    assert not select(rows, scores[:4], TOP2, min_score=0.95).any()


def test_negative_and_nan_scores():
    rows = pa.table({"file_id": ["a", "b", "c"], "file_size": pa.array([1, 1, 1], pa.int64())})
    scores = np.array([-np.inf, -5.0, np.nan])
    assert select(rows, scores, Budget("top_k", 3)).tolist() == [True, True, True]
    assert select(rows, scores, Budget("top_k", 1)).tolist() == [False, True, False]
    assert select(rows, scores, Budget("top_k", 3), min_score=-10).tolist() == [
        False,
        True,
        False,
    ]


# --- Aggregated metrics ---------------------------------------------------------------


def test_evaluate_hand_computed():
    snapshots, universe, scores = fixture()
    result = evaluate(snapshots, universe, scores, [TOP2, B30], split="val")
    o = result["overall"]
    assert result["n_ticks"] == 4 and result["n_rows"] == 6
    # denominators: n_positive_files 3+2+0+1 = 6; unknown 1
    assert o["n_positive_files"] == 6 and o["n_positive_unknown_at_t"] == 1
    assert o["coverage"] == pytest.approx(2 / 6)
    assert o["unknown_at_t_share"] == pytest.approx(1 / 7)
    # top_2: A {f1,f2} 1 pos; B {g1}; C {h1}; E none -> 1 pos / 4 selected
    assert o["recall@top_2"] == pytest.approx(1 / 6)
    assert o["precision@top_2"] == pytest.approx(1 / 4)
    assert o["warmed_bytes@top_2"] == (10 + 16 + 5 + 2) * MIB
    assert o["positive_warmed_share@top_2"] == pytest.approx(10 / 33)
    # bytes_30MiB: A {f1,f2,f4} cost 27; B 5; C 2 -> 34 MiB, 1 pos / 5 selected
    assert o["recall@bytes_30MiB"] == pytest.approx(1 / 6)
    assert o["precision@bytes_30MiB"] == pytest.approx(1 / 5)
    assert o["warmed_bytes@bytes_30MiB"] == 34 * MIB
    assert o["positive_warmed_share@bytes_30MiB"] == pytest.approx(10 / 34)
    # AP over all candidate rows: thresholds .9 (P1,R.5), .8 (P2/3,R1) -> .5 + .5*2/3
    assert o["pr_auc"] == pytest.approx(0.5 + 0.5 * 2 / 3)


def test_recall_never_exceeds_coverage_and_positives_outside_candidates_count():
    snapshots, universe, scores = fixture()
    perfect = snapshots.column("label").to_numpy().astype(float)  # oracle scores
    o = evaluate(snapshots, universe, perfect, [Budget("top_k", 50)], split="val")["overall"]
    assert o["recall@top_50"] == pytest.approx(o["coverage"]) == pytest.approx(2 / 6)


def test_per_volume_and_zero_positive_tick():
    snapshots, universe, scores = fixture()
    result = evaluate(snapshots, universe, scores, [TOP2], split="val")
    v2 = result["per_volume"]["vol-2"]
    assert v2["n_positive_files"] == 0
    assert v2["recall@top_2"] is None and v2["coverage"] is None and v2["pr_auc"] is None
    assert v2["precision@top_2"] == 0.0
    v1 = result["per_volume"]["vol-1"]
    assert v1["n_positive_files"] == 6 and v1["recall@top_2"] == pytest.approx(1 / 6)


def test_tick_without_rows_still_in_denominator():
    snapshots, universe, scores = fixture()
    per_tick = evaluate(snapshots, universe, scores, [TOP2], split="val")["per_tick"]
    e = per_tick.filter(pa.compute.equal(per_tick.column("snapshot_id"), "vol-1@E"))
    assert e.column("n_rows").to_pylist() == [0]
    assert e.column("n_positive_files").to_pylist() == [1]
    assert e.column("selected@top_2").to_pylist() == [0]


def test_split_inferred_from_snapshots_or_required():
    snapshots, universe, scores = fixture()
    inferred = evaluate(snapshots, universe, scores, [TOP2])  # snapshots are all "val"
    assert inferred["n_ticks"] == 4  # test-split tick and warmup tick excluded
    assert inferred["overall"]["n_positive_files"] == 6
    mixed = snapshots.set_column(
        snapshots.column_names.index("split"),
        "split",
        pa.array(["val"] * 5 + ["test"]),
    )
    with pytest.raises(ValueError, match="pass split="):
        evaluate(mixed, universe, scores, [TOP2])
    empty = snapshots.slice(0, 0)
    with pytest.raises(ValueError, match="pass split="):
        evaluate(empty, universe, scores[:0], [TOP2])
    assert evaluate(empty, universe, scores[:0], [TOP2], split="test")["n_ticks"] == 1


def test_rows_for_non_ok_tick_rejected():
    snapshots, universe, scores = fixture()
    bad = pa.concat_tables(
        [snapshots, snapshots.slice(0, 1).set_column(0, "snapshot_id", pa.array(["vol-1@W"]))]
    )
    with pytest.raises(ValueError):
        evaluate(bad, universe, np.r_[scores, 0.5], [TOP2], split="val")


def test_scores_length_checked():
    snapshots, universe, scores = fixture()
    with pytest.raises(ValueError):
        evaluate(snapshots, universe, scores[:-1], [TOP2])


def test_min_score_in_evaluate():
    snapshots, universe, scores = fixture()
    o = evaluate(snapshots, universe, scores, [B30], split="val", min_score=0.5)["overall"]
    # A {f1,f2} (f3 skipped by budget), B {g1 .5}, C h1 .3 below threshold
    assert o["precision@bytes_30MiB"] == pytest.approx(1 / 3)
    assert o["warmed_bytes@bytes_30MiB"] == (26 + 5) * MIB


def test_micro_matches_overall():
    snapshots, universe, scores = fixture()
    result = evaluate(snapshots, universe, scores, [TOP2, B30], split="val")
    for metric in ("recall@top_2", "precision@bytes_30MiB", "positive_warmed_share@top_2"):
        assert micro(result["per_tick"], metric) == pytest.approx(result["overall"][metric])


# --- PR-AUC and reliability -----------------------------------------------------------


def test_average_precision_cases():
    assert average_precision(np.array([0.9, 0.1]), np.array([1, 0])) == pytest.approx(1.0)
    assert average_precision(np.array([0.1, 0.9]), np.array([1, 0])) == pytest.approx(0.5)
    assert average_precision(np.array([0.5, 0.5]), np.array([1, 0])) == pytest.approx(0.5)
    assert average_precision(np.array([0.5]), np.array([0])) is None


def test_reliability_equal_count_ece():
    rows, ece = reliability(np.array([0.1, 0.2, 0.8, 0.9]), np.array([0, 0, 1, 1]), bins=2)
    assert [r["n"] for r in rows] == [2, 2]
    assert ece == pytest.approx(0.15)
    assert reliability(np.array([-1.0, 2.0]), np.array([0, 1])) is None


# --- Bootstrap -----------------------------------------------------------------------


def per_tick_for(scores):
    snapshots, universe, _ = fixture()
    return evaluate(snapshots, universe, scores, [TOP2, B30], split="val")["per_tick"]


def test_bootstrap_identical_models_zero_interval():
    _, _, scores = fixture()
    a = per_tick_for(scores)
    assert bootstrap_diff(a, a, "recall@top_2", reps=200, seed=1) == (0.0, 0.0)


def test_bootstrap_deterministic_and_sign():
    snapshots, _, scores = fixture()
    oracle = per_tick_for(snapshots.column("label").to_numpy().astype(float))
    worst = per_tick_for(-snapshots.column("label").to_numpy().astype(float))
    first = bootstrap_diff(oracle, worst, "recall@top_2", reps=500, seed=7)
    assert first == bootstrap_diff(oracle, worst, "recall@top_2", reps=500, seed=7)
    assert first[0] >= 0 and first[1] > 0


def test_bootstrap_rejects_mismatched_ticks_and_metrics():
    _, _, scores = fixture()
    a = per_tick_for(scores)
    with pytest.raises(ValueError):
        bootstrap_diff(a, a.slice(1), "recall@top_2")
    with pytest.raises(ValueError):
        bootstrap_diff(a, a, "coverage")
