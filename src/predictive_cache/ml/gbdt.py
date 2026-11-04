"""Gradient-boosted trees (docs/model PRD rung 4): trained with LightGBM, scored in numpy.

``fit`` imports LightGBM lazily (the ``ml`` extra; on macOS it also needs ``libomp``).
The package stores the trees as plain JSON arrays, so loading and scoring need only
numpy. That keeps packages hot-swappable in the Controller and runnable on any laptop.
Scoring reproduces LightGBM's numerical-split and missing-value rules exactly
(parity-tested against ``Booster.predict``).
"""

from __future__ import annotations

from typing import ClassVar

import numpy as np
import pyarrow as pa

from .scorer import float_matrix, register_scorer

ZERO_THRESHOLD = 1e-35
MISSING = {"None": 0, "Zero": 1, "NaN": 2}


def _flatten(tree: dict) -> dict:
    """LightGBM ``dump_model`` tree -> flat arrays (node 0 is the root)."""
    feature, threshold, default_left, missing, left, right, value = [], [], [], [], [], [], []

    def add(node: dict) -> int:
        index = len(feature)
        for column in (feature, threshold, default_left, missing, left, right, value):
            column.append(None)
        if "leaf_value" in node:
            feature[index], threshold[index], default_left[index] = -1, 0.0, False
            missing[index], left[index], right[index] = 0, -1, -1
            value[index] = float(node["leaf_value"])
            return index
        if node.get("decision_type", "<=") != "<=":
            raise ValueError("only numerical <= splits are supported")
        feature[index] = int(node["split_feature"])
        threshold[index] = float(node["threshold"])
        default_left[index] = bool(node["default_left"])
        missing[index] = MISSING[str(node.get("missing_type", "None"))]
        value[index] = float(node.get("internal_value", 0.0))
        left[index] = add(node["left_child"])
        right[index] = add(node["right_child"])
        return index

    add(tree)
    return {
        "feature": feature,
        "threshold": threshold,
        "default_left": default_left,
        "missing": missing,
        "left": left,
        "right": right,
        "value": value,
    }


def _walk(tree: dict, x: np.ndarray, contrib: np.ndarray | None = None) -> np.ndarray:
    """Leaf values for every row; optionally add Saabas path contributions."""
    feature = np.asarray(tree["feature"], dtype=np.int64)
    threshold = np.asarray(tree["threshold"], dtype=np.float64)
    default_left = np.asarray(tree["default_left"], dtype=bool)
    missing = np.asarray(tree["missing"], dtype=np.int64)
    left = np.asarray(tree["left"], dtype=np.int64)
    right = np.asarray(tree["right"], dtype=np.int64)
    value = np.asarray(tree["value"], dtype=np.float64)
    n = x.shape[0]
    rows = np.arange(n)
    node = np.zeros(n, dtype=np.int64)
    while True:
        f = feature[node]
        internal = f >= 0
        if not internal.any():
            break
        fval = x[rows, np.where(internal, f, 0)]
        nan = np.isnan(fval)
        kind = missing[node]
        fval = np.where(nan & (kind != MISSING["NaN"]), 0.0, fval)
        is_missing = ((kind == MISSING["Zero"]) & (np.abs(fval) <= ZERO_THRESHOLD)) | (
            (kind == MISSING["NaN"]) & nan
        )
        with np.errstate(invalid="ignore"):
            go_left = np.where(is_missing, default_left[node], fval <= threshold[node])
        child = np.where(go_left, left[node], right[node])
        if contrib is not None:
            delta = np.where(internal, value[np.where(internal, child, node)] - value[node], 0.0)
            np.add.at(contrib, (rows[internal], f[internal]), delta[internal])
        node = np.where(internal, child, node)
    return value[node]


@register_scorer
class GbdtScorer:
    """Binary LightGBM classifier over the package's raw feature columns."""

    model_type: ClassVar[str] = "gbdt-v1"
    objective: ClassVar[str] = "binary"

    def __init__(
        self,
        features: tuple[str, ...] = (),
        trees: list[dict] | None = None,
        params: dict | None = None,
    ) -> None:
        self._features = tuple(features)
        self.trees = list(trees or [])
        self.params = dict(params or {})

    @property
    def feature_columns(self) -> tuple[str, ...]:
        return self._features

    @property
    def fitted(self) -> bool:
        return bool(self.trees)

    def _matrix(self, rows: pa.Table) -> np.ndarray:
        return float_matrix(rows, self._features)

    def raw(self, rows: pa.Table, contrib: np.ndarray | None = None) -> np.ndarray:
        x = self._matrix(rows)
        out = np.zeros(x.shape[0])
        for tree in self.trees:
            out += _walk(tree, x, contrib)
        return out

    # --- Scorer ----------------------------------------------------------------------

    def fit(self, train: pa.Table, val: pa.Table | None, cfg: dict) -> None:
        """Fit on train; early-stop on val when given. ``cfg`` holds LightGBM settings."""
        try:
            import lightgbm as lgb
        except (ImportError, OSError) as exc:  # OSError: macOS without libomp
            raise RuntimeError(
                "gbdt training needs lightgbm (ml extra; on macOS also `brew install libomp`)"
            ) from exc
        if not self._features:
            raise ValueError(f"{self.model_type} needs feature columns before fit")
        params = {
            "objective": self.objective,
            "learning_rate": float(cfg.get("learning_rate", 0.05)),
            "num_leaves": int(cfg.get("num_leaves", 31)),
            "min_data_in_leaf": int(cfg.get("min_data_in_leaf", 200)),
            "feature_fraction": float(cfg.get("feature_fraction", 0.9)),
            "bagging_fraction": float(cfg.get("bagging_fraction", 0.8)),
            "bagging_freq": 1,
            "lambda_l2": float(cfg.get("lambda_l2", 1.0)),
            "seed": int(cfg.get("seed", 0)),
            "deterministic": True,
            "force_row_wise": True,
            "num_threads": int(cfg.get("num_threads", 0)),
            "verbose": -1,
        }
        rounds = int(cfg.get("rounds", 1000))
        if self.objective != "binary":
            import pyarrow.compute as pc

            train = train.take(pc.sort_indices(train, [("snapshot_id", "ascending")]))
            if val is not None:
                val = val.take(pc.sort_indices(val, [("snapshot_id", "ascending")]))
        train_set = self._dataset(lgb, train, weighted=True)
        callbacks, valid_sets = [], []
        if val is not None and val.num_rows:
            valid_sets = [self._dataset(lgb, val, weighted=False, reference=train_set)]
            callbacks.append(lgb.early_stopping(int(cfg.get("early_stopping", 50)), verbose=False))
            params["metric"] = "binary_logloss" if self.objective == "binary" else "ndcg"
            if self.objective != "binary":
                params["eval_at"] = [10]
        booster = lgb.train(params, train_set, rounds, valid_sets=valid_sets, callbacks=callbacks)
        best = booster.best_iteration or booster.current_iteration()
        dump = booster.dump_model(num_iteration=best)
        names = dump.get("feature_names") or []
        if names and list(names) != [f"Column_{i}" for i in range(len(self._features))] and list(
            names
        ) != list(self._features):
            raise ValueError("LightGBM feature order differs from the scorer features")
        self.trees = [_flatten(info["tree_structure"]) for info in dump["tree_info"]]
        gain = booster.feature_importance("gain", iteration=best)
        self.params = {
            **{k: params[k] for k in ("learning_rate", "num_leaves", "min_data_in_leaf",
                                      "feature_fraction", "bagging_fraction", "lambda_l2")},
            "rounds": int(best),
            "importance_gain": {
                name: float(g) for name, g in zip(self._features, gain, strict=True)
            },
        }
        self._booster = booster  # parity tests only; never serialised

    def _dataset(self, lgb, rows: pa.Table, *, weighted: bool, reference=None):
        y = np.asarray(rows.column("label").to_numpy(zero_copy_only=False), dtype=np.float64)
        weight = None
        if weighted and self.objective == "binary" and "sample_weight" in rows.column_names:
            weight = np.asarray(
                rows.column("sample_weight").to_numpy(zero_copy_only=False), dtype=np.float64
            )
        group = None
        if self.objective != "binary":
            ids = rows.column("snapshot_id").to_pylist()
            if any(a > b for a, b in zip(ids, ids[1:], strict=False)):
                raise ValueError("ranking rows must be sorted by snapshot_id")
            _, counts = np.unique(np.asarray(ids, dtype=object), return_counts=True)
            group = counts
        return lgb.Dataset(
            self._matrix(rows),
            label=y,
            weight=weight,
            group=group,
            feature_name=list(self._features),
            reference=reference,
            free_raw_data=False,
        )

    def score(self, rows: pa.Table) -> np.ndarray:
        if not self.fitted:
            raise ValueError(f"{self.model_type} is not fitted")
        raw = self.raw(rows)
        if self.objective != "binary":
            return raw
        with np.errstate(over="ignore"):
            return 1.0 / (1.0 + np.exp(-raw))

    def explain(self, rows: pa.Table, k: int = 2) -> list[list[str]]:
        contrib = np.zeros((rows.num_rows, len(self._features)))
        self.raw(rows, contrib)
        out = []
        for row in contrib:
            top = np.argsort(-row, kind="mergesort")[:k]
            out.append([f"{self._features[i]}:{row[i]:+.2f}" for i in top])
        return out

    def to_payload(self) -> dict:
        if not self.fitted:
            raise ValueError(f"{self.model_type} is not fitted")
        return {
            "model_type": self.model_type,
            "objective": self.objective,
            "features": list(self._features),
            "trees": self.trees,
            "params": self.params,
        }

    @classmethod
    def from_payload(cls, payload: dict, feature_schema: dict) -> GbdtScorer:
        if payload.get("model_type") != cls.model_type:
            raise ValueError(f"payload is not a {cls.model_type} payload")
        trees = list(payload["trees"])
        keys = ("feature", "threshold", "default_left", "missing", "left", "right", "value")
        for tree in trees:
            n = len(tree["feature"])
            if n == 0 or any(len(tree[key]) != n for key in keys):
                raise ValueError("malformed gbdt tree")
        return cls(tuple(payload["features"]), trees, dict(payload.get("params", {})))


@register_scorer
class GbdtRankScorer(GbdtScorer):
    """LambdaRank per snapshot (tick): optimises the per-tick ordering directly."""

    model_type: ClassVar[str] = "gbdt-rank-v1"
    objective: ClassVar[str] = "lambdarank"
