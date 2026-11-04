"""Logistic regression v1 (docs/ml/spec.md §6).

Preprocessing is fitted on train rows only and stored in model.json; scoring
is numpy-only. scikit-learn is imported lazily inside ``fit``.
"""

from __future__ import annotations

import math
from typing import ClassVar

import numpy as np
import pyarrow as pa

from .scorer import float_matrix, register_scorer

SECONDS_CAP = 604_800.0  # 7 days

# Feature kinds for fv1 (spec §6.1). A feature without a kind cannot be trained on.
FEATURE_KINDS: dict[str, str] = {
    "secs_since_last_open": "seconds",
    "secs_since_last_full_read": "seconds",
    "secs_since_last_write": "seconds",
    "file_age_s": "seconds",
    "opens_5m": "count",
    "opens_15m": "count",
    "opens_1h": "count",
    "opens_24h": "count",
    "opens_7d": "count",
    "distinct_sessions_1h": "count",
    "distinct_sessions_24h": "count",
    "dir_opens_15m": "count",
    "dir_opens_1h": "count",
    "dir_distinct_sessions_1h": "count",
    "project_opens_1h": "count",
    "volume_opens_5m": "count",
    "coacc_sources": "count",
    "file_size": "bytes",
    "mean_bytes_per_open_7d": "bytes",
    "growth_15m_vs_1h": "ratio",
    "full_read_share_7d": "ratio",
    "coacc_max_conf": "ratio",
    "coacc_sum_conf": "ratio",
    "coacc_max_lift": "lift",
    "read_count_5m": "count", "read_count_15m": "count", "read_count_1h": "count",
    "read_count_24h": "count", "read_count_7d": "count", "returned_bytes_5m": "bytes",
    "returned_bytes_1h": "bytes", "returned_bytes_24h": "bytes", "returned_bytes_7d": "bytes",
    "distinct_read_ranges_1h": "count", "distinct_read_ranges_24h": "count",
    "secs_since_last_read": "seconds", "read_days_7d": "count", "volume_read_share_1h": "ratio",
    "coread_from_anchor_24h": "count", "file_size_bytes": "bytes",
}
FILL = {"seconds": SECONDS_CAP, "count": 0.0, "bytes": 0.0, "ratio": 0.0, "lift": 0.0}
CLIP = 10.0


def _transform(kind: str, x: np.ndarray) -> np.ndarray:
    if kind == "seconds":
        return np.log1p(np.clip(x, 0.0, SECONDS_CAP))
    if kind in ("count", "bytes", "lift"):
        return np.log1p(np.maximum(x, 0.0))
    if kind == "ratio":
        return x
    raise ValueError(f"unknown feature kind {kind!r}")


def training_features(feature_schema: dict) -> tuple[str, ...]:
    """Available schema features, in schema order; fails on features without a kind."""
    names = [f["name"] for f in feature_schema.get("features", []) if f.get("available", True)]
    unknown = [n for n in names if n not in FEATURE_KINDS]
    if unknown:
        raise ValueError(
            f"features without a kind in spec §6.1: {', '.join(unknown)}; add them to FEATURE_KINDS"
        )
    if not names:
        raise ValueError("feature schema has no available features")
    return tuple(names)


@register_scorer
class LogRegScorer:
    """L2 logistic regression over preprocessed fv1 features."""

    model_type: ClassVar[str] = "logreg-v1"

    def __init__(
        self,
        features: tuple[str, ...] = (),
        preprocess: list[dict] | None = None,
        coef: list[float] | None = None,
        intercept: float = 0.0,
        params: dict | None = None,
    ) -> None:
        self._features = tuple(features)
        self.preprocess = preprocess or []
        self.coef = np.asarray(coef or [], dtype=np.float64)
        self.intercept = float(intercept)
        self.params = dict(params or {})

    @property
    def feature_columns(self) -> tuple[str, ...]:
        return self._features

    @property
    def fitted(self) -> bool:
        return bool(self.preprocess) and self.coef.size == len(self.transformed_columns)

    @property
    def transformed_columns(self) -> list[str]:
        out = []
        for p in self.preprocess:
            if p["null_indicator"]:
                out.append(f"isnull__{p['name']}")
            out.append(p["name"])
        return out

    # --- preprocessing -------------------------------------------------------------

    def _fit_preprocess(self, raw: np.ndarray) -> None:
        self.preprocess = []
        for j, name in enumerate(self._features):
            kind = FEATURE_KINDS[name]
            col = raw[:, j]
            isnull = np.isnan(col)
            values = _transform(kind, np.where(isnull, FILL[kind], col))
            entry = {
                "name": name,
                "kind": kind,
                "fill": FILL[kind],
                "null_indicator": bool(isnull.any()),
            }
            entry["mean"], entry["std"] = _stats(values)
            if entry["null_indicator"]:
                entry["null_mean"], entry["null_std"] = _stats(isnull.astype(np.float64))
            self.preprocess.append(entry)

    def design(self, rows: pa.Table) -> np.ndarray:
        """Standardised design matrix (n_rows, len(transformed_columns))."""
        raw = float_matrix(rows, self._features)
        blocks = []
        for j, p in enumerate(self.preprocess):
            col = raw[:, j]
            isnull = np.isnan(col)
            if p["null_indicator"]:
                blocks.append((isnull.astype(np.float64) - p["null_mean"]) / p["null_std"])
            values = _transform(p["kind"], np.where(isnull, p["fill"], col))
            blocks.append((values - p["mean"]) / p["std"])
        x = np.column_stack(blocks) if blocks else np.zeros((rows.num_rows, 0))
        return np.clip(x, -CLIP, CLIP)

    # --- Scorer ----------------------------------------------------------------------

    def fit(self, train: pa.Table, val: pa.Table | None, cfg: dict) -> None:
        """Fit preprocessing and an L2 logistic regression with ``C = cfg["C"]`` on train only."""
        try:
            from sklearn.linear_model import LogisticRegression
        except ImportError as exc:  # pragma: no cover - exercised only without the extra
            raise RuntimeError("training needs the ml extra: uv sync --extra ml") from exc
        if not self._features:
            raise ValueError("LogRegScorer needs feature columns before fit")
        y = np.asarray(train.column("label").to_numpy(zero_copy_only=False), dtype=np.int64)
        if np.unique(y).size < 2:
            raise ValueError("train rows need both positive and negative labels")
        weights = (
            np.asarray(
                train.column("sample_weight").to_numpy(zero_copy_only=False), dtype=np.float64
            )
            if "sample_weight" in train.column_names
            else np.ones(train.num_rows)
        )
        self._fit_preprocess(float_matrix(train, self._features))
        model = LogisticRegression(
            C=float(cfg.get("C", 1.0)), solver="lbfgs", max_iter=int(cfg.get("max_iter", 2000))
        )
        model.fit(self.design(train), y, sample_weight=weights)
        self.coef = model.coef_[0].astype(np.float64)
        self.intercept = float(model.intercept_[0])
        self.params = {"C": float(cfg.get("C", 1.0)), "max_iter": int(cfg.get("max_iter", 2000))}
        self._sklearn = model  # kept only for parity tests; never serialised

    def score(self, rows: pa.Table) -> np.ndarray:
        if not self.fitted:
            raise ValueError("logreg-v1 is not fitted")
        z = self.design(rows) @ self.coef + self.intercept
        with np.errstate(over="ignore"):
            return 1.0 / (1.0 + np.exp(-z))

    def explain(self, rows: pa.Table, k: int = 2) -> list[list[str]]:
        terms = self.design(rows) * self.coef
        names = self.transformed_columns
        out = []
        for row in terms:
            top = np.argsort(-row, kind="mergesort")[:k]
            out.append([f"{names[i]}:{row[i]:+.2f}" for i in top])
        return out

    def to_payload(self) -> dict:
        if not self.fitted:
            raise ValueError("logreg-v1 is not fitted")
        return {
            "features": list(self._features),
            "preprocess": self.preprocess,
            "columns": self.transformed_columns,
            "coef": [float(c) for c in self.coef],
            "intercept": self.intercept,
            "params": self.params,
        }

    @classmethod
    def from_payload(cls, payload: dict, feature_schema: dict) -> LogRegScorer:
        scorer = cls(
            tuple(payload["features"]),
            list(payload["preprocess"]),
            list(payload["coef"]),
            float(payload["intercept"]),
            dict(payload.get("params", {})),
        )
        if scorer.transformed_columns != list(payload["columns"]):
            raise ValueError("preprocess does not match the stored column order")
        if not scorer.fitted:
            raise ValueError("coefficient count does not match the transformed columns")
        return scorer


def _stats(values: np.ndarray) -> tuple[float, float]:
    mean = float(values.mean()) if values.size else 0.0
    std = float(values.std()) if values.size else 0.0
    return mean, (std if std > 0 and math.isfinite(std) else 1.0)
