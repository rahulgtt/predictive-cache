"""Baseline scorers (docs/ml/spec.md §4).

A trained model must beat these at equal budgets. They are packaged like any
model, so evaluation and the Controller treat them the same way.
``baseline-frequency`` is also the Controller's built-in fallback (§8.6).
Ties are broken downstream by ``file_id`` (metrics.select / planner).
"""

from __future__ import annotations

from typing import ClassVar

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

from .scorer import float_matrix, register_scorer, require_columns


def _fmt(value: float) -> str:
    return "null" if np.isnan(value) else f"{value:g}"


class _FormulaBaseline:
    """Stateless baseline: score is a fixed function of a few features."""

    model_type: ClassVar[str]
    columns: ClassVar[tuple[str, ...]]

    @property
    def feature_columns(self) -> tuple[str, ...]:
        return self.columns

    def fit(self, train: pa.Table, val: pa.Table | None, cfg: dict) -> None:
        return None

    def _formula(self, x: np.ndarray) -> np.ndarray:
        raise NotImplementedError

    def score(self, rows: pa.Table) -> np.ndarray:
        return self._formula(float_matrix(rows, self.columns))

    def explain(self, rows: pa.Table, k: int = 2) -> list[list[str]]:
        x = float_matrix(rows, self.columns)
        return [
            [f"{name}={_fmt(v)}" for name, v in zip(self.columns, row, strict=True)][:k]
            for row in x
        ]

    def to_payload(self) -> dict:
        return {"model_type": self.model_type}

    @classmethod
    def from_payload(cls, payload: dict, feature_schema: dict):
        if payload.get("model_type") != cls.model_type:
            raise ValueError(f"payload is not a {cls.model_type} payload")
        return cls()


@register_scorer
class MruScorer(_FormulaBaseline):
    """Most recently used: ``−secs_since_last_open``; never opened → −∞."""

    model_type = "baseline-mru"
    columns = ("secs_since_last_open",)

    def _formula(self, x: np.ndarray) -> np.ndarray:
        return np.where(np.isnan(x[:, 0]), -np.inf, -x[:, 0])


@register_scorer
class FrequencyScorer(_FormulaBaseline):
    """Recent frequency: ``opens_24h + 1e-3 × opens_7d``; nulls → 0."""

    model_type = "baseline-frequency"
    columns = ("opens_24h", "opens_7d")

    def _formula(self, x: np.ndarray) -> np.ndarray:
        x = np.nan_to_num(x, nan=0.0)
        return x[:, 0] + 1e-3 * x[:, 1]


@register_scorer
class CoaccessScorer(_FormulaBaseline):
    """Co-access only: ``coacc_max_conf + 1e-3 × opens_1h``; nulls → 0."""

    model_type = "baseline-coaccess"
    columns = ("coacc_max_conf", "opens_1h")

    def _formula(self, x: np.ndarray) -> np.ndarray:
        x = np.nan_to_num(x, nan=0.0)
        return x[:, 0] + 1e-3 * x[:, 1]


@register_scorer
class HotsetScorer:
    """Static hot set: ``top_n − rank`` for files in the frozen per-volume set, else 0.

    Fitted on the **train split only**: the top ``top_n`` ``file_id``s per
    volume by the ``sample_weight``-weighted count of ``label = 1`` rows
    (ties by ``file_id``). Validation data is never read.
    """

    model_type: ClassVar[str] = "baseline-hotset"

    def __init__(self, top_n: int = 500, hotset: dict[str, list[str]] | None = None) -> None:
        if top_n <= 0:
            raise ValueError("top_n must be positive")
        self.top_n = int(top_n)
        self.hotset = {v: list(files) for v, files in (hotset or {}).items()}
        self._rank: dict[tuple[str, str], int] | None = None if hotset is None else self._index()

    def _index(self) -> dict[tuple[str, str], int]:
        return {(v, f): r for v, files in self.hotset.items() for r, f in enumerate(files)}

    @property
    def feature_columns(self) -> tuple[str, ...]:
        return ()

    def fit(self, train: pa.Table, val: pa.Table | None, cfg: dict) -> None:
        require_columns(train, ("volume_id", "file_id", "label"), keys=False)
        if "split" in train.column_names:
            splits = set(train.column("split").to_pylist())
            if splits - {"train"}:
                raise ValueError(f"hot set must be fitted on train rows only, got {sorted(splits)}")
        self.top_n = int(cfg.get("top_n", self.top_n))
        positive = pc.equal(pc.cast(train.column("label"), pa.int64()), 1)
        rows = train.filter(positive)
        weights = (
            rows.column("sample_weight").to_pylist()
            if "sample_weight" in rows.column_names
            else [1.0] * rows.num_rows
        )
        counts: dict[str, dict[str, float]] = {}
        for volume, file_id, w in zip(
            rows.column("volume_id").to_pylist(),
            rows.column("file_id").to_pylist(),
            weights,
            strict=True,
        ):
            per_volume = counts.setdefault(volume, {})
            per_volume[file_id] = per_volume.get(file_id, 0.0) + (1.0 if w is None else float(w))
        self.hotset = {
            volume: [f for f, _ in sorted(files.items(), key=lambda kv: (-kv[1], kv[0]))][
                : self.top_n
            ]
            for volume, files in sorted(counts.items())
        }
        self._rank = self._index()

    def score(self, rows: pa.Table) -> np.ndarray:
        require_columns(rows, ())
        if self._rank is None:
            raise ValueError("baseline-hotset is not fitted")
        keys = zip(
            rows.column("volume_id").to_pylist(), rows.column("file_id").to_pylist(), strict=True
        )
        return np.array(
            [float(self.top_n - self._rank[k]) if k in self._rank else 0.0 for k in keys],
            dtype=np.float64,
        )

    def explain(self, rows: pa.Table, k: int = 2) -> list[list[str]]:
        scores = self.score(rows)
        return [[f"hotset_rank={self.top_n - int(s) + 1}"] if s > 0 else [] for s in scores]

    def to_payload(self) -> dict:
        if self._rank is None:
            raise ValueError("baseline-hotset is not fitted")
        return {"model_type": self.model_type, "top_n": self.top_n, "hotset": self.hotset}

    @classmethod
    def from_payload(cls, payload: dict, feature_schema: dict) -> HotsetScorer:
        if payload.get("model_type") != cls.model_type:
            raise ValueError("payload is not a baseline-hotset payload")
        return cls(int(payload["top_n"]), dict(payload["hotset"]))


BASELINES: tuple[type, ...] = (MruScorer, FrequencyScorer, HotsetScorer, CoaccessScorer)
