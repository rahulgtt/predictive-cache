"""Dummy (constant) scorer and package fixture.

Lets Controller and evaluation work start before real snapshots and trained
models exist. Its feature schema mirrors data-prep fv1 names; real packages
copy feature_schema.json from the snapshots they were trained on.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import ClassVar

import numpy as np
import pyarrow as pa

from .package import DEFAULT_SERVING_POLICY, write_package
from .scorer import register_scorer, require_columns

# (name, dtype, group) for data-prep fv1 (docs/data-prep/spec.md §5.2), time group disabled.
FV1_FEATURES: tuple[tuple[str, str, str], ...] = (
    ("secs_since_last_open", "float64", "recency"),
    ("secs_since_last_full_read", "float64", "recency"),
    ("opens_5m", "int64", "frequency"),
    ("opens_15m", "int64", "frequency"),
    ("opens_1h", "int64", "frequency"),
    ("opens_24h", "int64", "frequency"),
    ("opens_7d", "int64", "frequency"),
    ("distinct_sessions_1h", "int64", "sessions"),
    ("distinct_sessions_24h", "int64", "sessions"),
    ("growth_15m_vs_1h", "float64", "growth"),
    ("file_size", "int64", "size_bytes"),
    ("mean_bytes_per_open_7d", "float64", "size_bytes"),
    ("full_read_share_7d", "float64", "size_bytes"),
    ("dir_opens_15m", "int64", "activity"),
    ("dir_opens_1h", "int64", "activity"),
    ("dir_distinct_sessions_1h", "int64", "activity"),
    ("project_opens_1h", "int64", "activity"),
    ("volume_opens_5m", "int64", "activity"),
    ("coacc_max_conf", "float64", "coaccess"),
    ("coacc_max_lift", "float64", "coaccess"),
    ("coacc_sum_conf", "float64", "coaccess"),
    ("coacc_sources", "int64", "coaccess"),
    ("file_age_s", "float64", "age_churn"),
    ("secs_since_last_write", "float64", "age_churn"),
)

_ARROW_TYPES = {"float64": pa.float64(), "int64": pa.int64()}


def default_feature_schema(feature_version: str = "fv1") -> dict:
    return {
        "feature_version": feature_version,
        "features": [
            {
                "name": name,
                "dtype": dtype,
                "group": group,
                "definition": "data-prep §5.2",
                "available": True,
            }
            for name, dtype, group in FV1_FEATURES
        ],
    }


def fixture_rows(n: int = 8, seed: int = 0, volume_id: str = "vol-0001") -> pa.Table:
    """Schema-valid feature rows (key columns + every fv1 feature) with some nulls."""
    rng = np.random.default_rng(seed)
    t = datetime(2026, 1, 12, 9, 0, tzinfo=UTC)
    columns: dict[str, pa.Array] = {
        "volume_id": pa.array([volume_id] * n),
        "file_id": pa.array([f"file-{i:06d}" for i in range(n)]),
        "t": pa.array([t] * n, type=pa.timestamp("us", tz="UTC")),
    }
    for name, dtype, _ in FV1_FEATURES:
        if dtype == "int64":
            values = rng.integers(0, 50, size=n).tolist()
        else:
            values = rng.uniform(0, 10_000, size=n).round(3).tolist()
        if n > 1:
            values[int(rng.integers(0, n))] = None
        columns[name] = pa.array(values, type=_ARROW_TYPES[dtype])
    return pa.table(columns)


@register_scorer
class ConstantScorer:
    """Scores every row with the same value; for plumbing only."""

    model_type: ClassVar[str] = "dummy-constant"

    def __init__(self, value: float = 0.5, feature_columns: tuple[str, ...] = ()) -> None:
        self.value = float(value)
        self._feature_columns = tuple(feature_columns)

    @property
    def feature_columns(self) -> tuple[str, ...]:
        return self._feature_columns

    def fit(self, train: pa.Table, val: pa.Table | None, cfg: dict) -> None:
        return None

    def score(self, rows: pa.Table) -> np.ndarray:
        require_columns(rows, self._feature_columns)
        return np.full(rows.num_rows, self.value, dtype=np.float64)

    def explain(self, rows: pa.Table, k: int = 2) -> list[list[str]]:
        return [["constant"] for _ in range(rows.num_rows)]

    def to_payload(self) -> dict:
        return {"value": self.value, "feature_columns": list(self._feature_columns)}

    @classmethod
    def from_payload(cls, payload: dict, feature_schema: dict) -> ConstantScorer:
        return cls(float(payload["value"]), tuple(payload.get("feature_columns", [])))


def make_dummy_package(
    out_dir: Path,
    value: float = 0.5,
    feature_columns: tuple[str, ...] = ("secs_since_last_open",),
    serving_policy: dict | None = None,
    contract_config: dict | None = None,
) -> Path:
    """Write a loadable constant-score package (data_kind ``fixture``); returns its zip."""
    meta = {
        "serving_contract": {
            "config": contract_config or {"feature_version": "fv1", "fixture": True}
        },
        "serving_policy": dict(serving_policy or DEFAULT_SERVING_POLICY),
        "data": {"data_kind": "fixture"},
        "training": {"note": "dummy constant scorer; not a trained model"},
    }
    return write_package(
        ConstantScorer(value, feature_columns),
        default_feature_schema(),
        None,
        fixture_rows(),
        meta,
        out_dir,
    )
