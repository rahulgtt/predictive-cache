"""Scorer interface shared by baselines, trained models, evaluation and the Controller.

See docs/ml/spec.md §3. Runtime scoring depends only on numpy and pyarrow.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import ClassVar, Protocol, runtime_checkable

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

KEY_COLUMNS: tuple[str, ...] = ("volume_id", "file_id", "t")


class MissingColumnError(ValueError):
    """Rows lack a key or feature column that a scorer requires."""


@runtime_checkable
class Scorer(Protocol):
    model_type: ClassVar[str]

    @property
    def feature_columns(self) -> tuple[str, ...]: ...

    def fit(self, train: pa.Table, val: pa.Table | None, cfg: dict) -> None: ...

    def score(self, rows: pa.Table) -> np.ndarray: ...

    def explain(self, rows: pa.Table, k: int = 2) -> list[list[str]]: ...

    def to_payload(self) -> dict:
        """JSON-serialisable model.json content.

        It is written with sorted keys, so encode any ordering (e.g. feature
        columns and their coefficients) in lists, never in dict key order.
        """
        ...

    @classmethod
    def from_payload(cls, payload: dict, feature_schema: dict) -> Scorer: ...


_REGISTRY: dict[str, type] = {}


def register_scorer(cls: type) -> type:
    """Class decorator: make a scorer loadable by its ``model_type``."""
    model_type = getattr(cls, "model_type", None)
    if not isinstance(model_type, str) or not model_type:
        raise TypeError(f"{cls.__name__} must define a non-empty model_type")
    existing = _REGISTRY.get(model_type)
    if existing is not None and existing is not cls:
        raise ValueError(f"model_type {model_type!r} is already registered")
    _REGISTRY[model_type] = cls
    return cls


def scorer_class(model_type: str) -> type:
    try:
        return _REGISTRY[model_type]
    except KeyError:
        raise KeyError(f"unknown model_type {model_type!r}") from None


def registered_model_types() -> tuple[str, ...]:
    return tuple(sorted(_REGISTRY))


def require_columns(rows: pa.Table, columns: Sequence[str], *, keys: bool = True) -> None:
    needed = (*KEY_COLUMNS, *columns) if keys else tuple(columns)
    missing = [name for name in needed if name not in rows.column_names]
    if missing:
        raise MissingColumnError(f"rows are missing required columns: {', '.join(missing)}")


def feature_matrix(rows: pa.Table, columns: Sequence[str]) -> pa.Table:
    """Return only ``columns``, in that order; extra columns are ignored."""
    require_columns(rows, columns)
    return rows.select(list(columns))


def float_matrix(rows: pa.Table, columns: Sequence[str]) -> np.ndarray:
    """Feature values as a float64 (n_rows, n_columns) array; nulls become NaN."""
    require_columns(rows, columns)
    out = np.empty((rows.num_rows, len(columns)), dtype=np.float64)
    for index, name in enumerate(columns):
        column = pc.cast(rows.column(name), pa.float64())
        out[:, index] = column.to_numpy(zero_copy_only=False)
    return out
