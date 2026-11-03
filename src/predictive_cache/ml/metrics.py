"""Offline evaluation metrics (docs/ml/spec.md §7.1-§7.3).

All metrics are micro-aggregated over ``universe`` ticks with ``status = ok``.
Recall uses the universe denominator ``n_positive_files`` (positives eligible
and known at *t*, candidates or not), so a narrow candidate set cannot inflate it.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import Literal

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

MIB = 2**20
RANGE_CAP = 16 * MIB
DEFAULT_BUDGETS_SPEC = (
    "top_10",
    "top_25",
    "top_50",
    "bytes_64MiB",
    "bytes_256MiB",
    "bytes_1024MiB",
)
PRIMARY_METRIC = "recall@bytes_256MiB"

SNAPSHOT_COLUMNS = ("snapshot_id", "volume_id", "file_id", "label", "file_size")
UNIVERSE_COLUMNS = (
    "snapshot_id",
    "t",
    "split",
    "status",
    "n_positive_files",
    "n_positive_in_candidates",
    "n_positive_unknown_at_t",
)
_BUDGET_RE = re.compile(r"^(top)_(\d+)$|^(bytes)_(\d+)MiB$")


@dataclass(frozen=True)
class Budget:
    kind: Literal["top_k", "bytes"]
    value: int  # K rows, or bytes

    @property
    def name(self) -> str:
        if self.kind == "top_k":
            return f"top_{self.value}"
        if self.value % MIB:
            raise ValueError("byte budgets must be whole MiB")
        return f"bytes_{self.value // MIB}MiB"

    @classmethod
    def parse(cls, text: str) -> Budget:
        match = _BUDGET_RE.match(text)
        if not match:
            raise ValueError(f"budget must look like top_25 or bytes_256MiB, got {text!r}")
        if match.group(1):
            value = int(match.group(2))
            kind = "top_k"
        else:
            value = int(match.group(4)) * MIB
            kind = "bytes"
        if value <= 0:
            raise ValueError("budget must be positive")
        return cls(kind, value)


def default_budgets() -> list[Budget]:
    return [Budget.parse(text) for text in DEFAULT_BUDGETS_SPEC]


def _require(table: pa.Table, columns: Iterable[str], what: str) -> None:
    missing = [c for c in columns if c not in table.column_names]
    if missing:
        raise ValueError(f"{what} is missing columns: {', '.join(missing)}")


def costs(file_size: np.ndarray, range_cap: int = RANGE_CAP) -> np.ndarray:
    """Warm cost per row: ``min(file_size, range_cap)``; null/NaN size costs ``range_cap``."""
    size = np.asarray(file_size, dtype=np.float64)
    return np.where(np.isnan(size), float(range_cap), np.minimum(size, float(range_cap)))


def _order(scores: np.ndarray, file_ids: Sequence[str]) -> list[int]:
    """Indices sorted by (score desc, file_id asc); NaN scores rank last."""
    keyed = np.where(np.isnan(scores), -np.inf, scores)
    return sorted(range(len(keyed)), key=lambda i: (-keyed[i], file_ids[i]))


def select(
    rows: pa.Table,
    scores: np.ndarray,
    budget: Budget,
    range_cap: int = RANGE_CAP,
    min_score: float = float("-inf"),
) -> np.ndarray:
    """Boolean mask of the rows one tick would select under ``budget``.

    Rows with ``score < min_score`` (or a NaN score while a threshold is set)
    are never selected. Byte budgets skip a row that does not fit and continue.
    """
    _require(rows, ("file_id", "file_size"), "rows")
    scores = np.asarray(scores, dtype=np.float64)
    if scores.shape != (rows.num_rows,):
        raise ValueError("scores must have one value per row")
    file_ids = rows.column("file_id").to_pylist()
    cost = costs(
        pc.cast(rows.column("file_size"), pa.float64()).to_numpy(zero_copy_only=False), range_cap
    )
    if min_score == float("-inf"):
        allowed = np.ones(rows.num_rows, dtype=bool)
    else:
        allowed = scores >= min_score  # NaN compares False, so it is never selected
    mask = np.zeros(rows.num_rows, dtype=bool)
    if budget.kind == "top_k":
        taken = 0
        for i in _order(scores, file_ids):
            if taken >= budget.value:
                break
            if allowed[i]:
                mask[i] = True
                taken += 1
        return mask
    spent = 0.0
    for i in _order(scores, file_ids):
        if allowed[i] and spent + cost[i] <= budget.value:
            mask[i] = True
            spent += cost[i]
    return mask


def average_precision(scores: np.ndarray, labels: np.ndarray) -> float | None:
    """Step-wise average precision; tied scores share one threshold. None without positives."""
    scores = np.where(np.isnan(scores), -np.inf, np.asarray(scores, dtype=np.float64))
    labels = np.asarray(labels, dtype=np.int64)
    n_pos = int(labels.sum())
    if n_pos == 0:
        return None
    order = np.argsort(-scores, kind="mergesort")
    s, y = scores[order], labels[order]
    last_of_group = np.r_[s[1:] != s[:-1], True]
    tp = np.cumsum(y)[last_of_group]
    seen = (np.arange(len(y)) + 1)[last_of_group]
    precision = tp / seen
    recall = tp / n_pos
    return float(np.sum(np.diff(np.r_[0.0, recall]) * precision))


def reliability(
    scores: np.ndarray, labels: np.ndarray, bins: int = 10
) -> tuple[list[dict], float] | None:
    """Equal-count reliability bins and ECE; None unless every score is in [0, 1]."""
    scores = np.asarray(scores, dtype=np.float64)
    labels = np.asarray(labels, dtype=np.float64)
    if scores.size == 0 or np.isnan(scores).any() or scores.min() < 0 or scores.max() > 1:
        return None
    order = np.argsort(scores, kind="mergesort")
    rows, ece = [], 0.0
    for chunk in np.array_split(order, min(bins, scores.size)):
        mean_score = float(scores[chunk].mean())
        rate = float(labels[chunk].mean())
        rows.append({"n": int(chunk.size), "mean_score": mean_score, "positive_rate": rate})
        ece += chunk.size / scores.size * abs(mean_score - rate)
    return rows, float(ece)


def _ratio(numerator: float, denominator: float) -> float | None:
    return float(numerator) / float(denominator) if denominator else None


def _volume_of(snapshot_id: str) -> str:
    return snapshot_id.split("@", 1)[0]


def _infer_split(snapshots: pa.Table, ok_ticks: pa.Table) -> str | None:
    """The split to evaluate when the caller passed none.

    Universe tables span every split, so silently mixing them would inflate
    the recall denominator. Use the snapshots' single split if they have one;
    otherwise require all ok ticks to share one split.
    """
    if "split" in snapshots.column_names and snapshots.num_rows:
        values = set(snapshots.column("split").to_pylist())
        if len(values) == 1:
            return values.pop()
        raise ValueError("snapshots span several splits; pass split=")
    values = set(ok_ticks.column("split").to_pylist())
    if len(values) > 1:
        raise ValueError("universe ok ticks span several splits; pass split=")
    return values.pop() if values else None


def evaluate(
    snapshots: pa.Table,
    universe: pa.Table,
    scores: np.ndarray,
    budgets: Sequence[Budget],
    *,
    split: str | None = None,
    range_cap: int = RANGE_CAP,
    min_score: float = float("-inf"),
) -> dict:
    """Metrics for one model's ``scores`` (aligned with ``snapshots`` rows).

    Uses universe ticks with ``status = ok`` (and ``split`` if given). Ticks
    without candidate rows still count in the denominators. Returns
    ``{"overall", "per_volume", "per_tick", "n_ticks", "n_rows"}``.
    """
    _require(snapshots, SNAPSHOT_COLUMNS, "snapshots")
    _require(universe, UNIVERSE_COLUMNS, "universe")
    scores = np.asarray(scores, dtype=np.float64)
    if scores.shape != (snapshots.num_rows,):
        raise ValueError("scores must have one value per snapshot row")

    keep = pc.equal(universe.column("status"), "ok")
    if split is None:
        split = _infer_split(snapshots, universe.filter(keep))
    if split is not None:
        keep = pc.and_(keep, pc.equal(universe.column("split"), split))
    ticks = universe.filter(keep).sort_by("snapshot_id")
    tick_ids = ticks.column("snapshot_id").to_pylist()
    tick_set = set(tick_ids)

    row_ids = snapshots.column("snapshot_id").to_pylist()
    stray = sorted({sid for sid in row_ids if sid not in tick_set})
    if stray:
        raise ValueError(f"snapshot rows reference ticks not ok in universe: {stray[:3]}")
    by_tick: dict[str, list[int]] = {}
    for index, sid in enumerate(row_ids):
        by_tick.setdefault(sid, []).append(index)

    labels = pc.cast(snapshots.column("label"), pa.int64()).to_numpy(zero_copy_only=False)
    cost_all = costs(
        pc.cast(snapshots.column("file_size"), pa.float64()).to_numpy(zero_copy_only=False),
        range_cap,
    )

    per_tick: dict[str, list] = {
        "snapshot_id": tick_ids,
        "t": ticks.column("t").to_pylist(),
        "volume_id": [_volume_of(sid) for sid in tick_ids],
        "n_rows": [],
        "n_positive_files": ticks.column("n_positive_files").to_pylist(),
        "n_positive_in_candidates": ticks.column("n_positive_in_candidates").to_pylist(),
        "n_positive_unknown_at_t": ticks.column("n_positive_unknown_at_t").to_pylist(),
    }
    for b in budgets:
        for field in ("selected", "selected_pos", "cost", "pos_cost"):
            per_tick[f"{field}@{b.name}"] = []

    for sid in tick_ids:
        idx = np.asarray(by_tick.get(sid, []), dtype=np.int64)
        per_tick["n_rows"].append(int(idx.size))
        tick_rows = snapshots.take(pa.array(idx, type=pa.int64()))
        for b in budgets:
            if idx.size:
                mask = select(tick_rows, scores[idx], b, range_cap, min_score)
                y, c = labels[idx][mask], cost_all[idx][mask]
            else:
                y = c = np.zeros(0)
            per_tick[f"selected@{b.name}"].append(int(y.size))
            per_tick[f"selected_pos@{b.name}"].append(int(y.sum()))
            per_tick[f"cost@{b.name}"].append(int(c.sum()))
            per_tick[f"pos_cost@{b.name}"].append(int(c[y == 1].sum()))

    table = pa.table(per_tick)
    in_ticks = np.isin(np.asarray(row_ids, dtype=object), np.asarray(tick_ids, dtype=object))
    overall = _summary(table, budgets)
    overall["pr_auc"] = average_precision(scores[in_ticks], labels[in_ticks])
    per_volume = {}
    for volume in sorted(set(per_tick["volume_id"])):
        part = table.filter(pc.equal(table.column("volume_id"), volume))
        vol_rows = in_ticks & (
            snapshots.column("volume_id").to_numpy(zero_copy_only=False) == volume
        )
        per_volume[volume] = _summary(part, budgets)
        per_volume[volume]["pr_auc"] = average_precision(scores[vol_rows], labels[vol_rows])
    return {
        "n_ticks": table.num_rows,
        "n_rows": int(in_ticks.sum()),
        "overall": overall,
        "per_volume": per_volume,
        "per_tick": table,
    }


def _sum(table: pa.Table, column: str) -> int:
    return int(pc.sum(table.column(column)).as_py() or 0)


def _summary(table: pa.Table, budgets: Sequence[Budget]) -> dict:
    n_pos = _sum(table, "n_positive_files")
    n_unknown = _sum(table, "n_positive_unknown_at_t")
    out: dict = {
        "n_ticks": table.num_rows,
        "n_positive_files": n_pos,
        "n_positive_unknown_at_t": n_unknown,
        "coverage": _ratio(_sum(table, "n_positive_in_candidates"), n_pos),
        "unknown_at_t_share": _ratio(n_unknown, n_pos + n_unknown),
    }
    for b in budgets:
        selected = _sum(table, f"selected@{b.name}")
        selected_pos = _sum(table, f"selected_pos@{b.name}")
        cost = _sum(table, f"cost@{b.name}")
        out[f"recall@{b.name}"] = _ratio(selected_pos, n_pos)
        out[f"precision@{b.name}"] = _ratio(selected_pos, selected)
        out[f"warmed_bytes@{b.name}"] = cost
        out[f"positive_warmed_share@{b.name}"] = _ratio(_sum(table, f"pos_cost@{b.name}"), cost)
    return out


_METRIC_PARTS = {
    "recall": ("selected_pos", "n_positive_files"),
    "precision": ("selected_pos", "selected"),
    "positive_warmed_share": ("pos_cost", "cost"),
}


def micro(per_tick: pa.Table, metric: str) -> float | None:
    """Point estimate of a ratio metric (``recall@…``, ``precision@…``, ``positive_warmed_share@…``)."""
    num, den = _parts(metric)
    return _ratio(_sum(per_tick, num), _sum(per_tick, den))


def _parts(metric: str) -> tuple[str, str]:
    name, _, budget = metric.partition("@")
    if name not in _METRIC_PARTS or not budget:
        raise ValueError(f"unsupported bootstrap metric {metric!r}")
    num, den = _METRIC_PARTS[name]
    return f"{num}@{budget}", den if den == "n_positive_files" else f"{den}@{budget}"


def _per_day(per_tick: pa.Table, column: str, days: list) -> np.ndarray:
    values = np.asarray(per_tick.column(column).to_pylist(), dtype=np.float64)
    index = {day: i for i, day in enumerate(sorted(set(days)))}
    out = np.zeros(len(index))
    np.add.at(out, [index[d] for d in days], values)
    return out


def bootstrap_diff(
    per_tick_a: pa.Table,
    per_tick_b: pa.Table,
    metric: str,
    reps: int = 1000,
    seed: int = 11,
    alpha: float = 0.05,
) -> tuple[float, float]:
    """Day-block bootstrap percentile CI for ``metric(a) − metric(b)``.

    Both tables must cover the same ticks. UTC days of ``t`` are resampled with
    replacement; replicates with a zero denominator are ignored. Returns
    ``(nan, nan)`` if no replicate is defined.
    """
    if per_tick_a.column("snapshot_id").to_pylist() != per_tick_b.column("snapshot_id").to_pylist():
        raise ValueError("per-tick tables must cover the same ticks in the same order")
    num, den = _parts(metric)
    days = [t.date() for t in per_tick_a.column("t").to_pylist()]
    if not days:
        return float("nan"), float("nan")
    a_num, a_den = _per_day(per_tick_a, num, days), _per_day(per_tick_a, den, days)
    b_num, b_den = _per_day(per_tick_b, num, days), _per_day(per_tick_b, den, days)
    rng = np.random.default_rng(seed)
    pick = rng.integers(0, a_num.size, size=(reps, a_num.size))
    with np.errstate(invalid="ignore", divide="ignore"):
        a = a_num[pick].sum(1) / a_den[pick].sum(1)
        b = b_num[pick].sum(1) / b_den[pick].sum(1)
    diff = a - b
    diff = diff[np.isfinite(diff)]
    if diff.size == 0:
        return float("nan"), float("nan")
    lo, hi = np.percentile(diff, [100 * alpha / 2, 100 * (1 - alpha / 2)])
    return float(lo), float(hi)
