"""Baseline scorers (docs/ml/spec.md §4).

A trained model must beat these at equal budgets. They are packaged like any
model, so evaluation and the Controller treat them the same way.
``baseline-frequency`` is also the Controller's built-in fallback (§8.6).
Ties are broken downstream by ``file_id`` (metrics.select / planner).
"""

from __future__ import annotations

import json
import re
import subprocess
from pathlib import Path
from typing import ClassVar

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

from .hashing import file_sha256
from .package import DEFAULT_SERVING_POLICY, write_package
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
class ReadDemandFrequencyScorer(_FormulaBaseline):
    """Narrow READ-demand fallback: score is the complete 24-hour READ count."""

    model_type = "baseline-read-frequency"
    columns = ("read_count_24h",)

    def _formula(self, x: np.ndarray) -> np.ndarray:
        if np.isnan(x[:, 0]).any():
            raise ValueError("read_count_24h requires complete coverage")
        return x[:, 0]


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


BASELINES: tuple[type, ...] = (
    MruScorer,
    FrequencyScorer,
    HotsetScorer,
    CoaccessScorer,
    ReadDemandFrequencyScorer,
)


def repository_revision() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            cwd=Path(__file__).resolve().parents[3],
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except (OSError, subprocess.CalledProcessError):
        return "unknown"


def write_read_demand_frequency_package(
    snapshot_root: Path,
    out_dir: Path,
    *,
    repository_sha: str | None = None,
    repository_state: str = "clean",
    source_patch_sha256: str | None = None,
    min_score: float | None = None,
    max_items: int = 20,
    max_bytes: int = 256 * 2**20,
    range_cap: int = 16 * 2**20,
) -> Path:
    """Package the explicit non-trained synthetic READ-frequency fallback."""
    from .read_demand import READ_DEMAND_SERVING_CONTRACT, feature_schema
    from .snapshots import load_narrow_read_demand_snapshot

    snapshot_root = Path(snapshot_root)
    manifest, rows = load_narrow_read_demand_snapshot(snapshot_root)
    if manifest.get("data_kind") != "synthetic":
        raise ValueError("baseline-read-frequency requires synthetic snapshot data")
    repository_sha = repository_sha or repository_revision()
    if not isinstance(repository_sha, str) or not re.fullmatch(
        r"(?:[0-9a-f]{40}|[0-9a-f]{64})", repository_sha
    ):
        raise ValueError("repository revision must be a canonical Git object ID")
    if repository_state not in {"clean", "patched"}:
        raise ValueError("repository state must be clean or patched")
    if repository_state == "patched":
        if not isinstance(source_patch_sha256, str) or not re.fullmatch(
            r"[0-9a-f]{64}", source_patch_sha256
        ):
            raise ValueError(
                "patched repository state requires a canonical source patch SHA-256"
            )
    elif source_patch_sha256 is not None:
        raise ValueError("clean repository state cannot declare a source patch SHA-256")
    policy = {
        **DEFAULT_SERVING_POLICY,
        "min_score": min_score,
        "max_items": max_items,
        "max_bytes": max_bytes,
        "range_cap": range_cap,
    }
    data = {
        "data_kind": "synthetic",
        "source_dataset_id": manifest["source"]["dataset_id"],
        "source_revision": manifest["source"]["revision"],
        "source_config_sha256": manifest["source"]["config_sha256"],
        "source_manifest_sha256": manifest["source"]["manifest_sha256"],
        "clean_dataset_id": manifest["clean"]["dataset_id"],
        "clean_manifest_sha256": manifest["clean"]["manifest_sha256"],
        "clean_table_sha256": manifest["clean"]["table_sha256"],
        "snapshots_dataset_id": manifest["dataset_id"],
        "snapshots_manifest_sha256": file_sha256(snapshot_root / "manifest.json"),
        "snapshot_table_sha256": {
            entry["path"]: entry["sha256"]
            for entry in manifest["files"]
        },
        "candidate_profile_id": manifest["candidate_profile_id"],
        "candidate_profile_sha256": manifest["candidate_profile_sha256"],
        "snapshot_serving_contract_sha256": manifest[
            "snapshot_serving_contract_sha256"
        ],
    }
    package = write_package(
        ReadDemandFrequencyScorer(),
        feature_schema(),
        None,
        rows,
        {
            "serving_contract": {"config": READ_DEMAND_SERVING_CONTRACT},
            "semantic": {"semantic_view_id": manifest["semantic_view_id"]},
            "serving_policy": policy,
            "data_kind": "synthetic",
            "trained": False,
            "data": data,
            "training": {
                "trained": False,
                "data_kind": "synthetic",
                "repository_revision": repository_sha,
                "repository_state": repository_state,
                "source_patch_sha256": source_patch_sha256,
                "method": "deterministic_read_count_24h",
            },
        },
        Path(out_dir),
    )
    package_manifest = json.loads(
        (package.with_suffix("") / "package.json").read_text()
    )
    if package_manifest.get("trained") is not False:
        raise ValueError("baseline package did not preserve trained=false")
    return package
