"""Fake but schema-valid snapshot directories (data-prep spec §8.4-§8.6 layout).

Lets training/evaluation code run end to end before data prep (#30-#32)
produces real snapshots. Labels follow a planted logistic signal on a few
features, so a trained model has something to learn. The manifest says
``data_kind: fixture``: results on it are plumbing evidence only.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from .dummy import FV1_FEATURES, default_feature_schema
from .package import COACCESS_SCHEMA, contract_sha256

TS = pa.timestamp("us", tz="UTC")
MIB = 2**20


def _planted_logit(f: dict[str, np.ndarray]) -> np.ndarray:
    """The hidden rule: recent co-access and burst activity predict demand."""
    return (
        -4.0
        + 3.0 * np.nan_to_num(f["coacc_max_conf"])
        + 0.8 * np.log1p(np.nan_to_num(f["opens_1h"]))
        + 0.6 * np.log1p(np.nan_to_num(f["dir_opens_15m"]))
        - 0.25 * np.log1p(np.nan_to_num(f["secs_since_last_open"], nan=604_800.0)) / 3
    )


def _features(rng: np.random.Generator, n: int) -> dict[str, np.ndarray]:
    out: dict[str, np.ndarray] = {}
    for name, dtype, _ in FV1_FEATURES:
        if name.startswith("secs_") or name == "file_age_s":
            values = rng.exponential(20_000, n)
        elif name == "file_size":
            values = np.exp(rng.normal(np.log(4 * MIB), 1.5, n)).round()
        elif name == "mean_bytes_per_open_7d":
            values = np.exp(rng.normal(np.log(2 * MIB), 1.2, n))
        elif name in ("coacc_max_conf", "coacc_sum_conf", "full_read_share_7d"):
            values = rng.beta(0.6, 2.5, n)
        elif name == "coacc_max_lift":
            values = rng.exponential(2.0, n)
        elif name == "growth_15m_vs_1h":
            values = rng.exponential(0.5, n)
        else:  # counts
            values = rng.poisson(rng.choice([0.2, 2.0, 8.0], n)).astype(float)
        values = values.astype(float)
        values[rng.random(n) < 0.05] = np.nan  # some nulls in every feature
        out[name] = values
    return out


def make_fake_snapshots(
    out: Path,
    *,
    seed: int = 7,
    days: int = 10,
    ticks_per_day: int = 24,
    candidates_per_tick: int = 60,
    volumes: tuple[str, ...] = ("vol-0001", "vol-0002"),
    split_days: tuple[int, int] = (6, 8),
) -> Path:
    """Write a snapshot dir: days [0, split_days[0]) train, then val, then test."""
    out = Path(out)
    if out.exists() and any(out.iterdir()):
        raise FileExistsError(f"{out} is not empty")
    rng = np.random.default_rng(seed)
    start = datetime(2026, 1, 5, tzinfo=UTC)
    step = timedelta(days=1) / ticks_per_day
    dtype_of = {name: dtype for name, dtype, _ in FV1_FEATURES}
    split_of = lambda d: "train" if d < split_days[0] else "val" if d < split_days[1] else "test"  # noqa: E731

    rows: dict[tuple[str, str], list[dict]] = {}
    universe: dict[str, list[dict]] = {}
    counts = {"train": [0, 0], "val": [0, 0], "test": [0, 0]}
    for volume in volumes:
        for day in range(days):
            split = split_of(day)
            for k in range(ticks_per_day):
                t = start + timedelta(days=day) + k * step
                sid = f"{volume}@{t.isoformat().replace('+00:00', 'Z')}"
                n = int(rng.integers(candidates_per_tick // 2, candidates_per_tick + 1))
                feats = _features(rng, n)
                label = (rng.random(n) < 1 / (1 + np.exp(-_planted_logit(feats)))).astype(int)
                for i in range(n):
                    row = {
                        "snapshot_id": sid,
                        "t": t,
                        "split": split,
                        "volume_id": volume,
                        "file_id": f"file-{int(rng.integers(0, 5000)):06d}-{i}",
                        "candidate_sources": ["recent"],
                        "coaccess_fit_end": None,
                    }
                    for name, values in feats.items():
                        v = values[i]
                        row[name] = (
                            None
                            if np.isnan(v)
                            else (int(v) if dtype_of[name] == "int64" else float(v))
                        )
                    row["label"] = int(label[i])
                    row["label_first_time"] = t + timedelta(seconds=60) if label[i] else None
                    row["label_n_opens"] = int(label[i])
                    size = row["file_size"] or MIB
                    row["label_distinct_bytes"] = int(min(size, 16 * MIB)) if label[i] else 0
                    row["label_max_file_size"] = int(size) if label[i] else 0
                    row["label_bytes_exact"] = True
                    row["sample_weight"] = 1.0
                    rows.setdefault((split, volume), []).append(row)
                pos = int(label.sum())
                outside = int(rng.integers(0, 3))
                universe.setdefault(volume, []).append(
                    {
                        "snapshot_id": sid,
                        "t": t,
                        "split": split,
                        "status": "ok",
                        "n_candidates": n,
                        "n_ineligible": 0,
                        "n_positive_files": pos + outside,
                        "n_positive_in_candidates": pos,
                        "positive_bytes": 0,
                        "positive_bytes_in_candidates": 0,
                        "n_positive_unknown_at_t": int(rng.integers(0, 2)),
                        "n_positive_ineligible": 0,
                        "n_unknown_labels": 0,
                    }
                )
                counts[split][0] += n
                counts[split][1] += pos

    schema = default_feature_schema()
    fields = [
        ("snapshot_id", pa.string()),
        ("t", TS),
        ("split", pa.string()),
        ("volume_id", pa.string()),
        ("file_id", pa.string()),
        ("candidate_sources", pa.list_(pa.string())),
        ("coaccess_fit_end", TS),
        *[
            (name, pa.int64() if dtype == "int64" else pa.float64())
            for name, dtype, _ in FV1_FEATURES
        ],
        ("label", pa.int8()),
        ("label_first_time", TS),
        ("label_n_opens", pa.int32()),
        ("label_distinct_bytes", pa.int64()),
        ("label_max_file_size", pa.int64()),
        ("label_bytes_exact", pa.bool_()),
        ("sample_weight", pa.float64()),
    ]
    snapshot_schema = pa.schema(fields)
    for (split, volume), part in rows.items():
        path = out / "snapshots" / f"split={split}" / f"volume_id={volume}" / "part-00000.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.Table.from_pylist(part, schema=snapshot_schema), path)
    for volume, ticks in universe.items():
        path = out / "universe" / f"volume_id={volume}" / "part-00000.parquet"
        path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(pa.Table.from_pylist(ticks), path)

    (out / "artifacts").mkdir(parents=True, exist_ok=True)
    pq.write_table(COACCESS_SCHEMA.empty_table(), out / "artifacts" / "coaccess.parquet")
    (out / "feature_schema.json").write_text(json.dumps(schema, indent=2) + "\n")
    contract_config = {"feature_version": "fv1", "fixture": True, "seed": seed}
    manifest = {
        "manifest_version": 1,
        "dataset_id": f"fixture-snapshots-s{seed}",
        "data_kind": "fixture",
        "stage": "snapshots",
        "parent": {"dataset_id": f"fixture-clean-s{seed}", "manifest_sha256": None},
        "feature_version": "fv1",
        "labels": {"target": "demand", "lead_s": 0, "horizon_s": 300},
        "splits": {
            "val_start": (start + timedelta(days=split_days[0])).isoformat().replace("+00:00", "Z"),
            "test_start": (start + timedelta(days=split_days[1]))
            .isoformat()
            .replace("+00:00", "Z"),
        },
        "serving_contract": {"config": contract_config, "sha256": contract_sha256(contract_config)},
        "counts": {s: {"rows": c[0], "positives": c[1]} for s, c in counts.items()},
        "note": "fake snapshots with a planted signal; plumbing evidence only",
    }
    (out / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    return out
