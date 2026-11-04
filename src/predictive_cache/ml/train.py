"""``pcache-train`` core: fit on train, select on val, write model packages (spec §6).

Reads only the ``train`` and ``val`` splits of a data-prep snapshot directory.
For every requested model it chooses parameters (logreg ``C``) and the
serving-policy ``min_score`` on val with the #57 primary metric, writes a
model package, a val report and a training manifest, and reports to a
Tracker (no-op without ClearML).
"""

from __future__ import annotations

import hashlib
import json
import subprocess
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path

import numpy as np

from ..tracking import NullTracker, Tracker
from .baselines import HotsetScorer
from .gbdt import GbdtScorer
from .hashing import canonical_json
from .logreg import LogRegScorer, training_features
from .metrics import PRIMARY_METRIC, Budget, default_budgets, evaluate
from .package import RANGE_CAP_MAX, write_package
from .scorer import scorer_class
from .snapshots import (
    coaccess_path,
    load_split,
    load_universe,
    manifest_sha256,
    read_feature_schema,
    read_manifest,
    serving_contract,
    verify_read_demand_manifest,
)

MIB = 2**20
ALL_MODELS = (
    "logreg-v1",
    "baseline-mru",
    "baseline-frequency",
    "baseline-hotset",
    "baseline-coaccess",
)


@dataclass
class TrainConfig:
    models: list[str] = field(default_factory=lambda: list(ALL_MODELS))
    C_grid: list[float] = field(default_factory=lambda: [0.01, 0.1, 1.0, 10.0])
    max_iter: int = 2000
    hotset_top_n: int = 500
    gbdt_num_leaves_grid: list[int] = field(default_factory=lambda: [15, 63])
    gbdt_learning_rate: float = 0.05
    gbdt_rounds: int = 1000
    gbdt_early_stopping: int = 50
    gbdt_min_data_in_leaf: int = 200
    primary: str = PRIMARY_METRIC
    tick_s: int = 60
    recent_skip_s: int = 300
    cooldown_s: int = 1800
    max_items: int = 20
    max_bytes_mib: int = 256
    range_cap_mib: int = 16
    min_score_quantiles: list[float] = field(default_factory=lambda: [0, 0.5, 0.8, 0.9, 0.95])
    min_score_keep: float = 0.95

    @classmethod
    def from_dict(cls, data: dict | None) -> TrainConfig:
        """Accept the spec §11.2 ``train:`` section (nested logreg/hotset/serving_policy)."""
        data = dict(data or {})
        data = dict(data.get("train", data))
        flat: dict = {}
        if "models" in data:
            flat["models"] = list(data["models"])
        logreg = data.get("logreg", {})
        if "C_grid" in logreg:
            flat["C_grid"] = [float(c) for c in logreg["C_grid"]]
        if "max_iter" in logreg:
            flat["max_iter"] = int(logreg["max_iter"])
        if "top_n" in data.get("hotset", {}):
            flat["hotset_top_n"] = int(data["hotset"]["top_n"])
        gbdt = data.get("gbdt", {})
        if "num_leaves_grid" in gbdt:
            flat["gbdt_num_leaves_grid"] = [int(n) for n in gbdt["num_leaves_grid"]]
        for key in ("learning_rate", "rounds", "early_stopping", "min_data_in_leaf"):
            if key in gbdt:
                flat[f"gbdt_{key}"] = type(getattr(cls, f"gbdt_{key}"))(gbdt[key])
        if "primary" in data:
            flat["primary"] = str(data["primary"])
        for key, value in data.get("serving_policy", {}).items():
            if key in cls.__dataclass_fields__:
                flat[key] = value
        unknown = set(data) - {"models", "logreg", "hotset", "gbdt", "primary", "serving_policy"}
        if unknown:
            raise ValueError(f"unknown train config keys: {', '.join(sorted(unknown))}")
        cfg = cls(**flat)
        cfg.validate()
        return cfg

    def validate(self) -> None:
        for model in self.models:
            scorer_class(model)  # raises KeyError for unknown model types
        if not self.C_grid or any(c <= 0 for c in self.C_grid):
            raise ValueError("C_grid must contain positive values")
        if not self.gbdt_num_leaves_grid or any(n < 2 for n in self.gbdt_num_leaves_grid):
            raise ValueError("gbdt_num_leaves_grid must contain values >= 2")
        if not all(0 <= q < 1 for q in self.min_score_quantiles):
            raise ValueError("min_score_quantiles must be in [0, 1)")
        if not 0 < self.min_score_keep <= 1:
            raise ValueError("min_score_keep must be in (0, 1]")
        if self.range_cap_mib * MIB > RANGE_CAP_MAX:
            raise ValueError("range_cap_mib exceeds the Warmer cap")
        _primary_budget(self.primary)

    def sha256(self) -> str:
        return hashlib.sha256(canonical_json(asdict(self)).encode()).hexdigest()


def _primary_budget(metric: str) -> Budget:
    name, _, budget = metric.partition("@")
    if name != "recall" or not budget:
        raise ValueError(f"primary metric must look like recall@<budget>, got {metric!r}")
    return Budget.parse(budget)


def _git_sha() -> str:
    try:
        out = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            check=True,
            cwd=Path(__file__).resolve().parent,
        )
        return out.stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return "unknown"


def _primary_value(val, universe, scores, budget, metric, min_score=float("-inf")):
    result = evaluate(val, universe, scores, [budget], split="val", min_score=min_score)
    return result["overall"][metric]


def _choose_threshold(val, universe, scores, budget, metric, cfg) -> tuple[float | None, float]:
    """Highest val-score quantile keeping >= keep x the unthresholded primary (spec §6.3)."""
    base = _primary_value(val, universe, scores, budget, metric)
    finite = scores[np.isfinite(scores)]
    best: tuple[float | None, float] = (None, 0.0)
    if not base or finite.size == 0:
        return best
    for q in sorted(set(cfg.min_score_quantiles)):
        if q == 0:
            continue
        threshold = float(np.quantile(finite, q))
        if threshold <= float(finite.min()):
            continue  # excludes nothing: equivalent to no threshold
        value = _primary_value(val, universe, scores, budget, metric, threshold)
        if value is not None and value >= cfg.min_score_keep * base:
            best = (threshold, q)
    return best


def _build(model_type, features, train, val, universe, budget, cfg, tracker):
    """Fit one model on train; for logreg select C on val. Returns (scorer, params)."""
    if model_type == "logreg-v1":
        chosen, chosen_value = None, None
        for index, C in enumerate(sorted(cfg.C_grid)):
            candidate = LogRegScorer(features)
            candidate.fit(train, None, {"C": C, "max_iter": cfg.max_iter})
            value = _primary_value(val, universe, candidate.score(val), budget, cfg.primary)
            tracker.scalar(f"val C sweep ({cfg.primary})", f"C={C:g}", value or 0.0, index)
            if chosen is None or (value or 0.0) > (chosen_value or 0.0):
                chosen, chosen_value = candidate, value
        return chosen, dict(chosen.params)
    scorer = scorer_class(model_type)()
    if isinstance(scorer, GbdtScorer):
        chosen, chosen_value = None, None
        for index, leaves in enumerate(sorted(cfg.gbdt_num_leaves_grid)):
            candidate = type(scorer)(features)
            candidate.fit(
                train,
                val,
                {
                    "num_leaves": leaves,
                    "learning_rate": cfg.gbdt_learning_rate,
                    "rounds": cfg.gbdt_rounds,
                    "early_stopping": cfg.gbdt_early_stopping,
                    "min_data_in_leaf": cfg.gbdt_min_data_in_leaf,
                },
            )
            value = _primary_value(val, universe, candidate.score(val), budget, cfg.primary)
            tracker.scalar(f"val leaves sweep ({cfg.primary})", f"{model_type} leaves={leaves}", value or 0.0, index)
            if chosen is None or (value or 0.0) > (chosen_value or 0.0):
                chosen, chosen_value = candidate, value
        return chosen, {k: v for k, v in chosen.params.items() if k != "importance_gain"}
    if isinstance(scorer, HotsetScorer):
        scorer.fit(train, None, {"top_n": cfg.hotset_top_n})
        return scorer, {"top_n": cfg.hotset_top_n}
    scorer.fit(train, None, {})
    return scorer, {}


def train(
    snapshots_dir: Path, cfg: TrainConfig, out_dir: Path, tracker: Tracker | None = None
) -> dict:
    """Train ``cfg.models``; never reads the test split. Returns a summary dict."""
    tracker = tracker or NullTracker()
    snapshots_dir, out_dir = Path(snapshots_dir), Path(out_dir)
    if out_dir.exists() and any(out_dir.iterdir()):
        raise FileExistsError(f"{out_dir} is not empty")
    cfg.validate()
    manifest = read_manifest(snapshots_dir)
    schema = read_feature_schema(snapshots_dir)
    contract = serving_contract(manifest)
    verify_read_demand_manifest(snapshots_dir, manifest)
    if manifest.get("semantic_view_id") == "read-demand-v1":
        from predictive_cache.semantics.read_demand import SEMANTIC_VIEW_SHA256

        from .package import contract_sha256
        if manifest.get("semantic_view_sha256") != SEMANTIC_VIEW_SHA256:
            raise ValueError("read-demand semantic hash mismatch")
        schema_hash = hashlib.sha256((snapshots_dir / "feature_schema.json").read_bytes()).hexdigest()
        if manifest.get("feature_schema_sha256") != schema_hash:
            raise ValueError("snapshot feature schema hash mismatch")
        if manifest.get("serving_contract_sha256") != contract_sha256(contract["config"]):
            raise ValueError("snapshot serving contract hash mismatch")
    labels = manifest.get("labels", {})
    features = training_features(schema)
    budget = _primary_budget(cfg.primary)
    train_rows = load_split(snapshots_dir, "train")
    val_rows = load_split(snapshots_dir, "val")
    universe = load_universe(snapshots_dir)
    report_budgets = default_budgets()
    if budget.name not in {b.name for b in report_budgets}:
        report_budgets.append(budget)

    data_kind = manifest.get("data_kind", "unknown")
    data = {
        "snapshots_dataset_id": manifest.get("dataset_id"),
        "snapshots_manifest_sha256": manifest_sha256(snapshots_dir),
        "clean_dataset_id": (manifest.get("parent") or {}).get("dataset_id"),
        "data_kind": data_kind,
    }
    git_sha, config_sha = _git_sha(), cfg.sha256()
    packages_dir = out_dir / "packages"
    report: dict = {}
    summary: dict = {"models": {}, "primary": cfg.primary, "data": data}

    for model_type in cfg.models:
        scorer, params = _build(
            model_type, features, train_rows, val_rows, universe, budget, cfg, tracker
        )
        scores = scorer.score(val_rows)
        min_score, quantile = _choose_threshold(
            val_rows, universe, scores, budget, cfg.primary, cfg
        )
        metrics = evaluate(val_rows, universe, scores, report_budgets, split="val")["overall"]
        thresholded = _primary_value(
            val_rows,
            universe,
            scores,
            budget,
            cfg.primary,
            float("-inf") if min_score is None else min_score,
        )
        policy = {
            "target": labels.get("target", "demand"),
            "lead_s": int(labels.get("lead_s", 0)),
            "horizon_s": int(labels.get("horizon_s", 300)),
            "tick_s": cfg.tick_s,
            "min_score": min_score,
            "min_score_quantile": quantile,
            "recent_skip_s": cfg.recent_skip_s,
            "cooldown_s": cfg.cooldown_s,
            "max_items": cfg.max_items,
            "max_bytes": cfg.max_bytes_mib * MIB,
            "range_cap": cfg.range_cap_mib * MIB,
        }
        val_summary = {
            cfg.primary: metrics.get(cfg.primary),
            f"{cfg.primary}@min_score": thresholded,
            "pr_auc": metrics.get("pr_auc"),
            "coverage": metrics.get("coverage"),
        }
        zip_path = write_package(
            scorer,
            schema,
            coaccess_path(snapshots_dir),
            val_rows.slice(0, 50),
            {
                "serving_contract": {"config": contract["config"]},
                "semantic": ({
                    "semantic_view_id": manifest.get("semantic_view_id"),
                } if manifest.get("semantic_view_id") else {}),
                "serving_policy": policy,
                "data": data,
                "training": {
                    "git_sha": git_sha,
                    "config_sha256": config_sha,
                    "params": params,
                    "selected_by": f"val:{cfg.primary}",
                    "val_metrics": val_summary,
                },
            },
            packages_dir,
        )
        package_json = json.loads((zip_path.with_suffix("") / "package.json").read_text())
        if package_json["serving_contract"]["sha256"] != contract.get("sha256"):
            raise ValueError("serving contract hash differs from the snapshots manifest")
        model_id = tracker.register_model(
            zip_path,
            model_type,
            ["candidate", f"data:{data_kind}"]
            + ([manifest["semantic_view_id"]] if manifest.get("semantic_view_id") else []),
            {"package_id": package_json["package_id"]},
        )
        for name, value in metrics.items():
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                tracker.scalar(f"val {name}", model_type, value)
        report[model_type] = {"params": params, "serving_policy": policy, "val": metrics}
        summary["models"][model_type] = {
            "package": str(zip_path),
            "package_id": package_json["package_id"],
            "clearml_model_id": model_id,
            "params": params,
            "min_score": min_score,
            cfg.primary: metrics.get(cfg.primary),
        }

    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "val_report.json").write_text(json.dumps(report, indent=2, default=str) + "\n")
    (out_dir / "val_report.md").write_text(_markdown(report, cfg, data))
    train_manifest = {
        "manifest_version": 1,
        "stage": "train",
        "parent": {
            "dataset_id": data["snapshots_dataset_id"],
            "manifest_sha256": data["snapshots_manifest_sha256"],
        },
        "data_kind": data_kind,
        "git_sha": git_sha,
        "config": asdict(cfg),
        "config_sha256": config_sha,
        "clearml_task_id": tracker.task_id,
        "packages": {m: s["package_id"] for m, s in summary["models"].items()},
        "created_at": datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z"),
    }
    (out_dir / "train_manifest.json").write_text(json.dumps(train_manifest, indent=2) + "\n")
    tracker.artifact("val_report", out_dir / "val_report.json")
    tracker.table(
        "val summary",
        [
            {"model": m, **{k: v for k, v in s.items() if k != "package"}}
            for m, s in summary["models"].items()
        ],
    )
    return summary


def _fmt(value) -> str:
    return "—" if value is None else f"{value:.4f}" if isinstance(value, float) else str(value)


def _markdown(report: dict, cfg: TrainConfig, data: dict) -> str:
    kind = data.get("data_kind")
    lines = [
        "# Validation report",
        "",
        f"Snapshots `{data.get('snapshots_dataset_id')}` (data_kind: **{kind}**).",
    ]
    if kind != "real":
        lines.append(
            "Synthetic/fixture data: plumbing and planted-pattern evidence only, "
            "never model quality."
        )
    cols = [cfg.primary, "precision@" + cfg.primary.split("@")[1], "coverage", "pr_auc"]
    lines += [
        "",
        "| model | " + " | ".join(cols) + " | min_score | params |",
        "|---" * (len(cols) + 3) + "|",
    ]
    for model, entry in report.items():
        val = entry["val"]
        lines.append(
            f"| {model} | "
            + " | ".join(_fmt(val.get(c)) for c in cols)
            + f" | {_fmt(entry['serving_policy']['min_score'])} | {entry['params'] or ''} |"
        )
    return "\n".join(lines) + "\n"


def load_train_config(path: Path | None) -> TrainConfig:
    if path is None:
        return TrainConfig()
    import yaml

    return TrainConfig.from_dict(yaml.safe_load(Path(path).read_text()) or {})
