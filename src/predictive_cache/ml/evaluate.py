"""``pcache-eval``: freeze, then score frozen packages once on test (spec §7.4-§7.6).

``freeze`` records the exact packages, the evaluation config and a binding to
the test data (hashes of the manifest, every test-split and universe file,
split boundaries, label config). ``run --split test`` verifies all of it
**before** any test row is read and refuses a second run of the same freeze
without a recorded reason. Lab-grade enforcement, not access control.
"""

from __future__ import annotations

import hashlib
import json
import subprocess
import zipfile
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path

import numpy as np

from ..tracking import NullTracker, Tracker
from .hashing import canonical_json, file_sha256
from .metrics import (
    DEFAULT_BUDGETS_SPEC,
    MIB,
    PRIMARY_METRIC,
    Budget,
    bootstrap_diff,
    evaluate,
    micro,
    reliability,
)
from .package import LoadedPackage, PackageError, load_package
from .snapshots import (
    load_split,
    load_universe,
    manifest_sha256,
    read_manifest,
    runtime_available_features,
    serving_contract,
    verify_read_demand_manifest,
)

FREEZE_VERSION = 1
LEDGER = "test_runs.jsonl"
CLEARML_PREFIX = "clearml://"


class EvalError(ValueError):
    """Evaluation refused; ``reason`` is a stable code."""

    def __init__(self, reason: str, detail: str = "") -> None:
        super().__init__(f"{reason}: {detail}" if detail else reason)
        self.reason = reason


@dataclass
class EvalConfig:
    budgets: list[str] = field(default_factory=lambda: list(DEFAULT_BUDGETS_SPEC))
    range_cap_mib: int = 16
    primary: str = PRIMARY_METRIC
    bootstrap_reps: int = 1000
    bootstrap_seed: int = 11

    @classmethod
    def from_dict(cls, data: dict | None) -> EvalConfig:
        """Accept the spec §11.2 ``eval:`` section."""
        data = dict(data or {})
        data = dict(data.get("eval", data))
        cfg = cls()
        budgets = data.pop("budgets", None)
        if budgets is not None:
            cfg.budgets = [f"top_{k}" for k in budgets.get("top_k", [])] + [
                f"bytes_{m}MiB" for m in budgets.get("bytes_mib", [])
            ]
        if "range_cap_mib" in data:
            cfg.range_cap_mib = int(data.pop("range_cap_mib"))
        if "primary" in data:
            cfg.primary = str(data.pop("primary"))
        boot = data.pop("bootstrap", {})
        cfg.bootstrap_reps = int(boot.get("reps", cfg.bootstrap_reps))
        cfg.bootstrap_seed = int(boot.get("seed", cfg.bootstrap_seed))
        if data:
            raise ValueError(f"unknown eval config keys: {', '.join(sorted(data))}")
        cfg.validate()
        return cfg

    def validate(self) -> None:
        budgets = self.budget_objects()
        name, _, budget = self.primary.partition("@")
        if name != "recall" or budget not in {b.name for b in budgets}:
            raise ValueError("primary must be recall@<one of the budgets>")
        if self.range_cap_mib <= 0 or self.bootstrap_reps <= 0:
            raise ValueError("range_cap_mib and bootstrap_reps must be positive")

    def budget_objects(self) -> list[Budget]:
        return [Budget.parse(b) for b in self.budgets]

    def sha256(self) -> str:
        return hashlib.sha256(canonical_json(asdict(self)).encode()).hexdigest()


def load_eval_config(path: Path | None) -> EvalConfig:
    if path is None:
        return EvalConfig()
    import yaml

    return EvalConfig.from_dict(yaml.safe_load(Path(path).read_text()) or {})


# --- packages ---------------------------------------------------------------------------


def resolve_ref(ref: str, tracker: Tracker) -> Path:
    """A local package path, or ``clearml://<model_id>`` fetched through the tracker."""
    if ref.startswith(CLEARML_PREFIX):
        return tracker.get_model_copy(ref[len(CLEARML_PREFIX) :])
    path = Path(ref)
    if not path.exists():
        raise EvalError("package_not_found", ref)
    return path


def package_json_sha256(path: Path) -> str:
    """Digest of package.json; with the loader's file checks it pins the whole package."""
    try:
        if path.is_dir():
            return file_sha256(path / "package.json")
        with zipfile.ZipFile(path) as archive:
            return hashlib.sha256(archive.read("package.json")).hexdigest()
    except (OSError, KeyError, zipfile.BadZipFile) as exc:
        raise EvalError("package_invalid", f"{path}: {exc}") from exc


# --- data binding -------------------------------------------------------------------------


def data_binding(snapshots_dir: Path) -> dict:
    """Hashes of everything a test run reads, plus boundaries and label config."""
    root = Path(snapshots_dir)
    manifest = read_manifest(root)
    files = {}
    for base in (root / "snapshots" / "split=test", root / "universe"):
        for path in sorted(p for p in base.rglob("*") if p.is_file()):
            files[path.relative_to(root).as_posix()] = file_sha256(path)
    if not any(k.startswith("snapshots/split=test/") for k in files):
        raise EvalError("no_test_split", str(root))
    return {
        "snapshots_dataset_id": manifest.get("dataset_id"),
        "manifest_sha256": manifest_sha256(root),
        "files": files,
        "splits": manifest.get("splits"),
        "labels": manifest.get("labels"),
    }


def compute_freeze_id(record: dict) -> str:
    core = {k: v for k, v in record.items() if k != "freeze_id"}
    return hashlib.sha256(canonical_json(core).encode()).hexdigest()[:16]


def _git_sha() -> str:
    try:
        return subprocess.run(
            ["git", "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            check=True,
            cwd=Path(__file__).resolve().parent,
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return "unknown"


def _load_all(
    refs: list[str], snapshots_dir: Path, tracker: Tracker
) -> list[tuple[str, Path, LoadedPackage]]:
    manifest = read_manifest(snapshots_dir)
    verify_read_demand_manifest(snapshots_dir, manifest)
    contract = serving_contract(manifest).get("sha256")
    version = manifest.get("feature_version", "fv1")
    out = []
    for ref in refs:
        path = resolve_ref(ref, tracker)
        try:
            semantic_id = manifest.get("semantic_view_id")
            available = runtime_available_features(manifest) if semantic_id else None
            out.append((ref, path, load_package(path, version, contract, expected_semantic_view_id=semantic_id, expected_semantic_view_sha256=manifest.get("semantic_view_sha256"), expected_feature_schema_sha256=manifest.get("feature_schema_sha256"), available_feature_names=available)))
        except PackageError as exc:
            _close(out)
            raise EvalError("package_invalid", f"{ref}: {exc}") from exc
    return out


def _close(loaded: list[tuple[str, Path, LoadedPackage]]) -> None:
    for _, _, pkg in loaded:
        pkg.close()


def _is_baseline(pkg: LoadedPackage) -> bool:
    return pkg.model_type.startswith("baseline-")


# --- freeze -------------------------------------------------------------------------------


def freeze(
    snapshots_dir: Path, refs: list[str], cfg: EvalConfig, out: Path, tracker: Tracker | None = None
) -> dict:
    """Write ``freeze.json`` for ``refs``; the best baseline is chosen on val."""
    tracker = tracker or NullTracker()
    cfg.validate()
    if len(set(refs)) != len(refs) or not refs:
        raise EvalError("bad_package_set", "packages must be non-empty and unique")
    budgets = cfg.budget_objects()
    primary_budget = Budget.parse(cfg.primary.split("@")[1])
    loaded = _load_all(refs, snapshots_dir, tracker)
    try:
        if not any(_is_baseline(pkg) for _, _, pkg in loaded):
            raise EvalError("no_baseline", "freeze needs at least one baseline-* package")
        val = load_split(snapshots_dir, "val")
        universe = load_universe(snapshots_dir)
        packages, val_primary = [], {}
        for ref, path, pkg in loaded:
            value = evaluate(
                val,
                universe,
                pkg.score(val),
                [primary_budget],
                split="val",
                range_cap=cfg.range_cap_mib * MIB,
            )["overall"][cfg.primary]
            val_primary[pkg.package_id] = value
            packages.append(
                {
                    "ref": ref,
                    "package_id": pkg.package_id,
                    "model_type": pkg.model_type,
                    "model_version": pkg.model_version,
                    "package_json_sha256": package_json_sha256(path),
                    "serving_policy": pkg.serving_policy,
                    "contract_sha256": pkg.serving_contract_sha256,
                }
            )
        baselines = [p for p in packages if p["model_type"].startswith("baseline-")]
        best = max(baselines, key=lambda p: (val_primary[p["package_id"]] or 0.0, p["model_type"]))
        manifest = read_manifest(snapshots_dir)
        record = {
            "freeze_version": FREEZE_VERSION,
            "packages": packages,
            "best_baseline": {
                "package_id": best["package_id"],
                "model_type": best["model_type"],
                "val_primary": val_primary[best["package_id"]],
            },
            "val_primary": val_primary,
            "primary": cfg.primary,
            "budgets": [b.name for b in budgets],
            "eval_config": asdict(cfg),
            "eval_config_sha256": cfg.sha256(),
            "data_kind": manifest.get("data_kind", "unknown"),
            "data": data_binding(snapshots_dir),
            "git_sha": _git_sha(),
            "created_at": datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z"),
        }
        record["freeze_id"] = compute_freeze_id(record)
    finally:
        _close(loaded)
    out = Path(out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(record, indent=2, sort_keys=True) + "\n")
    tracker.artifact("freeze", out)
    for p in packages:
        if p["ref"].startswith(CLEARML_PREFIX):
            tracker.tag_model(
                p["ref"][len(CLEARML_PREFIX) :], ["frozen", f"freeze:{record['freeze_id']}"]
            )
    return record


def verify_freeze(
    record: dict, snapshots_dir: Path, refs: list[str] | None, cfg: EvalConfig, tracker: Tracker
) -> list[Path]:
    """Every check that must pass before a test row is read; returns package paths."""
    if record.get("freeze_version") != FREEZE_VERSION:
        raise EvalError("bad_freeze", "unknown freeze_version")
    if record.get("freeze_id") != compute_freeze_id(record):
        raise EvalError("freeze_tampered", "freeze_id does not recompute")
    frozen_refs = [p["ref"] for p in record["packages"]]
    if refs is not None and sorted(refs) != sorted(frozen_refs):
        raise EvalError("package_set_mismatch", "test must use exactly the frozen packages")
    if cfg.sha256() != record["eval_config_sha256"]:
        raise EvalError("config_mismatch", "eval config differs from the frozen one")
    paths = []
    for entry in record["packages"]:
        path = resolve_ref(entry["ref"], tracker)
        if package_json_sha256(path) != entry["package_json_sha256"]:
            raise EvalError("package_changed", entry["ref"])
        paths.append(path)
    current = data_binding(snapshots_dir)
    frozen = record["data"]
    if current["manifest_sha256"] != frozen["manifest_sha256"]:
        raise EvalError("data_changed", "snapshots manifest differs")
    if set(current["files"]) != set(frozen["files"]):
        extra = sorted(set(current["files"]) ^ set(frozen["files"]))
        raise EvalError("data_changed", f"test/universe file set differs: {extra[:3]}")
    changed = [k for k, v in current["files"].items() if frozen["files"][k] != v]
    if changed:
        raise EvalError("data_changed", f"file content differs: {changed[:3]}")
    if current["splits"] != frozen["splits"] or current["labels"] != frozen["labels"]:
        raise EvalError("data_changed", "split boundaries or label config differ")
    return paths


# --- run ----------------------------------------------------------------------------------


def _ledger_count(ledger: Path, freeze_id: str) -> int:
    if not ledger.is_file():
        return 0
    return sum(
        1
        for line in ledger.read_text().splitlines()
        if line.strip() and json.loads(line).get("freeze_id") == freeze_id
    )


def run(
    snapshots_dir: Path,
    split: str,
    refs: list[str] | None,
    freeze_path: Path | None,
    cfg: EvalConfig,
    out_dir: Path,
    tracker: Tracker | None = None,
    rerun_reason: str | None = None,
) -> dict:
    """Score packages on ``split``; test requires a verified freeze (spec §7.5)."""
    tracker = tracker or NullTracker()
    out_dir = Path(out_dir)
    if out_dir.exists() and any(out_dir.iterdir()):
        raise FileExistsError(f"{out_dir} is not empty")
    record = json.loads(Path(freeze_path).read_text()) if freeze_path else None
    prior = 0
    if split == "test":
        if record is None:
            raise EvalError("freeze_required", "test runs require --freeze")
        verify_freeze(record, snapshots_dir, refs, cfg, tracker)
        ledger = Path(freeze_path).parent / LEDGER
        prior = max(
            _ledger_count(ledger, record["freeze_id"]),
            tracker.prior_runs(
                "predictive-cache/eval", [f"freeze:{record['freeze_id']}", "split:test"]
            ),
        )
        if prior and not rerun_reason:
            raise EvalError(
                "already_evaluated",
                f"freeze {record['freeze_id']} has {prior} test run(s); "
                "pass --rerun-reason to run again",
            )
        refs = [p["ref"] for p in record["packages"]]
    elif split != "val":
        raise EvalError("bad_split", split)
    elif refs is None:
        if record is None:
            raise EvalError("no_packages", "pass --packages or --freeze")
        refs = [p["ref"] for p in record["packages"]]
    elif record is not None:
        best_ref = next(
            p["ref"]
            for p in record["packages"]
            if p["package_id"] == record["best_baseline"]["package_id"]
        )
        if best_ref not in refs:
            refs = [*refs, best_ref]  # always compare against the frozen best baseline

    budgets = cfg.budget_objects()
    range_cap = cfg.range_cap_mib * MIB
    loaded = _load_all(refs, snapshots_dir, tracker)
    try:
        rows = load_split(snapshots_dir, split, allowed=(split,))
        universe = load_universe(snapshots_dir)
        results, per_tick = {}, {}
        for ref, _, pkg in loaded:
            scores = pkg.score(rows)
            result = evaluate(rows, universe, scores, budgets, split=split, range_cap=range_cap)
            min_score = pkg.serving_policy.get("min_score")
            thresholded = evaluate(
                rows,
                universe,
                scores,
                [Budget.parse(cfg.primary.split("@")[1])],
                split=split,
                range_cap=range_cap,
                min_score=float("-inf") if min_score is None else min_score,
            )["overall"][cfg.primary]
            labels = rows.column("label").to_numpy(zero_copy_only=False)
            rel = reliability(scores, labels)
            per_tick[pkg.package_id] = result["per_tick"]
            results[pkg.package_id] = {
                "ref": ref,
                "model_type": pkg.model_type,
                "model_version": pkg.model_version,
                "overall": result["overall"],
                "per_volume": result["per_volume"],
                f"{cfg.primary}@min_score": thresholded,
                "min_score": min_score,
                "reliability": None if rel is None else {"bins": rel[0], "ece": rel[1]},
                "curve": {b.name: result["overall"][f"recall@{b.name}"] for b in budgets},
            }
    finally:
        _close(loaded)

    if record is not None:
        best_id = record["best_baseline"]["package_id"]
    else:
        baselines = [k for k, v in results.items() if v["model_type"].startswith("baseline-")]
        best_id = (
            max(
                baselines,
                key=lambda k: (results[k]["overall"][cfg.primary] or 0.0, results[k]["model_type"]),
            )
            if baselines
            else None
        )
    for pid, entry in results.items():
        if best_id is None or pid == best_id:
            entry["vs_best_baseline"] = None
            continue
        lo, hi = bootstrap_diff(
            per_tick[pid], per_tick[best_id], cfg.primary, cfg.bootstrap_reps, cfg.bootstrap_seed
        )
        point = (micro(per_tick[pid], cfg.primary) or 0.0) - (
            micro(per_tick[best_id], cfg.primary) or 0.0
        )
        entry["vs_best_baseline"] = {"diff": point, "ci95": [lo, hi]}

    manifest = read_manifest(snapshots_dir)
    data_kind = manifest.get("data_kind", "unknown")
    summary = {
        "split": split,
        "primary": cfg.primary,
        "data_kind": data_kind,
        "snapshots_dataset_id": manifest.get("dataset_id"),
        "freeze_id": record["freeze_id"] if record else None,
        "best_baseline": best_id,
        "test_run_number": prior + 1 if split == "test" else None,
        "rerun_reason": rerun_reason if split == "test" else None,
        "git_sha": _git_sha(),
        "clearml_task_id": tracker.task_id,
        "models": results,
    }
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "metrics.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True, default=str) + "\n"
    )
    (out_dir / "report.md").write_text(_report(summary, cfg))
    for index, b in enumerate(budgets):
        for entry in results.values():
            value = entry["curve"][b.name]
            if value is not None:
                tracker.scalar(f"{split} recall by budget", entry["model_type"], value, index)
    tracker.artifact("metrics", out_dir / "metrics.json")
    tracker.artifact("report", out_dir / "report.md")

    if split == "test":
        digest = file_sha256(out_dir / "metrics.json")
        line = {
            "freeze_id": record["freeze_id"],
            "run": prior + 1,
            "at": datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z"),
            "git_sha": summary["git_sha"],
            "metrics_sha256": digest,
            "rerun_reason": rerun_reason,
            "clearml_task_id": tracker.task_id,
        }
        with (Path(freeze_path).parent / LEDGER).open("a") as ledger:
            ledger.write(json.dumps(line, sort_keys=True) + "\n")
        for entry in record["packages"]:
            if entry["ref"].startswith(CLEARML_PREFIX):
                tracker.tag_model(entry["ref"][len(CLEARML_PREFIX) :], ["evaluated"])
    return summary


def _fmt(value) -> str:
    if value is None:
        return "—"
    return f"{value:.4f}" if isinstance(value, float) else str(value)


def _report(summary: dict, cfg: EvalConfig) -> str:
    primary = cfg.primary
    budget = primary.split("@")[1]
    lines = [f"# Evaluation report: {summary['split']}", ""]
    lines.append(
        f"- Snapshots `{summary['snapshots_dataset_id']}`, data_kind **{summary['data_kind']}**"
    )
    if summary["data_kind"] != "real":
        lines.append(
            "- **Synthetic/fixture data: plumbing / planted-pattern evidence only**, "
            "never real-workload model quality or performance."
        )
    if summary["freeze_id"]:
        lines.append(f"- Freeze `{summary['freeze_id']}`")
    if summary["split"] == "test":
        lines.append(
            f"- Test run #{summary['test_run_number']} for this freeze"
            + (f"; rerun reason: {summary['rerun_reason']}" if summary["rerun_reason"] else "")
        )
        lines.append("- Once-per-freeze is lab-grade enforcement, not access control.")
    best = summary["models"].get(summary["best_baseline"], {})
    lines.append(f"- Best baseline (chosen on val): {best.get('model_type', '—')}")
    lines += [
        "",
        f"| model | {primary} | precision@{budget} | coverage | pr_auc | "
        f"{primary} at min_score | diff vs best baseline [95% CI] |",
        "|---|---|---|---|---|---|---|",
    ]
    for entry in summary["models"].values():
        o, vs = entry["overall"], entry.get("vs_best_baseline")
        ci = (
            "—"
            if not vs
            else (
                f"{vs['diff']:+.4f} [{vs['ci95'][0]:+.4f}, {vs['ci95'][1]:+.4f}]"
                if np.isfinite(vs["ci95"][0])
                else f"{vs['diff']:+.4f} [n/a]"
            )
        )
        lines.append(
            f"| {entry['model_type']} | {_fmt(o.get(primary))} | "
            f"{_fmt(o.get('precision@' + budget))} | {_fmt(o.get('coverage'))} | "
            f"{_fmt(o.get('pr_auc'))} | {_fmt(entry[primary + '@min_score'])} | {ci} |"
        )
    budgets = list(next(iter(summary["models"].values()))["curve"]) if summary["models"] else []
    lines += [
        "",
        "## Recall by budget",
        "",
        "| model | " + " | ".join(budgets) + " |",
        "|---" * (len(budgets) + 1) + "|",
    ]
    for entry in summary["models"].values():
        lines.append(
            f"| {entry['model_type']} | "
            + " | ".join(_fmt(entry["curve"][b]) for b in budgets)
            + " |"
        )
    calibrated = [e for e in summary["models"].values() if e["reliability"]]
    if calibrated:
        lines += ["", "## Calibration (ECE, 10 equal-count bins)", ""]
        for entry in calibrated:
            ece = entry["reliability"]["ece"]
            verdict = "probability-like" if ece <= 0.05 else "ranking score only"
            lines.append(f"- {entry['model_type']}: ECE {ece:.4f} ({verdict})")
    return "\n".join(lines) + "\n"
