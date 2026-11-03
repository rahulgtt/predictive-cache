"""``pcache-eval``: freeze packages, then evaluate on val or (once) on test (spec §7.4-§7.5)."""

from __future__ import annotations

import json
from pathlib import Path

import click

from ..tracking import (
    ClearMLTracker,
    NullTracker,
    Tracker,
    fail_and_exit,
    get_dataset_copy,
    tracking_enabled,
)
from .evaluate import EvalConfig, EvalError, freeze, load_eval_config, run

PROJECT = "predictive-cache/eval"


def _snapshots(snapshots_dir: Path | None, dataset_id: str | None) -> Path:
    if (snapshots_dir is None) == (dataset_id is None):
        raise click.UsageError("provide exactly one of --snapshots or --snapshots-dataset")
    return get_dataset_copy(dataset_id) if dataset_id else snapshots_dir


def _tracker(no_track: bool, name: str, tags: list[str]) -> Tracker:
    if not tracking_enabled(no_track):
        return NullTracker()
    return ClearMLTracker.start(PROJECT, name, ["#58", *tags], task_type="testing")


def _finish(tracker: Tracker, error: Exception | None) -> None:
    if error is not None:
        fail_and_exit(tracker, str(error))
    tracker.close()


snapshots_options = [
    click.option("--snapshots", "snapshots_dir", type=click.Path(file_okay=False, path_type=Path)),
    click.option("--snapshots-dataset", "dataset_id", help="ClearML Dataset ID of a snapshot dir."),
]


def with_snapshots(func):
    for option in reversed(snapshots_options):
        func = option(func)
    return func


@click.group()
def main():
    """Freeze model packages and evaluate them (docs/ml/spec.md §7)."""


@main.command("freeze")
@with_snapshots
@click.option(
    "--packages",
    "refs",
    multiple=True,
    required=True,
    help="Package zip/dir path or clearml://<model_id>; repeat per package.",
)
@click.option("--out", "out_path", required=True, type=click.Path(dir_okay=False, path_type=Path))
@click.option(
    "--config",
    "config_path",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    help="YAML with a spec §11.2 `eval:` section.",
)
@click.option("--tag", "tags", multiple=True)
@click.option("--no-track", is_flag=True)
def freeze_cmd(snapshots_dir, dataset_id, refs, out_path, config_path, tags, no_track):
    """Record packages, config and a test-data binding before any test run."""
    if out_path.exists():
        raise click.ClickException(f"{out_path} exists; a freeze record is never overwritten")
    cfg = load_eval_config(config_path)
    tracker = _tracker(no_track, "eval:freeze", list(tags))
    error = None
    try:
        record = freeze(_snapshots(snapshots_dir, dataset_id), list(refs), cfg, out_path, tracker)
        tracker.add_tags([f"freeze:{record['freeze_id']}"])
        click.echo(
            f"Froze {len(record['packages'])} packages as {record['freeze_id']} "
            f"(best baseline on val: {record['best_baseline']['model_type']}) -> {out_path}"
        )
    except (EvalError, ValueError, OSError, KeyError) as exc:
        error = exc
    _finish(tracker, error)


@main.command("run")
@with_snapshots
@click.option("--split", type=click.Choice(["val", "test"]), required=True)
@click.option(
    "--packages",
    "refs",
    multiple=True,
    help="Package paths or clearml://<model_id>; test runs use the frozen set.",
)
@click.option(
    "--freeze", "freeze_path", type=click.Path(exists=True, dir_okay=False, path_type=Path)
)
@click.option("--out", "out_dir", required=True, type=click.Path(file_okay=False, path_type=Path))
@click.option(
    "--config", "config_path", type=click.Path(exists=True, dir_okay=False, path_type=Path)
)
@click.option("--rerun-reason", default=None, help="Required to evaluate a freeze on test again.")
@click.option("--tag", "tags", multiple=True)
@click.option("--no-track", is_flag=True)
def run_cmd(
    snapshots_dir,
    dataset_id,
    split,
    refs,
    freeze_path,
    out_dir,
    config_path,
    rerun_reason,
    tags,
    no_track,
):
    """Evaluate packages on val (free) or test (frozen, once)."""
    record = json.loads(freeze_path.read_text()) if freeze_path is not None else None
    if record is not None and config_path is None:
        cfg = EvalConfig(**record["eval_config"])  # frozen runs reuse the frozen config
    else:
        cfg = load_eval_config(config_path)
    freeze_id = record.get("freeze_id") if record else None
    run_tags = [f"split:{split}", *([f"freeze:{freeze_id}"] if freeze_id else []), *tags]
    tracker = _tracker(no_track, f"eval:{split}-{freeze_id or 'unfrozen'}", run_tags)
    error = None
    try:
        summary = run(
            _snapshots(snapshots_dir, dataset_id),
            split,
            list(refs) or None,
            freeze_path,
            cfg,
            out_dir,
            tracker,
            rerun_reason,
        )
        click.echo(
            f"Evaluated {len(summary['models'])} packages on {split} "
            f"({summary['data_kind']}) -> {out_dir}/report.md"
        )
        for entry in summary["models"].values():
            vs = entry.get("vs_best_baseline")
            diff = "" if not vs else f" diff_vs_best={vs['diff']:+.4f}"
            click.echo(
                f"  {entry['model_type']:20s} {summary['primary']}="
                f"{entry['overall'][summary['primary']]}{diff}"
            )
    except (EvalError, ValueError, OSError, KeyError) as exc:
        error = exc
    _finish(tracker, error)
