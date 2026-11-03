"""``pcache-train``: train baselines + logistic regression from snapshots (spec §6.3).

Input is a local snapshot directory (``--snapshots``) or a ClearML Dataset
(``--snapshots-dataset``). With ClearML installed and tracking on, the run is
a ClearML task whose ``inputs`` and ``train`` configuration can be edited on a
clone and re-enqueued; ``--queue`` sends it to an agent (use the file-path
entry ``src/predictive_cache/clearml/train.py`` for remote runs).
"""

from __future__ import annotations

import tempfile
from dataclasses import asdict
from pathlib import Path

import click

from ..tracking import (
    ClearMLTracker,
    NullTracker,
    Tracker,
    get_dataset_copy,
    on_clearml_agent,
    tracking_enabled,
)
from .train import ALL_MODELS, TrainConfig, load_train_config, train


def _fail(tracker: Tracker, message: str) -> None:
    """Mark a tracked run failed (so ClearML never shows it completed), then exit non-zero."""
    tracker.fail(message)
    tracker.close()
    raise click.ClickException(message)


@click.command()
@click.option(
    "--snapshots",
    "snapshots_dir",
    type=click.Path(file_okay=False, path_type=Path),
    help="Local data-prep snapshot directory.",
)
@click.option("--snapshots-dataset", "dataset_id", help="ClearML Dataset ID of a snapshot dir.")
@click.option(
    "--out",
    "out_dir",
    type=click.Path(file_okay=False, path_type=Path),
    help="Fresh output directory (default: temporary when running on an agent).",
)
@click.option(
    "--config",
    "config_path",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
    help="YAML with a spec §11.2 `train:` section.",
)
@click.option("--models", help=f"Comma-separated subset of: {', '.join(ALL_MODELS)}.")
@click.option("--tag", "tags", multiple=True, help="Extra ClearML tags, e.g. '#56'.")
@click.option("--project", default="predictive-cache/train", show_default=True)
@click.option("--task-name", default=None, help="Default: train:<models>.")
@click.option("--queue", default=None, help="Enqueue on this ClearML queue and exit locally.")
@click.option("--no-track", is_flag=True, help="Run without ClearML (also PCACHE_TRACKING=off).")
def main(
    snapshots_dir,
    dataset_id,
    out_dir,
    config_path,
    models,
    tags,
    project,
    task_name,
    queue,
    no_track,
):
    """Fit on train, select on val, write model packages and a val report."""
    agent = on_clearml_agent()
    if not agent and (snapshots_dir is None) == (dataset_id is None):
        raise click.UsageError("provide exactly one of --snapshots or --snapshots-dataset")
    try:
        cfg = load_train_config(config_path)
        if models:
            cfg.models = [m.strip() for m in models.split(",") if m.strip()]
        cfg.validate()
    except (ValueError, KeyError) as exc:
        raise click.ClickException(str(exc)) from exc

    tracker: Tracker = NullTracker()
    if agent or tracking_enabled(no_track):
        tracker = ClearMLTracker.start(
            project,
            task_name or f"train:{'+'.join(cfg.models)}",
            ["#56", *tags],
            requirements=("scikit-learn",),
        )
        # Only the dataset ID is a task parameter: local paths never go to the server, and
        # on an agent (no CLI arguments) this is where the cloned/edited value comes from.
        inputs = tracker.connect({"snapshots_dataset_id": dataset_id or ""}, name="inputs")
        cfg = TrainConfig(**tracker.connect_configuration(asdict(cfg), name="train"))
        dataset_id = inputs.get("snapshots_dataset_id") or None
        if agent and not dataset_id:
            _fail(tracker, "agent run needs inputs.snapshots_dataset_id")
        if queue and not agent:
            if not dataset_id:
                _fail(
                    tracker,
                    "--queue needs --snapshots-dataset (agents cannot see local directories)",
                )
            tracker.execute_remotely(queue)  # exits this process; the agent re-runs the file
    elif queue:
        raise click.UsageError(
            "--queue needs ClearML tracking: install the clearml extra and configure a server "
            "(CLEARML_API_HOST or ~/clearml.conf)"
        )

    try:
        if dataset_id:
            snapshots_dir = get_dataset_copy(dataset_id)
        if out_dir is None:
            out_dir = Path(tempfile.mkdtemp(prefix="pcache-train-")) / "out"
        summary = train(snapshots_dir, cfg, out_dir, tracker)
    except (ValueError, OSError, KeyError, RuntimeError) as exc:
        _fail(tracker, str(exc))
    tracker.close()

    click.echo(
        f"Trained {len(summary['models'])} models from "
        f"{summary['data']['snapshots_dataset_id']} ({summary['data']['data_kind']}) "
        f"-> {out_dir}"
    )
    for model, entry in summary["models"].items():
        click.echo(
            f"  {model:20s} {summary['primary']}={entry[summary['primary']]} "
            f"min_score={entry['min_score']} package={entry['package_id']}"
        )
