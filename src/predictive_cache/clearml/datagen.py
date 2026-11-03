"""Run the existing synthetic generator as one ClearML compute task."""

from __future__ import annotations

import argparse
import sys
import tempfile
from contextlib import ExitStack
from pathlib import Path

# The agent executes this tracked source file from a fresh repository clone.
if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    scenario = parser.add_mutually_exclusive_group()
    scenario.add_argument("--preset", choices=("tiny", "small", "medium"))
    scenario.add_argument("--config", type=Path, help="Scenario YAML on the submitting machine")
    parser.add_argument("--seed", type=int, help="Override the scenario seed")
    parser.add_argument("--format", choices=("parquet", "jsonl"))
    parser.add_argument("--queue", default="default", help="Compute queue for remote execution")
    parser.add_argument("--out", type=Path, help="Fresh output directory; otherwise temporary")
    parser.add_argument("--local", action="store_true", help="Run here while tracking in ClearML")
    parser.add_argument("--no-track", action="store_true", help="Run here without ClearML")
    return parser


def main(argv: list[str] | None = None) -> None:
    parser = _parser()
    args = parser.parse_args(argv)
    from predictive_cache.datagen.config import Config, load_config

    task = None
    if args.no_track:
        if (args.preset is None) == (args.config is None):
            parser.error("provide exactly one of --preset or --config")
        if args.out is None:
            raise SystemExit("--no-track requires --out so the output remains available")
        cfg = load_config(path=args.config, preset=args.preset, seed=args.seed, format=args.format)
    else:
        try:
            from clearml import Task
        except ImportError as exc:
            raise SystemExit("Install the ClearML extra: uv sync --extra clearml") from exc

        local = Task.running_locally()
        if local and (args.preset is None) == (args.config is None):
            parser.error("provide exactly one of --preset or --config")
        task = Task.init(
            project_name="predictive-cache/datagen",
            task_name=f"datagen:{args.preset or args.config.stem}" if local else None,
            task_type=Task.TaskTypes.data_processing,
            tags=["#71", "data:synthetic"],
            output_uri=True,
            reuse_last_task_id=False,
            auto_connect_arg_parser=False,
        )
        # The submitted, resolved scenario lives in the Task, so a worker does
        # not need the submitting machine's YAML path or uncommitted files.
        scenario = (
            load_config(
                path=args.config, preset=args.preset, seed=args.seed, format=args.format
            ).model_dump(mode="json")
            if local
            else {}
        )
        cfg = Config.model_validate(task.connect_configuration(scenario, name="scenario"))
        if local:
            task.set_repo(repo="https://github.com/rahulgtt/predictive-cache.git")
            task.add_tags(f"seed:{cfg.seed}")
        if not args.local:
            task.execute_remotely(queue_name=args.queue, clone=False)

    from predictive_cache.datagen.pipeline import run
    from predictive_cache.datagen.validate import validate_dataset

    with ExitStack() as stack:
        out = (
            args.out
            or Path(stack.enter_context(tempfile.TemporaryDirectory(prefix="pcache-"))) / "dataset"
        )
        manifest = run(cfg, out)
        result = validate_dataset(out)
        print(f"Validated {manifest.dataset_id}: {result['events']} events at {out}")
        if task:
            if not task.upload_artifact(
                "synthetic-dataset-test-only",
                str(out),
                metadata={"dataset_id": manifest.dataset_id, "contains_truth": "true"},
                wait_on_upload=True,
            ):
                raise RuntimeError("ClearML dataset artifact upload failed")
            task.get_logger().report_scalar(
                title="datagen", series="delivered_events", value=result["events"], iteration=0
            )
            print(f"ClearML task {task.id} uploaded the complete test-only dataset")


if __name__ == "__main__":
    main()
