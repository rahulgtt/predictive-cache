"""Build read-demand-v1 training snapshots from a synthetic ClearML Dataset (#114).

Raw datagen Dataset -> ``pcache-prep clean --semantic-view read-demand-v1`` ->
``pcache-prep read-snapshots`` -> Dataset in ``predictive-cache/datasets/snapshots``.

Submit from a pushed commit (the agent clones it):

    uv run --extra clearml --extra ml python src/predictive_cache/clearml/read_snapshots.py \
        --input-dataset <raw-dataset-id> --queue default
"""

from __future__ import annotations

import argparse
import sys
import tempfile
from dataclasses import asdict
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dataset", help="Synthetic datagen ClearML Dataset ID")
    parser.add_argument("--queue", default="default")
    parser.add_argument("--tick-s", type=int, default=900)
    parser.add_argument("--tag", action="append", default=[], help="Extra Dataset tag")
    parser.add_argument("--local", action="store_true", help="Run here while tracking in ClearML")
    return parser


def main(argv: list[str] | None = None) -> None:
    from clearml import Dataset, Task

    from predictive_cache.clearml.clean import validate_source_files
    from predictive_cache.prep.clean.pipeline import run_clean
    from predictive_cache.prep.config import CleanConfig
    from predictive_cache.prep.read_snapshots import ReadSnapshotConfig, build_read_snapshots
    from predictive_cache.semantics.read_demand import SEMANTIC_VIEW_ID

    parser = _parser()
    args = parser.parse_args(argv)
    local = Task.running_locally()
    if local and not args.input_dataset:
        parser.error("--input-dataset is required when submitting")
    task = Task.init(
        project_name="predictive-cache/prep",
        task_name=f"read-snapshots:{args.input_dataset[:8]}" if local else None,
        task_type=Task.TaskTypes.data_processing,
        tags=["#114", "data:synthetic", SEMANTIC_VIEW_ID],
        output_uri=True,
        reuse_last_task_id=False,
        auto_connect_arg_parser=False,
    )
    inputs = task.connect(
        {"source_dataset_id": args.input_dataset or "", "tags": ",".join(args.tag)},
        name="inputs",
    )
    snap_cfg = ReadSnapshotConfig(
        **task.connect_configuration(
            asdict(ReadSnapshotConfig(tick_s=args.tick_s)) if local else {}, name="snapshots"
        )
    )
    if local:
        task.set_repo(repo="https://github.com/rahulgtt/predictive-cache.git")
    if not args.local:
        task.execute_remotely(queue_name=args.queue, clone=False)

    source_id = inputs["source_dataset_id"]
    source = Dataset.get(dataset_id=source_id, only_completed=True)
    source_root = Path(source.get_local_copy())
    validate_source_files(source_root, source.list_files(), source.name)
    with tempfile.TemporaryDirectory(prefix="pcache-read-snap-") as tmp:
        clean_dir, snap_dir = Path(tmp) / "clean", Path(tmp) / "snapshots"
        clean = run_clean(source_root, CleanConfig(), clean_dir, semantic_view=SEMANTIC_VIEW_ID)
        print(f"READ clean: {clean['counts']['read_events']} read events")
        manifest = build_read_snapshots(clean_dir, snap_dir, snap_cfg)
        tags = ["data:synthetic", "#114", SEMANTIC_VIEW_ID] + [
            t for t in inputs.get("tags", "").split(",") if t
        ]
        dataset = Dataset.create(
            dataset_name=manifest["dataset_id"],
            dataset_project="predictive-cache/datasets/snapshots",
            dataset_tags=tags,
            output_uri=task.get_output_destination(),
            description=(
                f"read-demand-v1 training snapshots from raw Dataset {source_id}; "
                "synthetic plumbing evidence"
            ),
        )
        dataset.add_files(path=str(snap_dir))
        dataset.set_metadata(
            {"source_dataset_id": source_id, "counts": manifest["counts"]},
            metadata_name="lineage",
        )
        dataset.upload(show_progress=False)
        if not dataset.finalize():
            raise RuntimeError("ClearML snapshots Dataset finalization failed")
        task.connect({"snapshots_dataset_id": dataset.id}, name="outputs")
        task.upload_artifact("snapshots-manifest", str(snap_dir / "manifest.json"), wait_on_upload=True)
        logger = task.get_logger()
        for split, counts in manifest["counts"].items():
            for key, value in counts.items():
                logger.report_scalar(f"snapshots {key}", split, value, 0)
            coverage = manifest["candidate_coverage"][split]
            if coverage is not None:
                logger.report_scalar("candidate coverage", split, coverage, 0)
        print(f"Snapshots {manifest['dataset_id']} -> ClearML Dataset {dataset.id}")


if __name__ == "__main__":
    main()
