"""Run the existing cleaner on a truth-free ClearML synthetic Dataset."""

from __future__ import annotations

import argparse
import sys
import tempfile
from contextlib import ExitStack
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))


def validate_source_files(root: Path, listed_files: list[str], dataset_name: str) -> str:
    """Check the downloaded source against the generator manifest before cleaning."""
    from predictive_cache.clearml.datasets import data_only_files
    from predictive_cache.datagen.config import load_config as load_generator_config
    from predictive_cache.datagen.io import config_hash
    from predictive_cache.datagen.schema import DataOnlyManifest

    root = Path(root)
    manifest_path = root / "manifest.json"
    if manifest_path.is_symlink() or not manifest_path.is_file():
        raise ValueError("source Dataset has no safe manifest.json")
    manifest = DataOnlyManifest.model_validate_json(manifest_path.read_text())
    if manifest.dataset_id != dataset_name:
        raise ValueError("source Dataset name differs from generator manifest dataset_id")
    if (
        config_hash(load_generator_config(path=root / "config.resolved.yaml"))
        != manifest.config_sha256
    ):
        raise ValueError("source Dataset resolved config hash differs from manifest")
    expected = {str(path) for path in data_only_files(root, manifest)} | {"manifest.json"}
    if set(listed_files) != expected:
        raise ValueError("source Dataset must contain exactly the data-only manifest files")
    actual = {path.relative_to(root).as_posix() for path in root.rglob("*") if path.is_file()}
    if actual != expected:
        raise ValueError("downloaded source files differ from ClearML Dataset file list")
    return manifest.dataset_id


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dataset", help="Immutable truth-free synthetic ClearML Dataset ID")
    parser.add_argument("--config", type=Path, help="YAML clean config on submitting machine")
    parser.add_argument("--queue", default="default")
    parser.add_argument("--out", type=Path, help="Fresh local output directory")
    parser.add_argument("--local", action="store_true", help="Run here while tracking in ClearML")
    return parser


def main(argv: list[str] | None = None) -> None:
    from clearml import Dataset, Task

    from predictive_cache.prep.clean.pipeline import run_clean
    from predictive_cache.prep.config import CleanConfig, load_config

    parser = _parser()
    args = parser.parse_args(argv)
    local = Task.running_locally()
    if local and not args.input_dataset:
        parser.error("--input-dataset is required when submitting")
    task = Task.init(
        project_name="predictive-cache/prep",
        task_name=f"clean:{args.input_dataset[:8]}" if local else None,
        task_type=Task.TaskTypes.data_processing,
        tags=["#88", "data:synthetic"],
        output_uri=True,
        reuse_last_task_id=False,
        auto_connect_arg_parser=False,
    )
    inputs = task.connect({"source_dataset_id": args.input_dataset or ""}, name="inputs")
    source_id = inputs["source_dataset_id"]
    cfg = CleanConfig.model_validate(
        task.connect_configuration(
            load_config(args.config).model_dump(mode="json") if local else {}, name="clean"
        )
    )
    if local:
        task.set_repo(repo="https://github.com/rahulgtt/predictive-cache.git")
    if not args.local:
        task.execute_remotely(queue_name=args.queue, clone=False)

    source = Dataset.get(dataset_id=source_id, only_completed=True)
    source_project = Task.get_task(task_id=source_id).get_project_name()
    expected_project = f"predictive-cache/datasets/synthetic/.datasets/{source.name}"
    if source_project != expected_project:
        raise ValueError("source Dataset is not in the synthetic data project")
    source_root = Path(source.get_local_copy())
    source_name = validate_source_files(source_root, source.list_files(), source.name)
    with ExitStack() as stack:
        out = (
            args.out
            or Path(stack.enter_context(tempfile.TemporaryDirectory(prefix="pcache-clean-")))
            / "dataset"
        )
        manifest = run_clean(source_root, cfg, out)
        if manifest["parent"]["dataset_id"] != source_name:
            raise ValueError("cleaned manifest parent differs from source Dataset")
        dataset = Dataset.create(
            dataset_name=manifest["dataset_id"],
            dataset_project="predictive-cache/datasets/clean",
            dataset_tags=["data:synthetic", "#88"],
            parent_datasets=[source_id],
            output_uri=task.get_output_destination(),
            description="Cleaned synthetic demand tables; not model-ready snapshots",
        )
        dataset.sync_folder(local_path=str(out))
        expected = {entry["path"] for entry in manifest["files"]} | {"manifest.json"}
        if set(dataset.list_files()) != expected:
            raise RuntimeError("clean Dataset contains inherited or missing source files")
        dataset.upload(show_progress=False)
        if not dataset.finalize():
            raise RuntimeError("ClearML clean Dataset finalization failed")
        task.connect({"clean_dataset_id": dataset.id}, name="outputs")
        task.upload_artifact("clean-manifest", str(out / "manifest.json"), wait_on_upload=True)
        task.get_logger().report_scalar(
            "clean", "kept_events", manifest["counts"]["kept_events"], 0
        )
        task.get_logger().report_scalar(
            "clean", "complete_hours", manifest["coverage"]["complete"], 0
        )
        print(
            f"Cleaned {source_name} into {manifest['dataset_id']} "
            f"({manifest['counts']['kept_events']} kept); ClearML Dataset {dataset.id}"
        )


if __name__ == "__main__":
    main()
