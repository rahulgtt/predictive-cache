"""Publish a validated generator output without test-only truth or reports."""

from __future__ import annotations

import shutil
import tempfile
from pathlib import Path

from predictive_cache.datagen.io import canonical_json, sha256
from predictive_cache.datagen.schema import DataOnlyManifest, Manifest


def data_only_files(root: Path, manifest: Manifest | DataOnlyManifest) -> list[Path]:
    """Return the source files needed by clean, refusing unrecognized paths."""
    kept = []
    for entry in manifest.files:
        relative = Path(entry.path)
        if entry.path == "config.resolved.yaml" or entry.path.startswith(
            ("catalog/", "events/", "loss/")
        ):
            source = root / relative
            if source.is_symlink() or not source.is_file() or sha256(source) != entry.sha256:
                raise ValueError(f"missing or corrupted data file: {entry.path}")
            kept.append(relative)
        elif not entry.path.startswith(("truth/", "report/")):
            raise ValueError(f"unexpected generator file: {entry.path}")
    if not any(str(path).startswith("events/") for path in kept):
        raise ValueError("generator output has no events")
    return kept


def project_data_manifest(manifest: Manifest) -> DataOnlyManifest:
    """Keep observed provenance and file hashes while removing oracle fields."""
    safe_entries = [
        entry
        for entry in manifest.files
        if entry.path == "config.resolved.yaml"
        or entry.path.startswith(("catalog/", "events/", "loss/"))
    ]
    return DataOnlyManifest(
        dataset_id=manifest.dataset_id,
        generator=manifest.generator,
        seed=manifest.seed,
        preset=manifest.preset,
        config_sha256=manifest.config_sha256,
        period=manifest.period,
        volumes=manifest.volumes,
        counts={
            "events": manifest.counts.events,
            "files": manifest.counts.files,
            "actors": manifest.counts.actors,
            "by_operation": manifest.counts.by_operation,
            "by_origin": manifest.counts.by_origin,
        },
        known_gaps=manifest.known_gaps,
        files=safe_entries,
        created_at=manifest.created_at,
        limitations=[*manifest.limitations, "data-only projection; truth and reports omitted"],
    )


def publish_data_only(
    root: Path,
    manifest: Manifest,
    output_uri: str,
    version: str | None = None,
    extra_tags: tuple[str, ...] = (),
) -> str:
    """Create a finalized ClearML Dataset with a truth-free projected manifest.

    ``version`` sets the ClearML dataset version (None lets ClearML choose);
    ``extra_tags`` are added to the standard ``data:synthetic``/``seed`` tags.
    """
    from clearml import Dataset

    projected = project_data_manifest(manifest)
    files = data_only_files(root, projected)
    with tempfile.TemporaryDirectory(prefix="pcache-data-only-") as scratch:
        stage = Path(scratch)
        for relative in files:
            destination = stage / relative
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(root / relative, destination)
        (stage / "manifest.json").write_text(
            canonical_json(projected.model_dump(mode="json")) + "\n"
        )
        dataset = Dataset.create(
            dataset_name=manifest.dataset_id,
            dataset_project="predictive-cache/datasets/synthetic",
            dataset_version=version or None,
            dataset_tags=["data:synthetic", f"seed:{manifest.seed}", "#71", *extra_tags],
            output_uri=output_uri,
            description="Data-only synthetic events for cleaning; truth and reports excluded",
        )
        count = dataset.add_files(path=str(stage))
        if count != len(files) + 1:
            raise RuntimeError(f"ClearML registered {count} files; expected {len(files) + 1}")
        # ClearML 2.1 returns None after a successful upload; finalization
        # verifies that all files are uploaded before sealing the Dataset.
        dataset.upload(show_progress=False)
        if not dataset.finalize():
            raise RuntimeError("ClearML data-only Dataset finalization failed")
        return dataset.id
