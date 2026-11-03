"""The ClearML source manifest must not expose test-only generator truth."""

from predictive_cache.clearml.datasets import data_only_files, project_data_manifest
from predictive_cache.datagen.config import load_config
from predictive_cache.datagen.pipeline import run
from predictive_cache.datagen.validate import validate_dataset


def test_projected_manifest_has_only_observed_fields(tmp_path):
    root = tmp_path / "generated"
    full = run(load_config(preset="tiny"), root)
    assert validate_dataset(root)["events"] == 13092

    projected = project_data_manifest(full)
    document = projected.model_dump(mode="json")
    assert document["publication_kind"] == "data_only"
    assert document["stage"] == "events"
    assert "known_gaps" not in document
    assert "dropped_events" not in document["counts"]
    assert "patterns" not in document["counts"]
    assert "by_fault" not in document["counts"]
    assert all(
        entry["path"] == "config.resolved.yaml"
        or entry["path"].startswith(("catalog/", "events/", "loss/"))
        for entry in document["files"]
    )
    assert len(data_only_files(root, projected)) == len(projected.files)
