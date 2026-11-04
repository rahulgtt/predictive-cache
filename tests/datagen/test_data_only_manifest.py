"""The ClearML source manifest must not expose test-only generator truth."""

from datetime import timedelta

from predictive_cache.clearml.datasets import data_only_files, project_data_manifest
from predictive_cache.datagen.config import load_config
from predictive_cache.datagen.pipeline import run
from predictive_cache.datagen.schema import KnownGap
from predictive_cache.datagen.validate import validate_dataset
from predictive_cache.prep.config import CleanConfig
from predictive_cache.prep.coverage import build_coverage


def test_projected_manifest_has_only_observed_fields(tmp_path):
    root = tmp_path / "generated"
    full = run(load_config(preset="tiny"), root)
    assert validate_dataset(root)["events"] == 13092

    projected = project_data_manifest(full)
    document = projected.model_dump(mode="json")
    assert document["publication_kind"] == "data_only"
    assert document["stage"] == "events"
    assert document["known_gaps"] == []
    assert "dropped_events" not in document["counts"]
    assert "patterns" not in document["counts"]
    assert "by_fault" not in document["counts"]
    assert all(
        entry["path"] == "config.resolved.yaml"
        or entry["path"].startswith(("catalog/", "events/", "loss/"))
        for entry in document["files"]
    )
    assert len(data_only_files(root, projected)) == len(projected.files)


def test_projected_known_gap_still_masks_zero_drop_hour(tmp_path):
    full = run(load_config(preset="tiny"), tmp_path / "generated")
    gap = KnownGap(
        volume_id=full.volumes[0],
        start=full.period.start,
        end=full.period.start + timedelta(hours=1),
        reason="loss_window",
        loss_fraction=0.5,
    )
    projected = project_data_manifest(full.model_copy(update={"known_gaps": [gap]}))
    counter = {
        "volume_id": full.volumes[0],
        "hour_start": full.period.start,
        "offered": 10,
        "emitted": 10,
        "dropped": 0,
        "sampled_out": 0,
    }
    rows = build_coverage(projected.model_dump(mode="python"), [counter], CleanConfig())
    assert rows[0]["status"] == "incomplete"
