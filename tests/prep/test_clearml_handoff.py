"""The ClearML clean worker must only consume the data-only generator subset."""

import shutil

import pytest

from predictive_cache.clearml.clean import validate_source_files
from predictive_cache.clearml.datasets import data_only_files
from predictive_cache.datagen.config import load_config
from predictive_cache.datagen.pipeline import run


def test_truth_free_source_contract(tmp_path):
    full = tmp_path / "full"
    manifest = run(load_config(preset="tiny"), full)
    data_only = tmp_path / "data-only"
    files = data_only_files(full, manifest)
    for relative in files + ["manifest.json"]:
        destination = data_only / relative
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(full / relative, destination)
    listed = [
        path.relative_to(data_only).as_posix() for path in data_only.rglob("*") if path.is_file()
    ]

    assert validate_source_files(data_only, listed, manifest.dataset_id) == manifest.dataset_id
    with pytest.raises(ValueError, match="exactly the data-only"):
        validate_source_files(
            data_only, listed + ["truth/event_truth.parquet"], manifest.dataset_id
        )
    with pytest.raises(ValueError, match="name differs"):
        validate_source_files(data_only, listed, "another-dataset")
    event = next(data_only.glob("events/**/*.parquet"))
    event.write_bytes(b"corrupted")
    with pytest.raises(ValueError, match="corrupted"):
        validate_source_files(data_only, listed, manifest.dataset_id)
