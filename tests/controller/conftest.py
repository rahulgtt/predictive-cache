"""Fictional event fixtures and tiny source datasets, never workload evidence."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from predictive_cache.datagen.io import canonical_json, write_table
from predictive_cache.prep.clean.io import sha256

NOW = datetime(2026, 1, 5, 12, tzinfo=UTC)


@pytest.fixture
def event():
    def make(**changes):
        row = {
            "schema_version": 1,
            "event_id": "ev-1",
            "event_time": NOW,
            "ingest_time": NOW,
            "volume_id": "vol-1",
            "file_id": "file-1",
            "dir_id": "dir-1",
            "operation": "open",
            "handle_id": "hdl-1",
            "offset": None,
            "bytes_requested": None,
            "bytes_read": None,
            "handle_bytes_read": None,
            "handle_duration_ms": None,
            "file_size": 100,
            "result": "ok",
            "origin": "foreground",
            "session_id": None,
            "cohort_id": None,
            "client_id": None,
            "app": None,
            "cache_outcome": None,
            "latency_ms": None,
        }
        row.update(changes)
        return row

    return make


@pytest.fixture
def dataset(tmp_path):
    def make(partitions, format="jsonl"):
        root = tmp_path / f"source-{format}"
        root.mkdir()
        entries = []
        for index, rows in enumerate(partitions):
            relative = f"events/volume_id=vol-1/date=2026-01-05/part-{index:05}.{format}"
            path = root / relative
            write_table(path, "events", rows)
            entries.append({"path": relative, "rows": len(rows), "sha256": sha256(path)})
        # A truth entry is present but intentionally absent on disk. Production
        # replay and cleaning must never open it.
        entries.append({"path": "truth/do-not-open.jsonl", "rows": 0, "sha256": "0" * 64})
        manifest = {
            "manifest_version": 1,
            "dataset_id": "fixture-collector",
            "data_kind": "synthetic",
            "generator": {
                "name": "pcache-datagen",
                "version": "0.1.0",
                "git_sha": "fixture",
            },
            "event_schema_version": 1,
            "seed": 0,
            "preset": "custom",
            "config_sha256": "0" * 64,
            "period": {"start": NOW, "end": NOW + timedelta(hours=2)},
            "volumes": ["vol-1"],
            "counts": {
                "events": sum(map(len, partitions)),
                "dropped_events": 0,
                "files": 1,
                "actors": 0,
                "patterns": 0,
            },
            "known_gaps": [],
            "files": entries,
            "created_at": NOW,
        }
        (root / "manifest.json").write_text(canonical_json(manifest))
        return root

    return make
