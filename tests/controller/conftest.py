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


@pytest.fixture
def raw_chimera():
    import base64
    import hashlib

    VOLUME = "01" * 16
    FILE = "02" * 32
    HANDLE = base64.urlsafe_b64encode(b"private-fictional-handle").decode().rstrip("=")

    def make(**changes):
        row = dict(
            schema="nfs4_read_v2",
            local_id="11111111-1111-4111-8111-111111111111",
            producer_epoch="22222222-2222-4222-8222-222222222222",
            producer_sequence="1",
            event_time_ns="1",
            sink_time_ns="2",
            target_created_ns="1",
            volume_id=VOLUME,
            file_id=FILE,
            identity_valid=True,
            export_name_b64="L3VudHJ1c3RlZA",
            export_id=7,
            target_fh=HANDLE,
            operation_id="03" * 32,
            client_ref="04" * 32,
            session_ref="",
            compound_op_index=1,
            slot_id=1,
            sequence_id=1,
            compound_tag_b64="",
            compound_tag_original_length=0,
            compound_tag_truncated=False,
            compound_tag_id=hashlib.sha256(b"").hexdigest(),
            origin_claim="unknown",
            origin_trust="client_asserted",
            requested_offset="0",
            requested_count=4096,
            returned_bytes=4096,
            nfs_status=0,
            outcome="success",
            eof=False,
            file_size_valid=True,
            file_size="65536",
        )
        row.update(changes)
        return row

    return make


@pytest.fixture
def raw_envelope(raw_chimera):
    def make(*events, **changes):
        rows = list(events) if events else [raw_chimera()]
        sequences = [int(row["producer_sequence"]) for row in rows]
        body = {
            "schema": "nfs4_read_v2",
            "pod_uid": "fixture-pod",
            "producer_epoch": rows[0]["producer_epoch"],
            "first_sequence": str(min(sequences)),
            "last_sequence": str(max(sequences)),
            "health": {
                "scraped_at_ns": "100",
                "last_synced_sequence": str(max(sequences)),
                "barrier_ready": False,
                "delivery_clean": True,
                "queue_depth": 1,
                "worker_in_flight": 1,
                "dropped": dict.fromkeys(("queue", "encode", "sink", "rollback", "rotation"), 0),
                "shipper_dropped": {"rotated_unsent": 0, "stale_epoch": 0},
            },
            "events": rows,
        }
        body.update(changes)
        return body

    return make
