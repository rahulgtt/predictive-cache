"""Fixture-only parity against real merged run_clean output, not a fake cleaner.

updates_from_clean/FeatureStore do not exist in this checkout. The independent
projection below follows data-prep §5.1/§10 from the actual batch opens/meta
artifacts. It establishes update-contract parity only, not feature-store parity
or model accuracy on real workloads.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from predictive_cache.controller.ingest import Ingestor
from predictive_cache.controller.sources import ReplayClock, ReplaySource
from predictive_cache.datagen.io import canonical_json
from predictive_cache.prep.clean.io import read_rows
from predictive_cache.prep.clean.pipeline import run_clean
from predictive_cache.prep.config import CleanConfig
from predictive_cache.prep.coverage import utc

NOW = datetime(2026, 1, 5, 12, tzinfo=UTC)


def batch_updates(cleaned, manifest):
    updates = []
    for entry in manifest["files"]:
        family = entry["path"].split("/")[0]
        if family not in {"opens", "meta"}:
            continue
        for row in read_rows(cleaned / entry["path"]):
            if family == "meta":
                updates.append({"kind": "MetaChanged", **row})
                continue
            if not row["open_missing"]:
                updates.append(
                    {
                        "kind": "OpenObserved",
                        "open_id": row["open_id"],
                        "volume_id": row["volume_id"],
                        "file_id": row["file_id"],
                        "dir_id": row["dir_id"],
                        "cohort_id": row["cohort_id"],
                        "app": row["app"],
                        "session_key": row["session_id"] or row["client_id"],
                        "open_time": row["open_time"],
                        "known_at": row["open_known_at"],
                        "file_size": row["file_size"],
                    }
                )
            updates.append(
                {
                    "kind": "OpenCompleted",
                    "open_id": row["open_id"],
                    "volume_id": row["volume_id"],
                    "file_id": row["file_id"],
                    "known_at": row["completed_known_at"],
                    "bytes_read": row["bytes_read"],
                    "distinct_bytes_read": row["distinct_bytes_read"],
                    "full_read": row["full_read"],
                    "consumed": row["consumed"],
                }
            )
    # Independently implement the documented order (do not call core's sorter).
    return sorted(
        updates,
        key=lambda u: (
            utc(u["known_at"]),
            ["MetaChanged", "OpenObserved", "OpenCompleted"].index(u["kind"]),
            u.get("event_id", u.get("open_id", "")),
        ),
    )


@pytest.mark.parametrize("read_mode", ["ranges", "summary"])
@pytest.mark.parametrize("format", ["jsonl", "parquet"])
def test_fixture_batch_stream_update_parity(event, dataset, tmp_path, read_mode, format):
    rows = [
        event(event_id="open-z", handle_id="z", client_id="client"),
        event(event_id="open-a", handle_id="a"),
        event(
            event_id="close-z",
            operation="close",
            handle_id="z",
            handle_bytes_read=20,
            ingest_time=NOW + timedelta(seconds=1),
        ),
        event(
            event_id="close-a",
            operation="close",
            handle_id="a",
            handle_bytes_read=0,
            ingest_time=NOW + timedelta(seconds=2),
        ),
        event(
            event_id="read-z",
            operation="read",
            handle_id="z",
            offset=0,
            bytes_requested=20,
            bytes_read=20,
            ingest_time=NOW + timedelta(seconds=11),
        ),
        event(
            event_id="write",
            operation="write",
            offset=0,
            bytes_requested=10,
            ingest_time=NOW + timedelta(seconds=11),
        ),
        event(event_id="open-b", handle_id="b", ingest_time=NOW + timedelta(seconds=11)),
        event(
            event_id="orphan",
            operation="read",
            handle_id=None,
            offset=0,
            bytes_requested=5,
            bytes_read=5,
            ingest_time=NOW + timedelta(seconds=20),
        ),
        event(event_id="warm", origin="warmer"),
        event(event_id="synthetic", origin="synthetic"),
        event(event_id="unknown", origin="unknown"),
        event(event_id="failure", result="eio"),
    ]
    rows.append(dict(rows[0]))  # canonical identical duplicate, inside window/cap
    path = dataset([list(reversed(rows[::2])), list(reversed(rows[1::2]))], format)
    cfg = CleanConfig(read_mode=read_mode, completion_grace_s=10, handle_timeout_s=30)
    output = tmp_path / "cleaned"
    manifest = run_clean(path, cfg, output, format=format)
    expected = batch_updates(output, manifest)
    received = []  # test-only receiver, not a FeatureStore implementation
    core = Ingestor(cfg, received.append, allowed_volumes={"vol-1"}, queue_max=100)
    clock = ReplayClock(NOW + timedelta(hours=3), speed=0)
    source = ReplaySource(path, clock, batch_size=2)
    try:

        def offer(batch):
            assert core.offer(batch, replay=True) == (len(batch), 0)
            core.drain(utc(batch[-1]["ingest_time"]))

        source.emit_ready(offer)
        core.drain(clock.now())
    finally:
        source.stop()
    assert canonical_json(received) == canonical_json(expected)
    stats = core.stats()
    assert stats["dropped_overflow"] == stats["dedupe_capacity_evictions"] == 0
    assert stats["dedupe_expired"] == 0  # no older-window duplicate exception in fixture
    assert stats["tracker_exhausted"] is False
    assert stats["excluded_by_reason"] == manifest["counts"]["excluded_by_reason"]
    assert (
        stats["unexpected_reads_in_summary_mode"]
        == manifest["counts"]["unexpected_reads_in_summary_mode"]
    )
