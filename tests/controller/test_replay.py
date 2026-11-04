"""Dataset replay is ordered, clock-gated, bounded and metadata-only."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from predictive_cache.controller.sources import ReplayClock, ReplaySource
from predictive_cache.prep.clean.io import sha256


@pytest.mark.parametrize("format", ["jsonl", "parquet"])
def test_unsorted_partitions_global_order_clock_and_batches(event, dataset, format):
    path = dataset(
        [
            [
                event(event_id="z", ingest_time=NOW + timedelta(seconds=2)),
                event(event_id="b"),
            ],
            [
                event(event_id="a"),
                event(event_id="middle", ingest_time=NOW + timedelta(seconds=1)),
            ],
        ],
        format,
    )
    clock = ReplayClock(NOW, speed=0)
    source = ReplaySource(path, clock, batch_size=1)
    batches = []
    try:
        assert source.next_ingest_time == NOW
        assert source.emit_ready(batches.append) == 2
        assert [batch[0]["event_id"] for batch in batches] == ["a", "b"]
        assert source.completed is False
        clock.advance(NOW + timedelta(seconds=2))
        assert source.emit_ready(batches.append) == 2
        assert [batch[0]["event_id"] for batch in batches] == ["a", "b", "middle", "z"]
        assert source.completed is True
        assert source.next_ingest_time is None
        assert max(map(len, batches)) == 1
    finally:
        source.stop()
    with pytest.raises(RuntimeError, match="closed"):
        source.emit_ready(batches.append)


NOW = datetime(2026, 1, 5, 12, tzinfo=UTC)


def test_clock_and_replay_bounds(event, dataset):
    with pytest.raises(ValueError):
        ReplayClock(NOW, speed=-1)
    clock = ReplayClock(NOW, speed=0)
    clock.advance(NOW + timedelta(seconds=10))
    with pytest.raises(ValueError, match="backwards"):
        clock.advance(NOW)
    path = dataset([[event()]])
    with pytest.raises(ValueError, match="bounds"):
        ReplaySource(path, clock, batch_size=0)


def test_source_rejects_corruption_and_does_not_follow_manifest_escape(event, dataset):
    import json

    path = dataset([[event()]])
    manifest_file = path / "manifest.json"
    manifest = json.loads(manifest_file.read_text())
    artifact = path / manifest["files"][0]["path"]
    artifact.write_text("corrupt")
    with pytest.raises(ValueError, match="corrupted"):
        ReplaySource(path)
    manifest["files"][0]["path"] = "events/../../outside.jsonl"
    manifest["files"][0]["sha256"] = sha256(artifact)
    manifest_file.write_text(json.dumps(manifest))
    with pytest.raises(ValueError, match="escapes"):
        ReplaySource(path)


def test_background_source_surfaces_sink_failure(event, dataset):
    from threading import Event

    path = dataset([[event()]])
    source = ReplaySource(path, ReplayClock(NOW, speed=0))
    attempted = Event()

    def broken(batch):
        attempted.set()
        raise RuntimeError("fixture sink")

    source.start(broken)
    assert attempted.wait(2)
    source.stop()
    assert isinstance(source.error, RuntimeError)


def test_jsonl_event_size_is_bounded_before_parsing(event, dataset):
    path = dataset([[event(app="x" * 5000)]])
    with pytest.raises(ValueError, match="size limit"):
        ReplaySource(path, max_event_bytes=4096)
