"""Opt-in truth-oracle checks for datagen small; production never reads truth/."""

from __future__ import annotations

import json
import os
from collections import Counter, defaultdict
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from predictive_cache.prep.coverage import utc


def rows(path: Path, columns: list[str]):
    for batch in pq.ParquetFile(path).iter_batches(columns=columns, batch_size=65536):
        yield from batch.to_pylist()


@pytest.mark.skipif(
    not os.getenv("PCACHE_SMALL_INPUT") or not os.getenv("PCACHE_SMALL_CLEAN"),
    reason="set PCACHE_SMALL_INPUT and PCACHE_SMALL_CLEAN for the small acceptance run",
)
def test_small_truth_thresholds():
    source = Path(os.environ["PCACHE_SMALL_INPUT"])
    cleaned = Path(os.environ["PCACHE_SMALL_CLEAN"])
    parent = json.loads((source / "manifest.json").read_text())
    manifest = json.loads((cleaned / "manifest.json").read_text())
    assert parent["preset"] == "small"
    original = {}
    deliveries = Counter()
    for entry in parent["files"]:
        if entry["path"].startswith("events/"):
            for row in rows(
                source / entry["path"], ["event_id", "ingest_time", "origin", "result"]
            ):
                event_id = row["event_id"]
                deliveries[event_id] += 1
                if (
                    event_id not in original
                    or row["ingest_time"] < original[event_id]["ingest_time"]
                ):
                    original[event_id] = row
    kept = {}
    excluded = defaultdict(Counter)
    for entry in manifest["files"]:
        if entry["path"].startswith("events_clean/"):
            kept.update(
                {
                    row["event_id"]: row["ingest_time"]
                    for row in rows(cleaned / entry["path"], ["event_id", "ingest_time"])
                }
            )
        elif entry["path"].startswith("excluded/"):
            for row in rows(cleaned / entry["path"], ["event_id", "reason"]):
                excluded[row["event_id"]][row["reason"]] += 1
    assert sum(deliveries.values()) == manifest["counts"]["input_events"]
    assert len(kept) == manifest["counts"]["kept_events"]
    assert sum(sum(reasons.values()) for reasons in excluded.values()) == sum(
        manifest["counts"]["excluded_by_reason"].values()
    )
    for event_id, delivery_count in deliveries.items():
        reasons = excluded[event_id]
        assert reasons["duplicate"] == delivery_count - 1
        if event_id in kept:
            assert kept[event_id] == original[event_id]["ingest_time"]
        assert (event_id in kept) + sum(
            count for reason, count in reasons.items() if reason != "duplicate"
        ) == 1

    valid_foreground = false_excluded = invalid_ranges = truth_rows = 0
    for entry in parent["files"]:
        if not entry["path"].startswith("truth/event_truth."):
            continue
        for truth in rows(source / entry["path"], ["event_id", "faults"]):
            event_id = truth["event_id"]
            if event_id not in original:
                continue
            truth_rows += 1
            canonical = original[event_id]
            reason = next((name for name in excluded[event_id] if name != "duplicate"), None)
            if canonical["origin"] in {"warmer", "synthetic", "unknown", None}:
                assert reason == "origin_" + (canonical["origin"] or "unknown")
            faults = set(truth["faults"])
            if "invalid_range" in faults and canonical["origin"] == "foreground":
                invalid_ranges += 1
                assert reason == "invalid_range"
            if (
                canonical["origin"] == "foreground"
                and canonical["result"] == "ok"
                and not (faults & {"invalid_range", "failed_result", "origin_unknown"})
            ):
                valid_foreground += 1
                false_excluded += reason is not None
    assert truth_rows == len(original)
    assert invalid_ranges > 0 and valid_foreground > 0
    print(
        f"valid_foreground={valid_foreground} false_excluded={false_excluded} invalid_ranges={invalid_ranges} truth_rows={truth_rows}"
    )
    assert false_excluded / valid_foreground <= 0.001

    coverage = [
        row
        for entry in manifest["files"]
        if entry["path"].startswith("coverage/")
        for row in rows(cleaned / entry["path"], ["volume_id", "hour_start", "status"])
    ]
    for gap in parent["known_gaps"]:
        for row in coverage:
            if row["volume_id"] == gap["volume_id"] and (
                utc(row["hour_start"]) < utc(gap["end"])
                and utc(row["hour_start"]).timestamp() + 3600 > utc(gap["start"]).timestamp()
            ):
                assert row["status"] != "complete"
    report = json.loads((cleaned / "report/quality.json").read_text())
    assert report["counts"] == manifest["counts"]
    assert report["excluded_by_reason"] == manifest["counts"]["excluded_by_reason"]
    assert sum(report["coverage_hours"].values()) == manifest["coverage"]["hours"]
