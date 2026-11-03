"""Focused contract, rate and leakage checks for synthetic telemetry faults."""

import json
from collections import Counter
from dataclasses import replace
from datetime import timedelta

from predictive_cache.datagen.config import load_config
from predictive_cache.datagen.faults import Faults
from predictive_cache.datagen.io import canonical_json
from predictive_cache.datagen.model import Intent
from predictive_cache.datagen.pipeline import component_rng
from predictive_cache.datagen.realize import EventRealizer
from predictive_cache.datagen.schema import SCHEMAS, validate_rows
from predictive_cache.datagen.stubs import build_world


def setup():
    cfg = load_config(preset="small")
    for field in type(cfg.faults).model_fields:
        getattr(cfg.faults, field).enabled = False
    cfg.realize.eacces_rate = cfg.realize.stale_open_rate = 0
    cfg.realize.interactive_think_time_max_s = 0
    world = build_world(cfg, component_rng(cfg.seed, "world"))
    return cfg, world


def clean_rows(world, count=20000):
    volume = world.volumes[0]
    span = (volume.period_end - volume.period_start).total_seconds()
    for index in range(count):
        at = volume.period_start + timedelta(seconds=span * index / count)
        event = dict.fromkeys(SCHEMAS["events"].names)
        event.update(
            schema_version=1,
            event_id=f"ev-{index:06d}",
            event_time=at,
            ingest_time=at,
            volume_id=volume.volume_id,
            file_id=world.files[0].file_id,
            dir_id=world.directories[0].dir_id,
            operation="read",
            file_size=1,
            offset=0,
            bytes_requested=1,
            bytes_read=1,
            result="ok",
            client_id=f"client-{index % 1000}",
            origin="foreground",
        )
        truth = dict(
            event_id=event["event_id"],
            intent_id=None,
            cause="background",
            faults=[],
            true_event_time=at,
            true_file_id=event["file_id"],
            original_fields_json="{}",
        )
        yield event, truth


def apply(cfg, world, clean):
    faults = Faults(cfg)
    records = list(faults.apply(iter(clean), world, component_rng(cfg.seed, "faults")))
    return faults, records


def test_disabled_exact_equality_and_fault_rates_restore_clean():
    cfg, world = setup()
    clean = list(clean_rows(world))
    faults, records = apply(cfg, world, clean)
    assert [(r.event, r.truth) for r in records] == clean
    assert all(r.disposition == "emitted" for r in records)
    for name in ("unknown_origin", "failed_result", "invalid_range", "late", "duplicate"):
        getattr(cfg.faults, name).enabled = True
        getattr(cfg.faults, name).rate = 0.1
    cfg.faults.clock_skew.enabled = True
    cfg.faults.clock_skew.outlier_share = 0.2
    cfg.faults.loss_window.enabled = True
    cfg.faults.loss_window.per_volume_month = 20
    cfg.faults.loss_window.hours = (6, 6)
    cfg.faults.loss_window.loss_fraction = (0.5, 0.5)
    cfg.faults.sampling.enabled = True
    cfg.faults.sampling.rate = 0.8
    faults, records = apply(cfg, world, clean)
    for stage, stat in faults.stats.items():
        assert stat["expected"] >= 100, (stage, stat)
        assert abs(stat["affected"] - stat["expected"]) <= 0.2 * stat["expected"], (stage, stat)
    assert records == apply(cfg, world, clean)[1]
    original = {event["event_id"]: (event, truth) for event, truth in clean}
    unique = {}
    for record in records:
        unique.setdefault(record.event["event_id"], record)
        restored = {**record.event, **json.loads(record.truth["original_fields_json"])}
        if record.disposition != "duplicate":
            assert canonical_json(restored) == canonical_json(original[record.event["event_id"]][0])
        validate_rows("events", [record.event])
        validate_rows("event_truth", [record.truth])
    assert len(unique) == len(clean)
    assert any(r.disposition == "dropped" for r in records)
    assert any(r.disposition == "sampled_out" for r in records)
    for row in faults.counters:
        assert row["offered"] == row["emitted"] + row["dropped"] + row["sampled_out"]
    delivered = [r for r in records if r.disposition in ("emitted", "duplicate")]
    assert len(delivered) == sum(row["emitted"] + row["duplicates"] for row in faults.counters)
    assert sum(row["offered"] for row in faults.counters) == len(clean)
    assert {gap["reason"] for gap in faults.known_gaps} == {"sampling", "loss_window"}
    for duplicate in (r for r in records if r.disposition == "duplicate"):
        first = unique[duplicate.event["event_id"]]
        assert first.disposition == "emitted"
        assert duplicate.event["ingest_time"] > first.event["ingest_time"]
        assert duplicate.truth is first.truth and "duplicate" in first.truth["faults"]


def test_identity_break_keeps_truth_and_sampling_zero_is_terminal():
    cfg, world = setup()
    clean = list(clean_rows(world, 3))
    clean[1][0]["operation"] = "rename"
    cfg.faults.identity_break.enabled = True
    _, records = apply(cfg, world, clean)
    assert records[0].event["file_id"] == clean[0][0]["file_id"]
    assert records[1].event["file_id"] == records[2].event["file_id"] != clean[0][0]["file_id"]
    assert all(r.truth["true_file_id"] == clean[0][0]["file_id"] for r in records)
    cfg.faults.sampling.enabled = True
    cfg.faults.sampling.rate = 0
    cfg.faults.duplicate.enabled = True
    cfg.faults.duplicate.rate = 1
    spill = world.volumes[0].period_end + timedelta(seconds=5)
    clean[-1][0].update(event_time=spill, ingest_time=spill)
    clean[-1][1]["true_event_time"] = spill
    faults, records = apply(cfg, world, clean)
    assert any(gap["end"] > spill for gap in faults.known_gaps)
    assert len(records) == 3 and all(r.disposition == "sampled_out" for r in records)
    assert sum(row["duplicates"] for row in faults.counters) == 0


def test_past_only_warmer_correlates_with_future_demand_and_complete_probe_handles():
    cfg, world = setup()
    cfg.period.weeks = 2
    world = build_world(cfg, component_rng(cfg.seed, "world"))
    cfg.faults.warmer_origin.enabled = cfg.faults.synthetic_origin.enabled = True
    cfg.faults.warmer_origin.top_n = 1
    start = world.volumes[0].period_start
    intents = [
        Intent(
            f"int-{day}",
            start + timedelta(days=day, hours=9),
            world.volumes[0].volume_id,
            world.actors[0].actor_id,
            world.files[0].file_id,
            "office",
            "full",
            None,
            "background",
        )
        for day in range(14)
    ]
    clean = list(
        EventRealizer(cfg, churn=False).realize(
            world,
            iter(intents),
            (start, world.volumes[0].period_end),
            component_rng(42, "realizer"),
        )
    )
    _, records = apply(cfg, world, clean)
    opens = [r.event for r in records if r.event["operation"] == "open"]
    warmer = [e for e in opens if e["origin"] == "warmer"]
    probes = [e for e in opens if e["origin"] == "synthetic"]
    assert warmer and probes
    assert all(e["event_time"] > intents[0].at for e in warmer)
    assert all(
        any(
            i.file_id == e["file_id"]
            and e["event_time"] < i.at < e["event_time"] + timedelta(days=1)
            for i in intents
        )
        for e in warmer
    )
    handles = {}
    for record in records:
        handles.setdefault(record.event["handle_id"], []).append(record.event)
    for events in handles.values():
        operations = Counter(e["operation"] for e in events)
        assert operations["open"] == operations["close"] == 1
        assert next(e["handle_bytes_read"] for e in events if e["operation"] == "close") == sum(
            e["bytes_read"] for e in events if e["operation"] == "read"
        )
    assert all(r.truth["cause"] in {"warmer_sim", "probe_sim", "background"} for r in records)


def test_duplicate_delay_and_lateness_use_true_time_not_skew():
    cfg, world = setup()
    cfg.faults.clock_skew.enabled = cfg.faults.late.enabled = cfg.faults.duplicate.enabled = True
    cfg.faults.clock_skew.sigma_s = 0
    cfg.faults.clock_skew.outlier_share = 1
    cfg.faults.clock_skew.outlier_s = 300
    cfg.faults.late.rate = cfg.faults.duplicate.rate = 1
    cfg.faults.late.max_delay_s = 0.5
    cfg.faults.late.threshold_s = 0.1
    cfg.faults.duplicate.max_delay_s = 0.2
    clean = list(clean_rows(world, 3))
    faults, records = apply(cfg, world, clean)
    for original, duplicate in zip(records[::2], records[1::2], strict=True):
        delay = (original.event["ingest_time"] - original.truth["true_event_time"]).total_seconds()
        assert 0 < delay <= 0.5
        assert (
            abs((original.event["event_time"] - original.truth["true_event_time"]).total_seconds())
            == 300
        )
        assert (
            0
            < (duplicate.event["ingest_time"] - original.event["ingest_time"]).total_seconds()
            <= 0.2
        )
    assert sum(row["late"] for row in faults.counters) == 3


def test_origin_handles_crossing_finalized_deletion_are_removed_whole():
    cfg, world = setup()
    cfg.faults.synthetic_origin.enabled = True
    file = world.files[0]
    world.files[0] = replace(file, deleted_at=file.created_at + timedelta(microseconds=1))
    _, records = apply(cfg, world, [])
    assert records == []
