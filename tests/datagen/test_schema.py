from datetime import UTC, datetime

import pyarrow.parquet as pq
import pytest

from predictive_cache.datagen.config import load_config
from predictive_cache.datagen.pipeline import component_rng
from predictive_cache.datagen.schema import SCHEMAS, rows_to_table, validate_rows
from predictive_cache.datagen.stubs import StubRealizer, build_world, catalog_rows, fixture_intents


def sample_rows():
    cfg = load_config(preset="tiny")
    world = build_world(cfg, component_rng(cfg.seed, "world"))
    tables = catalog_rows(world)
    events = list(
        StubRealizer(cfg).realize(
            world,
            fixture_intents(world, component_rng(cfg.seed, "fixture_intents")),
            (world.volumes[0].period_start, world.volumes[0].period_end),
            component_rng(cfg.seed, "realizer"),
        )
    )
    tables.update(events=[e for e, _ in events], event_truth=[t for _, t in events])
    tables["dropped_events"] = tables["events"][:1]
    tables["loss_counters"] = [
        dict(
            volume_id=world.volumes[0].volume_id,
            hour_start=world.volumes[0].period_start,
            offered=3,
            emitted=3,
            dropped=0,
            sampled_out=0,
            duplicates=0,
            late=0,
        )
    ]
    tables["patterns"] = [
        dict(
            pattern_id="pat-test",
            kind="weekly_schedule",
            volume_id=world.volumes[0].volume_id,
            active_from=world.volumes[0].period_start,
            active_to=None,
            actors=[],
            files=[],
            params={"weekday": 1},
        )
    ]
    return tables


@pytest.mark.parametrize("name", list(SCHEMAS))
def test_all_schemas_roundtrip(name, tmp_path):
    table = rows_to_table(name, sample_rows()[name])
    path = tmp_path / "table.parquet"
    pq.write_table(table, path)
    actual = pq.read_table(path)
    assert actual.equals(table)
    validate_rows(name, actual.to_pylist())
    assert rows_to_table(name, []).schema == SCHEMAS[name]


@pytest.mark.parametrize(
    "changes",
    [
        {"unexpected": 1},
        {"event_id": None},
        {"operation": "oops"},
        {"origin": "robot"},
        {"result": "success"},
        {"schema_version": 2},
        {"latency_ms": 1},
        {"cache_outcome": "hit"},
        {"file_size": True},
        {"event_time": datetime(2026, 1, 5)},
        {"event_time": "2026-01-05T01:00:00+01:00"},
        {"event_time": "2026-01-05T00:00:00.0000001Z"},
    ],
)
def test_strict_events(changes):
    row = sample_rows()["events"][0] | changes
    with pytest.raises(ValueError):
        validate_rows("events", [row])


def test_nullable_and_utc():
    row = sample_rows()["events"][0] | {"origin": None, "client_id": None}
    row["event_time"] = "2026-01-05T00:00:00.000000Z"
    assert rows_to_table("events", [row]).to_pylist()[0]["event_time"] == datetime(
        2026, 1, 5, tzinfo=UTC
    )


@pytest.mark.parametrize(
    "table,changes",
    [
        ("files", {"kind": "invalid"}),
        ("actors", {"kind": "invalid"}),
        ("actors", {"project_ids": [None]}),
        ("event_truth", {"faults": ["invented"]}),
        ("event_truth", {"original_fields_json": "[]"}),
        ("patterns", {"params": '{"b": 1, "a": 2}'}),
    ],
)
def test_catalog_and_truth_reject_invalid(table, changes):
    with pytest.raises(ValueError):
        validate_rows(table, [sample_rows()[table][0] | changes])
