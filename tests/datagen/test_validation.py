"""Rule-specific corruption tests; generated artifacts contain fictional data only."""

import json
import shutil
from datetime import timedelta

import pytest

from predictive_cache.datagen.catalog import catalog_rows
from predictive_cache.datagen.config import load_config
from predictive_cache.datagen.faults import Faults
from predictive_cache.datagen.io import canonical_json, sha256, write_dataset
from predictive_cache.datagen.model import Intent
from predictive_cache.datagen.pipeline import component_rng
from predictive_cache.datagen.realize import EventRealizer
from predictive_cache.datagen.stubs import build_world
from predictive_cache.datagen.validate import validate_dataset


def dataset(out, faulted=False, sampling=False):
    cfg = load_config(preset="tiny", format="jsonl")
    cfg.period.weeks = 1
    cfg.realize.eacces_rate = cfg.realize.stale_open_rate = 0
    cfg.realize.interactive_think_time_max_s = 0
    for name in type(cfg.faults).model_fields:
        getattr(cfg.faults, name).enabled = False
    if faulted:
        for name in ("unknown_origin", "invalid_range", "failed_result", "late", "duplicate"):
            getattr(cfg.faults, name).enabled = True
            getattr(cfg.faults, name).rate = 1
        cfg.faults.clock_skew.enabled = True
    if sampling:
        cfg.faults.sampling.enabled = True
        cfg.faults.sampling.rate = 0
    world = build_world(cfg, component_rng(42, "world"))
    volume = world.volumes[0]
    intents = [
        Intent(
            f"int-{index}",
            volume.period_start + timedelta(hours=8 + index),
            volume.volume_id,
            world.actors[0].actor_id,
            world.files[0].file_id,
            "office",
            "full",
            None,
            "background",
        )
        for index in range(6)
    ]
    clean = EventRealizer(cfg, churn=False).realize(
        world, intents, (volume.period_start, volume.period_end), component_rng(42, "realizer")
    )
    faults = Faults(cfg)
    records = list(faults.apply(clean, world, component_rng(42, "faults")))
    tables = catalog_rows(world)
    tables.update(
        events=[r.event for r in records if r.disposition in {"emitted", "duplicate"}],
        dropped_events=[r.event for r in records if r.disposition in {"dropped", "sampled_out"}],
        event_truth=list({r.event["event_id"]: r.truth for r in records}.values()),
        loss_counters=faults.counters,
    )
    write_dataset(cfg, out, tables, [], faults.known_gaps)
    return out


@pytest.fixture(scope="module")
def baseline(tmp_path_factory):
    return dataset(tmp_path_factory.mktemp("semantics") / "data")


def edit(out, prefix, mutate):
    manifest = json.loads((out / "manifest.json").read_text())
    entry = next(e for e in manifest["files"] if e["path"].startswith(prefix))
    path = out / entry["path"]
    rows = [json.loads(line) for line in path.read_text().splitlines()]
    mutate(rows)
    path.write_text("".join(canonical_json(row) + "\n" for row in rows))
    entry["sha256"] = sha256(path)
    entry["rows"] = len(rows)
    (out / "manifest.json").write_text(canonical_json(manifest))


def first(rows, operation):
    return next(r for r in rows if r["operation"] == operation)


@pytest.mark.parametrize(
    "rule",
    ["schema", "reference", "pairing", "totals", "range", "loss", "leakage", "truth", "hash"],
)
def test_validate_rejects_corruption_after_rehash(tmp_path, baseline, rule):
    out = tmp_path / "data"
    shutil.copytree(baseline, out)
    if rule == "schema":
        edit(out, "events/", lambda rows: first(rows, "read").update(operation="made_up"))
    elif rule == "reference":
        event_id = None

        def corrupt(rows):
            nonlocal event_id
            row = first(rows, "read")
            event_id = row["event_id"]
            row["file_id"] = "file-missing"

        edit(out, "events/", corrupt)
        edit(
            out,
            "truth/event_truth",
            lambda rows: next(r for r in rows if r["event_id"] == event_id).update(
                true_file_id="file-missing"
            ),
        )
    elif rule == "pairing":
        edit(out, "events/", lambda rows: first(rows, "close").update(handle_id="hdl-unmatched"))
    elif rule == "totals":
        edit(out, "events/", lambda rows: first(rows, "close").update(handle_bytes_read=100))
    elif rule == "range":
        edit(out, "events/", lambda rows: first(rows, "read").update(bytes_requested=0))
    elif rule == "loss":
        edit(out, "loss/loss_counters", lambda rows: rows[0].update(offered=1))
    elif rule == "leakage":
        edit(out, "events/", lambda rows: first(rows, "open").update(app="background"))
    elif rule == "truth":
        edit(out, "truth/event_truth", lambda rows: rows[0].update(true_file_id="file-invented"))
    else:
        path = next((out / "events").rglob("*.jsonl"))
        path.write_text(path.read_text() + " ")
    with pytest.raises(ValueError):
        validate_dataset(out)


def test_expected_faults_restore_and_missing_gaps_fail(tmp_path):
    out = dataset(tmp_path / "faulted", faulted=True)
    result = validate_dataset(out)
    assert result["checks"]["fault_restoration"] == "passed"
    assert result["expected_faults"]["invalid_range"] == 6
    assert result["handles"] == 6
    sampled = dataset(tmp_path / "sampled", sampling=True)
    assert validate_dataset(sampled)["clean_events"] == 18
    manifest = json.loads((sampled / "manifest.json").read_text())
    manifest["known_gaps"] = []
    (sampled / "manifest.json").write_text(canonical_json(manifest))
    with pytest.raises(ValueError, match="known gaps"):
        validate_dataset(sampled)


def test_fault_claim_without_original_values_fails(tmp_path):
    out = dataset(tmp_path / "faulted", faulted=True)
    edit(out, "truth/event_truth", lambda rows: rows[0].update(original_fields_json="{}"))
    with pytest.raises(ValueError, match="original fields"):
        validate_dataset(out)


@pytest.mark.parametrize("field,value", [("operation", "made_up"), ("event_id", None)])
def test_parquet_schema_corruption_is_rejected(tmp_path, baseline, field, value):
    import pyarrow as pa
    import pyarrow.parquet as pq

    from predictive_cache.datagen.io import read_table
    from predictive_cache.datagen.schema import SCHEMAS

    source = next((baseline / "events").rglob("*.jsonl"))
    rows = read_table(source, "events").to_pylist()
    rows[0][field] = value
    path = tmp_path / "corrupt.parquet"
    schema = SCHEMAS["events"]
    if value is None:
        # Parquet itself refuses nulls under a required field; corrupt its declaration too.
        schema = schema.set(
            schema.get_field_index(field), pa.field(field, schema.field(field).type)
        )
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), path)
    with pytest.raises(ValueError):
        read_table(path, "events")
