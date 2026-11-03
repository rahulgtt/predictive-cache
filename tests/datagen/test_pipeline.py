from dataclasses import replace
from datetime import timedelta

from predictive_cache.datagen.config import load_config
from predictive_cache.datagen.pipeline import component_rng, run
from predictive_cache.datagen.stubs import build_world


def scenario(seed=42):
    cfg = load_config(preset="tiny", seed=seed)
    cfg.period.weeks = 1
    cfg.projects.initial, cfg.projects.new_per_month = 2, 0
    cfg.files.target_count = 40
    cfg.actors.users, cfg.actors.teams = 4, 1
    cfg.patterns.new_file.new_files_per_project_week = 3
    for _, profile in cfg.files.size_distributions:
        profile.median_bytes, profile.min_bytes, profile.max_bytes = 4096, 100, 8192
    return cfg


def capture(monkeypatch, cfg):
    result = {}

    def writer(config, out_dir, tables, patterns, known_gaps, overwrite=False, **kwargs):
        result.update(tables=tables, patterns=patterns, known_gaps=known_gaps)
        return result

    monkeypatch.setattr("predictive_cache.datagen.io.write_dataset", writer)
    return run(cfg, "unused")


def test_seeded_nonempty_patterns(monkeypatch):
    cfg = scenario()
    first = capture(monkeypatch, cfg)
    assert first == capture(monkeypatch, cfg)
    other = capture(monkeypatch, scenario(seed=43))
    assert first != other
    assert set(first["tables"]) == {
        "events",
        "volumes",
        "directories",
        "files",
        "file_paths",
        "actors",
        "loss_counters",
        "event_truth",
        "dropped_events",
    }
    assert {r["operation"] for r in first["tables"]["events"]} >= {
        "open",
        "read",
        "close",
        "create",
        "write",
    }
    assert {p["kind"] for p in first["patterns"]} >= {
        "popularity",
        "project_lifecycle",
        "new_file",
        "noise",
    }
    for row in first["tables"]["events"]:
        assert row["cache_outcome"] is None and row["latency_ms"] is None
        assert "cause" not in row and "intent_id" not in row


def test_emit_and_read_controls(monkeypatch):
    cfg = scenario()
    cfg.patterns.new_file.enabled = cfg.patterns.churn.enabled = False
    cfg.realize.eacces_rate = cfg.realize.stale_open_rate = 0
    for fault in cfg.faults:
        fault[1].enabled = False
    cfg.realize.read_events = "none"
    cfg.emit.session_id = cfg.emit.cohort_id = cfg.emit.client_id = False
    result = capture(monkeypatch, cfg)
    events = result["tables"]["events"]
    assert len(events) > 0
    assert len(events) % 2 == 0
    assert {r["operation"] for r in events} == {"open", "close"}
    assert all(r[k] is None for r in events for k in ("session_id", "cohort_id", "client_id"))
    assert all(r["handle_bytes_read"] >= 0 for r in events if r["operation"] == "close")


def test_world_helpers():
    cfg = scenario()
    world = build_world(cfg, component_rng(cfg.seed, "world"))
    file = world.files[0]
    later = file.created_at + timedelta(hours=1)
    file = replace(file, size_history=[(later, 20)])
    assert file.size_at(file.created_at) == 1
    assert file.size_at(later) == 20
    actor = world.actors[0]
    assert actor.is_working(world.volumes[0].period_start + timedelta(hours=10))
    assert not actor.is_working(world.volumes[0].period_start)


def test_fault_switch_does_not_resample_catalog(monkeypatch):
    cfg = scenario()
    first = capture(monkeypatch, cfg)
    cfg.faults.clock_skew.enabled = False
    cfg.faults.duplicate.enabled = False
    second = capture(monkeypatch, cfg)
    for name in ("volumes", "files", "file_paths", "actors", "directories"):
        assert first["tables"][name] == second["tables"][name]
    assert first["tables"]["events"] != second["tables"]["events"]
    counters = first["tables"]["loss_counters"]
    assert len(counters) == cfg.period.weeks * 7 * 24
    assert all(c["offered"] == c["emitted"] + c["dropped"] + c["sampled_out"] for c in counters)
    assert sum(c["emitted"] + c["duplicates"] for c in counters) == len(first["tables"]["events"])
