"""Focused read/session/churn invariants for synthetic event realization."""

from copy import deepcopy
from dataclasses import replace
from datetime import timedelta

import numpy as np
import pytest

from predictive_cache.datagen.config import load_config
from predictive_cache.datagen.model import Intent
from predictive_cache.datagen.realize import EventRealizer
from predictive_cache.datagen.schema import validate_rows
from predictive_cache.datagen.stubs import build_world


def fixture():
    cfg = load_config(preset="tiny")
    cfg.realize.eacces_rate = 0
    cfg.realize.stale_open_rate = 0
    cfg.realize.read_mbps_sigma = 0
    cfg.realize.interactive_think_time_max_s = 0
    cfg.realize.service_think_time_max_s = 0
    world = build_world(cfg, np.random.default_rng(1))
    world.files[0] = replace(world.files[0], size_at_creation=10000)
    start = world.volumes[0].period_start
    return cfg, world, (start, world.volumes[0].period_end)


def item(world, at, identifier="int-a", **kwargs):
    file, actor = world.files[0], world.actors[0]
    return Intent(
        identifier,
        at,
        file.volume_id,
        actor.actor_id,
        file.file_id,
        "office",
        kwargs.pop("read_profile", "full"),
        kwargs.pop("head_bytes", None),
        "background",
        **kwargs,
    )


def run(cfg, world, intents, window, realizer=None):
    return list(
        (realizer or EventRealizer(cfg, churn=False)).realize(
            world, iter(intents), window, np.random.default_rng(7)
        )
    )


@pytest.mark.parametrize("profile,head", [("full", None), ("head", 1234), ("ranges", None)])
def test_range_bounds_totals_summary_and_determinism(profile, head):
    cfg, world, span = fixture()
    cfg.realize.max_read_event_bytes = 1024
    intents = [item(world, span[0], read_profile=profile, head_bytes=head)]
    pairs = run(cfg, deepcopy(world), intents, span)
    assert pairs == run(cfg, deepcopy(world), intents, span)
    events = [e for e, _ in pairs]
    validate_rows("events", events)
    validate_rows("event_truth", [t for _, t in pairs])
    reads = [e for e in events if e["operation"] == "read"]
    assert all(0 <= r["offset"] < r["offset"] + r["bytes_read"] <= r["file_size"] for r in reads)
    assert all(r["bytes_read"] <= 1024 for r in reads)
    assert events[0]["operation"] == "open" and events[-1]["operation"] == "close"
    assert events[-1]["handle_bytes_read"] == sum(r["bytes_read"] for r in reads)
    assert events[-1]["event_time"] > events[0]["event_time"]
    if profile == "full":
        assert events[-1]["handle_bytes_read"] == 10000
    elif profile == "head":
        assert events[-1]["handle_bytes_read"] == 1234
    cfg.realize.read_events = "none"
    summary = run(cfg, deepcopy(world), intents, span)
    assert summary == [(e, t) for e, t in pairs if e["operation"] != "read"]


def test_actor_sessions_job_runs_and_failed_opens():
    cfg, world, span = fixture()
    first = world.actors[0]
    second = replace(first, actor_id="act-b", client_id="act-b")
    service = replace(first, actor_id="act-service", client_id="act-service", kind="service")
    world.actors.extend([second, service])
    intents = [
        item(world, span[0], "i0"),
        replace(item(world, span[0] + timedelta(seconds=30), "i1"), actor_id=second.actor_id),
        item(world, span[0] + timedelta(minutes=1), "i2"),
        item(world, span[0] + timedelta(minutes=12), "i3"),
        replace(item(world, span[0], "i4", job_run_id="j1"), actor_id=service.actor_id),
        replace(
            item(world, span[0] + timedelta(hours=1), "i5", job_run_id="j1"),
            actor_id=service.actor_id,
        ),
        replace(
            item(world, span[0] + timedelta(hours=2), "i6", job_run_id="j2"),
            actor_id=service.actor_id,
        ),
    ]
    pairs = run(cfg, deepcopy(world), intents, span)
    opens = {t["intent_id"]: e for e, t in pairs if e["operation"] == "open"}
    assert opens["i0"]["session_id"] == opens["i2"]["session_id"]
    assert len({opens[i]["session_id"] for i in ("i0", "i1", "i3")}) == 3
    assert opens["i4"]["session_id"] == opens["i5"]["session_id"] != opens["i6"]["session_id"]
    cfg.realize.eacces_rate = 1
    failed = run(cfg, deepcopy(world), intents, span)
    assert len(failed) == len(intents)
    assert all(e["operation"] == "open" and e["result"] == "eacces" for e, _ in failed)
    cfg.emit.session_id = cfg.emit.cohort_id = cfg.emit.client_id = False
    assert all(
        e[k] is None
        for e, _ in run(cfg, deepcopy(world), intents, span)
        for k in ("session_id", "cohort_id", "client_id")
    )


class ScriptedChurn(EventRealizer):
    def __init__(self, cfg, proposals):
        super().__init__(cfg)
        self.proposals = proposals

    def plan_churn(self, world, window, rng):
        return self.proposals


def test_interleaved_write_rename_delete_defer_until_close():
    cfg, world, span = fixture()
    cfg.realize.read_mbps_median = 1
    cfg.realize.max_read_event_bytes = 2**20
    cfg.realize.stale_open_rate = 1
    world.files[0] = replace(world.files[0], size_at_creation=8 * 2**20)
    fid = world.files[0].file_id
    proposals = [
        item(world, span[0] + timedelta(seconds=s), f"meta-{op}", operation=op)
        for s, op in ((3, "write"), (4, "rename"), (5, "delete"))
    ]
    intents = [
        item(world, span[0] + timedelta(seconds=1), "start"),
        item(world, span[0] + timedelta(seconds=20), "stale"),
    ]
    pairs = run(cfg, world, intents, span, ScriptedChurn(cfg, proposals))
    events = [e for e, _ in pairs]
    close = next(e for e in events if e["operation"] == "close")
    assert all(
        e["event_time"] > close["event_time"]
        for e in events
        if e["operation"] in {"write", "delete"}
    )
    rename = next(e for e in events if e["operation"] == "rename")
    assert rename["file_id"] == fid
    assert len(world.file_paths) == 2
    assert world.file_paths[0].valid_to == rename["event_time"]
    assert world.files[0].size_history and world.files[0].deleted_at
    assert events[-1]["operation"] == "open" and events[-1]["result"] == "enoent"
    assert all(e["file_size"] == 8 * 2**20 for e in events if e["operation"] == "read")


def test_replacement_identity_path_and_historical_origin_reads():
    cfg, world, span = fixture()
    old = world.files[0]
    path = world.file_paths[0].path
    replacement = item(world, span[0] + timedelta(seconds=5), "replace", operation="replace")
    pairs = run(
        cfg,
        world,
        [item(world, span[0], "before"), item(world, span[0] + timedelta(seconds=6), "after")],
        span,
        ScriptedChurn(cfg, [replacement]),
    )
    old, new = world.files
    assert old.replaced_by_file_id == new.file_id and old.file_id != new.file_id
    assert world.file_paths[-1].path == path
    after = [e for e, t in pairs if t["intent_id"] == "after"]
    assert after and all(e["file_id"] == new.file_id for e in after)
    before = deepcopy(world)
    history = run(cfg, world, [item(world, span[0] + timedelta(seconds=1))], span)
    assert history and history[0][0]["file_id"] == old.file_id
    assert world == before


def test_churn_seed_and_handle_spill():
    cfg, world, span = fixture()
    cfg.patterns.churn.writes_per_file_month = 20
    cfg.patterns.churn.rename_rate = 20
    cfg.patterns.churn.delete_rate = cfg.patterns.churn.replace_rate = 0
    intents = [item(world, span[1] - timedelta(microseconds=1))]
    first, second = deepcopy(world), deepcopy(world)
    a = run(cfg, first, intents, span, EventRealizer(cfg))
    b = run(cfg, second, intents, span, EventRealizer(cfg))
    assert a == b and first == second
    assert any(e["operation"] == "write" for e, _ in a)
    assert any(e["operation"] == "rename" for e, _ in a)
    assert max(e["event_time"] for e, _ in a) > span[1]
