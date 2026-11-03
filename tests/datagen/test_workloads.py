"""Focused #16 behavior and planted-pattern recovery; fictional metadata only."""

from collections import Counter
from dataclasses import replace
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import numpy as np

from predictive_cache.datagen.config import load_config
from predictive_cache.datagen.patterns.common import intent
from predictive_cache.datagen.patterns.popularity import Popularity
from predictive_cache.datagen.patterns.workloads import WorkflowChain, Workloads
from predictive_cache.datagen.realize import EventRealizer
from predictive_cache.datagen.world import build_world


def component_rng(seed, component):
    keys = {"world": 1, "popularity": 101, "weekly_schedule": 102, "workflow_chain": 103}
    return np.random.default_rng(np.random.SeedSequence(seed, spawn_key=(keys[component],)))


def scenario(preset="tiny"):
    cfg = load_config(preset=preset)
    cfg.actors.vacation_weeks_per_quarter = 0
    cfg.projects.new_per_month = 0
    cfg.patterns.project_lifecycle.enabled = False
    world = build_world(cfg, component_rng(42, "world"))
    span = (world.volumes[0].period_start, world.volumes[0].period_end)
    return cfg, world, span


def opened(cfg, world, inputs):
    clean = cfg.model_copy(deep=True)
    clean.realize.read_events = "none"
    clean.realize.eacces_rate = 0
    span = (
        min(v.period_start for v in world.volumes),
        max([v.period_end for v in world.volumes] + [i.at + timedelta(seconds=1) for i in inputs]),
    )
    pairs = list(
        EventRealizer(clean, churn=False).realize(
            world,
            iter(sorted(inputs, key=lambda i: (i.at, i.intent_id))),
            span,
            np.random.default_rng(20),
        )
    )
    return [
        (event, truth["cause"])
        for event, truth in pairs
        if event["operation"] == "open" and event["result"] == "ok"
    ]


def test_small_schedules_recovery_holidays_batch_and_seed():
    cfg, world, span = scenario("small")
    gen = Workloads()
    truth = gen.plan(world, cfg, component_rng(42, "weekly_schedule"))
    output = list(gen.intents(world, span, component_rng(42, "weekly_schedule")))
    patterns = {r.pattern_id: r for r in truth}
    observed = opened(cfg, world, output)
    schedules = [(e, cause) for e, cause in observed if patterns[cause].kind == "weekly_schedule"]
    assert len(schedules) > 100
    near = 0
    for event, cause in schedules:
        record = patterns[cause]
        slots = [datetime.fromisoformat(t) for t in record.params["slots"]]
        near += min(abs((event["event_time"] - t).total_seconds()) for t in slots) <= 3600
        local = event["event_time"].astimezone(ZoneInfo(world.volumes[0].timezone))
        assert local.date() not in world.volumes[0].holidays
    assert near / len(schedules) >= 0.8
    print(f"P2 event slot recovery={near / len(schedules):.4f}, opens={len(schedules)}")
    jobs = [i for i in output if patterns[i.cause].kind == "batch_job"]
    assert jobs and all(
        i.app == "batch-indexer" and i.read_profile == "head" and i.job_run_id for i in jobs
    )
    assert all(
        i.at.astimezone(ZoneInfo(world.volumes[0].timezone)).strftime("%H:%M")
        == cfg.patterns.batch_job.local_time
        for i in jobs
    )
    again = Workloads()
    assert truth == again.plan(world, cfg, component_rng(42, "weekly_schedule"))
    assert output == list(again.intents(world, span, component_rng(42, "weekly_schedule")))
    cut = span[0] + timedelta(days=7, minutes=17)
    assert output == list(gen.intents(world, (span[0], cut), None)) + list(
        gen.intents(world, (cut, span[1]), None)
    )


def test_bursts_one_off_shared_sets_and_scans():
    cfg, world, span = scenario("small")
    cfg.patterns.burst.bursts_per_volume_month = 30
    cfg.patterns.burst.scan_share = 0
    cfg.patterns.burst.participation = 1
    cfg.patterns.burst.shared_file_share = 1
    gen = Workloads()
    truth = gen.plan(world, cfg, None)
    bursts = [r for r in truth if r.kind == "burst"]
    assert len(bursts) >= 20
    observed = opened(cfg, world, gen.items)
    onsets = [
        min(e["event_time"] for e, cause in observed if cause == r.pattern_id)
        for r in bursts
        if any(cause == r.pattern_id for _, cause in observed)
    ]
    # Use first observed open times, not planted onset timestamps.
    phases = (
        np.array([(at - span[0]).total_seconds() % (7 * 86400) for at in onsets])
        * 2
        * np.pi
        / (7 * 86400)
    )
    concentration = abs(np.mean(np.exp(1j * phases)))
    assert concentration < 0.5
    print(f"P5 observed-onset weekly concentration={concentration:.4f}, instances={len(onsets)}")
    multi = next(r for r in bursts if len(r.actors) > 1)
    selected = [i for i in gen.items if i.cause == multi.pattern_id]
    assert all(
        {i.file_id for i in selected if i.actor_id == actor} == set(multi.files)
        for actor in multi.actors
    )
    cfg.patterns.burst.participation = 0
    gen.plan(world, cfg, None)
    assert not [
        i for i in gen.items if i.cause in {r.pattern_id for r in gen.records if r.kind == "burst"}
    ]
    cfg.patterns.burst.scan_share = 1
    gen.plan(world, cfg, None)
    scans = {r.pattern_id for r in gen.records if r.kind == "burst"}
    assert scans and all(i.read_profile == "head" for i in gen.items if i.cause in scans)


def test_workflow_lift_context_interruptions_and_no_recursive_trigger():
    cfg, world, span = scenario("small")
    cfg.patterns.workflow_chain.context_order = 2
    cfg.patterns.workflow_chain.chains_per_project = 1
    cfg.patterns.workflow_chain.continue_p = 1
    cfg.patterns.workflow_chain.drift_share = 0
    cfg.patterns.workflow_chain.delay_sigma = 0
    cfg.patterns.workflow_chain.delay_median_s = 30
    cfg.patterns.workflow_chain.max_steps = 2
    cfg.patterns.workflow_chain.interruption_p = 1
    chain = WorkflowChain()
    truth = chain.plan(world, cfg, component_rng(42, "workflow_chain"))
    record = next(r for r in truth if len(r.params["entries"]) == 2 and r.actors)
    actor = next(a for a in world.actors if a.actor_id == record.actors[0])
    actor = replace(
        actor,
        work_days=list(range(1, 8)),
        schedule_start_local="00:00",
        schedule_end_local="23:59",
        holidays=[],
        vacations=[],
    )
    world.actors[:] = [actor if a.actor_id == actor.actor_id else a for a in world.actors]
    # Controlled two-step graph: test every planted edge, with explicit terminal nodes.
    edges = [
        e
        for e in record.params["edges"]
        if e["from"] in record.params["entries"] or e["from"] == record.files[1]
    ]
    record.params["edges"] = record.params["edges_after"] = edges
    chain.chains = [record]
    files = {f.file_id: f for f in world.files}
    inputs = []
    rng = np.random.default_rng(4)
    for index in range(100):
        entry = record.params["entries"][index % 2]
        inputs.append(
            intent(
                span[0] + timedelta(hours=10, minutes=index * 30), actor, files[entry], "entry", rng
            )
        )
    extra = chain.expand(world, inputs, rng, {"entry"})
    runs = {}
    for item in extra:
        runs.setdefault(item.workflow_run_id, []).append(item)
    assert len(runs) == 100
    observed = [e for e, _ in opened(cfg, world, inputs + extra)]
    populations = Counter(e["file_id"] for e in observed)
    sessions = {}
    for event in observed:
        sessions.setdefault((event["client_id"], event["session_id"]), []).append(event)
    lifts = []
    for edge in record.params["edges"]:
        if edge["p"] < 0.5:
            continue
        eligible = hits = 0
        for events in sessions.values():
            chain_events = sorted(
                [e for e in events if e["file_id"] in record.files],
                key=lambda e: (e["event_time"], e["event_id"]),
            )
            for index, event in enumerate(chain_events):
                previous = chain_events[index - 1]["file_id"] if index else None
                if event["file_id"] != edge["from"]:
                    continue
                if edge["previous"] is not None and previous != edge["previous"]:
                    continue
                eligible += 1
                hits += any(
                    future["file_id"] == edge["to"]
                    and 0 < (future["event_time"] - event["event_time"]).total_seconds() <= 900
                    for future in chain_events[index + 1 :]
                )
        assert eligible >= 50
        lift = (hits / eligible) / (populations[edge["to"]] / len(observed))
        assert lift >= 3
        lifts.append(lift)
    print(f"P3 event edge lifts={lifts}, opens={len(observed)}")
    assert all(len(items) == 4 for items in runs.values())  # 2 interruptions + 2 steps
    assert not chain.expand(world, extra, rng, {"entry"})
    cfg.patterns.workflow_chain.continue_p = 0
    assert not chain.expand(world, inputs, rng, {"entry"})
    cfg.patterns.workflow_chain.continue_p = 1
    cfg.patterns.workflow_chain.interruption_p = 0
    assert len(chain.expand(world, inputs, rng, {"entry"})) == 200


def test_personal_working_sets_revisits_and_chunk_history():
    cfg, world, span = scenario()
    cfg.patterns.popularity.working_set_size = 2
    cfg.patterns.popularity.working_set_share = 1
    cfg.patterns.popularity.revisit_share = 0
    gen = Popularity()
    gen.plan(world, cfg, component_rng(42, "popularity"))
    output = list(gen.intents(world, span, None))
    assert output
    files = {f.file_id: f for f in world.files}
    for item in output:
        working = gen.working_sets[item.actor_id]
        project_working = {
            fid for fid in working if files[fid].project_id == files[item.file_id].project_id
        }
        if project_working:
            assert item.file_id in project_working
    cut = span[0] + timedelta(days=7, minutes=17)
    assert output == list(gen.intents(world, (span[0], cut), None)) + list(
        gen.intents(world, (cut, span[1]), None)
    )
    cfg.patterns.popularity.revisit_share = 1
    gen = Popularity()
    gen.plan(world, cfg, component_rng(42, "popularity"))
    repeated = list(gen.intents(world, span, None))
    history = {}
    for item in repeated:
        previous = history.setdefault(item.actor_id, [])
        eligible = [
            fid for fid in previous if files[fid].project_id == files[item.file_id].project_id
        ]
        if eligible:
            assert item.file_id in eligible
        previous.append(item.file_id)
        history[item.actor_id] = previous[-cfg.patterns.popularity.revisit_history :]
