"""Controlled small-preset recovery from emitted opens and catalog metadata.

Truth supplies planted parameters/instance selection only. These checks establish
synthetic pattern recovery, never performance or real-workload model quality.
"""

from collections import Counter
from dataclasses import replace
from datetime import timedelta

import numpy as np

from predictive_cache.datagen.config import load_config
from predictive_cache.datagen.patterns import NewFile, Popularity
from predictive_cache.datagen.pipeline import component_rng
from predictive_cache.datagen.stubs import StubRealizer
from predictive_cache.datagen.world import build_world


def fixture():
    cfg = load_config(preset="small")
    cfg.actors.vacation_weeks_per_quarter = 0
    cfg.projects.new_per_month = 0
    cfg.volumes[0].holidays = []
    cfg.patterns.popularity.recency_boost_half_life_days = 0
    cfg.patterns.popularity.working_set_share = cfg.patterns.popularity.revisit_share = 0
    world = build_world(cfg, component_rng(cfg.seed, "world"))
    start, end = world.volumes[0].period_start, world.volumes[0].period_end
    project = world.projects[0]
    # Isolate one well-populated project without changing small's files/users/weeks.
    world.actors[:] = [
        replace(
            a,
            project_ids=[project.project_id],
            schedule_start_local="08:00",
            schedule_end_local="18:00",
            holidays=[],
            vacations=[],
        )
        for a in world.actors
    ]
    return cfg, world, project, (start, end)


def open_rows(generator, key, cfg, world, span):
    truth = generator.plan(world, cfg, component_rng(cfg.seed, key))
    intents = generator.intents(world, span, component_rng(cfg.seed, key))
    opens = [
        e
        for e, _ in StubRealizer(cfg).realize(
            world, intents, span, component_rng(cfg.seed, "realizer")
        )
        if e["operation"] == "open"
    ]
    return opens, truth


def test_small_popularity_zipf_recovery():
    cfg, world, project, span = fixture()
    cfg.patterns.project_lifecycle.enabled = False
    opens, truth = open_rows(Popularity(), "popularity", cfg, world, span)
    # Rank using observed counts, never private planted file ranks.
    counts = sorted(Counter(e["file_id"] for e in opens).values(), reverse=True)
    assert len(counts) > 100 and len(opens) > 10000
    exponent = -np.polyfit(np.log(np.arange(1, len(counts) + 1)), np.log(counts), 1)[0]
    planted = next(
        t.params["zipf_s"] for t in truth if t.params["project_id"] == project.project_id
    )
    print(f"Zipf: fitted={exponent:.4f}, planted={planted}, opens={len(opens)}")
    assert abs(exponent - planted) <= 0.15


def test_small_lifecycle_recovery():
    cfg, world, project, span = fixture()
    start, end = span
    world.projects[0] = replace(
        project,
        started_at=start - timedelta(weeks=1),
        ramp_end=start,
        active_end=start + timedelta(weeks=2),
        cooldown_end=start + timedelta(weeks=6),
        phase_activity=(0.3, 1.0, 0.05, 0.02),
    )
    opens, _ = open_rows(Popularity(), "popularity", cfg, world, span)
    # Event counts, with equal weekday exposure across all complete weeks.
    weekly = np.zeros(cfg.period.weeks)
    for event in opens:
        weekly[(event["event_time"] - start).days // 7] += 1
    from predictive_cache.datagen.patterns import ProjectLifecycle

    lifecycle = ProjectLifecycle().plan(world, cfg, component_rng(cfg.seed, "lifecycle"))
    planted = next(t.params for t in lifecycle if t.params["project_id"] == project.project_id)
    # Reconstruct the truth curve independently rather than calling Project.activity.
    from datetime import datetime

    active_end = datetime.fromisoformat(planted["active_end"])
    cooldown_end = datetime.fromisoformat(planted["cooldown_end"])
    _, active, cooldown, archived = planted["phase_activity"]
    expected = []
    for week in range(cfg.period.weeks):
        values = []
        for day in range(5):
            at = start + timedelta(weeks=week, days=day, hours=12)
            if at < active_end:
                values.append(active)
            elif at < cooldown_end:
                fraction = (at - active_end) / (cooldown_end - active_end)
                values.append(active + fraction * (cooldown - active))
            else:
                values.append(archived)
        expected.append(sum(values))
    correlation = np.corrcoef(weekly, expected)[0, 1]
    print(f"Lifecycle: Pearson={correlation:.4f}, weekly_opens={weekly.tolist()}")
    assert np.std(expected) > 0 and correlation > 0.8


def average_ranks(values):
    _, inverse, counts = np.unique(values, return_inverse=True, return_counts=True)
    ends = np.cumsum(counts)
    return ((ends - counts + ends - 1) / 2)[inverse]


def test_small_new_file_decay_recovery():
    cfg, world, _, span = fixture()
    cfg.patterns.new_file.initial_opens_per_day = 24
    project_ids = [p.project_id for p in world.projects]
    world.actors[:] = [replace(a, project_ids=project_ids) for a in world.actors]
    opens, truth = open_rows(NewFile(), "new_file", cfg, world, span)
    new_ids = {fid for t in truth for fid in t.files}
    # Equal follow-up: only files with ten complete days before the observation end.
    births = {
        f.file_id: f.created_at
        for f in world.files
        if f.file_id in new_ids and f.created_at + timedelta(days=10) <= span[1]
    }
    daily = np.zeros(10)
    for event in opens:
        if event["file_id"] in births:
            age = (event["event_time"] - births[event["file_id"]]).days
            if 0 <= age < len(daily):
                daily[age] += 1
    correlation = np.corrcoef(np.arange(10), average_ranks(daily))[0, 1]
    print(
        f"New-file decay: Spearman={correlation:.4f}, daily_opens={daily.tolist()}, files={len(births)}"
    )
    assert len(births) > 100 and daily.sum() > 1000 and correlation < -0.5
