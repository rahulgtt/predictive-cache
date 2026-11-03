"""Behavioral checks of issue #15 patterns and their existing event bridge."""

from collections import Counter
from dataclasses import replace
from datetime import timedelta

import pytest

from predictive_cache.datagen.config import Config, load_config
from predictive_cache.datagen.patterns import NewFile, Noise, Popularity, ProjectLifecycle
from predictive_cache.datagen.pipeline import component_rng
from predictive_cache.datagen.stubs import StubRealizer
from predictive_cache.datagen.world import build_world


def setup(preset="tiny", seed=42):
    cfg = load_config(preset=preset, seed=seed)
    cfg.actors.vacation_weeks_per_quarter = 0
    world = build_world(cfg, component_rng(seed, "world"))
    return cfg, world


def window(world):
    return min(v.period_start for v in world.volumes), max(v.period_end for v in world.volumes)


def planned(cls, key, world, cfg):
    gen = cls()
    truth = gen.plan(world, cfg, component_rng(cfg.seed, key))
    return gen, truth


def rows(gen, key, world, cfg, span=None):
    return list(gen.intents(world, span or window(world), component_rng(cfg.seed, key)))


def realized_opens(intents, world, cfg):
    return [
        event
        for event, _ in StubRealizer(cfg).realize(
            world, iter(intents), window(world), component_rng(cfg.seed, "realizer")
        )
        if event["operation"] == "open"
    ]


@pytest.mark.parametrize("cls,key", [(Popularity, "popularity"), (NewFile, "new_file")])
def test_deterministic_ordered_and_chunk_invariant(cls, key):
    cfg, world = setup()
    gen, truth = planned(cls, key, world, cfg)
    all_rows = rows(gen, key, world, cfg)
    start, end = window(world)
    cut = start + timedelta(days=7, minutes=17)
    split = rows(gen, key, world, cfg, (start, cut)) + rows(gen, key, world, cfg, (cut, end))
    assert all_rows and split == all_rows
    assert all_rows == sorted(all_rows, key=lambda i: (i.at, i.intent_id))
    cfg2, world2 = setup()
    gen2, truth2 = planned(cls, key, world2, cfg2)
    assert truth == truth2
    assert all_rows == rows(gen2, key, world2, cfg2)
    cfg3, world3 = setup(seed=43)
    gen3, _ = planned(cls, key, world3, cfg3)
    assert all_rows != rows(gen3, key, world3, cfg3)


@pytest.mark.parametrize(
    "cls,key", [(Popularity, "popularity"), (NewFile, "new_file"), (Noise, "noise")]
)
def test_disabled_and_zero_rate(cls, key):
    cfg, world = setup()
    getattr(cfg.patterns, key).enabled = False
    gen, truth = planned(cls, key, world, cfg)
    assert not truth and not rows(gen, key, world, cfg)
    getattr(cfg.patterns, key).enabled = True
    cfg.randomness.rate_scale = 0
    gen, _ = planned(cls, key, world, cfg)
    assert not [i for i in rows(gen, key, world, cfg) if i.operation == "open"]


def test_noise_mixture_endpoints_rate_and_metadata_preservation():
    cfg, world = setup()
    new, _ = planned(NewFile, "new_file", world, cfg)
    base = rows(new, "new_file", world, cfg)
    pop, _ = planned(Popularity, "popularity", world, cfg)
    base = sorted(base + rows(pop, "popularity", world, cfg), key=lambda i: (i.at, i.intent_id))
    metadata = [i for i in base if i.operation != "open"]
    count = sum(i.operation == "open" for i in base)
    for share in (0.0, 0.2, 0.7, 1.0):
        cfg.randomness.noise_share = share
        noise = Noise(base)
        noise.plan(world, cfg, component_rng(cfg.seed, "noise"))
        mixed = rows(noise, "noise", world, cfg)
        assert len(mixed) == len(base)
        assert [i for i in mixed if i.operation != "open"] == metadata
        fraction = sum(i.cause == "noise" for i in mixed) / count
        assert abs(fraction - share) <= max(0.01, 0.2 * share)
        if share == 0:
            assert mixed == base
        cut = window(world)[0] + timedelta(days=7, minutes=13)
        assert mixed == (
            rows(noise, "noise", world, cfg, (window(world)[0], cut))
            + rows(noise, "noise", world, cfg, (cut, window(world)[1]))
        )
    cfg.patterns.noise.enabled = False
    noise = Noise(base)
    assert not noise.plan(world, cfg, component_rng(42, "noise"))
    assert rows(noise, "noise", world, cfg) == base


def test_noise_uniform_live_files_including_archived():
    cfg, world = setup()
    cfg.randomness.noise_share = 1
    cfg.patterns.popularity.enabled = cfg.patterns.new_file.enabled = False
    noise, _ = planned(Noise, "noise", world, cfg)
    result = rows(noise, "noise", world, cfg)
    files = {f.file_id: f for f in world.files}
    projects = {p.project_id: p for p in world.projects}
    assert result and all(i.cause == "noise" for i in result)
    assert any(
        files[i.file_id].project_id and projects[files[i.file_id].project_id].cooldown_end <= i.at
        for i in result
    )
    initial = [f for f in world.files if f.created_at <= window(world)[0]]
    counts = Counter(i.file_id for i in result if i.file_id in {f.file_id for f in initial})
    expected = sum(counts.values()) / len(initial)
    chi2 = sum((counts[f.file_id] - expected) ** 2 / expected for f in initial)
    assert chi2 < 2 * len(initial)


def test_new_file_catalog_metadata_and_liveness():
    cfg, world = setup()
    initial = len(world.files)
    gen, truth = planned(NewFile, "new_file", world, cfg)
    intents = rows(gen, "new_file", world, cfg)
    files = {f.file_id: f for f in world.files}
    assert len(world.files) == initial + len(truth)
    assert len({p.path for p in world.file_paths}) == len(world.file_paths)
    events = [
        e
        for e, _ in StubRealizer(cfg).realize(
            world, iter(intents), window(world), component_rng(42, "realizer")
        )
    ]
    for record in truth:
        fid = record.files[0]
        selected = [e for e in events if e["file_id"] == fid]
        assert sum(e["operation"] == "create" for e in selected) == 1
        assert sum(e["operation"] == "write" for e in selected) == 1
        assert all(e["event_time"] >= files[fid].created_at for e in selected)
        assert all(e["file_size"] == files[fid].size_at_creation for e in selected)
        assert all(
            e["handle_id"] is None for e in selected if e["operation"] in {"create", "write"}
        )


def test_multi_volume_order_isolation_and_dead_file_exclusion():
    cfg = load_config(preset="tiny")
    cfg.volumes.append(cfg.volumes[0].model_copy(update={"timezone": "Asia/Kolkata"}))
    cfg.patterns.new_file.new_files_per_project_week = 0.2
    world = build_world(cfg, component_rng(42, "world"))
    dead = world.files[0]
    world.files[0] = replace(dead, deleted_at=window(world)[0])
    for cls, key in ((NewFile, "new_file"), (Popularity, "popularity"), (Noise, "noise")):
        gen, _ = planned(cls, key, world, cfg)
        result = rows(gen, key, world, cfg)
        assert result == sorted(result, key=lambda i: (i.at, i.intent_id))
        files = {f.file_id: f for f in world.files}
        actors = {a.actor_id: a for a in world.actors}
        for i in result:
            assert i.volume_id == files[i.file_id].volume_id == actors[i.actor_id].volume_id
            assert i.file_id != dead.file_id
            assert i.at >= files[i.file_id].created_at


def test_calendar_zero_jitter_and_config_validation():
    cfg, world = setup()
    cfg.randomness.jitter_scale = 0
    cfg.calendar.holiday_activity = 0
    actor = world.actors[0]
    world.actors[:] = [
        replace(
            actor,
            holidays=[cfg.period.start],
            vacations=[
                (window(world)[0] + timedelta(days=1), window(world)[0] + timedelta(days=2))
            ],
        )
    ]
    gen, _ = planned(Popularity, "popularity", world, cfg)
    result = rows(gen, "popularity", world, cfg)
    assert result
    assert all(i.at.minute == 30 and i.at.second == 0 for i in result)
    assert all(world.actors[0].is_working(i.at) for i in result)
    data = cfg.model_dump()
    data["patterns"]["popularity"]["diurnal_curve"] = [(10.0, 1.0), (9.0, 0.0)]
    with pytest.raises(ValueError, match="diurnal"):
        Config.model_validate(data)
    data["patterns"]["popularity"]["diurnal_curve"] = [(0.0, 0.0), (24.0, 0.0)]
    cfg = Config.model_validate(data)
    gen, _ = planned(Popularity, "popularity", world, cfg)
    assert not rows(gen, "popularity", world, cfg)


def test_lifecycle_truth_and_disable():
    cfg, world = setup()
    gen, truth = planned(ProjectLifecycle, "lifecycle", world, cfg)
    assert len(truth) == len(world.projects)
    assert not rows(gen, "lifecycle", world, cfg)
    assert all(t.params["phase_activity"] for t in truth)
    cfg.patterns.project_lifecycle.enabled = False
    assert not gen.plan(world, cfg, component_rng(42, "lifecycle"))


def test_recency_uses_past_writes_and_lifecycle_can_amplify():
    cfg, world = setup()
    start, end = window(world)
    cfg.patterns.project_lifecycle.enabled = False
    cfg.patterns.popularity.zipf_s = 0.00001
    cfg.patterns.popularity.rate_per_user_hour = 40
    first = next(f for f in world.files if f.project_id is not None and f.created_at < start)
    second = next(f for f in world.files if f.file_id != first.file_id and f.created_at < start)
    world.files[:] = [
        replace(first, created_at=start - timedelta(days=100), size_history=[(start, 100)]),
        replace(
            second,
            project_id=first.project_id,
            created_at=start - timedelta(days=100),
            size_history=[(end + timedelta(days=1), 200)],
        ),
    ]
    world.actors[:] = [replace(a, project_ids=[first.project_id]) for a in world.actors[:2]]
    cfg.patterns.popularity.recency_boost_half_life_days = 3
    gen, _ = planned(Popularity, "popularity", world, cfg)
    short = (start, start + timedelta(days=1))
    boosted = Counter(i.file_id for i in rows(gen, "popularity", world, cfg, short))
    assert boosted[first.file_id] > 1.4 * boosted[second.file_id]
    cfg.patterns.popularity.recency_boost_half_life_days = 0
    gen, _ = planned(Popularity, "popularity", world, cfg)
    neutral = Counter(i.file_id for i in rows(gen, "popularity", world, cfg, short))
    assert 0.7 < neutral[first.file_id] / neutral[second.file_id] < 1.3
    cfg.patterns.project_lifecycle.enabled = True
    world.projects[:] = [
        replace(
            p,
            started_at=None,
            ramp_end=None,
            active_end=None,
            cooldown_end=None,
            activity_history=[(start, 2.0)],
        )
        for p in world.projects
    ]
    gen, _ = planned(Popularity, "popularity", world, cfg)
    amplified = rows(gen, "popularity", world, cfg, short)
    assert len(amplified) > 1.7 * sum(neutral.values())
