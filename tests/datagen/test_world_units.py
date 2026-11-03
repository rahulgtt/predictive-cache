"""Focused checks for configurable distributions and private world controls."""

from datetime import UTC, date, datetime, timedelta
from zoneinfo import ZoneInfo

import numpy as np
import pytest
from pydantic import ValidationError

from predictive_cache.datagen.config import Config, SizeDistribution, load_config
from predictive_cache.datagen.model import Actor, Project
from predictive_cache.datagen.world import build_world


@pytest.mark.parametrize(
    "overrides",
    [
        {"median_bytes": 0},
        {"median_bytes": float("inf")},
        {"median_bytes": float("nan")},
        {"sigma": -1},
        {"sigma": float("inf")},
        {"sigma": float("nan")},
        {"min_bytes": 0},
        {"min_bytes": 11},
        {"max_bytes": 0},
        {"max_bytes": 2**63},
        {"min_bytes": 1.5},
    ],
)
def test_size_distribution_rejects_invalid_values(overrides):
    values = {"median_bytes": 5, "sigma": 1.0, "min_bytes": 1, "max_bytes": 10}
    with pytest.raises(ValidationError):
        SizeDistribution.model_validate(values | overrides)


def test_partial_yaml_size_overrides_use_defaults_and_clipping(tmp_path):
    path = tmp_path / "config.yaml"
    path.write_text(
        "preset: tiny\nfiles:\n  size_distributions:\n    doc: {median_bytes: 100, sigma: 0.0}\n"
    )
    cfg = load_config(path)
    assert cfg.files.size_distributions.doc.min_bytes == 4096
    world = build_world(cfg, np.random.default_rng(8))
    docs = [file for file in world.files if file.kind == "doc"]
    assert docs
    assert {file.size_at_creation for file in docs} == {4096}


def test_project_phase_interpolation_and_legacy_step_history():
    start = datetime(2026, 1, 1, tzinfo=UTC)
    week = timedelta(weeks=1)
    project = Project(
        "project",
        "volume",
        started_at=start,
        ramp_end=start + week,
        active_end=start + 3 * week,
        cooldown_end=start + 5 * week,
        phase_activity=(0.2, 1.0, 0.4, 0.03),
    )
    assert project.activity(start - timedelta(microseconds=1)) == 0
    assert project.activity(start) == 0.2
    assert project.activity(start + week / 2) == pytest.approx(0.6)
    assert project.activity(start + 2 * week) == 1
    assert project.activity(start + 4 * week) == pytest.approx(0.7)
    assert project.activity(start + 5 * week) == 0.03
    legacy = Project("legacy", "volume", [(start, 0.4), (start + week, 0.8)])
    assert legacy.activity(start - week) == 0
    assert legacy.activity(start + week / 2) == 0.4
    assert legacy.activity(start + week) == 0.8


def test_actor_calendar_boundaries_dst_and_vacations():
    zone = ZoneInfo("America/New_York")
    vacation_start = datetime(2026, 3, 16, 9, tzinfo=zone)
    vacation_end = vacation_start + timedelta(weeks=1)
    actor = Actor(
        "actor",
        "volume",
        "user",
        None,
        None,
        [],
        "09:00",
        "17:00",
        [1, 2, 3, 4, 5],
        "America/New_York",
        [date(2026, 3, 10)],
        [(vacation_start.astimezone(UTC), vacation_end.astimezone(UTC))],
    )
    before = datetime(2026, 3, 6, 9, tzinfo=zone)
    after = datetime(2026, 3, 9, 9, tzinfo=zone)
    assert before.utcoffset() != after.utcoffset()
    for at in (before, after):
        assert actor.is_working(at.astimezone(UTC))
        assert not actor.is_working(at - timedelta(microseconds=1))
        assert not actor.is_working(at.replace(hour=17))
    assert not actor.is_working(datetime(2026, 3, 10, 12, tzinfo=zone))
    assert not actor.is_working(vacation_start)
    assert not actor.is_working(vacation_end - timedelta(microseconds=1))
    assert actor.is_working(vacation_end)


def test_world_controls_and_private_sets_are_stable():
    cfg = load_config(preset="tiny")
    values = cfg.model_dump()
    values["actors"]["vacation_weeks_per_quarter"] = 13.0
    values["projects"]["new_per_month"] = 10.0
    cfg = Config.model_validate(values)
    world = build_world(cfg, np.random.default_rng(17))
    assert world == build_world(cfg, np.random.default_rng(17))
    users = [actor for actor in world.actors if actor.kind == "user"]
    services = [actor for actor in world.actors if actor.kind == "service"]
    assert any(actor.vacations for actor in users)
    for actor in users:
        assert actor.schedule_start_local < actor.schedule_end_local
        assert sum(actor.app_weights.values()) == pytest.approx(1)
        assert actor.working_set_file_ids == []
        for start, end in actor.vacations:
            local_start = start.astimezone(ZoneInfo(actor.timezone))
            local_end = end.astimezone(ZoneInfo(actor.timezone))
            assert (local_end - local_start).days in (7, 14)
    assert services
    assert all(not actor.vacations and not actor.holidays for actor in services)
    assert any(p.started_at > world.volumes[0].period_start for p in world.projects)
