"""Issue #14 acceptance checks independent of builder implementation details."""

from dataclasses import replace
from datetime import UTC, date, datetime, time, timedelta
from math import exp
from zoneinfo import ZoneInfo

import numpy as np
import pytest

from predictive_cache.datagen.catalog import catalog_rows
from predictive_cache.datagen.config import Config, Volume, load_config
from predictive_cache.datagen.io import read_table, write_table
from predictive_cache.datagen.pipeline import component_rng, run
from predictive_cache.datagen.schema import SCHEMAS, rows_to_table
from predictive_cache.datagen.validate import validate_dataset
from predictive_cache.datagen.world import build_world


@pytest.fixture(scope="module")
def small_world():
    cfg = load_config(preset="small")
    return cfg, build_world(cfg, component_rng(cfg.seed, "world"))


def test_small_counts_and_catalog_integrity(small_world):
    cfg, world = small_world
    assert len(world.files) == cfg.files.target_count
    assert sum(a.kind == "user" for a in world.actors) == cfg.actors.users
    assert sum(a.kind == "service" for a in world.actors) == cfg.actors.service
    volumes = {v.volume_id: v for v in world.volumes}
    dirs = {d.dir_id: d for d in world.directories}
    files = {f.file_id: f for f in world.files}
    projects = {p.project_id: p for p in world.projects}
    assert len(dirs) == len(world.directories)
    assert len(files) == len(world.files)
    assert len(
        {(f.file_id, p.valid_from) for p in world.file_paths for f in [files[p.file_id]]}
    ) == len(world.file_paths)
    assert len({(files[p.file_id].volume_id, p.path) for p in world.file_paths}) == len(
        world.file_paths
    )
    assert len(world.file_paths) == len(files)
    for directory in world.directories:
        assert directory.volume_id in volumes
        if directory.parent_dir_id is not None:
            parent = dirs[directory.parent_dir_id]
            assert parent.volume_id == directory.volume_id
            assert parent.depth + 1 == directory.depth
            assert parent.created_at <= directory.created_at
    for path in world.file_paths:
        file = files[path.file_id]
        directory = dirs[path.dir_id]
        assert file.volume_id == directory.volume_id
        assert file.created_at >= directory.created_at
        assert path.valid_from == file.created_at
        assert path.path.startswith("/") and ".." not in path.path.split("/")
        assert "/home/" not in path.path and "/Users/" not in path.path
        if file.project_id:
            assert file.created_at >= projects[file.project_id].started_at
    for name, rows in catalog_rows(world).items():
        assert rows_to_table(name, rows).schema == SCHEMAS[name]
        assert all("working_set_file_ids" not in r and "app_weights" not in r for r in rows)


@pytest.mark.parametrize("fmt", ["jsonl", "parquet"])
def test_all_catalog_formats_roundtrip(small_world, tmp_path, fmt):
    _, world = small_world
    for name, rows in catalog_rows(world).items():
        out = tmp_path / f"{name}.{fmt}"
        write_table(out, name, rows)
        assert read_table(out, name).equals(rows_to_table(name, rows))


def test_size_distributions_small(small_world):
    cfg, world = small_world
    for kind, share in cfg.files.kinds.model_dump().items():
        values = np.array([f.size_at_creation for f in world.files if f.kind == kind])
        profile = getattr(cfg.files.size_distributions, kind)
        assert abs(len(values) / len(world.files) - share) < 0.03
        assert len(values) >= 100
        assert values.min() >= profile.min_bytes
        assert values.max() <= profile.max_bytes
        expected = np.clip(profile.median_bytes, profile.min_bytes, profile.max_bytes)
        assert abs(np.median(values) / expected - 1) < 0.25
        for q, z in [(0.1, -1.2815515655), (0.9, 1.2815515655)]:
            expected = np.clip(
                profile.median_bytes * exp(z * profile.sigma), profile.min_bytes, profile.max_bytes
            )
            assert abs(np.quantile(values, q) / expected - 1) < 0.35
    assert any(f.kind == "cad_model" and f.size_at_creation >= 100 * 1024**2 for f in world.files)
    assert any(f.kind == "dataset" and f.size_at_creation >= 1024**3 for f in world.files)


def test_size_override_is_used():
    data = load_config(preset="tiny").model_dump(mode="json")
    for kind in data["files"]["size_distributions"]:
        data["files"]["size_distributions"][kind] = {
            "median_bytes": 4096,
            "min_bytes": 4096,
            "max_bytes": 4096,
            "sigma": 0.0,
        }
    cfg = Config.model_validate(data)
    world = build_world(cfg, component_rng(cfg.seed, "world"))
    assert {f.size_at_creation for f in world.files} == {4096}


def test_actors_teams_and_profiles(small_world):
    cfg, world = small_world
    projects = {p.project_id: p for p in world.projects}
    teams = {}
    for actor in world.actors:
        assert all(projects[p].volume_id == actor.volume_id for p in actor.project_ids)
        if actor.kind == "user":
            assert 1 <= len(actor.project_ids) <= 3
            if actor.cohort_id:
                pair = (actor.volume_id, actor.cohort_id)
                assert teams.setdefault(pair, actor.project_ids) == actor.project_ids
            assert actor.working_set_file_ids == []
            assert actor.app_weights and sum(actor.app_weights.values()) == pytest.approx(1)
        else:
            assert actor.vacations == []
    for f in world.files:
        expected = {"dataset": {"ranges"}, "media": {"head", "full"}}.get(f.kind, {"full"})
        assert f.read_profile in expected
    media = [f for f in world.files if f.kind == "media"]
    assert 0.4 < sum(f.read_profile == "head" for f in media) / len(media) < 0.8


def test_dst_local_schedule_and_absences():
    cfg = load_config(preset="medium")
    world = build_world(cfg, component_rng(cfg.seed, "world"))
    actor = next(a for a in world.actors if a.kind == "user" and a.timezone == "America/New_York")
    actor = replace(actor, holidays=[], vacations=[])
    tz = ZoneInfo(actor.timezone)
    hour = time.fromisoformat(actor.schedule_start_local)
    before = datetime.combine(date(2026, 3, 6), hour, tz) + timedelta(minutes=1)
    after = datetime.combine(date(2026, 3, 9), hour, tz) + timedelta(minutes=1)
    assert actor.is_working(before.astimezone(UTC))
    assert actor.is_working(after.astimezone(UTC))
    assert before.utcoffset() != after.utcoffset()
    assert before.hour == after.hour
    end = datetime.combine(after.date(), time.fromisoformat(actor.schedule_end_local), tz)
    assert not actor.is_working(end)
    assert not replace(actor, holidays=[after.date()]).is_working(after)
    assert not replace(
        actor, vacations=[(after - timedelta(days=1), after + timedelta(days=1))]
    ).is_working(after)
    v = next(v for v in world.volumes if v.volume_id == actor.volume_id)
    assert v.period_start.astimezone(tz).time() == time()
    assert v.period_end.astimezone(tz).time() == time()


def test_project_lifecycle_and_new_births(small_world):
    _, world = small_world
    volumes = {v.volume_id: v for v in world.volumes}
    assert any(p.started_at < volumes[p.volume_id].period_start for p in world.projects)
    assert any(
        volumes[p.volume_id].period_start < p.started_at < volumes[p.volume_id].period_end
        for p in world.projects
    )
    for p in world.projects:
        assert p.started_at < p.ramp_end < p.active_end < p.cooldown_end
        assert p.activity(p.started_at - timedelta(seconds=1)) == 0
        assert p.activity(p.ramp_end) == pytest.approx(p.phase_activity[1])
        midpoint = p.started_at + (p.ramp_end - p.started_at) / 2
        assert p.activity(midpoint) == pytest.approx(
            (p.phase_activity[0] + p.phase_activity[1]) / 2
        )
        assert p.activity(p.cooldown_end + timedelta(seconds=1)) == pytest.approx(
            p.phase_activity[3]
        )


def test_seed_determinism_and_sparse_pipeline(tmp_path):
    cfg = load_config(preset="tiny")
    assert build_world(cfg, component_rng(42, "world")) == build_world(
        cfg, component_rng(42, "world")
    )
    assert build_world(cfg, component_rng(42, "world")) != build_world(
        cfg, component_rng(43, "world")
    )
    cfg.volumes = [Volume(timezone="UTC"), Volume(timezone="America/New_York")]
    cfg.actors.users = 3
    cfg.actors.teams = 1
    cfg.actors.service = 0
    cfg.files.target_count = 1
    cfg.projects.initial = 1
    cfg.projects.new_per_month = 0
    world = build_world(cfg, component_rng(42, "world"))
    assert len(world.files) == 1 and len(world.actors) == 3
    cohorts = {(a.volume_id, a.cohort_id) for a in world.actors if a.cohort_id}
    assert len(cohorts) == 1
    assert any(a.cohort_id is None for a in world.actors)
    run(cfg, tmp_path / "sparse")
    assert validate_dataset(tmp_path / "sparse")["data_kind"] == "synthetic"


def test_future_project_membership_preserves_deferred_working_set_interface():
    cfg = load_config(preset="tiny")
    cfg.projects.initial = 1
    cfg.projects.new_per_month = 30.0
    cfg.actors.teams = 10
    cfg.actors.users = 30
    world = build_world(cfg, component_rng(cfg.seed, "world"))
    start = world.volumes[0].period_start
    future = {p.project_id for p in world.projects if p.started_at > start}
    assert future
    assert any(future.intersection(a.project_ids) for a in world.actors if a.kind == "user")
    assert all(a.working_set_file_ids == [] for a in world.actors)
