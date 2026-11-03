"""Seeded synthetic world metadata; no modeled paths or file contents are created."""

from __future__ import annotations

from collections import deque
from datetime import UTC, datetime, time, timedelta
from math import ceil, exp, log
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np

from .config import Config
from .model import Actor, Directory, File, FilePath, Project, Volume, World


def _id(prefix: str, rng: np.random.Generator) -> str:
    return f"{prefix}-{rng.bytes(12).hex()}"


def _share(total: int, index: int, count: int) -> int:
    quotient, remainder = divmod(total, count)
    return quotient + (index < remainder)


def _projects(cfg: Config, volume: Volume, rng: np.random.Generator) -> list[Project]:
    result = []
    settings = cfg.projects
    new_count = int(rng.poisson(settings.new_per_month * cfg.period.weeks * 7 / 30.4375))
    for index in range(settings.initial + new_count):
        durations = [
            int(rng.integers(low, high + 1))
            for low, high in (
                settings.lifecycle_weeks.ramp,
                settings.lifecycle_weeks.active,
                settings.lifecycle_weeks.cooldown,
            )
        ]
        if index < settings.initial:
            # Ages cover all phases, including already archived projects.
            age_days = int(rng.integers(1, 7 * (sum(durations) + 2) + 1))
            start = volume.period_start - timedelta(days=age_days)
        else:
            seconds = (volume.period_end - volume.period_start).total_seconds()
            start = volume.period_start + timedelta(seconds=float(rng.uniform(0, seconds)))
        ramp_end = start + timedelta(weeks=durations[0])
        active_end = ramp_end + timedelta(weeks=durations[1])
        cooldown_end = active_end + timedelta(weeks=durations[2])
        activity = settings.lifecycle_activity
        result.append(
            Project(
                _id("proj", rng),
                volume.volume_id,
                started_at=start,
                ramp_end=ramp_end,
                active_end=active_end,
                cooldown_end=cooldown_end,
                phase_activity=(
                    activity.ramp,
                    activity.active,
                    activity.cooldown,
                    activity.archived,
                ),
            )
        )
    return result


def _directories(
    cfg: Config,
    volume: Volume,
    projects: list[Project],
    target: int,
    words: tuple[str, ...],
    rng: np.random.Generator,
) -> tuple[list[Directory], dict[str, str], list[Directory]]:
    # Volume root predates every child. Shared files are long-lived too.
    birth = min(p.started_at for p in projects)
    root = Directory(_id("dir", rng), volume.volume_id, None, None, 0, birth)
    shared = Directory(_id("dir", rng), volume.volume_id, root.dir_id, None, 1, birth)
    directories = [root, shared]
    paths = {root.dir_id: "", shared.dir_id: "/shared"}
    candidates = []
    for name in ("templates", "standards"):
        directory = Directory(_id("dir", rng), volume.volume_id, shared.dir_id, None, 2, birth)
        directories.append(directory)
        candidates.append(directory)
        paths[directory.dir_id] = f"/shared/{name}"
    frontier = deque()
    for index, project in enumerate(projects):
        directory = Directory(
            _id("dir", rng),
            volume.volume_id,
            root.dir_id,
            project.project_id,
            1,
            project.started_at,
        )
        directories.append(directory)
        candidates.append(directory)
        frontier.append(directory)
        paths[directory.dir_id] = f"/{words[int(rng.integers(len(words)))]}-{index:04d}"
    # Mandatory project/shared dirs can exceed the occupancy-derived budget.
    budget = max(len(candidates), ceil(target / cfg.files.per_dir_mean))
    while frontier and len(candidates) < budget:
        parent = frontier.popleft()
        if parent.depth >= cfg.projects.max_depth + 1:
            continue
        children = min(int(rng.poisson(cfg.projects.branching_mean)), budget - len(candidates))
        for _ in range(children):
            directory = Directory(
                _id("dir", rng),
                volume.volume_id,
                parent.dir_id,
                parent.project_id,
                parent.depth + 1,
                parent.created_at,
            )
            name = f"{words[int(rng.integers(len(words)))]}-{len(directories):04d}"
            paths[directory.dir_id] = f"{paths[parent.dir_id]}/{name}"
            directories.append(directory)
            candidates.append(directory)
            frontier.append(directory)
    return directories, paths, candidates


def _files(
    cfg: Config,
    volume: Volume,
    target: int,
    candidates: list[Directory],
    directory_paths: dict[str, str],
    words: tuple[str, ...],
    rng: np.random.Generator,
) -> tuple[list[File], list[FilePath]]:
    # Poisson occupancy weights followed by multinomial allocation enforce the
    # exact target. Final occupancies are dependent, not independent Poissons.
    weights = rng.poisson(cfg.files.per_dir_mean, len(candidates)).astype(float)
    if weights.sum() == 0:
        weights[:] = 1
    occupancies = rng.multinomial(target, weights / weights.sum())
    kinds = tuple(cfg.files.kinds.model_dump())
    shares = tuple(cfg.files.kinds.model_dump().values())
    extensions = {
        "doc": "docx",
        "drawing": "pdf",
        "cad_model": "cad",
        "image": "png",
        "media": "mp4",
        "dataset": "bin",
        "small": "txt",
    }
    files, paths = [], []
    for directory, count in zip(candidates, occupancies, strict=True):
        for _ in range(int(count)):
            kind = str(rng.choice(kinds, p=shares))
            profile = getattr(cfg.files.size_distributions, kind)
            # Clip in log space to avoid overflow for valid extreme sigma.
            sampled = float(rng.normal(log(profile.median_bytes), profile.sigma))
            size = min(
                profile.max_bytes,
                max(
                    profile.min_bytes,
                    round(exp(min(log(profile.max_bytes), max(log(profile.min_bytes), sampled)))),
                ),
            )
            read_profile = "ranges" if kind == "dataset" else "full"
            if kind == "media" and rng.random() < 0.6:
                read_profile = "head"
            file = File(
                _id("file", rng),
                volume.volume_id,
                kind,
                size,
                directory.created_at,
                project_id=directory.project_id,
                read_profile=read_profile,
            )
            name = f"{words[int(rng.integers(len(words)))]}-{len(files):06d}.{extensions[kind]}"
            files.append(file)
            paths.append(
                FilePath(
                    file.file_id,
                    directory.dir_id,
                    f"{directory_paths[directory.dir_id]}/{name}",
                    file.created_at,
                )
            )
    return files, paths


def _hhmm(minutes: int) -> str:
    return f"{minutes // 60:02d}:{minutes % 60:02d}"


def _schedule(volume: Volume, rng: np.random.Generator) -> tuple[str, str]:
    def minutes(value: str) -> int:
        hour, minute = map(int, value.split(":"))
        return hour * 60 + minute

    start = max(0, min(1438, round(rng.normal(minutes(volume.work_start_local), 30))))
    end = max(start + 1, min(1439, round(rng.normal(minutes(volume.work_end_local), 45))))
    return _hhmm(start), _hhmm(end)


def _vacations(cfg: Config, volume: Volume, rng: np.random.Generator):
    # Interpret the control as expected vacation weeks per 13-week quarter.
    # Uniform 1–2-week blocks average 1.5 weeks. Retain full blocks at boundaries.
    mean = cfg.actors.vacation_weeks_per_quarter * cfg.period.weeks / 13 / 1.5
    blocks = []
    timezone = ZoneInfo(volume.timezone)
    for _ in range(int(rng.poisson(mean))):
        day = cfg.period.start + timedelta(days=int(rng.integers(cfg.period.weeks * 7)))
        start = datetime.combine(day, time(), timezone)
        end = start + timedelta(weeks=int(rng.integers(1, 3)))
        blocks.append((start.astimezone(UTC), end.astimezone(UTC)))
    return sorted(blocks)


def _actors(
    cfg: Config,
    volume: Volume,
    projects: list[Project],
    files: list[File],
    users: int,
    teams: int,
    services: int,
    rng: np.random.Generator,
) -> list[Actor]:
    # Membership may include future projects; working sets below include only
    # currently live files. Later patterns must check project/file availability.
    project_ids = [p.project_id for p in projects]

    def assignments() -> list[str]:
        count = min(len(project_ids), int(rng.integers(1, 4)))
        return [str(value) for value in rng.choice(project_ids, size=count, replace=False)]

    cohorts = [(_id("coh", rng), assignments()) for _ in range(teams)]
    live = {}
    for file in files:
        if file.project_id is not None and file.created_at <= volume.period_start:
            live.setdefault(file.project_id, []).append(file)
    actors = []
    app_for_kind = {
        "doc": "office",
        "drawing": "cad",
        "cad_model": "cad",
        "image": "viewer",
        "media": "media-player",
        "dataset": "analytics",
        "small": "office",
    }
    apps = tuple(dict.fromkeys(app_for_kind.values()))
    for index in range(users):
        # Sparse volumes have explicitly unteamed users; never cross volumes.
        cohort, assigned = cohorts[index % teams] if teams else (None, assignments())
        pool = [file for project in assigned for file in live.get(project, [])]
        weights = np.ones(len(apps))
        for file in pool:
            weights[apps.index(app_for_kind[file.kind])] += 1
        mix = rng.dirichlet(weights)
        actors.append(
            Actor(
                _id("act", rng),
                volume.volume_id,
                "user",
                cohort,
                _id("client", rng),
                assigned,
                *_schedule(volume, rng),
                volume.work_days,
                volume.timezone,
                volume.holidays,
                _vacations(cfg, volume, rng),
                [],  # Working-set selection belongs to issue #16.
                dict(zip(apps, map(float, mix), strict=True)),
            )
        )
    for _ in range(services):
        actors.append(
            Actor(
                _id("act", rng),
                volume.volume_id,
                "service",
                None,
                _id("client", rng),
                [p.project_id for p in projects],
                "00:00",
                "23:59",
                list(range(1, 8)),
                volume.timezone,
                app_weights={"batch-indexer": 0.5, "backup": 0.5},
            )
        )
    return actors


def build_world(cfg: Config, rng: np.random.Generator) -> World:
    """Build the exact global file/user/service totals from a supplied RNG.

    Projects are per volume. Team slots are global, including implicitly empty
    slots when teams outnumber users. Depth zero is the volume root; max_depth
    bounds levels beneath each project root (global depth <= max_depth + 1).
    """
    words = tuple(Path(__file__).with_name("wordlist.txt").read_text().split())
    volumes, projects, directories, files, actors, paths = [], [], [], [], [], []
    for index, settings in enumerate(cfg.volumes):
        start = datetime.combine(cfg.period.start, time(), ZoneInfo(settings.timezone))
        end = start + timedelta(weeks=cfg.period.weeks)
        volume = Volume(
            _id("vol", rng),
            settings.timezone,
            settings.work_days,
            *settings.work_hours,
            settings.holidays,
            start.astimezone(UTC),
            end.astimezone(UTC),
        )
        volumes.append(volume)
        local_projects = _projects(cfg, volume, rng)
        target = _share(cfg.files.target_count, index, len(cfg.volumes))
        local_dirs, dir_paths, candidates = _directories(
            cfg,
            volume,
            local_projects,
            target,
            words,
            rng,
        )
        local_files, local_paths = _files(cfg, volume, target, candidates, dir_paths, words, rng)
        local_actors = _actors(
            cfg,
            volume,
            local_projects,
            local_files,
            _share(cfg.actors.users, index, len(cfg.volumes)),
            _share(cfg.actors.teams, index, len(cfg.volumes)),
            _share(cfg.actors.service, index, len(cfg.volumes)),
            rng,
        )
        projects.extend(local_projects)
        directories.extend(local_dirs)
        files.extend(local_files)
        paths.extend(local_paths)
        actors.extend(local_actors)
    return World(volumes, projects, directories, files, actors, paths)
