"""Scheduled work, shared bursts, batch jobs and bounded co-access chains."""

from dataclasses import replace
from datetime import UTC, datetime, time, timedelta
from hashlib import blake2b
from zoneinfo import ZoneInfo

import numpy as np

from ..model import PatternRecord
from .common import intent, live, opaque_id


def _days(volume):
    zone = ZoneInfo(volume.timezone)
    day = volume.period_start.astimezone(zone).date()
    while datetime.combine(day, time(), zone) < volume.period_end:
        yield day
        day += timedelta(days=1)


def _at(day, hour, volume):
    return datetime.combine(day, time.fromisoformat(hour), ZoneInfo(volume.timezone)).astimezone(
        UTC
    )


def _sample(items, count, rng):
    return [items[int(i)] for i in rng.choice(len(items), min(count, len(items)), replace=False)]


def _record(kind, volume, actors, files, params, rng, start=None, end=None):
    return PatternRecord(
        opaque_id("pat", rng),
        kind,
        volume.volume_id,
        start or volume.period_start,
        end or volume.period_end,
        [a.actor_id for a in actors],
        [f.file_id for f in files],
        params,
    )


def _subtree(world, directory):
    ids = {directory.dir_id}
    while True:
        expanded = ids | {d.dir_id for d in world.directories if d.parent_dir_id in ids}
        if expanded == ids:
            break
        ids = expanded
    file_ids = {p.file_id for p in world.file_paths if p.dir_id in ids}
    return [f for f in world.files if f.file_id in file_ids]


class Workloads:
    """P2/P5/P6 plan once; windows only filter the deterministic buffered plan."""

    def plan(self, world, cfg, rng):
        self.items, self.records = [], []
        self.cfg = cfg
        for key, method in ((102, self._weekly), (105, self._bursts), (106, self._jobs)):
            stream = np.random.default_rng(np.random.SeedSequence(cfg.seed, spawn_key=(key,)))
            method(world, stream)
        self.items.sort(key=lambda i: (i.at, i.intent_id))
        return self.records

    def intents(self, world, window, rng):
        ends = {v.volume_id: v.period_end for v in world.volumes}
        bursts = {r.pattern_id for r in self.records if r.kind == "burst"}
        return (
            i
            for i in self.items
            if window[0] <= i.at
            and (
                i.at < window[1]
                or (
                    i.cause in bursts
                    and window[0] < ends[i.volume_id] <= window[1]
                    and i.at >= ends[i.volume_id]
                )
            )
        )

    def _weekly(self, world, rng):
        cfg, settings = self.cfg, self.cfg.patterns.weekly_schedule
        if not settings.enabled:
            return
        for volume in world.volumes:
            users = [
                a for a in world.actors if a.volume_id == volume.volume_id and a.kind == "user"
            ]
            teams = {a.cohort_id or a.actor_id for a in users}
            for team in sorted(teams):
                actors = [a for a in users if (a.cohort_id or a.actor_id) == team]
                projects = [
                    p
                    for p in world.projects
                    if p.volume_id == volume.volume_id
                    and any(p.project_id in a.project_ids for a in actors)
                ]
                for _ in range(settings.instances_per_team):
                    if not projects:
                        continue
                    project = projects[int(rng.integers(len(projects)))]
                    pool = [f for f in world.files if f.project_id == project.project_id]
                    if not pool:
                        continue
                    files = _sample(
                        pool,
                        int(rng.integers(settings.files_per_set[0], settings.files_per_set[1] + 1)),
                        rng,
                    )
                    weekday = int(rng.integers(1, 6))
                    weekdays = list(volume.work_days) if rng.random() < 0.2 else [weekday]
                    hour = f"{int(rng.integers(9, 16)):02d}:15"
                    first = int(rng.integers(max(1, cfg.period.weeks // 2)))
                    last = int(rng.integers(first + 1, cfg.period.weeks + 1))
                    start = volume.period_start + timedelta(weeks=first)
                    end = min(volume.period_end, volume.period_start + timedelta(weeks=last))
                    record = _record(
                        "weekly_schedule",
                        volume,
                        actors,
                        files,
                        {
                            **settings.model_dump(),
                            "weekdays": weekdays,
                            "local_time": hour,
                            "slots": [],
                            "project_id": project.project_id,
                        },
                        rng,
                        start,
                        end,
                    )
                    for day in _days(volume):
                        slot = _at(day, hour, volume)
                        if not start <= slot < end or day.isoweekday() not in weekdays:
                            continue
                        if day in volume.holidays:
                            continue
                        record.params["slots"].append(slot.isoformat())
                        activity = (
                            project.activity(slot) if cfg.patterns.project_lifecycle.enabled else 1
                        )
                        for actor in actors:
                            if not actor.is_working(slot):
                                continue
                            expected = settings.attendance * activity * cfg.randomness.rate_scale
                            repeats = int(expected) + int(rng.random() < expected % 1)
                            for _ in range(repeats):
                                at = slot + timedelta(
                                    minutes=float(
                                        rng.normal(
                                            0, settings.jitter_min * cfg.randomness.jitter_scale
                                        )
                                    )
                                )
                                if not start <= at < end or not actor.is_working(at):
                                    continue
                                for file in files:
                                    if live(file, at):
                                        self.items.append(
                                            intent(at, actor, file, record.pattern_id, rng)
                                        )
                    self.records.append(record)

    def _bursts(self, world, rng):
        cfg, settings = self.cfg, self.cfg.patterns.burst
        if not settings.enabled:
            return
        for volume in world.volumes:
            users = [
                a for a in world.actors if a.volume_id == volume.volume_id and a.kind == "user"
            ]
            directories = [d for d in world.directories if d.volume_id == volume.volume_id]
            count = int(
                rng.poisson(
                    settings.bursts_per_volume_month
                    * cfg.period.weeks
                    * 7
                    / 30.4375
                    * cfg.randomness.rate_scale
                )
            )
            for _ in range(count):
                if not users or not directories:
                    continue
                at = volume.period_start + timedelta(
                    seconds=float(
                        rng.uniform(0, (volume.period_end - volume.period_start).total_seconds())
                    )
                )
                directory = directories[int(rng.integers(len(directories)))]
                pool = [f for f in _subtree(world, directory) if live(f, at)]
                if not pool:
                    continue
                scan = rng.random() < settings.scan_share
                actor = users[int(rng.integers(len(users)))]
                team = (
                    [a for a in users if a.cohort_id == actor.cohort_id]
                    if actor.cohort_id
                    else [actor]
                )
                actors = (
                    [actor] if scan else [a for a in team if rng.random() < settings.participation]
                )
                common = _sample(pool, int(len(pool) * settings.shared_file_share), rng)
                common_ids = {f.file_id for f in common}
                record = _record(
                    "burst",
                    volume,
                    actors,
                    pool,
                    {
                        **settings.model_dump(),
                        "scan": bool(scan),
                        "dir_id": directory.dir_id,
                        "onset": at.isoformat(),
                        "shared_files": sorted(common_ids),
                    },
                    rng,
                    at,
                    at + timedelta(seconds=settings.duration_s),
                )
                for actor in actors:
                    selected = (
                        pool
                        if scan
                        else common
                        + [f for f in pool if f.file_id not in common_ids and rng.random() < 0.5]
                    )
                    arrival = 0 if scan else float(rng.uniform(0, settings.arrival_spread_s))
                    for file in selected:
                        delay = float(rng.uniform(arrival, settings.duration_s))
                        item = intent(
                            at + timedelta(seconds=delay), actor, file, record.pattern_id, rng
                        )
                        if live(file, item.at):
                            self.items.append(
                                replace(item, read_profile="head", head_bytes=1048576)
                                if scan
                                else item
                            )
                self.records.append(record)

    def _jobs(self, world, rng):
        cfg, settings = self.cfg, self.cfg.patterns.batch_job
        if not settings.enabled:
            return
        for volume in world.volumes:
            actors = [
                a for a in world.actors if a.volume_id == volume.volume_id and a.kind == "service"
            ]
            directories = [d for d in world.directories if d.volume_id == volume.volume_id]
            for _ in range(settings.jobs_per_volume):
                if not actors or not directories:
                    continue
                actor = actors[int(rng.integers(len(actors)))]
                directory = directories[int(rng.integers(len(directories)))]
                files = _subtree(world, directory)
                record = _record(
                    "batch_job",
                    volume,
                    [actor],
                    files,
                    {**settings.model_dump(), "dir_id": directory.dir_id},
                    rng,
                )
                for day in _days(volume):
                    at = _at(day, settings.local_time, volume)
                    if not volume.period_start <= at < volume.period_end:
                        continue
                    scale = cfg.randomness.rate_scale
                    for _ in range(int(scale) + int(rng.random() < scale % 1)):
                        run = opaque_id("job", rng)
                        for file in files:
                            if live(file, at):
                                self.items.append(
                                    replace(
                                        intent(at, actor, file, record.pattern_id, rng),
                                        app="batch-indexer",
                                        read_profile="head",
                                        head_bytes=settings.head_bytes,
                                        job_run_id=run,
                                    )
                                )
                self.records.append(record)


class WorkflowChain:
    """P3 expands original P1/P2 entry intents once; inserted opens never trigger."""

    def plan(self, world, cfg, rng):
        self.cfg, self.chains = cfg, []
        self.seed = int(rng.integers(2**63))
        settings = cfg.patterns.workflow_chain
        if not settings.enabled:
            return []
        for project in world.projects:
            volume = next(v for v in world.volumes if v.volume_id == project.volume_id)
            pool = [f for f in world.files if f.project_id == project.project_id]
            actors = [a for a in world.actors if project.project_id in a.project_ids]
            if len(pool) < 2:
                continue
            for _ in range(settings.chains_per_project):
                size = min(
                    len(pool),
                    max(5 if settings.context_order == 2 else 2, settings.max_steps + 1),
                    max(
                        5 if settings.context_order == 2 else 3,
                        int(rng.geometric(settings.length_geom_p)) + 1,
                    ),
                )
                files = _sample(pool, size, rng)
                ids = [f.file_id for f in files]
                entries, edges = [ids[0]], []
                for index, fid in enumerate(ids[:-1]):
                    edges.append({"previous": None, "from": fid, "to": ids[index + 1], "p": 0.8})
                    if index + 2 < len(ids):
                        edges.append(
                            {"previous": None, "from": fid, "to": ids[index + 2], "p": 0.2}
                        )
                    else:
                        edges[-1]["p"] = 1.0
                if settings.context_order == 2 and len(ids) >= 5:
                    entries.append(ids[3])
                    edges = [e for e in edges if e["from"] not in (ids[0], ids[1], ids[3])]
                    edges += [
                        {"previous": None, "from": ids[0], "to": ids[1], "p": 1.0},
                        {"previous": None, "from": ids[3], "to": ids[1], "p": 1.0},
                        {"previous": None, "from": ids[1], "to": ids[2], "p": 1.0},
                        {"previous": ids[0], "from": ids[1], "to": ids[2], "p": 1.0},
                        {"previous": ids[3], "from": ids[1], "to": ids[4], "p": 1.0},
                    ]
                drift = rng.random() < settings.drift_share and len(ids) > 2
                after = [dict(e) for e in edges]
                if drift:
                    first = after[0]
                    first["to"] = next(
                        fid for fid in reversed(ids) if fid not in (first["from"], first["to"])
                    )
                record = _record(
                    "workflow_chain",
                    volume,
                    actors,
                    files,
                    {
                        **settings.model_dump(),
                        "project_id": project.project_id,
                        "entries": entries,
                        "edges": edges,
                        "edges_after": after,
                        "drift_at": (
                            volume.period_start + (volume.period_end - volume.period_start) / 2
                        ).isoformat()
                        if drift
                        else None,
                    },
                    rng,
                )
                self.chains.append(record)
        return self.chains

    def expand(self, world, base, rng, trigger_causes=None):
        settings = self.cfg.patterns.workflow_chain
        if not settings.enabled or self.cfg.randomness.rate_scale == 0:
            return []
        files = {f.file_id: f for f in world.files}
        actors = {a.actor_id: a for a in world.actors}
        additions = []
        for entry in base:
            if entry.operation != "open" or entry.workflow_run_id or entry.job_run_id:
                continue
            if trigger_causes is not None and entry.cause not in trigger_causes:
                continue
            for record in self.chains:
                if entry.file_id not in record.params["entries"]:
                    continue
                digest = blake2b(
                    f"{self.seed}:{record.pattern_id}:{entry.intent_id}".encode(), digest_size=16
                ).digest()
                stream = np.random.default_rng(int.from_bytes(digest, "little"))
                actor, previous, current, at = actors[entry.actor_id], None, entry.file_id, entry.at
                run = opaque_id("workflow", stream)
                for _ in range(settings.max_steps):
                    if stream.random() >= settings.continue_p or not actor.is_working(at):
                        break
                    drift = record.params["drift_at"]
                    edges = (
                        record.params["edges_after"]
                        if drift and at >= datetime.fromisoformat(drift)
                        else record.params["edges"]
                    )
                    eligible = [
                        e for e in edges if e["from"] == current and e["previous"] == previous
                    ]
                    if not eligible:
                        eligible = [
                            e for e in edges if e["from"] == current and e["previous"] is None
                        ]
                    if not eligible:
                        break
                    probabilities = np.array([e["p"] for e in eligible])
                    target = eligible[
                        int(stream.choice(len(eligible), p=probabilities / probabilities.sum()))
                    ]["to"]
                    delay = min(
                        604800,
                        max(
                            0.000002,
                            float(
                                stream.lognormal(
                                    np.log(settings.delay_median_s), settings.delay_sigma
                                )
                            )
                            * self.cfg.randomness.jitter_scale,
                        ),
                    )
                    at += timedelta(seconds=delay)
                    if not actor.is_working(at) or not live(files[target], at):
                        break
                    if stream.random() < settings.interruption_p:
                        unrelated = [
                            f
                            for f in world.files
                            if f.project_id in actor.project_ids
                            and f.file_id not in record.files
                            and live(f, at)
                        ]
                        if unrelated:
                            interrupted = intent(
                                at - timedelta(microseconds=1),
                                actor,
                                unrelated[int(stream.integers(len(unrelated)))],
                                record.pattern_id,
                                stream,
                            )
                            additions.append(replace(interrupted, workflow_run_id=run))
                    item = intent(at, actor, files[target], record.pattern_id, stream)
                    additions.append(replace(item, workflow_run_id=run))
                    previous, current = current, target
        return sorted(additions, key=lambda i: (i.at, i.intent_id))
