"""P1: project Zipf popularity, local calendars, diurnal rate and recency."""

from datetime import timedelta
from zoneinfo import ZoneInfo

import numpy as np

from ..model import PatternRecord
from .common import hour_rng, hours, in_window, intent, live, opaque_id, timestamp, working_factor


class Popularity:
    kind = "popularity"

    def plan(self, world, cfg, rng):
        self.cfg = cfg
        self.seed = int(rng.integers(2**63))
        self.pools, self.causes, self.working_sets = {}, {}, {}
        self.cached = None
        self.projects = {p.project_id: p for p in world.projects}
        self.users = [a for a in world.actors if a.kind == "user"]
        records = []
        if not cfg.patterns.popularity.enabled:
            return records
        for project in world.projects:
            pool = [f for f in world.files if f.project_id == project.project_id]
            pool = [pool[i] for i in rng.permutation(len(pool))]
            self.pools[project.project_id] = pool
            cause = opaque_id("pat", rng)
            self.causes[project.project_id] = cause
            volume = next(v for v in world.volumes if v.volume_id == project.volume_id)
            records.append(
                PatternRecord(
                    cause,
                    self.kind,
                    project.volume_id,
                    volume.period_start,
                    volume.period_end,
                    [a.actor_id for a in self.users if project.project_id in a.project_ids],
                    [f.file_id for f in pool],
                    {
                        **cfg.patterns.popularity.model_dump(),
                        **cfg.randomness.model_dump(),
                        "project_id": project.project_id,
                        "files_order": "Zipf rank, starting at 1",
                        "lifecycle_enabled": cfg.patterns.project_lifecycle.enabled,
                    },
                )
            )
        settings = cfg.patterns.popularity
        for actor in self.users:
            pool = [
                f.file_id
                for f in world.files
                if f.project_id in actor.project_ids and f.volume_id == actor.volume_id
            ]
            self.working_sets[actor.actor_id] = (
                set(
                    rng.choice(
                        pool, min(settings.working_set_size, len(pool)), replace=False
                    ).tolist()
                )
                if pool
                else set()
            )
            actor.working_set_file_ids[:] = sorted(self.working_sets[actor.actor_id])
        for record in records:
            record.params["working_sets"] = {a: sorted(self.working_sets[a]) for a in record.actors}
        return records

    def intents(self, world, window, rng):
        # One buffered period keeps chronological history independent of caller chunks.
        if self.cached is None:
            whole = (
                min(v.period_start for v in world.volumes),
                max(v.period_end for v in world.volumes),
            )
            self.cached = sorted(
                self._intents(world, whole, rng), key=lambda i: (i.at, i.intent_id)
            )
        return (i for i in self.cached if window[0] <= i.at < window[1])

    def _intents(self, world, window, rng):
        cfg, settings = self.cfg, self.cfg.patterns.popularity
        if not settings.enabled or cfg.randomness.rate_scale == 0:
            return
        points = np.asarray(settings.diurnal_curve)
        peak = float(points[:, 1].max())
        if peak == 0:
            return
        history = {a.actor_id: [] for a in self.users}
        for vi, volume in enumerate(world.volumes):
            users = [a for a in self.users if a.volume_id == volume.volume_id]
            for hi, hour in hours(volume, window):
                local_rng = hour_rng(self.seed, vi, hi)
                candidates = []
                midpoint = hour + timedelta(minutes=30)
                for actor in users:
                    assigned = [p for p in actor.project_ids if p in self.pools]
                    for pid in assigned:
                        project = self.projects[pid]
                        activity = (
                            project.activity(midpoint)
                            if cfg.patterns.project_lifecycle.enabled
                            else float(project.started_at is None or midpoint >= project.started_at)
                        )
                        rate = settings.rate_per_user_hour * cfg.randomness.rate_scale * peak
                        for _ in range(
                            int(local_rng.poisson(rate * activity / max(1, len(assigned))))
                        ):
                            at = timestamp(hour, local_rng, cfg.randomness.jitter_scale)
                            local = at.astimezone(ZoneInfo(actor.timezone))
                            decimal_hour = local.hour + local.minute / 60 + local.second / 3600
                            factor = working_factor(actor, at, cfg.calendar.holiday_activity)
                            factor *= np.interp(decimal_hour, points[:, 0], points[:, 1]) / peak
                            if local_rng.random() < factor:
                                candidates.append((at, actor, pid))
                for at, actor, pid in sorted(candidates, key=lambda c: (c[0], c[1].actor_id, c[2])):
                    available = [
                        (rank, f) for rank, f in enumerate(self.pools[pid], 1) if live(f, at)
                    ]
                    if not available:
                        continue
                    recent = set(history[actor.actor_id])
                    revisit = [(rank, f) for rank, f in available if f.file_id in recent]
                    working = [
                        (rank, f)
                        for rank, f in available
                        if f.file_id in self.working_sets[actor.actor_id]
                    ]
                    if local_rng.random() < settings.revisit_share and revisit:
                        available = revisit
                    elif local_rng.random() < settings.working_set_share and working:
                        available = working
                    weights = np.array([rank**-settings.zipf_s for rank, _ in available])
                    half_life = settings.recency_boost_half_life_days
                    if half_life:
                        ages = [
                            (
                                at - max([f.created_at] + [t for t, _ in f.size_history if t <= at])
                            ).total_seconds()
                            / 86400
                            for _, f in available
                        ]
                        weights *= 1 + np.exp2(-np.asarray(ages) / half_life)
                    file = available[
                        int(local_rng.choice(len(available), p=weights / weights.sum()))
                    ][1]
                    history[actor.actor_id].append(file.file_id)
                    history[actor.actor_id] = history[actor.actor_id][-settings.revisit_history :]
                    if in_window(at, volume, window):
                        yield intent(at, actor, file, self.causes[pid], local_rng)
