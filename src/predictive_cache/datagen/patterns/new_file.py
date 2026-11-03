"""P7: fictional new-file metadata and decaying team opens."""

from dataclasses import replace
from datetime import timedelta
from pathlib import PurePosixPath

import numpy as np

from ..model import PatternRecord
from ..world import _files
from .common import hour_rng, hours, in_window, intent, live, opaque_id, timestamp, working_factor


class NewFile:
    kind = "new_file"

    def plan(self, world, cfg, rng):
        self.cfg = cfg
        self.seed = int(rng.integers(2**63))
        self.created, self.metadata = [], []
        records = []
        settings = cfg.patterns.new_file
        if not settings.enabled:
            return records
        directory_paths = {p.dir_id: str(PurePosixPath(p.path).parent) for p in world.file_paths}
        for project in world.projects:
            volume = next(v for v in world.volumes if v.volume_id == project.volume_id)
            start = max(volume.period_start, project.started_at or volume.period_start)
            seconds = (volume.period_end - start).total_seconds()
            if seconds <= 0:
                continue
            count = int(rng.poisson(settings.new_files_per_project_week * seconds / (7 * 86400)))
            directories = [d for d in world.directories if d.project_id == project.project_id]
            if not directories:
                continue
            directory = min(directories, key=lambda d: (d.depth, d.dir_id))
            directory_paths.setdefault(directory.dir_id, f"/generated/{project.project_id}")
            files, paths = _files(cfg, volume, count, [directory], directory_paths, ("new",), rng)
            team = [
                a
                for a in world.actors
                if a.kind == "user"
                and a.volume_id == volume.volume_id
                and project.project_id in a.project_ids
            ]
            fallback = next((a for a in world.actors if a.volume_id == volume.volume_id), None)
            for file, path in zip(files, paths, strict=True):
                at = start + timedelta(seconds=float(rng.uniform(0, seconds)))
                at = min(at, volume.period_end - timedelta(microseconds=2))
                file = replace(file, created_at=at)
                path = replace(
                    path,
                    valid_from=at,
                    path=f"{directory_paths[directory.dir_id]}/{file.file_id}{PurePosixPath(path.path).suffix}",
                )
                world.files.append(file)
                world.file_paths.append(path)
                cause = opaque_id("pat", rng)
                self.created.append((file, team, cause))
                actor = team[int(rng.integers(len(team)))] if team else fallback
                if actor:
                    self.metadata.extend(
                        [
                            intent(at, actor, file, cause, rng, "create"),
                            intent(
                                at + timedelta(microseconds=1), actor, file, cause, rng, "write"
                            ),
                        ]
                    )
                records.append(
                    PatternRecord(
                        cause,
                        self.kind,
                        volume.volume_id,
                        at,
                        volume.period_end,
                        [a.actor_id for a in team],
                        [file.file_id],
                        {
                            **settings.model_dump(),
                            **cfg.randomness.model_dump(),
                            "project_id": project.project_id,
                            "created_at": at.isoformat(),
                            "metadata_actor": actor.actor_id if actor else None,
                        },
                    )
                )
        return records

    def intents(self, world, window, rng):
        return iter(sorted(self._intents(world, window, rng), key=lambda i: (i.at, i.intent_id)))

    def _intents(self, world, window, rng):
        settings = self.cfg.patterns.new_file
        if not settings.enabled:
            return
        for vi, volume in enumerate(world.volumes):
            created = [(f, a, c) for f, a, c in self.created if f.volume_id == volume.volume_id]
            for hi, hour in hours(volume, window):
                local_rng = hour_rng(self.seed, vi, hi)
                output = [
                    i
                    for i in self.metadata
                    if i.volume_id == volume.volume_id
                    and hour <= i.at < hour + timedelta(hours=1)
                    and in_window(i.at, volume, window)
                ]
                for file, team, cause in created:
                    if not team or file.created_at >= hour + timedelta(hours=1):
                        continue
                    # Upper envelope over the hour, then thin at the actual timestamp.
                    age = max(0, (hour - file.created_at).total_seconds() / 86400)
                    peak = np.exp2(-age / settings.open_decay_half_life_days)
                    rate = settings.initial_opens_per_day / 24 * self.cfg.randomness.rate_scale
                    for _ in range(int(local_rng.poisson(rate * peak))):
                        at = timestamp(hour, local_rng, self.cfg.randomness.jitter_scale)
                        if at < file.created_at + timedelta(microseconds=2) or not live(file, at):
                            continue
                        decay = np.exp2(
                            -(at - file.created_at).total_seconds()
                            / 86400
                            / settings.open_decay_half_life_days
                        )
                        if local_rng.random() >= decay / peak:
                            continue
                        eligible = [
                            a
                            for a in team
                            if working_factor(a, at, self.cfg.calendar.holiday_activity) > 0
                        ]
                        if not eligible:
                            continue
                        actor = eligible[int(local_rng.integers(len(eligible)))]
                        if local_rng.random() >= working_factor(
                            actor, at, self.cfg.calendar.holiday_activity
                        ):
                            continue
                        item = intent(at, actor, file, cause, local_rng)
                        if in_window(at, volume, window):
                            output.append(item)
                yield from sorted(output, key=lambda i: (i.at, i.intent_id))
