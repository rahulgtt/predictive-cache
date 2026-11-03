"""P4: lifecycle truth; P1 consumes the existing project activity function."""

from ..model import PatternRecord
from .common import opaque_id


class ProjectLifecycle:
    kind = "project_lifecycle"

    def plan(self, world, cfg, rng):
        if not cfg.patterns.project_lifecycle.enabled:
            return []
        records = []
        for project in world.projects:
            volume = next(v for v in world.volumes if v.volume_id == project.volume_id)
            times = {
                name: getattr(project, name)
                for name in ("started_at", "ramp_end", "active_end", "cooldown_end")
            }
            records.append(
                PatternRecord(
                    opaque_id("pat", rng),
                    self.kind,
                    project.volume_id,
                    volume.period_start,
                    volume.period_end,
                    [a.actor_id for a in world.actors if project.project_id in a.project_ids],
                    [f.file_id for f in world.files if f.project_id == project.project_id],
                    {
                        "project_id": project.project_id,
                        **{
                            key: value.isoformat() if value else None
                            for key, value in times.items()
                        },
                        "phase_activity": list(project.phase_activity),
                        "activity_history": [
                            [t.isoformat(), value] for t, value in project.activity_history
                        ],
                        **cfg.randomness.model_dump(),
                    },
                )
            )
        return records

    def intents(self, world, window, rng):
        return iter(())
