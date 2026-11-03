"""P8: configurable uniform live-file mixture, including archived projects."""

from dataclasses import replace
from hashlib import blake2b

import numpy as np

from ..model import PatternRecord
from .common import hour_rng, hours, in_window, intent, live, opaque_id, timestamp, working_factor


class Noise:
    kind = "noise"

    def __init__(self, base=None):
        self.base = base

    def plan(self, world, cfg, rng):
        self.cfg = cfg
        self.seed = int(rng.integers(2**63))
        self.files = {
            v.volume_id: [f for f in world.files if f.volume_id == v.volume_id]
            for v in world.volumes
        }
        self.users = {
            v.volume_id: [
                a for a in world.actors if a.volume_id == v.volume_id and a.kind == "user"
            ]
            for v in world.volumes
        }
        if not cfg.patterns.noise.enabled:
            return []
        return [
            PatternRecord(
                opaque_id("pat", rng),
                self.kind,
                v.volume_id,
                v.period_start,
                v.period_end,
                [a.actor_id for a in self.users[v.volume_id]],
                [f.file_id for f in self.files[v.volume_id]],
                {
                    **cfg.randomness.model_dump(),
                    "cause": "noise",
                    "mix": "replace base opens; metadata retained; non-noise amplitude reduced",
                    "standalone": self.base is None,
                    "standalone_rate_per_user_hour": cfg.patterns.popularity.rate_per_user_hour,
                    "standalone_denominator": "all emitted opens are noise; share thins proposals",
                },
            )
            for v in world.volumes
        ]

    def intents(self, world, window, rng):
        enabled = self.cfg.patterns.noise.enabled
        share = self.cfg.randomness.noise_share if enabled else 0
        if self.base is None:
            if not share or not self.cfg.randomness.rate_scale:
                return
            output = []
            for vi, volume in enumerate(world.volumes):
                users = self.users[volume.volume_id]
                for hi, hour in hours(volume, window):
                    local_rng = hour_rng(self.seed, vi, hi)
                    rate = (
                        self.cfg.patterns.popularity.rate_per_user_hour
                        * len(users)
                        * self.cfg.randomness.rate_scale
                        * share
                    )
                    for _ in range(int(local_rng.poisson(rate))):
                        at = timestamp(hour, local_rng, self.cfg.randomness.jitter_scale)
                        actor = users[int(local_rng.integers(len(users)))]
                        if local_rng.random() >= working_factor(
                            actor, at, self.cfg.calendar.holiday_activity
                        ):
                            continue
                        files = [f for f in self.files[volume.volume_id] if live(f, at)]
                        if files:
                            item = intent(
                                at,
                                actor,
                                files[int(local_rng.integers(len(files)))],
                                "noise",
                                local_rng,
                            )
                            if in_window(at, volume, window):
                                output.append(item)
            yield from sorted(output, key=lambda i: (i.at, i.intent_id))
            return
        for item in self.base:
            if not window[0] <= item.at < window[1]:
                continue
            # Per-intent randomness prevents windows and metadata from shifting noise decisions.
            digest = blake2b(f"{self.seed}:{item.intent_id}".encode(), digest_size=16).digest()
            local_rng = np.random.default_rng(int.from_bytes(digest, "little"))
            if item.operation == "open" and local_rng.random() < share:
                files = [f for f in self.files[item.volume_id] if live(f, item.at)]
                users = self.users[item.volume_id]
                if files and users:
                    actor = users[int(local_rng.integers(len(users)))]
                    file = files[int(local_rng.integers(len(files)))]
                    item = replace(
                        intent(item.at, actor, file, "noise", local_rng), intent_id=item.intent_id
                    )
            yield item
