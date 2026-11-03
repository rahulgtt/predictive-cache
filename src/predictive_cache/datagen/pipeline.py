"""Foundation runner with fixed RNG streams and explicitly limited fixture behavior."""

from collections import Counter
from dataclasses import asdict
from datetime import timedelta
from pathlib import Path

import numpy as np

from .catalog import catalog_rows
from .config import Config
from .patterns import NewFile, Noise, Popularity, ProjectLifecycle
from .schema import Manifest
from .stubs import NoFaults, StubRealizer
from .world import build_world

# Stable numeric keys, independent of which components are enabled.
COMPONENT_KEYS = {
    "world": 1,
    "fixture_intents": 2,
    "realizer": 3,
    "faults": 4,
    "popularity": 101,
    "weekly_schedule": 102,
    "workflow_chain": 103,
    "lifecycle": 104,
    "burst": 105,
    "batch_job": 106,
    "new_file": 107,
    "noise": 108,
    "churn": 109,
}


def component_rng(seed: int, component: str, volume: int = 0) -> np.random.Generator:
    return np.random.default_rng(
        np.random.SeedSequence(seed, spawn_key=(COMPONENT_KEYS[component], volume))
    )


def run(cfg: Config, out_dir: Path, overwrite: bool = False) -> Manifest:
    from .io import write_dataset

    world = build_world(cfg, component_rng(cfg.seed, "world"))
    window = (min(v.period_start for v in world.volumes), max(v.period_end for v in world.volumes))
    patterns, base = [], []
    # P7 establishes metadata before other patterns rank/select the population.
    for generator, component in (
        (NewFile(), "new_file"),
        (ProjectLifecycle(), "lifecycle"),
        (Popularity(), "popularity"),
    ):
        patterns.extend(generator.plan(world, cfg, component_rng(cfg.seed, component)))
        base.extend(generator.intents(world, window, component_rng(cfg.seed, component)))
    base.sort(key=lambda i: (i.at, i.intent_id))
    standalone = not cfg.patterns.popularity.enabled and not cfg.patterns.new_file.enabled
    noise = Noise(None if standalone else base)
    patterns.extend(noise.plan(world, cfg, component_rng(cfg.seed, "noise")))
    intents = noise.intents(world, window, component_rng(cfg.seed, "noise"))
    tables = catalog_rows(world)
    tables.update(events=[], event_truth=[], dropped_events=[], loss_counters=[])
    events = StubRealizer(cfg).realize(world, intents, window, component_rng(cfg.seed, "realizer"))
    records = NoFaults().apply(events, world, component_rng(cfg.seed, "faults"))
    counts = Counter()
    for record in records:
        tables["events"].append(record.event)
        tables["event_truth"].append(record.truth)
        counts[
            (
                record.event["volume_id"],
                record.truth["true_event_time"].replace(minute=0, second=0, microsecond=0),
            )
        ] += 1
    for volume in world.volumes:
        hour = volume.period_start.replace(minute=0, second=0, microsecond=0)
        while hour < volume.period_end:
            counts.setdefault((volume.volume_id, hour), 0)
            hour += timedelta(hours=1)
    for (volume_id, hour), count in sorted(counts.items()):
        tables["loss_counters"].append(
            dict(
                volume_id=volume_id,
                hour_start=hour,
                offered=count,
                emitted=count,
                dropped=0,
                sampled_out=0,
                duplicates=0,
                late=0,
            )
        )
    return write_dataset(
        cfg, out_dir, tables, [asdict(p) for p in patterns], [], overwrite=overwrite
    )
