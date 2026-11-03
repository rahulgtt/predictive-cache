"""Synthetic workloads -> complete operations -> telemetry faults -> dataset."""

from dataclasses import asdict
from datetime import timedelta
from pathlib import Path

import numpy as np

from .catalog import catalog_rows
from .config import Config
from .faults import Faults
from .patterns import NewFile, Noise, Popularity, ProjectLifecycle
from .patterns.workloads import WorkflowChain, Workloads
from .realize import EventRealizer
from .schema import Manifest
from .world import build_world

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
    # New-file metadata must exist before other patterns choose their file populations.
    for generator, component in (
        (NewFile(), "new_file"),
        (ProjectLifecycle(), "lifecycle"),
        (Popularity(), "popularity"),
        (Workloads(), "weekly_schedule"),
    ):
        patterns.extend(generator.plan(world, cfg, component_rng(cfg.seed, component)))
        base.extend(generator.intents(world, window, component_rng(cfg.seed, component)))
    base.sort(key=lambda i: (i.at, i.intent_id))
    chains = WorkflowChain()
    patterns.extend(chains.plan(world, cfg, component_rng(cfg.seed, "workflow_chain")))
    triggers = {p.pattern_id for p in patterns if p.kind in {"popularity", "weekly_schedule"}}
    base.extend(chains.expand(world, base, component_rng(cfg.seed, "workflow_chain"), triggers))
    base.sort(key=lambda i: (i.at, i.intent_id))
    standalone = not any(
        getattr(cfg.patterns, p).enabled
        for p in ("popularity", "new_file", "weekly_schedule", "burst", "batch_job")
    )
    noise = Noise(None if standalone else base)
    patterns.extend(noise.plan(world, cfg, component_rng(cfg.seed, "noise")))
    # Finish admitted burst/workflow work even when it spills past the demand period.
    end = max(window[1], max((i.at for i in base), default=window[1]) + timedelta(microseconds=1))
    realization_window = (window[0], end)
    intents = noise.intents(world, realization_window, component_rng(cfg.seed, "noise"))
    realizer = EventRealizer(cfg)
    clean = realizer.realize(
        world, intents, realization_window, component_rng(cfg.seed, "realizer")
    )
    faults = Faults(cfg)
    emitted, removed, truth = [], [], {}
    for record in faults.apply(clean, world, component_rng(cfg.seed, "faults")):
        (removed if record.disposition in {"dropped", "sampled_out"} else emitted).append(
            record.event
        )
        truth[record.event["event_id"]] = record.truth
    # Churn mutates histories during realization, so project catalogs after consuming it.
    tables = catalog_rows(world)
    tables.update(
        events=emitted,
        dropped_events=removed,
        event_truth=list(truth.values()),
        loss_counters=faults.counters,
    )
    patterns.extend(realizer.patterns)
    return write_dataset(
        cfg,
        out_dir,
        tables,
        [asdict(p) for p in patterns],
        faults.known_gaps,
        overwrite=overwrite,
        report_updates={
            "fault_rates": faults.stats,
            "demand_period_end": window[1],
            "last_true_operation": max(
                (t["true_event_time"] for t in truth.values()), default=None
            ),
            "last_delivery": max((e["ingest_time"] for e in emitted), default=None),
        },
    )
