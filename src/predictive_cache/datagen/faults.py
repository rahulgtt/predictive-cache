"""Synthetic telemetry faults; no reads or changes to modeled file contents."""

import json
from collections import Counter, defaultdict
from datetime import UTC, datetime, time, timedelta
from itertools import islice
from zoneinfo import ZoneInfo

import numpy as np

from .io import canonical_json
from .model import FaultRecord, Intent
from .patterns.common import live, opaque_id

_KEYS = (
    "warmer",
    "probe",
    "unknown",
    "failed",
    "invalid",
    "identity",
    "skew",
    "loss",
    "sampling",
    "late",
    "duplicate",
)


class Faults:
    name = "telemetry"

    def __init__(self, cfg):
        self.cfg = cfg
        self.known_gaps, self.counters, self.stats = [], [], {}

    def _origins(self, clean, world, streams):
        from .realize import EventRealizer

        cfg = self.cfg
        intents = []
        for volume in world.volumes:
            actors = [a for a in world.actors if a.volume_id == volume.volume_id]
            if not actors:
                continue
            actor = next((a for a in actors if a.kind == "service"), actors[0])
            files = {f.file_id: f for f in world.files if f.volume_id == volume.volume_id}
            opens = [
                (t["true_event_time"], t["true_file_id"])
                for e, t in clean
                if e["volume_id"] == volume.volume_id
                and e["origin"] == "foreground"
                and e["operation"] == "open"
                and e["result"] == "ok"
            ]
            zone = ZoneInfo(volume.timezone)
            if cfg.faults.warmer_origin.enabled:
                rng = streams["warmer"]
                day = volume.period_start.astimezone(zone).date()
                while True:
                    at = datetime.combine(
                        day, time.fromisoformat(cfg.faults.warmer_origin.local_time), zone
                    ).astimezone(UTC)
                    if at >= volume.period_end:
                        break
                    if at >= volume.period_start:
                        counts = Counter(
                            fid for moment, fid in opens if at - timedelta(days=7) <= moment < at
                        )
                        top = sorted(counts, key=lambda fid: (-counts[fid], fid))
                        selected = [fid for fid in top if fid in files and live(files[fid], at)]
                        run = opaque_id("job", rng)
                        for fid in selected[: cfg.faults.warmer_origin.top_n]:
                            intents.append(
                                Intent(
                                    opaque_id("int", rng),
                                    at,
                                    volume.volume_id,
                                    actor.actor_id,
                                    fid,
                                    "warmer",
                                    "head",
                                    2**20,
                                    "warmer_sim",
                                    job_run_id=run,
                                )
                            )
                    day += timedelta(days=1)
            if cfg.faults.synthetic_origin.enabled:
                rng = streams["probe"]
                # Fixed random ranks, not future popularity, choose a few probes each hour.
                ranked = list(files)
                rng.shuffle(ranked)
                at = volume.period_start
                while at < volume.period_end:
                    selected = list(islice((fid for fid in ranked if live(files[fid], at)), 3))
                    run = opaque_id("job", rng)
                    for fid in selected:
                        intents.append(
                            Intent(
                                opaque_id("int", rng),
                                at,
                                volume.volume_id,
                                actor.actor_id,
                                fid,
                                "probe",
                                "head",
                                2**20,
                                "probe_sim",
                                job_run_id=run,
                            )
                        )
                    at += timedelta(hours=1)
        if not intents:
            return []
        settings = cfg.model_copy(deep=True)
        settings.realize.eacces_rate = settings.realize.stale_open_rate = 0
        settings.realize.interactive_think_time_max_s = (
            settings.realize.service_think_time_max_s
        ) = 0
        window = (
            min(v.period_start for v in world.volumes),
            max(v.period_end for v in world.volumes),
        )
        # Dedicated stream keeps added-handle timing separate from all fault mutations.
        rng = np.random.default_rng(np.random.SeedSequence([cfg.seed, 220]))
        pairs = list(
            EventRealizer(settings, churn=False).realize(
                world, sorted(intents, key=lambda i: (i.at, i.intent_id)), window, rng
            )
        )
        files = {f.file_id: f for f in world.files}
        invalid = set()
        for event, _ in pairs:
            try:
                size = files[event["file_id"]].size_at(event["event_time"])
            except ValueError:
                size = None
            if size != event["file_size"]:
                invalid.add(event["handle_id"])
        # Added historical handles cannot postpone already finalized churn.
        pairs = [(event, truth) for event, truth in pairs if event["handle_id"] not in invalid]
        for event, truth in pairs:
            event["origin"] = "warmer" if truth["cause"] == "warmer_sim" else "synthetic"
        return pairs

    def apply(self, events, world, rng):
        settings = self.cfg.faults
        seed = int(rng.integers(2**63))
        streams = {
            name: np.random.default_rng(np.random.SeedSequence([seed, i]))
            for i, name in enumerate(_KEYS)
        }
        clean = list(events)
        clean += self._origins(clean, world, streams)
        clean.sort(key=lambda pair: (pair[1]["true_event_time"], pair[0]["event_id"]))
        self.known_gaps, self.stats = [], {}
        counts = defaultdict(Counter)
        windows = defaultdict(list)
        for volume in world.volumes:
            start, end = volume.period_start, volume.period_end
            loss = settings.loss_window
            if loss.enabled:
                random = streams["loss"]
                months = (end - start).total_seconds() / (30.4375 * 86400)
                for _ in range(int(random.poisson(loss.per_volume_month * months))):
                    at = start + timedelta(
                        seconds=float(random.uniform(0, (end - start).total_seconds()))
                    )
                    until = min(end, at + timedelta(hours=float(random.uniform(*loss.hours))))
                    fraction = float(random.uniform(*loss.loss_fraction))
                    if fraction:
                        windows[volume.volume_id].append((at, until, fraction))
                        self.known_gaps.append(
                            dict(
                                volume_id=volume.volume_id,
                                start=at,
                                end=until,
                                reason="loss_window",
                                loss_fraction=fraction,
                            )
                        )
            if settings.sampling.enabled and settings.sampling.rate < 1:
                self.known_gaps.append(
                    dict(
                        volume_id=volume.volume_id,
                        start=start,
                        end=max(
                            [end]
                            + [
                                t["true_event_time"] + timedelta(microseconds=1)
                                for e, t in clean
                                if e["volume_id"] == volume.volume_id
                            ]
                        ),
                        reason="sampling",
                        loss_fraction=1 - settings.sampling.rate,
                    )
                )
            hour = start.replace(minute=0, second=0, microsecond=0)
            while hour < end:
                counts[(volume.volume_id, hour)]
                hour += timedelta(hours=1)

        def trial(name, probability):
            stat = self.stats.setdefault(name, dict(eligible=0, affected=0, expected=0.0))
            stat["eligible"] += 1
            stat["expected"] += probability
            hit = bool(streams[name].random() < probability)
            stat["affected"] += int(hit)
            return hit

        # Source skew is per volume/client; missing clients share a volume source.
        skew = {}
        clients = sorted({(e["volume_id"], e["client_id"] or "") for e, _ in clean})
        if settings.clock_skew.enabled:
            for client in clients:
                amount = float(streams["skew"].normal(0, settings.clock_skew.sigma_s))
                if trial("skew", settings.clock_skew.outlier_share):
                    amount += float(streams["skew"].choice([-1, 1])) * settings.clock_skew.outlier_s
                skew[client] = amount
        identities = {}
        for original_event, original_truth in clean:
            event = dict(original_event)
            truth = {**original_truth, "faults": list(original_truth["faults"])}
            originals = json.loads(truth["original_fields_json"])

            def mutate(tag, **values):
                if tag not in truth["faults"]:
                    truth["faults"].append(tag)
                for key, value in values.items():
                    originals.setdefault(key, event[key])
                    event[key] = value

            at = truth["true_event_time"]
            bucket = counts[(event["volume_id"], at.replace(minute=0, second=0, microsecond=0))]
            bucket["offered"] += 1
            if settings.unknown_origin.enabled and event["origin"] == "foreground":
                if trial("unknown", settings.unknown_origin.rate):
                    mutate(
                        "origin_unknown",
                        origin=None if streams["unknown"].random() < 0.5 else "unknown",
                    )
            if event["operation"] == "read":
                if settings.failed_result.enabled and trial("failed", settings.failed_result.rate):
                    mutate("failed_result", result="eio", bytes_read=0)
                if settings.invalid_range.enabled and trial("invalid", settings.invalid_range.rate):
                    mutate("invalid_range", offset=event["file_size"] + 1, result="einval")
            identity = (event["volume_id"], truth["true_file_id"])
            if settings.identity_break.enabled:
                if event["operation"] == "rename":
                    identities[identity] = opaque_id("file", streams["identity"])
                if identity in identities:
                    mutate("identity_break", file_id=identities[identity])
            offset = skew.get((event["volume_id"], event["client_id"] or ""), 0)
            if offset:
                mutate("skew", event_time=event["event_time"] + timedelta(seconds=offset))
            loss = max(
                (
                    fraction
                    for start, end, fraction in windows[event["volume_id"]]
                    if start <= at < end
                ),
                default=0,
            )
            disposition = "emitted"
            if loss and trial("loss", loss):
                disposition = "dropped"
                truth["faults"].append("dropped")
            elif settings.sampling.enabled and trial("sampling", 1 - settings.sampling.rate):
                disposition = "sampled_out"
                truth["faults"].append("sampled_out")
            bucket[disposition] += 1
            duplicate = None
            if disposition == "emitted":
                if settings.late.enabled and trial("late", settings.late.rate):
                    delay = min(
                        settings.late.max_delay_s,
                        float(streams["late"].lognormal(np.log(settings.late.median_s), 1)),
                    )
                    mutate("late", ingest_time=at + timedelta(seconds=delay))
                bucket["late"] += int(
                    (event["ingest_time"] - at).total_seconds() > settings.late.threshold_s
                )
                if settings.duplicate.enabled and trial("duplicate", settings.duplicate.rate):
                    # Positive microsecond even when a zero max-delay was configured.
                    delay = max(
                        1e-6, float(streams["duplicate"].uniform(0, settings.duplicate.max_delay_s))
                    )
                    duplicate = {
                        **event,
                        "ingest_time": event["ingest_time"] + timedelta(seconds=delay),
                    }
                    truth["faults"].append("duplicate")
                    bucket["duplicates"] += 1
            truth["original_fields_json"] = canonical_json(originals)
            yield FaultRecord(event, truth, disposition)
            if duplicate is not None:
                yield FaultRecord(duplicate, truth, "duplicate")
        self.counters = [
            dict(
                volume_id=volume,
                hour_start=hour,
                **{
                    key: int(values[key])
                    for key in (
                        "offered",
                        "emitted",
                        "dropped",
                        "sampled_out",
                        "duplicates",
                        "late",
                    )
                },
            )
            for (volume, hour), values in sorted(counts.items())
        ]
