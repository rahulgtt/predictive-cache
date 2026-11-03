"""Synthetic handle realization and metadata churn; never performs filesystem I/O."""

import heapq
from dataclasses import replace
from datetime import timedelta
from math import exp, log

import numpy as np

from .model import FilePath, Intent, PatternRecord
from .patterns.common import opaque_id
from .schema import SCHEMAS


def read_ranges(size, profile, head_bytes, maximum, rng):
    """Plan nonoverlapping byte ranges, coalesced then split at the event cap."""
    if size <= 0:
        return []
    if profile == "full":
        spans = [(0, size)]
    elif profile == "head":
        spans = [(0, min(size, head_bytes or int(rng.integers(1, 9)) * 2**20))]
    else:
        spans = []
        for _ in range(int(rng.integers(1, 21))):
            start = int(rng.integers(size))
            length = min(size - start, int(rng.integers(64 * 1024, 64 * 2**20 + 1)))
            spans.append((start, start + length))
        merged = []
        for start, end in sorted(spans):
            if merged and start <= merged[-1][1]:
                merged[-1] = (merged[-1][0], max(end, merged[-1][1]))
            else:
                merged.append((start, end))
        spans = merged
    return [
        (at, min(maximum, end - at)) for start, end in spans for at in range(start, end, maximum)
    ]


class EventRealizer:
    def __init__(self, cfg, churn=True):
        self.cfg, self.churn = cfg, churn
        self.patterns = []

    def plan_churn(self, world, window, rng):
        """Metadata proposals have their own seed, independent of read plans."""
        self.patterns = []
        result = []
        settings = self.cfg.patterns.churn
        if not self.churn or not settings.enabled:
            return result
        for volume in world.volumes:
            files = [f for f in world.files if f.volume_id == volume.volume_id]
            actors = [a for a in world.actors if a.volume_id == volume.volume_id]
            if not actors:
                continue
            cause = opaque_id("pat", rng)
            self.patterns.append(
                PatternRecord(
                    cause,
                    "churn",
                    volume.volume_id,
                    volume.period_start,
                    volume.period_end,
                    [a.actor_id for a in actors],
                    [f.file_id for f in files],
                    settings.model_dump(),
                )
            )
            for file in files:
                start = max(window[0], volume.period_start, file.created_at)
                end = min(window[1], volume.period_end, file.deleted_at or window[1])
                seconds = max(0, (end - start).total_seconds())
                for operation, rate in (
                    ("write", settings.writes_per_file_month),
                    ("rename", settings.rename_rate),
                    ("delete", settings.delete_rate),
                    ("replace", settings.replace_rate),
                ):
                    count = rng.poisson(
                        rate * self.cfg.randomness.rate_scale * seconds / (30.4375 * 86400)
                    )
                    for _ in range(int(count)):
                        actor = actors[int(rng.integers(len(actors)))]
                        result.append(
                            Intent(
                                opaque_id("int", rng),
                                start + timedelta(seconds=float(rng.uniform(0, seconds))),
                                volume.volume_id,
                                actor.actor_id,
                                file.file_id,
                                "office",
                                file.read_profile,
                                None,
                                cause,
                                operation=operation,
                            )
                        )
        return result

    def realize(self, world, intents, window, rng):
        cfg = self.cfg
        churn_rng = np.random.default_rng(np.random.SeedSequence(cfg.seed, spawn_key=(109,)))
        metadata = self.plan_churn(world, window, churn_rng)
        churn_ids = {i.intent_id for i in metadata}
        queue = [
            (i.at, i.operation == "open", i.at, i.intent_id, i)
            for i in list(intents) + metadata
            if window[0] <= i.at < window[1]
        ]
        heapq.heapify(queue)
        files = {f.file_id: f for f in world.files}
        actors = {a.actor_id: a for a in world.actors}
        paths = {}
        for path in world.file_paths:
            paths.setdefault(path.file_id, []).append(path)
        busy, sessions, jobs = {}, {}, {}
        output = []

        def path_at(fid, at):
            candidates = [
                p
                for p in paths[fid]
                if p.valid_from <= at and (p.valid_to is None or at < p.valid_to)
            ]
            # Failed stale opens retain their last known locator.
            return max(candidates or paths[fid], key=lambda p: p.valid_from)

        def set_file(file):
            files[file.file_id] = file

        def record(item, file, at, operation, size, **fields):
            actor = actors[item.actor_id]
            event = dict.fromkeys(SCHEMAS["events"].names)
            event.update(
                schema_version=1,
                event_id=opaque_id("ev", rng),
                event_time=at,
                ingest_time=at,
                volume_id=file.volume_id,
                file_id=file.file_id,
                dir_id=path_at(file.file_id, at).dir_id,
                operation=operation,
                file_size=size,
                result="ok",
                origin="foreground",
                app=item.app,
                cohort_id=actor.cohort_id if cfg.emit.cohort_id else None,
                client_id=actor.client_id if cfg.emit.client_id else None,
            )
            event.update(fields)
            truth = dict(
                event_id=event["event_id"],
                intent_id=None if item.intent_id in churn_ids else item.intent_id,
                cause=item.cause,
                faults=[],
                true_event_time=at,
                true_file_id=file.file_id,
                original_fields_json="{}",
            )
            if operation != "read" or cfg.realize.read_events == "ranges":
                output.append((event, truth))

        def end_path(fid, at):
            old = path_at(fid, at)
            paths[fid] = [replace(p, valid_to=at) if p is old else p for p in paths[fid]]
            return old

        while queue:
            at, _, _, _, item = heapq.heappop(queue)
            file = files.get(item.file_id)
            if file is None or at < file.created_at:
                continue
            actor = actors[item.actor_id]
            is_churn = item.intent_id in churn_ids
            if item.operation != "open":
                if file.deleted_at is not None and at >= file.deleted_at:
                    continue
                if item.operation in {"write", "delete", "replace"} and at <= busy.get(
                    file.file_id, at - timedelta(microseconds=1)
                ):
                    later = busy[file.file_id] + timedelta(microseconds=1)
                    heapq.heappush(queue, (later, False, item.at, item.intent_id, item))
                    continue
                size = file.size_at(at)
                if item.operation == "write":
                    if is_churn:
                        size = max(
                            1, min(2**63 - 1, int(size * float(churn_rng.uniform(0.5, 1.5))))
                        )
                        file = replace(file, size_history=file.size_history + [(at, size)])
                        set_file(file)
                    record(item, file, at, "write", size, offset=0, bytes_requested=size)
                elif item.operation == "rename":
                    end_path(file.file_id, at)
                    candidates = [
                        d
                        for d in world.directories
                        if d.volume_id == file.volume_id and d.created_at <= at
                    ]
                    directory = candidates[int(churn_rng.integers(len(candidates)))]
                    paths[file.file_id].append(
                        FilePath(
                            file.file_id,
                            directory.dir_id,
                            f"/renamed/{directory.dir_id}/{opaque_id('name', churn_rng)}",
                            at,
                        )
                    )
                    record(item, file, at, "rename", size)
                elif item.operation in {"delete", "replace"}:
                    old = end_path(file.file_id, at)
                    new_id = opaque_id("file", churn_rng) if item.operation == "replace" else None
                    set_file(replace(file, deleted_at=at, replaced_by_file_id=new_id))
                    record(item, file, at, "delete", size)
                    if new_id:
                        born = at + timedelta(microseconds=1)
                        new_file = replace(
                            file,
                            file_id=new_id,
                            created_at=born,
                            size_at_creation=size,
                            size_history=[],
                            deleted_at=None,
                            replaced_by_file_id=None,
                        )
                        set_file(new_file)
                        paths[new_id] = [
                            replace(old, file_id=new_id, valid_from=born, valid_to=None)
                        ]
                        record(item, new_file, born, "create", size)
                else:
                    record(item, file, at, item.operation, size)
                continue

            key = (actor.actor_id, file.volume_id)
            if actor.kind == "service" and item.job_run_id:
                session = jobs.setdefault((key, item.job_run_id), opaque_id("ses", rng))
            else:
                previous, session = sessions.get(key, (None, None))
                if previous is None or (at - previous).total_seconds() > cfg.sessions.gap_min * 60:
                    session = opaque_id("ses", rng)
                sessions[key] = at, session
            if file.deleted_at is not None and at >= file.deleted_at:
                if rng.random() < cfg.realize.stale_open_rate:
                    last_size = max(
                        file.size_history, default=(file.created_at, file.size_at_creation)
                    )[1]
                    record(
                        item,
                        file,
                        at,
                        "open",
                        last_size,
                        result="enoent",
                        session_id=session if cfg.emit.session_id else None,
                    )
                    continue
                # Replacement is the same logical path with a fresh identity.
                while file.replaced_by_file_id and file.deleted_at and at >= file.deleted_at:
                    file = files[file.replaced_by_file_id]
                if at < file.created_at or (file.deleted_at and at >= file.deleted_at):
                    continue
            size = file.size_at(at)
            if rng.random() < cfg.realize.eacces_rate:
                record(
                    item,
                    file,
                    at,
                    "open",
                    size,
                    result="eacces",
                    session_id=session if cfg.emit.session_id else None,
                )
                continue
            handle = opaque_id("hdl", rng)
            fields = dict(handle_id=handle, session_id=session if cfg.emit.session_id else None)
            record(item, file, at, "open", size, **fields)
            ranges = read_ranges(
                size, item.read_profile, item.head_bytes, cfg.realize.max_read_event_bytes, rng
            )
            speed = (
                exp(
                    float(
                        np.clip(
                            rng.normal(
                                log(cfg.realize.read_mbps_median), cfg.realize.read_mbps_sigma
                            ),
                            -7,
                            28,
                        )
                    )
                )
                * 2**20
            )
            elapsed, total = 0.0, 0
            for offset, length in ranges:
                elapsed += max(0.000001, length / speed)
                total += length
                record(
                    item,
                    file,
                    at + timedelta(seconds=elapsed),
                    "read",
                    size,
                    offset=offset,
                    bytes_requested=length,
                    bytes_read=length,
                    **fields,
                )
            think = (
                cfg.realize.service_think_time_max_s
                if actor.kind == "service"
                else cfg.realize.interactive_think_time_max_s
            )
            elapsed += float(rng.uniform(0, think)) + 0.000001
            close = at + timedelta(seconds=elapsed)
            busy[file.file_id] = max(busy.get(file.file_id, close), close)
            record(
                item,
                file,
                close,
                "close",
                size,
                handle_bytes_read=total,
                handle_duration_ms=round((close - at).total_seconds() * 1000),
                **fields,
            )

        # Resolve read locators after interleaved renames; sizes cannot change under a handle.
        for event, _ in output:
            event["dir_id"] = path_at(event["file_id"], event["event_time"]).dir_id
        world.files[:] = files.values()
        world.file_paths[:] = [p for history in paths.values() for p in history]
        yield from sorted(output, key=lambda pair: (pair[0]["event_time"], pair[0]["event_id"]))
