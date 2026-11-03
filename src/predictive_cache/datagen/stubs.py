"""Small plumbing fixtures, deliberately not the #14–#18 workloads."""

from datetime import UTC, datetime, time, timedelta

import numpy as np

from .catalog import catalog_rows as catalog_rows
from .model import Actor, Directory, File, FilePath, Intent, Volume, World
from .schema import SCHEMAS

LIMITATION = (
    "foundation_stub: one file, directory, actor and successful handle per configured volume; "
    "target sizes, workload patterns, churn, sessions and fault controls are not implemented. "
    "Only timezone/period, seed, read_events, max_read_event_bytes and emit controls affect fixtures."
)


def opaque_id(prefix: str, rng: np.random.Generator) -> str:
    return f"{prefix}-{rng.bytes(12).hex()}"


def build_world(cfg, rng: np.random.Generator) -> World:
    from zoneinfo import ZoneInfo

    volumes, directories, files, paths, actors = [], [], [], [], []
    for settings in cfg.volumes:
        start = datetime.combine(cfg.period.start, time(), ZoneInfo(settings.timezone))
        end = start + timedelta(weeks=cfg.period.weeks)
        start, end = start.astimezone(UTC), end.astimezone(UTC)
        vid, did, fid, aid = [opaque_id(p, rng) for p in ("vol", "dir", "file", "act")]
        volumes.append(
            Volume(
                vid,
                settings.timezone,
                settings.work_days,
                *settings.work_hours,
                settings.holidays,
                start,
                end,
            )
        )
        directories.append(Directory(did, vid, None, None, 0, start))
        files.append(File(fid, vid, "small", 1, start))
        paths.append(FilePath(fid, did, "/shared/amber.txt", start))
        actors.append(
            Actor(
                aid,
                vid,
                "user",
                opaque_id("coh", rng),
                aid,
                [],
                *settings.work_hours,
                settings.work_days,
                settings.timezone,
                settings.holidays,
            )
        )
    return World(volumes, [], directories, files, actors, paths)


def fixture_intents(world, rng):
    for volume in world.volumes:
        at = volume.period_start + timedelta(seconds=1)
        actor = next((a for a in world.actors if a.volume_id == volume.volume_id), None)
        file = next(
            (
                f
                for f in world.files
                if f.volume_id == volume.volume_id
                and f.created_at <= at
                and (f.deleted_at is None or at < f.deleted_at)
            ),
            None,
        )
        if actor is None or file is None:
            continue
        yield Intent(
            opaque_id("int", rng),
            volume.period_start + timedelta(seconds=1),
            volume.volume_id,
            actor.actor_id,
            file.file_id,
            "office",
            "full",
            None,
            "background",
        )


class StubRealizer:
    def __init__(self, cfg):
        self.cfg = cfg

    def realize(self, world, intents, window, rng):
        files = {f.file_id: f for f in world.files}
        paths = {p.file_id: p for p in world.file_paths}
        actors = {a.actor_id: a for a in world.actors}
        for intent in intents:
            if not window[0] <= intent.at < window[1]:
                continue
            file = files[intent.file_id]
            path = paths[file.file_id]
            actor = actors[intent.actor_id]
            handle, session = opaque_id("hdl", rng), opaque_id("ses", rng)
            metadata = intent.operation != "open"
            operations = [intent.operation] if metadata else ["open", "read", "close"]
            if not metadata and self.cfg.realize.read_events == "none":
                operations.remove("read")
            for operation in operations:
                at = intent.at + timedelta(
                    seconds={"open": 0, "read": 1, "close": 2, "create": 0, "write": 0}[operation]
                )
                row = dict.fromkeys(SCHEMAS["events"].names)
                row.update(
                    schema_version=1,
                    event_id=opaque_id("ev", rng),
                    event_time=at,
                    ingest_time=at,
                    volume_id=file.volume_id,
                    file_id=file.file_id,
                    dir_id=path.dir_id,
                    operation=operation,
                    handle_id=None if metadata else handle,
                    file_size=file.size_at(intent.at),
                    result="ok",
                    origin="foreground",
                    app=intent.app,
                    session_id=session if self.cfg.emit.session_id else None,
                    cohort_id=actor.cohort_id if self.cfg.emit.cohort_id else None,
                    client_id=actor.client_id if self.cfg.emit.client_id else None,
                )
                if operation == "read":
                    row.update(offset=0, bytes_requested=1, bytes_read=1)
                if operation == "close":
                    row.update(handle_bytes_read=1, handle_duration_ms=2000)
                truth = dict(
                    event_id=row["event_id"],
                    intent_id=intent.intent_id,
                    cause=intent.cause,
                    faults=[],
                    true_event_time=at,
                    true_file_id=file.file_id,
                    original_fields_json="{}",
                )
                yield row, truth


class NoFaults:
    name = "foundation_no_faults"

    def apply(self, events, world, rng):
        from .model import FaultRecord

        for event, truth in events:
            yield FaultRecord(event, truth, "emitted")


class StubPatternGenerator:
    """Interface fixture: no planted pattern; one background intent per volume."""

    kind = "foundation_fixture"

    def plan(self, world, cfg, rng):
        return []

    def intents(self, world, window, rng):
        yield from sorted(
            (
                intent
                for intent in fixture_intents(world, rng)
                if window[0] <= intent.at < window[1]
            ),
            key=lambda intent: (intent.at, intent.intent_id),
        )
