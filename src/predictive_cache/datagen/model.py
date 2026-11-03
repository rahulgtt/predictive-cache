"""Shared typed interfaces; workload implementations are supplied by later stories."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, time
from typing import TYPE_CHECKING, Any, ClassVar, Iterator, Literal, Protocol
from zoneinfo import ZoneInfo

import numpy as np

if TYPE_CHECKING:
    from .config import Config

EventRow = dict[str, Any]
EventTruthRow = dict[str, Any]


@dataclass(frozen=True)
class Volume:
    volume_id: str
    timezone: str
    work_days: list[int]
    work_start_local: str
    work_end_local: str
    holidays: list[date]
    period_start: datetime
    period_end: datetime


@dataclass(frozen=True)
class Project:
    project_id: str
    volume_id: str
    # Effective-time activity points allow later lifecycle implementations.
    activity_history: list[tuple[datetime, float]] = field(default_factory=list)

    started_at: datetime | None = None
    ramp_end: datetime | None = None
    active_end: datetime | None = None
    cooldown_end: datetime | None = None
    phase_activity: tuple[float, float, float, float] = (0.3, 1.0, 0.3, 0.02)

    def activity(self, t: datetime) -> float:
        # Legacy activity_history remains a step function for old fixtures.
        if all(
            x is not None
            for x in (self.started_at, self.ramp_end, self.active_end, self.cooldown_end)
        ):
            ramp, active, cooldown, archived = self.phase_activity
            if t < self.started_at:
                return 0.0
            if t < self.ramp_end:
                fraction = (t - self.started_at) / (self.ramp_end - self.started_at)
                return ramp + fraction * (active - ramp)
            if t < self.active_end:
                return active
            if t < self.cooldown_end:
                fraction = (t - self.active_end) / (self.cooldown_end - self.active_end)
                return active + fraction * (cooldown - active)
            return archived
        points = [point for point in self.activity_history if point[0] <= t]
        return max(points, key=lambda point: point[0])[1] if points else 0.0


@dataclass(frozen=True)
class Directory:
    dir_id: str
    volume_id: str
    parent_dir_id: str | None
    project_id: str | None
    depth: int
    created_at: datetime


@dataclass(frozen=True)
class File:
    file_id: str
    volume_id: str
    kind: str
    size_at_creation: int
    created_at: datetime
    deleted_at: datetime | None = None
    replaced_by_file_id: str | None = None
    project_id: str | None = None
    size_history: list[tuple[datetime, int]] = field(default_factory=list)

    read_profile: Literal["full", "head", "ranges"] = "full"

    def size_at(self, t: datetime) -> int:
        if t < self.created_at or (self.deleted_at is not None and t >= self.deleted_at):
            raise ValueError("file does not exist at the requested time")
        points = [point for point in self.size_history if point[0] <= t]
        return max(points, key=lambda point: point[0])[1] if points else self.size_at_creation


@dataclass(frozen=True)
class FilePath:
    file_id: str
    dir_id: str
    path: str
    valid_from: datetime
    valid_to: datetime | None = None


@dataclass(frozen=True)
class Actor:
    actor_id: str
    volume_id: str
    kind: Literal["user", "service"]
    cohort_id: str | None
    client_id: str | None
    project_ids: list[str]
    schedule_start_local: str
    schedule_end_local: str
    work_days: list[int]
    timezone: str = "UTC"
    holidays: list[date] = field(default_factory=list)
    vacations: list[tuple[datetime, datetime]] = field(default_factory=list)

    working_set_file_ids: list[str] = field(default_factory=list)
    app_weights: dict[str, float] = field(default_factory=dict)

    def is_working(self, t: datetime) -> bool:
        local = t.astimezone(ZoneInfo(self.timezone))
        return (
            local.isoweekday() in self.work_days
            and local.date() not in self.holidays
            and time.fromisoformat(self.schedule_start_local)
            <= local.time()
            < time.fromisoformat(self.schedule_end_local)
            and not any(start <= t < end for start, end in self.vacations)
        )


@dataclass(frozen=True)
class World:
    volumes: list[Volume]
    projects: list[Project]
    directories: list[Directory]
    files: list[File]
    actors: list[Actor]
    file_paths: list[FilePath] = field(default_factory=list)

    def activity(self, project_id: str, t: datetime) -> float:
        return next(p for p in self.projects if p.project_id == project_id).activity(t)

    def is_holiday(self, volume_id: str, t: datetime) -> bool:
        volume = next(v for v in self.volumes if v.volume_id == volume_id)
        return t.astimezone(ZoneInfo(volume.timezone)).date() in volume.holidays


@dataclass(frozen=True)
class Intent:
    intent_id: str
    at: datetime
    volume_id: str
    actor_id: str
    file_id: str
    app: str
    read_profile: Literal["full", "head", "ranges"]
    head_bytes: int | None
    cause: str
    workflow_run_id: str | None = None
    job_run_id: str | None = None
    operation: Literal["open", "create", "write", "rename", "delete", "replace"] = "open"


@dataclass(frozen=True)
class PatternRecord:
    pattern_id: str
    kind: str
    volume_id: str
    active_from: datetime
    active_to: datetime | None
    actors: list[str]
    files: list[str]
    params: dict[str, Any]


@dataclass(frozen=True)
class FaultRecord:
    event: EventRow
    truth: EventTruthRow
    disposition: Literal["emitted", "dropped", "sampled_out", "duplicate"]


class PatternGenerator(Protocol):
    kind: ClassVar[str]

    def plan(self, world: World, cfg: Any, rng: np.random.Generator) -> list[PatternRecord]: ...

    def intents(
        self, world: World, window: tuple[datetime, datetime], rng: np.random.Generator
    ) -> Iterator[Intent]: ...


class WorldBuilder(Protocol):
    def __call__(self, cfg: Config, rng: np.random.Generator) -> World: ...


class EventRealizer(Protocol):
    def realize(
        self,
        world: World,
        intents: Iterator[Intent],
        window: tuple[datetime, datetime],
        rng: np.random.Generator,
    ) -> Iterator[tuple[EventRow, EventTruthRow]]: ...


class FaultInjector(Protocol):
    name: ClassVar[str]

    def apply(
        self,
        events: Iterator[tuple[EventRow, EventTruthRow]],
        world: World,
        rng: np.random.Generator,
    ) -> Iterator[FaultRecord]: ...
