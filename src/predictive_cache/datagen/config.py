"""Validated scenario configuration; controls are contracts for later generators."""

from __future__ import annotations

import re
from copy import deepcopy
from datetime import date, datetime, timedelta
from math import isclose
from pathlib import Path
from typing import Annotated, Any, Literal
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

Probability = Annotated[float, Field(ge=0, le=1, strict=True)]
NonNegative = Annotated[float, Field(ge=0, allow_inf_nan=False, strict=True)]
Positive = Annotated[float, Field(gt=0, allow_inf_nan=False, strict=True)]
Count = Annotated[int, Field(ge=0, strict=True)]
PositiveCount = Annotated[int, Field(gt=0, strict=True)]
Seconds = Annotated[float, Field(ge=0, le=604800, strict=True)]
Preset = Literal["tiny", "small", "medium", "custom"]


class Settings(BaseModel):
    model_config = ConfigDict(extra="forbid", validate_default=True)


def validate_local_time(value: str) -> str:
    if not re.fullmatch(r"(?:[01][0-9]|2[0-3]):[0-5][0-9]", value):
        raise ValueError("local time must be HH:MM")
    return value


class Period(Settings):
    start: date = date(2026, 1, 5)
    weeks: PositiveCount = 8

    @field_validator("start", mode="before")
    @classmethod
    def date_only(cls, value: Any) -> Any:
        if isinstance(value, datetime) or isinstance(value, (int, float)):
            raise ValueError("start must be a calendar date")
        if isinstance(value, str) and not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
            raise ValueError("start must be YYYY-MM-DD")
        return value

    @model_validator(mode="after")
    def bounded_period(self) -> Period:
        try:
            self.start + timedelta(weeks=self.weeks, days=8)
        except (OverflowError, ValueError) as exc:
            raise ValueError("period and spill must fit the supported calendar") from exc
        if self.start.year < 2:
            raise ValueError("period must leave room for timezone offsets")
        return self


class Volume(Settings):
    timezone: str = "America/New_York"
    holidays: list[date] = Field(default_factory=lambda: [date(2026, 1, 19), date(2026, 2, 16)])
    work_days: list[Annotated[int, Field(ge=1, le=7, strict=True)]] = Field(
        default_factory=lambda: [1, 2, 3, 4, 5]
    )
    work_hours: tuple[str, str] = ("08:00", "18:00")

    @field_validator("timezone")
    @classmethod
    def known_timezone(cls, value: str) -> str:
        try:
            ZoneInfo(value)
        except (ZoneInfoNotFoundError, ValueError) as exc:
            raise ValueError("timezone must be a known IANA zone") from exc
        return value

    @field_validator("work_days")
    @classmethod
    def unique_days(cls, value: list[int]) -> list[int]:
        if len(set(value)) != len(value):
            raise ValueError("work_days must be unique")
        return value

    @field_validator("work_hours")
    @classmethod
    def ordered_hours(cls, value: tuple[str, str]) -> tuple[str, str]:
        start, end = map(validate_local_time, value)
        if start >= end:
            raise ValueError("work_hours must be increasing within one day")
        return value


class Calendar(Settings):
    holiday_activity: Probability = 0.05


class LifecycleWeeks(Settings):
    ramp: tuple[PositiveCount, PositiveCount] = (1, 3)
    active: tuple[PositiveCount, PositiveCount] = (4, 16)
    cooldown: tuple[PositiveCount, PositiveCount] = (2, 6)

    @model_validator(mode="after")
    def ordered(self) -> LifecycleWeeks:
        for value in (self.ramp, self.active, self.cooldown):
            if value[0] > value[1]:
                raise ValueError("lifecycle ranges must be increasing")
        return self


class LifecycleActivity(Settings):
    ramp: NonNegative = 0.3
    active: NonNegative = 1.0
    cooldown: NonNegative = 0.3
    archived: NonNegative = 0.02


class Projects(Settings):
    initial: PositiveCount = 20
    new_per_month: NonNegative = 2
    max_depth: PositiveCount = 3
    branching_mean: Positive = 3
    lifecycle_weeks: LifecycleWeeks = Field(default_factory=LifecycleWeeks)
    lifecycle_activity: LifecycleActivity = Field(default_factory=LifecycleActivity)


class FileKinds(Settings):
    doc: Probability = 0.35
    drawing: Probability = 0.15
    cad_model: Probability = 0.08
    image: Probability = 0.12
    media: Probability = 0.05
    dataset: Probability = 0.05
    small: Probability = 0.20

    @model_validator(mode="after")
    def sum_to_one(self) -> FileKinds:
        if not isclose(sum(self.model_dump().values()), 1.0, abs_tol=1e-9):
            raise ValueError("file kind shares must sum to one")
        return self


class SizeDistribution(Settings):
    median_bytes: Positive
    sigma: NonNegative = 1.0
    min_bytes: PositiveCount
    max_bytes: Annotated[int, Field(gt=0, le=2**63 - 1, strict=True)]

    @model_validator(mode="after")
    def ordered_bounds(self) -> SizeDistribution:
        if self.min_bytes > self.max_bytes:
            raise ValueError("size min_bytes must not exceed max_bytes")
        return self


class SizeDistributions(Settings):
    doc: SizeDistribution = Field(
        default_factory=lambda: SizeDistribution(
            median_bytes=400 * 1024, min_bytes=4 * 1024, max_bytes=50 * 2**20
        )
    )
    drawing: SizeDistribution = Field(
        default_factory=lambda: SizeDistribution(
            median_bytes=20 * 2**20, min_bytes=100 * 1024, max_bytes=500 * 2**20
        )
    )
    cad_model: SizeDistribution = Field(
        default_factory=lambda: SizeDistribution(
            median_bytes=150 * 2**20, min_bytes=5 * 2**20, max_bytes=4 * 2**30
        )
    )
    image: SizeDistribution = Field(
        default_factory=lambda: SizeDistribution(
            median_bytes=4 * 2**20, min_bytes=50 * 1024, max_bytes=200 * 2**20
        )
    )
    media: SizeDistribution = Field(
        default_factory=lambda: SizeDistribution(
            median_bytes=300 * 2**20, min_bytes=10 * 2**20, max_bytes=20 * 2**30
        )
    )
    dataset: SizeDistribution = Field(
        default_factory=lambda: SizeDistribution(
            median_bytes=2**30, min_bytes=50 * 2**20, max_bytes=50 * 2**30
        )
    )
    small: SizeDistribution = Field(
        default_factory=lambda: SizeDistribution(
            median_bytes=8 * 1024, min_bytes=100, max_bytes=256 * 1024
        )
    )


class Files(Settings):
    target_count: PositiveCount = 5000
    per_dir_mean: Positive = 25
    kinds: FileKinds = Field(default_factory=FileKinds)
    size_distributions: SizeDistributions = Field(default_factory=SizeDistributions)


class Actors(Settings):
    users: PositiveCount = 50
    teams: PositiveCount = 6
    service: Count = 2
    vacation_weeks_per_quarter: Annotated[float, Field(ge=0, le=13, strict=True)] = 0.5


class Enabled(Settings):
    enabled: Annotated[bool, Field(strict=True)] = True


class Popularity(Enabled):
    rate_per_user_hour: NonNegative = 6
    zipf_s: Positive = 1.1
    recency_boost_half_life_days: NonNegative = 3
    diurnal_curve: list[tuple[Annotated[float, Field(ge=0, le=24)], NonNegative]] = Field(
        default_factory=lambda: [
            (0.0, 0.0),
            (8.0, 0.3),
            (10.0, 1.0),
            (12.0, 0.55),
            (14.0, 1.0),
            (17.0, 0.6),
            (19.0, 0.0),
            (24.0, 0.0),
        ]
    )

    @field_validator("diurnal_curve")
    @classmethod
    def ordered_curve(cls, value):
        if len(value) < 2 or any(a[0] >= b[0] for a, b in zip(value, value[1:])):
            raise ValueError("diurnal_curve needs at least two strictly increasing hours")
        return value

    working_set_size: PositiveCount = 20
    working_set_share: Probability = 0.7
    revisit_share: Probability = 0.2
    revisit_history: PositiveCount = 10


class WeeklySchedule(Enabled):
    instances_per_team: Count = 2
    jitter_min: Annotated[float, Field(ge=0, le=10080, strict=True)] = 10
    attendance: Probability = 0.85
    files_per_set: tuple[PositiveCount, PositiveCount] = (1, 20)

    @field_validator("files_per_set")
    @classmethod
    def ordered(cls, value: tuple[int, int]) -> tuple[int, int]:
        if value[0] > value[1]:
            raise ValueError("files_per_set must be increasing")
        return value


class WorkflowChain(Enabled):
    chains_per_project: Count = 3
    length_geom_p: Annotated[float, Field(gt=0, le=1, strict=True)] = 0.35
    delay_median_s: Annotated[float, Field(gt=0, le=604800, strict=True)] = 120
    delay_sigma: NonNegative = 1.0
    continue_p: Probability = 0.7
    drift_share: Probability = 0.2
    context_order: Literal[1, 2] = 1
    max_steps: PositiveCount = 20
    interruption_p: Probability = 0.05

    @field_validator("context_order", mode="before")
    @classmethod
    def integer_order(cls, value: Any) -> Any:
        if type(value) is not int:
            raise ValueError("context_order must be integer 1 or 2")
        return value


class Burst(Enabled):
    bursts_per_volume_month: NonNegative = 3
    scan_share: Probability = 0.3
    participation: Probability = 0.7
    shared_file_share: Probability = 0.6
    arrival_spread_s: Seconds = 3600
    duration_s: Annotated[float, Field(gt=0, le=604800, strict=True)] = 10800

    @model_validator(mode="after")
    def arrival_before_deadline(self) -> Burst:
        if self.arrival_spread_s > self.duration_s:
            raise ValueError("arrival_spread_s must not exceed duration_s")
        return self


class BatchJob(Enabled):
    jobs_per_volume: Count = 2
    local_time: str = "02:00"
    head_bytes: PositiveCount = 1048576

    _time = field_validator("local_time")(validate_local_time)


class NewFile(Enabled):
    new_files_per_project_week: NonNegative = 5
    open_decay_half_life_days: Positive = 2
    initial_opens_per_day: NonNegative = 12


class Churn(Enabled):
    writes_per_file_month: NonNegative = 0.5
    rename_rate: NonNegative = 0.01
    delete_rate: NonNegative = 0.005
    replace_rate: NonNegative = 0.005


class Patterns(Settings):
    popularity: Popularity = Field(default_factory=Popularity)
    weekly_schedule: WeeklySchedule = Field(default_factory=WeeklySchedule)
    workflow_chain: WorkflowChain = Field(default_factory=WorkflowChain)
    project_lifecycle: Enabled = Field(default_factory=Enabled)
    burst: Burst = Field(default_factory=Burst)
    batch_job: BatchJob = Field(default_factory=BatchJob)
    new_file: NewFile = Field(default_factory=NewFile)
    noise: Enabled = Field(default_factory=Enabled)
    churn: Churn = Field(default_factory=Churn)


class Randomness(Settings):
    noise_share: Probability = 0.05
    jitter_scale: NonNegative = 1.0
    rate_scale: NonNegative = 1.0


class Realize(Settings):
    read_events: Literal["ranges", "none"] = "ranges"
    max_read_event_bytes: PositiveCount = 16777216
    read_mbps_median: Positive = 200
    read_mbps_sigma: NonNegative = 0.5
    interactive_think_time_max_s: Seconds = 300
    service_think_time_max_s: Seconds = 0
    stale_open_rate: Probability = 0.002
    eacces_rate: Probability = 0.001


class Sessions(Settings):
    gap_min: Annotated[float, Field(gt=0, le=10080, strict=True)] = 10


class Emit(Settings):
    session_id: Annotated[bool, Field(strict=True)] = True
    cohort_id: Annotated[bool, Field(strict=True)] = True
    client_id: Annotated[bool, Field(strict=True)] = True


class WarmerOrigin(Enabled):
    top_n: PositiveCount = 200
    local_time: str = "01:00"

    _time = field_validator("local_time")(validate_local_time)


class RateFault(Enabled):
    rate: Probability = 0


class IdentityBreak(Enabled):
    enabled: Annotated[bool, Field(strict=True)] = False


class ClockSkew(Enabled):
    sigma_s: Seconds = 2
    outlier_share: Probability = 0.01
    outlier_s: Seconds = 300


class LossWindow(Enabled):
    per_volume_month: NonNegative = 2
    hours: tuple[
        Annotated[float, Field(gt=0, le=168, strict=True)],
        Annotated[float, Field(gt=0, le=168, strict=True)],
    ] = (1, 6)
    loss_fraction: tuple[Probability, Probability] = (0.3, 1.0)

    @model_validator(mode="after")
    def ordered(self) -> LossWindow:
        if self.hours[0] > self.hours[1] or self.loss_fraction[0] > self.loss_fraction[1]:
            raise ValueError("loss window ranges must be increasing")
        return self


class Late(RateFault):
    rate: Probability = 0.01
    median_s: Annotated[float, Field(gt=0, le=21600, strict=True)] = 300
    threshold_s: Annotated[float, Field(ge=0, le=21600, strict=True)] = 60
    max_delay_s: Annotated[float, Field(gt=0, le=21600, strict=True)] = 21600


class Duplicate(RateFault):
    rate: Probability = 0.005
    max_delay_s: Annotated[float, Field(gt=0, le=21600, strict=True)] = 21600


class Faults(Settings):
    warmer_origin: WarmerOrigin = Field(default_factory=WarmerOrigin)
    synthetic_origin: Enabled = Field(default_factory=Enabled)
    unknown_origin: RateFault = Field(default_factory=lambda: RateFault(rate=0.002))
    failed_result: RateFault = Field(default_factory=lambda: RateFault(rate=0.003))
    invalid_range: RateFault = Field(default_factory=lambda: RateFault(rate=0.0005))
    identity_break: IdentityBreak = Field(default_factory=IdentityBreak)
    clock_skew: ClockSkew = Field(default_factory=ClockSkew)
    loss_window: LossWindow = Field(default_factory=LossWindow)
    sampling: RateFault = Field(default_factory=lambda: RateFault(rate=1.0))
    late: Late = Field(default_factory=Late)
    duplicate: Duplicate = Field(default_factory=Duplicate)


class Config(Settings):
    preset: Preset = "small"
    seed: Annotated[int, Field(ge=0, strict=True)] = 42
    format: Literal["parquet", "jsonl"] = "parquet"
    period: Period = Field(default_factory=Period)
    volumes: Annotated[list[Volume], Field(min_length=1)] = Field(
        default_factory=lambda: [Volume()]
    )
    calendar: Calendar = Field(default_factory=Calendar)
    projects: Projects = Field(default_factory=Projects)
    files: Files = Field(default_factory=Files)
    actors: Actors = Field(default_factory=Actors)
    patterns: Patterns = Field(default_factory=Patterns)
    randomness: Randomness = Field(default_factory=Randomness)
    realize: Realize = Field(default_factory=Realize)
    sessions: Sessions = Field(default_factory=Sessions)
    emit: Emit = Field(default_factory=Emit)
    faults: Faults = Field(default_factory=Faults)


PRESETS: dict[str, dict[str, Any]] = {
    "small": {},
    "custom": {},
    "tiny": {
        "period": {"weeks": 2},
        "files": {"target_count": 300},
        "actors": {"users": 10},
        "randomness": {"rate_scale": 0.5},
        "faults": {"warmer_origin": {"top_n": 10}},
        "realize": {"max_read_event_bytes": 64 * 2**20},
    },
    "medium": {
        "period": {"weeks": 12},
        "files": {"target_count": 50000},
        "actors": {"users": 300},
        "volumes": [
            {"timezone": "America/New_York"},
            {"timezone": "Europe/London", "holidays": []},
            {"timezone": "Asia/Kolkata", "holidays": []},
        ],
    },
}


def _merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    result = deepcopy(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = _merge(result[key], value)
        else:
            result[key] = deepcopy(value)
    return result


def load_config(
    path: Path | None = None,
    preset: str | None = None,
    seed: int | None = None,
    format: str | None = None,
) -> Config:
    """Apply preset defaults, recursive file overrides, then explicit CLI values.

    Lists replace as units; an omitted file preset selects small. Custom starts
    from small defaults. Resolution never modifies caller data or preset tables.
    """
    overrides: dict[str, Any] = {}
    if path is not None:
        loaded = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
        if loaded is not None:
            if not isinstance(loaded, dict):
                raise ValueError("configuration YAML must contain a mapping")
            overrides = loaded
    selected = preset if preset is not None else overrides.get("preset", "small")
    if not isinstance(selected, str) or selected not in PRESETS:
        raise ValueError(f"unknown preset: {selected!r}")
    resolved = _merge(Config().model_dump(mode="json"), PRESETS[selected])
    resolved = _merge(resolved, overrides)
    resolved["preset"] = selected
    if seed is not None:
        resolved["seed"] = seed
    if format is not None:
        resolved["format"] = format
    return Config.model_validate(resolved)
