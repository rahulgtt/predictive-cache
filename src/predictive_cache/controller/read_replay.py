"""Bounded, synthetic-only READ replay using the controller components."""

from __future__ import annotations

import os
import stat
from dataclasses import asdict
from datetime import datetime, timedelta
from pathlib import Path

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator

from predictive_cache.controller.health import ControllerHealth
from predictive_cache.controller.models import ModelManager
from predictive_cache.controller.planner import PlannerConfig, PlannerPublisher
from predictive_cache.controller.read_memory import CoverageObservation, ReadMemory
from predictive_cache.controller.read_runtime import ReadReplayRuntime
from predictive_cache.controller.sources import ReplayClock
from predictive_cache.dispatcher import securefs
from predictive_cache.dispatcher.core import Dispatcher, DispatcherConfig
from predictive_cache.dispatcher.schema import format_utc_z, parse_utc_z
from predictive_cache.semantics.read_demand import ReadDemandEvent


class ReplayInputError(ValueError):
    """Invalid replay input; never include source payloads in its text."""


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")


class ReplayLimits(_Strict):
    max_events: int = Field(default=100_000, strict=True, ge=1, le=100_000)
    max_files: int = Field(default=10_000, strict=True, ge=1, le=100_000)
    max_coverage: int = Field(default=10_000, strict=True, ge=1, le=10_000)
    max_input_bytes: int = Field(default=32 * 2**20, strict=True, ge=1, le=64 * 2**20)
    max_line_bytes: int = Field(default=64 * 2**10, strict=True, ge=1, le=64 * 2**10)
    max_replay_seconds: int = Field(default=3600, strict=True, ge=1, le=86400)


class ReadReplayConfig(_Strict):
    schema_version: int = Field(default=1, strict=True, ge=1, le=1)
    volume_id: str = Field(pattern=r"^[A-Za-z0-9._:-]{1,128}$")
    run_id: str = Field(pattern=r"^[A-Za-z0-9._:-]{1,80}$")
    controller_id: str = Field(default="read-replay", pattern=r"^[A-Za-z0-9._:-]{1,80}$")
    events: Path
    coverage: Path
    model_pointer: Path
    dispatcher_root: Path
    start: datetime
    ticks: int = Field(default=3, strict=True, ge=1, le=1000)
    verified_file_size: bool = Field(default=False, strict=True)
    planner_max_items: int | None = Field(default=None, strict=True, ge=0)
    planner_max_bytes: int | None = Field(default=None, strict=True, ge=0)
    limits: ReplayLimits = Field(default_factory=ReplayLimits)

    @field_validator("start", mode="before")
    @classmethod
    def start_utc(cls, value):
        return parse_utc_z(value)


def _regular_bytes(path: Path, maximum: int) -> bytes:
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, "rb") as stream:
        if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
            raise ReplayInputError("replay input must be a regular file")
        data = stream.read(maximum + 1)
    if len(data) > maximum:
        raise ReplayInputError("replay input byte limit exceeded")
    return data


def _jsonl(path: Path, model, limits: ReplayLimits, count_limit: int):
    """Bound allocations even for an appended file or a giant single line."""
    result = []
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    with os.fdopen(fd, "rb") as stream:
        info = os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_size > limits.max_input_bytes:
            raise ReplayInputError("invalid replay input file or size")
        total = 0
        while line := stream.readline(limits.max_line_bytes + 1):
            total += len(line)
            if len(line) > limits.max_line_bytes or total > limits.max_input_bytes:
                raise ReplayInputError("replay input byte limit exceeded")
            if not line.strip():
                continue
            if len(result) >= count_limit:
                raise ReplayInputError("replay input row limit exceeded")
            result.append(model.model_validate(securefs.loads_strict_json(line.decode("utf-8"))))
    return result


def _load(path: Path):
    cfg = ReadReplayConfig.model_validate(yaml.safe_load(_regular_bytes(path, 64 * 2**10)))
    for field in ("events", "coverage", "model_pointer", "dispatcher_root"):
        value = getattr(cfg, field)
        if not value.is_absolute():
            setattr(cfg, field, path.absolute().parent / value)
    events = _jsonl(cfg.events, ReadDemandEvent, cfg.limits, cfg.limits.max_events)
    # A replay never interprets real data as a fictional, authorized NFS target.
    if any(row.data_kind != "synthetic" for row in events):
        raise ReplayInputError("replay-read accepts synthetic fixtures only")
    coverage = _jsonl(cfg.coverage, CoverageObservation, cfg.limits, cfg.limits.max_coverage)
    events.sort(key=lambda row: (row.known_at, row.event_id))
    coverage.sort(key=lambda row: (row.known_at, row.hour_start))
    return cfg, events, coverage


def _empty_private_root(path: Path) -> None:
    fd = securefs.require_secure_directory(path)
    try:
        with os.scandir(fd) as entries:
            if next(entries, None) is not None:
                raise ReplayInputError("replay requires a fresh empty private root")
    finally:
        os.close(fd)


def _save_summary(root: Path, summary: dict, metrics: str) -> None:
    fd = securefs.require_secure_directory(root)
    try:
        if not securefs.atomic_write_json(
            fd, "read-replay-summary.json", summary, no_overwrite=True
        ):
            raise ReplayInputError("replay summary already exists")
        if not securefs.atomic_write_bytes(
            fd, "read-replay-metrics.prom", metrics.encode(), no_overwrite=True
        ):
            raise ReplayInputError("replay metrics already exist")
    finally:
        os.close(fd)


def run_read_replay(config_path: Path) -> dict:
    """Run virtual ticks; live reads and live target inputs are unconfigurable."""
    cfg = clock = health = runtime = None
    try:
        cfg, events, coverage = _load(config_path)
        # Validate file-backed identifiers before creating any artifacts.
        publisher = PlannerPublisher(
            cfg.dispatcher_root, run_id=cfg.run_id, controller_id=cfg.controller_id
        )
        _empty_private_root(cfg.dispatcher_root)
        memory = ReadMemory(
            cfg.volume_id,
            verified_file_size=cfg.verified_file_size,
            max_events=cfg.limits.max_events,
            max_files=cfg.limits.max_files,
            max_coverage=cfg.limits.max_coverage,
        )
        clock = ReplayClock(cfg.start, speed=0)
        health = ControllerHealth()
        models = ModelManager(
            cfg.model_pointer, contract=memory.contract, clock=lambda: clock.now().timestamp()
        )
        trace = []
        event_index = coverage_index = skipped = 0
        t = cfg.start
        skip_next = False
        stopped_for_duration = False
        try:
            dispatcher_cfg = DispatcherConfig(
                root=cfg.dispatcher_root,
                warmer_command=cfg.dispatcher_root / "unused-replay-warmer",
                host="192.0.2.1",
                real_reads=False,
            )
            with Dispatcher(dispatcher_cfg, now=clock.now) as dispatcher:
                runtime = ReadReplayRuntime(
                    memory,
                    models,
                    publisher,
                    dispatcher,
                    health=health,
                    planner_config=PlannerConfig(
                        max_items=cfg.planner_max_items, max_bytes=cfg.planner_max_bytes
                    ),
                    max_ticks=cfg.ticks,
                )
                tick_s = 60
                for _ in range(cfg.ticks):
                    if (t - cfg.start).total_seconds() > cfg.limits.max_replay_seconds:
                        stopped_for_duration = True
                        break
                    clock.advance(t)
                    if skip_next:
                        skipped += 1
                        skip_next = False
                    else:
                        event_end, coverage_end = event_index, coverage_index
                        while event_end < len(events) and events[event_end].known_at <= t:
                            event_end += 1
                        while coverage_end < len(coverage) and coverage[coverage_end].known_at <= t:
                            coverage_end += 1
                        result = runtime.tick(
                            t,
                            events=events[event_index:event_end],
                            coverage=coverage[coverage_index:coverage_end],
                        )
                        event_index, coverage_index = event_end, coverage_end
                        tick_s, skip_next = result.tick_s, result.overrun
                        entry = asdict(result)
                        entry["t"] = format_utc_z(result.t)
                        trace.append(entry)
                    # Keep all timestamp arithmetic inside the bounded replay interval.
                    if tick_s > cfg.limits.max_replay_seconds:
                        stopped_for_duration = True
                        break
                    t += timedelta(seconds=tick_s)
                summary = runtime.summary(clock.now())
                summary["replay"] = {
                    "scheduled_slots": len(trace) + skipped,
                    "executed_ticks": len(trace),
                    "skipped_overrun_slots": skipped,
                    "duration_limit_reached": stopped_for_duration,
                    "remaining_events": len(events) - event_index,
                    "remaining_coverage": len(coverage) - coverage_index,
                    "ticks": trace,
                }
                _save_summary(cfg.dispatcher_root, summary, health.metrics())
                return summary
        finally:
            models.close()
    except (OSError, ValueError, TypeError, OverflowError, RuntimeError, yaml.YAMLError):
        # Pydantic and filesystem exceptions can contain payloads or private paths.
        if runtime is not None:
            try:
                state = health.snapshot()
                reasons = (set(state["reasons"]) - {"not_started", "no_scorer"}) | {
                    "controller_error"
                }
                health.set_state(
                    started=True, has_scorer=state["status"] != "not_ready", reasons=reasons
                )
                summary = runtime.summary(clock.now())
                summary["failed"] = True
                _save_summary(cfg.dispatcher_root, summary, health.metrics())
            except (OSError, ValueError, TypeError, RuntimeError, OverflowError):
                # Never overwrite previous evidence or hide the original failure.
                pass
        raise ReplayInputError(
            "READ replay failed; check fixture inputs and private run artifacts"
        ) from None
