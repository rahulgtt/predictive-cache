"""Bounded, single-volume canonical READ memory for synthetic replay only.

Feature formulas belong to ReadDemandEngine. This adapter owns admission,
source-availability evidence and candidate selection, without retaining handles.
"""

from __future__ import annotations

import math
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Literal

import pyarrow as pa
from pydantic import BaseModel, ConfigDict, field_validator, model_validator

from predictive_cache.controller.models import RuntimeContract
from predictive_cache.dispatcher.schema import ID_PATTERN, parse_utc_z
from predictive_cache.ml.read_demand import READ_DEMAND_FEATURES, ReadDemandEngine
from predictive_cache.semantics.read_demand import ReadDemandEvent


class CoverageObservation(BaseModel):
    """Coverage checkpoint whose availability and observed extent are explicit."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    volume_id: str
    hour_start: datetime
    status: Literal["complete", "incomplete", "unknown"]
    known_at: datetime
    observed_through: datetime

    @field_validator("hour_start", "known_at", "observed_through")
    @classmethod
    def utc(cls, value: datetime) -> datetime:
        return parse_utc_z(value)

    @model_validator(mode="after")
    def valid_checkpoint(self):
        if not ID_PATTERN.fullmatch(self.volume_id):
            raise ValueError("invalid coverage volume")
        if self.hour_start != self.hour_start.replace(minute=0, second=0, microsecond=0):
            raise ValueError("coverage bucket must start on a UTC hour")
        if not self.hour_start <= self.observed_through <= self.hour_start + timedelta(hours=1):
            raise ValueError("coverage extent must be inside its hour")
        if self.observed_through > self.known_at:
            raise ValueError("coverage cannot be known before its observed extent")
        return self


@dataclass(frozen=True)
class ReadCandidate:
    volume_id: str
    file_id: str
    file_size: int | None
    candidate_sources: tuple[str, ...]


@dataclass(frozen=True)
class ReadSnapshot:
    rows: pa.Table
    candidates: tuple[ReadCandidate, ...]
    anchor_file_id: str | None
    gap: bool
    complete: bool

    def is_eligible(self, volume_id: str, file_id: str) -> bool:
        return any((c.volume_id, c.file_id) == (volume_id, file_id) for c in self.candidates)


_ROW_SCHEMA = pa.schema(
    [
        ("volume_id", pa.string()),
        ("file_id", pa.string()),
        ("t", pa.timestamp("us", tz="UTC")),
        *[(name, pa.float64()) for name in READ_DEMAND_FEATURES],
    ]
)


class ReadMemory:
    """Synthetic replay admission plus canonical features, bounded without eviction.

    Call apply_* with the prefix available at a tick, then snapshot that tick.
    Never backdate newly delivered evidence across an already completed snapshot.
    Retention exhaustion latches a gap; history is not silently forgotten.
    """

    def __init__(
        self,
        volume_id: str,
        *,
        verified_file_size: bool = False,
        max_events: int = 100_000,
        max_files: int = 10_000,
        max_coverage: int = 10_000,
        max_candidates: int = 500,
        recent_cap: int = 200,
        neighbor_cap: int = 200,
        popular_cap: int = 100,
    ):
        if not isinstance(volume_id, str) or not ID_PATTERN.fullmatch(volume_id):
            raise ValueError("invalid memory volume")
        if type(verified_file_size) is not bool:
            raise ValueError("metadata capability must be explicit")
        for value in (max_events, max_files, max_coverage, max_candidates):
            if type(value) is not int or value < 1:
                raise ValueError("memory bounds must be positive integers")
        for value, cap in (
            (max_candidates, 500),
            (recent_cap, 200),
            (neighbor_cap, 200),
            (popular_cap, 100),
        ):
            if type(value) is not int or not 0 <= value <= cap:
                raise ValueError("candidate configuration may only lower READ profile caps")
        self.volume_id, self.verified_file_size = volume_id, verified_file_size
        self.max_events, self.max_files, self.max_coverage = max_events, max_files, max_coverage
        self.max_candidates = max_candidates
        self.recent_cap, self.neighbor_cap, self.popular_cap = recent_cap, neighbor_cap, popular_cap
        self.contract = RuntimeContract.read_demand(
            verified_file_size=verified_file_size, anchor_context=True
        )
        self._engine = ReadDemandEngine()
        self._reads: dict[str, ReadDemandEvent] = {}
        self._files: set[str] = set()
        self._coverage: dict[tuple[datetime, datetime], CoverageObservation] = {}
        self._counts = Counter()
        self._excluded = Counter()
        self._gap = False
        self._last_snapshot: datetime | None = None
        self._last_read_key: tuple[datetime, str] | None = None
        self._last_coverage_known: datetime | None = None
        self._watermark: datetime | None = None

    @property
    def gap(self) -> bool:
        return self._gap

    def _reject(self, reason: str, *, gap: bool = False, read: bool = True) -> bool:
        self._gap |= gap
        self._excluded[reason] += 1
        if read:
            self._counts["events_excluded"] += 1
        return False

    def apply_read(self, row: ReadDemandEvent | dict, *, available_at: datetime) -> bool:
        self._counts["events_received"] += 1
        try:
            now = parse_utc_z(available_at)
            raw = row.model_dump() if isinstance(row, ReadDemandEvent) else row
            event = ReadDemandEvent.model_validate(raw)
            if not all(ID_PATTERN.fullmatch(v) for v in (event.volume_id, event.file_id)):
                raise ValueError("invalid READ identity")
            if len(event.event_id) > 1024 or (
                event.dir_id is not None and len(event.dir_id) > 1024
            ):
                raise ValueError("READ metadata exceeds bounds")
            if any(
                value > 2**64 - 1
                for value in (event.offset, event.bytes_requested, event.bytes_read)
            ):
                raise ValueError("invalid READ range")
            if event.file_size is not None and event.file_size > 2**64 - 1:
                raise ValueError("invalid READ size")
        except (ValueError, TypeError, AttributeError):
            return self._reject("schema_invalid", gap=True)
        if event.volume_id != self.volume_id:
            return self._reject("volume")
        if event.origin == "warmer" or event.demand_class == "warmer":
            return self._reject("origin_warmer")
        if event.origin == "unknown" or event.demand_class == "unknown":
            return self._reject("origin_unknown")
        if (
            event.data_kind != "synthetic"
            or event.demand_class not in {"controlled_demand", "fixture_demand"}
            or event.origin_trust not in {"fixture", "client_asserted"}
        ):
            return self._reject("operation_unsupported")
        existing = self._reads.get(event.event_id)
        if existing is not None:
            return self._reject(
                "duplicate" if existing == event else "conflict", gap=existing != event
            )
        if event.known_at > now or event.event_time > event.known_at:
            return self._reject("time_invalid", gap=True)
        if self._last_snapshot is not None and event.known_at <= self._last_snapshot:
            return self._reject("time_invalid", gap=True)
        key = event.known_at, event.event_id
        if self._last_read_key is not None and key < self._last_read_key:
            return self._reject("time_invalid", gap=True)
        if (
            self.verified_file_size
            and event.file_size is not None
            and event.metadata_status != "fixture"
        ):
            return self._reject("schema_invalid", gap=True)
        if len(self._reads) >= self.max_events or (
            event.file_id not in self._files and len(self._files) >= self.max_files
        ):
            return self._reject("overflow", gap=True)
        self._reads[event.event_id] = event
        self._files.add(event.file_id)
        self._engine.update(event)
        self._last_read_key = key
        self._watermark = max(self._watermark or event.known_at, event.known_at)
        self._counts["events_kept"] += 1
        return True

    def apply_coverage(self, row: CoverageObservation | dict, *, available_at: datetime) -> bool:
        self._counts["coverage_received"] += 1
        try:
            now = parse_utc_z(available_at)
            raw = row.model_dump() if isinstance(row, CoverageObservation) else row
            observation = CoverageObservation.model_validate(raw)
        except (ValueError, TypeError, AttributeError):
            return self._reject("schema_invalid", gap=True, read=False)
        if observation.volume_id != self.volume_id:
            return self._reject("volume", read=False)
        key = observation.hour_start, observation.known_at
        existing = self._coverage.get(key)
        if existing is not None:
            return self._reject(
                "duplicate" if existing == observation else "conflict",
                gap=existing != observation,
                read=False,
            )
        if (
            observation.known_at > now
            or (self._last_snapshot is not None and observation.known_at <= self._last_snapshot)
            or (
                self._last_coverage_known is not None
                and observation.known_at < self._last_coverage_known
            )
        ):
            return self._reject("time_invalid", gap=True, read=False)
        if len(self._coverage) >= self.max_coverage:
            return self._reject("overflow", gap=True, read=False)
        self._coverage[key] = observation
        self._last_coverage_known = observation.known_at
        self._watermark = max(self._watermark or observation.known_at, observation.known_at)
        self._counts["coverage_applied"] += 1
        return True

    def _check_time(self, t: datetime) -> datetime:
        now = parse_utc_z(t)
        if (self._last_snapshot is not None and now < self._last_snapshot) or (
            self._watermark is not None and now < self._watermark
        ):
            raise ValueError("READ memory cannot query before its watermark")
        return now

    def latest_read(self, volume_id: str, file_id: str, t: datetime) -> ReadDemandEvent | None:
        now = self._check_time(t)
        if volume_id != self.volume_id:
            return None
        return next(
            (
                row
                for row in reversed(self._engine.events)
                if row.file_id == file_id and row.known_at <= now
            ),
            None,
        )

    def _coverage_at(self, t: datetime) -> dict[tuple[str, datetime], str]:
        latest = {}
        for observation in self._coverage.values():
            if observation.known_at <= t:
                previous = latest.get(observation.hour_start)
                if previous is None or observation.known_at > previous.known_at:
                    latest[observation.hour_start] = observation
        output = {}
        for hour, observation in latest.items():
            through = min(t, hour + timedelta(hours=1))
            output[self.volume_id, hour] = (
                "complete"
                if observation.status == "complete" and observation.observed_through >= through
                else observation.status
                if observation.status != "complete"
                else "unknown"
            )
        return output

    def _complete(self, t: datetime) -> bool:
        hour = (t - timedelta(days=7)).replace(minute=0, second=0, microsecond=0)
        while hour < t:
            if self._engine.coverage.get((self.volume_id, hour)) != "complete":
                return False
            hour += timedelta(hours=1)
        return True

    def snapshot(self, t: datetime, coaccess: pa.Table | None = None) -> ReadSnapshot:
        now = self._check_time(t)
        self._engine.coverage = self._coverage_at(now)
        visible = [row for row in self._engine.events if row.known_at <= now]
        timed = [row for row in visible if row.event_time <= now]
        metadata = {row.file_id: row for row in visible}
        recent_times = {}
        counts = Counter()
        for row in timed:
            age = (now - row.event_time).total_seconds()
            if age <= 3600:
                recent_times[row.file_id] = max(
                    recent_times.get(row.file_id, row.event_time), row.event_time
                )
            if age <= 86400:
                counts[row.file_id] += 1
        recent = sorted(recent_times, key=lambda file: (-recent_times[file].timestamp(), file))[
            : self.recent_cap
        ]
        latest = max(timed, key=lambda row: (row.event_time, row.event_id), default=None)
        anchor = (
            latest.file_id
            if latest is not None and (now - latest.event_time).total_seconds() <= 300
            else None
        )
        neighbors = []
        if anchor is not None and coaccess is not None and coaccess.num_rows:
            if not {"from_file_id", "to_file_id", "conf"} <= set(coaccess.column_names):
                raise ValueError("invalid co-read table")
            edges = []
            for edge in coaccess.select(["from_file_id", "to_file_id", "conf"]).to_pylist():
                if edge["from_file_id"] != anchor:
                    continue
                destination, confidence = edge["to_file_id"], edge["conf"]
                if not isinstance(destination, str) or not ID_PATTERN.fullmatch(destination):
                    raise ValueError("invalid co-read identity")
                if (
                    not isinstance(confidence, (float, int))
                    or isinstance(confidence, bool)
                    or not math.isfinite(confidence)
                ):
                    raise ValueError("invalid co-read score")
                if destination != anchor:
                    edges.append((destination, confidence))
            neighbors = [
                file
                for file, _ in sorted(edges, key=lambda edge: (-edge[1], edge[0]))[
                    : self.neighbor_cap
                ]
            ]
        popular = sorted(counts, key=lambda file: (-counts[file], file))[: self.popular_cap]
        sources = {}
        for tag, files in (("recent", recent), ("neighbor", neighbors), ("popular", popular)):
            for file in files:
                tags = sources.setdefault(file, [])
                if tag not in tags:
                    tags.append(tag)
        candidates = tuple(
            ReadCandidate(self.volume_id, file, metadata[file].file_size, tuple(tags))
            for file, tags in sources.items()
            if file in metadata and metadata[file].file_size != 0
        )[: self.max_candidates]
        rows = []
        for candidate in candidates:
            features = self._engine.features(now, self.volume_id, candidate.file_id, anchor)
            rows.append(
                {"volume_id": self.volume_id, "file_id": candidate.file_id, "t": now, **features}
            )
        self._last_snapshot = now
        return ReadSnapshot(
            pa.Table.from_pylist(rows, schema=_ROW_SCHEMA),
            candidates,
            anchor,
            self._gap,
            self._complete(now),
        )

    def stats(self) -> dict:
        return {
            **{
                name: self._counts[name]
                for name in (
                    "events_received",
                    "events_kept",
                    "events_excluded",
                    "coverage_received",
                    "coverage_applied",
                )
            },
            "events": len(self._reads),
            "files": len(self._files),
            "coverage_observations": len(self._coverage),
            "gap": self._gap,
            "excluded_by_reason": dict(self._excluded),
        }
