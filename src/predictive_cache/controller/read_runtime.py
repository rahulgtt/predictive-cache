"""Bounded fixture READ ticks through the merged planner and dry-run Dispatcher."""

from __future__ import annotations

import base64
import hashlib
import math
import os
import stat
import time
from dataclasses import dataclass, field
from datetime import datetime

import numpy as np

from predictive_cache.controller.feedback import UsefulByteFeedback
from predictive_cache.controller.health import EXCLUSIONS, PLAN_GATES, REASONS, ControllerHealth
from predictive_cache.controller.planner import PlannerConfig, ScoredCandidate
from predictive_cache.controller.read_memory import CoverageObservation
from predictive_cache.dispatcher import securefs
from predictive_cache.dispatcher.schema import (
    InvalidPlanItem,
    Plan,
    ResultRecord,
    format_utc_z,
    parse_utc_z,
    task_from_plan,
)
from predictive_cache.ml.package import DEFAULT_SERVING_POLICY
from predictive_cache.semantics.read_demand import ReadDemandEvent


class ReadRuntimeError(RuntimeError):
    """A tick failed; do not restart this in-memory run automatically."""


@dataclass(frozen=True)
class _SyntheticTarget:
    volume_id: str
    file_id: str
    export_id: int
    export_name: str
    target_fh_b64: str = field(repr=False)
    file_size: int | None
    seen_at: datetime


class SyntheticTargetResolver:
    """Invented handles for fixture-only dry runs, never live execution bindings."""

    def __init__(self, memory, *, target_max_age_s: int = 3600):
        if type(target_max_age_s) is not int or target_max_age_s < 1:
            raise ValueError("invalid synthetic target age")
        self.memory = memory
        self.target_max_age_s = target_max_age_s

    def resolve(self, volume_id: str, file_id: str, t: datetime):
        t = parse_utc_z(t)
        row = self.memory.latest_read(volume_id, file_id, t)
        if row is None or row.data_kind != "synthetic":
            return None
        if not 0 <= (t - row.known_at).total_seconds() < self.target_max_age_s:
            return None
        digest = hashlib.sha256(
            b"predictive-cache-fixture-only\0" + volume_id.encode() + b"\0" + file_id.encode()
        ).digest()
        handle = base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")
        return _SyntheticTarget(
            volume_id, file_id, 1, "fixture-export", handle, row.file_size, row.known_at
        )


@dataclass(frozen=True)
class TickResult:
    t: datetime
    tick_s: int
    duration_s: float
    overrun: bool
    candidates: int
    planned_items: int
    planned_bytes: int
    results: int
    plans_allowed: bool
    scored: bool
    model_version: str | None

    def as_dict(self) -> dict:
        return {**self.__dict__, "t": format_utc_z(self.t)}


def _result_directory(root, run_id: str, plan_id: str) -> int:
    """Open existing owner-only directories by FD without creating evidence."""
    fd = securefs.require_secure_directory(root)
    try:
        for name in ("runs", run_id, "plans", plan_id, "results"):
            securefs.validate_name(name)
            child = os.open(name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
            info = os.fstat(child)
            if (
                not stat.S_ISDIR(info.st_mode)
                or stat.S_IMODE(info.st_mode) != 0o700
                or info.st_uid != os.getuid()
            ):
                os.close(child)
                raise securefs.SecureFileError("invalid result directory")
            os.close(fd)
            fd = child
        return fd
    except BaseException:
        os.close(fd)
        raise


class ReadReplayRuntime:
    """One in-memory replay run; caller owns the model manager and Dispatcher.

    Input batches contain only observations available by this tick. The caller
    advances the shared replay clock before ``tick`` and skips one scheduled
    slot after an overrun. This class never sleeps or enables real reads.
    """

    def __init__(
        self,
        memory,
        models,
        publisher,
        dispatcher,
        *,
        targets=None,
        health=None,
        feedback=None,
        monotonic=time.monotonic,
        planner_config: PlannerConfig = PlannerConfig(),
        max_ticks: int = 1000,
        max_duration_s: int = 86400,
    ):
        if dispatcher.config.real_reads is not False:
            raise ValueError("READ replay requires dry-run Dispatcher")
        if dispatcher.config.root != publisher.root:
            raise ValueError("READ replay requires one shared output root")
        if type(max_ticks) is not int or not 1 <= max_ticks <= 100_000:
            raise ValueError("invalid tick bound")
        if type(max_duration_s) is not int or not 1 <= max_duration_s <= 86400:
            raise ValueError("invalid replay duration")
        self.memory, self.models = memory, models
        self.publisher, self.dispatcher = publisher, dispatcher
        self.targets = targets if targets is not None else SyntheticTargetResolver(memory)
        self.health = health if health is not None else ControllerHealth()
        self.feedback = (
            feedback if feedback is not None else UsefulByteFeedback(experiment_id=publisher.run_id)
        )
        self.monotonic, self.planner_config = monotonic, planner_config
        self.max_ticks, self.max_duration_s = max_ticks, max_duration_s
        self._durations: list[float] = []
        self._first_t = self._last_t = None
        self._failed = False
        self._plans = self._items = self._bytes = self._results = 0
        self._score_unavailable = self._prediction_omissions = 0

    @property
    def failed(self) -> bool:
        return self._failed

    def _memory_metrics(self, before: dict) -> None:
        after = self.memory.stats()
        for key in ("events_received", "events_kept"):
            self.health.increment(key, after[key] - before[key])
        self.health.increment("updates_applied", after["events_kept"] - before["events_kept"])
        old = before["excluded_by_reason"]
        for reason, count in after["excluded_by_reason"].items():
            delta = count - old.get(reason, 0)
            if reason == "overflow":
                self.health.increment("dropped_overflow", delta)
            if reason in EXCLUSIONS:
                self.health.exclude(reason, delta)
            elif delta:
                self.health.increment("events_excluded", delta)
        self.health.gauge("dedupe_size", after["events"])

    def _join_results(self, raw_plan: dict, t: datetime) -> int:
        plan = Plan.model_validate(raw_plan)
        if any(isinstance(item, InvalidPlanItem) for item in plan.items):
            raise ValueError("invalid published plan")
        fd = _result_directory(self.publisher.root, plan.run_id, plan.plan_id)
        try:
            for item in plan.items:
                raw = securefs.load_secure_json(fd, item.task_id + ".json", max_bytes=256 * 1024)
                result = ResultRecord.model_validate(raw)
                if (
                    result.status not in {"skipped", "cancelled"}
                    or result.bytes_read != 0
                    or result.bytes_read_is_lower_bound
                    or result.warmer_terminal is not None
                ):
                    raise ValueError("unexpected execution evidence in dry run")
                task = task_from_plan(plan, item)
                if not self.feedback.observe_result(task, result, known_at=t):
                    raise ValueError("feedback result accounting unavailable")
        finally:
            os.close(fd)
        return len(plan.items)

    def tick(self, t: datetime, *, events=(), coverage=(), source_gates=()) -> TickResult:
        if self._failed:
            raise ReadRuntimeError("READ replay stopped; manual reconciliation required")
        started = self.monotonic()
        try:
            if (
                not isinstance(source_gates, (tuple, list, set, frozenset))
                or len(source_gates) > len(PLAN_GATES)
                or any(not isinstance(reason, str) for reason in source_gates)
                or not set(source_gates) <= PLAN_GATES
            ):
                raise ValueError("invalid source readiness gates")
            t = parse_utc_z(t)
            if self._last_t is not None and t <= self._last_t:
                raise ValueError("READ ticks must increase")
            if len(self._durations) >= self.max_ticks:
                raise ValueError("READ replay tick bound exceeded")
            first = self._first_t if self._first_t is not None else t
            if (t - first).total_seconds() > self.max_duration_s:
                raise ValueError("READ replay duration bound exceeded")
            if not isinstance(events, (tuple, list)) or len(events) > 100_000:
                raise ValueError("invalid READ tick batch")
            if not isinstance(coverage, (tuple, list)) or len(coverage) > 10_000:
                raise ValueError("invalid coverage tick batch")
            if self.dispatcher.config.real_reads is not False:
                raise ValueError("READ replay requires dry-run Dispatcher")
            self._first_t, self._last_t = first, t
            before = self.memory.stats()
            rows = [
                row if isinstance(row, ReadDemandEvent) else ReadDemandEvent.model_validate(row)
                for row in events
            ]
            for row in sorted(rows, key=lambda row: (row.known_at, row.event_id)):
                if self.memory.apply_read(row, available_at=t):
                    self.feedback.observe_read(row)
            # READ counters exclude coverage observations. Coverage loss still
            # gates plans and remains visible in the memory diagnostics.
            self._memory_metrics(before)
            observations = [
                row
                if isinstance(row, CoverageObservation)
                else CoverageObservation.model_validate(row)
                for row in coverage
            ]
            for row in sorted(observations, key=lambda row: (row.known_at, row.hour_start)):
                self.memory.apply_coverage(row, available_at=t)

            # Polling is synchronous in this finite replay; one bundle is held
            # until this tick has published and joined its results.
            self.models.poll()
            self.models.activate()
            bundle = self.models.active()
            _, model_reasons = self.models.readiness()
            reasons = set(model_reasons) & (REASONS - {"not_started", "no_scorer"})
            reasons.update(source_gates)
            reasons.update(
                set(self.health.snapshot()["reasons"])
                & {"restore_failed", "ingest_loss", "controller_error"}
            )
            contract_ok = bundle is not None and bundle.contract == self.memory.contract
            if bundle is not None and not contract_ok:
                reasons.add("contract_mismatch")
            view = self.memory.snapshot(t, coaccess=bundle.coaccess if contract_ok else None)
            if view.gap or not view.complete:
                reasons.add("memory_gap")
            self.health.gauge("candidates", len(view.candidates))
            self.health.set_state(started=True, has_scorer=bundle is not None, reasons=reasons)
            tick_s = (
                int(bundle.serving_policy["tick_s"])
                if contract_ok
                else DEFAULT_SERVING_POLICY["tick_s"]
            )
            candidates = []
            scored = False
            if contract_ok and view.candidates:
                required = bundle.scorer.feature_columns
                missing = any(
                    value is None
                    for name in required
                    for value in view.rows.column(name).to_pylist()
                )
                try:
                    raw_scores = np.asarray(bundle.score(view.rows))
                except (ValueError, TypeError):
                    if not missing:
                        raise
                    reasons.add("memory_gap")
                    self._score_unavailable += 1
                else:
                    if (
                        raw_scores.shape != (len(view.candidates),)
                        or raw_scores.dtype.kind not in "iuf"
                        or not np.isfinite(raw_scores).all()
                    ):
                        raise ValueError("invalid READ scores")
                    scored = True
                    recencies = view.rows.column("secs_since_last_read").to_pylist()
                    for candidate, score, recency in zip(
                        view.candidates, raw_scores, recencies, strict=True
                    ):
                        if recency is None:
                            self._prediction_omissions += 1
                            reasons.add("memory_gap")
                            continue
                        candidates.append(
                            ScoredCandidate(
                                candidate.volume_id,
                                candidate.file_id,
                                float(score),
                                recency,
                                candidate.file_size,
                                candidate.candidate_sources,
                            )
                        )
            self.health.set_state(started=True, has_scorer=bundle is not None, reasons=reasons)
            plans_allowed = bool(
                contract_ok
                and self.models.plans_allowed()
                and self.health.snapshot()["plans_allowed"]
            )
            selection = None
            if contract_ok:
                selection = self.publisher.publish(
                    candidates,
                    t=t,
                    serving_policy=dict(bundle.serving_policy),
                    is_eligible=view.is_eligible,
                    targets=self.targets,
                    model_version=bundle.model_version,
                    plans_allowed=plans_allowed,
                    config=self.planner_config,
                )
            if not plans_allowed:
                self.health.increment("plans_suppressed")
            items = len(selection.planned) if selection is not None else 0
            planned_bytes = (
                selection.plan["max_bytes"] if selection is not None and selection.plan else 0
            )
            self.health.increment("planned_items", items)
            self.health.increment("planned_bytes", planned_bytes)
            results = 0
            if selection is not None and selection.plan is not None:
                self.dispatcher.run_once()
                results = self._join_results(selection.plan, t)
                self._plans += 1
            self._items += items
            self._bytes += planned_bytes
            self._results += results
            duration = self.monotonic() - started
            if not math.isfinite(duration) or duration < 0:
                raise ValueError("invalid monotonic duration")
            self._durations.append(duration)
            self.health.observe_tick(duration)
            overrun = duration > 0.8 * tick_s
            if overrun:
                self.health.increment("tick_overruns")
            return TickResult(
                t,
                tick_s,
                duration,
                overrun,
                len(view.candidates),
                items,
                planned_bytes,
                results,
                plans_allowed,
                scored,
                bundle.model_version if bundle is not None else None,
            )
        except Exception:
            self._failed = True
            snapshot = self.health.snapshot()
            reasons = (set(snapshot["reasons"]) - {"not_started", "no_scorer"}) | {
                "controller_error"
            }
            self.health.set_state(
                started=True, has_scorer=snapshot["status"] != "not_ready", reasons=reasons
            )
            self.health.increment("consumer_errors")
            raise ReadRuntimeError(
                "READ replay tick failed; manual reconciliation required"
            ) from None

    def summary(self, as_of: datetime) -> dict:
        as_of = parse_utc_z(as_of)
        if self._last_t is not None and as_of < self._last_t:
            raise ValueError("summary precedes last tick")
        return {
            "schema_version": 1,
            "data_kind": "synthetic",
            "evidence_mode": "fixture",
            "failed": self._failed,
            "ticks": len(self._durations),
            "plans": self._plans,
            "planned_items": self._items,
            "planned_bytes": self._bytes,
            "results": self._results,
            "score_unavailable_ticks": self._score_unavailable,
            "prediction_omissions": self._prediction_omissions,
            "timing": {
                "samples": len(self._durations),
                "p50_s": float(np.quantile(self._durations, 0.5)) if self._durations else None,
                "p95_s": float(np.quantile(self._durations, 0.95)) if self._durations else None,
            },
            "health": self.health.snapshot(),
            "memory": self.memory.stats(),
            "feedback": self.feedback.report(as_of=as_of),
            "evidence": "Synthetic plumbing only; no cache residency, latency or model-quality claim.",
        }
