"""READ-only selection and private file publication, independent of the tick loop."""

from __future__ import annotations

import json
import math
import os
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from threading import RLock
from typing import Protocol

from predictive_cache.dispatcher import securefs
from predictive_cache.dispatcher.schema import (
    ID_PATTERN,
    MAX_TTL_SECONDS,
    MODEL_VERSION_PATTERN,
    InvalidPlanItem,
    Plan,
    PlanItem,
    format_utc_z,
    parse_utc_z,
)
from predictive_cache.ml.package import validate_serving_policy

Identity = tuple[str, str]


class ResolvedTarget(Protocol):
    volume_id: str
    file_id: str
    export_id: int
    export_name: str
    target_fh_b64: str
    file_size: int | None
    seen_at: datetime


class TargetResolver(Protocol):
    target_max_age_s: int

    def resolve(self, volume_id: str, file_id: str, t: datetime) -> ResolvedTarget | None: ...


@dataclass(frozen=True)
class ScoredCandidate:
    """Already scored, past-only READ metadata, never an access event or target."""

    volume_id: str
    file_id: str
    score: float
    secs_since_last_read: float
    file_size: int | None
    candidate_sources: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        for value in (self.volume_id, self.file_id):
            if not isinstance(value, str) or not ID_PATTERN.fullmatch(value):
                raise ValueError("invalid candidate identity")
        for value in (self.score, self.secs_since_last_read):
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(value)
            ):
                raise ValueError("candidate score and READ recency must be finite")
        if self.secs_since_last_read < 0:
            raise ValueError("READ recency must be non-negative")
        if self.file_size is not None and (
            type(self.file_size) is not int or not 0 <= self.file_size <= 2**64 - 1
        ):
            raise ValueError("invalid candidate size")
        # Fixed vocabulary prevents arbitrary metadata/handles leaking into predictions.
        if (
            not isinstance(self.candidate_sources, tuple)
            or len(self.candidate_sources) > 3
            or any(
                source not in {"recent", "neighbor", "popular"} for source in self.candidate_sources
            )
        ):
            raise ValueError("unknown candidate source")

    @property
    def identity(self) -> Identity:
        return self.volume_id, self.file_id


@dataclass(frozen=True)
class PlannerConfig:
    max_items: int | None = None
    max_bytes: int | None = None
    target_valid_s: int = 120
    max_candidates: int = 100_000
    log_top_n: int = 100

    def __post_init__(self) -> None:
        for value in (self.max_items, self.max_bytes):
            if value is not None and (type(value) is not int or value < 0):
                raise ValueError("operational caps must be non-negative integers")
        for value in (self.target_valid_s, self.max_candidates):
            if type(value) is not int or value < 1:
                raise ValueError("planner bounds must be positive integers")
        if type(self.log_top_n) is not int or not 0 <= self.log_top_n <= self.max_candidates:
            raise ValueError("invalid prediction log bound")


@dataclass(frozen=True, repr=False)
class Selection:
    """Private plan and exact resolver versions; do not log or serialize the object."""

    plan: dict | None = field(repr=False)
    predictions: tuple[dict, ...]
    bindings: tuple[ResolvedTarget, ...] = field(repr=False)
    planned: tuple[Identity, ...]
    prediction_time: datetime
    cooldown_s: int

    def __repr__(self) -> str:
        return f"Selection(planned_count={len(self.planned)}, predictions={len(self.predictions)})"


def select_plan(
    candidates: Sequence[ScoredCandidate],
    *,
    serving_policy: dict,
    is_eligible: Callable[[str, str], bool],
    targets: TargetResolver,
    cooldown: Mapping[Identity, datetime],
    t: datetime,
    run_id: str,
    plan_id: str,
    model_version: str,
    plans_allowed: bool,
    config: PlannerConfig = PlannerConfig(),
    cooldown_capacity: int = 100_000,
) -> Selection:
    """Select without mutating state. ``plans_allowed`` is the readiness gate.

    Call only with the active package's policy and a past-only, coverage-checked
    candidate view. This function does not establish source trust or coverage.
    """
    t = parse_utc_z(t)
    validate_serving_policy(serving_policy)
    if serving_policy["target"] != "demand":
        raise ValueError("live READ planner requires demand policy")
    for value, pattern in (
        (run_id, ID_PATTERN),
        (plan_id, ID_PATTERN),
        (model_version, MODEL_VERSION_PATTERN),
    ):
        if not isinstance(value, str) or not pattern.fullmatch(value):
            raise ValueError("invalid plan identity")
    # Schema identifiers also become file/directory names at the dispatcher seam.
    securefs.validate_name(run_id)
    securefs.validate_committed_json_name(f"{plan_id}.json")
    if type(plans_allowed) is not bool:
        raise ValueError("plans_allowed must be explicit boolean")
    if (
        type(cooldown_capacity) is not int
        or cooldown_capacity < 1
        or len(cooldown) > cooldown_capacity
    ):
        raise ValueError("invalid cooldown capacity")
    if len(candidates) > config.max_candidates:
        raise ValueError("candidate bound exceeded")
    if any(not isinstance(candidate, ScoredCandidate) for candidate in candidates):
        raise ValueError("expected validated scored candidates")
    ordered = sorted(candidates, key=lambda c: (-c.score, c.file_id, c.volume_id))
    if len({c.identity for c in ordered}) != len(ordered):
        raise ValueError("duplicate candidate identity")
    for planned_at in cooldown.values():
        if parse_utc_z(planned_at) > t:
            raise ValueError("cooldown is from the future")
    max_age = targets.target_max_age_s
    if type(max_age) is not int or max_age < 1:
        raise ValueError("resolver must declare a positive target age bound")
    item_cap = min(
        serving_policy["max_items"],
        config.max_items if config.max_items is not None else serving_policy["max_items"],
    )
    byte_cap = min(
        serving_policy["max_bytes"],
        config.max_bytes if config.max_bytes is not None else serving_policy["max_bytes"],
    )
    items: list[dict] = []
    bindings: list[ResolvedTarget] = []
    planned: list[Identity] = []
    records: list[dict] = []
    used = 0
    new_cooldowns = 0
    for index, candidate in enumerate(ordered):
        reason = None
        size = candidate.file_size
        target = None
        expiry = None
        if not is_eligible(*candidate.identity):
            reason = "ineligible"
        elif candidate.secs_since_last_read < serving_policy["recent_skip_s"]:
            reason = "recently_read"
        elif (
            candidate.identity in cooldown
            and (t - cooldown[candidate.identity]).total_seconds() < serving_policy["cooldown_s"]
        ):
            reason = "cooldown"
        elif (
            serving_policy["min_score"] is not None
            and candidate.score < serving_policy["min_score"]
        ):
            reason = "below_threshold"
        elif size is None or size == 0:
            reason = "unknown_size" if size is None else "empty_file"
        else:
            target = targets.resolve(*candidate.identity, t)
            if target is not None:
                try:
                    seen = parse_utc_z(target.seen_at)
                    expiry = min(
                        t + timedelta(seconds=min(config.target_valid_s, MAX_TTL_SECONDS)),
                        seen + timedelta(seconds=max_age),
                    )
                    if (
                        seen > t
                        or expiry <= t
                        or (target.volume_id, target.file_id) != candidate.identity
                    ):
                        target = None
                    elif target.file_size is not None:
                        if (
                            type(target.file_size) is not int
                            or not 0 <= target.file_size <= 2**64 - 1
                        ):
                            target = None
                        else:
                            size = min(size, target.file_size)
                except (ValueError, OverflowError, AttributeError):
                    target = None
            if target is None:
                reason = "no_target"
            elif size == 0:
                reason = "empty_file"
            else:
                length = min(size, serving_policy["range_cap"])
                if len(items) >= item_cap or used + length > byte_cap:
                    reason = "budget"
                elif (
                    candidate.identity not in cooldown
                    and len(cooldown) + new_cooldowns >= cooldown_capacity
                ):
                    reason = "cooldown_capacity"
                elif not plans_allowed:
                    reason = "plans_suppressed"
                else:
                    item = {
                        "task_id": f"{plan_id}-{len(items) + 1:06d}",
                        "volume_id": candidate.volume_id,
                        "file_id": candidate.file_id,
                        "export_name": target.export_name,
                        "export_id": str(target.export_id),
                        "target_fh_b64": target.target_fh_b64,
                        "target_valid_until": format_utc_z(expiry),
                        "offset": 0,
                        "length": length,
                        "max_bytes": length,
                        "score": float(candidate.score),
                        "reasons": [],
                    }
                    try:
                        if type(target.export_id) is not int or not 0 <= target.export_id <= 65535:
                            raise ValueError("invalid export")
                        PlanItem.model_validate(item)
                    except (ValueError, TypeError):
                        reason = "no_target"
                    else:
                        items.append(item)
                        bindings.append(target)
                        planned.append(candidate.identity)
                        used += length
                        new_cooldowns += int(candidate.identity not in cooldown)
        if index < config.log_top_n or reason is None:
            records.append(
                {
                    "schema_version": 1,
                    "model_version": model_version,
                    "prediction_time": format_utc_z(t),
                    "volume_id": candidate.volume_id,
                    "file_id": candidate.file_id,
                    "horizon_seconds": serving_policy["horizon_s"],
                    "score": float(candidate.score),
                    "calibrated": False,
                    "candidate_sources": list(candidate.candidate_sources),
                    "reasons": [],
                    "planned": reason is None,
                    "skip_reason": reason,
                }
            )
    plan = None
    if items:
        deadline = min(
            t + timedelta(seconds=serving_policy["horizon_s"]),
            *(parse_utc_z(item["target_valid_until"]) for item in items),
        )
        plan = {
            "schema_version": 2,
            "run_id": run_id,
            "plan_id": plan_id,
            "model_version": model_version,
            "created_at": format_utc_z(t),
            "deadline": format_utc_z(deadline),
            "max_bytes": used,
            "items": items,
        }
        if (
            len(json.dumps(plan, allow_nan=False, separators=(",", ":")).encode()) + 1
            > securefs.DEFAULT_MAX_JSON_BYTES
        ):
            raise ValueError("plan exceeds dispatcher file size limit")
        parsed = Plan.model_validate(plan)
        if any(isinstance(item, InvalidPlanItem) for item in parsed.items):
            raise ValueError("planner produced an invalid dispatcher item")
    return Selection(
        plan, tuple(records), tuple(bindings), tuple(planned), t, serving_policy["cooldown_s"]
    )


class PublicationError(RuntimeError):
    """Publication stopped; private artifacts require manual reconciliation."""


class PlannerPublisher:
    """Single-process owner of cooldown/sequence and secure output publication.

    In-memory state is deliberately not restart recovery. Use a fresh run_id
    per process and do not resume live publication until durable state exists.
    """

    def __init__(
        self, root: Path, *, run_id: str, controller_id: str, max_cooldowns: int = 100_000
    ):
        for value in (run_id, controller_id):
            if not isinstance(value, str) or not ID_PATTERN.fullmatch(value) or len(value) > 80:
                raise ValueError("invalid publisher identity")
        securefs.validate_name(run_id)
        securefs.validate_committed_json_name(f"{controller_id}-000000000001.json")
        if type(max_cooldowns) is not int or max_cooldowns < 1:
            raise ValueError("invalid cooldown bound")
        self.root = root
        self.run_id = run_id
        self.controller_id = controller_id
        self.max_cooldowns = max_cooldowns
        self._cooldown: dict[Identity, datetime] = {}
        self._sequence = 0
        self._pending: Selection | None = None
        self._failed = False
        self._lock = RLock()

    @property
    def sequence(self) -> int:
        return self._sequence

    @property
    def failed(self) -> bool:
        return self._failed

    @property
    def cooldown(self) -> dict[Identity, datetime]:
        return dict(self._cooldown)

    @property
    def pending(self) -> Selection | None:
        """Private exact-version reconciliation slot; never include in diagnostics."""
        return self._pending

    def publish(
        self,
        candidates: Sequence[ScoredCandidate],
        *,
        t: datetime,
        serving_policy: dict,
        is_eligible: Callable[[str, str], bool],
        targets: TargetResolver,
        model_version: str,
        plans_allowed: bool,
        config: PlannerConfig = PlannerConfig(),
    ) -> Selection:
        with self._lock:
            if self._failed:
                raise PublicationError("publisher stopped; manual reconciliation required")
            plan_id = f"{self.controller_id}-{self._sequence + 1:012d}"
            selection = select_plan(
                candidates,
                serving_policy=serving_policy,
                is_eligible=is_eligible,
                targets=targets,
                cooldown=self._cooldown,
                t=t,
                run_id=self.run_id,
                plan_id=plan_id,
                model_version=model_version,
                plans_allowed=plans_allowed,
                config=config,
                cooldown_capacity=self.max_cooldowns,
            )
            # Reserve before either artifact: fsync can fail after its rename.
            self._pending = selection
            root_fd = predictions_fd = plans_fd = None
            try:
                root_fd = securefs.require_secure_directory(self.root)
                predictions_fd = securefs.ensure_child_directory(root_fd, "predictions")
                plans_fd = securefs.ensure_child_directory(root_fd, "plans")
                data = b"".join(
                    json.dumps(row, allow_nan=False, separators=(",", ":")).encode() + b"\n"
                    for row in selection.predictions
                )
                if not securefs.atomic_write_bytes(
                    predictions_fd, f"{plan_id}.jsonl", data, no_overwrite=True
                ):
                    raise PublicationError("prediction artifact collision")
                if selection.plan is not None and not securefs.atomic_write_json(
                    plans_fd, f"{plan_id}.json", selection.plan, no_overwrite=True
                ):
                    raise PublicationError("plan artifact collision")
            except BaseException:
                self._failed = True
                raise PublicationError(
                    "publication incomplete; manual reconciliation required"
                ) from None
            finally:
                close_failed = False
                for fd in (plans_fd, predictions_fd, root_fd):
                    if fd is not None:
                        try:
                            os.close(fd)
                        except OSError:
                            close_failed = True
                if close_failed:
                    self._failed = True
                    raise PublicationError(
                        "publication close failed; manual reconciliation required"
                    ) from None
            for identity in selection.planned:
                self._cooldown[identity] = selection.prediction_time
            self._sequence += 1
            self._pending = None
            return selection
