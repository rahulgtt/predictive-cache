"""Batch Pass-2 publication of frozen logical predictions as schema-v2 plans."""

from __future__ import annotations

import hashlib
import math
import os
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from predictive_cache.dispatcher.schema import (
    ID_PATTERN,
    InvalidPlanItem,
    Plan,
    PlanItem,
    format_utc_z,
    parse_utc_z,
    warmer_ref,
)
from predictive_cache.dispatcher.securefs import (
    atomic_write_bytes,
    atomic_write_json,
    ensure_child_directory,
    exclusive_lock,
    load_secure_json,
    require_secure_directory,
    secure_control_exists,
)
from predictive_cache.ml.hashing import canonical_json

from .pass2_schedule import ScheduleItem, load_schedule
from .pass2_targets import ApprovedTarget, load_target_map

RATE_WINDOW_SECONDS = 60
MAX_PLAN_TTL_SECONDS = 300


class Pass2RuntimeError(ValueError):
    """The Pass-2 bridge cannot safely continue."""


@dataclass(frozen=True)
class Pass2RuntimeConfig:
    schedule_path: Path
    target_map_path: Path
    dispatcher_root: Path
    run_id: str
    run_start: datetime
    publish_lead_seconds: int = 5
    plan_ttl_seconds: int = 120
    max_bytes_per_min: int = 64 * 2**20
    max_items_per_min: int = 60
    poll_seconds: float = 0.1
    clock_tolerance_seconds: float = 1.0
    stop_file: str = "stop"

    def __post_init__(self) -> None:
        if not self.dispatcher_root.is_absolute():
            raise ValueError("dispatcher root must be an absolute path")
        if not ID_PATTERN.fullmatch(self.run_id):
            raise ValueError("run_id is invalid")
        if (
            self.run_start.tzinfo is None
            or self.run_start.utcoffset() != UTC.utcoffset(self.run_start)
        ):
            raise ValueError("run_start must be timezone-aware UTC")
        integer_controls = {
            "plan_ttl_seconds": self.plan_ttl_seconds,
            "max_bytes_per_min": self.max_bytes_per_min,
            "max_items_per_min": self.max_items_per_min,
        }
        for field, value in integer_controls.items():
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise ValueError(f"{field} must be a positive integer")
        if (
            isinstance(self.publish_lead_seconds, bool)
            or not isinstance(self.publish_lead_seconds, int)
            or self.publish_lead_seconds < 0
        ):
            raise ValueError("publish_lead_seconds must be a non-negative integer")
        if self.plan_ttl_seconds > MAX_PLAN_TTL_SECONDS:
            raise ValueError("plan_ttl_seconds cannot exceed 300")
        if (
            isinstance(self.poll_seconds, bool)
            or not isinstance(self.poll_seconds, (int, float))
            or not math.isfinite(self.poll_seconds)
            or self.poll_seconds <= 0
        ):
            raise ValueError("poll_seconds must be finite and positive")
        if (
            isinstance(self.clock_tolerance_seconds, bool)
            or not isinstance(self.clock_tolerance_seconds, (int, float))
            or not math.isfinite(self.clock_tolerance_seconds)
            or self.clock_tolerance_seconds < 0
        ):
            raise ValueError("clock_tolerance_seconds must be finite and non-negative")
        if not self.stop_file or "/" in self.stop_file or self.stop_file in {".", ".."}:
            raise ValueError("stop_file must be a simple root-relative name")


@dataclass
class Pass2Stats:
    published_plans: int = 0
    published_items: int = 0
    skipped_items: int = 0
    pending_items: int = 0
    status: str = "running"
    reason: str | None = None


def _encoded(value: object) -> bytes:
    return canonical_json(value).encode("utf-8") + b"\n"


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _stable_id(prefix: str, value: object) -> str:
    return f"{prefix}-{_sha256(canonical_json(value).encode('utf-8'))[:24]}"


def _utc(value: datetime, field: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() != UTC.utcoffset(value):
        raise Pass2RuntimeError(f"{field} must be timezone-aware UTC")
    return value.astimezone(UTC)


class Pass2Scheduler:
    """Publish due schedule groups once, with durable fail-closed recovery."""

    def __init__(
        self,
        config: Pass2RuntimeConfig,
        *,
        now: Callable[[], datetime] | None = None,
        monotonic: Callable[[], float] | None = None,
        sleep: Callable[[float], None] | None = None,
    ) -> None:
        self.config = config
        self._now = now or (lambda: datetime.now(UTC))
        self._monotonic = monotonic or time.monotonic
        self._sleep = sleep or time.sleep
        self._closed = False
        self._previous_wall: datetime | None = None
        self._previous_monotonic: float | None = None

        self.schedule, self.schedule_sha256 = load_schedule(config.schedule_path)
        self._validate_schedule_rates()
        self.root_fd = require_secure_directory(config.dispatcher_root)
        try:
            self.lock_fd = exclusive_lock(self.root_fd, "pass2-runtime.lock")
            self._open_runtime()
        except Exception:
            os.close(self.root_fd)
            raise

    def _validate_schedule_rates(self) -> None:
        ordered = sorted(self.schedule.items, key=lambda item: item.warm_offset_s)
        for item in ordered:
            window = [
                candidate
                for candidate in ordered
                if item.warm_offset_s - RATE_WINDOW_SECONDS
                < candidate.warm_offset_s
                <= item.warm_offset_s
            ]
            if (
                len(window) > self.config.max_items_per_min
                or sum(candidate.logical_range.length for candidate in window)
                > self.config.max_bytes_per_min
            ):
                raise Pass2RuntimeError(
                    "schedule exceeds configured Pass-2 per-minute limits"
                )

    def _open_runtime(self) -> None:
        self.pass2_fd = ensure_child_directory(self.root_fd, "pass2-runs")
        existing_runs = set(os.listdir(self.pass2_fd))
        if self.config.run_id not in existing_runs and existing_runs:
            raise Pass2RuntimeError("dispatcher root is already bound to another Pass-2 run")
        self.run_fd = ensure_child_directory(self.pass2_fd, self.config.run_id)
        self.audit_fd = ensure_child_directory(self.run_fd, "audit")
        self.plans_fd = ensure_child_directory(self.root_fd, "plans")
        self.provenance_fd = ensure_child_directory(self.root_fd, "provenance")
        self.indexes_fd = ensure_child_directory(self.root_fd, "indexes")
        try:
            self.state = self._load_state()
        except FileNotFoundError:
            self._assert_fresh_dedicated_root()
            self.state = self._initial_state()
            if not atomic_write_json(
                self.run_fd,
                "state.json",
                self.state,
                no_overwrite=True,
            ):
                raise Pass2RuntimeError("Pass-2 state appeared concurrently")
        self._validate_state_binding(self.state)
        if self.state["status"] == "publishing":
            self._mark_ambiguous("restart_during_publication")

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        for name in (
            "indexes_fd",
            "provenance_fd",
            "plans_fd",
            "audit_fd",
            "run_fd",
            "pass2_fd",
            "lock_fd",
            "root_fd",
        ):
            fd = getattr(self, name, None)
            if fd is not None:
                os.close(fd)

    def __enter__(self) -> Pass2Scheduler:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def _assert_fresh_dedicated_root(self) -> None:
        for name in ("plans", "claimed", "completed-plans", "runs", "provenance", "indexes"):
            try:
                child = ensure_child_directory(self.root_fd, name)
            except Exception:
                raise
            try:
                if os.listdir(child):
                    raise Pass2RuntimeError(
                        "Pass-2 requires a fresh dedicated Dispatcher root"
                    )
            finally:
                os.close(child)
        try:
            os.stat("rate-state.json", dir_fd=self.root_fd, follow_symlinks=False)
        except FileNotFoundError:
            pass
        else:
            raise Pass2RuntimeError("Pass-2 root already contains Dispatcher rate state")

    def _run_binding(self) -> dict[str, Any]:
        value = {
            "schema_version": 1,
            "run_id": self.config.run_id,
            "run_start": format_utc_z(self.config.run_start),
            "schedule_id": self.schedule.schedule_id,
            "schedule_sha256": self.schedule_sha256,
            "publish_lead_seconds": self.config.publish_lead_seconds,
            "plan_ttl_seconds": self.config.plan_ttl_seconds,
            "max_bytes_per_min": self.config.max_bytes_per_min,
            "max_items_per_min": self.config.max_items_per_min,
            "clock_tolerance_seconds": self.config.clock_tolerance_seconds,
            "stop_file": self.config.stop_file,
        }
        return {
            **value,
            "binding_sha256": _sha256(canonical_json(value).encode("utf-8")),
        }

    def _initial_state(self) -> dict[str, Any]:
        return {
            "schema_version": 1,
            "run_binding": self._run_binding(),
            "status": "running",
            "terminal_reason": None,
            "last_observed_at": None,
            "active_publication": None,
            "reservations": [],
            "items": {
                item.prediction_id: {
                    "status": "pending",
                    "reason": None,
                    "plan_id": None,
                    "warm_offset_s": item.warm_offset_s,
                }
                for item in self.schedule.items
            },
        }

    def _load_state(self) -> dict[str, Any]:
        raw = load_secure_json(self.run_fd, "state.json", max_bytes=16 * 2**20)
        if not isinstance(raw, dict) or set(raw) != {
            "schema_version",
            "run_binding",
            "status",
            "terminal_reason",
            "last_observed_at",
            "active_publication",
            "reservations",
            "items",
        }:
            raise Pass2RuntimeError("Pass-2 state fields are invalid")
        if raw["schema_version"] != 1:
            raise Pass2RuntimeError("unsupported Pass-2 state version")
        if raw["status"] not in {
            "running",
            "publishing",
            "completed",
            "stopped",
            "ambiguous",
            "failed",
        }:
            raise Pass2RuntimeError("Pass-2 state status is invalid")
        if not isinstance(raw["items"], dict) or not isinstance(raw["reservations"], list):
            raise Pass2RuntimeError("Pass-2 state collections are invalid")
        return raw

    def _validate_state_binding(self, state: dict[str, Any]) -> None:
        expected = self._run_binding()
        if state["run_binding"] != expected:
            raise Pass2RuntimeError("Pass-2 run binding mismatch")
        expected_items = {item.prediction_id for item in self.schedule.items}
        if set(state["items"]) != expected_items:
            raise Pass2RuntimeError("Pass-2 state item binding mismatch")
        for item in state["items"].values():
            if not isinstance(item, dict) or set(item) != {
                "status",
                "reason",
                "plan_id",
                "warm_offset_s",
            }:
                raise Pass2RuntimeError("Pass-2 item state is invalid")
            if item["status"] not in {"pending", "published", "skipped"}:
                raise Pass2RuntimeError("Pass-2 item state status is invalid")
        last = state["last_observed_at"]
        if last is not None:
            parse_utc_z(last)

    def _write_state(self) -> None:
        atomic_write_json(self.run_fd, "state.json", self.state, no_overwrite=False)

    def _observe_clock(self, now: datetime, monotonic: float) -> str | None:
        now = _utc(now, "clock")
        if not math.isfinite(monotonic):
            return "clock_monotonic_invalid"
        persisted = self.state["last_observed_at"]
        if persisted is not None and now < parse_utc_z(persisted):
            return "clock_wall_regressed"
        if self._previous_wall is not None and self._previous_monotonic is not None:
            wall_delta = (now - self._previous_wall).total_seconds()
            monotonic_delta = monotonic - self._previous_monotonic
            if monotonic_delta < 0:
                return "clock_monotonic_regressed"
            if wall_delta < 0:
                return "clock_wall_regressed"
            if abs(wall_delta - monotonic_delta) > self.config.clock_tolerance_seconds:
                return "clock_drift"
        self._previous_wall = now
        self._previous_monotonic = monotonic
        self.state["last_observed_at"] = format_utc_z(now)
        return None

    def _audit(
        self,
        item: ScheduleItem,
        *,
        status: str,
        reason: str,
        at: datetime,
        plan_id: str | None = None,
        target_map_id: str | None = None,
        target_map_sha256: str | None = None,
    ) -> None:
        record = {
            "schema_version": 1,
            "run_id": self.config.run_id,
            "schedule_id": self.schedule.schedule_id,
            "prediction_id": item.prediction_id,
            "volume_id": item.volume_id,
            "file_id": item.file_id,
            "warm_offset_s": item.warm_offset_s,
            "status": status,
            "reason": reason,
            "recorded_at": format_utc_z(at),
            "plan_id": plan_id,
            "target_map_id": target_map_id,
            "target_map_sha256": target_map_sha256,
        }
        if not atomic_write_json(
            self.audit_fd,
            f"{item.prediction_id}.json",
            record,
            no_overwrite=True,
        ):
            raise Pass2RuntimeError("Pass-2 audit record already exists")

    def _mark_items(
        self,
        items: tuple[ScheduleItem, ...] | list[ScheduleItem],
        *,
        status: str,
        reason: str,
        at: datetime,
        plan_id: str | None = None,
        target_map_id: str | None = None,
        target_map_sha256: str | None = None,
    ) -> None:
        for item in items:
            self._audit(
                item,
                status=status,
                reason=reason,
                at=at,
                plan_id=plan_id,
                target_map_id=target_map_id,
                target_map_sha256=target_map_sha256,
            )
            state = self.state["items"][item.prediction_id]
            state.update(status=status, reason=reason, plan_id=plan_id)

    def _pending_items(self) -> list[ScheduleItem]:
        return [
            item
            for item in self.schedule.items
            if self.state["items"][item.prediction_id]["status"] == "pending"
        ]

    def _groups(self) -> list[tuple[int, tuple[ScheduleItem, ...]]]:
        grouped: dict[int, list[ScheduleItem]] = {}
        for item in self._pending_items():
            grouped.setdefault(item.warm_offset_s, []).append(item)
        return [
            (offset, tuple(grouped[offset]))
            for offset in sorted(grouped)
        ]

    def _terminalize_pending(self, status: str, reason: str, now: datetime) -> None:
        pending = self._pending_items()
        if pending:
            self._mark_items(pending, status="skipped", reason=reason, at=now)
        self.state["status"] = status
        self.state["terminal_reason"] = reason
        self.state["active_publication"] = None
        self._write_state()

    def _mark_ambiguous(self, reason: str) -> None:
        now = _utc(self._now(), "clock")
        active = self.state.get("active_publication")
        active_ids = set(active.get("prediction_ids", ())) if isinstance(active, dict) else set()
        items = [item for item in self._pending_items() if item.prediction_id in active_ids]
        if items:
            self._mark_items(items, status="skipped", reason=reason, at=now)
        self.state["status"] = "ambiguous"
        self.state["terminal_reason"] = reason
        self._write_state()

    def _target_item(
        self,
        schedule_item: ScheduleItem,
        target: ApprovedTarget | None,
        *,
        created_at: datetime,
        deadline: datetime,
        task_id: str,
        target_map_id: str,
    ) -> tuple[dict[str, Any] | None, str | None]:
        if target is None:
            return None, "missing_target"
        if target.approved is not True:
            return None, "unauthorized_target"
        if target.target_valid_until < deadline:
            return None, "stale_target"
        logical = schedule_item.logical_range
        logical_end = logical.offset + logical.length
        target_end = target.offset + target.length
        if logical.offset < target.offset or logical_end > target_end:
            return None, "target_range_conflict"
        raw = {
            "task_id": task_id,
            "volume_id": schedule_item.volume_id,
            "file_id": schedule_item.file_id,
            "export_name": target.export_name,
            "export_id": target.export_id,
            "target_fh_b64": target.target_fh_b64,
            "target_valid_until": format_utc_z(deadline),
            "offset": logical.offset,
            "length": logical.length,
            "max_bytes": logical.length,
            "score": schedule_item.score,
            "reasons": [
                *schedule_item.reasons,
                f"pass2-schedule:{self.schedule.schedule_id}",
                f"pass2-target-map:{target_map_id}",
            ],
            "operation": "read",
        }
        try:
            PlanItem.model_validate(raw)
        except ValueError:
            return None, "invalid_target_binding"
        return raw, None

    def _rate_conflict(self, now: datetime, item_count: int, requested_bytes: int) -> bool:
        cutoff = now - timedelta(seconds=RATE_WINDOW_SECONDS)
        retained = []
        for entry in self.state["reservations"]:
            published_at = parse_utc_z(entry["published_at"])
            if published_at > now:
                raise Pass2RuntimeError("Pass-2 reservation is in the future")
            if published_at > cutoff:
                retained.append(entry)
        self.state["reservations"] = retained
        return (
            sum(entry["items"] for entry in retained) + item_count
            > self.config.max_items_per_min
            or sum(entry["bytes"] for entry in retained) + requested_bytes
            > self.config.max_bytes_per_min
        )

    def _commit(self, fd: int, name: str, data: bytes) -> None:
        if not atomic_write_bytes(fd, name, data, no_overwrite=True):
            raise Pass2RuntimeError("Pass-2 publication artifact already exists")

    def _publish_group(
        self,
        offset: int,
        items: tuple[ScheduleItem, ...],
        now: datetime,
    ) -> tuple[int, int]:
        target_map, target_map_sha256 = load_target_map(
            self.config.target_map_path,
            expected_schedule_id=self.schedule.schedule_id,
            evaluated_at=now,
        )
        deadline = now + timedelta(seconds=self.config.plan_ttl_seconds)
        if (deadline - now).total_seconds() > MAX_PLAN_TTL_SECONDS:
            raise Pass2RuntimeError("Pass-2 plan TTL exceeds Dispatcher limit")

        plan_identity = {
            "run_binding_sha256": self.state["run_binding"]["binding_sha256"],
            "warm_offset_s": offset,
            "prediction_ids": [item.prediction_id for item in items],
        }
        plan_id = _stable_id("plan", plan_identity)
        selected: list[tuple[ScheduleItem, dict[str, Any]]] = []
        for item in items:
            task_id = _stable_id(
                "task",
                {"plan_id": plan_id, "prediction_id": item.prediction_id},
            )
            raw, reason = self._target_item(
                item,
                target_map.resolve(item.volume_id, item.file_id),
                created_at=now,
                deadline=deadline,
                task_id=task_id,
                target_map_id=target_map.target_map_id,
            )
            if reason is not None:
                self._mark_items(
                    [item],
                    status="skipped",
                    reason=reason,
                    at=now,
                    target_map_id=target_map.target_map_id,
                    target_map_sha256=target_map_sha256,
                )
            else:
                selected.append((item, raw))

        if not selected:
            self._write_state()
            return 0, len(items)

        requested_bytes = sum(raw["length"] for _, raw in selected)
        if self._rate_conflict(now, len(selected), requested_bytes):
            selected_items = [item for item, _ in selected]
            self._mark_items(
                selected_items,
                status="skipped",
                reason="rate_budget_conflict",
                at=now,
                target_map_id=target_map.target_map_id,
                target_map_sha256=target_map_sha256,
            )
            self._write_state()
            return 0, len(items)

        plan_raw = {
            "schema_version": 2,
            "run_id": self.config.run_id,
            "plan_id": plan_id,
            "model_version": self.schedule.package["model_version"],
            "created_at": format_utc_z(now),
            "deadline": format_utc_z(deadline),
            "max_bytes": requested_bytes,
            "items": [raw for _, raw in selected],
        }
        plan = Plan.model_validate(plan_raw)
        if any(isinstance(item, InvalidPlanItem) for item in plan.items):
            raise Pass2RuntimeError("Pass-2 produced a non-executable plan item")
        if plan.max_bytes != sum(item.length for item in plan.items):
            raise Pass2RuntimeError("Pass-2 plan budget does not equal requested bytes")

        provenance = {
            "schema_version": 1,
            "run_id": self.config.run_id,
            "run_binding_sha256": self.state["run_binding"]["binding_sha256"],
            "schedule_id": self.schedule.schedule_id,
            "schedule_sha256": self.schedule_sha256,
            "target_map_id": target_map.target_map_id,
            "target_map_sha256": target_map_sha256,
            "warm_offset_s": offset,
            "due_at": format_utc_z(
                self.config.run_start + timedelta(seconds=offset)
            ),
            "created_at": format_utc_z(now),
            "deadline": format_utc_z(deadline),
            "plan_id": plan_id,
            "tasks": [
                {
                    "prediction_id": item.prediction_id,
                    "task_id": raw["task_id"],
                    "volume_id": item.volume_id,
                    "file_id": item.file_id,
                    "plan_ref": warmer_ref(plan_id),
                    "task_ref": warmer_ref(raw["task_id"]),
                }
                for item, raw in selected
            ],
        }
        plan_bytes = _encoded(plan_raw)
        provenance_bytes = _encoded(provenance)
        index = {
            "schema_version": 1,
            "run_id": self.config.run_id,
            "plan_id": plan_id,
            "schedule_id": self.schedule.schedule_id,
            "plan_path": f"plans/{plan_id}.json",
            "plan_sha256": _sha256(plan_bytes),
            "provenance_path": f"provenance/{plan_id}.json",
            "provenance_sha256": _sha256(provenance_bytes),
        }
        index_bytes = _encoded(index)
        self.state["status"] = "publishing"
        self.state["active_publication"] = {
            "plan_id": plan_id,
            "prediction_ids": [item.prediction_id for item, _ in selected],
            "target_map_id": target_map.target_map_id,
            "target_map_sha256": target_map_sha256,
            "plan_sha256": _sha256(plan_bytes),
            "provenance_sha256": _sha256(provenance_bytes),
            "index_sha256": _sha256(index_bytes),
        }
        self._write_state()
        try:
            self._commit(
                self.provenance_fd,
                f"{plan_id}.json",
                provenance_bytes,
            )
            self._commit(self.indexes_fd, f"{plan_id}.json", index_bytes)
            self._commit(self.plans_fd, f"{plan_id}.json", plan_bytes)
        except Exception:
            self._mark_ambiguous("ambiguous_publication")
            raise

        selected_items = [item for item, _ in selected]
        self._mark_items(
            selected_items,
            status="published",
            reason="published",
            at=now,
            plan_id=plan_id,
            target_map_id=target_map.target_map_id,
            target_map_sha256=target_map_sha256,
        )
        self.state["reservations"].append(
            {
                "published_at": format_utc_z(now),
                "plan_id": plan_id,
                "items": len(selected),
                "bytes": requested_bytes,
            }
        )
        self.state["status"] = "running"
        self.state["active_publication"] = None
        self._write_state()
        return len(selected), len(items) - len(selected)

    def step(self) -> Pass2Stats:
        now = _utc(self._now(), "clock")
        clock_reason = self._observe_clock(now, self._monotonic())
        if clock_reason is not None:
            self._terminalize_pending("failed", clock_reason, now)
            return self.stats()
        if self.state["status"] in {"completed", "stopped", "ambiguous", "failed"}:
            self._write_state()
            return self.stats()
        if secure_control_exists(self.root_fd, self.config.stop_file):
            self._terminalize_pending("stopped", "stop_requested", now)
            return self.stats()

        for offset, items in self._groups():
            due_at = self.config.run_start + timedelta(seconds=offset)
            release_at = due_at - timedelta(seconds=self.config.publish_lead_seconds)
            if now < release_at:
                continue
            if now > due_at:
                self._mark_items(items, status="skipped", reason="late_prediction", at=now)
                continue
            self._publish_group(offset, items, now)

        if not self._pending_items() and self.state["status"] == "running":
            self.state["status"] = "completed"
            self.state["terminal_reason"] = "schedule_complete"
        self._write_state()
        return self.stats()

    def run(self, *, once: bool = False) -> Pass2Stats:
        result = self.step()
        while not once and result.status == "running":
            now = _utc(self._now(), "clock")
            releases = [
                self.config.run_start
                + timedelta(seconds=item.warm_offset_s)
                - timedelta(seconds=self.config.publish_lead_seconds)
                for item in self._pending_items()
            ]
            delay = self.config.poll_seconds
            if releases:
                delay = min(delay, max(0.0, (min(releases) - now).total_seconds()))
            self._sleep(max(0.001, delay))
            result = self.step()
        return result

    def stats(self) -> Pass2Stats:
        items = list(self.state["items"].values())
        statuses = [item["status"] for item in items]
        plan_ids = {
            item["plan_id"]
            for item in items
            if item["status"] == "published" and item["plan_id"] is not None
        }
        return Pass2Stats(
            published_plans=len(plan_ids),
            published_items=statuses.count("published"),
            skipped_items=statuses.count("skipped"),
            pending_items=statuses.count("pending"),
            status=self.state["status"],
            reason=self.state["terminal_reason"],
        )
