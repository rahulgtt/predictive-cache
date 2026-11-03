"""Sequential, crash-conservative dispatcher for schema-v2 warm plans."""

from __future__ import annotations

import ctypes
import ipaddress
import json
import math
import os
import selectors
import signal
import subprocess
import sys
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from .schema import (
    InvalidPlanItem,
    Plan,
    PlanItem,
    ResultRecord,
    WarmerTerminal,
    format_utc_z,
    parse_utc_z,
    task_from_plan,
    warmer_ref,
)
from .securefs import (
    DEFAULT_MAX_JSON_BYTES,
    SecureFileError,
    atomic_write_json,
    claim_no_replace,
    ensure_child_directory,
    exclusive_lock,
    load_secure_json,
    loads_strict_json,
    open_trusted_executable,
    require_secure_directory,
    secure_control_exists,
    validate_committed_json_name,
)

MIN_WARMER_TIMEOUT_SECONDS = 0.1
MAX_WARMER_TIMEOUT_SECONDS = 300.0
RATE_WINDOW_SECONDS = 60.0
PR_SET_PDEATHSIG = 1
MAX_STDOUT_LINES = 5_000
MAX_STDOUT_LINE_BYTES = 16 * 1024
MAX_STDOUT_BYTES = 16 * 2**20
MAX_STDERR_BYTES = 1 * 2**20


class DispatcherError(RuntimeError):
    """The dispatcher cannot safely continue."""


def _require_linux_runtime(
    *,
    platform: str = sys.platform,
    libc_loader: Callable[..., Any] = ctypes.CDLL,
) -> None:
    if platform != "linux":
        raise DispatcherError("pcache-dispatcher requires Linux")
    libc = libc_loader(None, use_errno=True)
    missing = [name for name in ("renameat2", "prctl") if getattr(libc, name, None) is None]
    if missing:
        raise DispatcherError(f"Linux runtime is missing required API(s): {', '.join(missing)}")


def _apply_parent_death_signal(
    expected_parent_pid: int,
    *,
    prctl: Callable[..., int] | None = None,
    getppid: Callable[[], int] = os.getppid,
    kill: Callable[[int, int], None] = os.kill,
) -> None:
    if prctl is None:
        libc = ctypes.CDLL(None, use_errno=True)
        prctl = getattr(libc, "prctl", None)
        if prctl is None:
            raise OSError("Linux prctl(PR_SET_PDEATHSIG) is unavailable")
    if prctl(PR_SET_PDEATHSIG, signal.SIGTERM, 0, 0, 0) != 0:
        error = ctypes.get_errno()
        raise OSError(error, os.strerror(error))
    if getppid() != expected_parent_pid:
        kill(os.getpid(), signal.SIGTERM)


def _configure_warmer_child(expected_parent_pid: int) -> None:
    signal.signal(signal.SIGTERM, signal.SIG_DFL)
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    _apply_parent_death_signal(expected_parent_pid)


@dataclass(frozen=True)
class DispatcherConfig:
    root: Path
    warmer_command: Path
    host: str
    port: int = 2049
    uid: int = 1002
    gid: int = 1002
    max_plan_bytes: int = DEFAULT_MAX_JSON_BYTES
    max_output_bytes: int = MAX_STDOUT_BYTES
    max_stderr_bytes: int = MAX_STDERR_BYTES
    real_reads: bool = False
    max_bytes_per_min: int | None = 64 * 2**20
    max_items_per_min: int | None = 60
    enable_file: str = "enable-real-reads"
    stop_file: str = "stop"
    poll_seconds: float = 1.0

    def __post_init__(self) -> None:
        if not self.root.is_absolute():
            raise ValueError("dispatcher root must be an absolute path")
        if not self.warmer_command.is_absolute():
            raise ValueError("warmer command must be an absolute path")
        if "%" in self.host:
            raise ValueError("scoped IP addresses are not allowed")
        ipaddress.ip_address(self.host)
        if not 1 <= self.port <= 65535:
            raise ValueError("port must be in 1..65535")
        if self.uid < 0 or self.gid < 0:
            raise ValueError("uid and gid must be non-negative")
        if self.max_plan_bytes <= 0 or self.max_output_bytes <= 0:
            raise ValueError("file size limits must be positive")
        if self.max_output_bytes > MAX_STDOUT_BYTES:
            raise ValueError("max_output_bytes cannot exceed 16 MiB")
        if not 0 < self.max_stderr_bytes <= MAX_STDERR_BYTES:
            raise ValueError("max_stderr_bytes must be in 1..1 MiB")
        if self.max_bytes_per_min is not None and self.max_bytes_per_min <= 0:
            raise ValueError("max_bytes_per_min must be positive")
        if self.max_items_per_min is not None and self.max_items_per_min <= 0:
            raise ValueError("max_items_per_min must be positive")
        if self.real_reads and (
            self.max_bytes_per_min is None or self.max_items_per_min is None
        ):
            raise ValueError("real_reads requires both per-minute rate limits")
        if self.poll_seconds <= 0:
            raise ValueError("poll_seconds must be positive")
        for name in (self.enable_file, self.stop_file):
            if not name or "/" in name or name in {".", ".."}:
                raise ValueError("control file names must be simple root-relative names")


@dataclass
class DispatchStats:
    claimed: int = 0
    plans: int = 0
    results: int = 0


class Dispatcher:
    """Own one secure root and dispatch at most one Warmer process at a time."""

    def __init__(
        self,
        config: DispatcherConfig,
        *,
        now: Callable[[], datetime] | None = None,
        monotonic: Callable[[], float] | None = None,
        sleep: Callable[[float], None] | None = None,
    ) -> None:
        _require_linux_runtime()
        self.config = config
        self._now = now or (lambda: datetime.now(UTC))
        self._monotonic = monotonic or time.monotonic
        self._sleep = sleep or time.sleep
        self._stopping = False
        self._closed = False

        self.root_fd = require_secure_directory(config.root)
        try:
            self.lock_fd = exclusive_lock(self.root_fd)
            self.plans_fd = ensure_child_directory(self.root_fd, "plans")
            self.claimed_fd = ensure_child_directory(self.root_fd, "claimed")
            self.completed_fd = ensure_child_directory(self.root_fd, "completed-plans")
            self.runs_fd = ensure_child_directory(self.root_fd, "runs")
        except Exception:
            os.close(self.root_fd)
            raise

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        for name in (
            "runs_fd",
            "completed_fd",
            "claimed_fd",
            "plans_fd",
            "lock_fd",
            "root_fd",
        ):
            fd = getattr(self, name, None)
            if fd is not None:
                os.close(fd)

    def __enter__(self) -> Dispatcher:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def request_stop(self) -> None:
        self._stopping = True

    def stop_requested(self) -> bool:
        return self._stopping or secure_control_exists(self.root_fd, self.config.stop_file)

    def real_reads_enabled(self) -> bool:
        return self._real_read_gate_reason() is None

    def _real_read_gate_reason(self) -> str | None:
        if not self.config.real_reads:
            return "config_disabled"
        if not secure_control_exists(self.root_fd, self.config.enable_file):
            return "enable_file_missing"
        return None

    def run_once(self) -> DispatchStats:
        stats = DispatchStats()
        self._recover_state(stats)
        stats.claimed += self._claim_pending()
        for name in sorted(os.listdir(self.claimed_fd)):
            try:
                validate_committed_json_name(name)
            except SecureFileError:
                continue
            if self._plan_complete(name):
                continue
            stats.plans += 1
            stats.results += self._process_plan(name)
        return stats

    def _plan_complete(self, name: str) -> bool:
        try:
            marker = load_secure_json(self.completed_fd, name, max_bytes=16 * 1024)
        except FileNotFoundError:
            return False
        if not isinstance(marker, dict) or set(marker) != {
            "schema_version",
            "run_id",
            "plan_id",
            "completed_at",
        }:
            raise DispatcherError(f"invalid completed-plan marker: {name}")
        if marker["schema_version"] != 1:
            raise DispatcherError(f"unsupported completed-plan marker: {name}")
        parse_utc_z(marker["completed_at"])
        return True

    def _mark_plan_complete(self, name: str, plan: Plan) -> None:
        marker = {
            "schema_version": 1,
            "run_id": plan.run_id,
            "plan_id": plan.plan_id,
            "completed_at": format_utc_z(self._now()),
        }
        atomic_write_json(self.completed_fd, name, marker, no_overwrite=True)

    def run(self) -> None:
        while not self.stop_requested():
            self.run_once()
            if not self.stop_requested():
                time.sleep(self.config.poll_seconds)

    def _claim_pending(self) -> int:
        claimed = 0
        for name in sorted(os.listdir(self.plans_fd)):
            try:
                validate_committed_json_name(name)
            except SecureFileError:
                continue
            if claim_no_replace(
                self.plans_fd,
                self.claimed_fd,
                name,
                max_bytes=self.config.max_plan_bytes,
            ):
                claimed += 1
        return claimed

    def _load_plan(self, name: str) -> Plan:
        try:
            raw = load_secure_json(
                self.claimed_fd,
                name,
                max_bytes=self.config.max_plan_bytes,
            )
            plan = Plan.model_validate(raw)
        except (SecureFileError, ValueError) as exc:
            raise DispatcherError(f"invalid claimed plan {name}: {exc}") from exc
        return plan

    def _process_plan(self, name: str) -> int:
        plan = self._load_plan(name)
        written = 0
        reserved = self._reserved_plan_bytes(plan)
        real_started = False
        for index, item in enumerate(plan.items):
            if self._result_exists(plan.run_id, plan.plan_id, item.task_id):
                continue
            if isinstance(item, InvalidPlanItem):
                written += self._write_simple_result(
                    plan,
                    item,
                    "skipped",
                    item.skip_reason,
                    self._now(),
                )
                continue
            state = self._task_state(plan.run_id, plan.plan_id, item.task_id)
            if state is not None:
                if self._recover_state_record(state):
                    written += 1
                continue

            now = self._now()
            if self.stop_requested():
                written += self._write_simple_result(
                    plan, item, "cancelled", "stop_requested", now
                )
                continue
            if now >= plan.deadline:
                written += self._write_simple_result(plan, item, "skipped", "deadline", now)
                continue
            if now >= item.target_valid_until:
                written += self._write_simple_result(
                    plan, item, "skipped", "target_expired", now
                )
                continue
            if reserved + item.length > plan.max_bytes:
                written += self._write_simple_result(plan, item, "skipped", "budget", now)
                continue
            gate_reason = self._real_read_gate_reason()
            if gate_reason is not None:
                if real_started:
                    written += self._cancel_remaining(
                        plan,
                        plan.items[index:],
                        "real_read_disabled",
                    )
                    break
                written += self._write_simple_result(
                    plan, item, "skipped", "dry_run", now
                )
                continue

            remaining = min(
                (plan.deadline - now).total_seconds(),
                (item.target_valid_until - now).total_seconds(),
                MAX_WARMER_TIMEOUT_SECONDS,
            )
            if remaining < MIN_WARMER_TIMEOUT_SECONDS:
                written += self._write_simple_result(plan, item, "skipped", "deadline", now)
                continue
            rate_outcome = self._wait_for_rate(plan, item)
            if rate_outcome != "ready":
                now = self._now()
                if rate_outcome == "stop_requested":
                    written += self._write_simple_result(
                        plan, item, "cancelled", rate_outcome, now
                    )
                elif rate_outcome == "dry_run":
                    written += self._write_simple_result(
                        plan, item, "skipped", rate_outcome, now
                    )
                else:
                    written += self._write_simple_result(
                        plan, item, "skipped", "rate_limit_deadline", now
                    )
                continue
            now = self._now()
            real_started = True

            subprocess_deadline = now + timedelta(seconds=remaining)
            state = self._state_record(plan, item, now, subprocess_deadline)
            if not self._write_state(state, no_overwrite=True):
                raise DispatcherError(f"task state already exists for {item.task_id}")
            reserved += item.length

            task_path = self._write_task(plan, item)
            result = self._invoke_warmer(plan, item, task_path, remaining, now, state)
            if self._write_result(result):
                self._transition_state(
                    state,
                    status="terminal",
                    finished_at=result.finished_at,
                    result_status=result.status,
                )
                written += 1
            if self._should_stop_plan(result):
                written += self._cancel_remaining(
                    plan,
                    plan.items[index + 1 :],
                    f"plan_stopped:{result.reason}",
                )
                break
        self._mark_plan_complete(name, plan)
        return written

    def _should_stop_plan(self, result: ResultRecord) -> bool:
        if result.reason.startswith("ambiguous_"):
            return True
        if result.status == "cancelled":
            return True
        if result.warmer_terminal is None:
            return result.status == "failed"
        terminal = result.warmer_terminal
        if terminal.status in {"timeout", "cancelled"}:
            return True
        if terminal.status == "completed":
            return False
        if terminal.status == "partial":
            return terminal.reason not in {"eof", "short_read"}
        item_specific = {
            "getattr_not_regular",
            "getattr_range_exceeds_size",
        }
        target_statuses = (
            "NFS4ERR_ACCESS",
            "NFS4ERR_BADHANDLE",
            "NFS4ERR_FHEXPIRED",
            "NFS4ERR_PERM",
            "NFS4ERR_STALE",
            "NFS4ERR_WRONGSEC",
        )
        return terminal.reason not in item_specific and not terminal.reason.endswith(
            target_statuses
        )

    def _cancel_remaining(
        self,
        plan: Plan,
        items: tuple[PlanItem | InvalidPlanItem, ...],
        reason: str,
    ) -> int:
        written = 0
        for item in items:
            if self._result_exists(plan.run_id, plan.plan_id, item.task_id):
                continue
            written += self._write_simple_result(
                plan,
                item,
                "cancelled",
                reason[:256],
                self._now(),
            )
        return written

    def _reserved_plan_bytes(self, plan: Plan) -> int:
        total = 0
        for item in plan.items:
            result = self._load_result(plan.run_id, plan.plan_id, item.task_id)
            if result is not None and result.status in {
                "completed",
                "partial",
                "failed",
                "cancelled",
            }:
                total += item.length
        return total

    def _state_record(
        self,
        plan: Plan,
        item: PlanItem,
        started_at: datetime,
        subprocess_deadline: datetime,
    ) -> dict[str, Any]:
        return {
            "schema_version": 1,
            "status": "running",
            "attempt_id": uuid.uuid4().hex,
            "automatic_retry_allowed": False,
            "run_id": plan.run_id,
            "plan_id": plan.plan_id,
            "task_id": item.task_id,
            "volume_id": item.volume_id,
            "bytes_requested": item.length,
            "started_at": format_utc_z(started_at),
            "subprocess_deadline": format_utc_z(subprocess_deadline),
            "dispatcher_pid": os.getpid(),
            "warmer_pid": None,
            "warmer_command": str(self.config.warmer_command),
            "launched_at": None,
            "finished_at": None,
            "result_status": None,
            "supersedes": (
                None if item.supersedes is None else item.supersedes.model_dump()
            ),
        }

    def _plan_area_fd(self, run_id: str, plan_id: str, area: str) -> int:
        run_fd = ensure_child_directory(self.runs_fd, run_id)
        try:
            plans_fd = ensure_child_directory(run_fd, "plans")
        finally:
            os.close(run_fd)
        try:
            plan_fd = ensure_child_directory(plans_fd, plan_id)
        finally:
            os.close(plans_fd)
        try:
            return ensure_child_directory(plan_fd, area)
        finally:
            os.close(plan_fd)

    def _write_state(self, state: dict[str, Any], *, no_overwrite: bool) -> bool:
        plan_fd = self._plan_area_fd(state["run_id"], state["plan_id"], "state")
        try:
            return atomic_write_json(
                plan_fd,
                f"{state['task_id']}.json",
                state,
                no_overwrite=no_overwrite,
            )
        finally:
            os.close(plan_fd)

    def _task_state(
        self, run_id: str, plan_id: str, task_id: str
    ) -> dict[str, Any] | None:
        try:
            plan_fd = self._plan_area_fd(run_id, plan_id, "state")
        except FileNotFoundError:
            return None
        try:
            name = f"{task_id}.json"
            try:
                value = load_secure_json(plan_fd, name, max_bytes=16 * 1024)
            except FileNotFoundError:
                return None
            return self._validate_state(value)
        finally:
            os.close(plan_fd)

    def _validate_state(self, value: Any) -> dict[str, Any]:
        required = {
            "schema_version",
            "status",
            "attempt_id",
            "automatic_retry_allowed",
            "run_id",
            "plan_id",
            "task_id",
            "volume_id",
            "bytes_requested",
            "started_at",
            "subprocess_deadline",
            "dispatcher_pid",
            "warmer_pid",
            "warmer_command",
            "launched_at",
            "finished_at",
            "result_status",
            "supersedes",
        }
        if not isinstance(value, dict) or set(value) != required or value["schema_version"] != 1:
            raise DispatcherError("invalid task state")
        if value["status"] not in {"running", "terminal", "ambiguous"}:
            raise DispatcherError("invalid task state status")
        if (
            not isinstance(value["attempt_id"], str)
            or len(value["attempt_id"]) != 32
            or any(character not in "0123456789abcdef" for character in value["attempt_id"])
        ):
            raise DispatcherError("invalid task attempt identifier")
        if value["automatic_retry_allowed"] is not False:
            raise DispatcherError("automatic retry must remain disabled")
        for key in ("run_id", "plan_id", "task_id", "volume_id"):
            if not isinstance(value[key], str):
                raise DispatcherError("invalid task state identifier")
        if not isinstance(value["bytes_requested"], int) or value["bytes_requested"] <= 0:
            raise DispatcherError("invalid task state byte reservation")
        if not isinstance(value["dispatcher_pid"], int) or value["dispatcher_pid"] <= 0:
            raise DispatcherError("invalid dispatcher process evidence")
        if value["warmer_pid"] is not None and (
            not isinstance(value["warmer_pid"], int) or value["warmer_pid"] <= 0
        ):
            raise DispatcherError("invalid Warmer process evidence")
        if not isinstance(value["warmer_command"], str) or not value["warmer_command"]:
            raise DispatcherError("invalid Warmer command evidence")
        parse_utc_z(value["started_at"])
        parse_utc_z(value["subprocess_deadline"])
        for key in ("launched_at", "finished_at"):
            if value[key] is not None:
                parse_utc_z(value[key])
        if value["result_status"] is not None and value["result_status"] not in {
            "completed",
            "partial",
            "skipped",
            "failed",
            "cancelled",
        }:
            raise DispatcherError("invalid task result status evidence")
        supersedes = value["supersedes"]
        if supersedes is not None:
            if not isinstance(supersedes, dict) or set(supersedes) != {
                "run_id",
                "plan_id",
                "task_id",
            }:
                raise DispatcherError("invalid supersedes task reference")
            for key, identifier in supersedes.items():
                if not isinstance(identifier, str) or not identifier:
                    raise DispatcherError(f"invalid supersedes {key}")
        return value

    def _recover_state(self, stats: DispatchStats) -> None:
        states: list[dict[str, Any]] = []
        for run_id in sorted(os.listdir(self.runs_fd)):
            run_fd = ensure_child_directory(self.runs_fd, run_id)
            try:
                plans_fd = ensure_child_directory(run_fd, "plans")
            finally:
                os.close(run_fd)
            try:
                for plan_id in sorted(os.listdir(plans_fd)):
                    state_fd = self._plan_area_fd(run_id, plan_id, "state")
                    try:
                        for name in sorted(os.listdir(state_fd)):
                            try:
                                validate_committed_json_name(name)
                            except SecureFileError:
                                continue
                            states.append(
                                self._validate_state(
                                    load_secure_json(
                                        state_fd,
                                        name,
                                        max_bytes=16 * 1024,
                                    )
                                )
                            )
                    finally:
                        os.close(state_fd)
            finally:
                os.close(plans_fd)
        for state in states:
            result = self._load_result(
                state["run_id"], state["plan_id"], state["task_id"]
            )
            if result is not None:
                if state["status"] != "terminal":
                    self._transition_state(
                        state,
                        status="terminal",
                        finished_at=result.finished_at,
                        result_status=result.status,
                    )
                continue
            if state["status"] == "running":
                stats.results += self._recover_state_record(state)
            elif state["status"] == "terminal":
                raise DispatcherError("terminal task state is missing its terminal result")

    def _recover_state_record(self, state: dict[str, Any]) -> int:
        if state["status"] == "ambiguous":
            return 0
        if state["status"] == "terminal":
            if not self._result_exists(
                state["run_id"], state["plan_id"], state["task_id"]
            ):
                raise DispatcherError("terminal task state is missing its terminal result")
            return 0
        now = self._now()
        result = ResultRecord(
            run_id=state["run_id"],
            plan_id=state["plan_id"],
            task_id=state["task_id"],
            status="failed",
            reason="ambiguous_manual_review",
            bytes_requested=state["bytes_requested"],
            bytes_read=0,
            bytes_read_is_lower_bound=True,
            started_at=parse_utc_z(state["started_at"]),
            finished_at=now,
        )
        written = int(self._write_result(result))
        self._transition_state(
            state,
            status="ambiguous",
            finished_at=now,
            result_status="failed",
        )
        return written

    def _transition_state(
        self,
        state: dict[str, Any],
        *,
        status: str,
        finished_at: datetime | None = None,
        result_status: str | None = None,
        warmer_pid: int | None = None,
        launched_at: datetime | None = None,
    ) -> dict[str, Any]:
        updated = dict(state)
        updated["status"] = status
        if finished_at is not None:
            updated["finished_at"] = format_utc_z(finished_at)
        if result_status is not None:
            updated["result_status"] = result_status
        if warmer_pid is not None:
            updated["warmer_pid"] = warmer_pid
        if launched_at is not None:
            updated["launched_at"] = format_utc_z(launched_at)
        self._validate_state(updated)
        self._write_state(updated, no_overwrite=False)
        state.clear()
        state.update(updated)
        return state

    def _rate_entries(self, now: datetime) -> list[dict[str, Any]]:
        try:
            raw = load_secure_json(self.root_fd, "rate-state.json", max_bytes=2 * 2**20)
        except FileNotFoundError:
            return []
        if not isinstance(raw, list):
            raise DispatcherError("invalid persistent rate state")
        cutoff = now - timedelta(seconds=RATE_WINDOW_SECONDS)
        entries = []
        for entry in raw:
            if not isinstance(entry, dict) or set(entry) != {
                "reserved_at",
                "plan_id",
                "task_id",
                "bytes",
            }:
                raise DispatcherError("invalid persistent rate reservation")
            reserved_at = parse_utc_z(entry["reserved_at"])
            if reserved_at > now + timedelta(seconds=1):
                raise DispatcherError("rate reservation is in the future")
            if reserved_at > cutoff:
                entries.append(entry)
        return entries

    def _reserve_rate(self, plan: Plan, item: PlanItem, now: datetime) -> bool:
        entries = self._rate_entries(now)
        if self.config.max_items_per_min is None or self.config.max_bytes_per_min is None:
            raise DispatcherError("real-read rate limits are not configured")
        if len(entries) + 1 > self.config.max_items_per_min:
            return False
        if sum(entry["bytes"] for entry in entries) + item.length > self.config.max_bytes_per_min:
            return False
        entries.append(
            {
                "reserved_at": format_utc_z(now),
                "plan_id": plan.plan_id,
                "task_id": item.task_id,
                "bytes": item.length,
            }
        )
        atomic_write_json(self.root_fd, "rate-state.json", entries, no_overwrite=False)
        return True

    def _rate_delay(self, item: PlanItem, now: datetime) -> float:
        entries = sorted(
            self._rate_entries(now),
            key=lambda entry: parse_utc_z(entry["reserved_at"]),
        )
        if self.config.max_items_per_min is None or self.config.max_bytes_per_min is None:
            raise DispatcherError("real-read rate limits are not configured")

        def fits(current: list[dict[str, Any]]) -> bool:
            return (
                len(current) + 1 <= self.config.max_items_per_min
                and sum(entry["bytes"] for entry in current) + item.length
                <= self.config.max_bytes_per_min
            )

        if fits(entries):
            return 0.0
        if not entries:
            return math.inf
        remaining = list(entries)
        for entry in entries:
            expiry = parse_utc_z(entry["reserved_at"]) + timedelta(seconds=RATE_WINDOW_SECONDS)
            remaining.remove(entry)
            if fits(remaining):
                return max(0.0, (expiry - now).total_seconds())
        raise DispatcherError("task exceeds configured per-minute byte capacity")

    def _wait_for_rate(self, plan: Plan, item: PlanItem) -> str:
        while True:
            now = self._now()
            if self.stop_requested():
                return "stop_requested"
            gate_reason = self._real_read_gate_reason()
            if gate_reason is not None:
                return "dry_run"
            deadline = min(plan.deadline, item.target_valid_until)
            if now >= deadline:
                return "rate_limit_deadline"
            delay = self._rate_delay(item, now)
            if delay <= 0:
                if self._reserve_rate(plan, item, now):
                    return "ready"
                continue
            if math.isinf(delay):
                return "rate_limit_deadline"
            if now + timedelta(seconds=delay) >= deadline:
                return "rate_limit_deadline"
            self._sleep(min(delay, 0.1))

    def _write_task(self, plan: Plan, item: PlanItem) -> Path:
        plan_fd = self._plan_area_fd(plan.run_id, plan.plan_id, "tasks")
        try:
            task = task_from_plan(plan, item)
            name = f"{item.task_id}.json"
            created = atomic_write_json(
                plan_fd,
                name,
                task.model_dump(mode="json"),
                no_overwrite=True,
            )
            if not created:
                raise DispatcherError(f"task file already exists for {item.task_id}")
        finally:
            os.close(plan_fd)
        return (
            self.config.root
            / "runs"
            / plan.run_id
            / "plans"
            / plan.plan_id
            / "tasks"
            / name
        )

    def _invoke_warmer(
        self,
        plan: Plan,
        item: PlanItem,
        task_path: Path,
        timeout_seconds: float,
        started_at: datetime,
        state: dict[str, Any],
    ) -> ResultRecord:
        launch_time = self._now()
        timeout_seconds = min(
            timeout_seconds,
            (plan.deadline - launch_time).total_seconds(),
            (item.target_valid_until - launch_time).total_seconds(),
            MAX_WARMER_TIMEOUT_SECONDS,
        )
        if timeout_seconds < MIN_WARMER_TIMEOUT_SECONDS:
            return ResultRecord(
                run_id=plan.run_id,
                plan_id=plan.plan_id,
                task_id=item.task_id,
                status="skipped",
                reason="deadline",
                bytes_requested=item.length,
                bytes_read=0,
                bytes_read_is_lower_bound=False,
                started_at=started_at,
                finished_at=launch_time,
            )
        executable_fd = open_trusted_executable(self.config.warmer_command)
        state["subprocess_deadline"] = format_utc_z(
            launch_time + timedelta(seconds=timeout_seconds)
        )
        self._validate_state(state)
        self._write_state(state, no_overwrite=False)
        command = [
            f"/proc/self/fd/{executable_fd}",
            "--host",
            self.config.host,
            "--port",
            str(self.config.port),
            "--task-file",
            str(task_path),
            "--timeout-seconds",
            f"{math.floor(timeout_seconds * 1_000_000) / 1_000_000:.6f}",
            "--uid",
            str(self.config.uid),
            "--gid",
            str(self.config.gid),
        ]
        output_plan_fd = self._plan_area_fd(plan.run_id, plan.plan_id, "outputs")
        attempt_id = state["attempt_id"]
        output_name = f"{item.task_id}-{attempt_id}.jsonl"
        stderr_name = f"{item.task_id}-{attempt_id}.stderr"
        output_fd = self._create_evidence_file(output_plan_fd, output_name)
        stderr_fd = self._create_evidence_file(output_plan_fd, stderr_name)
        os.fsync(output_plan_fd)
        try:
            expected_parent_pid = os.getpid()
            try:
                process = subprocess.Popen(
                    command,
                    stdin=subprocess.DEVNULL,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    close_fds=True,
                    pass_fds=(executable_fd,),
                    start_new_session=True,
                    bufsize=0,
                    preexec_fn=lambda: _configure_warmer_child(expected_parent_pid),
                )
            except (OSError, subprocess.SubprocessError):
                return self._failed_result(
                    plan,
                    item,
                    started_at,
                    self._now(),
                    "warmer_launch_failed",
                    lower_bound=False,
                )
            finally:
                os.close(executable_fd)
            self._transition_state(
                state,
                status="running",
                warmer_pid=process.pid,
                launched_at=self._now(),
            )
            (
                return_code,
                records,
                framing_error,
                stopped,
                timed_out,
                orphaned_group,
            ) = self._capture_process(
                process,
                output_fd,
                stderr_fd,
                timeout_seconds,
            )
        finally:
            os.close(output_fd)
            os.close(stderr_fd)
            os.close(output_plan_fd)

        finished_at = self._now()
        if framing_error is not None:
            return self._failed_result(
                plan,
                item,
                started_at,
                finished_at,
                f"ambiguous_{framing_error}",
                lower_bound=True,
            )
        if orphaned_group:
            return self._failed_result(
                plan,
                item,
                started_at,
                finished_at,
                "ambiguous_orphaned_process_group",
                lower_bound=True,
            )

        terminal, parse_reason = self._parse_terminal(records, plan, item, return_code)
        if terminal is None:
            if stopped:
                parse_reason = "ambiguous_stop_without_terminal"
            elif timed_out:
                parse_reason = "ambiguous_timeout_without_terminal"
            else:
                parse_reason = f"ambiguous_{parse_reason}"
            return self._failed_result(
                plan,
                item,
                started_at,
                finished_at,
                parse_reason,
                lower_bound=True,
            )

        expected_exit_codes = {
            "completed": {0},
            "partial": {1},
            "error": {1},
            "timeout": {1},
            "cancelled": {130, 143},
        }
        if not terminal.execution_started and terminal.status == "error":
            expected_exit_codes["error"] = {1, 2}
        if return_code not in expected_exit_codes[terminal.status]:
            return self._failed_result(
                plan,
                item,
                started_at,
                finished_at,
                "ambiguous_terminal_exit_mismatch",
                lower_bound=True,
            )
        status_map = {
            "completed": "completed",
            "partial": "partial",
            "error": "failed",
            "timeout": "failed",
            "cancelled": "cancelled",
        }
        return ResultRecord(
            run_id=plan.run_id,
            plan_id=plan.plan_id,
            task_id=item.task_id,
            status=status_map[terminal.status],
            reason=terminal.reason,
            bytes_requested=terminal.bytes_requested,
            bytes_read=terminal.bytes_read,
            bytes_read_is_lower_bound=terminal.bytes_read_is_lower_bound,
            started_at=started_at,
            finished_at=finished_at,
            warmer_terminal=terminal,
        )

    def _create_evidence_file(self, dir_fd: int, name: str) -> int:
        return os.open(
            name,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
            0o600,
            dir_fd=dir_fd,
        )

    def _capture_process(
        self,
        process: subprocess.Popen[Any],
        output_fd: int,
        stderr_fd: int,
        timeout_seconds: float,
    ) -> tuple[int, list[Any], str | None, bool, bool, bool]:
        if process.stdout is None or process.stderr is None:
            raise DispatcherError("Warmer pipes were not created")
        selector = selectors.DefaultSelector()
        streams = {
            process.stdout.fileno(): ("stdout", process.stdout),
            process.stderr.fileno(): ("stderr", process.stderr),
        }
        for descriptor, (kind, stream) in streams.items():
            os.set_blocking(descriptor, False)
            selector.register(stream, selectors.EVENT_READ, kind)

        records: list[Any] = []
        stdout_buffer = bytearray()
        stdout_total = 0
        stderr_total = 0
        line_count = 0
        framing_error: str | None = None
        stopped = False
        timed_out = False
        orphaned_group = False
        termination_requested = False
        end = self._monotonic() + timeout_seconds

        try:
            while selector.get_map() or process.poll() is None:
                for key, _mask in selector.select(timeout=0.05):
                    kind = key.data
                    try:
                        chunk = os.read(key.fileobj.fileno(), 64 * 1024)
                    except BlockingIOError:
                        continue
                    if not chunk:
                        selector.unregister(key.fileobj)
                        key.fileobj.close()
                        if kind == "stdout" and stdout_buffer and framing_error is None:
                            os.write(output_fd, stdout_buffer)
                            os.fsync(output_fd)
                            framing_error = "stdout_partial_line"
                        continue
                    if kind == "stderr":
                        stderr_total += len(chunk)
                        stderr_size = os.fstat(stderr_fd).st_size
                        remaining = max(0, self.config.max_stderr_bytes - stderr_size)
                        if remaining:
                            os.write(stderr_fd, chunk[:remaining])
                            os.fsync(stderr_fd)
                        if stderr_total > self.config.max_stderr_bytes:
                            framing_error = framing_error or "stderr_overflow"
                        continue

                    stdout_total += len(chunk)
                    remaining = max(0, self.config.max_output_bytes - os.fstat(output_fd).st_size)
                    stdout_buffer.extend(chunk[:remaining])
                    if stdout_total > self.config.max_output_bytes:
                        framing_error = framing_error or "stdout_overflow"
                    while b"\n" in stdout_buffer:
                        raw_line, _, remainder = stdout_buffer.partition(b"\n")
                        stdout_buffer = bytearray(remainder)
                        line_count += 1
                        if line_count > MAX_STDOUT_LINES:
                            framing_error = framing_error or "stdout_line_count"
                            break
                        if not raw_line or len(raw_line) > MAX_STDOUT_LINE_BYTES:
                            framing_error = framing_error or "stdout_line_length"
                            break
                        os.write(output_fd, raw_line + b"\n")
                        os.fsync(output_fd)
                        try:
                            records.append(loads_strict_json(raw_line.decode("utf-8")))
                        except (UnicodeDecodeError, json.JSONDecodeError, SecureFileError):
                            framing_error = framing_error or "stdout_invalid_json"
                            break
                    if len(stdout_buffer) > MAX_STDOUT_LINE_BYTES:
                        framing_error = framing_error or "stdout_line_length"

                if framing_error is not None and not termination_requested:
                    termination_requested = True
                    self._terminate_process_group(process)
                if self.stop_requested() and not termination_requested:
                    stopped = True
                    termination_requested = True
                    self._terminate_process_group(process)
                if self._monotonic() >= end and not termination_requested:
                    timed_out = True
                    termination_requested = True
                    self._terminate_process_group(process)
                if process.poll() is not None and self._process_group_exists(process.pid):
                    orphaned_group = True
                    termination_requested = True
                    self._terminate_process_group(process)
            return_code = process.wait()
        finally:
            selector.close()
            for _kind, stream in streams.values():
                if not stream.closed:
                    stream.close()
            os.fsync(output_fd)
            os.fsync(stderr_fd)
        return return_code, records, framing_error, stopped, timed_out, orphaned_group

    def _process_group_exists(self, process_group_id: int) -> bool:
        try:
            os.killpg(process_group_id, 0)
        except ProcessLookupError:
            return False
        except PermissionError:
            return True
        return True

    def _terminate_process_group(self, process: subprocess.Popen[Any]) -> None:
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        deadline = time.monotonic() + 2
        while self._process_group_exists(process.pid) and time.monotonic() < deadline:
            time.sleep(0.02)
        if self._process_group_exists(process.pid):
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
        try:
            process.wait(timeout=2)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=2)

    def _parse_terminal(
        self,
        records: list[Any],
        plan: Plan,
        item: PlanItem,
        return_code: int,
    ) -> tuple[WarmerTerminal | None, str]:
        terminals = [
            (index, record)
            for index, record in enumerate(records)
            if isinstance(record, dict) and record.get("type") == "result"
        ]
        if not terminals:
            synthesized = self._synthesize_prevalidation_terminal(
                records,
                plan,
                item,
                return_code,
            )
            if synthesized is not None:
                return synthesized, "ok"
        if len(terminals) != 1 or terminals[0][0] != len(records) - 1:
            return None, "warmer_terminal_missing_or_multiple"
        try:
            terminal = WarmerTerminal.model_validate(terminals[0][1])
        except ValueError:
            return None, "warmer_terminal_invalid"
        if (
            terminal.plan_ref != warmer_ref(plan.plan_id)
            or terminal.task_ref != warmer_ref(item.task_id)
        ):
            return None, "warmer_terminal_reference_mismatch"
        if terminal.bytes_requested != item.length:
            return None, "warmer_terminal_requested_bytes_mismatch"
        return terminal, "ok"

    def _synthesize_prevalidation_terminal(
        self,
        records: list[Any],
        plan: Plan,
        item: PlanItem,
        return_code: int,
    ) -> WarmerTerminal | None:
        if len(records) != 1:
            return None
        raw = records[0]
        if not isinstance(raw, dict):
            return None
        if set(raw) != {"type", "status", "reason", "execution_started"}:
            return None
        status = raw["status"]
        reason = raw["reason"]
        expected = {"timeout": {1}, "cancelled": {130, 143}, "error": {1, 2}}
        if status not in expected or return_code not in expected[status]:
            return None
        if (
            raw["type"] != "pre_validation_result"
            or raw["execution_started"] is not False
            or not isinstance(reason, str)
            or not 1 <= len(reason) <= 256
            or any(ord(character) < 32 for character in reason)
        ):
            return None
        return WarmerTerminal(
            type="result",
            status=status,
            reason=reason,
            bytes_requested=item.length,
            bytes_read=0,
            bytes_read_is_lower_bound=False,
            execution_started=False,
            plan_ref=warmer_ref(plan.plan_id),
            task_ref=warmer_ref(item.task_id),
        )

    def _failed_result(
        self,
        plan: Plan,
        item: PlanItem,
        started_at: datetime,
        finished_at: datetime,
        reason: str,
        *,
        lower_bound: bool,
    ) -> ResultRecord:
        return ResultRecord(
            run_id=plan.run_id,
            plan_id=plan.plan_id,
            task_id=item.task_id,
            status="failed",
            reason=reason,
            bytes_requested=item.length,
            bytes_read=0,
            bytes_read_is_lower_bound=lower_bound,
            started_at=started_at,
            finished_at=finished_at,
        )

    def _write_simple_result(
        self,
        plan: Plan,
        item: PlanItem | InvalidPlanItem,
        status: str,
        reason: str,
        now: datetime,
    ) -> int:
        result = ResultRecord(
            run_id=plan.run_id,
            plan_id=plan.plan_id,
            task_id=item.task_id,
            status=status,
            reason=reason,
            bytes_requested=item.length,
            bytes_read=0,
            bytes_read_is_lower_bound=False,
            started_at=now,
            finished_at=now,
        )
        return int(self._write_result(result))

    def _write_result(self, result: ResultRecord) -> bool:
        plan_fd = self._plan_area_fd(
            result.run_id,
            result.plan_id,
            "results",
        )
        try:
            return atomic_write_json(
                plan_fd,
                f"{result.task_id}.json",
                result.model_dump(mode="json"),
                no_overwrite=True,
            )
        finally:
            os.close(plan_fd)

    def _result_exists(self, run_id: str, plan_id: str, task_id: str) -> bool:
        return self._load_result(run_id, plan_id, task_id) is not None

    def _load_result(
        self, run_id: str, plan_id: str, task_id: str
    ) -> ResultRecord | None:
        plan_fd = self._plan_area_fd(run_id, plan_id, "results")
        try:
            try:
                raw = load_secure_json(
                    plan_fd,
                    f"{task_id}.json",
                    max_bytes=256 * 1024,
                )
            except FileNotFoundError:
                return None
            try:
                return ResultRecord.model_validate(raw)
            except ValueError as exc:
                raise DispatcherError(
                    f"invalid terminal result for {plan_id}/{task_id}: {exc}"
                ) from exc
        finally:
            os.close(plan_fd)
