"""Strict schema-v2 records shared by the dispatcher file boundaries."""

from __future__ import annotations

import base64
import hashlib
import math
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, ClassVar, Literal

SCHEMA_VERSION = 2
MAX_TASK_BYTES = 16 * 2**20
MAX_TTL_SECONDS = 300
ID_PATTERN = re.compile(r"^[A-Za-z0-9._:-]{1,128}$")
MODEL_VERSION_PATTERN = re.compile(r"^[A-Za-z0-9._:@-]{1,128}$")
UTC_Z_PATTERN = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?Z$"
)


def parse_utc_z(value: object) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str) and UTC_Z_PATTERN.fullmatch(value):
        parsed = datetime.fromisoformat(value[:-1] + "+00:00")
    else:
        raise ValueError("timestamp must be RFC3339 UTC with a trailing Z")
    if parsed.tzinfo is None or parsed.utcoffset() != UTC.utcoffset(parsed):
        raise ValueError("timestamp must be UTC")
    return parsed.astimezone(UTC)


def format_utc_z(value: datetime) -> str:
    value = value.astimezone(UTC)
    if value.microsecond:
        return value.isoformat(timespec="microseconds").replace("+00:00", "Z")
    return value.isoformat(timespec="seconds").replace("+00:00", "Z")


def validate_handle(value: object) -> str:
    if not isinstance(value, str) or not value or "=" in value:
        raise ValueError("target_fh_b64 must be unpadded canonical base64url")
    try:
        decoded = base64.b64decode(
            value + "=" * (-len(value) % 4),
            altchars=b"-_",
            validate=True,
        )
    except (ValueError, base64.binascii.Error) as exc:
        raise ValueError("target_fh_b64 is not valid base64url") from exc
    if not 1 <= len(decoded) <= 128:
        raise ValueError("target_fh_b64 must decode to 1..128 bytes")
    canonical = base64.urlsafe_b64encode(decoded).rstrip(b"=").decode("ascii")
    if canonical != value:
        raise ValueError("target_fh_b64 is not canonical base64url")
    return value


def warmer_ref(identifier: str) -> str:
    return hashlib.sha256(identifier.encode("ascii")).hexdigest()[:32]


def _exact(
    value: object,
    fields: set[str],
    record: str,
    *,
    optional: set[str] | None = None,
) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{record} must be an object")
    optional = optional or set()
    missing = fields - optional - set(value)
    extra = set(value) - fields
    if missing or extra:
        details = []
        if missing:
            details.append(f"missing {sorted(missing)}")
        if extra:
            details.append(f"extra {sorted(extra)}")
        raise ValueError(f"{record} fields are invalid: {', '.join(details)}")
    return value


def _identifier(value: object, field: str) -> str:
    if not isinstance(value, str) or not ID_PATTERN.fullmatch(value):
        raise ValueError(f"{field} must match {ID_PATTERN.pattern}")
    return value


def _model_version(value: object) -> str:
    if not isinstance(value, str) or not MODEL_VERSION_PATTERN.fullmatch(value):
        raise ValueError(f"model_version must match {MODEL_VERSION_PATTERN.pattern}")
    return value


def _integer(
    value: object,
    field: str,
    *,
    minimum: int = 0,
    maximum: int | None = None,
) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ValueError(f"{field} must be an integer >= {minimum}")
    if maximum is not None and value > maximum:
        raise ValueError(f"{field} must be <= {maximum}")
    return value


def _finite(value: object, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{field} must be numeric")
    result = float(value)
    if not math.isfinite(result):
        raise ValueError(f"{field} must be finite")
    return result


@dataclass(frozen=True)
class TaskRef:
    run_id: str
    plan_id: str
    task_id: str

    @classmethod
    def model_validate(cls, raw: object) -> TaskRef:
        value = _exact(raw, {"run_id", "plan_id", "task_id"}, "supersedes")
        return cls(
            run_id=_identifier(value["run_id"], "supersedes.run_id"),
            plan_id=_identifier(value["plan_id"], "supersedes.plan_id"),
            task_id=_identifier(value["task_id"], "supersedes.task_id"),
        )

    def model_dump(self) -> dict[str, str]:
        return {
            "run_id": self.run_id,
            "plan_id": self.plan_id,
            "task_id": self.task_id,
        }


@dataclass(frozen=True)
class PlanItem:
    task_id: str
    volume_id: str
    file_id: str
    export_name: str
    export_id: str
    target_fh_b64: str
    target_valid_until: datetime
    offset: int
    length: int
    max_bytes: int
    score: float
    reasons: tuple[str, ...]
    operation: Literal["read"] = "read"
    supersedes: TaskRef | None = None

    model_fields: ClassVar[dict[str, None]] = {
        key: None
        for key in (
            "task_id",
            "volume_id",
            "file_id",
            "export_name",
            "export_id",
            "target_fh_b64",
            "target_valid_until",
            "offset",
            "length",
            "max_bytes",
            "score",
            "reasons",
            "operation",
            "supersedes",
        )
    }

    @classmethod
    def model_validate(cls, raw: object) -> PlanItem:
        value = _exact(
            raw,
            set(cls.model_fields),
            "plan item",
            optional={"operation", "supersedes"},
        )
        operation = value.get("operation", "read")
        if operation != "read":
            raise ValueError("operation must be read when present")
        reasons = value["reasons"]
        if not isinstance(reasons, list) or len(reasons) > 64:
            raise ValueError("reasons must be a list with at most 64 entries")
        if any(
            not isinstance(reason, str)
            or not reason
            or len(reason.encode("utf-8")) > 512
            for reason in reasons
        ):
            raise ValueError("reasons must be non-empty strings of at most 512 UTF-8 bytes")
        item = cls(
            task_id=_identifier(value["task_id"], "task_id"),
            volume_id=_identifier(value["volume_id"], "volume_id"),
            file_id=_identifier(value["file_id"], "file_id"),
            export_name=_identifier(value["export_name"], "export_name"),
            export_id=_identifier(value["export_id"], "export_id"),
            target_fh_b64=validate_handle(value["target_fh_b64"]),
            target_valid_until=parse_utc_z(value["target_valid_until"]),
            offset=_integer(value["offset"], "offset"),
            length=_integer(value["length"], "length", minimum=1, maximum=MAX_TASK_BYTES),
            max_bytes=_integer(
                value["max_bytes"], "max_bytes", minimum=1, maximum=MAX_TASK_BYTES
            ),
            score=_finite(value["score"], "score"),
            reasons=tuple(reasons),
            operation="read",
            supersedes=(
                None
                if value.get("supersedes") is None
                else TaskRef.model_validate(value["supersedes"])
            ),
        )
        if item.max_bytes != item.length:
            raise ValueError("item max_bytes must equal length because retries are disabled")
        if item.offset + item.length > 2**63 - 1:
            raise ValueError("offset + length exceeds the supported range")
        return item

    def model_dump(self, *, mode: str = "python") -> dict[str, Any]:
        del mode
        return {
            "task_id": self.task_id,
            "volume_id": self.volume_id,
            "file_id": self.file_id,
            "export_name": self.export_name,
            "export_id": self.export_id,
            "target_fh_b64": self.target_fh_b64,
            "target_valid_until": format_utc_z(self.target_valid_until),
            "offset": self.offset,
            "length": self.length,
            "max_bytes": self.max_bytes,
            "score": self.score,
            "reasons": list(self.reasons),
            "operation": self.operation,
            "supersedes": (
                None if self.supersedes is None else self.supersedes.model_dump()
            ),
        }


@dataclass(frozen=True)
class InvalidPlanItem:
    task_id: str
    length: int
    skip_reason: str


def _invalid_item(raw: object, error: ValueError) -> InvalidPlanItem:
    if not isinstance(raw, dict):
        raise ValueError("plan item without an object/task_id is a plan integrity error") from error
    task_id = _identifier(raw.get("task_id"), "task_id")
    length_value = raw.get("length")
    length = (
        length_value
        if isinstance(length_value, int)
        and not isinstance(length_value, bool)
        and length_value >= 0
        else 0
    )
    detail = str(error)
    if (
        "fields are invalid" in detail
        or "supersedes" in detail
        or "score" in detail
        or "reasons" in detail
    ):
        raise ValueError(f"plan item integrity error: {detail}") from error
    if "operation must be read" in detail:
        reason = "unauthorized_operation"
    elif "target_fh_b64" in detail or any(
        field in detail
        for field in ("volume_id", "file_id", "export_name", "export_id")
    ):
        reason = "malformed_target"
    elif any(
        phrase in detail
        for phrase in (
            "offset",
            "length",
            "max_bytes",
            "supported range",
            "retries are disabled",
        )
    ):
        reason = "target_out_of_range"
    elif "timestamp" in detail or "deadline" in detail or "validity" in detail:
        reason = "target_time_invalid"
    else:
        reason = "malformed_item"
    return InvalidPlanItem(task_id=task_id, length=length, skip_reason=reason)


@dataclass(frozen=True)
class Plan:
    schema_version: Literal[2]
    run_id: str
    plan_id: str
    model_version: str
    created_at: datetime
    deadline: datetime
    max_bytes: int
    items: tuple[PlanItem | InvalidPlanItem, ...]

    model_fields: ClassVar[dict[str, None]] = {
        key: None
        for key in (
            "schema_version",
            "run_id",
            "plan_id",
            "model_version",
            "created_at",
            "deadline",
            "max_bytes",
            "items",
        )
    }

    @classmethod
    def model_validate(cls, raw: object) -> Plan:
        value = _exact(raw, set(cls.model_fields), "plan")
        if value["schema_version"] != SCHEMA_VERSION:
            raise ValueError(f"unsupported schema_version: {value['schema_version']!r}")
        raw_items = value["items"]
        if not isinstance(raw_items, list) or not 1 <= len(raw_items) <= 100_000:
            raise ValueError("items must contain 1..100000 entries")
        created_at = parse_utc_z(value["created_at"])
        deadline = parse_utc_z(value["deadline"])
        if created_at >= deadline:
            raise ValueError("created_at must be earlier than deadline")
        items: list[PlanItem | InvalidPlanItem] = []
        for raw_item in raw_items:
            try:
                item: PlanItem | InvalidPlanItem = PlanItem.model_validate(raw_item)
            except ValueError as exc:
                item = _invalid_item(raw_item, exc)
            if isinstance(item, PlanItem):
                if deadline > item.target_valid_until:
                    item = InvalidPlanItem(
                        task_id=item.task_id,
                        length=item.length,
                        skip_reason="target_valid_before_deadline",
                    )
                else:
                    ttl = (item.target_valid_until - created_at).total_seconds()
                    if ttl <= 0 or ttl > MAX_TTL_SECONDS:
                        item = InvalidPlanItem(
                            task_id=item.task_id,
                            length=item.length,
                            skip_reason="target_ttl_invalid",
                        )
            items.append(item)
        task_ids = [item.task_id for item in items]
        if len(task_ids) != len(set(task_ids)):
            raise ValueError("task_id values must be unique within a plan")
        return cls(
            schema_version=SCHEMA_VERSION,
            run_id=_identifier(value["run_id"], "run_id"),
            plan_id=_identifier(value["plan_id"], "plan_id"),
            model_version=_model_version(value["model_version"]),
            created_at=created_at,
            deadline=deadline,
            max_bytes=_integer(value["max_bytes"], "max_bytes", minimum=1),
            items=tuple(items),
        )


@dataclass(frozen=True)
class WarmerTask:
    run_id: str
    plan_id: str
    task_id: str
    volume_id: str
    file_id: str
    export_name: str
    export_id: str
    target_fh_b64: str
    offset: int
    length: int
    max_bytes: int
    created_at: datetime
    deadline: datetime
    target_valid_until: datetime
    schema_version: Literal[2] = SCHEMA_VERSION

    model_fields: ClassVar[dict[str, None]] = {
        key: None
        for key in (
            "schema_version",
            "run_id",
            "plan_id",
            "task_id",
            "volume_id",
            "file_id",
            "export_name",
            "export_id",
            "target_fh_b64",
            "offset",
            "length",
            "max_bytes",
            "created_at",
            "deadline",
            "target_valid_until",
        )
    }

    def __post_init__(self) -> None:
        identifier_fields = (
            "run_id",
            "plan_id",
            "task_id",
            "volume_id",
            "file_id",
            "export_name",
            "export_id",
        )
        for field in identifier_fields:
            _identifier(getattr(self, field), field)
        validate_handle(self.target_fh_b64)
        _integer(self.offset, "offset")
        _integer(self.length, "length", minimum=1, maximum=MAX_TASK_BYTES)
        _integer(self.max_bytes, "max_bytes", minimum=1, maximum=MAX_TASK_BYTES)
        if self.max_bytes != self.length:
            raise ValueError("max_bytes must equal length because retries are disabled")
        if not self.created_at < self.deadline <= self.target_valid_until:
            raise ValueError("task times must satisfy created_at < deadline <= target_valid_until")
        if (self.target_valid_until - self.created_at).total_seconds() > MAX_TTL_SECONDS:
            raise ValueError("task TTL cannot exceed 300 seconds")

    def model_dump(self, *, mode: str = "python") -> dict[str, Any]:
        del mode
        return {
            "schema_version": self.schema_version,
            "run_id": self.run_id,
            "plan_id": self.plan_id,
            "task_id": self.task_id,
            "volume_id": self.volume_id,
            "file_id": self.file_id,
            "export_name": self.export_name,
            "export_id": self.export_id,
            "target_fh_b64": self.target_fh_b64,
            "offset": self.offset,
            "length": self.length,
            "max_bytes": self.max_bytes,
            "created_at": format_utc_z(self.created_at),
            "deadline": format_utc_z(self.deadline),
            "target_valid_until": format_utc_z(self.target_valid_until),
        }


@dataclass(frozen=True)
class WarmerTerminal:
    type: Literal["result"]
    status: Literal["completed", "partial", "error", "timeout", "cancelled"]
    reason: str
    bytes_requested: int
    bytes_read: int
    bytes_read_is_lower_bound: bool
    execution_started: bool
    plan_ref: str
    task_ref: str

    model_fields: ClassVar[dict[str, None]] = {
        key: None
        for key in (
            "type",
            "status",
            "reason",
            "bytes_requested",
            "bytes_read",
            "bytes_read_is_lower_bound",
            "execution_started",
            "plan_ref",
            "task_ref",
        )
    }

    @classmethod
    def model_validate(
        cls,
        raw: object,
        *,
        persisted: bool = False,
    ) -> WarmerTerminal:
        fields = set(cls.model_fields)
        if not persisted:
            fields.remove("execution_started")
        value = _exact(raw, fields, "Warmer terminal")
        if value["type"] != "result":
            raise ValueError("Warmer terminal type must be result")
        if value["status"] not in {"completed", "partial", "error", "timeout", "cancelled"}:
            raise ValueError("unknown Warmer terminal status")
        reason = value["reason"]
        if not isinstance(reason, str) or not 1 <= len(reason) <= 256:
            raise ValueError("Warmer reason must contain 1..256 characters")
        lower_bound = value["bytes_read_is_lower_bound"]
        if not isinstance(lower_bound, bool):
            raise ValueError("bytes_read_is_lower_bound must be boolean")
        execution_started = value.get("execution_started", True)
        if not isinstance(execution_started, bool):
            raise ValueError("execution_started must be boolean")
        terminal = cls(
            type="result",
            status=value["status"],
            reason=reason,
            bytes_requested=_integer(value["bytes_requested"], "bytes_requested"),
            bytes_read=_integer(value["bytes_read"], "bytes_read"),
            bytes_read_is_lower_bound=lower_bound,
            execution_started=execution_started,
            plan_ref=_identifier(value["plan_ref"], "plan_ref"),
            task_ref=_identifier(value["task_ref"], "task_ref"),
        )
        if terminal.bytes_read > terminal.bytes_requested:
            raise ValueError("bytes_read cannot exceed bytes_requested")
        if terminal.status == "completed" and (
            terminal.bytes_read != terminal.bytes_requested
            or lower_bound
        ):
            raise ValueError("completed requires exact requested bytes")
        if not terminal.execution_started and (
            terminal.status not in {"error", "timeout", "cancelled"}
            or terminal.bytes_read != 0
            or terminal.bytes_read_is_lower_bound
        ):
            raise ValueError(
                "non-executed terminal requires zero exact bytes and a failure status"
            )
        return terminal

    def model_dump(self, *, mode: str = "python") -> dict[str, Any]:
        del mode
        return {
            "type": self.type,
            "status": self.status,
            "reason": self.reason,
            "bytes_requested": self.bytes_requested,
            "bytes_read": self.bytes_read,
            "bytes_read_is_lower_bound": self.bytes_read_is_lower_bound,
            "execution_started": self.execution_started,
            "plan_ref": self.plan_ref,
            "task_ref": self.task_ref,
        }


@dataclass(frozen=True)
class ResultRecord:
    run_id: str
    plan_id: str
    task_id: str
    status: Literal["completed", "partial", "skipped", "failed", "cancelled"]
    reason: str
    bytes_requested: int
    bytes_read: int
    bytes_read_is_lower_bound: bool
    started_at: datetime
    finished_at: datetime
    warmer_terminal: WarmerTerminal | None = None
    schema_version: Literal[2] = SCHEMA_VERSION

    model_fields: ClassVar[dict[str, None]] = {
        key: None
        for key in (
            "schema_version",
            "run_id",
            "plan_id",
            "task_id",
            "status",
            "reason",
            "bytes_requested",
            "bytes_read",
            "bytes_read_is_lower_bound",
            "started_at",
            "finished_at",
            "warmer_terminal",
        )
    }

    def __post_init__(self) -> None:
        for field in ("run_id", "plan_id", "task_id"):
            _identifier(getattr(self, field), field)
        if self.status not in {"completed", "partial", "skipped", "failed", "cancelled"}:
            raise ValueError("unknown dispatcher result status")
        if not isinstance(self.reason, str) or not 1 <= len(self.reason) <= 256:
            raise ValueError("result reason must contain 1..256 characters")
        _integer(self.bytes_requested, "bytes_requested")
        _integer(self.bytes_read, "bytes_read")
        if not isinstance(self.bytes_read_is_lower_bound, bool):
            raise ValueError("bytes_read_is_lower_bound must be boolean")
        if self.finished_at < self.started_at:
            raise ValueError("finished_at cannot precede started_at")
        if self.bytes_read > self.bytes_requested and not self.bytes_read_is_lower_bound:
            raise ValueError("exact bytes_read cannot exceed bytes_requested")
        if (
            self.status in {"skipped", "failed"}
            and self.warmer_terminal is None
            and self.bytes_read != 0
        ):
            raise ValueError("non-executed results cannot claim bytes read")

    @classmethod
    def model_validate(cls, raw: object) -> ResultRecord:
        value = _exact(raw, set(cls.model_fields), "result")
        if value["schema_version"] != SCHEMA_VERSION:
            raise ValueError("unsupported result schema_version")
        terminal = value["warmer_terminal"]
        return cls(
            run_id=_identifier(value["run_id"], "run_id"),
            plan_id=_identifier(value["plan_id"], "plan_id"),
            task_id=_identifier(value["task_id"], "task_id"),
            status=value["status"],
            reason=value["reason"],
            bytes_requested=value["bytes_requested"],
            bytes_read=value["bytes_read"],
            bytes_read_is_lower_bound=value["bytes_read_is_lower_bound"],
            started_at=parse_utc_z(value["started_at"]),
            finished_at=parse_utc_z(value["finished_at"]),
            warmer_terminal=(
                None
                if terminal is None
                else WarmerTerminal.model_validate(
                    terminal,
                    persisted=True,
                )
            ),
        )

    def model_dump(self, *, mode: str = "python") -> dict[str, Any]:
        del mode
        return {
            "schema_version": self.schema_version,
            "run_id": self.run_id,
            "plan_id": self.plan_id,
            "task_id": self.task_id,
            "status": self.status,
            "reason": self.reason,
            "bytes_requested": self.bytes_requested,
            "bytes_read": self.bytes_read,
            "bytes_read_is_lower_bound": self.bytes_read_is_lower_bound,
            "started_at": format_utc_z(self.started_at),
            "finished_at": format_utc_z(self.finished_at),
            "warmer_terminal": (
                None if self.warmer_terminal is None else self.warmer_terminal.model_dump()
            ),
        }


def task_from_plan(plan: Plan, item: PlanItem) -> WarmerTask:
    return WarmerTask(
        run_id=plan.run_id,
        plan_id=plan.plan_id,
        task_id=item.task_id,
        volume_id=item.volume_id,
        file_id=item.file_id,
        export_name=item.export_name,
        export_id=item.export_id,
        target_fh_b64=item.target_fh_b64,
        offset=item.offset,
        length=item.length,
        max_bytes=item.max_bytes,
        created_at=plan.created_at,
        deadline=plan.deadline,
        target_valid_until=item.target_valid_until,
    )
