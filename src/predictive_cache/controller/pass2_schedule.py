"""Strict, handle-free Pass-2 prediction schedule contract."""

from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from types import MappingProxyType
from typing import Any, Literal, Mapping

from predictive_cache.dispatcher.schema import (
    ID_PATTERN,
    MAX_TASK_BYTES,
    MODEL_VERSION_PATTERN,
    parse_utc_z,
)
from predictive_cache.dispatcher.securefs import SecureFileError, loads_strict_json
from predictive_cache.ml.hashing import canonical_json

SCHEMA_VERSION = 1
MAX_SCHEDULE_BYTES = 16 * 2**20
MAX_INTEGER = 2**63 - 1
SCORE_QUANTIZATION = "12-significant-digits"
ORDERING = ("score_desc", "file_id_asc", "volume_id_asc")
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
SCHEDULE_ID_PATTERN = re.compile(r"^schedule-[0-9a-f]{24}$")
EXECUTION_IDENTITY_KEYS = (
    "target_fh_b64",
    "target_fh",
    "export_id",
    "export_name",
    "path",
    "endpoint",
    "host",
)


class ScheduleError(ValueError):
    """A schedule violated the frozen Pass-2 producer contract."""


def _exact(value: object, fields: set[str], record: str) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != fields:
        raise ScheduleError(f"{record} fields are invalid")
    return value


def _identifier(value: object, field_name: str) -> str:
    if not isinstance(value, str) or ID_PATTERN.fullmatch(value) is None:
        raise ScheduleError(f"{field_name} is invalid")
    return value


def _model_version(value: object) -> str:
    if not isinstance(value, str) or MODEL_VERSION_PATTERN.fullmatch(value) is None:
        raise ScheduleError("package.model_version is invalid")
    return value


def _sha256(value: object, field_name: str) -> str:
    if not isinstance(value, str) or SHA256_PATTERN.fullmatch(value) is None:
        raise ScheduleError(f"{field_name} must be a lowercase SHA-256")
    return value


def _integer(
    value: object,
    field_name: str,
    *,
    minimum: int = 0,
    maximum: int = MAX_INTEGER,
) -> int:
    if type(value) is not int or not minimum <= value <= maximum:
        raise ScheduleError(
            f"{field_name} must be an integer in {minimum}..{maximum}"
        )
    return value


def _finite(value: object, field_name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ScheduleError(f"{field_name} must be numeric")
    result = float(value)
    if not math.isfinite(result):
        raise ScheduleError(f"{field_name} must be finite")
    return result


def _object(value: object, field_name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ScheduleError(f"{field_name} must be an object")
    return value


def _reject_execution_identity(value: object) -> None:
    pending = [value]
    while pending:
        current = pending.pop()
        if isinstance(current, dict):
            for key, child in current.items():
                lowered = key.lower()
                if any(token in lowered for token in EXECUTION_IDENTITY_KEYS):
                    raise ScheduleError(
                        "schedule must not contain execution identity fields"
                    )
                pending.append(child)
        elif isinstance(current, list):
            pending.extend(current)


def _readonly(value: dict[str, Any]) -> Mapping[str, Any]:
    return MappingProxyType(dict(value))


@dataclass(frozen=True)
class LogicalRange:
    offset: int
    length: int
    unit: Literal["bytes"] = "bytes"

    def model_dump(self, *, mode: str = "python") -> dict[str, Any]:
        del mode
        return {"offset": self.offset, "length": self.length, "unit": self.unit}


@dataclass(frozen=True)
class ScheduleItem:
    prediction_id: str
    rank: int
    volume_id: str
    file_id: str
    prediction_cutoff: datetime
    warm_offset_s: int
    score: float
    reasons: tuple[str, ...]
    logical_range: LogicalRange

    def model_dump(self, *, mode: str = "python") -> dict[str, Any]:
        del mode
        return {
            "prediction_id": self.prediction_id,
            "rank": self.rank,
            "volume_id": self.volume_id,
            "file_id": self.file_id,
            "prediction_cutoff": self.prediction_cutoff.isoformat().replace(
                "+00:00", "Z"
            ),
            "warm_offset_s": self.warm_offset_s,
            "score": self.score,
            "reasons": list(self.reasons),
            "logical_range": self.logical_range.model_dump(),
        }


@dataclass(frozen=True)
class Schedule:
    schema_version: Literal[1]
    schedule_id: str
    semantic_view: Mapping[str, Any]
    snapshot: Mapping[str, Any]
    package: Mapping[str, Any]
    policy: Mapping[str, Any]
    policy_sha256: str
    time_basis: Mapping[str, Any]
    ordering: tuple[str, str, str]
    budget: Mapping[str, int]
    rejection_counts: Mapping[str, int]
    admitted_items: int
    requested_bytes: int
    items: tuple[ScheduleItem, ...]
    _canonical_json: str = field(repr=False, compare=False)

    def model_dump(self, *, mode: str = "python") -> dict[str, Any]:
        del mode
        return json.loads(self._canonical_json)

    @property
    def raw_canonical_json(self) -> str:
        return self._canonical_json


# Compatibility name used by the runtime bridge while ``Schedule`` remains
# the public contract name.
Pass2Schedule = Schedule


def _validate_semantic_view(raw: object) -> Mapping[str, Any]:
    value = _exact(
        raw,
        {
            "id",
            "sha256",
            "feature_version",
            "feature_schema_sha256",
            "serving_contract_sha256",
        },
        "semantic_view",
    )
    _identifier(value["id"], "semantic_view.id")
    _sha256(value["sha256"], "semantic_view.sha256")
    _identifier(value["feature_version"], "semantic_view.feature_version")
    _sha256(
        value["feature_schema_sha256"],
        "semantic_view.feature_schema_sha256",
    )
    _sha256(
        value["serving_contract_sha256"],
        "semantic_view.serving_contract_sha256",
    )
    return _readonly(value)


def _validate_snapshot(raw: object) -> tuple[Mapping[str, Any], str]:
    value = _exact(
        raw,
        {
            "dataset_id",
            "manifest_sha256",
            "files_sha256",
            "candidate_profile",
            "runtime_capabilities",
            "scoring_rows_sha256",
            "prediction_cutoff",
        },
        "snapshot",
    )
    _identifier(value["dataset_id"], "snapshot.dataset_id")
    _sha256(value["manifest_sha256"], "snapshot.manifest_sha256")
    files = _object(value["files_sha256"], "snapshot.files_sha256")
    if not files:
        raise ScheduleError("snapshot.files_sha256 must not be empty")
    for path, digest in files.items():
        if not isinstance(path, str) or not path or path.startswith("/"):
            raise ScheduleError("snapshot file path is invalid")
        _sha256(digest, f"snapshot.files_sha256.{path}")
    _identifier(value["candidate_profile"], "snapshot.candidate_profile")
    _object(value["runtime_capabilities"], "snapshot.runtime_capabilities")
    _sha256(value["scoring_rows_sha256"], "snapshot.scoring_rows_sha256")
    parse_utc_z(value["prediction_cutoff"])
    return _readonly(value), value["prediction_cutoff"]


def _validate_package(raw: object) -> Mapping[str, Any]:
    value = _exact(
        raw,
        {
            "package_id",
            "artifact_sha256",
            "package_manifest_sha256",
            "model_type",
            "model_version",
            "trained",
            "trained_evidence",
            "data_kind",
        },
        "package",
    )
    _identifier(value["package_id"], "package.package_id")
    _sha256(value["artifact_sha256"], "package.artifact_sha256")
    _sha256(
        value["package_manifest_sha256"],
        "package.package_manifest_sha256",
    )
    _identifier(value["model_type"], "package.model_type")
    _model_version(value["model_version"])
    if type(value["trained"]) is not bool:
        raise ScheduleError("package.trained must be boolean")
    _identifier(value["trained_evidence"], "package.trained_evidence")
    _identifier(value["data_kind"], "package.data_kind")
    return _readonly(value)


def _validate_time_basis(raw: object) -> Mapping[str, Any]:
    value = _exact(
        raw,
        {
            "kind",
            "warm_start_offset_s",
            "warm_spacing_s",
            "score_quantization",
        },
        "time_basis",
    )
    if value["kind"] != "pass2_start_relative":
        raise ScheduleError("time_basis.kind is invalid")
    if value["score_quantization"] != SCORE_QUANTIZATION:
        raise ScheduleError("time_basis.score_quantization is invalid")
    _integer(value["warm_start_offset_s"], "time_basis.warm_start_offset_s")
    _integer(value["warm_spacing_s"], "time_basis.warm_spacing_s")
    return _readonly(value)


def _validate_budget(raw: object) -> Mapping[str, int]:
    value = _exact(
        raw,
        {"max_items", "max_bytes", "range_cap_bytes"},
        "budget",
    )
    _integer(value["max_items"], "budget.max_items")
    _integer(value["max_bytes"], "budget.max_bytes")
    _integer(
        value["range_cap_bytes"],
        "budget.range_cap_bytes",
        minimum=1,
        maximum=MAX_TASK_BYTES,
    )
    return MappingProxyType(dict(value))


def _validate_rejection_counts(raw: object) -> Mapping[str, int]:
    value = _object(raw, "rejection_counts")
    counts: dict[str, int] = {}
    for reason, count in value.items():
        _identifier(reason, "rejection_counts reason")
        counts[reason] = _integer(count, f"rejection_counts.{reason}")
    return MappingProxyType(counts)


def _validate_item(
    raw: object,
    *,
    expected_rank: int,
    prediction_cutoff: str,
    warm_start_offset_s: int,
    warm_spacing_s: int,
    range_cap_bytes: int,
) -> ScheduleItem:
    value = _exact(
        raw,
        {
            "prediction_id",
            "rank",
            "volume_id",
            "file_id",
            "prediction_cutoff",
            "warm_offset_s",
            "score",
            "reasons",
            "logical_range",
        },
        "schedule item",
    )
    prediction_id = _identifier(value["prediction_id"], "prediction_id")
    rank = _integer(value["rank"], "rank", minimum=1)
    if rank != expected_rank:
        raise ScheduleError("item ranks must be contiguous starting at 1")
    volume_id = _identifier(value["volume_id"], "volume_id")
    file_id = _identifier(value["file_id"], "file_id")
    parse_utc_z(value["prediction_cutoff"])
    if value["prediction_cutoff"] != prediction_cutoff:
        raise ScheduleError("item prediction_cutoff differs from snapshot")
    warm_offset_s = _integer(value["warm_offset_s"], "warm_offset_s")
    expected_offset = warm_start_offset_s + (rank - 1) * warm_spacing_s
    if expected_offset > MAX_INTEGER or warm_offset_s != expected_offset:
        raise ScheduleError("item warm_offset_s violates the due-time formula")
    score = _finite(value["score"], "score")
    reasons = value["reasons"]
    if not isinstance(reasons, list) or len(reasons) > 62 or any(
        not isinstance(reason, str)
        or not reason
        or len(reason.encode("utf-8")) > 512
        for reason in reasons
    ):
        raise ScheduleError(
            "reasons must contain at most 62 non-empty strings of 512 UTF-8 bytes"
        )
    range_value = _exact(
        value["logical_range"],
        {"offset", "length", "unit"},
        "logical_range",
    )
    if range_value["unit"] != "bytes":
        raise ScheduleError("logical_range.unit must be bytes")
    offset = _integer(range_value["offset"], "logical_range.offset")
    length = _integer(
        range_value["length"],
        "logical_range.length",
        minimum=1,
        maximum=range_cap_bytes,
    )
    if offset + length > MAX_INTEGER:
        raise ScheduleError("logical range exceeds the supported range")
    return ScheduleItem(
        prediction_id=prediction_id,
        rank=rank,
        volume_id=volume_id,
        file_id=file_id,
        prediction_cutoff=parse_utc_z(value["prediction_cutoff"]),
        warm_offset_s=warm_offset_s,
        score=score,
        reasons=tuple(reasons),
        logical_range=LogicalRange(offset=offset, length=length),
    )


def _parse_schedule(raw: object) -> Schedule:
    value = _exact(
        raw,
        {
            "schema_version",
            "schedule_id",
            "semantic_view",
            "snapshot",
            "package",
            "policy",
            "policy_sha256",
            "time_basis",
            "ordering",
            "budget",
            "rejection_counts",
            "admitted_items",
            "requested_bytes",
            "items",
        },
        "schedule",
    )
    _reject_execution_identity(value)
    if type(value["schema_version"]) is not int or value["schema_version"] != SCHEMA_VERSION:
        raise ScheduleError("unsupported schedule schema_version")
    schedule_id = value["schedule_id"]
    if not isinstance(schedule_id, str) or SCHEDULE_ID_PATTERN.fullmatch(schedule_id) is None:
        raise ScheduleError("schedule_id is invalid")
    identity_document = dict(value)
    del identity_document["schedule_id"]
    try:
        identity_json = canonical_json(identity_document)
    except ValueError:
        raise ScheduleError("schedule contains noncanonical JSON numbers") from None
    expected_schedule_id = "schedule-" + hashlib.sha256(
        identity_json.encode("utf-8")
    ).hexdigest()[:24]
    if schedule_id != expected_schedule_id:
        raise ScheduleError("schedule_id does not match the canonical document")

    semantic_view = _validate_semantic_view(value["semantic_view"])
    snapshot, prediction_cutoff = _validate_snapshot(value["snapshot"])
    package = _validate_package(value["package"])
    policy = _object(value["policy"], "policy")
    policy_sha256 = _sha256(value["policy_sha256"], "policy_sha256")
    if policy_sha256 != hashlib.sha256(
        canonical_json(policy).encode("utf-8")
    ).hexdigest():
        raise ScheduleError("policy_sha256 does not match policy")
    time_basis = _validate_time_basis(value["time_basis"])
    if value["ordering"] != list(ORDERING):
        raise ScheduleError("ordering is invalid")
    budget = _validate_budget(value["budget"])
    for policy_field, budget_field in (
        ("max_items", "max_items"),
        ("max_bytes", "max_bytes"),
        ("range_cap", "range_cap_bytes"),
    ):
        policy_value = _integer(
            policy.get(policy_field),
            f"policy.{policy_field}",
            minimum=1 if policy_field == "range_cap" else 0,
            maximum=MAX_TASK_BYTES if policy_field == "range_cap" else MAX_INTEGER,
        )
        if policy_value != budget[budget_field]:
            raise ScheduleError("policy budget differs from schedule budget")
    rejection_counts = _validate_rejection_counts(value["rejection_counts"])
    raw_items = value["items"]
    if not isinstance(raw_items, list):
        raise ScheduleError("items must be a list")
    admitted_items = _integer(value["admitted_items"], "admitted_items")
    if admitted_items != len(raw_items) or admitted_items > budget["max_items"]:
        raise ScheduleError("admitted_items differs from items or exceeds max_items")
    items = tuple(
        _validate_item(
            raw_item,
            expected_rank=index,
            prediction_cutoff=prediction_cutoff,
            warm_start_offset_s=time_basis["warm_start_offset_s"],
            warm_spacing_s=time_basis["warm_spacing_s"],
            range_cap_bytes=budget["range_cap_bytes"],
        )
        for index, raw_item in enumerate(raw_items, start=1)
    )
    prediction_ids = {item.prediction_id for item in items}
    logical_ids = {(item.volume_id, item.file_id) for item in items}
    if len(prediction_ids) != len(items):
        raise ScheduleError("prediction_id values must be unique")
    if len(logical_ids) != len(items):
        raise ScheduleError("logical file identities must be unique")
    if list(items) != sorted(
        items,
        key=lambda item: (-item.score, item.file_id, item.volume_id),
    ):
        raise ScheduleError("items violate the declared ordering")
    requested_bytes = _integer(value["requested_bytes"], "requested_bytes")
    if (
        requested_bytes != sum(item.logical_range.length for item in items)
        or requested_bytes > budget["max_bytes"]
    ):
        raise ScheduleError("requested_bytes differs from ranges or exceeds max_bytes")
    return Schedule(
        schema_version=SCHEMA_VERSION,
        schedule_id=schedule_id,
        semantic_view=semantic_view,
        snapshot=snapshot,
        package=package,
        policy=_readonly(policy),
        policy_sha256=policy_sha256,
        time_basis=time_basis,
        ordering=ORDERING,
        budget=budget,
        rejection_counts=rejection_counts,
        admitted_items=admitted_items,
        requested_bytes=requested_bytes,
        items=items,
        _canonical_json=canonical_json(value),
    )


def load_schedule(path: Path) -> tuple[Schedule, str]:
    """Load a complete bounded JSON schedule and return its exact file digest."""
    try:
        with Path(path).open("rb") as stream:
            data = stream.read(MAX_SCHEDULE_BYTES + 1)
    except OSError:
        raise ScheduleError("unable to read schedule file") from None
    if len(data) > MAX_SCHEDULE_BYTES:
        raise ScheduleError("schedule file exceeds 16 MiB")
    try:
        text = data.decode("utf-8")
        raw = loads_strict_json(text)
    except UnicodeDecodeError:
        raise ScheduleError("schedule is not complete UTF-8 JSON") from None
    except json.JSONDecodeError:
        raise ScheduleError("schedule is not complete UTF-8 JSON") from None
    except SecureFileError as exc:
        raise ScheduleError(str(exc)) from None
    return _parse_schedule(raw), hashlib.sha256(data).hexdigest()
