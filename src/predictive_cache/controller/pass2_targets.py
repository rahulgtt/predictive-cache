"""Owner-controlled logical identity to Dispatcher target bindings for Pass 2."""

from __future__ import annotations

import hashlib
import json
import os
import re
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from types import MappingProxyType
from typing import Any, Literal, Mapping

from predictive_cache.dispatcher.schema import (
    ID_PATTERN,
    MAX_TASK_BYTES,
    format_utc_z,
    parse_utc_z,
    validate_handle,
)
from predictive_cache.dispatcher.securefs import (
    SecureFileError,
    loads_strict_json,
    read_secure_bytes,
    require_secure_directory,
)
from predictive_cache.ml.hashing import canonical_json

SCHEMA_VERSION = 1
AUTHORIZATION = "approved-pass2-treatment"
MAX_TARGET_MAP_BYTES = 16 * 2**20
MAX_INTEGER = 2**63 - 1
SCHEDULE_ID_PATTERN = re.compile(r"^schedule-[0-9a-f]{24}$")


class TargetMapError(ValueError):
    """A target map violated the owner-controlled Pass-2 contract."""


def _exact(value: object, fields: set[str], record: str) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != fields:
        raise TargetMapError(f"{record} fields are invalid")
    return value


def _identifier(value: object, field_name: str) -> str:
    if not isinstance(value, str) or ID_PATTERN.fullmatch(value) is None:
        raise TargetMapError(f"{field_name} is invalid")
    return value


def _schedule_id(value: object, field_name: str) -> str:
    if not isinstance(value, str) or SCHEDULE_ID_PATTERN.fullmatch(value) is None:
        raise TargetMapError(f"{field_name} is invalid")
    return value


def _integer(
    value: object,
    field_name: str,
    *,
    minimum: int = 0,
    maximum: int = MAX_INTEGER,
) -> int:
    if type(value) is not int or not minimum <= value <= maximum:
        raise TargetMapError(
            f"{field_name} must be an integer in {minimum}..{maximum}"
        )
    return value


@dataclass(frozen=True)
class ApprovedTarget:
    approved: bool
    volume_id: str
    file_id: str
    export_name: str
    export_id: str
    target_fh_b64: str = field(repr=False)
    target_valid_until: datetime
    offset: int
    length: int

    def model_dump(self, *, mode: str = "python") -> dict[str, Any]:
        del mode
        return {
            "approved": self.approved,
            "volume_id": self.volume_id,
            "file_id": self.file_id,
            "export_name": self.export_name,
            "export_id": self.export_id,
            "target_fh_b64": self.target_fh_b64,
            "target_valid_until": format_utc_z(self.target_valid_until),
            "offset": self.offset,
            "length": self.length,
        }


@dataclass(frozen=True)
class TargetMap:
    schema_version: Literal[1]
    target_map_id: str
    authorization: Literal["approved-pass2-treatment"]
    schedule_id: str
    created_at: datetime
    targets: tuple[ApprovedTarget, ...]
    _index: Mapping[tuple[str, str], ApprovedTarget] = field(
        repr=False,
        compare=False,
    )
    _canonical_json: str = field(repr=False, compare=False)

    def resolve(self, volume_id: str, file_id: str) -> ApprovedTarget | None:
        return self._index.get((volume_id, file_id))

    def model_dump(self, *, mode: str = "python") -> dict[str, Any]:
        del mode
        return json.loads(self._canonical_json)

    @property
    def raw_canonical_json(self) -> str:
        return self._canonical_json


def _validate_target(raw: object) -> ApprovedTarget:
    value = _exact(
        raw,
        {
            "approved",
            "volume_id",
            "file_id",
            "export_name",
            "export_id",
            "target_fh_b64",
            "target_valid_until",
            "offset",
            "length",
        },
        "target",
    )
    if type(value["approved"]) is not bool:
        raise TargetMapError("target.approved must be boolean")
    volume_id = _identifier(value["volume_id"], "target.volume_id")
    file_id = _identifier(value["file_id"], "target.file_id")
    export_name = _identifier(value["export_name"], "target.export_name")
    export_id = _identifier(value["export_id"], "target.export_id")
    try:
        handle = validate_handle(value["target_fh_b64"])
    except ValueError as exc:
        raise TargetMapError(str(exc)) from None
    target_valid_until = parse_utc_z(value["target_valid_until"])
    offset = _integer(value["offset"], "target.offset")
    length = _integer(
        value["length"],
        "target.length",
        minimum=1,
        maximum=MAX_TASK_BYTES,
    )
    if offset + length > MAX_INTEGER:
        raise TargetMapError("target range exceeds the supported range")
    return ApprovedTarget(
        approved=value["approved"],
        volume_id=volume_id,
        file_id=file_id,
        export_name=export_name,
        export_id=export_id,
        target_fh_b64=handle,
        target_valid_until=target_valid_until,
        offset=offset,
        length=length,
    )


def _parse_target_map(
    raw: object,
    *,
    expected_schedule_id: str,
    evaluated_at: datetime,
) -> TargetMap:
    value = _exact(
        raw,
        {
            "schema_version",
            "target_map_id",
            "authorization",
            "schedule_id",
            "created_at",
            "targets",
        },
        "target map",
    )
    if type(value["schema_version"]) is not int or value["schema_version"] != SCHEMA_VERSION:
        raise TargetMapError("unsupported target map schema_version")
    target_map_id = _identifier(value["target_map_id"], "target_map_id")
    if value["authorization"] != AUTHORIZATION:
        raise TargetMapError("target map authorization is invalid")
    schedule_id = _schedule_id(value["schedule_id"], "schedule_id")
    if schedule_id != expected_schedule_id:
        raise TargetMapError("target map schedule_id does not match expected schedule")
    created_at = parse_utc_z(value["created_at"])
    if created_at > evaluated_at:
        raise TargetMapError("target map created_at is in the future")
    raw_targets = value["targets"]
    if not isinstance(raw_targets, list):
        raise TargetMapError("targets must be a list")
    targets = tuple(_validate_target(raw_target) for raw_target in raw_targets)
    index = {(target.volume_id, target.file_id): target for target in targets}
    if len(index) != len(targets):
        raise TargetMapError("target logical identities must be unique")
    return TargetMap(
        schema_version=SCHEMA_VERSION,
        target_map_id=target_map_id,
        authorization=AUTHORIZATION,
        schedule_id=schedule_id,
        created_at=created_at,
        targets=targets,
        _index=MappingProxyType(index),
        _canonical_json=canonical_json(value),
    )


def load_target_map(
    path: Path,
    expected_schedule_id: str,
    evaluated_at: datetime,
) -> tuple[TargetMap, str]:
    """Load an owner-only target map bound to one schedule."""
    expected_schedule_id = _schedule_id(
        expected_schedule_id,
        "expected_schedule_id",
    )
    evaluated_at = parse_utc_z(evaluated_at)
    parent_fd: int | None = None
    try:
        parent_fd = require_secure_directory(Path(path).parent)
        data = read_secure_bytes(
            parent_fd,
            Path(path).name,
            max_bytes=MAX_TARGET_MAP_BYTES,
        )
    except (OSError, SecureFileError):
        raise TargetMapError("target map file security validation failed") from None
    finally:
        if parent_fd is not None:
            os.close(parent_fd)
    try:
        text = data.decode("utf-8")
        raw = loads_strict_json(text)
    except UnicodeDecodeError:
        raise TargetMapError("target map is not complete UTF-8 JSON") from None
    except json.JSONDecodeError:
        raise TargetMapError("target map is not complete UTF-8 JSON") from None
    except SecureFileError as exc:
        raise TargetMapError(str(exc)) from None
    target_map = _parse_target_map(
        raw,
        expected_schedule_id=expected_schedule_id,
        evaluated_at=evaluated_at,
    )
    return target_map, hashlib.sha256(data).hexdigest()
