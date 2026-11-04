"""Executable successful-READ semantic contract for the hackathon MVP."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

SEMANTIC_VIEW_ID = "read-demand-v1"

READ_DEMAND_CONTRACT = {
    "semantic_view_id": SEMANTIC_VIEW_ID,
    "protocol": "NFSv4.1",
    "operation": "READ",
    "success": "result=ok and 0 <= bytes_read <= bytes_requested",
    "identity_scope": ["volume_id", "file_id"],
    "time": {
        "event_time": "UTC operation completion time",
        "known_at": "UTC durable availability time; features order by known_at",
    },
    "units": {
        "offset": "bytes",
        "bytes_requested": "bytes",
        "bytes_read": "bytes returned",
        "file_size": "bytes",
    },
    "metadata": {
        "dir_id": "nullable; verified catalog join only",
        "file_size": "nullable; verified producer or catalog join only",
        "rename_replacement": "unresolved in production; stale identity blocks execution",
    },
    "provenance": {
        "origin": ["foreground", "warmer", "synthetic", "unknown"],
        "demand_class": [
            "foreground",
            "controlled_demand",
            "fixture_demand",
            "warmer",
            "unknown",
        ],
        "real_demand": "foreground + trusted origin + complete coverage only",
        "controlled_demand": "synthetic plumbing evidence only",
    },
    "loss": {
        "complete": "eligible for negative labels after the full horizon",
        "incomplete": "affected labels are unknown",
        "unknown": "affected labels are unknown or package activation blocks",
    },
    "forbidden": [
        "OPEN/CLOSE demand",
        "read-to-open coalescing",
        "handle duration",
        "application session inference",
        "WRITE/CREATE/REMOVE",
        "NFSv3/SMB/S3/READ_PLUS",
        "cold_open labels",
    ],
}


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


SEMANTIC_VIEW_SHA256 = hashlib.sha256(
    canonical_json(READ_DEMAND_CONTRACT).encode()
).hexdigest()


class ReadDemandEvent(BaseModel):
    """Canonical successful READ row; lifecycle and synthetic truth are absent."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal[1] = 1
    event_id: str = Field(min_length=1)
    event_time: datetime
    known_at: datetime
    volume_id: str = Field(min_length=1)
    file_id: str = Field(min_length=1)
    dir_id: str | None = None
    offset: int = Field(ge=0)
    bytes_requested: int = Field(ge=0)
    bytes_read: int = Field(ge=0)
    file_size: int | None = Field(default=None, ge=0)
    origin: Literal["foreground", "warmer", "synthetic", "unknown"]
    demand_class: Literal[
        "foreground", "controlled_demand", "fixture_demand", "warmer", "unknown"
    ]
    origin_trust: Literal["trusted", "fixture", "client_asserted", "unknown"]
    coverage_status: Literal["complete", "incomplete", "unknown"]
    metadata_status: Literal["verified", "fixture", "missing"]
    data_kind: Literal["real", "synthetic"]
    protocol: Literal["NFSv4.1"] = "NFSv4.1"
    operation: Literal["read"] = "read"

    @field_validator("event_time", "known_at")
    @classmethod
    def utc_time(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() != timedelta(0):
            raise ValueError("timestamps must be timezone-aware UTC")
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def valid_read(self):
        if self.bytes_read > self.bytes_requested:
            raise ValueError("bytes_read exceeds bytes_requested")
        if self.file_size is not None and self.offset + self.bytes_read > self.file_size:
            raise ValueError("returned range exceeds file_size")
        if (self.dir_id is None or self.file_size is None) != (
            self.metadata_status == "missing"
        ):
            raise ValueError("metadata_status must reflect nullable metadata")
        if self.demand_class == "foreground" and self.origin != "foreground":
            raise ValueError("foreground demand requires foreground origin")
        return self

    @property
    def eligible_real_demand(self) -> bool:
        return (
            self.data_kind == "real"
            and self.origin == "foreground"
            and self.demand_class == "foreground"
            and self.origin_trust == "trusted"
            and self.coverage_status == "complete"
        )


def _event_time(value: Any) -> datetime:
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if not isinstance(value, datetime):
        raise ValueError("timestamp must be a datetime")
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError("timestamp must be UTC")
    return value.astimezone(UTC)


def project_datagen_read(
    event: dict[str, Any],
    *,
    coverage_status: Literal["complete", "incomplete", "unknown"] = "complete",
) -> ReadDemandEvent:
    """Project one generic datagen row without exposing generator lifecycle truth."""
    if event.get("operation") != "read" or event.get("result") != "ok":
        raise ValueError("read-demand-v1 accepts successful READ rows only")
    origin = event.get("origin") or "unknown"
    demand_class = {
        "foreground": "fixture_demand",
        "warmer": "warmer",
        "synthetic": "fixture_demand",
        "unknown": "unknown",
    }.get(origin, "unknown")
    dir_id = event.get("dir_id")
    file_size = event.get("file_size")
    return ReadDemandEvent(
        event_id=event["event_id"],
        event_time=_event_time(event["event_time"]),
        known_at=_event_time(event["ingest_time"]),
        volume_id=event["volume_id"],
        file_id=event["file_id"],
        dir_id=dir_id,
        offset=event["offset"],
        bytes_requested=event["bytes_requested"],
        bytes_read=event["bytes_read"],
        file_size=file_size,
        origin=origin,
        demand_class=demand_class,
        origin_trust="fixture",
        coverage_status=coverage_status,
        metadata_status="fixture" if dir_id is not None and file_size is not None else "missing",
        data_kind="synthetic",
    )


def project_chimera_read(
    event: Any,
    *,
    durable_at: datetime,
    dir_id: str | None,
    file_size: int | None,
    coverage_status: Literal["complete", "incomplete", "unknown"] = "complete",
) -> ReadDemandEvent:
    """Project one validated raw nfs4_read_v2 event without lifecycle invention."""
    if event.outcome != "success" or event.nfs_status != 0:
        raise ValueError("read-demand-v1 accepts successful READ rows only")
    demand_class = {
        "controlled_demand": "controlled_demand",
        "warmer": "warmer",
        "synthetic": "fixture_demand",
        "unknown": "unknown",
    }[event.origin_claim]
    return ReadDemandEvent(
        event_id=event.local_id,
        event_time=datetime.fromtimestamp(event.event_ns / 1_000_000_000, tz=UTC),
        known_at=_event_time(durable_at),
        volume_id=event.volume_id,
        file_id=event.file_id,
        dir_id=dir_id,
        offset=event.offset,
        bytes_requested=event.requested_count,
        bytes_read=event.returned_bytes,
        file_size=file_size,
        origin="synthetic",
        demand_class=demand_class,
        origin_trust="client_asserted",
        coverage_status=coverage_status,
        metadata_status="fixture" if dir_id is not None and file_size is not None else "missing",
        data_kind="synthetic",
    )


def semantic_payload(row: ReadDemandEvent) -> dict[str, Any]:
    """Comparable intent facts, excluding source delivery/provenance evidence."""
    return {
        key: value
        for key, value in row.model_dump(mode="json").items()
        if key
        not in {
            "event_id",
            "known_at",
            "origin",
            "demand_class",
            "origin_trust",
            "data_kind",
        }
    }

