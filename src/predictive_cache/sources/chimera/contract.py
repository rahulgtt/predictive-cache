"""Strict contracts for Chimera v2 and controlled collection evidence."""

from __future__ import annotations

import base64
import hashlib
import json
import os
import re
import stat
import uuid
from datetime import datetime, timedelta
from pathlib import Path, PurePosixPath
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

HOUR_NS = 3_600_000_000_000
Sha256 = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
VolumeId = Annotated[str, Field(pattern=r"^[0-9a-f]{32}$")]
SafeId = Annotated[str, Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")]
DecimalU64 = Annotated[str, Field(pattern=r"^(?:0|[1-9][0-9]{0,19})$")]


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


def utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError("timestamp must be UTC and timezone-aware")
    return value


def canonical_uuid(value: str) -> str:
    try:
        parsed = uuid.UUID(value)
    except ValueError as error:
        raise ValueError("expected canonical UUID") from error
    if str(parsed) != value:
        raise ValueError("expected canonical lowercase UUID")
    return value


def safe_relative(value: str) -> str:
    path = PurePosixPath(value)
    if (
        not value
        or path.is_absolute()
        or ".." in path.parts
        or "\\" in value
        or str(path) != value
    ):
        raise ValueError("expected a safe relative POSIX path")
    return value


def decode_base64url(value: str, field: str) -> bytes:
    if not re.fullmatch(r"[A-Za-z0-9_-]*", value):
        raise ValueError(f"{field} must be unpadded base64url")
    try:
        decoded = base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))
    except ValueError as error:
        raise ValueError(f"{field} is invalid base64url") from error
    canonical = base64.urlsafe_b64encode(decoded).decode().rstrip("=")
    if canonical != value:
        raise ValueError(f"{field} must use canonical unpadded base64url")
    return decoded


class ChimeraEvent(StrictModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)

    schema_name: Literal["nfs4_read_v2"] = Field(alias="schema")
    local_id: str
    producer_epoch: str
    producer_sequence: DecimalU64
    event_time_ns: DecimalU64
    sink_time_ns: DecimalU64
    target_created_ns: DecimalU64
    volume_id: VolumeId
    file_id: Sha256
    identity_valid: bool
    export_name_b64: str
    export_id: int = Field(ge=0, le=65535)
    target_fh: str
    operation_id: Sha256
    client_ref: Sha256
    session_ref: Sha256 | Literal[""]
    compound_op_index: int = Field(ge=0)
    slot_id: int = Field(ge=0)
    sequence_id: int = Field(ge=0)
    compound_tag_b64: str
    compound_tag_original_length: int = Field(ge=0)
    compound_tag_truncated: bool
    compound_tag_id: Sha256
    origin_claim: Literal["controlled_demand", "warmer", "synthetic", "unknown"]
    origin_trust: Literal["client_asserted"]
    requested_offset: DecimalU64
    requested_count: int = Field(ge=0, le=2**32 - 1)
    returned_bytes: int = Field(ge=0, le=2**32 - 1)
    nfs_status: int = Field(ge=0, le=2**32 - 1)
    outcome: Literal["success", "error"]
    eof: bool
    file_size_valid: bool
    file_size: DecimalU64 | None

    _local_id = field_validator("local_id")(canonical_uuid)
    _producer_epoch = field_validator("producer_epoch")(canonical_uuid)

    @model_validator(mode="after")
    def valid_event(self):
        if (self.nfs_status == 0) != (self.outcome == "success"):
            raise ValueError("outcome must agree with nfs_status")
        if self.returned_bytes > self.requested_count:
            raise ValueError("returned_bytes exceeds requested_count")
        if self.nfs_status != 0 and (self.returned_bytes != 0 or self.eof):
            raise ValueError("failed reads must return zero bytes with eof=false")
        if self.file_size_valid != (self.file_size is not None):
            raise ValueError("file_size_valid must agree with file_size")
        if not self.identity_valid:
            raise ValueError("identity_valid=false cannot be imported")
        export_name = decode_base64url(self.export_name_b64, "export_name_b64")
        if not export_name or len(export_name) > 64:
            raise ValueError("export_name_b64 must encode a nonempty bounded name")
        decode_base64url(self.target_fh, "target_fh")
        tag = decode_base64url(self.compound_tag_b64, "compound_tag_b64")
        if self.compound_tag_original_length < len(tag):
            raise ValueError("compound tag length is inconsistent")
        if not self.compound_tag_truncated and self.compound_tag_original_length != len(tag):
            raise ValueError("untruncated compound tag length is inconsistent")
        return self

    @property
    def sequence(self) -> int:
        return int(self.producer_sequence)

    @property
    def event_ns(self) -> int:
        return int(self.event_time_ns)

    @property
    def sink_ns(self) -> int:
        return int(self.sink_time_ns)

    @property
    def offset(self) -> int:
        return int(self.requested_offset)

    @property
    def source_file_size(self) -> int | None:
        return int(self.file_size) if self.file_size is not None else None

    @property
    def export_name(self) -> str:
        return decode_base64url(self.export_name_b64, "export_name_b64").decode("utf-8")

    @property
    def compound_tag(self) -> str:
        return decode_base64url(self.compound_tag_b64, "compound_tag_b64").decode("utf-8")


class SoftwareVersions(StrictModel):
    predictive_cache_revision: str
    chimera_image: str
    chimera_image_sha256: Sha256
    chimera_revision: str
    cufs_image: str
    cufs_image_sha256: Sha256
    cufs_revision: str
    collector_schema_version: str
    harness_version: Literal[2]


class StopRules(StrictModel):
    stop_on_any_drop: Literal[True]
    stop_on_epoch_mismatch: Literal[True]
    stop_on_unexplained_restart: Literal[True]
    stop_on_sink_fault: Literal[True]
    stop_on_parse_or_identity_mismatch: Literal[True]
    stop_on_checksum_mismatch: Literal[True]
    stop_on_read_error: Literal[True]
    latency_guardrail_ms: int = Field(gt=0)


class RetentionPolicy(StrictModel):
    owner: str = Field(min_length=1)
    cleanup_owner: str = Field(min_length=1)
    retain_until: datetime
    cleanup_action: str = Field(min_length=1)

    _retain_until = field_validator("retain_until")(utc)


class CollectionManifest(StrictModel):
    manifest_version: Literal[2]
    run_id: SafeId
    run_ref: Annotated[str, Field(pattern=r"^[0-9a-f]{32}$")]
    data_kind: Literal["synthetic"]
    experiment_kind: Literal["controlled"]
    representative_production_demand: Literal[False]
    protocol: Literal["NFSv4.1"]
    operation: Literal["READ"]
    warmer_enabled: Literal[False]
    cluster_id: SafeId
    namespace: SafeId
    volume_id: VolumeId
    export_id: int = Field(ge=0)
    cluster_manifest_sha256: Sha256
    fixture_catalog_sha256: Sha256
    event_schema_sha256: Sha256
    seed: int = Field(ge=0)
    start_time: datetime
    end_time: datetime
    offered_rate_per_s: float = Field(gt=0)
    offered_count: int = Field(gt=0)
    max_concurrency: Literal[1]
    checkpoint_interval_s: Literal[900]
    checkpoint_tolerance_s: int = Field(ge=0, le=300)
    fixture_distribution: Literal["seeded_unique_nonoverlapping"]
    range_distribution: Literal["seeded_unique_nonoverlapping"]
    range_bytes: tuple[int, ...]
    range_offsets: tuple[int, ...]
    untagged_correlation_window_s: int = Field(gt=0)
    client_id: SafeId
    tag_mode: Literal["pc1:d", "empty"]
    tag_contract_status: Literal["proposed-unverified"]
    allowed_exclusion_reasons: tuple[str, ...] = ()
    software: SoftwareVersions
    stop_rules: StopRules
    retention: RetentionPolicy
    limitations: tuple[str, ...]

    _start = field_validator("start_time")(utc)
    _end = field_validator("end_time")(utc)

    @model_validator(mode="after")
    def valid_run(self):
        if self.end_time - self.start_time != timedelta(hours=5):
            raise ValueError("controlled run must be exactly five hours")
        expected_run_ref = hashlib.sha256(self.run_id.encode()).hexdigest()[:32]
        if self.run_ref != expected_run_ref:
            raise ValueError("run_ref must be sha256(run_id UTF-8)[:32]")
        if self.start_time.minute or self.start_time.second or self.start_time.microsecond:
            raise ValueError("run start must align to a UTC hour")
        if len(self.allowed_exclusion_reasons) != len(set(self.allowed_exclusion_reasons)):
            raise ValueError("allowed exclusion reasons must be unique")
        if not self.range_bytes or any(value <= 0 for value in self.range_bytes):
            raise ValueError("range_bytes must contain positive values")
        if (
            not self.range_offsets
            or len(self.range_offsets) != len(set(self.range_offsets))
            or any(value < 0 for value in self.range_offsets)
        ):
            raise ValueError("range_offsets must be unique nonnegative values")
        if any(
            right - left < self.range_bytes[0]
            for left, right in zip(
                sorted(self.range_offsets), sorted(self.range_offsets)[1:], strict=False
            )
        ):
            raise ValueError("declared range offsets must not overlap")
        if self.retention.retain_until <= self.end_time:
            raise ValueError("retention must extend beyond the run")
        return self


class FixtureEntry(StrictModel):
    fixture_id: SafeId
    volume_id: VolumeId
    file_id: Sha256
    dir_id: SafeId
    export_id: int = Field(ge=0, le=65535)
    export_name: SafeId
    export_path: str
    relative_path: str
    size_bytes: int = Field(gt=0)
    sha256: Sha256
    weight: int = Field(gt=0)

    _relative_path = field_validator("relative_path")(safe_relative)

    @field_validator("export_path")
    @classmethod
    def safe_export_path(cls, value: str) -> str:
        path = PurePosixPath(value)
        if not path.is_absolute() or ".." in path.parts or "\\" in value:
            raise ValueError("export_path must be a safe absolute POSIX path")
        return value

    @model_validator(mode="after")
    def export_identity(self):
        if PurePosixPath(self.export_path).name != self.export_name:
            raise ValueError("export_name must match the final export_path component")
        return self


class FixtureCatalog(StrictModel):
    catalog_version: Literal[2]
    identity_scheme: Literal["chimera-v2-canonical"]
    fixtures: tuple[FixtureEntry, ...]

    @model_validator(mode="after")
    def unique_entries(self):
        keys = [(row.volume_id, row.file_id) for row in self.fixtures]
        fixture_ids = [row.fixture_id for row in self.fixtures]
        targets = [(row.export_id, row.relative_path) for row in self.fixtures]
        if not keys or len(fixture_ids) != len(set(fixture_ids)):
            raise ValueError("fixture catalog must have unique fixture IDs")
        if len(keys) != len(set(keys)):
            raise ValueError("fixture catalog must have unique volume/file identities")
        if len(targets) != len(set(targets)):
            raise ValueError("fixture catalog must not alias physical export paths")
        return self


class OfferedOperation(StrictModel):
    ledger_version: Literal[1]
    operation_id: Sha256
    operation_index: int = Field(ge=0)
    run_id: SafeId
    fixture_id: SafeId
    offered_at_utc: datetime
    offered_at_monotonic_ns: int = Field(ge=0)
    volume_id: VolumeId
    file_id: Sha256
    dir_id: SafeId
    export_id: int = Field(ge=0, le=65535)
    relative_path: str
    file_size: int = Field(gt=0)
    offset: int = Field(ge=0)
    count: int = Field(gt=0)
    expected_outcome: Literal["ok"]
    client_id: SafeId
    tag: str
    timing_mode: Literal["wallclock", "simulated"]
    origin: Literal["synthetic"]

    _offered_at = field_validator("offered_at_utc")(utc)
    _relative_path = field_validator("relative_path")(safe_relative)


class JsonlEvidence(StrictModel):
    path: str
    size_bytes: int = Field(ge=0)
    sha256: Sha256

    _path = field_validator("path")(safe_relative)


class DropCounters(StrictModel):
    enqueue: int = Field(ge=0)
    serialize: int = Field(ge=0)
    sink: int = Field(ge=0)
    rotate: int = Field(ge=0)
    unknown: int = Field(ge=0)

    @property
    def total(self) -> int:
        return sum((self.enqueue, self.serialize, self.sink, self.rotate, self.unknown))


class Checkpoint(StrictModel):
    checkpoint_version: Literal[1]
    run_id: SafeId
    scheduled_at: datetime
    captured_at: datetime
    pod_name: SafeId
    pod_uid: SafeId
    producer_epoch: str
    restart_count: int = Field(ge=0)
    restart_explained: bool
    last_enqueued_sequence: int = Field(ge=0)
    last_synced_sequence: int = Field(ge=0)
    synced_count: int = Field(ge=0)
    queue_depth: int = Field(ge=0)
    worker_in_flight: int = Field(ge=0, le=1)
    drops: DropCounters
    active_jsonl: JsonlEvidence
    backup_jsonl: JsonlEvidence | None = None
    collector_sink_healthy: bool
    storage_healthy: bool
    cluster_healthy: bool
    volume_healthy: bool
    export_healthy: bool
    parse_errors: int = Field(ge=0)
    identity_mismatches: int = Field(ge=0)
    checksum_mismatches: int = Field(ge=0)
    foreground_read_errors: int = Field(ge=0)
    max_observed_latency_ms: float = Field(ge=0)

    _scheduled_at = field_validator("scheduled_at")(utc)
    _captured_at = field_validator("captured_at")(utc)
    _producer_epoch = field_validator("producer_epoch")(canonical_uuid)

    @model_validator(mode="after")
    def valid_sequence(self):
        if self.last_synced_sequence > self.last_enqueued_sequence:
            raise ValueError("last synced sequence exceeds last enqueued sequence")
        if self.synced_count != self.last_synced_sequence:
            raise ValueError("Chimera 1-based synced_count must equal last synced sequence")
        return self


class ObservedEvent(StrictModel):
    observed_event_version: Literal[2]
    operation_id: Sha256 | None
    event_id: str
    pod_uid: SafeId
    producer_epoch: str
    producer_sequence: int = Field(gt=0)
    server_operation_id: Sha256 | None = None
    sink_time_ns: int | None = Field(default=None, ge=0)
    volume_id: VolumeId
    file_id: Sha256
    offset: int = Field(ge=0)
    count: int = Field(gt=0)
    durable_at: datetime
    status: Literal["durable", "excluded", "unknown"]
    exclusion_reason: str | None = None
    tag: str | None = None

    _event_id = field_validator("event_id")(canonical_uuid)
    _producer_epoch = field_validator("producer_epoch")(canonical_uuid)
    _durable_at = field_validator("durable_at")(utc)

    @model_validator(mode="after")
    def valid_disposition(self):
        if (self.status == "excluded") != (self.exclusion_reason is not None):
            raise ValueError("exclusion reason is required only for excluded events")
        if self.status in {"durable", "excluded"} and self.operation_id is None:
            raise ValueError("durable and excluded events require operation_id")
        if self.status == "unknown" and self.operation_id is not None:
            raise ValueError("unknown events cannot claim operation_id")
        return self


class CollectorSourceFile(StrictModel):
    raw_jsonl_path: str
    sha256: Sha256
    pod_name: SafeId
    pod_uid: SafeId
    node_name: SafeId
    producer_epoch: str

    _path = field_validator("raw_jsonl_path")(safe_relative)
    _producer_epoch = field_validator("producer_epoch")(canonical_uuid)

    @model_validator(mode="after")
    def path_convention(self):
        parts = PurePosixPath(self.raw_jsonl_path).parts
        if (
            len(parts) != 4
            or parts[0] != "collector"
            or parts[1] != self.pod_uid
            or parts[2] != self.producer_epoch
            or not parts[3].endswith(".jsonl")
        ):
            raise ValueError(
                "raw path must be collector/<pod_uid>/<producer_epoch>/<file>.jsonl"
            )
        return self


class CollectorFileManifest(StrictModel):
    collector_file_manifest_version: Literal[1]
    run_id: SafeId
    files: tuple[CollectorSourceFile, ...]

    @model_validator(mode="after")
    def unique_sources(self):
        paths = [source.raw_jsonl_path for source in self.files]
        streams = [(source.pod_uid, source.producer_epoch) for source in self.files]
        if not paths or len(paths) != len(set(paths)):
            raise ValueError("event source paths must be unique")
        if len(streams) != len(set(streams)):
            raise ValueError("each pod/epoch must have one event source")
        return self


def read_bytes(path: Path) -> bytes:
    if path.is_symlink() or not path.is_file():
        raise ValueError(f"{path}: expected a regular non-symlink file")
    metadata = path.stat()
    if metadata.st_uid != os.geteuid():
        raise PermissionError(f"{path}: evidence file must be owned by the importing user")
    if stat.S_IMODE(metadata.st_mode) & 0o077:
        raise PermissionError(f"{path}: evidence file must be owner-only (0600)")
    try:
        return path.read_bytes()
    except OSError as error:
        raise ValueError(f"{path}: unable to read: {error}") from error


def parse_json(data: bytes, source: Path, model: type[StrictModel]):
    try:
        value = json.loads(data)
    except json.JSONDecodeError as error:
        raise ValueError(f"{source}: invalid JSON: {error}") from error
    return model.model_validate(value)


def parse_jsonl(data: bytes, source: Path, model: type[StrictModel]) -> list[StrictModel]:
    try:
        lines = data.decode("utf-8").splitlines()
    except UnicodeDecodeError as error:
        raise ValueError(f"{source}: expected UTF-8 JSONL: {error}") from error
    rows = []
    for index, line in enumerate(lines, start=1):
        if not line.strip():
            raise ValueError(f"{source}:{index}: blank lines are not allowed")
        try:
            rows.append(model.model_validate_json(line))
        except ValueError as error:
            raise ValueError(f"{source}:{index}: {error}") from error
    return rows
