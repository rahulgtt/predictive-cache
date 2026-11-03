"""Strict, versioned contracts for controlled collection evidence."""

from __future__ import annotations

import hashlib
import re
from datetime import datetime, timedelta
from pathlib import PurePosixPath
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

Sha256 = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
SafeId = Annotated[str, Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")]
ProducerEpoch = Annotated[
    str,
    Field(pattern=r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"),
]
CanonicalUuid = ProducerEpoch


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError("timestamp must be UTC and timezone-aware")
    return value


def _safe_relative(value: str) -> str:
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


class FixtureEntry(StrictModel):
    fixture_id: SafeId
    volume_id: Annotated[str, Field(pattern=r"^[0-9a-f]{32}$")]
    file_id: Sha256
    dir_id: SafeId
    export_id: int = Field(ge=0)
    export_name: str
    export_path: str
    relative_path: str
    size_bytes: int = Field(gt=0)
    sha256: Sha256
    weight: int = Field(default=1, gt=0)

    @field_validator("export_path")
    @classmethod
    def absolute_export(cls, value: str) -> str:
        path = PurePosixPath(value)
        if not path.is_absolute() or ".." in path.parts or "\\" in value:
            raise ValueError("export_path must be a safe absolute POSIX path")
        return value

    @model_validator(mode="after")
    def export_identity(self) -> FixtureEntry:
        if PurePosixPath(self.export_path).name != self.export_name:
            raise ValueError("export_name must match the final export_path component")
        return self

    @field_validator("relative_path")
    @classmethod
    def safe_relative(cls, value: str) -> str:
        return _safe_relative(value)


class FixtureCatalog(StrictModel):
    catalog_version: Literal[2] = 2
    identity_scheme: Literal["chimera-v2-canonical"] = "chimera-v2-canonical"
    fixtures: tuple[FixtureEntry, ...]

    @model_validator(mode="after")
    def unique_fixtures(self) -> FixtureCatalog:
        ids = [fixture.fixture_id for fixture in self.fixtures]
        if not ids or len(ids) != len(set(ids)):
            raise ValueError("fixture catalog must contain unique fixtures")
        canonical = [(fixture.volume_id, fixture.file_id) for fixture in self.fixtures]
        if len(canonical) != len(set(canonical)):
            raise ValueError("fixture catalog must not alias canonical file identities")
        targets = [(fixture.export_id, fixture.relative_path) for fixture in self.fixtures]
        if len(targets) != len(set(targets)):
            raise ValueError("fixture catalog must not alias physical export paths")
        return self


class SoftwareVersions(StrictModel):
    predictive_cache_revision: str
    chimera_image: str
    chimera_image_sha256: Sha256
    chimera_revision: str = "unknown"
    cufs_image: str
    cufs_image_sha256: Sha256
    cufs_revision: str = "unknown"
    collector_schema_version: str
    harness_version: Literal[2] = 2


class StopRules(StrictModel):
    stop_on_any_drop: Literal[True] = True
    stop_on_epoch_mismatch: Literal[True] = True
    stop_on_unexplained_restart: Literal[True] = True
    stop_on_sink_fault: Literal[True] = True
    stop_on_parse_or_identity_mismatch: Literal[True] = True
    stop_on_checksum_mismatch: Literal[True] = True
    stop_on_read_error: Literal[True] = True
    latency_guardrail_ms: int = Field(gt=0)


class RetentionPolicy(StrictModel):
    owner: str = Field(min_length=1)
    cleanup_owner: str = Field(min_length=1)
    cleanup_action: str = Field(min_length=1)
    trigger: Literal["validated_dataset_finalization"] = "validated_dataset_finalization"
    delete_within_days: Literal[7] = 7
    retain_until: None = None


class AdapterValidationPermit(StrictModel):
    validation_permit_version: Literal[1] = 1
    permit_id: CanonicalUuid
    run_id: SafeId
    full_manifest_sha256: Sha256
    validation_operations: int = Field(ge=1, le=3)
    operation_indexes: tuple[int, ...]
    operation_ids: tuple[Sha256, ...]
    provider_package_sha256: Sha256
    provider_sha256: Sha256
    collector_sha256: Sha256
    validator_sha256: Sha256
    runbook_sha256: Sha256
    status: Literal["approved"]
    expires_at: datetime

    _expires_utc = field_validator("expires_at")(_utc)

    @model_validator(mode="after")
    def exact_prefix_shape(self) -> AdapterValidationPermit:
        expected = tuple(range(self.validation_operations))
        if self.operation_indexes != expected:
            raise ValueError("validation operation indexes must be the exact prefix")
        if len(self.operation_ids) != self.validation_operations:
            raise ValueError("validation operation IDs must match the approved count")
        return self


class CollectionManifest(StrictModel):
    manifest_version: Literal[2] = 2
    run_id: SafeId
    run_ref: Annotated[str, Field(pattern=r"^[0-9a-f]{32}$")]
    data_kind: Literal["synthetic"] = "synthetic"
    experiment_kind: Literal["controlled"] = "controlled"
    representative_production_demand: Literal[False] = False
    protocol: Literal["NFSv4.1"] = "NFSv4.1"
    operation: Literal["READ"] = "READ"
    warmer_enabled: Literal[False] = False
    cluster_id: SafeId
    namespace: SafeId
    volume_id: Annotated[str, Field(pattern=r"^[0-9a-f]{32}$")]
    export_id: int = Field(ge=0)
    cluster_manifest_sha256: Sha256
    fixture_catalog_sha256: Sha256
    event_schema_sha256: Sha256
    budget_evidence_sha256: Sha256
    seed: int = Field(ge=0)
    start_time: datetime
    end_time: datetime
    offered_rate_per_s: float = Field(gt=0)
    offered_count: int = Field(gt=0)
    max_concurrency: Literal[1] = 1
    checkpoint_interval_s: Literal[900] = 900
    checkpoint_tolerance_s: int = Field(default=30, ge=0, le=300)
    operation_schedule_tolerance_s: int = Field(default=30, ge=0, le=59)
    clock_skew_tolerance_s: int = Field(default=5, ge=0, le=30)
    pre_read_plus_read_guardrail_ms: int = Field(default=5000, gt=0)
    fixture_distribution: Literal["seeded_unique_nonoverlapping"] = (
        "seeded_unique_nonoverlapping"
    )
    range_distribution: Literal["seeded_unique_nonoverlapping"] = (
        "seeded_unique_nonoverlapping"
    )
    range_bytes: tuple[int, ...]
    range_offsets: tuple[int, ...]
    untagged_correlation_window_s: int = Field(default=120, gt=0)
    client_id: SafeId
    tag_mode: Literal["pc1:d", "empty"]
    tag_contract_status: Literal["proposed-unverified"] = "proposed-unverified"
    allowed_exclusion_reasons: tuple[str, ...] = ()
    software: SoftwareVersions
    stop_rules: StopRules
    retention: RetentionPolicy
    limitations: tuple[str, ...] = (
        "Synthetic controlled collection is plumbing evidence only.",
        "Correlation tags do not authenticate origin or prove production demand.",
        "No completed read proves cache residency or performance benefit.",
    )

    _start_utc = field_validator("start_time")(_utc)
    _end_utc = field_validator("end_time")(_utc)

    @field_validator("range_bytes")
    @classmethod
    def valid_ranges(cls, value: tuple[int, ...]) -> tuple[int, ...]:
        if len(value) != 1 or value[0] <= 0:
            raise ValueError("first collection requires exactly one positive range size")
        return value

    @field_validator("range_offsets")
    @classmethod
    def valid_offsets(cls, value: tuple[int, ...]) -> tuple[int, ...]:
        if not value or len(value) != len(set(value)) or any(item < 0 for item in value):
            raise ValueError("range_offsets must be unique nonnegative values")
        return value

    @field_validator("allowed_exclusion_reasons")
    @classmethod
    def exclusion_names(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if len(value) != len(set(value)) or any(
            not re.fullmatch(r"[a-z][a-z0-9_-]*", item) for item in value
        ):
            raise ValueError("exclusion reasons must be unique lower-case identifiers")
        return value

    @model_validator(mode="after")
    def controlled_run(self) -> CollectionManifest:
        expected_run_ref = hashlib.sha256(self.run_id.encode("utf-8")).hexdigest()[:32]
        if self.run_ref != expected_run_ref:
            raise ValueError("run_ref must be sha256(run_id UTF-8)[:32]")
        range_size = self.range_bytes[0]
        ordered_offsets = sorted(self.range_offsets)
        if any(
            right - left < range_size
            for left, right in zip(ordered_offsets, ordered_offsets[1:])
        ):
            raise ValueError("declared range offsets must not overlap")
        duration = (self.end_time - self.start_time).total_seconds()
        if duration != 5 * 60 * 60:
            raise ValueError("controlled collection duration must be exactly five hours")
        latest_offer_s = (self.offered_count - 1) / self.offered_rate_per_s
        if latest_offer_s + self.operation_schedule_tolerance_s > duration:
            raise ValueError("offered_count exceeds the declared rate cap")
        return self


class CleanupScheduleRecord(StrictModel):
    cleanup_schedule_version: Literal[1] = 1
    run_id: SafeId
    finalized_at: datetime
    delete_by: datetime
    cleanup_owner: str = Field(min_length=1)
    validation_report_sha256: Sha256
    deletion_inventory_sha256: Sha256

    _finalized_utc = field_validator("finalized_at")(_utc)
    _delete_utc = field_validator("delete_by")(_utc)

    @model_validator(mode="after")
    def seven_day_deadline(self) -> CleanupScheduleRecord:
        if self.delete_by != self.finalized_at + timedelta(days=7):
            raise ValueError("delete_by must equal finalized_at plus seven days")
        return self


class CleanupCompletionRecord(StrictModel):
    cleanup_completion_version: Literal[1] = 1
    run_id: SafeId
    delete_by: datetime
    completed_at: datetime
    cleanup_owner: str = Field(min_length=1)
    evidence: str = Field(min_length=1)

    _delete_utc = field_validator("delete_by")(_utc)
    _completed_utc = field_validator("completed_at")(_utc)


class OfferedOperation(StrictModel):
    ledger_version: Literal[1] = 1
    operation_id: Sha256
    operation_index: int = Field(ge=0)
    run_id: SafeId
    fixture_id: SafeId
    offered_at_utc: datetime
    offered_at_monotonic_ns: int = Field(ge=0)
    volume_id: Annotated[str, Field(pattern=r"^[0-9a-f]{32}$")]
    file_id: Sha256
    dir_id: SafeId
    export_id: int = Field(ge=0)
    relative_path: str
    file_size: int = Field(gt=0)
    offset: int = Field(ge=0)
    count: int = Field(gt=0)
    expected_outcome: Literal["ok"] = "ok"
    client_id: SafeId
    tag: str
    timing_mode: Literal["wallclock", "simulated"]
    origin: Literal["synthetic"] = "synthetic"

    _offered_utc = field_validator("offered_at_utc")(_utc)
    _relative = field_validator("relative_path")(_safe_relative)


class ExecutionResult(StrictModel):
    result_version: Literal[2] = 2
    operation_id: Sha256
    completed_at_utc: datetime
    status: Literal["ok", "short", "eof", "error", "cancelled", "timeout"]
    bytes_read: int = Field(ge=0)
    latency_ms: float = Field(ge=0)
    supervised_latency_ms: float | None = Field(default=None, ge=0)
    content_sha256: Sha256 | None = None
    executor_kind: Literal["dry-run", "direct-mount"]
    filesystem_type: str
    error: str | None = None

    _completed_utc = field_validator("completed_at_utc")(_utc)

    @model_validator(mode="after")
    def error_shape(self) -> ExecutionResult:
        failed = self.status in ("error", "cancelled", "timeout")
        if failed != (self.error is not None):
            raise ValueError("error detail must be present only for failed reads")
        if self.executor_kind == "direct-mount" and (
            (self.bytes_read > 0) != (self.content_sha256 is not None)
        ):
            raise ValueError("mount reads require a checksum exactly when bytes were returned")
        if self.executor_kind == "dry-run" and self.content_sha256 is not None:
            raise ValueError("dry-run must not fabricate a content checksum")
        return self


class JsonlEvidence(StrictModel):
    path: str
    size_bytes: int = Field(ge=0)
    sha256: Sha256

    _path = field_validator("path")(_safe_relative)


class DropCounters(StrictModel):
    queue: int = Field(ge=0)
    encode: int = Field(ge=0)
    sink: int = Field(ge=0)
    rollback: int = Field(ge=0)
    rotation: int = Field(ge=0)

    @property
    def total(self) -> int:
        return self.queue + self.encode + self.sink + self.rollback + self.rotation


class Checkpoint(StrictModel):
    checkpoint_version: Literal[1] = 1
    run_id: SafeId
    scheduled_at: datetime
    captured_at: datetime
    pod_name: SafeId
    pod_uid: CanonicalUuid
    node_name: SafeId
    producer_epoch: ProducerEpoch
    restart_count: int = Field(ge=0)
    restart_explained: bool = False
    last_enqueued_sequence: int = Field(ge=0)
    last_synced_sequence: int = Field(ge=0)
    synced_count: int = Field(ge=0)
    queue_depth: int = Field(ge=0)
    worker_in_flight: int = Field(ge=0, le=1)
    delivery_clean: Literal[0, 1]
    barrier_ready: Literal[0, 1]
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

    _scheduled_utc = field_validator("scheduled_at")(_utc)
    _captured_utc = field_validator("captured_at")(_utc)

    @model_validator(mode="after")
    def sequence_consistency(self) -> Checkpoint:
        if self.captured_at < self.scheduled_at:
            raise ValueError("checkpoint cannot be captured before its schedule")
        if self.last_synced_sequence > self.last_enqueued_sequence:
            raise ValueError("last synced sequence cannot exceed last enqueued sequence")
        if self.synced_count != self.last_synced_sequence:
            raise ValueError("synced_count does not match last synced sequence")
        if self.synced_count == 0 and (
            self.last_enqueued_sequence != 0 or self.last_synced_sequence != 0
        ):
            raise ValueError("zero-event checkpoint must use 0/0/count0 sentinels")
        if (
            self.last_synced_sequence != self.last_enqueued_sequence
            or self.queue_depth != 0
            or self.worker_in_flight != 0
            or self.delivery_clean != 1
            or self.barrier_ready != 1
        ):
            raise ValueError("checkpoint barrier must be fully drained and ready")
        return self


class PreReadHealth(StrictModel):
    health_version: Literal[1] = 1
    run_id: SafeId
    operation_id: Sha256
    captured_at: datetime
    pod_name: SafeId
    pod_uid: CanonicalUuid
    node_name: SafeId
    producer_epoch: ProducerEpoch
    last_enqueued_sequence: int = Field(ge=0)
    last_synced_sequence: int = Field(ge=0)
    synced_count: int = Field(ge=0)
    queue_depth: int = Field(ge=0)
    worker_in_flight: int = Field(ge=0, le=1)
    delivery_clean: Literal[0, 1]
    barrier_ready: Literal[0, 1]
    drops: DropCounters
    collector_sink_healthy: bool
    storage_healthy: bool
    cluster_healthy: bool
    volume_healthy: bool
    export_healthy: bool
    parse_errors: int = Field(ge=0)
    identity_mismatches: int = Field(ge=0)
    checksum_mismatches: int = Field(ge=0)
    foreground_read_errors: int = Field(ge=0)

    _captured_utc = field_validator("captured_at")(_utc)

    @model_validator(mode="after")
    def barrier_consistency(self) -> PreReadHealth:
        if self.last_synced_sequence > self.last_enqueued_sequence:
            raise ValueError("last synced sequence cannot exceed last enqueued sequence")
        if self.synced_count != self.last_synced_sequence:
            raise ValueError("synced_count does not match last synced sequence")
        if self.synced_count == 0 and (
            self.last_enqueued_sequence != 0 or self.last_synced_sequence != 0
        ):
            raise ValueError("zero-event health must use 0/0/count0 sentinels")
        if (
            self.last_synced_sequence != self.last_enqueued_sequence
            or self.queue_depth != 0
            or self.worker_in_flight != 0
            or self.delivery_clean != 1
            or self.barrier_ready != 1
        ):
            raise ValueError("pre-read barrier must be fully drained and ready")
        return self


class ObservedEvent(StrictModel):
    observed_event_version: Literal[2] = 2
    operation_id: Sha256 | None
    event_id: SafeId
    pod_uid: CanonicalUuid
    producer_epoch: ProducerEpoch
    producer_sequence: int = Field(gt=0)
    server_operation_id: str | None = None
    sink_time_ns: int | None = Field(default=None, ge=0)
    volume_id: Annotated[str, Field(pattern=r"^[0-9a-f]{32}$")]
    file_id: Sha256
    offset: int = Field(ge=0)
    count: int = Field(gt=0)
    durable_at: datetime
    status: Literal["durable", "excluded", "unknown"]
    exclusion_reason: str | None = None
    tag: str | None = None

    _durable_utc = field_validator("durable_at")(_utc)

    @model_validator(mode="after")
    def exclusion_shape(self) -> ObservedEvent:
        if (self.status == "excluded") != (self.exclusion_reason is not None):
            raise ValueError("exclusion reason is required only for excluded events")
        if self.status in ("durable", "excluded") and self.operation_id is None:
            raise ValueError("durable and excluded events require an operation ID")
        if self.status == "unknown" and self.operation_id is not None:
            raise ValueError("unknown events must not claim an operation ID")
        if (
            self.operation_id is not None
            and self.server_operation_id is not None
            and self.operation_id == self.server_operation_id
        ):
            raise ValueError("server and offered operation IDs must remain distinct")
        return self


class CollectorDurableEvent(StrictModel):
    provider_event_version: Literal[1] = 1
    run_id: SafeId
    event_id: CanonicalUuid
    pod_name: SafeId
    pod_uid: CanonicalUuid
    node_name: SafeId
    producer_epoch: ProducerEpoch
    producer_sequence: int = Field(gt=0)
    server_operation_id: Sha256
    sink_time_ns: int = Field(ge=0)
    volume_id: Annotated[str, Field(pattern=r"^[0-9a-f]{32}$")]
    file_id: Sha256
    export_id: int = Field(ge=0)
    export_name: str
    offset: int = Field(ge=0)
    count: int = Field(gt=0)
    returned_bytes: int = Field(ge=0)
    nfs_status: int = Field(ge=0)
    outcome: Literal["success", "error"]
    eof: bool
    durable_at: datetime
    tag: str
    raw_jsonl_path: str
    raw_jsonl_sha256: Sha256
    raw_line_number: int = Field(gt=0)
    raw_record_sha256: Sha256

    _durable_utc = field_validator("durable_at")(_utc)
    _raw_path = field_validator("raw_jsonl_path")(_safe_relative)

    @model_validator(mode="after")
    def raw_path_convention(self) -> CollectorDurableEvent:
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


class ReconciliationReport(StrictModel):
    report_version: Literal[1] = 1
    run_id: SafeId
    manifest_sha256: Sha256
    ledger_sha256: Sha256
    observed_sha256: Sha256
    offered_count: int = Field(ge=0)
    unique_durable_count: int = Field(ge=0)
    duplicate_operation_ids: tuple[Sha256, ...]
    missing_operation_ids: tuple[Sha256, ...]
    unexpected_operation_ids: tuple[Sha256, ...]
    unknown_event_ids: tuple[SafeId, ...]
    exclusions: dict[str, tuple[Sha256, ...]]
    pod_epochs: dict[SafeId, tuple[ProducerEpoch, ...]]
    success: bool
    failures: tuple[str, ...]


class CollectorSourceFile(StrictModel):
    raw_jsonl_path: str
    sha256: Sha256
    pod_name: SafeId
    pod_uid: CanonicalUuid
    node_name: SafeId
    producer_epoch: ProducerEpoch

    _path = field_validator("raw_jsonl_path")(_safe_relative)

    @model_validator(mode="after")
    def path_convention(self) -> CollectorSourceFile:
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
    collector_file_manifest_version: Literal[1] = 1
    run_id: SafeId
    files: tuple[CollectorSourceFile, ...]
