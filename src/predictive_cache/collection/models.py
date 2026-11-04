"""Strict, versioned contracts for controlled collection evidence."""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timedelta
from pathlib import PurePosixPath
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from predictive_cache.semantics.read_demand import SEMANTIC_VIEW_SHA256

Sha256 = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
GitObjectId = Annotated[str, Field(pattern=r"^[0-9a-f]{40}$")]
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


WorkloadPermitProfile = Literal["bounded_validation", "full_collection"]
LAB_FULL_RUN_WAIVER = (
    "Unvalidated one-shot lab full run: bounded PASS attestation was "
    "explicitly waived by the user."
)
WorkloadValidationRole = Literal[
    "ordinary",
    "repeated_range",
    "sequential_range",
]


class WorkloadCollectionPermit(StrictModel):
    """Immutable manifest-v3 approval, distinct from the v2 adapter permit."""

    validation_permit_version: Literal[3] = 3
    permit_id: CanonicalUuid
    profile: WorkloadPermitProfile
    run_id: SafeId
    manifest_sha256: Sha256
    manifest_file_sha256: Sha256
    workload_config_sha256: Sha256
    workload_config_file_sha256: Sha256
    execution_plan_sha256: Sha256
    execution_plan_file_sha256: Sha256
    workload_truth_sha256: Sha256
    workload_truth_file_sha256: Sha256
    truth_binding_sha256: Sha256
    truth_binding_file_sha256: Sha256
    fixture_catalog_sha256: Sha256
    fixture_catalog_file_sha256: Sha256
    semantic_view_sha256: Sha256
    validated_code_head_sha256: GitObjectId
    validated_merge_commit_sha256: GitObjectId
    validated_tree_sha256: GitObjectId
    provider_package_sha256: Sha256
    provider_sha256: Sha256
    collector_sha256: Sha256
    validator_sha256: Sha256
    runbook_sha256: Sha256
    adapter_wrapper_sha256: Sha256
    cleaner_wrapper_sha256: Sha256
    interpreter_path: str
    interpreter_sha256: Sha256
    bounded_validation_attestation_sha256: Sha256 | None = None
    lab_unvalidated_full_run: bool = False
    lab_waiver_statement: Literal[
        "Unvalidated one-shot lab full run: bounded PASS attestation was "
        "explicitly waived by the user."
    ] | None = None
    operation_indexes: tuple[Annotated[int, Field(ge=0, strict=True)], ...]
    intent_ids: tuple[Sha256, ...]
    operation_ids: tuple[Sha256, ...]
    roles: tuple[WorkloadValidationRole, ...]
    original_relative_ms: tuple[Annotated[int, Field(ge=0, strict=True)], ...]
    execution_start_time: datetime
    execution_relative_ms: tuple[Annotated[int, Field(ge=0, strict=True)], ...]
    schedule_tolerance_s: Annotated[int, Field(ge=0, le=59, strict=True)]
    status: Literal["approved"] = "approved"
    expires_at: datetime

    _execution_start_utc = field_validator("execution_start_time")(_utc)
    _expires_utc = field_validator("expires_at")(_utc)

    @model_validator(mode="after")
    def exact_profile_shape(self) -> WorkloadCollectionPermit:
        selected = len(self.operation_indexes)
        if (
            selected == 0
            or len(self.intent_ids) != selected
            or len(self.operation_ids) != selected
            or len(self.roles) != selected
            or len(self.original_relative_ms) != selected
            or len(self.execution_relative_ms) != selected
        ):
            raise ValueError(
                "workload permit selection fields must have equal nonzero length"
            )
        if len(set(self.operation_indexes)) != selected:
            raise ValueError("workload permit operation indexes must be unique")
        if len(set(self.intent_ids)) != selected:
            raise ValueError("workload permit intent IDs must be unique")
        if len(set(self.operation_ids)) != selected:
            raise ValueError("workload permit operation IDs must be unique")
        if self.operation_indexes != tuple(sorted(self.operation_indexes)):
            raise ValueError("workload permit operations must remain in plan order")
        if self.original_relative_ms != tuple(sorted(self.original_relative_ms)):
            raise ValueError("original plan timestamps must remain ordered")
        if self.execution_relative_ms != tuple(sorted(self.execution_relative_ms)):
            raise ValueError("permit execution timestamps must remain ordered")
        if self.profile == "bounded_validation":
            if self.lab_unvalidated_full_run or self.lab_waiver_statement is not None:
                raise ValueError("bounded permit must not carry a full-run lab waiver")
            if self.bounded_validation_attestation_sha256 is not None:
                raise ValueError(
                    "bounded permit must not bind a prior bounded attestation"
                )
            if not 1 <= selected <= 3:
                raise ValueError(
                    "bounded validation requires one to three operations"
                )
            expected_roles = (
                "ordinary",
                "repeated_range",
                "sequential_range",
            )[:selected]
            if self.roles != expected_roles:
                raise ValueError(
                    "bounded validation roles must be the canonical relation prefix"
                )
        else:
            has_attestation = self.bounded_validation_attestation_sha256 is not None
            has_lab_waiver = (
                self.lab_unvalidated_full_run
                and self.lab_waiver_statement == LAB_FULL_RUN_WAIVER
            )
            if has_attestation == has_lab_waiver:
                raise ValueError(
                    "full collection permit requires exactly one of bounded "
                    "PASS attestation or explicit lab waiver"
                )
            if self.operation_indexes != tuple(range(selected)):
                raise ValueError(
                    "full collection operations must be complete and in original order"
                )
            if self.roles != tuple("ordinary" for _ in range(selected)):
                raise ValueError("full collection does not use bounded validation roles")
        return self


WorkloadValidationPermit = WorkloadCollectionPermit


class WorkloadEvidenceBinding(StrictModel):
    """Harness-owned v3 lineage from the plan occurrence to immutable raw evidence."""

    workload_evidence_binding_version: Literal[1] = 1
    run_id: SafeId
    permit_sha256: Sha256
    execution_plan_sha256: Sha256
    operation_index: int = Field(ge=0)
    intent_id: Sha256
    operation_id: Sha256
    role: WorkloadValidationRole | None = None
    original_relative_ms: int = Field(ge=0)
    scheduled_at_utc: datetime
    offered_at_utc: datetime
    event_id: CanonicalUuid
    producer_epoch: ProducerEpoch
    producer_sequence: int = Field(gt=0)
    raw_jsonl_path: str
    raw_jsonl_sha256: Sha256
    raw_line_number: int = Field(gt=0)
    raw_record_sha256: Sha256
    observed_sha256: Sha256

    _scheduled_utc = field_validator("scheduled_at_utc")(_utc)
    _offered_utc = field_validator("offered_at_utc")(_utc)
    _raw_path = field_validator("raw_jsonl_path")(_safe_relative)


CleanupRootKind = Literal[
    "bounded_harness_evidence",
    "bounded_provider_evidence",
    "bounded_adapter_dataset",
    "bounded_clean_dataset",
    "live_harness_evidence",
    "live_provider_evidence",
    "live_adapter_dataset",
    "live_clean_dataset",
]


class CleanupFileEvidence(StrictModel):
    sha256: Sha256
    mode: int = Field(ge=0, le=0o777)


class CleanupRootEvidence(StrictModel):
    root_id: SafeId
    kind: CleanupRootKind
    canonical_path: str = Field(min_length=1)
    path_sha256: Sha256
    device: int = Field(ge=0)
    inode: int = Field(gt=0)
    root_manifest_sha256: Sha256
    files: dict[str, CleanupFileEvidence]

    @field_validator("files")
    @classmethod
    def safe_files(cls, value: dict[str, CleanupFileEvidence]):
        if not value:
            raise ValueError("cleanup root must contain evidence files")
        for path in value:
            _safe_relative(path)
        return value


class CleanupEvidenceReference(StrictModel):
    root_id: SafeId
    path: str
    sha256: Sha256

    _path = field_validator("path")(_safe_relative)


HistoricalRootRole = Literal[
    "validation_1_run",
    "validation_1_input",
    "validation_2_run",
    "validation_2_input",
    "validation_3_run",
    "validation_3_input",
    "validation_4_run",
    "validation_4_input",
    "live_run",
    "live_input",
    "shared_provider",
    "live_adapter_dataset",
    "live_clean_dataset",
]


class HistoricalDirectoryEvidence(StrictModel):
    mode: int = Field(ge=0, le=0o777)


class HistoricalFileEvidence(CleanupFileEvidence):
    size_bytes: int = Field(ge=0)


class HistoricalRootEvidence(StrictModel):
    root_id: SafeId
    role: HistoricalRootRole
    canonical_path: str = Field(min_length=1)
    path_sha256: Sha256
    owner_uid: int = Field(ge=0)
    mode: Literal[0o700]
    device: int = Field(ge=0)
    inode: int = Field(gt=0)
    root_manifest_sha256: Sha256
    retention_action: Literal["delete", "retain"]
    directories: dict[str, HistoricalDirectoryEvidence]
    files: dict[str, HistoricalFileEvidence]

    @field_validator("directories", "files")
    @classmethod
    def safe_entries(cls, value):
        for path in value:
            _safe_relative(path)
        return value

    @model_validator(mode="after")
    def nonempty_root(self):
        if not self.files:
            raise ValueError("historical root must contain evidence files")
        if self.role == "shared_provider" and self.retention_action != "retain":
            raise ValueError("shared provider evidence must be retained")
        if self.role != "shared_provider" and self.retention_action != "delete":
            raise ValueError("historical collection evidence must be deleted")
        return self


class HistoricalEvidenceReference(StrictModel):
    role: HistoricalRootRole
    path: str
    sha256: Sha256

    _path = field_validator("path")(_safe_relative)


class HistoricalExternalFileEvidence(StrictModel):
    canonical_path: str = Field(min_length=1)
    sha256: Sha256
    owner_uid: int = Field(ge=0)
    mode: int = Field(ge=0, le=0o777)
    device: int = Field(ge=0)
    inode: int = Field(gt=0)
    size_bytes: int = Field(ge=0)


class HistoricalValidationAttempt(StrictModel):
    attempt: Literal[1, 2, 3, 4]
    run_role: HistoricalRootRole
    input_role: HistoricalRootRole
    run_id: SafeId
    outcome: Literal["failed", "passed"]
    result_code: Literal[
        "offered_count_mismatch",
        "tag_mismatch",
        "per_chunk_baseline_reset",
        "passed",
    ]
    log: HistoricalEvidenceReference

    @model_validator(mode="after")
    def exact_attempt_roles(self):
        if self.run_role != f"validation_{self.attempt}_run":
            raise ValueError("historical validation run role is inconsistent")
        if self.input_role != f"validation_{self.attempt}_input":
            raise ValueError("historical validation input role is inconsistent")
        if (self.outcome == "passed") != (self.result_code == "passed"):
            raise ValueError("historical validation outcome is inconsistent")
        return self


class HistoricalExecutionProvenance(StrictModel):
    commit: GitObjectId
    tree: GitObjectId
    source_archive: HistoricalExternalFileEvidence


class HistoricalIntegrationProvenance(StrictModel):
    commit: GitObjectId
    tree: GitObjectId
    executed: Literal[False]


class HistoricalVerificationProvenance(StrictModel):
    commit: GitObjectId
    tree: GitObjectId
    source_archive: HistoricalExternalFileEvidence


class HistoricalCompatibilityApproval(StrictModel):
    approval_version: Literal[1] = 1
    status: Literal["approved"]
    approved_at: datetime
    compatibility_scope: Literal["historical_pre_finalizer"]
    provider_execution_binding_status: Literal["legacy_not_recorded"]
    missing_controls: tuple[
        Literal[
            "provider_invocation_record",
            "run_local_provider_snapshot",
            "run_local_package_snapshot",
        ],
        ...,
    ]
    statement: Literal[
        "Approve compatibility finalization of the frozen historical run "
        "without fabricating controls that were not recorded at execution."
    ]

    _approved_utc = field_validator("approved_at")(_utc)

    @field_validator("missing_controls")
    @classmethod
    def exact_missing_controls(cls, value):
        expected = {
            "provider_invocation_record",
            "run_local_provider_snapshot",
            "run_local_package_snapshot",
        }
        if set(value) != expected or len(value) != len(expected):
            raise ValueError("historical approval must acknowledge exact missing controls")
        return value


class HistoricalRunEvidence(StrictModel):
    manifest: HistoricalEvidenceReference
    collection_permit: HistoricalEvidenceReference
    fixture_catalog: HistoricalEvidenceReference
    offered_ledger: HistoricalEvidenceReference
    execution_ledger: HistoricalEvidenceReference
    observed: HistoricalEvidenceReference
    source_manifest: HistoricalEvidenceReference
    durable_sidecar: HistoricalEvidenceReference
    checkpoints: HistoricalEvidenceReference
    pre_read_health: HistoricalEvidenceReference
    reconciliation: HistoricalEvidenceReference | None = None
    validation_permit: HistoricalEvidenceReference | None = None
    imported_manifest: HistoricalEvidenceReference
    imported_report: HistoricalEvidenceReference
    cleaned_manifest: HistoricalEvidenceReference
    cleaned_report: HistoricalEvidenceReference


class HistoricalProviderEvidence(StrictModel):
    package: HistoricalEvidenceReference
    provider: HistoricalEvidenceReference
    collector: HistoricalEvidenceReference
    validator: HistoricalEvidenceReference
    runbook: HistoricalEvidenceReference


class HistoricalFinalizationAttestation(StrictModel):
    historical_finalization_attestation_version: Literal[1] = 1
    compatibility_scope: Literal["historical_pre_finalizer"]
    run_id: SafeId
    run_manifest_sha256: Sha256
    frozen_root_declaration_sha256: Sha256
    roots: tuple[HistoricalRootEvidence, ...]
    attempts: tuple[HistoricalValidationAttempt, ...]
    successful_bounded: HistoricalRunEvidence
    live: HistoricalRunEvidence
    provider: HistoricalProviderEvidence
    executed: HistoricalExecutionProvenance
    integration: HistoricalIntegrationProvenance
    verification: HistoricalVerificationProvenance
    adapter_wrapper: HistoricalExternalFileEvidence
    cleaner_wrapper: HistoricalExternalFileEvidence
    approval: HistoricalCompatibilityApproval

    @model_validator(mode="after")
    def exact_historical_topology(self):
        expected_roles = set(HistoricalRootRole.__args__)
        roles = [root.role for root in self.roots]
        if set(roles) != expected_roles or len(roles) != len(expected_roles):
            raise ValueError("historical attestation must contain the exact root roles")
        if tuple(item.attempt for item in self.attempts) != (1, 2, 3, 4):
            raise ValueError("historical validation attempts must be ordered 1 through 4")
        if tuple(item.outcome for item in self.attempts) != (
            "failed",
            "failed",
            "failed",
            "passed",
        ):
            raise ValueError("historical validation outcomes are inconsistent")
        return self


class HistoricalFrozenRootDeclaration(StrictModel):
    historical_frozen_root_declaration_version: Literal[1] = 1
    run_id: SafeId
    run_manifest_sha256: Sha256
    roots: tuple[HistoricalRootEvidence, ...]

    @model_validator(mode="after")
    def exact_declared_topology(self):
        expected_roles = set(HistoricalRootRole.__args__)
        roles = [root.role for root in self.roots]
        if set(roles) != expected_roles or len(roles) != len(expected_roles):
            raise ValueError("frozen declaration must contain exact root roles")
        return self


class HistoricalDeletionTarget(StrictModel):
    canonical_path: str = Field(min_length=1)
    device: int = Field(ge=0)
    inode: int = Field(gt=0)
    action: Literal["delete", "retain"]


class HistoricalDeletionApproval(StrictModel):
    historical_deletion_approval_version: Literal[1] = 1
    status: Literal["approved"]
    approved_at: datetime
    run_id: SafeId
    attestation_sha256: Sha256
    verification_commit: GitObjectId
    verification_tree: GitObjectId
    merge_confirmed: Literal[True]
    targets: dict[HistoricalRootRole, HistoricalDeletionTarget]
    statement: Literal[
        "Approve the exact historical cleanup targets and actions recorded "
        "in this artifact after the compatibility verifier merged."
    ]

    _approved_utc = field_validator("approved_at")(_utc)

    @model_validator(mode="after")
    def exact_targets(self):
        expected = set(HistoricalRootRole.__args__)
        if set(self.targets) != expected:
            raise ValueError("historical deletion approval must bind every root role")
        if self.targets["shared_provider"].action != "retain":
            raise ValueError("historical deletion approval must retain provider evidence")
        if any(
            target.action != "delete"
            for role, target in self.targets.items()
            if role != "shared_provider"
        ):
            raise ValueError("historical deletion approval must delete collection roots")
        return self


class HistoricalRetentionApproval(StrictModel):
    historical_retention_approval_version: Literal[1] = 1
    status: Literal["approved"]
    approved_at: datetime
    run_id: SafeId
    attestation_sha256: Sha256
    verification_commit: GitObjectId
    verification_tree: GitObjectId
    merge_confirmed: Literal[True]
    targets: dict[HistoricalRootRole, HistoricalDeletionTarget]
    statement: Literal[
        "Approve indefinite retention of every exact historical evidence root; "
        "do not create a deletion schedule or delete_by deadline."
    ]

    _approved_utc = field_validator("approved_at")(_utc)

    @model_validator(mode="after")
    def retain_every_target(self):
        expected = set(HistoricalRootRole.__args__)
        if set(self.targets) != expected:
            raise ValueError("historical retention approval must bind every root role")
        if any(target.action != "retain" for target in self.targets.values()):
            raise ValueError("historical retention approval must retain every root")
        return self


class HistoricalRetentionRecord(StrictModel):
    historical_retention_record_version: Literal[1] = 1
    run_id: SafeId
    finalized_at: datetime
    retention_policy: Literal["retain_indefinitely"]
    attestation_sha256: Sha256
    retention_approval_sha256: Sha256
    root_actions: dict[HistoricalRootRole, Literal["retain"]]
    delete_by: Literal[None] = None

    _finalized_utc = field_validator("finalized_at")(_utc)

    @field_validator("root_actions")
    @classmethod
    def retain_all_roots(cls, value):
        expected = set(HistoricalRootRole.__args__)
        if set(value) != expected or any(
            action != "retain" for action in value.values()
        ):
            raise ValueError("historical retention record must retain every root")
        return value


class HistoricalDispositionClaim(StrictModel):
    historical_disposition_version: Literal[1] = 1
    run_id: SafeId
    outcome: Literal["retain_all", "delete"]
    claimed_at: datetime
    attestation_sha256: Sha256
    approval_sha256: Sha256
    verification_commit: GitObjectId
    verification_tree: GitObjectId
    root_manifest_sha256: dict[HistoricalRootRole, Sha256]

    _claimed_utc = field_validator("claimed_at")(_utc)

    @field_validator("root_manifest_sha256")
    @classmethod
    def exact_roots(cls, value):
        if set(value) != set(HistoricalRootRole.__args__):
            raise ValueError("historical disposition must bind every root")
        return value


class BoundedValidationAttestation(StrictModel):
    bounded_validation_manifest_version: Literal[1] = 1
    validation_run_id: SafeId
    full_live_candidate_manifest: CleanupEvidenceReference
    permit_id: CanonicalUuid
    permit: CleanupEvidenceReference
    offered_ledger: CleanupEvidenceReference
    execution_ledger: CleanupEvidenceReference
    observed: CleanupEvidenceReference
    source_manifest: CleanupEvidenceReference
    checkpoints: CleanupEvidenceReference
    durable_sidecar: CleanupEvidenceReference
    pre_read_health: CleanupEvidenceReference
    provider_manifest: CleanupEvidenceReference
    validation_operations: int = Field(ge=1, le=3)
    operation_ids: tuple[Sha256, ...]
    validated_code_head_sha256: GitObjectId
    validated_merge_commit_sha256: GitObjectId
    validated_tree_sha256: GitObjectId
    executed_source_archive: CleanupEvidenceReference
    provider_package: CleanupEvidenceReference
    provider: CleanupEvidenceReference
    collector: CleanupEvidenceReference
    validator: CleanupEvidenceReference
    runbook: CleanupEvidenceReference
    adapter_wrapper: CleanupEvidenceReference
    cleaner_wrapper: CleanupEvidenceReference
    cluster_provenance: CleanupEvidenceReference
    event_schema: CleanupEvidenceReference
    budget_evidence: CleanupEvidenceReference
    fixture_catalog: CleanupEvidenceReference
    imported_manifest: CleanupEvidenceReference
    imported_report: CleanupEvidenceReference
    cleaned_manifest: CleanupEvidenceReference
    cleaned_report: CleanupEvidenceReference
    started_at: datetime
    completed_at: datetime
    scope: Literal["bounded_validation"]
    data_kind: Literal["synthetic"]
    representative_production_demand: Literal[False]
    status: Literal["passed"]
    cleanup_roots: tuple[CleanupRootEvidence, ...]

    _started_utc = field_validator("started_at")(_utc)
    _completed_utc = field_validator("completed_at")(_utc)

    @model_validator(mode="after")
    def exact_bounded_contract(self):
        if len(self.operation_ids) != self.validation_operations:
            raise ValueError("bounded operation IDs must match the validated count")
        if self.completed_at < self.started_at:
            raise ValueError("bounded validation completion predates its start")
        kinds = {root.kind for root in self.cleanup_roots}
        expected = {
            "bounded_harness_evidence",
            "bounded_provider_evidence",
            "bounded_adapter_dataset",
            "bounded_clean_dataset",
        }
        if len(self.cleanup_roots) != 4 or kinds != expected:
            raise ValueError("bounded attestation must declare its four exact roots")
        return self


class ProviderInvocationRecord(StrictModel):
    provider_invocation_version: Literal[1] = 1
    run_id: SafeId
    manifest_sha256: Sha256
    provider_package_sha256: Sha256
    provider_package_path: Literal[".approved-tools/provider-package.zip"]
    provider_snapshot_path: Literal[".approved-tools/checkpoint-provider"]
    provider_snapshot_sha256: Sha256
    collector_sha256: Sha256
    validator_sha256: Sha256
    interpreter_path: str
    interpreter_sha256: Sha256
    argv_contract: tuple[str, ...]
    actions: tuple[str, ...]
    created_at: datetime

    _created_utc = field_validator("created_at")(_utc)


class WorkloadProviderInvocationRecord(StrictModel):
    provider_invocation_version: Literal[3] = 3
    run_id: SafeId
    manifest_sha256: Sha256
    permit_sha256: Sha256
    execution_plan_sha256: Sha256
    provider_package_sha256: Sha256
    provider_package_path: Literal[".approved-tools/provider-package.zip"]
    provider_snapshot_path: Literal[".approved-tools/checkpoint-provider"]
    provider_snapshot_sha256: Sha256
    collector_sha256: Sha256
    validator_sha256: Sha256
    runbook_sha256: Sha256
    adapter_wrapper_sha256: Sha256
    cleaner_wrapper_sha256: Sha256
    interpreter_path: str
    interpreter_sha256: Sha256
    validated_code_head_sha256: GitObjectId
    validated_merge_commit_sha256: GitObjectId
    validated_tree_sha256: GitObjectId
    lab_unvalidated_full_run: bool = False
    lab_waiver_statement: str | None = None
    argv_contract: tuple[str, ...]
    actions: tuple[str, ...]
    created_at: datetime

    _created_utc = field_validator("created_at")(_utc)


class LiveProviderDeclaration(StrictModel):
    live_provider_declaration_version: Literal[1] = 1
    run_id: SafeId
    manifest_sha256: Sha256
    provider_package_sha256: Sha256
    provider_sha256: Sha256
    collector_sha256: Sha256
    validator_sha256: Sha256
    provider_snapshot_sha256: Sha256
    provider_package_artifact: CleanupEvidenceReference
    collector_artifact: CleanupEvidenceReference
    validator_artifact: CleanupEvidenceReference
    invocation_record: CleanupEvidenceReference
    interpreter_path: str
    interpreter_sha256: Sha256
    argv_contract: tuple[str, ...]
    operation_count: int = Field(ge=1, le=300)
    event_count: int = Field(ge=1, le=300)
    content_sha256_by_operation: dict[Sha256, Sha256]
    source_manifest: CleanupEvidenceReference
    fixture_catalog: CleanupEvidenceReference
    source_chunks: tuple[CleanupEvidenceReference, ...]
    durable_sidecar: CleanupEvidenceReference
    checkpoints: CleanupEvidenceReference
    pre_read_health: CleanupEvidenceReference
    execution_ledger: CleanupEvidenceReference
    offered_ledger: CleanupEvidenceReference
    observed: CleanupEvidenceReference
    reconciliation: CleanupEvidenceReference
    scope: Literal["live_collection"]

    @field_validator("source_chunks")
    @classmethod
    def source_chunks_present(cls, value):
        if not value:
            raise ValueError("live provider declaration requires source chunks")
        identities = [(item.root_id, item.path) for item in value]
        if len(identities) != len(set(identities)):
            raise ValueError("live provider declaration contains duplicate source chunks")
        return value

    @field_validator("content_sha256_by_operation")
    @classmethod
    def exact_content_count(cls, value):
        if not value:
            raise ValueError("provider content evidence must not be empty")
        return value


class WorkloadProviderDeclaration(LiveProviderDeclaration):
    live_provider_declaration_version: Literal[3] = 3
    permit: CleanupEvidenceReference
    provider_artifact: CleanupEvidenceReference
    runbook_artifact: CleanupEvidenceReference
    adapter_wrapper_artifact: CleanupEvidenceReference
    cleaner_wrapper_artifact: CleanupEvidenceReference
    permit_sha256: Sha256
    execution_plan_sha256: Sha256
    workload_truth_sha256: Sha256
    truth_binding_sha256: Sha256
    operation_indexes: tuple[int, ...]
    operation_ids: tuple[Sha256, ...]
    workload_evidence: CleanupEvidenceReference
    validated_code_head_sha256: GitObjectId
    validated_merge_commit_sha256: GitObjectId
    validated_tree_sha256: GitObjectId
    runbook_sha256: Sha256
    adapter_wrapper_sha256: Sha256
    cleaner_wrapper_sha256: Sha256
    lab_unvalidated_full_run: bool = False
    lab_waiver_statement: str | None = None


class WorkloadBoundedValidationAttestation(StrictModel):
    bounded_validation_manifest_version: Literal[3] = 3
    validation_run_id: SafeId
    permit_sha256: Sha256
    manifest_sha256: Sha256
    manifest_file_sha256: Sha256
    workload_config_file_sha256: Sha256
    execution_plan_sha256: Sha256
    execution_plan_file_sha256: Sha256
    workload_truth_sha256: Sha256
    workload_truth_file_sha256: Sha256
    truth_binding_sha256: Sha256
    truth_binding_file_sha256: Sha256
    fixture_catalog_sha256: Sha256
    fixture_catalog_file_sha256: Sha256
    semantic_view_sha256: Sha256
    operation_indexes: tuple[int, int, int]
    intent_ids: tuple[Sha256, Sha256, Sha256]
    operation_ids: tuple[Sha256, Sha256, Sha256]
    roles: tuple[
        Literal["ordinary"],
        Literal["repeated_range"],
        Literal["sequential_range"],
    ]
    reconciliation_sha256: Sha256
    provider_manifest_sha256: Sha256
    imported_manifest_sha256: Sha256
    cleaned_manifest_sha256: Sha256
    workload_evidence_sha256: Sha256
    validated_code_head_sha256: GitObjectId
    validated_merge_commit_sha256: GitObjectId
    validated_tree_sha256: GitObjectId
    provider_package_sha256: Sha256
    provider_sha256: Sha256
    collector_sha256: Sha256
    validator_sha256: Sha256
    runbook_sha256: Sha256
    adapter_wrapper_sha256: Sha256
    cleaner_wrapper_sha256: Sha256
    interpreter_path: str
    interpreter_sha256: Sha256
    completed_at: datetime
    expires_at: datetime
    status: Literal["passed"]

    _completed_utc = field_validator("completed_at")(_utc)
    _expires_utc = field_validator("expires_at")(_utc)

    @model_validator(mode="after")
    def valid_freshness(self):
        if self.completed_at >= self.expires_at:
            raise ValueError(
                "bounded attestation must complete before its expiry"
            )
        return self


class WorkloadLiveFinalizationDeclaration(StrictModel):
    live_finalization_version: Literal[3] = 3
    run_id: SafeId
    permit_sha256: Sha256
    manifest_sha256: Sha256
    execution_plan_sha256: Sha256
    workload_truth_sha256: Sha256
    truth_binding_sha256: Sha256
    semantic_view_sha256: Sha256
    reconciliation_sha256: Sha256
    provider_manifest_sha256: Sha256
    imported_manifest_sha256: Sha256
    cleaned_manifest_sha256: Sha256
    bounded_attestation_sha256: Sha256 | None = None
    lab_unvalidated_full_run: bool = False
    lab_waiver_statement: str | None = None
    validated_code_head_sha256: GitObjectId
    validated_merge_commit_sha256: GitObjectId
    validated_tree_sha256: GitObjectId


class DatasetArtifact(StrictModel):
    path: str
    rows: int | None = Field(default=None, ge=0)
    sha256: Sha256

    _path = field_validator("path")(_safe_relative)


class DatasetPeriod(StrictModel):
    start: datetime
    end: datetime

    _start_utc = field_validator("start")(_utc)
    _end_utc = field_validator("end")(_utc)


class LiveAdapterRuntime(StrictModel):
    adapter: Literal["pcache-chimera-import"]
    adapter_version: str
    python: str
    run_id: SafeId
    chimera_image_sha256: Sha256
    event_schema_sha256: Sha256
    event_sources_sha256: Sha256
    observed_sha256: Sha256
    run_manifest_sha256: Sha256
    offered_ledger_sha256: Sha256
    fixture_catalog_sha256: Sha256
    checkpoints_sha256: Sha256
    semantic_view_id: Literal["read-demand-v1"] | None = None
    semantic_view_sha256: Sha256 | None = None
    source_evidence_mode: Literal[
        "controlled_demand", "offline_simulated_fixture"
    ] | None = None

    @model_validator(mode="after")
    def semantic_view_binding(self):
        values = (
            self.semantic_view_id,
            self.semantic_view_sha256,
            self.source_evidence_mode,
        )
        if any(value is not None for value in values) and any(
            value is None for value in values
        ):
            raise ValueError("adapter semantic view fields must be complete")
        if self.semantic_view_id is not None:
            if self.semantic_view_sha256 != SEMANTIC_VIEW_SHA256:
                raise ValueError("adapter semantic view hash mismatch")
        return self


class LiveAdapterGenerator(StrictModel):
    name: Literal["pcache-datagen"]
    version: str
    git_sha: str
    runtime: LiveAdapterRuntime


class LiveAdapterCounts(StrictModel):
    events: Literal[300]
    dropped_events: Literal[0]
    files: int = Field(gt=0)
    actors: int = Field(ge=0)
    patterns: int = Field(ge=0)
    by_operation: dict[str, int]
    by_origin: dict[str, int]
    by_fault: dict[str, int]


class LiveAdapterManifest(StrictModel):
    manifest_version: Literal[1]
    dataset_id: str = Field(min_length=1)
    data_kind: Literal["synthetic"]
    generator: LiveAdapterGenerator
    event_schema_version: Literal[1]
    seed: Literal[0]
    preset: Literal["custom"]
    config_sha256: Sha256
    period: DatasetPeriod
    volumes: tuple[str, ...]
    counts: LiveAdapterCounts
    known_gaps: tuple[dict[str, Any], ...]
    files: tuple[DatasetArtifact, ...]
    created_at: datetime
    limitations: tuple[str, ...]

    _created_utc = field_validator("created_at")(_utc)

    @field_validator("known_gaps")
    @classmethod
    def no_known_gaps(cls, value):
        if value:
            raise ValueError("live adapter manifest must not report known gaps")
        return value


class LiveAdapterInputHashes(StrictModel):
    event_sources: Sha256
    observed: Sha256
    run_manifest: Sha256
    offered_ledger: Sha256
    fixture_catalog: Sha256
    checkpoints: Sha256


class LiveAdapterReportCounts(StrictModel):
    event_sources: int = Field(gt=0)
    raw_events: Literal[300]
    durable_events: Literal[300]
    approved_exclusions: Literal[0]
    offered_operations: Literal[300]


class LiveAdapterReport(StrictModel):
    schema_name: Literal["chimera_import_report_v2"] = Field(alias="schema")
    run_id: SafeId
    counts: LiveAdapterReportCounts
    event_quality: dict[str, Any]
    approved_exclusions: tuple[dict[str, Any], ...]
    known_gaps: tuple[dict[str, Any], ...]
    input_sha256: LiveAdapterInputHashes
    event_source_sha256: dict[str, Sha256]

    @field_validator("approved_exclusions", "known_gaps")
    @classmethod
    def no_exclusions_or_gaps(cls, value):
        if value:
            raise ValueError("live adapter report must not contain exclusions or gaps")
        return value


class BoundedAdapterRuntime(LiveAdapterRuntime):
    validation_permit_sha256: Sha256


class BoundedAdapterCounts(StrictModel):
    events: int = Field(ge=1, le=3)
    dropped_events: Literal[0]
    files: int = Field(gt=0)
    actors: int = Field(ge=0)
    patterns: int = Field(ge=0)
    by_operation: dict[str, int]
    by_origin: dict[str, int]
    by_fault: dict[str, int]


class BoundedAdapterGenerator(StrictModel):
    name: Literal["pcache-datagen"]
    version: str
    git_sha: str
    runtime: BoundedAdapterRuntime


class BoundedAdapterManifest(LiveAdapterManifest):
    generator: BoundedAdapterGenerator
    counts: BoundedAdapterCounts


class BoundedAdapterReportCounts(StrictModel):
    event_sources: int = Field(gt=0)
    raw_events: int = Field(ge=1, le=3)
    durable_events: int = Field(ge=1, le=3)
    approved_exclusions: Literal[0]
    offered_operations: int = Field(ge=1, le=3)


class BoundedAdapterInputHashes(LiveAdapterInputHashes):
    validation_permit: Sha256


class BoundedAdapterReport(LiveAdapterReport):
    counts: BoundedAdapterReportCounts
    input_sha256: BoundedAdapterInputHashes


class CleanParent(StrictModel):
    dataset_id: str = Field(min_length=1)
    manifest_sha256: Sha256


class CleanTool(StrictModel):
    name: Literal["pcache-prep"]
    version: str
    git_sha: str


class LiveCleanCounts(StrictModel):
    input_events: Literal[300]
    kept_events: Literal[0]
    excluded_by_reason: dict[str, int]
    open_ranges: Literal[0]
    opens: Literal[0]
    meta_events: Literal[0]
    catalog_miss_events: Literal[0]
    catalog_miss_files: Literal[0]
    late_reads_after_completion: Literal[0]
    opens_open_missing: Literal[0]
    opens_close_missing: Literal[0]
    opens_bytes_inconsistent: Literal[0]
    unexpected_reads_in_summary_mode: Literal[0]

    @model_validator(mode="after")
    def exact_synthetic_exclusion(self):
        if self.excluded_by_reason != {"origin_synthetic": 300}:
            raise ValueError("live clean counts must exclude all synthetic events")
        return self


class LiveCleanCoverage(StrictModel):
    hours: int = Field(gt=0)
    complete: int = Field(gt=0)
    incomplete: Literal[0]
    no_counters: Literal[0]
    skew_margin_s: int = Field(ge=0)
    assume_complete_without_counters: Literal[False]

    @model_validator(mode="after")
    def all_complete(self):
        if self.complete != self.hours:
            raise ValueError("live clean coverage must be fully complete")
        return self


class LiveCleanManifest(StrictModel):
    manifest_version: Literal[1]
    dataset_id: str = Field(min_length=1)
    data_kind: Literal["synthetic"]
    stage: Literal["clean"]
    parent: CleanParent
    tool: CleanTool
    config_sha256: Sha256
    period: DatasetPeriod
    counts: LiveCleanCounts
    coverage: LiveCleanCoverage
    files: tuple[DatasetArtifact, ...]
    created_at: datetime

    _created_utc = field_validator("created_at")(_utc)


class BoundedCleanCounts(LiveCleanCounts):
    input_events: int = Field(ge=1, le=3)

    @model_validator(mode="after")
    def exact_synthetic_exclusion(self):
        if self.excluded_by_reason != {
            "origin_synthetic": self.input_events
        }:
            raise ValueError("bounded clean counts must exclude all synthetic events")
        return self


class BoundedCleanManifest(LiveCleanManifest):
    counts: BoundedCleanCounts


class LiveCoverageRow(StrictModel):
    volume_id: str
    hour_start: datetime
    offered: int = Field(ge=0)
    emitted: int = Field(ge=0)
    dropped: Literal[0]
    sampled_out: Literal[0]
    loss_fraction: Literal[0.0]
    status: Literal["complete"]

    _hour_utc = field_validator("hour_start")(_utc)

    @model_validator(mode="after")
    def zero_loss(self):
        if self.offered != self.emitted:
            raise ValueError("coverage offered and emitted counts must match")
        return self


class AdapterLossRow(StrictModel):
    volume_id: str
    hour_start: datetime
    offered: int = Field(ge=0)
    emitted: int = Field(ge=0)
    dropped: Literal[0]
    sampled_out: Literal[0]
    duplicates: Literal[0]
    late: Literal[0]

    _hour_utc = field_validator("hour_start")(_utc)

    @model_validator(mode="after")
    def zero_loss(self):
        if self.offered != self.emitted:
            raise ValueError("adapter loss offered and emitted counts must match")
        return self


class LiveCleanReport(StrictModel):
    counts: LiveCleanCounts
    by_volume_day: tuple["VolumeDayEvidence", ...]
    excluded_by_reason: dict[str, int]
    exclusions_by_volume_day_reason: tuple["ExclusionEvidence", ...]
    duplicate_rate: float
    late_arrival_s: "LateArrivalEvidence"
    coverage_hours: dict[str, int]
    incomplete_windows: tuple[dict[str, Any], ...]
    opens: "OpenEvidence"
    successful_returned_bytes: int = Field(ge=0)
    distinct_bytes: int = Field(ge=0)
    catalog_misses: dict[str, Any]
    origin_mix: dict[str, int]
    app_mix: dict[str, int]
    opens_by_volume_hour: tuple[dict[str, Any], ...]
    opens_by_volume_weekday: tuple[dict[str, Any], ...]


class BoundedCleanReport(LiveCleanReport):
    counts: BoundedCleanCounts


class VolumeDayEvidence(StrictModel):
    volume_id: str
    date: str
    input: int = Field(ge=0)
    kept: int = Field(ge=0)
    excluded: int = Field(ge=0)


class ExclusionEvidence(StrictModel):
    volume_id: str
    date: str
    reason: str
    count: int = Field(ge=0)


class LateArrivalEvidence(StrictModel):
    p50: float | None = Field(default=None, ge=0)
    p90: float | None = Field(default=None, ge=0)
    p99: float | None = Field(default=None, ge=0)
    max: float | None = Field(default=None, ge=0)
    negative_count: Literal[0]

    @model_validator(mode="after")
    def ordered(self):
        values = [self.p50, self.p90, self.p99, self.max]
        if any(value is not None for value in values):
            if any(value is None for value in values) or values != sorted(values):
                raise ValueError("late-arrival quantiles must be complete and ordered")
        return self


class OpenEvidence(StrictModel):
    count: Literal[0]
    open_missing: Literal[0]
    close_missing: Literal[0]
    bytes_inconsistent: Literal[0]
    consumed_share: Literal[0.0]


class LiveFinalizationDeclaration(StrictModel):
    live_finalization_version: Literal[1] = 1
    run_id: SafeId
    manifest_sha256: Sha256
    reconciliation_sha256: Sha256
    validated_code_head_sha256: GitObjectId
    validated_merge_commit_sha256: GitObjectId
    validated_tree_sha256: GitObjectId
    executed_source_archive_sha256: Sha256
    provider_manifest: CleanupEvidenceReference
    adapter_manifest: CleanupEvidenceReference
    adapter_report: CleanupEvidenceReference
    cleaned_manifest: CleanupEvidenceReference
    cleaned_report: CleanupEvidenceReference
    cleanup_roots: tuple[CleanupRootEvidence, ...]

    @model_validator(mode="after")
    def exact_live_roots(self):
        kinds = {root.kind for root in self.cleanup_roots}
        expected = {
            "live_harness_evidence",
            "live_provider_evidence",
            "live_adapter_dataset",
            "live_clean_dataset",
        }
        if len(self.cleanup_roots) != 4 or kinds != expected:
            raise ValueError("live finalization must declare its four exact roots")
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


class WorkloadCollectionManifest(StrictModel):
    manifest_version: Literal[3] = 3
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
    execution_config_sha256: Sha256
    execution_plan_sha256: Sha256
    semantic_view_id: SafeId
    semantic_view_sha256: Sha256
    seed: int = Field(ge=0)
    start_time: datetime
    end_time: datetime
    planned_operation_count: int = Field(gt=0)
    max_concurrency: Literal[1] = 1
    checkpoint_interval_s: Literal[900] = 900
    checkpoint_tolerance_s: int = Field(default=30, ge=0, le=300)
    operation_schedule_tolerance_s: int = Field(default=30, ge=0, le=59)
    clock_skew_tolerance_s: int = Field(default=5, ge=0, le=30)
    pre_read_plus_read_guardrail_ms: int = Field(default=5000, gt=0)
    fixture_distribution: Literal["explicit_repeating_plan"] = "explicit_repeating_plan"
    range_distribution: Literal["explicit_repeating_plan"] = "explicit_repeating_plan"
    range_bytes: tuple[int, ...]
    range_offsets: tuple[int, ...]
    untagged_correlation_window_s: int = Field(default=120, gt=0)
    client_id: SafeId
    tag_mode: Literal["empty"] = "empty"
    tag_contract_status: Literal["proposed-unverified"] = "proposed-unverified"
    allowed_exclusion_reasons: tuple[str, ...] = ()
    evidence_mode: Literal["offline_simulated_fixture"] = "offline_simulated_fixture"
    software: SoftwareVersions
    stop_rules: StopRules
    retention: RetentionPolicy
    limitations: tuple[str, ...] = (
        "Offline simulated controlled workload; not live collection evidence.",
        "Synthetic READ rows are plumbing and planted-pattern diagnostics only.",
        "No completed read proves cache residency or performance benefit.",
    )

    _start_utc = field_validator("start_time")(_utc)
    _end_utc = field_validator("end_time")(_utc)

    @field_validator("range_bytes")
    @classmethod
    def valid_ranges(cls, value: tuple[int, ...]) -> tuple[int, ...]:
        if value != (4096,):
            raise ValueError("workload collection requires one 4096-byte range size")
        return value

    @field_validator("range_offsets")
    @classmethod
    def valid_offsets(cls, value: tuple[int, ...]) -> tuple[int, ...]:
        if not value or len(value) != len(set(value)) or any(item < 0 for item in value):
            raise ValueError("range_offsets must be unique nonnegative values")
        if any(item % 4096 for item in value):
            raise ValueError("range_offsets must be 4096-byte aligned")
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
    def controlled_run(self) -> WorkloadCollectionManifest:
        expected_run_ref = hashlib.sha256(self.run_id.encode("utf-8")).hexdigest()[:32]
        if self.run_ref != expected_run_ref:
            raise ValueError("run_ref must be sha256(run_id UTF-8)[:32]")
        duration = (self.end_time - self.start_time).total_seconds()
        if duration <= 0 or duration > 24 * 60 * 60:
            raise ValueError("workload duration must be in (0, 24 hours]")
        return self


CollectionRunManifest = CollectionManifest | WorkloadCollectionManifest


def parse_collection_manifest(value: str | bytes) -> CollectionRunManifest:
    def unique_object(pairs):
        result = {}
        for key, item in pairs:
            if key in result:
                raise ValueError(f"duplicate JSON key: {key}")
            result[key] = item
        return result

    document = json.loads(value, object_pairs_hook=unique_object)
    version = document.get("manifest_version") if isinstance(document, dict) else None
    if version == 2:
        return CollectionManifest.model_validate(document)
    if version == 3:
        return WorkloadCollectionManifest.model_validate(document)
    raise ValueError(f"unsupported collection manifest version: {version!r}")


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
    schedule_sha256: Sha256
    validation_report_sha256: Sha256
    deletion_inventory_sha256: Sha256
    root_dispositions: dict[CleanupRootKind, Literal["deleted"]]

    _delete_utc = field_validator("delete_by")(_utc)
    _completed_utc = field_validator("completed_at")(_utc)

    @field_validator("root_dispositions")
    @classmethod
    def all_roots_deleted(cls, value):
        expected = {
            "bounded_harness_evidence",
            "bounded_provider_evidence",
            "bounded_adapter_dataset",
            "bounded_clean_dataset",
            "live_harness_evidence",
            "live_provider_evidence",
            "live_adapter_dataset",
            "live_clean_dataset",
        }
        if set(value) != expected:
            raise ValueError("cleanup completion must account for all eight roots")
        return value

    @model_validator(mode="after")
    def completed_on_time(self):
        if self.completed_at > self.delete_by:
            raise ValueError("cleanup completion must not exceed delete_by")
        return self


class HistoricalCleanupScheduleRecord(StrictModel):
    historical_cleanup_schedule_version: Literal[1] = 1
    run_id: SafeId
    finalized_at: datetime
    delete_by: datetime
    cleanup_owner: str = Field(min_length=1)
    attestation_sha256: Sha256
    deletion_approval_sha256: Sha256
    deletion_inventory_sha256: Sha256
    root_actions: dict[HistoricalRootRole, Literal["delete", "retain"]]

    _finalized_utc = field_validator("finalized_at")(_utc)
    _delete_utc = field_validator("delete_by")(_utc)

    @model_validator(mode="after")
    def exact_historical_schedule(self):
        if self.delete_by != self.finalized_at + timedelta(days=7):
            raise ValueError("delete_by must equal finalized_at plus seven days")
        expected = set(HistoricalRootRole.__args__)
        if set(self.root_actions) != expected:
            raise ValueError("historical schedule must account for every root role")
        if self.root_actions["shared_provider"] != "retain":
            raise ValueError("historical schedule must retain shared provider evidence")
        if any(
            action != "delete"
            for role, action in self.root_actions.items()
            if role != "shared_provider"
        ):
            raise ValueError("historical collection roots must be scheduled for deletion")
        return self


class HistoricalCleanupCompletionRecord(StrictModel):
    historical_cleanup_completion_version: Literal[1] = 1
    run_id: SafeId
    delete_by: datetime
    completed_at: datetime
    cleanup_owner: str = Field(min_length=1)
    evidence: str = Field(min_length=1)
    schedule_sha256: Sha256
    attestation_sha256: Sha256
    deletion_approval_sha256: Sha256
    deletion_inventory_sha256: Sha256
    root_dispositions: dict[
        HistoricalRootRole,
        Literal["deleted", "retained_verified"],
    ]

    _delete_utc = field_validator("delete_by")(_utc)
    _completed_utc = field_validator("completed_at")(_utc)

    @model_validator(mode="after")
    def exact_historical_completion(self):
        expected = set(HistoricalRootRole.__args__)
        if set(self.root_dispositions) != expected:
            raise ValueError("historical completion must account for every root role")
        if self.root_dispositions["shared_provider"] != "retained_verified":
            raise ValueError("historical completion must verify retained provider evidence")
        if any(
            disposition != "deleted"
            for role, disposition in self.root_dispositions.items()
            if role != "shared_provider"
        ):
            raise ValueError("historical deletion roots must be deleted")
        if self.completed_at > self.delete_by:
            raise ValueError("historical cleanup completion must not exceed delete_by")
        return self


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

    @model_validator(mode="after")
    def consistent_success(self) -> ReconciliationReport:
        has_failures = bool(
            self.failures
            or self.duplicate_operation_ids
            or self.missing_operation_ids
            or self.unexpected_operation_ids
            or self.unknown_event_ids
        )
        if self.success == has_failures:
            raise ValueError("reconciliation success conflicts with failure details")
        return self


class WorkloadReconciliationReport(ReconciliationReport):
    reconciliation_version: Literal[3] = 3
    permit_sha256: Sha256
    execution_plan_sha256: Sha256
    operation_indexes: tuple[int, ...]
    operation_ids: tuple[Sha256, ...]
    workload_evidence_sha256: Sha256


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

    @field_validator("files")
    @classmethod
    def unique_source_paths(cls, value):
        if not value:
            raise ValueError("collector file manifest requires source files")
        paths = [item.raw_jsonl_path for item in value]
        if len(paths) != len(set(paths)):
            raise ValueError("collector file manifest contains duplicate source paths")
        return value
