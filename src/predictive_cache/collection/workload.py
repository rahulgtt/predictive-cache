"""Offline-only synthetic READ workload planning and recovery.

The execution contract intentionally contains only fixture/range READ intents.
Actor, session, project, popularity, co-access, and pattern labels live only in
``WorkloadTruth``. In particular, a synthetic session is oracle metadata: READ
evidence cannot prove OPEN, CLOSE, handle, or protocol-session lifecycle facts,
and this module never invents those facts from temporal proximity.
"""

from __future__ import annotations

import base64
import hashlib
import random
import uuid
from collections import Counter, defaultdict, deque
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Annotated, Any, Iterable, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from predictive_cache.datagen.config import Popularity
from predictive_cache.datagen.pipeline import component_rng
from predictive_cache.semantics.read_demand import (
    SEMANTIC_VIEW_ID,
    SEMANTIC_VIEW_SHA256,
)

from .io import (
    atomic_write_bytes,
    atomic_write_model,
    canonical_json,
    ensure_private_directory,
    sha256,
    write_jsonl,
)
from .models import (
    Checkpoint,
    CollectorFileManifest,
    CollectorSourceFile,
    DropCounters,
    ExecutionResult,
    FixtureCatalog,
    JsonlEvidence,
    LAB_FULL_RUN_WAIVER,
    ObservedEvent,
    OfferedOperation,
    WorkloadCollectionManifest,
    WorkloadCollectionPermit,
    WorkloadPermitProfile,
    WorkloadValidationRole,
)

Sha256 = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]
SafeId = Annotated[str, Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")]
StrictNonNegativeInt = Annotated[int, Field(ge=0, strict=True)]
StrictPositiveInt = Annotated[int, Field(gt=0, strict=True)]
PopularityTier = Literal["high", "medium", "low"]
WindowKind = Literal["burst", "quiet"]

DEFAULT_RANGE_OFFSETS = (0, 12288, 24576, 36864, 49152)
DEFAULT_TIER_MEMBERSHIP = (6, 18, 36)
_TIER_NAMES: tuple[PopularityTier, ...] = ("high", "medium", "low")
_TIER_PATTERN: tuple[PopularityTier, ...] = (
    "high",
    "medium",
    "low",
    "high",
    "high",
    "medium",
    "high",
    "high",
    "medium",
    "high",
)


class StrictFrozenModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, validate_default=True)


def _model_sha256(value: BaseModel) -> str:
    return hashlib.sha256(
        canonical_json(value.model_dump(mode="json")).encode("utf-8")
    ).hexdigest()


def canonical_model_file_sha256(value: BaseModel) -> str:
    """Hash the exact canonical bytes written by atomic_write_model."""

    payload = (
        canonical_json(value.model_dump(mode="json")) + "\n"
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


class WorkloadConfig(StrictFrozenModel):
    """Frozen controls for an offline candidate workload; defaults are five hours."""

    workload_config_version: Literal[1] = 1
    seed: StrictNonNegativeInt = 42
    fixture_count: Literal[60] = 60
    duration_ms: StrictPositiveInt = 10_800_000
    operation_count: StrictPositiveInt | None = None
    synthetic_actor_count: StrictPositiveInt = 10
    popularity_rate_per_actor_hour: Annotated[
        float, Field(gt=0, allow_inf_nan=False, strict=True)
    ] = float(Popularity().rate_per_user_hour)
    range_bytes: Literal[4096] = 4096
    range_offsets: tuple[StrictNonNegativeInt, ...] = DEFAULT_RANGE_OFFSETS
    popularity_membership: tuple[StrictPositiveInt, StrictPositiveInt, StrictPositiveInt] = (
        DEFAULT_TIER_MEMBERSHIP
    )
    coaccess_group_count: Literal[4] = 4
    checkpoint_interval_s: StrictPositiveInt = 900
    min_offer_gap_ms: StrictPositiveInt = 20_000
    short_window_ms: StrictPositiveInt = 60_000
    short_window_max_offers: StrictPositiveInt = 3
    long_window_ms: StrictPositiveInt = 900_000
    long_window_max_offers: StrictPositiveInt = 25
    untagged_correlation_window_s: StrictPositiveInt = 120
    operation_schedule_tolerance_s: StrictNonNegativeInt = 30
    simulated_durability_margin_ms: Literal[1000] = 1000

    @field_validator("range_offsets")
    @classmethod
    def aligned_unique_offsets(cls, value: tuple[int, ...]) -> tuple[int, ...]:
        if value != DEFAULT_RANGE_OFFSETS:
            raise ValueError(f"default candidate requires offsets {DEFAULT_RANGE_OFFSETS}")
        if len(set(value)) != len(value) or any(offset % 4096 for offset in value):
            raise ValueError("range offsets must be unique and 4096-byte aligned")
        return value

    @model_validator(mode="after")
    def exact_candidate_shape(self) -> WorkloadConfig:
        if self.popularity_membership != DEFAULT_TIER_MEMBERSHIP:
            raise ValueError("default candidate requires exact 6/18/36 tier membership")
        if sum(self.popularity_membership) != self.fixture_count:
            raise ValueError("popularity membership must cover every fixture")
        if self.short_window_ms != 60_000 or self.long_window_ms != 900_000:
            raise ValueError("candidate rate caps use exact 60s and 900s half-open windows")
        if self.short_window_max_offers != 3 or self.long_window_max_offers != 25:
            raise ValueError("candidate rate caps are exactly 3/60s and 25/900s")
        count = resolved_operation_count(self)
        if count < 50:
            raise ValueError("generated workload needs at least 50 operations")
        if self.duration_ms // count <= (
            self.operation_schedule_tolerance_s * 1000
            + self.simulated_durability_margin_ms
        ):
            raise ValueError("generated count leaves no safe final-offer margin")
        return self


def resolved_operation_count(config: WorkloadConfig) -> int:
    if config.operation_count is not None:
        return config.operation_count
    expected = (
        config.popularity_rate_per_actor_hour
        * config.synthetic_actor_count
        * config.duration_ms
        / 3_600_000
    )
    return int(component_rng(config.seed, "popularity").poisson(expected))


class WorkloadIntent(StrictFrozenModel):
    """One unique occurrence in a truth-free execution plan."""

    intent_version: Literal[1] = 1
    intent_id: Sha256
    operation: Literal["READ"] = "READ"
    relative_ms: StrictNonNegativeInt
    fixture_id: SafeId
    offset: StrictNonNegativeInt
    count: StrictPositiveInt

    @model_validator(mode="after")
    def aligned_range(self) -> WorkloadIntent:
        if self.offset % 4096 or self.count % 4096:
            raise ValueError("READ range must be 4096-byte aligned")
        return self


class WorkloadExecutionPlan(StrictFrozenModel):
    """Truth-free, stable execution contract containing only READ fixture ranges."""

    execution_plan_version: Literal[1] = 1
    workload_config_sha256: Sha256
    fixture_catalog_sha256: Sha256
    seed: StrictNonNegativeInt
    duration_ms: StrictPositiveInt
    planned_operation_count: StrictPositiveInt
    range_bytes: Literal[4096] = 4096
    range_offsets: tuple[StrictNonNegativeInt, ...] = DEFAULT_RANGE_OFFSETS
    min_offer_gap_ms: StrictPositiveInt
    short_window_ms: StrictPositiveInt
    short_window_max_offers: StrictPositiveInt
    long_window_ms: StrictPositiveInt
    long_window_max_offers: StrictPositiveInt
    untagged_correlation_window_s: StrictPositiveInt
    operation_schedule_tolerance_s: StrictNonNegativeInt
    simulated_durability_margin_ms: Literal[1000] = 1000
    semantic_view_id: SafeId | None = None
    semantic_view_sha256: Sha256 | None = None
    operation: Literal["READ"] = "READ"
    intents: tuple[WorkloadIntent, ...]

    @model_validator(mode="after")
    def execution_invariants(self) -> WorkloadExecutionPlan:
        if (self.semantic_view_id is None) != (self.semantic_view_sha256 is None):
            raise ValueError("semantic view ID and hash must be supplied together")
        if self.range_offsets != DEFAULT_RANGE_OFFSETS:
            raise ValueError("execution plan uses the frozen candidate offsets")
        if len(self.intents) != self.planned_operation_count:
            raise ValueError("planned operation count does not match intent entries")
        ids = [intent.intent_id for intent in self.intents]
        if len(ids) != len(set(ids)):
            raise ValueError("each intent occurrence must have a unique intent_id")
        times = [intent.relative_ms for intent in self.intents]
        if times != sorted(times) or len(times) != len(set(times)):
            raise ValueError("intent relative times must be strictly increasing")
        if any(right - left < self.min_offer_gap_ms for left, right in zip(times, times[1:])):
            raise ValueError("intent schedule violates the minimum offer gap")
        _validate_half_open_cap(times, self.short_window_ms, self.short_window_max_offers)
        _validate_half_open_cap(times, self.long_window_ms, self.long_window_max_offers)
        repeat_gap_ms = (
            self.untagged_correlation_window_s + self.operation_schedule_tolerance_s
        ) * 1000
        previous: dict[tuple[str, int, int], int] = {}
        for intent in self.intents:
            key = (intent.fixture_id, intent.offset, intent.count)
            if key in previous and intent.relative_ms - previous[key] <= repeat_gap_ms:
                raise ValueError(
                    "identical fixture/range repeats must exceed correlation plus "
                    "schedule tolerance"
                )
            previous[key] = intent.relative_ms
        if (
            times[-1]
            + self.operation_schedule_tolerance_s * 1000
            + self.simulated_durability_margin_ms
            >= self.duration_ms
        ):
            raise ValueError("last offer plus tolerance and durability margin must precede end")
        return self


class FixturePopularityTruth(StrictFrozenModel):
    fixture_id: SafeId
    tier: PopularityTier


class CoaccessGroupTruth(StrictFrozenModel):
    group_id: SafeId
    fixture_ids: tuple[SafeId, ...]

    @field_validator("fixture_ids")
    @classmethod
    def unique_group_members(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if len(value) < 3 or len(value) != len(set(value)):
            raise ValueError("co-access groups require at least three unique fixtures")
        return value


class WorkloadWindowTruth(StrictFrozenModel):
    kind: WindowKind
    start_relative_ms: StrictNonNegativeInt
    end_relative_ms: StrictPositiveInt

    @model_validator(mode="after")
    def ordered_window(self) -> WorkloadWindowTruth:
        if self.start_relative_ms >= self.end_relative_ms:
            raise ValueError("workload truth windows must be nonempty")
        if self.start_relative_ms % 3_600_000 == 0:
            raise ValueError("burst and quiet windows must not start on hour boundaries")
        return self


class IntentOracleTruth(StrictFrozenModel):
    """Oracle metadata for one intent; synthetic_session_id is not observed fact."""

    oracle_truth_version: Literal[1] = 1
    intent_id: Sha256
    fixture_id: SafeId
    popularity_tier: PopularityTier
    synthetic_actor_id: SafeId
    synthetic_session_id: SafeId = Field(
        description="Truth metadata only; never inferred from READ evidence."
    )
    synthetic_project_id: SafeId
    synthetic_pattern_id: SafeId
    fixture_occurrence_index: StrictNonNegativeInt
    sequential_offset_index: StrictNonNegativeInt
    coaccess_group_id: SafeId | None = None
    coaccess_step: Annotated[int, Field(ge=0, le=2, strict=True)] | None = None
    schedule_phase: Literal["steady", "burst"]

    @model_validator(mode="after")
    def coaccess_shape(self) -> IntentOracleTruth:
        if (self.coaccess_group_id is None) != (self.coaccess_step is None):
            raise ValueError("co-access group and step must be supplied together")
        return self


class WorkloadTruth(StrictFrozenModel):
    workload_truth_version: Literal[1] = 1
    execution_plan_sha256: Sha256
    fixture_popularity: tuple[FixturePopularityTruth, ...]
    coaccess_groups: tuple[CoaccessGroupTruth, ...]
    windows: tuple[WorkloadWindowTruth, ...]
    intent_truth: tuple[IntentOracleTruth, ...]
    lifecycle_invariant: Literal[
        "READ evidence cannot prove OPEN, CLOSE, handle, or protocol-session lifecycle facts."
    ] = "READ evidence cannot prove OPEN, CLOSE, handle, or protocol-session lifecycle facts."

    @model_validator(mode="after")
    def truth_invariants(self) -> WorkloadTruth:
        fixture_ids = [item.fixture_id for item in self.fixture_popularity]
        if len(fixture_ids) != len(set(fixture_ids)):
            raise ValueError("fixture popularity membership must be unique")
        group_ids = [item.group_id for item in self.coaccess_groups]
        if len(group_ids) != 4 or len(group_ids) != len(set(group_ids)):
            raise ValueError("workload truth requires four unique co-access groups")
        intent_ids = [item.intent_id for item in self.intent_truth]
        if len(intent_ids) != len(set(intent_ids)):
            raise ValueError("oracle truth must be unique per intent occurrence")
        return self


class WorkloadTruthBinding(StrictFrozenModel):
    truth_binding_version: Literal[1] = 1
    execution_plan_sha256: Sha256
    workload_truth_sha256: Sha256
    semantic_view_id: SafeId
    semantic_view_sha256: Sha256


class OfflineSimulatedRead(StrictFrozenModel):
    """A successful synthetic READ row with no protocol lifecycle fields."""

    read_row_version: Literal[1] = 1
    source_kind: Literal["offline_simulated_fixture"] = "offline_simulated_fixture"
    operation: Literal["READ"] = "READ"
    result: Literal["ok"] = "ok"
    origin: Literal["synthetic"] = "synthetic"
    run_id: SafeId
    execution_plan_sha256: Sha256
    intent_id: Sha256
    operation_id: Sha256
    fixture_id: SafeId
    volume_id: Annotated[str, Field(pattern=r"^[0-9a-f]{32}$")]
    file_id: Sha256
    offset: StrictNonNegativeInt
    count: StrictPositiveInt
    offered_at_utc: datetime
    durable_at_utc: datetime
    durability_delay_ms: StrictNonNegativeInt
    source_sequence: StrictPositiveInt

    @field_validator("offered_at_utc", "durable_at_utc")
    @classmethod
    def utc_timestamp(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() != timedelta(0):
            raise ValueError("offline simulated timestamps must be UTC-aware")
        return value

    @model_validator(mode="after")
    def simulated_timing(self) -> OfflineSimulatedRead:
        expected = self.offered_at_utc + timedelta(milliseconds=self.durability_delay_ms)
        if self.durable_at_utc != expected:
            raise ValueError("durability timestamp must equal offered time plus delay")
        if self.durability_delay_ms >= 1000:
            raise ValueError("offline durability delay must be below 1000ms")
        return self


class OfflineSimulatedCheckpoint(StrictFrozenModel):
    checkpoint_version: Literal[1] = 1
    source_kind: Literal["offline_simulated_fixture"] = "offline_simulated_fixture"
    operation: Literal["READ"] = "READ"
    run_id: SafeId
    execution_plan_sha256: Sha256
    scheduled_relative_ms: StrictNonNegativeInt
    durable_read_count: StrictNonNegativeInt


class OfflineSimulatedSourceManifest(StrictFrozenModel):
    source_manifest_version: Literal[1] = 1
    source_kind: Literal["offline_simulated_fixture"] = "offline_simulated_fixture"
    evidence_scope: Literal["offline_contract_fixture_only"] = "offline_contract_fixture_only"
    live_or_provider_evidence: Literal[False] = False
    operation: Literal["READ"] = "READ"
    run_id: SafeId
    execution_plan_sha256: Sha256
    offered_count: StrictNonNegativeInt
    successful_read_count: StrictNonNegativeInt
    checkpoint_relative_ms: tuple[StrictNonNegativeInt, ...]
    limitations: tuple[str, ...] = (
        "Offline simulated fixture; not live or provider evidence.",
        "Synthetic READ rows do not prove cache residency or performance.",
        "READ timing does not establish OPEN, CLOSE, handle, or session lifecycle.",
    )


class OfflineSimulatedSourceResult(StrictFrozenModel):
    result_version: Literal[1] = 1
    manifest: OfflineSimulatedSourceManifest
    raw_reads: tuple[OfflineSimulatedRead, ...]
    checkpoints: tuple[OfflineSimulatedCheckpoint, ...]
    observed_reads: tuple[OfflineSimulatedRead, ...]

    @model_validator(mode="after")
    def consistent_artifacts(self) -> OfflineSimulatedSourceResult:
        if self.raw_reads != self.observed_reads:
            raise ValueError("offline raw and observed READ fixtures must agree exactly")
        if self.manifest.successful_read_count != len(self.observed_reads):
            raise ValueError("source manifest successful READ count is inconsistent")
        return self


class RecoveryCounts(StrictFrozenModel):
    planned: StrictNonNegativeInt
    imported: StrictNonNegativeInt
    matched_unique: StrictNonNegativeInt
    missing: StrictNonNegativeInt
    unexpected: StrictNonNegativeInt
    duplicate: StrictNonNegativeInt


class TierQuotaRecovery(StrictFrozenModel):
    tier: PopularityTier
    planned: StrictNonNegativeInt
    realized: StrictNonNegativeInt


class TransitionRealization(StrictFrozenModel):
    coaccess_group_id: SafeId
    from_fixture_id: SafeId
    to_fixture_id: SafeId
    planned: StrictNonNegativeInt
    realized: StrictNonNegativeInt


class RecoveryLineage(StrictFrozenModel):
    execution_plan_sha256: Sha256
    workload_truth_sha256: Sha256
    imported_reads_sha256: Sha256
    imported_manifest_sha256: Sha256 | None = None
    cleaned_manifest_sha256: Sha256 | None = None
    cleaned_parent_manifest_sha256: Sha256 | None = None


class WorkloadRecoveryReport(StrictFrozenModel):
    """Aggregate diagnostics only; this is deliberately not a dataset manifest."""

    recovery_report_version: Literal[1] = 1
    report_kind: Literal["offline_workload_aggregate_recovery"] = (
        "offline_workload_aggregate_recovery"
    )
    counts: RecoveryCounts
    tier_quotas: tuple[TierQuotaRecovery, ...]
    transitions: tuple[TransitionRealization, ...]
    lineage: RecoveryLineage


def workload_config_sha256(config: WorkloadConfig) -> str:
    return _model_sha256(config)


def fixture_catalog_sha256(catalog: FixtureCatalog) -> str:
    return _model_sha256(catalog)


def execution_plan_sha256(plan: WorkloadExecutionPlan) -> str:
    return _model_sha256(plan)


def workload_truth_sha256(truth: WorkloadTruth) -> str:
    return _model_sha256(truth)


def workload_truth_binding_sha256(binding: WorkloadTruthBinding) -> str:
    return _model_sha256(binding)


def workload_manifest_sha256(manifest: WorkloadCollectionManifest) -> str:
    return _model_sha256(manifest)


def bind_workload_truth(
    plan: WorkloadExecutionPlan,
    truth: WorkloadTruth,
) -> WorkloadTruthBinding:
    """Create the canonical, separate plan/truth/semantic binding."""

    validate_plan_truth(plan, truth)
    if plan.semantic_view_id is None or plan.semantic_view_sha256 is None:
        raise ValueError("workload truth binding requires an explicit semantic view")
    return WorkloadTruthBinding(
        execution_plan_sha256=execution_plan_sha256(plan),
        workload_truth_sha256=workload_truth_sha256(truth),
        semantic_view_id=plan.semantic_view_id,
        semantic_view_sha256=plan.semantic_view_sha256,
    )


def _bounded_validation_selection(
    plan: WorkloadExecutionPlan,
    truth: WorkloadTruth,
) -> tuple[int, int, int]:
    """Choose the first relation-complete ordinary/repeat/sequential triple."""

    validate_plan_truth(plan, truth)
    truth_by_id = {item.intent_id: item for item in truth.intent_truth}
    candidates: list[tuple[int, int, int]] = []
    for ordinary_index, ordinary in enumerate(plan.intents):
        ordinary_truth = truth_by_id[ordinary.intent_id]
        if not (
            ordinary_truth.fixture_occurrence_index == 0
            and ordinary_truth.coaccess_group_id is None
            and ordinary_truth.schedule_phase == "steady"
        ):
            continue
        for repeated_index in range(ordinary_index + 1, len(plan.intents)):
            repeated = plan.intents[repeated_index]
            repeated_truth = truth_by_id[repeated.intent_id]
            if (
                repeated.fixture_id,
                repeated.offset,
                repeated.count,
            ) != (
                ordinary.fixture_id,
                ordinary.offset,
                ordinary.count,
            ):
                continue
            for sequential_index in range(repeated_index + 1, len(plan.intents)):
                sequential = plan.intents[sequential_index]
                sequential_truth = truth_by_id[sequential.intent_id]
                if (
                    sequential.fixture_id == repeated.fixture_id
                    and sequential_truth.fixture_occurrence_index
                    == repeated_truth.fixture_occurrence_index + 1
                    and sequential_truth.sequential_offset_index
                    == (
                        repeated_truth.sequential_offset_index + 1
                    )
                    % len(plan.range_offsets)
                ):
                    candidates.append(
                        (ordinary_index, repeated_index, sequential_index)
                    )
                    break
            if candidates and candidates[-1][0] == ordinary_index:
                break
    if not candidates:
        raise ValueError(
            "workload plan lacks a relation-complete ordinary/repeat/sequential triple"
        )
    return min(candidates)


def _bounded_validation_schedule(
    config: WorkloadConfig,
) -> tuple[int, int, int]:
    repeat_delay_ms = (
        (
            config.untagged_correlation_window_s
            + config.operation_schedule_tolerance_s
        )
        * 1000
        + config.min_offer_gap_ms
    )
    values = (
        0,
        repeat_delay_ms,
        repeat_delay_ms + config.min_offer_gap_ms,
    )
    _validate_half_open_cap(
        list(values),
        config.short_window_ms,
        config.short_window_max_offers,
    )
    _validate_half_open_cap(
        list(values),
        config.long_window_ms,
        config.long_window_max_offers,
    )
    return values


def select_workload_permit_operations(
    run_id: str,
    plan: WorkloadExecutionPlan,
    truth: WorkloadTruth,
    *,
    profile: WorkloadPermitProfile,
    config: WorkloadConfig,
    validation_operations: int | None = None,
) -> tuple[
    tuple[int, ...],
    tuple[str, ...],
    tuple[str, ...],
    tuple[WorkloadValidationRole, ...],
    tuple[int, ...],
    tuple[int, ...],
]:
    """Select relation-complete bounded operations or the complete plan."""

    if profile == "bounded_validation":
        if (
            type(validation_operations) is not int
            or not 1 <= validation_operations <= 3
        ):
            raise ValueError(
                "bounded validation requires validation_operations in 1..3"
            )
        indexes = _bounded_validation_selection(
            plan,
            truth,
        )[:validation_operations]
        roles: tuple[WorkloadValidationRole, ...] = (
            "ordinary",
            "repeated_range",
            "sequential_range",
        )[:validation_operations]
        execution_relative_ms = _bounded_validation_schedule(
            config,
        )[:validation_operations]
    elif profile == "full_collection":
        if validation_operations is not None:
            raise ValueError(
                "full collection does not accept a validation operation count"
            )
        indexes = tuple(range(len(plan.intents)))
        roles = tuple("ordinary" for _ in indexes)
        execution_relative_ms = tuple(
            intent.relative_ms for intent in plan.intents
        )
    else:
        raise ValueError(f"unsupported workload permit profile: {profile!r}")

    plan_hash = execution_plan_sha256(plan)
    return (
        indexes,
        tuple(plan.intents[index].intent_id for index in indexes),
        tuple(
            run_bound_operation_id(
                run_id,
                plan_hash,
                plan.intents[index].intent_id,
            )
            for index in indexes
        ),
        roles,
        tuple(plan.intents[index].relative_ms for index in indexes),
        execution_relative_ms,
    )


def build_workload_collection_permit(
    manifest: WorkloadCollectionManifest,
    config: WorkloadConfig,
    plan: WorkloadExecutionPlan,
    truth: WorkloadTruth,
    truth_binding: WorkloadTruthBinding,
    catalog: FixtureCatalog,
    *,
    permit_id: str,
    profile: WorkloadPermitProfile,
    expires_at: datetime,
    validated_code_head_sha256: str,
    validated_merge_commit_sha256: str,
    validated_tree_sha256: str,
    provider_package_sha256: str,
    provider_sha256: str,
    collector_sha256: str,
    validator_sha256: str,
    runbook_sha256: str,
    adapter_wrapper_sha256: str,
    cleaner_wrapper_sha256: str,
    interpreter_path: str,
    interpreter_sha256: str,
    execution_start_time: datetime,
    validation_operations: int | None = None,
    bounded_validation_attestation_sha256: str | None = None,
    lab_unvalidated_full_run: bool = False,
) -> WorkloadCollectionPermit:
    """Build and immediately cross-check a manifest-v3 workload permit."""

    (
        indexes,
        intent_ids,
        operation_ids,
        roles,
        original_relative_ms,
        execution_relative_ms,
    ) = (
        select_workload_permit_operations(
            manifest.run_id,
            plan,
            truth,
            profile=profile,
            config=config,
            validation_operations=validation_operations,
        )
    )
    permit = WorkloadCollectionPermit(
        permit_id=permit_id,
        profile=profile,
        run_id=manifest.run_id,
        manifest_sha256=workload_manifest_sha256(manifest),
        manifest_file_sha256=canonical_model_file_sha256(manifest),
        workload_config_sha256=workload_config_sha256(config),
        workload_config_file_sha256=canonical_model_file_sha256(config),
        execution_plan_sha256=execution_plan_sha256(plan),
        execution_plan_file_sha256=canonical_model_file_sha256(plan),
        workload_truth_sha256=workload_truth_sha256(truth),
        workload_truth_file_sha256=canonical_model_file_sha256(truth),
        truth_binding_sha256=workload_truth_binding_sha256(truth_binding),
        truth_binding_file_sha256=canonical_model_file_sha256(truth_binding),
        fixture_catalog_sha256=fixture_catalog_sha256(catalog),
        fixture_catalog_file_sha256=canonical_model_file_sha256(catalog),
        semantic_view_sha256=SEMANTIC_VIEW_SHA256,
        validated_code_head_sha256=validated_code_head_sha256,
        validated_merge_commit_sha256=validated_merge_commit_sha256,
        validated_tree_sha256=validated_tree_sha256,
        provider_package_sha256=provider_package_sha256,
        provider_sha256=provider_sha256,
        collector_sha256=collector_sha256,
        validator_sha256=validator_sha256,
        runbook_sha256=runbook_sha256,
        adapter_wrapper_sha256=adapter_wrapper_sha256,
        cleaner_wrapper_sha256=cleaner_wrapper_sha256,
        interpreter_path=interpreter_path,
        interpreter_sha256=interpreter_sha256,
        bounded_validation_attestation_sha256=(
            bounded_validation_attestation_sha256
        ),
        lab_unvalidated_full_run=lab_unvalidated_full_run,
        lab_waiver_statement=(
            LAB_FULL_RUN_WAIVER if lab_unvalidated_full_run else None
        ),
        operation_indexes=indexes,
        intent_ids=intent_ids,
        operation_ids=operation_ids,
        roles=roles,
        original_relative_ms=original_relative_ms,
        execution_start_time=execution_start_time,
        execution_relative_ms=execution_relative_ms,
        schedule_tolerance_s=manifest.operation_schedule_tolerance_s,
        expires_at=expires_at,
    )
    validate_workload_collection_permit(
        permit,
        manifest,
        config,
        plan,
        truth,
        truth_binding,
        catalog,
    )
    return permit


def validate_workload_collection_permit(
    permit: WorkloadCollectionPermit,
    manifest: WorkloadCollectionManifest,
    config: WorkloadConfig,
    plan: WorkloadExecutionPlan,
    truth: WorkloadTruth,
    truth_binding: WorkloadTruthBinding,
    catalog: FixtureCatalog,
) -> None:
    """Cross-check every canonical object and selected operation in a v3 permit."""

    validate_plan_truth(plan, truth)
    config_hash = workload_config_sha256(config)
    plan_hash = execution_plan_sha256(plan)
    truth_hash = workload_truth_sha256(truth)
    binding_hash = workload_truth_binding_sha256(truth_binding)
    catalog_hash = fixture_catalog_sha256(catalog)
    if (
        permit.status != "approved"
        or permit.expires_at.tzinfo is None
        or permit.expires_at.utcoffset() != timedelta(0)
        or permit.run_id != manifest.run_id
        or permit.manifest_sha256 != workload_manifest_sha256(manifest)
        or permit.manifest_file_sha256
        != canonical_model_file_sha256(manifest)
        or permit.workload_config_sha256 != config_hash
        or permit.workload_config_file_sha256
        != canonical_model_file_sha256(config)
        or permit.execution_plan_sha256 != plan_hash
        or permit.execution_plan_file_sha256
        != canonical_model_file_sha256(plan)
        or permit.workload_truth_sha256 != truth_hash
        or permit.workload_truth_file_sha256
        != canonical_model_file_sha256(truth)
        or permit.truth_binding_sha256 != binding_hash
        or permit.truth_binding_file_sha256
        != canonical_model_file_sha256(truth_binding)
        or permit.fixture_catalog_sha256 != catalog_hash
        or permit.fixture_catalog_file_sha256
        != canonical_model_file_sha256(catalog)
        or permit.semantic_view_sha256 != SEMANTIC_VIEW_SHA256
    ):
        raise ValueError("workload permit canonical binding mismatch")
    if (
        manifest.execution_config_sha256 != config_hash
        or manifest.execution_plan_sha256 != plan_hash
        or manifest.fixture_catalog_sha256 != catalog_hash
        or manifest.semantic_view_id != SEMANTIC_VIEW_ID
        or manifest.semantic_view_sha256 != SEMANTIC_VIEW_SHA256
        or manifest.semantic_view_id != plan.semantic_view_id
        or manifest.semantic_view_sha256 != plan.semantic_view_sha256
        or manifest.planned_operation_count != len(plan.intents)
        or manifest.seed != config.seed
        or manifest.end_time - manifest.start_time
        != timedelta(milliseconds=config.duration_ms)
        or manifest.checkpoint_interval_s != config.checkpoint_interval_s
        or manifest.operation_schedule_tolerance_s
        != config.operation_schedule_tolerance_s
        or manifest.range_bytes != (config.range_bytes,)
        or manifest.range_offsets != config.range_offsets
        or manifest.untagged_correlation_window_s
        != config.untagged_correlation_window_s
        or plan.workload_config_sha256 != config_hash
        or plan.fixture_catalog_sha256 != catalog_hash
        or plan.seed != config.seed
        or plan.duration_ms != config.duration_ms
        or plan.planned_operation_count != resolved_operation_count(config)
        or plan.range_bytes != config.range_bytes
        or plan.range_offsets != config.range_offsets
        or plan.min_offer_gap_ms != config.min_offer_gap_ms
        or plan.short_window_ms != config.short_window_ms
        or plan.short_window_max_offers != config.short_window_max_offers
        or plan.long_window_ms != config.long_window_ms
        or plan.long_window_max_offers != config.long_window_max_offers
        or plan.untagged_correlation_window_s
        != config.untagged_correlation_window_s
        or plan.operation_schedule_tolerance_s
        != config.operation_schedule_tolerance_s
        or plan.simulated_durability_margin_ms
        != config.simulated_durability_margin_ms
    ):
        raise ValueError("workload manifest does not match config, plan, and catalog")
    if (
        truth_binding.execution_plan_sha256 != plan_hash
        or truth_binding.workload_truth_sha256 != truth_hash
        or truth_binding.semantic_view_id != manifest.semantic_view_id
        or truth_binding.semantic_view_sha256 != SEMANTIC_VIEW_SHA256
    ):
        raise ValueError("workload truth binding mismatch")

    expected = select_workload_permit_operations(
        manifest.run_id,
        plan,
        truth,
        profile=permit.profile,
        config=config,
        validation_operations=(
            len(permit.operation_indexes)
            if permit.profile == "bounded_validation"
            else None
        ),
    )
    actual = (
        permit.operation_indexes,
        permit.intent_ids,
        permit.operation_ids,
        permit.roles,
        permit.original_relative_ms,
        permit.execution_relative_ms,
    )
    if actual != expected:
        raise ValueError("workload permit selected operation binding mismatch")
    expected_start = (
        permit.execution_start_time
        if permit.profile == "bounded_validation"
        else manifest.start_time
    )
    if (
        permit.execution_start_time != expected_start
        or permit.schedule_tolerance_s
        != manifest.operation_schedule_tolerance_s
        or permit.execution_start_time < manifest.start_time
        or permit.execution_start_time
        + timedelta(milliseconds=permit.execution_relative_ms[-1])
        + timedelta(seconds=permit.schedule_tolerance_s)
        >= manifest.end_time
        or permit.execution_start_time >= permit.expires_at
        or permit.execution_start_time
        + timedelta(milliseconds=permit.execution_relative_ms[-1])
        + timedelta(seconds=permit.schedule_tolerance_s)
        >= permit.expires_at
    ):
        raise ValueError("workload permit execution schedule mismatch")


def run_bound_operation_id(
    run_id: str,
    execution_plan_hash: str,
    intent_id: str,
) -> str:
    """Bind one planned occurrence to a run without changing the v2 helper."""

    payload = {
        "execution_plan_sha256": execution_plan_hash,
        "intent_id": intent_id,
        "run_id": run_id,
    }
    return hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()


def checkpoint_schedule(duration_ms: int, checkpoint_interval_s: int) -> tuple[int, ...]:
    """Return start, full intervals strictly before end, and the exact end once."""

    if type(duration_ms) is not int or duration_ms <= 0:
        raise ValueError("duration_ms must be a positive integer")
    if type(checkpoint_interval_s) is not int or checkpoint_interval_s <= 0:
        raise ValueError("checkpoint_interval_s must be a positive integer")
    interval_ms = checkpoint_interval_s * 1000
    values = list(range(0, duration_ms, interval_ms))
    if not values or values[-1] != duration_ms:
        values.append(duration_ms)
    return tuple(values)


checkpoint_schedule_ms = checkpoint_schedule


def build_default_workload(
    catalog: FixtureCatalog,
    config: WorkloadConfig | None = None,
    *,
    semantic_view_id: str | None = None,
    semantic_view_sha256: str | None = None,
) -> tuple[WorkloadExecutionPlan, WorkloadTruth]:
    """Build the deterministic 60-fixture offline candidate and separate truth."""

    cfg = config or WorkloadConfig()
    fixtures = sorted(catalog.fixtures, key=lambda fixture: fixture.fixture_id)
    random.Random(cfg.seed).shuffle(fixtures)
    if len(fixtures) != cfg.fixture_count:
        raise ValueError("default workload requires exactly 60 fixtures")
    if any(
        fixture.size_bytes < max(cfg.range_offsets) + cfg.range_bytes for fixture in fixtures
    ):
        raise ValueError("every fixture must cover all planned aligned ranges")
    if (semantic_view_id is None) != (semantic_view_sha256 is None):
        raise ValueError("semantic view ID and hash must be explicit together")
    operation_count = resolved_operation_count(cfg)

    membership: dict[PopularityTier, list[Any]] = {}
    cursor = 0
    for tier, size in zip(_TIER_NAMES, cfg.popularity_membership, strict=True):
        membership[tier] = fixtures[cursor : cursor + size]
        cursor += size

    times, windows = _candidate_schedule(cfg, operation_count)
    config_hash = workload_config_sha256(cfg)
    catalog_hash = fixture_catalog_sha256(catalog)
    tier_counters: Counter[PopularityTier] = Counter()
    fixture_occurrences: Counter[str] = Counter()
    fixture_positions = {
        fixture.fixture_id: index for index, fixture in enumerate(fixtures)
    }
    intents: list[WorkloadIntent] = []
    oracle_rows: list[IntentOracleTruth] = []
    group_members: dict[int, set[str]] = defaultdict(set)

    for index, (relative_ms, tier) in enumerate(
        zip(times, _tier_sequence(operation_count), strict=True)
    ):
        cycle, position = divmod(index, len(_TIER_PATTERN))
        group = cycle % cfg.coaccess_group_count
        if position == 0:
            fixture = membership["high"][group]
        elif position == 1:
            fixture = membership["medium"][group]
        elif position == 2:
            fixture = membership["low"][cycle % len(membership["low"])]
        else:
            pool = membership[tier]
            fixture = pool[tier_counters[tier] % len(pool)]
        tier_counters[tier] += 1

        occurrence = fixture_occurrences[fixture.fixture_id]
        offset_index = occurrence % len(cfg.range_offsets)
        offset = cfg.range_offsets[offset_index]
        fixture_occurrences[fixture.fixture_id] += 1
        identity_payload = {
            "fixture_id": fixture.fixture_id,
            "occurrence_index": occurrence,
            "operation_index": index,
            "offset": offset,
            "relative_ms": relative_ms,
            "workload_config_sha256": config_hash,
        }
        intent_id = hashlib.sha256(
            canonical_json(identity_payload).encode("utf-8")
        ).hexdigest()
        intent = WorkloadIntent(
            intent_id=intent_id,
            relative_ms=relative_ms,
            fixture_id=fixture.fixture_id,
            offset=offset,
            count=cfg.range_bytes,
        )
        intents.append(intent)

        in_burst = any(
            window.kind == "burst"
            and window.start_relative_ms <= relative_ms < window.end_relative_ms
            for window in windows
        )
        complete_sequence = position < 3 and cycle * len(_TIER_PATTERN) + 2 < operation_count
        coaccess_group_id = f"coaccess-{group}" if complete_sequence else None
        if coaccess_group_id is not None:
            group_members[group].add(fixture.fixture_id)
        oracle_rows.append(
            IntentOracleTruth(
                intent_id=intent_id,
                fixture_id=fixture.fixture_id,
                popularity_tier=tier,
                synthetic_actor_id=(
                    f"actor-{fixture_positions[fixture.fixture_id] % cfg.synthetic_actor_count:02d}"
                ),
                synthetic_session_id=f"session-{cycle:03d}",
                synthetic_project_id=f"project-{fixtures.index(fixture) % 8:02d}",
                synthetic_pattern_id=(
                    coaccess_group_id
                    if coaccess_group_id is not None
                    else f"{'burst' if in_burst else 'popularity'}-{tier}"
                ),
                fixture_occurrence_index=occurrence,
                sequential_offset_index=offset_index,
                coaccess_group_id=coaccess_group_id,
                coaccess_step=position if complete_sequence else None,
                schedule_phase="burst" if in_burst else "steady",
            )
        )

    plan = WorkloadExecutionPlan(
        workload_config_sha256=config_hash,
        fixture_catalog_sha256=catalog_hash,
        seed=cfg.seed,
        duration_ms=cfg.duration_ms,
        planned_operation_count=operation_count,
        min_offer_gap_ms=cfg.min_offer_gap_ms,
        short_window_ms=cfg.short_window_ms,
        short_window_max_offers=cfg.short_window_max_offers,
        long_window_ms=cfg.long_window_ms,
        long_window_max_offers=cfg.long_window_max_offers,
        untagged_correlation_window_s=cfg.untagged_correlation_window_s,
        operation_schedule_tolerance_s=cfg.operation_schedule_tolerance_s,
        semantic_view_id=semantic_view_id,
        semantic_view_sha256=semantic_view_sha256,
        intents=tuple(intents),
    )
    plan_hash = execution_plan_sha256(plan)
    popularity_truth = tuple(
        FixturePopularityTruth(fixture_id=fixture.fixture_id, tier=tier)
        for tier in _TIER_NAMES
        for fixture in membership[tier]
    )
    groups = tuple(
        CoaccessGroupTruth(
            group_id=f"coaccess-{group}",
            fixture_ids=tuple(sorted(group_members[group])),
        )
        for group in range(cfg.coaccess_group_count)
    )
    truth = WorkloadTruth(
        execution_plan_sha256=plan_hash,
        fixture_popularity=popularity_truth,
        coaccess_groups=groups,
        windows=windows,
        intent_truth=tuple(oracle_rows),
    )
    validate_plan_truth(plan, truth)
    _validate_built_workload(cfg, operation_count, plan, truth)
    return plan, truth


build_workload = build_default_workload


def materialize_offered_operations(
    run_id: str,
    start_time: datetime,
    plan: WorkloadExecutionPlan,
    catalog: FixtureCatalog,
    *,
    client_id: str = "offline-workload",
) -> tuple[OfferedOperation, ...]:
    """Materialize simulated offered rows without touching a filesystem or network."""

    if start_time.tzinfo is None or start_time.utcoffset() != timedelta(0):
        raise ValueError("start_time must be UTC-aware")
    if fixture_catalog_sha256(catalog) != plan.fixture_catalog_sha256:
        raise ValueError("fixture catalog does not match execution plan")
    fixtures = {fixture.fixture_id: fixture for fixture in catalog.fixtures}
    plan_hash = execution_plan_sha256(plan)
    rows = []
    for index, intent in enumerate(plan.intents):
        fixture = fixtures[intent.fixture_id]
        rows.append(
            OfferedOperation(
                operation_id=run_bound_operation_id(run_id, plan_hash, intent.intent_id),
                operation_index=index,
                run_id=run_id,
                fixture_id=fixture.fixture_id,
                offered_at_utc=start_time + timedelta(milliseconds=intent.relative_ms),
                offered_at_monotonic_ns=intent.relative_ms * 1_000_000,
                volume_id=fixture.volume_id,
                file_id=fixture.file_id,
                dir_id=fixture.dir_id,
                export_id=fixture.export_id,
                relative_path=fixture.relative_path,
                file_size=fixture.size_bytes,
                offset=intent.offset,
                count=intent.count,
                client_id=client_id,
                tag="",
                timing_mode="simulated",
            )
        )
    return tuple(rows)


def materialize_offline_execution_results(
    plan: WorkloadExecutionPlan,
    offered: Iterable[OfferedOperation],
) -> tuple[ExecutionResult, ...]:
    """Create deterministic successful dry-run results for offline fixture use."""

    offered_rows = tuple(offered)
    if len(offered_rows) != len(plan.intents):
        raise ValueError("offered rows must cover the complete execution plan")
    plan_hash = execution_plan_sha256(plan)
    results = []
    for index, (intent, operation) in enumerate(
        zip(plan.intents, offered_rows, strict=True)
    ):
        expected_id = run_bound_operation_id(
            operation.run_id,
            plan_hash,
            intent.intent_id,
        )
        if (
            operation.operation_id != expected_id
            or operation.operation_index != index
            or operation.fixture_id != intent.fixture_id
            or operation.offset != intent.offset
            or operation.count != intent.count
        ):
            raise ValueError("offered row does not match the execution plan")
        spacing = (
            plan.intents[index + 1].relative_ms - intent.relative_ms
            if index + 1 < len(plan.intents)
            else plan.simulated_durability_margin_ms
        )
        limit = min(1000, spacing)
        delay_ms = _deterministic_delay_ms(intent.intent_id, limit)
        results.append(
            ExecutionResult(
                operation_id=operation.operation_id,
                completed_at_utc=operation.offered_at_utc
                + timedelta(milliseconds=delay_ms),
                status="ok",
                bytes_read=intent.count,
                latency_ms=delay_ms,
                executor_kind="dry-run",
                filesystem_type="none",
            )
        )
    return tuple(results)


def build_offline_simulated_source(
    plan: WorkloadExecutionPlan,
    offered: Iterable[OfferedOperation],
    execution: Iterable[ExecutionResult],
    *,
    checkpoint_interval_s: int = 900,
) -> OfflineSimulatedSourceResult:
    """Build in-memory offline raw/checkpoint/observed READ artifacts only."""

    offered_rows = tuple(offered)
    execution_rows = tuple(execution)
    if len(offered_rows) != len(plan.intents) or len(execution_rows) != len(plan.intents):
        raise ValueError("offline source inputs must cover the complete plan")
    plan_hash = execution_plan_sha256(plan)
    reads = []
    run_ids = {row.run_id for row in offered_rows}
    if len(run_ids) != 1:
        raise ValueError("offered rows must share one run_id")
    run_id = next(iter(run_ids))
    for index, (intent, operation, result) in enumerate(
        zip(plan.intents, offered_rows, execution_rows, strict=True)
    ):
        expected_id = run_bound_operation_id(run_id, plan_hash, intent.intent_id)
        if operation.operation_id != expected_id or result.operation_id != expected_id:
            raise ValueError("offered/execution row does not match run-bound intent identity")
        if (
            operation.fixture_id != intent.fixture_id
            or operation.offset != intent.offset
            or operation.count != intent.count
            or result.status != "ok"
            or result.bytes_read != intent.count
        ):
            raise ValueError("offline source accepts exact successful READ rows only")
        delay_ms = round(
            (result.completed_at_utc - operation.offered_at_utc).total_seconds() * 1000
        )
        spacing = (
            plan.intents[index + 1].relative_ms - intent.relative_ms
            if index + 1 < len(plan.intents)
            else plan.duration_ms - intent.relative_ms
        )
        if delay_ms < 0 or delay_ms >= 1000 or delay_ms >= spacing:
            raise ValueError("offline durability delay must be nonnegative and below spacing")
        reads.append(
            OfflineSimulatedRead(
                run_id=run_id,
                execution_plan_sha256=plan_hash,
                intent_id=intent.intent_id,
                operation_id=expected_id,
                fixture_id=intent.fixture_id,
                volume_id=operation.volume_id,
                file_id=operation.file_id,
                offset=intent.offset,
                count=intent.count,
                offered_at_utc=operation.offered_at_utc,
                durable_at_utc=result.completed_at_utc,
                durability_delay_ms=delay_ms,
                source_sequence=index + 1,
            )
        )
    schedule = checkpoint_schedule(plan.duration_ms, checkpoint_interval_s)
    start_time = offered_rows[0].offered_at_utc - timedelta(
        milliseconds=plan.intents[0].relative_ms
    )
    checkpoints = tuple(
        OfflineSimulatedCheckpoint(
            run_id=run_id,
            execution_plan_sha256=plan_hash,
            scheduled_relative_ms=scheduled,
            durable_read_count=sum(
                read.durable_at_utc
                <= start_time + timedelta(milliseconds=scheduled)
                for read in reads
            ),
        )
        for scheduled in schedule
    )
    manifest = OfflineSimulatedSourceManifest(
        run_id=run_id,
        execution_plan_sha256=plan_hash,
        offered_count=len(offered_rows),
        successful_read_count=len(reads),
        checkpoint_relative_ms=schedule,
    )
    read_tuple = tuple(reads)
    return OfflineSimulatedSourceResult(
        manifest=manifest,
        raw_reads=read_tuple,
        checkpoints=checkpoints,
        observed_reads=read_tuple,
    )


def write_offline_importer_fixture(
    out_dir: Path,
    manifest: WorkloadCollectionManifest,
    plan: WorkloadExecutionPlan,
    catalog: FixtureCatalog,
    offered: Iterable[OfferedOperation],
    execution: Iterable[ExecutionResult],
) -> dict[str, Path]:
    """Write importer-compatible, explicitly offline simulated source evidence."""

    root = ensure_private_directory(Path(out_dir))
    if execution_plan_sha256(plan) != manifest.execution_plan_sha256:
        raise ValueError("execution plan does not match workload manifest")
    if fixture_catalog_sha256(catalog) != manifest.fixture_catalog_sha256:
        raise ValueError("fixture catalog does not match workload manifest")
    offered_rows = tuple(offered)
    execution_rows = tuple(execution)
    simulated = build_offline_simulated_source(
        plan,
        offered_rows,
        execution_rows,
        checkpoint_interval_s=manifest.checkpoint_interval_s,
    )
    if simulated.manifest.run_id != manifest.run_id:
        raise ValueError("offline simulated source run_id mismatch")

    fixtures = {fixture.fixture_id: fixture for fixture in catalog.fixtures}
    pod_uid = str(uuid.UUID("33333333-3333-4333-8333-333333333333"))
    producer_epoch = str(uuid.UUID("11111111-1111-4111-8111-111111111111"))
    pod_name = "offline-simulator-0"
    node_name = "offline-node-0"
    relative_raw = Path("collector") / pod_uid / producer_epoch / "events-00000.jsonl"
    raw_path = root / relative_raw
    raw_records = []
    observed = []
    for index, (intent, operation, result, read) in enumerate(
        zip(plan.intents, offered_rows, execution_rows, simulated.raw_reads, strict=True)
    ):
        fixture = fixtures[intent.fixture_id]
        event_id = str(uuid.UUID(int=int(operation.operation_id[:32], 16)))
        server_operation_id = hashlib.sha256(
            f"offline-server:{operation.operation_id}".encode()
        ).hexdigest()
        event_time_ns = _epoch_ns(read.durable_at_utc)
        raw_records.append(
            {
                "schema": "nfs4_read_v2",
                "local_id": event_id,
                "producer_epoch": producer_epoch,
                "producer_sequence": str(index + 1),
                "event_time_ns": str(event_time_ns),
                "sink_time_ns": str(event_time_ns),
                "target_created_ns": "1",
                "volume_id": fixture.volume_id,
                "file_id": fixture.file_id,
                "identity_valid": True,
                "export_name_b64": _base64url(fixture.export_path.encode()),
                "export_id": fixture.export_id,
                "target_fh": _base64url(fixture.fixture_id.encode()),
                "operation_id": server_operation_id,
                "client_ref": hashlib.sha256(operation.client_id.encode()).hexdigest(),
                "session_ref": "",
                "compound_op_index": 0,
                "slot_id": 0,
                "sequence_id": index,
                "compound_tag_b64": "",
                "compound_tag_original_length": 0,
                "compound_tag_truncated": False,
                "compound_tag_id": hashlib.sha256(b"").hexdigest(),
                "origin_claim": "unknown",
                "origin_trust": "client_asserted",
                "requested_offset": str(intent.offset),
                "requested_count": intent.count,
                "returned_bytes": result.bytes_read,
                "nfs_status": 0,
                "outcome": "success",
                "eof": False,
                "file_size_valid": True,
                "file_size": str(fixture.size_bytes),
            }
        )
        observed.append(
            ObservedEvent(
                operation_id=operation.operation_id,
                event_id=event_id,
                pod_uid=pod_uid,
                producer_epoch=producer_epoch,
                producer_sequence=index + 1,
                server_operation_id=server_operation_id,
                sink_time_ns=event_time_ns,
                volume_id=fixture.volume_id,
                file_id=fixture.file_id,
                offset=intent.offset,
                count=intent.count,
                durable_at=read.durable_at_utc,
                status="durable",
                tag=None,
            )
        )
    raw_payload = "".join(canonical_json(row) + "\n" for row in raw_records).encode()
    atomic_write_bytes(raw_path, raw_payload)
    raw_evidence = JsonlEvidence(
        path=relative_raw.as_posix(),
        size_bytes=raw_path.stat().st_size,
        sha256=sha256(raw_path),
    )
    source_manifest = CollectorFileManifest(
        run_id=manifest.run_id,
        files=(
            CollectorSourceFile(
                raw_jsonl_path=relative_raw.as_posix(),
                sha256=raw_evidence.sha256,
                pod_name=pod_name,
                pod_uid=pod_uid,
                node_name=node_name,
                producer_epoch=producer_epoch,
            ),
        ),
    )
    event_sources_path = root / "event-sources.json"
    observed_path = root / "observed.jsonl"
    checkpoints_path = root / "checkpoints.jsonl"
    offline_manifest_path = root / "offline-simulated-source.json"
    atomic_write_model(event_sources_path, source_manifest)
    write_jsonl(observed_path, observed)

    checkpoints = []
    for relative_ms in checkpoint_schedule(
        plan.duration_ms, manifest.checkpoint_interval_s
    ):
        scheduled_at = manifest.start_time + timedelta(milliseconds=relative_ms)
        synced = sum(row.durable_at < scheduled_at for row in observed)
        checkpoints.append(
            Checkpoint(
                run_id=manifest.run_id,
                scheduled_at=scheduled_at,
                captured_at=scheduled_at,
                pod_name=pod_name,
                pod_uid=pod_uid,
                node_name=node_name,
                producer_epoch=producer_epoch,
                restart_count=0,
                restart_explained=False,
                last_enqueued_sequence=synced,
                last_synced_sequence=synced,
                synced_count=synced,
                queue_depth=0,
                worker_in_flight=0,
                delivery_clean=1,
                barrier_ready=1,
                drops=DropCounters(
                    queue=0,
                    encode=0,
                    sink=0,
                    rollback=0,
                    rotation=0,
                ),
                active_jsonl=raw_evidence,
                backup_jsonl=None,
                collector_sink_healthy=True,
                storage_healthy=True,
                cluster_healthy=True,
                volume_healthy=True,
                export_healthy=True,
                parse_errors=0,
                identity_mismatches=0,
                checksum_mismatches=0,
                foreground_read_errors=0,
                max_observed_latency_ms=max(
                    (row.latency_ms for row in execution_rows), default=0
                ),
            )
        )
    write_jsonl(checkpoints_path, checkpoints)
    atomic_write_model(offline_manifest_path, simulated.manifest)
    return {
        "event_sources": event_sources_path,
        "observed": observed_path,
        "checkpoints": checkpoints_path,
        "offline_source": offline_manifest_path,
        "raw": raw_path,
    }


def aggregate_recovery(
    plan: WorkloadExecutionPlan,
    truth: WorkloadTruth,
    imported_successful_reads: Iterable[OfflineSimulatedRead | dict[str, Any]],
    *,
    imported_manifest_sha256: str | None = None,
    cleaned_manifest_sha256: str | None = None,
    cleaned_parent_manifest_sha256: str | None = None,
) -> WorkloadRecoveryReport:
    """Aggregate successful READ realization without emitting a dataset manifest."""

    plan_hash = execution_plan_sha256(plan)
    validate_plan_truth(plan, truth)
    rows = tuple(_coerce_imported_read(row) for row in imported_successful_reads)
    planned_by_id = {intent.intent_id: intent for intent in plan.intents}
    planned_ids = set(planned_by_id)
    truth_by_intent = {item.intent_id: item for item in truth.intent_truth}
    if set(truth_by_intent) != planned_ids:
        raise ValueError("oracle truth must cover exactly the planned intent occurrences")
    counts = Counter(row.intent_id for row in rows)
    matched_rows = [
        row
        for row in rows
        if row.intent_id in planned_by_id
        and row.execution_plan_sha256 == plan_hash
        and row.operation_id
        == run_bound_operation_id(row.run_id, plan_hash, row.intent_id)
        and row.fixture_id == planned_by_id[row.intent_id].fixture_id
        and row.offset == planned_by_id[row.intent_id].offset
        and row.count == planned_by_id[row.intent_id].count
    ]
    matched = {row.intent_id for row in matched_rows}
    unexpected_rows = len(rows) - sum(counts[intent_id] for intent_id in matched)

    planned_tiers = Counter(item.popularity_tier for item in truth.intent_truth)
    realized_tiers = Counter(
        truth_by_intent[intent_id].popularity_tier
        for intent_id in matched
        if intent_id in truth_by_intent
    )
    quotas = tuple(
        TierQuotaRecovery(
            tier=tier,
            planned=planned_tiers[tier],
            realized=realized_tiers[tier],
        )
        for tier in _TIER_NAMES
    )

    planned_transitions: Counter[tuple[str, str, str]] = Counter()
    realized_transitions: Counter[tuple[str, str, str]] = Counter()
    by_session: dict[tuple[str, str], list[IntentOracleTruth]] = defaultdict(list)
    for item in truth.intent_truth:
        if item.coaccess_group_id is not None:
            by_session[(item.coaccess_group_id, item.synthetic_session_id)].append(item)
    for (group_id, _session_id), items in by_session.items():
        ordered = sorted(items, key=lambda item: item.coaccess_step)
        for before, after in zip(ordered, ordered[1:]):
            key = (group_id, before.fixture_id, after.fixture_id)
            planned_transitions[key] += 1
            if before.intent_id in matched and after.intent_id in matched:
                realized_transitions[key] += 1
    transitions = tuple(
        TransitionRealization(
            coaccess_group_id=key[0],
            from_fixture_id=key[1],
            to_fixture_id=key[2],
            planned=planned_transitions[key],
            realized=realized_transitions[key],
        )
        for key in sorted(planned_transitions)
    )
    imported_payload = [
        row.model_dump(mode="json")
        for row in sorted(rows, key=lambda item: (item.intent_id, item.source_sequence))
    ]
    return WorkloadRecoveryReport(
        counts=RecoveryCounts(
            planned=len(plan.intents),
            imported=len(rows),
            matched_unique=len(matched),
            missing=len(planned_ids - matched),
            unexpected=unexpected_rows,
            duplicate=sum(count - 1 for count in counts.values() if count > 1),
        ),
        tier_quotas=quotas,
        transitions=transitions,
        lineage=RecoveryLineage(
            execution_plan_sha256=plan_hash,
            workload_truth_sha256=workload_truth_sha256(truth),
            imported_reads_sha256=hashlib.sha256(
                canonical_json(imported_payload).encode("utf-8")
            ).hexdigest(),
            imported_manifest_sha256=imported_manifest_sha256,
            cleaned_manifest_sha256=cleaned_manifest_sha256,
            cleaned_parent_manifest_sha256=cleaned_parent_manifest_sha256,
        ),
    )


recover_workload = aggregate_recovery
operation_id = run_bound_operation_id
execution_config_sha256 = workload_config_sha256
canonical_model_sha256 = _model_sha256
WorkloadIntentEntry = WorkloadIntent
PerIntentOracleTruth = IntentOracleTruth
AggregateRecoveryReport = WorkloadRecoveryReport
ImportedSuccessfulRead = OfflineSimulatedRead
SimulatedSourceManifest = OfflineSimulatedSourceManifest
SimulatedSourceResult = OfflineSimulatedSourceResult


def validate_plan_truth(
    plan: WorkloadExecutionPlan,
    truth: WorkloadTruth,
) -> None:
    if truth.execution_plan_sha256 != execution_plan_sha256(plan):
        raise ValueError("truth lineage does not match the execution plan")
    intent_by_id = {intent.intent_id: intent for intent in plan.intents}
    truth_by_id = {item.intent_id: item for item in truth.intent_truth}
    if set(truth_by_id) != set(intent_by_id):
        raise ValueError("oracle truth must cover exactly the planned intent occurrences")
    tier_by_fixture = {
        item.fixture_id: item.tier for item in truth.fixture_popularity
    }
    group_members = {
        group.group_id: set(group.fixture_ids) for group in truth.coaccess_groups
    }
    occurrences: Counter[str] = Counter()
    grouped: dict[tuple[str, str], list[IntentOracleTruth]] = defaultdict(list)
    for intent in plan.intents:
        item = truth_by_id[intent.intent_id]
        occurrence = occurrences[intent.fixture_id]
        if (
            item.fixture_id != intent.fixture_id
            or item.popularity_tier != tier_by_fixture.get(intent.fixture_id)
            or item.fixture_occurrence_index != occurrence
            or item.sequential_offset_index != occurrence % len(plan.range_offsets)
            or intent.offset != plan.range_offsets[item.sequential_offset_index]
        ):
            raise ValueError("oracle truth differs from its execution-plan intent")
        occurrences[intent.fixture_id] += 1
        if item.coaccess_group_id is not None:
            if item.fixture_id not in group_members.get(item.coaccess_group_id, set()):
                raise ValueError("co-access truth references a fixture outside its group")
            grouped[
                (item.coaccess_group_id, item.synthetic_session_id)
            ].append(item)
    relative_by_id = {
        intent.intent_id: intent.relative_ms for intent in plan.intents
    }
    for items in grouped.values():
        ordered = sorted(items, key=lambda item: item.coaccess_step)
        if [item.coaccess_step for item in ordered] != [0, 1, 2]:
            raise ValueError("co-access sequences require complete ordered steps 0/1/2")
        times = [relative_by_id[item.intent_id] for item in ordered]
        if times != sorted(times):
            raise ValueError("co-access sequence timing contradicts its planted order")


def _coerce_imported_read(
    value: OfflineSimulatedRead | dict[str, Any],
) -> OfflineSimulatedRead:
    if isinstance(value, OfflineSimulatedRead):
        return value
    return OfflineSimulatedRead.model_validate(value)


def _tier_sequence(operation_count: int) -> tuple[PopularityTier, ...]:
    cycles, remainder = divmod(operation_count, len(_TIER_PATTERN))
    return _TIER_PATTERN * cycles + _TIER_PATTERN[:remainder]


def _candidate_schedule(
    config: WorkloadConfig,
    operation_count: int,
) -> tuple[tuple[int, ...], tuple[WorkloadWindowTruth, ...]]:
    interval_ms = config.duration_ms // operation_count
    times = [index * interval_ms for index in range(operation_count)]
    windows = []
    indexes = tuple(
        min(operation_count - 5, max(1, round(fraction * operation_count)))
        for fraction in (0.057, 0.29, 0.523, 0.757)
    )
    if len(set(indexes)) != 4:
        raise ValueError("generated count cannot place four distinct burst windows")
    for index in indexes:
        base = index * interval_ms
        times[index : index + 5] = [
            base,
            base + 20_000,
            base + 40_000,
            base + 4 * interval_ms - 20_000,
            base + 5 * interval_ms - 20_000,
        ]
        windows.extend(
            (
                WorkloadWindowTruth(
                    kind="burst",
                    start_relative_ms=base,
                    end_relative_ms=base + 60_000,
                ),
                WorkloadWindowTruth(
                    kind="quiet",
                    start_relative_ms=base + 60_000,
                    end_relative_ms=base + 4 * interval_ms - 20_000,
                ),
            )
        )
    return tuple(times), tuple(windows)


def _validate_half_open_cap(times: list[int], window_ms: int, cap: int) -> None:
    active: deque[int] = deque()
    for current in times:
        while active and active[0] <= current - window_ms:
            active.popleft()
        active.append(current)
        if len(active) > cap:
            raise ValueError(f"offer schedule exceeds {cap} in half-open {window_ms}ms")


def _validate_built_workload(
    config: WorkloadConfig,
    operation_count: int,
    plan: WorkloadExecutionPlan,
    truth: WorkloadTruth,
) -> None:
    tier_members = Counter(item.tier for item in truth.fixture_popularity)
    if tuple(tier_members[tier] for tier in _TIER_NAMES) != config.popularity_membership:
        raise AssertionError("built popularity membership is inconsistent")
    tier_operations = Counter(item.popularity_tier for item in truth.intent_truth)
    expected_tiers = Counter(_tier_sequence(operation_count))
    if tier_operations != expected_tiers:
        raise AssertionError("built popularity operation quotas are inconsistent")
    if {item.intent_id for item in plan.intents} != {
        item.intent_id for item in truth.intent_truth
    }:
        raise AssertionError("execution plan and oracle truth intent identities differ")
    quiet = [window for window in truth.windows if window.kind == "quiet"]
    if any(
        window.start_relative_ms <= intent.relative_ms < window.end_relative_ms
        for window in quiet
        for intent in plan.intents
    ):
        raise AssertionError("quiet truth window contains an offered READ")


def _deterministic_delay_ms(intent_id: str, limit: int) -> int:
    if limit <= 0:
        raise ValueError("deterministic delay requires positive spacing")
    return int(intent_id[:16], 16) % limit


def _base64url(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode().rstrip("=")


def _epoch_ns(value: datetime) -> int:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError("timestamp must be UTC-aware")
    delta = value - datetime(1970, 1, 1, tzinfo=UTC)
    return (
        (delta.days * 86_400 + delta.seconds) * 1_000_000_000
        + delta.microseconds * 1000
    )
