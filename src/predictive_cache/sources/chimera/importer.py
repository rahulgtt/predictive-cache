"""Convert controlled Chimera v2 evidence into the prep source contract."""

from __future__ import annotations

import hashlib
import importlib.metadata
import json
import os
import platform
import shutil
import tempfile
from collections import Counter, defaultdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from predictive_cache.collection.harness import planned_operations
from predictive_cache.collection.io import strict_json_loads
from predictive_cache.collection.models import (
    AdapterValidationPermit,
    Checkpoint,
    CollectionManifest,
    CollectorFileManifest,
    FixtureCatalog,
    FixtureEntry,
    ObservedEvent,
    OfferedOperation,
    WorkloadCollectionManifest,
    WorkloadCollectionPermit,
    WorkloadEvidenceBinding,
    parse_collection_manifest,
)
from predictive_cache.collection.provenance import (
    APPROVED_COLLECTOR_SHA256,
    APPROVED_PROVIDER_PACKAGE_SHA256,
    APPROVED_PROVIDER_SHA256,
    APPROVED_VALIDATION_RUNBOOK_SHA256,
    APPROVED_VALIDATOR_SHA256,
)
from predictive_cache.collection.workload import (
    WorkloadConfig,
    WorkloadExecutionPlan,
    WorkloadTruthBinding,
    canonical_model_file_sha256,
    checkpoint_schedule,
    execution_plan_sha256,
    fixture_catalog_sha256,
    resolved_operation_count,
    run_bound_operation_id,
    workload_config_sha256,
    workload_manifest_sha256,
    workload_truth_binding_sha256,
)
from predictive_cache.datagen.io import canonical_json, sha256, source_revision, write_table
from predictive_cache.datagen.schema import Manifest, normalize_rows
from predictive_cache.semantics.read_demand import (
    SEMANTIC_VIEW_ID,
    SEMANTIC_VIEW_SHA256,
)

from .contract import (
    ChimeraEvent,
    parse_json,
    parse_jsonl,
    read_bytes,
)

STATUS_RESULTS = {0: "ok", 1: "eacces", 2: "enoent", 5: "eio", 13: "eacces", 22: "einval"}


def _ns_datetime(value: int) -> datetime:
    try:
        return datetime(1970, 1, 1, tzinfo=UTC) + timedelta(microseconds=value // 1000)
    except OverflowError as error:
        raise ValueError("nanosecond timestamp is outside the supported UTC range") from error


def _hour_start(value: datetime) -> datetime:
    return value.replace(minute=0, second=0, microsecond=0)


def _catalog_hash(catalog: FixtureCatalog) -> str:
    payload = canonical_json(catalog.model_dump(mode="json")).encode()
    return hashlib.sha256(payload).hexdigest()


def _validate_catalog(
    catalog: FixtureCatalog,
    manifest: CollectionManifest | WorkloadCollectionManifest,
) -> dict[tuple[str, str], FixtureEntry]:
    if _catalog_hash(catalog) != manifest.fixture_catalog_sha256:
        raise ValueError("fixture catalog checksum does not match the run manifest")
    fixtures = {(row.volume_id, row.file_id): row for row in catalog.fixtures}
    for row in catalog.fixtures:
        if row.volume_id != manifest.volume_id:
            raise ValueError("fixture volume does not match the run manifest")
        if row.export_id != manifest.export_id:
            raise ValueError("fixture export identity does not match the run manifest")
    return fixtures


def _validate_ledger(
    rows: list[OfferedOperation],
    manifest: CollectionManifest | WorkloadCollectionManifest,
    fixtures: dict[tuple[str, str], FixtureEntry],
    expected_validation_operation_ids: tuple[str, ...] | None = None,
    execution_plan: WorkloadExecutionPlan | None = None,
    workload_permit: WorkloadCollectionPermit | None = None,
) -> dict[str, OfferedOperation]:
    expected_count = (
        len(workload_permit.operation_ids)
        if workload_permit is not None
        else len(expected_validation_operation_ids)
        if expected_validation_operation_ids is not None
        else manifest.planned_operation_count
        if isinstance(manifest, WorkloadCollectionManifest)
        else manifest.offered_count
    )
    if len(rows) != expected_count:
        raise ValueError("offered ledger count does not match the run manifest")
    by_id: dict[str, OfferedOperation] = {}
    indexes = []
    ranges: dict[tuple[str, str], list[tuple[int, int]]] = defaultdict(list)
    for selected_position, row in enumerate(rows):
        if row.operation_id in by_id:
            raise ValueError("offered ledger operation IDs must be unique")
        by_id[row.operation_id] = row
        indexes.append(row.operation_index)
        if row.run_id != manifest.run_id:
            raise ValueError("offered operation run_id mismatch")
        if not manifest.start_time <= row.offered_at_utc < manifest.end_time:
            raise ValueError("offered operation timestamp falls outside the run")
        fixture = fixtures.get((row.volume_id, row.file_id))
        if fixture is None or any(
            (
                row.fixture_id != fixture.fixture_id,
                row.dir_id != fixture.dir_id,
                row.export_id != fixture.export_id,
                row.relative_path != fixture.relative_path,
                row.file_size != fixture.size_bytes,
            )
        ):
            raise ValueError("offered operation does not match its fixture catalog entry")
        if row.offset + row.count > fixture.size_bytes:
            raise ValueError("offered operation extends beyond the fixture")
        if row.count != manifest.range_bytes[0] or row.offset not in manifest.range_offsets:
            raise ValueError("offered operation range is absent from the frozen manifest")
        if isinstance(manifest, WorkloadCollectionManifest):
            if execution_plan is None or row.operation_index >= len(execution_plan.intents):
                raise ValueError("workload ledger lacks a matching execution-plan intent")
            intent = execution_plan.intents[row.operation_index]
            expected_operation_id = run_bound_operation_id(
                manifest.run_id,
                manifest.execution_plan_sha256,
                intent.intent_id,
            )
            planned_at = (
                workload_permit.execution_start_time
                + timedelta(
                    milliseconds=workload_permit.execution_relative_ms[
                        selected_position
                    ]
                )
                if workload_permit is not None
                else manifest.start_time + timedelta(milliseconds=intent.relative_ms)
            )
            if (
                row.operation_id != expected_operation_id
                or row.fixture_id != intent.fixture_id
                or row.offset != intent.offset
                or row.count != intent.count
                or abs((row.offered_at_utc - planned_at).total_seconds())
                > manifest.operation_schedule_tolerance_s
            ):
                raise ValueError("offered operation differs from the execution plan")
        else:
            for start, end in ranges[(row.volume_id, row.file_id)]:
                if row.offset < end and start < row.offset + row.count:
                    raise ValueError(
                        "offered operation ranges must be unique and non-overlapping"
                    )
        ranges[(row.volume_id, row.file_id)].append((row.offset, row.offset + row.count))
        expected_tag = (
            f"pc1:d:{manifest.run_ref}:{row.operation_id[:32]}:{row.operation_id[32:]}"
            if manifest.tag_mode == "pc1:d"
            else ""
        )
        if row.tag != expected_tag:
            raise ValueError("offered operation correlation tag mismatch")
        if row.client_id != manifest.client_id:
            raise ValueError("offered operation client_id mismatch")
    expected_indexes = (
        list(workload_permit.operation_indexes)
        if workload_permit is not None
        else list(range(expected_count))
    )
    if indexes != expected_indexes:
        if workload_permit is not None:
            raise ValueError(
                "offered operation indexes differ from the permitted selection"
            )
        raise ValueError("offered operation indexes must be contiguous from zero")
    if isinstance(manifest, WorkloadCollectionManifest):
        minimum = timedelta(
            seconds=(
                manifest.untagged_correlation_window_s
                + manifest.operation_schedule_tolerance_s
            )
        )
        previous: dict[tuple[str, str, int, int], datetime] = {}
        for row in rows:
            key = (row.volume_id, row.file_id, row.offset, row.count)
            if key in previous and row.offered_at_utc - previous[key] <= minimum:
                raise ValueError(
                    "identical workload ranges must exceed correlation plus "
                    "schedule tolerance"
                )
            previous[key] = row.offered_at_utc
        if execution_plan is None:
            raise ValueError("workload ledger requires its execution plan")
        actual_ms = [
            int(row.offered_at_utc.timestamp() * 1000)
            for row in rows
        ]
        if any(
            right - left < execution_plan.min_offer_gap_ms
            for left, right in zip(actual_ms, actual_ms[1:])
        ):
            raise ValueError(
                "actual workload offer gap is below the frozen minimum"
            )
        for window_ms, cap in (
            (
                execution_plan.short_window_ms,
                execution_plan.short_window_max_offers,
            ),
            (
                execution_plan.long_window_ms,
                execution_plan.long_window_max_offers,
            ),
        ):
            active: list[int] = []
            for current in actual_ms:
                active = [
                    value
                    for value in active
                    if value > current - window_ms
                ]
                active.append(current)
                if len(active) > cap:
                    raise ValueError(
                        f"actual workload offers exceed {cap} in "
                        f"half-open {window_ms}ms"
                    )
    if expected_validation_operation_ids is not None:
        catalog = FixtureCatalog(fixtures=tuple(fixtures.values()))
        expected_prefix = tuple(
            planned_operations(manifest, catalog)[
                : len(expected_validation_operation_ids)
            ]
        )
        if tuple(item[4] for item in expected_prefix) != (
            expected_validation_operation_ids
        ):
            raise ValueError(
                "validation permit operation IDs differ from deterministic prefix"
            )
        for row, expected in zip(rows, expected_prefix, strict=True):
            index, fixture, offset, count, operation_id = expected
            if (
                row.operation_index,
                row.operation_id,
                row.fixture_id,
                row.volume_id,
                row.file_id,
                row.dir_id,
                row.export_id,
                row.relative_path,
                row.file_size,
                row.offset,
                row.count,
            ) != (
                index,
                operation_id,
                fixture.fixture_id,
                fixture.volume_id,
                fixture.file_id,
                fixture.dir_id,
                fixture.export_id,
                fixture.relative_path,
                fixture.size_bytes,
                offset,
                count,
            ):
                raise ValueError(
                    "offered ledger does not match the permitted deterministic prefix"
                )
    return by_id


def _workload_collection_permit(
    path: Path,
    data: bytes,
    manifest: WorkloadCollectionManifest,
    manifest_bytes: bytes,
    config: WorkloadConfig,
    config_bytes: bytes,
    plan: WorkloadExecutionPlan,
    plan_bytes: bytes,
    truth_binding: WorkloadTruthBinding,
    truth_binding_bytes: bytes,
    catalog: FixtureCatalog,
    catalog_bytes: bytes,
    *,
    evaluated_at: datetime | None = None,
) -> WorkloadCollectionPermit:
    permit = parse_json(data, path, WorkloadCollectionPermit)
    canonical = (
        canonical_json(permit.model_dump(mode="json")) + "\n"
    ).encode("utf-8")
    if data != canonical:
        raise ValueError("workload collection permit must use exact canonical bytes")

    plan_hash = execution_plan_sha256(plan)
    config_hash = workload_config_sha256(config)
    binding_hash = workload_truth_binding_sha256(truth_binding)
    catalog_hash = fixture_catalog_sha256(catalog)
    if (
        permit.expires_at <= (evaluated_at or datetime.now(UTC))
        or permit.run_id != manifest.run_id
        or permit.manifest_sha256 != workload_manifest_sha256(manifest)
        or permit.manifest_file_sha256
        != hashlib.sha256(manifest_bytes).hexdigest()
        or permit.workload_config_sha256 != config_hash
        or permit.workload_config_file_sha256
        != hashlib.sha256(config_bytes).hexdigest()
        or permit.execution_plan_sha256 != plan_hash
        or permit.execution_plan_file_sha256
        != hashlib.sha256(plan_bytes).hexdigest()
        or permit.truth_binding_sha256 != binding_hash
        or permit.truth_binding_file_sha256
        != hashlib.sha256(truth_binding_bytes).hexdigest()
        or permit.fixture_catalog_sha256 != catalog_hash
        or permit.fixture_catalog_file_sha256
        != hashlib.sha256(catalog_bytes).hexdigest()
        or permit.semantic_view_sha256 != SEMANTIC_VIEW_SHA256
    ):
        raise ValueError("workload collection permit canonical binding mismatch")
    if (
        permit.manifest_file_sha256 != canonical_model_file_sha256(manifest)
        or permit.workload_config_file_sha256
        != canonical_model_file_sha256(config)
        or permit.execution_plan_file_sha256
        != canonical_model_file_sha256(plan)
        or permit.truth_binding_file_sha256
        != canonical_model_file_sha256(truth_binding)
        or permit.fixture_catalog_file_sha256
        != canonical_model_file_sha256(catalog)
        or manifest.execution_config_sha256 != config_hash
        or manifest.execution_plan_sha256 != plan_hash
        or manifest.fixture_catalog_sha256 != catalog_hash
        or manifest.semantic_view_id != SEMANTIC_VIEW_ID
        or manifest.semantic_view_sha256 != SEMANTIC_VIEW_SHA256
        or manifest.software.predictive_cache_revision
        != permit.validated_merge_commit_sha256
        or plan.workload_config_sha256 != config_hash
        or plan.fixture_catalog_sha256 != catalog_hash
        or plan.semantic_view_id != manifest.semantic_view_id
        or plan.semantic_view_sha256 != manifest.semantic_view_sha256
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
        or truth_binding.execution_plan_sha256 != plan_hash
        or truth_binding.workload_truth_sha256 != permit.workload_truth_sha256
        or truth_binding.semantic_view_id != manifest.semantic_view_id
        or truth_binding.semantic_view_sha256 != manifest.semantic_view_sha256
    ):
        raise ValueError("workload collection permit artifact binding mismatch")

    try:
        selected_intents = tuple(
            plan.intents[index] for index in permit.operation_indexes
        )
    except IndexError as error:
        raise ValueError("workload collection permit selects an unknown plan index") from error
    expected_intent_ids = tuple(intent.intent_id for intent in selected_intents)
    expected_operation_ids = tuple(
        run_bound_operation_id(manifest.run_id, plan_hash, intent.intent_id)
        for intent in selected_intents
    )
    expected_original_relative_ms = tuple(
        intent.relative_ms for intent in selected_intents
    )
    if (
        permit.intent_ids != expected_intent_ids
        or permit.operation_ids != expected_operation_ids
        or permit.original_relative_ms != expected_original_relative_ms
        or permit.schedule_tolerance_s
        != manifest.operation_schedule_tolerance_s
    ):
        raise ValueError("workload collection permit selected operation mismatch")
    if permit.profile == "full_collection":
        expected_schedule = tuple(intent.relative_ms for intent in plan.intents)
        if (
            permit.operation_indexes != tuple(range(len(plan.intents)))
            or permit.execution_start_time != manifest.start_time
            or permit.execution_relative_ms != expected_schedule
        ):
            raise ValueError("full workload permit must preserve the complete plan schedule")
    else:
        repeat_delay_ms = (
            (
                config.untagged_correlation_window_s
                + config.operation_schedule_tolerance_s
            )
            * 1000
            + config.min_offer_gap_ms
        )
        expected_schedule = (
            0,
            repeat_delay_ms,
            repeat_delay_ms + config.min_offer_gap_ms,
        )[: len(permit.operation_indexes)]
        if permit.execution_relative_ms != expected_schedule:
            raise ValueError("bounded workload permit execution schedule mismatch")
    if (
        permit.execution_start_time >= permit.expires_at
        or permit.execution_start_time
        + timedelta(milliseconds=permit.execution_relative_ms[-1])
        + timedelta(seconds=permit.schedule_tolerance_s)
        >= permit.expires_at
    ):
        raise ValueError("workload collection permit execution schedule mismatch")
    return permit


def _validation_permit(
    path: Path,
    manifest: CollectionManifest,
    manifest_bytes: bytes,
    catalog: FixtureCatalog,
    *,
    evaluated_at: datetime | None = None,
) -> AdapterValidationPermit:
    data = read_bytes(path)

    def unique_object(pairs):
        value = {}
        for key, item in pairs:
            if key in value:
                raise ValueError("validation permit contains duplicate JSON keys")
            value[key] = item
        return value

    try:
        raw = json.loads(data, object_pairs_hook=unique_object)
        permit = AdapterValidationPermit.model_validate(raw)
    except (json.JSONDecodeError, ValueError) as error:
        raise ValueError(f"{path}: invalid validation permit") from error
    if (
        manifest.seed != 32
        or manifest.offered_count != 300
        or manifest.offered_rate_per_s != 1 / 60
        or manifest.max_concurrency != 1
        or manifest.range_bytes != (4096,)
        or manifest.range_offsets != (0, 12288, 24576, 36864, 49152)
        or manifest.checkpoint_interval_s != 900
        or manifest.checkpoint_tolerance_s != 30
        or manifest.pre_read_plus_read_guardrail_ms != 5000
        or manifest.stop_rules.latency_guardrail_ms != 5000
    ):
        raise ValueError("validation permit requires the approved full collection manifest")
    expected_ids = tuple(
        item[4]
        for item in planned_operations(manifest, catalog)[: permit.validation_operations]
    )
    if (
        permit.run_id != manifest.run_id
        or permit.full_manifest_sha256 != hashlib.sha256(manifest_bytes).hexdigest()
        or permit.operation_ids != expected_ids
        or permit.provider_package_sha256 != APPROVED_PROVIDER_PACKAGE_SHA256
        or permit.provider_sha256 != APPROVED_PROVIDER_SHA256
        or permit.collector_sha256 != APPROVED_COLLECTOR_SHA256
        or permit.validator_sha256 != APPROVED_VALIDATOR_SHA256
        or permit.runbook_sha256 != APPROVED_VALIDATION_RUNBOOK_SHA256
        or permit.expires_at <= (evaluated_at or datetime.now(UTC))
    ):
        raise ValueError("validation permit does not match approved provenance")
    return permit


def _validate_observed(
    rows: list[ObservedEvent],
    manifest: CollectionManifest,
    offered: dict[str, OfferedOperation],
) -> dict[tuple[str, str, str], ObservedEvent]:
    by_event: dict[tuple[str, str, str], ObservedEvent] = {}
    dispositions = Counter()
    for row in rows:
        key = (row.event_id, row.pod_uid, row.producer_epoch)
        if key in by_event:
            raise ValueError("observed event identities must be unique")
        by_event[key] = row
        if row.status == "unknown":
            raise ValueError("unknown observed events make reconciliation incomplete")
        if row.exclusion_reason not in manifest.allowed_exclusion_reasons:
            if row.status == "excluded":
                raise ValueError("observed event uses an unapproved exclusion reason")
        operation = offered.get(row.operation_id or "")
        if operation is None:
            raise ValueError("observed event references an unexpected operation")
        dispositions[row.operation_id] += 1
        if dispositions[row.operation_id] != 1:
            raise ValueError("operation has duplicate or contradictory dispositions")
        if not manifest.start_time <= row.durable_at < manifest.end_time:
            raise ValueError("observed durable_at falls outside the run")
        if row.durable_at < operation.offered_at_utc:
            raise ValueError("observed event predates its offered operation")
        if manifest.tag_mode == "empty":
            if operation.tag != "" or row.tag is not None:
                raise ValueError("observed event correlation tag mismatch")
        elif row.tag != operation.tag:
            raise ValueError("observed event correlation tag mismatch")
    if set(dispositions) != set(offered):
        raise ValueError("every offered operation requires exactly one disposition")
    if manifest.tag_mode == "empty":
        ordered = sorted(rows, key=lambda row: (row.durable_at, row.event_id))
        indexes = [offered[row.operation_id or ""].operation_index for row in ordered]
        if indexes != sorted(indexes):
            raise ValueError("empty-tag dispositions conflict with offered operation order")
    return by_event


def _validate_workload_evidence(
    rows: list[WorkloadEvidenceBinding],
    permit: WorkloadCollectionPermit,
    permit_sha256: str,
    plan: WorkloadExecutionPlan,
    offered: dict[str, OfferedOperation],
    observed: dict[tuple[str, str, str], ObservedEvent],
    source_root: Path,
) -> None:
    if (
        len(rows) != len(permit.operation_ids)
        or tuple(row.operation_id for row in rows)
        != permit.operation_ids
        or tuple(row.operation_index for row in rows)
        != permit.operation_indexes
    ):
        raise ValueError(
            "workload evidence rows differ from the permitted selection"
        )
    observed_by_operation = {
        row.operation_id: row
        for row in observed.values()
        if row.operation_id is not None
    }
    for selected_position, row in enumerate(rows):
        operation = offered.get(row.operation_id)
        observation = observed_by_operation.get(row.operation_id)
        intent = plan.intents[row.operation_index]
        scheduled = permit.execution_start_time + timedelta(
            milliseconds=permit.execution_relative_ms[selected_position]
        )
        if (
            operation is None
            or observation is None
            or row.run_id != permit.run_id
            or row.permit_sha256 != permit_sha256
            or row.execution_plan_sha256 != permit.execution_plan_sha256
            or row.intent_id != intent.intent_id
            or row.original_relative_ms != intent.relative_ms
            or row.scheduled_at_utc != scheduled
            or row.offered_at_utc != operation.offered_at_utc
            or row.event_id != observation.event_id
            or row.producer_epoch != observation.producer_epoch
            or row.producer_sequence != observation.producer_sequence
            or row.observed_sha256
            != hashlib.sha256(
                (
                    canonical_json(observation.model_dump(mode="json"))
                    + "\n"
                ).encode()
            ).hexdigest()
        ):
            raise ValueError(
                "workload evidence plan/observed binding mismatch"
            )
        raw_path = source_root / row.raw_jsonl_path
        if (
            raw_path.is_symlink()
            or not raw_path.is_file()
            or hashlib.sha256(raw_path.read_bytes()).hexdigest()
            != row.raw_jsonl_sha256
        ):
            raise ValueError(
                "workload evidence raw chunk binding mismatch"
            )
        raw_lines = raw_path.read_bytes().splitlines()
        if row.raw_line_number > len(raw_lines):
            raise ValueError(
                "workload evidence raw line is unavailable"
            )
        raw_line = raw_lines[row.raw_line_number - 1]
        if (
            hashlib.sha256(raw_line).hexdigest()
            != row.raw_record_sha256
        ):
            raise ValueError(
                "workload evidence raw line hash mismatch"
            )
        try:
            raw = strict_json_loads(raw_line.decode())
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as error:
            raise ValueError(
                "workload evidence raw line is invalid"
            ) from error
        if (
            raw.get("local_id") != row.event_id
            or raw.get("producer_epoch") != row.producer_epoch
            or raw.get("producer_sequence")
            != str(row.producer_sequence)
        ):
            raise ValueError(
                "workload evidence raw identity mismatch"
            )


def _resolve_source(root: Path, relative: str) -> Path:
    candidate = root / relative
    if candidate.is_symlink() or not candidate.is_file():
        raise ValueError(f"{relative}: event source must be a regular non-symlink file")
    resolved_root = root.resolve()
    resolved = candidate.resolve(strict=True)
    if resolved_root != resolved.parent and resolved_root not in resolved.parents:
        raise ValueError(f"{relative}: event source escapes its evidence directory")
    return candidate


def _load_raw_events(
    source_manifest_path: Path,
    sources: CollectorFileManifest,
    start_baselines: dict[tuple[str, str, str, str], tuple[int, datetime]],
) -> tuple[dict[tuple[str, str], list[ChimeraEvent]], dict[str, str], dict[str, int]]:
    root = source_manifest_path.parent
    streams = {}
    hashes = {}
    sizes = {}
    source_paths = set()
    source_inodes = set()
    source_groups = defaultdict(list)
    for source in sources.files:
        identity = (
            source.pod_name,
            source.pod_uid,
            source.node_name,
            source.producer_epoch,
        )
        source_groups[identity].append(source)
    for identity, source_files in source_groups.items():
        events = []
        stream = (identity[1], identity[3])
        for source in source_files:
            path = _resolve_source(root, source.raw_jsonl_path)
            resolved = path.resolve(strict=True)
            inode = (resolved.stat().st_dev, resolved.stat().st_ino)
            if resolved in source_paths or inode in source_inodes:
                raise ValueError(
                    f"{source.raw_jsonl_path}: event source aliases another chunk"
                )
            source_paths.add(resolved)
            source_inodes.add(inode)
            data = read_bytes(path)
            digest = hashlib.sha256(data).hexdigest()
            if digest != source.sha256:
                raise ValueError(f"{source.raw_jsonl_path}: event source checksum mismatch")
            chunk = parse_jsonl(data, path, ChimeraEvent)
            if any(event.producer_epoch != source.producer_epoch for event in chunk):
                raise ValueError(
                    f"{source.raw_jsonl_path}: producer epoch differs from its trusted envelope"
                )
            events.extend(chunk)
            hashes[source.raw_jsonl_path] = digest
            sizes[source.raw_jsonl_path] = len(data)
        if identity not in start_baselines:
            raise ValueError(f"{identity}: source stream lacks a start checkpoint baseline")
        baseline, captured_at = start_baselines[identity]
        if events and _ns_datetime(min(event.sink_ns for event in events)) < captured_at:
            raise ValueError(f"{identity}: first event predates the start checkpoint baseline")
        sequences = [event.sequence for event in events]
        if sequences != list(range(baseline + 1, baseline + len(events) + 1)):
            raise ValueError(
                f"{identity}: producer sequences must be contiguous "
                "from the start checkpoint baseline"
            )
        streams[stream] = events
    return streams, hashes, sizes


def _start_baselines(
    checkpoints: list[Checkpoint],
    manifest: CollectionManifest | WorkloadCollectionManifest,
    scheduled_at: datetime | None = None,
) -> dict[tuple[str, str, str, str], tuple[int, datetime]]:
    baselines = {}
    baseline_time = scheduled_at or manifest.start_time
    for row in checkpoints:
        if row.scheduled_at != baseline_time:
            continue
        key = (row.pod_name, row.pod_uid, row.node_name, row.producer_epoch)
        if key in baselines:
            raise ValueError(f"{key}: duplicate start checkpoint baseline")
        baseline = row.last_synced_sequence
        if any(
            (
                row.last_enqueued_sequence != baseline,
                row.queue_depth != 0,
                row.worker_in_flight != 0,
            )
        ):
            raise ValueError(f"{key}: start checkpoint does not prove a synced baseline")
        baselines[key] = (baseline, row.captured_at)
    if not baselines:
        raise ValueError("collection lacks start checkpoint baselines")
    return baselines


def _validate_checkpoints(
    checkpoints: list[Checkpoint],
    manifest: CollectionManifest | WorkloadCollectionManifest,
    sources: CollectorFileManifest,
    streams: dict[tuple[str, str], list[ChimeraEvent]],
    source_hashes: dict[str, str],
    source_sizes: dict[str, int],
    start_baselines: dict[tuple[str, str, str, str], tuple[int, datetime]],
    validation_permit: AdapterValidationPermit | WorkloadCollectionPermit | None = None,
) -> None:
    source_by_stream = {}
    source_paths_by_stream = defaultdict(set)
    source_order_by_stream = defaultdict(list)
    for source in sources.files:
        stream = (source.pod_uid, source.producer_epoch)
        source_paths_by_stream[stream].add(source.raw_jsonl_path)
        source_order_by_stream[stream].append(source.raw_jsonl_path)
        if stream in source_by_stream:
            current = source_by_stream[stream]
            if (
                current.pod_name != source.pod_name
                or current.node_name != source.node_name
            ):
                raise ValueError(f"{stream}: source envelope identity changed across chunks")
        else:
            source_by_stream[stream] = source
    grouped: dict[tuple[str, str], list[Checkpoint]] = defaultdict(list)
    for row in checkpoints:
        if row.run_id != manifest.run_id:
            raise ValueError("checkpoint run_id mismatch")
        grouped[(row.pod_uid, row.producer_epoch)].append(row)
        if row.drops.total:
            raise ValueError("checkpoint reports collector drops")
        if not all(
            (
                row.collector_sink_healthy,
                row.storage_healthy,
                row.cluster_healthy,
                row.volume_healthy,
                row.export_healthy,
            )
        ):
            raise ValueError("checkpoint reports unhealthy collection state")
        if any(
            (
                row.parse_errors,
                row.identity_mismatches,
                row.checksum_mismatches,
                row.foreground_read_errors,
            )
        ):
            raise ValueError("checkpoint reports parse, identity, checksum, or read errors")
        if row.max_observed_latency_ms > manifest.stop_rules.latency_guardrail_ms:
            raise ValueError("checkpoint reports a latency guardrail breach")
    if set(grouped) != set(source_by_stream):
        raise ValueError("checkpoint streams must exactly match event source streams")
    full_expected_times = [
        manifest.start_time + timedelta(milliseconds=relative_ms)
        for relative_ms in checkpoint_schedule(
            int((manifest.end_time - manifest.start_time).total_seconds() * 1000),
            manifest.checkpoint_interval_s,
        )
    ]
    for stream, rows in grouped.items():
        rows.sort(key=lambda row: row.scheduled_at)
        bounded_validation = isinstance(
            validation_permit, AdapterValidationPermit
        ) or (
            isinstance(validation_permit, WorkloadCollectionPermit)
            and validation_permit.profile == "bounded_validation"
        )
        expected_times = (
            [validation_permit.execution_start_time]
            if isinstance(validation_permit, WorkloadCollectionPermit)
            and validation_permit.profile == "bounded_validation"
            else full_expected_times[: len(rows)]
            if isinstance(validation_permit, AdapterValidationPermit)
            else full_expected_times
        )
        if not rows or [row.scheduled_at for row in rows] != expected_times:
            raise ValueError(f"{stream}: incomplete 15-minute checkpoint coverage")
        if any(
            row.captured_at < row.scheduled_at
            or             row.captured_at
            > row.scheduled_at + timedelta(seconds=manifest.checkpoint_tolerance_s)
            for row in rows
        ):
            raise ValueError(f"{stream}: checkpoint captured outside the 30-second tolerance")
        source = source_by_stream[stream]
        if any(
            row.pod_name != source.pod_name or row.node_name != source.node_name
            for row in rows
        ):
            raise ValueError(f"{stream}: checkpoint identity differs from source envelope")
        if rows[0].restart_count and not rows[0].restart_explained:
            raise ValueError(f"{stream}: unexplained restart before first checkpoint")
        for previous, current in zip(rows, rows[1:], strict=False):
            if current.restart_count < previous.restart_count:
                raise ValueError(f"{stream}: restart count reset")
            if current.restart_count > previous.restart_count and not current.restart_explained:
                raise ValueError(f"{stream}: unexplained collector restart")
            if (
                current.last_enqueued_sequence < previous.last_enqueued_sequence
                or current.last_synced_sequence < previous.last_synced_sequence
            ):
                raise ValueError(f"{stream}: producer sequence reset")
        if not bounded_validation:
            final = rows[-1]
            source = source_by_stream[stream]
            identity = (
                source.pod_name,
                source.pod_uid,
                source.node_name,
                source.producer_epoch,
            )
            baseline, _captured_at = start_baselines[identity]
            expected_count = baseline + len(streams[stream])
            if any(
                (
                    final.last_enqueued_sequence != expected_count,
                    final.last_synced_sequence != expected_count,
                    final.synced_count != expected_count,
                    final.queue_depth != 0,
                    final.worker_in_flight != 0,
                )
            ):
                raise ValueError(f"{stream}: final checkpoint does not prove a synced barrier")
            evidence = [
                item
                for row in rows
                for item in (row.active_jsonl, row.backup_jsonl)
                if item is not None
            ]
            if (
                not evidence
                or any(
                    item.path not in source_paths_by_stream[stream]
                    or item.sha256 != source_hashes[item.path]
                    or item.size_bytes != source_sizes[item.path]
                    for item in evidence
                )
                or rows[0].active_jsonl is None
                or rows[0].active_jsonl.path != source_order_by_stream[stream][0]
                or final.active_jsonl is None
                or final.active_jsonl.path != source_order_by_stream[stream][-1]
            ):
                raise ValueError(
                    f"{stream}: checkpoint source evidence does not match manifest order"
                )


def _event_result(status: int) -> str:
    return STATUS_RESULTS.get(status, "eio")


def _validate_and_build_events(
    streams: dict[tuple[str, str], list[ChimeraEvent]],
    observed: dict[tuple[str, str, str], ObservedEvent],
    offered: dict[str, OfferedOperation],
    fixtures: dict[tuple[str, str], FixtureEntry],
    manifest: CollectionManifest,
) -> tuple[list[dict], list[dict], dict[str, Any]]:
    output = []
    excluded = []
    diagnostics = []
    seen_observed = set()
    for (pod_uid, epoch), events in sorted(streams.items()):
        for event in events:
            key = (event.local_id, pod_uid, epoch)
            disposition = observed.get(key)
            if disposition is None:
                raise ValueError(f"{event.local_id}: raw event lacks exact observed evidence")
            seen_observed.add(key)
            operation = offered[disposition.operation_id or ""]
            fixture = fixtures.get((event.volume_id, event.file_id))
            if fixture is None:
                raise ValueError(f"{event.local_id}: canonical file identity is absent from catalog")
            if any(
                (
                    operation.volume_id != event.volume_id,
                    operation.file_id != event.file_id,
                    event.export_id != fixture.export_id,
                    event.export_name != fixture.export_path,
                )
            ):
                raise ValueError(f"{event.local_id}: raw event identity conflicts with fixture evidence")
            if disposition.producer_sequence != event.sequence:
                raise ValueError(f"{event.local_id}: observed producer sequence mismatch")
            if any(
                (
                    disposition.volume_id != event.volume_id,
                    disposition.file_id != event.file_id,
                    disposition.offset != event.offset,
                    disposition.count != event.requested_count,
                )
            ):
                raise ValueError(f"{event.local_id}: observed range identity mismatch")
            if (
                disposition.server_operation_id is not None
                and disposition.server_operation_id != event.operation_id
            ):
                raise ValueError(f"{event.local_id}: observed server operation identity mismatch")
            if disposition.sink_time_ns is not None and disposition.sink_time_ns != event.sink_ns:
                raise ValueError(f"{event.local_id}: observed sink-time diagnostic mismatch")
            if event.compound_tag_truncated or event.compound_tag != operation.tag:
                raise ValueError(f"{event.local_id}: raw correlation tag mismatch")
            expected_tag_hash = hashlib.sha256(operation.tag.encode()).hexdigest()
            if event.compound_tag_id != expected_tag_hash:
                raise ValueError(f"{event.local_id}: compound tag digest mismatch")
            expected_claim = "controlled_demand" if manifest.tag_mode == "pc1:d" else "unknown"
            if event.origin_claim != expected_claim:
                raise ValueError(f"{event.local_id}: source origin claim conflicts with tag mode")
            if not (
                manifest.start_time <= _ns_datetime(event.event_ns) < manifest.end_time
                and manifest.start_time <= _ns_datetime(event.sink_ns) < manifest.end_time
            ):
                raise ValueError(f"{event.local_id}: source timestamps fall outside the run")
            if (
                _ns_datetime(event.event_ns) < operation.offered_at_utc
                or _ns_datetime(event.sink_ns) < operation.offered_at_utc
            ):
                raise ValueError(f"{event.local_id}: source event predates its offered operation")
            if event.source_file_size is not None and event.source_file_size != fixture.size_bytes:
                raise ValueError(f"{event.local_id}: source and fixture file sizes differ")
            if event.offset != operation.offset or event.requested_count != operation.count:
                raise ValueError(f"{event.local_id}: raw range conflicts with offered operation")
            if event.offset + event.requested_count > fixture.size_bytes:
                raise ValueError(f"{event.local_id}: requested range exceeds fixture size")
            if event.offset + event.returned_bytes > fixture.size_bytes:
                raise ValueError(f"{event.local_id}: returned range exceeds fixture size")
            if manifest.tag_mode == "empty":
                max_delay = timedelta(seconds=manifest.untagged_correlation_window_s)
                candidates = [
                    candidate
                    for candidate in offered.values()
                    if candidate.volume_id == event.volume_id
                    and candidate.file_id == event.file_id
                    and candidate.export_id == event.export_id
                    and candidate.offset == event.offset
                    and candidate.count == event.requested_count
                    and candidate.offered_at_utc <= disposition.durable_at
                    and disposition.durable_at - candidate.offered_at_utc <= max_delay
                ]
                if len(candidates) != 1 or candidates[0].operation_id != operation.operation_id:
                    raise ValueError(
                        f"{event.local_id}: empty-tag correlation is ambiguous or inconsistent"
                    )
            diagnostics.append(
                {
                    "event_id": event.local_id,
                    "pod_uid": pod_uid,
                    "producer_epoch": epoch,
                    "producer_sequence": event.sequence,
                    "sink_time_ns": event.sink_ns,
                    "nfs_operation_id": event.operation_id,
                    "nfs_status": event.nfs_status,
                }
            )
            if disposition.status == "excluded":
                excluded.append(
                    {
                        "operation_id": operation.operation_id,
                        "event_id": event.local_id,
                        "reason": disposition.exclusion_reason,
                        "durable_at": disposition.durable_at,
                    }
                )
                continue
            output.append(
                {
                    "schema_version": 1,
                    "event_id": event.local_id,
                    "event_time": _ns_datetime(event.event_ns),
                    "ingest_time": disposition.durable_at,
                    "volume_id": event.volume_id,
                    "file_id": event.file_id,
                    "dir_id": operation.dir_id,
                    "operation": "read",
                    "handle_id": None,
                    "offset": event.offset,
                    "bytes_requested": event.requested_count,
                    "bytes_read": event.returned_bytes,
                    "handle_bytes_read": None,
                    "handle_duration_ms": None,
                    "file_size": fixture.size_bytes,
                    "result": _event_result(event.nfs_status),
                    "origin": "synthetic",
                    "session_id": event.session_ref or None,
                    "cohort_id": None,
                    "client_id": operation.client_id,
                    "app": "nfs4",
                    "cache_outcome": None,
                    "latency_ms": None,
                }
            )
    if seen_observed != set(observed):
        raise ValueError("one or more observed events lack an exact raw Chimera event")
    return normalize_rows("events", output), excluded, {"raw_events": diagnostics}


def _build_counters(
    manifest: CollectionManifest,
    offered: dict[str, OfferedOperation],
    observed: dict[tuple[str, str, str], ObservedEvent],
) -> tuple[list[dict], list[dict]]:
    by_operation = {row.operation_id: row for row in observed.values()}
    offered_by_hour = Counter(_hour_start(row.offered_at_utc) for row in offered.values())
    emitted_by_hour = Counter()
    dropped_by_hour = Counter()
    reasons_by_hour: dict[datetime, Counter] = defaultdict(Counter)
    for operation_id, operation in offered.items():
        disposition = by_operation[operation_id]
        hour = _hour_start(operation.offered_at_utc)
        if disposition.status == "durable":
            emitted_by_hour[hour] += 1
        else:
            dropped_by_hour[hour] += 1
            reasons_by_hour[hour][disposition.exclusion_reason or "excluded"] += 1
    counters = []
    known_gaps = []
    hour = _hour_start(manifest.start_time)
    while hour < manifest.end_time:
        offered_count = offered_by_hour[hour]
        emitted_count = emitted_by_hour[hour]
        dropped_count = dropped_by_hour[hour]
        if offered_count != emitted_count + dropped_count:
            raise ValueError("hourly offered/durable/excluded reconciliation failed")
        counters.append(
            {
                "volume_id": manifest.volume_id,
                "hour_start": hour,
                "offered": offered_count,
                "emitted": emitted_count,
                "dropped": dropped_count,
                "sampled_out": 0,
                "duplicates": 0,
                "late": 0,
            }
        )
        if dropped_count:
            known_gaps.append(
                {
                    "volume_id": manifest.volume_id,
                    "start": hour,
                    "end": min(hour + timedelta(hours=1), manifest.end_time),
                    "reason": "approved_collection_exclusion",
                    "loss_fraction": dropped_count / offered_count,
                }
            )
        hour += timedelta(hours=1)
    return normalize_rows("loss_counters", counters), known_gaps


def _safe_output(out: Path) -> None:
    if any(parent.is_symlink() for parent in (out, *out.parents) if parent.exists()):
        raise ValueError("output path must not traverse symlinks")
    if not out.exists():
        return
    raise ValueError("output already exists; atomic replacement requires a new directory")


def _version() -> str:
    try:
        return importlib.metadata.version("predictive-cache")
    except importlib.metadata.PackageNotFoundError:
        return "0.1.0"


def import_dataset(
    *,
    event_sources_path: Path,
    observed_path: Path,
    run_manifest_path: Path,
    offered_ledger_path: Path,
    fixture_catalog_path: Path,
    checkpoints_path: Path,
    out_dir: Path,
    execution_plan_path: Path | None = None,
    workload_config_path: Path | None = None,
    truth_binding_path: Path | None = None,
    workload_permit_path: Path | None = None,
    workload_evidence_path: Path | None = None,
    validation_permit_path: Path | None = None,
    validation_permit_reference_time: datetime | None = None,
    format: str = "parquet",
) -> Manifest:
    """Import one immutable controlled collection run atomically."""
    if format not in {"parquet", "jsonl"}:
        raise ValueError("format must be parquet or jsonl")
    inputs = {
        "event_sources": Path(event_sources_path),
        "observed": Path(observed_path),
        "run_manifest": Path(run_manifest_path),
        "offered_ledger": Path(offered_ledger_path),
        "fixture_catalog": Path(fixture_catalog_path),
        "checkpoints": Path(checkpoints_path),
    }
    if execution_plan_path is not None:
        inputs["execution_plan"] = Path(execution_plan_path)
    if workload_config_path is not None:
        inputs["workload_config"] = Path(workload_config_path)
    if truth_binding_path is not None:
        inputs["truth_binding"] = Path(truth_binding_path)
    if workload_permit_path is not None:
        inputs["workload_permit"] = Path(workload_permit_path)
    if workload_evidence_path is not None:
        inputs["workload_evidence"] = Path(workload_evidence_path)
    input_bytes = {name: read_bytes(path) for name, path in inputs.items()}
    input_hashes = {name: hashlib.sha256(data).hexdigest() for name, data in input_bytes.items()}
    manifest = parse_collection_manifest(input_bytes["run_manifest"])
    catalog = parse_json(input_bytes["fixture_catalog"], inputs["fixture_catalog"], FixtureCatalog)
    execution_plan = None
    workload_config = None
    truth_binding = None
    workload_permit = None
    if isinstance(manifest, WorkloadCollectionManifest):
        if (
            manifest.semantic_view_id != SEMANTIC_VIEW_ID
            or manifest.semantic_view_sha256 != SEMANTIC_VIEW_SHA256
        ):
            raise ValueError(
                "workload manifest semantic view differs from read-demand-v1"
            )
        if execution_plan_path is None:
            raise ValueError("workload manifest v3 requires its execution plan")
        execution_plan = parse_json(
            input_bytes["execution_plan"],
            inputs["execution_plan"],
            WorkloadExecutionPlan,
        )
        if execution_plan_sha256(execution_plan) != manifest.execution_plan_sha256:
            raise ValueError("execution plan checksum mismatch")
        if execution_plan.fixture_catalog_sha256 != manifest.fixture_catalog_sha256:
            raise ValueError("execution plan catalog checksum mismatch")
        if execution_plan.workload_config_sha256 != manifest.execution_config_sha256:
            raise ValueError("execution plan config checksum mismatch")
        if (
            execution_plan.semantic_view_id != manifest.semantic_view_id
            or execution_plan.semantic_view_sha256 != manifest.semantic_view_sha256
        ):
            raise ValueError("execution plan semantic view mismatch")
        if any(
            (
                execution_plan.operation_schedule_tolerance_s
                != manifest.operation_schedule_tolerance_s,
                execution_plan.seed != manifest.seed,
                execution_plan.untagged_correlation_window_s
                != manifest.untagged_correlation_window_s,
                (execution_plan.range_bytes,) != manifest.range_bytes,
                execution_plan.range_offsets != manifest.range_offsets,
                execution_plan.planned_operation_count
                != manifest.planned_operation_count,
                execution_plan.duration_ms
                != int(
                    (manifest.end_time - manifest.start_time).total_seconds() * 1000
                ),
            )
        ):
            raise ValueError("execution plan controls differ from the run manifest")
        workload_inputs = (
            workload_config_path,
            truth_binding_path,
            workload_permit_path,
            workload_evidence_path,
        )
        if any(path is not None for path in workload_inputs):
            if not all(path is not None for path in workload_inputs):
                raise ValueError(
                    "workload live import requires config, truth binding, and permit"
                )
            workload_config = parse_json(
                input_bytes["workload_config"],
                inputs["workload_config"],
                WorkloadConfig,
            )
            truth_binding = parse_json(
                input_bytes["truth_binding"],
                inputs["truth_binding"],
                WorkloadTruthBinding,
            )
            workload_permit = _workload_collection_permit(
                inputs["workload_permit"],
                input_bytes["workload_permit"],
                manifest,
                input_bytes["run_manifest"],
                workload_config,
                input_bytes["workload_config"],
                execution_plan,
                input_bytes["execution_plan"],
                truth_binding,
                input_bytes["truth_binding"],
                catalog,
                input_bytes["fixture_catalog"],
                evaluated_at=validation_permit_reference_time,
            )
    elif execution_plan_path is not None:
        raise ValueError("manifest v2 must not receive a workload execution plan")
    elif any(
        path is not None
        for path in (
            workload_config_path,
            truth_binding_path,
            workload_permit_path,
            workload_evidence_path,
        )
    ):
        raise ValueError("manifest v2 must not receive workload-v3 permit inputs")
    if validation_permit_path is not None and isinstance(
        manifest, WorkloadCollectionManifest
    ):
        raise ValueError(
            "workload manifest v3 must not receive a v2 adapter validation permit"
        )
    permit = (
        _validation_permit(
            Path(validation_permit_path),
            manifest,
            input_bytes["run_manifest"],
            catalog,
            evaluated_at=validation_permit_reference_time,
        )
        if validation_permit_path is not None
        else None
    )
    if permit is not None:
        input_hashes["validation_permit"] = sha256(Path(validation_permit_path))
    sources = parse_json(
        input_bytes["event_sources"], inputs["event_sources"], CollectorFileManifest
    )
    if sources.run_id != manifest.run_id:
        raise ValueError("collector source manifest run_id mismatch")
    ledger = parse_jsonl(input_bytes["offered_ledger"], inputs["offered_ledger"], OfferedOperation)
    observed_rows = parse_jsonl(input_bytes["observed"], inputs["observed"], ObservedEvent)
    checkpoints = parse_jsonl(input_bytes["checkpoints"], inputs["checkpoints"], Checkpoint)
    fixtures = _validate_catalog(catalog, manifest)
    offered = _validate_ledger(
        ledger,
        manifest,
        fixtures,
        permit.operation_ids if permit is not None else None,
        execution_plan,
        workload_permit,
    )
    observed = _validate_observed(observed_rows, manifest, offered)
    if workload_permit is not None:
        workload_evidence = parse_jsonl(
            input_bytes["workload_evidence"],
            inputs["workload_evidence"],
            WorkloadEvidenceBinding,
        )
        _validate_workload_evidence(
            workload_evidence,
            workload_permit,
            input_hashes["workload_permit"],
            execution_plan,
            offered,
            observed,
            inputs["event_sources"].parent,
        )
    start_baselines = _start_baselines(
        checkpoints,
        manifest,
        workload_permit.execution_start_time
        if workload_permit is not None
        and workload_permit.profile == "bounded_validation"
        else None,
    )
    streams, source_hashes, source_sizes = _load_raw_events(
        inputs["event_sources"],
        sources,
        start_baselines,
    )
    _validate_checkpoints(
        checkpoints,
        manifest,
        sources,
        streams,
        source_hashes,
        source_sizes,
        start_baselines,
        workload_permit or permit,
    )
    output_events, exclusions, event_report = _validate_and_build_events(
        streams, observed, offered, fixtures, manifest
    )
    counters, known_gaps = _build_counters(manifest, offered, observed)
    source_evidence_mode = (
        "controlled_demand"
        if workload_permit is not None
        else manifest.evidence_mode
        if isinstance(manifest, WorkloadCollectionManifest)
        else "controlled_demand"
    )
    workload_lineage = (
        {
            "execution_plan_sha256": workload_permit.execution_plan_sha256,
            "workload_config_sha256": workload_permit.workload_config_sha256,
            "truth_binding_sha256": workload_permit.truth_binding_sha256,
            "workload_truth_sha256": workload_permit.workload_truth_sha256,
            "workload_permit_sha256": input_hashes["workload_permit"],
            "workload_evidence_sha256": input_hashes["workload_evidence"],
            "operation_indexes_json": canonical_json(
                list(workload_permit.operation_indexes)
            ),
            "intent_ids_json": canonical_json(
                list(workload_permit.intent_ids)
            ),
            "operation_ids_json": canonical_json(
                list(workload_permit.operation_ids)
            ),
        }
        if workload_permit is not None
        else {}
    )
    config = {
        "adapter": "pcache-chimera-import",
        "adapter_version": _version(),
        "format": format,
        "run_id": manifest.run_id,
        "chimera_image_sha256": manifest.software.chimera_image_sha256,
        "event_schema_sha256": manifest.event_schema_sha256,
        "semantic_view_id": SEMANTIC_VIEW_ID,
        "semantic_view_sha256": SEMANTIC_VIEW_SHA256,
        "source_evidence_mode": source_evidence_mode,
        "input_sha256": input_hashes,
        "event_source_sha256": dict(sorted(source_hashes.items())),
        **workload_lineage,
    }
    config_sha = hashlib.sha256(canonical_json(config).encode()).hexdigest()
    out = Path(out_dir).absolute()
    _safe_output(out)
    out.parent.mkdir(parents=True, exist_ok=True)
    stage = Path(tempfile.mkdtemp(prefix=f".{out.name}-staging-", dir=out.parent))
    try:
        entries = []

        def register(relative: str, rows: int | None = None) -> None:
            entries.append({"path": relative, "rows": rows, "sha256": sha256(stage / relative)})

        suffix = format
        relative = f"loss/loss_counters.{suffix}"
        register(relative, write_table(stage / relative, "loss_counters", counters))
        grouped = defaultdict(list)
        for row in output_events:
            grouped[(row["volume_id"], row["ingest_time"].date().isoformat())].append(row)
        if not grouped:
            grouped[(manifest.volume_id, manifest.start_time.date().isoformat())] = []
        for (volume, day), rows in sorted(grouped.items()):
            rows.sort(key=lambda row: (row["ingest_time"], row["event_id"]))
            relative = f"events/volume_id={volume}/date={day}/part-00000.{suffix}"
            register(relative, write_table(stage / relative, "events", rows))

        report = {
            "schema": "chimera_import_report_v2",
            "run_id": manifest.run_id,
            "counts": {
                "event_sources": len(sources.files),
                "raw_events": sum(len(rows) for rows in streams.values()),
                "durable_events": len(output_events),
                "approved_exclusions": len(exclusions),
                "offered_operations": len(offered),
            },
            "event_quality": event_report,
            "approved_exclusions": exclusions,
            "known_gaps": known_gaps,
            "input_sha256": input_hashes,
            "event_source_sha256": dict(sorted(source_hashes.items())),
        }
        if workload_lineage:
            report.update(
                {
                    **workload_lineage,
                    "source_evidence_mode": source_evidence_mode,
                }
            )
        (stage / "report").mkdir()
        (stage / "report/import.json").write_text(canonical_json(report) + "\n")
        register("report/import.json")
        summary = [
            "# Chimera import",
            "",
            f"- Run: `{manifest.run_id}`",
            f"- Offered operations: {len(offered)}",
            f"- Raw Chimera events: {report['counts']['raw_events']}",
            f"- Durable events emitted: {len(output_events)}",
            f"- Approved exclusions: {len(exclusions)}",
            "",
            "All emitted events retain `origin=synthetic`; the client correlation tag "
            "is not trusted origin evidence.",
            "",
        ]
        (stage / "report/summary.md").write_text("\n".join(summary))
        register("report/summary.md")

        runtime = {
            "adapter": "pcache-chimera-import",
            "adapter_version": _version(),
            "python": platform.python_version(),
            "run_id": manifest.run_id,
            "chimera_image_sha256": manifest.software.chimera_image_sha256,
            "event_schema_sha256": manifest.event_schema_sha256,
            "semantic_view_id": SEMANTIC_VIEW_ID,
            "semantic_view_sha256": SEMANTIC_VIEW_SHA256,
            "source_evidence_mode": source_evidence_mode,
            **{f"{name}_sha256": digest for name, digest in input_hashes.items()},
            **workload_lineage,
        }
        counts = {
            "events": len(output_events),
            "dropped_events": len(exclusions),
            "files": len(fixtures),
            "actors": 0,
            "patterns": 0,
            "by_operation": dict(Counter(row["operation"] for row in output_events)),
            "by_origin": dict(Counter(row["origin"] or "unknown" for row in output_events)),
            "by_fault": {},
        }
        manifest_data = {
            "manifest_version": 1,
            "dataset_id": f"chimera-{manifest.run_id}-{config_sha[:12]}",
            "data_kind": "synthetic",
            "generator": {
                "name": "pcache-datagen",
                "version": _version(),
                "git_sha": source_revision(),
                "runtime": runtime,
            },
            "event_schema_version": 1,
            "seed": 0,
            "preset": "custom",
            "config_sha256": config_sha,
            "period": {"start": manifest.start_time, "end": manifest.end_time},
            "volumes": [manifest.volume_id],
            "counts": counts,
            "known_gaps": known_gaps,
            "files": sorted(entries, key=lambda entry: entry["path"]),
            "created_at": manifest.end_time,
            "limitations": [
                "controlled synthetic run; correlation tags do not establish foreground origin",
                "fixture catalog validates identities but is not copied because it lacks the "
                "datagen catalog metadata contract",
                "handle/session lifecycle is unavailable and read-demand-v1 does not reconstruct it",
                "ingest_time is collector durable_at; Chimera sink_time_ns remains diagnostic",
                "cache outcome and latency are unavailable and remain null",
                "controlled demand remains synthetic plumbing evidence",
            ],
        }
        output_manifest = Manifest.model_validate(manifest_data)
        (stage / "manifest.json").write_text(
            canonical_json(output_manifest.model_dump(mode="python")) + "\n"
        )
        stage.rename(out)
        os.chmod(out, 0o700)
        return output_manifest
    finally:
        if stage.exists():
            shutil.rmtree(stage)
