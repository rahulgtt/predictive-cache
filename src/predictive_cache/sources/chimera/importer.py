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
from predictive_cache.collection.models import (
    AdapterValidationPermit,
    Checkpoint,
    CollectionManifest,
    CollectorFileManifest,
    FixtureCatalog,
    FixtureEntry,
    ObservedEvent,
    OfferedOperation,
)
from predictive_cache.collection.provenance import (
    APPROVED_COLLECTOR_SHA256,
    APPROVED_PROVIDER_PACKAGE_SHA256,
    APPROVED_PROVIDER_SHA256,
    APPROVED_VALIDATION_RUNBOOK_SHA256,
    APPROVED_VALIDATOR_SHA256,
)
from predictive_cache.datagen.io import canonical_json, sha256, source_revision, write_table
from predictive_cache.datagen.schema import Manifest, normalize_rows

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
    catalog: FixtureCatalog, manifest: CollectionManifest
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
    manifest: CollectionManifest,
    fixtures: dict[tuple[str, str], FixtureEntry],
    expected_validation_prefix: (
        tuple[tuple[int, FixtureEntry, int, int, str], ...] | None
    ) = None,
) -> dict[str, OfferedOperation]:
    expected_count = (
        len(expected_validation_prefix)
        if expected_validation_prefix is not None
        else manifest.offered_count
    )
    if len(rows) != expected_count:
        raise ValueError("offered ledger count does not match the run manifest")
    by_id: dict[str, OfferedOperation] = {}
    indexes = []
    ranges: dict[tuple[str, str], list[tuple[int, int]]] = defaultdict(list)
    for row in rows:
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
        for start, end in ranges[(row.volume_id, row.file_id)]:
            if row.offset < end and start < row.offset + row.count:
                raise ValueError("offered operation ranges must be unique and non-overlapping")
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
    if indexes != list(range(expected_count)):
        raise ValueError("offered operation indexes must be contiguous from zero")
    if expected_validation_prefix is not None:
        for row, expected in zip(rows, expected_validation_prefix, strict=True):
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


def _validation_permit(
    path: Path,
    manifest: CollectionManifest,
    manifest_bytes: bytes,
    catalog: FixtureCatalog,
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
        or permit.expires_at <= datetime.now(UTC)
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
    start_baselines: dict[tuple[str, str, str, str], int],
) -> tuple[dict[tuple[str, str], list[ChimeraEvent]], dict[str, str], dict[str, int]]:
    root = source_manifest_path.parent
    streams = {}
    hashes = {}
    sizes = {}
    for source in sources.files:
        path = _resolve_source(root, source.raw_jsonl_path)
        data = read_bytes(path)
        digest = hashlib.sha256(data).hexdigest()
        if digest != source.sha256:
            raise ValueError(f"{source.raw_jsonl_path}: event source checksum mismatch")
        events = parse_jsonl(data, path, ChimeraEvent)
        if any(event.producer_epoch != source.producer_epoch for event in events):
            raise ValueError(
                f"{source.raw_jsonl_path}: producer epoch differs from its trusted envelope"
            )
        sequences = [event.sequence for event in events]
        identity = (
            source.pod_name,
            source.pod_uid,
            source.node_name,
            source.producer_epoch,
        )
        stream = (source.pod_uid, source.producer_epoch)
        if identity not in start_baselines:
            raise ValueError(
                f"{source.raw_jsonl_path}: source stream lacks a start checkpoint baseline"
            )
        baseline = start_baselines[identity]
        if sequences != list(range(baseline + 1, baseline + len(events) + 1)):
            raise ValueError(
                f"{source.raw_jsonl_path}: producer sequences must be contiguous "
                "from the start checkpoint baseline"
            )
        streams[stream] = events
        hashes[source.raw_jsonl_path] = digest
        sizes[source.raw_jsonl_path] = len(data)
    return streams, hashes, sizes


def _start_baselines(
    checkpoints: list[Checkpoint],
    manifest: CollectionManifest,
) -> dict[tuple[str, str, str, str], int]:
    baselines = {}
    for row in checkpoints:
        if row.scheduled_at != manifest.start_time:
            continue
        key = (row.pod_name, row.pod_uid, row.node_name, row.producer_epoch)
        if key in baselines:
            raise ValueError(f"{key}: duplicate start checkpoint baseline")
        baselines[key] = row.last_synced_sequence
    if not baselines:
        raise ValueError("collection lacks start checkpoint baselines")
    return baselines


def _validate_checkpoints(
    checkpoints: list[Checkpoint],
    manifest: CollectionManifest,
    sources: CollectorFileManifest,
    streams: dict[tuple[str, str], list[ChimeraEvent]],
    source_hashes: dict[str, str],
    source_sizes: dict[str, int],
    start_baselines: dict[tuple[str, str, str, str], int],
    validation_permit: AdapterValidationPermit | None = None,
) -> None:
    source_by_stream = {
        (source.pod_uid, source.producer_epoch): source for source in sources.files
    }
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
        manifest.start_time + timedelta(seconds=manifest.checkpoint_interval_s * index)
        for index in range(21)
    ]
    for stream, rows in grouped.items():
        rows.sort(key=lambda row: row.scheduled_at)
        expected_times = (
            full_expected_times[: len(rows)]
            if validation_permit is not None
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
        if validation_permit is None:
            final = rows[-1]
            source = source_by_stream[stream]
            identity = (
                source.pod_name,
                source.pod_uid,
                source.node_name,
                source.producer_epoch,
            )
            expected_count = start_baselines[identity] + len(streams[stream])
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
            if not any(
                item.path == source.raw_jsonl_path
                and item.sha256 == source_hashes[source.raw_jsonl_path]
                and item.size_bytes == source_sizes[source.raw_jsonl_path]
                for item in evidence
            ):
                raise ValueError(f"{stream}: source file lacks matching checkpoint evidence")


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
                    "end": hour + timedelta(hours=1),
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
    validation_permit_path: Path | None = None,
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
    input_bytes = {name: read_bytes(path) for name, path in inputs.items()}
    input_hashes = {name: hashlib.sha256(data).hexdigest() for name, data in input_bytes.items()}
    manifest = parse_json(input_bytes["run_manifest"], inputs["run_manifest"], CollectionManifest)
    catalog = parse_json(input_bytes["fixture_catalog"], inputs["fixture_catalog"], FixtureCatalog)
    permit = (
        _validation_permit(
            Path(validation_permit_path),
            manifest,
            input_bytes["run_manifest"],
            catalog,
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
        (
            tuple(
                planned_operations(manifest, catalog)[: permit.validation_operations]
            )
            if permit is not None
            else None
        ),
    )
    observed = _validate_observed(observed_rows, manifest, offered)
    start_baselines = _start_baselines(checkpoints, manifest)
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
        permit,
    )
    output_events, exclusions, event_report = _validate_and_build_events(
        streams, observed, offered, fixtures, manifest
    )
    counters, known_gaps = _build_counters(manifest, offered, observed)
    config = {
        "adapter": "pcache-chimera-import",
        "adapter_version": _version(),
        "format": format,
        "run_id": manifest.run_id,
        "chimera_image_sha256": manifest.software.chimera_image_sha256,
        "event_schema_sha256": manifest.event_schema_sha256,
        "input_sha256": input_hashes,
        "event_source_sha256": dict(sorted(source_hashes.items())),
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
            **{f"{name}_sha256": digest for name, digest in input_hashes.items()},
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
                "handle_id is unavailable; each read becomes an orphan open group",
                "ingest_time is collector durable_at; Chimera sink_time_ns remains diagnostic",
                "cache outcome and latency are unavailable and remain null",
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
