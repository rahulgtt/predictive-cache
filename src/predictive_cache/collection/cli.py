"""CLI for offline controlled collection preparation and reconciliation."""

from __future__ import annotations

import hashlib
import io
import json
import os
import signal
import stat
import subprocess
import tarfile
import tempfile
import uuid
from collections import Counter, defaultdict
from datetime import UTC, datetime, timedelta
from pathlib import Path, PurePosixPath
from types import SimpleNamespace

import click
import pyarrow.parquet as pq
import yaml

from predictive_cache.datagen.io import config_hash, read_table
from predictive_cache.datagen.schema import normalize_rows
from predictive_cache.prep.clean.pipeline import run_clean
from predictive_cache.prep.clean.report import render_summary
from predictive_cache.prep.config import CleanConfig
from predictive_cache.sources.chimera.importer import (
    _load_raw_events,
    _start_baselines,
    _validate_checkpoints,
    import_dataset,
)

from .harness import (
    ISOLATED_PROVIDER_BOOTSTRAP,
    CollectionRunner,
    CommandCheckpointProvider,
    DirectMountExecutor,
    DryRunExecutor,
    _validate_provider_event,
    catalog_sha256,
    checkpoint_stop_reasons,
    planned_operations,
    planned_workload_operations,
    reconcile,
)
from .io import (
    atomic_write_bytes,
    atomic_write_model,
    canonical_json,
    read_models,
    sha256,
    strict_json_loads,
)
from .models import (
    AdapterLossRow,
    AdapterValidationPermit,
    BoundedAdapterManifest,
    BoundedAdapterReport,
    BoundedCleanManifest,
    BoundedCleanReport,
    BoundedValidationAttestation,
    Checkpoint,
    CleanupCompletionRecord,
    CleanupFileEvidence,
    CleanupRootEvidence,
    CleanupScheduleRecord,
    CollectionManifest,
    CollectorDurableEvent,
    CollectorFileManifest,
    DatasetArtifact,
    ExecutionResult,
    FixtureCatalog,
    HistoricalCleanupCompletionRecord,
    HistoricalCleanupScheduleRecord,
    HistoricalCompatibilityApproval,
    HistoricalDeletionApproval,
    HistoricalDirectoryEvidence,
    HistoricalDispositionClaim,
    HistoricalEvidenceReference,
    HistoricalExecutionProvenance,
    HistoricalExternalFileEvidence,
    HistoricalFileEvidence,
    HistoricalFinalizationAttestation,
    HistoricalFrozenRootDeclaration,
    HistoricalIntegrationProvenance,
    HistoricalProviderEvidence,
    HistoricalRetentionApproval,
    HistoricalRetentionRecord,
    HistoricalRootEvidence,
    HistoricalRunEvidence,
    HistoricalValidationAttempt,
    HistoricalVerificationProvenance,
    LiveAdapterManifest,
    LiveAdapterReport,
    LiveCleanManifest,
    LiveCleanReport,
    LiveCoverageRow,
    LiveFinalizationDeclaration,
    LiveProviderDeclaration,
    ObservedEvent,
    OfferedOperation,
    PreReadHealth,
    ProviderInvocationRecord,
    ReconciliationReport,
    WorkloadCollectionManifest,
    parse_collection_manifest,
)
from .provenance import (
    APPROVED_ADAPTER_WRAPPER_SHA256,
    APPROVED_CLEANED_MANIFEST_SHA256,
    APPROVED_CLEANER_WRAPPER_SHA256,
    APPROVED_COLLECTOR_SHA256,
    APPROVED_EXECUTED_SOURCE_ARCHIVE_SHA256,
    APPROVED_IMPORTED_MANIFEST_SHA256,
    APPROVED_PROVIDER_INTERPRETER_PATH,
    APPROVED_PROVIDER_INTERPRETER_SHA256,
    APPROVED_PROVIDER_PACKAGE_SHA256,
    APPROVED_PROVIDER_SHA256,
    APPROVED_VALIDATED_CODE_HEAD_SHA256,
    APPROVED_VALIDATED_MERGE_COMMIT_SHA256,
    APPROVED_VALIDATED_TREE_SHA256,
    APPROVED_VALIDATION_RUNBOOK_SHA256,
    APPROVED_VALIDATOR_SHA256,
    HISTORICAL_BOUNDED_CLEANED_MANIFEST_SHA256,
    HISTORICAL_BOUNDED_CLEANED_REPORT_SHA256,
    HISTORICAL_BOUNDED_COLLECTION_PERMIT_SHA256,
    HISTORICAL_BOUNDED_IMPORTED_MANIFEST_SHA256,
    HISTORICAL_BOUNDED_IMPORTED_REPORT_SHA256,
    HISTORICAL_BOUNDED_MANIFEST_SHA256,
    HISTORICAL_BOUNDED_PERMIT_SHA256,
    HISTORICAL_EXECUTED_ARCHIVE_SHA256,
    HISTORICAL_EXECUTED_COMMIT,
    HISTORICAL_EXECUTED_TREE,
    HISTORICAL_INTEGRATION_COMMIT,
    HISTORICAL_INTEGRATION_TREE,
    HISTORICAL_LEGACY_MODE_COUNTS,
    HISTORICAL_LIVE_PERMIT_SHA256,
    HISTORICAL_RECONCILIATION_SHA256,
    HISTORICAL_REPLAY_IMPORT_REPORT_SHA256,
    HISTORICAL_REPLAY_QUALITY_REPORT_SHA256,
    HISTORICAL_ROOT_DECLARATION_SHA256,
    HISTORICAL_RUN_ID,
    HISTORICAL_RUN_MANIFEST_SHA256,
    HISTORICAL_TOTAL_FILES,
    HISTORICAL_VALIDATION_ATTEMPTS,
)
from .workload import (
    OfflineSimulatedRead,
    WorkloadConfig,
    WorkloadExecutionPlan,
    WorkloadTruth,
    WorkloadTruthBinding,
    aggregate_recovery,
    build_default_workload,
    execution_plan_sha256,
    validate_plan_truth,
    workload_config_sha256,
    workload_truth_sha256,
    write_offline_importer_fixture,
)


def load_model(path: Path, model):
    return model.model_validate_json(path.read_text())


def load_run_manifest(path: Path):
    return parse_collection_manifest(path.read_bytes())


def load_v2_manifest(path: Path, action: str) -> CollectionManifest:
    manifest = load_run_manifest(path)
    if isinstance(manifest, WorkloadCollectionManifest):
        raise click.ClickException(
            f"{action} does not support offline workload manifest v3"
        )
    return manifest


def _load_json(path: Path) -> object:
    return strict_json_loads(path.read_text())


def _existing_private_root(path: Path, label: str) -> Path:
    if not path.is_absolute() or Path(os.path.normpath(path)) != path:
        raise click.ClickException(f"{label} must be an absolute normalized path")
    for candidate in (path, *path.parents):
        if candidate.is_symlink():
            raise click.ClickException(f"{label} must not traverse symlinks")
    if not path.is_dir():
        raise click.ClickException(f"{label} must be an existing directory")
    if stat.S_IMODE(path.stat().st_mode) & 0o077:
        raise click.ClickException(f"{label} must be owner-only")
    return path.resolve(strict=True)


def _approved_executable(path: Path, expected_sha256: str, label: str) -> Path:
    if not path.is_absolute() or path.is_symlink() or not path.is_file():
        raise click.ClickException(f"{label} must be an absolute regular non-symlink file")
    resolved = path.resolve(strict=True)
    metadata = resolved.stat()
    if metadata.st_uid != os.geteuid() or stat.S_IMODE(metadata.st_mode) & 0o022:
        raise click.ClickException(f"{label} has unsafe ownership or mode")
    if not os.access(resolved, os.X_OK) or sha256(resolved) != expected_sha256:
        raise click.ClickException(f"{label} does not match the approved executable")
    return resolved


def _approved_artifact(path: Path, expected_sha256: str, label: str) -> Path:
    if not path.is_absolute() or path.is_symlink() or not path.is_file():
        raise click.ClickException(f"{label} must be an absolute regular non-symlink file")
    resolved = path.resolve(strict=True)
    metadata = resolved.stat()
    if metadata.st_uid != os.geteuid() or stat.S_IMODE(metadata.st_mode) & 0o022:
        raise click.ClickException(f"{label} has unsafe ownership or mode")
    if sha256(resolved) != expected_sha256:
        raise click.ClickException(f"{label} does not match the approved artifact")
    return resolved


def _snapshot_executable(
    source: Path,
    run_dir: Path,
    name: str,
    expected_sha256: str,
) -> Path:
    directory = run_dir / ".approved-tools"
    directory.mkdir(mode=0o700, exist_ok=True)
    if directory.is_symlink() or stat.S_IMODE(directory.stat().st_mode) & 0o077:
        raise click.ClickException("approved tool directory is unsafe")
    target = directory / name
    with source.open("rb") as stream:
        payload = stream.read()
    if hashlib.sha256(payload).hexdigest() != expected_sha256:
        raise click.ClickException("approved executable changed before snapshot")
    atomic_write_bytes(target, payload)
    target.chmod(0o500)
    if sha256(target) != expected_sha256:
        raise click.ClickException("approved executable snapshot mismatch")
    return target


def _snapshot_artifact(
    source: Path,
    run_dir: Path,
    name: str,
    expected_sha256: str,
) -> Path:
    directory = run_dir / ".approved-tools"
    directory.mkdir(mode=0o700, exist_ok=True)
    if directory.is_symlink() or stat.S_IMODE(directory.stat().st_mode) & 0o077:
        raise click.ClickException("approved tool directory is unsafe")
    target = directory / name
    with source.open("rb") as stream:
        payload = stream.read()
    if hashlib.sha256(payload).hexdigest() != expected_sha256:
        raise click.ClickException("approved artifact changed before snapshot")
    atomic_write_bytes(target, payload)
    target.chmod(0o400)
    if sha256(target) != expected_sha256:
        raise click.ClickException("approved artifact snapshot mismatch")
    return target


def _open_verified_fd(path: Path, expected_sha256: str) -> int:
    fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    try:
        metadata = os.fstat(fd)
        if not stat.S_ISREG(metadata.st_mode):
            raise click.ClickException("runtime snapshot must be a regular file")
        digest = hashlib.sha256()
        with os.fdopen(os.dup(fd), "rb") as stream:
            for block in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(block)
        if digest.hexdigest() != expected_sha256:
            raise click.ClickException("runtime snapshot changed before use")
        os.lseek(fd, 0, os.SEEK_SET)
        return fd
    except BaseException:
        os.close(fd)
        raise


def _approved_interpreter() -> Path:
    path = Path(APPROVED_PROVIDER_INTERPRETER_PATH)
    if (
        not path.is_absolute()
        or path.is_symlink()
        or not path.is_file()
        or path.resolve(strict=True) != path
        or sha256(path) != APPROVED_PROVIDER_INTERPRETER_SHA256
    ):
        raise click.ClickException("provider interpreter is not approved")
    metadata = path.stat()
    if metadata.st_uid != 0 or stat.S_IMODE(metadata.st_mode) & 0o022:
        raise click.ClickException("provider interpreter has unsafe ownership or mode")
    return path


def _inventory_root(path: Path, label: str) -> dict[str, str]:
    root = _existing_private_root(path, label)
    inventory = {}
    for candidate in sorted(root.rglob("*")):
        if candidate.is_symlink():
            raise click.ClickException(f"{label} must not contain symlinks")
        mode = candidate.stat().st_mode
        if stat.S_ISDIR(mode):
            if stat.S_IMODE(mode) & 0o077:
                raise click.ClickException(f"{label} directories must be owner-only")
            continue
        if not stat.S_ISREG(mode):
            raise click.ClickException(f"{label} must contain only regular files")
        if stat.S_IMODE(mode) & 0o077:
            raise click.ClickException(f"{label} files must be owner-only")
        relative = candidate.relative_to(root).as_posix()
        inventory[relative] = sha256(candidate)
    if not inventory:
        raise click.ClickException(f"{label} must contain evidence files")
    return inventory


def _declared_hashes(value: object, label: str) -> dict[str, str]:
    if not isinstance(value, dict) or not value:
        raise click.ClickException(f"dataset report requires {label}")
    declared = {}
    for name, digest in value.items():
        path = PurePosixPath(name) if isinstance(name, str) else None
        if (
            path is None
            or path.is_absolute()
            or ".." in path.parts
            or path.as_posix() != name
            or not isinstance(digest, str)
            or len(digest) != 64
            or any(character not in "0123456789abcdef" for character in digest)
        ):
            raise click.ClickException(f"dataset report has invalid {label}")
        declared[name] = digest
    return declared


def _artifact_rows(path: Path, declared_rows: int | None):
    if path.suffix == ".jsonl":
        try:
            rows = [
                strict_json_loads(line)
                for line in path.read_text().splitlines()
                if line
            ]
        except (json.JSONDecodeError, OSError, ValueError) as error:
            raise click.ClickException(f"{path}: invalid JSONL artifact") from error
        if declared_rows != len(rows):
            raise click.ClickException(f"{path}: declared row count mismatch")
        return rows
    if path.suffix == ".parquet":
        try:
            table = pq.read_table(path)
        except (OSError, ValueError) as error:
            raise click.ClickException(f"{path}: invalid Parquet artifact") from error
        if declared_rows != table.num_rows:
            raise click.ClickException(f"{path}: declared row count mismatch")
        return table.to_pylist()
    if declared_rows is not None:
        raise click.ClickException(f"{path}: non-tabular artifact declares rows")
    return None


def _validate_dataset_artifacts(
    root: CleanupRootEvidence,
    artifacts: tuple[DatasetArtifact, ...],
    manifest_path: str,
    allowed_prefixes: tuple[str, ...],
) -> dict[str, list[dict] | None]:
    paths = [item.path for item in artifacts]
    if len(paths) != len(set(paths)):
        raise click.ClickException("dataset manifest contains duplicate artifact paths")
    declared = {item.path: item.sha256 for item in artifacts}
    actual = {
        path: evidence.sha256
        for path, evidence in root.files.items()
        if path != manifest_path
    }
    if declared != actual:
        raise click.ClickException("dataset artifact hashes are incomplete")
    rows = {}
    for item in artifacts:
        if not any(
            item.path == allowed
            or (allowed.endswith("/") and item.path.startswith(allowed))
            for allowed in allowed_prefixes
        ):
            raise click.ClickException("dataset manifest contains an unsupported artifact path")
        rows[item.path] = _artifact_rows(
            Path(root.canonical_path) / item.path,
            item.rows,
        )
    return rows


def _utc_date(value: object, label: str) -> str:
    if not isinstance(value, str):
        raise click.ClickException(f"{label} requires an ISO UTC timestamp")
    try:
        instant = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise click.ClickException(f"{label} has an invalid timestamp") from error
    if instant.tzinfo is None or instant.utcoffset() != timedelta(0):
        raise click.ClickException(f"{label} timestamp must be UTC")
    return instant.date().isoformat()


def _validate_partitioned_rows(
    artifacts: dict[str, list[dict] | None],
    prefix: str,
    volume_id: str,
    valid_dates: set[str],
) -> list[dict]:
    partitioned = []
    for path, rows in artifacts.items():
        if not path.startswith(f"{prefix}/"):
            continue
        parts = PurePosixPath(path).parts
        if (
            len(parts) != 4
            or parts[0] != prefix
            or parts[1] != f"volume_id={volume_id}"
            or not parts[2].startswith("date=")
            or parts[2][5:] not in valid_dates
            or parts[3] not in {"part-00000.jsonl", "part-00000.parquet"}
            or rows is None
        ):
            raise click.ClickException(
                f"{prefix} artifact does not use the canonical partition path"
            )
        partition_date = parts[2][5:]
        expected_order = sorted(
            rows,
            key=lambda row: (
                row.get("ingest_time", ""),
                row.get("event_id", ""),
            ),
        )
        if rows != expected_order:
            raise click.ClickException(f"{prefix} partition rows are not canonical")
        for row in rows:
            if (
                row.get("volume_id") != volume_id
                or _utc_date(row.get("ingest_time"), prefix) != partition_date
            ):
                raise click.ClickException(
                    f"{prefix} partition does not match row identity"
                )
        partitioned.extend(rows)
    if not any(path.startswith(f"{prefix}/") for path in artifacts):
        raise click.ClickException(f"{prefix} partition is missing")
    return partitioned


def _validate_clean_daily_summary(
    report: LiveCleanReport,
    adapter_rows: list[dict],
    volume_id: str,
    label: str,
) -> None:
    expected_days = Counter(
        (row["volume_id"], _utc_date(row.get("ingest_time"), "adapter event"))
        for row in adapter_rows
    )
    actual_days = {
        (item.volume_id, item.date): (item.input, item.kept, item.excluded)
        for item in report.by_volume_day
    }
    expected_day_rows = {
        key: (count, 0, count) for key, count in expected_days.items()
    }
    actual_exclusions = {
        (item.volume_id, item.date, item.reason): item.count
        for item in report.exclusions_by_volume_day_reason
    }
    expected_exclusions = {
        (volume, date, "origin_synthetic"): count
        for (volume, date), count in expected_days.items()
    }
    if (
        len(actual_days) != len(report.by_volume_day)
        or actual_days != expected_day_rows
        or len(actual_exclusions) != len(report.exclusions_by_volume_day_reason)
        or actual_exclusions != expected_exclusions
        or any(item.volume_id != volume_id for item in report.by_volume_day)
        or any(
            item.volume_id != volume_id or item.reason != "origin_synthetic"
            for item in report.exclusions_by_volume_day_reason
        )
    ):
        raise click.ClickException(
            f"{label} clean per-day distributions do not match adapter evidence"
        )


def _validate_clean_artifact_contract(
    root: CleanupRootEvidence,
    artifacts: dict[str, list[dict] | None],
    manifest: LiveCleanManifest,
    report: LiveCleanReport,
    adapter_rows: list[dict],
) -> None:
    coverage_paths = [
        path for path in artifacts if path.startswith("coverage/")
    ]
    if (
        len(coverage_paths) != 1
        or coverage_paths[0]
        not in {"coverage/coverage.jsonl", "coverage/coverage.parquet"}
    ):
        raise click.ClickException("clean coverage artifact is not canonical")
    suffix = PurePosixPath(coverage_paths[0]).suffix[1:]
    if not adapter_rows:
        raise click.ClickException("clean contract requires adapter rows")
    volumes = {row.get("volume_id") for row in adapter_rows}
    if len(volumes) != 1 or None in volumes:
        raise click.ClickException("clean adapter volume identity is invalid")
    volume = next(iter(volumes))
    start_date = manifest.period.start.date().isoformat()
    exclusion_dates = {
        _utc_date(row.get("ingest_time"), "adapter event")
        for row in adapter_rows
    }
    expected = {
        (
            f"events_clean/volume_id={volume}/date={start_date}/"
            f"part-00000.{suffix}"
        ),
        (
            f"opens/volume_id={volume}/date={start_date}/"
            f"part-00000.{suffix}"
        ),
        (
            f"open_ranges/volume_id={volume}/date={start_date}/"
            f"part-00000.{suffix}"
        ),
        f"meta/volume_id={volume}/part-00000.{suffix}",
        coverage_paths[0],
        "config.resolved.yaml",
        "report/quality.json",
        "report/summary.md",
        "validation.json",
    }
    expected.update(
        (
            f"excluded/volume_id={volume}/date={date}/"
            f"part-00000.{suffix}"
        )
        for date in exclusion_dates
    )
    if set(artifacts) != expected:
        raise click.ClickException("clean artifact set is not canonical")
    for prefix in ("events_clean/", "opens/", "open_ranges/", "meta/"):
        rows = [
            row
            for path, values in artifacts.items()
            if path.startswith(prefix) and values is not None
            for row in values
        ]
        if rows:
            raise click.ClickException(
                "synthetic clean auxiliary artifacts must be empty"
            )
    config_path = Path(root.canonical_path) / "config.resolved.yaml"
    summary_path = Path(root.canonical_path) / "report/summary.md"
    validation_path = Path(root.canonical_path) / "validation.json"
    try:
        config_document = yaml.safe_load(config_path.read_text())
        if (
            not isinstance(config_document, dict)
            or set(config_document) != {"clean"}
        ):
            raise ValueError("resolved config must contain only clean")
        clean_config = CleanConfig.model_validate(config_document["clean"])
        validation = _load_json(validation_path)
    except (OSError, ValueError, yaml.YAMLError) as error:
        raise click.ClickException("clean auxiliary artifact is invalid") from error
    if (
        config_hash(clean_config) != manifest.config_sha256
        or validation != {"status": "passed"}
        or summary_path.read_text() != render_summary(
            report.model_dump(mode="json")
        )
    ):
        raise click.ClickException(
            "clean auxiliary artifacts do not match validated output"
        )


def _validate_provider_provenance(
    provider: LiveProviderDeclaration,
    roots: dict[str, CleanupRootEvidence],
    harness_root_kind: str,
    run_id: str,
    manifest_sha256: str,
    expected_count: int,
    source_manifest: CollectorFileManifest,
    label: str,
) -> None:
    harness_root = roots[harness_root_kind]
    snapshot_path = ".approved-tools/checkpoint-provider"
    package_path = ".approved-tools/provider-package.zip"
    snapshot = Path(harness_root.canonical_path) / snapshot_path
    package_snapshot = Path(harness_root.canonical_path) / package_path
    snapshot_evidence = harness_root.files.get(snapshot_path)
    package_evidence = harness_root.files.get(package_path)
    approved_interpreter = _approved_interpreter()
    if (
        snapshot.is_symlink()
        or not snapshot.is_file()
        or snapshot_evidence is None
        or snapshot_evidence.sha256 != APPROVED_PROVIDER_SHA256
        or sha256(snapshot) != APPROVED_PROVIDER_SHA256
    ):
        raise click.ClickException(
            f"{label} provider snapshot is not the approved executable"
        )
    if (
        package_snapshot.is_symlink()
        or not package_snapshot.is_file()
        or package_evidence is None
        or package_evidence.sha256 != APPROVED_PROVIDER_PACKAGE_SHA256
        or sha256(package_snapshot) != APPROVED_PROVIDER_PACKAGE_SHA256
    ):
        raise click.ClickException(
            f"{label} provider package snapshot is not approved"
        )
    if (
        provider.run_id != run_id
        or provider.manifest_sha256 != manifest_sha256
        or provider.provider_package_sha256 != APPROVED_PROVIDER_PACKAGE_SHA256
        or provider.provider_sha256 != APPROVED_PROVIDER_SHA256
        or provider.collector_sha256 != APPROVED_COLLECTOR_SHA256
        or provider.validator_sha256 != APPROVED_VALIDATOR_SHA256
        or provider.provider_snapshot_sha256 != APPROVED_PROVIDER_SHA256
        or provider.interpreter_path != APPROVED_PROVIDER_INTERPRETER_PATH
        or provider.interpreter_sha256 != APPROVED_PROVIDER_INTERPRETER_SHA256
        or provider.interpreter_path != str(approved_interpreter)
        or provider.argv_contract
        != (
            APPROVED_PROVIDER_INTERPRETER_PATH,
            "-I",
            "-S",
            "-c",
            ISOLATED_PROVIDER_BOOTSTRAP,
            package_path,
            snapshot_path,
            "<action>",
            "<run_dir>",
        )
        or provider.operation_count != expected_count
        or provider.event_count != expected_count
    ):
        raise click.ClickException(
            f"{label} provider declaration provenance is inconsistent"
        )
    provider_root_kind = harness_root_kind.replace("harness", "provider")
    expected_artifacts = {
        "provider_package_artifact": (
            harness_root_kind,
            package_path,
            APPROVED_PROVIDER_PACKAGE_SHA256,
        ),
        "collector_artifact": (
            provider_root_kind,
            "collector.py",
            APPROVED_COLLECTOR_SHA256,
        ),
        "validator_artifact": (
            provider_root_kind,
            "validator.py",
            APPROVED_VALIDATOR_SHA256,
        ),
        "invocation_record": (
            harness_root_kind,
            "provider-invocation.json",
            None,
        ),
    }
    for name, (root_id, path, expected_hash) in expected_artifacts.items():
        reference = getattr(provider, name)
        if (reference.root_id, reference.path) != (root_id, path):
            raise click.ClickException(
                f"{label} provider artifact uses the wrong canonical reference"
            )
        actual_hash = _reference_hash(reference, roots)
        if expected_hash is not None and actual_hash != expected_hash:
            raise click.ClickException(
                f"{label} provider artifact is not approved"
            )
    try:
        invocation = ProviderInvocationRecord.model_validate(
            _load_json(
                Path(harness_root.canonical_path)
                / provider.invocation_record.path
            )
        )
    except (json.JSONDecodeError, OSError, ValueError) as error:
        raise click.ClickException(
            f"{label} provider invocation record is invalid"
        ) from error
    if (
        invocation.run_id != run_id
        or invocation.manifest_sha256 != manifest_sha256
        or invocation.provider_package_sha256
        != APPROVED_PROVIDER_PACKAGE_SHA256
        or invocation.provider_package_path != package_path
        or invocation.provider_snapshot_sha256 != APPROVED_PROVIDER_SHA256
        or invocation.collector_sha256 != APPROVED_COLLECTOR_SHA256
        or invocation.validator_sha256 != APPROVED_VALIDATOR_SHA256
        or invocation.interpreter_path != str(approved_interpreter)
        or invocation.interpreter_sha256
        != APPROVED_PROVIDER_INTERPRETER_SHA256
        or invocation.argv_contract != provider.argv_contract
        or invocation.actions != ("checkpoint", "probe", "post-read")
    ):
        raise click.ClickException(
            f"{label} provider invocation record is inconsistent"
        )
    expected_references = {
        "source_manifest": "collector-file-manifest.json",
        "fixture_catalog": "fixture-catalog.json",
        "durable_sidecar": "durable-events.jsonl",
        "checkpoints": "checkpoints.jsonl",
        "pre_read_health": "pre-read-health.jsonl",
        "execution_ledger": "execution.jsonl",
        "offered_ledger": "offered.jsonl",
        "observed": "observed.jsonl",
        "reconciliation": "reconciliation.json",
    }
    for name, path in expected_references.items():
        reference = getattr(provider, name)
        if (reference.root_id, reference.path) != (harness_root_kind, path):
            raise click.ClickException(
                f"{label} provider reference uses the wrong canonical artifact"
            )
        _reference_hash(reference, roots)
    offered = read_models(
        Path(harness_root.canonical_path) / provider.offered_ledger.path,
        OfferedOperation,
    )
    if (
        len(offered) != expected_count
        or set(provider.content_sha256_by_operation)
        != {operation.operation_id for operation in offered}
    ):
        raise click.ClickException(f"{label} provider content evidence is incomplete")
    expected_chunks = {
        (source.raw_jsonl_path, source.sha256)
        for source in source_manifest.files
    }
    actual_chunks = {
        (reference.path, reference.sha256) for reference in provider.source_chunks
    }
    if (
        any(reference.root_id != harness_root_kind for reference in provider.source_chunks)
        or actual_chunks != expected_chunks
    ):
        raise click.ClickException(
            f"{label} provider source chunks do not match the collector source manifest"
        )
    for reference in provider.source_chunks:
        _reference_hash(reference, roots)
    collector_root = Path(harness_root.canonical_path) / "collector"
    actual_collector_files = {
        path.relative_to(Path(harness_root.canonical_path)).as_posix()
        for path in collector_root.rglob("*")
        if path.is_file() and not path.is_symlink()
    }
    if (
        not collector_root.is_dir()
        or any(path.is_symlink() for path in collector_root.rglob("*"))
        or actual_collector_files != {path for path, _ in expected_chunks}
    ):
        raise click.ClickException(
            f"{label} collector chunk inventory is not exact"
        )


def _validate_exact_raw_identities(
    streams,
    durable_by_key: dict[tuple[str, str, str, int], CollectorDurableEvent],
    expected_count: int,
) -> None:
    raw_keys = [
        (event.local_id, pod_uid, producer_epoch, event.sequence)
        for (pod_uid, producer_epoch), events in streams.items()
        for event in events
    ]
    if (
        len(raw_keys) != expected_count
        or len(raw_keys) != len(set(raw_keys))
        or set(raw_keys) != set(durable_by_key)
    ):
        raise click.ClickException(
            "live raw event identities do not exactly match durable evidence"
        )


def _validate_live_row_chain(
    roots: dict[str, CleanupRootEvidence],
    manifest: CollectionManifest,
    fixture_catalog: FixtureCatalog,
    provider: LiveProviderDeclaration,
    adapter_event_rows: list[dict],
    adapter_loss_rows: list[dict],
    clean_excluded_rows: list[dict],
    coverage_rows: list[LiveCoverageRow],
    *,
    expected_count: int = 300,
    harness_root_kind: str = "live_harness_evidence",
    historical_timestamp_normalization: bool = False,
) -> None:
    run_root = Path(roots[harness_root_kind].canonical_path)
    offered = read_models(
        run_root / provider.offered_ledger.path,
        OfferedOperation,
    )
    executions = read_models(
        run_root / provider.execution_ledger.path,
        ExecutionResult,
    )
    durable = read_models(
        run_root / provider.durable_sidecar.path,
        CollectorDurableEvent,
    )
    observed = read_models(
        run_root / provider.observed.path,
        ObservedEvent,
    )
    health_rows = read_models(
        run_root / provider.pre_read_health.path,
        PreReadHealth,
    )
    checkpoints = read_models(
        run_root / provider.checkpoints.path,
        Checkpoint,
    )
    if not all(
        len(rows) == expected_count
        for rows in (offered, executions, durable, observed)
    ):
        raise click.ClickException("live row evidence does not contain 300 records")
    operation_ids = [row.operation_id for row in offered]
    if (
        [row.operation_index for row in offered] != list(range(expected_count))
        or len(operation_ids) != len(set(operation_ids))
    ):
        raise click.ClickException("live offered ledger identity is inconsistent")
    expected_plan = planned_operations(manifest, fixture_catalog)
    expected_plan = expected_plan[:expected_count]
    if len(expected_plan) != expected_count:
        raise click.ClickException("frozen plan does not contain the expected operations")
    for operation, planned in zip(offered, expected_plan, strict=True):
        index, fixture, offset, count, operation_id = planned
        scheduled = manifest.start_time + timedelta(
            seconds=index / manifest.offered_rate_per_s
        )
        expected_tag = (
            f"pc1:d:{manifest.run_ref}:{operation_id[:32]}:{operation_id[32:]}"
            if manifest.tag_mode == "pc1:d"
            else ""
        )
        if (
            operation.operation_index,
            operation.operation_id,
            operation.run_id,
            operation.fixture_id,
            operation.volume_id,
            operation.file_id,
            operation.dir_id,
            operation.export_id,
            operation.relative_path,
            operation.file_size,
            operation.offset,
            operation.count,
            operation.expected_outcome,
            operation.client_id,
            operation.tag,
            operation.timing_mode,
            operation.origin,
        ) != (
            index,
            operation_id,
            manifest.run_id,
            fixture.fixture_id,
            fixture.volume_id,
            fixture.file_id,
            fixture.dir_id,
            fixture.export_id,
            fixture.relative_path,
            fixture.size_bytes,
            offset,
            count,
            "ok",
            manifest.client_id,
            expected_tag,
            "wallclock",
            "synthetic",
        ) or not (
            scheduled
            <= operation.offered_at_utc
            <= scheduled
            + timedelta(seconds=manifest.operation_schedule_tolerance_s)
        ):
            raise click.ClickException("live offered ledger differs from frozen plan")
    execution_by_id = {row.operation_id: row for row in executions}
    observed_by_id = {row.operation_id: row for row in observed}
    health_by_id = defaultdict(list)
    for row in health_rows:
        health_by_id[row.operation_id].append(row)
    durable_by_key = {
        (
            row.event_id,
            row.pod_uid,
            row.producer_epoch,
            row.producer_sequence,
        ): row
        for row in durable
    }
    if (
        set(execution_by_id) != set(operation_ids)
        or set(observed_by_id) != set(operation_ids)
        or set(health_by_id) != set(operation_ids)
        or len(durable_by_key) != expected_count
        or len({row.event_id for row in durable}) != expected_count
        or len({row.event_id for row in observed}) != expected_count
    ):
        raise click.ClickException("live row evidence identity sets do not match")
    checkpoint_identities = {
        (row.pod_name, row.pod_uid, row.node_name, row.producer_epoch)
        for row in checkpoints
    }
    start_baselines = _start_baselines(checkpoints, manifest)
    try:
        source_manifest = CollectorFileManifest.model_validate(
            _load_json(run_root / provider.source_manifest.path)
        )
        streams, _, _ = _load_raw_events(
            run_root / provider.source_manifest.path,
            source_manifest,
            start_baselines,
        )
    except (json.JSONDecodeError, OSError, ValueError) as error:
        raise click.ClickException("live raw source evidence is invalid") from error
    _validate_exact_raw_identities(streams, durable_by_key, expected_count)
    expected_watermarks = {
        identity: value[0] if isinstance(value, tuple) else value
        for identity, value in start_baselines.items()
    }
    if len(health_rows) != expected_count * len(expected_watermarks):
        raise click.ClickException(
            "live pre-read health does not cover every producer stream"
        )
    expected_health_identities = set(expected_watermarks)
    for operation_id, rows in health_by_id.items():
        identities = {
            (row.pod_name, row.pod_uid, row.node_name, row.producer_epoch)
            for row in rows
        }
        if (
            len(rows) != len(expected_health_identities)
            or identities != expected_health_identities
        ):
            raise click.ClickException(
                f"{operation_id}: pre-read health stream identities are incomplete"
            )
    expected_by_event = {}
    for operation in offered:
        execution = execution_by_id[operation.operation_id]
        observation = observed_by_id[operation.operation_id]
        operation_health = health_by_id[operation.operation_id]
        key = (
            observation.event_id,
            observation.pod_uid,
            observation.producer_epoch,
            observation.producer_sequence,
        )
        sidecar = durable_by_key.get(key)
        if sidecar is None:
            raise click.ClickException("live observed event lacks provider sidecar")
        try:
            raw = _validate_provider_event(
                run_root,
                manifest,
                operation,
                execution,
                sidecar,
                checkpoint_identities,
            )
        except RuntimeError as error:
            raise click.ClickException("live provider/raw evidence mismatch") from error
        expected_tag = None if manifest.tag_mode == "empty" else operation.tag
        if historical_timestamp_normalization:
            raw_event_time = datetime(1970, 1, 1, tzinfo=UTC) + timedelta(
                microseconds=int(raw["event_time_ns"]) // 1000
            )
            raw_sink_time = datetime(1970, 1, 1, tzinfo=UTC) + timedelta(
                microseconds=int(raw["sink_time_ns"]) // 1000
            )
        else:
            raw_event_time = datetime.fromtimestamp(
                int(raw["event_time_ns"]) / 1_000_000_000,
                UTC,
            )
            raw_sink_time = datetime.fromtimestamp(
                int(raw["sink_time_ns"]) / 1_000_000_000,
                UTC,
            )
        skew = timedelta(seconds=manifest.clock_skew_tolerance_s)
        conflicts = []
        if (
            execution.status != "ok"
            or execution.bytes_read != operation.count
            or execution.error is not None
            or execution.executor_kind != "direct-mount"
            or execution.filesystem_type not in {"nfs", "nfs4", "nfs4.1"}
            or execution.content_sha256 is None
            or execution.content_sha256
            != provider.content_sha256_by_operation.get(operation.operation_id)
            or execution.supervised_latency_ms is None
            or execution.supervised_latency_ms
            > manifest.pre_read_plus_read_guardrail_ms
            or execution.completed_at_utc < operation.offered_at_utc
        ):
            conflicts.append("execution")
        stream_identity = (
            sidecar.pod_name,
            sidecar.pod_uid,
            sidecar.node_name,
            sidecar.producer_epoch,
        )
        health = next(
            (
                row
                for row in operation_health
                if (
                    row.pod_name,
                    row.pod_uid,
                    row.node_name,
                    row.producer_epoch,
                )
                == stream_identity
            ),
            None,
        )
        for row in operation_health:
            identity = (
                row.pod_name,
                row.pod_uid,
                row.node_name,
                row.producer_epoch,
            )
            watermark = expected_watermarks.get(identity)
            if (
                not operation.offered_at_utc
                <= row.captured_at
                <= execution.completed_at_utc
                or row.run_id != operation.run_id
                or row.drops.total
                or row.delivery_clean != 1
                or row.barrier_ready != 1
                or not row.collector_sink_healthy
                or not row.storage_healthy
                or not row.cluster_healthy
                or not row.volume_healthy
                or not row.export_healthy
                or row.parse_errors
                or row.identity_mismatches
                or row.checksum_mismatches
                or row.foreground_read_errors
                or watermark is None
                or (
                    row.last_enqueued_sequence,
                    row.last_synced_sequence,
                    row.synced_count,
                )
                != (watermark, watermark, watermark)
            ):
                conflicts.append("pre-read health")
                break
        watermark = expected_watermarks.get(stream_identity)
        if (
            health is None
            or watermark is None
            or health.captured_at > raw_event_time + skew
            or sidecar.producer_sequence != watermark + 1
        ):
            conflicts.append("producer watermark")
        if (
            raw_event_time + skew < operation.offered_at_utc
            or raw_event_time > execution.completed_at_utc + skew
            or raw_sink_time > sidecar.durable_at
            or execution.completed_at_utc > sidecar.durable_at
        ):
            conflicts.append("timestamp ordering")
        if (
            observation.status != "durable"
            or observation.exclusion_reason is not None
            or observation.tag != expected_tag
            or observation.durable_at != sidecar.durable_at
            or (
                observation.volume_id,
                observation.file_id,
                observation.offset,
                observation.count,
                observation.server_operation_id,
                observation.sink_time_ns,
            )
            != (
                sidecar.volume_id,
                sidecar.file_id,
                sidecar.offset,
                sidecar.count,
                sidecar.server_operation_id,
                sidecar.sink_time_ns,
            )
        ):
            conflicts.append("observed")
        if conflicts:
            raise click.ClickException(
                "live execution/provider/observed rows conflict: "
                + ", ".join(conflicts)
            )
        expected_watermarks[stream_identity] = sidecar.producer_sequence
        if observation.event_id in expected_by_event:
            raise click.ClickException("live event IDs are not globally unique")
        expected_by_event[observation.event_id] = (
            operation,
            sidecar,
            observation,
            raw_event_time,
            raw,
        )
    adapter_by_event = {row.get("event_id"): row for row in adapter_event_rows}
    if (
        len(adapter_by_event) != expected_count
        or set(adapter_by_event) != set(expected_by_event)
    ):
        raise click.ClickException("live adapter event identities do not match observed")
    for event_id, (
        operation,
        sidecar,
        observation,
        raw_event_time,
        raw,
    ) in expected_by_event.items():
        try:
            row = normalize_rows("events", [adapter_by_event[event_id]])[0]
            expected_row = normalize_rows(
                "events",
                [
                    {
                        "schema_version": 1,
                        "event_id": event_id,
                        "event_time": raw_event_time,
                        "ingest_time": observation.durable_at,
                        "volume_id": operation.volume_id,
                        "file_id": operation.file_id,
                        "dir_id": operation.dir_id,
                        "operation": "read",
                        "handle_id": None,
                        "offset": operation.offset,
                        "bytes_requested": operation.count,
                        "bytes_read": sidecar.returned_bytes,
                        "handle_bytes_read": None,
                        "handle_duration_ms": None,
                        "file_size": operation.file_size,
                        "result": "ok",
                        "origin": "synthetic",
                        "session_id": raw["session_ref"] or None,
                        "cohort_id": None,
                        "client_id": operation.client_id,
                        "app": "nfs4",
                        "cache_outcome": None,
                        "latency_ms": None,
                    }
                ],
            )[0]
        except ValueError as error:
            raise click.ClickException("live adapter event schema is invalid") from error
        if row != expected_row:
            fields = sorted(
                key
                for key in set(row) | set(expected_row)
                if row.get(key) != expected_row.get(key)
            )
            raise click.ClickException(
                "live adapter event row conflicts with evidence: "
                + ", ".join(fields)
            )
    excluded_by_event = {
        row.get("event_id"): row for row in clean_excluded_rows
    }
    if (
        len(excluded_by_event) != expected_count
        or set(excluded_by_event) != set(expected_by_event)
    ):
        raise click.ClickException("live clean exclusions do not match adapter events")
    if any(
        row.get("reason") != "origin_synthetic"
        or row.get("detail") is not None
        or row.get("volume_id")
        != expected_by_event[event_id][0].volume_id
        or any(
            row.get(key) != adapter_by_event[event_id].get(key)
            for key in adapter_by_event[event_id]
        )
        or set(row) != set(adapter_by_event[event_id]) | {"reason", "detail"}
        for event_id, row in excluded_by_event.items()
    ):
        raise click.ClickException("live clean exclusion row conflicts with adapter event")
    expected_hours = Counter(
        row.offered_at_utc.replace(minute=0, second=0, microsecond=0)
        for row in offered
    )
    actual_coverage = {row.hour_start: row for row in coverage_rows}
    actual_loss = {
        row.hour_start: row
        for row in adapter_loss_rows
    }
    if (
        len(actual_coverage) != len(coverage_rows)
        or len(actual_loss) != len(adapter_loss_rows)
        or set(actual_coverage) != set(expected_hours)
        or set(actual_loss) != set(expected_hours)
    ):
        raise click.ClickException("live hourly evidence keys do not match offered ledger")
    for hour, expected in expected_hours.items():
        coverage = actual_coverage[hour]
        loss = actual_loss[hour]
        if (
            coverage.volume_id != manifest.volume_id
            or coverage.offered != expected
            or coverage.emitted != expected
            or loss.volume_id != manifest.volume_id
            or loss.offered != expected
            or loss.emitted != expected
        ):
            raise click.ClickException("live hourly evidence distribution is inconsistent")


def _validate_full_profile(manifest: CollectionManifest) -> None:
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
        raise click.ClickException("finalization requires the approved full collection profile")


def _cleanup_root(
    root_id: str,
    kind: str,
    path: Path,
    *,
    excluded_paths: tuple[str, ...] = (),
) -> CleanupRootEvidence:
    root = _existing_private_root(path, f"{kind} cleanup root")
    metadata = root.stat()
    files = {}
    for candidate in sorted(root.rglob("*")):
        if candidate.is_symlink():
            raise click.ClickException(f"{kind} cleanup root must not contain symlinks")
        mode = candidate.stat().st_mode
        if stat.S_ISDIR(mode):
            if stat.S_IMODE(mode) & 0o077:
                raise click.ClickException(f"{kind} directories must be owner-only")
            continue
        if not stat.S_ISREG(mode):
            raise click.ClickException(f"{kind} must contain only regular files")
        relative = candidate.relative_to(root).as_posix()
        if relative in excluded_paths:
            continue
        file_mode = stat.S_IMODE(mode)
        if file_mode & 0o077:
            raise click.ClickException(f"{kind} files must be owner-only")
        files[relative] = CleanupFileEvidence(
            sha256=sha256(candidate),
            mode=file_mode,
        )
    if not files:
        raise click.ClickException(f"{kind} cleanup root must contain evidence files")
    canonical_path = str(root)
    path_sha = hashlib.sha256(canonical_path.encode()).hexdigest()
    root_payload = {
        "root_id": root_id,
        "kind": kind,
        "canonical_path": canonical_path,
        "path_sha256": path_sha,
        "device": metadata.st_dev,
        "inode": metadata.st_ino,
        "files": {
            name: value.model_dump(mode="json") for name, value in sorted(files.items())
        },
    }
    return CleanupRootEvidence(
        **root_payload,
        root_manifest_sha256=hashlib.sha256(
            (canonical_json(root_payload) + "\n").encode()
        ).hexdigest(),
    )


HISTORICAL_ROOT_ROLES = {
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
}


def _historical_root(role: str, path: Path) -> HistoricalRootEvidence:
    root = _existing_private_root(path, f"{role} historical root")
    metadata = root.stat()
    if metadata.st_uid != os.geteuid() or stat.S_IMODE(metadata.st_mode) != 0o700:
        raise click.ClickException(
            f"{role} historical root must be owner-owned mode 0700"
        )
    directories = {}
    files = {}
    for candidate in sorted(root.rglob("*")):
        if candidate.is_symlink():
            raise click.ClickException(
                f"{role} historical root must not contain symlinks"
            )
        mode = candidate.stat().st_mode
        relative = candidate.relative_to(root).as_posix()
        if stat.S_ISDIR(mode):
            directories[relative] = HistoricalDirectoryEvidence(
                mode=stat.S_IMODE(mode)
            )
            continue
        if not stat.S_ISREG(mode):
            raise click.ClickException(
                f"{role} historical root must contain only regular files"
            )
        files[relative] = HistoricalFileEvidence(
            sha256=sha256(candidate),
            mode=stat.S_IMODE(mode),
            size_bytes=candidate.stat().st_size,
        )
    if not files:
        raise click.ClickException(f"{role} historical root contains no files")
    root_id = role.replace("_", "-")
    canonical_path = str(root)
    payload = {
        "root_id": root_id,
        "role": role,
        "canonical_path": canonical_path,
        "path_sha256": hashlib.sha256(canonical_path.encode()).hexdigest(),
        "owner_uid": metadata.st_uid,
        "mode": stat.S_IMODE(metadata.st_mode),
        "device": metadata.st_dev,
        "inode": metadata.st_ino,
        "retention_action": "retain" if role == "shared_provider" else "delete",
        "directories": {
            name: value.model_dump(mode="json")
            for name, value in sorted(directories.items())
        },
        "files": {
            name: value.model_dump(mode="json")
            for name, value in sorted(files.items())
        },
    }
    return HistoricalRootEvidence(
        **payload,
        root_manifest_sha256=hashlib.sha256(
            (canonical_json(payload) + "\n").encode()
        ).hexdigest(),
    )


def _parse_historical_roots(
    values: tuple[str, ...],
) -> dict[str, HistoricalRootEvidence]:
    if len(values) != len(HISTORICAL_ROOT_ROLES):
        raise click.ClickException("exactly thirteen historical roots are required")
    parsed = {}
    paths = []
    for value in values:
        role, separator, raw_path = value.partition("=")
        if not separator or role in parsed:
            raise click.ClickException(
                "historical roots must use unique ROLE=ABSOLUTE_PATH values"
            )
        if role not in HISTORICAL_ROOT_ROLES:
            raise click.ClickException("historical root role is not approved")
        root = _historical_root(role, Path(raw_path))
        parsed[role] = root
        paths.append(Path(root.canonical_path))
    if set(parsed) != HISTORICAL_ROOT_ROLES:
        raise click.ClickException("historical root roles are incomplete")
    for index, left in enumerate(paths):
        for right in paths[index + 1 :]:
            if (
                left == right
                or left.is_relative_to(right)
                or right.is_relative_to(left)
            ):
                raise click.ClickException(
                    "historical roots must be distinct and non-nested"
                )
    if len({(root.device, root.inode) for root in parsed.values()}) != len(parsed):
        raise click.ClickException("historical roots must not alias")
    return parsed


def _historical_reference_hash(
    reference: HistoricalEvidenceReference,
    roots: dict[str, HistoricalRootEvidence],
) -> str:
    root = roots.get(reference.role)
    if root is None or reference.path not in root.files:
        raise click.ClickException("historical evidence reference is absent")
    actual = root.files[reference.path].sha256
    if actual != reference.sha256:
        raise click.ClickException("historical evidence reference hash mismatch")
    return actual


def _external_file_snapshot(
    path: Path,
) -> tuple[bytes, HistoricalExternalFileEvidence, tuple[int, int]]:
    if not path.is_absolute() or path.is_symlink() or not path.is_file():
        raise click.ClickException(
            "historical external evidence must be an absolute regular file"
        )
    resolved = path.resolve(strict=True)
    fd = os.open(resolved, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    try:
        metadata = os.fstat(fd)
        if not stat.S_ISREG(metadata.st_mode):
            raise click.ClickException(
                "historical external evidence must be a regular file"
            )
        with os.fdopen(os.dup(fd), "rb") as stream:
            data = stream.read()
        evidence = HistoricalExternalFileEvidence(
            canonical_path=str(resolved),
            sha256=hashlib.sha256(data).hexdigest(),
            owner_uid=metadata.st_uid,
            mode=stat.S_IMODE(metadata.st_mode),
            device=metadata.st_dev,
            inode=metadata.st_ino,
            size_bytes=metadata.st_size,
        )
        return data, evidence, (metadata.st_dev, metadata.st_ino)
    finally:
        os.close(fd)


def _external_file_evidence(path: Path) -> HistoricalExternalFileEvidence:
    return _external_file_snapshot(path)[1]


def _verify_external_file_snapshot(
    path: Path,
    expected: HistoricalExternalFileEvidence,
    expected_identity: tuple[int, int],
    label: str,
) -> None:
    _data, actual, identity = _external_file_snapshot(path)
    if actual != expected or identity != expected_identity:
        raise click.ClickException(f"{label} changed during finalization")


def _authorization_snapshot(
    path: Path,
    label: str,
) -> tuple[bytes, str, tuple[int, int]]:
    fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    try:
        metadata = os.fstat(fd)
        if (
            not stat.S_ISREG(metadata.st_mode)
            or stat.S_IMODE(metadata.st_mode) & 0o077
        ):
            raise click.ClickException(
                f"{label} must be an owner-only regular file"
            )
        with os.fdopen(os.dup(fd), "rb") as stream:
            data = stream.read()
        return (
            data,
            hashlib.sha256(data).hexdigest(),
            (metadata.st_dev, metadata.st_ino),
        )
    finally:
        os.close(fd)


def _verify_authorization_snapshot(
    path: Path,
    expected_sha256: str,
    expected_identity: tuple[int, int],
    label: str,
) -> None:
    _data, digest, identity = _authorization_snapshot(path, label)
    if digest != expected_sha256 or identity != expected_identity:
        raise click.ClickException(f"{label} changed during finalization")


def _historical_disposition_claim(
    finalization_root: Path,
    *,
    outcome: str,
    run_id: str,
    attestation_sha256: str,
    approval_sha256: str,
    verification_commit: str,
    verification_tree: str,
    roots: dict[str, HistoricalRootEvidence],
) -> tuple[HistoricalDispositionClaim, bool]:
    path = finalization_root / "historical-disposition.json"
    expected = HistoricalDispositionClaim(
        run_id=run_id,
        outcome=outcome,
        claimed_at=datetime.now(UTC),
        attestation_sha256=attestation_sha256,
        approval_sha256=approval_sha256,
        verification_commit=verification_commit,
        verification_tree=verification_tree,
        root_manifest_sha256={
            role: root.root_manifest_sha256 for role, root in roots.items()
        },
    )
    try:
        atomic_write_model(path, expected)
        return expected, True
    except FileExistsError:
        try:
            existing = load_model(path, HistoricalDispositionClaim)
        except (OSError, ValueError) as error:
            raise click.ClickException(
                "historical disposition claim is invalid"
            ) from error
        if (
            existing.run_id != expected.run_id
            or existing.outcome != expected.outcome
            or existing.attestation_sha256 != expected.attestation_sha256
            or existing.approval_sha256 != expected.approval_sha256
            or existing.verification_commit != expected.verification_commit
            or existing.verification_tree != expected.verification_tree
            or existing.root_manifest_sha256
            != expected.root_manifest_sha256
        ):
            raise click.ClickException(
                "historical disposition conflicts with existing terminal outcome"
            )
        return existing, False


def _reject_opposite_historical_artifacts(
    finalization_root: Path,
    outcome: str,
) -> None:
    retain = finalization_root / "historical-retention-record.json"
    deletion = (
        finalization_root / "historical-deletion-inventory.json",
        finalization_root / "historical-cleanup-schedule.json",
        finalization_root / "historical-cleanup-completion.json",
    )
    if outcome == "retain_all":
        if any(path.exists() or path.is_symlink() for path in deletion):
            raise click.ClickException(
                "historical deletion artifacts conflict with retain-all outcome"
            )
    elif retain.exists() or retain.is_symlink():
        raise click.ClickException(
            "historical retention record conflicts with deletion outcome"
        )


def _verification_git_provenance(
    repository: Path,
    commit: str,
    source_archive: bytes,
) -> tuple[str, str]:
    if (
        not repository.is_absolute()
        or repository.is_symlink()
        or not repository.is_dir()
    ):
        raise click.ClickException(
            "verification repository must be an absolute non-symlink directory"
        )
    try:
        resolved_commit = subprocess.check_output(
            ["git", "-C", str(repository), "rev-parse", f"{commit}^{{commit}}"],
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
        tree = subprocess.check_output(
            ["git", "-C", str(repository), "rev-parse", f"{commit}^{{tree}}"],
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
        expected_tar = subprocess.check_output(
            ["git", "-C", str(repository), "archive", "--format=tar", resolved_commit],
        )
    except (OSError, subprocess.CalledProcessError) as error:
        raise click.ClickException("verification Git provenance is invalid") from error

    def content_root(path: Path) -> Path:
        entries = list(path.iterdir())
        return entries[0] if len(entries) == 1 and entries[0].is_dir() else path

    def file_inventory(path: Path) -> dict[str, tuple[str, bool]]:
        root = content_root(path)
        inventory = {}
        for candidate in sorted(root.rglob("*")):
            if candidate.is_symlink():
                raise click.ClickException(
                    "verification source archive contains symlinks"
                )
            if candidate.is_file():
                inventory[candidate.relative_to(root).as_posix()] = (
                    sha256(candidate),
                    bool(candidate.stat().st_mode & stat.S_IXUSR),
                )
        return inventory

    with tempfile.TemporaryDirectory(prefix=".verification-archive-") as temporary:
        temporary_root = Path(temporary)
        expected_root = temporary_root / "expected"
        actual_root = temporary_root / "actual"
        expected_root.mkdir()
        actual_root.mkdir()
        with tarfile.open(fileobj=io.BytesIO(expected_tar), mode="r:") as archive:
            archive.extractall(expected_root, filter="data")
        try:
            with tarfile.open(
                fileobj=io.BytesIO(source_archive),
                mode="r:*",
            ) as archive:
                archive.extractall(actual_root, filter="data")
        except (OSError, tarfile.TarError) as error:
            raise click.ClickException(
                "verification source archive is invalid"
            ) from error
        if file_inventory(expected_root) != file_inventory(actual_root):
            raise click.ClickException(
                "verification source archive does not materialize Git tree"
            )
    return resolved_commit, tree


def _validate_external_evidence(
    expected: HistoricalExternalFileEvidence,
    actual_path: Path,
    roots: dict[str, HistoricalRootEvidence],
    finalization_root: Path,
) -> None:
    actual = _external_file_evidence(actual_path)
    if actual != expected:
        raise click.ClickException("historical external evidence changed")
    path = Path(actual.canonical_path)
    if (
        path == finalization_root
        or path.is_relative_to(finalization_root)
        or finalization_root.is_relative_to(path)
    ):
        raise click.ClickException(
            "historical external evidence must be outside finalization control"
        )
    for root in roots.values():
        root_path = Path(root.canonical_path)
        if path.is_relative_to(root_path) or root_path.is_relative_to(path):
            raise click.ClickException(
                "historical external evidence must be outside evidence roots"
            )


def _historical_path(
    reference: HistoricalEvidenceReference,
    roots: dict[str, HistoricalRootEvidence],
) -> Path:
    _historical_reference_hash(reference, roots)
    return Path(roots[reference.role].canonical_path) / reference.path


def _manifest_artifact_rows(root: Path, manifest) -> dict[str, list[dict] | None]:
    return {
        item.path: _artifact_rows(root / item.path, item.rows)
        for item in manifest.files
    }


def _tabular_hashes(root: Path, manifest) -> dict[str, str]:
    return {
        item.path: sha256(root / item.path)
        for item in manifest.files
        if item.rows is not None
    }


def _revalidate_historical_run(
    evidence: HistoricalRunEvidence,
    roots: dict[str, HistoricalRootEvidence],
    finalization_root: Path,
    *,
    expected_count: int,
    expected_import_report_sha256: str,
    expected_quality_report_sha256: str,
) -> None:
    references = [
        evidence.manifest,
        evidence.collection_permit,
        evidence.fixture_catalog,
        evidence.offered_ledger,
        evidence.execution_ledger,
        evidence.observed,
        evidence.source_manifest,
        evidence.durable_sidecar,
        evidence.checkpoints,
        evidence.pre_read_health,
        evidence.imported_manifest,
        evidence.imported_report,
        evidence.cleaned_manifest,
        evidence.cleaned_report,
    ]
    if evidence.reconciliation is not None:
        references.append(evidence.reconciliation)
    if evidence.validation_permit is not None:
        references.append(evidence.validation_permit)
    for reference in references:
        _historical_reference_hash(reference, roots)
    manifest_path = _historical_path(evidence.manifest, roots)
    fixture_catalog_path = _historical_path(evidence.fixture_catalog, roots)
    execution_path = _historical_path(evidence.execution_ledger, roots)
    executions = read_models(execution_path, ExecutionResult)
    if len(executions) != expected_count:
        raise click.ClickException("historical execution count is inconsistent")
    permit_reference_time = (
        max(row.completed_at_utc for row in executions)
        if evidence.validation_permit is not None
        else None
    )
    with tempfile.TemporaryDirectory(
        prefix=".historical-revalidation-",
        dir=finalization_root,
    ) as temporary:
        temporary_root = Path(temporary)
        imported = temporary_root / "imported"
        cleaned = temporary_root / "cleaned"
        import_dataset(
            event_sources_path=_historical_path(evidence.source_manifest, roots),
            observed_path=_historical_path(evidence.observed, roots),
            run_manifest_path=manifest_path,
            offered_ledger_path=_historical_path(evidence.offered_ledger, roots),
            fixture_catalog_path=fixture_catalog_path,
            checkpoints_path=_historical_path(evidence.checkpoints, roots),
            out_dir=imported,
            validation_permit_path=(
                _historical_path(evidence.validation_permit, roots)
                if evidence.validation_permit is not None
                else None
            ),
            validation_permit_reference_time=permit_reference_time,
            format="jsonl",
        )
        run_clean(
            imported,
            CleanConfig(),
            cleaned,
            format="jsonl",
        )
        if (
            sha256(imported / "report/import.json")
            != expected_import_report_sha256
            or sha256(cleaned / "report/quality.json")
            != expected_quality_report_sha256
        ):
            raise click.ClickException(
                "historical replay reports differ from immutable evidence"
            )
        existing_import_root = (
            Path(roots[evidence.imported_manifest.role].canonical_path)
            / Path(evidence.imported_manifest.path).parent
        )
        existing_clean_root = (
            Path(roots[evidence.cleaned_manifest.role].canonical_path)
            / Path(evidence.cleaned_manifest.path).parent
        )
        imported_manifest = (
            BoundedAdapterManifest
            if expected_count <= 3
            else LiveAdapterManifest
        ).model_validate(_load_json(imported / "manifest.json"))
        existing_import_manifest = (
            BoundedAdapterManifest
            if expected_count <= 3
            else LiveAdapterManifest
        ).model_validate(
            _load_json(
                existing_import_root / Path(evidence.imported_manifest.path).name
            )
        )
        clean_manifest = (
            BoundedCleanManifest
            if expected_count <= 3
            else LiveCleanManifest
        ).model_validate(_load_json(cleaned / "manifest.json"))
        existing_clean_manifest = (
            BoundedCleanManifest
            if expected_count <= 3
            else LiveCleanManifest
        ).model_validate(
            _load_json(
                existing_clean_root / Path(evidence.cleaned_manifest.path).name
            )
        )
        if (
            _tabular_hashes(imported, imported_manifest)
            != _tabular_hashes(existing_import_root, existing_import_manifest)
            or _tabular_hashes(cleaned, clean_manifest)
            != _tabular_hashes(existing_clean_root, existing_clean_manifest)
        ):
            raise click.ClickException(
                "historical replay tables differ from immutable evidence"
            )
        adapter_artifacts = _manifest_artifact_rows(imported, imported_manifest)
        clean_artifacts = _manifest_artifact_rows(cleaned, clean_manifest)
        adapter_event_rows = [
            row
            for path, rows in adapter_artifacts.items()
            if path.startswith("events/") and rows is not None
            for row in rows
        ]
        adapter_loss_rows = [
            AdapterLossRow.model_validate(row)
            for path, rows in adapter_artifacts.items()
            if path.startswith("loss/") and rows is not None
            for row in rows
        ]
        clean_excluded_rows = [
            row
            for path, rows in clean_artifacts.items()
            if path.startswith("excluded/") and rows is not None
            for row in rows
        ]
        coverage_rows = [
            LiveCoverageRow.model_validate(row)
            for path, rows in clean_artifacts.items()
            if path.startswith("coverage/") and rows is not None
            for row in rows
        ]
        if (
            sum(row.offered for row in adapter_loss_rows) != expected_count
            or sum(row.emitted for row in adapter_loss_rows) != expected_count
            or sum(row.offered for row in coverage_rows) != expected_count
            or sum(row.emitted for row in coverage_rows) != expected_count
        ):
            raise click.ClickException(
                "historical replay hourly evidence is inconsistent"
            )
        content_sha256_by_operation = {
            row.operation_id: row.content_sha256
            for row in executions
            if row.content_sha256 is not None
        }
        provider_view = SimpleNamespace(
            offered_ledger=evidence.offered_ledger,
            execution_ledger=evidence.execution_ledger,
            durable_sidecar=evidence.durable_sidecar,
            observed=evidence.observed,
            pre_read_health=evidence.pre_read_health,
            checkpoints=evidence.checkpoints,
            source_manifest=evidence.source_manifest,
            fixture_catalog=evidence.fixture_catalog,
            content_sha256_by_operation=content_sha256_by_operation,
        )
        manifest = load_v2_manifest(manifest_path, "live row validation")
        fixture_catalog = load_model(fixture_catalog_path, FixtureCatalog)
        _validate_live_row_chain(
            roots,
            manifest,
            fixture_catalog,
            provider_view,
            adapter_event_rows,
            [row for row in adapter_loss_rows if row.offered],
            clean_excluded_rows,
            [row for row in coverage_rows if row.offered],
            expected_count=expected_count,
            harness_root_kind=evidence.manifest.role,
            historical_timestamp_normalization=True,
        )


def _reference_hash(reference, roots: dict[str, CleanupRootEvidence]) -> str:
    root = roots.get(reference.root_id)
    if root is None or reference.path not in root.files:
        raise click.ClickException("evidence reference is absent from cleanup roots")
    actual = root.files[reference.path].sha256
    if actual != reference.sha256:
        raise click.ClickException("evidence reference hash mismatch")
    return actual


def _manifest_profile(manifest: CollectionManifest) -> dict:
    value = manifest.model_dump(mode="json")
    for field in ("run_id", "run_ref", "start_time", "end_time"):
        value.pop(field)
    return value


def _parse_cleanup_roots(
    values: tuple[str, ...],
) -> dict[str, CleanupRootEvidence]:
    if len(values) != 8:
        raise click.ClickException("exactly eight cleanup roots are required")
    parsed = {}
    paths = []
    for value in values:
        kind, separator, raw_path = value.partition("=")
        if not separator or kind in parsed:
            raise click.ClickException("cleanup roots must use unique KIND=ABSOLUTE_PATH values")
        root = _cleanup_root(kind, kind, Path(raw_path))
        parsed[kind] = root
        paths.append(Path(root.canonical_path))
    expected_kinds = {
        "bounded_harness_evidence",
        "bounded_provider_evidence",
        "bounded_adapter_dataset",
        "bounded_clean_dataset",
        "live_harness_evidence",
        "live_provider_evidence",
        "live_adapter_dataset",
        "live_clean_dataset",
    }
    if set(parsed) != expected_kinds:
        raise click.ClickException("cleanup root kinds do not match the approved eight roots")
    for index, left in enumerate(paths):
        for right in paths[index + 1 :]:
            if (
                left == right
                or left.is_relative_to(right)
                or right.is_relative_to(left)
            ):
                raise click.ClickException("cleanup roots must be pairwise distinct and non-nested")
    devices = {(root.device, root.inode) for root in parsed.values()}
    if len(devices) != 8:
        raise click.ClickException("cleanup roots must not alias the same filesystem object")
    return parsed


def _validate_bounded_attestation(
    attestation: BoundedValidationAttestation,
    roots: dict[str, CleanupRootEvidence],
    live_manifest: CollectionManifest,
) -> None:
    if (
        attestation.validated_code_head_sha256
        != APPROVED_VALIDATED_CODE_HEAD_SHA256
        or attestation.validated_merge_commit_sha256
        != APPROVED_VALIDATED_MERGE_COMMIT_SHA256
        or attestation.validated_tree_sha256 != APPROVED_VALIDATED_TREE_SHA256
        or attestation.imported_manifest.sha256 != APPROVED_IMPORTED_MANIFEST_SHA256
        or attestation.cleaned_manifest.sha256 != APPROVED_CLEANED_MANIFEST_SHA256
    ):
        raise click.ClickException("bounded validation attestation provenance is not approved")
    bounded = {root.kind: root for root in attestation.cleanup_roots}
    for kind in (
        "bounded_harness_evidence",
        "bounded_provider_evidence",
        "bounded_adapter_dataset",
        "bounded_clean_dataset",
    ):
        if bounded.get(kind) != roots[kind]:
            raise click.ClickException("bounded cleanup root evidence does not match attestation")
    expected_references = {
        "full_live_candidate_manifest": (
            "bounded_harness_evidence",
            "candidate-manifest.json",
        ),
        "permit": ("bounded_harness_evidence", "adapter-validation-permit.json"),
        "offered_ledger": ("bounded_harness_evidence", "offered.jsonl"),
        "execution_ledger": ("bounded_harness_evidence", "execution.jsonl"),
        "observed": ("bounded_harness_evidence", "observed.jsonl"),
        "source_manifest": (
            "bounded_harness_evidence",
            "collector-file-manifest.json",
        ),
        "checkpoints": ("bounded_harness_evidence", "checkpoints.jsonl"),
        "durable_sidecar": ("bounded_harness_evidence", "durable-events.jsonl"),
        "pre_read_health": (
            "bounded_harness_evidence",
            "pre-read-health.jsonl",
        ),
        "provider_manifest": (
            "bounded_provider_evidence",
            "provider-manifest.json",
        ),
        "executed_source_archive": (
            "bounded_harness_evidence",
            "source-archive.tar.gz",
        ),
        "adapter_wrapper": ("bounded_harness_evidence", "adapter-wrapper"),
        "cleaner_wrapper": ("bounded_harness_evidence", "cleaner-wrapper"),
        "cluster_provenance": (
            "bounded_harness_evidence",
            "cluster-provenance-v1.json",
        ),
        "event_schema": ("bounded_harness_evidence", "event-schema-v2.json"),
        "budget_evidence": (
            "bounded_harness_evidence",
            "budget-evidence-v1.json",
        ),
        "fixture_catalog": (
            "bounded_harness_evidence",
            "fixture-catalog.json",
        ),
        "provider_package": (
            "bounded_provider_evidence",
            "provider-package.json",
        ),
        "provider": ("bounded_provider_evidence", "provider.py"),
        "collector": ("bounded_provider_evidence", "collector.py"),
        "validator": ("bounded_provider_evidence", "validator.py"),
        "runbook": ("bounded_provider_evidence", "runbook.md"),
        "imported_manifest": ("bounded_adapter_dataset", "manifest.json"),
        "imported_report": ("bounded_adapter_dataset", "report/import.json"),
        "cleaned_manifest": ("bounded_clean_dataset", "manifest.json"),
        "cleaned_report": ("bounded_clean_dataset", "report/quality.json"),
    }
    for name, expected in expected_references.items():
        reference = getattr(attestation, name)
        if (reference.root_id, reference.path) != expected:
            raise click.ClickException(
                "bounded evidence reference uses the wrong canonical artifact"
            )
    for reference in (
        attestation.full_live_candidate_manifest,
        attestation.permit,
        attestation.offered_ledger,
        attestation.execution_ledger,
        attestation.observed,
        attestation.source_manifest,
        attestation.checkpoints,
        attestation.durable_sidecar,
        attestation.pre_read_health,
        attestation.provider_manifest,
        attestation.executed_source_archive,
        attestation.provider_package,
        attestation.provider,
        attestation.collector,
        attestation.validator,
        attestation.runbook,
        attestation.adapter_wrapper,
        attestation.cleaner_wrapper,
        attestation.cluster_provenance,
        attestation.event_schema,
        attestation.budget_evidence,
        attestation.fixture_catalog,
        attestation.imported_manifest,
        attestation.imported_report,
        attestation.cleaned_manifest,
        attestation.cleaned_report,
    ):
        _reference_hash(reference, roots)
    expected_hashes = {
        "executed_source_archive": APPROVED_EXECUTED_SOURCE_ARCHIVE_SHA256,
        "provider_package": APPROVED_PROVIDER_PACKAGE_SHA256,
        "provider": APPROVED_PROVIDER_SHA256,
        "collector": APPROVED_COLLECTOR_SHA256,
        "validator": APPROVED_VALIDATOR_SHA256,
        "runbook": APPROVED_VALIDATION_RUNBOOK_SHA256,
        "adapter_wrapper": APPROVED_ADAPTER_WRAPPER_SHA256,
        "cleaner_wrapper": APPROVED_CLEANER_WRAPPER_SHA256,
    }
    for name, digest in expected_hashes.items():
        if getattr(attestation, name).sha256 != digest:
            raise click.ClickException("bounded validation artifact hash is not approved")
    candidate_root = roots[attestation.full_live_candidate_manifest.root_id]
    candidate_path = (
        Path(candidate_root.canonical_path)
        / attestation.full_live_candidate_manifest.path
    )
    try:
        candidate = CollectionManifest.model_validate(_load_json(candidate_path))
    except (json.JSONDecodeError, OSError, ValueError) as error:
        raise click.ClickException("bounded candidate manifest is invalid") from error
    if (
        candidate.run_id != attestation.validation_run_id
        or candidate.run_id == live_manifest.run_id
        or _manifest_profile(candidate) != _manifest_profile(live_manifest)
        or attestation.cluster_provenance.sha256
        != candidate.cluster_manifest_sha256
        or attestation.event_schema.sha256 != candidate.event_schema_sha256
        or attestation.budget_evidence.sha256 != candidate.budget_evidence_sha256
    ):
        raise click.ClickException(
            "bounded candidate manifest does not match the live experiment profile"
        )
    catalog_root = roots[attestation.fixture_catalog.root_id]
    catalog_path = Path(catalog_root.canonical_path) / attestation.fixture_catalog.path
    try:
        catalog = FixtureCatalog.model_validate(_load_json(catalog_path))
    except (json.JSONDecodeError, OSError, ValueError) as error:
        raise click.ClickException("bounded fixture catalog is invalid") from error
    if (
        catalog_sha256(catalog) != candidate.fixture_catalog_sha256
        or tuple(
            item[4]
            for item in planned_operations(candidate, catalog)[
                : attestation.validation_operations
            ]
        )
        != attestation.operation_ids
    ):
        raise click.ClickException("bounded operation prefix is inconsistent")
    permit_root = roots[attestation.permit.root_id]
    permit_path = Path(permit_root.canonical_path) / attestation.permit.path
    try:
        permit = AdapterValidationPermit.model_validate(_load_json(permit_path))
    except (json.JSONDecodeError, OSError, ValueError) as error:
        raise click.ClickException("bounded validation permit evidence is invalid") from error
    if (
        str(permit.permit_id) != str(attestation.permit_id)
        or permit.run_id != attestation.validation_run_id
        or permit.full_manifest_sha256
        != attestation.full_live_candidate_manifest.sha256
        or permit.validation_operations != attestation.validation_operations
        or permit.operation_ids != attestation.operation_ids
        or permit.provider_package_sha256 != attestation.provider_package.sha256
        or permit.provider_sha256 != attestation.provider.sha256
        or permit.collector_sha256 != attestation.collector.sha256
        or permit.validator_sha256 != attestation.validator.sha256
        or permit.runbook_sha256 != attestation.runbook.sha256
        or permit.provider_package_sha256 != APPROVED_PROVIDER_PACKAGE_SHA256
        or permit.provider_sha256 != APPROVED_PROVIDER_SHA256
        or permit.collector_sha256 != APPROVED_COLLECTOR_SHA256
        or permit.validator_sha256 != APPROVED_VALIDATOR_SHA256
        or permit.runbook_sha256 != APPROVED_VALIDATION_RUNBOOK_SHA256
    ):
        raise click.ClickException("bounded validation permit does not match attestation")
    bounded_harness_root = roots["bounded_harness_evidence"]
    offered_path = (
        Path(bounded_harness_root.canonical_path) / attestation.offered_ledger.path
    )
    execution_path = (
        Path(bounded_harness_root.canonical_path) / attestation.execution_ledger.path
    )
    try:
        offered_rows = read_models(offered_path, OfferedOperation)
        execution_rows = read_models(execution_path, ExecutionResult)
    except (OSError, ValueError) as error:
        raise click.ClickException("bounded execution evidence is invalid") from error
    offered_ids = tuple(row.operation_id for row in offered_rows)
    execution_ids = tuple(row.operation_id for row in execution_rows)
    if (
        len(offered_rows) != attestation.validation_operations
        or len(execution_rows) != attestation.validation_operations
        or offered_ids != attestation.operation_ids
        or execution_ids != attestation.operation_ids
        or attestation.started_at != min(row.offered_at_utc for row in offered_rows)
        or max(row.completed_at_utc for row in execution_rows)
        > attestation.completed_at
        or attestation.completed_at > permit.expires_at
        or any(
            offered.operation_id != execution.operation_id
            or execution.status != "ok"
            or execution.bytes_read != offered.count
            or execution.error is not None
            or execution.executor_kind != "direct-mount"
            or execution.filesystem_type not in {"nfs", "nfs4", "nfs4.1"}
            or execution.content_sha256 is None
            or execution.supervised_latency_ms is None
            or execution.supervised_latency_ms
            > candidate.pre_read_plus_read_guardrail_ms
            or offered.offered_at_utc > execution.completed_at_utc
            or execution.completed_at_utc > permit.expires_at
            for offered, execution in zip(
                offered_rows,
                execution_rows,
                strict=True,
            )
        )
    ):
        raise click.ClickException(
            "bounded execution interval does not match permit evidence"
        )
    bounded_root = roots["bounded_adapter_dataset"]
    bounded_manifest_path = (
        Path(bounded_root.canonical_path) / attestation.imported_manifest.path
    )
    bounded_report_path = (
        Path(bounded_root.canonical_path) / attestation.imported_report.path
    )
    try:
        bounded_manifest = BoundedAdapterManifest.model_validate(
            _load_json(bounded_manifest_path)
        )
        bounded_report = BoundedAdapterReport.model_validate(
            _load_json(bounded_report_path)
        )
    except (json.JSONDecodeError, OSError, ValueError) as error:
        raise click.ClickException("bounded adapter output is invalid") from error
    runtime = bounded_manifest.generator.runtime
    inputs = bounded_report.input_sha256
    count = attestation.validation_operations
    if (
        runtime.run_id != attestation.validation_run_id
        or runtime.validation_permit_sha256 != attestation.permit.sha256
        or runtime.run_manifest_sha256
        != attestation.full_live_candidate_manifest.sha256
        or runtime.fixture_catalog_sha256 != attestation.fixture_catalog.sha256
        or runtime.offered_ledger_sha256 != attestation.offered_ledger.sha256
        or runtime.observed_sha256 != attestation.observed.sha256
        or runtime.event_sources_sha256 != attestation.source_manifest.sha256
        or runtime.checkpoints_sha256 != attestation.checkpoints.sha256
        or inputs.run_manifest != runtime.run_manifest_sha256
        or inputs.fixture_catalog != runtime.fixture_catalog_sha256
        or inputs.validation_permit != runtime.validation_permit_sha256
        or inputs.offered_ledger != runtime.offered_ledger_sha256
        or inputs.observed != runtime.observed_sha256
        or inputs.event_sources != runtime.event_sources_sha256
        or inputs.checkpoints != runtime.checkpoints_sha256
        or bounded_report.run_id != attestation.validation_run_id
        or bounded_manifest.counts.events != count
        or bounded_manifest.counts.by_operation != {"read": count}
        or bounded_manifest.counts.by_origin != {"synthetic": count}
        or bounded_report.counts.raw_events != count
        or bounded_report.counts.durable_events != count
        or bounded_report.counts.offered_operations != count
    ):
        raise click.ClickException("bounded adapter output does not match its permit")
    bounded_artifacts = _validate_dataset_artifacts(
        bounded_root,
        bounded_manifest.files,
        attestation.imported_manifest.path,
        ("events/", "loss/", "report/"),
    )
    bounded_valid_dates = set()
    bounded_day = candidate.start_time.date()
    while bounded_day <= candidate.end_time.date():
        bounded_valid_dates.add(bounded_day.isoformat())
        bounded_day += timedelta(days=1)
    bounded_event_rows = _validate_partitioned_rows(
        bounded_artifacts,
        "events",
        candidate.volume_id,
        bounded_valid_dates,
    )
    bounded_loss_rows = [
        AdapterLossRow.model_validate(row)
        for path, rows in bounded_artifacts.items()
        if path.startswith("loss/") and rows is not None
        for row in rows
    ]
    if (
        len(bounded_event_rows) != count
        or any(
            row.get("origin") != "synthetic" or row.get("app") != "nfs4"
            for row in bounded_event_rows
        )
        or sum(row.offered for row in bounded_loss_rows) != count
        or sum(row.emitted for row in bounded_loss_rows) != count
    ):
        raise click.ClickException("bounded adapter artifacts do not reconcile")
    bounded_clean_root = roots["bounded_clean_dataset"]
    bounded_clean_path = (
        Path(bounded_clean_root.canonical_path) / attestation.cleaned_manifest.path
    )
    bounded_clean_report_path = (
        Path(bounded_clean_root.canonical_path) / attestation.cleaned_report.path
    )
    try:
        bounded_clean = BoundedCleanManifest.model_validate(
            _load_json(bounded_clean_path)
        )
        bounded_clean_report = BoundedCleanReport.model_validate(
            _load_json(bounded_clean_report_path)
        )
    except (json.JSONDecodeError, OSError, ValueError) as error:
        raise click.ClickException("bounded clean output is invalid") from error
    count = attestation.validation_operations
    if (
        bounded_clean.parent.manifest_sha256
        != attestation.imported_manifest.sha256
        or bounded_clean.parent.dataset_id != bounded_manifest.dataset_id
        or bounded_clean.period != bounded_manifest.period
        or bounded_clean.counts.input_events != count
        or bounded_clean.counts.excluded_by_reason
        != {"origin_synthetic": count}
        or bounded_clean_report.counts != bounded_clean.counts
        or bounded_clean_report.excluded_by_reason
        != bounded_clean.counts.excluded_by_reason
        or bounded_clean_report.origin_mix != {"synthetic": count}
        or bounded_clean_report.app_mix != {"nfs4": count}
        or bounded_clean_report.catalog_misses != {"files": 0, "events": 0}
        or bounded_clean.created_at != attestation.completed_at
    ):
        raise click.ClickException("bounded clean output does not match adapter evidence")
    bounded_clean_artifacts = _validate_dataset_artifacts(
        bounded_clean_root,
        bounded_clean.files,
        attestation.cleaned_manifest.path,
        (
            "events_clean/",
            "excluded/",
            "coverage/",
            "opens/",
            "open_ranges/",
            "meta/",
            "report/",
            "config.resolved.yaml",
            "validation.json",
        ),
    )
    bounded_kept_rows = _validate_partitioned_rows(
        bounded_clean_artifacts,
        "events_clean",
        candidate.volume_id,
        bounded_valid_dates,
    )
    bounded_excluded_rows = _validate_partitioned_rows(
        bounded_clean_artifacts,
        "excluded",
        candidate.volume_id,
        bounded_valid_dates,
    )
    _validate_clean_daily_summary(
        bounded_clean_report,
        bounded_event_rows,
        candidate.volume_id,
        "bounded",
    )
    _validate_clean_artifact_contract(
        bounded_clean_root,
        bounded_clean_artifacts,
        bounded_clean,
        bounded_clean_report,
        bounded_event_rows,
    )
    if (
        bounded_kept_rows
        or len(bounded_excluded_rows) != count
        or any(row.get("reason") != "origin_synthetic" for row in bounded_excluded_rows)
    ):
        raise click.ClickException("bounded clean artifacts do not reconcile")
    coverage_paths = [
        item.path for item in bounded_clean.files if item.path.startswith("coverage/")
    ]
    if len(coverage_paths) != 1:
        raise click.ClickException("bounded clean coverage artifact is missing")
    try:
        bounded_coverage = [
            LiveCoverageRow.model_validate(strict_json_loads(line))
            for line in (
                Path(bounded_clean_root.canonical_path) / coverage_paths[0]
            ).read_text().splitlines()
            if line
        ]
    except (json.JSONDecodeError, OSError, ValueError) as error:
        raise click.ClickException("bounded clean coverage artifact is invalid") from error
    if (
        not bounded_coverage
        or sum(row.offered for row in bounded_coverage) != count
        or sum(row.emitted for row in bounded_coverage) != count
        or bounded_clean.coverage.hours != len(bounded_coverage)
        or bounded_clean.coverage.complete != len(bounded_coverage)
    ):
        raise click.ClickException("bounded clean coverage does not reconcile")
    bounded_provider_path = (
        Path(roots["bounded_provider_evidence"].canonical_path)
        / attestation.provider_manifest.path
    )
    try:
        bounded_provider = LiveProviderDeclaration.model_validate(
            _load_json(bounded_provider_path)
        )
    except (json.JSONDecodeError, OSError, ValueError) as error:
        raise click.ClickException("bounded provider declaration is invalid") from error
    try:
        bounded_source_manifest = CollectorFileManifest.model_validate(
            _load_json(
                Path(roots["bounded_harness_evidence"].canonical_path)
                / bounded_provider.source_manifest.path
            )
        )
    except (json.JSONDecodeError, OSError, ValueError) as error:
        raise click.ClickException("bounded source manifest is invalid") from error
    _validate_provider_provenance(
        bounded_provider,
        roots,
        "bounded_harness_evidence",
        attestation.validation_run_id,
        attestation.full_live_candidate_manifest.sha256,
        count,
        bounded_source_manifest,
        "bounded",
    )
    _validate_live_row_chain(
        roots,
        candidate,
        catalog,
        bounded_provider,
        bounded_event_rows,
        bounded_loss_rows,
        bounded_excluded_rows,
        bounded_coverage,
        expected_count=count,
        harness_root_kind="bounded_harness_evidence",
    )


def _validate_historical_attestation(
    attestation: HistoricalFinalizationAttestation,
    roots: dict[str, HistoricalRootEvidence],
    finalization_root: Path,
    executed_source_archive: Path,
    verification_source_archive: Path,
    adapter_wrapper: Path,
    cleaner_wrapper: Path,
) -> None:
    if (
        attestation.run_id != HISTORICAL_RUN_ID
        or attestation.run_manifest_sha256 != HISTORICAL_RUN_MANIFEST_SHA256
        or attestation.frozen_root_declaration_sha256
        != HISTORICAL_ROOT_DECLARATION_SHA256
        or attestation.executed.commit != HISTORICAL_EXECUTED_COMMIT
        or attestation.executed.tree != HISTORICAL_EXECUTED_TREE
        or attestation.executed.source_archive.sha256
        != HISTORICAL_EXECUTED_ARCHIVE_SHA256
        or attestation.integration.commit != HISTORICAL_INTEGRATION_COMMIT
        or attestation.integration.tree != HISTORICAL_INTEGRATION_TREE
        or attestation.verification.commit
        in {HISTORICAL_EXECUTED_COMMIT, HISTORICAL_INTEGRATION_COMMIT}
        or sum(len(root.files) for root in attestation.roots)
        != HISTORICAL_TOTAL_FILES
    ):
        raise click.ClickException("historical provenance is inconsistent")
    attested_roots = {root.role: root for root in attestation.roots}
    if attested_roots != roots:
        raise click.ClickException("historical root inventory changed")
    _validate_external_evidence(
        attestation.executed.source_archive,
        executed_source_archive,
        roots,
        finalization_root,
    )
    _validate_external_evidence(
        attestation.verification.source_archive,
        verification_source_archive,
        roots,
        finalization_root,
    )
    _validate_external_evidence(
        attestation.adapter_wrapper,
        adapter_wrapper,
        roots,
        finalization_root,
    )
    _validate_external_evidence(
        attestation.cleaner_wrapper,
        cleaner_wrapper,
        roots,
        finalization_root,
    )
    if (
        attestation.adapter_wrapper.sha256 != APPROVED_ADAPTER_WRAPPER_SHA256
        or attestation.cleaner_wrapper.sha256 != APPROVED_CLEANER_WRAPPER_SHA256
    ):
        raise click.ClickException("historical wrapper provenance is inconsistent")
    for attempt, expected in zip(
        attestation.attempts,
        HISTORICAL_VALIDATION_ATTEMPTS,
        strict=True,
    ):
        run_id, outcome, result_code, log_sha = expected
        if (
            attempt.run_id != run_id
            or attempt.outcome != outcome
            or attempt.result_code != result_code
            or attempt.log.sha256 != log_sha
            or _historical_reference_hash(attempt.log, roots) != log_sha
        ):
            raise click.ClickException("historical validation attempt changed")
        run_manifest = load_v2_manifest(
            Path(roots[attempt.run_role].canonical_path) / "manifest.json",
            "historical validation",
        )
        if run_manifest.run_id != run_id:
            raise click.ClickException("historical validation run identity changed")
    bounded = attestation.successful_bounded
    live = attestation.live
    expected_bounded = {
        "manifest": ("validation_4_run", "manifest.json", HISTORICAL_BOUNDED_MANIFEST_SHA256),
        "collection_permit": (
            "validation_4_run",
            "approved-collection-permit.json",
            HISTORICAL_BOUNDED_COLLECTION_PERMIT_SHA256,
        ),
        "validation_permit": (
            "validation_4_run",
            "adapter-validation-permit.json",
            HISTORICAL_BOUNDED_PERMIT_SHA256,
        ),
        "imported_manifest": (
            "validation_4_run",
            "imported/manifest.json",
            HISTORICAL_BOUNDED_IMPORTED_MANIFEST_SHA256,
        ),
        "imported_report": (
            "validation_4_run",
            "imported/report/import.json",
            HISTORICAL_BOUNDED_IMPORTED_REPORT_SHA256,
        ),
        "cleaned_manifest": (
            "validation_4_run",
            "cleaned/manifest.json",
            HISTORICAL_BOUNDED_CLEANED_MANIFEST_SHA256,
        ),
        "cleaned_report": (
            "validation_4_run",
            "cleaned/report/quality.json",
            HISTORICAL_BOUNDED_CLEANED_REPORT_SHA256,
        ),
    }
    for name, (role, path, digest) in expected_bounded.items():
        reference = getattr(bounded, name)
        if (
            reference is None
            or (reference.role, reference.path, reference.sha256)
            != (role, path, digest)
            or _historical_reference_hash(reference, roots) != digest
        ):
            raise click.ClickException("historical bounded evidence changed")
    expected_live = {
        "manifest": ("live_run", "manifest.json", HISTORICAL_RUN_MANIFEST_SHA256),
        "collection_permit": (
            "live_run",
            "approved-collection-permit.json",
            HISTORICAL_LIVE_PERMIT_SHA256,
        ),
        "reconciliation": (
            "live_run",
            "reconciliation.json",
            HISTORICAL_RECONCILIATION_SHA256,
        ),
        "imported_report": (
            "live_adapter_dataset",
            "report/import.json",
            HISTORICAL_REPLAY_IMPORT_REPORT_SHA256,
        ),
        "cleaned_report": (
            "live_clean_dataset",
            "report/quality.json",
            HISTORICAL_REPLAY_QUALITY_REPORT_SHA256,
        ),
    }
    for name, (role, path, digest) in expected_live.items():
        reference = getattr(live, name)
        if (
            reference is None
            or (reference.role, reference.path, reference.sha256)
            != (role, path, digest)
            or _historical_reference_hash(reference, roots) != digest
        ):
            raise click.ClickException("historical live evidence changed")
    provider_expected = {
        "package": ("cluster2-provider-package.json", APPROVED_PROVIDER_PACKAGE_SHA256),
        "provider": ("cluster2-collection-provider.py", APPROVED_PROVIDER_SHA256),
        "collector": ("cluster2-jsonl-collection.py", APPROVED_COLLECTOR_SHA256),
        "validator": ("cluster2-jsonl-validation.py", APPROVED_VALIDATOR_SHA256),
        "runbook": (
            "cluster2-bounded-validation-runbook.md",
            APPROVED_VALIDATION_RUNBOOK_SHA256,
        ),
    }
    for name, (path, digest) in provider_expected.items():
        reference = getattr(attestation.provider, name)
        if (
            (reference.role, reference.path, reference.sha256)
            != ("shared_provider", path, digest)
            or _historical_reference_hash(reference, roots) != digest
        ):
            raise click.ClickException("historical provider evidence changed")
    live_files = roots["live_run"].files
    legacy_controls = {
        "provider-invocation.json",
        ".approved-tools/checkpoint-provider",
        ".approved-tools/provider-package.zip",
    }
    if legacy_controls & set(live_files):
        raise click.ClickException(
            "historical run contains partial modern provider controls"
        )
    nonprivate = {}
    for role, root in roots.items():
        nonprivate[role] = (
            sum(item.mode & 0o077 != 0 for item in root.directories.values()),
            sum(item.mode & 0o077 != 0 for item in root.files.values()),
        )
    if any(
        nonprivate[role] != counts
        for role, counts in HISTORICAL_LEGACY_MODE_COUNTS.items()
    ) or any(
        counts != (0, 0)
        for role, counts in nonprivate.items()
        if role not in HISTORICAL_LEGACY_MODE_COUNTS
    ):
        raise click.ClickException("historical descendant mode inventory changed")
    try:
        _revalidate_historical_run(
            bounded,
            roots,
            finalization_root,
            expected_count=1,
            expected_import_report_sha256=HISTORICAL_BOUNDED_IMPORTED_REPORT_SHA256,
            expected_quality_report_sha256=HISTORICAL_BOUNDED_CLEANED_REPORT_SHA256,
        )
    except (ValueError, click.ClickException) as error:
        raise click.ClickException(
            f"historical bounded revalidation failed: {error}"
        ) from error
    try:
        _revalidate_historical_run(
            live,
            roots,
            finalization_root,
            expected_count=300,
            expected_import_report_sha256=HISTORICAL_REPLAY_IMPORT_REPORT_SHA256,
            expected_quality_report_sha256=HISTORICAL_REPLAY_QUALITY_REPORT_SHA256,
        )
    except (ValueError, click.ClickException) as error:
        raise click.ClickException(
            f"historical live revalidation failed: {error}"
        ) from error


def _validate_live_declaration(
    declaration: LiveFinalizationDeclaration,
    roots: dict[str, CleanupRootEvidence],
    manifest: CollectionManifest,
    manifest_sha256: str,
    reconciliation: ReconciliationReport,
    reconciliation_sha256: str,
    source_manifest: CollectorFileManifest,
    attestation: BoundedValidationAttestation,
) -> None:
    if (
        declaration.run_id != manifest.run_id
        or declaration.run_id == attestation.validation_run_id
        or declaration.manifest_sha256 != manifest_sha256
        or declaration.reconciliation_sha256 != reconciliation_sha256
        or declaration.validated_code_head_sha256
        != APPROVED_VALIDATED_CODE_HEAD_SHA256
        or declaration.validated_merge_commit_sha256
        != APPROVED_VALIDATED_MERGE_COMMIT_SHA256
        or declaration.validated_tree_sha256 != APPROVED_VALIDATED_TREE_SHA256
        or declaration.executed_source_archive_sha256
        != APPROVED_EXECUTED_SOURCE_ARCHIVE_SHA256
    ):
        raise click.ClickException("live finalization declaration is inconsistent")
    if (
        (declaration.provider_manifest.root_id, declaration.provider_manifest.path)
        != ("live_provider_evidence", "provider-manifest.json")
        or (declaration.adapter_report.root_id, declaration.adapter_report.path)
        != ("live_adapter_dataset", "report/import.json")
        or (declaration.adapter_manifest.root_id, declaration.adapter_manifest.path)
        != ("live_adapter_dataset", "manifest.json")
        or (declaration.cleaned_manifest.root_id, declaration.cleaned_manifest.path)
        != ("live_clean_dataset", "manifest.json")
        or (declaration.cleaned_report.root_id, declaration.cleaned_report.path)
        != ("live_clean_dataset", "report/quality.json")
    ):
        raise click.ClickException("live evidence reference uses the wrong root kind")
    live = {root.kind: root for root in declaration.cleanup_roots}
    for kind in (
        "live_harness_evidence",
        "live_provider_evidence",
        "live_adapter_dataset",
        "live_clean_dataset",
    ):
        if live.get(kind) != roots[kind]:
            raise click.ClickException("live cleanup root evidence does not match declaration")
    _reference_hash(declaration.adapter_manifest, roots)
    _reference_hash(declaration.provider_manifest, roots)
    _reference_hash(declaration.adapter_report, roots)
    _reference_hash(declaration.cleaned_manifest, roots)
    _reference_hash(declaration.cleaned_report, roots)
    provider_path = (
        Path(roots["live_provider_evidence"].canonical_path)
        / declaration.provider_manifest.path
    )
    try:
        provider = LiveProviderDeclaration.model_validate(_load_json(provider_path))
    except (json.JSONDecodeError, OSError, ValueError) as error:
        raise click.ClickException("live provider declaration is invalid") from error
    _validate_provider_provenance(
        provider,
        roots,
        "live_harness_evidence",
        manifest.run_id,
        manifest_sha256,
        300,
        source_manifest,
        "live",
    )
    if (
        provider.run_id != manifest.run_id
        or provider.manifest_sha256 != manifest_sha256
        or provider.provider_package_sha256 != APPROVED_PROVIDER_PACKAGE_SHA256
        or provider.provider_sha256 != APPROVED_PROVIDER_SHA256
        or provider.collector_sha256 != APPROVED_COLLECTOR_SHA256
        or provider.validator_sha256 != APPROVED_VALIDATOR_SHA256
        or provider.provider_snapshot_sha256 != APPROVED_PROVIDER_SHA256
        or provider.interpreter_path != APPROVED_PROVIDER_INTERPRETER_PATH
        or provider.interpreter_sha256 != APPROVED_PROVIDER_INTERPRETER_SHA256
        or provider.argv_contract
        != (
            APPROVED_PROVIDER_INTERPRETER_PATH,
            "-I",
            "-S",
            "-c",
            ISOLATED_PROVIDER_BOOTSTRAP,
            ".approved-tools/provider-package.zip",
            ".approved-tools/checkpoint-provider",
            "<action>",
            "<run_dir>",
        )
        or provider.operation_count != 300
        or provider.event_count != 300
        or set(provider.content_sha256_by_operation)
        != set(
            read_models(
                Path(roots["live_harness_evidence"].canonical_path)
                / provider.offered_ledger.path,
                OfferedOperation,
            )[index].operation_id
            for index in range(300)
        )
    ):
        raise click.ClickException("live provider declaration provenance is inconsistent")
    expected_provider_references = {
        "source_manifest": ("live_harness_evidence", "collector-file-manifest.json"),
        "fixture_catalog": ("live_harness_evidence", "fixture-catalog.json"),
        "durable_sidecar": ("live_harness_evidence", "durable-events.jsonl"),
        "checkpoints": ("live_harness_evidence", "checkpoints.jsonl"),
        "pre_read_health": ("live_harness_evidence", "pre-read-health.jsonl"),
        "execution_ledger": ("live_harness_evidence", "execution.jsonl"),
        "offered_ledger": ("live_harness_evidence", "offered.jsonl"),
        "observed": ("live_harness_evidence", "observed.jsonl"),
        "reconciliation": ("live_harness_evidence", "reconciliation.json"),
    }
    for name, expected in expected_provider_references.items():
        reference = getattr(provider, name)
        if (reference.root_id, reference.path) != expected:
            raise click.ClickException(
                "live provider reference uses the wrong canonical artifact"
            )
        _reference_hash(reference, roots)
    expected_chunks = {
        (source.raw_jsonl_path, source.sha256)
        for source in source_manifest.files
    }
    actual_chunks = {
        (reference.path, reference.sha256) for reference in provider.source_chunks
    }
    if (
        any(reference.root_id != "live_harness_evidence" for reference in provider.source_chunks)
        or actual_chunks != expected_chunks
    ):
        raise click.ClickException(
            "live source chunks do not match the collector source manifest"
        )
    for reference in provider.source_chunks:
        _reference_hash(reference, roots)
    if (
        provider.offered_ledger.sha256 != reconciliation.ledger_sha256
        or provider.observed.sha256 != reconciliation.observed_sha256
        or provider.reconciliation.sha256 != reconciliation_sha256
    ):
        raise click.ClickException("live provider evidence does not match reconciliation")
    checkpoint_rows = read_models(
        Path(roots["live_harness_evidence"].canonical_path)
        / provider.checkpoints.path,
        Checkpoint,
    )
    try:
        start_baselines = _start_baselines(checkpoint_rows, manifest)
        streams, source_hashes, source_sizes = _load_raw_events(
            Path(roots["live_harness_evidence"].canonical_path)
            / provider.source_manifest.path,
            source_manifest,
            start_baselines,
        )
        _validate_checkpoints(
            checkpoint_rows,
            manifest,
            source_manifest,
            streams,
            source_hashes,
            source_sizes,
            start_baselines,
            None,
        )
    except ValueError as error:
        raise click.ClickException(
            "live source sequence/checkpoint evidence is inconsistent"
        ) from error
    fixture_catalog_path = (
        Path(roots["live_harness_evidence"].canonical_path)
        / provider.fixture_catalog.path
    )
    try:
        fixture_catalog = FixtureCatalog.model_validate(
            _load_json(fixture_catalog_path)
        )
    except (json.JSONDecodeError, OSError, ValueError) as error:
        raise click.ClickException("live fixture catalog is invalid") from error

    adapter_root = roots["live_adapter_dataset"]
    adapter_manifest_path = (
        Path(adapter_root.canonical_path) / declaration.adapter_manifest.path
    )
    adapter_report_path = (
        Path(adapter_root.canonical_path) / declaration.adapter_report.path
    )
    try:
        adapter_manifest = LiveAdapterManifest.model_validate(
            _load_json(adapter_manifest_path)
        )
        adapter_report = LiveAdapterReport.model_validate(
            _load_json(adapter_report_path)
        )
    except (json.JSONDecodeError, OSError, ValueError) as error:
        raise click.ClickException("live adapter output manifest is invalid") from error
    runtime = adapter_manifest.generator.runtime
    input_hashes = adapter_report.input_sha256
    expected_source_hashes = {
        reference.path: reference.sha256 for reference in provider.source_chunks
    }
    if (
        runtime.run_id != manifest.run_id
        or runtime.chimera_image_sha256 != manifest.software.chimera_image_sha256
        or runtime.event_schema_sha256 != manifest.event_schema_sha256
        or runtime.run_manifest_sha256 != manifest_sha256
        or runtime.offered_ledger_sha256 != reconciliation.ledger_sha256
        or runtime.observed_sha256 != reconciliation.observed_sha256
        or runtime.event_sources_sha256 != provider.source_manifest.sha256
        or runtime.fixture_catalog_sha256 != provider.fixture_catalog.sha256
        or runtime.checkpoints_sha256 != provider.checkpoints.sha256
        or input_hashes.run_manifest != runtime.run_manifest_sha256
        or input_hashes.offered_ledger != runtime.offered_ledger_sha256
        or input_hashes.observed != runtime.observed_sha256
        or input_hashes.event_sources != runtime.event_sources_sha256
        or input_hashes.fixture_catalog != runtime.fixture_catalog_sha256
        or input_hashes.checkpoints != runtime.checkpoints_sha256
        or adapter_report.run_id != manifest.run_id
        or adapter_report.event_source_sha256 != expected_source_hashes
        or adapter_manifest.period.start != manifest.start_time
        or adapter_manifest.period.end != manifest.end_time
        or adapter_manifest.volumes != (manifest.volume_id,)
        or adapter_manifest.counts.files != len(fixture_catalog.fixtures)
        or adapter_manifest.counts.actors != 0
        or adapter_manifest.counts.patterns != 0
        or adapter_manifest.counts.by_operation != {"read": 300}
        or adapter_manifest.counts.by_origin != {"synthetic": 300}
        or adapter_manifest.counts.by_fault != {}
        or adapter_report.counts.event_sources != len(source_manifest.files)
    ):
        raise click.ClickException("live adapter output does not match provider evidence")
    adapter_artifacts = _validate_dataset_artifacts(
        adapter_root,
        adapter_manifest.files,
        declaration.adapter_manifest.path,
        ("events/", "loss/", "report/"),
    )
    valid_dates = set()
    day = manifest.start_time.date()
    while day <= manifest.end_time.date():
        valid_dates.add(day.isoformat())
        day += timedelta(days=1)
    adapter_event_rows = _validate_partitioned_rows(
        adapter_artifacts,
        "events",
        manifest.volume_id,
        valid_dates,
    )
    adapter_loss_rows = [
        AdapterLossRow.model_validate(row)
        for path, rows in adapter_artifacts.items()
        if path.startswith("loss/") and rows is not None
        for row in rows
    ]
    if (
        len(adapter_event_rows) != 300
        or any(
            row.get("origin") != "synthetic"
            or row.get("app") != "nfs4"
            or row.get("volume_id") != manifest.volume_id
            for row in adapter_event_rows
        )
        or sum(row.offered for row in adapter_loss_rows) != 300
        or sum(row.emitted for row in adapter_loss_rows) != 300
    ):
        raise click.ClickException("live adapter artifacts do not reconcile")

    clean_root = roots["live_clean_dataset"]
    clean_path = Path(clean_root.canonical_path) / declaration.cleaned_manifest.path
    clean_report_path = (
        Path(clean_root.canonical_path) / declaration.cleaned_report.path
    )
    try:
        clean_manifest = LiveCleanManifest.model_validate(_load_json(clean_path))
        clean_report = LiveCleanReport.model_validate(
            _load_json(clean_report_path)
        )
    except (json.JSONDecodeError, OSError, ValueError) as error:
        raise click.ClickException("live clean manifest is invalid") from error
    arrival = clean_report.late_arrival_s
    arrival_values = [
        arrival.p50,
        arrival.p90,
        arrival.p99,
        arrival.max,
    ]
    _validate_clean_daily_summary(
        clean_report,
        adapter_event_rows,
        manifest.volume_id,
        "live",
    )
    if (
        clean_manifest.parent.manifest_sha256 != declaration.adapter_manifest.sha256
        or clean_manifest.parent.dataset_id != adapter_manifest.dataset_id
        or clean_manifest.period != adapter_manifest.period
        or clean_report.counts != clean_manifest.counts
        or clean_report.excluded_by_reason
        != clean_manifest.counts.excluded_by_reason
        or clean_report.coverage_hours != {"complete": clean_manifest.coverage.hours}
        or clean_report.origin_mix != {"synthetic": 300}
        or clean_report.app_mix != {"nfs4": 300}
        or any(
            item.volume_id != manifest.volume_id
            or item.date not in valid_dates
            for item in clean_report.by_volume_day
        )
        or sum(item.input for item in clean_report.by_volume_day) != 300
        or sum(item.kept for item in clean_report.by_volume_day) != 0
        or sum(item.excluded for item in clean_report.by_volume_day) != 300
        or sum(
            item.count
            for item in clean_report.exclusions_by_volume_day_reason
            if item.reason == "origin_synthetic"
        )
        != 300
        or any(
            item.reason != "origin_synthetic"
            or item.volume_id != manifest.volume_id
            or item.date not in valid_dates
            for item in clean_report.exclusions_by_volume_day_reason
        )
        or clean_report.duplicate_rate != 0.0
        or clean_report.catalog_misses != {"files": 0, "events": 0}
        or arrival.negative_count != 0
        or (
            any(value is not None for value in arrival_values)
            and (
                any(
                    not isinstance(value, (int, float)) or value < 0
                    for value in arrival_values
                )
                or arrival_values != sorted(arrival_values)
            )
        )
        or clean_report.incomplete_windows
        or clean_report.opens.model_dump()
        != {
            "count": 0,
            "open_missing": 0,
            "close_missing": 0,
            "bytes_inconsistent": 0,
            "consumed_share": 0.0,
        }
        or clean_report.successful_returned_bytes != 0
        or clean_report.distinct_bytes != 0
        or clean_report.opens_by_volume_hour
        or clean_report.opens_by_volume_weekday
    ):
        raise click.ClickException("live clean manifest does not reconcile synthetic input")
    clean_artifacts = _validate_dataset_artifacts(
        clean_root,
        clean_manifest.files,
        declaration.cleaned_manifest.path,
        (
            "events_clean/",
            "excluded/",
            "coverage/",
            "opens/",
            "open_ranges/",
            "meta/",
            "report/",
            "config.resolved.yaml",
            "validation.json",
        ),
    )
    clean_kept_rows = _validate_partitioned_rows(
        clean_artifacts,
        "events_clean",
        manifest.volume_id,
        valid_dates,
    )
    clean_excluded_rows = _validate_partitioned_rows(
        clean_artifacts,
        "excluded",
        manifest.volume_id,
        valid_dates,
    )
    _validate_clean_artifact_contract(
        clean_root,
        clean_artifacts,
        clean_manifest,
        clean_report,
        adapter_event_rows,
    )
    if (
        clean_kept_rows
        or len(clean_excluded_rows) != 300
        or any(
            row.get("reason") != "origin_synthetic"
            or row.get("volume_id") != manifest.volume_id
            for row in clean_excluded_rows
        )
    ):
        raise click.ClickException("live clean artifacts do not reconcile")
    coverage_paths = [
        item.path for item in clean_manifest.files if item.path.startswith("coverage/")
    ]
    if len(coverage_paths) != 1:
        raise click.ClickException("live clean coverage artifact is missing")
    coverage_file = Path(clean_root.canonical_path) / coverage_paths[0]
    try:
        coverage_rows = [
            LiveCoverageRow.model_validate(strict_json_loads(line))
            for line in coverage_file.read_text().splitlines()
            if line
        ]
    except (json.JSONDecodeError, OSError, ValueError) as error:
        raise click.ClickException("live clean coverage artifact is invalid") from error
    expected_hours = []
    hour = manifest.start_time.replace(minute=0, second=0, microsecond=0)
    while hour < manifest.end_time:
        expected_hours.append(hour)
        hour += timedelta(hours=1)
    expected_keys = {
        (manifest.volume_id, item.isoformat()) for item in expected_hours
    }
    actual_keys = {
        (row.volume_id, row.hour_start.isoformat())
        for row in coverage_rows
    }
    if (
        len(coverage_rows) != len(expected_keys)
        or actual_keys != expected_keys
        or clean_manifest.coverage.hours != len(expected_keys)
        or sum(row.offered for row in coverage_rows) != 300
        or sum(row.emitted for row in coverage_rows) != 300
    ):
        raise click.ClickException("live clean coverage artifact does not reconcile")
    _validate_live_row_chain(
        roots,
        manifest,
        fixture_catalog,
        provider,
        adapter_event_rows,
        adapter_loss_rows,
        clean_excluded_rows,
        coverage_rows,
    )


@click.group()
def main() -> None:
    """Prepare and validate controlled READ collection evidence."""


@main.command("plan-workload")
@click.option("--manifest-template", type=click.Path(path_type=Path), required=True)
@click.option("--catalog", type=click.Path(path_type=Path), required=True)
@click.option("--run-id", required=True)
@click.option("--start-time", required=True, help="UTC ISO-8601 timestamp.")
@click.option("--duration-s", type=click.IntRange(min=1), default=10_800, show_default=True)
@click.option("--operation-count", type=click.IntRange(min=50))
@click.option("--seed", type=click.IntRange(min=0), default=42, show_default=True)
@click.option("--out", type=click.Path(path_type=Path), required=True)
def plan_workload(
    manifest_template: Path,
    catalog: Path,
    run_id: str,
    start_time: str,
    duration_s: int,
    operation_count: int | None,
    seed: int,
    out: Path,
) -> None:
    """Create an offline-only deterministic READ workload bundle."""

    from predictive_cache.semantics.read_demand import (
        SEMANTIC_VIEW_ID,
        SEMANTIC_VIEW_SHA256,
    )

    template = load_v2_manifest(manifest_template, "workload planning")
    catalog_model = load_model(catalog, FixtureCatalog)
    try:
        start = datetime.fromisoformat(start_time.replace("Z", "+00:00"))
    except ValueError as error:
        raise click.ClickException("--start-time must be an ISO-8601 timestamp") from error
    if start.tzinfo is None or start.utcoffset() != timedelta(0):
        raise click.ClickException("--start-time must be UTC-aware")
    config = WorkloadConfig(
        seed=seed,
        duration_ms=duration_s * 1000,
        operation_count=operation_count,
    )
    plan, truth = build_default_workload(
        catalog_model,
        config,
        semantic_view_id=SEMANTIC_VIEW_ID,
        semantic_view_sha256=SEMANTIC_VIEW_SHA256,
    )
    common = {
        name: getattr(template, name)
        for name in WorkloadCollectionManifest.model_fields
        if hasattr(template, name)
    }
    manifest = WorkloadCollectionManifest.model_validate(
        common
        | {
            "manifest_version": 3,
            "run_id": run_id,
            "run_ref": hashlib.sha256(run_id.encode()).hexdigest()[:32],
            "fixture_catalog_sha256": catalog_sha256(catalog_model),
            "execution_config_sha256": workload_config_sha256(config),
            "execution_plan_sha256": execution_plan_sha256(plan),
            "semantic_view_id": SEMANTIC_VIEW_ID,
            "semantic_view_sha256": SEMANTIC_VIEW_SHA256,
            "seed": seed,
            "start_time": start,
            "end_time": start + timedelta(seconds=duration_s),
            "planned_operation_count": plan.planned_operation_count,
            "fixture_distribution": "explicit_repeating_plan",
            "range_distribution": "explicit_repeating_plan",
            "range_bytes": (config.range_bytes,),
            "range_offsets": config.range_offsets,
            "tag_mode": "empty",
        }
    )
    truth_binding = WorkloadTruthBinding(
        execution_plan_sha256=manifest.execution_plan_sha256,
        workload_truth_sha256=workload_truth_sha256(truth),
        semantic_view_id=manifest.semantic_view_id,
        semantic_view_sha256=manifest.semantic_view_sha256,
    )
    atomic_write_model(out / "workload-config.json", config)
    atomic_write_model(out / "execution-plan.json", plan)
    atomic_write_model(out / "workload-truth.json", truth)
    atomic_write_model(out / "workload-truth-binding.json", truth_binding)
    atomic_write_model(out / "manifest.json", manifest)
    atomic_write_model(out / "fixture-catalog.json", catalog_model)
    report = {
        "report_version": 1,
        "data_kind": "synthetic",
        "evidence_mode": "offline_simulated_fixture",
        "purpose": "planted_pattern_recovery_only",
        "duration_s": duration_s,
        "operations": plan.planned_operation_count,
        "average_interval_s": duration_s / plan.planned_operation_count,
        "average_rate_per_s": plan.planned_operation_count / duration_s,
        "requested_bytes": plan.planned_operation_count * config.range_bytes,
        "popularity_membership": {
            "high": config.popularity_membership[0],
            "medium": config.popularity_membership[1],
            "low": config.popularity_membership[2],
        },
        "popularity_operations": dict(
            Counter(item.popularity_tier for item in truth.intent_truth)
        ),
        "coaccess_groups": len(truth.coaccess_groups),
        "burst_windows": sum(window.kind == "burst" for window in truth.windows),
        "quiet_windows": sum(window.kind == "quiet" for window in truth.windows),
        "execution_config_sha256": manifest.execution_config_sha256,
        "execution_plan_sha256": manifest.execution_plan_sha256,
        "workload_truth_sha256": workload_truth_sha256(truth),
        "limitations": [
            "Offline synthetic workload; not live or representative demand.",
            "No OPEN, CLOSE, handle or session facts are inferred from READ timing.",
        ],
    }
    atomic_write_bytes(
        out / "workload-report.json",
        (canonical_json(report) + "\n").encode(),
    )
    click.echo(canonical_json(report))


@main.command("init")
@click.option("--manifest", type=click.Path(path_type=Path), required=True)
@click.option("--catalog", type=click.Path(path_type=Path), required=True)
@click.option("--cluster-provenance", type=click.Path(path_type=Path), required=True)
@click.option("--event-schema", type=click.Path(path_type=Path), required=True)
@click.option("--budget-evidence", type=click.Path(path_type=Path), required=True)
@click.option("--execution-plan", type=click.Path(path_type=Path))
@click.option("--workload-config", type=click.Path(path_type=Path))
@click.option("--workload-truth", type=click.Path(path_type=Path))
@click.option("--workload-truth-binding", type=click.Path(path_type=Path))
@click.option("--out", type=click.Path(path_type=Path), required=True)
def init(
    manifest: Path,
    catalog: Path,
    cluster_provenance: Path,
    event_schema: Path,
    budget_evidence: Path,
    execution_plan: Path | None,
    workload_config: Path | None,
    workload_truth: Path | None,
    workload_truth_binding: Path | None,
    out: Path,
) -> None:
    """Freeze a validated manifest and fixture catalog in a private run directory."""
    manifest_model = load_run_manifest(manifest)
    catalog_model = load_model(catalog, FixtureCatalog)
    if catalog_sha256(catalog_model) != manifest_model.fixture_catalog_sha256:
        raise click.ClickException("fixture catalog checksum mismatch")
    if sha256(cluster_provenance) != manifest_model.cluster_manifest_sha256:
        raise click.ClickException("cluster provenance checksum mismatch")
    if sha256(event_schema) != manifest_model.event_schema_sha256:
        raise click.ClickException("event schema checksum mismatch")
    if sha256(budget_evidence) != manifest_model.budget_evidence_sha256:
        raise click.ClickException("budget evidence checksum mismatch")
    workload_models = None
    if isinstance(manifest_model, WorkloadCollectionManifest):
        from predictive_cache.semantics.read_demand import (
            SEMANTIC_VIEW_ID,
            SEMANTIC_VIEW_SHA256,
        )

        if (
            manifest_model.semantic_view_id != SEMANTIC_VIEW_ID
            or manifest_model.semantic_view_sha256 != SEMANTIC_VIEW_SHA256
        ):
            raise click.ClickException(
                "workload manifest semantic view differs from read-demand-v1"
            )
        if None in (
            execution_plan,
            workload_config,
            workload_truth,
            workload_truth_binding,
        ):
            raise click.ClickException(
                "workload manifest v3 requires --execution-plan, "
                "--workload-config, --workload-truth and --workload-truth-binding"
            )
        config_model = load_model(workload_config, WorkloadConfig)
        plan_model = load_model(execution_plan, WorkloadExecutionPlan)
        truth_model = load_model(workload_truth, WorkloadTruth)
        validate_plan_truth(plan_model, truth_model)
        truth_binding_model = load_model(
            workload_truth_binding,
            WorkloadTruthBinding,
        )
        regenerated_plan, regenerated_truth = build_default_workload(
            catalog_model,
            config_model,
            semantic_view_id=manifest_model.semantic_view_id,
            semantic_view_sha256=manifest_model.semantic_view_sha256,
        )
        if regenerated_plan != plan_model or regenerated_truth != truth_model:
            raise click.ClickException(
                "workload plan/truth differ from deterministic generation"
            )
        if workload_config_sha256(config_model) != manifest_model.execution_config_sha256:
            raise click.ClickException("workload config checksum mismatch")
        if any(
            (
                manifest_model.seed != config_model.seed,
                manifest_model.checkpoint_interval_s
                != config_model.checkpoint_interval_s,
                manifest_model.operation_schedule_tolerance_s
                != config_model.operation_schedule_tolerance_s,
                manifest_model.untagged_correlation_window_s
                != config_model.untagged_correlation_window_s,
                manifest_model.range_bytes != (config_model.range_bytes,),
                manifest_model.range_offsets != config_model.range_offsets,
                manifest_model.planned_operation_count
                != plan_model.planned_operation_count,
                int(
                    (manifest_model.end_time - manifest_model.start_time).total_seconds()
                    * 1000
                )
                != plan_model.duration_ms,
            )
        ):
            raise click.ClickException(
                "workload manifest controls differ from the frozen config"
            )
        if execution_plan_sha256(plan_model) != manifest_model.execution_plan_sha256:
            raise click.ClickException("execution plan checksum mismatch")
        if plan_model.fixture_catalog_sha256 != manifest_model.fixture_catalog_sha256:
            raise click.ClickException("execution plan catalog checksum mismatch")
        if truth_model.execution_plan_sha256 != manifest_model.execution_plan_sha256:
            raise click.ClickException("workload truth plan checksum mismatch")
        if (
            truth_binding_model.execution_plan_sha256
            != manifest_model.execution_plan_sha256
            or truth_binding_model.workload_truth_sha256
            != workload_truth_sha256(truth_model)
            or truth_binding_model.semantic_view_id != manifest_model.semantic_view_id
            or truth_binding_model.semantic_view_sha256
            != manifest_model.semantic_view_sha256
        ):
            raise click.ClickException("workload truth binding mismatch")
        workload_models = (
            config_model,
            plan_model,
            truth_model,
            truth_binding_model,
        )
    elif any(
        value is not None
        for value in (
            execution_plan,
            workload_config,
            workload_truth,
            workload_truth_binding,
        )
    ):
        raise click.ClickException("manifest v2 must not include workload-plan artifacts")
    atomic_write_bytes(
        out / "cluster-provenance-v1.json",
        cluster_provenance.read_bytes(),
    )
    atomic_write_bytes(out / "event-schema-v2.json", event_schema.read_bytes())
    atomic_write_bytes(out / "budget-evidence-v1.json", budget_evidence.read_bytes())
    atomic_write_model(out / "manifest.json", manifest_model)
    atomic_write_model(out / "fixture-catalog.json", catalog_model)
    if workload_models is not None:
        config_model, plan_model, truth_model, truth_binding_model = workload_models
        atomic_write_model(out / "workload-config.json", config_model)
        atomic_write_model(out / "execution-plan.json", plan_model)
        atomic_write_model(out / "workload-truth.json", truth_model)
        atomic_write_model(
            out / "workload-truth-binding.json",
            truth_binding_model,
        )
    click.echo(out)


@main.command("run")
@click.option("--run-dir", type=click.Path(path_type=Path), required=True)
@click.option("--dry-run", is_flag=True)
@click.option("--mount-root", type=click.Path(path_type=Path))
@click.option("--checkpoint-command", type=click.Path(path_type=Path))
@click.option("--provider-package", type=click.Path(path_type=Path))
@click.option("--stop-file", type=click.Path(path_type=Path))
@click.option("--validation-operations", type=click.IntRange(1, 3))
@click.option("--adapter-command", type=click.Path(path_type=Path))
@click.option("--cleaner-command", type=click.Path(path_type=Path))
def run(
    run_dir: Path,
    dry_run: bool,
    mount_root: Path | None,
    checkpoint_command: Path | None,
    provider_package: Path | None,
    stop_file: Path | None,
    validation_operations: int | None,
    adapter_command: Path | None,
    cleaner_command: Path | None,
) -> None:
    """Run with either simulated timing or an approved read-only NFS mount."""
    if dry_run == (mount_root is not None):
        raise click.ClickException("choose exactly one of --dry-run or --mount-root")
    if validation_operations is not None and (
        dry_run or adapter_command is None or cleaner_command is None
    ):
        raise click.ClickException(
            "bounded validation requires mount mode plus adapter and cleaner commands"
        )
    manifest = load_run_manifest(run_dir / "manifest.json")
    catalog = load_model(run_dir / "fixture-catalog.json", FixtureCatalog)
    execution_plan_model = (
        load_model(run_dir / "execution-plan.json", WorkloadExecutionPlan)
        if isinstance(manifest, WorkloadCollectionManifest)
        else None
    )
    if isinstance(manifest, WorkloadCollectionManifest) and not dry_run:
        raise click.ClickException(
            "workload manifest v3 is offline-only; mount and bounded validation "
            "require a separately reviewed plan-bound evidence chain"
        )
    provider_interpreter = None
    adapter_fd = None
    cleaner_fd = None
    if not dry_run:
        _validate_full_profile(manifest)
        if checkpoint_command is None or provider_package is None:
            raise click.ClickException(
                "mount execution requires --checkpoint-command and --provider-package"
            )
        provider_package = _approved_artifact(
            provider_package,
            APPROVED_PROVIDER_PACKAGE_SHA256,
            "provider package",
        )
        provider_package = _snapshot_artifact(
            provider_package,
            run_dir,
            "provider-package.zip",
            APPROVED_PROVIDER_PACKAGE_SHA256,
        )
        checkpoint_command = _approved_executable(
            checkpoint_command,
            APPROVED_PROVIDER_SHA256,
            "checkpoint command",
        )
        checkpoint_command = _snapshot_executable(
            checkpoint_command,
            run_dir,
            "checkpoint-provider",
            APPROVED_PROVIDER_SHA256,
        )
        provider_interpreter = _approved_interpreter()
    if validation_operations is not None:
        adapter_command = _approved_executable(
            adapter_command,
            APPROVED_ADAPTER_WRAPPER_SHA256,
            "adapter command",
        )
        cleaner_command = _approved_executable(
            cleaner_command,
            APPROVED_CLEANER_WRAPPER_SHA256,
            "cleaner command",
        )
        adapter_command = _snapshot_executable(
            adapter_command,
            run_dir,
            "adapter-wrapper",
            APPROVED_ADAPTER_WRAPPER_SHA256,
        )
        cleaner_command = _snapshot_executable(
            cleaner_command,
            run_dir,
            "cleaner-wrapper",
            APPROVED_CLEANER_WRAPPER_SHA256,
        )
        adapter_fd = _open_verified_fd(
            adapter_command,
            APPROVED_ADAPTER_WRAPPER_SHA256,
        )
        try:
            cleaner_fd = _open_verified_fd(
                cleaner_command,
                APPROVED_CLEANER_WRAPPER_SHA256,
            )
        except BaseException:
            os.close(adapter_fd)
            raise
        planned = planned_operations(manifest, catalog)[:validation_operations]
        atomic_write_model(
            run_dir / "adapter-validation-permit.json",
            AdapterValidationPermit(
                permit_id=str(uuid.uuid4()),
                run_id=manifest.run_id,
                full_manifest_sha256=sha256(run_dir / "manifest.json"),
                validation_operations=validation_operations,
                operation_indexes=tuple(item[0] for item in planned),
                operation_ids=tuple(item[4] for item in planned),
                provider_package_sha256=APPROVED_PROVIDER_PACKAGE_SHA256,
                provider_sha256=APPROVED_PROVIDER_SHA256,
                collector_sha256=APPROVED_COLLECTOR_SHA256,
                validator_sha256=APPROVED_VALIDATOR_SHA256,
                runbook_sha256=APPROVED_VALIDATION_RUNBOOK_SHA256,
                status="approved",
                expires_at=datetime.now(UTC) + timedelta(hours=1),
            ),
        )
    if not dry_run:
        atomic_write_model(
            run_dir / "provider-invocation.json",
            ProviderInvocationRecord(
                run_id=manifest.run_id,
                manifest_sha256=sha256(run_dir / "manifest.json"),
                provider_package_sha256=APPROVED_PROVIDER_PACKAGE_SHA256,
                provider_package_path=".approved-tools/provider-package.zip",
                provider_snapshot_path=".approved-tools/checkpoint-provider",
                provider_snapshot_sha256=APPROVED_PROVIDER_SHA256,
                collector_sha256=APPROVED_COLLECTOR_SHA256,
                validator_sha256=APPROVED_VALIDATOR_SHA256,
                interpreter_path=str(provider_interpreter),
                interpreter_sha256=APPROVED_PROVIDER_INTERPRETER_SHA256,
                argv_contract=(
                    str(provider_interpreter),
                    "-I",
                    "-S",
                    "-c",
                    ISOLATED_PROVIDER_BOOTSTRAP,
                    ".approved-tools/provider-package.zip",
                    ".approved-tools/checkpoint-provider",
                    "<action>",
                    "<run_dir>",
                ),
                actions=("checkpoint", "probe", "post-read"),
                created_at=datetime.now(UTC),
            ),
        )
    if dry_run:
        executor = DryRunExecutor()
        runner = CollectionRunner(sleep=lambda _: None)
    else:
        if manifest.tag_mode != "empty":
            raise click.ClickException("kernel mount executor requires tag_mode=empty")
        if checkpoint_command is None:
            raise click.ClickException("mount execution requires --checkpoint-command")
        cancelled = False

        def request_cancel(_signum, _frame):
            nonlocal cancelled
            cancelled = True

        signal.signal(signal.SIGINT, request_cancel)
        signal.signal(signal.SIGTERM, request_cancel)
        executor = DirectMountExecutor(
            mount_root,
            latency_guardrail_ms=manifest.stop_rules.latency_guardrail_ms,
            cancelled=lambda: cancelled,
        )
        runner = CollectionRunner()
    provider = (
        None
        if dry_run
        else CommandCheckpointProvider(
            checkpoint_command,
            provider_package,
            run_dir,
            script_sha256=APPROVED_PROVIDER_SHA256,
            package_sha256=APPROVED_PROVIDER_PACKAGE_SHA256,
            timeout_s=manifest.checkpoint_tolerance_s,
            python_executable=str(provider_interpreter),
        )
    )

    def persistent_stop() -> str | None:
        if stop_file is None or not stop_file.exists():
            return None
        return stop_file.read_text().strip() or "external stop file present"

    try:
        count = runner.run(
            manifest,
            catalog,
            run_dir,
            executor,
            execution_plan=execution_plan_model,
            simulated_timing=dry_run,
            checkpoint_provider=provider,
            persistent_stop=persistent_stop,
            operation_limit=validation_operations,
            validation_mode=validation_operations is not None,
        )
        if validation_operations is not None:
            subprocess.run(
                [
                    f"/proc/self/fd/{adapter_fd}",
                    run_dir,
                    run_dir / "adapter-validation-permit.json",
                ],
                check=True,
                pass_fds=(adapter_fd,),
            )
            subprocess.run(
                [f"/proc/self/fd/{cleaner_fd}", run_dir],
                check=True,
                pass_fds=(cleaner_fd,),
            )
    finally:
        if provider is not None:
            provider.close()
        for fd in (adapter_fd, cleaner_fd):
            if fd is not None:
                os.close(fd)
    click.echo(f"offered={count}")


@main.command("simulate-workload-source")
@click.option("--run-dir", type=click.Path(path_type=Path), required=True)
@click.option("--out", type=click.Path(path_type=Path), required=True)
def simulate_workload_source(run_dir: Path, out: Path) -> None:
    """Create importer inputs from a completed offline workload dry run."""

    manifest = load_run_manifest(run_dir / "manifest.json")
    if not isinstance(manifest, WorkloadCollectionManifest):
        raise click.ClickException(
            "offline simulated source requires workload manifest v3"
        )
    plan = load_model(run_dir / "execution-plan.json", WorkloadExecutionPlan)
    catalog = load_model(run_dir / "fixture-catalog.json", FixtureCatalog)
    offered = read_models(run_dir / "offered.jsonl", OfferedOperation)
    execution = read_models(run_dir / "execution.jsonl", ExecutionResult)
    paths = write_offline_importer_fixture(
        out,
        manifest,
        plan,
        catalog,
        offered,
        execution,
    )
    click.echo(canonical_json({name: str(path) for name, path in paths.items()}))


def _verified_dataset_rows(
    root: Path,
    manifest: dict,
    prefix: str,
    table: str | None,
) -> list[dict]:
    declared = {
        entry["path"]: entry
        for entry in manifest.get("files", [])
        if isinstance(entry, dict) and isinstance(entry.get("path"), str)
    }
    actual = set()
    for path in root.rglob("*"):
        if path.is_symlink():
            raise click.ClickException("dataset artifacts must not contain symlinks")
        if path.is_file() and path.name != "manifest.json":
            actual.add(path.relative_to(root).as_posix())
    if set(declared) != actual:
        raise click.ClickException("dataset artifact inventory differs from its manifest")
    rows = []
    for relative, entry in declared.items():
        path = root / relative
        if sha256(path) != entry.get("sha256"):
            raise click.ClickException(f"dataset artifact checksum mismatch: {relative}")
        if table is not None and relative.startswith(prefix):
            rows.extend(read_table(path, table).to_pylist())
    return rows


@main.command("recover-workload")
@click.option("--run-dir", type=click.Path(path_type=Path), required=True)
@click.option("--imported", type=click.Path(path_type=Path), required=True)
@click.option("--cleaned", type=click.Path(path_type=Path), required=True)
@click.option("--out", type=click.Path(path_type=Path), required=True)
def recover_workload(
    run_dir: Path,
    imported: Path,
    cleaned: Path,
    out: Path,
) -> None:
    """Write aggregate planted-pattern diagnostics for an offline READ workload."""

    manifest = load_run_manifest(run_dir / "manifest.json")
    if not isinstance(manifest, WorkloadCollectionManifest):
        raise click.ClickException("workload recovery requires manifest v3")
    plan = load_model(run_dir / "execution-plan.json", WorkloadExecutionPlan)
    truth = load_model(run_dir / "workload-truth.json", WorkloadTruth)
    truth_binding = load_model(
        run_dir / "workload-truth-binding.json",
        WorkloadTruthBinding,
    )
    if (
        truth_binding.execution_plan_sha256 != execution_plan_sha256(plan)
        or truth_binding.workload_truth_sha256 != workload_truth_sha256(truth)
        or truth_binding.semantic_view_id != manifest.semantic_view_id
        or truth_binding.semantic_view_sha256 != manifest.semantic_view_sha256
    ):
        raise click.ClickException("workload truth binding mismatch")
    catalog = load_model(run_dir / "fixture-catalog.json", FixtureCatalog)
    offered = read_models(run_dir / "offered.jsonl", OfferedOperation)
    imported_manifest_path = imported / "manifest.json"
    cleaned_manifest_path = cleaned / "manifest.json"
    imported_manifest = strict_json_loads(imported_manifest_path.read_text())
    cleaned_manifest = strict_json_loads(cleaned_manifest_path.read_text())
    imported_sha = sha256(imported_manifest_path)
    cleaned_sha = sha256(cleaned_manifest_path)
    plan_file_sha = sha256(run_dir / "execution-plan.json")
    runtime = imported_manifest.get("generator", {}).get("runtime", {})
    if (
        imported_manifest.get("data_kind") != "synthetic"
        or imported_manifest.get("counts", {}).get("events") != len(plan.intents)
        or runtime.get("execution_plan_sha256") != plan_file_sha
        or runtime.get("run_manifest_sha256")
        != sha256(run_dir / "manifest.json")
        or runtime.get("offered_ledger_sha256")
        != sha256(run_dir / "offered.jsonl")
        or runtime.get("source_evidence_mode") != manifest.evidence_mode
    ):
        raise click.ClickException("imported dataset does not bind the workload plan")
    expected_offers = planned_workload_operations(manifest, catalog, plan)
    if len(offered) != len(expected_offers):
        raise click.ClickException("offered ledger count differs from the workload plan")
    for operation, expected in zip(offered, expected_offers, strict=True):
        index, fixture, offset, count, operation_id = expected
        planned_at = manifest.start_time + timedelta(
            milliseconds=plan.intents[index].relative_ms
        )
        if (
            operation.operation_index != index
            or operation.operation_id != operation_id
            or operation.fixture_id != fixture.fixture_id
            or operation.volume_id != fixture.volume_id
            or operation.file_id != fixture.file_id
            or operation.offset != offset
            or operation.count != count
            or abs((operation.offered_at_utc - planned_at).total_seconds())
            > manifest.operation_schedule_tolerance_s
        ):
            raise click.ClickException("offered ledger differs from the workload plan")
    imported_rows = _verified_dataset_rows(
        imported,
        imported_manifest,
        "events/",
        "events",
    )
    offered_by_event_id = {
        str(uuid.UUID(int=int(operation.operation_id[:32], 16))): (
            index,
            operation,
            plan.intents[index],
        )
        for index, operation in enumerate(offered)
    }
    recovered_reads = []
    for event in imported_rows:
        expected = offered_by_event_id.get(event.get("event_id"))
        if expected is None:
            continue
        index, operation, intent = expected
        if any(
            (
                event.get("operation") != "read",
                event.get("result") != "ok",
                event.get("origin") != "synthetic",
                event.get("volume_id") != operation.volume_id,
                event.get("file_id") != operation.file_id,
                event.get("offset") != intent.offset,
                event.get("bytes_requested") != intent.count,
                event.get("bytes_read") != intent.count,
            )
        ):
            continue
        durable_at = event["ingest_time"]
        delay_ms = round(
            (durable_at - operation.offered_at_utc).total_seconds() * 1000
        )
        recovered_reads.append(
            OfflineSimulatedRead(
                run_id=manifest.run_id,
                execution_plan_sha256=manifest.execution_plan_sha256,
                intent_id=intent.intent_id,
                operation_id=operation.operation_id,
                fixture_id=intent.fixture_id,
                volume_id=operation.volume_id,
                file_id=operation.file_id,
                offset=intent.offset,
                count=intent.count,
                offered_at_utc=operation.offered_at_utc,
                durable_at_utc=durable_at,
                durability_delay_ms=delay_ms,
                source_sequence=index + 1,
            )
        )
    parent = cleaned_manifest.get("parent", {})
    if (
        cleaned_manifest.get("data_kind") != "synthetic"
        or parent.get("manifest_sha256") != imported_sha
        or cleaned_manifest.get("counts", {}).get("kept_events") != 0
        or cleaned_manifest.get("counts", {})
        .get("excluded_by_reason", {})
        .get("origin_synthetic")
        != len(plan.intents)
    ):
        raise click.ClickException(
            "cleaned dataset does not preserve synthetic exclusion and lineage"
        )
    _verified_dataset_rows(cleaned, cleaned_manifest, "", None)
    report = aggregate_recovery(
        plan,
        truth,
        recovered_reads,
        imported_manifest_sha256=imported_sha,
        cleaned_manifest_sha256=cleaned_sha,
        cleaned_parent_manifest_sha256=parent["manifest_sha256"],
    )
    atomic_write_bytes(
        out,
        (canonical_json(report.model_dump(mode="json")) + "\n").encode(),
    )
    click.echo(report.model_dump_json())


@main.command("checkpoint")
@click.option("--run-dir", type=click.Path(path_type=Path), required=True)
@click.option("--input", "input_path", type=click.Path(path_type=Path), required=True)
def checkpoint(run_dir: Path, input_path: Path) -> None:
    """Validate collector checkpoints and fail on any stop condition."""
    manifest = load_v2_manifest(run_dir / "manifest.json", "checkpoint validation")
    checkpoints = read_models(input_path, Checkpoint)
    reasons = checkpoint_stop_reasons(manifest, checkpoints, run_dir)
    if reasons:
        raise click.ClickException("; ".join(reasons))
    click.echo(f"checkpoints={len(checkpoints)}")


@main.command("reconcile")
@click.option("--run-dir", type=click.Path(path_type=Path), required=True)
@click.option("--observed", type=click.Path(path_type=Path), required=True)
@click.option("--checkpoints", type=click.Path(path_type=Path), required=True)
@click.option("--sources", type=click.Path(path_type=Path), required=True)
def reconcile_command(
    run_dir: Path,
    observed: Path,
    checkpoints: Path,
    sources: Path,
) -> None:
    """Write an immutable exact-accounting report and fail discrepancies."""
    load_v2_manifest(run_dir / "manifest.json", "reconciliation")
    checkpoint_models = read_models(checkpoints, Checkpoint)
    source_manifest = load_model(sources, CollectorFileManifest)
    report = reconcile(
        run_dir / "manifest.json",
        run_dir / "offered.jsonl",
        observed,
        checkpoint_models,
        source_manifest,
        run_dir,
    )
    if not report.success:
        failed_path = run_dir / f"reconciliation-failed-{report.observed_sha256[:16]}.json"
        try:
            atomic_write_model(failed_path, report)
        except FileExistsError:
            pass
        raise click.ClickException("; ".join(report.failures))
    atomic_write_model(run_dir / "reconciliation.json", report)
    click.echo(ReconciliationReport.model_validate(report).model_dump_json())


@main.command("attest-historical")
@click.option("--finalization-root", type=click.Path(path_type=Path), required=True)
@click.option("--out", type=click.Path(path_type=Path), required=True)
@click.option(
    "--executed-source-archive",
    type=click.Path(path_type=Path),
    required=True,
)
@click.option(
    "--verification-source-archive",
    type=click.Path(path_type=Path),
    required=True,
)
@click.option(
    "--verification-repository",
    type=click.Path(path_type=Path),
    required=True,
)
@click.option("--verification-commit", required=True)
@click.option(
    "--frozen-root-declaration",
    type=click.Path(path_type=Path),
    required=True,
)
@click.option("--adapter-wrapper", type=click.Path(path_type=Path), required=True)
@click.option("--cleaner-wrapper", type=click.Path(path_type=Path), required=True)
@click.option("--evidence-root", "evidence_roots", multiple=True, required=True)
@click.option("--approve-missing-controls", is_flag=True, required=True)
def attest_historical(
    finalization_root: Path,
    out: Path,
    executed_source_archive: Path,
    verification_source_archive: Path,
    frozen_root_declaration: Path,
    verification_repository: Path,
    verification_commit: str,
    adapter_wrapper: Path,
    cleaner_wrapper: Path,
    evidence_roots: tuple[str, ...],
    approve_missing_controls: bool,
) -> None:
    """Build and validate the approved historical compatibility attestation."""
    if not approve_missing_controls:
        raise click.ClickException(
            "historical attestation requires explicit missing-control approval"
        )
    finalization_root = _existing_private_root(
        finalization_root,
        "historical finalization control root",
    )
    if out.parent.resolve(strict=True) != finalization_root:
        raise click.ClickException(
            "historical attestation output must be in finalization root"
        )
    roots = _parse_historical_roots(evidence_roots)
    (
        frozen_bytes,
        frozen_sha256,
        frozen_identity,
    ) = _authorization_snapshot(
        frozen_root_declaration,
        "frozen historical root declaration",
    )
    if frozen_sha256 != HISTORICAL_ROOT_DECLARATION_SHA256:
        raise click.ClickException(
            "frozen historical root declaration is not approved"
        )
    try:
        frozen = HistoricalFrozenRootDeclaration.model_validate(
            strict_json_loads(frozen_bytes.decode())
        )
    except (UnicodeDecodeError, json.JSONDecodeError, OSError, ValueError) as error:
        raise click.ClickException(
            "frozen historical root declaration is invalid"
        ) from error
    if (
        frozen.run_id != HISTORICAL_RUN_ID
        or frozen.run_manifest_sha256 != HISTORICAL_RUN_MANIFEST_SHA256
        or {root.role: root for root in frozen.roots} != roots
    ):
        raise click.ClickException(
            "runtime roots differ from frozen historical declaration"
        )
    (
        verification_archive_bytes,
        verification_archive_evidence,
        verification_archive_identity,
    ) = _external_file_snapshot(verification_source_archive)
    (
        _executed_archive_bytes,
        executed_archive_evidence,
        executed_archive_identity,
    ) = _external_file_snapshot(executed_source_archive)
    (
        _adapter_wrapper_bytes,
        adapter_wrapper_evidence,
        adapter_wrapper_identity,
    ) = _external_file_snapshot(adapter_wrapper)
    (
        _cleaner_wrapper_bytes,
        cleaner_wrapper_evidence,
        cleaner_wrapper_identity,
    ) = _external_file_snapshot(cleaner_wrapper)
    verification_commit, verification_tree = _verification_git_provenance(
        verification_repository,
        verification_commit,
        verification_archive_bytes,
    )

    def reference(role: str, path: str) -> HistoricalEvidenceReference:
        root = roots[role]
        if path not in root.files:
            raise click.ClickException(
                f"historical evidence is missing {role}:{path}"
            )
        return HistoricalEvidenceReference(
            role=role,
            path=path,
            sha256=root.files[path].sha256,
        )

    attempts = tuple(
        HistoricalValidationAttempt(
            attempt=index,
            run_role=f"validation_{index}_run",
            input_role=f"validation_{index}_input",
            run_id=expected[0],
            outcome=expected[1],
            result_code=expected[2],
            log=reference(f"validation_{index}_input", "validation.log"),
        )
        for index, expected in enumerate(
            HISTORICAL_VALIDATION_ATTEMPTS,
            start=1,
        )
    )
    bounded = HistoricalRunEvidence(
        manifest=reference("validation_4_run", "manifest.json"),
        collection_permit=reference(
            "validation_4_run",
            "approved-collection-permit.json",
        ),
        fixture_catalog=reference("validation_4_run", "fixture-catalog.json"),
        offered_ledger=reference("validation_4_run", "offered.jsonl"),
        execution_ledger=reference("validation_4_run", "execution.jsonl"),
        observed=reference("validation_4_run", "observed.jsonl"),
        source_manifest=reference(
            "validation_4_run",
            "collector-file-manifest.json",
        ),
        durable_sidecar=reference(
            "validation_4_run",
            "durable-events.jsonl",
        ),
        checkpoints=reference("validation_4_run", "checkpoints.jsonl"),
        pre_read_health=reference(
            "validation_4_run",
            "pre-read-health.jsonl",
        ),
        validation_permit=reference(
            "validation_4_run",
            "adapter-validation-permit.json",
        ),
        imported_manifest=reference(
            "validation_4_run",
            "imported/manifest.json",
        ),
        imported_report=reference(
            "validation_4_run",
            "imported/report/import.json",
        ),
        cleaned_manifest=reference(
            "validation_4_run",
            "cleaned/manifest.json",
        ),
        cleaned_report=reference(
            "validation_4_run",
            "cleaned/report/quality.json",
        ),
    )
    live = HistoricalRunEvidence(
        manifest=reference("live_run", "manifest.json"),
        collection_permit=reference(
            "live_run",
            "approved-collection-permit.json",
        ),
        fixture_catalog=reference("live_run", "fixture-catalog.json"),
        offered_ledger=reference("live_run", "offered.jsonl"),
        execution_ledger=reference("live_run", "execution.jsonl"),
        observed=reference("live_run", "observed.jsonl"),
        source_manifest=reference(
            "live_run",
            "collector-file-manifest.json",
        ),
        durable_sidecar=reference("live_run", "durable-events.jsonl"),
        checkpoints=reference("live_run", "checkpoints.jsonl"),
        pre_read_health=reference("live_run", "pre-read-health.jsonl"),
        reconciliation=reference("live_run", "reconciliation.json"),
        imported_manifest=reference(
            "live_adapter_dataset",
            "manifest.json",
        ),
        imported_report=reference(
            "live_adapter_dataset",
            "report/import.json",
        ),
        cleaned_manifest=reference(
            "live_clean_dataset",
            "manifest.json",
        ),
        cleaned_report=reference(
            "live_clean_dataset",
            "report/quality.json",
        ),
    )
    attestation = HistoricalFinalizationAttestation(
        compatibility_scope="historical_pre_finalizer",
        run_id=HISTORICAL_RUN_ID,
        run_manifest_sha256=HISTORICAL_RUN_MANIFEST_SHA256,
        frozen_root_declaration_sha256=HISTORICAL_ROOT_DECLARATION_SHA256,
        roots=frozen.roots,
        attempts=attempts,
        successful_bounded=bounded,
        live=live,
        provider=HistoricalProviderEvidence(
            package=reference(
                "shared_provider",
                "cluster2-provider-package.json",
            ),
            provider=reference(
                "shared_provider",
                "cluster2-collection-provider.py",
            ),
            collector=reference(
                "shared_provider",
                "cluster2-jsonl-collection.py",
            ),
            validator=reference(
                "shared_provider",
                "cluster2-jsonl-validation.py",
            ),
            runbook=reference(
                "shared_provider",
                "cluster2-bounded-validation-runbook.md",
            ),
        ),
        executed=HistoricalExecutionProvenance(
            commit=HISTORICAL_EXECUTED_COMMIT,
            tree=HISTORICAL_EXECUTED_TREE,
            source_archive=executed_archive_evidence,
        ),
        integration=HistoricalIntegrationProvenance(
            commit=HISTORICAL_INTEGRATION_COMMIT,
            tree=HISTORICAL_INTEGRATION_TREE,
            executed=False,
        ),
        verification=HistoricalVerificationProvenance(
            commit=verification_commit,
            tree=verification_tree,
            source_archive=verification_archive_evidence,
        ),
        adapter_wrapper=adapter_wrapper_evidence,
        cleaner_wrapper=cleaner_wrapper_evidence,
        approval=HistoricalCompatibilityApproval(
            status="approved",
            approved_at=datetime.now(UTC),
            compatibility_scope="historical_pre_finalizer",
            provider_execution_binding_status="legacy_not_recorded",
            missing_controls=(
                "provider_invocation_record",
                "run_local_provider_snapshot",
                "run_local_package_snapshot",
            ),
            statement=(
                "Approve compatibility finalization of the frozen historical run "
                "without fabricating controls that were not recorded at execution."
            ),
        ),
    )
    _validate_historical_attestation(
        attestation,
        roots,
        finalization_root,
        executed_source_archive,
        verification_source_archive,
        adapter_wrapper,
        cleaner_wrapper,
    )
    refreshed_roots = _parse_historical_roots(evidence_roots)
    if refreshed_roots != roots or {
        root.role: root for root in frozen.roots
    } != refreshed_roots:
        raise click.ClickException(
            "historical evidence roots changed during attestation"
        )
    _verify_authorization_snapshot(
        frozen_root_declaration,
        frozen_sha256,
        frozen_identity,
        "frozen historical root declaration",
    )
    _verify_external_file_snapshot(
        executed_source_archive,
        executed_archive_evidence,
        executed_archive_identity,
        "historical executed source archive",
    )
    _verify_external_file_snapshot(
        verification_source_archive,
        verification_archive_evidence,
        verification_archive_identity,
        "historical verification source archive",
    )
    _verify_external_file_snapshot(
        adapter_wrapper,
        adapter_wrapper_evidence,
        adapter_wrapper_identity,
        "historical adapter wrapper",
    )
    _verify_external_file_snapshot(
        cleaner_wrapper,
        cleaner_wrapper_evidence,
        cleaner_wrapper_identity,
        "historical cleaner wrapper",
    )
    atomic_write_model(out, attestation)
    click.echo(attestation.model_dump_json())


@main.command("finalize-historical")
@click.option("--attestation", type=click.Path(path_type=Path), required=True)
@click.option("--finalization-root", type=click.Path(path_type=Path), required=True)
@click.option(
    "--executed-source-archive",
    type=click.Path(path_type=Path),
    required=True,
)
@click.option(
    "--verification-source-archive",
    type=click.Path(path_type=Path),
    required=True,
)
@click.option(
    "--frozen-root-declaration",
    type=click.Path(path_type=Path),
    required=True,
)
@click.option(
    "--deletion-approval",
    type=click.Path(path_type=Path),
)
@click.option(
    "--retention-approval",
    type=click.Path(path_type=Path),
)
@click.option(
    "--verification-repository",
    type=click.Path(path_type=Path),
    required=True,
)
@click.option("--merged-ref", required=True)
@click.option("--adapter-wrapper", type=click.Path(path_type=Path), required=True)
@click.option("--cleaner-wrapper", type=click.Path(path_type=Path), required=True)
@click.option("--evidence-root", "evidence_roots", multiple=True, required=True)
def finalize_historical(
    attestation: Path,
    finalization_root: Path,
    executed_source_archive: Path,
    verification_source_archive: Path,
    frozen_root_declaration: Path,
    deletion_approval: Path | None,
    retention_approval: Path | None,
    verification_repository: Path,
    merged_ref: str,
    adapter_wrapper: Path,
    cleaner_wrapper: Path,
    evidence_roots: tuple[str, ...],
) -> None:
    """Finalize the one approved pre-finalizer historical collection."""
    if (deletion_approval is None) == (retention_approval is None):
        raise click.ClickException(
            "choose exactly one of --deletion-approval or --retention-approval"
        )
    finalization_root = _existing_private_root(
        finalization_root,
        "historical finalization control root",
    )
    if (
        attestation.parent.resolve(strict=True) != finalization_root
        or attestation.is_symlink()
        or not attestation.is_file()
        or stat.S_IMODE(attestation.stat().st_mode) & 0o077
    ):
        raise click.ClickException(
            "historical attestation must be owner-only in finalization root"
        )
    try:
        (
            attestation_bytes,
            attestation_sha256,
            attestation_identity,
        ) = _authorization_snapshot(attestation, "historical attestation")
        attestation_model = HistoricalFinalizationAttestation.model_validate(
            strict_json_loads(attestation_bytes.decode())
        )
    except (UnicodeDecodeError, json.JSONDecodeError, OSError, ValueError) as error:
        raise click.ClickException("historical attestation is invalid") from error
    roots = _parse_historical_roots(evidence_roots)
    (
        frozen_bytes,
        frozen_sha256,
        frozen_identity,
    ) = _authorization_snapshot(
        frozen_root_declaration,
        "frozen historical root declaration",
    )
    if frozen_sha256 != HISTORICAL_ROOT_DECLARATION_SHA256:
        raise click.ClickException(
            "frozen historical root declaration is not approved"
        )
    try:
        frozen = HistoricalFrozenRootDeclaration.model_validate(
            strict_json_loads(frozen_bytes.decode())
        )
    except (UnicodeDecodeError, json.JSONDecodeError, OSError, ValueError) as error:
        raise click.ClickException(
            "frozen historical root declaration is invalid"
        ) from error
    if (
        frozen.run_id != HISTORICAL_RUN_ID
        or frozen.run_manifest_sha256 != HISTORICAL_RUN_MANIFEST_SHA256
        or {root.role: root for root in frozen.roots} != roots
    ):
        raise click.ClickException(
            "runtime roots differ from frozen historical declaration"
        )
    (
        verification_archive_bytes,
        verification_archive_evidence,
        verification_archive_identity,
    ) = _external_file_snapshot(verification_source_archive)
    (
        _executed_archive_bytes,
        executed_archive_evidence,
        executed_archive_identity,
    ) = _external_file_snapshot(executed_source_archive)
    (
        _adapter_wrapper_bytes,
        adapter_wrapper_evidence,
        adapter_wrapper_identity,
    ) = _external_file_snapshot(adapter_wrapper)
    (
        _cleaner_wrapper_bytes,
        cleaner_wrapper_evidence,
        cleaner_wrapper_identity,
    ) = _external_file_snapshot(cleaner_wrapper)
    if (
        verification_archive_evidence
        != attestation_model.verification.source_archive
        or executed_archive_evidence != attestation_model.executed.source_archive
        or adapter_wrapper_evidence != attestation_model.adapter_wrapper
        or cleaner_wrapper_evidence != attestation_model.cleaner_wrapper
    ):
        raise click.ClickException(
            "historical external evidence differs from attestation"
        )
    verified_commit, verified_tree = _verification_git_provenance(
        verification_repository,
        attestation_model.verification.commit,
        verification_archive_bytes,
    )
    try:
        merged_commit = subprocess.check_output(
            [
                "git",
                "-C",
                str(verification_repository),
                "rev-parse",
                f"{merged_ref}^{{commit}}",
            ],
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except (OSError, subprocess.CalledProcessError) as error:
        raise click.ClickException("historical merged verifier ref is invalid") from error
    if (
        verified_commit != attestation_model.verification.commit
        or verified_tree != attestation_model.verification.tree
        or merged_commit != verified_commit
    ):
        raise click.ClickException(
            "historical verifier provenance is not the merged verifier"
        )
    approval_path = deletion_approval or retention_approval
    approval_label = (
        "historical deletion approval"
        if deletion_approval is not None
        else "historical retention approval"
    )
    if (
        approval_path.parent.resolve(strict=True) != finalization_root
        or approval_path.is_symlink()
        or not approval_path.is_file()
        or stat.S_IMODE(approval_path.stat().st_mode) & 0o077
    ):
        raise click.ClickException(
            f"{approval_label} must be owner-only in finalization root"
        )
    try:
        (
            approval_bytes,
            approval_sha256,
            approval_identity,
        ) = _authorization_snapshot(
            approval_path,
            approval_label,
        )
        approval_model = (
            HistoricalDeletionApproval
            if deletion_approval is not None
            else HistoricalRetentionApproval
        ).model_validate(
            strict_json_loads(approval_bytes.decode())
        )
    except (UnicodeDecodeError, json.JSONDecodeError, OSError, ValueError) as error:
        raise click.ClickException(f"{approval_label} is invalid") from error
    expected_targets = {
        role: {
            "canonical_path": root.canonical_path,
            "device": root.device,
            "inode": root.inode,
            "action": (
                root.retention_action
                if deletion_approval is not None
                else "retain"
            ),
        }
        for role, root in roots.items()
    }
    if (
        approval_model.run_id != attestation_model.run_id
        or approval_model.attestation_sha256 != attestation_sha256
        or approval_model.verification_commit != verified_commit
        or approval_model.verification_tree != verified_tree
        or approval_model.approved_at
        < attestation_model.approval.approved_at
        or {
            role: target.model_dump(mode="json")
            for role, target in approval_model.targets.items()
        }
        != expected_targets
    ):
        raise click.ClickException(
            f"{approval_label} does not match exact targets"
        )
    outcome = "delete" if deletion_approval is not None else "retain_all"
    _reject_opposite_historical_artifacts(finalization_root, outcome)
    for root in roots.values():
        root_path = Path(root.canonical_path)
        if (
            finalization_root == root_path
            or finalization_root.is_relative_to(root_path)
            or root_path.is_relative_to(finalization_root)
        ):
            raise click.ClickException(
                "historical finalization control must be separate from evidence"
            )
    _validate_historical_attestation(
        attestation_model,
        roots,
        finalization_root,
        executed_source_archive,
        verification_source_archive,
        adapter_wrapper,
        cleaner_wrapper,
    )
    refreshed_roots = _parse_historical_roots(evidence_roots)
    if refreshed_roots != roots or {
        root.role: root for root in frozen.roots
    } != refreshed_roots:
        raise click.ClickException(
            "historical evidence roots changed during finalization"
        )
    _verify_authorization_snapshot(
        attestation,
        attestation_sha256,
        attestation_identity,
        "historical attestation",
    )
    _verify_authorization_snapshot(
        approval_path,
        approval_sha256,
        approval_identity,
        approval_label,
    )
    _verify_authorization_snapshot(
        frozen_root_declaration,
        frozen_sha256,
        frozen_identity,
        "frozen historical root declaration",
    )
    _verify_external_file_snapshot(
        executed_source_archive,
        executed_archive_evidence,
        executed_archive_identity,
        "historical executed source archive",
    )
    _verify_external_file_snapshot(
        verification_source_archive,
        verification_archive_evidence,
        verification_archive_identity,
        "historical verification source archive",
    )
    _verify_external_file_snapshot(
        adapter_wrapper,
        adapter_wrapper_evidence,
        adapter_wrapper_identity,
        "historical adapter wrapper",
    )
    _verify_external_file_snapshot(
        cleaner_wrapper,
        cleaner_wrapper_evidence,
        cleaner_wrapper_identity,
        "historical cleaner wrapper",
    )
    refreshed_commit, refreshed_tree = _verification_git_provenance(
        verification_repository,
        attestation_model.verification.commit,
        verification_archive_bytes,
    )
    try:
        refreshed_merged_commit = subprocess.check_output(
            [
                "git",
                "-C",
                str(verification_repository),
                "rev-parse",
                f"{merged_ref}^{{commit}}",
            ],
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
    except (OSError, subprocess.CalledProcessError) as error:
        raise click.ClickException(
            "historical merged verifier ref changed during finalization"
        ) from error
    if (
        refreshed_commit != verified_commit
        or refreshed_tree != verified_tree
        or refreshed_merged_commit != verified_commit
    ):
        raise click.ClickException(
            "historical verifier provenance changed during finalization"
        )
    _claim, claim_created = _historical_disposition_claim(
        finalization_root,
        outcome=outcome,
        run_id=attestation_model.run_id,
        attestation_sha256=attestation_sha256,
        approval_sha256=approval_sha256,
        verification_commit=verified_commit,
        verification_tree=verified_tree,
        roots=roots,
    )
    terminal_path = finalization_root / (
        "historical-cleanup-schedule.json"
        if outcome == "delete"
        else "historical-retention-record.json"
    )
    if not claim_created and (terminal_path.exists() or terminal_path.is_symlink()):
        raise click.ClickException("historical outcome is already finalized")
    if retention_approval is not None:
        record = HistoricalRetentionRecord(
            run_id=attestation_model.run_id,
            finalized_at=datetime.now(UTC),
            retention_policy="retain_indefinitely",
            attestation_sha256=attestation_sha256,
            retention_approval_sha256=approval_sha256,
            root_actions={role: "retain" for role in roots},
            delete_by=None,
        )
        atomic_write_model(
            finalization_root / "historical-retention-record.json",
            record,
        )
        click.echo(record.model_dump_json())
        return
    inventory = {
        "historical_deletion_inventory_version": 1,
        "run_id": attestation_model.run_id,
        "attestation_sha256": attestation_sha256,
        "deletion_approval_sha256": approval_sha256,
        "roots": [
            root.model_dump(mode="json")
            for root in sorted(roots.values(), key=lambda item: item.role)
        ],
        "external_evidence": {
            "executed_source_archive": attestation_model.executed.source_archive.model_dump(
                mode="json"
            ),
            "verification_source_archive": attestation_model.verification.source_archive.model_dump(
                mode="json"
            ),
            "adapter_wrapper": attestation_model.adapter_wrapper.model_dump(mode="json"),
            "cleaner_wrapper": attestation_model.cleaner_wrapper.model_dump(mode="json"),
        },
    }
    inventory_path = finalization_root / "historical-deletion-inventory.json"
    inventory_bytes = (canonical_json(inventory) + "\n").encode()
    try:
        inventory_sha = atomic_write_bytes(inventory_path, inventory_bytes)
    except FileExistsError:
        existing = inventory_path.read_bytes()
        if existing != inventory_bytes:
            raise click.ClickException(
                "historical deletion inventory conflicts with disposition claim"
            )
        inventory_sha = hashlib.sha256(existing).hexdigest()
    finalized_at = datetime.now(UTC)
    schedule = HistoricalCleanupScheduleRecord(
        run_id=attestation_model.run_id,
        finalized_at=finalized_at,
        delete_by=finalized_at + timedelta(days=7),
        cleanup_owner=load_v2_manifest(
            _historical_path(attestation_model.live.manifest, roots),
            "historical finalization",
        ).retention.cleanup_owner,
        attestation_sha256=attestation_sha256,
        deletion_approval_sha256=approval_sha256,
        deletion_inventory_sha256=inventory_sha,
        root_actions={
            role: root.retention_action for role, root in roots.items()
        },
    )
    atomic_write_model(
        finalization_root / "historical-cleanup-schedule.json",
        schedule,
    )
    click.echo(schedule.model_dump_json())


@main.command("finalize")
@click.option("--run-dir", type=click.Path(path_type=Path), required=True)
@click.option("--dataset-report", type=click.Path(path_type=Path), required=True)
@click.option("--dataset-root", type=click.Path(path_type=Path), required=True)
@click.option("--finalization-root", type=click.Path(path_type=Path), required=True)
@click.option(
    "--bounded-validation-attestation",
    type=click.Path(path_type=Path),
    required=True,
)
@click.option("--cleanup-root", "cleanup_roots", multiple=True, required=True)
def finalize(
    run_dir: Path,
    dataset_report: Path,
    dataset_root: Path,
    finalization_root: Path,
    bounded_validation_attestation: Path,
    cleanup_roots: tuple[str, ...],
) -> None:
    """Record actual validated-dataset finalization and the deletion deadline."""
    manifest = load_v2_manifest(run_dir / "manifest.json", "live finalization")
    _validate_full_profile(manifest)
    manifest_sha = sha256(run_dir / "manifest.json")
    if dataset_report.is_symlink() or not dataset_report.is_file():
        raise click.ClickException("dataset report must be a regular file")
    if stat.S_IMODE(dataset_report.stat().st_mode) & 0o077:
        raise click.ClickException("dataset report must be owner-only")
    try:
        report = _load_json(dataset_report)
    except (json.JSONDecodeError, OSError, ValueError) as error:
        raise click.ClickException("dataset report is invalid") from error
    if (
        not isinstance(report, dict)
        or report.get("run_id") != manifest.run_id
        or report.get("status") != "success"
    ):
        raise click.ClickException("dataset report does not prove successful finalization")
    output_hashes = _declared_hashes(report.get("output_hashes"), "output_hashes")
    validation_hashes = _declared_hashes(
        report.get("validation_provenance"),
        "validation_provenance",
    )
    if set(output_hashes) & set(validation_hashes):
        raise click.ClickException("dataset report paths must be unique")
    dataset_files = _inventory_root(dataset_root, "dataset output root")
    if dataset_files != output_hashes | validation_hashes:
        raise click.ClickException("dataset report hashes do not match actual outputs")
    roots = _parse_cleanup_roots(cleanup_roots)
    finalization_root = _existing_private_root(
        finalization_root,
        "finalization control root",
    )
    if (
        bounded_validation_attestation.parent.resolve(strict=True)
        != finalization_root
        or bounded_validation_attestation.is_symlink()
        or not bounded_validation_attestation.is_file()
        or stat.S_IMODE(bounded_validation_attestation.stat().st_mode) & 0o077
    ):
        raise click.ClickException(
            "bounded attestation must be an owner-only file in finalization root"
        )
    try:
        attestation = BoundedValidationAttestation.model_validate(
            _load_json(bounded_validation_attestation)
        )
    except (json.JSONDecodeError, OSError, ValueError) as error:
        raise click.ClickException("bounded validation attestation is invalid") from error
    _validate_bounded_attestation(attestation, roots, manifest)
    run_root = run_dir.absolute().resolve(strict=True)
    dataset_root = dataset_root.resolve(strict=True)
    if Path(roots["live_harness_evidence"].canonical_path) != run_root:
        raise click.ClickException("live harness cleanup root must equal run_dir")
    if Path(roots["live_clean_dataset"].canonical_path) != dataset_root:
        raise click.ClickException("live clean cleanup root must equal dataset_root")
    for root in (Path(item.canonical_path) for item in roots.values()):
        if (
            finalization_root == root
            or finalization_root.is_relative_to(root)
            or root.is_relative_to(finalization_root)
        ):
            raise click.ClickException(
                "finalization control root must be separate from evidence roots"
            )
    source_manifest = load_model(
        run_dir / "collector-file-manifest.json",
        CollectorFileManifest,
    )
    if source_manifest.run_id != manifest.run_id:
        raise click.ClickException("collector file manifest run identity mismatch")
    required_run_files = (
        "manifest.json",
        "fixture-catalog.json",
        "offered.jsonl",
        "execution.jsonl",
        "durable-events.jsonl",
        "observed.jsonl",
        "collector-file-manifest.json",
        "pre-read-health.jsonl",
        "checkpoints.jsonl",
        "reconciliation.json",
    )
    missing = [name for name in required_run_files if not (run_dir / name).is_file()]
    if missing:
        raise click.ClickException("full-run evidence is incomplete")
    reconciliation = load_model(run_dir / "reconciliation.json", ReconciliationReport)
    if (
        reconciliation.run_id != manifest.run_id
        or not reconciliation.success
        or reconciliation.failures
        or reconciliation.duplicate_operation_ids
        or reconciliation.missing_operation_ids
        or reconciliation.unexpected_operation_ids
        or reconciliation.unknown_event_ids
        or reconciliation.manifest_sha256 != manifest_sha
        or reconciliation.ledger_sha256 != sha256(run_dir / "offered.jsonl")
        or reconciliation.observed_sha256 != sha256(run_dir / "observed.jsonl")
        or reconciliation.offered_count != manifest.offered_count
        or reconciliation.unique_durable_count != manifest.offered_count
    ):
        raise click.ClickException("successful reconciliation evidence is required")
    try:
        live_declaration = LiveFinalizationDeclaration.model_validate(
            report.get("live_finalization")
        )
    except ValueError as error:
        raise click.ClickException("dataset report lacks live finalization evidence") from error
    _validate_live_declaration(
        live_declaration,
        roots,
        manifest,
        manifest_sha,
        reconciliation,
        sha256(run_dir / "reconciliation.json"),
        source_manifest,
        attestation,
    )
    candidates = [item.raw_jsonl_path for item in source_manifest.files]
    candidates.extend(required_run_files)
    try:
        for name in sorted(set(candidates)):
            sha256(run_dir / name)
        inventory = {
            "inventory_version": 1,
            "run_id": manifest.run_id,
            "bounded_validation_run_id": attestation.validation_run_id,
            "roots": [
                root.model_dump(mode="json")
                for root in sorted(roots.values(), key=lambda item: item.kind)
            ],
            "control_files": {
                bounded_validation_attestation.name: {
                    "sha256": sha256(bounded_validation_attestation),
                    "mode": stat.S_IMODE(
                        bounded_validation_attestation.stat().st_mode
                    ),
                }
            },
        }
    except (FileNotFoundError, OSError, ValueError) as error:
        raise click.ClickException("deletion inventory source is unavailable") from error
    inventory_bytes = (canonical_json(inventory) + "\n").encode()
    inventory_sha = atomic_write_bytes(
        finalization_root / "deletion-inventory.json",
        inventory_bytes,
    )
    finalized_at = datetime.now(UTC)
    schedule = CleanupScheduleRecord(
        run_id=manifest.run_id,
        finalized_at=finalized_at,
        delete_by=finalized_at + timedelta(days=7),
        cleanup_owner=manifest.retention.cleanup_owner,
        validation_report_sha256=sha256(dataset_report),
        deletion_inventory_sha256=inventory_sha,
    )
    atomic_write_model(finalization_root / "cleanup-schedule.json", schedule)
    click.echo(schedule.model_dump_json())


@main.command("complete-cleanup")
@click.option("--schedule", type=click.Path(path_type=Path), required=True)
@click.option("--completion", type=click.Path(path_type=Path), required=True)
@click.option("--out", type=click.Path(path_type=Path), required=True)
def complete_cleanup(schedule: Path, completion: Path, out: Path) -> None:
    """Validate cleanup completion against the immutable schedule."""
    schedule_model = load_model(schedule, CleanupScheduleRecord)
    completion_model = load_model(completion, CleanupCompletionRecord)
    if (
        completion_model.schedule_sha256 != sha256(schedule)
        or completion_model.run_id != schedule_model.run_id
        or completion_model.cleanup_owner != schedule_model.cleanup_owner
        or completion_model.delete_by != schedule_model.delete_by
        or completion_model.validation_report_sha256
        != schedule_model.validation_report_sha256
        or completion_model.deletion_inventory_sha256
        != schedule_model.deletion_inventory_sha256
        or completion_model.completed_at < schedule_model.finalized_at
    ):
        raise click.ClickException("cleanup completion does not match schedule")
    atomic_write_model(out, completion_model)
    click.echo(completion_model.model_dump_json())


@main.command("complete-cleanup-historical")
@click.option("--schedule", type=click.Path(path_type=Path), required=True)
@click.option("--completion", type=click.Path(path_type=Path), required=True)
@click.option("--out", type=click.Path(path_type=Path), required=True)
def complete_cleanup_historical(
    schedule: Path,
    completion: Path,
    out: Path,
) -> None:
    """Validate cleanup completion for historical compatibility evidence."""
    disposition_path = schedule.parent / "historical-disposition.json"
    retention_path = schedule.parent / "historical-retention-record.json"
    if retention_path.exists() or retention_path.is_symlink():
        raise click.ClickException(
            "historical cleanup is forbidden by retain-all disposition"
        )
    disposition = load_model(
        disposition_path,
        HistoricalDispositionClaim,
    )
    if disposition.outcome != "delete":
        raise click.ClickException(
            "historical cleanup requires delete disposition"
        )
    schedule_model = load_model(schedule, HistoricalCleanupScheduleRecord)
    completion_model = load_model(
        completion,
        HistoricalCleanupCompletionRecord,
    )
    if (
        completion_model.schedule_sha256 != sha256(schedule)
        or completion_model.run_id != schedule_model.run_id
        or completion_model.cleanup_owner != schedule_model.cleanup_owner
        or completion_model.delete_by != schedule_model.delete_by
        or completion_model.attestation_sha256
        != schedule_model.attestation_sha256
        or completion_model.deletion_approval_sha256
        != schedule_model.deletion_approval_sha256
        or completion_model.deletion_inventory_sha256
        != schedule_model.deletion_inventory_sha256
        or completion_model.completed_at < schedule_model.finalized_at
        or disposition.run_id != schedule_model.run_id
        or disposition.attestation_sha256
        != schedule_model.attestation_sha256
        or disposition.approval_sha256
        != schedule_model.deletion_approval_sha256
        or any(
            (
                schedule_model.root_actions[role] == "delete"
                and disposition != "deleted"
            )
            or (
                schedule_model.root_actions[role] == "retain"
                and disposition != "retained_verified"
            )
            for role, disposition in completion_model.root_dispositions.items()
        )
    ):
        raise click.ClickException(
            "historical cleanup completion does not match schedule"
        )
    atomic_write_model(out, completion_model)
    click.echo(completion_model.model_dump_json())


if __name__ == "__main__":
    main()
