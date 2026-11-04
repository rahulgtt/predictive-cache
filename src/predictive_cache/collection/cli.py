"""CLI for offline controlled collection preparation and reconciliation."""

from __future__ import annotations

import hashlib
import json
import os
import signal
import stat
import subprocess
import uuid
from collections import Counter
from datetime import UTC, datetime, timedelta
from pathlib import Path, PurePosixPath

import click
import pyarrow.parquet as pq
import yaml

from predictive_cache.datagen.io import config_hash
from predictive_cache.datagen.schema import normalize_rows
from predictive_cache.prep.clean.report import render_summary
from predictive_cache.prep.config import CleanConfig
from predictive_cache.sources.chimera.importer import (
    _load_raw_events,
    _start_baselines,
    _validate_checkpoints,
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
)


def load_model(path: Path, model):
    return model.model_validate_json(path.read_text())


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
        for rows in (offered, executions, durable, observed, health_rows)
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
    health_by_id = {row.operation_id: row for row in health_rows}
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
        or len(health_by_id) != expected_count
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
    expected_by_event = {}
    for operation in offered:
        execution = execution_by_id[operation.operation_id]
        observation = observed_by_id[operation.operation_id]
        health = health_by_id[operation.operation_id]
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
        if (
            not operation.offered_at_utc
            <= health.captured_at
            <= execution.completed_at_utc
            or health.captured_at > raw_event_time + skew
            or health.run_id != operation.run_id
            or health.pod_name != sidecar.pod_name
            or health.pod_uid != sidecar.pod_uid
            or health.node_name != sidecar.node_name
            or health.producer_epoch != sidecar.producer_epoch
            or health.drops.total
            or health.delivery_clean != 1
            or health.barrier_ready != 1
            or not health.collector_sink_healthy
            or not health.storage_healthy
            or not health.cluster_healthy
            or not health.volume_healthy
            or not health.export_healthy
            or health.parse_errors
            or health.identity_mismatches
            or health.checksum_mismatches
            or health.foreground_read_errors
        ):
            conflicts.append("pre-read health")
        stream_identity = (
            sidecar.pod_name,
            sidecar.pod_uid,
            sidecar.node_name,
            sidecar.producer_epoch,
        )
        watermark = expected_watermarks.get(stream_identity)
        if (
            watermark is None
            or (
                health.last_enqueued_sequence,
                health.last_synced_sequence,
                health.synced_count,
            )
            != (watermark, watermark, watermark)
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
            raise click.ClickException("live adapter event row conflicts with evidence")
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


@main.command("init")
@click.option("--manifest", type=click.Path(path_type=Path), required=True)
@click.option("--catalog", type=click.Path(path_type=Path), required=True)
@click.option("--cluster-provenance", type=click.Path(path_type=Path), required=True)
@click.option("--event-schema", type=click.Path(path_type=Path), required=True)
@click.option("--budget-evidence", type=click.Path(path_type=Path), required=True)
@click.option("--out", type=click.Path(path_type=Path), required=True)
def init(
    manifest: Path,
    catalog: Path,
    cluster_provenance: Path,
    event_schema: Path,
    budget_evidence: Path,
    out: Path,
) -> None:
    """Freeze a validated manifest and fixture catalog in a private run directory."""
    manifest_model = load_model(manifest, CollectionManifest)
    catalog_model = load_model(catalog, FixtureCatalog)
    if catalog_sha256(catalog_model) != manifest_model.fixture_catalog_sha256:
        raise click.ClickException("fixture catalog checksum mismatch")
    if sha256(cluster_provenance) != manifest_model.cluster_manifest_sha256:
        raise click.ClickException("cluster provenance checksum mismatch")
    if sha256(event_schema) != manifest_model.event_schema_sha256:
        raise click.ClickException("event schema checksum mismatch")
    if sha256(budget_evidence) != manifest_model.budget_evidence_sha256:
        raise click.ClickException("budget evidence checksum mismatch")
    atomic_write_bytes(
        out / "cluster-provenance-v1.json",
        cluster_provenance.read_bytes(),
    )
    atomic_write_bytes(out / "event-schema-v2.json", event_schema.read_bytes())
    atomic_write_bytes(out / "budget-evidence-v1.json", budget_evidence.read_bytes())
    atomic_write_model(out / "manifest.json", manifest_model)
    atomic_write_model(out / "fixture-catalog.json", catalog_model)
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
    manifest = load_model(run_dir / "manifest.json", CollectionManifest)
    catalog = load_model(run_dir / "fixture-catalog.json", FixtureCatalog)
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


@main.command("checkpoint")
@click.option("--run-dir", type=click.Path(path_type=Path), required=True)
@click.option("--input", "input_path", type=click.Path(path_type=Path), required=True)
def checkpoint(run_dir: Path, input_path: Path) -> None:
    """Validate collector checkpoints and fail on any stop condition."""
    manifest = load_model(run_dir / "manifest.json", CollectionManifest)
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
    manifest = load_model(run_dir / "manifest.json", CollectionManifest)
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


if __name__ == "__main__":
    main()
