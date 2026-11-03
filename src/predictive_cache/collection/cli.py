"""CLI for offline controlled collection preparation and reconciliation."""

from __future__ import annotations

import json
import os
import signal
import stat
import subprocess
from datetime import UTC, datetime, timedelta
from pathlib import Path, PurePosixPath

import click

from .harness import (
    CollectionRunner,
    CommandCheckpointProvider,
    DirectMountExecutor,
    DryRunExecutor,
    catalog_sha256,
    checkpoint_stop_reasons,
    reconcile,
)
from .io import (
    atomic_write_bytes,
    atomic_write_model,
    canonical_json,
    read_models,
    sha256,
)
from .models import (
    Checkpoint,
    CleanupScheduleRecord,
    CollectionManifest,
    CollectorFileManifest,
    FixtureCatalog,
    ReconciliationReport,
)

APPROVED_PROVIDER_PACKAGE_SHA256 = (
    "3c6d3a4cb164cc4df87c36480dab49621e140f9307a170e393233eb8221feece"
)
APPROVED_PROVIDER_SHA256 = (
    "51d92af0b0beab61ac77dd4dd7b5d19cd9a4fd83e4a709e74582b3be244682d4"
)
APPROVED_COLLECTOR_SHA256 = (
    "09158174889f25a6abd7a70a33d24ad1d2efaba3f8ec935e0d5344eba5959225"
)
APPROVED_VALIDATOR_SHA256 = (
    "8d3dc5aad205c6fca55aa8b7c34e933906ca3f7758befb9c1bdf1d835c53e7da"
)
APPROVED_VALIDATION_RUNBOOK_SHA256 = (
    "cf5e2238aa026aa5f3f8f80c7e46b8658859a349c3a2748b7a425e4bb3b405be"
)


def load_model(path: Path, model):
    return model.model_validate_json(path.read_text())


def _load_json(path: Path) -> object:
    def unique_object(pairs):
        value = {}
        for key, item in pairs:
            if key in value:
                raise ValueError(f"duplicate JSON key: {key}")
            value[key] = item
        return value

    return json.loads(path.read_text(), object_pairs_hook=unique_object)


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


def _validate_external_evidence(
    root: Path,
    manifest: CollectionManifest,
    manifest_sha256: str,
) -> dict[str, str]:
    inventory = _inventory_root(root, "validation evidence root")
    manifest_name = "validation-manifest.json"
    if manifest_name not in inventory:
        raise click.ClickException("validation evidence manifest is missing")
    try:
        validation = _load_json(root / manifest_name)
    except (json.JSONDecodeError, OSError, ValueError) as error:
        raise click.ClickException("validation evidence manifest is invalid") from error
    required = {
        "validation_evidence_version",
        "run_id",
        "full_manifest_sha256",
        "provider_package_sha256",
        "provider_sha256",
        "collector_sha256",
        "validator_sha256",
        "runbook_sha256",
        "validation_operations",
        "status",
        "files",
    }
    if (
        not isinstance(validation, dict)
        or set(validation) != required
        or validation["validation_evidence_version"] != 1
        or validation["run_id"] != manifest.run_id
        or validation["full_manifest_sha256"] != manifest_sha256
        or validation["provider_package_sha256"] != APPROVED_PROVIDER_PACKAGE_SHA256
        or validation["provider_sha256"] != APPROVED_PROVIDER_SHA256
        or validation["collector_sha256"] != APPROVED_COLLECTOR_SHA256
        or validation["validator_sha256"] != APPROVED_VALIDATOR_SHA256
        or validation["runbook_sha256"] != APPROVED_VALIDATION_RUNBOOK_SHA256
        or type(validation["validation_operations"]) is not int
        or not 1 <= validation["validation_operations"] <= 3
        or validation["status"] != "success"
    ):
        raise click.ClickException("validation evidence manifest is not approved")
    declared = _declared_hashes(validation["files"], "validation evidence files")
    actual = {name: digest for name, digest in inventory.items() if name != manifest_name}
    if actual != declared:
        raise click.ClickException("validation evidence hashes do not match actual files")
    return inventory


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
@click.option("--stop-file", type=click.Path(path_type=Path))
@click.option("--validation-operations", type=click.IntRange(1, 3))
@click.option("--adapter-command", type=click.Path(path_type=Path))
@click.option("--cleaner-command", type=click.Path(path_type=Path))
def run(
    run_dir: Path,
    dry_run: bool,
    mount_root: Path | None,
    checkpoint_command: Path | None,
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
            run_dir,
            timeout_s=manifest.checkpoint_tolerance_s,
        )
    )

    def persistent_stop() -> str | None:
        if stop_file is None or not stop_file.exists():
            return None
        return stop_file.read_text().strip() or "external stop file present"

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
        subprocess.run([adapter_command, run_dir], check=True)
        subprocess.run([cleaner_command, run_dir], check=True)
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
@click.option(
    "--validation-evidence-root",
    type=click.Path(path_type=Path),
    required=True,
)
def finalize(
    run_dir: Path,
    dataset_report: Path,
    dataset_root: Path,
    validation_evidence_root: Path,
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
    validation_files = _validate_external_evidence(
        validation_evidence_root,
        manifest,
        manifest_sha,
    )
    run_root = run_dir.absolute().resolve(strict=True)
    dataset_root = dataset_root.resolve(strict=True)
    validation_evidence_root = validation_evidence_root.resolve(strict=True)
    if (
        validation_evidence_root == run_root
        or validation_evidence_root.is_relative_to(run_root)
        or run_root.is_relative_to(validation_evidence_root)
        or validation_evidence_root == dataset_root
        or validation_evidence_root.is_relative_to(dataset_root)
        or dataset_root.is_relative_to(validation_evidence_root)
    ):
        raise click.ClickException("validation evidence root must be independent")
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
        or reconciliation.manifest_sha256 != manifest_sha
        or reconciliation.ledger_sha256 != sha256(run_dir / "offered.jsonl")
        or reconciliation.observed_sha256 != sha256(run_dir / "observed.jsonl")
        or reconciliation.offered_count != manifest.offered_count
        or reconciliation.unique_durable_count != manifest.offered_count
    ):
        raise click.ClickException("successful reconciliation evidence is required")
    candidates = [item.raw_jsonl_path for item in source_manifest.files]
    candidates.extend(required_run_files)
    try:
        run_files = [
            {"path": name, "sha256": sha256(run_dir / name)}
            for name in sorted(set(candidates))
        ]
        inventory = {
            "inventory_version": 1,
            "run_id": manifest.run_id,
            "roots": [
                {
                    "kind": "run_evidence",
                    "path": str(run_root),
                    "files": run_files,
                },
                {
                    "kind": "validated_dataset",
                    "path": str(dataset_root),
                    "files": [
                        {"path": name, "sha256": digest}
                        for name, digest in sorted(dataset_files.items())
                    ],
                },
                {
                    "kind": "bounded_validation_evidence",
                    "path": str(validation_evidence_root),
                    "files": [
                        {"path": name, "sha256": digest}
                        for name, digest in sorted(validation_files.items())
                    ],
                },
            ],
        }
    except (FileNotFoundError, OSError, ValueError) as error:
        raise click.ClickException("deletion inventory source is unavailable") from error
    inventory_bytes = (canonical_json(inventory) + "\n").encode()
    inventory_sha = atomic_write_bytes(
        run_dir / "deletion-inventory.json",
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
    atomic_write_model(run_dir / "cleanup-schedule.json", schedule)
    click.echo(schedule.model_dump_json())


if __name__ == "__main__":
    main()
