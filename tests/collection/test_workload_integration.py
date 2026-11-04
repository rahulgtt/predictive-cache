from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from click.testing import CliRunner

from predictive_cache.collection.cli import main
from predictive_cache.collection.harness import (
    CollectionRunner,
    DryRunExecutor,
    _validate_actual_workload_cadence,
    catalog_sha256,
)
from predictive_cache.collection.io import atomic_write_model, read_models, sha256
from predictive_cache.collection.models import (
    ExecutionResult,
    FixtureCatalog,
    FixtureEntry,
    OfferedOperation,
    RetentionPolicy,
    SoftwareVersions,
    StopRules,
    WorkloadCollectionManifest,
    parse_collection_manifest,
)
from predictive_cache.collection.workload import (
    WorkloadConfig,
    WorkloadTruth,
    WorkloadTruthBinding,
    bind_workload_truth,
    build_default_workload,
    build_workload_collection_permit,
    execution_plan_sha256,
    resolved_operation_count,
    workload_config_sha256,
    workload_truth_sha256,
    write_offline_importer_fixture,
)
from predictive_cache.prep.clean.pipeline import run_clean
from predictive_cache.prep.config import CleanConfig
from predictive_cache.semantics.read_demand import (
    SEMANTIC_VIEW_ID,
    SEMANTIC_VIEW_SHA256,
)
from predictive_cache.sources.chimera.importer import import_dataset

START = datetime(2026, 11, 4, microsecond=2, tzinfo=UTC)
VOLUME_ID = "a" * 32


def _catalog() -> FixtureCatalog:
    return FixtureCatalog(
        fixtures=tuple(
            FixtureEntry(
                fixture_id=f"fixture-{index:02d}",
                volume_id=VOLUME_ID,
                file_id=hashlib.sha256(f"file-{index:02d}".encode()).hexdigest(),
                dir_id=f"dir-{index:02d}",
                export_id=7,
                export_name="controlled",
                export_path="/controlled",
                relative_path=f"fixtures/{index:02d}.bin",
                size_bytes=65536,
                sha256=hashlib.sha256(f"content-{index:02d}".encode()).hexdigest(),
            )
            for index in range(60)
        )
    )


def _copy_contract(source: Path, destination: Path) -> None:
    destination.write_bytes(source.read_bytes())
    destination.chmod(0o600)


def test_plan_workload_cli_binds_the_shared_semantic_contract(tmp_path: Path):
    root = Path(__file__).parents[2]
    catalog_path = tmp_path / "catalog.json"
    atomic_write_model(catalog_path, _catalog())
    bundle = tmp_path / "bundle"
    result = CliRunner().invoke(
        main,
        [
            "plan-workload",
            "--manifest-template",
            str(root / "tests/collection/fixtures/frozen-manifest.json"),
            "--catalog",
            str(catalog_path),
            "--run-id",
            "offline-cli-workload",
            "--start-time",
            "2026-11-04T00:00:00Z",
            "--out",
            str(bundle),
        ],
    )
    assert result.exit_code == 0, result.output
    manifest = parse_collection_manifest((bundle / "manifest.json").read_bytes())
    assert isinstance(manifest, WorkloadCollectionManifest)
    assert manifest.semantic_view_id == SEMANTIC_VIEW_ID
    assert manifest.semantic_view_sha256 == SEMANTIC_VIEW_SHA256
    assert manifest.planned_operation_count == resolved_operation_count(
        WorkloadConfig()
    )

    initialized = tmp_path / "initialized"
    init_args = [
        "init",
        "--manifest",
        str(bundle / "manifest.json"),
        "--catalog",
        str(bundle / "fixture-catalog.json"),
        "--cluster-provenance",
        str(root / "contracts/cluster-provenance-v1.json"),
        "--event-schema",
        str(root / "contracts/nfs4-read-v2.schema.json"),
        "--budget-evidence",
        str(root / "tests/collection/fixtures/budget-evidence-v1.json"),
        "--execution-plan",
        str(bundle / "execution-plan.json"),
        "--workload-config",
        str(bundle / "workload-config.json"),
        "--workload-truth",
        str(bundle / "workload-truth.json"),
        "--workload-truth-binding",
        str(bundle / "workload-truth-binding.json"),
        "--out",
        str(initialized),
    ]
    initialized_result = CliRunner().invoke(main, init_args)
    assert initialized_result.exit_code == 0, initialized_result.output

    for name, update in (
        ("seed", {"seed": manifest.seed + 1}),
        (
            "count",
            {"planned_operation_count": manifest.planned_operation_count - 1},
        ),
        ("duration", {"end_time": manifest.end_time - timedelta(seconds=1)}),
    ):
        tampered_manifest_path = tmp_path / f"tampered-manifest-{name}.json"
        atomic_write_model(
            tampered_manifest_path,
            manifest.model_copy(update=update),
        )
        manifest_args = [
            str(tampered_manifest_path)
            if value == str(bundle / "manifest.json")
            else value
            for value in init_args
        ]
        manifest_args[-1] = str(tmp_path / f"tampered-manifest-{name}-init")
        manifest_result = CliRunner().invoke(main, manifest_args)
        assert manifest_result.exit_code != 0
        assert "controls differ" in manifest_result.output

    truth = WorkloadTruth.model_validate_json(
        (bundle / "workload-truth.json").read_text()
    )
    tampered_row = truth.intent_truth[0].model_copy(
        update={"synthetic_actor_id": "actor-tampered"}
    )
    tampered_truth = truth.model_copy(
        update={"intent_truth": (tampered_row, *truth.intent_truth[1:])}
    )
    tampered_path = tmp_path / "tampered-truth.json"
    atomic_write_model(tampered_path, tampered_truth)
    tampered_args = [
        str(tampered_path) if value == str(bundle / "workload-truth.json") else value
        for value in init_args
    ]
    tampered_args[-1] = str(tmp_path / "tampered-init")
    tampered_result = CliRunner().invoke(main, tampered_args)
    assert tampered_result.exit_code != 0
    assert "differ from deterministic generation" in tampered_result.output


def test_offline_plan_dry_run_import_and_clean_excludes_all_synthetic_reads(
    tmp_path: Path,
):
    root = Path(__file__).parents[2]
    run_dir = tmp_path / "run"
    run_dir.mkdir(mode=0o700)
    cluster_path = run_dir / "cluster-provenance-v1.json"
    schema_path = run_dir / "event-schema-v2.json"
    budget_path = run_dir / "budget-evidence-v1.json"
    _copy_contract(root / "contracts/cluster-provenance-v1.json", cluster_path)
    _copy_contract(root / "contracts/nfs4-read-v2.schema.json", schema_path)
    _copy_contract(
        root / "tests/collection/fixtures/budget-evidence-v1.json",
        budget_path,
    )

    catalog = _catalog()
    config = WorkloadConfig()
    plan, truth = build_default_workload(
        catalog,
        config,
        semantic_view_id=SEMANTIC_VIEW_ID,
        semantic_view_sha256=SEMANTIC_VIEW_SHA256,
    )
    assert plan.semantic_view_id == SEMANTIC_VIEW_ID
    assert plan.semantic_view_sha256 == SEMANTIC_VIEW_SHA256
    manifest = WorkloadCollectionManifest(
        run_id="offline-workload-smoke",
        run_ref=hashlib.sha256(b"offline-workload-smoke").hexdigest()[:32],
        cluster_id="offline-fixture",
        namespace="offline",
        volume_id=VOLUME_ID,
        export_id=7,
        cluster_manifest_sha256=sha256(cluster_path),
        fixture_catalog_sha256=catalog_sha256(catalog),
        event_schema_sha256=sha256(schema_path),
        budget_evidence_sha256=sha256(budget_path),
        execution_config_sha256=workload_config_sha256(config),
        execution_plan_sha256=execution_plan_sha256(plan),
        semantic_view_id=SEMANTIC_VIEW_ID,
        semantic_view_sha256=SEMANTIC_VIEW_SHA256,
        seed=config.seed,
        start_time=START,
        end_time=START + timedelta(milliseconds=config.duration_ms),
        planned_operation_count=plan.planned_operation_count,
        range_bytes=(config.range_bytes,),
        range_offsets=config.range_offsets,
        client_id="offline-workload",
        software=SoftwareVersions(
            predictive_cache_revision="test",
            chimera_image="offline",
            chimera_image_sha256="2" * 64,
            cufs_image="offline",
            cufs_image_sha256="3" * 64,
            collector_schema_version="chimera-read-event-v2",
        ),
        stop_rules=StopRules(latency_guardrail_ms=5000),
        retention=RetentionPolicy(
            owner="test-owner",
            cleanup_owner="test-owner",
            cleanup_action="delete test evidence",
        ),
    )
    atomic_write_model(run_dir / "manifest.json", manifest)
    atomic_write_model(run_dir / "fixture-catalog.json", catalog)
    atomic_write_model(run_dir / "execution-plan.json", plan)
    atomic_write_model(run_dir / "workload-truth.json", truth)
    atomic_write_model(
        run_dir / "workload-truth-binding.json",
        WorkloadTruthBinding(
            execution_plan_sha256=manifest.execution_plan_sha256,
            workload_truth_sha256=workload_truth_sha256(truth),
            semantic_view_id=SEMANTIC_VIEW_ID,
            semantic_view_sha256=SEMANTIC_VIEW_SHA256,
        ),
    )

    live_attempt = CliRunner().invoke(
        main,
        ["run", "--run-dir", str(run_dir), "--mount-root", str(tmp_path / "mount")],
    )
    assert live_attempt.exit_code != 0
    assert "requires --workload-permit" in live_attempt.output

    count = CollectionRunner(sleep=lambda _: None).run(
        manifest,
        catalog,
        run_dir,
        DryRunExecutor(),
        execution_plan=plan,
        simulated_timing=True,
    )
    expected_count = plan.planned_operation_count
    assert count == expected_count

    simulated_dir = tmp_path / "simulated"
    paths = write_offline_importer_fixture(
        simulated_dir,
        manifest,
        plan,
        catalog,
        read_models(run_dir / "offered.jsonl", OfferedOperation),
        read_models(run_dir / "execution.jsonl", ExecutionResult),
    )
    raw_first = json.loads(paths["raw"].read_text().splitlines()[0])
    observed_first = json.loads(paths["observed"].read_text().splitlines()[0])
    assert raw_first["compound_tag_b64"] == ""
    assert observed_first["tag"] is None
    imported_dir = tmp_path / "imported"
    imported = import_dataset(
        event_sources_path=paths["event_sources"],
        observed_path=paths["observed"],
        run_manifest_path=run_dir / "manifest.json",
        offered_ledger_path=run_dir / "offered.jsonl",
        fixture_catalog_path=run_dir / "fixture-catalog.json",
        checkpoints_path=paths["checkpoints"],
        execution_plan_path=run_dir / "execution-plan.json",
        out_dir=imported_dir,
        format="jsonl",
    )
    assert imported.data_kind == "synthetic"
    assert imported.counts.events == expected_count
    assert imported.counts.by_operation == {"read": expected_count}
    assert imported.counts.by_origin == {"synthetic": expected_count}

    import_seed_manifest = tmp_path / "import-seed-manifest.json"
    atomic_write_model(
        import_seed_manifest,
        manifest.model_copy(update={"seed": manifest.seed + 1}),
    )
    with pytest.raises(ValueError, match="controls differ"):
        import_dataset(
            event_sources_path=paths["event_sources"],
            observed_path=paths["observed"],
            run_manifest_path=import_seed_manifest,
            offered_ledger_path=run_dir / "offered.jsonl",
            fixture_catalog_path=run_dir / "fixture-catalog.json",
            checkpoints_path=paths["checkpoints"],
            execution_plan_path=run_dir / "execution-plan.json",
            out_dir=tmp_path / "tampered-seed-import",
            format="jsonl",
        )

    cleaned_dir = tmp_path / "cleaned"
    cleaned = run_clean(
        imported_dir,
        CleanConfig(),
        cleaned_dir,
        format="jsonl",
        semantic_view=SEMANTIC_VIEW_ID,
    )
    assert cleaned["counts"]["kept_events"] == 0
    assert cleaned["counts"]["excluded_by_reason"] == {
        "origin_synthetic": expected_count
    }

    recovery_path = tmp_path / "recovery.json"
    recovery = CliRunner().invoke(
        main,
        [
            "recover-workload",
            "--run-dir",
            str(run_dir),
            "--imported",
            str(imported_dir),
            "--cleaned",
            str(cleaned_dir),
            "--out",
            str(recovery_path),
        ],
    )
    assert recovery.exit_code == 0, recovery.output
    assert recovery_path.is_file()

    ledger_path = run_dir / "offered.jsonl"
    ledger_bytes = ledger_path.read_bytes()
    ledger_lines = ledger_bytes.decode().splitlines()
    ledger_lines[0] += " "
    ledger_path.write_text("\n".join(ledger_lines) + "\n")
    stale_ledger = CliRunner().invoke(
        main,
        [
            "recover-workload",
            "--run-dir",
            str(run_dir),
            "--imported",
            str(imported_dir),
            "--cleaned",
            str(cleaned_dir),
            "--out",
            str(tmp_path / "stale-ledger-recovery.json"),
        ],
    )
    assert stale_ledger.exit_code != 0
    assert "does not bind" in stale_ledger.output
    ledger_path.write_bytes(ledger_bytes)

    imported_manifest = json.loads((imported_dir / "manifest.json").read_text())
    event_path = imported_dir / next(
        entry["path"]
        for entry in imported_manifest["files"]
        if entry["path"].startswith("events/")
    )
    event_path.write_bytes(event_path.read_bytes() + b"\n")
    tampered = CliRunner().invoke(
        main,
        [
            "recover-workload",
            "--run-dir",
            str(run_dir),
            "--imported",
            str(imported_dir),
            "--cleaned",
            str(cleaned_dir),
            "--out",
            str(tmp_path / "tampered-recovery.json"),
        ],
    )
    assert tampered.exit_code != 0
    assert "checksum mismatch" in tampered.output


def test_bounded_permit_runs_relation_complete_sparse_selection_promptly(
    tmp_path: Path,
):
    root = Path(__file__).parents[2]
    run_dir = tmp_path / "run"
    run_dir.mkdir(mode=0o700)
    cluster_path = run_dir / "cluster-provenance-v1.json"
    schema_path = run_dir / "event-schema-v2.json"
    budget_path = run_dir / "budget-evidence-v1.json"
    _copy_contract(root / "contracts/cluster-provenance-v1.json", cluster_path)
    _copy_contract(root / "contracts/nfs4-read-v2.schema.json", schema_path)
    _copy_contract(
        root / "tests/collection/fixtures/budget-evidence-v1.json",
        budget_path,
    )
    catalog = _catalog()
    config = WorkloadConfig()
    plan, truth = build_default_workload(
        catalog,
        config,
        semantic_view_id=SEMANTIC_VIEW_ID,
        semantic_view_sha256=SEMANTIC_VIEW_SHA256,
    )
    run_id = "workload-v3-bounded-validation"
    manifest = WorkloadCollectionManifest(
        run_id=run_id,
        run_ref=hashlib.sha256(run_id.encode()).hexdigest()[:32],
        cluster_id="offline-fixture",
        namespace="offline",
        volume_id=VOLUME_ID,
        export_id=7,
        cluster_manifest_sha256=sha256(cluster_path),
        fixture_catalog_sha256=catalog_sha256(catalog),
        event_schema_sha256=sha256(schema_path),
        budget_evidence_sha256=sha256(budget_path),
        execution_config_sha256=workload_config_sha256(config),
        execution_plan_sha256=execution_plan_sha256(plan),
        semantic_view_id=SEMANTIC_VIEW_ID,
        semantic_view_sha256=SEMANTIC_VIEW_SHA256,
        seed=config.seed,
        start_time=START,
        end_time=START + timedelta(milliseconds=config.duration_ms),
        planned_operation_count=plan.planned_operation_count,
        range_bytes=(config.range_bytes,),
        range_offsets=config.range_offsets,
        client_id="offline-workload",
        software=SoftwareVersions(
            predictive_cache_revision="7" * 40,
            chimera_image="offline",
            chimera_image_sha256="2" * 64,
            cufs_image="offline",
            cufs_image_sha256="3" * 64,
            collector_schema_version="chimera-read-event-v2",
        ),
        stop_rules=StopRules(latency_guardrail_ms=5000),
        retention=RetentionPolicy(
            owner="test-owner",
            cleanup_owner="test-owner",
            cleanup_action="delete test evidence",
        ),
    )
    binding = bind_workload_truth(plan, truth)
    permit = build_workload_collection_permit(
        manifest,
        config,
        plan,
        truth,
        binding,
        catalog,
        permit_id="11111111-1111-4111-8111-111111111111",
        profile="bounded_validation",
        expires_at=START + timedelta(hours=1),
        validated_code_head_sha256="6" * 40,
        validated_merge_commit_sha256="7" * 40,
        validated_tree_sha256="8" * 40,
        provider_package_sha256="9" * 64,
        provider_sha256="a" * 64,
        collector_sha256="b" * 64,
        validator_sha256="c" * 64,
        runbook_sha256="d" * 64,
        adapter_wrapper_sha256="e" * 64,
        cleaner_wrapper_sha256="f" * 64,
        interpreter_path="/usr/bin/python3.12",
        interpreter_sha256="0" * 64,
        execution_start_time=START,
        validation_operations=3,
    )
    atomic_write_model(run_dir / "manifest.json", manifest)
    atomic_write_model(run_dir / "fixture-catalog.json", catalog)
    for name, source in (
        ("execution-plan.json", plan),
        ("workload-config.json", config),
        ("workload-truth.json", truth),
        ("workload-truth-binding.json", binding),
    ):
        atomic_write_model(run_dir / name, source)
    count = CollectionRunner(sleep=lambda _: None).run(
        manifest,
        catalog,
        run_dir,
        DryRunExecutor(),
        execution_plan=plan,
        simulated_timing=True,
        workload_permit=permit,
        workload_permit_sha256="1" * 64,
    )
    offered = read_models(run_dir / "offered.jsonl", OfferedOperation)
    assert count == 3
    assert tuple(row.operation_index for row in offered) == (3, 43, 50)
    assert tuple(row.operation_id for row in offered) == permit.operation_ids
    assert tuple(
        int((row.offered_at_utc - START).total_seconds() * 1000)
        for row in offered
    ) == permit.execution_relative_ms
    assert permit.execution_relative_ms[-1] < 4 * 60 * 1000
    _validate_actual_workload_cadence(plan, offered)
    compressed = [
        offered[0],
        offered[1].model_copy(
            update={
                "offered_at_utc": offered[1].offered_at_utc
                + timedelta(seconds=1)
            }
        ),
        offered[2],
    ]
    with pytest.raises(ValueError, match="below the frozen minimum"):
        _validate_actual_workload_cadence(plan, compressed)
    full_permit = build_workload_collection_permit(
        manifest,
        config,
        plan,
        truth,
        binding,
        catalog,
        permit_id="22222222-2222-4222-8222-222222222222",
        profile="full_collection",
        expires_at=START + timedelta(hours=4),
        validated_code_head_sha256="6" * 40,
        validated_merge_commit_sha256="7" * 40,
        validated_tree_sha256="8" * 40,
        provider_package_sha256="9" * 64,
        provider_sha256="a" * 64,
        collector_sha256="b" * 64,
        validator_sha256="c" * 64,
        runbook_sha256="d" * 64,
        adapter_wrapper_sha256="e" * 64,
        cleaner_wrapper_sha256="f" * 64,
        interpreter_path="/usr/bin/python3.12",
        interpreter_sha256="0" * 64,
        execution_start_time=START,
        bounded_validation_attestation_sha256="1" * 64,
    )
    full_permit_path = run_dir / "full-permit.json"
    atomic_write_model(full_permit_path, full_permit)
    missing_attestation = CliRunner().invoke(
        main,
        [
            "run",
            "--run-dir",
            str(run_dir),
            "--mount-root",
            str(tmp_path / "mount"),
            "--workload-permit",
            str(full_permit_path),
        ],
    )
    assert missing_attestation.exit_code != 0
    assert "requires --bounded-attestation" in missing_attestation.output
