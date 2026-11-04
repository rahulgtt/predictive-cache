from __future__ import annotations

import hashlib
import os
from collections import Counter, defaultdict, deque
from datetime import UTC, datetime, timedelta
from pathlib import Path

import click
import pytest
from pydantic import ValidationError

from predictive_cache.collection.cli import (
    _approved_interpreter,
    _validate_full_workload_attestation,
    _validate_workload_permit_files,
    _validate_workload_provider_stack,
    _validated_workload_interpreter,
)
from predictive_cache.collection.harness import ISOLATED_PROVIDER_BOOTSTRAP
from predictive_cache.collection.io import (
    atomic_write_model,
    canonical_json,
    sha256,
)
from predictive_cache.collection.models import (
    AdapterValidationPermit,
    CleanupEvidenceReference,
    CollectionManifest,
    FixtureCatalog,
    FixtureEntry,
    LAB_FULL_RUN_WAIVER,
    RetentionPolicy,
    SoftwareVersions,
    StopRules,
    WorkloadBoundedValidationAttestation,
    WorkloadCollectionManifest,
    WorkloadProviderDeclaration,
    WorkloadProviderInvocationRecord,
    parse_collection_manifest,
)
from predictive_cache.collection.workload import (
    DEFAULT_RANGE_OFFSETS,
    OfflineSimulatedRead,
    WorkloadConfig,
    WorkloadExecutionPlan,
    aggregate_recovery,
    bind_workload_truth,
    build_default_workload,
    build_offline_simulated_source,
    build_workload_collection_permit,
    checkpoint_schedule,
    execution_plan_sha256,
    fixture_catalog_sha256,
    materialize_offered_operations,
    materialize_offline_execution_results,
    resolved_operation_count,
    run_bound_operation_id,
    select_workload_permit_operations,
    validate_plan_truth,
    validate_workload_collection_permit,
    workload_config_sha256,
    workload_truth_sha256,
)
from predictive_cache.semantics.read_demand import SEMANTIC_VIEW_SHA256

START = datetime(2026, 11, 4, tzinfo=UTC)
VOLUME_ID = "a" * 32


def catalog() -> FixtureCatalog:
    fixtures = []
    for index in range(60):
        digest = hashlib.sha256(f"file-{index:02d}".encode()).hexdigest()
        fixtures.append(
            FixtureEntry(
                fixture_id=f"fixture-{index:02d}",
                volume_id=VOLUME_ID,
                file_id=digest,
                dir_id=f"dir-{index:02d}",
                export_id=7,
                export_name="controlled",
                export_path="/controlled",
                relative_path=f"fixtures/{index:02d}.bin",
                size_bytes=65536,
                sha256=hashlib.sha256(f"content-{index:02d}".encode()).hexdigest(),
            )
        )
    return FixtureCatalog(fixtures=tuple(fixtures))


def built():
    return build_default_workload(
        catalog(),
        semantic_view_id="semantic-fixture-v1",
        semantic_view_sha256="b" * 64,
    )


def permitted(profile="bounded_validation", *, lab_unvalidated_full_run=False):
    fixture_catalog = catalog()
    config = WorkloadConfig()
    plan, truth = build_default_workload(
        fixture_catalog,
        config,
        semantic_view_id="read-demand-v1",
        semantic_view_sha256=SEMANTIC_VIEW_SHA256,
    )
    binding = bind_workload_truth(plan, truth)
    run_id = "workload-run-111"
    manifest = WorkloadCollectionManifest(
        run_id=run_id,
        run_ref=hashlib.sha256(run_id.encode()).hexdigest()[:32],
        cluster_id="cluster-fixture",
        namespace="predictive-cache",
        volume_id=VOLUME_ID,
        export_id=7,
        cluster_manifest_sha256="1" * 64,
        fixture_catalog_sha256=fixture_catalog_sha256(fixture_catalog),
        event_schema_sha256="2" * 64,
        budget_evidence_sha256="3" * 64,
        execution_config_sha256=workload_config_sha256(config),
        execution_plan_sha256=execution_plan_sha256(plan),
        semantic_view_id="read-demand-v1",
        semantic_view_sha256=SEMANTIC_VIEW_SHA256,
        seed=config.seed,
        start_time=START,
        end_time=START + timedelta(milliseconds=plan.duration_ms),
        planned_operation_count=len(plan.intents),
        range_bytes=(config.range_bytes,),
        range_offsets=config.range_offsets,
        untagged_correlation_window_s=config.untagged_correlation_window_s,
        client_id="workload-fixture",
        software=SoftwareVersions(
            predictive_cache_revision="fixture",
            chimera_image="chimera:fixture",
            chimera_image_sha256="4" * 64,
            cufs_image="cufs:fixture",
            cufs_image_sha256="5" * 64,
            collector_schema_version="nfs4_read_v2",
        ),
        stop_rules=StopRules(
            stop_on_any_drop=True,
            stop_on_epoch_mismatch=True,
            stop_on_unexplained_restart=True,
            stop_on_sink_fault=True,
            stop_on_parse_or_identity_mismatch=True,
            stop_on_checksum_mismatch=True,
            stop_on_read_error=True,
            latency_guardrail_ms=5000,
        ),
        retention=RetentionPolicy(
            owner="test-owner",
            cleanup_owner="test-cleaner",
            cleanup_action="delete fixture evidence",
        ),
    )
    permit = build_workload_collection_permit(
        manifest,
        config,
        plan,
        truth,
        binding,
        fixture_catalog,
        permit_id="11111111-1111-4111-8111-111111111111",
        profile=profile,
        expires_at=START + timedelta(days=1),
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
        execution_start_time=(
            START + timedelta(minutes=10)
            if profile == "bounded_validation"
            else START
        ),
        validation_operations=3 if profile == "bounded_validation" else None,
        bounded_validation_attestation_sha256=(
            "1" * 64
            if profile == "full_collection" and not lab_unvalidated_full_run
            else None
        ),
        lab_unvalidated_full_run=lab_unvalidated_full_run,
    )
    return manifest, config, plan, truth, binding, fixture_catalog, permit


def _expected_quotas(operation_count: int) -> Counter:
    pattern = ("high", "medium", "low", "high", "high", "medium", "high", "high", "medium", "high")
    cycles, remainder = divmod(operation_count, len(pattern))
    return Counter(pattern * cycles + pattern[:remainder])


def test_manifest_v2_frozen_fixture_still_dispatches_to_the_original_model():
    path = Path(__file__).parent / "fixtures" / "frozen-manifest.json"
    manifest = parse_collection_manifest(path.read_bytes())
    assert isinstance(manifest, CollectionManifest)
    assert manifest.manifest_version == 2
    assert manifest.fixture_distribution == "seeded_unique_nonoverlapping"


def test_v3_bounded_permit_selects_sparse_truth_coverage_with_run_bound_ids():
    manifest, config, plan, truth, binding, fixture_catalog, permit = permitted()

    validate_workload_collection_permit(
        permit,
        manifest,
        config,
        plan,
        truth,
        binding,
        fixture_catalog,
    )
    assert permit.profile == "bounded_validation"
    assert permit.operation_indexes == (3, 43, 50)
    assert permit.intent_ids == tuple(
        plan.intents[index].intent_id for index in (3, 43, 50)
    )
    assert permit.operation_ids == tuple(
        run_bound_operation_id(
            manifest.run_id,
            execution_plan_sha256(plan),
            plan.intents[index].intent_id,
        )
        for index in (3, 43, 50)
    )
    assert permit.roles == (
        "ordinary",
        "repeated_range",
        "sequential_range",
    )
    assert permit.original_relative_ms == tuple(
        plan.intents[index].relative_ms for index in (3, 43, 50)
    )
    assert permit.execution_relative_ms == (0, 170_000, 190_000)
    assert permit.semantic_view_sha256 == SEMANTIC_VIEW_SHA256
    assert permit.status == "approved"
    assert permit.expires_at.utcoffset() == timedelta(0)
    with pytest.raises(ValidationError):
        permit.profile = "full_collection"
    with pytest.raises(ValidationError, match="UTC"):
        type(permit).model_validate(
            {
                **permit.model_dump(mode="json"),
                "expires_at": datetime(2026, 11, 5),
            }
        )


def test_v3_bounded_permit_does_not_reuse_the_v2_prefix_contract():
    *_rest, permit = permitted()
    assert permit.operation_indexes == (3, 43, 50)
    assert permit.operation_indexes != (0, 1, 2)

    with pytest.raises(ValidationError, match="exact prefix"):
        AdapterValidationPermit(
            permit_id=permit.permit_id,
            run_id=permit.run_id,
            full_manifest_sha256=permit.manifest_sha256,
            validation_operations=3,
            operation_indexes=permit.operation_indexes,
            operation_ids=permit.operation_ids,
            provider_package_sha256=permit.provider_package_sha256,
            provider_sha256=permit.provider_sha256,
            collector_sha256=permit.collector_sha256,
            validator_sha256=permit.validator_sha256,
            runbook_sha256=permit.runbook_sha256,
            status="approved",
            expires_at=permit.expires_at,
        )


@pytest.mark.parametrize(
    "filename",
    [
        "manifest.json",
        "workload-config.json",
        "execution-plan.json",
        "workload-truth.json",
        "workload-truth-binding.json",
        "fixture-catalog.json",
    ],
)
def test_bounded_attestation_rejects_each_byte_mutated_artifact(
    tmp_path,
    filename,
):
    manifest, config, plan, truth, binding, catalog, permit = permitted()
    models = {
        "manifest.json": manifest,
        "workload-config.json": config,
        "execution-plan.json": plan,
        "workload-truth.json": truth,
        "workload-truth-binding.json": binding,
        "fixture-catalog.json": catalog,
    }
    for name, model in models.items():
        atomic_write_model(tmp_path / name, model)
    _validate_workload_permit_files(permit, tmp_path)
    path = tmp_path / filename
    path.write_bytes(path.read_bytes() + b" ")
    with pytest.raises(
        click.ClickException,
        match="exact artifact hash mismatch",
    ):
        _validate_workload_permit_files(permit, tmp_path)


@pytest.mark.parametrize(
    ("count", "indexes", "roles"),
    [
        (1, (3,), ("ordinary",)),
        (2, (3, 43), ("ordinary", "repeated_range")),
    ],
)
def test_v3_short_bounded_permits_are_wiring_only_relation_prefixes(
    count,
    indexes,
    roles,
):
    manifest, config, plan, truth, binding, fixture_catalog, _permit = (
        permitted()
    )
    permit = build_workload_collection_permit(
        manifest,
        config,
        plan,
        truth,
        binding,
        fixture_catalog,
        permit_id="22222222-2222-4222-8222-222222222222",
        profile="bounded_validation",
        expires_at=START + timedelta(days=1),
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
        execution_start_time=START + timedelta(minutes=10),
        validation_operations=count,
    )
    assert permit.operation_indexes == indexes
    assert permit.roles == roles


@pytest.mark.parametrize(
    "tamper",
    [
        "reorder",
        "missing",
        "extra",
        "classification",
        "hash",
        "operation_id",
        "intent_id",
        "run_id",
        "index",
        "schedule",
    ],
)
def test_v3_bounded_permit_rejects_tampered_selection_and_bindings(tamper):
    manifest, config, plan, truth, binding, fixture_catalog, permit = permitted()

    if tamper == "reorder":
        changed = permit.model_copy(
            update={
                "operation_indexes": tuple(reversed(permit.operation_indexes)),
                "intent_ids": tuple(reversed(permit.intent_ids)),
                "operation_ids": tuple(reversed(permit.operation_ids)),
                "roles": tuple(reversed(permit.roles)),
                "original_relative_ms": tuple(
                    reversed(permit.original_relative_ms)
                ),
                "execution_relative_ms": tuple(
                    reversed(permit.execution_relative_ms)
                ),
            }
        )
    elif tamper == "missing":
        changed = permit.model_copy(update={"intent_ids": permit.intent_ids[:-1]})
    elif tamper == "extra":
        changed = permit.model_copy(
            update={"operation_ids": (*permit.operation_ids, "0" * 64)}
        )
    elif tamper == "classification":
        changed = permit.model_copy(
            update={
                "roles": ("ordinary", "ordinary", "sequential_range")
            }
        )
    elif tamper == "hash":
        changed = permit.model_copy(update={"workload_truth_sha256": "0" * 64})
    elif tamper == "operation_id":
        changed = permit.model_copy(
            update={"operation_ids": ("0" * 64, *permit.operation_ids[1:])}
        )
    elif tamper == "intent_id":
        changed = permit.model_copy(
            update={"intent_ids": ("0" * 64, *permit.intent_ids[1:])}
        )
    elif tamper == "run_id":
        changed = permit.model_copy(update={"run_id": "different-run"})
    elif tamper == "schedule":
        changed = permit.model_copy(
            update={"execution_relative_ms": (0, 20_000, 40_000)}
        )
    else:
        changed = permit.model_copy(
            update={"operation_indexes": (4, *permit.operation_indexes[1:])}
        )

    with pytest.raises(ValueError, match="binding mismatch"):
        validate_workload_collection_permit(
            changed,
            manifest,
            config,
            plan,
            truth,
            binding,
            fixture_catalog,
        )


def test_v3_full_permit_selects_every_plan_operation_once_in_original_order():
    manifest, config, plan, truth, binding, fixture_catalog, permit = permitted(
        profile="full_collection"
    )

    validate_workload_collection_permit(
        permit,
        manifest,
        config,
        plan,
        truth,
        binding,
        fixture_catalog,
    )
    assert permit.operation_indexes == tuple(range(len(plan.intents)))
    assert permit.intent_ids == tuple(intent.intent_id for intent in plan.intents)
    assert len(permit.operation_ids) == len(plan.intents)
    assert len(permit.roles) == len(plan.intents)
    with pytest.raises(
        ValidationError,
        match="requires bounded PASS attestation",
    ):
        type(permit).model_validate(
            {
                **permit.model_dump(mode="json"),
                "bounded_validation_attestation_sha256": None,
            }
        )

    missing = permit.model_copy(
        update={
            "operation_indexes": permit.operation_indexes[:-1],
            "intent_ids": permit.intent_ids[:-1],
            "operation_ids": permit.operation_ids[:-1],
            "roles": permit.roles[:-1],
            "original_relative_ms": permit.original_relative_ms[:-1],
            "execution_relative_ms": permit.execution_relative_ms[:-1],
        }
    )
    with pytest.raises(ValueError, match="binding mismatch"):
        validate_workload_collection_permit(
            missing,
            manifest,
            config,
            plan,
            truth,
            binding,
            fixture_catalog,
        )


def test_v3_full_lab_waiver_is_explicit_and_mutually_exclusive():
    *_models, permit = permitted(
        profile="full_collection",
        lab_unvalidated_full_run=True,
    )
    assert permit.bounded_validation_attestation_sha256 is None
    assert permit.lab_unvalidated_full_run is True
    assert permit.lab_waiver_statement == LAB_FULL_RUN_WAIVER
    with pytest.raises(ValidationError, match="exactly one"):
        type(permit).model_validate(
            {
                **permit.model_dump(mode="json"),
                "bounded_validation_attestation_sha256": "1" * 64,
            }
        )
    with pytest.raises(ValidationError, match="exactly one"):
        type(permit).model_validate(
            {
                **permit.model_dump(mode="json"),
                "lab_unvalidated_full_run": False,
                "lab_waiver_statement": None,
            }
        )


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        ("short", "invalid"),
        ("failed", "invalid"),
        ("alternate", "canonical role closure"),
        ("stale", "stale or incompatible"),
        ("plan", "stale or incompatible"),
        ("catalog", "stale or incompatible"),
        ("provider", "stale or incompatible"),
    ],
)
def test_full_entrypoint_requires_exact_fresh_three_operation_pass(
    tmp_path,
    mutation,
    message,
):
    (
        _manifest,
        config,
        plan,
        truth,
        _binding,
        _catalog,
        permit,
    ) = permitted(profile="full_collection")
    selected = select_workload_permit_operations(
        "bounded-run",
        plan,
        truth,
        profile="bounded_validation",
        config=config,
        validation_operations=3,
    )
    raw = WorkloadBoundedValidationAttestation(
        validation_run_id="bounded-run",
        permit_sha256="2" * 64,
        manifest_sha256="3" * 64,
        manifest_file_sha256=permit.manifest_file_sha256,
        workload_config_file_sha256=permit.workload_config_file_sha256,
        execution_plan_sha256=permit.execution_plan_sha256,
        execution_plan_file_sha256=permit.execution_plan_file_sha256,
        workload_truth_sha256=permit.workload_truth_sha256,
        workload_truth_file_sha256=permit.workload_truth_file_sha256,
        truth_binding_sha256=permit.truth_binding_sha256,
        truth_binding_file_sha256=permit.truth_binding_file_sha256,
        fixture_catalog_sha256=permit.fixture_catalog_sha256,
        fixture_catalog_file_sha256=permit.fixture_catalog_file_sha256,
        semantic_view_sha256=permit.semantic_view_sha256,
        operation_indexes=selected[0],
        intent_ids=selected[1],
        operation_ids=selected[2],
        roles=selected[3],
        reconciliation_sha256="7" * 64,
        provider_manifest_sha256="8" * 64,
        imported_manifest_sha256="9" * 64,
        cleaned_manifest_sha256="a" * 64,
        workload_evidence_sha256="b" * 64,
        validated_code_head_sha256=permit.validated_code_head_sha256,
        validated_merge_commit_sha256=permit.validated_merge_commit_sha256,
        validated_tree_sha256=permit.validated_tree_sha256,
        provider_package_sha256=permit.provider_package_sha256,
        provider_sha256=permit.provider_sha256,
        collector_sha256=permit.collector_sha256,
        validator_sha256=permit.validator_sha256,
        runbook_sha256=permit.runbook_sha256,
        adapter_wrapper_sha256=permit.adapter_wrapper_sha256,
        cleaner_wrapper_sha256=permit.cleaner_wrapper_sha256,
        interpreter_path=permit.interpreter_path,
        interpreter_sha256=permit.interpreter_sha256,
        completed_at=START - timedelta(minutes=1),
        expires_at=START + timedelta(hours=1),
        status="passed",
    ).model_dump(mode="json")
    if mutation == "short":
        raw["operation_ids"] = raw["operation_ids"][:2]
    elif mutation == "failed":
        raw["status"] = "failed"
    elif mutation == "alternate":
        raw["operation_indexes"] = [0, 1, 2]
        raw["intent_ids"] = [
            plan.intents[index].intent_id for index in (0, 1, 2)
        ]
        raw["operation_ids"] = [
            run_bound_operation_id(
                "bounded-run",
                execution_plan_sha256(plan),
                plan.intents[index].intent_id,
            )
            for index in (0, 1, 2)
        ]
    elif mutation == "stale":
        raw["expires_at"] = START.isoformat()
    elif mutation == "plan":
        raw["execution_plan_sha256"] = "0" * 64
    elif mutation == "catalog":
        raw["fixture_catalog_sha256"] = "0" * 64
    else:
        raw["provider_sha256"] = "0" * 64
    path = tmp_path / f"{mutation}.json"
    path.write_text(canonical_json(raw) + "\n")
    path.chmod(0o600)
    bound = permit.model_copy(
        update={"bounded_validation_attestation_sha256": sha256(path)}
    )
    with pytest.raises(click.ClickException, match=message):
        _validate_full_workload_attestation(
            bound,
            path,
            config,
            plan,
            truth,
        )


def test_full_entrypoint_accepts_exact_fresh_three_operation_pass(tmp_path):
    (
        _manifest,
        config,
        plan,
        truth,
        _binding,
        _catalog,
        permit,
    ) = permitted(profile="full_collection")
    selected = select_workload_permit_operations(
        "bounded-run",
        plan,
        truth,
        profile="bounded_validation",
        config=config,
        validation_operations=3,
    )
    attestation = WorkloadBoundedValidationAttestation(
        validation_run_id="bounded-run",
        permit_sha256="2" * 64,
        manifest_sha256="3" * 64,
        manifest_file_sha256=permit.manifest_file_sha256,
        workload_config_file_sha256=permit.workload_config_file_sha256,
        execution_plan_sha256=permit.execution_plan_sha256,
        execution_plan_file_sha256=permit.execution_plan_file_sha256,
        workload_truth_sha256=permit.workload_truth_sha256,
        workload_truth_file_sha256=permit.workload_truth_file_sha256,
        truth_binding_sha256=permit.truth_binding_sha256,
        truth_binding_file_sha256=permit.truth_binding_file_sha256,
        fixture_catalog_sha256=permit.fixture_catalog_sha256,
        fixture_catalog_file_sha256=permit.fixture_catalog_file_sha256,
        semantic_view_sha256=permit.semantic_view_sha256,
        operation_indexes=selected[0],
        intent_ids=selected[1],
        operation_ids=selected[2],
        roles=selected[3],
        reconciliation_sha256="7" * 64,
        provider_manifest_sha256="8" * 64,
        imported_manifest_sha256="9" * 64,
        cleaned_manifest_sha256="a" * 64,
        workload_evidence_sha256="b" * 64,
        validated_code_head_sha256=permit.validated_code_head_sha256,
        validated_merge_commit_sha256=permit.validated_merge_commit_sha256,
        validated_tree_sha256=permit.validated_tree_sha256,
        provider_package_sha256=permit.provider_package_sha256,
        provider_sha256=permit.provider_sha256,
        collector_sha256=permit.collector_sha256,
        validator_sha256=permit.validator_sha256,
        runbook_sha256=permit.runbook_sha256,
        adapter_wrapper_sha256=permit.adapter_wrapper_sha256,
        cleaner_wrapper_sha256=permit.cleaner_wrapper_sha256,
        interpreter_path=permit.interpreter_path,
        interpreter_sha256=permit.interpreter_sha256,
        completed_at=START - timedelta(minutes=1),
        expires_at=START + timedelta(hours=1),
        status="passed",
    )
    path = tmp_path / "attestation.json"
    atomic_write_model(path, attestation)
    bound = permit.model_copy(
        update={"bounded_validation_attestation_sha256": sha256(path)}
    )
    assert (
        _validate_full_workload_attestation(
            bound,
            path,
            config,
            plan,
            truth,
        )
        == attestation
    )


def _provider_stack_fixture(tmp_path):
    *_models, original = permitted()
    run_dir = tmp_path / "run"
    provider_root = tmp_path / "provider"
    run_dir.mkdir(mode=0o700)
    provider_root.mkdir(mode=0o700)
    paths = {
        "provider_package_sha256": run_dir
        / ".approved-tools/provider-package.zip",
        "provider_sha256": provider_root / "provider.py",
        "collector_sha256": provider_root / "collector.py",
        "validator_sha256": provider_root / "validator.py",
        "runbook_sha256": provider_root / "runbook.md",
        "adapter_wrapper_sha256": run_dir
        / ".approved-tools/adapter-wrapper",
        "cleaner_wrapper_sha256": run_dir
        / ".approved-tools/cleaner-wrapper",
    }
    for index, path in enumerate(paths.values()):
        path.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
        path.write_text(f"artifact-{index}\n")
        path.chmod(0o600)
    hashes = {field: sha256(path) for field, path in paths.items()}
    interpreter = tmp_path / "python3.12"
    interpreter.write_text("test interpreter\n")
    interpreter.chmod(0o500)
    permit = original.model_copy(
        update={
            **hashes,
            "interpreter_path": str(interpreter),
            "interpreter_sha256": sha256(interpreter),
        }
    )
    permit_path = run_dir / "workload-permit.json"
    atomic_write_model(permit_path, permit)
    permit_sha = sha256(permit_path)
    invocation = WorkloadProviderInvocationRecord(
        run_id=permit.run_id,
        manifest_sha256=permit.manifest_file_sha256,
        permit_sha256=permit_sha,
        execution_plan_sha256=permit.execution_plan_sha256,
        provider_package_sha256=permit.provider_package_sha256,
        provider_package_path=".approved-tools/provider-package.zip",
        provider_snapshot_path=".approved-tools/checkpoint-provider",
        provider_snapshot_sha256=permit.provider_sha256,
        collector_sha256=permit.collector_sha256,
        validator_sha256=permit.validator_sha256,
        runbook_sha256=permit.runbook_sha256,
        adapter_wrapper_sha256=permit.adapter_wrapper_sha256,
        cleaner_wrapper_sha256=permit.cleaner_wrapper_sha256,
        interpreter_path=permit.interpreter_path,
        interpreter_sha256=permit.interpreter_sha256,
        validated_code_head_sha256=permit.validated_code_head_sha256,
        validated_merge_commit_sha256=permit.validated_merge_commit_sha256,
        validated_tree_sha256=permit.validated_tree_sha256,
        argv_contract=(
            permit.interpreter_path,
            "-I",
            "-S",
            "-c",
            ISOLATED_PROVIDER_BOOTSTRAP,
            ".approved-tools/provider-package.zip",
            ".approved-tools/checkpoint-provider",
            "<action>",
            "<run_dir>",
            "--permit-path",
            "<permit_path>",
        ),
        actions=("checkpoint", "probe", "post-read"),
        created_at=START,
    )
    invocation_path = run_dir / "provider-invocation.json"
    atomic_write_model(invocation_path, invocation)

    def reference(root_id, path):
        return CleanupEvidenceReference(
            root_id=root_id,
            path=path.relative_to(
                run_dir if root_id == "bounded_harness_evidence"
                else provider_root
            ).as_posix(),
            sha256=sha256(path),
        )

    provider = WorkloadProviderDeclaration.model_construct(
        provider_package_sha256=permit.provider_package_sha256,
        run_id=permit.run_id,
        manifest_sha256=permit.manifest_file_sha256,
        provider_sha256=permit.provider_sha256,
        provider_snapshot_sha256=permit.provider_sha256,
        collector_sha256=permit.collector_sha256,
        validator_sha256=permit.validator_sha256,
        runbook_sha256=permit.runbook_sha256,
        adapter_wrapper_sha256=permit.adapter_wrapper_sha256,
        cleaner_wrapper_sha256=permit.cleaner_wrapper_sha256,
        interpreter_path=permit.interpreter_path,
        interpreter_sha256=permit.interpreter_sha256,
        validated_code_head_sha256=permit.validated_code_head_sha256,
        validated_merge_commit_sha256=(
            permit.validated_merge_commit_sha256
        ),
        validated_tree_sha256=permit.validated_tree_sha256,
        argv_contract=invocation.argv_contract,
        permit_sha256=permit_sha,
        permit=reference("bounded_harness_evidence", permit_path),
        provider_package_artifact=reference(
            "bounded_harness_evidence",
            paths["provider_package_sha256"],
        ),
        provider_artifact=reference(
            "bounded_provider_evidence",
            paths["provider_sha256"],
        ),
        collector_artifact=reference(
            "bounded_provider_evidence",
            paths["collector_sha256"],
        ),
        validator_artifact=reference(
            "bounded_provider_evidence",
            paths["validator_sha256"],
        ),
        runbook_artifact=reference(
            "bounded_provider_evidence",
            paths["runbook_sha256"],
        ),
        adapter_wrapper_artifact=reference(
            "bounded_harness_evidence",
            paths["adapter_wrapper_sha256"],
        ),
        cleaner_wrapper_artifact=reference(
            "bounded_harness_evidence",
            paths["cleaner_wrapper_sha256"],
        ),
        invocation_record=reference(
            "bounded_harness_evidence",
            invocation_path,
        ),
    )
    return permit, permit_sha, provider, run_dir, provider_root


def test_provider_stack_matches_exact_permit_and_invocation(tmp_path):
    permit, permit_sha, provider, run_dir, provider_root = (
        _provider_stack_fixture(tmp_path)
    )
    _validate_workload_provider_stack(
        provider,
        permit,
        permit_sha,
        run_dir,
        provider_root,
        expected_run_id=permit.run_id,
        expected_manifest_sha256=permit.manifest_file_sha256,
        harness_root_id="bounded_harness_evidence",
        provider_root_id="bounded_provider_evidence",
    )


@pytest.mark.parametrize(
    "field",
    [
        "provider_package_sha256",
        "provider_sha256",
        "collector_sha256",
        "validator_sha256",
        "runbook_sha256",
        "adapter_wrapper_sha256",
        "cleaner_wrapper_sha256",
        "interpreter_sha256",
    ],
)
def test_provider_stack_rejects_each_laundered_hash(tmp_path, field):
    permit, permit_sha, provider, run_dir, provider_root = (
        _provider_stack_fixture(tmp_path)
    )
    changed = provider.model_copy(update={field: "0" * 64})
    with pytest.raises(click.ClickException, match="differs from the permit"):
        _validate_workload_provider_stack(
            changed,
            permit,
            permit_sha,
            run_dir,
            provider_root,
            expected_run_id=permit.run_id,
            expected_manifest_sha256=permit.manifest_file_sha256,
            harness_root_id="bounded_harness_evidence",
            provider_root_id="bounded_provider_evidence",
        )


def test_provider_stack_rejects_invocation_argv_mismatch(tmp_path):
    permit, permit_sha, provider, run_dir, provider_root = (
        _provider_stack_fixture(tmp_path)
    )
    changed = provider.model_copy(update={"argv_contract": ("wrong",)})
    with pytest.raises(click.ClickException, match="differs from the permit"):
        _validate_workload_provider_stack(
            changed,
            permit,
            permit_sha,
            run_dir,
            provider_root,
            expected_run_id=permit.run_id,
            expected_manifest_sha256=permit.manifest_file_sha256,
            harness_root_id="bounded_harness_evidence",
            provider_root_id="bounded_provider_evidence",
        )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("run_id", "foreign-run"),
        ("manifest_sha256", "0" * 64),
    ],
)
def test_provider_stack_rejects_foreign_run_or_manifest(
    tmp_path,
    field,
    value,
):
    permit, permit_sha, provider, run_dir, provider_root = (
        _provider_stack_fixture(tmp_path)
    )
    changed = provider.model_copy(update={field: value})
    with pytest.raises(click.ClickException, match="differs from the permit"):
        _validate_workload_provider_stack(
            changed,
            permit,
            permit_sha,
            run_dir,
            provider_root,
            expected_run_id=permit.run_id,
            expected_manifest_sha256=permit.manifest_file_sha256,
            harness_root_id="bounded_harness_evidence",
            provider_root_id="bounded_provider_evidence",
        )


def test_provider_stack_allows_consistent_same_byte_interpreter_path(tmp_path):
    permit, _permit_sha, provider, run_dir, provider_root = (
        _provider_stack_fixture(tmp_path)
    )
    alternate = tmp_path / "alternate-python"
    alternate.write_bytes(Path(permit.interpreter_path).read_bytes())
    alternate.chmod(0o500)
    changed_permit = permit.model_copy(
        update={
            "interpreter_path": str(alternate),
            "interpreter_sha256": permit.interpreter_sha256,
        }
    )
    permit_path = run_dir / provider.permit.path
    permit_path.unlink()
    atomic_write_model(permit_path, changed_permit)
    permit_sha = sha256(permit_path)
    argv = (
        str(alternate),
        *provider.argv_contract[1:],
    )
    invocation_path = run_dir / provider.invocation_record.path
    invocation = WorkloadProviderInvocationRecord.model_validate_json(
        invocation_path.read_text()
    ).model_copy(
        update={
            "permit_sha256": permit_sha,
            "interpreter_path": str(alternate),
            "argv_contract": argv,
        }
    )
    invocation_path.unlink()
    atomic_write_model(invocation_path, invocation)
    changed_provider = provider.model_copy(
        update={
            "permit_sha256": permit_sha,
            "permit": provider.permit.model_copy(
                update={"sha256": permit_sha}
            ),
            "interpreter_path": str(alternate),
            "argv_contract": argv,
            "invocation_record": provider.invocation_record.model_copy(
                update={"sha256": sha256(invocation_path)}
            ),
        }
    )
    _validate_workload_provider_stack(
        changed_provider,
        changed_permit,
        permit_sha,
        run_dir,
        provider_root,
        expected_run_id=permit.run_id,
        expected_manifest_sha256=permit.manifest_file_sha256,
        harness_root_id="bounded_harness_evidence",
        provider_root_id="bounded_provider_evidence",
    )


def test_v3_interpreter_accepts_current_uid_while_v2_requires_root(
    tmp_path,
    monkeypatch,
):
    *_models, permit = permitted()
    interpreter = tmp_path / "python3.12"
    interpreter.write_text("test interpreter\n")
    interpreter.chmod(0o500)
    original_stat = Path.stat
    owner_uid = {"value": 1002}

    def stat_as_uid(path, *args, **kwargs):
        result = original_stat(path, *args, **kwargs)
        if path == interpreter:
            values = list(result)
            values[4] = owner_uid["value"]
            return os.stat_result(values)
        return result

    monkeypatch.setattr(Path, "stat", stat_as_uid)
    monkeypatch.setattr(
        "predictive_cache.collection.cli.os.geteuid",
        lambda: 1002,
    )
    changed = permit.model_copy(
        update={
            "interpreter_path": str(interpreter),
            "interpreter_sha256": sha256(interpreter),
        }
    )
    assert _validated_workload_interpreter(changed) == interpreter
    owner_uid["value"] = 1003
    with pytest.raises(
        click.ClickException,
        match="unsafe ownership or mode",
    ):
        _validated_workload_interpreter(changed)
    owner_uid["value"] = 1002

    monkeypatch.setattr(
        "predictive_cache.collection.cli.APPROVED_PROVIDER_INTERPRETER_PATH",
        str(interpreter),
    )
    monkeypatch.setattr(
        "predictive_cache.collection.cli.APPROVED_PROVIDER_INTERPRETER_SHA256",
        sha256(interpreter),
    )
    with pytest.raises(
        click.ClickException,
        match="unsafe ownership or mode",
    ):
        _approved_interpreter()


@pytest.mark.parametrize(
    "field",
    [
        "provider_package_sha256",
        "provider_snapshot_sha256",
        "collector_sha256",
        "validator_sha256",
        "runbook_sha256",
        "adapter_wrapper_sha256",
        "cleaner_wrapper_sha256",
        "interpreter_sha256",
    ],
)
def test_provider_stack_rejects_each_invocation_hash_mismatch(
    tmp_path,
    field,
):
    permit, permit_sha, provider, run_dir, provider_root = (
        _provider_stack_fixture(tmp_path)
    )
    path = run_dir / provider.invocation_record.path
    invocation = WorkloadProviderInvocationRecord.model_validate_json(
        path.read_text()
    ).model_copy(update={field: "0" * 64})
    path.unlink()
    atomic_write_model(path, invocation)
    changed = provider.model_copy(
        update={
            "invocation_record": provider.invocation_record.model_copy(
                update={"sha256": sha256(path)}
            )
        }
    )
    with pytest.raises(click.ClickException, match="differs from the permit"):
        _validate_workload_provider_stack(
            changed,
            permit,
            permit_sha,
            run_dir,
            provider_root,
            expected_run_id=permit.run_id,
            expected_manifest_sha256=permit.manifest_file_sha256,
            harness_root_id="bounded_harness_evidence",
            provider_root_id="bounded_provider_evidence",
        )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("run_id", "foreign-run"),
        ("manifest_sha256", "0" * 64),
        ("interpreter_path", "/tmp/unapproved-python"),
    ],
)
def test_provider_stack_rejects_repackaged_invocation_identity(
    tmp_path,
    field,
    value,
):
    permit, permit_sha, provider, run_dir, provider_root = (
        _provider_stack_fixture(tmp_path)
    )
    path = run_dir / provider.invocation_record.path
    invocation = WorkloadProviderInvocationRecord.model_validate_json(
        path.read_text()
    ).model_copy(update={field: value})
    path.unlink()
    atomic_write_model(path, invocation)
    changed = provider.model_copy(
        update={
            "invocation_record": provider.invocation_record.model_copy(
                update={"sha256": sha256(path)}
            )
        }
    )
    with pytest.raises(click.ClickException, match="differs from the permit"):
        _validate_workload_provider_stack(
            changed,
            permit,
            permit_sha,
            run_dir,
            provider_root,
            expected_run_id=permit.run_id,
            expected_manifest_sha256=permit.manifest_file_sha256,
            harness_root_id="bounded_harness_evidence",
            provider_root_id="bounded_provider_evidence",
        )


def test_default_workload_is_deterministic_strict_frozen_and_truth_free():
    plan, truth = built()
    same_plan, same_truth = built()

    assert plan == same_plan
    assert truth == same_truth
    assert execution_plan_sha256(plan) == execution_plan_sha256(same_plan)
    assert workload_truth_sha256(truth) == workload_truth_sha256(same_truth)
    assert plan.planned_operation_count == resolved_operation_count(WorkloadConfig())
    assert plan.duration_ms == 3 * 60 * 60 * 1000
    assert plan.range_bytes == 4096
    assert plan.range_offsets == DEFAULT_RANGE_OFFSETS
    assert plan.semantic_view_id == "semantic-fixture-v1"
    assert plan.semantic_view_sha256 == "b" * 64

    plan_keys = set(plan.model_dump(mode="json"))
    assert not {
        "truth",
        "actor_id",
        "session_id",
        "project_id",
        "pattern_id",
        "popularity_tier",
        "coaccess_group_id",
    } & plan_keys
    with pytest.raises(ValidationError):
        WorkloadConfig(seed="42")
    with pytest.raises(ValidationError):
        plan.duration_ms = 1


def test_duration_and_operation_count_are_configurable_with_safe_exact_quotas():
    config = WorkloadConfig(
        duration_ms=6 * 60 * 60 * 1000,
        operation_count=200,
    )
    plan, truth = build_default_workload(
        catalog(),
        config,
        semantic_view_id="semantic-fixture-v1",
        semantic_view_sha256="b" * 64,
    )
    assert plan.duration_ms == 6 * 60 * 60 * 1000
    assert plan.planned_operation_count == 200
    assert Counter(item.popularity_tier for item in truth.intent_truth) == _expected_quotas(200)


def test_seed_changes_fixture_assignment_not_only_hashes():
    plan_42, truth_42 = built()
    plan_43, truth_43 = build_default_workload(
        catalog(),
        WorkloadConfig(seed=43),
        semantic_view_id="semantic-fixture-v1",
        semantic_view_sha256="b" * 64,
    )
    semantic_42 = [
        (item.relative_ms, item.fixture_id, item.offset) for item in plan_42.intents
    ]
    semantic_43 = [
        (item.relative_ms, item.fixture_id, item.offset) for item in plan_43.intents
    ]
    assert semantic_42 != semantic_43
    assert truth_42.fixture_popularity != truth_43.fixture_popularity


def test_exact_popularity_membership_and_operation_quotas():
    plan, truth = built()
    membership = Counter(item.tier for item in truth.fixture_popularity)
    quotas = Counter(item.popularity_tier for item in truth.intent_truth)

    assert membership == {"high": 6, "medium": 18, "low": 36}
    assert quotas == _expected_quotas(plan.planned_operation_count)
    assert len({item.fixture_id for item in truth.fixture_popularity}) == 60
    assert {intent.intent_id for intent in plan.intents} == {
        item.intent_id for item in truth.intent_truth
    }
    assert len({item.synthetic_actor_id for item in truth.intent_truth}) == (
        WorkloadConfig().synthetic_actor_count
    )


def test_truth_cross_validation_rejects_plan_mismatch():
    plan, truth = built()
    changed = truth.intent_truth[0].model_copy(
        update={"fixture_id": truth.intent_truth[1].fixture_id}
    )
    tampered = truth.model_copy(
        update={"intent_truth": (changed, *truth.intent_truth[1:])}
    )
    with pytest.raises(ValueError, match="execution-plan intent"):
        validate_plan_truth(plan, tampered)


def test_unique_per_occurrence_intents_and_sequential_offsets():
    plan, truth = built()
    oracle = {item.intent_id: item for item in truth.intent_truth}
    intent_ids = [intent.intent_id for intent in plan.intents]

    assert len(intent_ids) == len(set(intent_ids)) == plan.planned_operation_count
    by_fixture = defaultdict(list)
    for intent in plan.intents:
        by_fixture[intent.fixture_id].append(intent)
    for fixture_intents in by_fixture.values():
        for occurrence, intent in enumerate(fixture_intents):
            assert intent.offset == DEFAULT_RANGE_OFFSETS[occurrence % 5]
            assert oracle[intent.intent_id].fixture_occurrence_index == occurrence
            assert oracle[intent.intent_id].sequential_offset_index == occurrence % 5
    assert all(intent.operation == "READ" for intent in plan.intents)


def test_repeat_spacing_caps_bounds_and_alignment():
    plan, _truth = built()
    times = [intent.relative_ms for intent in plan.intents]

    assert min(right - left for left, right in zip(times, times[1:])) == 20_000
    assert all(intent.offset % 4096 == 0 for intent in plan.intents)
    assert all(intent.count == 4096 for intent in plan.intents)
    assert (
        times[-1]
        + plan.operation_schedule_tolerance_s * 1000
        + plan.simulated_durability_margin_ms
        < plan.duration_ms
    )

    for window_ms, cap in ((60_000, 3), (900_000, 25)):
        active = deque()
        for current in times:
            while active and active[0] <= current - window_ms:
                active.popleft()
            active.append(current)
            assert len(active) <= cap

    previous = {}
    minimum_repeat_gap = (
        plan.untagged_correlation_window_s + plan.operation_schedule_tolerance_s
    ) * 1000
    for intent in plan.intents:
        key = (intent.fixture_id, intent.offset, intent.count)
        if key in previous:
            assert intent.relative_ms - previous[key] > minimum_repeat_gap
        previous[key] = intent.relative_ms


def test_coaccess_truth_and_non_hour_burst_quiet_windows():
    plan, truth = built()
    intents = {intent.intent_id: intent for intent in plan.intents}
    groups = {group.group_id: set(group.fixture_ids) for group in truth.coaccess_groups}

    assert len(groups) == 4
    planted = defaultdict(list)
    for item in truth.intent_truth:
        if item.coaccess_group_id is not None:
            planted[(item.coaccess_group_id, item.synthetic_session_id)].append(item)
            assert item.fixture_id in groups[item.coaccess_group_id]
    assert len(planted) >= plan.planned_operation_count // 10
    assert all(
        sorted(item.coaccess_step for item in rows) == [0, 1, 2]
        for rows in planted.values()
    )
    assert all(
        [
            intents[item.intent_id].relative_ms
            for item in sorted(rows, key=lambda value: value.coaccess_step)
        ]
        == sorted(intents[item.intent_id].relative_ms for item in rows)
        for rows in planted.values()
    )

    burst_windows = [window for window in truth.windows if window.kind == "burst"]
    quiet_windows = [window for window in truth.windows if window.kind == "quiet"]
    assert len(burst_windows) == len(quiet_windows) == 4
    for window in truth.windows:
        assert window.start_relative_ms % 3_600_000 != 0
    for window in burst_windows:
        assert (
            sum(
                window.start_relative_ms <= intent.relative_ms < window.end_relative_ms
                for intent in plan.intents
            )
            == 3
        )
    for window in quiet_windows:
        assert not any(
            window.start_relative_ms <= intent.relative_ms < window.end_relative_ms
            for intent in plan.intents
        )


def test_run_bound_operation_ids_use_only_canonical_binding():
    plan, _truth = built()
    plan_hash = execution_plan_sha256(plan)
    intent_id = plan.intents[0].intent_id
    expected = hashlib.sha256(
        canonical_json(
            {
                "run_id": "offline-run-1",
                "execution_plan_sha256": plan_hash,
                "intent_id": intent_id,
            }
        ).encode()
    ).hexdigest()

    assert run_bound_operation_id("offline-run-1", plan_hash, intent_id) == expected
    assert run_bound_operation_id("offline-run-2", plan_hash, intent_id) != expected
    assert run_bound_operation_id("offline-run-1", "c" * 64, intent_id) != expected


def test_checkpoint_schedule_includes_start_intervals_and_exact_end_once():
    assert checkpoint_schedule(5 * 60 * 60 * 1000, 900) == tuple(
        index * 900_000 for index in range(21)
    )
    assert checkpoint_schedule(1_000_001, 300) == (0, 300_000, 600_000, 900_000, 1_000_001)
    assert checkpoint_schedule(900_000, 900) == (0, 900_000)


def test_truth_changes_do_not_change_execution_plan_hash():
    plan, truth = built()
    original_plan_hash = execution_plan_sha256(plan)
    changed_row = truth.intent_truth[0].model_copy(
        update={"synthetic_actor_id": "actor-alternative"}
    )
    changed_truth = truth.model_copy(
        update={"intent_truth": (changed_row, *truth.intent_truth[1:])}
    )

    assert workload_truth_sha256(changed_truth) != workload_truth_sha256(truth)
    assert execution_plan_sha256(plan) == original_plan_hash
    assert "synthetic_actor_id" not in canonical_json(plan.model_dump(mode="json"))


def test_offline_simulated_source_has_bounded_delays_and_no_lifecycle_facts():
    plan, truth = built()
    fixture_catalog = catalog()
    offered = materialize_offered_operations(
        "offline-run-1",
        START,
        plan,
        fixture_catalog,
    )
    execution = materialize_offline_execution_results(plan, offered)
    source = build_offline_simulated_source(plan, offered, execution)

    assert source.manifest.source_kind == "offline_simulated_fixture"
    assert source.manifest.live_or_provider_evidence is False
    assert len(source.raw_reads) == len(source.observed_reads) == len(plan.intents)
    assert len(source.checkpoints) == 13
    assert source.checkpoints[-1].scheduled_relative_ms == plan.duration_ms
    for index, row in enumerate(source.observed_reads):
        spacing = (
            plan.intents[index + 1].relative_ms - plan.intents[index].relative_ms
            if index + 1 < len(plan.intents)
            else plan.duration_ms - plan.intents[index].relative_ms
        )
        assert 0 <= row.durability_delay_ms < min(1000, spacing)
        assert row.operation == "READ"
        assert row.origin == "synthetic"

    forbidden = {"open", "close", "handle", "session", "session_id"}
    execution_documents = [
        plan.model_dump(mode="json"),
        source.manifest.model_dump(mode="json"),
        *(row.model_dump(mode="json") for row in source.raw_reads),
        *(row.model_dump(mode="json") for row in source.observed_reads),
    ]
    assert all(not forbidden & _recursive_keys(document) for document in execution_documents)
    assert all(item.synthetic_session_id for item in truth.intent_truth)


def test_aggregate_recovery_reports_only_counts_quotas_transitions_and_lineage():
    plan, truth = built()
    offered = materialize_offered_operations("offline-run-1", START, plan, catalog())
    execution = materialize_offline_execution_results(plan, offered)
    source = build_offline_simulated_source(plan, offered, execution)
    report = aggregate_recovery(plan, truth, source.observed_reads)

    assert report.counts.model_dump() == {
        "planned": len(plan.intents),
        "imported": len(plan.intents),
        "matched_unique": len(plan.intents),
        "missing": 0,
        "unexpected": 0,
        "duplicate": 0,
    }
    assert {
        quota.tier: (quota.planned, quota.realized) for quota in report.tier_quotas
    } == {
        tier: (count, count)
        for tier, count in _expected_quotas(plan.planned_operation_count).items()
    }
    assert sum(item.planned for item in report.transitions) > 0
    assert all(item.realized == item.planned for item in report.transitions)
    assert set(report.model_dump(mode="json")) == {
        "recovery_report_version",
        "report_kind",
        "counts",
        "tier_quotas",
        "transitions",
        "lineage",
    }

    incomplete = aggregate_recovery(plan, truth, source.observed_reads[:-1])
    assert incomplete.counts.missing == 1
    assert incomplete.counts.matched_unique == len(plan.intents) - 1


def test_offline_read_model_rejects_non_read_and_lifecycle_fields():
    plan, _truth = built()
    offered = materialize_offered_operations("offline-run-1", START, plan, catalog())
    execution = materialize_offline_execution_results(plan, offered)
    source = build_offline_simulated_source(plan, offered, execution)
    row = source.observed_reads[0].model_dump(mode="json")

    with pytest.raises(ValidationError):
        OfflineSimulatedRead.model_validate({**row, "operation": "OPEN"})
    with pytest.raises(ValidationError):
        OfflineSimulatedRead.model_validate({**row, "session_id": "invented"})
    with pytest.raises(ValidationError):
        WorkloadExecutionPlan.model_validate(
            {
                **plan.model_dump(mode="json"),
                "intents": [
                    {
                        **plan.intents[0].model_dump(mode="json"),
                        "operation": "CLOSE",
                    },
                    *(item.model_dump(mode="json") for item in plan.intents[1:]),
                ],
            }
        )


def _recursive_keys(value) -> set[str]:
    if isinstance(value, dict):
        return set(value) | {
            key
            for child in value.values()
            for key in _recursive_keys(child)
        }
    if isinstance(value, list):
        return {key for child in value for key in _recursive_keys(child)}
    return set()
