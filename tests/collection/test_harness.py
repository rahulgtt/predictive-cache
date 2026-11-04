from __future__ import annotations

import base64
import hashlib
import json
import os
import shutil
import stat
import subprocess
import uuid
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import click
import pytest
import yaml
from click.testing import CliRunner
from pydantic import ValidationError

from predictive_cache.collection.cli import (
    APPROVED_VALIDATED_CODE_HEAD_SHA256,
    APPROVED_VALIDATED_MERGE_COMMIT_SHA256,
    APPROVED_VALIDATED_TREE_SHA256,
    _approved_executable,
    _artifact_rows,
    _cleanup_root,
    _external_file_evidence,
    _historical_root,
    _manifest_profile,
    _open_verified_fd,
    _validate_clean_artifact_contract,
    _validate_clean_daily_summary,
    _validate_dataset_artifacts,
    _validate_exact_raw_identities,
    _validate_full_profile,
    _validate_live_row_chain,
    _validate_partitioned_rows,
    _validate_provider_provenance,
    main,
)
from predictive_cache.collection.harness import (
    ISOLATED_PROVIDER_BOOTSTRAP,
    CollectionRunner,
    CommandCheckpointProvider,
    DirectMountExecutor,
    DryRunExecutor,
    build_operation,
    catalog_sha256,
    checkpoint_stop_reasons,
    correlate_untagged_events,
    planned_operations,
    reconcile,
)
from predictive_cache.collection.io import (
    append_model,
    atomic_write_model,
    read_models,
    sha256,
    write_jsonl,
)
from predictive_cache.collection.models import (
    AdapterValidationPermit,
    BoundedCleanReport,
    BoundedValidationAttestation,
    Checkpoint,
    CleanupCompletionRecord,
    CleanupEvidenceReference,
    CleanupScheduleRecord,
    CollectionManifest,
    CollectorDurableEvent,
    CollectorFileManifest,
    CollectorSourceFile,
    DatasetArtifact,
    DropCounters,
    ExecutionResult,
    FixtureCatalog,
    FixtureEntry,
    HistoricalCleanupCompletionRecord,
    HistoricalCompatibilityApproval,
    HistoricalDeletionApproval,
    HistoricalDeletionTarget,
    HistoricalEvidenceReference,
    HistoricalExecutionProvenance,
    HistoricalFinalizationAttestation,
    HistoricalFrozenRootDeclaration,
    HistoricalIntegrationProvenance,
    HistoricalProviderEvidence,
    HistoricalRunEvidence,
    HistoricalValidationAttempt,
    HistoricalVerificationProvenance,
    JsonlEvidence,
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
    RetentionPolicy,
    SoftwareVersions,
    StopRules,
)
from predictive_cache.datagen.io import config_hash
from predictive_cache.prep.clean.pipeline import run_clean
from predictive_cache.prep.clean.report import render_summary
from predictive_cache.prep.config import CleanConfig
from predictive_cache.sources.chimera.importer import import_dataset

START = datetime(2026, 11, 4, tzinfo=UTC)
FIXTURE_DIR = Path(__file__).parent / "fixtures"
CONTRACT_DIR = Path(__file__).parents[2] / "contracts"
CLUSTER_PROVENANCE_HASH = sha256(CONTRACT_DIR / "cluster-provenance-v1.json")
EVENT_SCHEMA_HASH = sha256(CONTRACT_DIR / "nfs4-read-v2.schema.json")
BUDGET_EVIDENCE_HASH = sha256(FIXTURE_DIR / "budget-evidence-v1.json")
VOLUME_ID = "a" * 32
FILE_A = "b" * 64
FILE_B = "c" * 64
EPOCH = "11111111-1111-4111-8111-111111111111"
OTHER_EPOCH = "22222222-2222-4222-8222-222222222222"
POD_UID = "33333333-3333-4333-8333-333333333333"


def catalog() -> FixtureCatalog:
    return FixtureCatalog(
        fixtures=(
            FixtureEntry(
                fixture_id="fixture-a",
                volume_id=VOLUME_ID,
                file_id=FILE_A,
                dir_id="dir-a",
                export_id=7,
                export_name="controlled",
                export_path="/controlled",
                relative_path="fixtures/a.bin",
                size_bytes=8192,
                sha256="a" * 64,
                weight=2,
            ),
            FixtureEntry(
                fixture_id="fixture-b",
                volume_id=VOLUME_ID,
                file_id=FILE_B,
                dir_id="dir-b",
                export_id=7,
                export_name="controlled",
                export_path="/controlled",
                relative_path="fixtures/b.bin",
                size_bytes=8192,
                sha256="b" * 64,
            ),
        )
    )


def manifest(**updates) -> CollectionManifest:
    fixture_catalog = catalog()
    values = {
        "run_id": "controlled-001",
        "run_ref": hashlib.sha256(b"controlled-001").hexdigest()[:32],
        "cluster_id": "offline-fixture",
        "namespace": "offline",
        "volume_id": VOLUME_ID,
        "export_id": 7,
        "cluster_manifest_sha256": CLUSTER_PROVENANCE_HASH,
        "fixture_catalog_sha256": catalog_sha256(fixture_catalog),
        "event_schema_sha256": EVENT_SCHEMA_HASH,
        "budget_evidence_sha256": BUDGET_EVIDENCE_HASH,
        "seed": 42,
        "start_time": START,
        "end_time": START + timedelta(hours=5),
        "offered_rate_per_s": 1.0,
        "offered_count": 4,
        "range_bytes": (4096,),
        "range_offsets": (0, 4096),
        "client_id": "controlled-client",
        "tag_mode": "pc1:d",
        "allowed_exclusion_reasons": ("collector_shutdown",),
        "software": SoftwareVersions(
            predictive_cache_revision="test",
            chimera_image="offline",
            chimera_image_sha256="2" * 64,
            cufs_image="offline",
            cufs_image_sha256="3" * 64,
            collector_schema_version="read-correlation-v1-proposed",
        ),
        "stop_rules": StopRules(latency_guardrail_ms=250),
        "retention": RetentionPolicy(
            owner="experiment-owner",
            cleanup_owner="experiment-owner",
            cleanup_action="securely delete private evidence",
        ),
    }
    values.update(updates)
    return CollectionManifest(**values)


def private_dir(tmp_path: Path) -> Path:
    path = tmp_path / "run"
    path.mkdir(mode=0o700)
    sources = {
        "cluster-provenance-v1.json": CONTRACT_DIR / "cluster-provenance-v1.json",
        "event-schema-v2.json": CONTRACT_DIR / "nfs4-read-v2.schema.json",
        "budget-evidence-v1.json": FIXTURE_DIR / "budget-evidence-v1.json",
    }
    for name, source in sources.items():
        target = path / name
        target.write_bytes(source.read_bytes())
        target.chmod(0o600)
    return path


def checkpoint(
    root: Path,
    *,
    captured_at: datetime = START,
    scheduled_at: datetime | None = None,
    epoch: str = EPOCH,
    restart_count: int = 0,
    restart_explained: bool = False,
    drops: DropCounters | None = None,
    max_latency: float = 1,
) -> Checkpoint:
    active = root / "collector-active.jsonl"
    if not active.exists():
        active.write_text("{}\n")
        active.chmod(0o600)
    return Checkpoint(
        run_id="controlled-001",
        scheduled_at=scheduled_at or captured_at,
        captured_at=captured_at,
        pod_name="collector-0",
        pod_uid=POD_UID,
        node_name="node-1",
        producer_epoch=epoch,
        restart_count=restart_count,
        restart_explained=restart_explained,
        last_enqueued_sequence=10,
        last_synced_sequence=10,
        synced_count=10,
        queue_depth=0,
        worker_in_flight=0,
        delivery_clean=1,
        barrier_ready=1,
        drops=drops
        or DropCounters(queue=0, encode=0, sink=0, rollback=0, rotation=0),
        active_jsonl=JsonlEvidence(
            path=active.name,
            size_bytes=active.stat().st_size,
            sha256=sha256(active),
        ),
        collector_sink_healthy=True,
        storage_healthy=True,
        cluster_healthy=True,
        volume_healthy=True,
        export_healthy=True,
        parse_errors=0,
        identity_mismatches=0,
        checksum_mismatches=0,
        foreground_read_errors=0,
        max_observed_latency_ms=max_latency,
    )


def checkpoint_series(root: Path) -> list[Checkpoint]:
    return [
        checkpoint(
            root,
            scheduled_at=START + timedelta(minutes=15 * index),
            captured_at=START + timedelta(minutes=15 * index),
        )
        for index in range(0, 21)
    ]


def source_manifest(
    root: Path,
    *,
    run_id: str = "controlled-001",
) -> CollectorFileManifest:
    relative = Path("collector") / POD_UID / EPOCH / "events-00000.jsonl"
    active = root / relative
    active.parent.mkdir(parents=True, exist_ok=True)
    if not active.exists():
        active.write_text("{}\n")
        active.chmod(0o600)
    return CollectorFileManifest(
        run_id=run_id,
        files=(
            CollectorSourceFile(
                raw_jsonl_path=relative.as_posix(),
                sha256=sha256(active),
                pod_name="collector-0",
                pod_uid=POD_UID,
                node_name="node-1",
                producer_epoch=EPOCH,
            ),
        ),
    )


class FakeProvider:
    def __init__(
        self,
        root: Path,
        *,
        late: bool = False,
        missing: bool = False,
        fault=False,
        durable_at: datetime | None = None,
    ):
        self.root = root
        self.late = late
        self.missing = missing
        self.fault = fault
        self.durable_at = durable_at

    def collect(self, scheduled_at):
        if self.missing:
            return []
        captured_at = scheduled_at + timedelta(seconds=31 if self.late else 0)
        return [
            checkpoint(
                self.root,
                scheduled_at=scheduled_at,
                captured_at=captured_at,
            )
        ]

    def health(self, operation):
        return [
            PreReadHealth(
                run_id=operation.run_id,
                operation_id=operation.operation_id,
                captured_at=operation.offered_at_utc,
                pod_name="collector-0",
                pod_uid=POD_UID,
                node_name="node-1",
                producer_epoch=EPOCH,
                last_enqueued_sequence=1,
                last_synced_sequence=1,
                synced_count=1,
                queue_depth=0,
                worker_in_flight=0,
                delivery_clean=1,
                barrier_ready=1,
                drops=DropCounters(
                    queue=1 if self.fault else 0,
                    encode=0,
                    sink=0,
                    rollback=0,
                    rotation=0,
                ),
                collector_sink_healthy=True,
                storage_healthy=True,
                cluster_healthy=True,
                volume_healthy=True,
                export_healthy=True,
                parse_errors=0,
                identity_mismatches=0,
                checksum_mismatches=0,
                foreground_read_errors=0,
            )
        ]

    def post_read(self, operation, result):
        relative = (
            Path("collector")
            / POD_UID
            / EPOCH
            / f"event-{operation.operation_index:05d}.jsonl"
        )
        raw_path = self.root / relative
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        event_id = f"{operation.operation_index + 1:08x}-0000-4000-8000-000000000000"
        event_time_ns = int(
            (operation.offered_at_utc + timedelta(milliseconds=10)).timestamp()
            * 1_000_000_000
        )
        sink_time_ns = int(
            (operation.offered_at_utc + timedelta(milliseconds=20)).timestamp()
            * 1_000_000_000
        )
        raw_record = (
            '{"client_ref":"'
            + ("c" * 64)
            + '","compound_op_index":0,"compound_tag_b64":"","compound_tag_id":"'
            + hashlib.sha256(b"").hexdigest()
            + '","compound_tag_original_length":0,"compound_tag_truncated":false,'
            f'"eof":false,"event_time_ns":"{event_time_ns}",'
            f'"export_id":{operation.export_id},"export_name_b64":"Y29udHJvbGxlZA",'
            f'"file_id":"{operation.file_id}","file_size":"{operation.file_size}",'
            f'"file_size_valid":true,"local_id":"{event_id}",'
            f'"identity_valid":true,"nfs_status":0,'
            f'"operation_id":"{operation.operation_index + 900:064x}",'
            f'"origin_claim":"unknown","origin_trust":"client_asserted",'
            f'"outcome":"success","producer_epoch":"{EPOCH}",'
            f'"producer_sequence":"{operation.operation_index + 1}",'
            f'"requested_count":{operation.count},"requested_offset":"{operation.offset}",'
            f'"returned_bytes":{result.bytes_read},"sink_time_ns":"{sink_time_ns}",'
            f'"schema":"nfs4_read_v2","sequence_id":0,"session_ref":"","slot_id":0,'
            f'"target_created_ns":"1","target_fh":"AA",'
            f'"volume_id":"{operation.volume_id}"}}'
        ).encode()
        raw_path.write_bytes(raw_record + b"\n")
        raw_path.chmod(0o600)
        event = CollectorDurableEvent(
                run_id=operation.run_id,
                event_id=event_id,
                pod_name="collector-0",
                pod_uid=POD_UID,
                node_name="node-1",
                producer_epoch=EPOCH,
                producer_sequence=operation.operation_index + 1,
                server_operation_id=f"{operation.operation_index + 900:064x}",
                sink_time_ns=sink_time_ns,
                volume_id=operation.volume_id,
                file_id=operation.file_id,
                export_id=operation.export_id,
                export_name="controlled",
                offset=operation.offset,
                count=operation.count,
                returned_bytes=result.bytes_read,
                nfs_status=0,
                outcome="success",
                eof=False,
                durable_at=self.durable_at or operation.offered_at_utc,
                tag=operation.tag,
                raw_jsonl_path=relative.as_posix(),
                raw_jsonl_sha256=sha256(raw_path),
                raw_line_number=1,
                raw_record_sha256=hashlib.sha256(raw_record).hexdigest(),
            )
        append_model(self.root / "durable-events.jsonl", event)
        return [event]


def test_manifest_requires_exact_five_hours_and_explicit_rate():
    with pytest.raises(ValidationError, match="exactly five hours"):
        manifest(end_time=START + timedelta(hours=4))
    with pytest.raises(ValidationError):
        manifest(offered_rate_per_s=0)
    with pytest.raises(ValidationError):
        manifest(
            retention={
                "owner": "owner",
                "cleanup_owner": "owner",
                "cleanup_action": "delete",
                "retain_until": (START + timedelta(days=7)).isoformat(),
            }
        )


def test_bounded_and_live_manifest_profiles_allow_only_run_identity_and_time_changes():
    live = manifest(seed=32)
    candidate = live.model_copy(
        update={
            "run_id": "bounded-validation",
            "run_ref": hashlib.sha256(b"bounded-validation").hexdigest()[:32],
            "start_time": live.start_time + timedelta(days=1),
            "end_time": live.end_time + timedelta(days=1),
        }
    )
    assert _manifest_profile(candidate) == _manifest_profile(live)
    for update in (
        {"cluster_id": "other-cluster"},
        {"seed": 33},
        {"offered_count": live.offered_count - 1},
        {"fixture_catalog_sha256": "0" * 64},
        {
            "software": live.software.model_copy(
                update={"chimera_image_sha256": "0" * 64}
            )
        },
        {
            "stop_rules": live.stop_rules.model_copy(
                update={"latency_guardrail_ms": live.stop_rules.latency_guardrail_ms + 1}
            )
        },
        {
            "retention": live.retention.model_copy(
                update={"cleanup_owner": "different-owner"}
            )
        },
    ):
        assert _manifest_profile(candidate.model_copy(update=update)) != _manifest_profile(
            live
        )


def test_full_profile_allows_arbitrary_start_with_exact_five_hour_duration():
    live_catalog, live = live_catalog_manifest()
    shifted = live.model_copy(
    update={
        "start_time": live.start_time + timedelta(minutes=30),
        "end_time": live.end_time + timedelta(minutes=30),
    }
    )
    _validate_full_profile(shifted)
    operations = planned_operations(shifted, live_catalog)
    buckets = Counter(
    (shifted.start_time + timedelta(minutes=index)).replace(
        minute=0, second=0, microsecond=0
    )
    for index in range(len(operations))
    )
    assert len(buckets) == 6
    assert sum(buckets.values()) == 300


def test_cleanup_deadline_is_created_only_after_finalization():
    schedule = CleanupScheduleRecord(
        run_id="controlled-001",
        finalized_at=START,
        delete_by=START + timedelta(days=7),
        cleanup_owner="experiment-owner",
        validation_report_sha256="a" * 64,
        deletion_inventory_sha256="b" * 64,
    )
    assert schedule.delete_by == START + timedelta(days=7)
    with pytest.raises(ValidationError, match="seven days"):
        CleanupScheduleRecord(
            run_id="controlled-001",
            finalized_at=START,
            delete_by=START + timedelta(days=6),
            cleanup_owner="experiment-owner",
            validation_report_sha256="a" * 64,
            deletion_inventory_sha256="b" * 64,
        )
    completion = CleanupCompletionRecord(
        run_id="controlled-001",
        delete_by=schedule.delete_by,
        completed_at=START + timedelta(days=1),
        cleanup_owner="experiment-owner",
        evidence="private evidence removed",
        schedule_sha256="c" * 64,
        validation_report_sha256=schedule.validation_report_sha256,
        deletion_inventory_sha256=schedule.deletion_inventory_sha256,
        root_dispositions={
            kind: "deleted"
            for kind in (
                "bounded_harness_evidence",
                "bounded_provider_evidence",
                "bounded_adapter_dataset",
                "bounded_clean_dataset",
                "live_harness_evidence",
                "live_provider_evidence",
                "live_adapter_dataset",
                "live_clean_dataset",
            )
        },
    )
    assert completion.completed_at < completion.delete_by
    with pytest.raises(ValidationError, match="must not exceed"):
        completion.model_copy(
            update={"completed_at": schedule.delete_by + timedelta(seconds=1)}
        ).__class__.model_validate(
            completion.model_dump()
            | {"completed_at": schedule.delete_by + timedelta(seconds=1)}
        )
    with pytest.raises(ValidationError, match="all eight roots"):
        CleanupCompletionRecord(
            run_id="controlled-001",
            delete_by=schedule.delete_by,
            completed_at=START + timedelta(days=1),
            cleanup_owner="experiment-owner",
            evidence="incomplete cleanup",
            schedule_sha256="c" * 64,
            validation_report_sha256=schedule.validation_report_sha256,
            deletion_inventory_sha256=schedule.deletion_inventory_sha256,
            root_dispositions={"live_harness_evidence": "deleted"},
        )


def test_cleanup_completion_command_binds_immutable_schedule(tmp_path):
    root = private_dir(tmp_path)
    schedule_path = root / "cleanup-schedule.json"
    schedule = CleanupScheduleRecord(
        run_id="controlled-001",
        finalized_at=START,
        delete_by=START + timedelta(days=7),
        cleanup_owner="experiment-owner",
        validation_report_sha256="a" * 64,
        deletion_inventory_sha256="b" * 64,
    )
    atomic_write_model(schedule_path, schedule)
    completion = CleanupCompletionRecord(
        run_id=schedule.run_id,
        delete_by=schedule.delete_by,
        completed_at=START + timedelta(days=1),
        cleanup_owner=schedule.cleanup_owner,
        evidence="all roots deleted",
        schedule_sha256=sha256(schedule_path),
        validation_report_sha256=schedule.validation_report_sha256,
        deletion_inventory_sha256=schedule.deletion_inventory_sha256,
        root_dispositions={
            kind: "deleted"
            for kind in (
                "bounded_harness_evidence",
                "bounded_provider_evidence",
                "bounded_adapter_dataset",
                "bounded_clean_dataset",
                "live_harness_evidence",
                "live_provider_evidence",
                "live_adapter_dataset",
                "live_clean_dataset",
            )
        },
    )
    completion_path = root / "completion.json"
    atomic_write_model(completion_path, completion)
    result = CliRunner().invoke(
        main,
        [
            "complete-cleanup",
            "--schedule",
            str(schedule_path),
            "--completion",
            str(completion_path),
            "--out",
            str(root / "cleanup-completion.json"),
        ],
    )
    assert result.exit_code == 0, result.output
    bad = completion.model_copy(update={"delete_by": schedule.delete_by + timedelta(days=1)})
    bad_path = root / "bad-completion.json"
    atomic_write_model(bad_path, bad)
    result = CliRunner().invoke(
        main,
        [
            "complete-cleanup",
            "--schedule",
            str(schedule_path),
            "--completion",
            str(bad_path),
            "--out",
            str(root / "bad-output.json"),
        ],
    )
    assert result.exit_code != 0
    assert "does not match schedule" in result.output


def test_reconciliation_success_cannot_hide_failure_details():
    values = {
        "run_id": "controlled-001",
        "manifest_sha256": "a" * 64,
        "ledger_sha256": "b" * 64,
        "observed_sha256": "c" * 64,
        "offered_count": 1,
        "unique_durable_count": 1,
        "duplicate_operation_ids": (),
        "missing_operation_ids": ("d" * 64,),
        "unexpected_operation_ids": (),
        "unknown_event_ids": (),
        "exclusions": {},
        "pod_epochs": {},
        "success": True,
        "failures": (),
    }
    with pytest.raises(ValidationError, match="success conflicts"):
        ReconciliationReport(**values)


def test_generation_is_deterministic_and_ids_cover_range_inputs():
    first = planned_operations(manifest(), catalog())
    second = planned_operations(manifest(), catalog())
    assert first == second
    assert len({item[4] for item in first}) == 4
    changed = planned_operations(manifest(seed=43), catalog())
    assert [item[4] for item in changed] != [item[4] for item in first]
    operation = build_operation(
        manifest(),
        first[0],
        offered_at=START,
        monotonic_ns=1,
        timing_mode="wallclock",
    )
    assert len(operation.tag) == 104
    assert operation.tag[5] == operation.tag[38] == operation.tag[71] == ":"
    assert operation.tag.split(":")[2] == manifest().run_ref
    with pytest.raises(ValidationError, match="run_ref"):
        manifest(run_ref="0" * 32)


def test_catalog_rejects_canonical_and_path_aliases():
    first, second = catalog().fixtures
    with pytest.raises(ValidationError, match="canonical"):
        FixtureCatalog(fixtures=(first, second.model_copy(update={"file_id": first.file_id})))
    with pytest.raises(ValidationError, match="physical"):
        FixtureCatalog(
            fixtures=(
                first,
                second.model_copy(update={"relative_path": first.relative_path}),
            )
        )
    with pytest.raises(ValueError, match="fixture export"):
        planned_operations(manifest(export_id=8), catalog())


def live_catalog_manifest():
    fixtures = tuple(
        FixtureEntry(
            fixture_id=f"fixture-{index:02d}",
            volume_id=VOLUME_ID,
            file_id=f"{index + 1:064x}",
            dir_id=f"dir-{index:02d}",
            export_id=7,
            export_name="controlled",
            export_path="/controlled",
            relative_path=f"fixtures/file-{index:02d}.bin",
            size_bytes=65536,
            sha256=f"{index + 100:064x}",
        )
        for index in range(60)
    )
    live_catalog = FixtureCatalog(fixtures=fixtures)
    live_manifest = manifest(
        seed=32,
        offered_rate_per_s=1 / 60,
        offered_count=300,
        range_bytes=(4096,),
        range_offsets=(0, 12288, 24576, 36864, 49152),
        fixture_catalog_sha256=catalog_sha256(live_catalog),
        tag_mode="empty",
        stop_rules=StopRules(latency_guardrail_ms=5000),
    )
    return live_catalog, live_manifest


def test_authorized_live_shape_is_exactly_sixty_files_by_five_offsets():
    live_catalog, live_manifest = live_catalog_manifest()
    operations = planned_operations(live_manifest, live_catalog)
    assert len(operations) == 300
    assert len({(item[1].file_id, item[2], item[3]) for item in operations}) == 300


def test_manifest_is_atomic_private_and_immutable(tmp_path):
    root = private_dir(tmp_path)
    path = root / "manifest.json"
    atomic_write_model(path, manifest())
    assert path.stat().st_mode & 0o777 == 0o600
    with pytest.raises(FileExistsError):
        atomic_write_model(path, manifest())


def test_immutable_publication_allows_only_one_concurrent_writer(tmp_path):
    root = private_dir(tmp_path)
    path = root / "manifest.json"

    def publish():
        try:
            atomic_write_model(path, manifest())
            return "created"
        except FileExistsError:
            return "exists"

    with ThreadPoolExecutor(max_workers=2) as pool:
        outcomes = list(pool.map(lambda _: publish(), range(2)))
    assert sorted(outcomes) == ["created", "exists"]
    assert CollectionManifest.model_validate_json(path.read_text()) == manifest()


def test_jsonl_publication_is_atomic_and_create_only(tmp_path):
    root = private_dir(tmp_path)
    path = root / "records.jsonl"
    records = [
        mount_operation("data.bin", count=4096, offset=0, file_size=4096)
    ]

    def publish():
        try:
            write_jsonl(path, records)
            return "created"
        except FileExistsError:
            return "exists"

    with ThreadPoolExecutor(max_workers=2) as pool:
        outcomes = list(pool.map(lambda _: publish(), range(2)))
    assert sorted(outcomes) == ["created", "exists"]
    assert len(read_models(path, OfferedOperation)) == 1


def test_private_evidence_rejects_symlink_and_open_directory(tmp_path):
    target = tmp_path / "target"
    target.mkdir(mode=0o700)
    link = tmp_path / "link"
    link.symlink_to(target, target_is_directory=True)
    with pytest.raises(ValueError, match="real directory"):
        atomic_write_model(link / "manifest.json", manifest())
    open_dir = tmp_path / "open"
    open_dir.mkdir(mode=0o755)
    with pytest.raises(PermissionError, match="owner-only"):
        atomic_write_model(open_dir / "manifest.json", manifest())


def test_runner_write_ahead_restart_and_bounded_rate(tmp_path):
    root = private_dir(tmp_path)
    sleeps = []
    moments = iter(START + timedelta(seconds=i) for i in range(20))
    monotonic = iter(range(100, 120))
    runner = CollectionRunner(
        now=lambda: next(moments),
        monotonic_ns=lambda: next(monotonic),
        sleep=sleeps.append,
    )
    assert runner.run(manifest(), catalog(), root, DryRunExecutor(lambda: START)) == 4
    offered = read_models(root / "offered.jsonl", OfferedOperation)
    assert len(offered) == 4
    assert {item.timing_mode for item in offered} == {"wallclock"}
    assert sleeps == pytest.approx([1.0, 2.0, 3.0], abs=1e-6)
    assert runner.run(manifest(), catalog(), root, DryRunExecutor(lambda: START)) == 4
    assert len(read_models(root / "offered.jsonl", OfferedOperation)) == 4


def test_ledger_is_durable_before_executor_issue(tmp_path):
    root = private_dir(tmp_path)

    class InspectingExecutor:
        def read(self, operation):
            records = read_models(root / "offered.jsonl", OfferedOperation)
            assert records[-1].operation_id == operation.operation_id
            raise RuntimeError("simulated crash")

    with pytest.raises(RuntimeError, match="simulated crash"):
        CollectionRunner(now=lambda: START, sleep=lambda _: None).run(
            manifest(offered_count=1, tag_mode="empty"),
            catalog(),
            root,
            InspectingExecutor(),
        )
    assert len(read_models(root / "offered.jsonl", OfferedOperation)) == 1
    with pytest.raises(RuntimeError, match="ambiguous crash state"):
        CollectionRunner(now=lambda: START, sleep=lambda _: None).run(
            manifest(offered_count=1, tag_mode="empty"),
            catalog(),
            root,
            DryRunExecutor(lambda: START),
        )


def test_dry_run_never_opens_target(monkeypatch, tmp_path):
    root = private_dir(tmp_path)
    real_open = os.open

    def guarded_open(path, *args, **kwargs):
        assert "/controlled/" not in str(path)
        return real_open(path, *args, **kwargs)

    monkeypatch.setattr(os, "open", guarded_open)
    CollectionRunner(now=lambda: START, sleep=lambda _: None).run(
        manifest(offered_count=1, tag_mode="empty"),
        catalog(),
        root,
        DryRunExecutor(lambda: START),
    )


def test_runner_rejects_operations_outside_frozen_window(tmp_path):
    root = private_dir(tmp_path)
    with pytest.raises(RuntimeError, match="outside the frozen run window"):
        CollectionRunner(now=lambda: START - timedelta(seconds=1), sleep=lambda _: None).run(
            manifest(offered_count=1, tag_mode="empty"),
            catalog(),
            root,
            DryRunExecutor(lambda: START),
        )


class FakeClock:
    def __init__(self):
        self.wall = START
        self.monotonic = 0
        self.sleeps = []

    def now(self):
        return self.wall

    def monotonic_ns(self):
        return self.monotonic

    def advance(self, seconds):
        self.wall += timedelta(seconds=seconds)
        self.monotonic += int(seconds * 1_000_000_000)

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.advance(seconds)


class AdvancingExecutor:
    def __init__(self, clock, duration_s):
        self.clock = clock
        self.duration_s = duration_s

    def read(self, operation):
        self.clock.advance(self.duration_s)
        return ExecutionResult(
            operation_id=operation.operation_id,
            completed_at_utc=self.clock.now(),
            status="ok",
            bytes_read=operation.count,
            latency_ms=self.duration_s * 1000,
            executor_kind="dry-run",
            filesystem_type="none",
        )


def test_absolute_schedule_subtracts_executor_duration(tmp_path):
    root = private_dir(tmp_path)
    clock = FakeClock()
    runner = CollectionRunner(
        now=clock.now,
        monotonic_ns=clock.monotonic_ns,
        sleep=clock.sleep,
    )
    runner.run(
        manifest(
            offered_count=3,
            offered_rate_per_s=1 / 60,
            stop_rules=StopRules(latency_guardrail_ms=60000),
        ),
        catalog(),
        root,
        AdvancingExecutor(clock, 10),
    )
    assert clock.sleeps == pytest.approx([50, 50])
    assert [item.offered_at_utc for item in read_models(root / "offered.jsonl", OfferedOperation)] == [
        START,
        START + timedelta(minutes=1),
        START + timedelta(minutes=2),
    ]


def test_absolute_schedule_fails_closed_on_overrun_before_next_read(tmp_path):
    root = private_dir(tmp_path)
    clock = FakeClock()
    runner = CollectionRunner(
        now=clock.now,
        monotonic_ns=clock.monotonic_ns,
        sleep=clock.sleep,
    )
    with pytest.raises(RuntimeError, match="schedule overrun"):
        runner.run(
            manifest(
                offered_count=2,
                offered_rate_per_s=1 / 60,
                stop_rules=StopRules(latency_guardrail_ms=120000),
            ),
            catalog(),
            root,
            AdvancingExecutor(clock, 91),
        )
    assert len(read_models(root / "offered.jsonl", OfferedOperation)) == 1


@pytest.mark.parametrize(
    ("duration_s", "should_pass"),
    [(5.0, True), (5.001, False)],
)
def test_supervised_probe_plus_read_guardrail_boundary(tmp_path, duration_s, should_pass):
    root = private_dir(tmp_path)
    clock = FakeClock()
    runner = CollectionRunner(
        now=clock.now,
        monotonic_ns=clock.monotonic_ns,
        sleep=clock.sleep,
    )
    arguments = (
        manifest(
            offered_count=1,
            tag_mode="empty",
            pre_read_plus_read_guardrail_ms=5000,
            stop_rules=StopRules(latency_guardrail_ms=6000),
        ),
        catalog(),
        root,
        AdvancingExecutor(clock, duration_s),
    )
    if should_pass:
        assert runner.run(
            *arguments,
            simulated_timing=True,
            checkpoint_provider=FakeProvider(root),
        ) == 1
    else:
        with pytest.raises(RuntimeError, match="pre-read plus read guardrail"):
            runner.run(
                *arguments,
                simulated_timing=True,
                checkpoint_provider=FakeProvider(root),
            )


def test_probe_alone_over_guardrail_stops_before_read(tmp_path):
    root = private_dir(tmp_path)
    clock = FakeClock()

    class SlowProbeProvider(FakeProvider):
        def health(self, operation):
            clock.advance(5.001)
            return super().health(operation)

    class MustNotRead:
        def read(self, operation):
            raise AssertionError("read must not start after probe guardrail breach")

    with pytest.raises(RuntimeError, match="pre-read probe guardrail"):
        CollectionRunner(
            now=clock.now,
            monotonic_ns=clock.monotonic_ns,
            sleep=clock.sleep,
        ).run(
            manifest(
                offered_count=1,
                tag_mode="empty",
                pre_read_plus_read_guardrail_ms=5000,
            ),
            catalog(),
            root,
            MustNotRead(),
            simulated_timing=True,
            checkpoint_provider=SlowProbeProvider(root),
        )
    assert not (root / "offered.jsonl").exists()


def test_checkpoint_validation_covers_drop_epoch_restart_cadence_and_hash(tmp_path):
    root = private_dir(tmp_path)
    first = checkpoint(root)
    second = checkpoint(
        root,
        captured_at=START + timedelta(minutes=14),
        epoch=OTHER_EPOCH,
        restart_count=1,
        max_latency=300,
        drops=DropCounters(queue=1, encode=0, sink=0, rollback=0, rotation=0),
    )
    reasons = checkpoint_stop_reasons(manifest(), [first, second], root)
    assert any("drop observed" in reason for reason in reasons)
    assert any("epoch mismatch" in reason for reason in reasons)
    assert any("unexplained restart" in reason for reason in reasons)
    assert any("checkpoint cadence mismatch" in reason for reason in reasons)
    assert any("latency guardrail" in reason for reason in reasons)
    (root / "collector-active.jsonl").write_text("changed\n")
    assert any(
        "checkpoint file checksum mismatch" in reason
        for reason in checkpoint_stop_reasons(manifest(), [first], root)
    )


def test_checkpoint_coverage_requires_every_fifteen_minute_boundary(tmp_path):
    root = private_dir(tmp_path)
    assert "checkpoint evidence is missing" in checkpoint_stop_reasons(manifest(), [], root)
    reasons = checkpoint_stop_reasons(manifest(), checkpoint_series(root)[:-1], root)
    assert any("incomplete checkpoint coverage" in reason for reason in reasons)
    assert checkpoint_stop_reasons(manifest(), checkpoint_series(root), root) == []


def prepare_reconciliation(tmp_path: Path):
    root = private_dir(tmp_path)
    atomic_write_model(root / "manifest.json", manifest(offered_count=2))
    CollectionRunner(sleep=lambda _: None).run(
        manifest(offered_count=2),
        catalog(),
        root,
        DryRunExecutor(lambda: START),
        simulated_timing=True,
    )
    operations = read_models(root / "offered.jsonl", OfferedOperation)
    return root, operations


def observed(operation, event_id, status="durable", reason=None):
    return ObservedEvent(
        operation_id=operation.operation_id if operation else None,
        event_id=event_id,
        pod_uid=POD_UID,
        producer_epoch=EPOCH,
        producer_sequence=operation.operation_index + 1 if operation else 1,
        volume_id=operation.volume_id if operation else VOLUME_ID,
        file_id=operation.file_id if operation else FILE_A,
        offset=operation.offset if operation else 0,
        count=operation.count if operation else 1,
        durable_at=operation.offered_at_utc if operation else START,
        status=status,
        exclusion_reason=reason,
        tag=operation.tag if operation else None,
    )


def test_exact_reconciliation_passes_complete_unique_events(tmp_path):
    root, operations = prepare_reconciliation(tmp_path)
    observed_path = root / "observed.jsonl"
    write_jsonl(
        observed_path,
        [
            observed(operation, f"event-{index}").model_copy(
                update={
                    "server_operation_id": f"server-{index}",
                    "sink_time_ns": index,
                }
            )
            for index, operation in enumerate(operations)
        ],
    )
    report = reconcile(
        root / "manifest.json",
        root / "offered.jsonl",
        observed_path,
        checkpoint_series(root),
        source_manifest(root),
        root,
    )
    assert report.success
    assert report.unique_durable_count == 2


def test_reconciliation_reports_duplicate_missing_unknown_and_unexpected(tmp_path):
    root, operations = prepare_reconciliation(tmp_path)
    unexpected = operations[0].model_copy(update={"operation_id": "f" * 64})
    records = [
        observed(operations[0], "event-0"),
        observed(operations[0], "event-duplicate"),
        observed(unexpected, "event-unexpected"),
        observed(None, "event-unknown", status="unknown"),
    ]
    observed_path = root / "observed.jsonl"
    write_jsonl(observed_path, records)
    report = reconcile(
        root / "manifest.json",
        root / "offered.jsonl",
        observed_path,
        checkpoint_series(root),
        source_manifest(root),
        root,
    )
    assert not report.success
    assert report.duplicate_operation_ids == (operations[0].operation_id,)
    assert report.missing_operation_ids == (operations[1].operation_id,)
    assert report.unexpected_operation_ids == ("f" * 64,)
    assert report.unknown_event_ids == ("event-unknown",)


def test_approved_exclusion_explains_a_gap(tmp_path):
    root, operations = prepare_reconciliation(tmp_path)
    observed_path = root / "observed.jsonl"
    write_jsonl(
        observed_path,
        [
            observed(operations[0], "event-0"),
            observed(
                operations[1],
                "event-excluded",
                status="excluded",
                reason="collector_shutdown",
            ),
        ],
    )
    report = reconcile(
        root / "manifest.json",
        root / "offered.jsonl",
        observed_path,
        checkpoint_series(root),
        source_manifest(root),
        root,
    )
    assert report.success
    assert report.exclusions["collector_shutdown"] == (operations[1].operation_id,)


def test_reconciliation_rejects_tag_epoch_and_contradictory_disposition(tmp_path):
    root, operations = prepare_reconciliation(tmp_path)
    bad_epoch = observed(operations[0], "event-0").model_copy(
        update={"producer_epoch": OTHER_EPOCH, "tag": "pc1:d:wrong"}
    )
    excluded_same = observed(
        operations[0],
        "event-excluded",
        status="excluded",
        reason="collector_shutdown",
    )
    observed_path = root / "observed.jsonl"
    write_jsonl(observed_path, [bad_epoch, excluded_same, observed(operations[1], "event-1")])
    report = reconcile(
        root / "manifest.json",
        root / "offered.jsonl",
        observed_path,
        checkpoint_series(root),
        source_manifest(root),
        root,
    )
    assert not report.success
    assert "observed event pod/epoch lacks checkpoint evidence" in report.failures
    assert "observed event correlation tag mismatch" in report.failures
    assert "operation has duplicate or contradictory dispositions" in report.failures


def test_untagged_correlation_uses_range_time_and_order_and_fails_ambiguity():
    run_manifest = manifest(offered_count=2, tag_mode="empty")
    plans = planned_operations(run_manifest, catalog())
    offered = [
        build_operation(
            run_manifest,
            plan,
            offered_at=START + timedelta(minutes=index),
            monotonic_ns=index,
            timing_mode="wallclock",
        )
        for index, plan in enumerate(plans)
    ]
    unresolved = [
        ObservedEvent.model_validate(
            observed(operation, f"raw-{index}").model_dump()
            | {
                "operation_id": None,
                "status": "unknown",
                "tag": None,
                "durable_at": operation.offered_at_utc + timedelta(seconds=1),
            }
        )
        for index, operation in enumerate(offered)
    ]
    correlated = correlate_untagged_events(run_manifest, offered, unresolved)
    assert [item.operation_id for item in correlated] == [
        operation.operation_id for operation in offered
    ]
    alias = offered[0].model_copy(
        update={"operation_id": "e" * 64, "operation_index": 99}
    )
    with pytest.raises(ValueError, match="ambiguous"):
        correlate_untagged_events(run_manifest, [offered[0], alias], [unresolved[0]])


def test_untagged_correlation_orders_cross_pod_by_durable_time_and_scopes_sequences():
    run_manifest = manifest(offered_count=4, tag_mode="empty")
    plans = planned_operations(run_manifest, catalog())
    offered = [
        build_operation(
            run_manifest,
            plan,
            offered_at=START + timedelta(minutes=index),
            monotonic_ns=index,
            timing_mode="wallclock",
        )
        for index, plan in enumerate(plans)
    ]
    other_pod = "44444444-4444-4444-8444-444444444444"
    unresolved = []
    for index, operation in enumerate(offered):
        pod_uid = POD_UID if index % 2 == 0 else other_pod
        sequence = index // 2 + 1
        unresolved.append(
            ObservedEvent.model_validate(
                observed(operation, f"raw-{index}").model_dump()
                | {
                    "operation_id": None,
                    "status": "unknown",
                    "tag": None,
                    "pod_uid": pod_uid,
                    "producer_sequence": sequence,
                    "durable_at": operation.offered_at_utc + timedelta(seconds=1),
                }
            )
        )
    correlated = correlate_untagged_events(run_manifest, offered, unresolved)
    assert [item.operation_id for item in correlated] == [
        operation.operation_id for operation in offered
    ]
    broken = unresolved.copy()
    broken[2] = broken[2].model_copy(update={"producer_sequence": 3})
    with pytest.raises(ValueError, match="not contiguous"):
        correlate_untagged_events(run_manifest, offered, broken)


def test_checkpoint_schema_rejects_inconsistent_synced_count(tmp_path):
    root = private_dir(tmp_path)
    with pytest.raises(ValidationError, match="synced_count"):
        Checkpoint.model_validate(checkpoint(root).model_dump() | {"synced_count": 0})


def test_zero_event_checkpoint_and_source_envelope_contract(tmp_path):
    root = private_dir(tmp_path)
    zero = checkpoint(root).model_dump() | {
        "last_enqueued_sequence": 0,
        "last_synced_sequence": 0,
        "synced_count": 0,
    }
    assert Checkpoint.model_validate(zero).synced_count == 0
    with pytest.raises(ValidationError, match="fully drained"):
        Checkpoint.model_validate(checkpoint(root).model_dump() | {"queue_depth": 1})
    valid = source_manifest(root)
    assert valid.files[0].producer_epoch == EPOCH
    with pytest.raises(ValidationError, match="raw path"):
        CollectorSourceFile(
            raw_jsonl_path="events.jsonl",
            sha256="0" * 64,
            pod_name="collector-0",
            pod_uid=POD_UID,
            node_name="node-1",
            producer_epoch=EPOCH,
        )


def test_frozen_manifest_and_cli_complete_without_live_io(tmp_path):
    source = Path(__file__).parent / "fixtures"
    frozen = CollectionManifest.model_validate_json(
        (source / "frozen-manifest.json").read_text()
    )
    assert frozen.cluster_manifest_sha256 == sha256(
        CONTRACT_DIR / "cluster-provenance-v1.json"
    )
    assert frozen.event_schema_sha256 == sha256(
        CONTRACT_DIR / "nfs4-read-v2.schema.json"
    )
    assert frozen.budget_evidence_sha256 == sha256(
        source / "budget-evidence-v1.json"
    )
    run_dir = tmp_path / "cli-run"
    result = CliRunner().invoke(
        main,
        [
            "init",
            "--manifest",
            str(source / "frozen-manifest.json"),
            "--catalog",
            str(source / "frozen-catalog.json"),
            "--cluster-provenance",
            str(CONTRACT_DIR / "cluster-provenance-v1.json"),
            "--event-schema",
            str(CONTRACT_DIR / "nfs4-read-v2.schema.json"),
            "--budget-evidence",
            str(source / "budget-evidence-v1.json"),
            "--out",
            str(run_dir),
        ],
    )
    assert result.exit_code == 0, result.output
    result = CliRunner().invoke(main, ["run", "--run-dir", str(run_dir), "--dry-run"])
    assert result.exit_code == 0, result.output
    assert "offered=5" in result.output
    assert {
        item.timing_mode for item in read_models(run_dir / "offered.jsonl", OfferedOperation)
    } == {"simulated"}


def _finalization_fixture(tmp_path: Path, monkeypatch):
    tmp_path.mkdir(parents=True, exist_ok=True)
    source = FIXTURE_DIR
    live_manifest = json.loads((source / "frozen-manifest.json").read_text())
    live_manifest["offered_count"] = 300
    live_manifest["offered_rate_per_s"] = 1 / 60
    live_manifest["stop_rules"]["latency_guardrail_ms"] = 5000
    live_catalog, _ = live_catalog_manifest()
    live_manifest["fixture_catalog_sha256"] = catalog_sha256(live_catalog)
    manifest_path = tmp_path / "live-manifest.json"
    manifest_path.write_text(json.dumps(live_manifest) + "\n")
    manifest_path.chmod(0o600)
    catalog_path = tmp_path / "live-catalog.json"
    catalog_path.write_text(
        json.dumps(live_catalog.model_dump(mode="json")) + "\n"
    )
    catalog_path.chmod(0o600)
    run_dir = tmp_path / "run"
    result = CliRunner().invoke(
        main,
        [
            "init",
            "--manifest",
            str(manifest_path),
            "--catalog",
            str(catalog_path),
            "--cluster-provenance",
            str(CONTRACT_DIR / "cluster-provenance-v1.json"),
            "--event-schema",
            str(CONTRACT_DIR / "nfs4-read-v2.schema.json"),
            "--budget-evidence",
            str(source / "budget-evidence-v1.json"),
            "--out",
            str(run_dir),
        ],
    )
    assert result.exit_code == 0, result.output
    live_manifest_model = CollectionManifest.model_validate(live_manifest)
    planned = planned_operations(live_manifest_model, live_catalog)
    offered_rows = []
    execution_rows = []
    durable_rows = []
    observed_rows = []
    raw_lines = []
    raw_relative = Path("collector") / POD_UID / EPOCH / "events.jsonl"
    raw_path = run_dir / raw_relative
    raw_path.parent.mkdir(parents=True, mode=0o700)
    for index, item in enumerate(planned):
        offered = build_operation(
            live_manifest_model,
            item,
            offered_at=START + timedelta(minutes=index),
            monotonic_ns=index + 1,
            timing_mode="wallclock",
        )
        offered_rows.append(offered)
        completed = offered.offered_at_utc + timedelta(seconds=1)
        execution_rows.append(
            ExecutionResult(
                operation_id=offered.operation_id,
                completed_at_utc=completed,
                status="ok",
                bytes_read=offered.count,
                latency_ms=10,
                supervised_latency_ms=100,
                content_sha256=hashlib.sha256(
                    f"content-{index}".encode()
                ).hexdigest(),
                executor_kind="direct-mount",
                filesystem_type="nfs4",
            )
        )
        event_id = str(uuid.UUID(int=index + 1))
        server_operation_id = hashlib.sha256(
            f"server-{index}".encode()
        ).hexdigest()
        event_ns = int((offered.offered_at_utc + timedelta(milliseconds=10)).timestamp() * 1e9)
        sink_ns = int((offered.offered_at_utc + timedelta(milliseconds=20)).timestamp() * 1e9)
        raw = {
            "schema": "nfs4_read_v2",
            "local_id": event_id,
            "producer_epoch": EPOCH,
            "producer_sequence": str(index + 1),
            "event_time_ns": str(event_ns),
            "sink_time_ns": str(sink_ns),
            "target_created_ns": "1",
            "volume_id": offered.volume_id,
            "file_id": offered.file_id,
            "identity_valid": True,
            "export_name_b64": base64.urlsafe_b64encode(b"/controlled")
            .rstrip(b"=")
            .decode(),
            "export_id": offered.export_id,
            "target_fh": "AA",
            "operation_id": server_operation_id,
            "client_ref": "c" * 64,
            "session_ref": "",
            "compound_op_index": 0,
            "slot_id": 0,
            "sequence_id": index + 1,
            "compound_tag_b64": "",
            "compound_tag_original_length": 0,
            "compound_tag_truncated": False,
            "compound_tag_id": hashlib.sha256(b"").hexdigest(),
            "origin_claim": "unknown",
            "origin_trust": "client_asserted",
            "requested_offset": str(offered.offset),
            "requested_count": offered.count,
            "returned_bytes": offered.count,
            "nfs_status": 0,
            "outcome": "success",
            "eof": False,
            "file_size_valid": True,
            "file_size": str(offered.file_size),
        }
        raw_line = json.dumps(raw, sort_keys=True, separators=(",", ":")).encode()
        raw_lines.append(raw_line)
        durable_at = offered.offered_at_utc + timedelta(seconds=2)
        durable_rows.append(
            CollectorDurableEvent(
                run_id=offered.run_id,
                event_id=event_id,
                pod_name="collector-0",
                pod_uid=POD_UID,
                node_name="node-1",
                producer_epoch=EPOCH,
                producer_sequence=index + 1,
                server_operation_id=server_operation_id,
                sink_time_ns=sink_ns,
                volume_id=offered.volume_id,
                file_id=offered.file_id,
                export_id=offered.export_id,
                export_name="/controlled",
                offset=offered.offset,
                count=offered.count,
                returned_bytes=offered.count,
                nfs_status=0,
                outcome="success",
                eof=False,
                durable_at=durable_at,
                tag="",
                raw_jsonl_path=raw_relative.as_posix(),
                raw_jsonl_sha256="0" * 64,
                raw_line_number=index + 1,
                raw_record_sha256=hashlib.sha256(raw_line).hexdigest(),
            )
        )
        observed_rows.append(
            ObservedEvent(
                operation_id=offered.operation_id,
                event_id=event_id,
                pod_uid=POD_UID,
                producer_epoch=EPOCH,
                producer_sequence=index + 1,
                server_operation_id=server_operation_id,
                sink_time_ns=sink_ns,
                volume_id=offered.volume_id,
                file_id=offered.file_id,
                offset=offered.offset,
                count=offered.count,
                durable_at=durable_at,
                status="durable",
                tag=None,
            )
        )
    raw_path.write_bytes(b"\n".join(raw_lines) + b"\n")
    raw_path.chmod(0o600)
    raw_hash = sha256(raw_path)
    durable_rows = [
        row.model_copy(update={"raw_jsonl_sha256": raw_hash})
        for row in durable_rows
    ]
    source_files = (
        CollectorSourceFile(
            raw_jsonl_path=raw_relative.as_posix(),
            sha256=raw_hash,
            pod_name="collector-0",
            pod_uid=POD_UID,
            node_name="node-1",
            producer_epoch=EPOCH,
        ),
    )
    atomic_write_model(
        run_dir / "collector-file-manifest.json",
        CollectorFileManifest(run_id="controlled-demo", files=source_files),
    )
    write_jsonl(run_dir / "offered.jsonl", offered_rows)
    write_jsonl(run_dir / "execution.jsonl", execution_rows)
    write_jsonl(run_dir / "durable-events.jsonl", durable_rows)
    write_jsonl(run_dir / "observed.jsonl", observed_rows)
    checkpoints = []
    active = JsonlEvidence(
        path=raw_relative.as_posix(),
        size_bytes=raw_path.stat().st_size,
        sha256=raw_hash,
    )
    for checkpoint_index in range(21):
        scheduled = START + timedelta(minutes=15 * checkpoint_index)
        synced = sum(row.durable_at <= scheduled for row in durable_rows)
        checkpoints.append(
            Checkpoint(
                run_id="controlled-demo",
                scheduled_at=scheduled,
                captured_at=scheduled,
                pod_name="collector-0",
                pod_uid=POD_UID,
                node_name="node-1",
                producer_epoch=EPOCH,
                restart_count=0,
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
                active_jsonl=active,
                collector_sink_healthy=True,
                storage_healthy=True,
                cluster_healthy=True,
                volume_healthy=True,
                export_healthy=True,
                parse_errors=0,
                identity_mismatches=0,
                checksum_mismatches=0,
                foreground_read_errors=0,
                max_observed_latency_ms=10,
            )
        )
    write_jsonl(run_dir / "checkpoints.jsonl", checkpoints)
    write_jsonl(
        run_dir / "pre-read-health.jsonl",
        [
            PreReadHealth(
                run_id=offered.run_id,
                operation_id=offered.operation_id,
                captured_at=offered.offered_at_utc + timedelta(milliseconds=500),
                pod_name="collector-0",
                pod_uid=POD_UID,
                node_name="node-1",
                producer_epoch=EPOCH,
                last_enqueued_sequence=index,
                last_synced_sequence=index,
                synced_count=index,
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
                collector_sink_healthy=True,
                storage_healthy=True,
                cluster_healthy=True,
                volume_healthy=True,
                export_healthy=True,
                parse_errors=0,
                identity_mismatches=0,
                checksum_mismatches=0,
                foreground_read_errors=0,
            )
            for index, offered in enumerate(offered_rows)
        ],
    )
    for directory in (path for path in run_dir.rglob("*") if path.is_dir()):
        directory.chmod(0o700)
    atomic_write_model(
        run_dir / "reconciliation.json",
        ReconciliationReport(
            run_id="controlled-demo",
            manifest_sha256=sha256(run_dir / "manifest.json"),
            ledger_sha256=sha256(run_dir / "offered.jsonl"),
            observed_sha256=sha256(run_dir / "observed.jsonl"),
            offered_count=300,
            unique_durable_count=300,
            duplicate_operation_ids=(),
            missing_operation_ids=(),
            unexpected_operation_ids=(),
            unknown_event_ids=(),
            exclusions={},
            pod_epochs={},
            success=True,
            failures=(),
        ),
    )
    dataset_root = tmp_path / "dataset"
    dataset_root.mkdir(mode=0o700)
    output = (
        dataset_root
        / "events_clean"
        / f"volume_id={live_manifest['volume_id']}"
        / "date=2026-11-04"
        / "part-00000.jsonl"
    )
    output.parent.mkdir(parents=True, mode=0o700)
    output.write_text("")
    output.chmod(0o600)
    provenance = dataset_root / "validation.json"
    provenance.write_text('{"status":"passed"}\n')
    provenance.chmod(0o600)
    clean_manifest = dataset_root / "manifest.json"

    def evidence_root(name, files):
        root = tmp_path / name
        root.mkdir(mode=0o700)
        for path, content in files.items():
            target = root / path
            target.parent.mkdir(parents=True, exist_ok=True)
            for directory in (
                parent
                for parent in (target.parent, *target.parent.parents)
                if parent == root or root in parent.parents
            ):
                directory.chmod(0o700)
            target.write_text(content)
            target.chmod(0o600)
        return root

    def add_clean_auxiliary(root, volume_id, date, report):
        created = []
        for relative in (
            f"opens/volume_id={volume_id}/date={date}/part-00000.jsonl",
            f"open_ranges/volume_id={volume_id}/date={date}/part-00000.jsonl",
            f"meta/volume_id={volume_id}/part-00000.jsonl",
        ):
            target = root / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text("")
            target.chmod(0o600)
            created.append(target)
        config = CleanConfig()
        config_path = root / "config.resolved.yaml"
        config_path.write_text(
            yaml.safe_dump(
                {"clean": config.model_dump(mode="json")},
                sort_keys=True,
            )
        )
        config_path.chmod(0o600)
        summary_path = root / "report/summary.md"
        summary_path.write_text(render_summary(report))
        summary_path.chmod(0o600)
        created.extend((config_path, summary_path))
        return config_hash(config), created

    candidate_manifest = json.loads(json.dumps(live_manifest))
    candidate_manifest["run_id"] = "bounded-validation"
    candidate_manifest["run_ref"] = hashlib.sha256(
        b"bounded-validation"
    ).hexdigest()[:32]
    bounded_harness = evidence_root(
        "bounded-harness",
        {
            "candidate-manifest.json": json.dumps(candidate_manifest) + "\n",
            "cluster-provenance-v1.json": (
                CONTRACT_DIR / "cluster-provenance-v1.json"
            ).read_text(),
            "event-schema-v2.json": (
                CONTRACT_DIR / "nfs4-read-v2.schema.json"
            ).read_text(),
            "budget-evidence-v1.json": (
                FIXTURE_DIR / "budget-evidence-v1.json"
            ).read_text(),
            "fixture-catalog.json": (run_dir / "fixture-catalog.json").read_text(),
            "source-archive.tar.gz": "source archive\n",
            "adapter-wrapper": "adapter wrapper\n",
            "cleaner-wrapper": "cleaner wrapper\n",
            ".approved-tools/checkpoint-provider": "provider\n",
            ".approved-tools/provider-package.zip": "provider package\n",
        },
    )
    bounded_provider = evidence_root(
        "bounded-provider",
        {
            "provider-package.json": "provider package\n",
            "provider.py": "provider\n",
            "collector.py": "collector\n",
            "validator.py": "validator\n",
            "runbook.md": "runbook\n",
            "provider-evidence.jsonl": '{"status":"passed"}\n',
        },
    )
    artifact_constants = {
        "APPROVED_EXECUTED_SOURCE_ARCHIVE_SHA256": sha256(
            bounded_harness / "source-archive.tar.gz"
        ),
        "APPROVED_ADAPTER_WRAPPER_SHA256": sha256(
            bounded_harness / "adapter-wrapper"
        ),
        "APPROVED_CLEANER_WRAPPER_SHA256": sha256(
            bounded_harness / "cleaner-wrapper"
        ),
        "APPROVED_PROVIDER_PACKAGE_SHA256": sha256(
            bounded_provider / "provider-package.json"
        ),
        "APPROVED_PROVIDER_SHA256": sha256(bounded_provider / "provider.py"),
        "APPROVED_COLLECTOR_SHA256": sha256(bounded_provider / "collector.py"),
        "APPROVED_VALIDATOR_SHA256": sha256(bounded_provider / "validator.py"),
        "APPROVED_VALIDATION_RUNBOOK_SHA256": sha256(
            bounded_provider / "runbook.md"
        ),
    }
    for name, value in artifact_constants.items():
        monkeypatch.setattr(f"predictive_cache.collection.cli.{name}", value)
    live_snapshot = run_dir / ".approved-tools/checkpoint-provider"
    live_snapshot.parent.mkdir(mode=0o700)
    live_snapshot.write_text("provider\n")
    live_snapshot.chmod(0o500)
    live_package = run_dir / ".approved-tools/provider-package.zip"
    live_package.write_text("provider package\n")
    live_package.chmod(0o400)
    for root, run_id, manifest_path in (
        (
            bounded_harness,
            "bounded-validation",
            bounded_harness / "candidate-manifest.json",
        ),
        (run_dir, "controlled-demo", run_dir / "manifest.json"),
    ):
        atomic_write_model(
            root / "provider-invocation.json",
            ProviderInvocationRecord(
                run_id=run_id,
                manifest_sha256=sha256(manifest_path),
                provider_package_sha256=artifact_constants[
                    "APPROVED_PROVIDER_PACKAGE_SHA256"
                ],
                provider_package_path=".approved-tools/provider-package.zip",
                provider_snapshot_path=".approved-tools/checkpoint-provider",
                provider_snapshot_sha256=artifact_constants[
                    "APPROVED_PROVIDER_SHA256"
                ],
                collector_sha256=artifact_constants[
                    "APPROVED_COLLECTOR_SHA256"
                ],
                validator_sha256=artifact_constants[
                    "APPROVED_VALIDATOR_SHA256"
                ],
                interpreter_path="/usr/bin/python3.12",
                interpreter_sha256=(
                    "f5e62d752212336f7eaa112499e41bdd58f655d3cf152c7d3c7cc1e5a01545a0"
                ),
                argv_contract=(
                    "/usr/bin/python3.12",
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
                created_at=START,
            ),
        )
    candidate_model = CollectionManifest.model_validate(candidate_manifest)
    candidate_operation_id = planned_operations(
        candidate_model,
        live_catalog,
    )[0][4]
    permit = AdapterValidationPermit(
        permit_id="11111111-1111-4111-8111-111111111111",
        run_id="bounded-validation",
        full_manifest_sha256=sha256(bounded_harness / "candidate-manifest.json"),
        validation_operations=1,
        operation_indexes=(0,),
        operation_ids=(candidate_operation_id,),
        provider_package_sha256=artifact_constants[
            "APPROVED_PROVIDER_PACKAGE_SHA256"
        ],
        provider_sha256=artifact_constants["APPROVED_PROVIDER_SHA256"],
        collector_sha256=artifact_constants["APPROVED_COLLECTOR_SHA256"],
        validator_sha256=artifact_constants["APPROVED_VALIDATOR_SHA256"],
        runbook_sha256=artifact_constants["APPROVED_VALIDATION_RUNBOOK_SHA256"],
        status="approved",
        expires_at=START + timedelta(hours=1),
    )
    atomic_write_model(bounded_harness / "adapter-validation-permit.json", permit)
    bounded_operation = build_operation(
        candidate_model,
        planned_operations(candidate_model, live_catalog)[0],
        offered_at=START,
        monotonic_ns=1,
        timing_mode="wallclock",
    )
    write_jsonl(bounded_harness / "offered.jsonl", [bounded_operation])
    bounded_execution = ExecutionResult(
        operation_id=bounded_operation.operation_id,
        completed_at_utc=START + timedelta(seconds=30),
        status="ok",
        bytes_read=4096,
        latency_ms=10,
        supervised_latency_ms=100,
        content_sha256="f" * 64,
        executor_kind="direct-mount",
        filesystem_type="nfs4",
    )
    write_jsonl(bounded_harness / "execution.jsonl", [bounded_execution])
    bounded_event_id = str(uuid.UUID(int=1000))
    bounded_server_operation = hashlib.sha256(b"bounded-server").hexdigest()
    bounded_event_ns = int((START + timedelta(seconds=1)).timestamp() * 1e9)
    bounded_sink_ns = int((START + timedelta(seconds=2)).timestamp() * 1e9)
    bounded_raw = {
        "schema": "nfs4_read_v2",
        "local_id": bounded_event_id,
        "producer_epoch": EPOCH,
        "producer_sequence": "1",
        "event_time_ns": str(bounded_event_ns),
        "sink_time_ns": str(bounded_sink_ns),
        "target_created_ns": "1",
        "volume_id": bounded_operation.volume_id,
        "file_id": bounded_operation.file_id,
        "identity_valid": True,
        "export_name_b64": base64.urlsafe_b64encode(b"/controlled")
        .rstrip(b"=")
        .decode(),
        "export_id": bounded_operation.export_id,
        "target_fh": "AA",
        "operation_id": bounded_server_operation,
        "client_ref": "c" * 64,
        "session_ref": "",
        "compound_op_index": 0,
        "slot_id": 0,
        "sequence_id": 1,
        "compound_tag_b64": "",
        "compound_tag_original_length": 0,
        "compound_tag_truncated": False,
        "compound_tag_id": hashlib.sha256(b"").hexdigest(),
        "origin_claim": "unknown",
        "origin_trust": "client_asserted",
        "requested_offset": str(bounded_operation.offset),
        "requested_count": bounded_operation.count,
        "returned_bytes": bounded_operation.count,
        "nfs_status": 0,
        "outcome": "success",
        "eof": False,
        "file_size_valid": True,
        "file_size": str(bounded_operation.file_size),
    }
    bounded_raw_line = json.dumps(
        bounded_raw, sort_keys=True, separators=(",", ":")
    ).encode()
    bounded_raw_relative = Path("collector") / POD_UID / EPOCH / "events.jsonl"
    bounded_raw_path = bounded_harness / bounded_raw_relative
    bounded_raw_path.parent.mkdir(parents=True, mode=0o700)
    bounded_raw_path.write_bytes(bounded_raw_line + b"\n")
    bounded_raw_path.chmod(0o600)
    bounded_raw_hash = sha256(bounded_raw_path)
    bounded_durable_at = START + timedelta(seconds=60)
    bounded_durable = CollectorDurableEvent(
        run_id="bounded-validation",
        event_id=bounded_event_id,
        pod_name="collector-0",
        pod_uid=POD_UID,
        node_name="node-1",
        producer_epoch=EPOCH,
        producer_sequence=1,
        server_operation_id=bounded_server_operation,
        sink_time_ns=bounded_sink_ns,
        volume_id=bounded_operation.volume_id,
        file_id=bounded_operation.file_id,
        export_id=bounded_operation.export_id,
        export_name="/controlled",
        offset=bounded_operation.offset,
        count=bounded_operation.count,
        returned_bytes=bounded_operation.count,
        nfs_status=0,
        outcome="success",
        eof=False,
        durable_at=bounded_durable_at,
        tag="",
        raw_jsonl_path=bounded_raw_relative.as_posix(),
        raw_jsonl_sha256=bounded_raw_hash,
        raw_line_number=1,
        raw_record_sha256=hashlib.sha256(bounded_raw_line).hexdigest(),
    )
    write_jsonl(bounded_harness / "durable-events.jsonl", [bounded_durable])
    bounded_observed = ObservedEvent(
        operation_id=bounded_operation.operation_id,
        event_id=bounded_event_id,
        pod_uid=POD_UID,
        producer_epoch=EPOCH,
        producer_sequence=1,
        server_operation_id=bounded_server_operation,
        sink_time_ns=bounded_sink_ns,
        volume_id=bounded_operation.volume_id,
        file_id=bounded_operation.file_id,
        offset=bounded_operation.offset,
        count=bounded_operation.count,
        durable_at=bounded_durable_at,
        status="durable",
        tag=None,
    )
    write_jsonl(bounded_harness / "observed.jsonl", [bounded_observed])
    atomic_write_model(
        bounded_harness / "collector-file-manifest.json",
        CollectorFileManifest(
            run_id="bounded-validation",
            files=(
                CollectorSourceFile(
                    raw_jsonl_path=bounded_raw_relative.as_posix(),
                    sha256=bounded_raw_hash,
                    pod_name="collector-0",
                    pod_uid=POD_UID,
                    node_name="node-1",
                    producer_epoch=EPOCH,
                ),
            ),
        ),
    )
    bounded_active = JsonlEvidence(
        path=bounded_raw_relative.as_posix(),
        size_bytes=bounded_raw_path.stat().st_size,
        sha256=bounded_raw_hash,
    )
    write_jsonl(
        bounded_harness / "checkpoints.jsonl",
        [
            Checkpoint(
                run_id="bounded-validation",
                scheduled_at=START,
                captured_at=START,
                pod_name="collector-0",
                pod_uid=POD_UID,
                node_name="node-1",
                producer_epoch=EPOCH,
                restart_count=0,
                last_enqueued_sequence=0,
                last_synced_sequence=0,
                synced_count=0,
                queue_depth=0,
                worker_in_flight=0,
                delivery_clean=1,
                barrier_ready=1,
                drops=DropCounters(
                    queue=0, encode=0, sink=0, rollback=0, rotation=0
                ),
                active_jsonl=bounded_active,
                collector_sink_healthy=True,
                storage_healthy=True,
                cluster_healthy=True,
                volume_healthy=True,
                export_healthy=True,
                parse_errors=0,
                identity_mismatches=0,
                checksum_mismatches=0,
                foreground_read_errors=0,
                max_observed_latency_ms=10,
            )
        ],
    )
    write_jsonl(
        bounded_harness / "pre-read-health.jsonl",
        [
            PreReadHealth(
                run_id="bounded-validation",
                operation_id=bounded_operation.operation_id,
                captured_at=START + timedelta(milliseconds=500),
                pod_name="collector-0",
                pod_uid=POD_UID,
                node_name="node-1",
                producer_epoch=EPOCH,
                last_enqueued_sequence=0,
                last_synced_sequence=0,
                synced_count=0,
                queue_depth=0,
                worker_in_flight=0,
                delivery_clean=1,
                barrier_ready=1,
                drops=DropCounters(
                    queue=0, encode=0, sink=0, rollback=0, rotation=0
                ),
                collector_sink_healthy=True,
                storage_healthy=True,
                cluster_healthy=True,
                volume_healthy=True,
                export_healthy=True,
                parse_errors=0,
                identity_mismatches=0,
                checksum_mismatches=0,
                foreground_read_errors=0,
            )
        ],
    )
    atomic_write_model(
        bounded_harness / "reconciliation.json",
        ReconciliationReport(
            run_id="bounded-validation",
            manifest_sha256=sha256(
                bounded_harness / "candidate-manifest.json"
            ),
            ledger_sha256=sha256(bounded_harness / "offered.jsonl"),
            observed_sha256=sha256(bounded_harness / "observed.jsonl"),
            offered_count=1,
            unique_durable_count=1,
            duplicate_operation_ids=(),
            missing_operation_ids=(),
            unexpected_operation_ids=(),
            unknown_event_ids=(),
            exclusions={},
            pod_epochs={},
            success=True,
            failures=(),
        ),
    )
    atomic_write_model(
        bounded_provider / "provider-manifest.json",
        LiveProviderDeclaration(
            run_id="bounded-validation",
            manifest_sha256=sha256(
                bounded_harness / "candidate-manifest.json"
            ),
            provider_package_sha256=artifact_constants[
                "APPROVED_PROVIDER_PACKAGE_SHA256"
            ],
            provider_sha256=artifact_constants["APPROVED_PROVIDER_SHA256"],
            collector_sha256=artifact_constants["APPROVED_COLLECTOR_SHA256"],
            validator_sha256=artifact_constants["APPROVED_VALIDATOR_SHA256"],
            provider_snapshot_sha256=artifact_constants[
                "APPROVED_PROVIDER_SHA256"
            ],
            provider_package_artifact=CleanupEvidenceReference(
                root_id="bounded_harness_evidence",
                path=".approved-tools/provider-package.zip",
                sha256=sha256(
                    bounded_harness / ".approved-tools/provider-package.zip"
                ),
            ),
            collector_artifact=CleanupEvidenceReference(
                root_id="bounded_provider_evidence",
                path="collector.py",
                sha256=sha256(bounded_provider / "collector.py"),
            ),
            validator_artifact=CleanupEvidenceReference(
                root_id="bounded_provider_evidence",
                path="validator.py",
                sha256=sha256(bounded_provider / "validator.py"),
            ),
            invocation_record=CleanupEvidenceReference(
                root_id="bounded_harness_evidence",
                path="provider-invocation.json",
                sha256=sha256(
                    bounded_harness / "provider-invocation.json"
                ),
            ),
            interpreter_path="/usr/bin/python3.12",
            interpreter_sha256=(
                "f5e62d752212336f7eaa112499e41bdd58f655d3cf152c7d3c7cc1e5a01545a0"
            ),
            argv_contract=(
                "/usr/bin/python3.12",
                "-I",
                "-S",
                "-c",
                ISOLATED_PROVIDER_BOOTSTRAP,
                ".approved-tools/provider-package.zip",
                ".approved-tools/checkpoint-provider",
                "<action>",
                "<run_dir>",
            ),
            operation_count=1,
            event_count=1,
            content_sha256_by_operation={
                bounded_operation.operation_id: bounded_execution.content_sha256
            },
            source_manifest=CleanupEvidenceReference(
                root_id="bounded_harness_evidence",
                path="collector-file-manifest.json",
                sha256=sha256(
                    bounded_harness / "collector-file-manifest.json"
                ),
            ),
            fixture_catalog=CleanupEvidenceReference(
                root_id="bounded_harness_evidence",
                path="fixture-catalog.json",
                sha256=sha256(bounded_harness / "fixture-catalog.json"),
            ),
            source_chunks=(
                CleanupEvidenceReference(
                    root_id="bounded_harness_evidence",
                    path=bounded_raw_relative.as_posix(),
                    sha256=bounded_raw_hash,
                ),
            ),
            durable_sidecar=CleanupEvidenceReference(
                root_id="bounded_harness_evidence",
                path="durable-events.jsonl",
                sha256=sha256(bounded_harness / "durable-events.jsonl"),
            ),
            checkpoints=CleanupEvidenceReference(
                root_id="bounded_harness_evidence",
                path="checkpoints.jsonl",
                sha256=sha256(bounded_harness / "checkpoints.jsonl"),
            ),
            pre_read_health=CleanupEvidenceReference(
                root_id="bounded_harness_evidence",
                path="pre-read-health.jsonl",
                sha256=sha256(bounded_harness / "pre-read-health.jsonl"),
            ),
            execution_ledger=CleanupEvidenceReference(
                root_id="bounded_harness_evidence",
                path="execution.jsonl",
                sha256=sha256(bounded_harness / "execution.jsonl"),
            ),
            offered_ledger=CleanupEvidenceReference(
                root_id="bounded_harness_evidence",
                path="offered.jsonl",
                sha256=sha256(bounded_harness / "offered.jsonl"),
            ),
            observed=CleanupEvidenceReference(
                root_id="bounded_harness_evidence",
                path="observed.jsonl",
                sha256=sha256(bounded_harness / "observed.jsonl"),
            ),
            reconciliation=CleanupEvidenceReference(
                root_id="bounded_harness_evidence",
                path="reconciliation.json",
                sha256=sha256(bounded_harness / "reconciliation.json"),
            ),
            scope="live_collection",
        ),
    )
    bounded_adapter = evidence_root(
        "bounded-adapter",
        {
            (
                "events/"
                f"volume_id={candidate_manifest['volume_id']}/"
                "date=2026-11-04/part-00000.jsonl"
            ): json.dumps(
                {
                    "schema_version": 1,
                    "event_id": bounded_event_id,
                    "event_time": (
                        START + timedelta(seconds=1)
                    ).isoformat(),
                    "ingest_time": bounded_durable_at.isoformat(),
                    "volume_id": bounded_operation.volume_id,
                    "file_id": bounded_operation.file_id,
                    "dir_id": bounded_operation.dir_id,
                    "operation": "read",
                    "handle_id": None,
                    "offset": bounded_operation.offset,
                    "bytes_requested": bounded_operation.count,
                    "bytes_read": bounded_operation.count,
                    "handle_bytes_read": None,
                    "handle_duration_ms": None,
                    "file_size": bounded_operation.file_size,
                    "result": "ok",
                    "origin": "synthetic",
                    "session_id": None,
                    "cohort_id": None,
                    "client_id": bounded_operation.client_id,
                    "app": "nfs4",
                    "cache_outcome": None,
                    "latency_ms": None,
                }
            )
            + "\n",
            "loss/loss.jsonl": json.dumps(
                {
                    "volume_id": candidate_manifest["volume_id"],
                    "hour_start": START.isoformat(),
                    "offered": 1,
                    "emitted": 1,
                    "dropped": 0,
                    "sampled_out": 0,
                    "duplicates": 0,
                    "late": 0,
                }
            )
            + "\n",
            "report/summary.md": "bounded import\n",
        },
    )
    bounded_input_hashes = {
        "event_sources": sha256(
            bounded_harness / "collector-file-manifest.json"
        ),
        "observed": sha256(bounded_harness / "observed.jsonl"),
        "run_manifest": sha256(bounded_harness / "candidate-manifest.json"),
        "offered_ledger": sha256(bounded_harness / "offered.jsonl"),
        "fixture_catalog": sha256(bounded_harness / "fixture-catalog.json"),
        "checkpoints": sha256(bounded_harness / "checkpoints.jsonl"),
        "validation_permit": sha256(
            bounded_harness / "adapter-validation-permit.json"
        ),
    }
    bounded_report = {
        "schema": "chimera_import_report_v2",
        "run_id": "bounded-validation",
        "counts": {
            "event_sources": 1,
            "raw_events": 1,
            "durable_events": 1,
            "approved_exclusions": 0,
            "offered_operations": 1,
        },
        "event_quality": {},
        "approved_exclusions": [],
        "known_gaps": [],
        "input_sha256": bounded_input_hashes,
        "event_source_sha256": {"collector/chunk.jsonl": "5" * 64},
    }
    bounded_report_path = bounded_adapter / "report/import.json"
    bounded_report_path.write_text(json.dumps(bounded_report) + "\n")
    bounded_report_path.chmod(0o600)
    bounded_files = [
        {
            "path": path.relative_to(bounded_adapter).as_posix(),
            "sha256": sha256(path),
            "rows": (
                len(path.read_text().splitlines())
                if path.suffix == ".jsonl"
                else None
            ),
        }
        for path in sorted(bounded_adapter.rglob("*"))
        if path.is_file()
    ]
    bounded_manifest = {
        "manifest_version": 1,
        "dataset_id": "bounded-adapter-dataset",
        "data_kind": "synthetic",
        "generator": {
            "name": "pcache-datagen",
            "version": "0.1.0",
            "git_sha": "test",
            "runtime": {
                "adapter": "pcache-chimera-import",
                "adapter_version": "0.1.0",
                "python": "3.12",
                "run_id": "bounded-validation",
                "chimera_image_sha256": candidate_manifest["software"][
                    "chimera_image_sha256"
                ],
                "event_schema_sha256": candidate_manifest["event_schema_sha256"],
                **{
                    f"{name}_sha256": digest
                    for name, digest in bounded_input_hashes.items()
                },
            },
        },
        "event_schema_version": 1,
        "seed": 0,
        "preset": "custom",
        "config_sha256": "c" * 64,
        "period": {
            "start": candidate_manifest["start_time"],
            "end": candidate_manifest["end_time"],
        },
        "volumes": [candidate_manifest["volume_id"]],
        "counts": {
            "events": 1,
            "dropped_events": 0,
            "files": 60,
            "actors": 0,
            "patterns": 0,
            "by_operation": {"read": 1},
            "by_origin": {"synthetic": 1},
            "by_fault": {},
        },
        "known_gaps": [],
        "files": bounded_files,
        "created_at": candidate_manifest["end_time"],
        "limitations": ["synthetic"],
    }
    bounded_manifest_path = bounded_adapter / "manifest.json"
    bounded_manifest_path.write_text(json.dumps(bounded_manifest) + "\n")
    bounded_manifest_path.chmod(0o600)
    bounded_clean = evidence_root(
        "bounded-clean",
        {
            (
                "events_clean/"
                f"volume_id={candidate_manifest['volume_id']}/"
                "date=2026-11-04/part-00000.jsonl"
            ): "",
            (
                "excluded/"
                f"volume_id={candidate_manifest['volume_id']}/"
                "date=2026-11-04/part-00000.jsonl"
            ): json.dumps(
                {
                    **json.loads(
                        (
                            bounded_adapter
                            / "events"
                            / f"volume_id={candidate_manifest['volume_id']}"
                            / "date=2026-11-04"
                            / "part-00000.jsonl"
                        ).read_text()
                    ),
                    "reason": "origin_synthetic",
                    "detail": None,
                }
            )
            + "\n",
            "validation.json": '{"status":"passed"}\n',
        },
    )
    bounded_coverage_dir = bounded_clean / "coverage"
    bounded_coverage_dir.mkdir(mode=0o700)
    bounded_coverage_path = bounded_coverage_dir / "coverage.jsonl"
    bounded_coverage_rows = [
        {
            "volume_id": candidate_manifest["volume_id"],
            "hour_start": START.isoformat(),
            "offered": 1,
            "emitted": 1,
            "dropped": 0,
            "sampled_out": 0,
            "loss_fraction": 0.0,
            "status": "complete",
        }
    ]
    bounded_coverage_path.write_text(
        "".join(json.dumps(row) + "\n" for row in bounded_coverage_rows)
    )
    bounded_coverage_path.chmod(0o600)
    bounded_counts = {
        "input_events": 1,
        "kept_events": 0,
        "excluded_by_reason": {"origin_synthetic": 1},
        "open_ranges": 0,
        "opens": 0,
        "meta_events": 0,
        "catalog_miss_events": 0,
        "catalog_miss_files": 0,
        "late_reads_after_completion": 0,
        "opens_open_missing": 0,
        "opens_close_missing": 0,
        "opens_bytes_inconsistent": 0,
        "unexpected_reads_in_summary_mode": 0,
    }
    bounded_report_dir = bounded_clean / "report"
    bounded_report_dir.mkdir(mode=0o700)
    bounded_quality_path = bounded_report_dir / "quality.json"
    bounded_quality = {
                "counts": bounded_counts,
                "by_volume_day": [
                    {
                        "volume_id": candidate_manifest["volume_id"],
                        "date": "2026-11-04",
                        "input": 1,
                        "kept": 0,
                        "excluded": 1,
                    }
                ],
                "excluded_by_reason": {"origin_synthetic": 1},
                "exclusions_by_volume_day_reason": [
                    {
                        "volume_id": candidate_manifest["volume_id"],
                        "date": "2026-11-04",
                        "reason": "origin_synthetic",
                        "count": 1,
                    }
                ],
                "duplicate_rate": 0.0,
                "late_arrival_s": {
                    "p50": None,
                    "p90": None,
                    "p99": None,
                    "max": None,
                    "negative_count": 0,
                },
                "coverage_hours": {"complete": 1},
                "incomplete_windows": [],
                "opens": {
                    "count": 0,
                    "open_missing": 0,
                    "close_missing": 0,
                    "bytes_inconsistent": 0,
                    "consumed_share": 0.0,
                },
                "successful_returned_bytes": 0,
                "distinct_bytes": 0,
                "catalog_misses": {"files": 0, "events": 0},
                "origin_mix": {"synthetic": 1},
                "app_mix": {"nfs4": 1},
                "opens_by_volume_hour": [],
                "opens_by_volume_weekday": [],
            }
    bounded_quality_path.write_text(
        json.dumps(bounded_quality) + "\n"
    )
    bounded_quality_path.chmod(0o600)
    bounded_config_sha, _ = add_clean_auxiliary(
        bounded_clean,
        candidate_manifest["volume_id"],
        "2026-11-04",
        bounded_quality,
    )
    bounded_clean_files = [
        {
            "path": path.relative_to(bounded_clean).as_posix(),
            "sha256": sha256(path),
            "rows": (
                len(path.read_text().splitlines())
                if path.suffix == ".jsonl"
                else None
            ),
        }
        for path in sorted(bounded_clean.rglob("*"))
        if path.is_file()
    ]
    bounded_clean_manifest = {
        "manifest_version": 1,
        "dataset_id": "bounded-clean-dataset",
        "data_kind": "synthetic",
        "stage": "clean",
        "parent": {
            "dataset_id": "bounded-adapter-dataset",
            "manifest_sha256": sha256(bounded_manifest_path),
        },
        "tool": {"name": "pcache-prep", "version": "0.1.0", "git_sha": "test"},
        "config_sha256": bounded_config_sha,
        "period": {
            "start": candidate_manifest["start_time"],
            "end": candidate_manifest["end_time"],
        },
        "counts": bounded_counts,
        "coverage": {
            "hours": 1,
            "complete": 1,
            "incomplete": 0,
            "no_counters": 0,
            "skew_margin_s": 600,
            "assume_complete_without_counters": False,
        },
        "files": bounded_clean_files,
        "created_at": (START + timedelta(minutes=30)).isoformat(),
    }
    bounded_clean_manifest_path = bounded_clean / "manifest.json"
    bounded_clean_manifest_path.write_text(
        json.dumps(bounded_clean_manifest) + "\n"
    )
    bounded_clean_manifest_path.chmod(0o600)
    live_provider = evidence_root(
        "live-provider",
        {
            "provider-package.json": "provider package\n",
            "collector.py": "collector\n",
            "validator.py": "validator\n",
            "provider-evidence.jsonl": '{"status":"passed"}\n',
        },
    )
    live_source_manifest = CollectorFileManifest.model_validate_json(
        (run_dir / "collector-file-manifest.json").read_text()
    )
    provider_declaration = LiveProviderDeclaration(
        run_id="controlled-demo",
        manifest_sha256=sha256(run_dir / "manifest.json"),
        provider_package_sha256=artifact_constants[
            "APPROVED_PROVIDER_PACKAGE_SHA256"
        ],
        provider_sha256=artifact_constants["APPROVED_PROVIDER_SHA256"],
        collector_sha256=artifact_constants["APPROVED_COLLECTOR_SHA256"],
        validator_sha256=artifact_constants["APPROVED_VALIDATOR_SHA256"],
        provider_snapshot_sha256=artifact_constants[
            "APPROVED_PROVIDER_SHA256"
        ],
        provider_package_artifact=CleanupEvidenceReference(
            root_id="live_harness_evidence",
            path=".approved-tools/provider-package.zip",
            sha256=sha256(
                run_dir / ".approved-tools/provider-package.zip"
            ),
        ),
        collector_artifact=CleanupEvidenceReference(
            root_id="live_provider_evidence",
            path="collector.py",
            sha256=sha256(live_provider / "collector.py"),
        ),
        validator_artifact=CleanupEvidenceReference(
            root_id="live_provider_evidence",
            path="validator.py",
            sha256=sha256(live_provider / "validator.py"),
        ),
        invocation_record=CleanupEvidenceReference(
            root_id="live_harness_evidence",
            path="provider-invocation.json",
            sha256=sha256(run_dir / "provider-invocation.json"),
        ),
        interpreter_path="/usr/bin/python3.12",
        interpreter_sha256=(
            "f5e62d752212336f7eaa112499e41bdd58f655d3cf152c7d3c7cc1e5a01545a0"
        ),
        argv_contract=(
            "/usr/bin/python3.12",
            "-I",
            "-S",
            "-c",
            ISOLATED_PROVIDER_BOOTSTRAP,
            ".approved-tools/provider-package.zip",
            ".approved-tools/checkpoint-provider",
            "<action>",
            "<run_dir>",
        ),
        operation_count=300,
        event_count=300,
        content_sha256_by_operation={
            row.operation_id: row.content_sha256 for row in execution_rows
        },
        source_manifest=CleanupEvidenceReference(
            root_id="live_harness_evidence",
            path="collector-file-manifest.json",
            sha256=sha256(run_dir / "collector-file-manifest.json"),
        ),
        fixture_catalog=CleanupEvidenceReference(
            root_id="live_harness_evidence",
            path="fixture-catalog.json",
            sha256=sha256(run_dir / "fixture-catalog.json"),
        ),
        source_chunks=tuple(
            CleanupEvidenceReference(
                root_id="live_harness_evidence",
                path=source.raw_jsonl_path,
                sha256=source.sha256,
            )
            for source in live_source_manifest.files
        ),
        durable_sidecar=CleanupEvidenceReference(
            root_id="live_harness_evidence",
            path="durable-events.jsonl",
            sha256=sha256(run_dir / "durable-events.jsonl"),
        ),
        checkpoints=CleanupEvidenceReference(
            root_id="live_harness_evidence",
            path="checkpoints.jsonl",
            sha256=sha256(run_dir / "checkpoints.jsonl"),
        ),
        pre_read_health=CleanupEvidenceReference(
            root_id="live_harness_evidence",
            path="pre-read-health.jsonl",
            sha256=sha256(run_dir / "pre-read-health.jsonl"),
        ),
        execution_ledger=CleanupEvidenceReference(
            root_id="live_harness_evidence",
            path="execution.jsonl",
            sha256=sha256(run_dir / "execution.jsonl"),
        ),
        offered_ledger=CleanupEvidenceReference(
            root_id="live_harness_evidence",
            path="offered.jsonl",
            sha256=sha256(run_dir / "offered.jsonl"),
        ),
        observed=CleanupEvidenceReference(
            root_id="live_harness_evidence",
            path="observed.jsonl",
            sha256=sha256(run_dir / "observed.jsonl"),
        ),
        reconciliation=CleanupEvidenceReference(
            root_id="live_harness_evidence",
            path="reconciliation.json",
            sha256=sha256(run_dir / "reconciliation.json"),
        ),
        scope="live_collection",
    )
    atomic_write_model(
        live_provider / "provider-manifest.json",
        provider_declaration,
    )
    live_adapter_event_rows = [
        {
            "schema_version": 1,
            "event_id": observation.event_id,
            "event_time": (
                operation.offered_at_utc + timedelta(milliseconds=10)
            ).isoformat(),
            "ingest_time": observation.durable_at.isoformat(),
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
            "session_id": None,
            "cohort_id": None,
            "client_id": operation.client_id,
            "app": "nfs4",
            "cache_outcome": None,
            "latency_ms": None,
        }
        for operation, observation, sidecar in zip(
            offered_rows,
            observed_rows,
            durable_rows,
            strict=True,
        )
    ]
    live_adapter = evidence_root(
        "live-adapter",
        {
            (
                "events/"
                f"volume_id={live_manifest['volume_id']}/"
                "date=2026-11-04/part-00000.jsonl"
            ): "".join(
                json.dumps(row) + "\n" for row in live_adapter_event_rows
            ),
            "loss/loss.jsonl": "".join(
                json.dumps(
                    {
                        "volume_id": live_manifest["volume_id"],
                        "hour_start": (
                            START + timedelta(hours=index)
                        ).isoformat(),
                        "offered": 60,
                        "emitted": 60,
                        "dropped": 0,
                        "sampled_out": 0,
                        "duplicates": 0,
                        "late": 0,
                    }
                )
                + "\n"
                for index in range(5)
            ),
            "report/summary.md": "live import\n",
        },
    )
    adapter_report = {
        "schema": "chimera_import_report_v2",
        "run_id": "controlled-demo",
        "counts": {
            "event_sources": 1,
            "raw_events": 300,
            "durable_events": 300,
            "approved_exclusions": 0,
            "offered_operations": 300,
        },
        "input_sha256": {
            "run_manifest": sha256(run_dir / "manifest.json"),
            "offered_ledger": sha256(run_dir / "offered.jsonl"),
            "observed": sha256(run_dir / "observed.jsonl"),
            "event_sources": sha256(run_dir / "collector-file-manifest.json"),
            "checkpoints": sha256(run_dir / "checkpoints.jsonl"),
            "fixture_catalog": sha256(run_dir / "fixture-catalog.json"),
        },
        "event_source_sha256": {
            source.raw_jsonl_path: source.sha256
            for source in live_source_manifest.files
        },
        "event_quality": {},
        "approved_exclusions": [],
        "known_gaps": [],
    }
    adapter_report_path = live_adapter / "report/import.json"
    adapter_report_path.write_text(json.dumps(adapter_report) + "\n")
    adapter_report_path.chmod(0o600)
    adapter_files = [
        {
            "path": path.relative_to(live_adapter).as_posix(),
            "sha256": sha256(path),
            "rows": (
                len(path.read_text().splitlines())
                if path.suffix == ".jsonl"
                else None
            ),
        }
        for path in sorted(live_adapter.rglob("*"))
        if path.is_file()
    ]
    adapter_manifest = {
        "manifest_version": 1,
        "dataset_id": "live-adapter-dataset",
        "data_kind": "synthetic",
        "generator": {
            "name": "pcache-datagen",
            "version": "0.1.0",
            "git_sha": "test",
            "runtime": {
                "adapter": "pcache-chimera-import",
                "adapter_version": "0.1.0",
                "python": "3.12",
                "run_id": "controlled-demo",
                "chimera_image_sha256": live_manifest["software"][
                    "chimera_image_sha256"
                ],
                "event_schema_sha256": live_manifest["event_schema_sha256"],
                **{
                    f"{name}_sha256": digest
                    for name, digest in adapter_report["input_sha256"].items()
                },
            }
        },
        "event_schema_version": 1,
        "seed": 0,
        "preset": "custom",
        "config_sha256": "a" * 64,
        "period": {
            "start": live_manifest["start_time"],
            "end": live_manifest["end_time"],
        },
        "volumes": [live_manifest["volume_id"]],
        "counts": {
            "events": 300,
            "dropped_events": 0,
            "files": 60,
            "actors": 0,
            "patterns": 0,
            "by_operation": {"read": 300},
            "by_origin": {"synthetic": 300},
            "by_fault": {},
        },
        "known_gaps": [],
        "files": adapter_files,
        "created_at": live_manifest["end_time"],
        "limitations": ["synthetic"],
    }
    adapter_manifest_path = live_adapter / "manifest.json"
    adapter_manifest_path.write_text(json.dumps(adapter_manifest) + "\n")
    adapter_manifest_path.chmod(0o600)
    coverage_dir = dataset_root / "coverage"
    coverage_dir.mkdir(mode=0o700)
    coverage_path = coverage_dir / "coverage.jsonl"
    coverage_rows = [
        {
            "volume_id": live_manifest["volume_id"],
            "hour_start": (START + timedelta(hours=index)).isoformat(),
            "offered": 60,
            "emitted": 60,
            "dropped": 0,
            "sampled_out": 0,
            "loss_fraction": 0.0,
            "status": "complete",
        }
        for index in range(5)
    ]
    coverage_path.write_text(
        "".join(json.dumps(row) + "\n" for row in coverage_rows)
    )
    coverage_path.chmod(0o600)
    excluded_path = (
        dataset_root
        / "excluded"
        / f"volume_id={live_manifest['volume_id']}"
        / "date=2026-11-04"
        / "part-00000.jsonl"
    )
    excluded_path.parent.mkdir(parents=True, mode=0o700)
    excluded_path.write_text(
        "".join(
            json.dumps(
                {
                    **event,
                    "reason": "origin_synthetic",
                    "detail": None,
                }
            )
            + "\n"
            for event in live_adapter_event_rows
        )
    )
    excluded_path.chmod(0o600)
    counts = {
        "input_events": 300,
        "kept_events": 0,
        "excluded_by_reason": {"origin_synthetic": 300},
        "open_ranges": 0,
        "opens": 0,
        "meta_events": 0,
        "catalog_miss_events": 0,
        "catalog_miss_files": 0,
        "late_reads_after_completion": 0,
        "opens_open_missing": 0,
        "opens_close_missing": 0,
        "opens_bytes_inconsistent": 0,
        "unexpected_reads_in_summary_mode": 0,
    }
    clean_report = {
        "counts": counts,
        "by_volume_day": [
            {
                "volume_id": live_manifest["volume_id"],
                "date": "2026-11-04",
                "input": 300,
                "kept": 0,
                "excluded": 300,
            }
        ],
        "excluded_by_reason": {"origin_synthetic": 300},
        "exclusions_by_volume_day_reason": [
            {
                "volume_id": live_manifest["volume_id"],
                "date": "2026-11-04",
                "reason": "origin_synthetic",
                "count": 300,
            }
        ],
        "duplicate_rate": 0.0,
        "late_arrival_s": {
            "p50": None,
            "p90": None,
            "p99": None,
            "max": None,
            "negative_count": 0,
        },
        "coverage_hours": {"complete": 5},
        "incomplete_windows": [],
        "opens": {
            "count": 0,
            "open_missing": 0,
            "close_missing": 0,
            "bytes_inconsistent": 0,
            "consumed_share": 0.0,
        },
        "successful_returned_bytes": 0,
        "distinct_bytes": 0,
        "catalog_misses": {"files": 0, "events": 0},
        "origin_mix": {"synthetic": 300},
        "app_mix": {"nfs4": 300},
        "opens_by_volume_hour": [],
        "opens_by_volume_weekday": [],
    }
    report_dir = dataset_root / "report"
    report_dir.mkdir(mode=0o700)
    clean_report_path = report_dir / "quality.json"
    clean_report_path.write_text(json.dumps(clean_report) + "\n")
    clean_report_path.chmod(0o600)
    live_config_sha, live_auxiliary = add_clean_auxiliary(
        dataset_root,
        live_manifest["volume_id"],
        "2026-11-04",
        clean_report,
    )
    clean_files = [
        {
            "path": output.relative_to(dataset_root).as_posix(),
            "sha256": sha256(output),
            "rows": 0,
        },
        {
            "path": excluded_path.relative_to(dataset_root).as_posix(),
            "sha256": sha256(excluded_path),
            "rows": 300,
        },
        {"path": "validation.json", "sha256": sha256(provenance), "rows": None},
        {
            "path": "coverage/coverage.jsonl",
            "sha256": sha256(coverage_path),
            "rows": 5,
        },
        {
            "path": "report/quality.json",
            "sha256": sha256(clean_report_path),
            "rows": None,
        },
        *[
            {
                "path": path.relative_to(dataset_root).as_posix(),
                "sha256": sha256(path),
                "rows": (
                    len(path.read_text().splitlines())
                    if path.suffix == ".jsonl"
                    else None
                ),
            }
            for path in live_auxiliary
        ],
    ]
    clean_manifest.write_text(
        json.dumps(
            {
                "manifest_version": 1,
                "dataset_id": "live-clean-dataset",
                "data_kind": "synthetic",
                "stage": "clean",
                "parent": {
                    "dataset_id": "live-adapter-dataset",
                    "manifest_sha256": sha256(adapter_manifest_path),
                },
                "tool": {
                    "name": "pcache-prep",
                    "version": "0.1.0",
                    "git_sha": "test",
                },
                "config_sha256": live_config_sha,
                "period": {
                    "start": live_manifest["start_time"],
                    "end": live_manifest["end_time"],
                },
                "counts": counts,
                "coverage": {
                    "hours": 5,
                    "complete": 5,
                    "incomplete": 0,
                    "no_counters": 0,
                    "skew_margin_s": 600,
                    "assume_complete_without_counters": False,
                },
                "files": clean_files,
                "created_at": live_manifest["end_time"],
            }
        )
        + "\n"
    )
    clean_manifest.chmod(0o600)
    imported_hash = sha256(bounded_adapter / "manifest.json")
    cleaned_hash = sha256(bounded_clean / "manifest.json")
    monkeypatch.setattr(
        "predictive_cache.collection.cli.APPROVED_IMPORTED_MANIFEST_SHA256",
        imported_hash,
    )
    monkeypatch.setattr(
        "predictive_cache.collection.cli.APPROVED_CLEANED_MANIFEST_SHA256",
        cleaned_hash,
    )
    for root in (bounded_harness, bounded_provider, bounded_adapter, bounded_clean):
        for directory in (path for path in root.rglob("*") if path.is_dir()):
            directory.chmod(0o700)
    bounded_roots = (
        _cleanup_root(
            "bounded_harness_evidence",
            "bounded_harness_evidence",
            bounded_harness,
        ),
        _cleanup_root(
            "bounded_provider_evidence",
            "bounded_provider_evidence",
            bounded_provider,
        ),
        _cleanup_root(
            "bounded_adapter_dataset",
            "bounded_adapter_dataset",
            bounded_adapter,
        ),
        _cleanup_root(
            "bounded_clean_dataset",
            "bounded_clean_dataset",
            bounded_clean,
        ),
    )
    attestation = BoundedValidationAttestation(
        validation_run_id="bounded-validation",
        full_live_candidate_manifest=CleanupEvidenceReference(
            root_id="bounded_harness_evidence",
            path="candidate-manifest.json",
            sha256=sha256(bounded_harness / "candidate-manifest.json"),
        ),
        permit_id=permit.permit_id,
        permit=CleanupEvidenceReference(
            root_id="bounded_harness_evidence",
            path="adapter-validation-permit.json",
            sha256=sha256(bounded_harness / "adapter-validation-permit.json"),
        ),
        offered_ledger=CleanupEvidenceReference(
            root_id="bounded_harness_evidence",
            path="offered.jsonl",
            sha256=sha256(bounded_harness / "offered.jsonl"),
        ),
        execution_ledger=CleanupEvidenceReference(
            root_id="bounded_harness_evidence",
            path="execution.jsonl",
            sha256=sha256(bounded_harness / "execution.jsonl"),
        ),
        observed=CleanupEvidenceReference(
            root_id="bounded_harness_evidence",
            path="observed.jsonl",
            sha256=sha256(bounded_harness / "observed.jsonl"),
        ),
        source_manifest=CleanupEvidenceReference(
            root_id="bounded_harness_evidence",
            path="collector-file-manifest.json",
            sha256=sha256(bounded_harness / "collector-file-manifest.json"),
        ),
        checkpoints=CleanupEvidenceReference(
            root_id="bounded_harness_evidence",
            path="checkpoints.jsonl",
            sha256=sha256(bounded_harness / "checkpoints.jsonl"),
        ),
        durable_sidecar=CleanupEvidenceReference(
            root_id="bounded_harness_evidence",
            path="durable-events.jsonl",
            sha256=sha256(bounded_harness / "durable-events.jsonl"),
        ),
        pre_read_health=CleanupEvidenceReference(
            root_id="bounded_harness_evidence",
            path="pre-read-health.jsonl",
            sha256=sha256(bounded_harness / "pre-read-health.jsonl"),
        ),
        provider_manifest=CleanupEvidenceReference(
            root_id="bounded_provider_evidence",
            path="provider-manifest.json",
            sha256=sha256(bounded_provider / "provider-manifest.json"),
        ),
        validation_operations=1,
        operation_ids=permit.operation_ids,
        validated_code_head_sha256=APPROVED_VALIDATED_CODE_HEAD_SHA256,
        validated_merge_commit_sha256=APPROVED_VALIDATED_MERGE_COMMIT_SHA256,
        validated_tree_sha256=APPROVED_VALIDATED_TREE_SHA256,
        executed_source_archive=CleanupEvidenceReference(
            root_id="bounded_harness_evidence",
            path="source-archive.tar.gz",
            sha256=sha256(bounded_harness / "source-archive.tar.gz"),
        ),
        provider_package=CleanupEvidenceReference(
            root_id="bounded_provider_evidence",
            path="provider-package.json",
            sha256=sha256(bounded_provider / "provider-package.json"),
        ),
        provider=CleanupEvidenceReference(
            root_id="bounded_provider_evidence",
            path="provider.py",
            sha256=sha256(bounded_provider / "provider.py"),
        ),
        collector=CleanupEvidenceReference(
            root_id="bounded_provider_evidence",
            path="collector.py",
            sha256=sha256(bounded_provider / "collector.py"),
        ),
        validator=CleanupEvidenceReference(
            root_id="bounded_provider_evidence",
            path="validator.py",
            sha256=sha256(bounded_provider / "validator.py"),
        ),
        runbook=CleanupEvidenceReference(
            root_id="bounded_provider_evidence",
            path="runbook.md",
            sha256=sha256(bounded_provider / "runbook.md"),
        ),
        adapter_wrapper=CleanupEvidenceReference(
            root_id="bounded_harness_evidence",
            path="adapter-wrapper",
            sha256=sha256(bounded_harness / "adapter-wrapper"),
        ),
        cleaner_wrapper=CleanupEvidenceReference(
            root_id="bounded_harness_evidence",
            path="cleaner-wrapper",
            sha256=sha256(bounded_harness / "cleaner-wrapper"),
        ),
        cluster_provenance=CleanupEvidenceReference(
            root_id="bounded_harness_evidence",
            path="cluster-provenance-v1.json",
            sha256=sha256(bounded_harness / "cluster-provenance-v1.json"),
        ),
        event_schema=CleanupEvidenceReference(
            root_id="bounded_harness_evidence",
            path="event-schema-v2.json",
            sha256=sha256(bounded_harness / "event-schema-v2.json"),
        ),
        budget_evidence=CleanupEvidenceReference(
            root_id="bounded_harness_evidence",
            path="budget-evidence-v1.json",
            sha256=sha256(bounded_harness / "budget-evidence-v1.json"),
        ),
        fixture_catalog=CleanupEvidenceReference(
            root_id="bounded_harness_evidence",
            path="fixture-catalog.json",
            sha256=sha256(bounded_harness / "fixture-catalog.json"),
        ),
        imported_manifest=CleanupEvidenceReference(
            root_id="bounded_adapter_dataset",
            path="manifest.json",
            sha256=imported_hash,
        ),
        imported_report=CleanupEvidenceReference(
            root_id="bounded_adapter_dataset",
            path="report/import.json",
            sha256=sha256(bounded_adapter / "report/import.json"),
        ),
        cleaned_manifest=CleanupEvidenceReference(
            root_id="bounded_clean_dataset",
            path="manifest.json",
            sha256=cleaned_hash,
        ),
        cleaned_report=CleanupEvidenceReference(
            root_id="bounded_clean_dataset",
            path="report/quality.json",
            sha256=sha256(bounded_clean / "report/quality.json"),
        ),
        started_at=START,
        completed_at=START + timedelta(minutes=30),
        scope="bounded_validation",
        data_kind="synthetic",
        representative_production_demand=False,
        status="passed",
        cleanup_roots=bounded_roots,
    )
    finalization_root = tmp_path / "finalization-control"
    finalization_root.mkdir(mode=0o700)
    attestation_path = finalization_root / "validation-attestation.json"
    atomic_write_model(attestation_path, attestation)

    for root in (run_dir, live_provider, live_adapter, dataset_root):
        for directory in (path for path in root.rglob("*") if path.is_dir()):
            directory.chmod(0o700)
    live_roots = (
        _cleanup_root(
            "live_harness_evidence",
            "live_harness_evidence",
            run_dir,
        ),
        _cleanup_root(
            "live_provider_evidence",
            "live_provider_evidence",
            live_provider,
        ),
        _cleanup_root(
            "live_adapter_dataset",
            "live_adapter_dataset",
            live_adapter,
        ),
        _cleanup_root(
            "live_clean_dataset",
            "live_clean_dataset",
            dataset_root,
        ),
    )
    live_declaration = LiveFinalizationDeclaration(
        run_id="controlled-demo",
        manifest_sha256=sha256(run_dir / "manifest.json"),
        reconciliation_sha256=sha256(run_dir / "reconciliation.json"),
        validated_code_head_sha256=APPROVED_VALIDATED_CODE_HEAD_SHA256,
        validated_merge_commit_sha256=APPROVED_VALIDATED_MERGE_COMMIT_SHA256,
        validated_tree_sha256=APPROVED_VALIDATED_TREE_SHA256,
        executed_source_archive_sha256=artifact_constants[
            "APPROVED_EXECUTED_SOURCE_ARCHIVE_SHA256"
        ],
        provider_manifest=CleanupEvidenceReference(
            root_id="live_provider_evidence",
            path="provider-manifest.json",
            sha256=sha256(live_provider / "provider-manifest.json"),
        ),
        adapter_manifest=CleanupEvidenceReference(
            root_id="live_adapter_dataset",
            path="manifest.json",
            sha256=sha256(live_adapter / "manifest.json"),
        ),
        adapter_report=CleanupEvidenceReference(
            root_id="live_adapter_dataset",
            path="report/import.json",
            sha256=sha256(live_adapter / "report/import.json"),
        ),
        cleaned_manifest=CleanupEvidenceReference(
            root_id="live_clean_dataset",
            path="manifest.json",
            sha256=sha256(clean_manifest),
        ),
        cleaned_report=CleanupEvidenceReference(
            root_id="live_clean_dataset",
            path="report/quality.json",
            sha256=sha256(clean_report_path),
        ),
        cleanup_roots=live_roots,
    )
    report = tmp_path / "report.json"
    report.write_text(
        json.dumps(
            {
                "run_id": "controlled-demo",
                "status": "success",
                "output_hashes": {
                    output.relative_to(dataset_root).as_posix(): sha256(output),
                    excluded_path.relative_to(dataset_root).as_posix(): sha256(
                        excluded_path
                    ),
                    "manifest.json": sha256(clean_manifest),
                    "coverage/coverage.jsonl": sha256(coverage_path),
                    **{
                        path.relative_to(dataset_root).as_posix(): sha256(path)
                        for path in live_auxiliary
                    },
                },
                "validation_provenance": {
                    "validation.json": sha256(provenance),
                    "report/quality.json": sha256(clean_report_path),
                },
                "live_finalization": live_declaration.model_dump(mode="json"),
            }
        )
        + "\n"
    )
    report.chmod(0o600)
    roots = {
        "bounded_harness_evidence": bounded_harness,
        "bounded_provider_evidence": bounded_provider,
        "bounded_adapter_dataset": bounded_adapter,
        "bounded_clean_dataset": bounded_clean,
        "live_harness_evidence": run_dir,
        "live_provider_evidence": live_provider,
        "live_adapter_dataset": live_adapter,
        "live_clean_dataset": dataset_root,
    }
    return (
        run_dir,
        report,
        dataset_root,
        finalization_root,
        attestation_path,
        roots,
        output,
    )


def _finalize_args(
    run_dir,
    report,
    dataset_root,
    finalization_root,
    attestation_path,
    roots,
):
    arguments = [
        "finalize",
        "--run-dir",
        str(run_dir),
        "--dataset-report",
        str(report),
        "--dataset-root",
        str(dataset_root),
        "--finalization-root",
        str(finalization_root),
        "--bounded-validation-attestation",
        str(attestation_path),
    ]
    for kind, path in roots.items():
        arguments.extend(["--cleanup-root", f"{kind}={path}"])
    return arguments


def _historical_finalization_fixture(tmp_path, monkeypatch):
    (
        live_run,
        _report,
        live_clean,
        _generic_control,
        _bounded_attestation,
        generic_roots,
        _output,
    ) = _finalization_fixture(tmp_path / "generic", monkeypatch)
    (live_run / "provider-invocation.json").unlink()
    (live_run / ".approved-tools/checkpoint-provider").unlink()
    (live_run / ".approved-tools/provider-package.zip").unlink()
    (live_run / ".approved-tools").rmdir()
    history = tmp_path / "historical"
    history.mkdir(mode=0o700)

    def private_root(name):
        root = history / name
        root.mkdir(mode=0o700)
        return root

    validation_four = private_root("validation-four")
    shutil.copytree(
        generic_roots["bounded_harness_evidence"],
        validation_four,
        dirs_exist_ok=True,
    )
    shutil.copyfile(
        validation_four / "candidate-manifest.json",
        validation_four / "manifest.json",
    )
    (validation_four / "manifest.json").chmod(0o600)
    (validation_four / "approved-collection-permit.json").write_text(
        '{"status":"approved"}\n'
    )
    (validation_four / "approved-collection-permit.json").chmod(0o600)
    bounded_manifest = CollectionManifest.model_validate_json(
        (validation_four / "manifest.json").read_text()
    )
    attempt_ids = []
    roots = {}
    logs = []
    for attempt in range(1, 5):
        run_role = f"validation_{attempt}_run"
        input_role = f"validation_{attempt}_input"
        if attempt == 4:
            run_root = validation_four
            run_id = bounded_manifest.run_id
        else:
            run_root = private_root(run_role)
            run_id = f"historical-validation-{attempt}"
            manifest_value = bounded_manifest.model_copy(
                update={
                    "run_id": run_id,
                    "run_ref": hashlib.sha256(run_id.encode()).hexdigest()[:32],
                }
            )
            atomic_write_model(run_root / "manifest.json", manifest_value)
        input_root = private_root(input_role)
        log = input_root / "validation.log"
        log.write_text(f"attempt {attempt}\n")
        log.chmod(0o600)
        attempt_ids.append(run_id)
        logs.append(log)
        roots[run_role] = run_root
        roots[input_role] = input_root

    live_input = private_root("live-input")
    shutil.copyfile(live_run / "manifest.json", live_input / "manifest.json")
    (live_input / "manifest.json").chmod(0o600)
    (live_input / "live.log").write_text("collection passed\n")
    (live_input / "live.log").chmod(0o600)
    (live_run / "approved-collection-permit.json").write_text(
        '{"status":"approved"}\n'
    )
    (live_run / "approved-collection-permit.json").chmod(0o600)

    shared_provider = private_root("shared-provider")
    provider_source = generic_roots["bounded_provider_evidence"]
    provider_files = {
        "cluster2-provider-package.json": "provider-package.json",
        "cluster2-collection-provider.py": "provider.py",
        "cluster2-jsonl-collection.py": "collector.py",
        "cluster2-jsonl-validation.py": "validator.py",
        "cluster2-bounded-validation-runbook.md": "runbook.md",
    }
    for destination, source in provider_files.items():
        shutil.copyfile(provider_source / source, shared_provider / destination)
        (shared_provider / destination).chmod(0o600)
    importer_constants = {
        "APPROVED_PROVIDER_PACKAGE_SHA256": sha256(
            shared_provider / "cluster2-provider-package.json"
        ),
        "APPROVED_PROVIDER_SHA256": sha256(
            shared_provider / "cluster2-collection-provider.py"
        ),
        "APPROVED_COLLECTOR_SHA256": sha256(
            shared_provider / "cluster2-jsonl-collection.py"
        ),
        "APPROVED_VALIDATOR_SHA256": sha256(
            shared_provider / "cluster2-jsonl-validation.py"
        ),
        "APPROVED_VALIDATION_RUNBOOK_SHA256": sha256(
            shared_provider / "cluster2-bounded-validation-runbook.md"
        ),
    }
    for name, value in importer_constants.items():
        monkeypatch.setattr(
            f"predictive_cache.sources.chimera.importer.{name}",
            value,
        )
    executions = read_models(
        validation_four / "execution.jsonl",
        ExecutionResult,
    )
    import_dataset(
        event_sources_path=validation_four / "collector-file-manifest.json",
        observed_path=validation_four / "observed.jsonl",
        run_manifest_path=validation_four / "manifest.json",
        offered_ledger_path=validation_four / "offered.jsonl",
        fixture_catalog_path=validation_four / "fixture-catalog.json",
        checkpoints_path=validation_four / "checkpoints.jsonl",
        validation_permit_path=validation_four / "adapter-validation-permit.json",
        validation_permit_reference_time=max(
            row.completed_at_utc for row in executions
        ),
        out_dir=validation_four / "imported",
        format="jsonl",
    )
    run_clean(
        validation_four / "imported",
        CleanConfig(),
        validation_four / "cleaned",
        format="jsonl",
    )
    live_adapter = history / "live-adapter"
    live_clean_dataset = history / "live-clean"
    import_dataset(
        event_sources_path=live_run / "collector-file-manifest.json",
        observed_path=live_run / "observed.jsonl",
        run_manifest_path=live_run / "manifest.json",
        offered_ledger_path=live_run / "offered.jsonl",
        fixture_catalog_path=live_run / "fixture-catalog.json",
        checkpoints_path=live_run / "checkpoints.jsonl",
        out_dir=live_adapter,
        format="jsonl",
    )
    run_clean(
        live_adapter,
        CleanConfig(),
        live_clean_dataset,
        format="jsonl",
    )

    roots.update(
        {
            "live_run": live_run,
            "live_input": live_input,
            "shared_provider": shared_provider,
            "live_adapter_dataset": live_adapter,
            "live_clean_dataset": live_clean_dataset,
        }
    )
    for root in roots.values():
        root.chmod(0o700)
        for directory in (path for path in root.rglob("*") if path.is_dir()):
            directory.chmod(0o700)
        for file in (path for path in root.rglob("*") if path.is_file()):
            file.chmod(0o600)

    external = private_root("external")
    executed_archive = external / "executed-source.tar.gz"
    verification_archive = external / "verification-source.tar.gz"
    verification_repo = tmp_path / "verification-repo"
    verification_repo.mkdir(mode=0o700)
    subprocess.run(["git", "init", "-q", verification_repo], check=True)
    subprocess.run(
        ["git", "-C", verification_repo, "config", "user.name", "test"],
        check=True,
    )
    subprocess.run(
        [
            "git",
            "-C",
            verification_repo,
            "config",
            "user.email",
            "test@example.invalid",
        ],
        check=True,
    )
    (verification_repo / "source.txt").write_text("verification source\n")
    subprocess.run(
        ["git", "-C", verification_repo, "add", "source.txt"],
        check=True,
    )
    subprocess.run(
        ["git", "-C", verification_repo, "commit", "-qm", "verification"],
        check=True,
    )
    verification_commit = subprocess.check_output(
        ["git", "-C", verification_repo, "rev-parse", "HEAD"],
        text=True,
    ).strip()
    verification_tree = subprocess.check_output(
        ["git", "-C", verification_repo, "rev-parse", "HEAD^{tree}"],
        text=True,
    ).strip()
    adapter_wrapper = external / "adapter-wrapper"
    cleaner_wrapper = external / "cleaner-wrapper"
    executed_archive.write_text("executed source\n")
    with verification_archive.open("wb") as stream:
        subprocess.run(
            [
                "git",
                "-C",
                verification_repo,
                "archive",
                "--format=tar.gz",
                "--prefix=verification/",
                verification_commit,
            ],
            check=True,
            stdout=stream,
        )
    shutil.copyfile(
        generic_roots["bounded_harness_evidence"] / "adapter-wrapper",
        adapter_wrapper,
    )
    shutil.copyfile(
        generic_roots["bounded_harness_evidence"] / "cleaner-wrapper",
        cleaner_wrapper,
    )
    for file in external.iterdir():
        file.chmod(0o600)

    historical_roots = {
        role: _historical_root(role, path) for role, path in roots.items()
    }
    frozen_declaration = external / "frozen-root-declaration.json"
    atomic_write_model(
        frozen_declaration,
        HistoricalFrozenRootDeclaration(
            run_id="controlled-demo",
            run_manifest_sha256=sha256(live_run / "manifest.json"),
            roots=tuple(historical_roots.values()),
        ),
    )
    constants = {
        "HISTORICAL_RUN_ID": "controlled-demo",
        "HISTORICAL_RUN_MANIFEST_SHA256": sha256(live_run / "manifest.json"),
        "HISTORICAL_ROOT_DECLARATION_SHA256": sha256(frozen_declaration),
        "HISTORICAL_EXECUTED_COMMIT": "1" * 40,
        "HISTORICAL_EXECUTED_TREE": "2" * 40,
        "HISTORICAL_EXECUTED_ARCHIVE_SHA256": sha256(executed_archive),
        "HISTORICAL_INTEGRATION_COMMIT": "3" * 40,
        "HISTORICAL_INTEGRATION_TREE": "4" * 40,
        "HISTORICAL_LIVE_PERMIT_SHA256": sha256(
            live_run / "approved-collection-permit.json"
        ),
        "HISTORICAL_RECONCILIATION_SHA256": sha256(
            live_run / "reconciliation.json"
        ),
        "HISTORICAL_BOUNDED_MANIFEST_SHA256": sha256(
            validation_four / "manifest.json"
        ),
        "HISTORICAL_BOUNDED_COLLECTION_PERMIT_SHA256": sha256(
            validation_four / "approved-collection-permit.json"
        ),
        "HISTORICAL_BOUNDED_PERMIT_SHA256": sha256(
            validation_four / "adapter-validation-permit.json"
        ),
        "HISTORICAL_BOUNDED_IMPORTED_MANIFEST_SHA256": sha256(
            validation_four / "imported/manifest.json"
        ),
        "HISTORICAL_BOUNDED_IMPORTED_REPORT_SHA256": sha256(
            validation_four / "imported/report/import.json"
        ),
        "HISTORICAL_BOUNDED_CLEANED_MANIFEST_SHA256": sha256(
            validation_four / "cleaned/manifest.json"
        ),
        "HISTORICAL_BOUNDED_CLEANED_REPORT_SHA256": sha256(
            validation_four / "cleaned/report/quality.json"
        ),
        "HISTORICAL_REPLAY_IMPORT_REPORT_SHA256": sha256(
            roots["live_adapter_dataset"] / "report/import.json"
        ),
        "HISTORICAL_REPLAY_QUALITY_REPORT_SHA256": sha256(
            roots["live_clean_dataset"] / "report/quality.json"
        ),
        "HISTORICAL_TOTAL_FILES": sum(
            len(root.files) for root in historical_roots.values()
        ),
        "HISTORICAL_VALIDATION_ATTEMPTS": (
            (
                attempt_ids[0],
                "failed",
                "offered_count_mismatch",
                sha256(logs[0]),
            ),
            (
                attempt_ids[1],
                "failed",
                "tag_mismatch",
                sha256(logs[1]),
            ),
            (
                attempt_ids[2],
                "failed",
                "per_chunk_baseline_reset",
                sha256(logs[2]),
            ),
            (attempt_ids[3], "passed", "passed", sha256(logs[3])),
        ),
        "HISTORICAL_LEGACY_MODE_COUNTS": {
            "live_adapter_dataset": (0, 0),
            "live_clean_dataset": (0, 0),
        },
    }
    for name, value in constants.items():
        monkeypatch.setattr(f"predictive_cache.collection.cli.{name}", value)

    def reference(role, path):
        return HistoricalEvidenceReference(
            role=role,
            path=path,
            sha256=sha256(roots[role] / path),
        )

    bounded_evidence = HistoricalRunEvidence(
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
    live_evidence = HistoricalRunEvidence(
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
    attempts = tuple(
        HistoricalValidationAttempt(
            attempt=index,
            run_role=f"validation_{index}_run",
            input_role=f"validation_{index}_input",
            run_id=attempt_ids[index - 1],
            outcome="passed" if index == 4 else "failed",
            result_code=(
                "passed"
                if index == 4
                else (
                    "offered_count_mismatch",
                    "tag_mismatch",
                    "per_chunk_baseline_reset",
                )[index - 1]
            ),
            log=reference(f"validation_{index}_input", "validation.log"),
        )
        for index in range(1, 5)
    )
    attestation_model = HistoricalFinalizationAttestation(
        compatibility_scope="historical_pre_finalizer",
        run_id="controlled-demo",
        run_manifest_sha256=sha256(live_run / "manifest.json"),
        frozen_root_declaration_sha256=sha256(frozen_declaration),
        roots=tuple(historical_roots.values()),
        attempts=attempts,
        successful_bounded=bounded_evidence,
        live=live_evidence,
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
            commit="1" * 40,
            tree="2" * 40,
            source_archive=_external_file_evidence(executed_archive),
        ),
        integration=HistoricalIntegrationProvenance(
            commit="3" * 40,
            tree="4" * 40,
            executed=False,
        ),
        verification=HistoricalVerificationProvenance(
            commit=verification_commit,
            tree=verification_tree,
            source_archive=_external_file_evidence(verification_archive),
        ),
        adapter_wrapper=_external_file_evidence(adapter_wrapper),
        cleaner_wrapper=_external_file_evidence(cleaner_wrapper),
        approval=HistoricalCompatibilityApproval(
            status="approved",
            approved_at=START + timedelta(days=1),
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
    control = private_root("historical-control")
    attestation = control / "historical-attestation.json"
    atomic_write_model(attestation, attestation_model)
    deletion_approval = control / "historical-deletion-approval.json"
    atomic_write_model(
        deletion_approval,
        HistoricalDeletionApproval(
            status="approved",
            approved_at=START + timedelta(days=2),
            run_id=attestation_model.run_id,
            attestation_sha256=sha256(attestation),
            verification_commit=verification_commit,
            verification_tree=verification_tree,
            merge_confirmed=True,
            targets={
                role: HistoricalDeletionTarget(
                    canonical_path=root.canonical_path,
                    device=root.device,
                    inode=root.inode,
                    action=root.retention_action,
                )
                for role, root in historical_roots.items()
            },
            statement=(
                "Approve the exact historical cleanup targets and actions recorded "
                "in this artifact after the compatibility verifier merged."
            ),
        ),
    )
    return {
        "roots": roots,
        "control": control,
        "attestation": attestation,
        "deletion_approval": deletion_approval,
        "executed_archive": executed_archive,
        "verification_archive": verification_archive,
        "verification_repo": verification_repo,
        "verification_commit": verification_commit,
        "adapter_wrapper": adapter_wrapper,
        "cleaner_wrapper": cleaner_wrapper,
        "frozen_declaration": frozen_declaration,
    }


def _historical_finalize_args(fixture):
    arguments = [
        "finalize-historical",
        "--attestation",
        str(fixture["attestation"]),
        "--finalization-root",
        str(fixture["control"]),
        "--executed-source-archive",
        str(fixture["executed_archive"]),
        "--verification-source-archive",
        str(fixture["verification_archive"]),
        "--frozen-root-declaration",
        str(fixture["frozen_declaration"]),
        "--deletion-approval",
        str(fixture["deletion_approval"]),
        "--verification-repository",
        str(fixture["verification_repo"]),
        "--merged-ref",
        "HEAD",
        "--adapter-wrapper",
        str(fixture["adapter_wrapper"]),
        "--cleaner-wrapper",
        str(fixture["cleaner_wrapper"]),
    ]
    for role, path in fixture["roots"].items():
        arguments.extend(["--evidence-root", f"{role}={path}"])
    return arguments


def _historical_attest_args(fixture, out):
    arguments = [
        "attest-historical",
        "--finalization-root",
        str(fixture["control"]),
        "--out",
        str(out),
        "--executed-source-archive",
        str(fixture["executed_archive"]),
        "--verification-source-archive",
        str(fixture["verification_archive"]),
        "--verification-repository",
        str(fixture["verification_repo"]),
        "--verification-commit",
        fixture["verification_commit"],
        "--frozen-root-declaration",
        str(fixture["frozen_declaration"]),
        "--adapter-wrapper",
        str(fixture["adapter_wrapper"]),
        "--cleaner-wrapper",
        str(fixture["cleaner_wrapper"]),
        "--approve-missing-controls",
    ]
    for role, path in fixture["roots"].items():
        arguments.extend(["--evidence-root", f"{role}={path}"])
    return arguments


def test_attest_historical_derives_only_existing_evidence(
    tmp_path,
    monkeypatch,
):
    fixture = _historical_finalization_fixture(tmp_path, monkeypatch)
    fixture["attestation"].unlink()
    result = CliRunner().invoke(
        main,
        _historical_attest_args(fixture, fixture["attestation"]),
    )
    assert result.exit_code == 0, result.output
    attestation = HistoricalFinalizationAttestation.model_validate_json(
        fixture["attestation"].read_text()
    )
    assert attestation.approval.provider_execution_binding_status == (
        "legacy_not_recorded"
    )
    assert len(attestation.roots) == 13


def test_attest_historical_rejects_copied_genesis_root(
    tmp_path,
    monkeypatch,
):
    fixture = _historical_finalization_fixture(tmp_path, monkeypatch)
    fixture["attestation"].unlink()
    copied = tmp_path / "copied-genesis"
    shutil.copytree(fixture["roots"]["live_input"], copied)
    copied.chmod(0o700)
    arguments = _historical_attest_args(fixture, fixture["attestation"])
    original = f"live_input={fixture['roots']['live_input']}"
    arguments[arguments.index(original)] = f"live_input={copied}"
    result = CliRunner().invoke(main, arguments)
    assert result.exit_code != 0
    assert "runtime roots differ from frozen historical declaration" in result.output


def test_attest_historical_rejects_invented_verifier_archive(
    tmp_path,
    monkeypatch,
):
    fixture = _historical_finalization_fixture(tmp_path, monkeypatch)
    fixture["attestation"].unlink()
    fixture["verification_archive"].write_text("invented verifier\n")
    result = CliRunner().invoke(
        main,
        _historical_attest_args(fixture, fixture["attestation"]),
    )
    assert result.exit_code != 0
    assert "verification source archive" in result.output


def test_attest_historical_rejects_root_change_during_replay(
    tmp_path,
    monkeypatch,
):
    from predictive_cache.collection import cli as collection_cli

    fixture = _historical_finalization_fixture(tmp_path, monkeypatch)
    fixture["attestation"].unlink()
    original = collection_cli._validate_historical_attestation

    def validate_then_change(*args, **kwargs):
        original(*args, **kwargs)
        (fixture["roots"]["live_input"] / "live.log").write_text(
            "changed during attestation\n"
        )

    monkeypatch.setattr(
        collection_cli,
        "_validate_historical_attestation",
        validate_then_change,
    )
    result = CliRunner().invoke(
        main,
        _historical_attest_args(fixture, fixture["attestation"]),
    )
    assert result.exit_code != 0
    assert "roots changed during attestation" in result.output


def test_finalize_historical_rejects_invented_verifier_attestation(
    tmp_path,
    monkeypatch,
):
    fixture = _historical_finalization_fixture(tmp_path, monkeypatch)
    raw = json.loads(fixture["attestation"].read_text())
    raw["verification"]["commit"] = "7" * 40
    raw["verification"]["tree"] = "8" * 40
    fixture["attestation"].unlink()
    fixture["attestation"].write_text(json.dumps(raw) + "\n")
    fixture["attestation"].chmod(0o600)
    result = CliRunner().invoke(main, _historical_finalize_args(fixture))
    assert result.exit_code != 0
    assert "verification Git provenance" in result.output


def test_finalize_historical_requires_exact_deletion_approval(
    tmp_path,
    monkeypatch,
):
    fixture = _historical_finalization_fixture(tmp_path, monkeypatch)
    raw = json.loads(fixture["deletion_approval"].read_text())
    raw["targets"]["live_input"]["canonical_path"] = "/copied/root"
    fixture["deletion_approval"].unlink()
    fixture["deletion_approval"].write_text(json.dumps(raw) + "\n")
    fixture["deletion_approval"].chmod(0o600)
    result = CliRunner().invoke(main, _historical_finalize_args(fixture))
    assert result.exit_code != 0
    assert "does not match exact targets" in result.output


@pytest.mark.parametrize(
    ("artifact_name", "message"),
    [
        ("attestation", "historical attestation changed"),
        ("deletion_approval", "historical deletion approval changed"),
        (
            "frozen_declaration",
            "frozen historical root declaration changed",
        ),
        (
            "verification_archive",
            "historical verification source archive changed",
        ),
        (
            "evidence_root",
            "historical evidence roots changed",
        ),
    ],
)
def test_finalize_historical_rejects_authorization_swap_during_replay(
    tmp_path,
    monkeypatch,
    artifact_name,
    message,
):
    from predictive_cache.collection import cli as collection_cli

    fixture = _historical_finalization_fixture(tmp_path, monkeypatch)
    original = collection_cli._validate_historical_attestation

    def validate_then_replace(*args, **kwargs):
        original(*args, **kwargs)
        if artifact_name == "evidence_root":
            path = fixture["roots"]["live_input"] / "live.log"
            path.write_text("changed during replay\n")
        else:
            path = fixture[artifact_name]
            path.unlink()
            path.write_text('{"replacement":true}\n')
            path.chmod(0o600)

    monkeypatch.setattr(
        collection_cli,
        "_validate_historical_attestation",
        validate_then_replace,
    )
    result = CliRunner().invoke(main, _historical_finalize_args(fixture))
    assert result.exit_code != 0
    assert message in result.output


def test_finalize_historical_preserves_actual_topology(
    tmp_path,
    monkeypatch,
):
    fixture = _historical_finalization_fixture(tmp_path, monkeypatch)
    result = CliRunner().invoke(main, _historical_finalize_args(fixture))
    assert result.exit_code == 0, result.output
    schedule = json.loads(
        (fixture["control"] / "historical-cleanup-schedule.json").read_text()
    )
    assert schedule["root_actions"]["shared_provider"] == "retain"
    assert sum(
        action == "delete" for action in schedule["root_actions"].values()
    ) == 12
    inventory = json.loads(
        (fixture["control"] / "historical-deletion-inventory.json").read_text()
    )
    assert len(inventory["roots"]) == 13
    assert sum(len(root["files"]) for root in inventory["roots"]) == sum(
        len(root.files)
        for root in HistoricalFinalizationAttestation.model_validate_json(
            fixture["attestation"].read_text()
        ).roots
    )


def test_historical_root_accepts_confined_legacy_modes_but_rejects_open_root(
    tmp_path,
):
    root = tmp_path / "legacy"
    root.mkdir(mode=0o700)
    nested = root / "nested"
    nested.mkdir(mode=0o755)
    artifact = nested / "events.jsonl"
    artifact.write_text("{}\n")
    artifact.chmod(0o644)
    evidence = _historical_root("live_adapter_dataset", root)
    assert evidence.directories["nested"].mode == 0o755
    assert evidence.files["nested/events.jsonl"].mode == 0o644
    root.chmod(0o755)
    with pytest.raises(click.ClickException, match="owner-only"):
        _historical_root("live_adapter_dataset", root)


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        ("executed_is_verifier", "deletion approval"),
        ("external_hash", "external evidence differs"),
        ("missing_root", "thirteen historical roots"),
        ("duplicate_root", "distinct and non-nested"),
        ("modified_manifest", "runtime roots differ"),
    ],
)
def test_finalize_historical_rejects_substituted_evidence(
    tmp_path,
    monkeypatch,
    mutation,
    message,
):
    fixture = _historical_finalization_fixture(tmp_path, monkeypatch)
    arguments = _historical_finalize_args(fixture)
    if mutation == "executed_is_verifier":
        raw = json.loads(fixture["attestation"].read_text())
        raw["executed"]["commit"] = raw["verification"]["commit"]
        fixture["attestation"].unlink()
        fixture["attestation"].write_text(json.dumps(raw) + "\n")
        fixture["attestation"].chmod(0o600)
    elif mutation == "external_hash":
        fixture["executed_archive"].write_text("changed\n")
    elif mutation == "missing_root":
        arguments = arguments[:-2]
    elif mutation == "duplicate_root":
        duplicate = (
            f"live_clean_dataset={fixture['roots']['live_adapter_dataset']}"
        )
        index = arguments.index(
            f"live_clean_dataset={fixture['roots']['live_clean_dataset']}"
        )
        arguments[index] = duplicate
    else:
        manifest = fixture["roots"]["live_run"] / "manifest.json"
        manifest.write_text(manifest.read_text() + "\n")
    result = CliRunner().invoke(main, arguments)
    assert result.exit_code != 0
    assert message in result.output


def test_finalize_historical_rejects_invented_provider_controls(
    tmp_path,
    monkeypatch,
):
    fixture = _historical_finalization_fixture(tmp_path, monkeypatch)
    invented = fixture["roots"]["live_run"] / "provider-invocation.json"
    invented.write_text('{"invented":true}\n')
    invented.chmod(0o600)
    raw = json.loads(fixture["attestation"].read_text())
    raw["roots"] = [
        _historical_root(role, path).model_dump(mode="json")
        for role, path in fixture["roots"].items()
    ]
    fixture["attestation"].unlink()
    fixture["attestation"].write_text(json.dumps(raw) + "\n")
    fixture["attestation"].chmod(0o600)
    monkeypatch.setattr(
        "predictive_cache.collection.cli.HISTORICAL_TOTAL_FILES",
        sum(len(root["files"]) for root in raw["roots"]),
    )
    result = CliRunner().invoke(main, _historical_finalize_args(fixture))
    assert result.exit_code != 0
    assert "runtime roots differ" in result.output


def test_finalize_historical_rejects_copied_root(
    tmp_path,
    monkeypatch,
):
    fixture = _historical_finalization_fixture(tmp_path, monkeypatch)
    copied = tmp_path / "copied-live-input"
    shutil.copytree(fixture["roots"]["live_input"], copied)
    copied.chmod(0o700)
    arguments = _historical_finalize_args(fixture)
    original = f"live_input={fixture['roots']['live_input']}"
    arguments[arguments.index(original)] = f"live_input={copied}"
    result = CliRunner().invoke(main, arguments)
    assert result.exit_code != 0
    assert "runtime roots differ" in result.output


def test_historical_completion_rejects_late_deadline(tmp_path, monkeypatch):
    fixture = _historical_finalization_fixture(tmp_path, monkeypatch)
    result = CliRunner().invoke(main, _historical_finalize_args(fixture))
    assert result.exit_code == 0, result.output
    schedule_path = fixture["control"] / "historical-cleanup-schedule.json"
    schedule = json.loads(schedule_path.read_text())
    with pytest.raises(ValidationError, match="must not exceed"):
        HistoricalCleanupCompletionRecord(
            run_id=schedule["run_id"],
            delete_by=schedule["delete_by"],
            completed_at=(
                datetime.fromisoformat(schedule["delete_by"]) + timedelta(seconds=1)
            ),
            cleanup_owner=schedule["cleanup_owner"],
            evidence="late",
            schedule_sha256=sha256(schedule_path),
            attestation_sha256=schedule["attestation_sha256"],
            deletion_approval_sha256=schedule[
                "deletion_approval_sha256"
            ],
            deletion_inventory_sha256=schedule["deletion_inventory_sha256"],
            root_dispositions={
                role: (
                    "retained_verified"
                    if action == "retain"
                    else "deleted"
                )
                for role, action in schedule["root_actions"].items()
            },
        )


def test_complete_cleanup_historical_binds_all_roles(
    tmp_path,
    monkeypatch,
):
    fixture = _historical_finalization_fixture(tmp_path, monkeypatch)
    result = CliRunner().invoke(main, _historical_finalize_args(fixture))
    assert result.exit_code == 0, result.output
    schedule_path = fixture["control"] / "historical-cleanup-schedule.json"
    schedule = json.loads(schedule_path.read_text())
    completion = HistoricalCleanupCompletionRecord(
        run_id=schedule["run_id"],
        delete_by=schedule["delete_by"],
        completed_at=schedule["finalized_at"],
        cleanup_owner=schedule["cleanup_owner"],
        evidence="targeted deletion verified",
        schedule_sha256=sha256(schedule_path),
        attestation_sha256=schedule["attestation_sha256"],
        deletion_approval_sha256=schedule["deletion_approval_sha256"],
        deletion_inventory_sha256=schedule["deletion_inventory_sha256"],
        root_dispositions={
            role: (
                "retained_verified"
                if action == "retain"
                else "deleted"
            )
            for role, action in schedule["root_actions"].items()
        },
    )
    completion_path = fixture["control"] / "completion-input.json"
    atomic_write_model(completion_path, completion)
    out = fixture["control"] / "completion.json"
    complete = CliRunner().invoke(
        main,
        [
            "complete-cleanup-historical",
            "--schedule",
            str(schedule_path),
            "--completion",
            str(completion_path),
            "--out",
            str(out),
        ],
    )
    assert complete.exit_code == 0, complete.output


def test_finalize_requires_verified_outputs_and_reconciliation(tmp_path, monkeypatch):
    run_dir, report, dataset_root, finalization_root, attestation, roots, _ = _finalization_fixture(
        tmp_path, monkeypatch
    )
    result = CliRunner().invoke(
        main,
        _finalize_args(run_dir, report, dataset_root, finalization_root, attestation, roots),
    )
    assert result.exit_code == 0, result.output
    schedule = CleanupScheduleRecord.model_validate_json(
        (finalization_root / "cleanup-schedule.json").read_text()
    )
    assert schedule.delete_by == schedule.finalized_at + timedelta(days=7)
    inventory = json.loads((finalization_root / "deletion-inventory.json").read_text())
    serialized = json.dumps(inventory)
    assert '"reconciliation.json"' in serialized
    assert len(inventory["roots"]) == 8
    assert {root["kind"] for root in inventory["roots"]} == set(roots)
    for kind in roots:
        item = next(root for root in inventory["roots"] if root["kind"] == kind)
        assert item["canonical_path"] == str(roots[kind].resolve())


def test_bounded_run_seals_adapter_validation_permit_before_execution(
    tmp_path,
    monkeypatch,
):
    run_dir = private_dir(tmp_path)
    live_catalog, live_manifest = live_catalog_manifest()
    atomic_write_model(run_dir / "manifest.json", live_manifest)
    atomic_write_model(run_dir / "fixture-catalog.json", live_catalog)
    observed = {}
    subprocess_calls = []
    runtime_fds = []

    class FakeRunner:
        def run(self, manifest, catalog, evidence_dir, executor, **kwargs):
            permit_path = evidence_dir / "adapter-validation-permit.json"
            assert permit_path.is_file()
            observed["permit"] = AdapterValidationPermit.model_validate_json(
                permit_path.read_text()
            )
            return kwargs["operation_limit"]

    monkeypatch.setattr("predictive_cache.collection.cli.CollectionRunner", FakeRunner)
    monkeypatch.setattr(
        "predictive_cache.collection.cli.DirectMountExecutor",
        lambda *args, **kwargs: object(),
    )
    class FakeCommandProvider:
        def close(self):
            return None

    monkeypatch.setattr(
        "predictive_cache.collection.cli.CommandCheckpointProvider",
        lambda *args, **kwargs: FakeCommandProvider(),
    )
    monkeypatch.setattr(
        "predictive_cache.collection.cli.subprocess.run",
        lambda args, **kwargs: subprocess_calls.append(args),
    )
    monkeypatch.setattr(
        "predictive_cache.collection.cli._approved_executable",
        lambda path, _digest, _label: path,
    )
    monkeypatch.setattr(
        "predictive_cache.collection.cli._snapshot_executable",
        lambda path, _run_dir, _name, _digest: path,
    )
    monkeypatch.setattr(
        "predictive_cache.collection.cli._approved_artifact",
        lambda path, _digest, _label: path,
    )
    monkeypatch.setattr(
        "predictive_cache.collection.cli._snapshot_artifact",
        lambda path, _run_dir, _name, _digest: path,
    )
    def fake_open_verified_fd(_path, _digest):
        fd = os.open("/dev/null", os.O_RDONLY)
        runtime_fds.append(fd)
        return fd

    monkeypatch.setattr(
        "predictive_cache.collection.cli._open_verified_fd",
        fake_open_verified_fd,
    )
    monkeypatch.setattr(
        "predictive_cache.collection.cli._approved_interpreter",
        lambda: Path("/usr/bin/python3.12"),
    )
    result = CliRunner().invoke(
        main,
        [
            "run",
            "--run-dir",
            str(run_dir),
            "--mount-root",
            str(tmp_path),
            "--checkpoint-command",
            str(tmp_path / "provider"),
            "--provider-package",
            str(tmp_path / "provider-package.zip"),
            "--validation-operations",
            "2",
            "--adapter-command",
            str(tmp_path / "adapter"),
            "--cleaner-command",
            str(tmp_path / "cleaner"),
        ],
    )
    assert result.exit_code == 0, result.output
    permit = observed["permit"]
    manifest = CollectionManifest.model_validate_json((run_dir / "manifest.json").read_text())
    catalog_model = FixtureCatalog.model_validate_json(
        (run_dir / "fixture-catalog.json").read_text()
    )
    expected = planned_operations(manifest, catalog_model)[:2]
    assert permit.operation_indexes == (0, 1)
    assert permit.operation_ids == tuple(item[4] for item in expected)
    assert subprocess_calls == [
        [
            f"/proc/self/fd/{runtime_fds[0]}",
            run_dir,
            run_dir / "adapter-validation-permit.json",
        ],
        [f"/proc/self/fd/{runtime_fds[1]}", run_dir],
    ]


def test_unapproved_validation_command_is_rejected(tmp_path):
    command = tmp_path / "command"
    command.write_text("#!/bin/sh\nexit 0\n")
    command.chmod(0o700)
    with pytest.raises(click.ClickException, match="approved executable"):
        _approved_executable(command, "0" * 64, "adapter command")


def test_verified_runtime_fd_survives_path_replacement(tmp_path):
    wrapper = tmp_path / "wrapper"
    wrapper.write_text("approved\n")
    expected = sha256(wrapper)
    fd = _open_verified_fd(wrapper, expected)
    try:
        wrapper.unlink()
        wrapper.write_text("replacement\n")
        with os.fdopen(os.dup(fd), "r") as stream:
            assert stream.read() == "approved\n"
    finally:
        os.close(fd)


def test_provider_provenance_rejects_replaced_snapshot(tmp_path, monkeypatch):
    run_dir, _, _, _, _, roots, _ = _finalization_fixture(
        tmp_path, monkeypatch
    )
    snapshot = run_dir / ".approved-tools/checkpoint-provider"
    snapshot.chmod(0o600)
    snapshot.write_text("replacement\n")
    snapshot.chmod(0o500)
    evidence = {
        kind: _cleanup_root(kind, kind, path)
        for kind, path in roots.items()
    }
    provider = LiveProviderDeclaration.model_validate_json(
        (roots["live_provider_evidence"] / "provider-manifest.json").read_text()
    )
    source_manifest = CollectorFileManifest.model_validate_json(
        (run_dir / "collector-file-manifest.json").read_text()
    )
    with pytest.raises(click.ClickException, match="approved executable"):
        _validate_provider_provenance(
            provider,
            evidence,
            "live_harness_evidence",
            "controlled-demo",
            sha256(run_dir / "manifest.json"),
            300,
            source_manifest,
            "live",
        )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("provider_package_sha256", "0" * 64),
        ("interpreter_path", "/tmp/python3"),
        ("interpreter_sha256", "0" * 64),
        ("argv_contract", ("/usr/bin/python3.12", "wrong-provider")),
    ],
)
def test_bounded_provider_requires_complete_provenance(
    tmp_path,
    monkeypatch,
    field,
    value,
):
    _, _, _, _, _, roots, _ = _finalization_fixture(tmp_path, monkeypatch)
    evidence = {
        kind: _cleanup_root(kind, kind, path)
        for kind, path in roots.items()
    }
    provider_path = roots["bounded_provider_evidence"] / "provider-manifest.json"
    provider = LiveProviderDeclaration.model_validate_json(
        provider_path.read_text()
    ).model_copy(update={field: value})
    source_manifest = CollectorFileManifest.model_validate_json(
        (
            roots["bounded_harness_evidence"]
            / "collector-file-manifest.json"
        ).read_text()
    )
    with pytest.raises(click.ClickException, match="provenance is inconsistent"):
        _validate_provider_provenance(
            provider,
            evidence,
            "bounded_harness_evidence",
            "bounded-validation",
            sha256(
                roots["bounded_harness_evidence"]
                / "candidate-manifest.json"
            ),
            1,
            source_manifest,
            "bounded",
        )


@pytest.mark.parametrize("mutation", ["package", "invocation"])
def test_bounded_provider_rejects_resealed_runtime_artifacts(
    tmp_path,
    monkeypatch,
    mutation,
):
    _, _, _, _, _, roots, _ = _finalization_fixture(tmp_path, monkeypatch)
    provider_path = roots["bounded_provider_evidence"] / "provider-manifest.json"
    provider = LiveProviderDeclaration.model_validate_json(
        provider_path.read_text()
    )
    if mutation == "package":
        artifact = (
            roots["bounded_harness_evidence"]
            / ".approved-tools/provider-package.zip"
        )
        artifact.write_text("alternate package\n")
        artifact.chmod(0o400)
        provider = provider.model_copy(
            update={
                "provider_package_artifact": CleanupEvidenceReference(
                    root_id="bounded_harness_evidence",
                    path=".approved-tools/provider-package.zip",
                    sha256=sha256(artifact),
                )
            }
        )
        message = "package snapshot is not approved"
    else:
        artifact = (
            roots["bounded_harness_evidence"] / "provider-invocation.json"
        )
        raw = json.loads(artifact.read_text())
        raw["actions"] = ["checkpoint"]
        artifact.unlink()
        artifact.write_text(json.dumps(raw) + "\n")
        artifact.chmod(0o600)
        provider = provider.model_copy(
            update={
                "invocation_record": CleanupEvidenceReference(
                    root_id="bounded_harness_evidence",
                    path="provider-invocation.json",
                    sha256=sha256(artifact),
                )
            }
        )
        message = "invocation record is inconsistent"
    evidence = {
        kind: _cleanup_root(kind, kind, path)
        for kind, path in roots.items()
    }
    source_manifest = CollectorFileManifest.model_validate_json(
        (
            roots["bounded_harness_evidence"]
            / "collector-file-manifest.json"
        ).read_text()
    )
    with pytest.raises(click.ClickException, match=message):
        _validate_provider_provenance(
            provider,
            evidence,
            "bounded_harness_evidence",
            "bounded-validation",
            sha256(
                roots["bounded_harness_evidence"]
                / "candidate-manifest.json"
            ),
            1,
            source_manifest,
            "bounded",
        )


def test_provider_package_must_reference_executed_snapshot(
    tmp_path,
    monkeypatch,
):
    _, _, _, _, _, roots, _ = _finalization_fixture(tmp_path, monkeypatch)
    provider = LiveProviderDeclaration.model_validate_json(
        (
            roots["bounded_provider_evidence"] / "provider-manifest.json"
        ).read_text()
    ).model_copy(
        update={
            "provider_package_artifact": CleanupEvidenceReference(
                root_id="bounded_provider_evidence",
                path="provider-package.json",
                sha256=sha256(
                    roots["bounded_provider_evidence"]
                    / "provider-package.json"
                ),
            )
        }
    )
    evidence = {
        kind: _cleanup_root(kind, kind, path)
        for kind, path in roots.items()
    }
    source_manifest = CollectorFileManifest.model_validate_json(
        (
            roots["bounded_harness_evidence"]
            / "collector-file-manifest.json"
        ).read_text()
    )
    with pytest.raises(click.ClickException, match="canonical reference"):
        _validate_provider_provenance(
            provider,
            evidence,
            "bounded_harness_evidence",
            "bounded-validation",
            sha256(
                roots["bounded_harness_evidence"]
                / "candidate-manifest.json"
            ),
            1,
            source_manifest,
            "bounded",
        )


def test_provider_declaration_rejects_duplicate_chunk_reference(
    tmp_path,
    monkeypatch,
):
    _, _, _, _, _, roots, _ = _finalization_fixture(tmp_path, monkeypatch)
    raw = json.loads(
        (
            roots["bounded_provider_evidence"] / "provider-manifest.json"
        ).read_text()
    )
    raw["source_chunks"].append(dict(raw["source_chunks"][0]))
    with pytest.raises(ValidationError, match="duplicate source chunks"):
        LiveProviderDeclaration.model_validate(raw)


@pytest.mark.parametrize("mutation", ["extra", "missing", "symlink"])
def test_provider_rejects_nonexact_collector_chunk_inventory(
    tmp_path,
    monkeypatch,
    mutation,
):
    _, _, _, _, _, roots, _ = _finalization_fixture(tmp_path, monkeypatch)
    harness = roots["bounded_harness_evidence"]
    source_manifest = CollectorFileManifest.model_validate_json(
        (harness / "collector-file-manifest.json").read_text()
    )
    raw_path = harness / source_manifest.files[0].raw_jsonl_path
    if mutation == "extra":
        unexpected = raw_path.parent / "unexpected.jsonl"
        unexpected.write_text(raw_path.read_text())
        unexpected.chmod(0o600)
    elif mutation == "missing":
        raw_path.unlink()
    else:
        unexpected = raw_path.parent / "unexpected.jsonl"
        unexpected.symlink_to(raw_path)
        with pytest.raises(click.ClickException, match="symlink"):
            _cleanup_root(
                "bounded_harness_evidence",
                "bounded_harness_evidence",
                harness,
            )
        return
    evidence = {
        kind: _cleanup_root(kind, kind, path)
        for kind, path in roots.items()
    }
    provider = LiveProviderDeclaration.model_validate_json(
        (
            roots["bounded_provider_evidence"] / "provider-manifest.json"
        ).read_text()
    )
    with pytest.raises(click.ClickException):
        _validate_provider_provenance(
            provider,
            evidence,
            "bounded_harness_evidence",
            "bounded-validation",
            sha256(harness / "candidate-manifest.json"),
            1,
            source_manifest,
            "bounded",
        )


def test_partition_and_bounded_daily_summary_are_exact(
    tmp_path,
    monkeypatch,
):
    _, _, _, _, _, roots, _ = _finalization_fixture(tmp_path, monkeypatch)
    adapter_root = roots["bounded_adapter_dataset"]
    event_path = next((adapter_root / "events").rglob("part-00000.jsonl"))
    event = json.loads(event_path.read_text())
    with pytest.raises(click.ClickException, match="canonical partition path"):
        _validate_partitioned_rows(
            {"events/volume_id=wrong/date=2026-11-04/part-00000.jsonl": [event]},
            "events",
            event["volume_id"],
            {"2026-11-04"},
        )
    report = BoundedCleanReport.model_validate_json(
        (
            roots["bounded_clean_dataset"] / "report/quality.json"
        ).read_text()
    )
    wrong_day = report.model_copy(
        update={
            "by_volume_day": (
                report.by_volume_day[0].model_copy(
                    update={"date": "2026-11-05"}
                ),
            )
        }
    )
    with pytest.raises(click.ClickException, match="per-day distributions"):
        _validate_clean_daily_summary(
            wrong_day,
            [event],
            event["volume_id"],
            "bounded",
        )


def test_jsonl_artifact_rejects_duplicate_keys(tmp_path):
    artifact = tmp_path / "events.jsonl"
    artifact.write_text('{"event_id":"first","event_id":"second"}\n')
    with pytest.raises(click.ClickException, match="invalid JSONL artifact"):
        _artifact_rows(artifact, 1)


def test_raw_identity_reconciliation_rejects_unreferenced_event():
    streams = {
        ("pod-uid", "producer-epoch"): [
            SimpleNamespace(local_id="event-1", sequence=1),
            SimpleNamespace(local_id="event-2", sequence=2),
        ]
    }
    durable = {
        ("event-1", "pod-uid", "producer-epoch", 1): object(),
    }
    with pytest.raises(click.ClickException, match="exactly match durable"):
        _validate_exact_raw_identities(streams, durable, 1)


def test_clean_artifact_allowlist_rejects_events_prefix_smuggling(tmp_path):
    root = private_dir(tmp_path) / "clean"
    root.mkdir(mode=0o700)
    manifest = root / "manifest.json"
    manifest.write_text("{}\n")
    manifest.chmod(0o600)
    smuggled = root / "events-private" / "credentials.txt"
    smuggled.parent.mkdir(mode=0o700)
    smuggled.write_text("not dataset evidence\n")
    smuggled.chmod(0o600)
    evidence = _cleanup_root("live_clean_dataset", "live_clean_dataset", root)
    with pytest.raises(click.ClickException, match="unsupported artifact path"):
        _validate_dataset_artifacts(
            evidence,
            (
                DatasetArtifact(
                    path="events-private/credentials.txt",
                    sha256=sha256(smuggled),
                    rows=None,
                ),
            ),
            "manifest.json",
            (
                "events_clean/",
                "excluded/",
                "coverage/",
                "validation.json",
            ),
        )


def test_clean_artifact_contract_rejects_unvalidated_class_member(
    tmp_path,
    monkeypatch,
):
    _, _, _, _, _, roots, _ = _finalization_fixture(tmp_path, monkeypatch)
    clean_root_path = roots["live_clean_dataset"]
    clean_root = _cleanup_root(
        "live_clean_dataset",
        "live_clean_dataset",
        clean_root_path,
    )
    manifest = LiveCleanManifest.model_validate_json(
        (clean_root_path / "manifest.json").read_text()
    )
    report = LiveCleanReport.model_validate_json(
        (clean_root_path / "report/quality.json").read_text()
    )
    artifacts = _validate_dataset_artifacts(
        clean_root,
        manifest.files,
        "manifest.json",
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
    artifacts["meta/private-data.bin"] = None
    adapter_event_path = next(
        (roots["live_adapter_dataset"] / "events").rglob(
            "part-00000.jsonl"
        )
    )
    adapter_rows = [
        json.loads(line)
        for line in adapter_event_path.read_text().splitlines()
    ]
    with pytest.raises(click.ClickException, match="set is not canonical"):
        _validate_clean_artifact_contract(
            clean_root,
            artifacts,
            manifest,
            report,
            adapter_rows,
        )
    artifacts.pop("meta/private-data.bin")
    (clean_root_path / "validation.json").write_text(
        '{"status":"failed"}\n'
    )
    with pytest.raises(click.ClickException, match="auxiliary artifacts"):
        _validate_clean_artifact_contract(
            clean_root,
            artifacts,
            manifest,
            report,
            adapter_rows,
        )


def test_finalize_rejects_forged_and_tampered_dataset_hashes(tmp_path, monkeypatch):
    run_dir, report, dataset_root, finalization_root, attestation, roots, output = _finalization_fixture(
        tmp_path, monkeypatch
    )
    raw = json.loads(report.read_text())
    output_key = output.relative_to(dataset_root).as_posix()
    raw["output_hashes"][output_key] = "0" * 64
    report.write_text(json.dumps(raw) + "\n")
    result = CliRunner().invoke(
        main,
        _finalize_args(run_dir, report, dataset_root, finalization_root, attestation, roots),
    )
    assert result.exit_code != 0
    assert "do not match actual outputs" in result.output

    raw["output_hashes"][output_key] = sha256(output)
    report.write_text(json.dumps(raw) + "\n")
    output.write_text('{"event":"tampered"}\n')
    result = CliRunner().invoke(
        main,
        _finalize_args(run_dir, report, dataset_root, finalization_root, attestation, roots),
    )
    assert result.exit_code != 0
    assert "do not match actual outputs" in result.output


def test_finalize_rejects_missing_or_symlinked_outputs(tmp_path, monkeypatch):
    run_dir, report, dataset_root, finalization_root, attestation, roots, output = _finalization_fixture(
        tmp_path, monkeypatch
    )
    output.unlink()
    result = CliRunner().invoke(
        main,
        _finalize_args(run_dir, report, dataset_root, finalization_root, attestation, roots),
    )
    assert result.exit_code != 0

    target = tmp_path / "outside.jsonl"
    target.write_text('{"event":"outside"}\n')
    target.chmod(0o600)
    output.symlink_to(target)
    result = CliRunner().invoke(
        main,
        _finalize_args(run_dir, report, dataset_root, finalization_root, attestation, roots),
    )
    assert result.exit_code != 0
    assert "must not contain symlinks" in result.output


def test_finalize_rejects_unsafe_external_validation_root(tmp_path, monkeypatch):
    run_dir, report, dataset_root, finalization_root, attestation, roots, _ = _finalization_fixture(
        tmp_path, monkeypatch
    )
    validation_root = roots["bounded_provider_evidence"]
    validation_root.chmod(0o755)
    result = CliRunner().invoke(
        main,
        _finalize_args(run_dir, report, dataset_root, finalization_root, attestation, roots),
    )
    assert result.exit_code != 0
    assert "must be owner-only" in result.output

    validation_root.chmod(0o700)
    link = tmp_path / "validation-link"
    link.symlink_to(validation_root, target_is_directory=True)
    linked_roots = roots | {"bounded_provider_evidence": link}
    result = CliRunner().invoke(
        main,
        _finalize_args(run_dir, report, dataset_root, finalization_root, attestation, linked_roots),
    )
    assert result.exit_code != 0
    assert "must not traverse symlinks" in result.output


def test_finalize_rejects_missing_or_failed_reconciliation(tmp_path, monkeypatch):
    run_dir, report, dataset_root, finalization_root, attestation, roots, _ = _finalization_fixture(
        tmp_path, monkeypatch
    )
    (run_dir / "reconciliation.json").unlink()
    result = CliRunner().invoke(
        main,
        _finalize_args(run_dir, report, dataset_root, finalization_root, attestation, roots),
    )
    assert result.exit_code != 0
    assert "full-run evidence is incomplete" in result.output

    fixture = _finalization_fixture(tmp_path / "failed", monkeypatch)
    run_dir, report, dataset_root, finalization_root, attestation, roots, _ = fixture
    reconciliation = ReconciliationReport.model_validate_json(
        (run_dir / "reconciliation.json").read_text()
    )
    (run_dir / "reconciliation.json").unlink()
    atomic_write_model(
        run_dir / "reconciliation.json",
        reconciliation.model_copy(update={"success": False, "failures": ("gap",)}),
    )
    result = CliRunner().invoke(
        main,
        _finalize_args(run_dir, report, dataset_root, finalization_root, attestation, roots),
    )
    assert result.exit_code != 0
    assert "successful reconciliation evidence is required" in result.output


def test_finalize_authenticates_external_validation_manifest(tmp_path, monkeypatch):
    run_dir, report, dataset_root, finalization_root, attestation, roots, _ = _finalization_fixture(
        tmp_path, monkeypatch
    )
    raw = json.loads(attestation.read_text())
    raw["provider"]["sha256"] = "0" * 64
    attestation.unlink()
    attestation.write_text(json.dumps(raw) + "\n")
    attestation.chmod(0o600)
    result = CliRunner().invoke(
        main,
        _finalize_args(run_dir, report, dataset_root, finalization_root, attestation, roots),
    )
    assert result.exit_code != 0
    assert "hash mismatch" in result.output

    fixture = _finalization_fixture(tmp_path / "tampered", monkeypatch)
    run_dir, report, dataset_root, finalization_root, attestation, roots, _ = fixture
    (roots["bounded_provider_evidence"] / "provider-evidence.jsonl").write_text(
        '{"provider":"tampered"}\n'
    )
    result = CliRunner().invoke(
        main,
        _finalize_args(run_dir, report, dataset_root, finalization_root, attestation, roots),
    )
    assert result.exit_code != 0
    assert "does not match attestation" in result.output


def test_finalize_rejects_resealed_contract_substitution(tmp_path, monkeypatch):
    run_dir, report, dataset_root, finalization_root, attestation, roots, _ = (
        _finalization_fixture(tmp_path, monkeypatch)
    )
    contract = roots["bounded_harness_evidence"] / "cluster-provenance-v1.json"
    contract.write_text('{"substituted":true}\n')
    contract.chmod(0o600)
    resealed = _cleanup_root(
        "bounded_harness_evidence",
        "bounded_harness_evidence",
        roots["bounded_harness_evidence"],
    )
    raw = json.loads(attestation.read_text())
    raw["cluster_provenance"]["sha256"] = sha256(contract)
    raw["cleanup_roots"] = [
        resealed.model_dump(mode="json")
        if item["kind"] == "bounded_harness_evidence"
        else item
        for item in raw["cleanup_roots"]
    ]
    attestation.unlink()
    attestation.write_text(json.dumps(raw) + "\n")
    attestation.chmod(0o600)
    result = CliRunner().invoke(
        main,
        _finalize_args(
            run_dir,
            report,
            dataset_root,
            finalization_root,
            attestation,
            roots,
        ),
    )
    assert result.exit_code != 0
    assert "does not match the live experiment profile" in result.output


def test_finalize_rejects_decoy_artifact_path_with_matching_bytes(
    tmp_path,
    monkeypatch,
):
    run_dir, report, dataset_root, finalization_root, attestation, roots, _ = (
        _finalization_fixture(tmp_path, monkeypatch)
    )
    source = roots["bounded_adapter_dataset"] / "manifest.json"
    decoy = roots["bounded_adapter_dataset"] / "decoy-manifest.json"
    decoy.write_bytes(source.read_bytes())
    decoy.chmod(0o600)
    resealed = _cleanup_root(
        "bounded_adapter_dataset",
        "bounded_adapter_dataset",
        roots["bounded_adapter_dataset"],
    )
    raw = json.loads(attestation.read_text())
    raw["imported_manifest"]["path"] = "decoy-manifest.json"
    raw["cleanup_roots"] = [
        resealed.model_dump(mode="json")
        if item["kind"] == "bounded_adapter_dataset"
        else item
        for item in raw["cleanup_roots"]
    ]
    attestation.unlink()
    attestation.write_text(json.dumps(raw) + "\n")
    attestation.chmod(0o600)
    result = CliRunner().invoke(
        main,
        _finalize_args(
            run_dir,
            report,
            dataset_root,
            finalization_root,
            attestation,
            roots,
        ),
    )
    assert result.exit_code != 0
    assert "wrong canonical artifact" in result.output


def test_finalize_requires_exact_eight_distinct_roots(tmp_path, monkeypatch):
    run_dir, report, dataset_root, finalization_root, attestation, roots, _ = _finalization_fixture(
        tmp_path, monkeypatch
    )
    seven = dict(roots)
    seven.pop("bounded_clean_dataset")
    result = CliRunner().invoke(
        main,
        _finalize_args(run_dir, report, dataset_root, finalization_root, attestation, seven),
    )
    assert result.exit_code != 0
    assert "exactly eight" in result.output

    nine = dict(roots)
    extra = tmp_path / "extra"
    extra.mkdir(mode=0o700)
    evidence = extra / "evidence.json"
    evidence.write_text("{}\n")
    evidence.chmod(0o600)
    nine["extra_root"] = extra
    result = CliRunner().invoke(
        main,
        _finalize_args(run_dir, report, dataset_root, finalization_root, attestation, nine),
    )
    assert result.exit_code != 0
    assert "exactly eight" in result.output

    duplicate = roots | {
        "bounded_provider_evidence": roots["bounded_adapter_dataset"]
    }
    result = CliRunner().invoke(
        main,
        _finalize_args(run_dir, report, dataset_root, finalization_root, attestation, duplicate),
    )
    assert result.exit_code != 0
    assert "distinct and non-nested" in result.output

    nested_root = roots["bounded_adapter_dataset"] / "nested-provider"
    shutil.copytree(roots["bounded_provider_evidence"], nested_root)
    nested_root.chmod(0o700)
    for path in nested_root.rglob("*"):
        path.chmod(0o700 if path.is_dir() else 0o600)
    nested = roots | {"bounded_provider_evidence": nested_root}
    result = CliRunner().invoke(
        main,
        _finalize_args(run_dir, report, dataset_root, finalization_root, attestation, nested),
    )
    assert result.exit_code != 0
    assert "distinct and non-nested" in result.output


def test_finalize_rejects_root_kind_swap_and_copied_substitute(tmp_path, monkeypatch):
    run_dir, report, dataset_root, finalization_root, attestation, roots, _ = _finalization_fixture(
        tmp_path, monkeypatch
    )
    swapped = roots | {
        "bounded_adapter_dataset": roots["bounded_clean_dataset"],
        "bounded_clean_dataset": roots["bounded_adapter_dataset"],
    }
    result = CliRunner().invoke(
        main,
        _finalize_args(run_dir, report, dataset_root, finalization_root, attestation, swapped),
    )
    assert result.exit_code != 0
    assert "does not match attestation" in result.output

    copied_root = tmp_path / "copied-provider"
    shutil.copytree(roots["bounded_provider_evidence"], copied_root)
    copied_root.chmod(0o700)
    for path in copied_root.rglob("*"):
        path.chmod(0o700 if path.is_dir() else 0o600)
    copied = roots | {"bounded_provider_evidence": copied_root}
    result = CliRunner().invoke(
        main,
        _finalize_args(run_dir, report, dataset_root, finalization_root, attestation, copied),
    )
    assert result.exit_code != 0
    assert "does not match attestation" in result.output


def test_finalize_rejects_relabelled_live_run_and_code_hash(tmp_path, monkeypatch):
    run_dir, report, dataset_root, finalization_root, attestation, roots, _ = _finalization_fixture(
        tmp_path, monkeypatch
    )
    raw = json.loads(report.read_text())
    raw["live_finalization"]["run_id"] = "bounded-validation"
    report.write_text(json.dumps(raw) + "\n")
    result = CliRunner().invoke(
        main,
        _finalize_args(run_dir, report, dataset_root, finalization_root, attestation, roots),
    )
    assert result.exit_code != 0
    assert "live finalization declaration is inconsistent" in result.output

    fixture = _finalization_fixture(tmp_path / "code-mismatch", monkeypatch)
    run_dir, report, dataset_root, finalization_root, attestation, roots, _ = fixture
    raw = json.loads(attestation.read_text())
    raw["validated_tree_sha256"] = "0" * 40
    attestation.unlink()
    attestation.write_text(json.dumps(raw) + "\n")
    attestation.chmod(0o600)
    result = CliRunner().invoke(
        main,
        _finalize_args(run_dir, report, dataset_root, finalization_root, attestation, roots),
    )
    assert result.exit_code != 0
    assert "provenance is not approved" in result.output

    fixture = _finalization_fixture(tmp_path / "expired", monkeypatch)
    run_dir, report, dataset_root, finalization_root, attestation, roots, _ = fixture
    raw = json.loads(attestation.read_text())
    raw["started_at"] = (START + timedelta(hours=2)).isoformat()
    raw["completed_at"] = (START + timedelta(hours=2, minutes=1)).isoformat()
    attestation.unlink()
    attestation.write_text(json.dumps(raw) + "\n")
    attestation.chmod(0o600)
    result = CliRunner().invoke(
        main,
        _finalize_args(
            run_dir,
            report,
            dataset_root,
            finalization_root,
            attestation,
            roots,
        ),
    )
    assert result.exit_code != 0
    assert "execution interval does not match permit evidence" in result.output


@pytest.mark.parametrize(
    ("kind", "reference_name", "manifest_name", "mutation"),
    [
        (
            "live_provider_evidence",
            "provider_manifest",
            "provider-manifest.json",
            lambda value: value | {"operation_count": 299},
        ),
        (
            "live_adapter_dataset",
            "adapter_manifest",
            "manifest.json",
            lambda value: value
            | {
                "counts": value["counts"]
                | {"events": 299, "by_origin": {"synthetic": 299}}
            },
        ),
        (
            "live_adapter_dataset",
            "adapter_manifest",
            "manifest.json",
            lambda value: value
            | {
                "generator": value["generator"]
                | {
                    "runtime": value["generator"]["runtime"]
                    | {"run_manifest_sha256": "0" * 64}
                }
            },
        ),
        (
            "live_adapter_dataset",
            "adapter_manifest",
            "manifest.json",
            lambda value: value
            | {
                "counts": value["counts"]
                | {"dropped_events": 1}
            },
        ),
        (
            "live_adapter_dataset",
            "adapter_manifest",
            "manifest.json",
            lambda value: value
            | {"known_gaps": [{"reason": "unexpected"}]},
        ),
        (
            "live_adapter_dataset",
            "adapter_report",
            "report/import.json",
            lambda value: value | {"validation_permit_sha256": "0" * 64},
        ),
        (
            "live_adapter_dataset",
            "adapter_report",
            "report/import.json",
            lambda value: value
            | {"approved_exclusions": [{"reason": "unexpected"}]},
        ),
        (
            "live_clean_dataset",
            "cleaned_manifest",
            "manifest.json",
            lambda value: value
            | {"parent": {"manifest_sha256": "0" * 64}},
        ),
        (
            "live_clean_dataset",
            "cleaned_manifest",
            "manifest.json",
            lambda value: value
            | {
                "parent": value["parent"]
                | {"dataset_id": "wrong-parent"}
            },
        ),
        (
            "live_clean_dataset",
            "cleaned_manifest",
            "manifest.json",
            lambda value: value
            | {"counts": value["counts"] | {"opens": 1}},
        ),
        (
            "live_clean_dataset",
            "cleaned_report",
            "report/quality.json",
            lambda value: value
            | {"exclusions_by_volume_day_reason": []},
        ),
        (
            "live_clean_dataset",
            "cleaned_report",
            "report/quality.json",
            lambda value: value
            | {"app_mix": {"wrong-app": 300}},
        ),
    ],
)
def test_finalize_rejects_semantically_stale_live_outputs(
    tmp_path,
    monkeypatch,
    kind,
    reference_name,
    manifest_name,
    mutation,
):
    run_dir, report, dataset_root, finalization_root, attestation, roots, _ = (
        _finalization_fixture(tmp_path, monkeypatch)
    )
    manifest_path = roots[kind] / manifest_name
    raw_manifest = json.loads(manifest_path.read_text())
    manifest_path.write_text(json.dumps(mutation(raw_manifest)) + "\n")
    manifest_path.chmod(0o600)
    resealed = _cleanup_root(kind, kind, roots[kind])
    raw_report = json.loads(report.read_text())
    raw_report["live_finalization"][reference_name]["sha256"] = sha256(
        manifest_path
    )
    raw_report["live_finalization"]["cleanup_roots"] = [
        resealed.model_dump(mode="json") if item["kind"] == kind else item
        for item in raw_report["live_finalization"]["cleanup_roots"]
    ]
    if kind == "live_clean_dataset" and manifest_name == "manifest.json":
        raw_report["output_hashes"]["manifest.json"] = sha256(manifest_path)
    if kind == "live_clean_dataset" and manifest_name == "report/quality.json":
        raw_report["validation_provenance"]["report/quality.json"] = sha256(
            manifest_path
        )
    report.write_text(json.dumps(raw_report) + "\n")
    result = CliRunner().invoke(
        main,
        _finalize_args(
            run_dir,
            report,
            dataset_root,
            finalization_root,
            attestation,
            roots,
        ),
    )
    assert result.exit_code != 0
    assert "live " in result.output


@pytest.mark.parametrize(
    "mutation",
    ["zero", "missing", "duplicate", "wrong_status", "wrong_count"],
)
def test_finalize_rejects_invalid_live_coverage(
    tmp_path,
    monkeypatch,
    mutation,
):
    run_dir, report, dataset_root, finalization_root, attestation, roots, _ = (
        _finalization_fixture(tmp_path, monkeypatch)
    )
    coverage_path = dataset_root / "coverage/coverage.jsonl"
    rows = [json.loads(line) for line in coverage_path.read_text().splitlines()]
    if mutation == "zero":
        rows = []
    elif mutation == "missing":
        rows = rows[:-1]
    elif mutation == "duplicate":
        rows[-1] = dict(rows[0])
    elif mutation == "wrong_status":
        rows[0]["status"] = "incomplete"
    else:
        rows[0]["offered"] = 59
        rows[0]["emitted"] = 59
    coverage_path.write_text("".join(json.dumps(row) + "\n" for row in rows))
    coverage_path.chmod(0o600)

    clean_manifest_path = dataset_root / "manifest.json"
    clean_manifest = json.loads(clean_manifest_path.read_text())
    for item in clean_manifest["files"]:
        if item["path"] == "coverage/coverage.jsonl":
            item["sha256"] = sha256(coverage_path)
    if mutation == "zero":
        clean_manifest["coverage"]["hours"] = 0
        clean_manifest["coverage"]["complete"] = 0
    clean_manifest_path.write_text(json.dumps(clean_manifest) + "\n")
    clean_manifest_path.chmod(0o600)

    quality_path = dataset_root / "report/quality.json"
    quality = json.loads(quality_path.read_text())
    if mutation == "zero":
        quality["coverage_hours"] = {"complete": 0}
    quality_path.write_text(json.dumps(quality) + "\n")
    quality_path.chmod(0o600)

    resealed = _cleanup_root(
        "live_clean_dataset",
        "live_clean_dataset",
        dataset_root,
    )
    raw_report = json.loads(report.read_text())
    raw_report["output_hashes"]["coverage/coverage.jsonl"] = sha256(coverage_path)
    raw_report["output_hashes"]["manifest.json"] = sha256(clean_manifest_path)
    raw_report["validation_provenance"]["report/quality.json"] = sha256(quality_path)
    raw_report["live_finalization"]["cleaned_manifest"]["sha256"] = sha256(
        clean_manifest_path
    )
    raw_report["live_finalization"]["cleaned_report"]["sha256"] = sha256(
        quality_path
    )
    raw_report["live_finalization"]["cleanup_roots"] = [
        resealed.model_dump(mode="json")
        if item["kind"] == "live_clean_dataset"
        else item
        for item in raw_report["live_finalization"]["cleanup_roots"]
    ]
    report.write_text(json.dumps(raw_report) + "\n")
    result = CliRunner().invoke(
        main,
        _finalize_args(
            run_dir,
            report,
            dataset_root,
            finalization_root,
            attestation,
            roots,
        ),
    )
    assert result.exit_code != 0
    assert "live clean" in result.output or "row count mismatch" in result.output


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        ("duplicate", "event identities"),
        ("event_time", "adapter event row"),
    ],
)
def test_finalize_rejects_invalid_adapter_event_identity_or_time(
    tmp_path,
    monkeypatch,
    mutation,
    message,
):
    run_dir, _, _, _, _, roots, _ = (
        _finalization_fixture(tmp_path, monkeypatch)
    )
    events_path = next(
        (roots["live_adapter_dataset"] / "events").rglob("part-00000.jsonl")
    )
    rows = [json.loads(line) for line in events_path.read_text().splitlines()]
    if mutation == "duplicate":
        rows[1]["event_id"] = rows[0]["event_id"]
    else:
        rows[0]["event_time"] = (
            datetime.fromisoformat(rows[0]["event_time"])
            + timedelta(seconds=1)
        ).isoformat()
    loss_rows = [
        json.loads(line)
        for line in (
            roots["live_adapter_dataset"] / "loss/loss.jsonl"
        ).read_text().splitlines()
    ]
    excluded_rows = [
        json.loads(line)
        for line in (
            next(
                (roots["live_clean_dataset"] / "excluded").rglob(
                    "part-00000.jsonl"
                )
            )
        ).read_text().splitlines()
    ]
    coverage_rows = [
        LiveCoverageRow.model_validate_json(line)
        for line in (
            roots["live_clean_dataset"] / "coverage/coverage.jsonl"
        ).read_text().splitlines()
    ]
    provider = LiveProviderDeclaration.model_validate_json(
        (
            roots["live_provider_evidence"] / "provider-manifest.json"
        ).read_text()
    )
    live_manifest = CollectionManifest.model_validate_json(
        (run_dir / "manifest.json").read_text()
    )
    live_catalog = FixtureCatalog.model_validate_json(
        (run_dir / "fixture-catalog.json").read_text()
    )
    root_evidence = {
        kind: _cleanup_root(kind, kind, path)
        for kind, path in roots.items()
    }
    with pytest.raises(click.ClickException, match=message):
        _validate_live_row_chain(
            root_evidence,
            live_manifest,
            live_catalog,
            provider,
            rows,
            loss_rows,
            excluded_rows,
            coverage_rows,
        )


def mount_operation(
    relative_path: str,
    *,
    count: int,
    offset: int,
    file_size: int,
    tag: str = "",
) -> OfferedOperation:
    return OfferedOperation(
        operation_id="9" * 64,
        operation_index=0,
        run_id="controlled-001",
        fixture_id="fixture-a",
        offered_at_utc=START,
        offered_at_monotonic_ns=1,
        volume_id=VOLUME_ID,
        file_id=FILE_A,
        dir_id="dir-a",
        export_id=7,
        relative_path=relative_path,
        file_size=file_size,
        offset=offset,
        count=count,
        client_id="kernel-client",
        tag=tag,
        timing_mode="wallclock",
    )


def test_controlled_mount_executor_success_short_eof_and_error(tmp_path):
    root = tmp_path / "mount"
    root.mkdir()
    (root / "data.bin").write_bytes(b"a" * 8192)
    executor = DirectMountExecutor(
        root,
        latency_guardrail_ms=250,
        allow_non_nfs_for_test=True,
    )
    success = executor.read(
        mount_operation("data.bin", count=4096, offset=0, file_size=8192)
    )
    assert success.status == "ok"
    assert success.bytes_read == 4096
    assert success.content_sha256 == __import__("hashlib").sha256(b"a" * 4096).hexdigest()
    assert not hasattr(success, "content")
    short = executor.read(
        mount_operation("data.bin", count=4096, offset=4096, file_size=8192)
    )
    assert short.status == "ok"
    assert short.bytes_read == 4096
    eof = executor.read(
        mount_operation("data.bin", count=4096, offset=8192, file_size=8192)
    )
    assert eof.status == "eof"
    missing = executor.read(
        mount_operation("missing.bin", count=4096, offset=0, file_size=4096)
    )
    assert missing.status == "error"


def test_controlled_mount_executor_rejects_escape_symlink_nonregular_and_tag(tmp_path):
    root = tmp_path / "mount"
    root.mkdir()
    outside = tmp_path / "outside.bin"
    outside.write_bytes(b"x")
    (root / "link.bin").symlink_to(outside)
    (root / "data.bin").write_bytes(b"x" * 4096)
    os.mkfifo(root / "fifo")
    executor = DirectMountExecutor(
        root,
        latency_guardrail_ms=250,
        allow_non_nfs_for_test=True,
    )
    assert executor.read(
        mount_operation("link.bin", count=4096, offset=0, file_size=4096)
    ).status == "error"
    assert executor.read(
        mount_operation("fifo", count=4096, offset=0, file_size=4096)
    ).status == "error"
    tagged = executor.read(
        mount_operation("data.bin", count=4096, offset=0, file_size=4096, tag="pc1:d:x:y")
    )
    assert tagged.status == "error"
    assert "empty tag" in tagged.error
    with pytest.raises(ValidationError):
        mount_operation("../outside.bin", count=4096, offset=0, file_size=4096)


def test_controlled_mount_executor_rejects_final_cross_device(monkeypatch, tmp_path):
    root = tmp_path / "mount"
    root.mkdir()
    (root / "data.bin").write_bytes(b"x" * 4096)
    executor = DirectMountExecutor(
        root,
        latency_guardrail_ms=250,
        allow_non_nfs_for_test=True,
    )
    real_fstat = os.fstat

    def cross_device_for_regular(fd):
        value = real_fstat(fd)
        if stat.S_ISREG(value.st_mode):
            fields = list(value)
            fields[2] = value.st_dev + 1
            return os.stat_result(fields)
        return value

    monkeypatch.setattr(os, "fstat", cross_device_for_regular)
    result = executor.read(
        mount_operation("data.bin", count=4096, offset=0, file_size=4096)
    )
    assert result.status == "error"
    assert "nested mount" in result.error


def test_observed_event_rejects_zero_sequence_and_operation_alias():
    operation = mount_operation("data.bin", count=4096, offset=0, file_size=4096)
    values = observed(operation, "event-1").model_dump()
    with pytest.raises(ValidationError):
        ObservedEvent.model_validate(values | {"producer_sequence": 0})
    with pytest.raises(ValidationError, match="distinct"):
        ObservedEvent.model_validate(
            values | {"server_operation_id": operation.operation_id}
        )


def test_controlled_mount_cancellation_is_durable_and_stops_runner(tmp_path):
    root = private_dir(tmp_path)
    mount = tmp_path / "mount"
    (mount / "fixtures").mkdir(parents=True)
    (mount / "fixtures/a.bin").write_bytes(b"x" * 4096)
    executor = DirectMountExecutor(
        mount,
        latency_guardrail_ms=250,
        cancelled=lambda: True,
        allow_non_nfs_for_test=True,
    )

    class Provider:
        def collect(self, scheduled_at):
            return [
                checkpoint(
                    root,
                    scheduled_at=scheduled_at,
                    captured_at=scheduled_at,
                )
            ]

        def health(self, operation):
            return [
                PreReadHealth(
                    run_id=operation.run_id,
                    operation_id=operation.operation_id,
                    captured_at=operation.offered_at_utc,
                    pod_name="collector-0",
                    pod_uid=POD_UID,
                    node_name="node-1",
                    producer_epoch=EPOCH,
                    last_enqueued_sequence=1,
                    last_synced_sequence=1,
                    synced_count=1,
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
                    collector_sink_healthy=True,
                    storage_healthy=True,
                    cluster_healthy=True,
                    volume_healthy=True,
                    export_healthy=True,
                    parse_errors=0,
                    identity_mismatches=0,
                    checksum_mismatches=0,
                    foreground_read_errors=0,
                )
            ]

    with pytest.raises(RuntimeError, match="foreground read error"):
        CollectionRunner(now=lambda: START, sleep=lambda _: None).run(
            manifest(offered_count=1, tag_mode="empty"),
            catalog(),
            root,
            executor,
            checkpoint_provider=Provider(),
        )
    assert len(read_models(root / "offered.jsonl", OfferedOperation)) == 1
    results = read_models(root / "execution.jsonl", ExecutionResult)
    assert len(results) == 1
    assert results[0].status == "cancelled"


def test_supervisor_collects_full_cadence_with_fake_provider(tmp_path):
    root = private_dir(tmp_path)
    count = CollectionRunner(sleep=lambda _: None).run(
        manifest(offered_count=1, tag_mode="empty"),
        catalog(),
        root,
        DryRunExecutor(lambda: START),
        simulated_timing=True,
        checkpoint_provider=FakeProvider(root),
    )
    assert count == 1
    assert len(read_models(root / "checkpoints.jsonl", Checkpoint)) == 21
    canonical = read_models(root / "observed.jsonl", ObservedEvent)
    assert len(canonical) == 1
    assert canonical[0].operation_id is not None


def test_pre_read_provider_runs_before_offered_ledger_append(tmp_path):
    root = private_dir(tmp_path)

    class OrderingProvider(FakeProvider):
        def health(self, operation):
            ledger = root / "offered.jsonl"
            if ledger.exists():
                assert operation.operation_id not in {
                    item.operation_id for item in read_models(ledger, OfferedOperation)
                }
            return super().health(operation)

    CollectionRunner(sleep=lambda _: None).run(
        manifest(offered_count=1, tag_mode="empty"),
        catalog(),
        root,
        DryRunExecutor(lambda: START),
        simulated_timing=True,
        checkpoint_provider=OrderingProvider(root),
    )


@pytest.mark.parametrize(
    ("provider", "message"),
    [
        (lambda root: FakeProvider(root, late=True), "late checkpoint"),
        (lambda root: FakeProvider(root, missing=True), "missing checkpoint"),
        (lambda root: FakeProvider(root, fault=True), "pre-read health barrier failed"),
    ],
)
def test_supervisor_stops_before_read_on_checkpoint_or_health_failure(
    tmp_path,
    provider,
    message,
):
    root = private_dir(tmp_path)
    with pytest.raises(RuntimeError, match=message):
        CollectionRunner(sleep=lambda _: None).run(
            manifest(offered_count=1, tag_mode="empty"),
            catalog(),
            root,
            DryRunExecutor(lambda: START),
            simulated_timing=True,
            checkpoint_provider=provider(root),
        )
    assert not (root / "offered.jsonl").exists()


def test_supervisor_polls_persistent_stop_before_read(tmp_path):
    root = private_dir(tmp_path)
    with pytest.raises(RuntimeError, match="operator stop"):
        CollectionRunner(sleep=lambda _: None).run(
            manifest(offered_count=1, tag_mode="empty"),
            catalog(),
            root,
            DryRunExecutor(lambda: START),
            simulated_timing=True,
            checkpoint_provider=FakeProvider(root),
            persistent_stop=lambda: "operator stop",
        )
    assert not (root / "offered.jsonl").exists()


def test_post_read_final_event_must_be_durable_before_end(tmp_path):
    root = private_dir(tmp_path)

    with pytest.raises(RuntimeError, match="durable timestamp"):
        CollectionRunner(sleep=lambda _: None).run(
            manifest(offered_count=1, tag_mode="empty"),
            catalog(),
            root,
            DryRunExecutor(lambda: START),
            simulated_timing=True,
            checkpoint_provider=FakeProvider(
                root,
                durable_at=START + timedelta(hours=5),
            ),
        )


def test_command_provider_uses_deployment_checkpoint_and_probe_contract(monkeypatch, tmp_path):
    root = private_dir(tmp_path)
    operation = mount_operation("data.bin", count=4096, offset=0, file_size=4096)
    calls = []

    def fake_run(arguments, **kwargs):
        calls.append(arguments)
        if "checkpoint" in arguments:
            output = checkpoint(root).model_dump_json()
        elif "probe" in arguments:
            output = FakeProvider(root).health(operation)[0].model_dump_json()
        else:
            result = DryRunExecutor(lambda: START).read(operation)
            output = FakeProvider(root).post_read(operation, result)[0].model_dump_json()
        return type("Completed", (), {"stdout": output + "\n"})()

    monkeypatch.setattr("predictive_cache.collection.harness.subprocess.run", fake_run)
    script = tmp_path / "cluster2-collection-provider.py"
    package = tmp_path / "provider-package.zip"
    script.write_text("provider\n")
    package.write_text("package\n")
    provider = CommandCheckpointProvider(
        script,
        package,
        root,
        script_sha256=sha256(script),
        package_sha256=sha256(package),
    )
    assert provider.collect(START)
    assert provider.health(operation)
    script.unlink()
    script.write_text("replacement\n")
    package.unlink()
    package.write_text("replacement\n")
    with os.fdopen(os.dup(provider._script_fd), "r") as stream:
        assert stream.read() == "provider\n"
    with os.fdopen(os.dup(provider._package_fd), "r") as stream:
        assert stream.read() == "package\n"
    result = DryRunExecutor(lambda: START).read(operation)
    assert provider.post_read(operation, result)
    prefix = [
        "python3",
        "-I",
        "-S",
        "-c",
        ISOLATED_PROVIDER_BOOTSTRAP,
        f"/proc/self/fd/{provider._package_fd}",
        f"/proc/self/fd/{provider._script_fd}",
    ]
    assert calls[0] == [
        *prefix,
        "checkpoint",
        root.absolute(),
        "--scheduled-at",
        "2026-11-04T00:00:00Z",
    ]
    assert calls[1] == [
        *prefix,
        "probe",
        root.absolute(),
        "--operation-id",
        operation.operation_id,
    ]
    assert calls[2] == [
        *prefix,
        "post-read",
        root.absolute(),
        "--operation-id",
        operation.operation_id,
    ]
    provider.close()
