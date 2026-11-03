from __future__ import annotations

import hashlib
import json
import os
import stat
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from click.testing import CliRunner
from pydantic import ValidationError

from predictive_cache.collection.cli import (
    APPROVED_COLLECTOR_SHA256,
    APPROVED_PROVIDER_PACKAGE_SHA256,
    APPROVED_PROVIDER_SHA256,
    APPROVED_VALIDATION_RUNBOOK_SHA256,
    APPROVED_VALIDATOR_SHA256,
    main,
)
from predictive_cache.collection.harness import (
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
    Checkpoint,
    CleanupCompletionRecord,
    CleanupScheduleRecord,
    CollectionManifest,
    CollectorDurableEvent,
    CollectorFileManifest,
    CollectorSourceFile,
    DropCounters,
    ExecutionResult,
    FixtureCatalog,
    FixtureEntry,
    JsonlEvidence,
    ObservedEvent,
    OfferedOperation,
    PreReadHealth,
    ReconciliationReport,
    RetentionPolicy,
    SoftwareVersions,
    StopRules,
)

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
        raw_record = (
            '{"client_ref":"'
            + ("c" * 64)
            + '","compound_op_index":0,"compound_tag_b64":"","compound_tag_id":"'
            + hashlib.sha256(b"").hexdigest()
            + '","compound_tag_original_length":0,"compound_tag_truncated":false,'
            '"eof":false,"event_time_ns":"1",'
            f'"export_id":{operation.export_id},"export_name_b64":"Y29udHJvbGxlZA",'
            f'"file_id":"{operation.file_id}","file_size":"{operation.file_size}",'
            f'"file_size_valid":true,"local_id":"{event_id}",'
            f'"identity_valid":true,"nfs_status":0,'
            f'"operation_id":"{operation.operation_index + 900:064x}",'
            f'"origin_claim":"unknown","origin_trust":"client_asserted",'
            f'"outcome":"success","producer_epoch":"{EPOCH}",'
            f'"producer_sequence":"{operation.operation_index + 1}",'
            f'"requested_count":{operation.count},"requested_offset":"{operation.offset}",'
            f'"returned_bytes":{result.bytes_read},"sink_time_ns":"{operation.operation_index}",'
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
                sink_time_ns=operation.operation_index,
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
    )
    assert completion.completed_at < completion.delete_by


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


def _finalization_fixture(tmp_path: Path):
    tmp_path.mkdir(parents=True, exist_ok=True)
    source = FIXTURE_DIR
    live_manifest = json.loads((source / "frozen-manifest.json").read_text())
    live_manifest["offered_count"] = 300
    live_manifest["offered_rate_per_s"] = 1 / 60
    live_manifest["stop_rules"]["latency_guardrail_ms"] = 5000
    manifest_path = tmp_path / "live-manifest.json"
    manifest_path.write_text(json.dumps(live_manifest) + "\n")
    manifest_path.chmod(0o600)
    run_dir = tmp_path / "run"
    result = CliRunner().invoke(
        main,
        [
            "init",
            "--manifest",
            str(manifest_path),
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
    atomic_write_model(
        run_dir / "collector-file-manifest.json",
        source_manifest(run_dir, run_id="controlled-demo"),
    )
    for name in (
        "offered.jsonl",
        "execution.jsonl",
        "durable-events.jsonl",
        "observed.jsonl",
        "pre-read-health.jsonl",
        "checkpoints.jsonl",
    ):
        path = run_dir / name
        path.write_text("{}\n")
        path.chmod(0o600)
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
    output = dataset_root / "events.jsonl"
    output.write_text('{"event":"ok"}\n')
    output.chmod(0o600)
    provenance = dataset_root / "validation.json"
    provenance.write_text('{"status":"passed"}\n')
    provenance.chmod(0o600)
    validation_root = tmp_path / "validation-evidence"
    validation_root.mkdir(mode=0o700)
    provider = validation_root / "provider.jsonl"
    provider.write_text('{"provider":"ok"}\n')
    provider.chmod(0o600)
    validation_manifest = validation_root / "validation-manifest.json"
    validation_manifest.write_text(
        json.dumps(
            {
                "validation_evidence_version": 1,
                "run_id": "controlled-demo",
                "full_manifest_sha256": sha256(run_dir / "manifest.json"),
                "provider_package_sha256": APPROVED_PROVIDER_PACKAGE_SHA256,
                "provider_sha256": APPROVED_PROVIDER_SHA256,
                "collector_sha256": APPROVED_COLLECTOR_SHA256,
                "validator_sha256": APPROVED_VALIDATOR_SHA256,
                "runbook_sha256": APPROVED_VALIDATION_RUNBOOK_SHA256,
                "validation_operations": 1,
                "status": "success",
                "files": {"provider.jsonl": sha256(provider)},
            }
        )
        + "\n"
    )
    validation_manifest.chmod(0o600)
    report = tmp_path / "report.json"
    report.write_text(
        json.dumps(
            {
                "run_id": "controlled-demo",
                "status": "success",
                "output_hashes": {"events.jsonl": sha256(output)},
                "validation_provenance": {
                    "validation.json": sha256(provenance),
                },
            }
        )
        + "\n"
    )
    report.chmod(0o600)
    return run_dir, report, dataset_root, validation_root, output


def _finalize_args(run_dir, report, dataset_root, validation_root):
    return [
        "finalize",
        "--run-dir",
        str(run_dir),
        "--dataset-report",
        str(report),
        "--dataset-root",
        str(dataset_root),
        "--validation-evidence-root",
        str(validation_root),
    ]


def test_finalize_requires_verified_outputs_and_reconciliation(tmp_path):
    run_dir, report, dataset_root, validation_root, _ = _finalization_fixture(tmp_path)
    result = CliRunner().invoke(
        main,
        _finalize_args(run_dir, report, dataset_root, validation_root),
    )
    assert result.exit_code == 0, result.output
    schedule = CleanupScheduleRecord.model_validate_json(
        (run_dir / "cleanup-schedule.json").read_text()
    )
    assert schedule.delete_by == schedule.finalized_at + timedelta(days=7)
    inventory = (run_dir / "deletion-inventory.json").read_text()
    assert '"reconciliation.json"' in inventory
    assert '"validated_dataset"' in inventory
    assert '"bounded_validation_evidence"' in inventory


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
    monkeypatch.setattr(
        "predictive_cache.collection.cli.CommandCheckpointProvider",
        lambda *args, **kwargs: object(),
    )
    monkeypatch.setattr(
        "predictive_cache.collection.cli.subprocess.run",
        lambda args, **kwargs: subprocess_calls.append(args),
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
            tmp_path / "adapter",
            run_dir,
            run_dir / "adapter-validation-permit.json",
        ],
        [tmp_path / "cleaner", run_dir],
    ]


def test_finalize_rejects_forged_and_tampered_dataset_hashes(tmp_path):
    run_dir, report, dataset_root, validation_root, output = _finalization_fixture(tmp_path)
    raw = json.loads(report.read_text())
    raw["output_hashes"]["events.jsonl"] = "0" * 64
    report.write_text(json.dumps(raw) + "\n")
    result = CliRunner().invoke(
        main,
        _finalize_args(run_dir, report, dataset_root, validation_root),
    )
    assert result.exit_code != 0
    assert "do not match actual outputs" in result.output

    raw["output_hashes"]["events.jsonl"] = sha256(output)
    report.write_text(json.dumps(raw) + "\n")
    output.write_text('{"event":"tampered"}\n')
    result = CliRunner().invoke(
        main,
        _finalize_args(run_dir, report, dataset_root, validation_root),
    )
    assert result.exit_code != 0
    assert "do not match actual outputs" in result.output


def test_finalize_rejects_missing_or_symlinked_outputs(tmp_path):
    run_dir, report, dataset_root, validation_root, output = _finalization_fixture(tmp_path)
    output.unlink()
    result = CliRunner().invoke(
        main,
        _finalize_args(run_dir, report, dataset_root, validation_root),
    )
    assert result.exit_code != 0

    target = tmp_path / "outside.jsonl"
    target.write_text('{"event":"outside"}\n')
    target.chmod(0o600)
    output.symlink_to(target)
    result = CliRunner().invoke(
        main,
        _finalize_args(run_dir, report, dataset_root, validation_root),
    )
    assert result.exit_code != 0
    assert "must not contain symlinks" in result.output


def test_finalize_rejects_unsafe_external_validation_root(tmp_path):
    run_dir, report, dataset_root, validation_root, _ = _finalization_fixture(tmp_path)
    validation_root.chmod(0o755)
    result = CliRunner().invoke(
        main,
        _finalize_args(run_dir, report, dataset_root, validation_root),
    )
    assert result.exit_code != 0
    assert "must be owner-only" in result.output

    validation_root.chmod(0o700)
    link = tmp_path / "validation-link"
    link.symlink_to(validation_root, target_is_directory=True)
    result = CliRunner().invoke(
        main,
        _finalize_args(run_dir, report, dataset_root, link),
    )
    assert result.exit_code != 0
    assert "must not traverse symlinks" in result.output


def test_finalize_rejects_missing_or_failed_reconciliation(tmp_path):
    run_dir, report, dataset_root, validation_root, _ = _finalization_fixture(tmp_path)
    (run_dir / "reconciliation.json").unlink()
    result = CliRunner().invoke(
        main,
        _finalize_args(run_dir, report, dataset_root, validation_root),
    )
    assert result.exit_code != 0
    assert "full-run evidence is incomplete" in result.output

    fixture = _finalization_fixture(tmp_path / "failed")
    run_dir, report, dataset_root, validation_root, _ = fixture
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
        _finalize_args(run_dir, report, dataset_root, validation_root),
    )
    assert result.exit_code != 0
    assert "successful reconciliation evidence is required" in result.output


def test_finalize_authenticates_external_validation_manifest(tmp_path):
    run_dir, report, dataset_root, validation_root, _ = _finalization_fixture(tmp_path)
    validation_manifest = validation_root / "validation-manifest.json"
    raw = json.loads(validation_manifest.read_text())
    raw["provider_sha256"] = "0" * 64
    validation_manifest.write_text(json.dumps(raw) + "\n")
    result = CliRunner().invoke(
        main,
        _finalize_args(run_dir, report, dataset_root, validation_root),
    )
    assert result.exit_code != 0
    assert "not approved" in result.output

    fixture = _finalization_fixture(tmp_path / "tampered")
    run_dir, report, dataset_root, validation_root, _ = fixture
    (validation_root / "provider.jsonl").write_text('{"provider":"tampered"}\n')
    result = CliRunner().invoke(
        main,
        _finalize_args(run_dir, report, dataset_root, validation_root),
    )
    assert result.exit_code != 0
    assert "do not match actual files" in result.output


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
    provider = CommandCheckpointProvider(
        tmp_path / "cluster2-collection-provider.py",
        root,
    )
    assert provider.collect(START)
    assert provider.health(operation)
    result = DryRunExecutor(lambda: START).read(operation)
    assert provider.post_read(operation, result)
    assert calls[0] == [
        "python3",
        (tmp_path / "cluster2-collection-provider.py").absolute(),
        "checkpoint",
        root.absolute(),
        "--scheduled-at",
        "2026-11-04T00:00:00Z",
    ]
    assert calls[1] == [
        "python3",
        (tmp_path / "cluster2-collection-provider.py").absolute(),
        "probe",
        root.absolute(),
        "--operation-id",
        operation.operation_id,
    ]
    assert calls[2] == [
        "python3",
        (tmp_path / "cluster2-collection-provider.py").absolute(),
        "post-read",
        root.absolute(),
        "--operation-id",
        operation.operation_id,
    ]
