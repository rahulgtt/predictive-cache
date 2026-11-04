from __future__ import annotations

import hashlib
from collections import Counter, defaultdict, deque
from datetime import UTC, datetime
from pathlib import Path

import pytest
from pydantic import ValidationError

from predictive_cache.collection.io import canonical_json
from predictive_cache.collection.models import (
    CollectionManifest,
    FixtureCatalog,
    FixtureEntry,
    parse_collection_manifest,
)
from predictive_cache.collection.workload import (
    DEFAULT_RANGE_OFFSETS,
    OfflineSimulatedRead,
    WorkloadConfig,
    WorkloadExecutionPlan,
    aggregate_recovery,
    build_default_workload,
    build_offline_simulated_source,
    checkpoint_schedule,
    execution_plan_sha256,
    materialize_offered_operations,
    materialize_offline_execution_results,
    resolved_operation_count,
    run_bound_operation_id,
    validate_plan_truth,
    workload_truth_sha256,
)

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
