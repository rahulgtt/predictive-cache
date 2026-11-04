"""Fixture integration only: this suite must never issue NFS reads."""

from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import MappingProxyType, SimpleNamespace

import numpy as np
import pyarrow as pa
import pytest
import yaml

from predictive_cache.controller.models import ModelBundle, ModelManager
from predictive_cache.controller.planner import PlannerPublisher, PublicationError
from predictive_cache.controller.read_memory import CoverageObservation, ReadMemory
from predictive_cache.controller.read_runtime import (
    ReadReplayRuntime,
    ReadRuntimeError,
    SyntheticTargetResolver,
)
from predictive_cache.dispatcher.core import Dispatcher, DispatcherConfig
from predictive_cache.ml.baselines import ReadDemandFrequencyScorer
from predictive_cache.ml.package import COACCESS_SCHEMA, DEFAULT_SERVING_POLICY, write_package
from predictive_cache.ml.read_demand import READ_DEMAND_SERVING_CONTRACT, feature_schema
from predictive_cache.semantics.read_demand import SEMANTIC_VIEW_ID, ReadDemandEvent

T = datetime(2026, 11, 4, 9, tzinfo=UTC)


def event(event_id="event-a", file_id="file-a", *, at=T - timedelta(minutes=10), **changes):
    return ReadDemandEvent.model_validate(
        {
            "event_id": event_id,
            "event_time": at,
            "known_at": at,
            "volume_id": "vol-1",
            "file_id": file_id,
            "dir_id": "dir-fixture",
            "offset": 0,
            "bytes_requested": 64,
            "bytes_read": 64,
            "file_size": 4096,
            "origin": "synthetic",
            "demand_class": "fixture_demand",
            "origin_trust": "fixture",
            "coverage_status": "complete",
            "metadata_status": "fixture",
            "data_kind": "synthetic",
            **changes,
        }
    )


def coverage(*, days=7):
    return [
        CoverageObservation(
            volume_id="vol-1",
            hour_start=T - timedelta(hours=hour),
            status="complete",
            known_at=T - timedelta(hours=hour - 1),
            observed_through=T - timedelta(hours=hour - 1),
        )
        for hour in range(days * 24, 0, -1)
    ]


def checkpoint(t):
    return CoverageObservation(
        volume_id="vol-1", hour_start=T, status="complete", known_at=t, observed_through=t
    )


class Models:
    def __init__(self, bundle):
        self.bundle, self.staged = bundle, None
        self.calls = []
        self.reasons = []

    def poll(self):
        self.calls.append("poll")

    def activate(self):
        self.calls.append("activate")
        if self.staged is not None:
            self.bundle, self.staged = self.staged, None

    def active(self):
        return self.bundle

    def readiness(self):
        return ("ready", self.reasons) if self.bundle is not None else ("not_ready", ["no_model"])

    def plans_allowed(self):
        return self.bundle is not None and "contract_mismatch" not in self.reasons


def bundle(memory, *, scorer=None, version="model-fixture", policy=None, coaccess=None):
    return ModelBundle(
        version,
        "fixture-package",
        scorer or ReadDemandFrequencyScorer(),
        coaccess if coaccess is not None else pa.Table.from_pylist([], schema=COACCESS_SCHEMA),
        MappingProxyType(dict(DEFAULT_SERVING_POLICY, **(policy or {}))),
        memory.contract,
    )


@pytest.fixture
def rig(tmp_path):
    resources = []

    def create(*, models=None, memory=None, monotonic=None, max_ticks=1000, package=False):
        root = tmp_path / f"out-{len(resources)}"
        root.mkdir(mode=0o700)
        clock = SimpleNamespace(value=T)
        memory = memory if memory is not None else ReadMemory("vol-1", verified_file_size=True)
        if models is None:
            pointer = tmp_path / f"pointer-{len(resources)}.yaml"
            if package:
                rows = pa.table(
                    {
                        "volume_id": ["vol-1"],
                        "file_id": ["file-a"],
                        "t": [T],
                        "read_count_24h": [1.0],
                    }
                )
                archive = write_package(
                    ReadDemandFrequencyScorer(),
                    feature_schema(),
                    None,
                    rows,
                    {
                        "semantic": {"semantic_view_id": SEMANTIC_VIEW_ID},
                        "feature_version": "read-demand-fv1",
                        "serving_contract": {"config": READ_DEMAND_SERVING_CONTRACT},
                        "serving_policy": dict(DEFAULT_SERVING_POLICY),
                        "data": {"data_kind": "synthetic"},
                        "training": {"params": {}},
                    },
                    tmp_path / f"package-{len(resources)}",
                )
                pointer.write_text(
                    yaml.safe_dump(
                        {
                            "model_uri": archive.as_uri(),
                            "feature_version": "read-demand-fv1",
                            "fallback": "baseline-read-frequency",
                        }
                    )
                )
            models = ModelManager(
                pointer, contract=memory.contract, clock=lambda: clock.value.timestamp()
            )
        dispatcher = Dispatcher(
            DispatcherConfig(
                root=root, warmer_command=root / "unused", host="192.0.2.1", real_reads=False
            ),
            now=lambda: clock.value,
        )
        publisher = PlannerPublisher(root, run_id="run-fixture", controller_id="fixture-controller")
        kwargs = {"monotonic": monotonic} if monotonic is not None else {}
        runtime = ReadReplayRuntime(
            memory, models, publisher, dispatcher, max_ticks=max_ticks, **kwargs
        )
        item = SimpleNamespace(
            runtime=runtime,
            memory=memory,
            models=models,
            publisher=publisher,
            dispatcher=dispatcher,
            root=root,
            clock=clock,
        )

        def tick(t=T, **args):
            clock.value = t
            return runtime.tick(t, **args)

        item.tick = tick
        resources.append(item)
        return item

    yield create
    for item in resources:
        item.dispatcher.close()
        if isinstance(item.models, ModelManager):
            item.models.close()


def test_verified_model_planner_dispatcher_feedback_and_health(rig, monkeypatch):
    def forbidden(*_args, **_kwargs):
        raise AssertionError("dry run must not start a Warmer")

    monkeypatch.setattr("subprocess.Popen", forbidden)
    item = rig(package=True)
    result = item.tick(events=[event()], coverage=coverage())
    assert result.scored and result.plans_allowed
    assert result.planned_items == result.results == 1 and result.planned_bytes == 4096
    assert result.model_version.startswith("baseline-read-frequency@")
    assert result.as_dict()["t"].endswith("Z")
    summary = item.runtime.summary(T)
    assert summary["data_kind"] == "synthetic" and summary["evidence_mode"] == "fixture"
    assert summary["health"]["status"] == "ready"
    row = summary["feedback"]["runs"][0]
    assert row["requested_bytes"] == 4096 and row["statuses"] == {"skipped": 1}
    assert row["completed_bytes_sum"] == row["useful_bytes"] == 0
    assert summary["feedback"]["counters"]["excluded_reads"] == 1
    assert summary["timing"]["samples"] == 1
    assert not list(item.root.glob("runs/**/tasks"))
    assert "fixture-export" not in json.dumps(summary)


def test_synthetic_targets_keep_original_seen_at_and_expire(rig):
    item = rig()
    row = event()
    item.tick(events=[row], coverage=coverage())
    resolver = SyntheticTargetResolver(item.memory, target_max_age_s=1200)
    one = resolver.resolve("vol-1", "file-a", T)
    two = resolver.resolve("vol-1", "file-a", T + timedelta(seconds=1))
    assert one.seen_at == two.seen_at == row.known_at
    assert one.target_fh_b64 == two.target_fh_b64
    assert one.target_fh_b64 not in repr(one)
    assert resolver.resolve("vol-1", "file-a", T + timedelta(seconds=600)) is None
    assert resolver.resolve("vol-other", "file-a", T) is None


def test_null_recency_suppresses_even_valid_frequency_scores(rig):
    item = rig()
    result = item.tick(events=[event()], coverage=coverage(days=1))
    assert result.scored and not result.plans_allowed and result.planned_items == 0
    assert item.runtime.summary(T)["prediction_omissions"] == 1
    assert "memory_gap" in item.runtime.health.snapshot()["reasons"]
    assert not list((item.root / "plans").glob("*.json"))
    assert next((item.root / "predictions").glob("*.jsonl")).read_text() == ""


def test_missing_required_coverage_reports_unavailable_without_zero_scores(rig):
    item = rig()
    result = item.tick(events=[event()])
    assert not result.scored and not result.plans_allowed and result.results == 0
    assert not item.runtime.failed
    assert item.runtime.summary(T)["score_unavailable_ticks"] == 1


def test_coverage_completeness_withdraws_and_recovers(rig):
    item = rig()
    item.tick(events=[event()], coverage=coverage())
    second = item.tick(T + timedelta(seconds=1))
    assert not second.plans_allowed
    third = item.tick(T + timedelta(seconds=2), coverage=[checkpoint(T + timedelta(seconds=2))])
    assert third.plans_allowed and third.planned_items == 0  # existing cooldown remains


def test_memory_capacity_is_visible_and_gates_publication(rig):
    item = rig(memory=ReadMemory("vol-1", max_events=1))
    result = item.tick(events=[event(), event("event-b", "file-b")], coverage=coverage())
    assert not result.plans_allowed and item.memory.gap
    assert "pcache_controller_dropped_overflow_total 1" in item.runtime.health.metrics()


def test_coverage_loss_does_not_inflate_read_exclusion_counters(rig):
    item = rig(memory=ReadMemory("vol-1", max_coverage=1))
    result = item.tick(events=[event()], coverage=coverage()[:2])
    assert not result.plans_allowed and item.memory.gap
    assert item.memory.stats()["excluded_by_reason"]["overflow"] == 1
    metrics = item.runtime.health.metrics()
    assert "pcache_controller_events_received_total 1\n" in metrics
    assert "pcache_controller_events_excluded_total 0\n" in metrics


def test_bundle_swap_changes_policy_scorer_coaccess_and_cadence_together(rig):
    memory = ReadMemory("vol-1", verified_file_size=True)
    snapshots = []
    original = memory.snapshot

    def snapshot(t, coaccess=None):
        snapshots.append(coaccess)
        return original(t, coaccess)

    memory.snapshot = snapshot
    models = Models(None)
    old_table = pa.Table.from_pylist([], schema=COACCESS_SCHEMA)
    new_table = pa.Table.from_pylist([], schema=COACCESS_SCHEMA)

    class OldScorer:
        feature_columns = ("read_count_24h",)

        def score(self, rows):
            models.staged = newer
            return np.ones(rows.num_rows)

    newer = bundle(
        memory,
        version="new-model",
        policy={"tick_s": 120, "cooldown_s": 0, "max_items": 2, "range_cap": 16},
        coaccess=new_table,
    )
    models.bundle = bundle(
        memory,
        version="old-model",
        scorer=OldScorer(),
        policy={"max_items": 1, "range_cap": 32},
        coaccess=old_table,
    )
    item = rig(models=models, memory=memory)
    first = item.tick(events=[event(), event("event-b", "file-b")], coverage=coverage())
    assert first.model_version == "old-model" and first.tick_s == 60 and first.planned_items == 1
    second = item.tick(T + timedelta(seconds=60), coverage=[checkpoint(T + timedelta(seconds=60))])
    assert (
        second.model_version == "new-model" and second.tick_s == 120 and second.planned_items == 2
    )
    assert snapshots[0] is old_table and snapshots[1] is new_table
    assert models.calls == ["poll", "activate", "poll", "activate"]


@pytest.mark.parametrize(
    "scores",
    [
        np.array([np.nan]),
        np.array([np.inf]),
        np.array([True]),
        np.array(["0.8"]),
        np.array([1.0, 2.0]),
        np.array([[1.0]]),
    ],
)
def test_invalid_model_scores_fail_closed_and_stop_later_ticks(rig, scores):
    memory = ReadMemory("vol-1")

    class Scorer:
        feature_columns = ("read_count_24h",)

        def score(self, rows):
            return scores

    item = rig(models=Models(bundle(memory, scorer=Scorer())), memory=memory)
    with pytest.raises(ReadRuntimeError):
        item.tick(events=[event()], coverage=coverage())
    assert item.runtime.failed and item.publisher.sequence == 0
    with pytest.raises(ReadRuntimeError, match="stopped"):
        item.tick(T + timedelta(seconds=60))


def test_no_model_and_contract_mismatch_are_health_gates(rig):
    memory = ReadMemory("vol-1")
    item = rig(models=Models(None), memory=memory)
    result = item.tick(events=[event()], coverage=coverage())
    assert not result.plans_allowed and not result.scored
    assert "no_scorer" in item.runtime.health.snapshot()["reasons"]
    other = ReadMemory("vol-1", verified_file_size=True)
    mismatch = rig(models=Models(bundle(other)), memory=ReadMemory("vol-1"))
    result = mismatch.tick(events=[event()], coverage=coverage())
    assert not result.plans_allowed and not result.scored
    assert "contract_mismatch" in mismatch.runtime.health.snapshot()["reasons"]


def test_external_readiness_gates_are_preserved(rig):
    item = rig()
    item.runtime.health.set_state(started=True, has_scorer=True, reasons=["restore_failed"])
    result = item.tick(events=[event()], coverage=coverage())
    assert not result.plans_allowed
    assert "restore_failed" in item.runtime.health.snapshot()["reasons"]


def test_overrun_reports_skip_instruction_with_injected_duration(rig):
    times = iter([0.0, 50.0, 60.0, 61.0])
    item = rig(monotonic=lambda: next(times))
    result = item.tick(events=[event()], coverage=coverage())
    assert result.duration_s == 50 and result.overrun and result.tick_s == 60
    result = item.tick(
        T + timedelta(seconds=120), coverage=[checkpoint(T + timedelta(seconds=120))]
    )
    assert not result.overrun
    assert "pcache_controller_tick_overruns_total 1" in item.runtime.health.metrics()


def test_publication_failure_latches_runtime_without_retry(rig, monkeypatch):
    item = rig()
    calls = []

    def fail(*args, **kwargs):
        calls.append(1)
        raise PublicationError("PRIVATE_HANDLE_AND_PATH")

    monkeypatch.setattr(item.publisher, "publish", fail)
    with pytest.raises(ReadRuntimeError) as error:
        item.tick(events=[event()], coverage=coverage())
    assert "PRIVATE_HANDLE" not in str(error.value)
    with pytest.raises(ReadRuntimeError):
        item.tick(T + timedelta(seconds=60))
    assert calls == [1] and item.runtime.failed


@pytest.mark.parametrize("tamper", ["identity", "symlink", "oversized", "directory_mode"])
def test_result_join_rejects_untrusted_evidence(rig, monkeypatch, tmp_path, tamper):
    item = rig()
    original = item.dispatcher.run_once

    def run_once():
        outcome = original()
        path = next(item.root.glob("runs/**/results/*.json"))
        if tamper == "identity":
            data = json.loads(path.read_text())
            data["task_id"] = "different-task"
            path.write_text(json.dumps(data))
        elif tamper == "symlink":
            outside = tmp_path / "outside.json"
            outside.write_bytes(path.read_bytes())
            outside.chmod(0o600)
            path.unlink()
            path.symlink_to(outside)
        elif tamper == "oversized":
            path.write_bytes(b" " * (256 * 1024 + 1))
        else:
            path.parent.chmod(0o755)
        return outcome

    monkeypatch.setattr(item.dispatcher, "run_once", run_once)
    with pytest.raises(ReadRuntimeError):
        item.tick(events=[event()], coverage=coverage())
    assert item.runtime.failed
    assert not item.runtime.summary(T)["feedback"]["runs"]


def test_real_reads_refused_at_construction_and_each_tick(rig):
    item = rig()
    item.dispatcher.config = replace(item.dispatcher.config, real_reads=True)
    with pytest.raises(ValueError, match="dry-run"):
        ReadReplayRuntime(item.memory, item.models, item.publisher, item.dispatcher)
    with pytest.raises(ReadRuntimeError):
        item.tick(events=[event()], coverage=coverage())
    assert item.publisher.sequence == 0


def test_replay_tick_and_time_bounds_fail_before_more_publication(rig):
    item = rig(max_ticks=1)
    item.tick(events=[event()], coverage=coverage())
    with pytest.raises(ReadRuntimeError):
        item.tick(T + timedelta(seconds=60))
    assert item.publisher.sequence == 1
    other = rig()
    other.tick(events=[event()], coverage=coverage())
    with pytest.raises(ReadRuntimeError):
        other.tick(T)
    assert other.publisher.sequence == 1
