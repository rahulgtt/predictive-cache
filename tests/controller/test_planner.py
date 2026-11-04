"""Synthetic metadata only; no network reads, live targets or model-quality claim."""

from __future__ import annotations

import json
import os
import stat
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta

import pytest

from predictive_cache.controller.planner import (
    PlannerConfig,
    PlannerPublisher,
    PublicationError,
    ScoredCandidate,
    select_plan,
)
from predictive_cache.dispatcher import securefs
from predictive_cache.dispatcher.core import Dispatcher, DispatcherConfig
from predictive_cache.dispatcher.schema import InvalidPlanItem, Plan, parse_utc_z
from predictive_cache.ml.package import DEFAULT_SERVING_POLICY

T = datetime(2026, 11, 4, 9, tzinfo=UTC)
HANDLE = "c3ludGhldGljLWhhbmRsZQ"


@dataclass(frozen=True)
class Target:
    volume_id: str = "vol-1"
    file_id: str = "file-a"
    export_id: int = 1
    export_name: str = "export-1"
    target_fh_b64: str = field(default=HANDLE, repr=False)
    file_size: int | None = 4096
    seen_at: datetime = T


class Targets:
    target_max_age_s = 3600

    def __init__(self, targets=None):
        self.targets = targets if targets is not None else [Target()]

    def resolve(self, volume_id, file_id, t):
        return next(
            (
                target
                for target in self.targets
                if (target.volume_id, target.file_id) == (volume_id, file_id)
            ),
            None,
        )


def candidate(file_id="file-a", score=0.8, **kwargs):
    return ScoredCandidate(
        "vol-1", file_id, score, kwargs.pop("recency", 600), kwargs.pop("size", 4096), **kwargs
    )


def select(candidates=None, **kwargs):
    values = dict(
        serving_policy=dict(DEFAULT_SERVING_POLICY),
        is_eligible=lambda *_: True,
        targets=Targets(),
        cooldown={},
        t=T,
        run_id="run-1",
        plan_id="plan-1",
        model_version="model-v1",
        plans_allowed=True,
    )
    values.update(kwargs)
    return select_plan(candidates if candidates is not None else [candidate()], **values)


@pytest.fixture
def publisher(tmp_path):
    root = tmp_path / "private"
    root.mkdir(mode=0o700)
    return PlannerPublisher(root, run_id="run-1", controller_id="controller-1")


def publish(publisher, candidates=None, **kwargs):
    values = dict(
        t=T,
        serving_policy=dict(DEFAULT_SERVING_POLICY),
        is_eligible=lambda *_: True,
        targets=Targets(),
        model_version="model-v1",
        plans_allowed=True,
    )
    values.update(kwargs)
    return publisher.publish(candidates if candidates is not None else [candidate()], **values)


def test_default_horizon_is_separate_from_execution_deadline():
    selected = select()
    assert selected.predictions[0]["horizon_seconds"] == 300
    assert parse_utc_z(selected.plan["deadline"]) == T + timedelta(seconds=120)
    assert selected.plan["items"][0]["export_id"] == "1"
    assert selected.plan["max_bytes"] == 4096
    assert not any(
        isinstance(item, InvalidPlanItem) for item in Plan.model_validate(selected.plan).items
    )
    assert selected.bindings[0] is not None


def test_earliest_target_expiry_and_horizon_cap():
    targets = Targets(
        [Target(), replace(Target(), file_id="file-b", seen_at=T - timedelta(seconds=3590))]
    )
    selected = select([candidate(), candidate("file-b")], targets=targets)
    assert parse_utc_z(selected.plan["deadline"]) == T + timedelta(seconds=10)
    assert parse_utc_z(selected.plan["items"][0]["target_valid_until"]) == T + timedelta(
        seconds=120
    )
    policy = {**DEFAULT_SERVING_POLICY, "horizon_s": 5}
    assert parse_utc_z(select(serving_policy=policy).plan["deadline"]) == T + timedelta(seconds=5)
    assert parse_utc_z(
        select(config=PlannerConfig(target_valid_s=1000)).plan["deadline"]
    ) == T + timedelta(seconds=300)


@pytest.mark.parametrize(
    "seen", [T + timedelta(seconds=1), T - timedelta(seconds=3600), T - timedelta(seconds=4000)]
)
def test_future_or_expired_targets_are_skipped(seen):
    selected = select(targets=Targets([replace(Target(), seen_at=seen)]))
    assert selected.plan is None
    assert selected.predictions[0]["skip_reason"] == "no_target"


@pytest.mark.parametrize(
    "changes,expected",
    [
        ({"is_eligible": lambda *_: False}, "ineligible"),
        ({"cooldown": {("vol-1", "file-a"): T - timedelta(seconds=1)}}, "cooldown"),
        ({"serving_policy": {**DEFAULT_SERVING_POLICY, "min_score": 0.9}}, "below_threshold"),
        ({"targets": Targets([])}, "no_target"),
        ({"config": PlannerConfig(max_bytes=0)}, "budget"),
        ({"plans_allowed": False}, "plans_suppressed"),
    ],
)
def test_ordered_gates(changes, expected):
    result = select(**changes)
    assert result.plan is None
    assert result.predictions[0]["skip_reason"] == expected


def test_read_recency_unknown_and_empty_size_gates():
    assert select([candidate(recency=1)]).predictions[0]["skip_reason"] == "recently_read"
    assert select([candidate(size=None)]).predictions[0]["skip_reason"] == "unknown_size"
    assert select([candidate(size=0)]).predictions[0]["skip_reason"] == "empty_file"
    # Earlier gate wins, irrespective of downstream metadata.
    assert (
        select([candidate(size=None, recency=1)]).predictions[0]["skip_reason"] == "recently_read"
    )


def test_current_target_size_caps_prefix_and_range_cap():
    assert (
        select(targets=Targets([replace(Target(), file_size=32)])).plan["items"][0]["length"] == 32
    )
    policy = {**DEFAULT_SERVING_POLICY, "range_cap": 16}
    assert select(serving_policy=policy).plan["items"][0]["length"] == 16
    assert (
        select(targets=Targets([replace(Target(), file_size=None)])).plan["items"][0]["length"]
        == 4096
    )


def test_budget_walk_continues_and_ties_use_file_id():
    candidates = [
        candidate("file-c", 0.7, size=32),
        candidate("file-b", 0.8, size=32),
        candidate("file-a", 0.8),
    ]
    targets = Targets(
        [replace(Target(), file_id=c.file_id, file_size=c.file_size) for c in candidates]
    )
    result = select(candidates, targets=targets, config=PlannerConfig(max_bytes=64, max_items=2))
    assert result.planned == (("vol-1", "file-b"), ("vol-1", "file-c"))
    assert result.predictions[0]["file_id"] == "file-a"
    assert result.predictions[0]["skip_reason"] == "budget"
    assert result.plan["max_bytes"] == 64
    # Operational configuration cannot raise a package limit.
    assert (
        len(
            select(
                candidates,
                targets=targets,
                serving_policy={**DEFAULT_SERVING_POLICY, "max_items": 1},
                config=PlannerConfig(max_items=99),
            ).planned
        )
        == 1
    )


@pytest.mark.parametrize("value", [float("nan"), float("inf"), -float("inf"), True])
def test_nonfinite_scores_rejected(value):
    with pytest.raises(ValueError):
        candidate(score=value)


def test_duplicate_identity_and_invalid_runtime_inputs_fail_closed():
    assert candidate(candidate_sources=("popular",)).candidate_sources == ("popular",)
    with pytest.raises(ValueError, match="duplicate"):
        select([candidate(), candidate()])
    with pytest.raises(ValueError, match="future"):
        select(cooldown={("vol-1", "file-a"): T + timedelta(seconds=1)})
    with pytest.raises(ValueError, match="demand"):
        select(serving_policy={**DEFAULT_SERVING_POLICY, "target": "cold_open"})
    with pytest.raises(ValueError):
        candidate(candidate_sources=(HANDLE,))
    with pytest.raises(ValueError):
        select(t=T.replace(tzinfo=None))
    with pytest.raises(ValueError, match="bound"):
        select(
            [candidate(), candidate("file-b")], config=PlannerConfig(max_candidates=1, log_top_n=1)
        )


@pytest.mark.parametrize(
    "replacement",
    [
        dict(export_name="bad name"),
        dict(target_fh_b64="bad="),
        dict(export_id=True),
        dict(file_size=-1),
    ],
)
def test_malformed_target_is_skipped_without_handle_error(replacement):
    result = select(targets=Targets([replace(Target(), **replacement)]))
    assert result.plan is None
    assert result.predictions[0]["skip_reason"] == "no_target"


def test_prediction_bound_always_keeps_selected_and_private_bindings(publisher):
    b = candidate("file-b", 0.7, size=16)
    target_a, target_b = Target(), replace(Target(), file_id="file-b", file_size=16)
    selected = publish(
        publisher,
        [candidate(), b],
        targets=Targets([target_a, target_b]),
        config=PlannerConfig(log_top_n=0),
    )
    assert len(selected.predictions) == 2
    assert selected.bindings == (target_a, target_b)
    assert selected.bindings[0] is target_a
    prediction_path = next((publisher.root / "predictions").glob("*.jsonl"))
    text = prediction_path.read_text()
    assert HANDLE not in text and "target_fh" not in text and "export_name" not in text
    assert HANDLE not in repr(selected)
    for path in [prediction_path, next((publisher.root / "plans").glob("*.json"))]:
        assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert stat.S_IMODE((publisher.root / "plans").stat().st_mode) == 0o700
    assert publisher.sequence == 1 and publisher.pending is None
    assert publisher.cooldown == {("vol-1", "file-a"): T, ("vol-1", "file-b"): T}


def test_cooldown_is_committed_only_after_publish_and_capacity_never_evicts(publisher):
    publisher.max_cooldowns = 1
    publish(publisher)
    assert (
        publish(publisher, t=T + timedelta(seconds=1)).predictions[0]["skip_reason"] == "cooldown"
    )
    b = candidate("file-b")
    result = publish(
        publisher,
        [b],
        t=T + timedelta(seconds=2000),
        targets=Targets([replace(Target(), file_id="file-b")]),
    )
    assert result.plan is None
    assert result.predictions[0]["skip_reason"] == "cooldown_capacity"
    assert publisher.cooldown == {("vol-1", "file-a"): T}
    # An expired cooldown can update its existing slot.
    assert publish(publisher, t=T + timedelta(seconds=2000)).plan is not None


@pytest.mark.parametrize("phase", ["before", "after"])
def test_plan_write_failures_latch_even_after_final_file_exists(publisher, monkeypatch, phase):
    original = securefs.atomic_write_json

    def fail(fd, name, value, **kwargs):
        if phase == "after":
            assert original(fd, name, value, **kwargs)
        raise OSError("simulated directory fsync failure")

    monkeypatch.setattr(securefs, "atomic_write_json", fail)
    with pytest.raises(PublicationError):
        publish(publisher)
    assert bool(list((publisher.root / "plans").glob("*.json"))) == (phase == "after")
    assert publisher.failed and publisher.pending is not None
    assert publisher.sequence == 0 and publisher.cooldown == {}
    with pytest.raises(PublicationError, match="stopped"):
        publish(publisher, t=T + timedelta(seconds=60))
    assert len(list((publisher.root / "predictions").glob("*.jsonl"))) == 1


def test_real_post_rename_directory_fsync_failure(publisher, monkeypatch):
    # Fail exactly the directory fsync immediately following the plan rename.
    original_rename, original_fsync = securefs._renameat2_noreplace, os.fsync
    renamed_plan = False

    def rename(source_fd, source_name, dest_fd, dest_name):
        nonlocal renamed_plan
        original_rename(source_fd, source_name, dest_fd, dest_name)
        renamed_plan = dest_name.endswith(".json")

    def fsync(fd):
        if renamed_plan and stat.S_ISDIR(os.fstat(fd).st_mode):
            raise OSError("simulated fsync")
        original_fsync(fd)

    monkeypatch.setattr(securefs, "_renameat2_noreplace", rename)
    monkeypatch.setattr(os, "fsync", fsync)
    with pytest.raises(PublicationError):
        publish(publisher)
    assert list((publisher.root / "plans").glob("*.json"))
    assert publisher.failed and publisher.sequence == 0 and publisher.cooldown == {}


@pytest.mark.parametrize("area,suffix", [("plans", ".json"), ("predictions", ".jsonl")])
def test_existing_sink_collision_never_overwrites_and_stops(publisher, area, suffix):
    directory = publisher.root / area
    directory.mkdir(mode=0o700)
    path = directory / ("controller-1-000000000001" + suffix)
    path.write_text("existing")
    path.chmod(0o600)
    with pytest.raises(PublicationError):
        publish(publisher)
    assert path.read_text() == "existing"
    assert publisher.failed and publisher.sequence == 0 and publisher.cooldown == {}


def test_empty_and_suppressed_ticks_write_predictions_without_plans(publisher):
    selected = publish(publisher, plans_allowed=False)
    assert selected.plan is None and not list((publisher.root / "plans").iterdir())
    assert publisher.sequence == 1 and publisher.cooldown == {}
    publish(publisher, [])
    assert publisher.sequence == 2


def test_dispatcher_consumes_published_plan_as_dry_run(publisher):
    selected = publish(publisher)
    config = DispatcherConfig(
        root=publisher.root,
        warmer_command=publisher.root / "unused-warmer",
        host="192.0.2.1",
        real_reads=False,
    )
    with Dispatcher(config, now=lambda: T) as dispatcher:
        stats = dispatcher.run_once()
    assert stats.claimed == 1 and stats.results == 1
    result_path = (
        publisher.root
        / "runs"
        / "run-1"
        / "plans"
        / selected.plan["plan_id"]
        / "results"
        / (selected.plan["items"][0]["task_id"] + ".json")
    )
    result = json.loads(result_path.read_text())
    assert result["status"] == "skipped" and result["reason"] == "dry_run"
    assert result["bytes_requested"] == 4096
    assert result["bytes_read"] == 0
    assert not (result_path.parent.parent / "tasks").exists()


def test_selector_parity_at_equal_byte_budget():
    import numpy as np
    import pyarrow as pa

    from predictive_cache.ml.metrics import Budget
    from predictive_cache.ml.metrics import select as metric_select

    candidates = [
        candidate("file-c", 0.7, size=32),
        candidate("file-a", 0.8),
        candidate("file-b", 0.8, size=32),
    ]
    rows = pa.table(
        {"file_id": [c.file_id for c in candidates], "file_size": [c.file_size for c in candidates]}
    )
    expected = metric_select(rows, np.array([c.score for c in candidates]), Budget("bytes", 64))
    targets = Targets(
        [replace(Target(), file_id=c.file_id, file_size=c.file_size) for c in candidates]
    )
    actual = select(
        candidates,
        targets=targets,
        serving_policy={
            **DEFAULT_SERVING_POLICY,
            "recent_skip_s": 0,
            "cooldown_s": 0,
            "max_bytes": 64,
        },
    )
    assert set(actual.planned) == {
        c.identity for c, selected in zip(candidates, expected, strict=True) if selected
    }


def test_oversized_plan_refused_before_publication(publisher, monkeypatch):
    monkeypatch.setattr(securefs, "DEFAULT_MAX_JSON_BYTES", 100)
    with pytest.raises(ValueError, match="file size"):
        publish(publisher)
    assert publisher.pending is None and not publisher.failed
    assert publisher.sequence == 0 and not list(publisher.root.iterdir())


@pytest.mark.parametrize(
    "changes",
    [
        {"run_id": "."},
        {"run_id": ".."},
        {"plan_id": ".hidden"},
        {"plan_id": "."},
        {"plan_id": ".."},
        {"plan_id": "plan.tmp"},
    ],
)
def test_direct_selection_rejects_dispatcher_unsafe_names(changes):
    with pytest.raises(ValueError):
        select(**changes)


@pytest.mark.parametrize(
    "changes",
    [
        {"run_id": "."},
        {"run_id": ".."},
        {"controller_id": ".hidden"},
        {"controller_id": "."},
        {"controller_id": ".."},
    ],
)
def test_publisher_rejects_unsafe_names_before_artifacts(tmp_path, changes):
    values = {"run_id": "run-1", "controller_id": "controller-1", **changes}
    with pytest.raises(ValueError):
        PlannerPublisher(tmp_path, **values)
    assert not list(tmp_path.iterdir())
