"""CLI fixture evidence only; no real NFS or customer workload."""

from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pyarrow as pa
import pytest
import yaml
from click.testing import CliRunner

from predictive_cache.controller import read_replay
from predictive_cache.controller.cli import main
from predictive_cache.ml.baselines import ReadDemandFrequencyScorer
from predictive_cache.ml.package import DEFAULT_SERVING_POLICY, write_package
from predictive_cache.ml.read_demand import READ_DEMAND_SERVING_CONTRACT, feature_schema
from predictive_cache.semantics.read_demand import SEMANTIC_VIEW_ID

T = datetime(2026, 11, 4, 9, tzinfo=UTC)


def stamp(t):
    return t.isoformat().replace("+00:00", "Z")


@pytest.fixture
def fixture_run(tmp_path):
    event = dict(
        schema_version=1,
        event_id="fixture-read-1",
        event_time=stamp(T - timedelta(seconds=600)),
        known_at=stamp(T - timedelta(seconds=599)),
        volume_id="vol-1",
        file_id="file-b",
        dir_id=None,
        offset=0,
        bytes_requested=1024,
        bytes_read=1024,
        file_size=4096,
        origin="synthetic",
        demand_class="controlled_demand",
        origin_trust="client_asserted",
        coverage_status="complete",
        metadata_status="missing",
        data_kind="synthetic",
        protocol="NFSv4.1",
        operation="read",
    )
    events = tmp_path / "events.jsonl"
    events.write_text(json.dumps(event) + "\n")
    observations = []
    hour = T - timedelta(days=7)
    while hour < T:
        end = hour + timedelta(hours=1)
        observations.append(
            dict(
                volume_id="vol-1",
                hour_start=stamp(hour),
                status="complete",
                known_at=stamp(end),
                observed_through=stamp(end),
            )
        )
        hour = end
    for seconds in (60, 120):
        through = T + timedelta(seconds=seconds)
        observations.append(
            dict(
                volume_id="vol-1",
                hour_start=stamp(T),
                status="complete",
                known_at=stamp(through),
                observed_through=stamp(through),
            )
        )
    coverage = tmp_path / "coverage.jsonl"
    coverage.write_text("".join(json.dumps(row) + "\n" for row in observations))
    rows = pa.table(
        {"volume_id": ["vol-1"], "file_id": ["file-b"], "t": [T], "read_count_24h": [1.0]}
    )
    package = write_package(
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
        tmp_path / "packages",
    )
    pointer = tmp_path / "pointer.yaml"
    pointer.write_text(
        yaml.safe_dump(
            dict(
                model_uri=package.as_uri(),
                feature_version="read-demand-fv1",
                fallback="baseline-read-frequency",
            )
        )
    )
    root = tmp_path / "dispatcher"
    root.mkdir(mode=0o700)
    cfg = dict(
        schema_version=1,
        volume_id="vol-1",
        run_id="fixture-run",
        controller_id="fixture-controller",
        events="events.jsonl",
        coverage="coverage.jsonl",
        model_pointer="pointer.yaml",
        dispatcher_root="dispatcher",
        start=stamp(T),
        ticks=3,
    )
    config = tmp_path / "replay.yaml"
    config.write_text(yaml.safe_dump(cfg))
    return config, cfg, root, event


def invoke(fixture_run, changes=None):
    config, cfg, _, _ = fixture_run
    if changes:
        cfg = {**cfg, **changes}
        config.write_text(yaml.safe_dump(cfg))
    return CliRunner().invoke(main, ["replay-read", "--config", str(config)])


def test_cli_runs_verified_model_planner_dispatcher_and_feedback(fixture_run):
    result = invoke(fixture_run)
    assert result.exit_code == 0, result.output
    summary = json.loads(result.output)
    assert summary["data_kind"] == "synthetic"
    assert summary["replay"]["executed_ticks"] == 3
    assert all(
        tick["model_version"].startswith("baseline-read-frequency@")
        for tick in summary["replay"]["ticks"]
    )
    assert summary["feedback"]["runs"][0]["useful_bytes"] == 0
    assert summary["feedback"]["runs"][0]["eligible_completed_unique_bytes"] == 0
    root = fixture_run[2]
    assert json.loads((root / "read-replay-summary.json").read_text()) == summary
    records = [json.loads(p.read_text()) for p in root.glob("runs/*/plans/*/results/*.json")]
    assert len(records) == 1
    assert records[0]["status"] == "skipped" and records[0]["reason"] == "dry_run"
    assert records[0]["bytes_requested"] == 4096 and records[0]["bytes_read"] == 0
    assert not list(root.glob("runs/*/plans/*/tasks"))
    assert "target_fh" not in result.output and str(fixture_run[0].parent) not in result.output
    assert (root / "read-replay-summary.json").stat().st_mode & 0o777 == 0o600
    assert (root / "read-replay-metrics.prom").stat().st_mode & 0o777 == 0o600


def test_unknown_coverage_does_not_become_a_zero_score_or_plan(fixture_run):
    (fixture_run[0].parent / "coverage.jsonl").write_text("")
    result = invoke(fixture_run)
    assert result.exit_code == 0, result.output
    assert "memory_gap" in result.output
    assert not list((fixture_run[2] / "plans").glob("*.json"))


def test_real_data_rejected_before_output_artifacts(fixture_run):
    event = {
        **fixture_run[3],
        "data_kind": "real",
        "origin": "foreground",
        "demand_class": "foreground",
        "origin_trust": "trusted",
    }
    (fixture_run[0].parent / "events.jsonl").write_text(json.dumps(event) + "\n")
    result = invoke(fixture_run)
    assert result.exit_code != 0
    assert not list(fixture_run[2].iterdir())


@pytest.mark.parametrize(
    "changes",
    [
        {"real_reads": True},
        {"ticks": True},
        {"run_id": ".."},
        {"controller_id": ".hidden"},
        {"limits": {"max_input_bytes": 100}},
        {"limits": {"max_coverage": 1}},
    ],
)
def test_invalid_or_unsafe_config_refused_before_artifacts(fixture_run, changes):
    result = invoke(fixture_run, changes)
    assert result.exit_code != 0
    assert not list(fixture_run[2].iterdir())


def test_giant_line_errors_do_not_echo_input(fixture_run):
    marker = "PRIVATE_INPUT_DO_NOT_PRINT"
    (fixture_run[0].parent / "events.jsonl").write_text('{"secret":"' + marker * 4000 + '"}\n')
    result = invoke(fixture_run)
    assert result.exit_code != 0 and marker not in result.output
    assert not list(fixture_run[2].iterdir())


def test_duplicate_provenance_key_is_rejected(fixture_run):
    event = json.dumps(fixture_run[3])
    event = '{"data_kind":"real",' + event[1:]
    (fixture_run[0].parent / "events.jsonl").write_text(event + "\n")
    result = invoke(fixture_run)
    assert result.exit_code != 0 and not list(fixture_run[2].iterdir())


def test_event_symlink_is_not_followed(fixture_run):
    events = fixture_run[0].parent / "events.jsonl"
    original = events.with_name("original.jsonl")
    events.rename(original)
    events.symlink_to(original)
    result = invoke(fixture_run)
    assert result.exit_code != 0
    assert not list(fixture_run[2].iterdir())


def test_preexisting_dispatcher_root_is_preserved(fixture_run):
    marker = fixture_run[2] / "enable-real-reads"
    marker.write_text("existing state")
    result = invoke(fixture_run)
    assert result.exit_code != 0 and marker.read_text() == "existing state"
    assert list(fixture_run[2].iterdir()) == [marker]


def test_outer_scheduler_skips_next_slot_and_drains_backlog(fixture_run, monkeypatch):
    seen = []
    original = read_replay.ReadReplayRuntime

    class ForcedOverrun(original):
        def tick(self, t, *, events=(), coverage=()):
            seen.append((t, [event.event_id for event in events]))
            result = super().tick(t, events=events, coverage=coverage)
            return replace(result, overrun=len(seen) == 1)

    monkeypatch.setattr(read_replay, "ReadReplayRuntime", ForcedOverrun)
    event = {
        **fixture_run[3],
        "event_id": "fixture-read-2",
        "event_time": stamp(T + timedelta(seconds=59)),
        "known_at": stamp(T + timedelta(seconds=60)),
    }
    with (fixture_run[0].parent / "events.jsonl").open("a") as stream:
        stream.write(json.dumps(event) + "\n")
    result = invoke(fixture_run)
    assert result.exit_code == 0, result.output
    assert [t for t, _ in seen] == [T, T + timedelta(seconds=120)]
    assert seen[1][1] == ["fixture-read-2"]
    assert json.loads(result.output)["replay"]["skipped_overrun_slots"] == 1


def test_virtual_duration_is_bounded(fixture_run):
    result = invoke(fixture_run, {"limits": {"max_replay_seconds": 30}})
    assert result.exit_code == 0, result.output
    replay = json.loads(result.output)["replay"]
    assert replay["executed_ticks"] == 1 and replay["duration_limit_reached"]


def test_runtime_failure_preserves_private_diagnostics_without_error_text(fixture_run, monkeypatch):
    original = read_replay.ReadReplayRuntime

    class BrokenTick(original):
        def tick(self, t, *, events=(), coverage=()):
            super().tick(t, events=events, coverage=coverage)
            raise RuntimeError("PRIVATE_BACKEND_ERROR")

    monkeypatch.setattr(read_replay, "ReadReplayRuntime", BrokenTick)
    result = invoke(fixture_run)
    assert result.exit_code != 0 and "PRIVATE_BACKEND_ERROR" not in result.output
    diagnostics = (fixture_run[2] / "read-replay-summary.json").read_text()
    assert "PRIVATE_BACKEND_ERROR" not in diagnostics
    summary = json.loads(diagnostics)
    assert summary["failed"] and "controller_error" in summary["health"]["reasons"]
