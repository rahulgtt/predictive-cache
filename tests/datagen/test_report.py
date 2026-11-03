"""Small, explicit positive/negative fixtures for report formulas and readability."""

from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

from predictive_cache.datagen.config import load_config
from predictive_cache.datagen.recovery import recovery_checks
from predictive_cache.datagen.report import build_report, render_summary

START = datetime(2026, 1, 5, tzinfo=UTC)


def fixture(kind):
    cfg = load_config(preset="small")
    for name in type(cfg.patterns).model_fields:
        getattr(cfg.patterns, name).enabled = False
    getattr(cfg.patterns, kind).enabled = True
    cfg.actors.vacation_weeks_per_quarter = 0
    cfg.randomness.noise_share = 0
    cfg.patterns.popularity.working_set_share = cfg.patterns.popularity.revisit_share = 0
    cfg.patterns.popularity.recency_boost_half_life_days = 0
    tables = dict(
        volumes=[
            dict(
                volume_id="v",
                timezone="UTC",
                period_start=START,
                period_end=START + timedelta(weeks=8),
                holidays=[],
                work_days=[1, 2, 3, 4, 5],
            )
        ],
        files=[
            dict(
                file_id=f"f{i}",
                volume_id="v",
                project_id="p",
                kind="doc",
                created_at=START - timedelta(days=100),
                deleted_at=None,
                size_at_creation=1000,
            )
            for i in range(50)
        ],
        actors=[
            dict(
                actor_id="a",
                client_id="c",
                volume_id="v",
                work_days=[1, 2, 3, 4, 5],
                schedule_start_local="08:00",
                schedule_end_local="18:00",
            )
        ],
        events=[],
        dropped_events=[],
        event_truth=[],
        loss_counters=[],
    )
    return cfg, tables


def pattern(kind, params=None, pid="pat", files=None):
    return dict(
        kind=kind,
        pattern_id=pid,
        volume_id="v",
        actors=["a"],
        files=files or ["f0"],
        active_from=START,
        active_to=START + timedelta(weeks=8),
        params=params or {},
    )


def add(tables, fid, at, cause="pat", session="s", client="c", operation="open", **fields):
    event = dict(
        event_id=f"ev-{len(tables['events'])}",
        operation=operation,
        result="ok",
        origin="foreground",
        file_id=fid,
        volume_id="v",
        event_time=at,
        ingest_time=at,
        client_id=client,
        session_id=session,
        app="office",
        **fields,
    )
    tables["events"].append(event)
    tables["event_truth"].append(dict(event_id=event["event_id"], cause=cause, faults=[]))
    return event


def check(cfg, tables, patterns, kind):
    return next(
        c for c in recovery_checks(cfg, tables, patterns, tables["events"]) if c["kind"] == kind
    )


def test_summary_counts_histograms_distributions_and_fault_support():
    cfg, tables = fixture("popularity")
    add(tables, "f0", START + timedelta(hours=9))
    add(
        tables, "f0", START + timedelta(hours=9, seconds=2), operation="close", handle_bytes_read=42
    )
    clean = list(tables["events"])
    tables["events"].append(dict(tables["events"][0]))
    tables["loss_counters"] = [
        dict(offered=2, emitted=2, dropped=0, sampled_out=0, duplicates=1, late=0)
    ]
    extra = {
        "fault_rates": {
            "unknown": dict(eligible=20, affected=0, expected=0.2),
            "duplicate": dict(eligible=1000, affected=100, expected=100),
        }
    }
    report = build_report(cfg, tables, [], [], extra, clean=clean, validation={"handles": "passed"})
    assert report == build_report(
        cfg, tables, [], [], extra, clean=clean, validation={"handles": "passed"}
    )
    assert report["counts"]["delivered_rows"] == 3
    assert report["counts"]["unique_offered_events"] == 2
    assert report["hour_of_week"]["v"]["counts"][9] == 3
    assert sum(report["hour_of_week"]["v"]["counts"]) == 3
    assert report["popularity_curve"][0]["opens"] == 1
    assert report["bytes_read_per_successful_open"]["p50"] == 42
    assert report["sessions"]["duration_seconds"]["p50"] == 2
    assert report["fault_rates"]["unknown"]["status"] == "insufficient_data"
    assert report["fault_rates"]["duplicate"]["status"] == "passed"
    markdown = render_summary(report)
    assert markdown.startswith("# Synthetic access dataset\n\n")
    assert "| operation | open | 2 |" in markdown
    assert "## Pattern diagnostics\n\n" in markdown
    assert "| handles | passed |" in markdown
    assert "Mon 09:00: 3" in markdown
    assert "nan" not in markdown.lower()
    assert all(c["status"] == "not_applicable" for c in report["recovery"])


def test_zipf_positive_negative_sparse_and_mixed():
    cfg, tables = fixture("popularity")
    record = pattern(
        "popularity", dict(project_id="p", zipf_s=1.1), files=[f"f{i}" for i in range(50)]
    )
    for rank in range(1, 51):
        for index in range(round(4000 * rank**-1.1)):
            add(tables, f"f{rank - 1}", START + timedelta(hours=9, seconds=index))
    assert check(cfg, tables, [record], "popularity")["status"] == "passed"
    cfg.patterns.weekly_schedule.enabled = True
    assert check(cfg, tables, [record], "popularity")["status"] == "diagnostic"
    cfg.patterns.weekly_schedule.enabled = False
    tables["events"].clear()
    tables["event_truth"].clear()
    for i in range(2000):
        add(tables, f"f{i % 50}", START + timedelta(hours=9, seconds=i))
    assert check(cfg, tables, [record], "popularity")["status"] == "failed"
    tables["events"] = tables["events"][:5]
    assert check(cfg, tables, [record], "popularity")["status"] == "insufficient_data"


@pytest.mark.parametrize("kind", ["weekly_schedule", "batch_job"])
def test_schedule_and_job_timing_positive_negative_and_missing_identity(kind):
    cfg, tables = fixture(kind)
    slots = [START + timedelta(weeks=w, days=d, hours=9) for w in range(8) for d in range(5)]
    if kind == "weekly_schedule":
        record = pattern(
            kind, dict(project_id="p", attendance=0.85, slots=[t.isoformat() for t in slots])
        )
        for i, at in enumerate(slots):
            for _ in range(3):
                add(tables, "f0", at, session=f"s{i}")
    else:
        record = pattern(kind, dict(local_time="09:00"))
        for i, at in enumerate(slots):
            add(tables, "f0", at, session=f"s{i}")
    result = check(cfg, tables, [record], kind)
    assert result["status"] == "passed"
    for event in tables["events"]:
        event["event_time"] += timedelta(hours=4)
    assert check(cfg, tables, [record], kind)["status"] == "failed"
    for event in tables["events"]:
        event["client_id"] = None
    assert check(cfg, tables, [record], kind)["status"] == "insufficient_data"


def test_workflow_lift_uses_observed_actor_sessions_and_drift_periods():
    cfg, tables = fixture("workflow_chain")
    edge = dict(previous=None, **{"from": "f0", "to": "f1", "p": 1.0})
    drift = START + timedelta(weeks=4)
    record = pattern(
        "workflow_chain",
        dict(edges=[edge], edges_after=[edge], drift_at=drift.isoformat()),
        files=["f0", "f1"],
    )
    for phase in (START, drift):
        for session in range(30):
            at = phase + timedelta(hours=9, minutes=session * 30)
            key = f"{phase}:{session}"
            add(tables, "f0", at, session=key)
            add(tables, "f1", at + timedelta(seconds=30), session=key)
            for file in range(2, 10):
                add(tables, f"f{file}", at + timedelta(seconds=file * 30), session=key)
    result = check(cfg, tables, [record], "workflow_chain")
    assert result["status"] == "passed"
    assert [e["phase"] for e in result["metrics"]["edges"]] == ["before", "after"]
    assert all(e["lift"] == 10 for e in result["metrics"]["edges"])
    for event in tables["events"]:
        if event["file_id"] == "f1":
            event["session_id"] = "different-session"
    assert check(cfg, tables, [record], "workflow_chain")["status"] == "failed"


def test_lifecycle_and_new_file_decay_positive_negative_and_constant():
    cfg, tables = fixture("project_lifecycle")
    cfg.patterns.popularity.enabled = True
    record = pattern(
        "project_lifecycle",
        dict(
            project_id="p",
            started_at=(START - timedelta(weeks=1)).isoformat(),
            ramp_end=START.isoformat(),
            active_end=(START + timedelta(weeks=2)).isoformat(),
            cooldown_end=(START + timedelta(weeks=6)).isoformat(),
            phase_activity=[0.3, 1, 0.05, 0.02],
        ),
    )
    for week, count in enumerate((1000, 1000, 900, 700, 450, 200, 20, 20)):
        for index in range(count):
            add(tables, "f0", START + timedelta(weeks=week, hours=9, seconds=index))
    assert check(cfg, tables, [record], "project_lifecycle")["status"] == "passed"
    for event in tables["events"]:
        week = (event["event_time"] - START).days // 7
        event["event_time"] = START + timedelta(weeks=7 - week, hours=9)
    assert check(cfg, tables, [record], "project_lifecycle")["status"] == "failed"
    record["params"]["phase_activity"] = [1, 1, 1, 1]
    assert check(cfg, tables, [record], "project_lifecycle")["status"] == "not_applicable"

    # Eight local weeks remain eight bins across the spring DST transition.
    dst_cfg, dst_tables = fixture("project_lifecycle")
    zone = ZoneInfo("America/New_York")
    local_start = datetime(2026, 3, 2, tzinfo=zone)
    dst_tables["volumes"][0].update(
        timezone=str(zone),
        period_start=local_start.astimezone(UTC),
        period_end=(local_start + timedelta(weeks=8)).astimezone(UTC),
    )
    add(dst_tables, "f0", (local_start + timedelta(weeks=7, hours=9)).astimezone(UTC))
    dst_result = check(dst_cfg, dst_tables, [record], "project_lifecycle")
    assert len(dst_result["metrics"]["weekly_opens"]) == 8
    assert dst_result["metrics"]["weekly_opens"][-1] == 1

    cfg, tables = fixture("new_file")
    records = [pattern("new_file", pid=f"pat-{i}", files=[f"f{i}"]) for i in range(30)]
    for file in tables["files"][:30]:
        file["created_at"] = START
        for day in range(10):
            for index in range(max(1, round(100 * 2**-day))):
                add(tables, file["file_id"], START + timedelta(days=day, hours=9, seconds=index))
    assert check(cfg, tables, records, "new_file")["status"] == "passed"
    for event in tables["events"]:
        day = (event["event_time"] - START).days
        event["event_time"] = START + timedelta(days=9 - day, hours=9)
    assert check(cfg, tables, records, "new_file")["status"] == "failed"


def test_burst_periodicity_and_noise_fraction_positive_negative():
    cfg, tables = fixture("burst")
    records = []
    for i in range(28):
        record = pattern("burst", pid=f"pat-{i}")
        records.append(record)
        add(tables, "f0", START + timedelta(hours=i * 6), cause=record["pattern_id"])
    assert check(cfg, tables, records, "burst")["status"] == "passed"
    for i, event in enumerate(tables["events"]):
        event["event_time"] = START + timedelta(weeks=i % 8, hours=9, microseconds=i)
    assert check(cfg, tables, records, "burst")["status"] == "failed"

    cfg, tables = fixture("noise")
    cfg.randomness.noise_share = 0.1
    record = pattern("noise", dict(noise_share=0.1, standalone=False))
    for i in range(1000):
        add(tables, "f0", START + timedelta(seconds=i), cause="noise" if i < 100 else "background")
    assert check(cfg, tables, [record], "noise")["status"] == "passed"
    for row in tables["event_truth"]:
        row["cause"] = "background"
    assert check(cfg, tables, [record], "noise")["status"] == "failed"
    record["params"]["standalone"] = True
    assert check(cfg, tables, [record], "noise")["status"] == "not_applicable"


@pytest.mark.parametrize(
    "kind,params",
    [
        ("popularity", dict(project_id="p", zipf_s=1.1)),
        (
            "weekly_schedule",
            dict(project_id="p", attendance=0.85, slots=[(START + timedelta(hours=9)).isoformat()]),
        ),
        (
            "workflow_chain",
            dict(
                edges=[dict(previous=None, **{"from": "f0", "to": "f1", "p": 1.0})], drift_at=None
            ),
        ),
        (
            "project_lifecycle",
            dict(
                project_id="p",
                started_at=START.isoformat(),
                ramp_end=(START + timedelta(weeks=2)).isoformat(),
                active_end=(START + timedelta(weeks=3)).isoformat(),
                cooldown_end=(START + timedelta(weeks=6)).isoformat(),
                phase_activity=[0.3, 1, 0.3, 0.02],
            ),
        ),
        ("burst", {}),
        ("batch_job", dict(local_time="09:00")),
        ("new_file", {}),
        ("noise", dict(noise_share=0.1, standalone=False)),
    ],
)
def test_sparse_recovery_never_counts_as_passing(kind, params):
    cfg, tables = fixture(kind)
    record = pattern(kind, params)
    add(tables, "f0", START + timedelta(hours=9))
    result = check(cfg, tables, [record], kind)
    assert result["status"] == "insufficient_data"
    assert result["meets_threshold"] is None
