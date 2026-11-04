"""Hand-computed fictional feedback, not production-demand evidence."""

from datetime import UTC, datetime, timedelta

import pytest

from predictive_cache.controller.feedback import UsefulByteFeedback
from predictive_cache.dispatcher.schema import ResultRecord, WarmerTask, WarmerTerminal, warmer_ref
from predictive_cache.semantics.read_demand import ReadDemandEvent

NOW = datetime(2026, 1, 5, tzinfo=UTC)


def task(name="one", offset=0, length=100, run="run"):
    return WarmerTask(
        run_id=run,
        plan_id="plan",
        task_id=name,
        volume_id="volume",
        file_id="file",
        export_name="test-ro",
        export_id="7",
        target_fh_b64="YWJj",
        offset=offset,
        length=length,
        max_bytes=length,
        created_at=NOW,
        deadline=NOW + timedelta(seconds=200),
        target_valid_until=NOW + timedelta(seconds=200),
    )


def result(t, status="completed", lower=False, finished=NOW + timedelta(seconds=10), count=None):
    count = t.length if count is None else count
    terminal = WarmerTerminal(
        type="result",
        status="completed" if status == "completed" else "partial",
        reason="fixture",
        bytes_requested=t.length,
        bytes_read=count,
        bytes_read_is_lower_bound=lower,
        execution_started=True,
        plan_ref=warmer_ref(t.plan_id),
        task_ref=warmer_ref(t.task_id),
    )
    return ResultRecord(
        t.run_id,
        t.plan_id,
        t.task_id,
        status,
        "fixture",
        t.length,
        count,
        lower,
        NOW + timedelta(seconds=1),
        finished,
        terminal,
    )


def read(name="read", offset=0, length=40, second=20, known=None, **changes):
    row = dict(
        event_id=name,
        event_time=NOW + timedelta(seconds=second),
        known_at=NOW + timedelta(seconds=known if known is not None else second),
        volume_id="volume",
        file_id="file",
        offset=offset,
        bytes_requested=length,
        bytes_read=length,
        file_size=1000,
        dir_id="dir",
        origin="foreground",
        demand_class="foreground",
        origin_trust="trusted",
        coverage_status="complete",
        metadata_status="verified",
        data_kind="real",
    )
    row.update(changes)
    return ReadDemandEvent(**row)


def ledger(**kwargs):
    return UsefulByteFeedback(experiment_id="fixture-only", usefulness_s=30, **kwargs)


def add(feedback, t=None, **kwargs):
    t = t or task()
    r = result(t, **kwargs)
    feedback.observe_result(t, r, known_at=r.finished_at)
    return t, r


def complete(feedback):
    feedback.coverage(
        "volume",
        NOW + timedelta(seconds=10),
        NOW + timedelta(seconds=40),
        known_at=NOW + timedelta(seconds=40),
        complete=True,
    )


def row(feedback, at=40):
    return feedback.report(NOW + timedelta(seconds=at))["runs"][0]


def test_complete_useful_unused_partition_and_read_dedup():
    feedback = ledger()
    add(feedback)
    assert feedback.observe_read(read())
    assert not feedback.observe_read(read())
    complete(feedback)
    r = row(feedback)
    assert (
        r["eligible_completed_unique_bytes"],
        r["useful_bytes"],
        r["unused_bytes"],
        r["unknown_bytes"],
    ) == (100, 40, 60, 0)
    assert r["requested_bytes"] == r["completed_bytes_sum"] == 100


def test_warm_overlap_and_retry_union_not_task_sum():
    feedback = ledger()
    t, r = add(feedback)
    assert not feedback.observe_result(t, r, known_at=NOW + timedelta(seconds=11))
    add(feedback, task("retry"))
    add(feedback, task("overlap", offset=50))
    feedback.observe_read(read(offset=25, length=100))
    feedback.observe_read(read("overlap", offset=50, length=25))
    complete(feedback)
    r = row(feedback)
    assert r["completed_bytes_sum"] == 300
    assert (r["eligible_completed_unique_bytes"], r["useful_bytes"], r["unused_bytes"]) == (
        150,
        100,
        50,
    )


@pytest.mark.parametrize(
    "status,lower", [("partial", False), ("partial", True), ("failed", True), ("cancelled", True)]
)
def test_noncompleted_outcomes_have_no_invented_interval(status, lower):
    feedback = ledger()
    add(feedback, status=status, lower=lower, count=20)
    feedback.observe_read(read())
    complete(feedback)
    r = row(feedback)
    assert r["eligible_completed_unique_bytes"] == r["useful_bytes"] == 0
    assert r["uncertain_requested_bytes"] == 100
    assert r["statuses"] == {status: 1}


@pytest.mark.parametrize(
    "changes",
    [
        dict(origin="warmer", demand_class="warmer"),
        dict(
            origin="synthetic",
            demand_class="controlled_demand",
            data_kind="synthetic",
            origin_trust="client_asserted",
        ),
        dict(origin="unknown", demand_class="unknown"),
        dict(origin_trust="unknown"),
        dict(coverage_status="unknown"),
    ],
)
def test_nonforeground_or_untrusted_is_excluded(changes):
    feedback = ledger()
    add(feedback)
    assert not feedback.observe_read(read(**changes))
    complete(feedback)
    assert row(feedback)["useful_bytes"] == 0


def test_completion_and_window_boundaries_are_explicit():
    feedback = ledger()
    add(feedback)
    feedback.observe_read(read("before", second=9))
    feedback.observe_read(read("same", second=10))
    feedback.observe_read(read("end", offset=10, second=40))
    feedback.observe_read(read("after", offset=70, second=41))
    complete(feedback)
    assert row(feedback, 41)["useful_bytes"] == 40


def test_open_window_and_incomplete_coverage_are_unknown():
    feedback = ledger()
    add(feedback)
    feedback.observe_read(read(length=20))
    assert (row(feedback, 30)["unknown_bytes"], row(feedback, 30)["unused_bytes"]) == (80, 0)
    complete(feedback)
    feedback.coverage(
        "volume",
        NOW + timedelta(seconds=20),
        NOW + timedelta(seconds=25),
        known_at=NOW + timedelta(seconds=40),
        complete=False,
    )
    assert (row(feedback)["unknown_bytes"], row(feedback)["unused_bytes"]) == (80, 0)


def test_coverage_gaps_and_known_at_filter():
    feedback = ledger()
    add(feedback)
    feedback.observe_read(read(known=50))
    feedback.coverage(
        "volume",
        NOW + timedelta(seconds=10),
        NOW + timedelta(seconds=20),
        known_at=NOW + timedelta(seconds=40),
        complete=True,
    )
    feedback.coverage(
        "volume",
        NOW + timedelta(seconds=21),
        NOW + timedelta(seconds=40),
        known_at=NOW + timedelta(seconds=40),
        complete=True,
    )
    assert row(feedback)["useful_bytes"] == 0
    assert row(feedback)["unknown_bytes"] == 100
    assert row(feedback, 50)["useful_bytes"] == 40


def test_late_delivery_revises_unused_to_unknown():
    feedback = ledger()
    add(feedback)
    complete(feedback)
    assert row(feedback)["unused_bytes"] == 100
    assert feedback.observe_read(read(second=20, known=50, length=20))
    r = row(feedback, 50)
    assert (r["useful_bytes"], r["unknown_bytes"], r["unused_bytes"]) == (20, 80, 0)
    assert not feedback.report(NOW + timedelta(seconds=50))["complete_accounting"]


def test_invalidation_does_not_credit_replacement_demand():
    feedback = ledger()
    add(feedback)
    feedback.observe_read(read())
    complete(feedback)
    feedback.invalidate(
        "volume", "file", NOW + timedelta(seconds=15), known_at=NOW + timedelta(seconds=45)
    )
    assert row(feedback)["useful_bytes"] == 40
    r = row(feedback, 45)
    assert (r["useful_bytes"], r["unknown_bytes"], r["unused_bytes"]) == (0, 100, 0)


def test_capacity_loss_is_visible_and_never_makes_negative_evidence():
    feedback = ledger(max_reads=1, max_tasks=1)
    add(feedback)
    feedback.observe_read(read(length=20))
    assert not feedback.observe_read(read("overflow", offset=20))
    assert not feedback.observe_result(
        task("overflow"), result(task("overflow")), known_at=NOW + timedelta(seconds=10)
    )
    complete(feedback)
    r = row(feedback)
    assert (r["useful_bytes"], r["unknown_bytes"], r["unused_bytes"]) == (20, 80, 0)
    assert feedback.report(NOW + timedelta(seconds=40))["counters"]["capacity_rejected"] == 2


def test_join_and_completion_evidence_fail_closed():
    feedback = ledger()
    t = task()
    with pytest.raises(ValueError, match="identity mismatch"):
        feedback.observe_result(t, result(task("wrong")), known_at=NOW + timedelta(seconds=10))
    r = result(t)
    broken = ResultRecord(**{**r.__dict__, "warmer_terminal": None})
    with pytest.raises(ValueError, match="execution evidence"):
        feedback.observe_result(t, broken, known_at=NOW + timedelta(seconds=10))
    assert feedback.report(NOW + timedelta(seconds=40))["runs"] == []


def test_conflicting_identity_marks_accounting_incomplete():
    feedback = ledger()
    add(feedback)
    feedback.observe_read(read())
    with pytest.raises(ValueError, match="conflicting feedback read"):
        feedback.observe_read(read(length=20))
    complete(feedback)
    assert row(feedback)["unused_bytes"] == 0


def test_explicit_use_until_and_experiment_isolation():
    feedback = ledger()
    t = task()
    feedback.observe_result(
        t, result(t), known_at=NOW + timedelta(seconds=10), use_until=NOW + timedelta(seconds=25)
    )
    feedback.observe_read(read(second=30))
    complete(feedback)
    assert row(feedback)["useful_bytes"] == 0
    report = feedback.report(NOW + timedelta(seconds=40))
    assert report["experiment_id"] == "fixture-only"
    assert "YWJj" not in str(report)
    assert '"file_id"' not in str(report)
    with pytest.raises(ValueError, match="backwards"):
        feedback.report(NOW + timedelta(seconds=30))


def test_future_loss_does_not_rewrite_an_earlier_as_of_report():
    feedback = ledger(max_reads=1)
    add(feedback)
    complete(feedback)
    feedback.observe_read(read(known=50))
    assert not feedback.observe_read(read("overflow", known=60))
    assert row(feedback, 40)["unused_bytes"] == 100
    assert feedback.report(NOW + timedelta(seconds=40))["complete_accounting"]
    assert row(feedback, 60)["unknown_bytes"] == 60


def test_repeated_warm_and_demand_overlap_has_bounded_report_allocations():
    import tracemalloc

    feedback = ledger(max_tasks=151, max_reads=201)
    for i in range(150):
        add(feedback, task(str(i)))
    for i in range(200):
        feedback.observe_read(read(str(i), length=1))
    complete(feedback)
    tracemalloc.start()
    try:
        r = row(feedback)
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()
    assert (r["useful_bytes"], r["unused_bytes"]) == (1, 99)
    assert peak < 512 * 1024


@pytest.mark.parametrize("second,unknown", [(0, False), (1, True), (5, True), (10, True)])
def test_write_during_execution_or_at_completion_invalidates_warmed_content(second, unknown):
    feedback = ledger()
    add(feedback)
    feedback.observe_read(read(length=100))
    complete(feedback)
    feedback.invalidate(
        "volume", "file", NOW + timedelta(seconds=second), known_at=NOW + timedelta(seconds=45)
    )
    r = row(feedback, 45)
    assert r["useful_bytes"] == (0 if unknown else 100)
    assert r["unknown_bytes"] == (100 if unknown else 0)


def test_preloaded_future_read_still_invalidates_prior_unused_when_it_becomes_known():
    feedback = ledger()
    add(feedback)
    feedback.observe_read(read(known=50, length=20))
    complete(feedback)
    assert row(feedback, 40)["unused_bytes"] == 100
    report = row(feedback, 50)
    assert (report["useful_bytes"], report["unknown_bytes"], report["unused_bytes"]) == (20, 80, 0)
