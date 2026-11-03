from __future__ import annotations

import json
import multiprocessing
import os
import signal
import threading
import time
from datetime import timedelta
from pathlib import Path

import pytest

from predictive_cache.dispatcher.core import (
    Dispatcher,
    _apply_parent_death_signal,
)
from predictive_cache.dispatcher.schema import WarmerTask
from predictive_cache.dispatcher.securefs import SecureFileError

from .conftest import (
    config,
    make_warmer,
    plan_data,
    write_owner_file,
    write_plan,
)


def read_result(root, task_id="task-1"):
    return json.loads(
        (
            root
            / "runs"
            / "run-1"
            / "plans"
            / "plan-1"
            / "results"
            / f"{task_id}.json"
        ).read_text()
    )


def plan_area(root, plan_id, area, run_id="run-1"):
    return root / "runs" / run_id / "plans" / plan_id / area


def enable(root):
    write_owner_file(root / "enable-real-reads")


def test_completed_task_writes_exact_task_output_and_result(root, clock):
    warmer = make_warmer(root)
    write_plan(root, plan_data(clock))
    enable(root)
    with Dispatcher(
        config(root, warmer),
        now=clock.now,
        monotonic=clock.monotonic,
    ) as dispatcher:
        stats = dispatcher.run_once()

    assert stats.claimed == 1
    result = read_result(root)
    assert result["status"] == "completed"
    assert result["bytes_read"] == 4096
    assert result["warmer_terminal"]["status"] == "completed"
    task_path = plan_area(root, "plan-1", "tasks") / "task-1.json"
    assert oct(task_path.stat().st_mode & 0o777) == "0o600"
    assert set(json.loads(task_path.read_text())) == set(WarmerTask.model_fields)
    assert list(plan_area(root, "plan-1", "outputs").glob("task-1-*.jsonl"))
    state = json.loads((plan_area(root, "plan-1", "state") / "task-1.json").read_text())
    assert state["status"] == "terminal"
    assert state["automatic_retry_allowed"] is False
    assert state["warmer_pid"] > 0
    assert (root / "claimed" / "plan-1.json").exists()
    assert not (root / "plans" / "plan-1.json").exists()


def test_disabled_stop_deadline_budget_and_rate_results(root, clock):
    warmer = make_warmer(root)
    write_plan(root, plan_data(clock, item_count=2, max_bytes=4096))
    with Dispatcher(config(root, warmer), now=clock.now) as dispatcher:
        dispatcher.run_once()
    assert read_result(root, "task-1")["reason"] == "dry_run"
    assert read_result(root, "task-2")["reason"] == "dry_run"
    assert not plan_area(root, "plan-1", "tasks").exists()

    root2 = root.parent / "stop-root"
    root2.mkdir(mode=0o700)
    write_plan(root2, plan_data(clock))
    write_owner_file(root2 / "stop")
    with Dispatcher(config(root2, warmer), now=clock.now) as dispatcher:
        dispatcher.run_once()
    assert read_result(root2)["status"] == "cancelled"

    root3 = root.parent / "budget-root"
    root3.mkdir(mode=0o700)
    write_plan(root3, plan_data(clock, item_count=2, max_bytes=4096))
    enable(root3)
    with Dispatcher(
        config(root3, warmer),
        now=clock.now,
        monotonic=clock.monotonic,
    ) as dispatcher:
        dispatcher.run_once()
    assert read_result(root3, "task-1")["status"] == "completed"
    assert read_result(root3, "task-2")["reason"] == "budget"

    root4 = root.parent / "rate-root"
    root4.mkdir(mode=0o700)
    write_plan(root4, plan_data(clock, item_count=2))
    enable(root4)
    with Dispatcher(
        config(root4, warmer, max_items_per_min=1),
        now=clock.now,
        monotonic=clock.monotonic,
        sleep=clock.sleep,
    ) as dispatcher:
        dispatcher.run_once()
    assert read_result(root4, "task-2")["status"] == "completed"

    root5 = root.parent / "deadline-root"
    root5.mkdir(mode=0o700)
    plan = plan_data(clock)
    plan["deadline"] = (clock.value - timedelta(seconds=1)).isoformat().replace("+00:00", "Z")
    plan["created_at"] = (clock.value - timedelta(seconds=2)).isoformat().replace("+00:00", "Z")
    write_plan(root5, plan)
    enable(root5)
    with Dispatcher(config(root5, warmer), now=clock.now) as dispatcher:
        dispatcher.run_once()
    assert read_result(root5)["reason"] == "deadline"


def test_terminal_result_dedupes_repeated_runs(root, clock, monkeypatch):
    counter = root.parent / "count"
    warmer = make_warmer(root, counter=counter)
    write_plan(root, plan_data(clock))
    enable(root)
    with Dispatcher(
        config(root, warmer),
        now=clock.now,
        monotonic=clock.monotonic,
    ) as dispatcher:
        dispatcher.run_once()

        def historical_plan_must_not_be_reloaded(_name):
            raise AssertionError("completed plan was reparsed")

        monkeypatch.setattr(dispatcher, "_load_plan", historical_plan_must_not_be_reloaded)
        dispatcher.run_once()
    assert counter.read_text() == "1"


def test_global_lock_refuses_second_dispatcher(root, clock):
    warmer = make_warmer(root)
    with Dispatcher(config(root, warmer), now=clock.now):
        with pytest.raises(SecureFileError, match="another dispatcher"):
            Dispatcher(config(root, warmer), now=clock.now)


def test_running_state_becomes_ambiguous_without_retry(root, clock):
    warmer = make_warmer(root)
    with Dispatcher(config(root, warmer), now=clock.now) as dispatcher:
        state = {
            "schema_version": 1,
            "status": "running",
            "attempt_id": "a" * 32,
            "automatic_retry_allowed": False,
            "run_id": "run-1",
            "plan_id": "plan-1",
            "task_id": "task-1",
            "volume_id": "vol-1",
            "bytes_requested": 4096,
            "started_at": "2026-11-03T09:59:59Z",
            "subprocess_deadline": "2026-11-03T10:01:00Z",
            "dispatcher_pid": 1234,
            "warmer_pid": 5678,
            "warmer_command": str(warmer),
            "launched_at": "2026-11-03T09:59:59Z",
            "finished_at": None,
            "result_status": None,
            "supersedes": None,
        }
        assert dispatcher._write_state(state, no_overwrite=True)
    with Dispatcher(config(root, warmer), now=clock.now) as dispatcher:
        dispatcher.run_once()
    result = read_result(root)
    assert result["status"] == "failed"
    assert result["reason"] == "ambiguous_manual_review"
    assert result["bytes_read_is_lower_bound"] is True
    state = json.loads((plan_area(root, "plan-1", "state") / "task-1.json").read_text())
    assert state["status"] == "ambiguous"
    assert state["automatic_retry_allowed"] is False


def test_persistent_rate_state_survives_restart(root, clock):
    warmer = make_warmer(root)
    write_plan(root, plan_data(clock))
    enable(root)
    with Dispatcher(
        config(root, warmer, max_items_per_min=1),
        now=clock.now,
        monotonic=clock.monotonic,
    ) as dispatcher:
        dispatcher.run_once()

    second = plan_data(clock, plan_id="plan-2")
    second["items"][0]["task_id"] = "task-2"
    write_plan(root, second)
    with Dispatcher(
        config(root, warmer, max_items_per_min=1),
        now=clock.now,
        monotonic=clock.monotonic,
        sleep=clock.sleep,
    ) as dispatcher:
        dispatcher.run_once()
    result = json.loads((plan_area(root, "plan-2", "results") / "task-2.json").read_text())
    assert result["status"] == "completed"


def test_rate_capacity_that_misses_deadline_is_skipped(root, clock):
    warmer = make_warmer(root)
    write_plan(root, plan_data(clock))
    enable(root)
    with Dispatcher(
        config(root, warmer, max_items_per_min=1),
        now=clock.now,
        monotonic=clock.monotonic,
    ) as dispatcher:
        dispatcher.run_once()

    second = plan_data(clock, plan_id="plan-2")
    second["items"][0]["task_id"] = "task-2"
    second["deadline"] = (clock.value + timedelta(seconds=30)).isoformat().replace(
        "+00:00", "Z"
    )
    write_plan(root, second)
    with Dispatcher(
        config(root, warmer, max_items_per_min=1),
        now=clock.now,
        monotonic=clock.monotonic,
    ) as dispatcher:
        dispatcher.run_once()
    result = json.loads((plan_area(root, "plan-2", "results") / "task-2.json").read_text())
    assert result["reason"] == "rate_limit_deadline"


def test_rate_wait_reacts_to_stop_and_enable_removal(root, clock):
    warmer = make_warmer(root)
    write_plan(root, plan_data(clock))
    enable(root)
    with Dispatcher(
        config(root, warmer, max_items_per_min=1),
        now=clock.now,
        monotonic=clock.monotonic,
    ) as dispatcher:
        dispatcher.run_once()

    second = plan_data(clock, plan_id="plan-2")
    second["items"][0]["task_id"] = "task-2"
    write_plan(root, second)
    stopped = False

    def stop_sleep(seconds):
        nonlocal stopped
        clock.sleep(seconds)
        if not stopped:
            stopped = True
            write_owner_file(root / "stop")

    with Dispatcher(
        config(root, warmer, max_items_per_min=1),
        now=clock.now,
        monotonic=clock.monotonic,
        sleep=stop_sleep,
    ) as dispatcher:
        dispatcher.run_once()
    result = json.loads((plan_area(root, "plan-2", "results") / "task-2.json").read_text())
    assert result["status"] == "cancelled"
    (root / "stop").unlink()

    third = plan_data(clock, plan_id="plan-3")
    third["items"][0]["task_id"] = "task-3"
    write_plan(root, third)
    removed = False

    def disable_sleep(seconds):
        nonlocal removed
        clock.sleep(seconds)
        if not removed:
            removed = True
            (root / "enable-real-reads").unlink()

    with Dispatcher(
        config(root, warmer, max_items_per_min=1),
        now=clock.now,
        monotonic=clock.monotonic,
        sleep=disable_sleep,
    ) as dispatcher:
        dispatcher.run_once()
    result = json.loads((plan_area(root, "plan-3", "results") / "task-3.json").read_text())
    assert result["reason"] == "dry_run"


def test_rate_window_expires_at_sixty_seconds(root, clock):
    warmer = make_warmer(root)
    write_plan(root, plan_data(clock))
    enable(root)
    with Dispatcher(
        config(root, warmer, max_items_per_min=1),
        now=clock.now,
        monotonic=clock.monotonic,
    ) as dispatcher:
        dispatcher.run_once()
    clock.sleep(60)
    second = plan_data(clock, plan_id="plan-2")
    second["items"][0]["task_id"] = "task-2"
    write_plan(root, second)
    with Dispatcher(
        config(root, warmer, max_items_per_min=1),
        now=clock.now,
        monotonic=clock.monotonic,
    ) as dispatcher:
        dispatcher.run_once()
    result = json.loads((plan_area(root, "plan-2", "results") / "task-2.json").read_text())
    assert result["status"] == "completed"


def test_default_global_limits_are_safety_defaults(root):
    warmer = make_warmer(root)
    cfg = config(root, warmer)
    assert cfg.max_items_per_min == 60
    assert cfg.max_bytes_per_min == 64 * 2**20


def test_dispatcher_never_prunes_evidence_or_unrelated_files(root, clock):
    warmer = make_warmer(root)
    write_plan(root, plan_data(clock))
    enable(root)
    write_owner_file(root / "operator-note", "retain")
    with Dispatcher(
        config(root, warmer),
        now=clock.now,
        monotonic=clock.monotonic,
    ) as dispatcher:
        dispatcher.run_once()
    before = {
        path.relative_to(root): path.read_bytes()
        for path in root.rglob("*")
        if path.is_file()
    }
    clock.sleep(24 * 60 * 60)
    with Dispatcher(config(root, warmer), now=clock.now) as dispatcher:
        dispatcher.run_once()
    after = {
        path.relative_to(root): path.read_bytes()
        for path in root.rglob("*")
        if path.is_file()
    }
    assert after == before
    assert after[Path("operator-note")] == b"retain"


def test_mixed_plan_skips_bad_item_and_continues_in_order(root, clock):
    counter = root.parent / "mixed-count"
    warmer = make_warmer(root, counter=counter)
    plan = plan_data(clock, item_count=2)
    plan["items"][0]["operation"] = "write"
    write_plan(root, plan)
    enable(root)
    with Dispatcher(
        config(root, warmer),
        now=clock.now,
        monotonic=clock.monotonic,
    ) as dispatcher:
        dispatcher.run_once()
    assert read_result(root, "task-1")["reason"] == "unauthorized_operation"
    assert read_result(root, "task-2")["status"] == "completed"
    assert counter.read_text() == "1"


def test_durable_identity_uses_run_plan_task_tuple(root, clock):
    counter = root.parent / "identity-count"
    warmer = make_warmer(root, counter=counter)
    first = plan_data(clock)
    write_plan(root, first)
    enable(root)
    with Dispatcher(
        config(root, warmer),
        now=clock.now,
        monotonic=clock.monotonic,
    ) as dispatcher:
        dispatcher.run_once()
    first_result = (plan_area(root, "plan-1", "results") / "task-1.json").read_bytes()

    duplicate = plan_data(clock)
    write_plan(root, duplicate, suffix="-duplicate")
    with Dispatcher(
        config(root, warmer),
        now=clock.now,
        monotonic=clock.monotonic,
    ) as dispatcher:
        dispatcher.run_once()
    assert counter.read_text() == "1"
    assert (plan_area(root, "plan-1", "results") / "task-1.json").read_bytes() == first_result

    other_run = plan_data(clock)
    other_run["run_id"] = "run-2"
    write_plan(root, other_run, suffix="-run-2")
    with Dispatcher(
        config(root, warmer),
        now=clock.now,
        monotonic=clock.monotonic,
    ) as dispatcher:
        dispatcher.run_once()
    assert counter.read_text() == "2"
    assert (
        plan_area(root, "plan-1", "results", run_id="run-2") / "task-1.json"
    ).exists()


def test_explicit_requeue_uses_new_task_id_and_preserves_lineage(root, clock):
    warmer = make_warmer(root)
    original = plan_data(clock)
    write_plan(root, original)
    enable(root)
    with Dispatcher(
        config(root, warmer),
        now=clock.now,
        monotonic=clock.monotonic,
    ) as dispatcher:
        dispatcher.run_once()
    original_result = (plan_area(root, "plan-1", "results") / "task-1.json").read_bytes()

    retry = plan_data(clock, plan_id="plan-2")
    retry["items"][0]["task_id"] = "task-2"
    retry["items"][0]["supersedes"] = {
        "run_id": "run-1",
        "plan_id": "plan-1",
        "task_id": "task-1",
    }
    write_plan(root, retry)
    with Dispatcher(
        config(root, warmer),
        now=clock.now,
        monotonic=clock.monotonic,
    ) as dispatcher:
        dispatcher.run_once()
    state = json.loads((plan_area(root, "plan-2", "state") / "task-2.json").read_text())
    assert state["supersedes"] == retry["items"][0]["supersedes"]
    assert (plan_area(root, "plan-1", "results") / "task-1.json").read_bytes() == original_result


def test_near_deadline_does_not_spawn(root, clock):
    counter = root.parent / "near-deadline-count"
    warmer = make_warmer(root, counter=counter)
    plan = plan_data(clock)
    plan["deadline"] = (clock.value + timedelta(seconds=0.05)).isoformat().replace(
        "+00:00", "Z"
    )
    write_plan(root, plan)
    enable(root)
    with Dispatcher(config(root, warmer), now=clock.now) as dispatcher:
        dispatcher.run_once()
    assert read_result(root)["reason"] == "deadline"
    assert not counter.exists()


def test_stop_file_cancels_inflight_process(root, clock):
    warmer = make_warmer(root, sleep_seconds=10)
    write_plan(root, plan_data(clock))
    enable(root)

    def request_stop():
        time.sleep(0.15)
        write_owner_file(root / "stop")

    thread = threading.Thread(target=request_stop)
    thread.start()
    try:
        with Dispatcher(
            config(root, warmer),
            now=clock.now,
            monotonic=time.monotonic,
        ) as dispatcher:
            dispatcher.run_once()
    finally:
        thread.join()
    result = read_result(root)
    assert result["status"] == "failed"
    assert result["reason"] == "ambiguous_stop_without_terminal"
    assert result["bytes_read_is_lower_bound"] is True
    assert (root / "stop").exists()


def test_child_resets_ignored_stop_signals_before_exec(root, clock):
    observed = root.parent / "child-signal-state"
    terminated = root.parent / "child-terminated"
    warmer = root.parent / "signal-aware-warmer"
    warmer.write_text(
        f"""#!/usr/bin/env python3
import pathlib
import signal
import time
observed = pathlib.Path({str(observed)!r})
terminated = pathlib.Path({str(terminated)!r})
observed.write_text(
    f"{{signal.getsignal(signal.SIGTERM) is signal.SIG_IGN}},"
    f"{{signal.getsignal(signal.SIGINT) is signal.SIG_IGN}}"
)
def stop(_signum, _frame):
    terminated.write_text("terminated")
    raise SystemExit(143)
signal.signal(signal.SIGTERM, stop)
while True:
    time.sleep(0.1)
"""
    )
    os.chmod(warmer, 0o700)
    write_plan(root, plan_data(clock))
    enable(root)

    def request_stop():
        deadline = time.monotonic() + 5
        while not observed.exists() and time.monotonic() < deadline:
            time.sleep(0.02)
        write_owner_file(root / "stop")

    previous_sigterm = signal.signal(signal.SIGTERM, signal.SIG_IGN)
    previous_sigint = signal.signal(signal.SIGINT, signal.SIG_IGN)
    thread = threading.Thread(target=request_stop)
    thread.start()
    try:
        with Dispatcher(
            config(root, warmer),
            now=clock.now,
            monotonic=time.monotonic,
        ) as dispatcher:
            dispatcher.run_once()
    finally:
        thread.join()
        signal.signal(signal.SIGTERM, previous_sigterm)
        signal.signal(signal.SIGINT, previous_sigint)

    assert observed.read_text() == "False,False"
    assert terminated.read_text() == "terminated"
    assert read_result(root)["reason"] == "ambiguous_stop_without_terminal"


def test_stop_file_persists_across_restart(root, clock):
    warmer = make_warmer(root)
    write_owner_file(root / "stop")
    write_plan(root, plan_data(clock))
    enable(root)
    with Dispatcher(config(root, warmer), now=clock.now) as dispatcher:
        dispatcher.run_once()
    assert read_result(root)["status"] == "cancelled"
    assert (root / "stop").exists()

    second = plan_data(clock, plan_id="plan-2")
    second["items"][0]["task_id"] = "task-2"
    write_plan(root, second)
    with Dispatcher(config(root, warmer), now=clock.now) as dispatcher:
        dispatcher.run_once()
    result = json.loads((plan_area(root, "plan-2", "results") / "task-2.json").read_text())
    assert result["status"] == "cancelled"
    assert (root / "stop").exists()


def test_parent_death_setup_closes_registration_race():
    calls = []

    def prctl(*args):
        calls.append(args)
        return 0

    killed = []
    _apply_parent_death_signal(
        1234,
        prctl=prctl,
        getppid=lambda: 1,
        kill=lambda pid, signum: killed.append((pid, signum)),
    )
    assert calls == [(1, signal.SIGTERM, 0, 0, 0)]
    assert killed == [(os.getpid(), signal.SIGTERM)]


def test_dispatcher_death_terminates_warmer_process(root, clock):
    ready = root.parent / "warmer-ready"
    terminated = root.parent / "warmer-terminated"
    warmer = root.parent / "parent-bound-warmer"
    warmer.write_text(
        f"""#!/usr/bin/env python3
import pathlib
import signal
import time
ready = pathlib.Path({str(ready)!r})
terminated = pathlib.Path({str(terminated)!r})
def stop(_signum, _frame):
    terminated.write_text("terminated")
    raise SystemExit(143)
signal.signal(signal.SIGTERM, stop)
print('{{"type":"compound","operation":"read"}}', flush=True)
ready.write_text("ready")
while True:
    time.sleep(0.1)
"""
    )
    os.chmod(warmer, 0o700)
    write_plan(root, plan_data(clock))
    enable(root)

    def dispatch():
        with Dispatcher(
            config(root, warmer),
            now=clock.now,
            monotonic=time.monotonic,
        ) as dispatcher:
            dispatcher.run_once()

    process = multiprocessing.Process(target=dispatch)
    process.start()
    deadline = time.monotonic() + 5
    while not ready.exists() and time.monotonic() < deadline:
        time.sleep(0.02)
    assert ready.exists()
    os.kill(process.pid, signal.SIGKILL)
    process.join(timeout=5)
    deadline = time.monotonic() + 5
    while not terminated.exists() and time.monotonic() < deadline:
        time.sleep(0.02)
    assert terminated.exists()
    assert list(plan_area(root, "plan-1", "outputs").glob("task-1-*.jsonl"))
    state = json.loads((plan_area(root, "plan-1", "state") / "task-1.json").read_text())
    assert state["status"] == "running"


@pytest.mark.parametrize(
    ("real_reads", "enable_file", "expected"),
    [
        (False, False, "dry_run"),
        (False, True, "dry_run"),
        (True, False, "dry_run"),
        (True, True, "completed"),
    ],
)
def test_real_read_gate_truth_table(root, clock, real_reads, enable_file, expected):
    counter = root.parent / "gate-count"
    warmer = make_warmer(root, counter=counter)
    write_plan(root, plan_data(clock))
    if enable_file:
        write_owner_file(root / "enable-real-reads")
    with Dispatcher(
        config(root, warmer, real_reads=real_reads),
        now=clock.now,
        monotonic=clock.monotonic,
    ) as dispatcher:
        dispatcher.run_once()
    result = read_result(root)
    assert result["status"] == ("completed" if expected == "completed" else "skipped")
    assert result["reason"] == ("ok" if expected == "completed" else expected)
    assert counter.exists() is (expected == "completed")


def test_real_reads_require_rate_limits(root):
    warmer = make_warmer(root)
    with pytest.raises(ValueError, match="requires both"):
        config(root, warmer, max_bytes_per_min=None)
    with pytest.raises(ValueError, match="requires both"):
        config(root, warmer, max_items_per_min=None)


def test_enable_removal_switches_remaining_items_to_fake_reader(root, clock):
    enable_path = root / "enable-real-reads"
    warmer = make_warmer(root, remove_path=enable_path)
    write_plan(root, plan_data(clock, item_count=2))
    enable(root)
    with Dispatcher(
        config(root, warmer),
        now=clock.now,
        monotonic=clock.monotonic,
    ) as dispatcher:
        dispatcher.run_once()
    assert read_result(root, "task-1")["status"] == "completed"
    result = read_result(root, "task-2")
    assert result["status"] == "cancelled"
    assert result["reason"] == "real_read_disabled"
    assert result["bytes_requested"] == 4096
    assert result["bytes_read"] == 0
    assert result["bytes_read_is_lower_bound"] is False
