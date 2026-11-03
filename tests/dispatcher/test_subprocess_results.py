from __future__ import annotations

import json
import time
from datetime import timedelta

import pytest

from predictive_cache.dispatcher.core import Dispatcher
from predictive_cache.dispatcher.schema import warmer_ref

from .conftest import config, make_warmer, plan_data, write_owner_file, write_plan


def plan_area(root, plan_id, area, run_id="run-1"):
    return root / "runs" / run_id / "plans" / plan_id / area


@pytest.mark.parametrize(
    ("warmer_status", "expected"),
    [
        ("completed", "completed"),
        ("partial", "partial"),
        ("error", "failed"),
        ("timeout", "failed"),
        ("cancelled", "cancelled"),
    ],
)
def test_terminal_status_mapping(root, clock, warmer_status, expected):
    warmer = make_warmer(root, status=warmer_status, reason=f"warmer_{warmer_status}")
    write_plan(root, plan_data(clock))
    write_owner_file(root / "enable-real-reads")
    with Dispatcher(
        config(root, warmer),
        now=clock.now,
        monotonic=clock.monotonic,
    ) as dispatcher:
        dispatcher.run_once()
    result = json.loads((plan_area(root, "plan-1", "results") / "task-1.json").read_text())
    assert result["status"] == expected
    assert result["reason"] == f"warmer_{warmer_status}"


@pytest.mark.parametrize(
    ("malformed", "reason"),
    [
        ("not-json", "ambiguous_stdout_invalid_json"),
        ('{"type":"result"}', "ambiguous_warmer_terminal_invalid"),
        ('{"type":"compound"}', "ambiguous_warmer_terminal_missing_or_multiple"),
    ],
)
def test_malformed_terminal_fails_without_claiming_bytes(root, clock, malformed, reason):
    warmer = make_warmer(root, malformed=malformed)
    write_plan(root, plan_data(clock))
    write_owner_file(root / "enable-real-reads")
    with Dispatcher(
        config(root, warmer),
        now=clock.now,
        monotonic=clock.monotonic,
    ) as dispatcher:
        dispatcher.run_once()
    result = json.loads((plan_area(root, "plan-1", "results") / "task-1.json").read_text())
    assert result["status"] == "failed"
    assert result["reason"] == reason
    assert result["bytes_read"] == 0


def test_existing_result_is_never_overwritten(root, clock):
    warmer = make_warmer(root)
    write_plan(root, plan_data(clock))
    write_owner_file(root / "enable-real-reads")
    with Dispatcher(
        config(root, warmer),
        now=clock.now,
        monotonic=clock.monotonic,
    ) as dispatcher:
        dispatcher.run_once()
    result_path = plan_area(root, "plan-1", "results") / "task-1.json"
    original = result_path.read_bytes()
    with Dispatcher(
        config(root, warmer),
        now=clock.now,
        monotonic=clock.monotonic,
    ) as dispatcher:
        dispatcher.run_once()
    assert result_path.read_bytes() == original


def test_oversize_subprocess_output_is_bounded_and_failed(root, clock):
    warmer = make_warmer(root, malformed="x" * 2048)
    write_plan(root, plan_data(clock))
    write_owner_file(root / "enable-real-reads")
    with Dispatcher(
        config(root, warmer, max_output_bytes=128),
        now=clock.now,
        monotonic=clock.monotonic,
    ) as dispatcher:
        dispatcher.run_once()
    result = json.loads((plan_area(root, "plan-1", "results") / "task-1.json").read_text())
    output = next((plan_area(root, "plan-1", "outputs")).glob("task-1-*.jsonl"))
    assert result["reason"] == "ambiguous_stdout_overflow"
    assert output.stat().st_size <= 128


@pytest.mark.parametrize("terminal_status", ["partial", "timeout", "error", "cancelled"])
def test_terminal_failures_are_never_retried(root, clock, terminal_status):
    counter = root.parent / "terminal-count"
    warmer = make_warmer(root, status=terminal_status, counter=counter)
    write_plan(root, plan_data(clock))
    write_owner_file(root / "enable-real-reads")
    with Dispatcher(
        config(root, warmer),
        now=clock.now,
        monotonic=clock.monotonic,
    ) as dispatcher:
        dispatcher.run_once()
        dispatcher.run_once()
    assert counter.read_text() == "1"


def test_explicit_new_task_id_is_a_new_attempt(root, clock):
    counter = root.parent / "attempt-count"
    warmer = make_warmer(root, status="partial", reason="eof", counter=counter)
    write_plan(root, plan_data(clock, item_count=2))
    write_owner_file(root / "enable-real-reads")
    with Dispatcher(
        config(root, warmer),
        now=clock.now,
        monotonic=clock.monotonic,
    ) as dispatcher:
        dispatcher.run_once()
    assert counter.read_text() == "2"


def test_partial_line_is_preserved_and_ambiguous(root, clock):
    warmer = make_warmer(root, raw_output=b'{"type":"compound"', exit_code=1)
    write_plan(root, plan_data(clock))
    write_owner_file(root / "enable-real-reads")
    with Dispatcher(
        config(root, warmer),
        now=clock.now,
        monotonic=clock.monotonic,
    ) as dispatcher:
        dispatcher.run_once()
    result = json.loads((plan_area(root, "plan-1", "results") / "task-1.json").read_text())
    output = next((plan_area(root, "plan-1", "outputs")).glob("*.jsonl"))
    assert result["reason"] == "ambiguous_stdout_partial_line"
    assert output.read_bytes() == b'{"type":"compound"'


def test_stderr_overflow_is_bounded_and_ambiguous(root, clock):
    warmer = make_warmer(root, stderr_bytes=2048)
    write_plan(root, plan_data(clock))
    write_owner_file(root / "enable-real-reads")
    with Dispatcher(
        config(root, warmer, max_stderr_bytes=128),
        now=clock.now,
        monotonic=clock.monotonic,
    ) as dispatcher:
        dispatcher.run_once()
    result = json.loads((plan_area(root, "plan-1", "results") / "task-1.json").read_text())
    stderr = next((plan_area(root, "plan-1", "outputs")).glob("*.stderr"))
    assert result["reason"] == "ambiguous_stderr_overflow"
    assert stderr.stat().st_size <= 128


def test_stdout_line_count_is_bounded(root, clock):
    line = b'{"type":"compound"}\n'
    warmer = make_warmer(root, raw_output=line * 5_001, exit_code=1)
    write_plan(root, plan_data(clock))
    write_owner_file(root / "enable-real-reads")
    with Dispatcher(
        config(root, warmer),
        now=clock.now,
        monotonic=clock.monotonic,
    ) as dispatcher:
        dispatcher.run_once()
    result = json.loads((plan_area(root, "plan-1", "results") / "task-1.json").read_text())
    assert result["reason"] == "ambiguous_stdout_line_count"


def test_multiple_terminals_are_ambiguous(root, clock):
    terminal = json.dumps(
        {
            "type": "result",
            "status": "completed",
            "reason": "ok",
            "bytes_requested": 4096,
            "bytes_read": 4096,
            "bytes_read_is_lower_bound": False,
            "plan_ref": warmer_ref("plan-1"),
            "task_ref": warmer_ref("task-1"),
        }
    )
    warmer = make_warmer(root, raw_output=f"{terminal}\n{terminal}\n".encode(), exit_code=0)
    write_plan(root, plan_data(clock))
    write_owner_file(root / "enable-real-reads")
    with Dispatcher(
        config(root, warmer),
        now=clock.now,
        monotonic=clock.monotonic,
    ) as dispatcher:
        dispatcher.run_once()
    result = json.loads((plan_area(root, "plan-1", "results") / "task-1.json").read_text())
    assert result["reason"] == "ambiguous_warmer_terminal_missing_or_multiple"


def test_duplicate_terminal_keys_are_ambiguous(root, clock):
    raw = (
        '{"type":"result","status":"error","status":"completed",'
        '"reason":"ok","bytes_requested":4096,"bytes_read":4096,'
        '"bytes_read_is_lower_bound":false,'
        f'"plan_ref":"{warmer_ref("plan-1")}",'
        f'"task_ref":"{warmer_ref("task-1")}"}}\n'
    ).encode()
    warmer = make_warmer(root, raw_output=raw, exit_code=0)
    write_plan(root, plan_data(clock))
    write_owner_file(root / "enable-real-reads")
    with Dispatcher(
        config(root, warmer),
        now=clock.now,
        monotonic=clock.monotonic,
    ) as dispatcher:
        dispatcher.run_once()
    result = json.loads((plan_area(root, "plan-1", "results") / "task-1.json").read_text())
    assert result["reason"] == "ambiguous_stdout_invalid_json"


def test_terminal_exit_mismatch_is_ambiguous(root, clock):
    warmer = make_warmer(root, status="completed", exit_code=1)
    write_plan(root, plan_data(clock))
    write_owner_file(root / "enable-real-reads")
    with Dispatcher(
        config(root, warmer),
        now=clock.now,
        monotonic=clock.monotonic,
    ) as dispatcher:
        dispatcher.run_once()
    result = json.loads((plan_area(root, "plan-1", "results") / "task-1.json").read_text())
    assert result["reason"] == "ambiguous_terminal_exit_mismatch"


def test_orphaned_worker_is_terminated_before_next_admission(root, clock):
    sentinel = root.parent / "orphan-sentinel"
    warmer = root.parent / "forking-warmer"
    terminal = json.dumps(
        {
            "type": "result",
            "status": "completed",
            "reason": "ok",
            "bytes_requested": 4096,
            "bytes_read": 4096,
            "bytes_read_is_lower_bound": False,
            "plan_ref": warmer_ref("plan-1"),
            "task_ref": warmer_ref("task-1"),
        }
    )
    warmer.write_text(
        f"""#!/usr/bin/env python3
import os
import pathlib
import time
if os.fork() == 0:
    time.sleep(2)
    pathlib.Path({str(sentinel)!r}).write_text("survived")
    raise SystemExit(0)
print({terminal!r}, flush=True)
raise SystemExit(0)
"""
    )
    warmer.chmod(0o700)
    write_plan(root, plan_data(clock))
    write_owner_file(root / "enable-real-reads")
    with Dispatcher(
        config(root, warmer),
        now=clock.now,
        monotonic=time.monotonic,
    ) as dispatcher:
        dispatcher.run_once()
    result = json.loads((plan_area(root, "plan-1", "results") / "task-1.json").read_text())
    time.sleep(2.1)
    assert result["reason"] == "ambiguous_orphaned_process_group"
    assert not sentinel.exists()


def test_invalid_arguments_without_prevalidation_proof_is_ambiguous(root, clock):
    warmer = make_warmer(root, raw_output=b"", exit_code=2)
    write_plan(root, plan_data(clock))
    write_owner_file(root / "enable-real-reads")
    with Dispatcher(
        config(root, warmer),
        now=clock.now,
        monotonic=clock.monotonic,
    ) as dispatcher:
        dispatcher.run_once()
    result = json.loads((plan_area(root, "plan-1", "results") / "task-1.json").read_text())
    assert result["reason"] == "ambiguous_warmer_terminal_missing_or_multiple"
    assert result["bytes_read"] == 0
    assert result["bytes_read_is_lower_bound"] is True


@pytest.mark.parametrize(
    ("status", "reason", "exit_code", "expected_status"),
    [
        ("timeout", "task_load_timeout", 1, "failed"),
        ("cancelled", "task_load_cancelled", 130, "cancelled"),
        ("cancelled", "task_load_cancelled", 143, "cancelled"),
        ("error", "invalid_task", 2, "failed"),
    ],
)
def test_prevalidation_result_synthesizes_trusted_terminal(
    root,
    clock,
    status,
    reason,
    exit_code,
    expected_status,
):
    prevalidation = {
        "type": "pre_validation_result",
        "status": status,
        "reason": reason,
        "execution_started": False,
    }
    warmer = make_warmer(
        root,
        raw_output=(json.dumps(prevalidation) + "\n").encode(),
        exit_code=exit_code,
    )
    write_plan(root, plan_data(clock))
    write_owner_file(root / "enable-real-reads")
    with Dispatcher(
        config(root, warmer),
        now=clock.now,
        monotonic=clock.monotonic,
    ) as dispatcher:
        dispatcher.run_once()
    result = json.loads((plan_area(root, "plan-1", "results") / "task-1.json").read_text())
    terminal = result["warmer_terminal"]
    assert result["status"] == expected_status
    assert result["bytes_read"] == 0
    assert result["bytes_read_is_lower_bound"] is False
    assert terminal["plan_ref"] == warmer_ref("plan-1")
    assert terminal["task_ref"] == warmer_ref("task-1")
    assert terminal["bytes_requested"] == 4096
    assert terminal["execution_started"] is False


def test_prevalidation_result_remains_valid_after_restart(root, clock):
    prevalidation = {
        "type": "pre_validation_result",
        "status": "error",
        "reason": "invalid_task",
        "execution_started": False,
    }
    counter = root.parent / "prevalidation-count"
    warmer = make_warmer(
        root,
        raw_output=(json.dumps(prevalidation) + "\n").encode(),
        exit_code=2,
        counter=counter,
    )
    write_plan(root, plan_data(clock))
    write_owner_file(root / "enable-real-reads")
    with Dispatcher(
        config(root, warmer),
        now=clock.now,
        monotonic=clock.monotonic,
    ) as dispatcher:
        dispatcher.run_once()

    with Dispatcher(
        config(root, warmer),
        now=clock.now,
        monotonic=clock.monotonic,
    ) as dispatcher:
        dispatcher.run_once()

    result = json.loads((plan_area(root, "plan-1", "results") / "task-1.json").read_text())
    assert result["status"] == "failed"
    assert result["warmer_terminal"]["execution_started"] is False
    assert counter.read_text() == "1"


def test_prevalidation_after_compound_is_ambiguous(root, clock):
    output = (
        json.dumps({"type": "compound", "operation": "read"})
        + "\n"
        + json.dumps(
            {
                "type": "pre_validation_result",
                "status": "timeout",
                "reason": "task_load_timeout",
                "execution_started": False,
            }
        )
        + "\n"
    )
    warmer = make_warmer(root, raw_output=output.encode(), exit_code=1)
    write_plan(root, plan_data(clock))
    write_owner_file(root / "enable-real-reads")
    with Dispatcher(
        config(root, warmer),
        now=clock.now,
        monotonic=clock.monotonic,
    ) as dispatcher:
        dispatcher.run_once()
    result = json.loads((plan_area(root, "plan-1", "results") / "task-1.json").read_text())
    assert result["reason"] == "ambiguous_warmer_terminal_missing_or_multiple"
    assert result["bytes_read_is_lower_bound"] is True


def test_normal_terminal_cannot_claim_execution_never_started(root, clock):
    terminal = {
        "type": "result",
        "status": "error",
        "reason": "invalid_task",
        "bytes_requested": 4096,
        "bytes_read": 0,
        "bytes_read_is_lower_bound": False,
        "execution_started": False,
        "plan_ref": warmer_ref("plan-1"),
        "task_ref": warmer_ref("task-1"),
    }
    warmer = make_warmer(
        root,
        raw_output=(json.dumps(terminal) + "\n").encode(),
        exit_code=2,
    )
    write_plan(root, plan_data(clock))
    write_owner_file(root / "enable-real-reads")
    with Dispatcher(
        config(root, warmer),
        now=clock.now,
        monotonic=clock.monotonic,
    ) as dispatcher:
        dispatcher.run_once()
    result = json.loads((plan_area(root, "plan-1", "results") / "task-1.json").read_text())
    assert result["reason"] == "ambiguous_warmer_terminal_invalid"


def test_partial_attempt_still_consumes_durable_rate_capacity(root, clock):
    warmer = make_warmer(root, status="partial")
    write_plan(root, plan_data(clock))
    write_owner_file(root / "enable-real-reads")
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


@pytest.mark.parametrize(
    ("status", "reason", "expected_invocations", "second_status"),
    [
        ("partial", "eof", 2, "partial"),
        ("partial", "short_read", 2, "partial"),
        ("error", "getattr_range_exceeds_size", 2, "failed"),
        ("error", "getattr_NFS4ERR_STALE", 2, "failed"),
        ("error", "connection_error:ConnectionRefusedError", 1, "cancelled"),
        ("timeout", "deadline_exceeded", 1, "cancelled"),
    ],
)
def test_plan_continuation_classification(
    root,
    clock,
    status,
    reason,
    expected_invocations,
    second_status,
):
    counter = root.parent / "classification-count"
    warmer = make_warmer(root, status=status, reason=reason, counter=counter)
    write_plan(root, plan_data(clock, item_count=2))
    write_owner_file(root / "enable-real-reads")
    with Dispatcher(
        config(root, warmer),
        now=clock.now,
        monotonic=clock.monotonic,
    ) as dispatcher:
        dispatcher.run_once()
    second = json.loads(
        (plan_area(root, "plan-1", "results") / "task-2.json").read_text()
    )
    assert int(counter.read_text()) == expected_invocations
    assert second["status"] == second_status


def test_ambiguous_result_cancels_remaining_items(root, clock):
    counter = root.parent / "ambiguous-count"
    warmer = make_warmer(root, malformed="not-json", counter=counter)
    write_plan(root, plan_data(clock, item_count=2))
    write_owner_file(root / "enable-real-reads")
    with Dispatcher(
        config(root, warmer),
        now=clock.now,
        monotonic=clock.monotonic,
    ) as dispatcher:
        dispatcher.run_once()
    second = json.loads(
        (plan_area(root, "plan-1", "results") / "task-2.json").read_text()
    )
    assert counter.read_text() == "1"
    assert second["status"] == "cancelled"


@pytest.mark.parametrize(
    "terminal",
    [
        {
            "type": "result",
            "status": "completed",
            "reason": "ok",
            "bytes_requested": 4096,
            "bytes_read": 0,
            "bytes_read_is_lower_bound": False,
            "plan_ref": warmer_ref("plan-1"),
            "task_ref": warmer_ref("task-1"),
        },
        {
            "type": "result",
            "status": "completed",
            "reason": "ok",
            "bytes_requested": 4096,
            "bytes_read": 4096,
            "bytes_read_is_lower_bound": True,
            "plan_ref": warmer_ref("plan-1"),
            "task_ref": warmer_ref("task-1"),
        },
        {
            "type": "result",
            "status": "timeout",
            "reason": "timeout",
            "bytes_requested": 4096,
            "bytes_read": 4097,
            "bytes_read_is_lower_bound": True,
            "plan_ref": warmer_ref("plan-1"),
            "task_ref": warmer_ref("task-1"),
        },
    ],
)
def test_invalid_terminal_byte_claims_are_ambiguous(root, clock, terminal):
    exit_code = 0 if terminal["status"] == "completed" else 1
    warmer = make_warmer(
        root,
        raw_output=(json.dumps(terminal) + "\n").encode(),
        exit_code=exit_code,
    )
    write_plan(root, plan_data(clock))
    write_owner_file(root / "enable-real-reads")
    with Dispatcher(
        config(root, warmer),
        now=clock.now,
        monotonic=clock.monotonic,
    ) as dispatcher:
        dispatcher.run_once()
    result = json.loads((plan_area(root, "plan-1", "results") / "task-1.json").read_text())
    assert result["reason"] == "ambiguous_warmer_terminal_invalid"
