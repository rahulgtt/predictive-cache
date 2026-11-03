import json
import os
import resource
import select
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

from predictive_cache.warmer.cli import _parser, _validate, main
from predictive_cache.warmer.execution import execute
from predictive_cache.warmer.limits import MAX_WORKER_ADDRESS_SPACE

TAG = "0123456789abcdef0123456789abcdef"
READ_TAG = "fedcba9876543210fedcba9876543210"
CLEANUP_TAG = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
ARGS = [
    "--host",
    "192.0.2.1",
    "--path",
    "/export/example",
    "--offset",
    "5",
    "--length",
    "10",
    "--max-bytes",
    "10",
    "--timeout-seconds",
    "5",
    "--uid",
    "1000",
    "--gid",
    "1000",
]


def _successful_worker(request, emit):
    emit({"type": "compound", "tag": TAG, "phase": "open"})
    emit(
        {
            "type": "compound",
            "tag": READ_TAG,
            "phase": "read",
            "offset": request["offset"],
            "count": request["length"],
        }
    )
    emit({"type": "progress", "bytes_read": request["length"]})
    return {"status": "completed", "bytes_read": request["length"], "reason": "ok"}


def _partial_worker(request, emit):
    emit(
        {
            "type": "compound",
            "tag": TAG,
            "phase": "read",
            "offset": request["offset"],
            "count": request["length"],
        }
    )
    emit({"type": "progress", "bytes_read": 4})
    return {"status": "partial", "bytes_read": 4, "reason": "eof"}


def _blocked_worker(request, emit):
    emit(
        {
            "type": "compound",
            "tag": TAG,
            "phase": "read",
            "offset": request["offset"],
            "count": request["length"],
        }
    )
    time.sleep(20)


def _invalid_worker(request, emit):
    emit(
        {
            "type": "compound",
            "tag": TAG,
            "phase": "read",
            "offset": request["offset"] + 1,
            "count": request["length"],
        }
    )
    time.sleep(20)


def _exception_worker(request, emit):
    raise ValueError(f"private path: {request['path']}")


def _duplicate_tag_worker(request, emit):
    emit({"type": "compound", "tag": TAG, "phase": "open"})
    emit(
        {
            "type": "compound",
            "tag": TAG,
            "phase": "read",
            "offset": request["offset"],
            "count": request["length"],
        }
    )


def _retry_short_worker(request, emit):
    emit(
        {
            "type": "compound",
            "tag": TAG,
            "phase": "read",
            "offset": request["offset"],
            "count": request["length"],
        }
    )
    emit({"type": "progress", "bytes_read": 4})
    emit(
        {
            "type": "compound",
            "tag": READ_TAG,
            "phase": "read",
            "offset": request["offset"] + 4,
            "count": request["length"] - 4,
        }
    )


def _overcount_worker(request, emit):
    emit(
        {
            "type": "compound",
            "tag": TAG,
            "phase": "read",
            "offset": request["offset"],
            "count": request["length"],
        }
    )
    emit({"type": "progress", "bytes_read": request["length"] + 1})


def _acknowledged_worker(request, emit):
    emit({"type": "compound", "tag": TAG, "phase": "open"})
    Path(request["marker"]).write_text("continued")
    emit(
        {
            "type": "compound",
            "tag": READ_TAG,
            "phase": "read",
            "offset": request["offset"],
            "count": request["length"],
        }
    )
    emit({"type": "progress", "bytes_read": request["length"]})
    return {"status": "completed", "bytes_read": request["length"], "reason": "ok"}


def _small_chunk_worker(request, emit):
    for tag, offset, count, total in (
        (TAG, request["offset"], 4, 4),
        (READ_TAG, request["offset"] + 4, 6, 10),
    ):
        emit({"type": "compound", "tag": tag, "phase": "read", "offset": offset, "count": count})
        emit({"type": "progress", "bytes_read": total})
    return {"status": "completed", "bytes_read": 10, "reason": "ok"}


def _handle_worker(request, emit):
    tag = f"pc1:w:{request['plan_ref']}:{request['task_ref']}:{TAG}"
    emit(
        {
            "type": "compound",
            "tag": tag,
            "phase": "read",
            "offset": request["offset"],
            "count": request["length"],
            "file_handle": request["file_handle"],
        }
    )
    emit({"type": "progress", "bytes_read": request["length"]})
    return {"status": "completed", "bytes_read": request["length"], "reason": "ok"}


def _wrong_task_tag_worker(request, emit):
    tag = f"pc1:w:{request['plan_ref']}:{TAG}:{READ_TAG}"
    emit({"type": "compound", "tag": tag, "phase": "open"})


def _stale_handle_worker(request, emit):
    tag = f"pc1:w:{request['plan_ref']}:{request['task_ref']}:{TAG}"
    emit({"type": "compound", "tag": tag, "phase": "getattr"})
    return {"status": "error", "bytes_read": 0, "reason": "getattr_NFS4ERR_STALE"}


def _read_rejected_worker(request, emit):
    emit({"type": "compound", "tag": TAG, "phase": "read", "offset": 5, "count": 4})
    emit({"type": "progress", "bytes_read": 4})
    emit({"type": "compound", "tag": READ_TAG, "phase": "read", "offset": 9, "count": 6})
    emit({"type": "read_rejected"})
    return {"status": "error", "bytes_read": 4, "reason": "read_NFS4ERR_ACCESS"}


def _unexpected_read_rejection_worker(request, emit):
    emit({"type": "read_rejected"})


def _retry_rejected_read_worker(request, emit):
    emit({"type": "compound", "tag": TAG, "phase": "read", "offset": 5, "count": 4})
    emit({"type": "read_rejected"})
    emit({"type": "compound", "tag": READ_TAG, "phase": "read", "offset": 5, "count": 4})


def _limited_worker(request, emit):
    soft, _ = resource.getrlimit(resource.RLIMIT_AS)
    if soft == resource.RLIM_INFINITY or soft > MAX_WORKER_ADDRESS_SPACE:
        raise RuntimeError("worker_limit_not_applied")
    return _successful_worker(request, emit)


def _import_adapter_worker(request, emit):
    from predictive_cache.warmer import nfs41

    assert nfs41._OPS is not None
    return _successful_worker(request, emit)


def _request():
    return _validate(_parser().parse_args(ARGS))


@pytest.mark.parametrize(
    ("flag", "value"),
    [
        ("--host", "example.invalid"),
        ("--host", "fe80::1%eth0"),
        ("--host", "0.0.0.0"),
        ("--host", "::"),
        ("--host", "::ffff:0.0.0.0"),
        ("--port", "0"),
        ("--path", "/export/../secret"),
        ("--path", "/export//file"),
        ("--path", "/"),
        ("--offset", "-1"),
        ("--offset", str((1 << 64) - 5)),
        ("--length", "0"),
        ("--length", str(16 * 1024 * 1024 + 1)),
        ("--max-bytes", "9"),
        ("--timeout-seconds", "nan"),
        ("--timeout-seconds", "301"),
        ("--uid", "-1"),
        ("--gid", str(1 << 32)),
    ],
)
def test_invalid_arguments_never_start_worker(flag, value, monkeypatch):
    def unexpected_execute(*args, **kwargs):
        raise AssertionError("invalid input must not start a worker")

    monkeypatch.setattr("predictive_cache.warmer.cli.execute", unexpected_execute)
    with pytest.raises(SystemExit) as exc:
        main([*ARGS, flag, value])
    assert exc.value.code == 2


def test_supervisor_reports_only_acknowledged_bytes_and_safe_events():
    events = []
    result = execute(_request(), 5, events.append, worker=_successful_worker)
    assert result == {
        "type": "result",
        "status": "completed",
        "reason": "ok",
        "bytes_requested": 10,
        "bytes_read": 10,
        "bytes_read_is_lower_bound": False,
    }
    assert events == [
        {"type": "compound", "tag": TAG, "phase": "open"},
        {"type": "compound", "tag": READ_TAG, "phase": "read", "offset": 5, "count": 10},
    ]


def test_partial_is_not_completed():
    result = execute(_request(), 5, lambda event: None, worker=_partial_worker)
    assert (result["status"], result["bytes_read"], result["bytes_read_is_lower_bound"]) == (
        "partial",
        4,
        False,
    )


def test_timeout_stops_worker_and_reports_lower_bound():
    events = []
    start = time.monotonic()
    result = execute(_request(), 1, events.append, worker=_blocked_worker)
    assert result["status"] == "timeout"
    assert result["bytes_read"] == 0
    assert result["bytes_read_is_lower_bound"] is True
    assert events[-1]["phase"] == "read"
    assert time.monotonic() - start < 5


def test_invalid_worker_range_stops_without_emitting_it():
    events = []
    result = execute(_request(), 5, events.append, worker=_invalid_worker)
    assert result["reason"] == "invalid_worker_message"
    assert result["bytes_read_is_lower_bound"] is True
    assert events == []


def test_worker_exception_is_reported_without_private_message():
    result = execute(_request(), 5, lambda event: None, worker=_exception_worker)
    assert result["reason"] == "worker_exception:ValueError"
    assert "/export/example" not in repr(result)


def test_compound_log_is_emitted_before_worker_continues(tmp_path):
    marker = tmp_path / "continued"
    request = {**_request(), "marker": str(marker)}

    def observe(event):
        if event["phase"] == "open":
            assert not marker.exists()

    result = execute(request, 5, observe, worker=_acknowledged_worker)
    assert result["status"] == "completed"
    assert marker.exists()


def test_slow_output_does_not_admit_compound_after_deadline(tmp_path):
    marker = tmp_path / "continued"
    request = {**_request(), "marker": str(marker)}
    events = []

    def slow_output(event):
        events.append(event)
        time.sleep(1.2)

    result = execute(request, 1, slow_output, worker=_acknowledged_worker)
    assert events and events[0]["phase"] == "open"
    assert result["status"] == "timeout"
    assert result["reason"] == "deadline_exceeded"
    assert not marker.exists()


def test_smaller_negotiated_chunks_are_accepted():
    events = []
    result = execute(_request(), 5, events.append, worker=_small_chunk_worker)
    assert result["status"] == "completed"
    assert [(event["offset"], event["count"]) for event in events] == [(5, 4), (9, 6)]


def test_handle_task_tags_and_result_are_bound_to_plan_and_task():
    request = {
        **_request(),
        "file_handle": b"private-test-handle",
        "plan_ref": "a" * 32,
        "task_ref": "b" * 32,
    }
    events = []
    result = execute(request, 5, events.append, worker=_handle_worker)
    assert result["status"] == "completed"
    assert (result["plan_ref"], result["task_ref"]) == ("a" * 32, "b" * 32)
    assert events[0]["tag"] == f"pc1:w:{'a' * 32}:{'b' * 32}:{TAG}"
    assert len(events[0]["tag"].encode("ascii")) == 104
    assert "private-test-handle" not in repr(events) + repr(result)


def test_handle_task_rejects_tag_with_another_task_ref():
    request = {
        **_request(),
        "file_handle": b"private-test-handle",
        "plan_ref": "a" * 32,
        "task_ref": "b" * 32,
    }
    events = []
    result = execute(request, 5, events.append, worker=_wrong_task_tag_worker)
    assert result["status"] == "error"
    assert result["reason"] == "invalid_worker_message"
    assert events == []


def test_stale_handle_before_read_reports_exact_zero():
    request = {
        **_request(),
        "file_handle": b"private-test-handle",
        "plan_ref": "a" * 32,
        "task_ref": "b" * 32,
    }
    result = execute(request, 5, lambda event: None, worker=_stale_handle_worker)
    assert result["status"] == "error"
    assert result["reason"] == "getattr_NFS4ERR_STALE"
    assert result["bytes_read"] == 0
    assert result["bytes_read_is_lower_bound"] is False


def test_acknowledged_read_error_preserves_exact_prior_progress():
    result = execute(_request(), 5, lambda event: None, worker=_read_rejected_worker)
    assert result["status"] == "error"
    assert result["reason"] == "read_NFS4ERR_ACCESS"
    assert result["bytes_read"] == 4
    assert result["bytes_read_is_lower_bound"] is False


@pytest.mark.parametrize("worker", [_unexpected_read_rejection_worker, _retry_rejected_read_worker])
def test_supervisor_rejects_invalid_read_rejection_sequence(worker):
    result = execute(_request(), 5, lambda event: None, worker=worker)
    assert result["reason"] == "invalid_worker_message"
    assert result["bytes_read_is_lower_bound"] is True


def test_elapsed_timeout_during_process_setup_never_starts_worker(monkeypatch, tmp_path):
    from predictive_cache.warmer import execution

    marker = tmp_path / "continued"
    request = {**_request(), "marker": str(marker)}
    original_context = execution.multiprocessing.get_context
    now = [execution._elapsed_time()]
    monkeypatch.setattr(execution, "_elapsed_time", lambda: now[0])

    class DelayedContext:
        def __init__(self):
            self.inner = original_context("spawn")

        def Pipe(self, *args, **kwargs):
            now[0] += 10
            return self.inner.Pipe(*args, **kwargs)

        def Process(self, *args, **kwargs):
            return self.inner.Process(*args, **kwargs)

    monkeypatch.setattr(execution.multiprocessing, "get_context", lambda _: DelayedContext())
    events = []
    result = execute(request, 5, events.append, worker=_acknowledged_worker)
    assert result["status"] == "timeout"
    assert result["bytes_read_is_lower_bound"] is False
    assert not events
    assert not marker.exists()


def test_elapsed_clock_counts_linux_suspend(monkeypatch):
    from predictive_cache.warmer import execution

    calls = []

    def clock_gettime(clock):
        calls.append(clock)
        return 12

    monkeypatch.setattr(execution.time, "clock_gettime", clock_gettime)
    assert execution._elapsed_time() == 12
    assert calls == [time.CLOCK_BOOTTIME]


@pytest.mark.parametrize("phase", ["open", "read", "close"])
def test_worker_rechecks_deadline_after_admission(monkeypatch, phase):
    from predictive_cache.warmer import execution

    events = []
    continued = []

    class Sender:
        def send(self, event):
            events.append(event)

        def recv(self):
            return "continue"

        def close(self):
            pass

    def worker(request, emit):
        emit({"type": "compound", "tag": TAG, "phase": phase})
        continued.append(True)
        return {"status": "completed", "bytes_read": 0, "reason": "ok"}

    monkeypatch.setattr(execution, "_bind_worker_to_parent", lambda _: None)
    monkeypatch.setattr(execution, "_remaining_time", lambda *_: 0)
    monkeypatch.setattr(execution.signal, "signal", lambda *_: None)
    monkeypatch.setattr(execution.signal, "pthread_sigmask", lambda *_: None)
    monkeypatch.setattr(execution.resource, "setrlimit", lambda *_: None)
    execution._child_main({}, Sender(), worker, os.getpid(), 1, None)

    assert events[-1] == {"type": "deadline_expired", "tag": TAG}
    assert not continued


def _execute_scripted_events(
    monkeypatch, messages, *, poll_results=(), poll_calls=None, remaining_times=None
):
    from predictive_cache.warmer import execution

    messages = iter(messages)
    poll_results = iter(poll_results)
    admissions = []

    class Receiver:
        def poll(self, timeout):
            if poll_calls is not None:
                poll_calls.append(timeout)
            return next(poll_results, True)

        def recv(self):
            return next(messages)

        def send(self, message):
            admissions.append(message)

        def close(self):
            pass

    class Sender:
        def close(self):
            pass

    class Process:
        def start(self):
            pass

        def is_alive(self):
            return False

        def join(self, timeout):
            pass

    class Context:
        def Pipe(self, duplex):
            return Receiver(), Sender()

        def Process(self, **kwargs):
            return Process()

    monkeypatch.setattr(execution.multiprocessing, "get_context", lambda _: Context())
    if remaining_times is None:
        monkeypatch.setattr(execution, "_remaining_time", lambda *_: 5)
    else:
        remaining_times = iter(remaining_times)
        monkeypatch.setattr(execution, "_remaining_time", lambda *_: next(remaining_times))
    events = []
    result = execute(_request(), 5, events.append)
    return result, events, admissions


@pytest.mark.parametrize("poll_results", [[True, False, True, True], [True, True, False, True]])
def test_worker_exit_after_poll_timeout_drains_buffered_messages(monkeypatch, poll_results):
    messages = [
        {"type": "compound", "tag": TAG, "phase": "read", "offset": 5, "count": 10},
        {"type": "read_rejected"},
        {"type": "result", "status": "error", "reason": "read_NFS4ERR_ACCESS", "bytes_read": 0},
    ]
    poll_calls = []
    result, _, admissions = _execute_scripted_events(
        monkeypatch, messages, poll_results=poll_results, poll_calls=poll_calls
    )
    assert result["reason"] == "read_NFS4ERR_ACCESS"
    assert result["bytes_read_is_lower_bound"] is False
    assert admissions == ["continue"]
    assert poll_calls.count(0) == 1


def test_dead_worker_with_empty_pipe_reports_missing_result(monkeypatch):
    poll_calls = []
    result, _, _ = _execute_scripted_events(
        monkeypatch, [], poll_results=[False, False], poll_calls=poll_calls
    )
    assert result["reason"] == "worker_exited_without_result"
    assert poll_calls == [0.1, 0]


def test_dead_worker_pipe_is_not_drained_after_deadline(monkeypatch):
    messages = [{"type": "result", "status": "error", "reason": "late", "bytes_read": 0}]
    poll_calls = []
    result, _, _ = _execute_scripted_events(
        monkeypatch,
        messages,
        poll_results=[False, True],
        poll_calls=poll_calls,
        remaining_times=[5, 5, 0],
    )
    assert result["status"] == "timeout"
    assert result["reason"] == "deadline_exceeded"
    assert poll_calls == [0.1]


def _two_read_events():
    return [
        {"type": "compound", "tag": TAG, "phase": "read", "offset": 5, "count": 4},
        {"type": "progress", "bytes_read": 4},
        {"type": "compound", "tag": READ_TAG, "phase": "read", "offset": 9, "count": 6},
    ]


def test_cleanup_deadline_preserves_earlier_uncertain_read(monkeypatch):
    messages = [
        *_two_read_events(),
        {"type": "compound", "tag": CLEANUP_TAG, "phase": "close"},
        {"type": "deadline_expired", "tag": CLEANUP_TAG},
    ]
    result, events, admissions = _execute_scripted_events(monkeypatch, messages)
    assert (result["status"], result["reason"], result["bytes_read"]) == (
        "timeout",
        "deadline_exceeded",
        4,
    )
    assert result["bytes_read_is_lower_bound"] is True
    assert [event["phase"] for event in events] == ["read", "read", "close"]
    assert admissions == ["continue"] * 3


def test_read_deadline_proves_current_read_was_not_sent(monkeypatch):
    messages = [*_two_read_events(), {"type": "deadline_expired", "tag": READ_TAG}]
    result, _, admissions = _execute_scripted_events(monkeypatch, messages)
    assert (result["status"], result["reason"], result["bytes_read"]) == (
        "timeout",
        "deadline_exceeded",
        4,
    )
    assert result["bytes_read_is_lower_bound"] is False
    assert admissions == ["continue"] * 2


@pytest.mark.parametrize(
    "messages",
    [
        [{"type": "deadline_expired", "tag": READ_TAG}],
        [*_two_read_events(), {"type": "deadline_expired"}],
        [*_two_read_events(), {"type": "deadline_expired", "tag": TAG}],
        [*_two_read_events(), {"type": "deadline_expired", "tag": ["invalid"]}],
        [
            *_two_read_events(),
            {"type": "compound", "tag": CLEANUP_TAG, "phase": "close"},
            {"type": "deadline_expired", "tag": READ_TAG},
        ],
        [
            *_two_read_events(),
            {"type": "deadline_expired", "tag": READ_TAG, "detail": "private"},
        ],
        [
            *_two_read_events(),
            {"type": "progress", "bytes_read": 10},
            {"type": "deadline_expired", "tag": READ_TAG},
        ],
    ],
)
def test_invalid_deadline_messages_fail_closed(monkeypatch, messages):
    result, events, _ = _execute_scripted_events(monkeypatch, messages)
    assert result["status"] == "error"
    assert result["reason"] == "invalid_worker_message"
    assert result["bytes_read_is_lower_bound"] is True
    assert "private" not in repr(result) + repr(events)


@pytest.mark.parametrize("forgery", ["message", "exception"])
def test_worker_cannot_forge_relay_deadline(monkeypatch, forgery):
    from predictive_cache.warmer import execution

    events = []

    class Sender:
        def send(self, event):
            events.append(event)

        def close(self):
            pass

    def worker(request, emit):
        if forgery == "message":
            emit({"type": "deadline_expired", "tag": READ_TAG})
        else:
            raise execution._DeadlineExpired(READ_TAG)

    monkeypatch.setattr(execution, "_bind_worker_to_parent", lambda _: None)
    monkeypatch.setattr(execution.signal, "signal", lambda *_: None)
    monkeypatch.setattr(execution.signal, "pthread_sigmask", lambda *_: None)
    monkeypatch.setattr(execution.resource, "setrlimit", lambda *_: None)
    execution._child_main({}, Sender(), worker, os.getpid(), 1, None)
    assert events == [{"type": "invalid_worker_message"}]


def test_worker_cannot_swallow_relay_deadline(monkeypatch):
    from predictive_cache.warmer import execution

    events = []

    class Sender:
        def send(self, event):
            events.append(event)

        def recv(self):
            return "continue"

        def close(self):
            pass

    def worker(request, emit):
        try:
            emit({"type": "compound", "tag": READ_TAG, "phase": "read"})
        except execution._DeadlineExpired:
            return {"status": "completed", "bytes_read": 10, "reason": "ok"}

    monkeypatch.setattr(execution, "_bind_worker_to_parent", lambda _: None)
    monkeypatch.setattr(execution, "_remaining_time", lambda *_: 0)
    monkeypatch.setattr(execution.signal, "signal", lambda *_: None)
    monkeypatch.setattr(execution.signal, "pthread_sigmask", lambda *_: None)
    monkeypatch.setattr(execution.resource, "setrlimit", lambda *_: None)
    execution._child_main({}, Sender(), worker, os.getpid(), 1, None)
    assert events[-1] == {"type": "deadline_expired", "tag": READ_TAG}
    assert len(events) == 2


def test_suspend_elapsed_time_blocks_compound_admission(monkeypatch, tmp_path):
    from predictive_cache.warmer import execution

    marker = tmp_path / "continued"
    request = {**_request(), "marker": str(marker)}
    now = [execution._elapsed_time()]
    monkeypatch.setattr(execution, "_elapsed_time", lambda: now[0])

    def simulate_suspend(event):
        now[0] += 10

    result = execute(request, 5, simulate_suspend, worker=_acknowledged_worker)
    assert result["status"] == "timeout"
    assert result["bytes_read_is_lower_bound"] is False
    assert not marker.exists()


def test_forward_wall_clock_step_blocks_expired_task(monkeypatch, tmp_path):
    from predictive_cache.warmer import execution

    marker = tmp_path / "continued"
    now = [time.time()]
    request = {
        **_request(),
        "marker": str(marker),
        "_task_deadline_utc": now[0] + 20,
    }
    monkeypatch.setattr(execution.time, "time", lambda: now[0])

    def advance_wall_clock(event):
        now[0] += 30

    result = execute(request, 5, advance_wall_clock, worker=_acknowledged_worker)
    assert result["status"] == "timeout"
    assert result["bytes_read_is_lower_bound"] is False
    assert not marker.exists()


def test_backward_wall_clock_step_cannot_extend_elapsed_task_deadline(monkeypatch, tmp_path):
    from predictive_cache.warmer import execution

    marker = tmp_path / "continued"
    wall = [time.time()]
    elapsed = [execution._elapsed_time()]
    request = {
        **_request(),
        "marker": str(marker),
        "_task_deadline_utc": wall[0] + 5,
        "_task_deadline_elapsed": elapsed[0] + 5,
    }
    monkeypatch.setattr(execution.time, "time", lambda: wall[0])
    monkeypatch.setattr(execution, "_elapsed_time", lambda: elapsed[0])

    def step_backward(event):
        wall[0] -= 3600
        elapsed[0] += 6

    result = execute(request, 300, step_backward, worker=_acknowledged_worker)
    assert result["status"] == "timeout"
    assert not marker.exists()


def test_worker_address_space_is_bounded():
    result = execute(_request(), 5, lambda event: None, worker=_limited_worker)
    assert result["status"] == "completed"


def test_parent_death_signal_failure_fails_closed(monkeypatch):
    from predictive_cache.warmer import execution

    class FailedPrctl:
        def __call__(self, *args):
            return -1

    class Libc:
        prctl = FailedPrctl()

    monkeypatch.setattr(execution.os, "getppid", lambda: 123)
    monkeypatch.setattr(execution.ctypes, "CDLL", lambda *args, **kwargs: Libc())
    with pytest.raises(execution.ParentDeathSignalUnavailable):
        execution._bind_worker_to_parent(123)


def test_sigterm_cancellation_uses_conventional_exit_code(monkeypatch, capsys):
    monkeypatch.setattr(
        "predictive_cache.warmer.cli.execute",
        lambda *args: {"type": "result", "status": "cancelled", "reason": "terminated"},
    )
    assert main(ARGS) == 143
    assert json.loads(capsys.readouterr().out)["reason"] == "terminated"


def test_worker_stop_failure_is_reported(monkeypatch):
    from predictive_cache.warmer import execution

    original_stop = execution._stop

    def failing_stop(process):
        original_stop(process)
        raise PermissionError(1, "private details")

    monkeypatch.setattr(execution, "_stop", failing_stop)
    result = execute(_request(), 5, lambda event: None, worker=_successful_worker)
    assert result["status"] == "error"
    assert result["reason"] == "worker_stop_os_error:1:worker_stop_unconfirmed"
    assert result["bytes_read_is_lower_bound"] is True
    assert "private details" not in repr(result)


def test_compound_limit_is_reported_before_another_attempt(monkeypatch):
    monkeypatch.setattr("predictive_cache.warmer.execution.MAX_COMPOUNDS", 1)
    events = []
    result = execute(_request(), 5, events.append, worker=_successful_worker)
    assert result["status"] == "error"
    assert result["reason"] == "compound_limit_exceeded"
    assert [event["phase"] for event in events] == ["open"]


def test_adapter_imports_with_bounded_worker_memory():
    result = execute(_request(), 10, lambda event: None, worker=_import_adapter_worker)
    assert result["status"] == "completed"


@pytest.mark.parametrize(
    ("worker", "reason"),
    [
        (_duplicate_tag_worker, "duplicate_compound_tag"),
        (_retry_short_worker, "invalid_worker_message"),
        (_overcount_worker, "invalid_worker_message"),
    ],
)
def test_invalid_worker_protocol_is_not_reported_as_success(worker, reason):
    result = execute(_request(), 5, lambda event: None, worker=worker)
    assert result["status"] == "error"
    assert result["reason"] == reason
    assert result["bytes_read_is_lower_bound"] is True


@pytest.mark.parametrize(
    ("signum", "reason"),
    [(signal.SIGINT, "interrupted"), (signal.SIGTERM, "terminated")],
)
@pytest.mark.parametrize("initially_blocked", [False, True])
def test_signal_cancels_worker(tmp_path, signum, reason, initially_blocked):
    script = tmp_path / "supervisor.py"
    script.write_text(
        """
import json
import signal
import sys
import time
from predictive_cache.warmer.execution import execute

def worker(request, emit):
    emit({"type": "compound", "tag": "0123456789abcdef0123456789abcdef",
          "phase": "read", "offset": 5, "count": 10})
    time.sleep(20)

if __name__ == "__main__":
    blocked = sys.argv[1] == "blocked"
    if blocked:
        signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGINT, signal.SIGTERM})
    result = execute({"offset": 5, "length": 10}, 20,
                     lambda event: print(json.dumps(event), flush=True), worker=worker)
    if blocked:
        mask = signal.pthread_sigmask(signal.SIG_BLOCK, set())
        result["mask_restored"] = {signal.SIGINT, signal.SIGTERM} <= mask
    print(json.dumps(result), flush=True)
"""
    )
    proc = subprocess.Popen(
        [sys.executable, str(script), "blocked" if initially_blocked else "unblocked"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=os.environ.copy(),
    )
    try:
        assert select.select([proc.stdout], [], [], 5)[0]
        assert json.loads(proc.stdout.readline())["phase"] == "read"
        proc.send_signal(signum)
        stdout, stderr = proc.communicate(timeout=5)
        assert proc.returncode == 0, stderr
        result = json.loads(stdout)
        assert result["status"] == "cancelled"
        assert result["reason"] == reason
        assert result["bytes_read_is_lower_bound"] is True
        if initially_blocked:
            assert result["mask_restored"]
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait()


def test_worker_exits_when_supervisor_is_killed(tmp_path):
    script = tmp_path / "supervisor.py"
    marker = tmp_path / "worker.pid"
    script.write_text(
        """
import os
import time
from pathlib import Path
from predictive_cache.warmer.execution import execute

def worker(request, emit):
    Path(request["marker"]).write_text(str(os.getpid()))
    emit({"type": "compound", "tag": "0123456789abcdef0123456789abcdef",
          "phase": "read", "offset": 5, "count": 10})
    time.sleep(20)

if __name__ == "__main__":
    import sys
    execute({"offset": 5, "length": 10, "marker": sys.argv[1]}, 20,
            lambda event: print("ready", flush=True), worker=worker)
"""
    )
    proc = subprocess.Popen(
        [sys.executable, str(script), str(marker)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=os.environ.copy(),
    )
    worker_pid = None
    try:
        assert select.select([proc.stdout], [], [], 5)[0]
        assert proc.stdout.readline().strip() == "ready"
        worker_pid = int(marker.read_text())
        proc.kill()
        proc.wait(timeout=5)
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            stat = Path(f"/proc/{worker_pid}/stat")
            try:
                state = stat.read_text().split(") ", 1)[1][0]
            except (FileNotFoundError, ProcessLookupError):
                worker_pid = None
                break
            if state == "Z":
                worker_pid = None
                break
            time.sleep(0.05)
        else:
            pytest.fail("worker remained alive after supervisor death")
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait()
        if worker_pid is not None:
            try:
                os.kill(worker_pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
