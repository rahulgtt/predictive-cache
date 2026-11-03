"""Supervise one bounded read in an independently stoppable process."""

import ctypes
import multiprocessing
import os
import re
import resource
import signal
import sys
import time

from .limits import MAX_COMPOUNDS, MAX_WORKER_ADDRESS_SPACE, READ_BYTES

_TAG = re.compile(r"[0-9a-f]{32}\Z")
_PHASE = re.compile(r"[a-z0-9_-]{1,32}\Z")
_REASON = re.compile(r"[a-zA-Z0-9_.:-]{1,96}\Z")


class _Terminated(Exception):
    pass


class ParentDeathSignalUnavailable(RuntimeError):
    pass


class _DeadlineExpired(Exception):
    pass


class _WorkerProtocolViolation(Exception):
    pass


def _elapsed_time():
    if hasattr(time, "CLOCK_BOOTTIME"):
        return time.clock_gettime(time.CLOCK_BOOTTIME)
    return time.monotonic()


def _remaining_time(deadline, task_deadline):
    remaining = deadline - _elapsed_time()
    if task_deadline is not None:
        remaining = min(remaining, task_deadline - time.time())
    return remaining


def _handle_sigterm(signum, frame):
    raise _Terminated


def _bind_worker_to_parent(parent_pid):
    if sys.platform != "linux":
        raise ParentDeathSignalUnavailable("Linux is required")
    if os.getppid() != parent_pid:
        os._exit(1)
    libc = ctypes.CDLL(None, use_errno=True)
    prctl = libc.prctl
    prctl.argtypes = [ctypes.c_int, ctypes.c_ulong, ctypes.c_ulong, ctypes.c_ulong, ctypes.c_ulong]
    prctl.restype = ctypes.c_int
    if prctl(1, signal.SIGKILL, 0, 0, 0) != 0:  # Linux PR_SET_PDEATHSIG
        raise ParentDeathSignalUnavailable("cannot set parent-death signal")
    if os.getppid() != parent_pid:
        os._exit(1)


def _nfs_worker(request, emit):
    from .nfs41 import warm

    return warm(request, emit)


def _valid_tag(tag, request):
    if not isinstance(tag, str):
        return False
    if "file_handle" not in request:
        return bool(_TAG.fullmatch(tag))
    plan_ref = request.get("plan_ref")
    task_ref = request.get("task_ref")
    if (
        not isinstance(plan_ref, str)
        or not _TAG.fullmatch(plan_ref)
        or not isinstance(task_ref, str)
        or not _TAG.fullmatch(task_ref)
    ):
        return False
    prefix = f"pc1:w:{plan_ref}:{task_ref}:"
    return tag.startswith(prefix) and bool(_TAG.fullmatch(tag[len(prefix) :]))


def _child_main(request, sender, worker, parent_pid, deadline, task_deadline):
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    signal.signal(signal.SIGTERM, signal.SIG_DFL)
    expired_tag = None

    def relay(event):
        nonlocal expired_tag
        if expired_tag is not None:
            raise _DeadlineExpired
        if isinstance(event, dict) and event.get("type") == "deadline_expired":
            raise _WorkerProtocolViolation
        sender.send(event)
        if event.get("type") == "compound":
            if sender.recv() != "continue":
                raise RuntimeError("compound_not_admitted")
            if _remaining_time(deadline, task_deadline) <= 0:
                expired_tag = event["tag"]
                raise _DeadlineExpired

    try:
        _bind_worker_to_parent(parent_pid)
        signal.pthread_sigmask(signal.SIG_UNBLOCK, {signal.SIGINT, signal.SIGTERM})
        soft, hard = resource.getrlimit(resource.RLIMIT_AS)
        limit = (
            MAX_WORKER_ADDRESS_SPACE
            if soft == resource.RLIM_INFINITY
            else min(soft, MAX_WORKER_ADDRESS_SPACE)
        )
        resource.setrlimit(resource.RLIMIT_AS, (limit, hard))
        result = worker(request, relay)
        if expired_tag is not None:
            raise _DeadlineExpired
        sender.send({**result, "type": "result"})
    except _DeadlineExpired:
        if expired_tag is None:
            sender.send({"type": "invalid_worker_message"})
        else:
            sender.send({"type": "deadline_expired", "tag": expired_tag})
    except _WorkerProtocolViolation:
        sender.send({"type": "invalid_worker_message"})
    except Exception as exc:
        # Exception messages from dependencies can contain paths or handles.
        sender.send({"type": "failure", "reason": type(exc).__name__})
    finally:
        sender.close()


def _stop(process):
    if process.is_alive():
        process.terminate()
        process.join(timeout=1)
    if process.is_alive():
        process.kill()
        process.join(timeout=1)
    else:
        process.join(timeout=0)
    return not process.is_alive()


def execute(request, timeout, emit, *, worker=_nfs_worker):
    """Run one read, reporting only acknowledged bytes and sanitized events."""
    deadline = request.get("_timeout_deadline")
    if deadline is None:
        deadline = _elapsed_time() + timeout
    deadline = min(deadline, request.get("_task_deadline_elapsed", float("inf")))
    task_deadline = request.get("_task_deadline_utc")

    ctx = multiprocessing.get_context("spawn")
    receiver, sender = ctx.Pipe(duplex=True)
    process = ctx.Process(
        target=_child_main, args=(request, sender, worker, os.getpid(), deadline, task_deadline)
    )
    observed = 0
    status = "error"
    reason = "worker_failed"
    read_uncertain = False
    protocol_violation = False
    started = False
    read_pending = False
    pending_read_tag = None
    admitted_tag = None
    read_count = 0
    read_failed = False
    short_read = False
    tags = set()
    previous_sigterm = signal.signal(signal.SIGTERM, _handle_sigterm)
    previous_mask = None
    try:
        if _remaining_time(deadline, task_deadline) <= 0:
            raise _DeadlineExpired
        previous_mask = signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGINT, signal.SIGTERM})
        try:
            process.start()
            started = True
            sender.close()
        finally:
            signal.pthread_sigmask(
                signal.SIG_SETMASK, previous_mask - {signal.SIGINT, signal.SIGTERM}
            )
        while True:
            remaining = _remaining_time(deadline, task_deadline)
            if remaining <= 0:
                status, reason = "timeout", "deadline_exceeded"
                break
            if not receiver.poll(min(remaining, 0.1)):
                if process.is_alive():
                    continue
                if _remaining_time(deadline, task_deadline) <= 0:
                    status, reason = "timeout", "deadline_exceeded"
                    break
                # The child may have sent a message between poll and exit.
                if not receiver.poll(0):
                    reason = "worker_exited_without_result"
                    break
            try:
                event = receiver.recv()
            except EOFError:
                reason = "worker_exited_without_result"
                break
            if not isinstance(event, dict):
                reason = "invalid_worker_message"
                break
            kind = event.get("type")
            if kind == "compound":
                tag, phase = event.get("tag"), event.get("phase")
                if not _valid_tag(tag, request):
                    reason = "invalid_worker_message"
                    break
                if tag in tags:
                    reason = "duplicate_compound_tag"
                    break
                if len(tags) >= MAX_COMPOUNDS:
                    reason = "compound_limit_exceeded"
                    break
                tags.add(tag)
                if not isinstance(phase, str) or not _PHASE.fullmatch(phase):
                    reason = "invalid_worker_message"
                    break
                record = {"type": "compound", "tag": tag, "phase": phase}
                if phase == "read":
                    offset, count = event.get("offset"), event.get("count")
                    if (
                        read_pending
                        or read_failed
                        or short_read
                        or type(offset) is not int
                        or type(count) is not int
                        or offset != request["offset"] + observed
                        or not 1 <= count <= min(READ_BYTES, request["length"] - observed)
                    ):
                        reason = "invalid_worker_message"
                        break
                    read_pending = True
                    pending_read_tag = tag
                    read_count = count
                    record.update(offset=offset, count=count)
                emit(record)
                if _remaining_time(deadline, task_deadline) <= 0:
                    status, reason = "timeout", "deadline_exceeded"
                    break
                if phase == "read":
                    read_uncertain = True
                receiver.send("continue")
                admitted_tag = tag
            elif kind == "progress":
                count = event.get("bytes_read")
                if (
                    not read_pending
                    or type(count) is not int
                    or not observed <= count <= observed + read_count
                ):
                    reason = "invalid_worker_message"
                    break
                short_read = count - observed < read_count
                observed = count
                read_pending = False
                pending_read_tag = None
                admitted_tag = None
                read_uncertain = False
            elif kind == "read_rejected":
                if not read_pending or not read_uncertain:
                    reason = "invalid_worker_message"
                    break
                read_pending = False
                pending_read_tag = None
                admitted_tag = None
                read_uncertain = False
                read_failed = True
            elif kind == "result":
                child_status = event.get("status")
                child_reason = event.get("reason")
                count = event.get("bytes_read")
                if (
                    child_status not in {"completed", "partial", "error"}
                    or not isinstance(child_reason, str)
                    or not _REASON.fullmatch(child_reason)
                    or type(count) is not int
                    or count != observed
                    or (child_status == "completed" and count != request["length"])
                    or (child_status == "partial" and count >= request["length"])
                    or (child_status != "error" and read_pending)
                    or (child_status != "error" and read_failed)
                ):
                    reason = "invalid_worker_message"
                    break
                status, reason = child_status, child_reason
                break
            elif kind == "failure":
                child_reason = event.get("reason")
                if isinstance(child_reason, str) and _REASON.fullmatch(child_reason):
                    reason = f"worker_exception:{child_reason}"
                else:
                    reason = "invalid_worker_message"
                break
            elif kind == "deadline_expired":
                if (
                    set(event) != {"type", "tag"}
                    or admitted_tag is None
                    or event["tag"] != admitted_tag
                ):
                    reason = "invalid_worker_message"
                    break
                status, reason = "timeout", "deadline_exceeded"
                # Cleanup admission does not resolve a previously submitted READ.
                if read_pending and pending_read_tag == admitted_tag:
                    read_uncertain = False
                break
            else:
                reason = "invalid_worker_message"
                break
    except _DeadlineExpired:
        status, reason = "timeout", "deadline_exceeded"
    except KeyboardInterrupt:
        status, reason = "cancelled", "interrupted"
    except _Terminated:
        status, reason = "cancelled", "terminated"
    except OSError as exc:
        status, reason = "error", f"supervisor_os_error:{exc.errno}"
    finally:
        protocol_violation = reason in {
            "invalid_worker_message",
            "duplicate_compound_tag",
            "compound_limit_exceeded",
        }
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        previous_sigint = signal.signal(signal.SIGINT, signal.SIG_IGN)
        sender.close()
        if started:
            try:
                stopped = _stop(process)
            except OSError as exc:
                stopped = False
                reason = f"worker_stop_os_error:{exc.errno}"
            if not stopped:
                status = "error"
                reason += ":worker_stop_unconfirmed"
                read_uncertain = True
        receiver.close()
        if previous_mask is not None:
            signal.pthread_sigmask(signal.SIG_SETMASK, previous_mask)
        signal.signal(signal.SIGINT, previous_sigint)
        signal.signal(signal.SIGTERM, previous_sigterm)
    result = {
        "type": "result",
        "status": status,
        "reason": reason,
        "bytes_requested": request["length"],
        "bytes_read": observed,
        "bytes_read_is_lower_bound": read_uncertain or protocol_violation,
    }
    if "file_handle" in request and all(
        isinstance(request.get(key), str) and _TAG.fullmatch(request[key])
        for key in ("plan_ref", "task_ref")
    ):
        result.update(plan_ref=request["plan_ref"], task_ref=request["task_ref"])
    return result
