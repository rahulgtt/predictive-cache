"""Command-line entry point for the experimental manual reader."""

import argparse
import base64
import binascii
import hashlib
import ipaddress
import json
import math
import multiprocessing
import os
import re
import signal
import stat
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .execution import (
    _bind_worker_to_parent,
    _elapsed_time,
    _handle_sigterm,
    _stop,
    _Terminated,
    execute,
)
from .limits import MAX_BYTES

MAX_FILE_LENGTH = (1 << 64) - 1
MAX_OFFSET = MAX_FILE_LENGTH - 1
MAX_TASK_FILE_BYTES = 8192
_IDENTIFIER = re.compile(r"[A-Za-z0-9._:-]{1,128}\Z")
_BASE64URL = re.compile(r"[A-Za-z0-9_-]+={0,2}\Z")
_UTC_TIMESTAMP = re.compile(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d{1,6})?Z\Z")
_TASK_REQUIRED = {
    "schema_version",
    "run_id",
    "plan_id",
    "task_id",
    "volume_id",
    "file_id",
    "export_name",
    "export_id",
    "target_fh_b64",
    "offset",
    "length",
    "max_bytes",
    "created_at",
    "deadline",
    "target_valid_until",
}


def _parser():
    parser = argparse.ArgumentParser(
        description="Issue one bounded, read-only NFSv4.1 range request."
    )
    parser.add_argument("--host", required=True, help="Authorized numeric NFS server IP.")
    parser.add_argument("--port", type=int, default=2049)
    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument("--path", help="Absolute NFSv4 pseudo-filesystem path.")
    target.add_argument("--task-file", help="Private JSON task containing an opaque NFSv4 handle.")
    parser.add_argument("--offset", type=int)
    parser.add_argument("--length", type=int)
    parser.add_argument("--max-bytes", type=int)
    parser.add_argument("--timeout-seconds", required=True, type=float)
    parser.add_argument("--uid", required=True, type=int, help="AUTH_SYS user ID.")
    parser.add_argument("--gid", required=True, type=int, help="AUTH_SYS primary group ID.")
    return parser


def _validate_range(offset, length, max_bytes):
    if not 0 <= offset <= MAX_OFFSET:
        raise ValueError("--offset exceeds the maximum regular-file offset")
    if not 1 <= length <= MAX_BYTES or offset + length > MAX_FILE_LENGTH:
        raise ValueError("--length must be positive, at most 16 MiB, and fit in a regular file")
    if not length <= max_bytes <= MAX_BYTES:
        raise ValueError("--max-bytes must cover the range and be at most 16 MiB")


def _validate_transport(args):
    try:
        if "%" in args.host:
            raise ValueError
        address = ipaddress.ip_address(args.host)
        if address.is_unspecified or (
            isinstance(address, ipaddress.IPv6Address)
            and address.ipv4_mapped is not None
            and address.ipv4_mapped.is_unspecified
        ):
            raise ValueError
        host = str(address)
    except ValueError as exc:
        raise ValueError(
            "--host must be a specified numeric IPv4 or IPv6 address without a scope"
        ) from exc
    if not 1 <= args.port <= 65535:
        raise ValueError("--port must be between 1 and 65535")
    if not math.isfinite(args.timeout_seconds) or not 0.1 <= args.timeout_seconds <= 300:
        raise ValueError("--timeout-seconds must be between 0.1 and 300")
    if not all(0 <= value <= 0xFFFFFFFF for value in (args.uid, args.gid)):
        raise ValueError("--uid and --gid must fit in unsigned 32-bit integers")
    return {
        "host": host,
        "port": args.port,
        "uid": args.uid,
        "gid": args.gid,
    }


def _validate(args):
    request = _validate_transport(args)
    if any(value is None for value in (args.offset, args.length, args.max_bytes)):
        raise ValueError("--path requires --offset, --length, and --max-bytes")
    try:
        encoded_path = args.path.encode("utf-8")
    except UnicodeEncodeError as exc:
        raise ValueError("--path must be valid UTF-8") from exc
    components = args.path.split("/")
    if (
        not args.path.startswith("/")
        or len(encoded_path) > 4096
        or any(
            part in {"", ".", ".."} or "\0" in part or len(part.encode("utf-8")) > 255
            for part in components[1:]
        )
    ):
        raise ValueError("--path must be an absolute file path with safe, nonempty components")
    _validate_range(args.offset, args.length, args.max_bytes)
    request.update(path=args.path, offset=args.offset, length=args.length)
    return request


def _unique_pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate task field")
        result[key] = value
    return result


def _timestamp(value):
    if not isinstance(value, str) or not _UTC_TIMESTAMP.fullmatch(value):
        raise ValueError("task times must be RFC3339 UTC timestamps")
    try:
        return datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError as exc:
        raise ValueError("task times must be RFC3339 UTC timestamps") from exc


def _open_task_directory(path):
    parts = os.fspath(path).split("/")
    name = parts[-1]
    if (
        not name
        or name.startswith(".")
        or not name.endswith(".json")
        or name.endswith(".tmp.json")
        or ".." in parts
    ):
        raise ValueError("invalid private task path")
    flags = os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_DIRECTORY
    directory = os.open("/" if path.is_absolute() else ".", flags)
    succeeded = False
    try:
        for component in parts[:-1]:
            if component in {"", "."}:
                continue
            child = os.open(component, flags, dir_fd=directory)
            os.close(directory)
            directory = child
        info = os.fstat(directory)
        if info.st_uid != os.geteuid() or stat.S_IMODE(info.st_mode) != 0o700:
            raise ValueError("task directory must be owner-controlled with mode 0700")
        succeeded = True
        return directory, name
    finally:
        if not succeeded:
            os.close(directory)


def _load_task(path, timeout_seconds):
    """Load a trusted dispatcher's owner-only task without exposing its contents."""
    if (
        isinstance(timeout_seconds, bool)
        or not isinstance(timeout_seconds, (int, float))
        or not math.isfinite(timeout_seconds)
        or not 0.1 <= timeout_seconds <= 300
    ):
        raise ValueError("--timeout-seconds must be between 0.1 and 300")
    try:
        directory, name = _open_task_directory(Path(path))
        try:
            fd = os.open(
                name,
                os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW | os.O_NONBLOCK,
                dir_fd=directory,
            )
            with os.fdopen(fd, "rb") as task_file:
                info = os.fstat(task_file.fileno())
                if (
                    not stat.S_ISREG(info.st_mode)
                    or info.st_uid != os.geteuid()
                    or stat.S_IMODE(info.st_mode) != 0o600
                    or info.st_nlink != 1
                    or info.st_size > MAX_TASK_FILE_BYTES
                ):
                    raise ValueError("task file must be an owner-controlled regular file")
                raw = task_file.read(MAX_TASK_FILE_BYTES + 1)
                after = os.fstat(task_file.fileno())
                if any(
                    getattr(info, field) != getattr(after, field)
                    for field in (
                        "st_dev", "st_ino", "st_size", "st_mtime_ns", "st_ctime_ns", "st_nlink"
                    )
                ):
                    raise ValueError("task file changed during reading")
        finally:
            os.close(directory)
    except OSError as exc:
        raise ValueError("cannot read private task file") from exc
    if len(raw) > MAX_TASK_FILE_BYTES:
        raise ValueError("task file exceeds size limit")
    try:
        task = json.loads(raw, object_pairs_hook=_unique_pairs)
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError) as exc:
        raise ValueError("task file is not valid JSON") from exc
    if not isinstance(task, dict) or task.keys() != _TASK_REQUIRED:
        raise ValueError("task fields do not match the supported schema")
    if type(task["schema_version"]) is not int or task["schema_version"] != 2:
        raise ValueError("unsupported task schema")
    identifiers = ("run_id", "plan_id", "task_id", "volume_id", "file_id", "export_name", "export_id")
    if any(
        not isinstance(task[name], str) or not _IDENTIFIER.fullmatch(task[name])
        for name in identifiers
    ):
        raise ValueError("invalid task identity")
    encoded = task["target_fh_b64"]
    if not isinstance(encoded, str) or not _BASE64URL.fullmatch(encoded):
        raise ValueError("invalid NFS file handle encoding")
    try:
        raw_handle = base64.b64decode(encoded + "=" * (-len(encoded) % 4), altchars=b"-_", validate=True)
    except (ValueError, binascii.Error) as exc:
        raise ValueError("invalid NFS file handle encoding") from exc
    canonical = base64.urlsafe_b64encode(raw_handle).decode("ascii")
    if not 1 <= len(raw_handle) <= 128 or encoded not in {canonical, canonical.rstrip("=")}:
        raise ValueError("invalid NFS file handle encoding")
    if any(type(task[name]) is not int for name in ("offset", "length", "max_bytes")):
        raise ValueError("task range values must be integers")
    _validate_range(task["offset"], task["length"], task["max_bytes"])
    elapsed_at_validation = _elapsed_time()
    now = datetime.now(timezone.utc)
    created = _timestamp(task["created_at"])
    deadline = _timestamp(task["deadline"])
    expires = _timestamp(task["target_valid_until"])
    if not created <= now < deadline <= expires or expires - created > timedelta(seconds=300):
        raise ValueError("invalid or expired task deadline or target validity")
    remaining = (deadline - now).total_seconds()
    request = {
        "file_handle": raw_handle,
        "offset": task["offset"],
        "length": task["length"],
        **{name: task[name] for name in identifiers},
        "plan_ref": hashlib.sha256(task["plan_id"].encode("ascii")).hexdigest()[:32],
        "task_ref": hashlib.sha256(task["task_id"].encode("ascii")).hexdigest()[:32],
        "_task_deadline_utc": deadline.timestamp(),
        "_task_deadline_elapsed": elapsed_at_validation + remaining,
    }
    return request, min(timeout_seconds, remaining)


class _TaskLoadStopped(Exception):
    def __init__(self, status, reason):
        self.status = status
        self.reason = reason


def _task_load_worker(path, timeout_seconds, sender, parent_pid, loader):
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    signal.signal(signal.SIGTERM, signal.SIG_DFL)
    try:
        _bind_worker_to_parent(parent_pid)
        signal.pthread_sigmask(signal.SIG_UNBLOCK, {signal.SIGINT, signal.SIGTERM})
        try:
            request, timeout = loader(path, timeout_seconds)
            sender.send(("ok", request, timeout))
        except ValueError as exc:
            sender.send(("invalid", str(exc)))
    except Exception as exc:
        sender.send(("failure", type(exc).__name__))
    finally:
        sender.close()


def _load_task_supervised(path, timeout_seconds, deadline, *, loader=_load_task):
    """Bound task-file I/O separately from the NFS worker."""
    ctx = multiprocessing.get_context("spawn")
    receiver, sender = ctx.Pipe(duplex=False)
    process = ctx.Process(
        target=_task_load_worker, args=(path, timeout_seconds, sender, os.getpid(), loader)
    )
    cancelled = []

    def remember_cancellation(signum, frame):
        if not cancelled:
            cancelled.append(signum)

    controlled_signals = {signal.SIGINT, signal.SIGTERM}
    previous_mask = signal.pthread_sigmask(signal.SIG_BLOCK, controlled_signals)
    previous_sigint = signal.signal(signal.SIGINT, remember_cancellation)
    previous_sigterm = signal.signal(signal.SIGTERM, remember_cancellation)
    started = False
    outcome = None
    try:
        signal.pthread_sigmask(signal.SIG_SETMASK, previous_mask - controlled_signals)
        if cancelled:
            reason = "terminated" if cancelled[0] == signal.SIGTERM else "interrupted"
            raise _TaskLoadStopped("cancelled", reason)
        if deadline - _elapsed_time() <= 0:
            raise _TaskLoadStopped("timeout", "deadline_exceeded")
        signal.pthread_sigmask(signal.SIG_BLOCK, controlled_signals)
        try:
            process.start()
            started = True
            sender.close()
        finally:
            signal.pthread_sigmask(signal.SIG_SETMASK, previous_mask - controlled_signals)
        while True:
            if cancelled:
                raise _TaskLoadStopped(
                    "cancelled", "terminated" if cancelled[0] == signal.SIGTERM else "interrupted"
                )
            remaining = deadline - _elapsed_time()
            if remaining <= 0:
                raise _TaskLoadStopped("timeout", "deadline_exceeded")
            if not receiver.poll(min(remaining, 0.1)):
                if process.is_alive():
                    continue
                if not receiver.poll(0):
                    raise _TaskLoadStopped("error", "task_validation_worker_exited")
            try:
                outcome = receiver.recv()
            except EOFError:
                raise _TaskLoadStopped("error", "task_validation_worker_exited") from None
            break
    except KeyboardInterrupt:
        raise _TaskLoadStopped("cancelled", "interrupted") from None
    except _Terminated:
        raise _TaskLoadStopped("cancelled", "terminated") from None
    except OSError as exc:
        raise _TaskLoadStopped("error", f"task_validation_os_error:{exc.errno}") from None
    finally:
        sender.close()
        stopped = True
        if started:
            try:
                stopped = _stop(process)
            except OSError:
                stopped = False
        receiver.close()
        signal.pthread_sigmask(signal.SIG_BLOCK, controlled_signals)
        signal.signal(signal.SIGINT, previous_sigint)
        signal.signal(signal.SIGTERM, previous_sigterm)
        signal.pthread_sigmask(signal.SIG_SETMASK, previous_mask)
        if not stopped:
            raise _TaskLoadStopped("error", "task_validation_worker_stop_unconfirmed")
        if cancelled:
            reason = "terminated" if cancelled[0] == signal.SIGTERM else "interrupted"
            raise _TaskLoadStopped("cancelled", reason)
    if outcome[0] == "invalid":
        raise ValueError(outcome[1])
    if outcome[0] == "failure":
        raise _TaskLoadStopped("error", f"task_validation_worker_exception:{outcome[1]}")
    return outcome[1], outcome[2]


def _emit(event):
    print(json.dumps(event, separators=(",", ":"), sort_keys=True), flush=True)


def main(argv=None):
    started = _elapsed_time()
    transition = {"task_mode": False, "handoff_started": False}
    controlled_signals = {signal.SIGINT, signal.SIGTERM}
    previous_mask = signal.pthread_sigmask(signal.SIG_BLOCK, controlled_signals)
    previous_sigint = signal.signal(signal.SIGINT, signal.default_int_handler)
    previous_sigterm = signal.signal(signal.SIGTERM, _handle_sigterm)
    try:
        signal.pthread_sigmask(signal.SIG_SETMASK, previous_mask - controlled_signals)
        return _main(argv, started, transition)
    except (KeyboardInterrupt, _Terminated) as exc:
        reason = "terminated" if isinstance(exc, _Terminated) else "interrupted"
        if transition["task_mode"] and not transition["handoff_started"]:
            _emit_pre_validation("cancelled", reason)
        return 143 if reason == "terminated" else 130
    finally:
        signal.pthread_sigmask(signal.SIG_BLOCK, controlled_signals)
        signal.signal(signal.SIGINT, previous_sigint)
        signal.signal(signal.SIGTERM, previous_sigterm)
        signal.pthread_sigmask(signal.SIG_SETMASK, previous_mask)


def _emit_pre_validation(status, reason):
    _emit(
        {
            "type": "pre_validation_result",
            "status": status,
            "reason": reason,
            "execution_started": False,
        }
    )


def _main(argv, started, transition):
    parser = _parser()
    args = parser.parse_args(argv)
    transition["task_mode"] = args.task_file is not None
    try:
        if args.task_file is not None:
            if any(value is not None for value in (args.offset, args.length, args.max_bytes)):
                raise ValueError("--task-file cannot be combined with range options")
            request = _validate_transport(args)
            task, timeout = _load_task_supervised(
                args.task_file, args.timeout_seconds, started + args.timeout_seconds
            )
            request.update(task)
        else:
            request = _validate(args)
            timeout = args.timeout_seconds
        request["_timeout_deadline"] = min(
            started + args.timeout_seconds,
            request.pop("_task_deadline_elapsed", float("inf")),
        )
    except ValueError as exc:
        if args.task_file is not None:
            _emit_pre_validation("error", "invalid_task_or_arguments")
        parser.error(str(exc))
    except _TaskLoadStopped as exc:
        _emit_pre_validation(exc.status, exc.reason)
        return 143 if exc.reason == "terminated" else 130 if exc.status == "cancelled" else 1
    transition["handoff_started"] = True
    result = execute(request, timeout, _emit)
    _emit(result)
    if result["status"] == "completed":
        return 0
    if result["status"] == "cancelled":
        return 143 if result["reason"] == "terminated" else 130
    return 1


if __name__ == "__main__":
    sys.exit(main())
