from __future__ import annotations

import base64
import json
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from predictive_cache.dispatcher.core import DispatcherConfig


class Clock:
    def __init__(self, value: datetime) -> None:
        self.value = value
        self.monotonic_value = 100.0

    def now(self) -> datetime:
        return self.value

    def monotonic(self) -> float:
        return self.monotonic_value

    def sleep(self, seconds: float) -> None:
        self.value += timedelta(seconds=seconds)
        self.monotonic_value += seconds


@pytest.fixture
def clock() -> Clock:
    return Clock(datetime(2026, 11, 3, 10, 0, tzinfo=UTC))


@pytest.fixture
def root(tmp_path: Path) -> Path:
    path = tmp_path / "dispatcher"
    path.mkdir(mode=0o700)
    os.chmod(path, 0o700)
    return path


def write_owner_file(path: Path, data: str = "") -> None:
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        os.write(fd, data.encode())
        os.fsync(fd)
    finally:
        os.close(fd)


def plan_data(
    clock: Clock,
    *,
    plan_id: str = "plan-1",
    item_count: int = 1,
    length: int = 4096,
    max_bytes: int | None = None,
) -> dict:
    created = clock.value - timedelta(seconds=1)
    deadline = clock.value + timedelta(seconds=120)
    valid = clock.value + timedelta(seconds=180)
    handle = base64.urlsafe_b64encode(b"opaque-file-handle").rstrip(b"=").decode()
    items = []
    for index in range(item_count):
        items.append(
            {
                "task_id": f"task-{index + 1}",
                "volume_id": "vol-1",
                "file_id": f"file-{index + 1}",
                "export_name": "export-1",
                "export_id": "export-id-1",
                "target_fh_b64": handle,
                "target_valid_until": valid.isoformat().replace("+00:00", "Z"),
                "offset": 0,
                "length": length,
                "max_bytes": length,
                "score": 0.75,
                "reasons": ["fixture"],
            }
        )
    return {
        "schema_version": 2,
        "run_id": "run-1",
        "plan_id": plan_id,
        "model_version": "model-v1",
        "created_at": created.isoformat().replace("+00:00", "Z"),
        "deadline": deadline.isoformat().replace("+00:00", "Z"),
        "max_bytes": max_bytes if max_bytes is not None else length * item_count,
        "items": items,
    }


def write_plan(root: Path, plan: dict, *, suffix: str = "") -> Path:
    plans = root / "plans"
    plans.mkdir(mode=0o700, exist_ok=True)
    os.chmod(plans, 0o700)
    path = plans / f"{plan['plan_id']}{suffix}.json"
    write_owner_file(path, json.dumps(plan) + "\n")
    return path


def make_warmer(
    root: Path,
    *,
    status: str = "completed",
    reason: str = "ok",
    malformed: str | None = None,
    counter: Path | None = None,
    sleep_seconds: float = 0,
    remove_path: Path | None = None,
    stderr_bytes: int = 0,
    exit_code: int | None = None,
    raw_output: bytes | None = None,
) -> Path:
    path = root.parent / f"fake-warmer-{status}-{reason}"
    counter_code = ""
    if counter is not None:
        counter_code = (
            f"p=pathlib.Path({str(counter)!r}); "
            "p.write_text(str(int(p.read_text()) + 1) if p.exists() else '1')"
        )
    if raw_output is not None:
        output_code = f"sys.stdout.buffer.write({raw_output!r}); sys.stdout.buffer.flush()"
    elif malformed is not None:
        output_code = f"print({malformed!r})"
    else:
        output_code = f"""
task=json.loads(pathlib.Path(args[args.index('--task-file') + 1]).read_text())
plan_ref=hashlib.sha256(task["plan_id"].encode("ascii")).hexdigest()[:32]
task_ref=hashlib.sha256(task["task_id"].encode("ascii")).hexdigest()[:32]
print(json.dumps({{"type":"compound","operation":"read"}}))
print(json.dumps({{
    "type":"result",
    "status":{status!r},
    "reason":{reason!r},
    "bytes_requested":task["length"],
    "bytes_read":task["length"] if {status!r} == "completed" else task["length"] // 2,
    "bytes_read_is_lower_bound":{status!r} in ("timeout", "cancelled"),
    "plan_ref":plan_ref,
    "task_ref":task_ref,
}}))
"""
    resolved_exit_code = (
        exit_code
        if exit_code is not None
        else {"completed": 0, "partial": 1, "error": 1, "timeout": 1, "cancelled": 143}[status]
    )
    script = f"""#!/usr/bin/env python3
import json
import hashlib
import pathlib
import sys
import time
args=sys.argv[1:]
{counter_code}
time.sleep({sleep_seconds!r})
sys.stderr.write("e" * {stderr_bytes})
{output_code}
{f"pathlib.Path({str(remove_path)!r}).unlink(missing_ok=True)" if remove_path else ""}
raise SystemExit({resolved_exit_code})
"""
    path.write_text(script)
    os.chmod(path, 0o700)
    return path


def config(root: Path, warmer: Path, **overrides) -> DispatcherConfig:
    values = {
        "root": root,
        "warmer_command": warmer,
        "host": "192.0.2.10",
        "real_reads": True,
        "max_bytes_per_min": 64 * 2**20,
        "max_items_per_min": 60,
    }
    values.update(overrides)
    return DispatcherConfig(**values)
