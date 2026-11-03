from __future__ import annotations

import json
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace

import pytest

import predictive_cache.dispatcher.securefs as securefs
from predictive_cache.dispatcher.core import (
    Dispatcher,
    DispatcherError,
    _require_linux_runtime,
)
from predictive_cache.dispatcher.schema import MAX_TASK_BYTES, InvalidPlanItem, Plan
from predictive_cache.dispatcher.securefs import (
    SecureFileError,
    atomic_write_json,
    claim_no_replace,
    ensure_child_directory,
    open_trusted_executable,
    require_secure_directory,
)

from .conftest import config, make_warmer, plan_data, write_owner_file, write_plan


def test_plan_schema_is_exact_and_versioned(clock):
    value = plan_data(clock)
    assert Plan.model_validate(value).schema_version == 2
    assert Plan.model_validate(value).items[0].operation == "read"
    explicit_read = plan_data(clock)
    explicit_read["items"][0]["operation"] = "read"
    assert Plan.model_validate(explicit_read).items[0].operation == "read"

    write = plan_data(clock)
    write["items"][0]["operation"] = "write"
    parsed = Plan.model_validate(write)
    assert isinstance(parsed.items[0], InvalidPlanItem)
    assert parsed.items[0].skip_reason == "unauthorized_operation"

    for mutation in (
        lambda plan: plan.update(schema_version=1),
        lambda plan: plan.update(unexpected=True),
    ):
        invalid = plan_data(clock)
        mutation(invalid)
        with pytest.raises(ValueError):
            Plan.model_validate(invalid)
    invalid_item = plan_data(clock)
    invalid_item["items"][0]["unexpected"] = True
    with pytest.raises(ValueError, match="integrity"):
        Plan.model_validate(invalid_item)

    missing_identity = plan_data(clock)
    del missing_identity["items"][0]["volume_id"]
    with pytest.raises(ValueError, match="integrity"):
        Plan.model_validate(missing_identity)


def test_plan_rejects_noncanonical_handle_ttl_and_oversize(clock):
    invalid = plan_data(clock)
    invalid["items"][0]["target_fh_b64"] += "="
    assert Plan.model_validate(invalid).items[0].skip_reason == "malformed_target"

    invalid = plan_data(clock)
    invalid["items"][0]["length"] = MAX_TASK_BYTES + 1
    invalid["items"][0]["max_bytes"] = MAX_TASK_BYTES + 1
    assert Plan.model_validate(invalid).items[0].skip_reason == "target_out_of_range"

    invalid = plan_data(clock)
    invalid["items"][0]["target_valid_until"] = "2026-11-03T10:06:00Z"
    assert Plan.model_validate(invalid).items[0].skip_reason == "target_ttl_invalid"

    invalid = plan_data(clock)
    invalid["items"][0]["max_bytes"] = invalid["items"][0]["length"] - 1
    assert Plan.model_validate(invalid).items[0].skip_reason == "target_out_of_range"


def test_root_and_plan_modes_are_enforced(root, clock):
    warmer = make_warmer(root)
    os.chmod(root, 0o755)
    with pytest.raises(SecureFileError, match="0700"):
        Dispatcher(config(root, warmer))

    os.chmod(root, 0o700)
    plan = plan_data(clock)
    path = write_plan(root, plan)
    os.chmod(path, 0o644)
    with Dispatcher(config(root, warmer), now=clock.now) as dispatcher:
        with pytest.raises(SecureFileError, match="0600"):
            dispatcher.run_once()


def test_symlink_hardlink_and_oversize_plans_are_rejected(root, clock):
    warmer = make_warmer(root)
    plans = root / "plans"
    plans.mkdir(mode=0o700)
    target = root.parent / "target.json"
    write_owner_file(target, json.dumps(plan_data(clock)))
    (plans / "plan-1.json").symlink_to(target)
    with Dispatcher(config(root, warmer), now=clock.now) as dispatcher:
        with pytest.raises(OSError):
            dispatcher.run_once()

    (plans / "plan-1.json").unlink()
    os.link(target, plans / "plan-1.json")
    with Dispatcher(config(root, warmer), now=clock.now) as dispatcher:
        with pytest.raises(SecureFileError, match="one link"):
            dispatcher.run_once()

    (plans / "plan-1.json").unlink()
    target.unlink()
    write_owner_file(plans / "plan-1.json", "x" * 257)
    with Dispatcher(
        config(root, warmer, max_plan_bytes=256),
        now=clock.now,
    ) as dispatcher:
        with pytest.raises(SecureFileError, match="exceeds"):
            dispatcher.run_once()


def test_trailing_json_and_duplicate_keys_are_rejected(root, clock):
    warmer = make_warmer(root)
    plan = plan_data(clock)
    path = write_plan(root, plan)
    path.write_text(json.dumps(plan) + "\n{}")
    os.chmod(path, 0o600)
    with Dispatcher(config(root, warmer), now=clock.now) as dispatcher:
        with pytest.raises(DispatcherError, match="invalid complete"):
            dispatcher.run_once()

    path = root / "claimed" / "plan-1.json"
    path.unlink()
    duplicate = json.dumps(plan).replace('"run_id": "run-1"', '"run_id":"run-1","run_id":"x"')
    write_owner_file(root / "plans" / "plan-1.json", duplicate)
    with Dispatcher(config(root, warmer), now=clock.now) as dispatcher:
        with pytest.raises((DispatcherError, SecureFileError), match="duplicate"):
            dispatcher.run_once()


def test_noncommitted_names_are_not_consumed(root, clock):
    warmer = make_warmer(root)
    plan = plan_data(clock)
    plans = root / "plans"
    plans.mkdir(mode=0o700)
    write_owner_file(plans / ".hidden.json", json.dumps(plan))
    write_owner_file(plans / "plan-1.tmp.json", json.dumps(plan))
    with Dispatcher(config(root, warmer), now=clock.now) as dispatcher:
        stats = dispatcher.run_once()
    assert stats.claimed == 0
    assert sorted(path.name for path in plans.iterdir()) == [".hidden.json", "plan-1.tmp.json"]


def test_competing_claims_never_overwrite(root, clock):
    root_fd = require_secure_directory(root)
    source_a = ensure_child_directory(root_fd, "source-a")
    source_b = ensure_child_directory(root_fd, "source-b")
    destination = ensure_child_directory(root_fd, "destination")
    try:
        payload_a = json.dumps(plan_data(clock, plan_id="same"))
        payload_b = json.dumps(plan_data(clock, plan_id="same")).replace("run-1", "run-2")
        for fd, payload in ((source_a, payload_a), (source_b, payload_b)):
            file_fd = os.open("same.json", os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600, dir_fd=fd)
            os.write(file_fd, payload.encode())
            os.close(file_fd)
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(
                pool.map(
                    lambda source: claim_no_replace(source, destination, "same.json"),
                    (source_a, source_b),
                )
            )
        assert sorted(results) == [False, True]
        claimed = os.open("same.json", os.O_RDONLY, dir_fd=destination)
        try:
            content = os.read(claimed, 8192).decode()
        finally:
            os.close(claimed)
        assert content in {payload_a, payload_b}
    finally:
        for fd in (destination, source_b, source_a, root_fd):
            os.close(fd)


def test_claim_fails_closed_when_rename_noreplace_is_unsupported(
    root, clock, monkeypatch
):
    root_fd = require_secure_directory(root)
    source = ensure_child_directory(root_fd, "source")
    destination = ensure_child_directory(root_fd, "destination")
    try:
        payload = json.dumps(plan_data(clock, plan_id="unsupported"))
        file_fd = os.open(
            "unsupported.json",
            os.O_WRONLY | os.O_CREAT | os.O_EXCL,
            0o600,
            dir_fd=source,
        )
        os.write(file_fd, payload.encode())
        os.close(file_fd)

        def unsupported(*_args):
            raise SecureFileError("renameat2 unsupported")

        monkeypatch.setattr(
            "predictive_cache.dispatcher.securefs._renameat2_noreplace",
            unsupported,
        )
        with pytest.raises(SecureFileError, match="unsupported"):
            claim_no_replace(source, destination, "unsupported.json")
        assert "unsupported.json" in os.listdir(source)
        assert "unsupported.json" not in os.listdir(destination)
    finally:
        for fd in (destination, source, root_fd):
            os.close(fd)


def test_no_overwrite_publication_has_single_link(root):
    root_fd = require_secure_directory(root)
    try:
        assert atomic_write_json(root_fd, "record.json", {"value": 1}, no_overwrite=True)
        info = os.stat("record.json", dir_fd=root_fd, follow_symlinks=False)
        assert info.st_nlink == 1
        assert not atomic_write_json(
            root_fd,
            "record.json",
            {"value": 2},
            no_overwrite=True,
        )
        assert json.loads((root / "record.json").read_text()) == {"value": 1}
    finally:
        os.close(root_fd)


def test_failed_publication_removes_and_fsyncs_temporary_file(
    root, monkeypatch
):
    root_fd = require_secure_directory(root)

    def fail_write(_fd, _data):
        raise OSError("injected write failure")

    monkeypatch.setattr(
        "predictive_cache.dispatcher.securefs._write_all",
        fail_write,
    )
    try:
        with pytest.raises(OSError, match="injected"):
            atomic_write_json(root_fd, "record.json", {"value": 1}, no_overwrite=True)
        assert os.listdir(root_fd) == []
    finally:
        os.close(root_fd)


def test_config_requires_numeric_host_and_absolute_command(root):
    with pytest.raises(ValueError):
        config(root, Path("pcache-warmer"))
    with pytest.raises(ValueError):
        config(root, Path("/bin/echo"), host="warmer.example.com")
    with pytest.raises(ValueError):
        config(root, Path("/bin/echo"), host="fe80::1%eth0")


def test_dispatcher_fails_preflight_on_unsupported_platform_or_libc():
    with pytest.raises(DispatcherError, match="requires Linux"):
        _require_linux_runtime(platform="darwin")

    class MissingPrctl:
        renameat2 = object()

    with pytest.raises(DispatcherError, match="prctl"):
        _require_linux_runtime(libc_loader=lambda *_args, **_kwargs: MissingPrctl())


def test_trusted_executable_is_pinned_to_validated_inode(root):
    executable = root.parent / "warmer"
    executable.write_text("#!/bin/sh\nexit 0\n")
    executable.chmod(0o700)
    fd = open_trusted_executable(executable)
    try:
        executable.rename(root.parent / "warmer-original")
        executable.write_text("#!/bin/sh\nexit 99\n")
        executable.chmod(0o700)
        assert os.read(fd, 128) == b"#!/bin/sh\nexit 0\n"
    finally:
        os.close(fd)


def test_trusted_executable_rejects_untrusted_owner(root, monkeypatch):
    executable = root.parent / "warmer"
    executable.write_text("#!/bin/sh\nexit 0\n")
    executable.chmod(0o700)
    real_fstat = os.fstat

    def untrusted(fd):
        info = real_fstat(fd)
        return SimpleNamespace(
            st_mode=info.st_mode,
            st_uid=os.getuid() + 1,
            st_nlink=info.st_nlink,
        )

    class OsProxy:
        def __getattr__(self, name):
            return getattr(os, name)

        fstat = staticmethod(untrusted)

    monkeypatch.setattr(securefs, "os", OsProxy())
    with pytest.raises(SecureFileError, match="owned"):
        open_trusted_executable(executable)
