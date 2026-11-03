import base64
import hashlib
import json
import multiprocessing
import os
import signal
import stat
import time
from datetime import datetime, timedelta, timezone

import pytest

from predictive_cache.warmer.cli import _load_task, main

HANDLE = b"synthetic-handle-\xfb\xff"
HANDLE_B64 = base64.urlsafe_b64encode(HANDLE).decode("ascii")
MAX_BYTES = 16 * 1024 * 1024
MAX_OFFSET = (1 << 64) - 1
REQUIRED_FIELDS = (
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
)
IDENTITY_FIELDS = (
    "run_id",
    "plan_id",
    "task_id",
    "volume_id",
    "file_id",
    "export_name",
    "export_id",
)


@pytest.fixture
def task():
    now = datetime.now(timezone.utc)
    return {
        "schema_version": 2,
        "run_id": "run.synthetic-1",
        "plan_id": "plan.synthetic-1",
        "task_id": "task.synthetic-1",
        "volume_id": "volume-1",
        "file_id": "file.1:synthetic",
        "export_name": "export_name-1",
        "export_id": "export-id-1",
        "target_fh_b64": HANDLE_B64,
        "offset": 5,
        "length": 10,
        "max_bytes": 10,
        "created_at": _utc(now - timedelta(seconds=5)),
        "deadline": _utc(now + timedelta(seconds=120)),
        "target_valid_until": _utc(now + timedelta(seconds=240)),
    }


def _utc(value):
    return value.isoformat().replace("+00:00", "Z")


@pytest.fixture
def private_dir(tmp_path):
    # Some platforms expose their temporary directory through a symlink.
    path = tmp_path.resolve() / "synthetic-private-dir"
    path.mkdir(mode=0o700)
    path.chmod(0o700)
    return path


@pytest.fixture
def write_task(private_dir):
    def write(value, *, mode=0o600):
        path = private_dir / "synthetic-private-task.json"
        data = value if isinstance(value, bytes) else json.dumps(value).encode("utf-8")
        path.write_bytes(data)
        path.chmod(mode)
        return path

    return write


def assert_rejected(path, *, handle=HANDLE_B64):
    with pytest.raises(ValueError) as exc:
        _load_task(path, 5)
    message = str(exc.value)
    assert str(path) not in message
    assert path.name not in message
    assert handle not in message
    assert repr(HANDLE) not in message
    assert "synthetic-handle" not in message


def _simulate_wrong_owner(monkeypatch, path):
    """Alter only the selected inode's reported owner, not the other component."""
    info = path.stat()
    inode = (info.st_dev, info.st_ino)
    original_stat = os.stat
    original_lstat = os.lstat
    original_fstat = os.fstat

    def wrong_owner(info):
        if (info.st_dev, info.st_ino) != inode:
            return info
        fields = list(info)
        fields[4] = os.geteuid() + 1
        return os.stat_result(fields)

    monkeypatch.setattr(os, "stat", lambda *a, **kw: wrong_owner(original_stat(*a, **kw)))
    monkeypatch.setattr(os, "lstat", lambda *a, **kw: wrong_owner(original_lstat(*a, **kw)))
    monkeypatch.setattr(os, "fstat", lambda *a, **kw: wrong_owner(original_fstat(*a, **kw)))


@pytest.mark.parametrize("padded", [True, False])
def test_valid_task_decodes_handle_preserves_metadata_and_hashes_refs(task, write_task, padded):
    if not padded:
        task["target_fh_b64"] = task["target_fh_b64"].rstrip("=")
    request, effective_timeout = _load_task(write_task(task), 5)

    for field in ("run_id", "volume_id", "file_id", "export_name", "export_id"):
        assert request[field] == task[field]
    for field in ("offset", "length"):
        assert request[field] == task[field]
    for field in ("plan", "task"):
        expected = hashlib.sha256(task[f"{field}_id"].encode("ascii")).hexdigest()[:32]
        assert request[f"{field}_ref"] == expected
    assert request["file_handle"] == HANDLE
    assert isinstance(request["file_handle"], bytes)
    assert effective_timeout == 5


@pytest.mark.parametrize("same_deadline_and_valid_until", [False, True])
def test_effective_timeout_is_capped_by_deadline(
    task, write_task, same_deadline_and_valid_until
):
    now = datetime.now(timezone.utc)
    deadline = now + timedelta(seconds=60)
    task["created_at"] = _utc(now - timedelta(seconds=5))
    task["deadline"] = _utc(deadline)
    task["target_valid_until"] = _utc(
        deadline if same_deadline_and_valid_until else now + timedelta(seconds=180)
    )
    path = write_task(task)

    before = datetime.now(timezone.utc)
    _, effective_timeout = _load_task(path, 300)
    after = datetime.now(timezone.utc)

    assert (deadline - after).total_seconds() <= effective_timeout
    assert effective_timeout <= (deadline - before).total_seconds()
    assert 0 < effective_timeout < 300


def test_provided_timeout_wins_over_both_later_expiries(task, write_task):
    _, effective_timeout = _load_task(write_task(task), 0.1)
    assert effective_timeout == 0.1


def test_task_deadline_is_also_capped_by_elapsed_time(task, write_task):
    from predictive_cache.warmer.execution import _elapsed_time

    before = _elapsed_time()
    request, effective_timeout = _load_task(write_task(task), 300)
    after = _elapsed_time()
    assert before + effective_timeout <= request["_task_deadline_elapsed"]
    assert request["_task_deadline_elapsed"] <= after + effective_timeout


@pytest.mark.parametrize("timeout", [True, False, None, "5", 0, 0.099, 300.001, float("nan"), float("inf")])
def test_invalid_dispatch_timeout_budget_is_rejected(task, write_task, timeout):
    path = write_task(task)
    with pytest.raises(ValueError) as exc:
        _load_task(path, timeout)
    assert str(path) not in str(exc.value)
    assert HANDLE_B64 not in str(exc.value)


@pytest.mark.parametrize("length", [1, 128])
@pytest.mark.parametrize("padded", [True, False])
def test_handle_size_boundaries_and_base64url_forms_are_accepted(
    task, write_task, length, padded
):
    handle = b"\xff" * length
    encoded = base64.urlsafe_b64encode(handle).decode("ascii")
    task["target_fh_b64"] = encoded if padded else encoded.rstrip("=")
    request, _ = _load_task(write_task(task), 5)
    assert request["file_handle"] == handle


@pytest.mark.parametrize(
    "encoded",
    [
        "",
        base64.urlsafe_b64encode(b"x" * 129).decode("ascii"),
        "+/8=",  # Standard base64 alphabet, not base64url.
        "-_8==",
        "-_8===",
        "-_9=",  # Nonzero unused padding bits.
        "-_9",
        "Zg=",
        " Zg==",
        "Zg==\n",
        "!!!!",
    ],
)
def test_invalid_or_noncanonical_handle_is_rejected(task, write_task, encoded):
    task["target_fh_b64"] = encoded
    # An empty string is contained in every message.
    assert_rejected(write_task(task), handle=encoded or HANDLE_B64)


@pytest.mark.parametrize("field", REQUIRED_FIELDS)
def test_every_required_field_is_required(task, write_task, field):
    del task[field]
    assert_rejected(write_task(task))


@pytest.mark.parametrize(
    "field",
    [
        "extra",
        "origin",
        "operation",
        "operation_type",
        "write",
        "host",
        "port",
        "uid",
        "gid",
        "timeout_seconds",
        "file_handle_base64",
        "not_after",
        "target_expires_at",
    ],
)
def test_unknown_or_old_schema_fields_are_rejected(task, write_task, field):
    task[field] = "unexpected"
    assert_rejected(write_task(task))


def test_task_cannot_request_nfs_write(task, write_task):
    task["operation"] = "WRITE"
    assert_rejected(write_task(task))


@pytest.mark.parametrize(
    "field",
    ["schema_version", "file_id", "target_fh_b64", "created_at", "deadline", "target_valid_until"],
)
def test_duplicate_json_members_are_rejected_even_when_values_match(task, write_task, field):
    document = json.dumps(task)
    duplicate = f'{json.dumps(field)}: {json.dumps(task[field])}'
    document = document[:-1] + ", " + duplicate + "}"
    assert_rejected(write_task(document.encode("utf-8")))


@pytest.mark.parametrize("document", [b"", b"{}", b"[]", b"null", b"{", b'{"x":"\xff"}'])
def test_invalid_json_or_non_object_is_rejected(write_task, document):
    assert_rejected(write_task(document))


@pytest.mark.parametrize("field", ["schema_version", "offset", "length", "max_bytes"])
@pytest.mark.parametrize("value", [True, False, 1.0, "1", None])
def test_integer_fields_require_integers_not_booleans(task, write_task, field, value):
    task[field] = value
    assert_rejected(write_task(task))


@pytest.mark.parametrize(
    "field", [*IDENTITY_FIELDS, "target_fh_b64", "created_at", "deadline", "target_valid_until"]
)
@pytest.mark.parametrize("value", [None, True, 123, ["value"]])
def test_string_fields_require_strings(task, write_task, field, value):
    task[field] = value
    assert_rejected(write_task(task))


@pytest.mark.parametrize("field", IDENTITY_FIELDS)
@pytest.mark.parametrize(
    "value", ["", "a" * 129, "white space", "slash/name", "é", "line\n", "a\x00b"]
)
def test_opaque_identities_are_bounded_ascii_tokens(task, write_task, field, value):
    task[field] = value
    assert_rejected(write_task(task))


@pytest.mark.parametrize("field", IDENTITY_FIELDS)
@pytest.mark.parametrize("value", ["a", "a" * 128, "AZaz09._:-"])
def test_opaque_identity_boundaries_are_accepted(task, write_task, field, value):
    task[field] = value
    request, _ = _load_task(write_task(task), 5)
    if field in ("plan_id", "task_id"):
        expected = hashlib.sha256(value.encode("ascii")).hexdigest()[:32]
        assert request[field.replace("_id", "_ref")] == expected
    else:
        assert request[field] == value


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("schema_version", 0),
        ("schema_version", 1),
        ("schema_version", 3),
        ("offset", -1),
        ("offset", 1 << 64),
        ("length", 0),
        ("length", -1),
        ("length", MAX_BYTES + 1),
        ("max_bytes", 0),
        ("max_bytes", MAX_BYTES + 1),
        ("max_bytes", 9),
    ],
)
def test_out_of_range_integers_are_rejected(task, write_task, field, value):
    task[field] = value
    assert_rejected(write_task(task))


def test_range_cannot_overflow_unsigned_64_bit_offsets(task, write_task):
    task.update(offset=MAX_OFFSET, length=2, max_bytes=2)
    assert_rejected(write_task(task))


def test_range_cannot_exceed_maximum_nfs_regular_file_length(task, write_task):
    task.update(offset=MAX_OFFSET, length=1, max_bytes=1)
    assert_rejected(write_task(task))


@pytest.mark.parametrize(
    "updates",
    [
        {"offset": 0, "length": 1, "max_bytes": 1},
        {"offset": MAX_OFFSET - 1, "length": 1, "max_bytes": 1},
        {"length": MAX_BYTES, "max_bytes": MAX_BYTES},
    ],
)
def test_range_boundaries_are_accepted(task, write_task, updates):
    task.update(updates)
    request, _ = _load_task(write_task(task), 5)
    assert request["offset"] == task["offset"]
    assert request["length"] == task["length"]


@pytest.mark.parametrize("field", ["created_at", "deadline", "target_valid_until"])
@pytest.mark.parametrize(
    "value",
    [
        "",
        "2099-01-01T00:00:00",
        "2099-01-01T00:00:00+00:00",
        "2099-01-01T00:00:00z",
        "2099-01-01",
        "2099-13-01T00:00:00Z",
        "not-a-date",
    ],
)
def test_timestamps_must_be_utc_rfc3339_z(task, write_task, field, value):
    task[field] = value
    assert_rejected(write_task(task))


@pytest.mark.parametrize("field", ["deadline", "target_valid_until"])
def test_expiry_at_call_time_is_not_future(task, write_task, field):
    task[field] = _utc(datetime.now(timezone.utc))
    assert_rejected(write_task(task))


def test_created_at_must_not_be_in_the_future(task, write_task):
    task["created_at"] = _utc(datetime.now(timezone.utc) + timedelta(seconds=30))
    assert_rejected(write_task(task))


def test_deadline_must_not_exceed_target_valid_until(task, write_task):
    now = datetime.now(timezone.utc)
    task["deadline"] = _utc(now + timedelta(seconds=120))
    task["target_valid_until"] = _utc(now + timedelta(seconds=60))
    assert_rejected(write_task(task))


@pytest.mark.parametrize("seconds, accepted", [(300, True), (300.000001, False), (301, False)])
def test_target_validity_lifetime_cannot_exceed_300_seconds(
    task, write_task, seconds, accepted
):
    created = datetime.now(timezone.utc) - timedelta(seconds=5)
    task["created_at"] = _utc(created)
    task["deadline"] = _utc(created + timedelta(seconds=60))
    task["target_valid_until"] = _utc(created + timedelta(seconds=seconds))
    path = write_task(task)
    if accepted:
        request, _ = _load_task(path, 5)
        assert request["file_handle"] == HANDLE
    else:
        assert_rejected(path)


@pytest.mark.parametrize(
    "mode", [0o400, 0o000, 0o700, 0o640, 0o610, 0o604, 0o601, 0o666, 0o1600, 0o4600]
)
def test_task_file_mode_must_be_exactly_0600(task, write_task, mode):
    assert_rejected(write_task(task, mode=mode))


@pytest.mark.parametrize("mode", [0o500, 0o600, 0o710, 0o701, 0o750, 0o755, 0o777, 0o1700])
def test_parent_directory_mode_must_be_exactly_0700(task, write_task, private_dir, mode):
    path = write_task(task)
    private_dir.chmod(mode)
    try:
        assert_rejected(path)
    finally:
        private_dir.chmod(0o700)


def test_file_owned_by_another_uid_is_rejected(task, write_task, monkeypatch):
    path = write_task(task)
    _simulate_wrong_owner(monkeypatch, path)
    assert_rejected(path)


def test_parent_directory_owned_by_another_uid_is_rejected(task, write_task, monkeypatch):
    path = write_task(task)
    assert stat.S_ISDIR(path.parent.stat().st_mode)
    _simulate_wrong_owner(monkeypatch, path.parent)
    assert_rejected(path)


def test_ownership_is_checked_against_effective_not_real_uid(task, write_task, monkeypatch):
    path = write_task(task)
    monkeypatch.setattr(os, "getuid", lambda: os.geteuid() + 1)
    request, _ = _load_task(path, 5)
    assert request["file_handle"] == HANDLE


def test_symlink_to_private_file_is_rejected(task, write_task):
    target = write_task(task)
    link = target.parent / "synthetic-private-link.json"
    link.symlink_to(target)
    with pytest.raises(ValueError) as exc:
        _load_task(link, 5)
    assert str(link) not in str(exc.value)
    assert str(target) not in str(exc.value)
    assert HANDLE_B64 not in str(exc.value)


def test_symlink_to_private_parent_directory_is_rejected(task, write_task, tmp_path):
    target = write_task(task)
    link = tmp_path.resolve() / "synthetic-private-dir-link"
    link.symlink_to(target.parent, target_is_directory=True)
    path = link / target.name
    with pytest.raises(ValueError) as exc:
        _load_task(path, 5)
    message = str(exc.value)
    assert str(path) not in message
    assert str(target) not in message
    assert HANDLE_B64 not in message


def test_intermediate_directory_symlink_is_rejected(task, write_task, tmp_path):
    target = write_task(task)
    link = tmp_path.resolve() / "synthetic-ancestor-link"
    link.symlink_to(target.parent.parent, target_is_directory=True)
    path = link / target.parent.name / target.name
    with pytest.raises(ValueError) as exc:
        _load_task(path, 5)
    message = str(exc.value)
    assert str(path) not in message
    assert str(target) not in message
    assert HANDLE_B64 not in message


def test_file_with_multiple_hardlinks_is_rejected(task, write_task):
    path = write_task(task)
    alias = path.with_name("synthetic-second-link.json")
    os.link(path, alias)
    assert path.stat().st_nlink == 2
    assert_rejected(path)
    assert_rejected(alias)
    # Removing the extra name restores the ordinary single-link input.
    alias.unlink()
    request, _ = _load_task(path, 5)
    assert request["file_handle"] == HANDLE


@pytest.mark.parametrize(
    "name",
    [
        "synthetic-task.tmp",
        "synthetic-task.tmp.json",
        "synthetic-task.json.tmp",
        "synthetic-task",
        "synthetic-task.JSON",
        ".synthetic-hidden.json",
        ".synthetic-hidden.tmp.json",
    ],
)
def test_uncommitted_or_hidden_task_names_are_rejected(task, write_task, name):
    original = write_task(task)
    path = original.rename(original.with_name(name))
    assert_rejected(path)
    assert path.is_file()


@pytest.mark.parametrize("name", ["synthetic-task.json", "synthetic-task.tmp-not.json"])
def test_committed_nonhidden_json_names_are_accepted(task, write_task, name):
    original = write_task(task)
    path = original.rename(original.with_name(name))
    request, _ = _load_task(path, 5)
    assert request["file_handle"] == HANDLE


def test_repeated_loads_preserve_the_same_input(task, write_task):
    path = write_task(task)
    original_data = path.read_bytes()
    original_stat = path.stat()
    original_names = {item.name for item in path.parent.iterdir()}

    first, first_timeout = _load_task(path, 5)
    second, second_timeout = _load_task(path, 5)

    assert first.pop("_task_deadline_elapsed") == pytest.approx(
        second.pop("_task_deadline_elapsed"), rel=0, abs=0.1
    )
    assert first == second
    assert first["file_handle"] == HANDLE
    assert first_timeout == second_timeout == 5
    assert path.read_bytes() == original_data
    assert path.stat().st_ino == original_stat.st_ino
    assert path.stat().st_mode == original_stat.st_mode
    assert path.stat().st_nlink == 1
    assert {item.name for item in path.parent.iterdir()} == original_names


def test_replaced_task_file_symlink_is_rejected_after_a_valid_load(task, write_task):
    path = write_task(task)
    request, _ = _load_task(path, 5)
    assert request["file_handle"] == HANDLE
    target = path.with_name("synthetic-second-task.json")
    path.rename(target)
    path.symlink_to(target)
    assert_rejected(path)


def test_tampered_task_content_is_revalidated_on_next_load(task, write_task):
    path = write_task(task)
    request, _ = _load_task(path, 5)
    assert request["file_handle"] == HANDLE
    task["max_bytes"] = task["length"] - 1
    path.write_text(json.dumps(task), encoding="utf-8")
    assert_rejected(path)


def test_tampered_task_permissions_are_revalidated_on_next_load(task, write_task):
    path = write_task(task)
    request, _ = _load_task(path, 5)
    assert request["file_handle"] == HANDLE
    path.chmod(0o644)
    assert_rejected(path)


def test_task_changed_during_read_is_rejected(task, write_task, monkeypatch):
    path = write_task(task)
    original_fstat = os.fstat
    regular_checks = 0

    def changing_fstat(fd):
        nonlocal regular_checks
        info = original_fstat(fd)
        if stat.S_ISREG(info.st_mode):
            regular_checks += 1
            if regular_checks == 2:
                os.utime(path, ns=(info.st_atime_ns, info.st_mtime_ns + 1_000_000))
                info = original_fstat(fd)
        return info

    monkeypatch.setattr(os, "fstat", changing_fstat)
    assert_rejected(path)
    assert regular_checks == 2


def test_directory_is_not_a_regular_task_file(private_dir):
    path = private_dir / "synthetic-not-a-file"
    path.mkdir(mode=0o700)
    assert_rejected(path)


@pytest.mark.parametrize("size", [8192, 8193])
def test_task_file_size_limit_is_in_bytes(task, write_task, size):
    document = json.dumps(task).encode("utf-8")
    path = write_task(document + b" " * (size - len(document)))
    if size == 8192:
        request, _ = _load_task(path, 5)
        assert request["file_handle"] == HANDLE
    else:
        assert_rejected(path)


def test_nested_json_cannot_crash_validation(write_task):
    assert_rejected(write_task(b"[" * 1500 + b"]" * 1500))


def test_cli_dispatches_private_handle_without_printing_it(task, write_task, monkeypatch, capsys):
    path = write_task(task)
    calls = []

    def fake_execute(request, timeout, emit):
        calls.append((request, timeout))
        return {
            "type": "result",
            "status": "completed",
            "reason": "complete",
            "bytes_requested": request["length"],
            "bytes_read": request["length"],
            "bytes_read_is_lower_bound": False,
            "plan_ref": request["plan_ref"],
            "task_ref": request["task_ref"],
        }

    monkeypatch.setattr("predictive_cache.warmer.cli.execute", fake_execute)
    exit_code = main(
        [
            "--host", "192.0.2.1", "--task-file", str(path),
            "--timeout-seconds", "5", "--uid", "1001", "--gid", "1002",
        ]
    )
    output = capsys.readouterr()
    assert exit_code == 0
    assert len(calls) == 1
    request, timeout = calls[0]
    assert request["file_handle"] == HANDLE
    assert request["host"] == "192.0.2.1"
    assert (request["uid"], request["gid"]) == (1001, 1002)
    assert 0 < timeout <= 5
    assert "path" not in request
    assert json.loads(output.out)["status"] == "completed"
    assert HANDLE_B64 not in output.out + output.err
    assert "synthetic-handle" not in output.out + output.err


def test_cli_preserves_shorter_task_deadline_when_invocation_timeout_is_longer(
    task, write_task, monkeypatch
):
    from predictive_cache.warmer.execution import _elapsed_time

    path = write_task(task)
    captured = []

    def fake_execute(request, timeout, emit):
        captured.append(request)
        return {"status": "completed", "reason": "complete"}

    monkeypatch.setattr("predictive_cache.warmer.cli.execute", fake_execute)
    before = _elapsed_time()
    assert main(
        [
            "--host", "192.0.2.1", "--task-file", str(path),
            "--timeout-seconds", "300", "--uid", "1001", "--gid", "1002",
        ]
    ) == 0
    after = _elapsed_time()
    assert before + 110 < captured[0]["_timeout_deadline"] <= after + 120


def test_cli_rejects_tampered_task_before_dispatch(task, write_task, monkeypatch, capsys):
    task["max_bytes"] = task["length"] - 1
    path = write_task(task)

    def unexpected_execute(*args):
        pytest.fail("invalid task must not be dispatched")

    monkeypatch.setattr("predictive_cache.warmer.cli.execute", unexpected_execute)
    with pytest.raises(SystemExit) as exc:
        main(
            [
                "--host", "192.0.2.1", "--task-file", str(path),
                "--timeout-seconds", "5", "--uid", "1001", "--gid", "1002",
            ]
        )
    assert exc.value.code == 2
    output = capsys.readouterr()
    assert json.loads(output.out) == {
        "type": "pre_validation_result",
        "status": "error",
        "reason": "invalid_task_or_arguments",
        "execution_started": False,
    }
    assert str(path) not in output.out + output.err
    assert HANDLE_B64 not in output.out + output.err
    assert "synthetic-handle" not in output.out + output.err


def test_separate_cli_invocations_each_dispatch_and_preserve_input(
    task, write_task, monkeypatch, capsys
):
    path = write_task(task)
    original_data = path.read_bytes()
    original_ino = path.stat().st_ino
    original_names = {item.name for item in path.parent.iterdir()}
    calls = []

    def fake_execute(request, timeout, emit):
        calls.append((request, timeout))
        return {"type": "result", "status": "completed", "reason": "complete"}

    monkeypatch.setattr("predictive_cache.warmer.cli.execute", fake_execute)
    args = [
        "--host", "192.0.2.1", "--task-file", str(path),
        "--timeout-seconds", "5", "--uid", "1001", "--gid", "1002",
    ]
    for expected_calls in (1, 2):
        assert main(args) == 0
        assert len(calls) == expected_calls
        assert calls[-1][0]["file_handle"] == HANDLE
        assert 0 < calls[-1][1] <= 5
        assert path.read_bytes() == original_data
        assert path.stat().st_ino == original_ino
        assert {item.name for item in path.parent.iterdir()} == original_names
        output = capsys.readouterr()
        assert json.loads(output.out)["status"] == "completed"
        assert HANDLE_B64 not in output.out + output.err


def test_empty_task_path_fails_before_dispatch(monkeypatch, capsys):
    def unexpected_execute(*args):
        pytest.fail("invalid task must not be dispatched")

    monkeypatch.setattr("predictive_cache.warmer.cli.execute", unexpected_execute)
    with pytest.raises(SystemExit) as exc:
        main(
            [
                "--host", "192.0.2.1", "--task-file", "",
                "--timeout-seconds", "5", "--uid", "1001", "--gid", "1002",
            ]
        )
    assert exc.value.code == 2
    assert "invalid private task path" in capsys.readouterr().err


def test_slow_loading_does_not_subtract_elapsed_time_from_remaining_deadline(monkeypatch):
    observed = []

    def slow_load(path, timeout, deadline):
        time.sleep(0.12)
        return {
            "file_handle": HANDLE,
            "offset": 0,
            "length": 1,
            "_task_deadline_utc": time.time() + 0.05,
        }, 0.05

    def fake_execute(request, timeout, emit):
        observed.append((request, timeout))
        return {"status": "completed", "reason": "complete"}

    monkeypatch.setattr("predictive_cache.warmer.cli._load_task_supervised", slow_load)
    monkeypatch.setattr("predictive_cache.warmer.cli.execute", fake_execute)
    assert main(
        [
            "--host", "192.0.2.1", "--task-file", "task.json",
            "--timeout-seconds", "5", "--uid", "1001", "--gid", "1002",
        ]
    ) == 0
    assert len(observed) == 1
    assert observed[0][1] == 0.05
    assert "_timeout_deadline" in observed[0][0]


def _blocked_task_loader(path, timeout):
    with open(path, "w", encoding="ascii") as marker:
        marker.write(str(os.getpid()))
    time.sleep(20)


def _cancel_validation_runner(marker, sender):
    from predictive_cache.warmer.cli import _load_task_supervised, _TaskLoadStopped
    from predictive_cache.warmer.execution import _elapsed_time

    try:
        _load_task_supervised(marker, 10, _elapsed_time() + 10, loader=_blocked_task_loader)
    except _TaskLoadStopped as exc:
        sender.send((exc.status, exc.reason))
    finally:
        sender.close()


def test_blocked_task_loading_is_stopped_at_invocation_deadline(tmp_path):
    from predictive_cache.warmer.cli import _load_task_supervised, _TaskLoadStopped
    from predictive_cache.warmer.execution import _elapsed_time

    marker = tmp_path / "validation-started"
    started = _elapsed_time()
    with pytest.raises(_TaskLoadStopped) as exc:
        _load_task_supervised(
            str(marker), 1, started + 1, loader=_blocked_task_loader
        )
    assert marker.exists()
    assert (exc.value.status, exc.value.reason) == ("timeout", "deadline_exceeded")
    assert _elapsed_time() - started < 4
    with pytest.raises(ProcessLookupError):
        os.kill(int(marker.read_text()), 0)


def test_sigterm_stops_blocked_task_loading(tmp_path):
    ctx = multiprocessing.get_context("spawn")
    receiver, sender = ctx.Pipe(duplex=False)
    marker = tmp_path / "validation-started"
    runner = ctx.Process(target=_cancel_validation_runner, args=(str(marker), sender))
    runner.start()
    sender.close()
    try:
        deadline = time.monotonic() + 5
        while not marker.exists() and time.monotonic() < deadline:
            time.sleep(0.01)
        assert marker.exists()
        os.kill(runner.pid, signal.SIGTERM)
        assert receiver.poll(5)
        assert receiver.recv() == ("cancelled", "terminated")
        runner.join(5)
        assert runner.exitcode == 0
        with pytest.raises(ProcessLookupError):
            os.kill(int(marker.read_text()), 0)
    finally:
        if runner.is_alive():
            runner.kill()
            runner.join()
        receiver.close()


def test_task_loader_receives_result_sent_just_before_worker_exit(monkeypatch):
    from predictive_cache.warmer import cli
    from predictive_cache.warmer.execution import _elapsed_time

    class Receiver:
        def __init__(self):
            self.poll_results = iter((False, True))

        def poll(self, timeout):
            return next(self.poll_results)

        def recv(self):
            return "ok", {"length": 1}, 5

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

    class Context:
        def Pipe(self, duplex):
            return Receiver(), Sender()

        def Process(self, **kwargs):
            return Process()

    monkeypatch.setattr(cli.multiprocessing, "get_context", lambda _: Context())
    monkeypatch.setattr(cli, "_stop", lambda _: True)
    assert cli._load_task_supervised("task.json", 5, _elapsed_time() + 5) == ({"length": 1}, 5)


@pytest.mark.parametrize(
    ("signum", "reason", "exit_code"),
    [(signal.SIGTERM, "terminated", 143), (signal.SIGINT, "interrupted", 130)],
)
def test_cancellation_during_task_loader_cleanup_prevents_nfs_dispatch(
    task, write_task, monkeypatch, capsys, signum, reason, exit_code
):
    from predictive_cache.warmer import cli

    path = write_task(task)
    original_stop = cli._stop

    def stop_after_cancellation(process):
        os.kill(os.getpid(), signum)
        return original_stop(process)

    def unexpected_execute(*args):
        pytest.fail("NFS worker must not start after cancellation")

    monkeypatch.setattr(cli, "_stop", stop_after_cancellation)
    monkeypatch.setattr(cli, "execute", unexpected_execute)
    assert main(
        [
            "--host", "192.0.2.1", "--task-file", str(path),
            "--timeout-seconds", "5", "--uid", "1001", "--gid", "1002",
        ]
    ) == exit_code
    assert json.loads(capsys.readouterr().out) == {
        "type": "pre_validation_result",
        "status": "cancelled",
        "reason": reason,
        "execution_started": False,
    }


@pytest.mark.parametrize(
    ("signum", "reason", "exit_code"),
    [(signal.SIGTERM, "terminated", 143), (signal.SIGINT, "interrupted", 130)],
)
def test_cancellation_during_loader_handoff_overrides_inherited_ignore(
    monkeypatch, capsys, signum, reason, exit_code
):
    from predictive_cache.warmer import cli

    def interrupted_load(*args):
        os.kill(os.getpid(), signum)
        pytest.fail("the cancellation must not be ignored")

    def unexpected_execute(*args):
        pytest.fail("NFS worker must not start after cancellation")

    monkeypatch.setattr(cli, "_load_task_supervised", interrupted_load)
    monkeypatch.setattr(cli, "execute", unexpected_execute)
    previous = signal.signal(signum, signal.SIG_IGN)
    try:
        assert main(
            [
                "--host", "192.0.2.1", "--task-file", "task.json",
                "--timeout-seconds", "5", "--uid", "1001", "--gid", "1002",
            ]
        ) == exit_code
        assert signal.getsignal(signum) == signal.SIG_IGN
    finally:
        signal.signal(signum, previous)
    assert json.loads(capsys.readouterr().out) == {
        "type": "pre_validation_result",
        "status": "cancelled",
        "reason": reason,
        "execution_started": False,
    }


@pytest.mark.parametrize(("signum", "exit_code"), [(signal.SIGTERM, 143), (signal.SIGINT, 130)])
def test_cancellation_after_execution_handoff_does_not_claim_nfs_never_started(
    task, write_task, monkeypatch, capsys, signum, exit_code
):
    from predictive_cache.warmer import cli

    def interrupted_execute(*args):
        os.kill(os.getpid(), signum)
        pytest.fail("the cancellation must not be ignored")

    path = write_task(task)
    monkeypatch.setattr(cli, "execute", interrupted_execute)
    assert main(
        [
            "--host", "192.0.2.1", "--task-file", str(path),
            "--timeout-seconds", "5", "--uid", "1001", "--gid", "1002",
        ]
    ) == exit_code
    assert capsys.readouterr().out == ""


@pytest.mark.parametrize(
    ("status", "reason", "exit_code"),
    [("timeout", "deadline_exceeded", 1), ("cancelled", "terminated", 143)],
)
def test_cli_reports_stopped_validation_without_inventing_requested_bytes(
    monkeypatch, capsys, status, reason, exit_code
):
    from predictive_cache.warmer.cli import _TaskLoadStopped

    def stopped(*args):
        raise _TaskLoadStopped(status, reason)

    monkeypatch.setattr("predictive_cache.warmer.cli._load_task_supervised", stopped)
    assert main(
        [
            "--host", "192.0.2.1", "--task-file", "task.json",
            "--timeout-seconds", "5", "--uid", "1001", "--gid", "1002",
        ]
    ) == exit_code
    result = json.loads(capsys.readouterr().out)
    assert result == {
        "type": "pre_validation_result",
        "status": status,
        "reason": reason,
        "execution_started": False,
    }
