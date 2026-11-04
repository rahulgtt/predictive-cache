"""Deterministic operation generation, execution, safety checks, and reconciliation."""

from __future__ import annotations

import base64
import ctypes
import hashlib
import json
import os
import random
import re
import signal
import stat
import subprocess
import threading
import time
from collections import Counter, defaultdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Callable, Literal, Protocol

from .io import append_model, canonical_json, read_models, sha256, write_jsonl
from .models import (
    Checkpoint,
    CollectionManifest,
    CollectorDurableEvent,
    CollectorFileManifest,
    ExecutionResult,
    FixtureCatalog,
    FixtureEntry,
    ObservedEvent,
    OfferedOperation,
    PreReadHealth,
    ReconciliationReport,
)

_LIBC = ctypes.CDLL(None)
_LIBC.posix_memalign.argtypes = [
    ctypes.POINTER(ctypes.c_void_p),
    ctypes.c_size_t,
    ctypes.c_size_t,
]
_LIBC.posix_memalign.restype = ctypes.c_int
_LIBC.free.argtypes = [ctypes.c_void_p]
_LIBC.free.restype = None


class ReadExecutor(Protocol):
    def read(self, operation: OfferedOperation) -> ExecutionResult: ...


class CheckpointProvider(Protocol):
    def collect(self, scheduled_at: datetime) -> list[Checkpoint]: ...

    def health(self, operation: OfferedOperation) -> list[PreReadHealth]: ...

    def post_read(
        self,
        operation: OfferedOperation,
        result: ExecutionResult,
    ) -> list[CollectorDurableEvent]: ...


ISOLATED_PROVIDER_BOOTSTRAP = (
    "import runpy,sys;"
    "package,script,*args=sys.argv[1:];"
    "sys.path.insert(0,package);"
    "sys.argv=[script,*args];"
    "runpy.run_path(script,run_name='__main__')"
)


class CommandCheckpointProvider:
    """Invoke one trusted collector command for one scheduled checkpoint."""

    def __init__(
        self,
        script: Path,
        package: Path,
        run_dir: Path,
        *,
        script_sha256: str,
        package_sha256: str,
        timeout_s: int = 30,
        python_executable: str = "python3",
    ) -> None:
        self.script = script.absolute()
        self.package = package.absolute()
        self.run_dir = run_dir.absolute()
        self.timeout_s = timeout_s
        self.python_executable = python_executable
        self._script_fd = self._open_verified(self.script, script_sha256)
        try:
            self._package_fd = self._open_verified(
                self.package,
                package_sha256,
            )
        except BaseException:
            os.close(self._script_fd)
            raise

    @staticmethod
    def _open_verified(path: Path, expected_sha256: str) -> int:
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
        fd = os.open(path, flags)
        try:
            metadata = os.fstat(fd)
            if not stat.S_ISREG(metadata.st_mode):
                raise ValueError("provider runtime artifact must be a regular file")
            digest = hashlib.sha256()
            with os.fdopen(os.dup(fd), "rb") as stream:
                for block in iter(lambda: stream.read(1024 * 1024), b""):
                    digest.update(block)
            if digest.hexdigest() != expected_sha256:
                raise ValueError("provider runtime artifact changed before use")
            os.lseek(fd, 0, os.SEEK_SET)
            return fd
        except BaseException:
            os.close(fd)
            raise

    def close(self) -> None:
        for name in ("_script_fd", "_package_fd"):
            fd = getattr(self, name, None)
            if fd is not None:
                os.close(fd)
                setattr(self, name, None)

    def _command(self, action: str, *arguments: str) -> list[str | Path]:
        if self._script_fd is None or self._package_fd is None:
            raise RuntimeError("provider runtime artifacts are closed")
        os.lseek(self._script_fd, 0, os.SEEK_SET)
        os.lseek(self._package_fd, 0, os.SEEK_SET)
        return [
            self.python_executable,
            "-I",
            "-S",
            "-c",
            ISOLATED_PROVIDER_BOOTSTRAP,
            f"/proc/self/fd/{self._package_fd}",
            f"/proc/self/fd/{self._script_fd}",
            action,
            self.run_dir,
            *arguments,
        ]

    def collect(self, scheduled_at: datetime) -> list[Checkpoint]:
        completed = subprocess.run(
            self._command(
                "checkpoint",
                "--scheduled-at",
                scheduled_at.isoformat().replace("+00:00", "Z"),
            ),
            check=True,
            capture_output=True,
            text=True,
            timeout=self.timeout_s,
            pass_fds=(self._script_fd, self._package_fd),
        )
        return [
            Checkpoint.model_validate_json(line)
            for line in completed.stdout.splitlines()
            if line
        ]

    def health(self, operation: OfferedOperation) -> list[PreReadHealth]:
        completed = subprocess.run(
            self._command(
                "probe",
                "--operation-id",
                operation.operation_id,
            ),
            check=True,
            capture_output=True,
            text=True,
            timeout=self.timeout_s,
            pass_fds=(self._script_fd, self._package_fd),
        )
        return [
            PreReadHealth.model_validate_json(line)
            for line in completed.stdout.splitlines()
            if line
        ]

    def post_read(
        self,
        operation: OfferedOperation,
        result: ExecutionResult,
    ) -> list[CollectorDurableEvent]:
        completed = subprocess.run(
            self._command(
                "post-read",
                "--operation-id",
                operation.operation_id,
            ),
            check=True,
            capture_output=True,
            text=True,
            timeout=self.timeout_s,
            pass_fds=(self._script_fd, self._package_fd),
        )
        return [
            CollectorDurableEvent.model_validate_json(line)
            for line in completed.stdout.splitlines()
            if line
        ]


class DryRunExecutor:
    """Return a successful result without touching the target path or network."""

    def __init__(self, now: Callable[[], datetime] | None = None) -> None:
        self._now = now or (lambda: datetime.now(UTC))

    def read(self, operation: OfferedOperation) -> ExecutionResult:
        return ExecutionResult(
            operation_id=operation.operation_id,
            completed_at_utc=self._now(),
            status="ok",
            bytes_read=operation.count,
            latency_ms=0,
            content_sha256=None,
            executor_kind="dry-run",
            filesystem_type="none",
        )


class DirectMountExecutor:
    """Issue aligned O_DIRECT reads beneath an already-mounted export."""

    def __init__(
        self,
        mount_root: Path,
        *,
        latency_guardrail_ms: int,
        cancelled: Callable[[], bool] | None = None,
        allow_non_nfs_for_test: bool = False,
    ) -> None:
        self.mount_root = mount_root.absolute()
        self.allow_non_nfs_for_test = allow_non_nfs_for_test
        self.latency_guardrail_ms = latency_guardrail_ms
        self.cancelled = cancelled or (lambda: False)
        self.filesystem_type, read_only = self._mount_info()
        if not allow_non_nfs_for_test and (
            self.filesystem_type != "nfs4" or not read_only
        ):
            raise ValueError("mount root must be on a read-only nfs4 filesystem")
        if not hasattr(os, "O_DIRECT"):
            raise ValueError("O_DIRECT is unavailable on this platform")

    def _mount_info(self) -> tuple[str, bool]:
        target = self.mount_root.resolve(strict=True)
        best: tuple[int, str, bool] | None = None
        mountinfo = Path("/proc/self/mountinfo")
        if mountinfo.exists():
            for line in mountinfo.read_text().splitlines():
                before, after = line.split(" - ", 1)
                fields, post = before.split(), after.split()
                mount_point = Path(fields[4].replace("\\040", " "))
                try:
                    target.relative_to(mount_point)
                except ValueError:
                    continue
                candidate = (len(mount_point.parts), post[0], "ro" in fields[5].split(","))
                if best is None or candidate[0] >= best[0]:
                    best = candidate
        return (best[1], best[2]) if best else ("unknown", False)

    def read(self, operation: OfferedOperation) -> ExecutionResult:
        started = time.monotonic_ns()
        status = "error"
        data = b""
        error: str | None = None
        descriptors: list[int] = []
        previous_alarm = None
        try:
            if operation.offset % 4096 or operation.count % 4096:
                status, error = "error", "O_DIRECT offset/count must be 4096-byte aligned"
            elif operation.tag:
                status, error = "error", "controlled mount executor requires an empty tag"
            elif self.cancelled():
                status, error = "cancelled", "cancelled before open"
            else:
                if threading.current_thread() is not threading.main_thread():
                    raise RuntimeError("direct mount reads require the main thread")

                def timeout_handler(_signum, _frame):
                    raise TimeoutError("direct read latency guardrail expired")

                previous_alarm = signal.signal(signal.SIGALRM, timeout_handler)
                signal.setitimer(signal.ITIMER_REAL, self.latency_guardrail_ms / 1000)
                flags = os.O_RDONLY | os.O_DIRECTORY
                flags |= getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0)
                current = os.open(self.mount_root, flags)
                descriptors.append(current)
                root_device = os.fstat(current).st_dev
                parts = Path(operation.relative_path).parts
                for component in parts[:-1]:
                    child = os.open(component, flags, dir_fd=current)
                    descriptors.append(child)
                    if os.fstat(child).st_dev != root_device:
                        raise OSError("path crosses a nested mount")
                    current = child
                final_flags = os.O_RDONLY | os.O_DIRECT | getattr(os, "O_NOFOLLOW", 0)
                final_flags |= getattr(os, "O_NONBLOCK", 0) | getattr(os, "O_CLOEXEC", 0)
                file_fd = os.open(parts[-1], final_flags, dir_fd=current)
                descriptors.append(file_fd)
                file_stat = os.fstat(file_fd)
                if not stat.S_ISREG(file_stat.st_mode):
                    raise OSError("target is not a regular file")
                if file_stat.st_dev != root_device:
                    raise OSError("final file crosses a nested mount")
                if file_stat.st_size != operation.file_size:
                    raise OSError("fixture size changed after catalog freeze")
                if file_stat.st_size < operation.offset:
                    status = "eof"
                elif self.cancelled():
                    status, error = "cancelled", "cancelled before pread"
                else:
                    pointer = ctypes.c_void_p()
                    allocation = _LIBC.posix_memalign(
                        ctypes.byref(pointer),
                        4096,
                        operation.count,
                    )
                    if allocation != 0:
                        raise OSError(allocation, "posix_memalign failed")
                    try:
                        buffer = (ctypes.c_ubyte * operation.count).from_address(
                            pointer.value
                        )
                        view = memoryview(buffer).cast("B")
                        bytes_read = os.preadv(file_fd, [view], operation.offset)
                        data = bytes(view[:bytes_read])
                    finally:
                        _LIBC.free(pointer)
                    status = (
                        "ok"
                        if len(data) == operation.count
                        else "eof"
                        if not data
                        else "short"
                    )
        except TimeoutError as exception:
            status, error = "timeout", str(exception)
        except (OSError, RuntimeError) as exception:
            status, error = "error", f"{type(exception).__name__}: {exception}"
        finally:
            if previous_alarm is not None:
                signal.setitimer(signal.ITIMER_REAL, 0)
                signal.signal(signal.SIGALRM, previous_alarm)
            for descriptor in reversed(descriptors):
                os.close(descriptor)
        latency_ms = (time.monotonic_ns() - started) / 1_000_000
        return ExecutionResult(
            operation_id=operation.operation_id,
            completed_at_utc=datetime.now(UTC),
            status=status,
            bytes_read=len(data),
            latency_ms=latency_ms,
            content_sha256=hashlib.sha256(data).hexdigest() if data else None,
            executor_kind="direct-mount",
            filesystem_type=self.filesystem_type,
            error=error,
        )


def catalog_sha256(catalog: FixtureCatalog) -> str:
    payload = canonical_json(catalog.model_dump(mode="json")).encode()
    return hashlib.sha256(payload).hexdigest()


def operation_id(
    manifest: CollectionManifest,
    fixture_id: str,
    operation_index: int,
    offset: int,
    count: int,
) -> str:
    payload = {
        "count": count,
        "fixture_id": fixture_id,
        "index": operation_index,
        "offset": offset,
        "run_id": manifest.run_id,
        "seed": manifest.seed,
    }
    return hashlib.sha256(canonical_json(payload).encode()).hexdigest()


def planned_operations(
    manifest: CollectionManifest,
    catalog: FixtureCatalog,
) -> list[tuple[int, FixtureEntry, int, int, str]]:
    if catalog_sha256(catalog) != manifest.fixture_catalog_sha256:
        raise ValueError("fixture catalog checksum mismatch")
    fixtures = list(catalog.fixtures)
    if any(fixture.volume_id != manifest.volume_id for fixture in fixtures):
        raise ValueError("fixture volume does not match manifest")
    if any(fixture.export_id != manifest.export_id for fixture in fixtures):
        raise ValueError("fixture export does not match manifest")
    rng = random.Random(manifest.seed)
    count = manifest.range_bytes[0]
    candidates = [
        (fixture, offset, count)
        for fixture in fixtures
        for offset in manifest.range_offsets
        if offset + count <= fixture.size_bytes
    ]
    if manifest.offered_count > len(candidates):
        raise ValueError("fixture catalog lacks enough unique non-overlapping ranges")
    rng.shuffle(candidates)
    operations = []
    for index, (fixture, offset, actual_count) in enumerate(
        candidates[: manifest.offered_count]
    ):
        identity = operation_id(manifest, fixture.fixture_id, index, offset, actual_count)
        operations.append((index, fixture, offset, actual_count, identity))
    return operations


def correlate_untagged_events(
    manifest: CollectionManifest,
    offered: list[OfferedOperation],
    events: list[ObservedEvent],
) -> list[ObservedEvent]:
    """Resolve empty-tag events using exact range identity, bounded time, and order."""
    if manifest.tag_mode != "empty":
        raise ValueError("untagged correlation requires tag_mode=empty")
    by_range: dict[tuple[str, str, int, int], list[OfferedOperation]] = defaultdict(list)
    for operation in offered:
        by_range[
            (operation.volume_id, operation.file_id, operation.offset, operation.count)
        ].append(operation)
    _validate_producer_sequences(events)
    correlated: list[ObservedEvent] = []
    last_operation_index = -1
    for event in sorted(
        events,
        key=lambda item: (
            item.durable_at,
            item.pod_uid,
            item.producer_epoch,
            item.producer_sequence,
        ),
    ):
        if event.operation_id is not None or event.status != "unknown":
            raise ValueError("correlation input must be an unresolved unknown event")
        key = (event.volume_id, event.file_id, event.offset, event.count)
        matches = [
            operation
            for operation in by_range.get(key, [])
            if operation.offered_at_utc
            <= event.durable_at
            <= operation.offered_at_utc
            + timedelta(seconds=manifest.untagged_correlation_window_s)
        ]
        if len(matches) != 1:
            raise ValueError("untagged event correlation is missing or ambiguous")
        operation = matches[0]
        if operation.operation_index <= last_operation_index:
            raise ValueError("untagged event order contradicts offered operation order")
        last_operation_index = operation.operation_index
        correlated.append(
            ObservedEvent.model_validate(
                event.model_dump()
                | {
                    "operation_id": operation.operation_id,
                    "status": "durable",
                }
            )
        )
    return correlated


def _validate_producer_sequences(events: list[ObservedEvent]) -> None:
    for (_pod_uid, _epoch), scoped in _group_by_producer(events).items():
        ordered_sequences = sorted(scoped, key=lambda item: item.producer_sequence)
        if any(
            right.producer_sequence != left.producer_sequence + 1
            for left, right in zip(ordered_sequences, ordered_sequences[1:])
        ):
            raise ValueError("producer sequence is not contiguous within epoch")


def _group_by_producer(
    events: list[ObservedEvent],
) -> dict[tuple[str, str], list[ObservedEvent]]:
    grouped: dict[tuple[str, str], list[ObservedEvent]] = defaultdict(list)
    for event in events:
        grouped[(event.pod_uid, event.producer_epoch)].append(event)
    return grouped


def _observed_from_provider(event: CollectorDurableEvent) -> ObservedEvent:
    return ObservedEvent(
        operation_id=None,
        event_id=event.event_id,
        pod_uid=event.pod_uid,
        producer_epoch=event.producer_epoch,
        producer_sequence=event.producer_sequence,
        server_operation_id=event.server_operation_id,
        sink_time_ns=event.sink_time_ns,
        volume_id=event.volume_id,
        file_id=event.file_id,
        offset=event.offset,
        count=event.count,
        durable_at=event.durable_at,
        status="unknown",
        tag=event.tag or None,
    )


def _validate_provider_event(
    evidence_dir: Path,
    manifest: CollectionManifest,
    operation: OfferedOperation,
    result: ExecutionResult,
    event: CollectorDurableEvent,
    checkpoint_identities: set[tuple[str, str, str, str]],
) -> dict:
    if event.run_id != manifest.run_id:
        raise RuntimeError("provider event run identity mismatch")
    if event.durable_at < operation.offered_at_utc or event.durable_at >= manifest.end_time:
        raise RuntimeError("provider durable timestamp outside operation/run bounds")
    if (event.volume_id, event.file_id, event.export_id, event.offset, event.count) != (
        operation.volume_id,
        operation.file_id,
        operation.export_id,
        operation.offset,
        operation.count,
    ):
        raise RuntimeError("provider event range identity mismatch")
    if event.outcome != "success" or event.returned_bytes != result.bytes_read:
        raise RuntimeError("provider event outcome contradicts execution result")
    if event.nfs_status != 0 or event.tag != operation.tag:
        raise RuntimeError("provider event status/tag is not approved")
    if (
        event.pod_name,
        event.pod_uid,
        event.node_name,
        event.producer_epoch,
    ) not in checkpoint_identities:
        raise RuntimeError("provider event producer identity lacks checkpoint evidence")
    path = evidence_dir / event.raw_jsonl_path
    try:
        resolved = path.resolve(strict=True)
        if (
            evidence_dir.resolve() not in resolved.parents
            or path.is_symlink()
            or not path.is_file()
            or sha256(path) != event.raw_jsonl_sha256
        ):
            raise RuntimeError("provider raw source checksum mismatch")
        lines = path.read_bytes().splitlines()
        raw_record = lines[event.raw_line_number - 1]
        if hashlib.sha256(raw_record).hexdigest() != event.raw_record_sha256:
            raise RuntimeError("provider raw record checksum mismatch")
        def unique_object(pairs):
            value = {}
            for key, item in pairs:
                if key in value:
                    raise ValueError("duplicate raw JSON key")
                value[key] = item
            return value

        raw = json.loads(raw_record, object_pairs_hook=unique_object)
        schema = json.loads((evidence_dir / "event-schema-v2.json").read_text())

        def schema_matches(value, contract):
            if "anyOf" in contract:
                return any(schema_matches(value, item) for item in contract["anyOf"])
            expected_type = contract.get("type")
            if expected_type == "object" and not isinstance(value, dict):
                return False
            if expected_type == "string" and not isinstance(value, str):
                return False
            if expected_type == "integer" and type(value) is not int:
                return False
            if expected_type == "boolean" and type(value) is not bool:
                return False
            if expected_type == "null" and value is not None:
                return False
            if "const" in contract and value != contract["const"]:
                return False
            if "enum" in contract and value not in contract["enum"]:
                return False
            if "pattern" in contract and (
                not isinstance(value, str)
                or re.fullmatch(contract["pattern"], value) is None
            ):
                return False
            if "minimum" in contract and value < contract["minimum"]:
                return False
            if "maximum" in contract and value > contract["maximum"]:
                return False
            return True

        required = set(schema["required"])
        if (
            schema.get("additionalProperties") is not False
            or set(raw) != required
            or any(
                not schema_matches(raw[name], schema["properties"][name])
                for name in required
            )
        ):
            raise ValueError("raw event does not match the frozen schema")

        def decode_base64url(value):
            if (
                not isinstance(value, str)
                or re.fullmatch(r"[A-Za-z0-9_-]*", value) is None
            ):
                raise ValueError("invalid base64url")
            padded = value + ("=" * (-len(value) % 4))
            decoded = base64.b64decode(padded, altchars=b"-_", validate=True)
            canonical = base64.urlsafe_b64encode(decoded).rstrip(b"=").decode()
            if canonical != value:
                raise ValueError("noncanonical base64url")
            return decoded

        export_name = decode_base64url(raw["export_name_b64"]).decode()
        decode_base64url(raw["target_fh"])
        tag_bytes = decode_base64url(raw["compound_tag_b64"])
        tag = tag_bytes.decode()

        def canonical_uint_string(value):
            if (
                not isinstance(value, str)
                or not value
                or (value != "0" and (value[0] == "0" or not value.isdigit()))
                or (value == "0" and not value.isdigit())
            ):
                raise ValueError("expected canonical unsigned decimal string")
            return int(value)

        for name in ("export_id", "requested_count", "returned_bytes", "nfs_status"):
            if type(raw[name]) is not int or raw[name] < 0:
                raise ValueError("expected unsigned integer")
        expected_origin = (
            "controlled_demand" if manifest.tag_mode == "pc1:d" else "unknown"
        )
        if (
            raw["schema"] != "nfs4_read_v2"
            or raw["identity_valid"] is not True
            or raw["origin_claim"] != expected_origin
            or raw["origin_trust"] != "client_asserted"
            or raw["compound_tag_truncated"] is not False
            or raw["compound_tag_original_length"] != len(tag_bytes)
            or raw["compound_tag_id"] != hashlib.sha256(tag_bytes).hexdigest()
        ):
            raise ValueError("raw event trust or tag proof is invalid")
        expected = {
            "event_id": raw["local_id"],
            "producer_epoch": raw["producer_epoch"],
            "producer_sequence": canonical_uint_string(raw["producer_sequence"]),
            "server_operation_id": raw["operation_id"],
            "sink_time_ns": canonical_uint_string(raw["sink_time_ns"]),
            "volume_id": raw["volume_id"],
            "file_id": raw["file_id"],
            "export_id": raw["export_id"],
            "export_name": export_name,
            "offset": canonical_uint_string(raw["requested_offset"]),
            "count": raw["requested_count"],
            "returned_bytes": raw["returned_bytes"],
            "nfs_status": raw["nfs_status"],
            "outcome": raw["outcome"],
            "eof": raw["eof"],
            "tag": tag,
        }
        actual = {name: getattr(event, name) for name in expected}
        if actual != expected:
            raise RuntimeError("provider sidecar fields differ from hashed raw record")
        event_time = datetime.fromtimestamp(
            canonical_uint_string(raw["event_time_ns"]) / 1_000_000_000,
            UTC,
        )
        sink_time = datetime.fromtimestamp(
            canonical_uint_string(raw["sink_time_ns"]) / 1_000_000_000,
            UTC,
        )
        if (
            not manifest.start_time <= event_time < manifest.end_time
            or event_time > sink_time
        ):
            raise ValueError("raw event time is outside the approved run ordering")
    except (
        FileNotFoundError,
        IndexError,
        KeyError,
        OSError,
        TypeError,
        ValueError,
    ) as error:
        raise RuntimeError("provider raw source is unavailable") from error
    return raw


def build_operation(
    manifest: CollectionManifest,
    planned: tuple[int, FixtureEntry, int, int, str],
    *,
    offered_at: datetime,
    monotonic_ns: int,
    timing_mode: Literal["wallclock", "simulated"],
) -> OfferedOperation:
    index, fixture, offset, count, identity = planned
    tag = (
        f"pc1:d:{manifest.run_ref}:{identity[:32]}:{identity[32:]}"
        if manifest.tag_mode == "pc1:d"
        else ""
    )
    return OfferedOperation(
        operation_id=identity,
        operation_index=index,
        run_id=manifest.run_id,
        fixture_id=fixture.fixture_id,
        offered_at_utc=offered_at,
        offered_at_monotonic_ns=monotonic_ns,
        volume_id=fixture.volume_id,
        file_id=fixture.file_id,
        dir_id=fixture.dir_id,
        export_id=fixture.export_id,
        relative_path=fixture.relative_path,
        file_size=fixture.size_bytes,
        offset=offset,
        count=count,
        client_id=manifest.client_id,
        tag=tag,
        timing_mode=timing_mode,
    )


class CollectionRunner:
    def __init__(
        self,
        *,
        now: Callable[[], datetime] | None = None,
        monotonic_ns: Callable[[], int] | None = None,
        sleep: Callable[[float], None] | None = None,
    ) -> None:
        self.now = now or (lambda: datetime.now(UTC))
        self.monotonic_ns = monotonic_ns or time.monotonic_ns
        self.sleep = sleep or time.sleep

    def run(
        self,
        manifest: CollectionManifest,
        catalog: FixtureCatalog,
        evidence_dir: Path,
        executor: ReadExecutor,
        *,
        simulated_timing: bool = False,
        checkpoint_provider: CheckpointProvider | None = None,
        persistent_stop: Callable[[], str | None] | None = None,
        operation_limit: int | None = None,
        validation_mode: bool = False,
    ) -> int:
        if sha256(evidence_dir / "cluster-provenance-v1.json") != (
            manifest.cluster_manifest_sha256
        ):
            raise RuntimeError("cluster provenance checksum mismatch")
        if sha256(evidence_dir / "event-schema-v2.json") != manifest.event_schema_sha256:
            raise RuntimeError("event schema checksum mismatch")
        if sha256(evidence_dir / "budget-evidence-v1.json") != (
            manifest.budget_evidence_sha256
        ):
            raise RuntimeError("budget evidence checksum mismatch")
        ledger = evidence_dir / "offered.jsonl"
        results = evidence_dir / "execution.jsonl"
        existing = read_models(ledger, OfferedOperation) if ledger.exists() else []
        completed_results = read_models(results, ExecutionResult) if results.exists() else []
        expected = planned_operations(manifest, catalog)
        if operation_limit is not None:
            if operation_limit < 1 or operation_limit > 3:
                raise ValueError("bounded validation permits one to three operations")
            expected = expected[:operation_limit]
        live_mount = isinstance(executor, DirectMountExecutor)
        if live_mount and checkpoint_provider is None:
            raise ValueError("controlled mount execution requires a checkpoint provider")
        if live_mount and existing:
            raise RuntimeError("controlled mount restart is ambiguous; start a new run")
        if checkpoint_provider is not None and existing:
            raise RuntimeError("supervised restart is ambiguous; start a new run")
        if live_mount and not executor.allow_non_nfs_for_test:
            expected_offsets = (0, 12288, 24576, 36864, 49152)
            if (
                len(catalog.fixtures) != 60
                or any(fixture.size_bytes != 65536 for fixture in catalog.fixtures)
                or manifest.offered_count != 300
                or abs(manifest.offered_rate_per_s - (1 / 60)) > 1e-12
                or manifest.range_bytes != (4096,)
                or manifest.range_offsets != expected_offsets
                or manifest.tag_mode != "empty"
            ):
                raise ValueError("direct mount execution requires the authorized live profile")
        if len(existing) > len(expected):
            raise ValueError("ledger contains more operations than the manifest")
        if len(existing) != len(completed_results):
            raise RuntimeError("ambiguous crash state: offered operation lacks durable result")
        for actual, planned in zip(existing, expected, strict=False):
            if actual.operation_id != planned[4] or actual.operation_index != planned[0]:
                raise ValueError("ledger identity mismatch on restart")
        for operation, result in zip(existing, completed_results, strict=True):
            if operation.operation_id != result.operation_id:
                raise ValueError("execution result identity mismatch on restart")
            if not manifest.start_time <= operation.offered_at_utc <= manifest.end_time:
                raise ValueError("ledger timestamp falls outside the frozen run window")
            if result.status != "ok":
                raise RuntimeError(f"prior foreground read error for {operation.operation_id}")
            if result.latency_ms > manifest.stop_rules.latency_guardrail_ms:
                raise RuntimeError(f"prior latency guardrail breach for {operation.operation_id}")
        interval = 1.0 / manifest.offered_rate_per_s
        completed = len(existing)
        schedule_anchor_wall = self.now()
        schedule_anchor_monotonic_ns = self.monotonic_ns()
        checkpoints: list[Checkpoint] = []
        checkpoint_path = evidence_dir / "checkpoints.jsonl"
        health_path = evidence_dir / "pre-read-health.jsonl"
        scheduled = [
            manifest.start_time
            + timedelta(seconds=manifest.checkpoint_interval_s * index)
            for index in range(
                int(
                    (manifest.end_time - manifest.start_time).total_seconds()
                    / manifest.checkpoint_interval_s
                )
                + 1
            )
        ]
        next_checkpoint = 0
        durable_events: list[CollectorDurableEvent] = []
        canonical_observed: list[ObservedEvent] = []

        def stop_if_requested() -> None:
            if persistent_stop is not None:
                reason = persistent_stop()
                if reason:
                    raise RuntimeError(f"persistent stop requested: {reason}")

        def collect_due(through: datetime) -> None:
            nonlocal next_checkpoint
            if checkpoint_provider is None:
                return
            while next_checkpoint < len(scheduled) and scheduled[next_checkpoint] <= through:
                stop_if_requested()
                due = scheduled[next_checkpoint]
                records = checkpoint_provider.collect(due)
                if not records:
                    raise RuntimeError(f"missing checkpoint for {due.isoformat()}")
                for record in records:
                    if record.scheduled_at != due:
                        raise RuntimeError("checkpoint schedule identity mismatch")
                    append_model(checkpoint_path, record)
                checkpoints.extend(records)
                reasons = checkpoint_stop_reasons(
                    manifest,
                    checkpoints,
                    evidence_dir,
                    require_complete=False,
                )
                if reasons:
                    raise RuntimeError("; ".join(reasons))
                next_checkpoint += 1

        for planned in expected[completed:]:
            target_wall = manifest.start_time + timedelta(
                seconds=planned[0] * interval
            )
            if simulated_timing:
                offered_at = target_wall
            else:
                target_monotonic_ns = schedule_anchor_monotonic_ns + int(
                    (target_wall - schedule_anchor_wall).total_seconds()
                    * 1_000_000_000
                )
                remaining_s = (
                    target_monotonic_ns - self.monotonic_ns()
                ) / 1_000_000_000
                if remaining_s > 0:
                    self.sleep(remaining_s)
                actual_monotonic_ns = self.monotonic_ns()
                offered_at = self.now()
                lateness_s = (
                    actual_monotonic_ns - target_monotonic_ns
                ) / 1_000_000_000
                if lateness_s > manifest.operation_schedule_tolerance_s:
                    raise RuntimeError(
                        f"operation schedule overrun at index {planned[0]}: "
                        f"{lateness_s:.6f}s late"
                    )
            if not manifest.start_time <= offered_at <= manifest.end_time:
                raise RuntimeError("operation timestamp falls outside the frozen run window")
            collect_due(offered_at)
            stop_if_requested()
            operation = build_operation(
                manifest,
                planned,
                offered_at=offered_at,
                monotonic_ns=self.monotonic_ns(),
                timing_mode="simulated" if simulated_timing else "wallclock",
            )
            if checkpoint_provider is not None:
                supervised_started_ns = self.monotonic_ns()
                health_requested_at = (
                    operation.offered_at_utc if simulated_timing else self.now()
                )
                health_records = checkpoint_provider.health(operation)
                probe_latency_ms = (
                    self.monotonic_ns() - supervised_started_ns
                ) / 1_000_000
                if probe_latency_ms > manifest.pre_read_plus_read_guardrail_ms:
                    raise RuntimeError(
                        f"pre-read probe guardrail breached for {operation.operation_id}"
                    )
                if not health_records:
                    raise RuntimeError("missing pre-read health barrier")
                checkpoint_identities = {
                    (
                        item.pod_name,
                        item.pod_uid,
                        item.node_name,
                        item.producer_epoch,
                    )
                    for item in checkpoints
                }
                health_identities = [
                    (
                        item.pod_name,
                        item.pod_uid,
                        item.node_name,
                        item.producer_epoch,
                    )
                    for item in health_records
                ]
                if (
                    len(health_identities) != len(set(health_identities))
                    or set(health_identities) != checkpoint_identities
                ):
                    raise RuntimeError("pre-read health identity set mismatch")
                for health in health_records:
                    append_model(health_path, health)
                    identity = (
                        health.pod_name,
                        health.pod_uid,
                        health.node_name,
                        health.producer_epoch,
                    )
                    if (
                        health.run_id != manifest.run_id
                        or health.operation_id != operation.operation_id
                        or identity not in checkpoint_identities
                    ):
                        raise RuntimeError("pre-read health identity mismatch")
                    if not (
                        operation.offered_at_utc
                        - timedelta(seconds=manifest.clock_skew_tolerance_s)
                        <= health.captured_at
                        <= health_requested_at
                        + timedelta(seconds=manifest.checkpoint_tolerance_s)
                    ):
                        raise RuntimeError("pre-read health is stale or future-dated")
                    if (
                        health.drops.total
                        or health.delivery_clean != 1
                        or health.barrier_ready != 1
                        or not health.collector_sink_healthy
                        or not health.storage_healthy
                        or not health.cluster_healthy
                        or not health.volume_healthy
                        or not health.export_healthy
                        or health.parse_errors
                        or health.identity_mismatches
                        or health.checksum_mismatches
                        or health.foreground_read_errors
                    ):
                        raise RuntimeError("pre-read health barrier failed")
                stop_if_requested()
            append_model(ledger, operation)
            result = executor.read(operation)
            if checkpoint_provider is not None:
                supervised_latency_ms = (
                    self.monotonic_ns() - supervised_started_ns
                ) / 1_000_000
                result = result.model_copy(
                    update={"supervised_latency_ms": supervised_latency_ms}
                )
            append_model(results, result)
            completed += 1
            if result.status != "ok":
                raise RuntimeError(f"foreground read error for {operation.operation_id}")
            if result.latency_ms > manifest.stop_rules.latency_guardrail_ms:
                raise RuntimeError(f"latency guardrail breached for {operation.operation_id}")
            if (
                result.supervised_latency_ms is not None
                and result.supervised_latency_ms
                > manifest.pre_read_plus_read_guardrail_ms
            ):
                raise RuntimeError(
                    "pre-read plus read guardrail breached for "
                    f"{operation.operation_id}"
                )
            if checkpoint_provider is not None:
                provider_events = checkpoint_provider.post_read(operation, result)
                if len(provider_events) != 1:
                    raise RuntimeError("post-read provider must return exactly one event")
                provider_event = provider_events[0]
                sidecar_path = evidence_dir / "durable-events.jsonl"
                sidecar = read_models(sidecar_path, CollectorDurableEvent)
                if (
                    len(sidecar) != len(durable_events) + 1
                    or sidecar[:-1] != durable_events
                    or sidecar[-1] != provider_event
                ):
                    raise RuntimeError("provider durable sidecar/output mismatch")
                _validate_provider_event(
                    evidence_dir,
                    manifest,
                    operation,
                    result,
                    provider_event,
                    set(health_identities),
                )
                event_key = (
                    provider_event.event_id,
                    provider_event.pod_uid,
                    provider_event.producer_epoch,
                    provider_event.producer_sequence,
                )
                if any(
                    (
                        item.event_id,
                        item.pod_uid,
                        item.producer_epoch,
                        item.producer_sequence,
                    )
                    == event_key
                    for item in durable_events
                ):
                    raise RuntimeError("duplicate provider durable event identity")
                durable_events.append(provider_event)
                unresolved = [_observed_from_provider(item) for item in durable_events]
                canonical_observed = correlate_untagged_events(
                    manifest,
                    read_models(ledger, OfferedOperation),
                    unresolved,
                )
        if checkpoint_provider is not None:
            if validation_mode:
                reasons = checkpoint_stop_reasons(
                    manifest,
                    checkpoints,
                    evidence_dir,
                    require_complete=False,
                )
                if reasons:
                    raise RuntimeError("; ".join(reasons))
            else:
                if not simulated_timing:
                    remaining = (manifest.end_time - self.now()).total_seconds()
                    if remaining > 0:
                        self.sleep(remaining)
                collect_due(manifest.end_time)
                reasons = checkpoint_stop_reasons(
                    manifest,
                    checkpoints,
                    evidence_dir,
                    require_complete=True,
                )
                if reasons:
                    raise RuntimeError("; ".join(reasons))
            write_jsonl(evidence_dir / "observed.jsonl", canonical_observed)
        return completed


def checkpoint_stop_reasons(
    manifest: CollectionManifest,
    checkpoints: list[Checkpoint],
    evidence_root: Path | None = None,
    *,
    require_complete: bool = True,
) -> list[str]:
    reasons: list[str] = []
    by_pod: dict[str, list[Checkpoint]] = defaultdict(list)
    for checkpoint in checkpoints:
        if checkpoint.run_id != manifest.run_id:
            reasons.append(f"run identity mismatch: {checkpoint.pod_uid}")
        by_pod[checkpoint.pod_uid].append(checkpoint)
        if (
            checkpoint.captured_at - checkpoint.scheduled_at
        ).total_seconds() > manifest.checkpoint_tolerance_s:
            reasons.append(f"late checkpoint: {checkpoint.pod_uid}")
        if evidence_root is not None:
            for record in (checkpoint.active_jsonl, checkpoint.backup_jsonl):
                if record is None:
                    continue
                path = evidence_root / record.path
                try:
                    resolved = path.resolve(strict=True)
                    if (
                        evidence_root.resolve() not in resolved.parents
                        or path.is_symlink()
                        or not path.is_file()
                    ):
                        raise ValueError("unsafe checkpoint evidence path")
                    if path.stat().st_size != record.size_bytes or sha256(path) != record.sha256:
                        reasons.append(f"checkpoint file checksum mismatch: {checkpoint.pod_uid}")
                except (FileNotFoundError, OSError, ValueError):
                    reasons.append(f"checkpoint file unavailable: {checkpoint.pod_uid}")
        if checkpoint.drops.total:
            reasons.append(f"drop observed: {checkpoint.pod_uid}")
        if checkpoint.delivery_clean != 1 or checkpoint.barrier_ready != 1:
            reasons.append(f"delivery barrier not ready: {checkpoint.pod_uid}")
        if not checkpoint.collector_sink_healthy or not checkpoint.storage_healthy:
            reasons.append(f"sink or storage fault: {checkpoint.pod_uid}")
        if not all(
            (
                checkpoint.cluster_healthy,
                checkpoint.volume_healthy,
                checkpoint.export_healthy,
            )
        ):
            reasons.append(f"cluster/volume/export unhealthy: {checkpoint.pod_uid}")
        if (
            checkpoint.parse_errors
            or checkpoint.identity_mismatches
            or checkpoint.checksum_mismatches
        ):
            reasons.append(f"parse/identity/checksum mismatch: {checkpoint.pod_uid}")
        if checkpoint.foreground_read_errors:
            reasons.append(f"foreground read error: {checkpoint.pod_uid}")
        if checkpoint.max_observed_latency_ms > manifest.stop_rules.latency_guardrail_ms:
            reasons.append(f"latency guardrail breached: {checkpoint.pod_uid}")
    if not by_pod:
        reasons.append("checkpoint evidence is missing")
    expected_times = [
        manifest.start_time + timedelta(seconds=manifest.checkpoint_interval_s * index)
        for index in range(
            0,
            int(
                (manifest.end_time - manifest.start_time).total_seconds()
                / manifest.checkpoint_interval_s
            )
            + 1,
        )
    ]
    for pod_uid, pod_checkpoints in by_pod.items():
        ordered = sorted(pod_checkpoints, key=lambda item: item.captured_at)
        if require_complete and [item.scheduled_at for item in ordered] != expected_times:
            reasons.append(f"incomplete checkpoint coverage: {pod_uid}")
        epochs = {item.producer_epoch for item in ordered}
        if len(epochs) != 1:
            reasons.append(f"epoch mismatch: {pod_uid}")
        for before, after in zip(ordered, ordered[1:]):
            if after.scheduled_at <= before.scheduled_at:
                reasons.append(f"checkpoint schedule is not monotonic: {pod_uid}")
            if (after.scheduled_at - before.scheduled_at).total_seconds() != (
                manifest.checkpoint_interval_s
            ):
                reasons.append(f"checkpoint cadence mismatch: {pod_uid}")
            if after.restart_count > before.restart_count and not after.restart_explained:
                reasons.append(f"unexplained restart: {pod_uid}")
            if after.last_enqueued_sequence < before.last_enqueued_sequence:
                reasons.append(f"enqueued sequence reset: {pod_uid}")
            if after.last_synced_sequence < before.last_synced_sequence:
                reasons.append(f"synced sequence reset: {pod_uid}")
    return sorted(set(reasons))


def reconcile(
    manifest_path: Path,
    ledger_path: Path,
    observed_path: Path,
    checkpoints: list[Checkpoint],
    collector_files: CollectorFileManifest,
    evidence_root: Path | None = None,
) -> ReconciliationReport:
    manifest = CollectionManifest.model_validate_json(manifest_path.read_text())
    offered = read_models(ledger_path, OfferedOperation)
    observed = read_models(observed_path, ObservedEvent)
    if manifest.tag_mode == "empty":
        unresolved = [item for item in observed if item.status == "unknown"]
        if unresolved:
            observed = [
                item for item in observed if item.status != "unknown"
            ] + correlate_untagged_events(manifest, offered, unresolved)
    sequence_validation_failed = False
    try:
        _validate_producer_sequences(observed)
    except ValueError:
        sequence_validation_failed = True
    offered_ids = [item.operation_id for item in offered]
    offered_set = set(offered_ids)
    offered_by_id = {item.operation_id: item for item in offered}
    durable = [item.operation_id for item in observed if item.status == "durable"]
    durable_counts = Counter(durable)
    duplicate_ids = tuple(sorted(key for key, count in durable_counts.items() if count > 1))
    durable_set = set(durable)
    exclusions: dict[str, list[str]] = defaultdict(list)
    for item in observed:
        if item.status == "excluded" and item.operation_id is not None:
            exclusions[item.exclusion_reason].append(item.operation_id)
    explained = {
        identity
        for reason, identities in exclusions.items()
        if reason in manifest.allowed_exclusion_reasons
        for identity in identities
    }
    missing = tuple(sorted(offered_set - durable_set - explained))
    excluded_ids = {identity for identities in exclusions.values() for identity in identities}
    unexpected = tuple(sorted((durable_set | excluded_ids) - offered_set))
    unknown = tuple(sorted(item.event_id for item in observed if item.status == "unknown"))
    failures = []
    if sequence_validation_failed:
        failures.append("observed producer sequence validation failed")
    if collector_files.run_id != manifest.run_id:
        failures.append("collector source manifest run identity mismatch")
    checkpoint_sources = {
        (item.pod_name, item.pod_uid, item.node_name, item.producer_epoch)
        for item in checkpoints
    }
    envelope_sources = {
        (item.pod_name, item.pod_uid, item.node_name, item.producer_epoch)
        for item in collector_files.files
    }
    if checkpoint_sources != envelope_sources:
        failures.append("checkpoint/source envelope pod identity mismatch")
    if evidence_root is not None:
        for source in collector_files.files:
            path = evidence_root / source.raw_jsonl_path
            try:
                resolved = path.resolve(strict=True)
                if (
                    evidence_root.resolve() not in resolved.parents
                    or path.is_symlink()
                    or not path.is_file()
                    or sha256(path) != source.sha256
                ):
                    failures.append("collector source file checksum mismatch")
            except (FileNotFoundError, OSError, ValueError):
                failures.append("collector source file unavailable")
    if len(offered_ids) != manifest.offered_count or len(offered_ids) != len(offered_set):
        failures.append("offered ledger count or uniqueness mismatch")
    if duplicate_ids:
        failures.append("duplicate durable operation IDs")
    if missing:
        failures.append("missing durable operation IDs")
    if unexpected:
        failures.append("unexpected durable operation IDs")
    if unknown:
        failures.append("unknown/unmatched observed events")
    for item in observed:
        if not manifest.start_time <= item.durable_at <= manifest.end_time:
            failures.append("observed event timestamp outside the frozen run window")
        if (
            item.operation_id is not None
            and item.operation_id in offered_by_id
            and item.durable_at < offered_by_id[item.operation_id].offered_at_utc
        ):
            failures.append("observed event predates its offered operation")
        if item.operation_id is not None and item.operation_id in offered_by_id:
            operation = offered_by_id[item.operation_id]
            if (item.volume_id, item.file_id, item.offset, item.count) != (
                operation.volume_id,
                operation.file_id,
                operation.offset,
                operation.count,
            ):
                failures.append("observed event range identity mismatch")
    dispositions = Counter(
        item.operation_id for item in observed if item.status in ("durable", "excluded")
    )
    if any(count != 1 for count in dispositions.values()):
        failures.append("operation has duplicate or contradictory dispositions")
    invalid_exclusions = sorted(set(exclusions) - set(manifest.allowed_exclusion_reasons))
    if invalid_exclusions:
        failures.append("unapproved exclusion reasons")
    failures.extend(checkpoint_stop_reasons(manifest, checkpoints, evidence_root))
    checkpoint_epochs = {(item.pod_uid, item.producer_epoch) for item in checkpoints}
    for item in observed:
        if (item.pod_uid, item.producer_epoch) not in checkpoint_epochs:
            failures.append("observed event pod/epoch lacks checkpoint evidence")
        if manifest.tag_mode == "pc1:d" and item.operation_id is not None:
            expected_tag = (
                f"pc1:d:{manifest.run_ref}:"
                f"{item.operation_id[:32]}:{item.operation_id[32:]}"
            )
            if item.tag != expected_tag:
                failures.append("observed event correlation tag mismatch")
        if manifest.tag_mode == "empty" and item.tag not in (None, ""):
            failures.append("observed event tag must be empty")
    pod_epochs: dict[str, set[str]] = defaultdict(set)
    for checkpoint in checkpoints:
        pod_epochs[checkpoint.pod_uid].add(checkpoint.producer_epoch)
    final_synced: dict[tuple[str, str], int] = {}
    for checkpoint in checkpoints:
        key = (checkpoint.pod_uid, checkpoint.producer_epoch)
        final_synced[key] = max(
            final_synced.get(key, 0),
            checkpoint.last_synced_sequence,
        )
    for event in observed:
        if event.producer_sequence > final_synced.get(
            (event.pod_uid, event.producer_epoch),
            -1,
        ):
            failures.append("observed producer sequence exceeds checkpoint watermark")
    return ReconciliationReport(
        run_id=manifest.run_id,
        manifest_sha256=sha256(manifest_path),
        ledger_sha256=sha256(ledger_path),
        observed_sha256=sha256(observed_path),
        offered_count=len(offered),
        unique_durable_count=len(durable_set),
        duplicate_operation_ids=duplicate_ids,
        missing_operation_ids=missing,
        unexpected_operation_ids=unexpected,
        unknown_event_ids=unknown,
        exclusions={key: tuple(sorted(value)) for key, value in sorted(exclusions.items())},
        pod_epochs={key: tuple(sorted(value)) for key, value in sorted(pod_epochs.items())},
        success=not failures,
        failures=tuple(sorted(set(failures))),
    )
