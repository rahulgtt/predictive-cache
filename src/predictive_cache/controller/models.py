"""Verified model staging; activation is explicit at a single runtime's tick boundary."""

from __future__ import annotations

import hashlib
import math
import os
import re
import shutil
import stat
import tempfile
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType
from typing import Callable, Mapping
from urllib.parse import unquote, urlsplit

import pyarrow as pa
import yaml

from predictive_cache.ml.baselines import FrequencyScorer, ReadDemandFrequencyScorer
from predictive_cache.ml.package import (
    COACCESS_SCHEMA,
    DEFAULT_SERVING_POLICY,
    PACKAGE_FILES,
    PackageError,
    load_package,
)
from predictive_cache.ml.read_demand import (
    READ_DEMAND_FEATURE_SCHEMA_SHA256,
    READ_DEMAND_FEATURE_VERSION,
    READ_DEMAND_SERVING_CONTRACT_SHA256,
    runtime_available_features,
)
from predictive_cache.semantics.read_demand import SEMANTIC_VIEW_ID, SEMANTIC_VIEW_SHA256


@dataclass(frozen=True)
class RuntimeContract:
    """Trusted description of the memory that will supply model inputs."""

    feature_version: str
    serving_contract_sha256: str
    semantic_view_id: str | None = None
    semantic_view_sha256: str | None = None
    feature_schema_sha256: str | None = None
    available_feature_names: frozenset[str] | None = None

    @classmethod
    def read_demand(cls, *, verified_file_size=False, anchor_context=False):
        return cls(
            READ_DEMAND_FEATURE_VERSION,
            READ_DEMAND_SERVING_CONTRACT_SHA256,
            SEMANTIC_VIEW_ID,
            SEMANTIC_VIEW_SHA256,
            READ_DEMAND_FEATURE_SCHEMA_SHA256,
            runtime_available_features(
                verified_file_size=verified_file_size, anchor_context=anchor_context
            ),
        )

    def canonical_read_profile(self) -> bool:
        available = self.available_feature_names
        return available is not None and self == RuntimeContract.read_demand(
            verified_file_size="file_size_bytes" in available,
            anchor_context="coread_from_anchor_24h" in available,
        )

    def loader_options(self):
        return dict(
            expected_feature_version=self.feature_version,
            expected_contract_sha256=self.serving_contract_sha256,
            expected_semantic_view_id=self.semantic_view_id,
            expected_semantic_view_sha256=self.semantic_view_sha256,
            expected_feature_schema_sha256=self.feature_schema_sha256,
            available_feature_names=self.available_feature_names,
        )


@dataclass(frozen=True)
class ModelBundle:
    """One coherent scorer, policy and already-loaded co-read table."""

    model_version: str
    package_id: str | None
    scorer: object = field(repr=False)
    coaccess: pa.Table = field(repr=False)
    serving_policy: Mapping = field(repr=False)
    contract: RuntimeContract
    fallback: bool = False

    def score(self, rows):
        return self.scorer.score(rows)


def fetch_model(uri: str) -> Path:
    """Resolve explicit local packages or exact ClearML model IDs only."""
    parsed = urlsplit(uri)
    if parsed.query or parsed.fragment:
        raise ValueError("invalid model URI")
    if parsed.scheme == "file" and not parsed.netloc:
        path = Path(unquote(parsed.path))
        if not path.is_absolute():
            raise ValueError("invalid local model URI")
        return path
    if (
        parsed.scheme == "clearml"
        and re.fullmatch(r"[0-9a-f]{32}", parsed.netloc)
        and not parsed.path
    ):
        from clearml import InputModel

        path = InputModel(model_id=parsed.netloc).get_local_copy()
        if not path:
            raise ValueError("model unavailable")
        return Path(path)
    raise ValueError("invalid model URI")


class ModelManager:
    """Poll may run on a loader thread; activate() belongs between runtime ticks.

    A fetched package never changes active(). Runtime memory compatibility is
    pinned at first activation, or supplied explicitly by the caller.
    """

    def __init__(
        self,
        pointer: Path,
        *,
        contract: RuntimeContract | None = None,
        fetch: Callable[[str], Path] = fetch_model,
        clock=time.monotonic,
        poll_s=30.0,
        retry_s=300.0,
        cache_dir: Path | None = None,
        cache_max_packages=4,
        cache_max_bytes=128 * 2**20,
    ):
        if not all(
            isinstance(v, (int, float)) and not isinstance(v, bool) and math.isfinite(v) and v > 0
            for v in (poll_s, retry_s)
        ):
            raise ValueError("invalid model polling bounds")
        if (
            type(cache_max_packages) is not int
            or cache_max_packages < 1
            or type(cache_max_bytes) is not int
            or cache_max_bytes < 1
        ):
            raise ValueError("invalid model cache bounds")
        self.cache_dir = Path(cache_dir).absolute() if cache_dir is not None else None
        self.cache_max_packages, self.cache_max_bytes = cache_max_packages, cache_max_bytes
        if self.cache_dir is not None:
            self.cache_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
            status = self.cache_dir.lstat()
            if (
                not stat.S_ISDIR(status.st_mode)
                or status.st_uid != os.geteuid()
                or stat.S_IMODE(status.st_mode) != 0o700
            ):
                raise ValueError("model cache must be an owner-only directory")
        self.pointer, self.contract = Path(pointer), contract
        self.fetch, self.clock = fetch, clock
        self.poll_s, self.retry_s = poll_s, retry_s
        self._lock, self._poll_lock = threading.RLock(), threading.Lock()
        self._active = self._staged = self._staged_package = self._active_package = None
        self._digest = None
        self._next_poll = self._next_retry = float("-inf")
        self._error = None
        self._closed = False

    def _pointer(self):
        try:
            return self._pointer_value()
        except Exception:
            # A repaired pointer is a change, including after an in-flight fetch.
            with self._lock:
                self._digest = None
            raise

    def _pointer_value(self):
        # ConfigMap pointers may be symlinks. Limit bytes before YAML parsing.
        with self.pointer.open("rb") as stream:
            data = stream.read(16385)
        if len(data) > 16384:
            raise ValueError("model pointer too large")
        doc = yaml.safe_load(data)
        if not isinstance(doc, dict) or set(doc) != {"model_uri", "feature_version", "fallback"}:
            raise ValueError("invalid model pointer")
        if not all(isinstance(v, str) and v for v in doc.values()):
            raise ValueError("invalid model pointer")
        if doc["fallback"] not in {"baseline-frequency", "baseline-read-frequency"}:
            raise ValueError("invalid fallback")
        wanted_fallback = (
            "baseline-read-frequency"
            if doc["feature_version"] == READ_DEMAND_FEATURE_VERSION
            else "baseline-frequency"
        )
        if doc["fallback"] != wanted_fallback:
            raise ValueError("fallback differs from feature profile")
        return hashlib.sha256(data).digest(), doc

    def _failure(self, error):
        # No exception payload, URI, credentials or filesystem path escapes.
        reason = "model_load_failed"
        if isinstance(error, PackageError) and error.reason in {
            "contract_mismatch",
            "feature_version_mismatch",
            "semantic_view_mismatch",
            "semantic_hash_mismatch",
            "schema_hash_mismatch",
            "unavailable_feature",
            "semantic_activation_required",
            "feature_profile_required",
        }:
            reason = "contract_mismatch"
        with self._lock:
            self._error = reason
            self._next_retry = self.clock() + self.retry_s
            self._discard_stage()

    def _discard_stage(self):
        if self._staged_package is not None:
            self._staged_package.close()
        self._staged = self._staged_package = None

    def _cache(self, package, options):
        """Publish only verified payloads; never prune or overwrite caller data."""
        root = self.cache_dir
        if root is None:
            return package
        target = root / package.package_id
        if target.exists() or target.is_symlink():
            status = target.lstat()
            if (
                not stat.S_ISDIR(status.st_mode)
                or status.st_uid != os.geteuid()
                or stat.S_IMODE(status.st_mode) != 0o700
            ):
                raise ValueError("invalid cached model")
            for name in PACKAGE_FILES:
                status = (target / name).lstat()
                if (
                    not stat.S_ISREG(status.st_mode)
                    or status.st_nlink != 1
                    or status.st_uid != os.geteuid()
                    or stat.S_IMODE(status.st_mode) != 0o600
                ):
                    raise ValueError("invalid cached model")
            cached = load_package(target, **options)
            if cached.package_id != package.package_id:
                raise ValueError("cached model identity mismatch")
            package.close()
            return cached
        entries = list(root.iterdir())
        used = sum(
            p.stat().st_size
            for entry in entries
            if entry.is_dir() and not entry.is_symlink()
            for p in entry.iterdir()
            if p.is_file() and not p.is_symlink()
        )
        size = sum((package.root / name).stat().st_size for name in PACKAGE_FILES)
        # A full cache is not permission to remove prior models or to disable a valid model.
        if len(entries) >= self.cache_max_packages or used + size > self.cache_max_bytes:
            return package
        with tempfile.TemporaryDirectory(prefix=".model-stage-", dir=root) as temporary:
            stage = Path(temporary)
            for name in PACKAGE_FILES:
                destination = stage / name
                shutil.copyfile(package.root / name, destination)
                destination.chmod(0o600)
            verified = load_package(stage, **options)
            assert verified.package_id == package.package_id
            # rename to a nonempty existing directory fails rather than overwriting it.
            try:
                stage.rename(target)
            except FileExistsError:
                return self._cache(package, options)
        cached = load_package(target, **options)
        package.close()
        return cached

    def poll(self) -> bool:
        """Fetch and stage a verified model, returning whether staging succeeded."""
        if not self._poll_lock.acquire(blocking=False):
            return False
        package = None
        try:
            now = self.clock()
            with self._lock:
                if self._closed or now < self._next_poll:
                    return False
                self._next_poll = now + self.poll_s
            digest, pointer = self._pointer()
            with self._lock:
                if digest == self._digest and (self._error is None or now < self._next_retry):
                    return False
                self._digest = digest
                self._discard_stage()
                contract = self.contract
            if contract is None:
                # READ activation still uses executable canonical hashes/capabilities.
                if pointer["feature_version"] == READ_DEMAND_FEATURE_VERSION:
                    raise PackageError("semantic_activation_required")
                options = {"expected_feature_version": pointer["feature_version"]}
            else:
                if (
                    contract.feature_version == READ_DEMAND_FEATURE_VERSION
                    and not contract.canonical_read_profile()
                ):
                    raise PackageError("semantic_activation_required")
                if pointer["feature_version"] != contract.feature_version:
                    raise PackageError("feature_version_mismatch")
                options = contract.loader_options()
            package = load_package(self.fetch(pointer["model_uri"]), **options)
            if package.serving_policy["lead_s"] != 0:
                raise PackageError("invalid_serving_policy")
            if contract is None:
                contract = RuntimeContract(package.feature_version, package.serving_contract_sha256)
            package = self._cache(package, options)
            coaccess = package.coaccess()
            bundle = ModelBundle(
                package.model_version,
                package.package_id,
                package.scorer,
                coaccess,
                MappingProxyType(dict(package.serving_policy)),
                contract,
            )
            # Ignore a pointer replaced while an old download was in flight.
            if self._pointer()[0] != digest:
                with self._lock:
                    self._digest = None
                raise ValueError("pointer changed during fetch")
            with self._lock:
                if self._closed:
                    return False
                self._staged, self._staged_package = bundle, package
                package = None
                self._error = None
            return True
        except Exception as error:
            self._failure(error)
            return False
        finally:
            if package is not None:
                package.close()
            self._poll_lock.release()

    def activate(self) -> bool:
        """Atomically activate the whole staged bundle; call only between ticks."""
        with self._lock:
            if self._closed or self._staged is None:
                return False
            old = self._active_package
            self._active, self._active_package = self._staged, self._staged_package
            self.contract = self._active.contract
            self._staged = self._staged_package = None
            if old is not None:
                old.close()
            return True

    def active(self) -> ModelBundle | None:
        with self._lock:
            if self._closed:
                return None
            if self._active is not None:
                return self._active
            if self.contract is None:
                return None
            read = self.contract.feature_version == READ_DEMAND_FEATURE_VERSION
            if not read and self.contract.feature_version != "fv1":
                return None
            scorer = ReadDemandFrequencyScorer() if read else FrequencyScorer()
            available = self.contract.available_feature_names
            if read and (
                self.contract
                != RuntimeContract.read_demand(
                    verified_file_size=available is not None and "file_size_bytes" in available,
                    anchor_context=available is not None and "coread_from_anchor_24h" in available,
                )
                or "read_count_24h" not in (available or ())
            ):
                return None
            policy = dict(DEFAULT_SERVING_POLICY, max_items=5, max_bytes=64 * 2**20)
            return ModelBundle(
                "fallback:" + scorer.model_type,
                None,
                scorer,
                pa.Table.from_pylist([], schema=COACCESS_SCHEMA),
                MappingProxyType(policy),
                self.contract,
                True,
            )

    def readiness(self) -> tuple[str, list[str]]:
        with self._lock:
            active = self.active()
            reasons = [self._error] if self._error else []
            if active is None:
                return "not_ready", reasons or ["no_model"]
            if active.fallback:
                reasons.append("fallback_model")
            return ("degraded" if reasons else "ready"), reasons

    def plans_allowed(self) -> bool:
        state, reasons = self.readiness()
        return state != "not_ready" and "contract_mismatch" not in reasons

    def close(self) -> None:
        with self._lock:
            self._closed = True
            self._discard_stage()
            if self._active_package is not None:
                self._active_package.close()
            self._active = self._active_package = None
