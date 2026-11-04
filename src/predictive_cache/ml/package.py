"""Model package: write, seal and load (docs/ml/spec.md §5).

A package is a directory (also zipped for upload) holding package.json,
model.json, feature_schema.json, coaccess.parquet and smoke.parquet. The
loader verifies it in the spec's order and raises PackageError on the first
failure, so a half-valid package is never used.
"""

from __future__ import annotations

import hashlib
import json
import math
import shutil
import tempfile
import zipfile
import zlib
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from .hashing import canonical_json
from .hashing import file_sha256 as sha256
from .scorer import KEY_COLUMNS, MissingColumnError, Scorer, require_columns, scorer_class

PACKAGE_VERSION = 1
SMOKE_MAX_ROWS = 50
SMOKE_ATOL = 1e-9
RANGE_CAP_MAX = 16 * 2**20  # the manual Warmer's per-invocation cap
PAYLOAD_FILES = ("model.json", "feature_schema.json", "coaccess.parquet", "smoke.parquet")
PACKAGE_FILES = ("package.json", *PAYLOAD_FILES)
UNHASHED_FIELDS = ("package_id", "created_at")

COACCESS_SCHEMA = pa.schema(
    [
        ("from_file_id", pa.string()),
        ("to_file_id", pa.string()),
        ("support", pa.int64()),
        ("n_from", pa.int64()),
        ("conf", pa.float64()),
        ("lift", pa.float64()),
    ]
)

DEFAULT_SERVING_POLICY: dict[str, Any] = {
    "target": "demand",
    "lead_s": 0,
    "horizon_s": 300,
    "tick_s": 60,
    "min_score": None,  # null = no threshold
    "recent_skip_s": 300,
    "cooldown_s": 1800,
    "max_items": 20,
    "max_bytes": 256 * 2**20,
    "range_cap": RANGE_CAP_MAX,
}


class PackageError(Exception):
    """A package failed a load/verification check; ``reason`` is a stable code."""

    def __init__(self, reason: str, detail: str = "") -> None:
        super().__init__(f"{reason}: {detail}" if detail else reason)
        self.reason = reason
        self.detail = detail


@dataclass(frozen=True)
class LoadedPackage:
    root: Path
    package_id: str
    model_type: str
    model_version: str
    feature_version: str
    feature_columns: tuple[str, ...]
    scorer: Scorer
    serving_policy: dict
    serving_contract_sha256: str
    coaccess_path: Path
    manifest: dict
    owns_root: bool = False  # True when the loader extracted a zip into a temp dir

    def close(self) -> None:
        """Delete the extraction directory of a zip-loaded package (no-op for directories)."""
        if self.owns_root:
            shutil.rmtree(self.root, ignore_errors=True)

    def __enter__(self) -> LoadedPackage:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def coaccess(self) -> pa.Table:
        return pq.read_table(self.coaccess_path)

    def score(self, rows: pa.Table) -> np.ndarray:
        require_columns(rows, self.feature_columns)
        return self.scorer.score(rows)


def contract_sha256(config: Mapping[str, Any]) -> str:
    return hashlib.sha256(canonical_json(config).encode()).hexdigest()


def compute_package_id(manifest: Mapping[str, Any]) -> str:
    core = {key: value for key, value in manifest.items() if key not in UNHASHED_FIELDS}
    return hashlib.sha256(canonical_json(core).encode()).hexdigest()[:16]


def validate_serving_policy(policy: Any) -> None:
    """Raise PackageError(invalid_serving_policy) unless ``policy`` is servable in v1."""

    def bad(detail: str) -> PackageError:
        return PackageError("invalid_serving_policy", detail)

    if not isinstance(policy, dict):
        raise bad("serving_policy must be an object")
    missing = sorted(set(DEFAULT_SERVING_POLICY) - set(policy))
    if missing:
        raise bad(f"missing keys: {', '.join(missing)}")
    if policy["target"] not in ("demand", "cold_open"):
        raise bad(f"unknown target {policy['target']!r}")
    if policy["lead_s"] != 0:
        raise bad("v1 serving requires lead_s = 0")
    for key in ("horizon_s", "tick_s", "max_items", "max_bytes", "range_cap"):
        value = policy[key]
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise bad(f"{key} must be a positive integer")
    for key in ("recent_skip_s", "cooldown_s"):
        value = policy[key]
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise bad(f"{key} must be a non-negative integer")
    if policy["range_cap"] > RANGE_CAP_MAX:
        raise bad(f"range_cap exceeds the Warmer cap of {RANGE_CAP_MAX} bytes")
    min_score = policy["min_score"]
    if min_score is not None and (
        isinstance(min_score, bool)
        or not isinstance(min_score, (int, float))
        or not math.isfinite(min_score)
    ):
        raise bad("min_score must be null (no threshold) or a finite number")


def feature_names(feature_schema: Mapping[str, Any]) -> list[str]:
    features = feature_schema.get("features")
    if not isinstance(features, list):
        raise PackageError("bad_feature_schema", "feature_schema.features must be a list")
    names = []
    for entry in features:
        if not isinstance(entry, dict) or not isinstance(entry.get("name"), str):
            raise PackageError("bad_feature_schema", "every feature needs a string name")
        names.append(entry["name"])
    return names


def _write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n")


def _write_parquet(path: Path, table: pa.Table) -> None:
    pq.write_table(table, path, compression="zstd", use_dictionary=False, version="2.6")


def seal_manifest(root: Path, manifest: dict) -> dict:
    """Recompute file hashes and ``package_id`` for the files under ``root``."""
    manifest = dict(manifest)
    manifest["files"] = {name: sha256(root / name) for name in PAYLOAD_FILES}
    manifest["package_id"] = compute_package_id(manifest)
    _write_json(root / "package.json", manifest)
    return manifest


def _zip_dir(root: Path, zip_path: Path) -> None:
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name in PACKAGE_FILES:
            info = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o600 << 16
            archive.writestr(info, (root / name).read_bytes())


def write_package(
    scorer: Scorer,
    feature_schema: Mapping[str, Any],
    coaccess_path: Path | None,
    smoke_rows: pa.Table,
    meta: Mapping[str, Any],
    out_dir: Path,
) -> Path:
    """Write ``<out_dir>/<model_type>-<package_id>/`` and its ``.zip``; return the zip path.

    ``meta`` supplies ``serving_contract`` (``{"config": {...}}``; the sha256 is
    computed here), ``serving_policy``, and optional ``data`` / ``training``.
    ``coaccess_path`` None writes an empty co-access table.
    """
    feature_version = feature_schema.get("feature_version")
    if not isinstance(feature_version, str) or not feature_version:
        raise PackageError("bad_feature_schema", "feature_schema.feature_version is required")
    columns = tuple(scorer.feature_columns)
    unknown = sorted(set(columns) - set(feature_names(feature_schema)))
    if unknown:
        raise PackageError("unknown_feature_column", ", ".join(unknown))
    if len(set(columns)) != len(columns):
        raise PackageError("unknown_feature_column", "feature_columns contain duplicates")
    policy = dict(meta.get("serving_policy", DEFAULT_SERVING_POLICY))
    validate_serving_policy(policy)
    contract = meta.get("serving_contract", {})
    config = contract.get("config") if isinstance(contract, dict) else None
    if not isinstance(config, dict):
        raise PackageError("bad_serving_contract", "serving_contract.config must be an object")

    require_columns(smoke_rows, columns)
    smoke = smoke_rows.slice(0, SMOKE_MAX_ROWS).select([*KEY_COLUMNS, *columns])
    expected = np.asarray(scorer.score(smoke), dtype=np.float64)
    if expected.shape != (smoke.num_rows,):
        raise PackageError("bad_scores", "score() must return one float per row")
    smoke = smoke.append_column("expected_score", pa.array(expected, type=pa.float64()))

    out_dir.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=".package-", dir=out_dir))
    try:
        _write_json(staging / "model.json", scorer.to_payload())
        _write_json(staging / "feature_schema.json", dict(feature_schema))
        if coaccess_path is None:
            _write_parquet(staging / "coaccess.parquet", COACCESS_SCHEMA.empty_table())
        else:
            shutil.copyfile(coaccess_path, staging / "coaccess.parquet")
        _write_parquet(staging / "smoke.parquet", smoke)
        semantic_meta = dict(meta.get("semantic", {}))
        if semantic_meta.get("semantic_view_id") == "read-demand-v1":
            from predictive_cache.semantics.read_demand import (
                SEMANTIC_VIEW_ID,
                SEMANTIC_VIEW_SHA256,
            )

            from .read_demand import (
                READ_DEMAND_FEATURE_SCHEMA_SHA256,
                READ_DEMAND_SERVING_CONTRACT,
                READ_DEMAND_SERVING_CONTRACT_SHA256,
                validate_feature_names,
            )
            from .read_demand import feature_schema as canonical_feature_schema

            if dict(feature_schema) != canonical_feature_schema():
                raise PackageError(
                    "invalid_semantic_view", "read-demand feature schema is not canonical"
                )
            if config != READ_DEMAND_SERVING_CONTRACT:
                raise PackageError(
                    "invalid_semantic_view", "read-demand serving contract is not canonical"
                )
            semantic_meta["semantic_view_id"] = SEMANTIC_VIEW_ID
            semantic_meta["semantic_view_sha256"] = SEMANTIC_VIEW_SHA256
            semantic_meta["feature_schema_sha256"] = (
                READ_DEMAND_FEATURE_SCHEMA_SHA256
            )
            semantic_meta["serving_contract_sha256"] = (
                READ_DEMAND_SERVING_CONTRACT_SHA256
            )
            if policy.get("target") != "demand":
                raise PackageError("invalid_serving_policy", "read-demand requires demand target")
            if feature_version != "read-demand-fv1":
                raise PackageError("invalid_semantic_view", "read-demand requires read-demand-fv1")
            validate_feature_names(columns)
        manifest = {
            "package_version": PACKAGE_VERSION,
            **(
                {
                    "semantic_view_id": semantic_meta["semantic_view_id"],
                    "semantic_view_sha256": semantic_meta.get("semantic_view_sha256"),
                    "feature_schema_sha256": semantic_meta.get(
                        "feature_schema_sha256"
                    ),
                    "serving_contract_sha256": semantic_meta.get(
                        "serving_contract_sha256"
                    ),
                }
                if semantic_meta.get("semantic_view_id")
                else {}
            ),
            "model_type": scorer.model_type,
            "feature_version": feature_version,
            "feature_columns": list(columns),
            "serving_contract": {"config": config, "sha256": contract_sha256(config)},
            "serving_policy": policy,
            **({"data_kind": meta["data_kind"]} if "data_kind" in meta else {}),
            **({"trained": meta["trained"]} if "trained" in meta else {}),
            "data": dict(meta.get("data", {})),
            "training": dict(meta.get("training", {})),
            "created_at": datetime.now(UTC)
            .isoformat(timespec="microseconds")
            .replace("+00:00", "Z"),
        }
        manifest = seal_manifest(staging, manifest)
        name = f"{scorer.model_type}-{manifest['package_id']}"
        final = out_dir / name
        if final.exists():
            shutil.rmtree(final)
        staging.rename(final)
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    zip_path = out_dir / f"{name}.zip"
    _zip_dir(final, zip_path)
    return zip_path


def _extract(zip_path: Path, extract_dir: Path | None) -> Path:
    try:
        archive = zipfile.ZipFile(zip_path)
    except (zipfile.BadZipFile, OSError) as exc:
        raise PackageError("invalid_zip", str(exc)) from exc
    with archive:
        names = archive.namelist()
        unexpected = sorted(set(names) - set(PACKAGE_FILES))
        if unexpected:
            raise PackageError("unexpected_file", ", ".join(unexpected))
        root = Path(tempfile.mkdtemp(prefix="pkg-", dir=extract_dir))
        try:
            for name in names:
                (root / name).write_bytes(archive.read(name))
        except (zipfile.BadZipFile, zlib.error, OSError, EOFError) as exc:
            shutil.rmtree(root, ignore_errors=True)
            raise PackageError("invalid_zip", str(exc)) from exc
    return root


def _read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError) as exc:
        raise PackageError("bad_json", f"{path.name}: {exc}") from exc


def load_package(
    path: Path,
    expected_feature_version: str,
    expected_contract_sha256: str | None = None,
    expected_semantic_view_id: str | None = None, expected_semantic_view_sha256: str | None = None, expected_feature_schema_sha256: str | None = None, available_feature_names: set[str] | None = None,
    *,
    extract_dir: Path | None = None,
) -> LoadedPackage:
    """Verify and load a package directory or zip (spec §5 checks 1-7, in order)."""
    path = Path(path)
    if path.is_dir():
        unexpected = sorted(p.name for p in path.iterdir() if p.name not in PACKAGE_FILES)
        if unexpected:
            raise PackageError("unexpected_file", ", ".join(unexpected))
        return _verify(path, expected_feature_version, expected_contract_sha256, owns_root=False, expected_semantic_view_id=expected_semantic_view_id, expected_semantic_view_sha256=expected_semantic_view_sha256, expected_feature_schema_sha256=expected_feature_schema_sha256, available_feature_names=available_feature_names)
    root = _extract(path, extract_dir)
    try:
        return _verify(root, expected_feature_version, expected_contract_sha256, owns_root=True, expected_semantic_view_id=expected_semantic_view_id, expected_semantic_view_sha256=expected_semantic_view_sha256, expected_feature_schema_sha256=expected_feature_schema_sha256, available_feature_names=available_feature_names)
    except BaseException:
        shutil.rmtree(root, ignore_errors=True)
        raise


def _verify(
    root: Path,
    expected_feature_version: str,
    expected_contract_sha256: str | None,
    *,
    owns_root: bool, expected_semantic_view_id=None, expected_semantic_view_sha256=None, expected_feature_schema_sha256=None, available_feature_names=None,
) -> LoadedPackage:
    # 1. files exist and hashes match
    if not (root / "package.json").is_file():
        raise PackageError("missing_file", "package.json")
    manifest = _read_json(root / "package.json")
    if not isinstance(manifest, dict):
        raise PackageError("bad_json", "package.json must be an object")
    semantic = manifest.get("semantic_view_id")
    if expected_semantic_view_id is not None and semantic != expected_semantic_view_id:
        raise PackageError("semantic_view_mismatch", str(semantic))
    if semantic == "read-demand-v1":
        from .read_demand import (
            READ_DEMAND_FEATURE_SCHEMA_SHA256,
            READ_DEMAND_SERVING_CONTRACT,
            READ_DEMAND_SERVING_CONTRACT_SHA256,
        )
        from .read_demand import feature_schema as canonical_feature_schema

        if available_feature_names is None:
            raise PackageError("feature_profile_required", "read-demand activation requires available feature names")
        if expected_semantic_view_id is None or expected_semantic_view_sha256 is None or expected_feature_schema_sha256 is None:
            raise PackageError("semantic_activation_required", "read-demand activation requires expected semantic ID and hash")
        required = ("semantic_view_sha256", "feature_schema_sha256", "serving_contract_sha256")
        if manifest.get("feature_version") != "read-demand-fv1" or any(
            not manifest.get(key) for key in required
        ):
            raise PackageError("invalid_semantic_view", "missing read-demand semantic keys")
    if expected_semantic_view_sha256 and manifest.get("semantic_view_sha256") != expected_semantic_view_sha256:
        raise PackageError("semantic_hash_mismatch", str(manifest.get("semantic_view_sha256")))
    unavailable = set(manifest.get("feature_columns", ()))
    if available_feature_names is not None and unavailable - set(available_feature_names):
        raise PackageError("unavailable_feature", "package feature is unavailable")
    if semantic == "read-demand-v1":
        if expected_feature_schema_sha256 and manifest.get("feature_schema_sha256") != expected_feature_schema_sha256:
            raise PackageError("schema_hash_mismatch", "feature schema differs from expected snapshot")
        config = manifest.get("serving_contract", {}).get("config", {})
        if expected_feature_schema_sha256 != READ_DEMAND_FEATURE_SCHEMA_SHA256:
            raise PackageError("schema_hash_mismatch", "runtime feature schema is not canonical")
        if manifest.get("feature_schema_sha256") != READ_DEMAND_FEATURE_SCHEMA_SHA256:
            raise PackageError("schema_hash_mismatch", "package feature schema is not canonical")
        if config != READ_DEMAND_SERVING_CONTRACT:
            raise PackageError("invalid_semantic_view", "serving contract is not canonical")
        if (
            manifest.get("serving_contract_sha256")
            != READ_DEMAND_SERVING_CONTRACT_SHA256
        ):
            raise PackageError("semantic_hash_mismatch", "serving contract is not canonical")
        if manifest.get("serving_policy", {}).get("target") != "demand":
            raise PackageError("invalid_serving_policy", "read-demand requires demand target")
    files = manifest.get("files")
    if not isinstance(files, dict) or set(files) != set(PAYLOAD_FILES):
        raise PackageError("missing_file", "manifest.files must list exactly the payload files")
    for name in PAYLOAD_FILES:
        if not (root / name).is_file():
            raise PackageError("missing_file", name)
        if sha256(root / name) != files[name]:
            raise PackageError("hash_mismatch", name)

    if semantic == "read-demand-v1":
        if _read_json(root / "feature_schema.json") != canonical_feature_schema():
            raise PackageError("schema_hash_mismatch", "feature schema payload is not canonical")
        if (
            contract_sha256(manifest.get("serving_contract", {}).get("config", {}))
            != READ_DEMAND_SERVING_CONTRACT_SHA256
        ):
            raise PackageError("semantic_hash_mismatch", "serving contract hash does not recompute")
    # 2. package_id
    if manifest.get("package_id") != compute_package_id(manifest):
        raise PackageError("bad_package_id", str(manifest.get("package_id")))

    # 3. known version, model type and a servable policy
    if manifest.get("package_version") != PACKAGE_VERSION:
        raise PackageError("unknown_package_version", str(manifest.get("package_version")))
    if "trained" in manifest and not isinstance(manifest["trained"], bool):
        raise PackageError("bad_payload", "trained must be boolean")
    if "data_kind" in manifest and manifest["data_kind"] not in {
        "real",
        "synthetic",
        "fixture",
    }:
        raise PackageError("bad_payload", "data_kind is invalid")
    model_type = manifest.get("model_type")
    if not isinstance(model_type, str):
        raise PackageError("unknown_model_type", repr(model_type))
    try:
        cls = scorer_class(model_type)
    except KeyError:
        raise PackageError("unknown_model_type", str(model_type)) from None
    validate_serving_policy(manifest.get("serving_policy"))

    # 4. feature version
    feature_schema = _read_json(root / "feature_schema.json")
    if not isinstance(feature_schema, dict):
        raise PackageError("bad_feature_schema", "feature_schema.json must be an object")
    version = manifest.get("feature_version")
    if version != expected_feature_version or feature_schema.get("feature_version") != version:
        raise PackageError(
            "feature_version_mismatch",
            f"package={version} schema={feature_schema.get('feature_version')} "
            f"expected={expected_feature_version}",
        )

    # 5. serving contract
    contract = manifest.get("serving_contract")
    if (
        not isinstance(contract, dict)
        or not isinstance(contract.get("config"), dict)
        or contract.get("sha256") != contract_sha256(contract["config"])
    ):
        raise PackageError("contract_hash_mismatch", "serving_contract.sha256 does not recompute")
    if expected_contract_sha256 is not None and contract["sha256"] != expected_contract_sha256:
        raise PackageError("contract_mismatch", "package contract differs from the expected one")

    # 6. feature columns exist in the schema
    columns = manifest.get("feature_columns")
    if not isinstance(columns, list) or not all(isinstance(c, str) for c in columns):
        raise PackageError("unknown_feature_column", "feature_columns must be a list of names")
    unknown = sorted(set(columns) - set(feature_names(feature_schema)))
    if unknown or len(set(columns)) != len(columns):
        raise PackageError("unknown_feature_column", ", ".join(unknown) or "duplicates")

    # 7. smoke scores reproduce
    try:
        scorer = cls.from_payload(_read_json(root / "model.json"), feature_schema)
    except (KeyError, TypeError, ValueError) as exc:
        raise PackageError("bad_payload", str(exc)) from exc
    if tuple(scorer.feature_columns) != tuple(columns):
        raise PackageError("bad_payload", "scorer feature_columns differ from the manifest")
    try:
        smoke = pq.read_table(root / "smoke.parquet")
        coaccess_schema = pq.read_schema(root / "coaccess.parquet")
    except (OSError, pa.ArrowException) as exc:
        raise PackageError("bad_payload", str(exc)) from exc
    missing_coaccess = [n for n in COACCESS_SCHEMA.names if n not in coaccess_schema.names]
    if missing_coaccess:
        raise PackageError("bad_payload", f"coaccess.parquet lacks {', '.join(missing_coaccess)}")
    if "expected_score" not in smoke.column_names:
        raise PackageError("smoke_mismatch", "smoke.parquet lacks expected_score")
    try:
        actual = np.asarray(scorer.score(smoke), dtype=np.float64)
    except MissingColumnError as exc:
        raise PackageError("smoke_mismatch", str(exc)) from exc
    expected = smoke.column("expected_score").to_numpy(zero_copy_only=False).astype(np.float64)
    if actual.shape != expected.shape or not np.allclose(
        actual, expected, rtol=0, atol=SMOKE_ATOL, equal_nan=True
    ):
        raise PackageError("smoke_mismatch", "scores do not reproduce expected_score")

    package_id = manifest["package_id"]
    return LoadedPackage(
        root=root,
        package_id=package_id,
        model_type=model_type,
        model_version=f"{model_type}@{package_id}",
        feature_version=version,
        feature_columns=tuple(columns),
        scorer=scorer,
        serving_policy=dict(manifest["serving_policy"]),
        serving_contract_sha256=contract["sha256"],
        coaccess_path=root / "coaccess.parquet",
        manifest=manifest,
        owns_root=owns_root,
    )
