"""``pcache-model``: download, inspect, verify, score and create model packages."""

import hashlib
import json
import subprocess
import zipfile
from pathlib import Path

import click

from predictive_cache.dispatcher.schema import parse_utc_z
from predictive_cache.semantics.read_demand import (
    SEMANTIC_VIEW_ID,
    SEMANTIC_VIEW_SHA256,
)

from .baselines import write_read_demand_frequency_package
from .dummy import make_dummy_package
from .package import PackageError, load_package
from .read_demand import (
    READ_DEMAND_FEATURE_SCHEMA_SHA256,
    READ_DEMAND_FEATURE_VERSION,
    READ_DEMAND_SERVING_CONTRACT_SHA256,
)
from .read_demand_plan import (
    ZERO_ITEMS_EXIT_CODE,
    NoAdmittedItems,
    PlanPublishError,
    publish_read_demand_plan,
)
from .read_demand_schedule import (
    ZERO_ITEMS_EXIT_CODE as ZERO_SCHEDULE_ITEMS_EXIT_CODE,
)
from .read_demand_schedule import (
    NoScheduleItems,
    SchedulePublishError,
    publish_read_demand_schedule,
)
from .snapshots import (
    SnapshotError,
    build_narrow_read_demand_snapshot,
    read_manifest,
    runtime_available_features,
    verify_read_demand_manifest,
)


def _package_manifest(package: Path) -> dict:
    if package.is_dir():
        return json.loads((package / "package.json").read_text())
    with zipfile.ZipFile(package) as archive:
        return json.loads(archive.read("package.json"))


def open_package(package: Path, feature_version: str | None = None, contract_sha256=None,
                 available: set[str] | None = None):
    """Load with the checks the package's own semantic view requires."""
    try:
        manifest = _package_manifest(package)
    except (OSError, KeyError, ValueError, zipfile.BadZipFile) as exc:
        raise PackageError("bad_json", f"package.json: {exc}") from exc
    if manifest.get("semantic_view_id") == SEMANTIC_VIEW_ID:
        from .read_demand import READ_DEMAND_FEATURES

        return load_package(
            package,
            READ_DEMAND_FEATURE_VERSION,
            contract_sha256 or READ_DEMAND_SERVING_CONTRACT_SHA256,
            expected_semantic_view_id=SEMANTIC_VIEW_ID,
            expected_semantic_view_sha256=SEMANTIC_VIEW_SHA256,
            expected_feature_schema_sha256=READ_DEMAND_FEATURE_SCHEMA_SHA256,
            available_feature_names=available if available is not None else set(READ_DEMAND_FEATURES),
        )
    return load_package(package, feature_version or "fv1", contract_sha256)


@click.group()
def main():
    """Inspect and verify model packages (docs/ml/spec.md §5)."""


@main.command()
@click.argument("package", type=click.Path(exists=True, path_type=Path))
@click.option("--feature-version", default=None, help="Default: from the package (fv1 if unset).")
@click.option("--contract-sha256", default=None, help="Require this serving-contract hash.")
@click.option(
    "--snapshot",
    "snapshot_root",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    help="Verify semantic hashes and available features against this snapshot.",
)
def verify(package, feature_version, contract_sha256, snapshot_root):
    """Run every load-time check; exit non-zero on the first failure."""
    try:
        if snapshot_root:
            kwargs = _snapshot_expectations(snapshot_root)
            loaded = load_package(
                package,
                READ_DEMAND_FEATURE_VERSION,
                READ_DEMAND_SERVING_CONTRACT_SHA256,
                **kwargs,
            )
        else:
            loaded = open_package(package, feature_version, contract_sha256)
    except PackageError as exc:
        raise click.ClickException(
            f"invalid package ({exc.reason}): {exc.detail}"
        ) from exc
    except SnapshotError as exc:
        raise click.ClickException(f"invalid snapshot: {exc}") from exc
    with loaded:
        click.echo(f"OK {loaded.model_version} ({len(loaded.feature_columns)} features)")


@main.command()
@click.argument("package", type=click.Path(exists=True, path_type=Path))
@click.option("--feature-version", default=None, help="Default: from the package (fv1 if unset).")
@click.option(
    "--snapshot",
    "snapshot_root",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    help="Verify semantic hashes and available features against this snapshot.",
)
def inspect(package, feature_version, snapshot_root):
    """Verify, then print the package manifest summary."""
    try:
        if snapshot_root:
            kwargs = _snapshot_expectations(snapshot_root)
            loaded = load_package(package, READ_DEMAND_FEATURE_VERSION, **kwargs)
        else:
            loaded = open_package(package, feature_version)
    except PackageError as exc:
        raise click.ClickException(
            f"invalid package ({exc.reason}): {exc.detail}"
        ) from exc
    except SnapshotError as exc:
        raise click.ClickException(f"invalid snapshot: {exc}") from exc
    loaded.close()
    m = loaded.manifest
    click.echo(f"model_version:   {loaded.model_version}")
    click.echo(f"feature_version: {loaded.feature_version}")
    click.echo(f"features:        {', '.join(loaded.feature_columns) or '(none)'}")
    click.echo(f"contract_sha256: {loaded.serving_contract_sha256}")
    click.echo(f"serving_policy:  {m['serving_policy']}")
    click.echo(f"data:            {m.get('data', {})}")
    click.echo(f"created_at:      {m.get('created_at')}")
    if m.get("semantic_view_id"):
        click.echo(f"semantic_view:   {m['semantic_view_id']}")


@main.command()
@click.argument("model_id")
@click.option("--out", default=".", show_default=True, type=click.Path(file_okay=False, path_type=Path))
def download(model_id, out):
    """Download a registered model package from ClearML (needs SDK credentials)."""
    import shutil

    from predictive_cache.tracking import NullTracker

    try:
        local = NullTracker().get_model_copy(model_id)
    except (ValueError, OSError) as exc:
        raise click.ClickException(str(exc)) from exc
    out.mkdir(parents=True, exist_ok=True)
    target = out / f"{model_id}.zip"
    shutil.copyfile(local, target)
    try:
        with open_package(target) as loaded:
            click.echo(f"{target}  {loaded.model_version} ({len(loaded.feature_columns)} features)")
    except PackageError as exc:
        raise click.ClickException(f"downloaded package is invalid ({exc.reason}): {exc.detail}") from exc


def _read_rows(path: Path):
    import pyarrow as pa
    import pyarrow.csv as pcsv
    import pyarrow.json as pjson
    import pyarrow.parquet as pq

    if path.is_dir():
        parts = sorted(path.rglob("*.parquet"))
        if not parts:
            raise click.ClickException(f"{path} has no parquet files")
        return pa.concat_tables([pq.ParquetFile(p).read() for p in parts], promote_options="default")
    if path.suffix == ".parquet":
        return pq.ParquetFile(path).read()
    if path.suffix in (".jsonl", ".ndjson", ".json"):
        return pjson.read_json(path)
    if path.suffix == ".csv":
        return pcsv.read_csv(path)
    raise click.ClickException("rows must be .parquet, .jsonl or .csv (or a parquet directory)")


@main.command()
@click.argument("package", type=click.Path(exists=True, path_type=Path))
@click.option("--rows", "rows_path", required=True, type=click.Path(exists=True, path_type=Path),
              help="Feature rows: .parquet/.jsonl/.csv or a snapshot split directory.")
@click.option("--out", type=click.Path(dir_okay=False, path_type=Path),
              help="Write scored rows (.parquet or .csv); default prints the top rows.")
@click.option("--top-k", default=20, show_default=True, type=int)
def score(package, rows_path, out, top_k):
    """Score feature rows locally with a model package (no server needed)."""
    import pyarrow as pa
    import pyarrow.compute as pc

    rows = _read_rows(rows_path)
    try:
        with open_package(package, available=set(rows.column_names)) as loaded:
            scores = loaded.score(rows)
            model = loaded.model_version
    except PackageError as exc:
        raise click.ClickException(f"invalid package ({exc.reason}): {exc.detail}") from exc
    except ValueError as exc:
        raise click.ClickException(str(exc)) from exc
    keys = [c for c in ("snapshot_id", "t", "volume_id", "file_id", "label") if c in rows.column_names]
    result = rows.select(keys).append_column("score", pa.array(scores, pa.float64()))
    result = result.sort_by([("score", "descending")])
    if out:
        if out.suffix == ".csv":
            import pyarrow.csv as pcsv

            pcsv.write_csv(result, out)
        else:
            import pyarrow.parquet as pq

            pq.write_table(result, out)
        click.echo(f"scored {rows.num_rows} rows with {model} -> {out}")
        return
    click.echo(f"scored {rows.num_rows} rows with {model}; top {min(top_k, rows.num_rows)}:")
    for row in result.slice(0, top_k).to_pylist():
        click.echo("  " + "  ".join(f"{k}={row[k]}" for k in keys if k != "t") + f"  score={row['score']:.4f}")
    if "label" in keys:
        top = result.slice(0, top_k)
        hits = pc.sum(pc.cast(top.column("label"), pa.int64())).as_py() or 0
        click.echo(f"positives in top {top.num_rows}: {hits}")


@main.command()
@click.option("--out", required=True, type=click.Path(file_okay=False, path_type=Path))
@click.option("--value", default=0.5, show_default=True, type=float)
def dummy(out, value):
    """Write a constant-score package for Controller/eval plumbing (not a model)."""
    zip_path = make_dummy_package(out, value=value)
    click.echo(str(zip_path))


@main.command("fake-snapshots")
@click.option("--out", required=True, type=click.Path(file_okay=False, path_type=Path))
@click.option("--seed", default=7, show_default=True, type=int)
@click.option("--days", default=10, show_default=True, type=click.IntRange(min=3))
def fake_snapshots(out, seed, days):
    """Write a schema-valid fake snapshot dir with a planted signal (data_kind: fixture)."""
    from .fixtures import make_fake_snapshots

    train_days, val_days = max(1, days * 6 // 10), max(1, days * 2 // 10)
    try:
        path = make_fake_snapshots(
            out, seed=seed, days=days, split_days=(train_days, train_days + val_days)
        )
    except FileExistsError as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(str(path))


def _snapshot_expectations(snapshot_root: Path | None) -> dict:
    if snapshot_root is None:
        return {}
    manifest = read_manifest(snapshot_root)
    verify_read_demand_manifest(snapshot_root, manifest)
    return {
        "expected_semantic_view_id": SEMANTIC_VIEW_ID,
        "expected_semantic_view_sha256": SEMANTIC_VIEW_SHA256,
        "expected_feature_schema_sha256": READ_DEMAND_FEATURE_SCHEMA_SHA256,
        "available_feature_names": runtime_available_features(manifest),
    }


def _git_source_provenance(
    repo_root: Path | None = None,
) -> tuple[str, str | None]:
    repo_root = repo_root or Path(__file__).resolve().parents[3]
    try:
        revision = subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            cwd=repo_root,
            text=True,
            stderr=subprocess.DEVNULL,
        ).strip()
        tracked = subprocess.run(
            ["git", "diff", "HEAD", "--binary", "--full-index"],
            cwd=repo_root,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
        ).stdout
        untracked = subprocess.run(
            ["git", "ls-files", "--others", "--exclude-standard", "-z"],
            cwd=repo_root,
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
        ).stdout.split(b"\0")
        patch_parts = [tracked]
        for raw_path in sorted(path for path in untracked if path):
            path = raw_path.decode("utf-8")
            diff = subprocess.run(
                [
                    "git",
                    "diff",
                    "--binary",
                    "--full-index",
                    "--no-index",
                    "/dev/null",
                    path,
                ],
                cwd=repo_root,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
            )
            if diff.returncode not in {0, 1}:
                raise subprocess.CalledProcessError(diff.returncode, diff.args)
            patch_parts.append(diff.stdout)
        patch = b"".join(patch_parts)
        source_patch_sha256 = (
            hashlib.sha256(patch).hexdigest() if patch else None
        )
    except (OSError, subprocess.CalledProcessError) as exc:
        raise click.ClickException("cannot determine repository source provenance") from exc
    return revision, source_patch_sha256


@main.command("build-read-demand-snapshot")
@click.option(
    "--clean",
    "clean_root",
    required=True,
    type=click.Path(exists=True, file_okay=False, path_type=Path),
)
@click.option(
    "--source-manifest",
    required=True,
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
)
@click.option("--out", required=True, type=click.Path(path_type=Path))
@click.option("--tick", required=True, help="UTC RFC3339 inference tick.")
@click.option("--max-candidates", default=100, show_default=True, type=click.IntRange(1, 100000))
def build_read_demand_snapshot(clean_root, source_manifest, out, tick, max_candidates):
    """Build the deterministic read-demand-v1 narrow MVP inference snapshot."""
    try:
        manifest = build_narrow_read_demand_snapshot(
            clean_root,
            source_manifest,
            out,
            tick=parse_utc_z(tick),
            max_candidates=max_candidates,
        )
    except (SnapshotError, ValueError, OSError) as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(f"{manifest['dataset_id']} {manifest['candidate_profile_sha256']} {out}")


@main.command("build-read-frequency-package")
@click.option(
    "--snapshot",
    "snapshot_root",
    required=True,
    type=click.Path(exists=True, file_okay=False, path_type=Path),
)
@click.option("--out", required=True, type=click.Path(path_type=Path))
@click.option("--repository-revision", default=None)
@click.option(
    "--source-patch-sha256",
    default=None,
    help="Sealed patch/source-bundle SHA-256 required for a dirty worktree.",
)
@click.option("--min-score", type=float, default=None)
@click.option("--max-items", default=20, show_default=True, type=click.IntRange(min=1))
@click.option("--max-bytes", default=256 * 2**20, show_default=True, type=click.IntRange(min=1))
@click.option("--range-cap", default=16 * 2**20, show_default=True, type=click.IntRange(min=1))
def build_read_frequency_package(
    snapshot_root,
    out,
    repository_revision,
    source_patch_sha256,
    min_score,
    max_items,
    max_bytes,
    range_cap,
):
    """Write the non-trained synthetic baseline-read-frequency package."""
    try:
        detected_revision, detected_patch_sha256 = _git_source_provenance()
        if (
            repository_revision is not None
            and repository_revision != detected_revision
        ):
            raise click.ClickException(
                "--repository-revision does not match the detected HEAD"
            )
        if detected_patch_sha256 is not None and source_patch_sha256 is None:
            raise click.ClickException(
                "dirty worktree requires --source-patch-sha256"
            )
        if detected_patch_sha256 is None and source_patch_sha256 is not None:
            raise click.ClickException(
                "clean worktree cannot declare --source-patch-sha256"
            )
        if source_patch_sha256 != detected_patch_sha256:
            raise click.ClickException(
                "--source-patch-sha256 does not match the canonical worktree patch"
            )
        package = write_read_demand_frequency_package(
            snapshot_root,
            out,
            repository_sha=detected_revision,
            repository_state=(
                "patched" if detected_patch_sha256 is not None else "clean"
            ),
            source_patch_sha256=detected_patch_sha256,
            min_score=min_score,
            max_items=max_items,
            max_bytes=max_bytes,
            range_cap=range_cap,
        )
    except (SnapshotError, PackageError, ValueError, OSError) as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(str(package))


@main.command("publish-read-demand-plan")
@click.option(
    "--snapshot",
    "snapshot_root",
    required=True,
    type=click.Path(exists=True, file_okay=False, path_type=Path),
)
@click.option(
    "--package",
    "package_path",
    required=True,
    type=click.Path(exists=True, path_type=Path),
)
@click.option(
    "--target-map",
    required=True,
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
)
@click.option(
    "--dispatcher-root",
    required=True,
    type=click.Path(exists=True, file_okay=False, path_type=Path),
)
@click.option("--run-id", required=True)
@click.option("--created-at", required=True, help="UTC RFC3339 creation time.")
@click.option("--deadline", required=True, help="UTC RFC3339 deadline.")
@click.option("--expected-repository-revision", required=True)
@click.option("--expected-source-patch-sha256", default=None)
def publish_plan(
    snapshot_root,
    package_path,
    target_map,
    dispatcher_root,
    run_id,
    created_at,
    deadline,
    expected_repository_revision,
    expected_source_patch_sha256,
):
    """Score the snapshot and atomically publish one owner-only schema-v2 READ plan."""
    try:
        published = publish_read_demand_plan(
            snapshot_root,
            package_path,
            target_map,
            dispatcher_root,
            run_id=run_id,
            created_at=parse_utc_z(created_at),
            deadline=parse_utc_z(deadline),
            expected_repository_revision=expected_repository_revision,
            expected_source_patch_sha256=expected_source_patch_sha256,
        )
    except NoAdmittedItems as exc:
        click.echo(str(exc), err=True)
        raise click.exceptions.Exit(ZERO_ITEMS_EXIT_CODE) from exc
    except (PlanPublishError, SnapshotError, ValueError, OSError) as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(
        f"{published.plan_id} {published.prediction_id} "
        f"items={published.item_count} bytes={published.requested_bytes} "
        f"{published.plan_path}"
    )


@main.command("publish-read-demand-schedule")
@click.option(
    "--snapshot",
    "snapshot_root",
    required=True,
    type=click.Path(exists=True, file_okay=False, path_type=Path),
)
@click.option(
    "--package",
    "package_path",
    required=True,
    type=click.Path(exists=True, path_type=Path),
)
@click.option("--out", "out_dir", required=True, type=click.Path(path_type=Path))
@click.option("--prediction-cutoff", required=True, help="Exact UTC validation snapshot tick.")
@click.option("--warm-start-offset-s", default=0, show_default=True, type=click.IntRange(min=0))
@click.option("--warm-spacing-s", default=0, show_default=True, type=click.IntRange(min=0))
def publish_schedule(
    snapshot_root,
    package_path,
    out_dir,
    prediction_cutoff,
    warm_start_offset_s,
    warm_spacing_s,
):
    """Publish one deterministic, handle-free Pass-1 prediction schedule."""
    try:
        published = publish_read_demand_schedule(
            snapshot_root,
            package_path,
            out_dir,
            prediction_cutoff=parse_utc_z(prediction_cutoff),
            warm_start_offset_s=warm_start_offset_s,
            warm_spacing_s=warm_spacing_s,
        )
    except NoScheduleItems as exc:
        click.echo(str(exc), err=True)
        raise click.exceptions.Exit(ZERO_SCHEDULE_ITEMS_EXIT_CODE) from exc
    except (SchedulePublishError, SnapshotError, ValueError, OSError) as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(
        f"{published.schedule_id} items={published.item_count} "
        f"bytes={published.requested_bytes} {published.path}"
    )
