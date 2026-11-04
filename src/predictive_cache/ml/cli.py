"""``pcache-model``: inspect, verify and create model packages."""

import hashlib
import subprocess
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
from .snapshots import (
    SnapshotError,
    build_narrow_read_demand_snapshot,
    read_manifest,
    runtime_available_features,
    verify_read_demand_manifest,
)


@click.group()
def main():
    """Inspect and verify model packages (docs/ml/spec.md §5)."""


@main.command()
@click.argument("package", type=click.Path(exists=True, path_type=Path))
@click.option("--feature-version", default="fv1", show_default=True)
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
        kwargs = _snapshot_expectations(snapshot_root)
        if snapshot_root:
            feature_version = READ_DEMAND_FEATURE_VERSION
            contract_sha256 = READ_DEMAND_SERVING_CONTRACT_SHA256
        loaded = load_package(package, feature_version, contract_sha256, **kwargs)
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
@click.option("--feature-version", default="fv1", show_default=True)
@click.option(
    "--snapshot",
    "snapshot_root",
    type=click.Path(exists=True, file_okay=False, path_type=Path),
    help="Verify semantic hashes and available features against this snapshot.",
)
def inspect(package, feature_version, snapshot_root):
    """Verify, then print the package manifest summary."""
    try:
        kwargs = _snapshot_expectations(snapshot_root)
        if snapshot_root:
            feature_version = READ_DEMAND_FEATURE_VERSION
        loaded = load_package(package, feature_version, **kwargs)
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
