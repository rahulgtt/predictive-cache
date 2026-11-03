"""``pcache-model``: inspect, verify and create model packages."""

from pathlib import Path

import click

from .dummy import make_dummy_package
from .package import PackageError, load_package


@click.group()
def main():
    """Inspect and verify model packages (docs/ml/spec.md §5)."""


@main.command()
@click.argument("package", type=click.Path(exists=True, path_type=Path))
@click.option("--feature-version", default="fv1", show_default=True)
@click.option("--contract-sha256", default=None, help="Require this serving-contract hash.")
def verify(package, feature_version, contract_sha256):
    """Run every load-time check; exit non-zero on the first failure."""
    try:
        loaded = load_package(package, feature_version, contract_sha256)
    except PackageError as exc:
        raise click.ClickException(f"invalid package ({exc.reason}): {exc.detail}") from exc
    with loaded:
        click.echo(f"OK {loaded.model_version} ({len(loaded.feature_columns)} features)")


@main.command()
@click.argument("package", type=click.Path(exists=True, path_type=Path))
@click.option("--feature-version", default="fv1", show_default=True)
def inspect(package, feature_version):
    """Verify, then print the package manifest summary."""
    try:
        loaded = load_package(package, feature_version)
    except PackageError as exc:
        raise click.ClickException(f"invalid package ({exc.reason}): {exc.detail}") from exc
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
