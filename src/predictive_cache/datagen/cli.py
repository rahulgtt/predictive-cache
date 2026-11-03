"""Command-line entry points for synthetic dataset development."""

from pathlib import Path

import click

from .config import load_config


@click.group()
def main():
    """Generate synthetic access events (foundation fixture implementation)."""


@main.command()
@click.option("--preset", type=click.Choice(["tiny", "small", "medium"]))
@click.option(
    "--config", "config_path", type=click.Path(exists=True, dir_okay=False, path_type=Path)
)
@click.option("--out", required=True, type=click.Path(path_type=Path))
@click.option("--seed", type=click.IntRange(min=0))
@click.option("--format", "output_format", type=click.Choice(["parquet", "jsonl"]))
@click.option("--overwrite", is_flag=True, help="Replace only a verified generator-owned dataset.")
def generate(preset, config_path, out, seed, output_format, overwrite):
    """Write a synthetic foundation fixture, not production workload evidence."""
    if (preset is None) == (config_path is None):
        raise click.UsageError("provide exactly one of --preset or --config")
    try:
        cfg = load_config(path=config_path, preset=preset, seed=seed, format=output_format)
        from .pipeline import run

        manifest = run(cfg, out, overwrite=overwrite)
    except (ValueError, OSError, TypeError) as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(f"Generated {manifest.dataset_id} at {out}; foundation stubs only.")


@main.command()
@click.argument("directory", type=click.Path(exists=True, file_okay=False, path_type=Path))
def validate(directory):
    """Check schemas and artifact integrity; pattern recovery awaits issue #19."""
    from .validate import validate_dataset

    try:
        result = validate_dataset(directory)
    except (ValueError, OSError, TypeError) as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(
        f"Valid: {result['files']} artifacts, {result['events']} events. "
        "Scope: schemas and artifact integrity; no workload-quality claim."
    )


@main.command()
@click.argument("directory", type=click.Path(exists=True, file_okay=False, path_type=Path))
def describe(directory):
    """Display manifest summary and foundation report."""
    from .schema import Manifest

    try:
        manifest = Manifest.model_validate_json((directory / "manifest.json").read_text())
        report = (directory / "report/summary.md").read_text()
    except (ValueError, OSError) as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(
        f"{manifest.dataset_id}: {manifest.counts.events} events; "
        f"{manifest.data_kind}; seed={manifest.seed}"
    )
    click.echo(report)
