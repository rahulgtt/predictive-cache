"""Command-line entry point for versioned data preparation."""

from __future__ import annotations

import json
from pathlib import Path

import click

from predictive_cache.prep.clean.pipeline import run_clean
from predictive_cache.prep.config import load_config


@click.group()
def main():
    """Prepare trustworthy demand datasets from access events."""


@main.command()
@click.option(
    "--input",
    "input_dir",
    required=True,
    type=click.Path(exists=True, file_okay=False, path_type=Path),
)
@click.option("--out", required=True, type=click.Path(path_type=Path))
@click.option(
    "--config", "config_path", type=click.Path(exists=True, dir_okay=False, path_type=Path)
)
@click.option(
    "--format", "output_format", type=click.Choice(["parquet", "jsonl"]), default="parquet"
)
@click.option("--overwrite", is_flag=True)
def clean(input_dir, out, config_path, output_format, overwrite):
    """Clean a datagen-compatible dataset without reading its truth tables."""
    try:
        manifest = run_clean(
            input_dir, load_config(config_path), out, format=output_format, overwrite=overwrite
        )
    except (ValueError, OSError, TypeError, KeyError) as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(f"Cleaned {manifest['dataset_id']} at {out}")


@main.command()
@click.argument("directory", type=click.Path(exists=True, file_okay=False, path_type=Path))
def describe(directory):
    """Display lineage, counts and quality summary."""
    try:
        manifest = json.loads((directory / "manifest.json").read_text())
        summary = (directory / "report/summary.md").read_text()
    except (ValueError, OSError) as exc:
        raise click.ClickException(str(exc)) from exc
    click.echo(f"{manifest['dataset_id']} from {manifest['parent']['dataset_id']}")
    click.echo(summary)
