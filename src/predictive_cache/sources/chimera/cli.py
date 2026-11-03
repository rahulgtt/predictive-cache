"""Command-line entry point for controlled Chimera v2 imports."""

from __future__ import annotations

from pathlib import Path

import click

from .importer import import_dataset


@click.command()
@click.option(
    "--event-sources", "event_sources_path", type=click.Path(path_type=Path), required=True
)
@click.option("--observed", "observed_path", type=click.Path(path_type=Path), required=True)
@click.option(
    "--run-manifest", "run_manifest_path", type=click.Path(path_type=Path), required=True
)
@click.option(
    "--offered-ledger", "offered_ledger_path", type=click.Path(path_type=Path), required=True
)
@click.option(
    "--fixture-catalog", "fixture_catalog_path", type=click.Path(path_type=Path), required=True
)
@click.option("--checkpoints", "checkpoints_path", type=click.Path(path_type=Path), required=True)
@click.option(
    "--validation-permit",
    "validation_permit_path",
    type=click.Path(path_type=Path),
)
@click.option("--out", "out_dir", type=click.Path(path_type=Path), required=True)
@click.option("--format", "format_", type=click.Choice(["parquet", "jsonl"]), default="parquet")
def main(
    event_sources_path: Path,
    observed_path: Path,
    run_manifest_path: Path,
    offered_ledger_path: Path,
    fixture_catalog_path: Path,
    checkpoints_path: Path,
    validation_permit_path: Path | None,
    out_dir: Path,
    format_: str,
) -> None:
    """Import authoritative nfs4_read_v2 JSONL into the prep source layout."""
    try:
        manifest = import_dataset(
            event_sources_path=event_sources_path,
            observed_path=observed_path,
            run_manifest_path=run_manifest_path,
            offered_ledger_path=offered_ledger_path,
            fixture_catalog_path=fixture_catalog_path,
            checkpoints_path=checkpoints_path,
            validation_permit_path=validation_permit_path,
            out_dir=out_dir,
            format=format_,
        )
    except (OSError, ValueError) as error:
        raise click.ClickException(str(error)) from error
    click.echo(f"Imported {manifest.dataset_id} at {out_dir}")


if __name__ == "__main__":
    main()
