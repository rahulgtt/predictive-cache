"""Command-line entry point for the plan-file dispatcher."""

from __future__ import annotations

import signal
from pathlib import Path

import click

from .core import Dispatcher, DispatcherConfig, DispatcherError
from .securefs import SecureFileError


@click.group()
def main() -> None:
    """Dispatch owner-controlled schema-v2 plans to the Cache Warmer."""


@main.command()
@click.option(
    "--root",
    required=True,
    type=click.Path(exists=True, file_okay=False, path_type=Path),
)
@click.option(
    "--warmer-command",
    required=True,
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
)
@click.option("--host", required=True, help="Numeric IPv4 or IPv6 address.")
@click.option("--port", type=click.IntRange(1, 65535), default=2049, show_default=True)
@click.option("--uid", type=click.IntRange(min=0), default=1002, show_default=True)
@click.option("--gid", type=click.IntRange(min=0), default=1002, show_default=True)
@click.option("--real-reads/--no-real-reads", default=False, show_default=True)
@click.option("--max-plan-bytes", type=click.IntRange(min=1), default=4 * 2**20)
@click.option(
    "--max-output-bytes",
    type=click.IntRange(min=1, max=16 * 2**20),
    default=16 * 2**20,
    show_default=True,
)
@click.option(
    "--max-bytes-per-min",
    type=click.IntRange(min=1),
    default=64 * 2**20,
    show_default=True,
)
@click.option("--max-items-per-min", type=click.IntRange(min=1), default=60, show_default=True)
@click.option("--enable-file", default="enable-real-reads", show_default=True)
@click.option("--stop-file", default="stop", show_default=True)
@click.option("--poll-seconds", type=click.FloatRange(min=0.01), default=1.0, show_default=True)
@click.option("--once", is_flag=True, help="Process current work and exit.")
def run(
    root: Path,
    warmer_command: Path,
    host: str,
    port: int,
    uid: int,
    gid: int,
    real_reads: bool,
    max_plan_bytes: int,
    max_output_bytes: int,
    max_bytes_per_min: int | None,
    max_items_per_min: int | None,
    enable_file: str,
    stop_file: str,
    poll_seconds: float,
    once: bool,
) -> None:
    """Claim committed plans, execute sequential tasks, and persist results."""
    try:
        config = DispatcherConfig(
            root=root.absolute(),
            warmer_command=warmer_command.absolute(),
            host=host,
            port=port,
            uid=uid,
            gid=gid,
            real_reads=real_reads,
            max_plan_bytes=max_plan_bytes,
            max_output_bytes=max_output_bytes,
            max_bytes_per_min=max_bytes_per_min,
            max_items_per_min=max_items_per_min,
            enable_file=enable_file,
            stop_file=stop_file,
            poll_seconds=poll_seconds,
        )
        with Dispatcher(config) as dispatcher:
            previous: dict[int, object] = {}

            def stop(_signum: int, _frame: object) -> None:
                dispatcher.request_stop()

            for signum in (signal.SIGTERM, signal.SIGINT):
                previous[signum] = signal.signal(signum, stop)
            try:
                if once:
                    dispatcher.run_once()
                else:
                    dispatcher.run()
            finally:
                for signum, handler in previous.items():
                    signal.signal(signum, handler)
    except (DispatcherError, SecureFileError, OSError, ValueError) as exc:
        raise click.ClickException(str(exc)) from exc
