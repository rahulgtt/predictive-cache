"""Ingestion-only Controller commands; model/store integration is deferred."""

from __future__ import annotations

import json
import os
import time
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path
from typing import Literal

import click
import yaml
from pydantic import BaseModel, ConfigDict, Field

from predictive_cache.controller.http import HttpSource
from predictive_cache.controller.ingest import Ingestor
from predictive_cache.controller.pass2_runtime import (
    Pass2RuntimeConfig,
    Pass2RuntimeError,
    Pass2Scheduler,
)
from predictive_cache.controller.sources import RealClock, ReplayClock, ReplaySource
from predictive_cache.dispatcher.schema import parse_utc_z
from predictive_cache.dispatcher.securefs import SecureFileError
from predictive_cache.prep.config import CleanConfig


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")


class _HTTP(_Strict):
    max_body_mib: int = Field(default=4, ge=1, le=4)
    max_events: int = Field(default=10000, ge=1, le=10000)
    max_concurrent: int = Field(default=8, ge=1, le=8)
    read_timeout_s: float = Field(default=2, gt=0, le=2)
    token_env: str = "PCACHE_INGEST_TOKEN"


class _Replay(_Strict):
    dataset: str
    batch_size: int = Field(default=1000, ge=1, le=10000)


class _Ingest(_Strict):
    source: Literal["http", "replay"]
    listen: str = "127.0.0.1:8090"
    queue_max: int = Field(default=200000, ge=1)
    allowed_volumes: list[str] = Field(min_length=1)
    http: _HTTP = Field(default_factory=_HTTP)
    replay: _Replay | None = None


class _Controller(_Strict):
    ingest: _Ingest


class _Config(_Strict):
    clean: CleanConfig = Field(default_factory=CleanConfig)
    controller: _Controller


def _read_config(path):
    try:
        return _Config.model_validate(yaml.safe_load(path.read_text()))
    except (ValueError, TypeError, OSError, yaml.YAMLError):
        raise click.ClickException("invalid ingestion configuration") from None


def _summary(ingestor, updates):
    # Never print rows, paths, identifiers, source exceptions or credentials.
    stats = ingestor.stats()
    counters = {
        key: value
        for key, value in stats.items()
        if isinstance(value, int) and not isinstance(value, bool)
    }
    reason = stats.get("tracker_failure_reason")
    if reason is not None and reason not in {"lifecycle_conflict", "capacity"}:
        reason = "other"
    return {
        "counters": counters,
        "updates": dict(sorted(updates.items())),
        "tracker_exhausted": stats["tracker_exhausted"],
        "tracker_failed": stats.get("tracker_failed", False),
        "tracker_failure_reason": reason,
        "excluded_by_reason": stats["excluded_by_reason"],
        "degraded": bool(
            stats["tracker_exhausted"]
            or stats.get("tracker_failed", False)
            or stats.get("dropped", 0)
            or stats.get("consumer_errors", 0)
        ),
    }


@click.group()
def main():
    """Receive/replay events only. No model inference, planning or FeatureStore."""


@main.command()
@click.option("--config", "config_path", required=True, type=click.Path(path_type=Path))
def run(config_path):
    """Run ingestion only, from an explicit HTTP or deterministic replay source."""
    cfg = _read_config(config_path)
    settings = cfg.controller.ingest
    updates = Counter()

    def sink(update):
        kind = update.get("kind")
        if kind in {"OpenObserved", "OpenCompleted", "MetaChanged"}:
            updates[kind] += 1

    ingestor = Ingestor(
        cfg.clean, sink, queue_max=settings.queue_max, allowed_volumes=settings.allowed_volumes
    )
    try:
        if settings.source == "replay":
            if settings.replay is None:
                raise ValueError("missing replay configuration")
            source = ReplaySource(
                Path(settings.replay.dataset), batch_size=settings.replay.batch_size
            )
            clock = ReplayClock(source.period_start, speed=0)
            source.clock = clock

            def offer(events):
                result = ingestor.offer(events, replay=True)
                # Strictly earlier updates only: ties across batches stay pending.
                now = datetime.fromisoformat(events[-1]["ingest_time"].replace("Z", "+00:00"))
                ingestor.drain(now)
                return result

            try:
                while not source.completed:
                    clock.advance(max(clock.now(), source.next_ingest_time))
                    source.emit_ready(offer)
                ingestor.drain(
                    max(clock.now(), source.period_end)
                    + timedelta(
                        seconds=cfg.clean.handle_timeout_s + cfg.clean.completion_grace_s + 1
                    )
                )
            finally:
                source.stop()
        else:
            host, separator, port = settings.listen.rpartition(":")
            if not separator or not host:
                raise ValueError("invalid listen address")
            options = settings.http
            clock = RealClock()
            source = HttpSource(
                (host, int(port)),
                token=os.environ.get(options.token_env, ""),
                max_body_bytes=options.max_body_mib * 1024 * 1024,
                max_events=options.max_events,
                max_concurrent=options.max_concurrent,
                read_timeout_s=options.read_timeout_s,
            )
            source.start(ingestor.offer)
            click.echo("ingestion receiver started")
            try:
                while True:
                    ingestor.drain(clock.now())
                    time.sleep(0.05)
            except KeyboardInterrupt:
                pass
            finally:
                source.stop()
                ingestor.drain(clock.now())
    except Exception:
        raise click.ClickException("ingestion failed; check configuration and source") from None
    click.echo(json.dumps(_summary(ingestor, updates), sort_keys=True))


@main.command("run-pass2")
@click.option(
    "--schedule",
    "schedule_path",
    required=True,
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
)
@click.option(
    "--target-map",
    "target_map_path",
    required=True,
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
)
@click.option(
    "--dispatcher-root",
    required=True,
    type=click.Path(exists=True, file_okay=False, path_type=Path),
)
@click.option("--run-id", required=True)
@click.option("--run-start", required=True, help="Explicit Pass-2 UTC start time.")
@click.option(
    "--publish-lead-seconds",
    type=click.IntRange(min=0, max=299),
    default=5,
    show_default=True,
)
@click.option(
    "--plan-ttl-seconds",
    type=click.IntRange(min=1, max=300),
    default=120,
    show_default=True,
)
@click.option(
    "--max-bytes-per-min",
    type=click.IntRange(min=1),
    default=64 * 2**20,
    show_default=True,
)
@click.option(
    "--max-items-per-min",
    type=click.IntRange(min=1),
    default=60,
    show_default=True,
)
@click.option(
    "--poll-seconds",
    type=click.FloatRange(min=0.01),
    default=0.1,
    show_default=True,
)
@click.option(
    "--clock-tolerance-seconds",
    type=click.FloatRange(min=0),
    default=1.0,
    show_default=True,
)
@click.option("--stop-file", default="stop", show_default=True)
@click.option("--once", is_flag=True, help="Publish work due now and exit.")
def run_pass2(
    schedule_path: Path,
    target_map_path: Path,
    dispatcher_root: Path,
    run_id: str,
    run_start: str,
    publish_lead_seconds: int,
    plan_ttl_seconds: int,
    max_bytes_per_min: int,
    max_items_per_min: int,
    poll_seconds: float,
    clock_tolerance_seconds: float,
    stop_file: str,
    once: bool,
) -> None:
    """Publish due frozen Pass-2 predictions to the plan-file Dispatcher."""
    try:
        config = Pass2RuntimeConfig(
            schedule_path=schedule_path.absolute(),
            target_map_path=target_map_path.absolute(),
            dispatcher_root=dispatcher_root.absolute(),
            run_id=run_id,
            run_start=parse_utc_z(run_start),
            publish_lead_seconds=publish_lead_seconds,
            plan_ttl_seconds=plan_ttl_seconds,
            max_bytes_per_min=max_bytes_per_min,
            max_items_per_min=max_items_per_min,
            poll_seconds=poll_seconds,
            clock_tolerance_seconds=clock_tolerance_seconds,
            stop_file=stop_file,
        )
        with Pass2Scheduler(config) as scheduler:
            stats = scheduler.run(once=once)
    except Pass2RuntimeError as exc:
        raise click.ClickException(str(exc)) from None
    except (OSError, SecureFileError, ValueError):
        raise click.ClickException(
            "Pass-2 scheduling failed; inspect the owner-only state and audit records"
        ) from None
    click.echo(
        json.dumps(
            {
                "status": stats.status,
                "reason": stats.reason,
                "pending_items": stats.pending_items,
                "published_plans": stats.published_plans,
                "published_items": stats.published_items,
                "skipped_items": stats.skipped_items,
            },
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
