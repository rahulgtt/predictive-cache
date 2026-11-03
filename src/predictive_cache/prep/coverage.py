"""Hourly observation coverage and shared interval query."""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path

from predictive_cache.prep.clean.io import read_rows
from predictive_cache.prep.config import CleanConfig


def utc(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00")) if isinstance(value, str) else value


def hour_floor(value):
    return value.replace(minute=0, second=0, microsecond=0)


def build_coverage(manifest: dict, counters: list[dict], cfg: CleanConfig) -> list[dict]:
    start, end = utc(manifest["period"]["start"]), utc(manifest["period"]["end"])
    by_key = {(r["volume_id"], utc(r["hour_start"])): r for r in counters}
    volumes = manifest.get("volumes") or sorted({r["volume_id"] for r in counters})
    rows = []
    for volume in sorted(volumes):
        hour = hour_floor(start)
        while hour < end:
            source = by_key.get((volume, hour))
            offered = source["offered"] if source is not None else None
            fraction = (
                (source["dropped"] + source["sampled_out"]) / offered
                if source is not None and offered
                else 0.0
                if source is not None
                else None
            )
            status = (
                "no_counters"
                if source is None
                else "incomplete"
                if fraction > cfg.coverage.max_loss_fraction
                else "complete"
            )
            if any(
                gap["volume_id"] == volume
                and utc(gap["start"]) < hour + timedelta(hours=1)
                and utc(gap["end"]) > hour
                for gap in manifest.get("known_gaps", [])
            ):
                status = "incomplete"
            rows.append(
                {
                    "volume_id": volume,
                    "hour_start": hour,
                    "offered": offered,
                    "emitted": source["emitted"] if source else None,
                    "dropped": source["dropped"] if source else None,
                    "sampled_out": source["sampled_out"] if source else None,
                    "loss_fraction": fraction,
                    "status": status,
                }
            )
            hour += timedelta(hours=1)
    return rows


class Coverage:
    def __init__(self, rows: list[dict], period: dict, margin_s: int, assume_complete: bool):
        self.rows = {(r["volume_id"], utc(r["hour_start"])): r for r in rows}
        self.start, self.end = utc(period["start"]), utc(period["end"])
        self.margin = timedelta(seconds=margin_s)
        self.assume_complete = assume_complete

    @classmethod
    def load(cls, clean_dir: Path) -> "Coverage":
        manifest = json.loads((clean_dir / "manifest.json").read_text())
        files = list((clean_dir / "coverage").glob("coverage.*"))
        if len(files) != 1:
            raise ValueError("expected one coverage table")
        return cls(
            read_rows(files[0]),
            manifest["period"],
            manifest["coverage"]["skew_margin_s"],
            manifest["coverage"]["assume_complete_without_counters"],
        )

    def is_observed(self, volume_id: str, a: datetime, b: datetime) -> bool:
        if a < self.start or b > self.end or b <= a:
            return False
        hour = hour_floor(a)
        while hour < b:
            row = self.rows.get((volume_id, hour))
            if row is None:
                return False
            status = row["status"]
            if status != "complete" and not (status == "no_counters" and self.assume_complete):
                bad_start, bad_end = hour - self.margin, hour + timedelta(hours=1) + self.margin
                if bad_start < b and bad_end > a:
                    return False
            hour += timedelta(hours=1)
        # An incomplete adjacent hour may overlap the interval after margin expansion.
        before = hour_floor(a - self.margin)
        after = hour_floor(b + self.margin)
        while before <= after:
            row = self.rows.get((volume_id, before))
            if row is not None and row["status"] != "complete":
                if not (row["status"] == "no_counters" and self.assume_complete):
                    if before - self.margin < b and before + timedelta(hours=1) + self.margin > a:
                        return False
            before += timedelta(hours=1)
        return True
