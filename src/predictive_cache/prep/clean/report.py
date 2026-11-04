"""Descriptive data-quality evidence for a cleaned dataset."""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from datetime import timedelta

from predictive_cache.datagen.io import canonical_json
from predictive_cache.prep.coverage import utc


def _quantile(values: list[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = (len(ordered) - 1) * fraction
    low = int(index)
    high = min(low + 1, len(ordered) - 1)
    return ordered[low] + (ordered[high] - ordered[low]) * (index - low)


def _dimension(value, missing="unknown"):
    return value if isinstance(value, str) and value else missing


def build_report(input_rows, kept, excluded, opens, coverage, counts, margin_s, catalog_misses):
    by_volume_day = defaultdict(lambda: {"input": 0, "kept": 0, "excluded": 0})
    exclusions_by_volume_day_reason = Counter()
    for label, rows in (("input", input_rows), ("kept", kept), ("excluded", excluded)):
        for row in rows:
            instant = row.get("ingest_time")
            try:
                day = utc(instant).date().isoformat() if instant else "invalid"
            except (TypeError, ValueError, AttributeError):
                day = "invalid"
            by_volume_day[(_dimension(row.get("volume_id"), "invalid"), day)][label] += 1
    delays = []
    for row in input_rows:
        if row.get("ingest_time") and row.get("event_time"):
            try:
                delays.append((utc(row["ingest_time"]) - utc(row["event_time"])).total_seconds())
            except (ValueError, TypeError):
                pass
    reasons = Counter(row["reason"] for row in excluded)
    for row in excluded:
        instant = row.get("ingest_time")
        try:
            day = utc(instant).date().isoformat() if instant else "invalid"
        except (ValueError, TypeError, AttributeError):
            day = "invalid"
        exclusions_by_volume_day_reason[
            (row.get("volume_id") or "invalid", day, row["reason"])
        ] += 1
    origins = Counter(_dimension(row.get("origin")) for row in input_rows)
    apps = Counter(_dimension(row.get("app")) for row in input_rows)
    statuses = Counter(row["status"] for row in coverage)
    incomplete = [
        {
            "volume_id": row["volume_id"],
            "start": row["hour_start"] - timedelta(seconds=margin_s),
            "end": row["hour_start"] + timedelta(hours=1, seconds=margin_s),
            "status": row["status"],
        }
        for row in coverage
        if row["status"] != "complete"
    ]
    unique_ids = len({r.get("event_id") for r in input_rows if isinstance(r.get("event_id"), str)})
    duplicate_count = reasons["duplicate"]
    report = {
        "counts": counts,
        "by_volume_day": [
            {"volume_id": volume, "date": day, **values}
            for (volume, day), values in sorted(by_volume_day.items())
        ],
        "excluded_by_reason": dict(sorted(reasons.items())),
        "exclusions_by_volume_day_reason": [
            {"volume_id": volume, "date": day, "reason": reason, "count": count}
            for (volume, day, reason), count in sorted(exclusions_by_volume_day_reason.items())
        ],
        "duplicate_rate": duplicate_count / unique_ids if unique_ids else 0.0,
        "late_arrival_s": {
            "p50": _quantile(delays, 0.5),
            "p90": _quantile(delays, 0.9),
            "p99": _quantile(delays, 0.99),
            "max": max(delays) if delays else None,
            "negative_count": sum(value < 0 for value in delays),
        },
        "coverage_hours": dict(sorted(statuses.items())),
        "incomplete_windows": incomplete,
        "opens": {
            "count": len(opens),
            "open_missing": sum(r["open_missing"] for r in opens),
            "close_missing": sum(r["close_missing"] for r in opens),
            "bytes_inconsistent": sum(r["bytes_inconsistent"] for r in opens),
            "consumed_share": sum(r["consumed"] is True for r in opens) / len(opens)
            if opens
            else 0.0,
        },
        "successful_returned_bytes": sum(r["bytes_read"] or 0 for r in opens),
        "distinct_bytes": sum(r["distinct_bytes_read"] or 0 for r in opens),
        "catalog_misses": catalog_misses,
        "origin_mix": dict(sorted(origins.items())),
        "app_mix": dict(sorted(apps.items())),
        "opens_by_volume_hour": [
            {"volume_id": volume, "hour": hour, "count": count}
            for (volume, hour), count in sorted(
                Counter((r["volume_id"], r["open_time"].hour) for r in opens).items()
            )
        ],
        "opens_by_volume_weekday": [
            {"volume_id": volume, "weekday": day, "count": count}
            for (volume, day), count in sorted(
                Counter((r["volume_id"], r["open_time"].weekday()) for r in opens).items()
            )
        ],
    }
    return report


def render_summary(report: dict) -> str:
    counts = report["counts"]
    lines = [
        "# Cleaned dataset quality",
        "",
        f"Input events: {counts['input_events']}",
        f"Kept events: {counts['kept_events']}",
        f"Excluded events: {sum(counts['excluded_by_reason'].values())}",
        f"Duplicate rate: {report['duplicate_rate']:.6f}",
        f"Successful returned bytes: {report['successful_returned_bytes']}",
        f"Distinct bytes: {report['distinct_bytes']}",
        "",
        "## Exclusions",
    ]
    lines.extend(
        f"- {reason}: {count}" for reason, count in sorted(report["excluded_by_reason"].items())
    )
    lines.extend(["", "## Coverage"])
    lines.extend(
        f"- {status}: {count}" for status, count in sorted(report["coverage_hours"].items())
    )
    lines.extend(["", "## Opens"])
    lines.extend(f"- {key}: {value}" for key, value in sorted(report["opens"].items()))
    lines.extend(["", "## Arrival and origin"])
    lines.extend(f"- {key}: {value}" for key, value in sorted(report["late_arrival_s"].items()))
    lines.extend(f"- origin {key}: {value}" for key, value in sorted(report["origin_mix"].items()))
    lines.extend(f"- app {key}: {value}" for key, value in sorted(report["app_mix"].items()))
    lines.extend(
        [
            "",
            "## Complete quality data",
            "",
            "```json",
            json.dumps(json.loads(canonical_json(report)), sort_keys=True, indent=2),
            "```",
        ]
    )
    return "\n".join(lines) + "\n"
