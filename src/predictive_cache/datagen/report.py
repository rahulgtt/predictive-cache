"""Deterministic dataset summaries for teammates and recovery diagnostics."""

from collections import Counter, defaultdict
from zoneinfo import ZoneInfo

import numpy as np

from .recovery import recovery_checks


def _distribution(values):
    if not values:
        return {"count": 0, "min": None, "p50": None, "p95": None, "max": None}
    return dict(
        count=len(values),
        min=min(values),
        p50=float(np.quantile(values, 0.5)),
        p95=float(np.quantile(values, 0.95)),
        max=max(values),
    )


def _counts(rows, field):
    return dict(
        sorted(Counter(row[field] if row[field] is not None else "unknown" for row in rows).items())
    )


def build_report(cfg, tables, patterns, known_gaps, extra=None, *, clean=None, validation=None):
    if clean is None:
        from .semantic import restore_clean

        clean = restore_clean(tables)
    delivered = tables["events"]
    volumes = {v["volume_id"]: v for v in tables["volumes"]}
    zones = {vid: ZoneInfo(v["timezone"]) for vid, v in volumes.items()}
    hourly = {vid: [0] * 168 for vid in volumes}
    for event in delivered:
        local = event["event_time"].astimezone(zones[event["volume_id"]])
        hourly[event["volume_id"]][local.weekday() * 24 + local.hour] += 1
    frequency = Counter()
    sessions = {}
    byte_totals = []
    for event in clean:
        if event["origin"] != "foreground" or event["result"] != "ok":
            continue
        if event["operation"] == "open":
            frequency[(event["volume_id"], event["file_id"])] += 1
        if event["operation"] == "close":
            byte_totals.append(event["handle_bytes_read"])
        if event["session_id"] is not None and event["client_id"] is not None:
            key = (event["volume_id"], event["client_id"], event["session_id"])
            at = event["event_time"]
            if key not in sessions:
                sessions[key] = [at, at, 0]
            stats = sessions[key]
            stats[0], stats[1] = min(stats[0], at), max(stats[1], at)
            stats[2] += event["operation"] == "open"
    sizes = defaultdict(list)
    for file in tables["files"]:
        sizes[file["kind"]].append(file["size_at_creation"])
    curve = []
    for volume in sorted(volumes):
        ranked = sorted(
            ((fid, n) for (vid, fid), n in frequency.items() if vid == volume),
            key=lambda pair: (-pair[1], pair[0]),
        )
        curve.extend(
            dict(volume_id=volume, rank=rank, file_id=fid, opens=n)
            for rank, (fid, n) in enumerate(ranked, 1)
        )
    rates = {}
    for name, stat in sorted((extra or {}).get("fault_rates", {}).items()):
        expected = stat["expected"]
        supported = expected >= 100 or expected == 0
        meets = abs(stat["affected"] - expected) <= 0.2 * expected
        rates[name] = {
            **stat,
            "observed_rate": stat["affected"] / stat["eligible"] if stat["eligible"] else None,
            "status": (
                "not_applicable"
                if not stat["eligible"]
                else "insufficient_data"
                if not supported
                else "passed"
                if meets
                else "failed"
            ),
            "tolerance_fraction": 0.2,
        }
    counters = tables["loss_counters"]
    return {
        **(extra or {}),
        "implementation": "synthetic_workloads_events_and_faults",
        "data_kind": "synthetic",
        "validation_scope": "schema_artifact_and_restored_semantics",
        "counts": {
            "delivered_rows": len(delivered),
            "unique_offered_events": len(clean),
            "removed_rows": len(tables["dropped_events"]),
            "files": len(tables["files"]),
            "actors": len(tables["actors"]),
            "patterns": len(patterns),
            "by_operation": _counts(delivered, "operation"),
            "by_origin": _counts(delivered, "origin"),
            "by_app": _counts(delivered, "app"),
            "by_fault": dict(
                sorted(
                    Counter(tag for row in tables["event_truth"] for tag in row["faults"]).items()
                )
            ),
        },
        "loss": {
            key: sum(row[key] for row in counters)
            for key in ("offered", "emitted", "dropped", "sampled_out", "duplicates", "late")
        },
        "known_gap_count": len(known_gaps),
        "hour_of_week": {
            vid: dict(timezone=volumes[vid]["timezone"], counts=counts)
            for vid, counts in sorted(hourly.items())
        },
        "hour_of_week_basis": "Delivered event_time in each volume timezone; index 0 = Monday 00:00.",
        "popularity_curve": curve,
        "popularity_basis": "Restored unique successful foreground opens.",
        "file_size_bytes_by_kind": {
            kind: _distribution(values) for kind, values in sorted(sizes.items())
        },
        "bytes_read_per_successful_open": _distribution(byte_totals),
        "sessions": {
            "count": len(sessions),
            "duration_seconds": _distribution(
                [(end - start).total_seconds() for start, end, _ in sessions.values()]
            ),
            "opens": _distribution([n for _, _, n in sessions.values()]),
            "identity_required": "client_id and session_id; absent identity is not inferred",
        },
        "fault_rates": rates,
        "semantic_checks": validation or {},
        "recovery": recovery_checks(cfg, tables, patterns, clean),
        "evidence": "Synthetic plumbing and planted-pattern diagnostics only; not real-workload quality.",
    }


def render_summary(report):
    counts = report["counts"]
    lines = [
        "# Synthetic access dataset",
        "",
        report["evidence"],
        "",
        f"- Delivered rows: **{counts['delivered_rows']:,}**",
        f"- Unique offered events: **{counts['unique_offered_events']:,}**",
        f"- Removed rows: **{counts['removed_rows']:,}**",
        f"- Files / actors / planted patterns: **{counts['files']:,} / {counts['actors']:,} / {counts['patterns']:,}**",
        "",
        "## Observation breakdowns",
        "",
        "Operations/origins/apps count delivered rows; faults count unique offered events, including removals.",
        "",
        "| Breakdown | Value | Count |",
        "| --- | --- | ---: |",
    ]
    for name in ("by_operation", "by_origin", "by_app", "by_fault"):
        lines.extend(
            f"| {name.removeprefix('by_')} | {value} | {n:,} |" for value, n in counts[name].items()
        )
    lines += ["", "## Semantic validation", "", "| Check | Result |", "| --- | --- |"]
    checks = report["semantic_checks"].get("checks", report["semantic_checks"])
    lines.extend(f"| {name} | {value} |" for name, value in sorted(checks.items()))
    lines += [
        "",
        "## Local hour-of-week activity",
        "",
        report["hour_of_week_basis"],
        "",
        "| Volume | Timezone | Most active local bins (delivered rows) |",
        "| --- | --- | --- |",
    ]
    days = ("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")
    for volume, histogram in report["hour_of_week"].items():
        peaks = sorted(enumerate(histogram["counts"]), key=lambda p: (-p[1], p[0]))[:5]
        text = ", ".join(
            f"{days[index // 24]} {index % 24:02d}:00: {count:,}" for index, count in peaks if count
        )
        lines.append(f"| {volume} | {histogram['timezone']} | {text or 'No delivered events'} |")
    lines += [
        "",
        "## Loss accounting",
        "",
        "| Offered | Emitted unique | Dropped | Sampled out | Duplicate deliveries | Late unique |",
        "| ---: | ---: | ---: | ---: | ---: | ---: |",
        "| "
        + " | ".join(
            f"{report['loss'][key]:,}"
            for key in ("offered", "emitted", "dropped", "sampled_out", "duplicates", "late")
        )
        + " |",
        "",
        f"Known collection gaps: **{report['known_gap_count']}**.",
        "",
        "## Sizes and sessions",
        "",
        "| Measurement | Count | Minimum | Median | p95 | Maximum |",
        "| --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    distributions = {
        f"{kind} creation bytes": values
        for kind, values in report["file_size_bytes_by_kind"].items()
    }
    distributions.update(
        {
            "Bytes per successful open": report["bytes_read_per_successful_open"],
            "Session duration seconds": report["sessions"]["duration_seconds"],
            "Opens per session": report["sessions"]["opens"],
        }
    )
    for name, values in distributions.items():
        cells = [str(values["count"])] + [
            "—" if values[key] is None else f"{values[key]:,.2f}"
            for key in ("min", "p50", "p95", "max")
        ]
        lines.append(f"| {name} | " + " | ".join(cells) + " |")
    lines += [
        "",
        "## Pattern diagnostics",
        "",
        "Passed/failed applies only to supported controlled cases. Mixed workloads are diagnostic;",
        "sparse or constant cases are not counted as passing.",
        "",
        "| Pattern | Check statuses |",
        "| --- | --- |",
    ]
    statuses = defaultdict(Counter)
    for check in report["recovery"]:
        statuses[check["kind"]][check["status"]] += 1
    lines.extend(
        f"| {kind} | " + ", ".join(f"{status}: {n}" for status, n in sorted(values.items())) + " |"
        for kind, values in sorted(statuses.items())
    )
    lines += [
        "",
        "## Fault-rate checks",
        "",
        "| Stage | Eligible | Affected | Expected | Status |",
        "| --- | ---: | ---: | ---: | --- |",
    ]
    lines.extend(
        f"| {name} | {s['eligible']:,} | {s['affected']:,} | {s['expected']:.2f} | {s['status']} |"
        for name, s in report["fault_rates"].items()
    )
    lines += [
        "",
        "## Most opened files",
        "",
        report["popularity_basis"],
        "",
        "| Volume | Rank | File ID | Opens |",
        "| --- | ---: | --- | ---: |",
    ]
    lines.extend(
        f"| {r['volume_id']} | {r['rank']} | {r['file_id']} | {r['opens']:,} |"
        for r in report["popularity_curve"]
        if r["rank"] <= 5
    )
    lines += [
        "",
        "Full popularity curves, 168-bin local-hour histograms and per-pattern measurements",
        "are in quality.json. truth/ is test-only: never feed it into model features.",
        "Catalog histories must be resolved as of the feature cutoff.",
        "",
    ]
    return "\n".join(lines)
