"""Planted-pattern diagnostics from restored opens; truth only locates instances."""

import json
from bisect import bisect_left, bisect_right
from collections import Counter, defaultdict
from dataclasses import asdict, is_dataclass
from datetime import datetime, time, timedelta
from math import exp
from zoneinfo import ZoneInfo

import numpy as np

KINDS = (
    "popularity",
    "weekly_schedule",
    "workflow_chain",
    "project_lifecycle",
    "burst",
    "batch_job",
    "new_file",
    "noise",
)


def _dt(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00")) if isinstance(value, str) else value


def _record(value):
    result = asdict(value) if is_dataclass(value) else dict(value)
    if isinstance(result["params"], str):
        result["params"] = json.loads(result["params"])
    return result


def _activity(params, at):
    names = ("started_at", "ramp_end", "active_end", "cooldown_end")
    times = [_dt(params.get(name)) for name in names]
    if all(times):
        start, ramp_end, active_end, cool_end = times
        ramp, active, cool, archived = params["phase_activity"]
        if at < start:
            return 0
        if at < ramp_end:
            return ramp + (at - start) / (ramp_end - start) * (active - ramp)
        if at < active_end:
            return active
        if at < cool_end:
            return active + (at - active_end) / (cool_end - active_end) * (cool - active)
        return archived
    history = [(_dt(t), v) for t, v in params.get("activity_history", []) if _dt(t) <= at]
    return max(history)[1] if history else 0


def _controlled(cfg, kind):
    active = {name for name in KINDS if getattr(cfg.patterns, name).enabled}
    active.discard("project_lifecycle")
    if cfg.randomness.noise_share == 0:
        active.discard("noise")
    allowed = {
        "project_lifecycle": {"popularity"},
        "workflow_chain": {"popularity", "weekly_schedule", "workflow_chain"},
    }.get(kind, {kind})
    if active - allowed or cfg.patterns.churn.enabled:
        return False
    if kind == "popularity":
        p = cfg.patterns.popularity
        return not (p.working_set_share or p.revisit_share or p.recency_boost_half_life_days)
    if kind in {"weekly_schedule", "project_lifecycle"} and cfg.actors.vacation_weeks_per_quarter:
        return False  # vacation blocks are deliberately absent from public actor catalogs
    return True


def _result(
    kind, cfg, *, record=None, metrics=None, enough=False, meets=False, reason=None, applicable=True
):
    status = (
        "not_applicable"
        if not applicable
        else "insufficient_data"
        if not enough
        else "diagnostic"
        if not _controlled(cfg, kind)
        else "passed"
        if meets
        else "failed"
    )
    return dict(
        kind=kind,
        pattern_id=record and record["pattern_id"],
        status=status,
        controlled=_controlled(cfg, kind),
        metrics=metrics or {},
        meets_threshold=bool(meets) if enough and applicable else None,
        reason=reason
        or (
            "Mixed workload or unobservable controls; diagnostic only."
            if status == "diagnostic"
            else None
        ),
    )


def recovery_checks(cfg, tables, patterns, clean):
    patterns = [_record(p) for p in patterns]
    volumes = {v["volume_id"]: v for v in tables["volumes"]}
    files = {f["file_id"]: f for f in tables["files"]}
    actors = {a["actor_id"]: a for a in tables["actors"]}
    cause = {t["event_id"]: t["cause"] for t in tables["event_truth"]}
    opens = [
        e
        for e in clean
        if e["operation"] == "open" and e["result"] == "ok" and e["origin"] == "foreground"
    ]
    by_cause, by_file, by_project, by_client, by_volume = (defaultdict(list) for _ in range(5))
    for event in opens:
        by_cause[cause.get(event["event_id"])].append(event)
        by_file[event["file_id"]].append(event)
        by_project[files[event["file_id"]]["project_id"]].append(event)
        by_client[(event["volume_id"], event["client_id"])].append(event)
        by_volume[event["volume_id"]].append(event)
    life = {
        p["params"]["project_id"]: p["params"] for p in patterns if p["kind"] == "project_lifecycle"
    }
    checks = []
    for record in patterns:
        kind, params = record["kind"], record["params"]
        if kind not in KINDS or kind in {"burst", "new_file"}:
            continue
        observed = by_cause[record["pattern_id"]]
        volume = volumes[record["volume_id"]]
        zone = ZoneInfo(volume["timezone"])
        if kind == "popularity":
            population = by_project[params["project_id"]]
            counts = sorted(Counter(e["file_id"] for e in population).values(), reverse=True)
            enough = len(counts) >= 20 and sum(counts) >= max(1000, 10 * len(record["files"]))
            fitted = (
                float(-np.polyfit(np.log(np.arange(1, len(counts) + 1)), np.log(counts), 1)[0])
                if enough
                else None
            )
            checks.append(
                _result(
                    kind,
                    cfg,
                    record=record,
                    enough=enough,
                    meets=enough and abs(fitted - params["zipf_s"]) <= 0.15,
                    metrics=dict(
                        opens=sum(counts),
                        observed_files=len(counts),
                        fitted_zipf=fitted,
                        target=params["zipf_s"],
                        tolerance=0.15,
                    ),
                )
            )
        elif kind == "weekly_schedule":
            slots = sorted(_dt(t) for t in params.get("slots", []))
            stamps = [t.timestamp() for t in slots]
            near = 0
            per_client = defaultdict(list)
            for e in observed:
                stamp = e["event_time"].timestamp()
                index = bisect_left(stamps, stamp)
                near += any(
                    abs(stamps[j] - stamp) <= 3600
                    for j in (index - 1, index)
                    if 0 <= j < len(stamps)
                )
                per_client[e["client_id"]].append(stamp)
            for times in per_client.values():
                times.sort()
            expected = matched = 0
            for actor_id in record["actors"]:
                actor = actors[actor_id]
                client = actor["client_id"]
                for slot in slots:
                    local = slot.astimezone(zone)
                    if (
                        local.isoweekday() not in actor["work_days"]
                        or local.date() in volume["holidays"]
                        or not actor["schedule_start_local"]
                        <= local.strftime("%H:%M")
                        < actor["schedule_end_local"]
                        or not any(
                            files[f]["created_at"] <= slot
                            and (files[f]["deleted_at"] is None or slot < files[f]["deleted_at"])
                            for f in record["files"]
                        )
                    ):
                        continue
                    multiplier = (
                        _activity(life[params["project_id"]], slot)
                        if params["project_id"] in life
                        else 1
                    )
                    expected += min(
                        1, params["attendance"] * multiplier * cfg.randomness.rate_scale
                    )
                    values = per_client[client]
                    index = bisect_left(values, slot.timestamp() - 3600)
                    matched += index < len(values) and values[index] <= slot.timestamp() + 3600
            fraction = near / len(observed) if observed else None
            coverage = min(1.0, matched / expected) if expected else None
            enough = (
                len(observed) >= 100
                and expected >= 20
                and cfg.period.weeks >= 4
                and all(e["client_id"] for e in observed)
            )
            checks.append(
                _result(
                    kind,
                    cfg,
                    record=record,
                    enough=enough,
                    meets=enough and fraction >= 0.8 and coverage >= 0.8,
                    metrics=dict(
                        opens=len(observed),
                        within_hour_fraction=fraction,
                        attendance_adjusted_expected=expected,
                        observed_occurrences=matched,
                        occurrence_coverage=coverage,
                    ),
                )
            )
        elif kind == "workflow_chain":
            clients = {actors[a]["client_id"] for a in record["actors"]}
            relevant = [
                e for fid in record["files"] for e in by_file[fid] if e["client_id"] in clients
            ]
            population = [e for client in clients for e in by_client[(record["volume_id"], client)]]
            drift = _dt(params.get("drift_at"))
            phases = [("before", None, drift, params["edges"])]
            if drift:
                phases.append(("after", drift, None, params["edges_after"]))
            edge_checks = []
            identity_available = bool(relevant) and all(
                e["client_id"] and e["session_id"] for e in relevant
            )
            for phase, start, end, edges in phases:
                phase_population = [
                    e
                    for e in population
                    if (start is None or e["event_time"] >= start)
                    and (end is None or e["event_time"] < end)
                ]
                baseline = Counter(e["file_id"] for e in phase_population)
                groups = defaultdict(list)
                for e in relevant:
                    if (start is None or e["event_time"] >= start) and (
                        end is None or e["event_time"] < end
                    ):
                        groups[(e["client_id"], e["session_id"])].append(e)
                for group in groups.values():
                    group.sort(key=lambda e: (e["event_time"], e["event_id"]))
                for edge in edges:
                    if edge["p"] < 0.5:
                        continue
                    eligible = hits = 0
                    for group in groups.values():
                        targets = [
                            e["event_time"].timestamp() for e in group if e["file_id"] == edge["to"]
                        ]
                        for index, event in enumerate(group):
                            previous = group[index - 1]["file_id"] if index else None
                            if event["file_id"] != edge["from"] or (
                                edge["previous"] is not None and previous != edge["previous"]
                            ):
                                continue
                            eligible += 1
                            at = event["event_time"].timestamp()
                            j = bisect_right(targets, at)
                            hits += j < len(targets) and targets[j] <= at + 900
                    p = baseline[edge["to"]] / len(phase_population) if phase_population else 0
                    lift = (hits / eligible) / p if eligible and p else None
                    edge_checks.append(
                        dict(
                            phase=phase,
                            previous=edge["previous"],
                            source=edge["from"],
                            target=edge["to"],
                            eligible=eligible,
                            hits=hits,
                            baseline=p,
                            lift=lift,
                        )
                    )
            enough = (
                identity_available
                and bool(edge_checks)
                and all(e["eligible"] >= 20 and e["lift"] is not None for e in edge_checks)
            )
            checks.append(
                _result(
                    kind,
                    cfg,
                    record=record,
                    enough=enough,
                    meets=enough and all(e["lift"] >= 3 for e in edge_checks),
                    metrics=dict(edges=edge_checks),
                    reason=None if identity_available else "Actor/session identity is unavailable.",
                )
            )
        elif kind == "project_lifecycle":
            start = volume["period_start"]
            start_date = start.astimezone(zone).date()
            weeks = (volume["period_end"].astimezone(zone).date() - start_date).days // 7
            weekly = np.zeros(weeks)
            for e in by_project[params["project_id"]]:
                week = (e["event_time"].astimezone(zone).date() - start_date).days // 7
                if 0 <= week < weeks:
                    weekly[week] += 1
            expected = []
            for week in range(weeks):
                values = []
                for day in range(7):
                    at = datetime.combine(
                        start_date + timedelta(weeks=week, days=day), time(12), zone
                    )
                    local = at.astimezone(zone)
                    if local.isoweekday() in volume["work_days"]:
                        values.append(
                            _activity(params, at)
                            * (
                                cfg.calendar.holiday_activity
                                if local.date() in volume["holidays"]
                                else 1
                            )
                        )
                expected.append(sum(values))
            varying = len(expected) > 1 and float(np.std(expected)) > 1e-12
            enough = varying and weeks >= 4 and weekly.sum() >= 1000 and float(np.std(weekly)) > 0
            correlation = float(np.corrcoef(weekly, expected)[0, 1]) if enough else None
            checks.append(
                _result(
                    kind,
                    cfg,
                    record=record,
                    applicable=varying,
                    enough=enough,
                    meets=enough and correlation > 0.8,
                    metrics=dict(
                        weekly_opens=weekly.tolist(), activity=expected, correlation=correlation
                    ),
                    reason="Constant or absent lifecycle curve." if not varying else None,
                )
            )
        elif kind == "batch_job":
            groups = defaultdict(list)
            for event in observed:
                groups[(event["client_id"], event["session_id"])].append(event["event_time"])
            target = sum(int(v) * m for v, m in zip(params["local_time"].split(":"), (3600, 60)))
            near = 0
            for values in groups.values():
                local = min(values).astimezone(zone)
                delta = abs(local.hour * 3600 + local.minute * 60 + local.second - target)
                near += min(delta, 86400 - delta) <= 900
            fraction = near / len(groups) if groups else None
            enough = len(groups) >= 5 and all(client and session for client, session in groups)
            checks.append(
                _result(
                    kind,
                    cfg,
                    record=record,
                    enough=enough,
                    meets=enough and fraction >= 0.9,
                    metrics=dict(job_runs=len(groups), within_15_minutes_fraction=fraction),
                )
            )
        elif kind == "noise":
            population = by_volume[record["volume_id"]]
            count = sum(cause[e["event_id"]] == "noise" for e in population)
            target = params["noise_share"]
            expected = len(population) * target
            enough = len(population) > 0 and (expected >= 100 or target in (0, 1))
            standalone = params.get("standalone", False)
            # Noise attribution is a truth-based accounting diagnostic, not learned detection.
            result = _result(
                kind,
                cfg,
                record=record,
                enough=enough,
                applicable=not standalone,
                meets=enough and abs(count - expected) <= 0.2 * expected,
                metrics=dict(
                    noise_opens=count,
                    foreground_opens=len(population),
                    fraction=count / len(population) if population else None,
                    target=target,
                ),
                reason="Standalone noise has no mixed-fraction denominator."
                if standalone
                else "Truth-attributed fraction; not an independent noise-detection score.",
            )
            checks.append(result)
    for volume_id, volume in volumes.items():
        bursts = [p for p in patterns if p["kind"] == "burst" and p["volume_id"] == volume_id]
        onsets = [
            min(e["event_time"] for e in by_cause[p["pattern_id"]])
            for p in bursts
            if by_cause[p["pattern_id"]]
        ]
        local_onsets = [t.astimezone(ZoneInfo(volume["timezone"])) for t in onsets]
        phases = (
            np.asarray(
                [
                    t.weekday() * 86400
                    + t.hour * 3600
                    + t.minute * 60
                    + t.second
                    + t.microsecond / 1e6
                    for t in local_onsets
                ]
            )
            * 2
            * np.pi
            / 604800
        )
        concentration = float(abs(np.mean(np.exp(1j * phases)))) if len(phases) else None
        p_value = exp(-len(phases) * concentration**2) if len(phases) else None
        enough = len(phases) >= 20 and cfg.period.weeks >= 4
        if bursts:
            checks.append(
                _result(
                    "burst",
                    cfg,
                    enough=enough,
                    meets=enough and p_value >= 0.05,
                    metrics=dict(
                        volume_id=volume_id,
                        instances=len(phases),
                        weekly_concentration=concentration,
                        rayleigh_p_approx=p_value,
                    ),
                )
            )
        created = [p for p in patterns if p["kind"] == "new_file" and p["volume_id"] == volume_id]
        selected = {
            fid
            for p in created
            if p["actors"]
            for fid in p["files"]
            if files[fid]["created_at"] + timedelta(days=10) <= volume["period_end"]
        }
        daily = np.zeros(10)
        for fid in selected:
            birth = files[fid]["created_at"]
            for event in by_file[fid]:
                age = (event["event_time"] - birth).days
                if 0 <= age < 10:
                    daily[age] += 1
        enough = len(selected) >= 20 and daily.sum() >= 1000 and float(np.std(daily)) > 0
        if enough:
            _, inverse, counts = np.unique(daily, return_inverse=True, return_counts=True)
            ends = np.cumsum(counts)
            ranks = ((ends - counts + ends - 1) / 2)[inverse]
            correlation = float(np.corrcoef(np.arange(10), ranks)[0, 1])
        else:
            correlation = None
        if created:
            checks.append(
                _result(
                    "new_file",
                    cfg,
                    enough=enough,
                    meets=enough and correlation < -0.5,
                    metrics=dict(
                        volume_id=volume_id,
                        complete_followup_files=len(selected),
                        daily_opens=daily.tolist(),
                        spearman=correlation,
                    ),
                )
            )
    present = {c["kind"] for c in checks}
    checks.extend(
        _result(kind, cfg, applicable=False, reason="No planted instances.")
        for kind in KINDS
        if kind not in present
    )
    return checks
