"""Clean-stream restoration and semantic checks for synthetic access datasets."""

import json
from bisect import bisect_right
from collections import Counter, defaultdict
from datetime import timedelta
from itertools import chain

from .schema import SCHEMAS, utc_datetime

_MUTATIONS = {
    "origin_unknown": {"origin"},
    "failed_result": {"result", "bytes_read"},
    "invalid_range": {"offset", "bytes_read", "bytes_requested", "result"},
    "identity_break": {"file_id"},
    "skew": {"event_time"},
    "late": {"ingest_time"},
}
_RESERVED = {"background", "noise", "churn", "warmer_sim", "probe_sim"}


def _require(condition, message):
    if not condition:
        raise ValueError(message)


def _index(rows, key, label):
    result = {r[key]: r for r in rows}
    _require(len(result) == len(rows), f"duplicate {label}")
    return result


def _deliveries(tables):
    first, duplicates = {}, Counter()
    for row in tables["events"]:
        eid = row["event_id"]
        if eid not in first:
            first[eid] = row
        else:
            original = first[eid]
            _require(
                all(row[k] == original[k] for k in SCHEMAS["events"].names if k != "ingest_time"),
                "duplicate delivery changes event fields",
            )
            _require(
                row["ingest_time"] != original["ingest_time"], "duplicate delivery lacks delay"
            )
            if row["ingest_time"] < original["ingest_time"]:
                first[eid] = row
            duplicates[eid] += 1
    removed = _index(tables["dropped_events"], "event_id", "removed event ID")
    _require(not first.keys() & removed.keys(), "event both delivered and removed")
    return first, removed, duplicates


def restore_clean(tables):
    """Restore one clean row per original event, rejecting unsubstantiated fault truth."""
    truth = _index(tables["event_truth"], "event_id", "truth event ID")
    first, removed, duplicates = _deliveries(tables)
    _require(truth.keys() == first.keys() | removed.keys(), "event/truth ID mismatch")
    clean, source_skew = [], {}
    for eid, row in chain(first.items(), removed.items()):
        planted = truth[eid]
        tags = set(planted["faults"])
        _require(len(tags) == len(planted["faults"]), "duplicate fault tag")
        removal = tags & {"dropped", "sampled_out"}
        _require(len(removal) == int(eid in removed), "removal disposition disagrees with truth")
        _require(("duplicate" in tags) == bool(duplicates[eid]), "duplicate truth mismatch")
        originals = json.loads(planted["original_fields_json"])
        expected = set().union(*(_MUTATIONS[t] for t in tags if t in _MUTATIONS))
        required = set().union(
            *(_MUTATIONS[t] for t in tags if t in _MUTATIONS and t != "invalid_range")
        )
        if "invalid_range" in tags:
            required.add("result")
            _require(
                bool({"offset", "bytes_read", "bytes_requested"} & originals.keys()),
                "invalid-range original fields missing",
            )
        _require(required <= originals.keys() <= expected, "fault original fields mismatch")
        original = row.copy() if originals else row
        for field, value in originals.items():
            original[field] = utc_datetime(value) if field.endswith("_time") else value
        _require(original["event_time"] == planted["true_event_time"], "truth true-time mismatch")
        _require(original["file_id"] == planted["true_file_id"], "truth true-file mismatch")
        _require(original["ingest_time"] == original["event_time"], "clean delivery-time mismatch")
        source = row["volume_id"], row["client_id"]
        delta = row["event_time"] - original["event_time"]
        _require(delta == source_skew.setdefault(source, delta), "inconsistent source clock skew")
        if "origin_unknown" in tags:
            _require(
                row["origin"] in (None, "unknown") and original["origin"] == "foreground",
                "unmatched unknown-origin fault",
            )
        if "failed_result" in tags:
            _require(
                row["operation"] == "read"
                and (
                    row["bytes_read"] == 0
                    or ("invalid_range" in tags and row["bytes_read"] > row["bytes_requested"])
                )
                and row["result"] == ("einval" if "invalid_range" in tags else "eio"),
                "unmatched failed-result fault",
            )
        if "invalid_range" in tags:
            _require(
                row["operation"] == "read"
                and row["result"] == "einval"
                and (
                    row["offset"] > row["file_size"] or row["bytes_read"] > row["bytes_requested"]
                ),
                "unmatched invalid-range fault",
            )
        if "identity_break" in tags:
            _require(row["file_id"] != original["file_id"], "unmatched identity-break fault")
        if "late" in tags:
            _require(row["ingest_time"] >= planted["true_event_time"], "negative delivery delay")
        clean.append(original)
    return clean


def validate_semantics(tables, cfg, patterns, known_gaps, clean=None):
    """Validate restored semantics; expected injected violations remain diagnostics."""
    clean = restore_clean(tables) if clean is None else clean
    volumes = _index(tables["volumes"], "volume_id", "volume ID")
    files = _index(tables["files"], "file_id", "file ID")
    dirs = _index(tables["directories"], "dir_id", "directory ID")
    actors = _index(tables["actors"], "actor_id", "actor ID")
    clients = {(a["volume_id"], a["client_id"]) for a in actors.values() if a["client_id"]}
    cohorts = {(a["volume_id"], a["cohort_id"]) for a in actors.values() if a["cohort_id"]}
    paths = defaultdict(list)
    for row in tables["file_paths"]:
        _require(row["file_id"] in files and row["dir_id"] in dirs, "path reference missing")
        _require(
            files[row["file_id"]]["volume_id"] == dirs[row["dir_id"]]["volume_id"],
            "path crosses volumes",
        )
        _require(
            row["valid_to"] is None or row["valid_from"] <= row["valid_to"], "invalid path interval"
        )
        paths[row["file_id"]].append(row)
    for fid, history in paths.items():
        history.sort(key=lambda p: p["valid_from"])
        _require(
            all(
                a["valid_to"] is not None and a["valid_to"] <= b["valid_from"]
                for a, b in zip(history, history[1:])
            ),
            "overlapping path history",
        )
    starts = {fid: [p["valid_from"] for p in history] for fid, history in paths.items()}
    for row in dirs.values():
        parent = row["parent_dir_id"]
        _require(row["volume_id"] in volumes, "directory volume missing")
        _require(
            parent is None or (parent in dirs and dirs[parent]["volume_id"] == row["volume_id"]),
            "directory parent missing/cross-volume",
        )
    for row in files.values():
        _require(
            row["volume_id"] in volumes and row["file_id"] in paths, "file volume/path missing"
        )
        replacement = row["replaced_by_file_id"]
        _require(
            replacement is None
            or (replacement in files and files[replacement]["volume_id"] == row["volume_id"]),
            "replacement reference missing",
        )
    for row in actors.values():
        _require(row["volume_id"] in volumes, "actor volume missing")

    pattern_ids = {p["pattern_id"] for p in patterns}
    truths = _index(tables["event_truth"], "event_id", "truth event ID")
    forbidden = pattern_ids | _RESERVED
    # Wire-schema validation rejects extra columns; also reject private labels encoded as values.
    for row in tables["events"]:
        _require(
            not any(isinstance(v, str) and (v in forbidden or "pat-" in v) for v in row.values()),
            "truth leakage in event values",
        )
    for row in truths.values():
        _require(row["cause"] in pattern_ids | _RESERVED, "unknown truth cause")

    handles = {}
    for row in clean:
        vid, fid, did, at = row["volume_id"], row["file_id"], row["dir_id"], row["event_time"]
        _require(vid in volumes and fid in files and did in dirs, "event reference missing")
        file = files[fid]
        _require(file["volume_id"] == vid == dirs[did]["volume_id"], "event crosses volumes")
        _require(
            row["client_id"] is None or (vid, row["client_id"]) in clients,
            "event client reference missing",
        )
        _require(
            row["cohort_id"] is None or (vid, row["cohort_id"]) in cohorts,
            "event cohort reference missing",
        )
        _require(at >= file["created_at"], "event before file creation")
        failed = row["operation"] == "open" and row["result"] != "ok"
        _require(
            file["deleted_at"] is None
            or at < file["deleted_at"]
            or row["operation"] == "delete"
            or (failed and row["result"] == "enoent"),
            "successful operation after deletion",
        )
        history = paths[fid]
        index = bisect_right(starts[fid], at) - 1
        path = history[max(0, index)]
        if not (failed and row["result"] == "enoent") and row["operation"] != "delete":
            _require(
                index >= 0 and (path["valid_to"] is None or at < path["valid_to"]),
                "event outside path lifetime",
            )
        _require(path["dir_id"] == did, "event directory disagrees with path history")
        _require(row["file_size"] >= 0, "negative file size")
        _require(row["origin"] in {"foreground", "warmer", "synthetic"}, "unexplained clean origin")
        if row["operation"] == "delete":
            _require(file["deleted_at"] == at, "delete disagrees with catalog")

        if row["operation"] == "read":
            _require(
                row["result"] == "ok"
                and row["offset"] is not None
                and row["bytes_requested"] is not None
                and row["bytes_read"] is not None
                and 0 <= row["bytes_read"] <= row["bytes_requested"]
                and 0 <= row["offset"] <= row["file_size"] - row["bytes_read"],
                "invalid clean read range/result",
            )
        hid = row["handle_id"]
        if row["operation"] in {"open", "read", "close"}:
            if failed:
                _require(hid is None, "failed open has a handle")
                continue
            _require(hid is not None, "successful handle operation missing handle ID")
            state = handles.setdefault(
                hid,
                {
                    "open": None,
                    "close": None,
                    "bytes": 0,
                    "reads": 0,
                    "first_read": None,
                    "last_read": None,
                },
            )
            identity = (
                vid,
                fid,
                row["client_id"],
                row["session_id"],
                row["origin"],
                row["file_size"],
            )
            _require(state.setdefault("identity", identity) == identity, "handle identity changes")
            if row["operation"] == "read":
                state["bytes"] += row["bytes_read"]
                state["reads"] += 1
                state["first_read"] = min(state["first_read"] or at, at)
                state["last_read"] = max(state["last_read"] or at, at)
            else:
                _require(state[row["operation"]] is None, "duplicate open/close on handle")
                state[row["operation"]] = row
    for state in handles.values():
        opened, closed = state["open"], state["close"]
        _require(opened is not None and closed is not None, "unpaired open/close")
        _require(opened["event_time"] < closed["event_time"], "close does not follow open")
        _require(
            closed["handle_bytes_read"] is not None and closed["handle_bytes_read"] >= 0,
            "missing/negative handle total",
        )
        _require(
            closed["handle_duration_ms"]
            == round((closed["event_time"] - opened["event_time"]).total_seconds() * 1000),
            "handle duration mismatch",
        )
        if cfg.realize.read_events == "ranges":
            _require(state["bytes"] == closed["handle_bytes_read"], "handle read total mismatch")
        else:
            _require(state["reads"] == 0, "read rows present in summary mode")
        if state["reads"]:
            _require(
                opened["event_time"]
                <= state["first_read"]
                <= state["last_read"]
                <= closed["event_time"],
                "read outside handle lifetime",
            )

    first, removed, duplicates = _deliveries(tables)
    counts = defaultdict(Counter)
    gaps = defaultdict(list)
    for gap in known_gaps:
        gaps[(gap["volume_id"], gap["reason"])].append(gap)
    for eid, truth in truths.items():
        row = first.get(eid, removed.get(eid))
        at = truth["true_event_time"]
        bucket = counts[(row["volume_id"], at.replace(minute=0, second=0, microsecond=0))]
        bucket["offered"] += 1
        tags = set(truth["faults"])
        disposition = (
            "dropped"
            if "dropped" in tags
            else "sampled_out"
            if "sampled_out" in tags
            else "emitted"
        )
        bucket[disposition] += 1
        bucket["duplicates"] += duplicates[eid]
        if disposition == "emitted":
            bucket["late"] += int(
                (row["ingest_time"] - at).total_seconds() > cfg.faults.late.threshold_s
            )
        else:
            reason = "loss_window" if disposition == "dropped" else "sampling"
            _require(
                any(
                    g["start"] <= at < g["end"] and g["loss_fraction"] > 0
                    for g in gaps[(row["volume_id"], reason)]
                ),
                "removed event outside known gaps",
            )
    counters = {}
    keys = ("offered", "emitted", "dropped", "sampled_out", "duplicates", "late")
    for row in tables["loss_counters"]:
        key = row["volume_id"], row["hour_start"]
        _require(
            key not in counters
            and key[0] in volumes
            and key[1] == key[1].replace(minute=0, second=0, microsecond=0),
            "invalid/duplicate counter bucket",
        )
        _require(all(row[k] == counts[key][k] for k in keys), "loss counter mismatch")
        _require(
            row["offered"] == row["emitted"] + row["dropped"] + row["sampled_out"],
            "loss counter reconciliation failed",
        )
        counters[key] = row
    for volume in volumes.values():
        hour = volume["period_start"].replace(minute=0, second=0, microsecond=0)
        while hour < volume["period_end"]:
            _require((volume["volume_id"], hour) in counters, "missing counter hour")
            hour += timedelta(hours=1)
    _require(counts.keys() <= counters.keys(), "missing spill counter hour")
    return {
        "checks": {
            name: "passed"
            for name in (
                "references",
                "handles",
                "ranges",
                "loss",
                "truth_leakage",
                "fault_restoration",
            )
        },
        "clean_events": len(clean),
        "handles": len(handles),
        "expected_faults": dict(Counter(tag for row in truths.values() for tag in row["faults"])),
        "summary_totals": "realizer_plan_tests"
        if cfg.realize.read_events == "none"
        else "read_sums",
    }
