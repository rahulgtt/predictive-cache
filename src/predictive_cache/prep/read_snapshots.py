"""Build ``read-demand-v1`` training snapshots from a READ-mode cleaned dataset.

The feature values are a vectorized equivalent of
``predictive_cache.ml.read_demand.ReadDemandEngine`` (parity-tested). The co-read
table, candidate sources and per-tick anchor follow
docs/data-prep/read-demand-v1.md § Co-read table and candidates. Ticks, splits,
sampling and the output layout follow data-prep spec §8.
"""

from __future__ import annotations

import hashlib
import json
import shutil
import tempfile
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from predictive_cache.ml.package import COACCESS_SCHEMA, contract_sha256
from predictive_cache.ml.read_demand import (
    READ_DEMAND_FEATURE_SCHEMA_SHA256,
    READ_DEMAND_FEATURE_VERSION,
    READ_DEMAND_FEATURES,
    READ_DEMAND_SERVING_CONTRACT,
    READ_DEMAND_SERVING_CONTRACT_SHA256,
    feature_schema,
    runtime_available_features,
)
from predictive_cache.semantics.read_demand import (
    SEMANTIC_VIEW_ID,
    SEMANTIC_VIEW_SHA256,
    canonical_json,
)

US = 1_000_000
HOUR = 3600 * US
DAY = 86400 * US
TS = pa.timestamp("us", tz="UTC")
HORIZON = READ_DEMAND_SERVING_CONTRACT["horizon_s"] * US
COREAD = READ_DEMAND_SERVING_CONTRACT["coread_transition_seconds"] * US
EXCLUDED = {"warmer", "unknown"}

# (window seconds) per windowed feature; the others are computed separately.
COUNT_WINDOWS = {
    "read_count_5m": 300,
    "read_count_15m": 900,
    "read_count_1h": 3600,
    "read_count_24h": 86400,
    "read_count_7d": 604800,
}
BYTES_WINDOWS = {
    "returned_bytes_5m": 300,
    "returned_bytes_1h": 3600,
    "returned_bytes_24h": 86400,
    "returned_bytes_7d": 604800,
}
RANGE_WINDOWS = {"distinct_read_ranges_1h": 3600, "distinct_read_ranges_24h": 86400}
FLOAT_FEATURES = {"secs_since_last_read", "volume_read_share_1h"}


@dataclass
class ReadSnapshotConfig:
    tick_s: int = 900
    warmup_s: int = 86400
    val_fraction: float = 0.2
    test_fraction: float = 0.2
    neg_per_pos: int = 10
    min_negatives: int = 50
    seed: int = 0
    recent_window_s: int = 3600
    recent_cap: int = 200
    neighbor_cap: int = 200
    popular_window_s: int = 86400
    popular_k: int = 100
    max_candidates: int = 500
    min_support: int = 3
    min_lift: float = 1.5
    max_out_edges: int = 50
    refit_every_s: int = 86400
    skew_margin_s: int = 600
    verified_file_size: bool = True

    def sha256(self) -> str:
        return hashlib.sha256(canonical_json(asdict(self)).encode()).hexdigest()


EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def _us(value: datetime) -> int:
    return (value - EPOCH) // timedelta(microseconds=1)


def _dt(us: int) -> datetime:
    return EPOCH + timedelta(microseconds=int(us))


def _iso(us: int) -> str:
    return _dt(us).isoformat().replace("+00:00", "Z")


def _ts_us(column: pa.ChunkedArray) -> np.ndarray:
    return pc.cast(pc.cast(column, TS), pa.int64()).to_numpy(zero_copy_only=False)


class VolumeState:
    """One volume's eligible READs in ``(event_time, event_id)`` order plus coverage."""

    def __init__(
        self,
        volume_id: str,
        et: np.ndarray,
        ka: np.ndarray,
        eid_rank: np.ndarray,
        fid: np.ndarray,
        offset: np.ndarray,
        nbytes: np.ndarray,
        size: np.ndarray,
        coverage: dict[int, str],
        n_files: int,
    ) -> None:
        self.volume_id = volume_id
        order = np.lexsort((eid_rank, et))
        self.et, self.ka, self.fid = et[order], ka[order], fid[order]
        self.off, self.nbytes, self.size = offset[order], nbytes[order], size[order]
        self.eid_rank = eid_rank[order]
        # Engine insertion order is (known_at, event_id): drives "latest" file size.
        self.ka_order = np.lexsort((self.eid_rank, self.ka))
        self._cursor = 0
        self.n_files = n_files
        self.seen = np.zeros(n_files, dtype=bool)
        self.last_et = np.full(n_files, np.iinfo(np.int64).min, dtype=np.int64)
        self.last_size = np.full(n_files, -1, dtype=np.int64)
        hours = sorted(coverage)
        self._h0 = hours[0] if hours else 0
        flags = np.ones(len(hours) and (hours[-1] - self._h0 + 1), dtype=np.int64)
        for hour, status in coverage.items():
            flags[hour - self._h0] = 0 if status == "complete" else 1
        self._bad = np.concatenate([[0], np.cumsum(flags)])
        self._pos = np.full(n_files, -1, dtype=np.int64)

    def complete(self, start: int, end: int) -> bool:
        """Every hour bucket from floor(start) while bucket < end is complete."""
        if end <= start:
            return True
        first = start // HOUR - self._h0
        last = (end - 1) // HOUR - self._h0
        if first < 0 or last >= len(self._bad) - 1:
            return False
        return bool(self._bad[last + 1] - self._bad[first] == 0)

    def advance(self, t: int) -> None:
        """Apply every event with ``known_at <= t`` to the visible per-file state."""
        order = self.ka_order
        end = self._cursor
        while end < len(order) and self.ka[order[end]] <= t:
            end += 1
        if end == self._cursor:
            return
        idx = order[self._cursor : end]
        fids = self.fid[idx]
        np.maximum.at(self.last_et, fids, self.et[idx])
        self.seen[fids] = True
        self.last_size[fids] = self.size[idx]  # last write in (known_at, event_id) order wins
        self._cursor = end

    def _visible(self, t: int, window_us: int) -> tuple[np.ndarray, int]:
        """Indices (into the et-sorted arrays) visible at t with 0 <= t - et <= window."""
        lo = int(np.searchsorted(self.et, t - window_us, "left"))
        hi = int(np.searchsorted(self.et, t, "right"))
        rel = np.flatnonzero(self.ka[lo:hi] <= t)
        return rel + lo, lo

    def anchor(self, t: int) -> int:
        idx, _ = self._visible(t, COREAD)
        return int(self.fid[idx[-1]]) if len(idx) else -1

    def features(self, t: int, candidates: np.ndarray, anchor: int) -> dict[str, np.ndarray]:
        """Feature arrays for ``candidates`` (file indices); NaN marks null."""
        n = len(candidates)
        pos = self._pos
        pos[candidates] = np.arange(n)
        out: dict[str, np.ndarray] = {}
        try:
            week, _ = self._visible(t, 7 * DAY)
            wet, wfid = self.et[week], self.fid[week]
            wpos = pos[wfid]

            def window(seconds: int) -> np.ndarray:
                return wet >= t - seconds * US

            def nullable(seconds: int, values: np.ndarray) -> np.ndarray:
                values = values.astype(np.float64)
                if not self.complete(t - seconds * US, t):
                    values[:] = np.nan
                return values

            def per_candidate(mask: np.ndarray, weights: np.ndarray | None = None) -> np.ndarray:
                keep = mask & (wpos >= 0)
                return np.bincount(
                    wpos[keep],
                    weights=None if weights is None else weights[keep],
                    minlength=n,
                )[:n]

            for name, seconds in COUNT_WINDOWS.items():
                out[name] = nullable(seconds, per_candidate(window(seconds)))
            wbytes = self.nbytes[week].astype(np.float64)
            for name, seconds in BYTES_WINDOWS.items():
                out[name] = nullable(seconds, per_candidate(window(seconds), wbytes))
            woff, wnb = self.off[week], self.nbytes[week]
            for name, seconds in RANGE_WINDOWS.items():
                keep = window(seconds) & (wpos >= 0)
                triples = np.unique(np.stack([wpos[keep], woff[keep], wnb[keep]], axis=1), axis=0)
                out[name] = nullable(
                    seconds, np.bincount(triples[:, 0], minlength=n)[:n] if len(triples) else np.zeros(n)
                )
            keep = wpos >= 0
            days = np.unique(np.stack([wpos[keep], wet[keep] // DAY], axis=1), axis=0)
            out["read_days_7d"] = nullable(
                604800, np.bincount(days[:, 0], minlength=n)[:n] if len(days) else np.zeros(n)
            )
            seen = self.seen[candidates]
            secs = np.where(seen, (t - self.last_et[candidates]) / US, np.nan)
            out["secs_since_last_read"] = nullable(604800, secs)
            size = self.last_size[candidates].astype(np.float64)
            out["file_size_bytes"] = np.where(seen & (size >= 0), size, np.nan)
            hour = window(3600)
            volume_1h = int(hour.sum())
            share = per_candidate(hour) / volume_1h if volume_1h else np.zeros(n)
            out["volume_read_share_1h"] = nullable(3600, share)
            coread = np.full(n, np.nan)
            if anchor >= 0 and self.complete(t - DAY, t):
                day = window(86400)
                seq_f, seq_t = wfid[day], wet[day]
                if len(seq_f) > 1:
                    sel = (seq_f[:-1] == anchor) & (seq_t[1:] - seq_t[:-1] <= COREAD)
                    nxt = pos[seq_f[1:][sel]]
                    coread = np.bincount(nxt[nxt >= 0], minlength=n)[:n].astype(np.float64)
                else:
                    coread = np.zeros(n)
            out["coread_from_anchor_24h"] = coread
        finally:
            pos[candidates] = -1
        return out

    def recent(self, t: int, window_s: int, cap: int) -> list[int]:
        idx, _ = self._visible(t, window_s * US)
        if not len(idx):
            return []
        fids = self.fid[idx][::-1]
        ets = self.et[idx][::-1]
        uniq, first = np.unique(fids, return_index=True)
        order = np.lexsort((uniq, -ets[first]))
        return [int(f) for f in uniq[order][:cap]]

    def popular(self, t: int, window_s: int, k: int) -> list[int]:
        idx, _ = self._visible(t, window_s * US)
        if not len(idx):
            return []
        uniq, counts = np.unique(self.fid[idx], return_counts=True)
        order = np.lexsort((uniq, -counts))
        return [int(f) for f in uniq[order][:k]]

    def future(self, t: int) -> tuple[np.ndarray, int, int]:
        """Event indices with t < event_time <= t + horizon (labels use all known data)."""
        lo = int(np.searchsorted(self.et, t, "right"))
        hi = int(np.searchsorted(self.et, t + HORIZON, "right"))
        return np.arange(lo, hi), lo, hi

    def fit_coread(
        self, fit_start: int, fit_end: int, cfg: ReadSnapshotConfig
    ) -> tuple[dict[int, np.ndarray], list[tuple[int, int, int, int, float, float]]]:
        """Direct-successor co-read table over runs (read-demand-v1 § CoReadTable)."""
        mask = (self.ka < fit_end) & (self.et >= fit_start) & (self.et < fit_end)
        f, e = self.fid[mask], self.et[mask]
        if len(f) < 2:
            return {}, []
        boundary = np.concatenate([[True], f[1:] != f[:-1]])
        run_files = f[boundary]
        n_runs = np.bincount(run_files, minlength=self.n_files)
        total_runs = len(run_files)
        trans = (f[1:] != f[:-1]) & (e[1:] - e[:-1] <= COREAD)
        src, dst = f[:-1][trans], f[1:][trans]
        if not len(src):
            return {}, []
        keys, support = np.unique(src.astype(np.int64) * self.n_files + dst, return_counts=True)
        a, b = keys // self.n_files, keys % self.n_files
        in_support = np.bincount(b, weights=support, minlength=self.n_files)
        conf = support / n_runs[a]
        base = in_support[b] / total_runs
        lift = conf / base
        ok = (support >= cfg.min_support) & (lift >= cfg.min_lift)
        a, b, support, conf, lift = a[ok], b[ok], support[ok], conf[ok], lift[ok]
        if not len(a):
            return {}, []
        order = np.lexsort((b, -conf, a))
        edges: dict[int, np.ndarray] = {}
        rows = []
        grouped = a[order]
        starts = np.flatnonzero(np.concatenate([[True], grouped[1:] != grouped[:-1]]))
        for lo, hi in zip(starts, [*starts[1:], len(order)], strict=True):
            sel = order[lo:hi][: cfg.max_out_edges]
            edges[int(a[sel[0]])] = b[sel]
            rows.extend(
                (int(a[i]), int(b[i]), int(support[i]), int(n_runs[a[i]]), float(conf[i]), float(lift[i]))
                for i in sel
            )
        return edges, rows


def load_clean(clean_dir: Path) -> tuple[dict, pa.Table, pa.Table]:
    root = Path(clean_dir)
    manifest = json.loads((root / "manifest.json").read_text())
    if manifest.get("semantic_view_id") != SEMANTIC_VIEW_ID:
        raise ValueError("input must be cleaned with --semantic-view read-demand-v1")
    if manifest.get("semantic_view_sha256") != SEMANTIC_VIEW_SHA256:
        raise ValueError("input semantic view hash differs from this code")
    parts = sorted((root / "read_events").rglob("*.parquet"))
    if not parts:
        raise ValueError(f"{root} has no read_events")
    events = pa.concat_tables(
        [pq.ParquetFile(p).read() for p in parts], promote_options="default"
    )
    coverage = pq.ParquetFile(root / "coverage" / "coverage.parquet").read()
    return manifest, events, coverage


def build_states(events: pa.Table, coverage: pa.Table) -> tuple[list[str], list[VolumeState]]:
    origin = events.column("origin").to_pylist()
    demand = events.column("demand_class").to_pylist()
    keep = np.array([o not in EXCLUDED and d not in EXCLUDED for o, d in zip(origin, demand, strict=True)])
    events = events.filter(pa.array(keep)) if len(keep) else events
    files, fid = np.unique(
        np.asarray(events.column("file_id").to_pylist(), dtype=object), return_inverse=True
    )
    _, eid_rank = np.unique(
        np.asarray(events.column("event_id").to_pylist(), dtype=object), return_inverse=True
    )
    volumes = np.asarray(events.column("volume_id").to_pylist(), dtype=object)
    et, ka = _ts_us(events.column("event_time")), _ts_us(events.column("known_at"))
    off = events.column("offset").to_numpy(zero_copy_only=False).astype(np.int64)
    nbytes = events.column("bytes_read").to_numpy(zero_copy_only=False).astype(np.int64)
    size = np.asarray(
        [-1 if v is None else v for v in events.column("file_size").to_pylist()], dtype=np.int64
    )
    cov_volume = coverage.column("volume_id").to_pylist()
    cov_hour = _ts_us(coverage.column("hour_start")) // HOUR
    cov_status = coverage.column("status").to_pylist()
    by_volume: dict[str, dict[int, str]] = {}
    for v, h, s in zip(cov_volume, cov_hour, cov_status, strict=True):
        by_volume.setdefault(v, {})[int(h)] = s
    states = []
    for volume in sorted(set(by_volume) | set(volumes.tolist())):
        m = volumes == volume
        states.append(
            VolumeState(
                volume, et[m], ka[m], eid_rank[m], fid[m], off[m], nbytes[m], size[m],
                by_volume.get(volume, {}), len(files),
            )
        )
    return [str(f) for f in files], states


def _keep_negative(seed: int, snapshot_id: str, file_id: str, q: float) -> bool:
    digest = hashlib.blake2b(f"{seed}|{snapshot_id}|{file_id}".encode(), digest_size=8).digest()
    return int.from_bytes(digest, "big") / 2**64 < q


def _midnight(us: int) -> int:
    return us // DAY * DAY


def build_read_snapshots(
    clean_dir: Path, out_dir: Path, cfg: ReadSnapshotConfig | None = None
) -> dict:
    """Write a snapshot directory that ``pcache-train`` accepts; returns the manifest."""
    cfg = cfg or ReadSnapshotConfig()
    out = Path(out_dir).absolute()
    if out.exists():
        raise FileExistsError(f"{out} already exists")
    parent, events, coverage = load_clean(clean_dir)
    start = _us(datetime.fromisoformat(parent["period"]["start"].replace("Z", "+00:00")))
    end = _us(datetime.fromisoformat(parent["period"]["end"].replace("Z", "+00:00")))
    files, states = build_states(events, coverage)
    tick = cfg.tick_s * US
    body = start + cfg.warmup_s * US
    test_start = _midnight(body + int((end - body) * (1 - cfg.test_fraction)))
    val_start = _midnight(
        body + int((end - body) * (1 - cfg.test_fraction - cfg.val_fraction))
    )
    margin = cfg.skew_margin_s * US

    def status_of(t: int) -> tuple[str, str | None]:
        if t < body:
            return "warmup", None
        if t + HORIZON > end:
            return "dropped_period_end", None
        split = "train" if t < val_start else "val" if t < test_start else "test"
        boundary = {"train": val_start, "val": test_start}.get(split)
        if boundary is not None and t + HORIZON > boundary - margin:
            return "dropped_boundary", None
        return "ok", split

    feature_names = list(READ_DEMAND_FEATURES)
    rows: dict[tuple[str, str], dict[str, list]] = {}
    universe: dict[str, list[dict]] = {}
    coaccess_rows: list[tuple] = []
    counts = {s: {"ticks": 0, "rows": 0, "positives": 0, "unknown_dropped": 0} for s in ("train", "val", "test")}
    dropped: dict[str, int] = {}
    coverage_ratio: dict[str, list[float]] = {"train": [], "val": [], "test": []}
    refits = 0

    for state in states:
        refit_times = list(range(start + cfg.refit_every_s * US, val_start + 1, cfg.refit_every_s * US))
        refits = max(refits, len(refit_times))
        frozen_edges, frozen_rows = state.fit_coread(start, val_start, cfg)
        coaccess_rows.extend(frozen_rows)
        fit_cache: dict[int, dict[int, np.ndarray]] = {}
        first = -(-start // tick) * tick
        for t in range(first, end, tick):
            state.advance(t)
            sid = f"{state.volume_id}@{_iso(t)}"
            status, split = status_of(t)
            urow = {
                "snapshot_id": sid, "t": _dt(t), "split": split, "status": status,
                "n_candidates": 0, "n_ineligible": 0, "n_positive_files": 0,
                "n_positive_in_candidates": 0, "positive_bytes": 0,
                "positive_bytes_in_candidates": 0, "n_positive_unknown_at_t": 0,
                "n_positive_ineligible": 0, "n_unknown_labels": 0,
            }
            universe.setdefault(state.volume_id, []).append(urow)
            if status != "ok":
                dropped[status] = dropped.get(status, 0) + 1
                continue
            if split == "train":
                fit_end = max((r for r in refit_times if r <= t), default=None)
                if fit_end is not None and fit_end not in fit_cache:
                    fit_cache.clear()
                    fit_cache[fit_end] = state.fit_coread(start, fit_end, cfg)[0]
                edges = fit_cache.get(fit_end, {}) if fit_end is not None else None
            else:
                fit_end, edges = val_start, frozen_edges

            anchor = state.anchor(t)
            sources: dict[int, list[str]] = {}
            for f in state.recent(t, cfg.recent_window_s, cfg.recent_cap):
                sources.setdefault(f, []).append("recent")
            if edges and anchor >= 0 and anchor in edges:
                for f in edges[anchor][: cfg.neighbor_cap]:
                    sources.setdefault(int(f), []).append("neighbor")
            for f in state.popular(t, cfg.popular_window_s, cfg.popular_k):
                sources.setdefault(f, []).append("popular")
            candidates = []
            n_ineligible = 0
            for f in sources:
                if not state.seen[f]:
                    continue
                if state.last_size[f] == 0:
                    n_ineligible += 1
                    continue
                candidates.append(f)
            candidates = candidates[: cfg.max_candidates]
            cand = np.asarray(candidates, dtype=np.int64)

            fut, _, _ = state.future(t)
            pos_files: dict[int, dict] = {}
            for i in fut:
                f = int(state.fid[i])
                entry = pos_files.setdefault(
                    f, {"first": int(state.et[i]), "n": 0, "ranges": set(), "size": 0}
                )
                entry["n"] += 1
                entry["ranges"].add((int(state.off[i]), int(state.nbytes[i])))
                entry["size"] = max(entry["size"], int(state.size[i]))
            for entry in pos_files.values():
                entry["bytes"] = sum(nb for _, nb in entry["ranges"])
            horizon_complete = state.complete(t, t + HORIZON)
            cand_set = set(candidates)
            for f, entry in pos_files.items():
                if not state.seen[f]:
                    urow["n_positive_unknown_at_t"] += 1
                elif state.last_size[f] == 0:
                    urow["n_positive_ineligible"] += 1
                else:
                    urow["n_positive_files"] += 1
                    urow["positive_bytes"] += entry["bytes"]
                    if f in cand_set:
                        urow["n_positive_in_candidates"] += 1
                        urow["positive_bytes_in_candidates"] += entry["bytes"]
            urow["n_candidates"] = len(candidates)
            urow["n_ineligible"] = n_ineligible
            if urow["n_positive_files"]:
                coverage_ratio[split].append(urow["n_positive_in_candidates"] / urow["n_positive_files"])
            counts[split]["ticks"] += 1
            if not candidates:
                continue

            feats = state.features(t, cand, anchor)
            label = np.array([1 if f in pos_files else (0 if horizon_complete else -1) for f in candidates])
            known = label >= 0
            urow["n_unknown_labels"] = int((~known).sum())
            counts[split]["unknown_dropped"] += int((~known).sum())
            weight = np.ones(len(candidates))
            if split == "train":
                n_pos = int((label == 1).sum())
                negatives = np.flatnonzero(label == 0)
                q = min(1.0, max(cfg.min_negatives, cfg.neg_per_pos * n_pos) / len(negatives)) if len(negatives) else 1.0
                if q < 1.0:
                    for j in negatives:
                        if not _keep_negative(cfg.seed, sid, files[candidates[j]], q):
                            known[j] = False
                        else:
                            weight[j] = 1.0 / q
            sel = np.flatnonzero(known)
            if not len(sel):
                continue
            bucket = rows.setdefault((split, state.volume_id), {})
            sizes = state.last_size[cand[sel]]

            def put(name: str, values: list) -> None:
                bucket.setdefault(name, []).extend(values)

            put("snapshot_id", [sid] * len(sel))
            put("t", [_dt(t)] * len(sel))
            put("split", [split] * len(sel))
            put("volume_id", [state.volume_id] * len(sel))
            put("file_id", [files[candidates[j]] for j in sel])
            put("candidate_sources", [sources[candidates[j]] for j in sel])
            put("coaccess_fit_end", [None if fit_end is None else _dt(fit_end)] * len(sel))
            put("anchor_file_id", [None if anchor < 0 else files[anchor]] * len(sel))
            for name in feature_names:
                v = feats[name][sel]
                if name in FLOAT_FEATURES:
                    put(name, [None if np.isnan(x) else float(x) for x in v])
                else:
                    put(name, [None if np.isnan(x) else int(x) for x in v])
            put("file_size", [None if s < 0 else int(s) for s in sizes])
            put("label", [int(label[j]) for j in sel])
            lab = [pos_files.get(candidates[j]) for j in sel]
            put("label_first_time", [None if e is None else _dt(e["first"]) for e in lab])
            put("label_n_reads", [0 if e is None else e["n"] for e in lab])
            put("label_distinct_bytes", [0 if e is None else e["bytes"] for e in lab])
            put("label_max_file_size", [0 if e is None else e["size"] for e in lab])
            put("label_bytes_exact", [True] * len(sel))
            put("sample_weight", [float(weight[j]) for j in sel])
            counts[split]["rows"] += len(sel)
            counts[split]["positives"] += int((label[sel] == 1).sum())

    schema = pa.schema(
        [
            ("snapshot_id", pa.string()), ("t", TS), ("split", pa.string()),
            ("volume_id", pa.string()), ("file_id", pa.string()),
            ("candidate_sources", pa.list_(pa.string())), ("coaccess_fit_end", TS),
            ("anchor_file_id", pa.string()),
            *[(n, pa.float64() if n in FLOAT_FEATURES else pa.int64()) for n in feature_names],
            ("file_size", pa.int64()), ("label", pa.int8()), ("label_first_time", TS),
            ("label_n_reads", pa.int32()), ("label_distinct_bytes", pa.int64()),
            ("label_max_file_size", pa.int64()), ("label_bytes_exact", pa.bool_()),
            ("sample_weight", pa.float64()),
        ]
    )
    out.parent.mkdir(parents=True, exist_ok=True)
    stage = Path(tempfile.mkdtemp(prefix=f".{out.name}-staging-", dir=out.parent))
    try:
        for (split, volume), columns in rows.items():
            path = stage / "snapshots" / f"split={split}" / f"volume_id={volume}" / "part-00000.parquet"
            path.parent.mkdir(parents=True, exist_ok=True)
            pq.write_table(pa.table(columns, schema=schema), path)
        for volume, ticks in universe.items():
            path = stage / "universe" / f"volume_id={volume}" / "part-00000.parquet"
            path.parent.mkdir(parents=True, exist_ok=True)
            pq.write_table(pa.Table.from_pylist(ticks), path)
        (stage / "artifacts").mkdir()
        pq.write_table(
            pa.Table.from_pylist(
                [
                    {"from_file_id": files[a], "to_file_id": files[b], "support": s,
                     "n_from": n, "conf": c, "lift": li}
                    for a, b, s, n, c, li in coaccess_rows
                ],
                schema=COACCESS_SCHEMA,
            ),
            stage / "artifacts" / "coaccess.parquet",
        )
        (stage / "feature_schema.json").write_text(canonical_json(feature_schema()))
        contract = dict(READ_DEMAND_SERVING_CONTRACT)
        capabilities = {"verified_file_size": cfg.verified_file_size, "anchor_context": True}
        manifest = {
            "manifest_version": 1,
            "dataset_id": f"snapshots-{parent['dataset_id']}-{cfg.sha256()[:8]}",
            "data_kind": parent.get("data_kind", "unknown"),
            "stage": "snapshots",
            "parent": {
                "dataset_id": parent["dataset_id"],
                "manifest_sha256": hashlib.sha256(
                    (Path(clean_dir) / "manifest.json").read_bytes()
                ).hexdigest(),
            },
            "period": parent["period"],
            "feature_version": READ_DEMAND_FEATURE_VERSION,
            "semantic_view_id": SEMANTIC_VIEW_ID,
            "semantic_view_sha256": SEMANTIC_VIEW_SHA256,
            "feature_schema_sha256": READ_DEMAND_FEATURE_SCHEMA_SHA256,
            "serving_contract": {"config": contract, "sha256": contract_sha256(contract)},
            "serving_contract_sha256": READ_DEMAND_SERVING_CONTRACT_SHA256,
            "runtime_capabilities": capabilities,
            "runtime_available_features": sorted(runtime_available_features(**capabilities)),
            "labels": {"target": "demand", "lead_s": 0, "horizon_s": HORIZON // US},
            "splits": {"val_start": _iso(val_start), "test_start": _iso(test_start)},
            "candidate_profile": SEMANTIC_VIEW_ID,
            "candidates": {
                "sources": ["recent", "neighbor", "popular"],
                "recent_window_s": cfg.recent_window_s, "recent_cap": cfg.recent_cap,
                "neighbor_cap": cfg.neighbor_cap, "popular_window_s": cfg.popular_window_s,
                "popular_k": cfg.popular_k, "max_candidates": cfg.max_candidates,
            },
            "coaccess": {
                "coaccess_kind": "coread-direct-v1",
                "refit_every_s": cfg.refit_every_s, "train_refits": refits,
                "frozen_fit": {"start": _iso(start), "end": _iso(val_start)},
                "edges": len(coaccess_rows), "min_support": cfg.min_support,
                "min_lift": cfg.min_lift, "max_out_edges": cfg.max_out_edges,
            },
            "snapshots": {"tick_s": cfg.tick_s, "warmup_s": cfg.warmup_s, "seed": cfg.seed,
                          "neg_per_pos": cfg.neg_per_pos, "min_negatives": cfg.min_negatives,
                          "skew_margin_s": cfg.skew_margin_s, "config_sha256": cfg.sha256()},
            "counts": counts,
            "ticks_dropped": dropped,
            "candidate_coverage": {
                s: (float(np.mean(v)) if v else None) for s, v in coverage_ratio.items()
            },
            "note": "synthetic plumbing evidence; not real model-quality evidence"
            if parent.get("data_kind") != "real" else "",
        }
        (stage / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
        (stage / "report").mkdir()
        (stage / "report" / "stats.json").write_text(
            json.dumps({"counts": counts, "ticks_dropped": dropped,
                        "candidate_coverage": manifest["candidate_coverage"]}, indent=2) + "\n"
        )
        stage.rename(out)
    except BaseException:
        shutil.rmtree(stage, ignore_errors=True)
        raise
    return manifest
