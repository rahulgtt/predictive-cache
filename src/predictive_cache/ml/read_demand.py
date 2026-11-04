"""Read-demand fv1 features with canonical event and as-of semantics."""
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

from predictive_cache.semantics.read_demand import ReadDemandEvent, canonical_json

READ_DEMAND_FEATURE_VERSION = "read-demand-fv1"
READ_DEMAND_FEATURES = (
    "read_count_5m",
    "read_count_15m",
    "read_count_1h",
    "read_count_24h",
    "read_count_7d",
    "returned_bytes_5m",
    "returned_bytes_1h",
    "returned_bytes_24h",
    "returned_bytes_7d",
    "distinct_read_ranges_1h",
    "distinct_read_ranges_24h",
    "secs_since_last_read",
    "read_days_7d",
    "volume_read_share_1h",
    "coread_from_anchor_24h",
    "file_size_bytes",
)
FORBIDDEN_FEATURE_NAMES = ("open", "session", "lifecycle", "write")


def validate_feature_names(names: Any) -> tuple[str, ...]:
    names = tuple(names)
    bad = [
        name
        for name in names
        if name not in READ_DEMAND_FEATURES
        or any(token in name.lower() for token in FORBIDDEN_FEATURE_NAMES)
    ]
    if bad:
        raise ValueError(f"unsupported read-demand features: {sorted(set(bad))}")
    return names


def runtime_available_features(
    *,
    verified_file_size: bool = False,
    anchor_context: bool = True,
) -> frozenset[str]:
    available = set(READ_DEMAND_FEATURES)
    if not verified_file_size:
        available.remove("file_size_bytes")
    if not anchor_context:
        available.remove("coread_from_anchor_24h")
    return frozenset(available)


def feature_schema() -> dict:
    return {
        "feature_version": READ_DEMAND_FEATURE_VERSION,
        "features": [
            {
                "name": name,
                "available": True,
                "semantic": "metadata" if name == "file_size_bytes" else "read",
            }
            for name in READ_DEMAND_FEATURES
        ],
    }


def feature_schema_sha256(schema: dict | None = None) -> str:
    import hashlib

    return hashlib.sha256(canonical_json(schema or feature_schema()).encode()).hexdigest()


READ_DEMAND_FEATURE_SCHEMA_SHA256 = feature_schema_sha256()
READ_DEMAND_SERVING_CONTRACT = {
    "semantic_view_id": "read-demand-v1",
    "feature_version": READ_DEMAND_FEATURE_VERSION,
    "target": "demand",
    "lead_s": 0,
    "horizon_s": 300,
    "coread_transition_seconds": 300,
    "coread_semantics": "direct_predecessor_same_volume",
    "time_semantics": "known_at_as_of_event_time_windows",
    "coverage_semantics": "per_feature_complete_windows",
}
READ_DEMAND_SERVING_CONTRACT_SHA256 = feature_schema_sha256(
    READ_DEMAND_SERVING_CONTRACT
)


class ReadDemandEngine:
    """One canonical-event state machine for replay and incremental serving."""

    def __init__(self) -> None:
        self.events: list[ReadDemandEvent] = []
        self.coverage: dict[tuple[str, datetime], str] = {}

    def update_coverage(self, volume_id: str, bucket: datetime, status: str) -> None:
        self.coverage[(volume_id, bucket)] = status

    def update(self, event: ReadDemandEvent | dict) -> None:
        row = event if isinstance(event, ReadDemandEvent) else ReadDemandEvent.model_validate(event)
        if row.origin in {"warmer", "unknown"} or row.demand_class in {"warmer", "unknown"}:
            return
        self.events.append(row)

    def _complete(self, volume_id: str, start: datetime, end: datetime) -> bool:
        bucket = start.replace(minute=0, second=0, microsecond=0)
        while bucket < end:
            if self.coverage.get((volume_id, bucket), "unknown") != "complete":
                return False
            bucket += timedelta(hours=1)
        return True

    def replay(self, events, ticks, **kwargs):
        output = []
        cursor = 0
        for tick in ticks:
            while cursor < len(events):
                row = events[cursor]
                known = row.known_at if isinstance(row, ReadDemandEvent) else row["known_at"]
                if known > tick:
                    break
                self.update(row)
                cursor += 1
            output.append(self.features(tick, **kwargs))
        return output

    def features(
        self,
        now: datetime,
        volume_id: str | None = None,
        file_id: str | None = None,
        anchor_file_id: str | None = None,
    ) -> dict:
        visible = [event for event in self.events if event.known_at <= now]
        rows = [event for event in visible if volume_id is None or event.volume_id == volume_id]
        mine = [event for event in rows if file_id is None or event.file_id == file_id]

        def window(seconds: int, values=mine):
            return [
                event
                for event in values
                if 0 <= (now - event.event_time).total_seconds() <= seconds
            ]

        def complete(seconds: int) -> bool:
            return volume_id is not None and self._complete(volume_id, now - timedelta(seconds=seconds), now)

        def value(seconds: int, result):
            return result if complete(seconds) else None

        result = {name: None for name in READ_DEMAND_FEATURES}
        result.update(
            {
                "read_count_5m": value(300, len(window(300))),
                "read_count_15m": value(900, len(window(900))),
                "read_count_1h": value(3600, len(window(3600))),
                "read_count_24h": value(86400, len(window(86400))),
                "read_count_7d": value(604800, len(window(604800))),
                "returned_bytes_5m": value(300, sum(e.bytes_read for e in window(300))),
                "returned_bytes_1h": value(3600, sum(e.bytes_read for e in window(3600))),
                "returned_bytes_24h": value(
                    86400, sum(e.bytes_read for e in window(86400))
                ),
                "returned_bytes_7d": value(
                    604800, sum(e.bytes_read for e in window(604800))
                ),
                "distinct_read_ranges_1h": value(
                    3600, len({(e.offset, e.bytes_read) for e in window(3600)})
                ),
                "distinct_read_ranges_24h": value(
                    86400, len({(e.offset, e.bytes_read) for e in window(86400)})
                ),
                "secs_since_last_read": value(
                    604800,
                    None
                    if not mine
                    else (now - max(e.event_time for e in mine)).total_seconds(),
                ),
                "read_days_7d": value(
                    604800, len({e.event_time.date() for e in window(604800)})
                ),
                "file_size_bytes": mine[-1].file_size if mine else None,
            }
        )
        volume_window = [e for e in rows if 0 <= (now - e.event_time).total_seconds() <= 3600]
        result["volume_read_share_1h"] = value(
            3600, len(window(3600)) / len(volume_window) if volume_window else 0.0
        )
        if anchor_file_id is not None and complete(86400):
            ordered = sorted(
                [
                    event
                    for event in rows
                    if 0 <= (now - event.event_time).total_seconds() <= 86400
                ],
                key=lambda event: (event.event_time, event.event_id),
            )
            result["coread_from_anchor_24h"] = sum(
                previous.file_id == anchor_file_id
                and target.file_id == file_id
                and 0
                <= (target.event_time - previous.event_time).total_seconds()
                <= 300
                for previous, target in zip(ordered, ordered[1:])
            )
        return result

    def label(
        self,
        tick: datetime,
        volume_id: str,
        file_id: str,
        *,
        horizon_s: int = 300,
        as_of: datetime,
    ) -> int | None:
        """Return a label for the explicit ``(volume_id, file_id)`` identity."""
        horizon_end = tick + timedelta(seconds=horizon_s)
        if as_of < tick:
            return None
        future = [
            event
            for event in self.events
            if event.volume_id == volume_id
            and event.file_id == file_id
            and tick < event.event_time <= horizon_end
            and event.known_at <= as_of
        ]
        if future:
            return 1
        if as_of < horizon_end or not self._complete(volume_id, tick, horizon_end):
            return None
        return 0
