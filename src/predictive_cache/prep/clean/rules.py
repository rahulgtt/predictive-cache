"""Per-event classification shared by batch cleaning and the live Controller."""

from __future__ import annotations

from datetime import datetime

from predictive_cache.datagen.schema import normalize_rows
from predictive_cache.prep.config import CleanConfig


def classify_event(
    event: dict,
    known_at: datetime,
    cfg: CleanConfig,
    *,
    semantic_view: str = "generic-v1",
    admit_controlled_synthetic: bool = False,
) -> str | None:
    """Return the first applicable reason; duplicate detection belongs to the caller."""
    try:
        row = normalize_rows("events", [event])[0]
    except (ValueError, TypeError, KeyError, OverflowError):
        return "schema_invalid"
    if semantic_view == "read-demand-v1" and row["operation"] != "read":
        return "operation_unsupported"
    origin = row["origin"]
    if origin == "warmer":
        return "origin_warmer"
    if origin == "synthetic" and not admit_controlled_synthetic:
        return "origin_synthetic"
    if origin in (None, "unknown"):
        return "origin_unknown"
    if row["operation"] in ("read", "write"):
        offset, requested = row["offset"], row["bytes_requested"]
        if offset is None or requested is None or offset < 0 or requested < 0:
            return "invalid_range"
        if row["operation"] == "read":
            returned = row["bytes_read"]
            if (
                offset > row["file_size"]
                or (returned is None and row["result"] == "ok")
                or (returned is not None and returned < 0)
                or (returned is not None and returned > requested)
                or (
                    row["result"] == "ok"
                    and returned is not None
                    and offset + returned > row["file_size"]
                )
            ):
                return "invalid_range"
    if row["result"] != "ok":
        return "result_failed"
    if (row["event_time"] - known_at).total_seconds() > cfg.max_future_skew_s:
        return "time_invalid"
    if (known_at - row["event_time"]).total_seconds() > cfg.max_late_s:
        return "time_invalid"
    return None
