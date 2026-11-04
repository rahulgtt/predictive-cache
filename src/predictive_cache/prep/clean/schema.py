"""Arrow output contracts for the cleaning stage."""

from __future__ import annotations

import pyarrow as pa

from predictive_cache.datagen.schema import EVENT_SCHEMA, TS

S = pa.string()
INT = pa.int64()
B = pa.bool_()


def fields(*items):
    return pa.schema([pa.field(name, typ, nullable=True) for name, typ in items])


EVENTS_CLEAN = EVENT_SCHEMA.append(pa.field("effective_time", TS)).append(pa.field("known_at", TS))
READ_EVENTS = fields(
    ("schema_version", pa.int16()),
    ("event_id", S),
    ("event_time", TS),
    ("known_at", TS),
    ("volume_id", S),
    ("file_id", S),
    ("dir_id", S),
    ("offset", INT),
    ("bytes_requested", INT),
    ("bytes_read", INT),
    ("file_size", INT),
    ("origin", S),
    ("demand_class", S),
    ("origin_trust", S),
    ("coverage_status", S),
    ("metadata_status", S),
    ("data_kind", S),
    ("protocol", S),
    ("operation", S),
)
EXCLUDED = fields(*[(f.name, f.type) for f in EVENT_SCHEMA], ("reason", S), ("detail", S))
OPENS = fields(
    ("open_id", S),
    ("volume_id", S),
    ("file_id", S),
    ("dir_id", S),
    ("session_id", S),
    ("cohort_id", S),
    ("client_id", S),
    ("app", S),
    ("open_time", TS),
    ("open_known_at", TS),
    ("close_time", TS),
    ("close_known_at", TS),
    ("completed_known_at", TS),
    ("file_size", INT),
    ("bytes_read", INT),
    ("distinct_bytes_read", INT),
    ("read_event_count", pa.int32()),
    ("full_read", B),
    ("consumed", B),
    ("open_missing", B),
    ("close_missing", B),
    ("bytes_inconsistent", B),
)
OPEN_RANGES = fields(
    ("open_id", S),
    ("volume_id", S),
    ("file_id", S),
    ("offset", INT),
    ("length", INT),
    ("effective_time", TS),
    ("known_at", TS),
    ("file_size", INT),
)
META = fields(
    ("event_id", S),
    ("volume_id", S),
    ("file_id", S),
    ("dir_id", S),
    ("operation", S),
    ("effective_time", TS),
    ("known_at", TS),
    ("file_size", INT),
)
COVERAGE = fields(
    ("volume_id", S),
    ("hour_start", TS),
    ("offered", INT),
    ("emitted", INT),
    ("dropped", INT),
    ("sampled_out", INT),
    ("loss_fraction", pa.float64()),
    ("status", S),
)
SCHEMAS = {
    "events_clean": EVENTS_CLEAN,
    "read_events": READ_EVENTS,
    "excluded": EXCLUDED,
    "opens": OPENS,
    "open_ranges": OPEN_RANGES,
    "meta": META,
    "coverage": COVERAGE,
}
