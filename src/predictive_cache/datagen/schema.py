"""Versioned Arrow contracts and strict serialization validation."""

from __future__ import annotations

import json
import re
from datetime import date, datetime, timedelta
from functools import lru_cache
from pathlib import PurePosixPath
from typing import Any, Literal

import pyarrow as pa
import pyarrow.compute as pc
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

OPERATIONS = frozenset("open read close create write rename delete".split())
RESULTS = frozenset("ok enoent eacces eio einval".split())
ORIGINS = frozenset("foreground warmer synthetic unknown".split())
FILE_KINDS = frozenset("doc drawing cad_model image media dataset small".split())
FAULTS = frozenset(
    "duplicate late skew dropped sampled_out origin_unknown failed_result invalid_range identity_break".split()
)
PATTERN_KINDS = frozenset(
    "popularity weekly_schedule workflow_chain project_lifecycle burst batch_job new_file noise churn".split()
)
TS = pa.timestamp("us", tz="UTC")
S, INT = pa.string(), pa.int64()


def schema(fields):
    return pa.schema([pa.field(name, typ, nullable=nullable) for name, typ, nullable in fields])


EVENT_SCHEMA = schema(
    [
        ("schema_version", pa.int16(), False),
        ("event_id", S, False),
        ("event_time", TS, False),
        ("ingest_time", TS, False),
        ("volume_id", S, False),
        ("file_id", S, False),
        ("dir_id", S, False),
        ("operation", S, False),
        ("handle_id", S, True),
        ("offset", INT, True),
        ("bytes_requested", INT, True),
        ("bytes_read", INT, True),
        ("handle_bytes_read", INT, True),
        ("handle_duration_ms", INT, True),
        ("file_size", INT, False),
        ("result", S, False),
        ("origin", S, True),
        ("session_id", S, True),
        ("cohort_id", S, True),
        ("client_id", S, True),
        ("app", S, True),
        ("cache_outcome", S, True),
        ("latency_ms", INT, True),
    ]
)
SCHEMAS = {
    "events": EVENT_SCHEMA,
    "dropped_events": EVENT_SCHEMA,
    "volumes": schema(
        [
            ("volume_id", S, False),
            ("timezone", S, False),
            ("work_days", pa.list_(pa.int64()), False),
            ("work_start_local", S, False),
            ("work_end_local", S, False),
            ("holidays", pa.list_(pa.date32()), False),
            ("period_start", TS, False),
            ("period_end", TS, False),
        ]
    ),
    "directories": schema(
        [
            ("dir_id", S, False),
            ("volume_id", S, False),
            ("parent_dir_id", S, True),
            ("project_id", S, True),
            ("depth", INT, False),
            ("created_at", TS, False),
        ]
    ),
    "files": schema(
        [
            ("file_id", S, False),
            ("volume_id", S, False),
            ("kind", S, False),
            ("size_at_creation", INT, False),
            ("created_at", TS, False),
            ("deleted_at", TS, True),
            ("replaced_by_file_id", S, True),
            ("project_id", S, True),
        ]
    ),
    "file_paths": schema(
        [
            ("file_id", S, False),
            ("dir_id", S, False),
            ("path", S, False),
            ("valid_from", TS, False),
            ("valid_to", TS, True),
        ]
    ),
    "actors": schema(
        [
            ("actor_id", S, False),
            ("volume_id", S, False),
            ("kind", S, False),
            ("cohort_id", S, True),
            ("client_id", S, True),
            ("project_ids", pa.list_(S), False),
            ("schedule_start_local", S, False),
            ("schedule_end_local", S, False),
            ("work_days", pa.list_(INT), False),
        ]
    ),
    "loss_counters": schema(
        [
            ("volume_id", S, False),
            ("hour_start", TS, False),
            *[
                (n, INT, False)
                for n in "offered emitted dropped sampled_out duplicates late".split()
            ],
        ]
    ),
    "event_truth": schema(
        [
            ("event_id", S, False),
            ("intent_id", S, True),
            ("cause", S, False),
            ("faults", pa.list_(S), False),
            ("true_event_time", TS, False),
            ("true_file_id", S, False),
            ("original_fields_json", S, False),
        ]
    ),
    "patterns": schema(
        [
            ("pattern_id", S, False),
            ("kind", S, False),
            ("volume_id", S, False),
            ("active_from", TS, False),
            ("active_to", TS, True),
            ("actors", pa.list_(S), False),
            ("files", pa.list_(S), False),
            ("params", S, False),
        ]
    ),
}
ENUMS = {
    "events": {"operation": OPERATIONS, "result": RESULTS, "origin": ORIGINS},
    "dropped_events": {"operation": OPERATIONS, "result": RESULTS, "origin": ORIGINS},
    "files": {"kind": FILE_KINDS},
    "actors": {"kind": {"user", "service"}},
    "patterns": {"kind": PATTERN_KINDS},
}


def utc_datetime(value):
    if isinstance(value, str):
        if not re.fullmatch(
            r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|\+00:00)", value
        ):
            raise ValueError("timestamp must use UTC and at most microsecond precision")
        value = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError("timestamp must be UTC and timezone-aware")
    return value


@lru_cache(maxsize=None)
def _checker(typ):
    """Compile the fixed Arrow contract once, not once per cell."""
    if pa.types.is_timestamp(typ):
        return utc_datetime
    if pa.types.is_integer(typ):
        bound = 2 ** (typ.bit_width - 1)

        def integer(value):
            if type(value) is not int or not -bound <= value < bound:
                raise ValueError("expected in-range integer")
            return value

        return integer
    if pa.types.is_string(typ):

        def string(value):
            if not isinstance(value, str):
                raise ValueError("expected string")
            return value

        return string
    if pa.types.is_date32(typ):

        def day(value):
            if isinstance(value, str):
                value = date.fromisoformat(value)
            if type(value) is not date:
                raise ValueError("expected date")
            return value

        return day
    if pa.types.is_list(typ):
        item = _checker(typ.value_type)

        def items(value):
            if not isinstance(value, list) or any(v is None for v in value):
                raise ValueError("expected non-null list elements")
            return [item(v) for v in value]

        return items
    raise ValueError(f"unsupported contract type: {typ}")


@lru_cache(maxsize=4096)
def _canonical_object(raw):
    obj = json.loads(raw)
    if (
        not isinstance(obj, dict)
        or json.dumps(obj, sort_keys=True, separators=(",", ":"), allow_nan=False) != raw
    ):
        raise ValueError("expected a canonical JSON object")


def validate_table(name, table):
    """Validate already typed Parquet columns without a Python row round trip."""
    contract = SCHEMAS[name]
    if not table.schema.equals(contract, check_metadata=False):
        raise ValueError(f"Arrow schema mismatch for {name}")
    table.validate(full=True)
    for field in contract:
        column = table[field.name]
        if not field.nullable and column.null_count:
            raise ValueError(f"{name}.{field.name}: null required value")
        if pa.types.is_list(field.type) and pc.list_flatten(column).null_count:
            raise ValueError(f"{name}.{field.name}: null list element")
        allowed = ENUMS.get(name, {}).get(field.name)
        if (
            allowed is not None
            and pc.any(
                pc.invert(pc.is_in(column.drop_null(), value_set=pa.array(sorted(allowed))))
            ).as_py()
        ):
            raise ValueError(f"invalid {field.name}")
    if name in ("events", "dropped_events"):
        if pc.any(pc.not_equal(table["schema_version"], 1)).as_py():
            raise ValueError("unsupported event schema version")
        if any(table[k].null_count != table.num_rows for k in ("cache_outcome", "latency_ms")):
            raise ValueError("reserved cache/latency fields must be null")
    if name == "event_truth":
        if pc.any(
            pc.invert(
                pc.is_in(pc.list_flatten(table["faults"]), value_set=pa.array(sorted(FAULTS)))
            )
        ).as_py():
            raise ValueError("unknown fault tag")
    json_field = (
        "original_fields_json"
        if name == "event_truth"
        else "params"
        if name == "patterns"
        else None
    )
    if json_field:
        for value in pc.unique(table[json_field]).to_pylist():
            _canonical_object(value)
    return table


def normalize_rows(name: str, rows) -> list[dict[str, Any]]:
    """Validate rows and return Arrow-ready copies; accept ISO JSON dates/times."""
    result = []
    contract = SCHEMAS[name]
    names = frozenset(contract.names)
    fields = [
        (f.name, f.nullable, _checker(f.type), ENUMS.get(name, {}).get(f.name)) for f in contract
    ]
    json_field = (
        "original_fields_json"
        if name == "event_truth"
        else "params"
        if name == "patterns"
        else None
    )
    for index, row in enumerate(rows):
        if not isinstance(row, dict) or row.keys() - names:
            raise ValueError(f"{name}[{index}]: unknown columns or invalid row")
        normalized = {}
        for field, nullable, check, allowed in fields:
            value = row.get(field)
            if value is None:
                if not nullable:
                    raise ValueError(f"{name}[{index}].{field}: null/missing required value")
            else:
                if name == "patterns" and field == "params" and isinstance(value, dict):
                    value = json.dumps(
                        value, sort_keys=True, separators=(",", ":"), allow_nan=False
                    )
                value = check(value)
                if allowed is not None and value not in allowed:
                    raise ValueError(f"invalid {field}: {value}")
            normalized[field] = value
        if name in ("events", "dropped_events"):
            if normalized["schema_version"] != 1:
                raise ValueError("unsupported event schema version")
            if normalized["cache_outcome"] is not None or normalized["latency_ms"] is not None:
                raise ValueError("reserved cache/latency fields must be null")
        if name == "event_truth" and not set(normalized["faults"]) <= FAULTS:
            raise ValueError("unknown fault tag")
        if json_field:
            _canonical_object(normalized[json_field])
        result.append(normalized)
    return result


def validate_rows(name: str, rows) -> None:
    normalize_rows(name, rows)


def rows_to_table(name: str, rows) -> pa.Table:
    return pa.Table.from_pylist(normalize_rows(name, rows), schema=SCHEMAS[name])


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class GeneratorInfo(StrictModel):
    name: Literal["pcache-datagen"] = "pcache-datagen"
    version: str
    git_sha: str
    runtime: dict[str, str] = Field(default_factory=dict)


class Period(StrictModel):
    start: datetime
    end: datetime

    _utc = field_validator("start", "end", mode="before")(utc_datetime)

    @model_validator(mode="after")
    def ordered(self):
        if self.end <= self.start:
            raise ValueError("period end must follow start")
        return self


class Counts(StrictModel):
    events: int = Field(ge=0)
    dropped_events: int = Field(ge=0)
    files: int = Field(ge=0)
    actors: int = Field(ge=0)
    patterns: int = Field(ge=0)
    by_operation: dict[str, int] = Field(default_factory=dict)
    by_origin: dict[str, int] = Field(default_factory=dict)
    by_fault: dict[str, int] = Field(default_factory=dict)

    @field_validator("by_operation", "by_origin", "by_fault")
    @classmethod
    def nonnegative(cls, values):
        if any(value < 0 for value in values.values()):
            raise ValueError("counts cannot be negative")
        return values


class KnownGap(Period):
    volume_id: str
    reason: str
    loss_fraction: float = Field(ge=0, le=1)


class FileEntry(StrictModel):
    path: str
    rows: int | None = Field(default=None, ge=0)
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")

    @field_validator("path")
    @classmethod
    def relative_path(cls, value):
        path = PurePosixPath(value)
        if (
            not value
            or path.is_absolute()
            or ".." in path.parts
            or "\\" in value
            or str(path) != value
        ):
            raise ValueError("expected safe relative POSIX path")
        return value


class Manifest(StrictModel):
    manifest_version: Literal[1] = 1
    dataset_id: str
    data_kind: Literal["synthetic"] = "synthetic"
    generator: GeneratorInfo
    event_schema_version: Literal[1] = 1
    seed: int = Field(ge=0)
    preset: Literal["tiny", "small", "medium", "custom"]
    config_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    period: Period
    volumes: list[str]
    counts: Counts
    known_gaps: list[KnownGap] = Field(default_factory=list)
    files: list[FileEntry]
    created_at: datetime
    limitations: list[str] = Field(default_factory=list)

    _utc = field_validator("created_at", mode="before")(utc_datetime)
