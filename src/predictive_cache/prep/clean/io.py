"""Clean-stage artifact IO with stable serialization."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from predictive_cache.datagen.io import canonical_json
from predictive_cache.prep.clean.schema import SCHEMAS


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def read_rows(path: Path) -> list[dict]:
    if path.suffix == ".parquet":
        return pq.ParquetFile(path).read().to_pylist()
    return [json.loads(line) for line in path.read_text().splitlines() if line]


def write_rows(path: Path, name: str, rows: list[dict]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    schema = SCHEMAS[name]
    table = pa.Table.from_pylist(rows, schema=schema)
    if path.suffix == ".parquet":
        pq.write_table(
            table,
            path,
            compression="zstd",
            use_dictionary=False,
            row_group_size=65536,
            version="2.6",
            coerce_timestamps="us",
            allow_truncated_timestamps=False,
        )
    else:
        with path.open("w", encoding="utf-8", newline="\n") as stream:
            for row in table.to_pylist():
                stream.write(canonical_json(row) + "\n")
    return len(rows)
