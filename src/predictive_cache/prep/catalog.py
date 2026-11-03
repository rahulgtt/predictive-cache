"""The catalog state that was already available at the period start."""

from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa

from predictive_cache.prep.clean.io import read_rows
from predictive_cache.prep.coverage import utc


class InitialCatalog:
    def __init__(self, period_start, files, directories, paths, volumes):
        self.period_start = period_start
        self._files = files
        self._directories = {r["dir_id"]: r for r in directories}
        self._volumes = {r["volume_id"]: r for r in volumes}
        self._paths = {}
        for row in paths:
            if utc(row["valid_from"]) <= period_start:
                old = self._paths.get(row["file_id"])
                if old is None or utc(row["valid_from"]) > utc(old["valid_from"]):
                    self._paths[row["file_id"]] = row

    @classmethod
    def load(cls, clean_dir: Path) -> "InitialCatalog | None":
        root = clean_dir / "catalog"
        if not root.exists():
            return None
        manifest = json.loads((clean_dir / "manifest.json").read_text())
        start = utc(manifest["period"]["start"])

        def table(name):
            paths = list(root.glob(f"{name}.*"))
            return read_rows(paths[0]) if paths else []

        files = [r for r in table("files") if utc(r["created_at"]) < start]
        directories = [r for r in table("directories") if utc(r["created_at"]) < start]
        return cls(start, files, directories, table("file_paths"), table("volumes"))

    def files(self, volume_id: str) -> pa.Table:
        rows = []
        for item in self._files:
            if item["volume_id"] != volume_id:
                continue
            path = self._paths.get(item["file_id"])
            rows.append(
                {
                    "file_id": item["file_id"],
                    "dir_id": path["dir_id"] if path else None,
                    "size": item["size_at_creation"],
                    "created_at": utc(item["created_at"]),
                    "kind": item["kind"],
                }
            )
        return pa.Table.from_pylist(
            rows,
            schema=pa.schema(
                [
                    ("file_id", pa.string()),
                    ("dir_id", pa.string()),
                    ("size", pa.int64()),
                    ("created_at", pa.timestamp("us", tz="UTC")),
                    ("kind", pa.string()),
                ]
            ),
        )

    def project_of_dir(self, dir_id: str) -> str | None:
        row = self._directories.get(dir_id)
        return row["project_id"] if row else None

    def parent_of_dir(self, dir_id: str) -> str | None:
        row = self._directories.get(dir_id)
        return row["parent_dir_id"] if row else None

    def timezone(self, volume_id: str) -> str | None:
        row = self._volumes.get(volume_id)
        return row["timezone"] if row else None
