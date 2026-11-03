"""Hashing helpers with no dependencies beyond the standard library.

Kept separate from datagen.io so runtime model loading/scoring needs only
numpy and pyarrow. Canonical JSON matches datagen.io.canonical_json for plain
JSON values: sorted keys, no whitespace, no NaN/inf.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()
