"""Run ``pcache-train`` as a ClearML task by file path (agents re-run this file).

Example (from a committed checkout with SDK credentials):

    uv run --extra clearml --extra ml python src/predictive_cache/clearml/train.py \
        --snapshots-dataset <dataset-id> --queue default
"""

from __future__ import annotations

import sys
from pathlib import Path

# The agent executes this tracked source file from a fresh repository clone.
if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))


def main() -> None:
    from predictive_cache.ml.train_cli import main as train_main

    train_main()


if __name__ == "__main__":
    main()
