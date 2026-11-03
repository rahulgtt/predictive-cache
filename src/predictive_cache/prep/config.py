"""Resolved configuration for the cleaning stage."""

from __future__ import annotations

from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class CoverageConfig(StrictModel):
    max_loss_fraction: float = Field(default=0.0, ge=0, le=1)
    skew_margin_s: int = Field(default=600, ge=0)
    assume_complete_without_counters: bool = False


class CleanConfig(StrictModel):
    max_future_skew_s: int = Field(default=3600, ge=0)
    max_late_s: int = Field(default=604800, ge=0)
    completion_grace_s: int = Field(default=60, ge=0)
    handle_timeout_s: int = Field(default=3600, gt=0)
    read_mode: Literal["ranges", "summary"] = "ranges"
    dedupe_window_s: int = Field(default=21600, gt=0)
    coverage: CoverageConfig = Field(default_factory=CoverageConfig)
    timezone_overrides: dict[str, str] = Field(default_factory=dict)


def load_config(path: Path | None = None) -> CleanConfig:
    if path is None:
        return CleanConfig()
    document = yaml.safe_load(path.read_text()) or {}
    if not isinstance(document, dict):
        raise ValueError("config must be a YAML mapping")
    return CleanConfig.model_validate(document.get("clean", {}))
