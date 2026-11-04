"""Parity and regression tests for the additive read-demand-v1 contract."""

from __future__ import annotations

import base64
import hashlib
import json
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

from predictive_cache.datagen.config import load_config
from predictive_cache.datagen.io import canonical_json, config_hash
from predictive_cache.datagen.pipeline import run as run_datagen
from predictive_cache.prep.clean.io import read_rows
from predictive_cache.prep.clean.pipeline import _event_coverage_status, run_clean
from predictive_cache.prep.config import CleanConfig
from predictive_cache.semantics.read_demand import (
    READ_DEMAND_CONTRACT,
    SEMANTIC_VIEW_ID,
    SEMANTIC_VIEW_SHA256,
    ReadDemandEvent,
    project_chimera_read,
    project_datagen_read,
    semantic_payload,
)
from predictive_cache.sources.chimera.contract import ChimeraEvent

NOW = datetime(2026, 1, 5, 12, tzinfo=UTC)


def _b64(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode().rstrip("=")


def _chimera_event() -> ChimeraEvent:
    tag = ""
    return ChimeraEvent.model_validate(
        {
            "schema": "nfs4_read_v2",
            "local_id": str(uuid.UUID(int=1)),
            "producer_epoch": "11111111-2222-4333-8444-555555555555",
            "producer_sequence": "1",
            "event_time_ns": str(int(NOW.timestamp() * 1_000_000_000)),
            "sink_time_ns": str(int((NOW + timedelta(milliseconds=1)).timestamp() * 1_000_000_000)),
            "target_created_ns": "1",
            "volume_id": "01" * 16,
            "file_id": "02" * 32,
            "identity_valid": True,
            "export_name_b64": _b64(b"export"),
            "export_id": 1,
            "target_fh": _b64(b"handle"),
            "operation_id": "03" * 32,
            "client_ref": "04" * 32,
            "session_ref": "05" * 32,
            "compound_op_index": 0,
            "slot_id": 0,
            "sequence_id": 1,
            "compound_tag_b64": _b64(tag.encode()),
            "compound_tag_original_length": 0,
            "compound_tag_truncated": False,
            "compound_tag_id": hashlib.sha256(tag.encode()).hexdigest(),
            "origin_claim": "controlled_demand",
            "origin_trust": "client_asserted",
            "requested_offset": "10",
            "requested_count": 20,
            "returned_bytes": 20,
            "nfs_status": 0,
            "outcome": "success",
            "eof": False,
            "file_size_valid": True,
            "file_size": "100",
        }
    )


def test_contract_file_and_hash_are_stable():
    contract_path = Path(__file__).parents[2] / "contracts/read-demand-v1.json"
    assert json.loads(contract_path.read_text()) == READ_DEMAND_CONTRACT
    assert hashlib.sha256(canonical_json(READ_DEMAND_CONTRACT).encode()).hexdigest() == (
        SEMANTIC_VIEW_SHA256
    )


def test_datagen_and_chimera_same_read_intent_have_equal_semantic_payload():
    raw = _chimera_event()
    datagen = project_datagen_read(
        {
            "event_id": "fixture-event",
            "event_time": NOW,
            "ingest_time": NOW,
            "volume_id": raw.volume_id,
            "file_id": raw.file_id,
            "dir_id": "dir-1",
            "operation": "read",
            "offset": 10,
            "bytes_requested": 20,
            "bytes_read": 20,
            "file_size": 100,
            "result": "ok",
            "origin": "foreground",
        }
    )
    chimera = project_chimera_read(
        raw,
        durable_at=NOW + timedelta(milliseconds=2),
        dir_id="dir-1",
        file_size=100,
    )
    assert semantic_payload(datagen) == semantic_payload(chimera)
    assert datagen.demand_class == "fixture_demand"
    assert chimera.demand_class == "controlled_demand"
    assert chimera.origin == "synthetic"
    assert "session_id" not in chimera.model_dump()
    assert "handle_id" not in chimera.model_dump()


def test_missing_metadata_stays_missing():
    row = ReadDemandEvent(
        event_id="event-1",
        event_time=NOW,
        known_at=NOW,
        volume_id="volume-1",
        file_id="file-1",
        dir_id=None,
        offset=0,
        bytes_requested=10,
        bytes_read=10,
        file_size=None,
        origin="foreground",
        demand_class="foreground",
        origin_trust="trusted",
        coverage_status="complete",
        metadata_status="missing",
        data_kind="real",
    )
    assert row.file_size is None
    assert row.dir_id is None
    assert row.eligible_real_demand


def test_coverage_uses_event_hour_not_late_known_at_hour():
    event_time = NOW
    event = {
        "volume_id": "volume-1",
        "event_time": event_time,
        "known_at": event_time + timedelta(hours=1),
    }
    coverage = {
        ("volume-1", event_time): "complete",
        ("volume-1", event_time + timedelta(hours=1)): "incomplete",
    }
    assert _event_coverage_status(event, coverage) == "complete"


def test_read_clean_mode_emits_no_lifecycle_rows_and_preserves_config_hash(tmp_path: Path):
    source = tmp_path / "source"
    cfg = load_config(preset="tiny")
    cfg.period.weeks = 1
    cfg.files.target_count = 30
    cfg.actors.users = 2
    run_datagen(cfg, source)
    clean_cfg = CleanConfig()
    expected_hash = config_hash(clean_cfg)
    output = tmp_path / "read-clean"
    manifest = run_clean(
        source,
        clean_cfg,
        output,
        format="jsonl",
        semantic_view=SEMANTIC_VIEW_ID,
    )
    assert manifest["config_sha256"] == expected_hash
    assert manifest["semantic_view_id"] == SEMANTIC_VIEW_ID
    assert manifest["semantic_view_sha256"] == SEMANTIC_VIEW_SHA256
    assert manifest["counts"]["opens"] == 0
    assert manifest["counts"]["open_ranges"] == 0
    assert manifest["counts"]["read_events"] > 0
    prefixes = {entry["path"].split("/")[0] for entry in manifest["files"]}
    assert {"opens", "open_ranges", "read_events"} <= prefixes
    assert sum(entry["rows"] for entry in manifest["files"] if entry["path"].startswith("opens/")) == 0
    assert (
        sum(
            entry["rows"]
            for entry in manifest["files"]
            if entry["path"].startswith("open_ranges/")
        )
        == 0
    )
    rows = [
        row
        for entry in manifest["files"]
        if entry["path"].startswith("read_events/")
        for row in read_rows(output / entry["path"])
    ]
    assert rows
    assert {row["operation"] for row in rows} == {"read"}
    assert all("open_id" not in row for row in rows)
