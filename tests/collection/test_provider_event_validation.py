from __future__ import annotations

import base64
import hashlib
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from predictive_cache.collection.harness import _validate_provider_event
from predictive_cache.collection.io import sha256
from predictive_cache.collection.models import (
    CollectionManifest,
    CollectorDurableEvent,
    ExecutionResult,
    OfferedOperation,
    RetentionPolicy,
    SoftwareVersions,
    StopRules,
)

START = datetime(2026, 11, 4, tzinfo=UTC)
POD = "33333333-3333-4333-8333-333333333333"
EPOCH = "11111111-1111-4111-8111-111111111111"
OTHER_EPOCH = "22222222-2222-4222-8222-222222222222"
VOLUME = "a" * 32
FILE = "b" * 64
EVENT_SCHEMA = Path(__file__).parents[2] / "contracts" / "nfs4-read-v2.schema.json"


def manifest(**updates) -> CollectionManifest:
    values = {
        "run_id": "controlled-001",
        "run_ref": hashlib.sha256(b"controlled-001").hexdigest()[:32],
        "cluster_id": "cluster",
        "namespace": "ns",
        "volume_id": VOLUME,
        "export_id": 7,
        "cluster_manifest_sha256": "1" * 64,
        "fixture_catalog_sha256": "2" * 64,
        "event_schema_sha256": "3" * 64,
        "budget_evidence_sha256": "4" * 64,
        "seed": 32,
        "start_time": START,
        "end_time": START + timedelta(hours=5),
        "offered_rate_per_s": 1 / 60,
        "offered_count": 1,
        "range_bytes": (4096,),
        "range_offsets": (0,),
        "client_id": "client",
        "tag_mode": "empty",
        "software": SoftwareVersions(
            predictive_cache_revision="test",
            chimera_image="image",
            chimera_image_sha256="7" * 64,
            cufs_image="image",
            cufs_image_sha256="8" * 64,
            collector_schema_version="v2",
        ),
        "stop_rules": StopRules(latency_guardrail_ms=5000),
        "retention": RetentionPolicy(
            owner="owner",
            cleanup_owner="owner",
            cleanup_action="delete",
        ),
    }
    values.update(updates)
    return CollectionManifest(**values)


def evidence(tmp_path):
    root = tmp_path
    root.chmod(0o700)
    schema = root / "event-schema-v2.json"
    schema.write_bytes(EVENT_SCHEMA.read_bytes())
    schema.chmod(0o600)
    raw_dir = root / "collector" / POD / EPOCH
    raw_dir.mkdir(parents=True)
    raw = (
        '{"client_ref":"'
        + ("c" * 64)
        + '","compound_op_index":0,"compound_tag_b64":"","compound_tag_id":"'
        + hashlib.sha256(b"").hexdigest()
        + '","compound_tag_original_length":0,"compound_tag_truncated":false,'
        '"eof":false,"event_time_ns":"1","export_id":7,'
        '"export_name_b64":"Y29udHJvbGxlZA","file_id":"'
        + FILE
        + '","file_size":"4096","file_size_valid":true,'
        '"local_id":"44444444-4444-4444-8444-444444444444",'
        '"identity_valid":true,"nfs_status":0,"operation_id":"'
        + ("9" * 64)
        + '","origin_claim":"unknown","origin_trust":"client_asserted",'
        '"outcome":"success","producer_epoch":"'
        + EPOCH
        + '","producer_sequence":"1","requested_count":4096,'
        '"requested_offset":"0","returned_bytes":4096,"sequence_id":0,'
        '"session_ref":"","sink_time_ns":"1","slot_id":0,'
        '"schema":"nfs4_read_v2","target_created_ns":"1","target_fh":"AA",'
        '"volume_id":"'
        + VOLUME
        + '"}'
    ).encode()
    path = raw_dir / "events-00001.jsonl"
    path.write_bytes(raw + b"\n")
    operation = OfferedOperation(
        operation_id="8" * 64,
        operation_index=0,
        run_id="controlled-001",
        fixture_id="fixture",
        offered_at_utc=START,
        offered_at_monotonic_ns=1,
        volume_id=VOLUME,
        file_id=FILE,
        dir_id="dir",
        export_id=7,
        relative_path="fixture.bin",
        file_size=4096,
        offset=0,
        count=4096,
        client_id="client",
        tag="",
        timing_mode="wallclock",
    )
    result = ExecutionResult(
        operation_id=operation.operation_id,
        completed_at_utc=START,
        status="ok",
        bytes_read=4096,
        latency_ms=1,
        executor_kind="direct-mount",
        filesystem_type="nfs4",
        content_sha256="a" * 64,
    )
    event = CollectorDurableEvent(
        run_id=operation.run_id,
        event_id="44444444-4444-4444-8444-444444444444",
        pod_name="pod",
        pod_uid=POD,
        node_name="node",
        producer_epoch=EPOCH,
        producer_sequence=1,
        server_operation_id="9" * 64,
        sink_time_ns=1,
        volume_id=VOLUME,
        file_id=FILE,
        export_id=7,
        export_name="controlled",
        offset=0,
        count=4096,
        returned_bytes=4096,
        nfs_status=0,
        outcome="success",
        eof=False,
        durable_at=START,
        tag="",
        raw_jsonl_path=f"collector/{POD}/{EPOCH}/events-00001.jsonl",
        raw_jsonl_sha256=sha256(path),
        raw_line_number=1,
        raw_record_sha256=hashlib.sha256(raw).hexdigest(),
    )
    identities = {("pod", POD, "node", EPOCH)}
    return operation, result, event, identities


def rewrite_raw(root, event, raw, *, lines=None, line_number=1):
    encoded = json.dumps(raw, sort_keys=True, separators=(",", ":")).encode()
    records = lines or [encoded]
    path = root / event.raw_jsonl_path
    path.write_bytes(b"\n".join(records) + b"\n")
    return event.model_copy(
        update={
            "raw_jsonl_sha256": sha256(path),
            "raw_line_number": line_number,
            "raw_record_sha256": hashlib.sha256(records[line_number - 1]).hexdigest(),
        }
    )


def test_provider_event_rejects_tamper_epoch_tag_and_status(tmp_path):
    operation, result, event, identities = evidence(tmp_path)
    _validate_provider_event(tmp_path, manifest(), operation, result, event, identities)
    with pytest.raises(RuntimeError, match="differ"):
        _validate_provider_event(
            tmp_path,
            manifest(),
            operation,
            result,
            event.model_copy(update={"server_operation_id": "f" * 64}),
            identities,
        )
    with pytest.raises(RuntimeError, match="checkpoint"):
        _validate_provider_event(
            tmp_path,
            manifest(),
            operation,
            result,
            event.model_copy(update={"producer_epoch": OTHER_EPOCH}),
            identities,
        )
    with pytest.raises(RuntimeError, match="status/tag"):
        _validate_provider_event(
            tmp_path,
            manifest(),
            operation,
            result,
            event.model_copy(update={"tag": "not-empty"}),
            identities,
        )
    with pytest.raises(RuntimeError, match="status/tag"):
        _validate_provider_event(
            tmp_path,
            manifest(),
            operation,
            result,
            event.model_copy(update={"nfs_status": 5}),
            identities,
        )
    raw_path = tmp_path / event.raw_jsonl_path
    original = raw_path.read_bytes()
    for old, new in (
        (b'"compound_tag_truncated":false', b'"compound_tag_truncated":true'),
        (b'"identity_valid":true', b'"identity_valid":false'),
    ):
        mutated = original.replace(old, new)
        raw_path.write_bytes(mutated)
        raw_line = mutated.rstrip(b"\n")
        mutated_event = event.model_copy(
            update={
                "raw_jsonl_sha256": sha256(raw_path),
                "raw_record_sha256": hashlib.sha256(raw_line).hexdigest(),
            }
        )
        with pytest.raises(RuntimeError, match="unavailable"):
            _validate_provider_event(
                tmp_path,
                manifest(),
                operation,
                result,
                mutated_event,
                identities,
            )
        raw_path.write_bytes(original)


@pytest.mark.parametrize(
    "mutation",
    ["missing", "extra", "wrong_origin", "padded_base64url", "noncanonical_base64url"],
)
def test_provider_event_rejects_schema_and_trust_mismatch(tmp_path, mutation):
    operation, result, event, identities = evidence(tmp_path)
    raw = json.loads((tmp_path / event.raw_jsonl_path).read_text())
    if mutation == "missing":
        raw.pop("client_ref")
    elif mutation == "extra":
        raw["unexpected"] = True
    elif mutation == "wrong_origin":
        raw["origin_claim"] = "controlled_demand"
    elif mutation == "padded_base64url":
        raw["target_fh"] = "AA=="
    else:
        raw["target_fh"] = "AB"
    mutated = rewrite_raw(tmp_path, event, raw)
    with pytest.raises(RuntimeError, match="unavailable"):
        _validate_provider_event(
            tmp_path,
            manifest(),
            operation,
            result,
            mutated,
            identities,
        )


def test_provider_event_rejects_hashed_wrong_line(tmp_path):
    operation, result, event, identities = evidence(tmp_path)
    raw = json.loads((tmp_path / event.raw_jsonl_path).read_text())
    wrong = raw | {"local_id": "55555555-5555-4555-8555-555555555555"}
    wrong_bytes = json.dumps(wrong, sort_keys=True, separators=(",", ":")).encode()
    right_bytes = json.dumps(raw, sort_keys=True, separators=(",", ":")).encode()
    mutated = rewrite_raw(
        tmp_path,
        event,
        wrong,
        lines=[wrong_bytes, right_bytes],
        line_number=1,
    )
    with pytest.raises(RuntimeError, match="differ"):
        _validate_provider_event(
            tmp_path,
            manifest(),
            operation,
            result,
            mutated,
            identities,
        )


def test_provider_event_accepts_structured_controlled_demand_tag(tmp_path):
    operation, result, event, identities = evidence(tmp_path)
    controlled_manifest = manifest(tag_mode="pc1:d")
    tag = (
        f"pc1:d:{controlled_manifest.run_ref}:"
        f"{operation.operation_id[:32]}:{operation.operation_id[32:]}"
    )
    operation = operation.model_copy(update={"tag": tag})
    raw = json.loads((tmp_path / event.raw_jsonl_path).read_text())
    tag_bytes = tag.encode()
    raw.update(
        {
            "compound_tag_b64": base64.urlsafe_b64encode(tag_bytes)
            .rstrip(b"=")
            .decode(),
            "compound_tag_id": hashlib.sha256(tag_bytes).hexdigest(),
            "compound_tag_original_length": len(tag_bytes),
            "origin_claim": "controlled_demand",
        }
    )
    event = rewrite_raw(tmp_path, event.model_copy(update={"tag": tag}), raw)
    _validate_provider_event(
        tmp_path,
        controlled_manifest,
        operation,
        result,
        event,
        identities,
    )
