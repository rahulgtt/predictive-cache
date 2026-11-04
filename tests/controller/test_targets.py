"""Fictional private execution-target contract checks."""

import base64
import hashlib
import json
from datetime import UTC, datetime, timedelta

import pytest

from predictive_cache.controller.targets import TargetMap
from predictive_cache.sources.chimera.contract import ChimeraEvent

NOW = datetime(2026, 1, 5, tzinfo=UTC)
VOLUME = "01" * 16
FILE = "02" * 32
HANDLE = base64.urlsafe_b64encode(b"private-fictional-handle").decode().rstrip("=")


def event(**changes):
    row = dict(
        schema="nfs4_read_v2",
        local_id="11111111-1111-4111-8111-111111111111",
        producer_epoch="22222222-2222-4222-8222-222222222222",
        producer_sequence="1",
        event_time_ns="1",
        sink_time_ns="2",
        target_created_ns="1",
        volume_id=VOLUME,
        file_id=FILE,
        identity_valid=True,
        export_name_b64="L3VudHJ1c3RlZA",
        export_id=7,
        target_fh=HANDLE,
        operation_id="03" * 32,
        client_ref="04" * 32,
        session_ref="",
        compound_op_index=1,
        slot_id=1,
        sequence_id=1,
        compound_tag_b64="",
        compound_tag_original_length=0,
        compound_tag_truncated=False,
        compound_tag_id=hashlib.sha256(b"").hexdigest(),
        origin_claim="unknown",
        origin_trust="client_asserted",
        requested_offset="0",
        requested_count=4096,
        returned_bytes=4096,
        nfs_status=0,
        outcome="success",
        eof=False,
        file_size_valid=True,
        file_size="65536",
    )
    row.update(changes)
    return ChimeraEvent.model_validate(row)


@pytest.mark.parametrize("origin", ["unknown", "warmer", "synthetic", "controlled_demand"])
def test_all_origins_update_execution_only(origin):
    targets = TargetMap({7: "test-ro"})
    assert targets.update(event(origin_claim=origin), NOW)
    target = targets.resolve(VOLUME, FILE, NOW)
    assert target.export_name == "test-ro"
    assert target.target_fh_b64 == HANDLE
    assert target.file_size == 65536
    assert HANDLE not in repr(target)
    assert HANDLE not in repr(targets.stats())


def test_feedback_cannot_stale_refresh():
    targets = TargetMap({7: "test-ro"})
    assert targets.update(event(), NOW)
    old = targets.resolve(VOLUME, FILE, NOW)
    assert targets.mark_stale(old)
    assert targets.resolve(VOLUME, FILE, NOW) is None
    assert not targets.update(event(), NOW)
    assert targets.resolve(VOLUME, FILE, NOW) is None
    later = NOW + timedelta(seconds=1)
    assert targets.update(event(), later)
    assert not targets.mark_stale(old)
    assert targets.resolve(VOLUME, FILE, later)
    assert not targets.update(event(), NOW - timedelta(seconds=1))


def test_age_boundary_future_and_lru():
    targets = TargetMap({7: "test-ro"}, max_entries=2, target_max_age_s=10)
    assert targets.update(event(), NOW)
    assert targets.resolve(VOLUME, FILE, NOW - timedelta(seconds=1)) is None
    assert targets.resolve(VOLUME, FILE, NOW + timedelta(seconds=10))
    assert targets.resolve(VOLUME, FILE, NOW + timedelta(seconds=10, microseconds=1)) is None
    assert targets.stats()["expired"] == 1
    assert targets.update(event(), NOW)
    assert targets.update(event(file_id="05" * 32), NOW)
    assert targets.resolve(VOLUME, FILE, NOW)
    assert targets.update(event(file_id="06" * 32), NOW)
    assert targets.resolve(VOLUME, "05" * 32, NOW) is None
    assert targets.stats()["entries"] == 2
    assert targets.stats()["evicted"] == 1


@pytest.mark.parametrize(
    "changes", [dict(export_id=8), dict(target_fh="bad="), dict(identity_valid=False)]
)
def test_revalidation_fails_closed(changes):
    raw = event().model_dump()
    raw.update(changes)
    constructed = ChimeraEvent.model_construct(**raw)
    targets = TargetMap({7: "test-ro"})
    assert not targets.update(constructed, NOW)
    assert targets.stats()["entries"] == 0
    assert targets.stats()["rejected"] == 1


def test_unknown_size_and_invalid_time():
    targets = TargetMap({7: "test-ro"})
    assert targets.update(event(file_size_valid=False, file_size=None), NOW)
    assert targets.resolve(VOLUME, FILE, NOW).file_size is None
    assert not targets.update(event(), NOW.replace(tzinfo=None))
    with pytest.raises(ValueError, match="timezone-aware UTC"):
        targets.resolve(VOLUME, FILE, NOW.replace(tzinfo=None))


def test_checkpoint_roundtrip_and_stale():
    targets = TargetMap({7: "test-ro"})
    assert targets.update(event(), NOW)
    targets.mark_stale(targets.resolve(VOLUME, FILE, NOW))
    saved = targets.state()
    restored = TargetMap({7: "test-ro"})
    restored.restore(saved)
    assert restored.state() == saved
    assert restored.resolve(VOLUME, FILE, NOW) is None
    assert restored.update(event(), NOW + timedelta(seconds=1))
    assert restored.resolve(VOLUME, FILE, NOW + timedelta(seconds=1))


@pytest.mark.parametrize(
    "mutation",
    [
        lambda d: d.update(version=2),
        lambda d: d.update(exports={"7": "other"}),
        lambda d: d["entries"][0].update(target_fh_b64="bad="),
        lambda d: d["entries"].append(d["entries"][0]),
        lambda d: d["entries"][0].update(stale="yes"),
    ],
)
def test_restore_rejects_without_partial_change(mutation):
    targets = TargetMap({7: "test-ro"})
    targets.update(event(), NOW)
    saved = targets.state()
    document = json.loads(saved)
    mutation(document)
    with pytest.raises(ValueError, match="invalid target map checkpoint") as error:
        targets.restore(json.dumps(document).encode())
    assert HANDLE not in str(error.value)
    assert targets.state() == saved


@pytest.mark.parametrize(
    "kwargs", [dict(max_entries=0), dict(max_entries=True), dict(target_max_age_s=0)]
)
def test_invalid_bounds(kwargs):
    with pytest.raises(ValueError, match="bounds"):
        TargetMap({7: "test-ro"}, **kwargs)


def test_file_size_uint64_boundary_roundtrips_and_overflow_is_rejected():
    targets = TargetMap({7: "test-ro"})
    assert targets.update(event(file_size=str(2**64 - 1)), NOW)
    saved = targets.state()
    restored = TargetMap({7: "test-ro"})
    restored.restore(saved)
    assert restored.state() == saved
    assert restored.resolve(VOLUME, FILE, NOW).file_size == 2**64 - 1
    assert not targets.update(event(file_size=str(2**64)), NOW + timedelta(seconds=1))
    assert targets.state() == saved
