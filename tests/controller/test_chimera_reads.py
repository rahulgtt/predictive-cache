"""Fictional raw producer records, never evidence of live source coverage."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import UUID

import pytest
from test_targets import FILE, HANDLE, VOLUME, event

from predictive_cache.controller.chimera_reads import (
    ChimeraReadAdapter,
    EnvelopeError,
    tick_chimera,
)
from predictive_cache.controller.targets import TargetMap
from predictive_cache.semantics.read_demand import project_chimera_read
from predictive_cache.sources.chimera.contract import ChimeraEvent

T = datetime(2026, 11, 4, 9, tzinfo=UTC)
EPOCH = "22222222-2222-4222-8222-222222222222"


def raw(sequence=1, **changes):
    base = event(
        producer_sequence=str(sequence),
        local_id=str(UUID(int=sequence)),
        event_time_ns=str(int((T - timedelta(minutes=10)).timestamp()) * 10**9),
        sink_time_ns=str(int(T.timestamp()) * 10**9),
        origin_claim="controlled_demand",
    )
    payload = base.model_dump(by_alias=True)
    payload.update(changes)
    return payload


def envelope(rows=None, **changes):
    rows = [raw()] if rows is None else rows
    sequence = [int(row["producer_sequence"]) for row in rows]
    body = dict(
        schema="nfs4_read_v2",
        pod_uid="pod-1",
        producer_epoch=EPOCH,
        first_sequence=str(min(sequence) if sequence else 1),
        last_sequence=str(max(sequence) if sequence else 1),
        health=dict(
            scraped_at_ns=str(int(T.timestamp()) * 10**9),
            last_synced_sequence=str(max(sequence) if sequence else 1),
            barrier_ready=True,
            delivery_clean=True,
            queue_depth=0,
            worker_in_flight=0,
            dropped=dict(queue=0, encode=0, sink=0, rollback=0, rotation=0),
            shipper_dropped=dict(rotated_unsent=0, stale_epoch=0),
        ),
        events=rows,
    )
    body.update(changes)
    return body


def adapter(**kwargs):
    return ChimeraReadAdapter(
        TargetMap({7: "fixture-export"}), allowed_volumes=[VOLUME], shippers=["pod-1"], **kwargs
    )


def test_projection_is_canonical_and_enqueue_does_not_update_targets():
    source = adapter()
    body = envelope()
    assert source.offer(body, T) == (1, 0, 0)
    assert source.targets.resolve(VOLUME, FILE, T) is None
    result = source.drain(T)
    expected = project_chimera_read(
        ChimeraEvent.model_validate(body["events"][0]),
        durable_at=T,
        dir_id=None,
        file_size=65536,
        coverage_status="unknown",
    )
    assert result == [expected]
    assert result[0].data_kind == "synthetic" and result[0].origin_trust == "client_asserted"
    assert source.targets.resolve(VOLUME, FILE, T).target_fh_b64 == HANDLE
    assert source.source_gates == {"memory_gap"}
    assert HANDLE not in repr(result) and HANDLE not in repr(source.stats())


@pytest.mark.parametrize("origin", ["unknown", "warmer", "synthetic"])
def test_excluded_origins_refresh_target_without_demand(origin):
    source = adapter()
    source.offer(envelope([raw(origin_claim=origin)]), T)
    assert source.drain(T) == []
    assert source.targets.resolve(VOLUME, FILE, T) is not None
    assert source.stats()["excluded_by_reason"]["origin_" + origin] == 1


def test_failed_read_refreshes_target_only():
    source = adapter()
    source.offer(envelope([raw(nfs_status=5, outcome="error", returned_bytes=0, eof=False)]), T)
    assert source.drain(T) == [] and source.targets.resolve(VOLUME, FILE, T) is not None
    assert source.stats()["excluded_by_reason"]["result_failed"] == 1


def test_receipt_boundary_and_post_tick_receipts_never_backdate():
    source = adapter()
    source.offer(envelope([raw(1), raw(2)]), T)
    assert [row.event_id for row in source.drain(T)] == [str(UUID(int=1))]
    assert source.offer(envelope([raw(3)]), T) == (1, 0, 0)
    rows = source.drain(T + timedelta(seconds=1))
    assert [row.event_id for row in rows] == [str(UUID(int=2)), str(UUID(int=3))]
    assert T < rows[0].known_at < rows[1].known_at
    with pytest.raises(ValueError):
        source.drain(T)


def test_duplicate_and_changed_sequence_do_not_create_more_demand():
    source = adapter()
    assert source.offer(envelope(), T) == (1, 0, 0)
    assert source.offer(envelope(), T) == (0, 0, 1)
    changed = raw(returned_bytes=1024)
    assert source.offer(envelope([changed]), T) == (0, 0, 1)
    assert len(source.drain(T)) == 1
    assert source.source_gates == {"memory_gap", "ingest_loss"}


@pytest.mark.parametrize(
    "changes,status",
    [
        ({"pod_uid": "unregistered"}, 403),
        ({"schema": "other"}, 400),
        ({"producer_epoch": "invalid"}, 400),
        ({"first_sequence": "00"}, 400),
        ({"first_sequence": "9"}, 400),
        ({"events": [1]}, 400),
    ],
)
def test_invalid_envelope_is_sanitized_and_charged(changes, status):
    source = adapter()
    with pytest.raises(EnvelopeError) as raised:
        source.offer(envelope(**changes), T)
    assert raised.value.status == status
    assert raised.value.loss_accounted is True
    assert source.stats()["rejected_requests"] == 1 and "ingest_loss" in source.source_gates
    assert source.stats()["queue_depth"] == 0


def test_new_envelope_error_does_not_claim_loss_accounting():
    assert EnvelopeError().loss_accounted is False


def test_queue_bound_matches_runtime_before_admitting_input():
    assert adapter(queue_max=100000, max_queue_bytes=256 * 2**20).queue_max == 100000
    with pytest.raises(ValueError, match="consumer batch limit"):
        adapter(queue_max=100001, max_queue_bytes=256 * 2**20)


def test_synced_watermark_and_health_types_are_enforced():
    source = adapter()
    body = envelope()
    body["health"]["last_synced_sequence"] = "0"
    with pytest.raises(EnvelopeError):
        source.offer(body, T)
    body = envelope()
    body["health"]["queue_depth"] = True
    with pytest.raises(EnvelopeError):
        source.offer(body, T)


@pytest.mark.parametrize(
    "kwargs", [{"queue_max": 1}, {"max_queue_bytes": 1}, {"max_event_bytes": 1}]
)
def test_capacity_losses_are_visible_and_bounded(kwargs):
    source = adapter(**kwargs)
    accepted, dropped, _ = source.offer(envelope([raw(1), raw(2)]), T)
    assert dropped and accepted <= 1 and "ingest_loss" in source.source_gates
    assert source.stats()["queue_depth"] <= source.queue_max
    assert source.stats()["queue_bytes"] <= source.max_queue_bytes


def test_sequence_holes_and_epoch_retirement_are_fail_closed():
    source = adapter(max_epochs_per_pod=2, max_sequence_gap=4)
    source.offer(envelope([raw(1), raw(3)]), T)
    assert source.stats()["pending_sequences"] == 1 and "ingest_loss" in source.source_gates
    assert source.offer(envelope([raw(20)]), T) == (0, 1, 0)
    second = str(UUID(int=20))
    body = envelope([raw(1, producer_epoch=second)], producer_epoch=second)
    assert source.offer(body, T) == (1, 0, 0)
    assert source.offer(envelope([raw(2)]), T) == (0, 1, 0)
    third = str(UUID(int=21))
    body = envelope([raw(1, producer_epoch=third)], producer_epoch=third)
    assert source.offer(body, T) == (0, 1, 0)
    assert source.stats()["stream_count"] == 2


def test_midstream_and_clean_heartbeat_never_certify_coverage():
    source = adapter()
    source.offer(envelope([raw(100)]), T)
    source.drain(T)
    heartbeat = envelope([], first_sequence="100", last_sequence="100")
    heartbeat["health"]["last_synced_sequence"] = "100"
    assert source.offer(heartbeat, T) == (0, 0, 0)
    assert source.stats()["coverage"] == "unknown"
    assert not source.stats()["loss"]
    assert "memory_gap" in source.source_gates


@pytest.mark.parametrize("change", ["drop", "regression", "dirty"])
def test_health_changes_latch_loss(change):
    source = adapter()
    source.offer(envelope(), T)
    body = envelope([])
    if change == "drop":
        body["health"]["dropped"]["sink"] = 1
    if change == "regression":
        body["health"]["scraped_at_ns"] = "1"
    if change == "dirty":
        body["health"]["delivery_clean"] = False
    source.offer(body, T)
    assert "ingest_loss" in source.source_gates


@pytest.mark.parametrize(
    "change",
    [
        {"identity_valid": False},
        {"unexpected": "private"},
        {"event_time_ns": str(int((T + timedelta(seconds=1)).timestamp()) * 10**9)},
    ],
)
def test_invalid_raw_rows_and_future_clock_never_enter_demand(change):
    source = adapter()
    source.offer(envelope([raw(**change)]), T)
    assert source.drain(T) == [] and "ingest_loss" in source.source_gates


def test_queue_owns_payload_and_concurrent_retries_dedupe():
    source = adapter()
    body = envelope()
    with ThreadPoolExecutor(max_workers=8) as pool:
        outcomes = list(pool.map(lambda _: source.offer(body, T), range(16)))
    assert sum(a for a, _, _ in outcomes) == 1
    assert sum(d for _, _, d in outcomes) == 15
    body["events"][0]["returned_bytes"] = 0
    assert source.drain(T)[0].bytes_read == 4096


def test_tick_bridge_requires_real_map_and_forwards_source_gates():
    source = adapter()
    source.offer(envelope(), T)
    calls = []
    runtime = SimpleNamespace(targets=source.targets, tick=lambda t, **kw: calls.append((t, kw)))
    tick_chimera(source, runtime, T)
    assert calls[0][1]["events"][0].coverage_status == "unknown"
    assert calls[0][1]["source_gates"] == {"memory_gap"}
    with pytest.raises(ValueError):
        tick_chimera(source, SimpleNamespace(targets=None), T)
