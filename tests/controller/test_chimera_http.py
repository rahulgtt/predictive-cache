"""Fictional raw READ transport and explicitly gated controller integration."""

from __future__ import annotations

import base64
import hashlib
import http.client
import json
import uuid
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from predictive_cache.controller.chimera_reads import (
    ChimeraReadAdapter,
    EnvelopeError,
    tick_chimera,
)
from predictive_cache.controller.http import HttpSource
from predictive_cache.controller.models import ModelManager
from predictive_cache.controller.planner import PlannerPublisher, ScoredCandidate
from predictive_cache.controller.read_memory import CoverageObservation, ReadMemory
from predictive_cache.controller.read_runtime import ReadReplayRuntime
from predictive_cache.controller.targets import TargetMap
from predictive_cache.dispatcher.core import Dispatcher, DispatcherConfig
from predictive_cache.ml.package import DEFAULT_SERVING_POLICY
from predictive_cache.sources.chimera.contract import ChimeraEvent

T = datetime(2026, 11, 4, 9, tzinfo=UTC)
VOLUME, FILE = "01" * 16, "02" * 32
EPOCH = "22222222-2222-4222-8222-222222222222"
HANDLE = base64.urlsafe_b64encode(b"observed-fictional-handle").rstrip(b"=").decode()


def envelope(sequence=1, **changes):
    nanos = str(int((T - timedelta(minutes=10)).timestamp()) * 10**9)
    raw = dict(
        schema="nfs4_read_v2",
        local_id=str(uuid.UUID(int=sequence)),
        producer_epoch=EPOCH,
        producer_sequence=str(sequence),
        event_time_ns=nanos,
        sink_time_ns=nanos,
        target_created_ns=nanos,
        volume_id=VOLUME,
        file_id=FILE,
        identity_valid=True,
        export_name_b64="L2xhYg",
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
        origin_claim="controlled_demand",
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
    raw.update(changes)
    return dict(
        schema="nfs4_read_v2",
        pod_uid="fixture-pod",
        producer_epoch=EPOCH,
        first_sequence=str(sequence),
        last_sequence=str(sequence),
        events=[raw],
        health=dict(
            scraped_at_ns=str(int(T.timestamp()) * 10**9),
            last_synced_sequence=str(sequence),
            barrier_ready=True,
            delivery_clean=True,
            queue_depth=0,
            worker_in_flight=0,
            dropped=dict(queue=0, encode=0, sink=0, rollback=0, rotation=0),
            shipper_dropped=dict(rotated_unsent=0, stale_epoch=0),
        ),
    )


@pytest.fixture
def receiver():
    targets = TargetMap({7: "lab-export"})
    adapter = ChimeraReadAdapter(
        targets,
        allowed_volumes=[VOLUME],
        shippers=["fixture-pod"],
        clock=SimpleNamespace(now=lambda: T),
    )
    source = HttpSource(
        ("127.0.0.1", 0),
        token="fixture-token",
        wire_format="nfs4_read_v2",
        on_reject=adapter.reject_input,
        read_timeout_s=0.3,
    )
    source.start(adapter.offer)
    try:
        yield source, adapter, targets
    finally:
        source.stop()


def send(source, body, *, token="fixture-token"):
    data = json.dumps(body).encode() if not isinstance(body, bytes) else body
    connection = http.client.HTTPConnection(*source.bound_address, timeout=2)
    connection.request(
        "POST",
        "/v1/events",
        data,
        {"Content-Type": "application/json", "Authorization": "Bearer " + token},
    )
    response = connection.getresponse()
    result = response.status, json.loads(response.read())
    connection.close()
    return result


def test_raw_http_is_enqueue_only_and_canonical_projection_is_controlled(receiver):
    source, adapter, targets = receiver
    status, counts = send(source, envelope())
    assert status == 202 and counts == dict(accepted=1, dropped=0, duplicate=0)
    assert targets.resolve(VOLUME, FILE, T) is None
    rows = adapter.drain(T)
    assert len(rows) == 1
    assert rows[0].known_at == T and rows[0].demand_class == "controlled_demand"
    assert rows[0].origin == "synthetic" and rows[0].origin_trust == "client_asserted"
    assert rows[0].coverage_status == "unknown"
    assert targets.resolve(VOLUME, FILE, T).target_fh_b64 == HANDLE
    assert "memory_gap" in adapter.source_gates
    assert HANDLE not in repr(adapter.stats())


def test_retry_does_not_duplicate_demand(receiver):
    source, adapter, _ = receiver
    assert send(source, envelope())[1]["accepted"] == 1
    assert send(source, envelope())[1]["duplicate"] == 1
    assert len(adapter.drain(T)) == 1


def test_unauthenticated_input_does_not_change_source_loss(receiver):
    source, adapter, _ = receiver
    before = adapter.stats()
    assert send(source, envelope(), token="wrong")[0] == 401
    assert adapter.stats() == before


@pytest.mark.parametrize("body,status", [(b"{", 400), (b"[]", 400), (b"[NaN]", 400)])
def test_authenticated_bad_input_has_visible_loss(receiver, body, status):
    source, adapter, _ = receiver
    assert send(source, body)[0] == status
    assert "ingest_loss" in adapter.source_gates
    assert adapter.drain(T) == []


def test_registered_envelope_rules_and_unregistered_pod(receiver):
    source, adapter, _ = receiver
    body = envelope()
    body["pod_uid"] = "unregistered-pod"
    assert send(source, body)[0] == 403
    assert "ingest_loss" in adapter.source_gates
    assert adapter.stats()["rejected_requests"] == 1


def test_unaccounted_sink_rejection_is_charged_by_http(receiver):
    source, adapter, _ = receiver

    def reject(_body):
        raise EnvelopeError()

    source.sink = reject
    assert send(source, envelope())[0] == 400
    assert adapter.stats()["rejected_requests"] == 1


def test_duplicate_json_fields_rejected_without_echoing_payload(receiver):
    source, adapter, _ = receiver
    body = b'{"schema":"PRIVATE_INPUT","schema":"nfs4_read_v2","events":[]}'
    status, response = send(source, body)
    assert status == 400 and "PRIVATE_INPUT" not in repr(response)
    assert "ingest_loss" in adapter.source_gates


def test_rejection_accounting_failure_is_visible_and_retryable(receiver):
    source, _, _ = receiver

    def broken():
        raise RuntimeError("PRIVATE_CALLBACK_ERROR")

    source.on_reject = broken
    status, response = send(source, b"{")
    assert status == 503 and source.rejection_failed
    assert "PRIVATE_CALLBACK_ERROR" not in repr(response)


def test_raw_mode_requires_rejection_accounting():
    with pytest.raises(ValueError):
        HttpSource(token="fixture-token", wire_format="nfs4_read_v2")


def test_raw_http_to_runtime_keeps_source_gate_despite_complete_fixture_history(receiver, tmp_path):
    source, adapter, targets = receiver
    memory = ReadMemory(VOLUME)
    root = tmp_path / "gated"
    root.mkdir(mode=0o700)
    models = ModelManager(tmp_path / "absent-pointer", contract=memory.contract)
    publisher = PlannerPublisher(root, run_id="raw-fixture", controller_id="raw-fixture")
    # These checkpoints are explicitly invented fixture history, not raw-source coverage.
    for hour in range(168, 0, -1):
        start = T - timedelta(hours=hour)
        end = start + timedelta(hours=1)
        memory.apply_coverage(
            CoverageObservation(
                volume_id=VOLUME,
                hour_start=start,
                known_at=end,
                observed_through=end,
                status="complete",
            ),
            available_at=T,
        )
    try:
        with Dispatcher(
            DispatcherConfig(
                root=root, warmer_command=root / "unused", host="192.0.2.1", real_reads=False
            ),
            now=lambda: T,
        ) as dispatcher:
            runtime = ReadReplayRuntime(memory, models, publisher, dispatcher, targets=targets)
            assert send(source, envelope())[0] == 202
            result = tick_chimera(adapter, runtime, T)
            assert result.scored and not result.plans_allowed
            assert result.planned_items == result.results == 0
            assert targets.resolve(VOLUME, FILE, T).target_fh_b64 == HANDLE
            assert "memory_gap" in runtime.summary(T)["health"]["reasons"]
            assert not list((root / "plans").glob("*.json"))
    finally:
        models.close()


def test_observed_target_plan_dispatcher_fixture_only_not_raw_source_coverage(tmp_path):
    targets = TargetMap({7: "lab-export"})
    assert targets.update(ChimeraEvent.model_validate(envelope()["events"][0]), T)
    root = tmp_path / "fixture-dispatch"
    root.mkdir(mode=0o700)
    publisher = PlannerPublisher(root, run_id="fixture-only", controller_id="fixture-only")
    selected = publisher.publish(
        [ScoredCandidate(VOLUME, FILE, 1.0, 600, 65536)],
        t=T,
        serving_policy=dict(DEFAULT_SERVING_POLICY),
        is_eligible=lambda *_: True,
        targets=targets,
        model_version="fixture-score",
        plans_allowed=True,
    )
    with Dispatcher(
        DispatcherConfig(
            root=root, warmer_command=root / "unused", host="192.0.2.1", real_reads=False
        ),
        now=lambda: T,
    ) as dispatcher:
        assert dispatcher.run_once().results == 1
    result = json.loads(next(root.glob("runs/*/plans/*/results/*.json")).read_text())
    assert result["reason"] == "dry_run" and result["bytes_read"] == 0
    assert selected.bindings[0] is targets.resolve(VOLUME, FILE, T)
