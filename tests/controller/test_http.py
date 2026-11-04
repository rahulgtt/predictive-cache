"""Local socket contract tests, using synthetic request bodies only."""

import http.client
import json
import socket
import time

import pytest

from predictive_cache.controller.http import HttpSource


@pytest.fixture
def receiver():
    received = []
    source = HttpSource(("127.0.0.1", 0), token="fixture-token", read_timeout_s=0.3)
    source.start(lambda events: (received.extend(events) or len(events), 0))
    try:
        yield source, received
    finally:
        source.stop()


def request(source, body=b"[]", *, headers=None, method="POST", path="/v1/events"):
    connection = http.client.HTTPConnection(*source.bound_address, timeout=2)
    auth = {"Authorization": "Bearer fixture-token", "Content-Type": "application/json"}
    if headers:
        auth.update(headers)
    connection.request(method, path, body, auth)
    response = connection.getresponse()
    result = response.status, response.read()
    connection.close()
    return result


def raw(source, data, *, half_close=False):
    connection = socket.create_connection(source.bound_address, timeout=2)
    try:
        connection.sendall(data)
        if half_close:
            connection.shutdown(socket.SHUT_WR)
        response = b""
        while part := connection.recv(8192):
            response += part
        return int(response.split(b" ", 2)[1]) if response else None
    finally:
        connection.close()


def test_enqueue_only(receiver):
    source, received = receiver
    status, body = request(source, b'[{"event_id":"fixture"}]')
    assert status == 202
    assert json.loads(body) == {"accepted": 1, "dropped": 0}
    assert received == [{"event_id": "fixture"}]


@pytest.mark.parametrize("authorization", ["", "Bearer wrong", "Basic fixture-token"])
def test_unauthorized(receiver, authorization):
    source, received = receiver
    assert request(source, headers={"Authorization": authorization})[0] == 401
    assert received == []


@pytest.mark.parametrize("body", [b"{", b"{}", b"[1]", b"[NaN]", b"\xff", b"[" * 2000])
def test_malformed(receiver, body):
    assert request(receiver[0], body)[0] == 400


@pytest.mark.parametrize(
    "headers,expected",
    [
        (b"", 411),
        (b"Content-Length: 0\r\nContent-Length: 0\r\n", 400),
        (b"Content-Length: -1\r\n", 400),
        (b"Content-Length: 4194305\r\n", 413),
        (b"Content-Length: 0\r\nTransfer-Encoding: chunked\r\n", 400),
    ],
)
def test_framing(receiver, headers, expected):
    data = (
        b"POST /v1/events HTTP/1.1\r\nHost: localhost\r\n"
        b"Authorization: Bearer fixture-token\r\nContent-Type: application/json\r\n"
        + headers
        + b"\r\n"
    )
    assert raw(receiver[0], data) == expected


def test_event_cap_and_paths(receiver):
    source, _ = receiver
    assert request(source, json.dumps([{}] * 10001).encode())[0] == 413
    assert request(source, path="/unknown/private-identifier")[0] == 404
    status, body = request(source, method="DELETE", path="/private-identifier")
    assert status == 501
    assert b"private-identifier" not in body
    assert request(source, headers={"Content-Type": "text/plain"})[0] == 415


def test_short_body_and_timeout(receiver):
    source, _ = receiver
    data = (
        b"POST /v1/events HTTP/1.1\r\nHost: localhost\r\n"
        b"Authorization: Bearer fixture-token\r\nContent-Type: application/json\r\n"
        b"Content-Length: 10\r\n\r\n[]"
    )
    assert raw(source, data, half_close=True) == 400
    assert raw(source, data) == 408


def test_bounded_workers_and_absolute_header_deadline():
    source = HttpSource(
        ("127.0.0.1", 0), token="fixture-token", max_concurrent=1, read_timeout_s=0.3
    )
    source.start(lambda events: (len(events), 0))
    slow = socket.create_connection(source.bound_address, timeout=2)
    try:
        slow.sendall(b"POST /v1/events HTTP/1.1\r\nX: ")
        deadline = time.monotonic() + 1
        while source.server.slots.acquire(blocking=False):
            source.server.slots.release()
            assert time.monotonic() < deadline
            time.sleep(0.005)
        assert request(source)[0] == 503
        # Every individual byte arrives before the socket timeout. Total header
        # duration still expires, releasing the sole worker.
        for _ in range(5):
            time.sleep(0.08)
            try:
                slow.sendall(b"x")
            except OSError:
                break
        assert request(source)[0] == 202
    finally:
        slow.close()
        source.stop()


def test_shutdown_waits_for_bounded_inflight_request():
    source = HttpSource(("127.0.0.1", 0), token="fixture-token", read_timeout_s=0.2)
    source.start(lambda events: (len(events), 0))
    connection = socket.create_connection(source.bound_address, timeout=2)
    connection.sendall(b"P")
    before = time.monotonic()
    source.stop()
    connection.close()
    assert time.monotonic() - before < 1
    assert source.server is None


def test_no_request_logging(receiver, capsys):
    source, _ = receiver
    request(
        source,
        b"PRIVATE_BODY",
        path="/PRIVATE_PATH",
        headers={"Authorization": "Bearer PRIVATE_TOKEN"},
    )
    output = capsys.readouterr()
    assert not output.out and not output.err


def test_queue_drop_response_without_cleaning():
    source = HttpSource(("127.0.0.1", 0), token="fixture-token")
    source.start(lambda events: (1, len(events) - 1))
    try:
        status, body = request(source, b"[{},{}]")
        assert status == 202
        assert json.loads(body) == {"accepted": 1, "dropped": 1}
    finally:
        source.stop()


def test_http_with_real_ingestor_does_not_call_consumer():
    from predictive_cache.controller.ingest import Ingestor
    from predictive_cache.prep.config import CleanConfig

    updates = []
    ingestor = Ingestor(
        CleanConfig(), updates.append, allowed_volumes=["fixture-volume"], queue_max=1
    )
    source = HttpSource(("127.0.0.1", 0), token="fixture-token")
    source.start(ingestor.offer)
    try:
        status, body = request(source, b'[{"volume_id":"not-allowed"}]')
        assert status == 202
        assert json.loads(body) == {"accepted": 0, "dropped": 1}
        assert updates == []
        status, body = request(
            source,
            b'[{"volume_id":"fixture-volume","event_id":"a"},{"volume_id":"fixture-volume","event_id":"b"}]',
        )
        assert status == 202
        assert json.loads(body) == {"accepted": 1, "dropped": 1}
        assert ingestor.stats()["queue_depth"] == 1
        assert updates == []
    finally:
        source.stop()


def test_cli_configuration_errors_do_not_echo_private_values(tmp_path):
    from click.testing import CliRunner

    from predictive_cache.controller.cli import main

    config = tmp_path / "private-config.yaml"
    config.write_text("controller: {ingest: {source: PRIVATE_VALUE}}")
    result = CliRunner().invoke(main, ["run", "--config", str(config)])
    assert result.exit_code != 0
    assert "PRIVATE_VALUE" not in result.output
    assert str(config) not in result.output


def test_cli_live_pumps_and_stops(monkeypatch, tmp_path):
    from click.testing import CliRunner

    from predictive_cache.controller import cli

    calls = []

    class FakeSource:
        def __init__(self, *args, **kwargs):
            assert kwargs["token"] == "fixture-token"

        def start(self, sink):
            calls.append("start")

        def stop(self):
            calls.append("stop")

    def interrupt(delay):
        raise KeyboardInterrupt

    monkeypatch.setattr(cli, "HttpSource", FakeSource)
    monkeypatch.setattr(cli.time, "sleep", interrupt)
    monkeypatch.setenv("PCACHE_INGEST_TOKEN", "fixture-token")
    config = tmp_path / "config.yaml"
    config.write_text("controller:\n  ingest:\n    source: http\n    allowed_volumes: [fixture]\n")
    result = CliRunner().invoke(cli.main, ["run", "--config", str(config)])
    assert result.exit_code == 0, result.output
    assert calls == ["start", "stop"]
    assert "fixture-token" not in result.output
    assert "queue_depth" in result.output


def test_cli_replay_includes_late_arrival_and_cleans_spool(tmp_path):
    import hashlib

    from click.testing import CliRunner

    from predictive_cache.controller.cli import main

    dataset = tmp_path / "PRIVATE_DATASET"
    events = dataset / "events" / "fixture.jsonl"
    events.parent.mkdir(parents=True)
    row = {
        "schema_version": 1,
        "event_id": "PRIVATE_EVENT",
        "event_time": "2026-01-01T00:00:00Z",
        "ingest_time": "2026-01-01T00:01:00Z",
        "volume_id": "fixture",
        "file_id": "PRIVATE_FILE",
        "dir_id": "fixture-dir",
        "operation": "open",
        "handle_id": "fixture-handle",
        "file_size": 10,
        "result": "ok",
        "origin": "foreground",
    }
    events.write_text(json.dumps(row) + "\n")
    manifest = {
        "event_schema_version": 1,
        "period": {"start": "2026-01-01T00:00:00Z", "end": "2026-01-01T00:00:01Z"},
        "files": [
            {
                "path": "events/fixture.jsonl",
                "sha256": hashlib.sha256(events.read_bytes()).hexdigest(),
            }
        ],
    }
    (dataset / "manifest.json").write_text(json.dumps(manifest))
    config = tmp_path / "config.yaml"
    config.write_text(
        "controller:\n  ingest:\n    source: replay\n    allowed_volumes: [fixture]\n"
        f"    replay: {{dataset: {dataset}, batch_size: 1}}\n"
    )
    first = CliRunner().invoke(main, ["run", "--config", str(config)])
    second = CliRunner().invoke(main, ["run", "--config", str(config)])
    assert first.exit_code == 0, first.output
    assert first.output == second.output
    summary = json.loads(first.output)
    assert summary["counters"]["accepted"] == 1
    assert summary["counters"]["queue_depth"] == 0
    assert summary["updates"] == {"OpenObserved": 1, "OpenCompleted": 1}
    assert "PRIVATE" not in first.output


@pytest.mark.parametrize("reason", ["lifecycle_conflict", "capacity", "PRIVATE_SOURCE_VALUE"])
def test_cli_summary_exposes_tracker_failure_without_source_details(reason):
    from collections import Counter

    from predictive_cache.controller.cli import _summary

    class FailedIngestor:
        def stats(self):
            return {
                "tracker_exhausted": False,
                "tracker_failed": True,
                "tracker_failure_reason": reason,
                "excluded_by_reason": {},
                "lifecycle_conflicts": 1,
                "dropped": 0,
                "consumer_errors": 0,
            }

    summary = _summary(FailedIngestor(), Counter())
    assert summary["tracker_failed"] is True
    assert summary["degraded"] is True
    assert summary["counters"]["lifecycle_conflicts"] == 1
    assert summary["tracker_failure_reason"] == (
        reason if reason != "PRIVATE_SOURCE_VALUE" else "other"
    )
    assert "PRIVATE_SOURCE_VALUE" not in json.dumps(summary)


@pytest.fixture
def raw_receiver():
    from predictive_cache.controller.ingest import ChimeraTargetIngestor
    from predictive_cache.controller.targets import TargetMap

    core = ChimeraTargetIngestor(
        TargetMap({7: "test-ro"}),
        allowed_volumes=["01" * 16],
        shippers=["fixture-pod"],
        queue_max=2,
    )
    source = HttpSource(("127.0.0.1", 0), token="fixture-token")
    source.start(raw_sink=core.offer)
    try:
        yield source, core
    finally:
        source.stop()


def test_raw_http_enqueues_then_updates_private_target(raw_receiver, raw_envelope, capsys):
    from datetime import UTC, datetime, timedelta

    source, core = raw_receiver
    payload = json.dumps(raw_envelope()).encode()
    status, body = request(source, payload)
    assert status == 202
    assert json.loads(body) == {"accepted": 1, "dropped": 0, "duplicate": 0}
    assert core.targets.stats()["entries"] == 0
    core.drain(datetime.now(UTC) + timedelta(seconds=1))
    assert core.targets.resolve("01" * 16, "02" * 32, datetime.now(UTC)) is not None
    assert json.loads(request(source, payload)[1]) == {"accepted": 0, "dropped": 0, "duplicate": 1}
    assert core.stats()["updates_emitted"] == 0
    assert not capsys.readouterr().out


def test_raw_http_rejections_are_sanitized(raw_receiver, raw_envelope):
    source, core = raw_receiver
    payload = json.dumps(raw_envelope(pod_uid="PRIVATE_UNKNOWN_POD")).encode()
    status, body = request(source, payload)
    assert status == 403 and b"PRIVATE" not in body
    assert request(source, b"[]")[0] == 400
    assert request(source, payload, headers={"Authorization": "Bearer wrong"})[0] == 401
    assert core.stats()["queue_depth"] == 0


def test_raw_http_limit_is_applied_to_envelope(raw_envelope):
    source = HttpSource(("127.0.0.1", 0), token="fixture-token", max_events=1)
    called = []
    source.start(raw_sink=lambda envelope: (called.append(envelope) or 0, 0, 0))
    try:
        body = raw_envelope()
        body["events"] *= 2
        assert request(source, json.dumps(body).encode())[0] == 413
        assert called == []
    finally:
        source.stop()


def test_raw_cli_mode_creates_map_without_demand(monkeypatch, tmp_path):
    from click.testing import CliRunner

    from predictive_cache.controller import cli

    calls = []

    class FakeSource:
        def __init__(self, *args, **kwargs):
            pass

        def start(self, sink=None, *, raw_sink=None):
            assert sink is None and raw_sink is not None
            calls.append("raw-start")

        def stop(self):
            calls.append("stop")

    def interrupt(_):
        raise KeyboardInterrupt

    monkeypatch.setattr(cli, "HttpSource", FakeSource)
    monkeypatch.setattr(cli.time, "sleep", interrupt)
    monkeypatch.setenv("PCACHE_INGEST_TOKEN", "fixture-token")
    config = tmp_path / "config.yaml"
    config.write_text(
        "controller:\n  ingest:\n    source: http\n    mode: chimera-targets\n"
        "    allowed_volumes: [fixture-volume]\n    shippers: [fixture-pod]\n"
        "    targets: {exports: {7: test-ro}}\n"
    )
    result = CliRunner().invoke(cli.main, ["run", "--config", str(config)])
    assert result.exit_code == 0, result.output
    summary = json.loads(result.output.splitlines()[-1])
    assert summary["demand_enabled"] is False
    assert summary["coverage_status"] == "unknown"
    assert summary["restart_continuity"] is False
    assert calls == ["raw-start", "stop"]
    assert "fixture-token" not in result.output
