"""Controller diagnostics use cached state and aggregate metrics only."""

import http.client
import json
import socket
import time

import pytest

from predictive_cache.controller.health import ControllerHealth, HealthServer


def test_readiness_and_plan_gates_are_distinct():
    health = ControllerHealth()
    assert health.snapshot()["status"] == "not_ready"
    health.set_state(started=True, has_scorer=False)
    assert not health.snapshot()["plans_allowed"]
    health.set_state(started=True, has_scorer=True)
    assert health.snapshot() == {"status": "ready", "reasons": [], "plans_allowed": True}
    health.set_state(started=True, has_scorer=True, reasons=["model_load_failed"])
    assert health.snapshot()["status"] == "degraded"
    assert health.snapshot()["plans_allowed"]
    health.set_state(started=True, has_scorer=True, reasons=["contract_mismatch"])
    assert health.snapshot()["status"] == "degraded"
    assert not health.snapshot()["plans_allowed"]


def test_metrics_are_bounded_and_reject_arbitrary_labels():
    health = ControllerHealth()
    health.increment("events_received", 3)
    health.exclude("origin_unknown", 2)
    health.gauge("queue_depth", 1)
    health.observe_tick(0.25)
    metrics = health.metrics()
    assert "pcache_controller_events_received_total 3\n" in metrics
    assert 'pcache_controller_excluded_total{reason="origin_unknown"} 2\n' in metrics
    assert "pcache_controller_events_excluded_total 2\n" in metrics
    assert "pcache_controller_tick_seconds_count 1\n" in metrics
    for action in (
        lambda: health.increment("PRIVATE_TOKEN"),
        lambda: health.gauge("PRIVATE_HANDLE", 1),
        lambda: health.exclude("PRIVATE_PATH"),
        lambda: health.set_state(started=True, has_scorer=True, reasons=["PRIVATE_REASON"]),
    ):
        with pytest.raises(ValueError) as error:
            action()
        assert "PRIVATE" not in str(error.value)


@pytest.mark.parametrize("value", [-1, True, 1.5])
def test_invalid_counter_values(value):
    with pytest.raises(ValueError):
        ControllerHealth().increment("events_received", value)


@pytest.mark.parametrize("value", [float("nan"), float("inf"), -1, True])
def test_invalid_durations(value):
    with pytest.raises(ValueError):
        ControllerHealth().observe_tick(value)


def request(server, path):
    connection = http.client.HTTPConnection(*server.bound_address, timeout=3)
    connection.request("GET", path)
    response = connection.getresponse()
    result = response.status, response.getheader("Content-Type"), response.read()
    connection.close()
    return result


def test_real_http_endpoints_and_quiet_errors(capsys):
    health = ControllerHealth()
    server = HealthServer(health)
    server.start()
    try:
        assert request(server, "/healthz")[0] == 200
        assert request(server, "/readyz")[0] == 503
        health.set_state(started=True, has_scorer=True, reasons=["memory_gap"])
        status, _, body = request(server, "/readyz")
        assert status == 200 and not json.loads(body)["plans_allowed"]
        assert request(server, "/metrics")[1].startswith("text/plain")
        assert request(server, "/PRIVATE_PATH")[0] == 404
    finally:
        server.stop()
        server.stop()
    assert server.bound_address is None
    assert capsys.readouterr().err == ""


def test_slow_header_has_an_absolute_deadline():
    server = HealthServer(ControllerHealth())
    server.start()
    client = socket.create_connection(server.bound_address, timeout=3)
    try:
        client.sendall(b"GET /healthz HTTP/1.1\r\nX: ")
        started = time.monotonic()
        for _ in range(12):
            time.sleep(0.2)
            try:
                client.sendall(b"a")
            except OSError:
                break
        assert request(server, "/healthz")[0] == 200
        assert time.monotonic() - started < 4
    finally:
        client.close()
        server.stop()
