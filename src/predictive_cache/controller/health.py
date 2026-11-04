"""Cached controller diagnostics, independent of events and model loading."""

from __future__ import annotations

import io
import json
import math
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Lock, Thread

from .http import _DeadlineReader

REASONS = frozenset(
    {
        "not_started",
        "no_scorer",
        "fallback_model",
        "model_load_failed",
        "contract_mismatch",
        "memory_gap",
        "restore_failed",
        "ingest_loss",
        "controller_error",
    }
)
PLAN_GATES = frozenset(
    {"contract_mismatch", "memory_gap", "restore_failed", "ingest_loss", "controller_error"}
)
COUNTERS = frozenset(
    {
        "events_received",
        "events_kept",
        "events_excluded",
        "dropped_overflow",
        "updates_applied",
        "tick_overruns",
        "plans_suppressed",
        "planned_items",
        "planned_bytes",
        "consumer_errors",
    }
)
GAUGES = frozenset({"queue_depth", "dedupe_size", "candidates"})
EXCLUSIONS = frozenset(
    {
        "schema_invalid",
        "origin_warmer",
        "origin_synthetic",
        "origin_unknown",
        "result_failed",
        "invalid_range",
        "time_invalid",
        "duplicate",
        "operation_unsupported",
        "volume",
    }
)


def _count(value: int) -> None:
    if type(value) is not int or value < 0:
        raise ValueError("metric count must be a nonnegative integer")


class ControllerHealth:
    """Runtime explicitly publishes status; HTTP never loads or probes a model."""

    def __init__(self):
        self._lock = Lock()
        self._started = self._has_scorer = False
        self._reasons: frozenset[str] = frozenset()
        self._counters = dict.fromkeys(COUNTERS, 0)
        self._gauges = dict.fromkeys(GAUGES, 0)
        self._excluded = dict.fromkeys(EXCLUSIONS, 0)
        self._tick_sum, self._tick_count = 0.0, 0

    def set_state(self, *, started: bool, has_scorer: bool, reasons=()) -> None:
        if type(started) is not bool or type(has_scorer) is not bool:
            raise ValueError("runtime state requires booleans")
        reasons = frozenset(reasons)
        if not reasons <= REASONS or reasons & {"not_started", "no_scorer"}:
            raise ValueError("unsupported runtime reason")
        with self._lock:
            self._started, self._has_scorer, self._reasons = started, has_scorer, reasons

    def _snapshot(self):
        reasons = set(self._reasons)
        if not self._started:
            reasons.add("not_started")
        if not self._has_scorer:
            reasons.add("no_scorer")
        status = (
            "not_ready"
            if not self._started or not self._has_scorer
            else ("degraded" if reasons else "ready")
        )
        return {
            "status": status,
            "reasons": sorted(reasons),
            "plans_allowed": status != "not_ready" and not reasons & PLAN_GATES,
        }

    def snapshot(self) -> dict:
        with self._lock:
            return self._snapshot()

    def increment(self, name: str, value: int = 1) -> None:
        if name not in COUNTERS:
            raise ValueError("unsupported counter")
        _count(value)
        with self._lock:
            self._counters[name] += value

    def gauge(self, name: str, value: int) -> None:
        if name not in GAUGES:
            raise ValueError("unsupported gauge")
        _count(value)
        with self._lock:
            self._gauges[name] = value

    def exclude(self, reason: str, value: int = 1) -> None:
        if reason not in EXCLUSIONS:
            raise ValueError("unsupported exclusion reason")
        _count(value)
        with self._lock:
            self._excluded[reason] += value
            self._counters["events_excluded"] += value

    def observe_tick(self, seconds: float) -> None:
        if (
            isinstance(seconds, bool)
            or not isinstance(seconds, (float, int))
            or not math.isfinite(seconds)
            or seconds < 0
        ):
            raise ValueError("invalid tick duration")
        with self._lock:
            updated = self._tick_sum + seconds
            if not math.isfinite(updated):
                raise ValueError("tick duration sum overflow")
            self._tick_sum = updated
            self._tick_count += 1

    def metrics(self) -> str:
        with self._lock:
            lines = [
                f"pcache_controller_{key}_total {value}"
                for key, value in sorted(self._counters.items())
            ]
            lines += [
                f"pcache_controller_{key} {value}" for key, value in sorted(self._gauges.items())
            ]
            lines += [
                f'pcache_controller_excluded_total{{reason="{key}"}} {value}'
                for key, value in sorted(self._excluded.items())
            ]
            snapshot = self._snapshot()
            lines += [
                f'pcache_controller_reason{{reason="{key}"}} {int(key in snapshot["reasons"])}'
                for key in sorted(REASONS)
            ]
            lines += [
                f"pcache_controller_plans_allowed {int(snapshot['plans_allowed'])}",
                f"pcache_controller_has_scorer {int(self._has_scorer)}",
                f"pcache_controller_tick_seconds_sum {self._tick_sum:.17g}",
                f"pcache_controller_tick_seconds_count {self._tick_count}",
            ]
            return "\n".join(lines) + "\n"


class _Handler(BaseHTTPRequestHandler):
    def setup(self):
        super().setup()
        self.rfile.close()
        self.rfile = io.BufferedReader(_DeadlineReader(self.connection, 2))

    def do_GET(self):
        health = self.server.health
        status, content_type = 200, "application/json"
        if self.path == "/healthz":
            body = b'{"alive": true}'
        elif self.path == "/readyz":
            snapshot = health.snapshot()
            status = 503 if snapshot["status"] == "not_ready" else 200
            body = json.dumps(snapshot, sort_keys=True).encode()
        elif self.path == "/metrics":
            content_type = "text/plain; version=0.0.4; charset=utf-8"
            body = health.metrics().encode()
        else:
            status, body = 404, b'{"error": 404}'
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(body)
        self.close_connection = True

    def log_message(self, *args):
        pass


class _Server(HTTPServer):
    def get_request(self):
        connection, address = super().get_request()
        connection.settimeout(2)
        return connection, address

    def handle_error(self, *args):
        pass


class HealthServer:
    """One bounded diagnostics worker on a separate listener; opt-in only."""

    def __init__(self, health: ControllerHealth, address=("127.0.0.1", 0)):
        self.health, self.address = health, address
        self._server = self._thread = None

    @property
    def bound_address(self):
        return self._server.server_address if self._server else None

    def start(self):
        if self._server is not None:
            raise RuntimeError("health server already started")
        self._server = _Server(self.address, _Handler)
        self._server.health = self.health
        self._thread = Thread(
            target=self._server.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True
        )
        self._thread.start()

    def stop(self):
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
            self._thread.join(timeout=3)
            self._server = self._thread = None
