"""Bounded, enqueue-only JSON event receiver. No request data is logged."""

from __future__ import annotations

import hmac
import io
import json
import socket
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, HTTPServer

from .ingest import EnvelopeError


class _DeadlineReader(io.RawIOBase):
    """Apply an absolute deadline, including clients trickling header bytes."""

    def __init__(self, connection, timeout):
        self.connection = connection
        self.timeout = timeout
        self.reset()

    def reset(self):
        self.deadline = time.monotonic() + self.timeout

    def readable(self):
        return True

    def readinto(self, buffer):
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("request read timeout")
        self.connection.settimeout(remaining)
        return self.connection.recv_into(buffer)


class _Server(HTTPServer):
    def __init__(self, address, source):
        self.source = source
        self.slots = threading.BoundedSemaphore(source.max_concurrent)
        self.pool = ThreadPoolExecutor(max_workers=source.max_concurrent)
        try:
            super().__init__(address, _Handler)
        except BaseException:
            self.pool.shutdown(wait=True)
            raise

    def get_request(self):
        connection, address = super().get_request()
        connection.settimeout(self.source.read_timeout_s)
        return connection, address

    def process_request(self, request, client_address):
        if not self.slots.acquire(blocking=False):
            # The accept loop cannot wait on an overloaded/slow client's writes.
            try:
                request.setblocking(False)
                request.send(
                    b"HTTP/1.1 503 Service Unavailable\r\nConnection: close\r\n"
                    b"Content-Length: 0\r\n\r\n"
                )
            except OSError:
                pass
            self.shutdown_request(request)
            return
        try:
            self.pool.submit(self._work, request, client_address)
        except BaseException:
            self.slots.release()
            self.shutdown_request(request)
            raise

    def _work(self, request, client_address):
        try:
            self.finish_request(request, client_address)
        except (OSError, ValueError):
            pass
        finally:
            self.shutdown_request(request)
            self.slots.release()

    def handle_error(self, request, client_address):
        # Never print tracebacks containing request or credential material.
        pass

    def server_close(self):
        super().server_close()
        self.pool.shutdown(wait=True)


class _Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "pcache-collector"
    sys_version = ""

    def setup(self):
        super().setup()
        self.rfile.close()
        self.reader = _DeadlineReader(self.connection, self.server.source.read_timeout_s)
        self.rfile = io.BufferedReader(self.reader)

    def log_message(self, format, *args):
        pass

    def send_error(self, code, message=None, explain=None):
        # Base error pages echo paths/methods. Emit only a fixed status code.
        self._respond(code, {"error": code})

    def _respond(self, status, payload):
        self.close_connection = True
        body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
        try:
            self.connection.settimeout(self.server.source.read_timeout_s)
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Connection", "close")
            self.end_headers()
            self.wfile.write(body)
        except OSError:
            pass

    def do_POST(self):
        source = self.server.source
        if self.path != "/v1/events":
            self._respond(404, {"error": 404})
            return
        authorization = self.headers.get_all("Authorization", [])
        expected = b"Bearer " + source.token.encode("utf-8")
        if len(authorization) != 1 or not hmac.compare_digest(
            authorization[0].encode("utf-8"), expected
        ):
            self._respond(401, {"error": 401})
            return
        lengths = self.headers.get_all("Content-Length", [])
        if self.headers.get_all("Transfer-Encoding") or len(lengths) > 1:
            self._respond(400, {"error": 400})
            return
        if not lengths:
            self._respond(411, {"error": 411})
            return
        length_text = lengths[0]
        if not length_text.isascii() or not length_text.isdecimal():
            self._respond(400, {"error": 400})
            return
        # Avoid int conversions of attacker-controlled thousands-digit integers.
        if len(length_text) > 10:
            self._respond(413, {"error": 413})
            return
        length = int(length_text)
        if length > source.max_body_bytes:
            self._respond(413, {"error": 413})
            return
        if self.headers.get_content_type() != "application/json":
            self._respond(415, {"error": 415})
            return
        try:
            self.reader.reset()
            body = self.rfile.read(length)
            if len(body) != length:
                self._respond(400, {"error": 400})
                return
            events = json.loads(body, parse_constant=_invalid_constant)
        except (TimeoutError, socket.timeout):
            self._respond(408, {"error": 408})
            return
        except (ValueError, UnicodeError, RecursionError):
            self._respond(400, {"error": 400})
            return
        if isinstance(events, dict) and source.raw_sink is not None:
            raw_events = events.get("events")
            if isinstance(raw_events, list) and len(raw_events) > source.max_events:
                self._respond(413, {"error": 413})
                return
            try:
                accepted, dropped, duplicate = source.raw_sink(events)
            except EnvelopeError as error:
                self._respond(error.status, {"error": error.status})
                return
            except Exception:
                self._respond(503, {"error": 503})
                return
            self._respond(202, {"accepted": accepted, "dropped": dropped, "duplicate": duplicate})
            return
        if source.sink is None:
            self._respond(400, {"error": 400})
            return
        if not isinstance(events, list) or any(not isinstance(row, dict) for row in events):
            self._respond(400, {"error": 400})
            return
        if len(events) > source.max_events:
            self._respond(413, {"error": 413})
            return
        try:
            accepted, dropped = source.sink(events)
        except Exception:
            # Response is sanitized; the consumer owns error counters.
            self._respond(503, {"error": 503})
            return
        self._respond(202, {"accepted": accepted, "dropped": dropped})


def _invalid_constant(value):
    raise ValueError("non-finite JSON number")


class HttpSource:
    """Receiver with eight workers and no unbounded pending-request queue.

    ``sink`` must only enqueue, returning ``(accepted, dropped)``. Cleaning
    and feature updates belong in the separate consumer, never this callback.
    Limits may be lowered, but cannot exceed the frozen protocol bounds.
    """

    def __init__(
        self,
        address=("127.0.0.1", 8090),
        *,
        token,
        max_body_bytes=4 * 1024 * 1024,
        max_events=10000,
        max_concurrent=8,
        read_timeout_s=2,
    ):
        if not isinstance(token, str) or not token or any(c.isspace() for c in token):
            raise ValueError("a nonempty bearer token without whitespace is required")
        for value, cap in (
            (max_body_bytes, 4 * 1024 * 1024),
            (max_events, 10000),
            (max_concurrent, 8),
        ):
            if type(value) is not int or not 1 <= value <= cap:
                raise ValueError("HTTP limit outside supported bounds")
        if not 0 < read_timeout_s <= 2:
            raise ValueError("HTTP read timeout outside supported bounds")
        self.address = address
        self.token = token
        self.max_body_bytes = max_body_bytes
        self.max_events = max_events
        self.max_concurrent = max_concurrent
        self.read_timeout_s = read_timeout_s
        self.server = None
        self.thread = None
        self.sink = self.raw_sink = None

    @property
    def bound_address(self):
        return self.server.server_address if self.server else None

    def start(self, sink=None, *, raw_sink=None):
        if self.server is not None:
            raise RuntimeError("HTTP source already started")
        self.sink, self.raw_sink = sink, raw_sink
        self.server = _Server(self.address, self)
        self.thread = threading.Thread(
            target=self.server.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True
        )
        self.thread.start()

    def stop(self):
        if self.server is not None:
            self.server.shutdown()
            self.server.server_close()
            self.thread.join()
            self.server = self.thread = None
