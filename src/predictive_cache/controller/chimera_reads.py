"""Bounded controlled READ bridge; source completeness remains unproven."""

from __future__ import annotations

import hashlib
import json
from collections import Counter, OrderedDict, deque
from datetime import UTC, datetime, timedelta
from threading import Lock, RLock

from predictive_cache.dispatcher.schema import parse_utc_z
from predictive_cache.semantics.read_demand import canonical_json, project_chimera_read
from predictive_cache.sources.chimera.contract import ChimeraEvent, canonical_uuid

_US = timedelta(microseconds=1)
_MIN = datetime.min.replace(tzinfo=UTC)


class EnvelopeError(ValueError):
    def __init__(self, status=400):
        super().__init__("invalid raw READ envelope")
        self.status = status
        self.loss_accounted = False


def _u64(value):
    if (
        not isinstance(value, str)
        or not value.isascii()
        or not value.isdecimal()
        or len(value) > 20
        or (len(value) > 1 and value.startswith("0"))
    ):
        raise EnvelopeError()
    result = int(value)
    if result > 2**64 - 1:
        raise EnvelopeError()
    return result


class ChimeraReadAdapter:
    """Enqueue raw rows, refresh private targets and project controlled demand.

    The synced watermark permits forwarding. It does not prove complete demand
    history. This bridge never emits complete coverage or permits warming.
    State is volatile; a new instance is not recovery of an acknowledged queue.
    """

    def __init__(
        self,
        targets,
        *,
        allowed_volumes,
        shippers,
        queue_max=100000,
        max_queue_bytes=32 * 2**20,
        max_event_bytes=65536,
        max_epochs_per_pod=4,
        max_sequence_gap=4096,
        clock=None,
    ):
        if not allowed_volumes or not shippers or len(shippers) > 64:
            raise ValueError("bounded volume and shipper allowlists required")
        if any(
            not isinstance(v, str) or not v or len(v) > 128 for v in (*allowed_volumes, *shippers)
        ):
            raise ValueError("invalid source allowlist")
        if any(
            type(v) is not int or v < 1
            for v in (
                queue_max,
                max_queue_bytes,
                max_event_bytes,
                max_epochs_per_pod,
                max_sequence_gap,
            )
        ):
            raise ValueError("invalid raw bridge bounds")
        # One drain feeds one READ runtime tick, whose admission cap is 100000.
        if queue_max > 100000:
            raise ValueError("raw queue exceeds READ consumer batch limit")
        self.targets, self.clock = targets, clock
        self.allowed_volumes, self.shippers = frozenset(allowed_volumes), frozenset(shippers)
        self.queue_max, self.max_queue_bytes, self.max_event_bytes = (
            queue_max,
            max_queue_bytes,
            max_event_bytes,
        )
        self.max_epochs_per_pod, self.max_sequence_gap = max_epochs_per_pod, max_sequence_gap
        self._lock, self._consumer_lock = Lock(), RLock()
        self._queue, self._streams, self._active = deque(), {}, {}
        self._bytes = 0
        self._last_known = self._advanced = _MIN
        self._counts, self._excluded = Counter(), Counter()
        self._loss = False

    @property
    def source_gates(self):
        with self._lock:
            return frozenset({"memory_gap", "ingest_loss"} if self._loss else {"memory_gap"})

    def reject_input(self):
        """Account for an authenticated request rejected before row admission."""
        with self._lock:
            self._loss = True
            self._counts["rejected_requests"] += 1

    def _envelope(self, body):
        try:
            if (
                not isinstance(body, dict)
                or set(body)
                != {
                    "schema",
                    "pod_uid",
                    "producer_epoch",
                    "first_sequence",
                    "last_sequence",
                    "health",
                    "events",
                }
                or body["schema"] != "nfs4_read_v2"
            ):
                raise EnvelopeError()
            pod, epoch = body["pod_uid"], body["producer_epoch"]
            if not isinstance(pod, str):
                raise EnvelopeError()
            if pod not in self.shippers:
                raise EnvelopeError(403)
            canonical_uuid(epoch)
            first, last = _u64(body["first_sequence"]), _u64(body["last_sequence"])
            if first > last:
                raise EnvelopeError()
            health = body["health"]
            if not isinstance(health, dict) or set(health) != {
                "scraped_at_ns",
                "last_synced_sequence",
                "barrier_ready",
                "delivery_clean",
                "queue_depth",
                "worker_in_flight",
                "dropped",
                "shipper_dropped",
            }:
                raise EnvelopeError()
            scraped, synced = _u64(health["scraped_at_ns"]), _u64(health["last_synced_sequence"])
            if last > synced or any(
                type(health[k]) is not bool for k in ("barrier_ready", "delivery_clean")
            ):
                raise EnvelopeError()
            counters = {k: health[k] for k in ("queue_depth", "worker_in_flight")}
            losses = {}
            for name, keys in (
                ("dropped", {"queue", "encode", "sink", "rollback", "rotation"}),
                ("shipper_dropped", {"rotated_unsent", "stale_epoch"}),
            ):
                if not isinstance(health[name], dict) or set(health[name]) != keys:
                    raise EnvelopeError()
                losses.update({name + ":" + key: value for key, value in health[name].items()})
            if any(
                type(value) is not int or not 0 <= value <= 2**64 - 1
                for value in (*counters.values(), *losses.values())
            ):
                raise EnvelopeError()
            events = body["events"]
            if not isinstance(events, list) or any(not isinstance(row, dict) for row in events):
                raise EnvelopeError()
            if len(events) > 10000:
                raise EnvelopeError(413)
            ordered = []
            for row in events:
                sequence = _u64(row.get("producer_sequence"))
                if (
                    sequence == 0
                    or row.get("producer_epoch") != epoch
                    or not first <= sequence <= last
                ):
                    raise EnvelopeError()
                ordered.append((sequence, row))
            if ordered and (
                min(s for s, _ in ordered) != first or max(s for s, _ in ordered) != last
            ):
                raise EnvelopeError()
            return (
                pod,
                epoch,
                sorted(ordered, key=lambda pair: pair[0]),
                health,
                losses,
                scraped,
                synced,
            )
        except EnvelopeError:
            raise
        except (ValueError, TypeError, KeyError, OverflowError, AttributeError):
            raise EnvelopeError() from None

    def offer(self, body, receipt=None):
        """Validate bounded framing and enqueue; never parse READs or call models."""
        try:
            pod, epoch, events, health, losses, scraped, synced = self._envelope(body)
            received = parse_utc_z(
                receipt
                if receipt is not None
                else (self.clock.now() if self.clock else datetime.now(UTC))
            )
        except EnvelopeError as exc:
            self.reject_input()
            exc.loss_accounted = True
            raise
        except (ValueError, TypeError):
            self.reject_input()
            raise
        with self._lock:
            key = pod, epoch
            stream = self._streams.get(key)
            active = self._active.get(pod)
            if stream is None:
                if sum(p == pod for p, _ in self._streams) >= self.max_epochs_per_pod:
                    self._loss = True
                    self._counts["epoch_capacity"] += 1
                    self._counts["dropped"] += len(events)
                    return 0, len(events), 0
                if active is not None:
                    self._loss = True
                    self._counts["epoch_changes"] += 1
                first = events[0][0] if events else synced + 1
                stream = {
                    "high": first - 1,
                    "pending": set(),
                    "digests": OrderedDict(),
                    "losses": losses,
                    "scraped": scraped,
                    "synced": synced,
                }
                self._streams[key] = stream
                self._active[pod] = epoch
                self._counts["unknown_start"] += 1
            elif active != epoch:
                self._loss = True
                self._counts["stale_epoch"] += len(events)
                self._counts["dropped"] += len(events)
                return 0, len(events), 0
            if (
                scraped < stream["scraped"]
                or synced < stream["synced"]
                or losses != stream["losses"]
                or any(losses.values())
                or not health["delivery_clean"]
            ):
                self._loss = True
                self._counts["source_health_loss"] += 1
            stream["losses"], stream["scraped"], stream["synced"] = losses, scraped, synced
            accepted = dropped = duplicate = 0
            for sequence, raw in events:
                try:
                    encoded = canonical_json(raw).encode()
                    digest = hashlib.sha256(encoded).digest()
                except (ValueError, TypeError, OverflowError, RecursionError):
                    encoded = None
                if encoded is None or len(encoded) > self.max_event_bytes:
                    self._loss = True
                    dropped += 1
                    self._counts["invalid_row_size"] += 1
                    continue
                previous = stream["digests"].get(sequence)
                if sequence <= stream["high"] or sequence in stream["pending"]:
                    duplicate += 1
                    if previous is not None and previous != digest:
                        self._loss = True
                        self._counts["conflicting_sequence"] += 1
                    continue
                reason = None
                if (
                    not isinstance(raw.get("volume_id"), str)
                    or raw["volume_id"] not in self.allowed_volumes
                ):
                    reason = "volume"
                elif sequence - stream["high"] > self.max_sequence_gap:
                    reason = "sequence_gap"
                elif (
                    len(self._queue) >= self.queue_max
                    or self._bytes + len(encoded) > self.max_queue_bytes
                ):
                    reason = "overflow"
                if reason:
                    self._loss = True
                    dropped += 1
                    self._counts["dropped_" + reason] += 1
                    continue
                if sequence > stream["high"] + 1:
                    self._loss = True
                    self._counts["sequence_holes"] += 1
                known = max(received, self._last_known + _US, self._advanced + _US)
                self._last_known = known
                self._queue.append((encoded, known))
                self._bytes += len(encoded)
                stream["digests"][sequence] = digest
                while len(stream["digests"]) > self.max_sequence_gap:
                    stream["digests"].popitem(last=False)
                stream["pending"].add(sequence)
                while stream["high"] + 1 in stream["pending"]:
                    stream["high"] += 1
                    stream["pending"].remove(stream["high"])
                accepted += 1
            self._counts["accepted"] += accepted
            self._counts["dropped"] += dropped
            self._counts["duplicate"] += duplicate
            self._counts["heartbeats"] += not events
            return accepted, dropped, duplicate

    def drain(self, t=None):
        now = parse_utc_z(
            t if t is not None else (self.clock.now() if self.clock else datetime.now(UTC))
        )
        with self._consumer_lock:
            with self._lock:
                if now < self._advanced:
                    raise ValueError("READ drain clock cannot move backwards")
                ready = []
                while self._queue and self._queue[0][1] <= now:
                    encoded, known = self._queue.popleft()
                    self._bytes -= len(encoded)
                    ready.append((encoded, known))
                self._advanced = now
            output = []
            for encoded, known in ready:
                reason = None
                try:
                    event = ChimeraEvent.model_validate(json.loads(encoded))
                    if not self.targets.update(event, known):
                        raise ValueError("target rejected")
                    with self._lock:
                        self._counts["targets_updated"] += 1
                    if event.outcome != "success":
                        reason = "result_failed"
                    elif event.origin_claim != "controlled_demand":
                        reason = "origin_" + event.origin_claim
                    else:
                        row = project_chimera_read(
                            event,
                            durable_at=known,
                            dir_id=None,
                            file_size=event.source_file_size,
                            coverage_status="unknown",
                        )
                        if row.event_time > row.known_at:
                            raise ValueError("source clock ahead")
                        output.append(row)
                except (ValueError, TypeError, KeyError, OverflowError, AttributeError):
                    reason = "invalid_read"
                    with self._lock:
                        self._loss = True
                with self._lock:
                    self._counts["processed"] += 1
                    if reason:
                        self._excluded[reason] += 1
            with self._lock:
                self._counts["updates_emitted"] += len(output)
            return output

    def stats(self):
        with self._lock:
            return {
                **{
                    key: self._counts[key]
                    for key in (
                        "accepted",
                        "processed",
                        "dropped",
                        "duplicate",
                        "updates_emitted",
                        "heartbeats",
                        "rejected_requests",
                    )
                },
                **dict(self._counts),
                "queue_depth": len(self._queue),
                "queue_bytes": self._bytes,
                "stream_count": len(self._streams),
                "pending_sequences": sum(len(s["pending"]) for s in self._streams.values()),
                "excluded_by_reason": dict(self._excluded),
                "coverage": "unknown",
                "loss": self._loss,
            }


def tick_chimera(adapter, runtime, t):
    """Connect the controlled source without converting unknown coverage to ready."""
    if runtime.targets is not adapter.targets:
        raise ValueError("controlled runtime must use the observed target map")
    rows = adapter.drain(t)
    return runtime.tick(t, events=rows, source_gates=adapter.source_gates)
