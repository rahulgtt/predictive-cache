"""Bounded ingress and shared batch/stream cleaning, without a feature store."""

from __future__ import annotations

import json
from collections import Counter, OrderedDict, deque
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from threading import Lock, RLock

from predictive_cache.datagen.io import canonical_json
from predictive_cache.datagen.schema import normalize_rows
from predictive_cache.prep.clean.handles import HandleTracker
from predictive_cache.prep.clean.rules import classify_event
from predictive_cache.prep.config import CleanConfig

US = timedelta(microseconds=1)
MIN_TIME = datetime.min.replace(tzinfo=UTC)


def instant(value: datetime | str) -> datetime:
    value = (
        datetime.fromisoformat(value.replace("Z", "+00:00")) if isinstance(value, str) else value
    )
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ValueError("timestamp must include a UTC offset")
    return value.astimezone(UTC)


def update_key(update: dict) -> tuple:
    return (
        update["known_at"],
        {"MetaChanged": 0, "OpenObserved": 1, "OpenCompleted": 2}[update["kind"]],
        update.get("event_id", update.get("open_id", "")),
    )


def _project(update: dict) -> dict:
    kind = update["kind"]
    if kind == "OpenCompleted":
        row = update["row"]
        return {
            "kind": kind,
            **{
                key: row[key]
                for key in (
                    "open_id",
                    "volume_id",
                    "file_id",
                    "bytes_read",
                    "distinct_bytes_read",
                    "full_read",
                    "consumed",
                )
            },
            "known_at": row["completed_known_at"],
        }
    event = update["event"]
    if kind == "MetaChanged":
        return {
            "kind": kind,
            **{
                key: event[key]
                for key in (
                    "event_id",
                    "volume_id",
                    "file_id",
                    "dir_id",
                    "operation",
                    "effective_time",
                    "known_at",
                    "file_size",
                )
            },
        }
    return {
        "kind": kind,
        "open_id": HandleTracker.key(event),
        **{
            key: event[key]
            for key in (
                "volume_id",
                "file_id",
                "dir_id",
                "cohort_id",
                "app",
                "known_at",
                "file_size",
            )
        },
        "session_key": event["session_id"] or event["client_id"],
        "open_time": event["effective_time"],
    }


class Ingestor:
    """One consumer, nonblocking producers; drain emits only facts strictly before now.

    A tracker capacity breach fails closed for the rest of this instance's life.
    Its partial handles are discarded and loss is exposed in stats, rather than
    silently forgetting completed IDs or reporting partial consumption as complete.
    Checkpoints are local JSON snapshots, not the aggregate Controller checkpoint.
    """

    def __init__(
        self,
        clean_config: CleanConfig,
        sink: Callable[[dict], None] | None = None,
        *,
        queue_max: int = 200_000,
        allowed_volumes: set[str] | list[str],
        clock=None,
        dedupe_max: int = 1_000_000,
        tracker_max_events: int = 1_000_000,
        max_event_bytes: int = 65_536,
    ):
        if min(queue_max, dedupe_max, tracker_max_events, max_event_bytes) < 1:
            raise ValueError("ingestion capacities must be positive")
        if not allowed_volumes or any(not isinstance(v, str) or not v for v in allowed_volumes):
            raise ValueError("allowed_volumes must contain nonempty volume IDs")
        self.cfg, self.sink, self.clock = clean_config, sink, clock
        self.queue_max, self.dedupe_max = queue_max, dedupe_max
        self.tracker_max_events, self.max_event_bytes = (
            tracker_max_events,
            max_event_bytes,
        )
        self.allowed_volumes = frozenset(allowed_volumes)
        self._queue = deque()
        self._lock, self._consumer_lock = Lock(), RLock()
        self._last_known = self._advanced = MIN_TIME
        self._last_replay_key = None
        self._seen = OrderedDict()
        self._tracker = HandleTracker(clean_config)
        self._tracker_exhausted = False
        self._tracker_failed = False
        self._tracker_failure_reason = None
        self._counts, self._excluded = Counter({"dropped_busy": 0, "dropped": 0}), Counter()
        self._last_error = None

    def _now(self):
        return instant(self.clock.now() if self.clock else datetime.now(UTC))

    def offer(self, events: list[dict], receipt: datetime | None = None, *, replay=False):
        """Return (queued, dropped), without waiting for a consumer or queue space.

        Acceptance means queued, not validated demand. Live source ingest_time is
        diagnostic only. Queue contention is itself a counted drop.
        """
        if not isinstance(events, list):
            raise ValueError("events must be a JSON array")
        received = instant(receipt) if receipt is not None else self._now()
        if not self._lock.acquire(blocking=False):
            self._counts["dropped_busy"] += len(events)
            self._counts["dropped"] += len(events)
            return 0, len(events)
        accepted = dropped = 0
        try:
            ordered = (
                events
                if replay
                else sorted(
                    events,
                    key=lambda row: (
                        row.get("event_id", "")
                        if isinstance(row, dict) and isinstance(row.get("event_id", ""), str)
                        else ""
                    ),
                )
            )
            for raw in ordered:
                reason = None
                if not isinstance(raw, dict):
                    reason = "schema_invalid"
                elif (
                    not isinstance(raw.get("volume_id"), str)
                    or raw["volume_id"] not in self.allowed_volumes
                ):
                    reason = "volume"
                elif len(self._queue) >= self.queue_max:
                    reason = "overflow"
                if reason is None:
                    try:
                        encoded = canonical_json(raw)
                        if len(encoded.encode("utf-8")) > self.max_event_bytes:
                            reason = "event_too_large"
                        else:
                            raw = json.loads(encoded)  # own the queued snapshot
                    except (TypeError, ValueError, OverflowError):
                        reason = "schema_invalid"
                if reason is None and replay:
                    try:
                        known = instant(raw.get("ingest_time"))
                        key = (known, raw.get("event_id", ""))
                        if (
                            not isinstance(key[1], str)
                            or known < self._advanced
                            or known < self._last_known
                            or (self._last_replay_key is not None and key < self._last_replay_key)
                        ):
                            reason = "replay_order"
                    except (ValueError, TypeError, OverflowError):
                        reason = "schema_invalid"
                elif reason is None:
                    known = max(received, self._last_known + US, self._advanced)
                if reason:
                    dropped += 1
                    self._counts[f"dropped_{reason}"] += 1
                    continue
                if replay:
                    self._last_replay_key = key
                self._last_known = max(self._last_known, known)
                self._queue.append((raw, known))
                accepted += 1
            self._counts["accepted"] += accepted
            self._counts["dropped"] += dropped
            return accepted, dropped
        finally:
            self._lock.release()

    def _tracker_size(self):
        return len(self._tracker.completed) + sum(
            1 + bool(g["open"]) + bool(g["close"]) + len(g["reads"])
            for g in self._tracker.groups.values()
        )

    def _fail_tracker(self, reason):
        self._counts["tracker_discarded_handles"] += len(self._tracker.groups)
        self._tracker.groups.clear()
        self._tracker.completed.clear()
        self._tracker_failed = True
        self._tracker_failure_reason = reason

    def _advance_tracker(self, now):
        if self._tracker_failed:
            return []
        return self._tracker.advance(now)

    def _consume(self, raw, known):
        # Schema errors precede duplicates, but even invalid first deliveries
        # reserve their ID, matching the canonical-delivery rule in run_clean.
        reason = classify_event(raw, known, self.cfg)
        raw_id = raw.get("event_id")
        cutoff = known - timedelta(seconds=self.cfg.dedupe_window_s)
        while self._seen and next(iter(self._seen.values())) < cutoff:
            self._seen.popitem(last=False)
            self._counts["dedupe_expired"] += 1
        duplicate = isinstance(raw_id, str) and raw_id in self._seen
        if isinstance(raw_id, str):
            self._seen[raw_id] = known
            self._seen.move_to_end(raw_id)
            if len(self._seen) > self.dedupe_max:
                self._seen.popitem(last=False)
                self._counts["dedupe_capacity_evictions"] += 1
        if reason != "schema_invalid" and duplicate:
            reason = "duplicate"
        if reason:
            self._excluded[reason] += 1
            return []
        event = normalize_rows("events", [raw])[0]
        event.update(known_at=known, effective_time=min(event["event_time"], known))
        if self._tracker_failed:
            self._counts["dropped_tracker_failed"] += 1
            if self._tracker_exhausted:
                self._counts["dropped_tracker_capacity"] += 1
            return []
        key = self._tracker.key(event)
        group = self._tracker.groups.get(key)
        if event["operation"] == "open" and (
            key in self._tracker.completed or (group is not None and group["open"] is not None)
        ):
            self._counts["lifecycle_conflicts"] += 1
            self._counts["dropped_lifecycle_conflict"] += 1
            self._fail_tracker("lifecycle_conflict")
            return []
        # Conservative reserve of two units covers a new group and its event.
        if self._tracker_size() + 2 > self.tracker_max_events:
            self._tracker_exhausted = True
            self._fail_tracker("capacity")
            self._counts["dropped_tracker_capacity"] += 1
            return []
        self._counts["kept_events"] += 1
        return self._tracker.push(event)

    def drain(self, now: datetime | None = None) -> list[dict]:
        """Clean ready events, finalize expired handles, sort and deliver updates.

        Sink failures propagate and count the failed and unattempted updates as
        lost. They never block offer; callers must surface this degraded state.
        """
        now = instant(now) if now is not None else self._now()
        with self._consumer_lock:
            if now < self._advanced:
                raise ValueError("drain clock cannot move backwards")
            with self._lock:
                ready = []
                while self._queue and self._queue[0][1] < now:
                    ready.append(self._queue.popleft())
                self._advanced = now
            updates = []
            for raw, known in ready:
                updates.extend(self._advance_tracker(known - US))
                updates.extend(self._consume(raw, known))
            updates.extend(self._advance_tracker(now - US))
            result = sorted((_project(u) for u in updates), key=update_key)
            self._counts["processed"] += len(ready)
            if self.sink is not None:
                for index, update in enumerate(result):
                    try:
                        self.sink(update)
                    except Exception as exc:
                        self._counts["consumer_errors"] += 1
                        self._counts["updates_lost_consumer_error"] += len(result) - index
                        self._last_error = type(exc).__name__
                        raise
            self._counts["updates_emitted"] += len(result)
            return result

    def advance(self, now: datetime) -> list[dict]:
        return self.drain(now)

    def stats(self) -> dict:
        with self._consumer_lock, self._lock:
            return {
                **{
                    key: self._counts[key]
                    for key in (
                        "accepted",
                        "dropped",
                        "dropped_overflow",
                        "dropped_volume",
                        "dropped_busy",
                        "processed",
                        "kept_events",
                        "dedupe_capacity_evictions",
                        "dedupe_expired",
                        "dropped_tracker_capacity",
                        "dropped_tracker_failed",
                        "dropped_lifecycle_conflict",
                        "lifecycle_conflicts",
                        "tracker_discarded_handles",
                        "consumer_errors",
                        "updates_lost_consumer_error",
                        "updates_emitted",
                    )
                },
                **dict(self._counts),
                "excluded_by_reason": dict(self._excluded),
                "queue_depth": len(self._queue),
                "dedupe_size": len(self._seen),
                "tracker_size": self._tracker_size(),
                "tracker_exhausted": self._tracker_exhausted,
                "tracker_failed": self._tracker_failed,
                "tracker_failure_reason": self._tracker_failure_reason,
                "late_reads_after_completion": self._tracker.late_reads_after_completion,
                "unexpected_reads_in_summary_mode": self._tracker.unexpected_reads_in_summary_mode,
                "last_error": self._last_error,
            }

    def _contract(self):
        return {
            "clean": self.cfg.model_dump(mode="json"),
            "queue_max": self.queue_max,
            "dedupe_max": self.dedupe_max,
            "tracker_max_events": self.tracker_max_events,
            "max_event_bytes": self.max_event_bytes,
            "allowed_volumes": sorted(self.allowed_volumes),
        }

    def state(self) -> bytes:
        with self._consumer_lock, self._lock:
            return canonical_json(
                {
                    "version": 1,
                    "contract": self._contract(),
                    "queue": list(self._queue),
                    "last_known": self._last_known,
                    "advanced": self._advanced,
                    "last_replay_key": self._last_replay_key,
                    "seen": list(self._seen.items()),
                    "tracker": {
                        "groups": self._tracker.groups,
                        "completed": sorted(self._tracker.completed),
                        "late": self._tracker.late_reads_after_completion,
                        "summary": self._tracker.unexpected_reads_in_summary_mode,
                    },
                    "tracker_exhausted": self._tracker_exhausted,
                    "tracker_failed": self._tracker_failed,
                    "tracker_failure_reason": self._tracker_failure_reason,
                    "counts": dict(self._counts),
                    "excluded": dict(self._excluded),
                    "last_error": self._last_error,
                }
            ).encode("utf-8")

    def restore(self, data: bytes) -> None:
        """Restore only an intact local snapshot with the same serving contract."""
        document = json.loads(data)
        if document["version"] != 1 or document["contract"] != self._contract():
            raise ValueError("ingestor checkpoint contract mismatch")
        tracker = HandleTracker(self.cfg)
        tracker.groups = document["tracker"]["groups"]
        for group in tracker.groups.values():
            group["last_known"] = instant(group["last_known"])
            for event in [group["open"], group["close"], *group["reads"]]:
                if event is not None:
                    for field in (
                        "event_time",
                        "ingest_time",
                        "known_at",
                        "effective_time",
                    ):
                        event[field] = instant(event[field])
        tracker.completed = set(document["tracker"]["completed"])
        tracker.late_reads_after_completion = document["tracker"]["late"]
        tracker.unexpected_reads_in_summary_mode = document["tracker"]["summary"]
        queue = deque((raw, instant(known)) for raw, known in document["queue"])
        seen = OrderedDict((key, instant(known)) for key, known in document["seen"])
        size = len(tracker.completed) + sum(
            1 + bool(g["open"]) + bool(g["close"]) + len(g["reads"])
            for g in tracker.groups.values()
        )
        if (
            len(queue) > self.queue_max
            or len(seen) > self.dedupe_max
            or size > self.tracker_max_events
        ):
            raise ValueError("ingestor checkpoint exceeds capacity")
        with self._consumer_lock, self._lock:
            self._queue, self._seen, self._tracker = queue, seen, tracker
            self._last_known, self._advanced = (
                instant(document["last_known"]),
                instant(document["advanced"]),
            )
            key = document["last_replay_key"]
            self._last_replay_key = (instant(key[0]), key[1]) if key is not None else None
            self._tracker_exhausted = document["tracker_exhausted"]
            self._tracker_failed = document["tracker_failed"]
            self._tracker_failure_reason = document["tracker_failure_reason"]
            self._counts, self._excluded = (
                Counter(document["counts"]),
                Counter(document["excluded"]),
            )
            self._last_error = document["last_error"]


class EnvelopeError(ValueError):
    """Sanitized raw-ingress rejection; never carries request data."""

    def __init__(self, status=400):
        super().__init__("invalid raw event envelope")
        self.status = status


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


class ChimeraTargetIngestor:
    """Bounded, target-only raw ingress. No demand or coverage assertions.

    Dedupe is exact for admitted sequence numbers while this instance lives.
    New epochs fail closed at the per-pod cap rather than forgetting identities.
    Raw queue and dedupe restart persistence are deliberately unsupported.
    """

    def __init__(
        self,
        targets,
        *,
        allowed_volumes,
        shippers,
        queue_max=200_000,
        max_event_bytes=65_536,
        max_epochs_per_pod=4,
        max_sequence_gap=4096,
        clock=None,
    ):
        if not allowed_volumes or not shippers:
            raise ValueError("raw ingress requires volume and shipper allowlists")
        if any(not isinstance(v, str) or not v for v in (*allowed_volumes, *shippers)):
            raise ValueError("invalid ingress allowlists")
        if any(
            type(v) is not int or v < 1
            for v in (queue_max, max_event_bytes, max_epochs_per_pod, max_sequence_gap)
        ):
            raise ValueError("invalid raw ingress bounds")
        self.targets, self.clock = targets, clock
        self.allowed_volumes, self.shippers = frozenset(allowed_volumes), frozenset(shippers)
        self.queue_max, self.max_event_bytes = queue_max, max_event_bytes
        self.max_epochs_per_pod, self.max_sequence_gap = max_epochs_per_pod, max_sequence_gap
        self._queue, self._streams = deque(), {}
        self._lock, self._consumer_lock = Lock(), RLock()
        self._last_known = self._advanced = MIN_TIME
        self._counts, self._excluded = Counter(), Counter()

    def _envelope(self, body):
        from predictive_cache.sources.chimera.contract import canonical_uuid

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
            events, health = body["events"], body["health"]
            if not isinstance(events, list) or any(not isinstance(e, dict) for e in events):
                raise EnvelopeError()
            if len(events) > 10000:
                raise EnvelopeError(413)
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
            _u64(health["scraped_at_ns"])
            synced = _u64(health["last_synced_sequence"])
            if last > synced:
                raise EnvelopeError()
            for name in ("barrier_ready", "delivery_clean"):
                if type(health[name]) is not bool:
                    raise EnvelopeError()
            counters = {name: health[name] for name in ("queue_depth", "worker_in_flight")}
            for name, keys in (
                ("dropped", {"queue", "encode", "sink", "rollback", "rotation"}),
                ("shipper_dropped", {"rotated_unsent", "stale_epoch"}),
            ):
                if not isinstance(health[name], dict) or set(health[name]) != keys:
                    raise EnvelopeError()
                counters.update({name + key: value for key, value in health[name].items()})
            if any(
                type(value) is not int or not 0 <= value <= 2**64 - 1 for value in counters.values()
            ):
                raise EnvelopeError()
            ordered = []
            for raw in events:
                sequence = _u64(raw.get("producer_sequence"))
                if (
                    raw.get("producer_epoch") != epoch
                    or not first <= sequence <= last
                    or sequence == 0
                ):
                    raise EnvelopeError()
                ordered.append((sequence, raw))
            if ordered and (
                min(s for s, _ in ordered) != first or max(s for s, _ in ordered) != last
            ):
                raise EnvelopeError()
            return pod, epoch, sorted(ordered, key=lambda pair: pair[0])
        except EnvelopeError:
            raise
        except (TypeError, ValueError, KeyError, OverflowError, AttributeError):
            raise EnvelopeError() from None

    def offer(self, body, receipt=None):
        """Validate envelope/admit only; raw row parsing and target updates wait for drain."""
        pod, epoch, events = self._envelope(body)
        received = (
            instant(receipt)
            if receipt is not None
            else instant(self.clock.now() if self.clock else datetime.now(UTC))
        )
        if not self._lock.acquire(blocking=False):
            self._counts["dropped_busy"] += len(events)
            self._counts["dropped"] += len(events)
            return 0, len(events), 0
        accepted = dropped = duplicate = 0
        try:
            key = pod, epoch
            if key not in self._streams and events:
                if sum(p == pod for p, _ in self._streams) >= self.max_epochs_per_pod:
                    self._counts["dropped_epoch_capacity"] += len(events)
                    self._counts["dropped"] += len(events)
                    return 0, len(events), 0
            stream = self._streams.get(key, [events[0][0] - 1, set()] if events else None)
            for sequence, raw in events:
                high, pending = stream
                if sequence <= high or sequence in pending:
                    duplicate += 1
                    continue
                reason = None
                if (
                    not isinstance(raw.get("volume_id"), str)
                    or raw["volume_id"] not in self.allowed_volumes
                ):
                    reason = "volume"
                elif len(self._queue) >= self.queue_max:
                    reason = "overflow"
                elif sequence - high > self.max_sequence_gap:
                    reason = "sequence_gap"
                try:
                    encoded = canonical_json(raw)
                    if len(encoded.encode()) > self.max_event_bytes:
                        reason = "event_too_large"
                except (TypeError, ValueError, OverflowError):
                    reason = "schema_invalid"
                if reason:
                    dropped += 1
                    self._counts["dropped_" + reason] += 1
                    continue
                known = max(received, self._last_known + US, self._advanced)
                self._last_known = known
                self._queue.append((json.loads(encoded), known))
                self._streams[key] = stream
                pending.add(sequence)
                while stream[0] + 1 in pending:
                    stream[0] += 1
                    pending.remove(stream[0])
                accepted += 1
            self._counts["accepted"] += accepted
            self._counts["dropped"] += dropped
            self._counts["duplicate"] += duplicate
            self._counts["heartbeats"] += not events
            return accepted, dropped, duplicate
        finally:
            self._lock.release()

    def drain(self, now=None):
        from predictive_cache.sources.chimera.contract import ChimeraEvent

        now = (
            instant(now)
            if now is not None
            else instant(self.clock.now() if self.clock else datetime.now(UTC))
        )
        with self._consumer_lock:
            with self._lock:
                if now < self._advanced:
                    raise ValueError("drain clock cannot move backwards")
                rows = []
                while self._queue and self._queue[0][1] < now:
                    rows.append(self._queue.popleft())
                self._advanced = now
            for raw, known in rows:
                try:
                    event = ChimeraEvent.model_validate(raw)
                except (ValueError, TypeError, KeyError, OverflowError):
                    self._excluded["schema_invalid"] += 1
                else:
                    if self.targets.update(event, known):
                        self._counts["targets_updated"] += 1
                    else:
                        self._excluded["target_rejected"] += 1
                    if event.outcome != "success":
                        self._excluded["result_failed"] += 1
                    elif event.origin_claim != "controlled_demand":
                        self._excluded["origin_" + event.origin_claim] += 1
                    else:
                        self._excluded["controlled_demand_not_enabled"] += 1
                self._counts["processed"] += 1
        return []

    def stats(self):
        with self._consumer_lock, self._lock:
            return {
                **dict(self._counts),
                "accepted": self._counts["accepted"],
                "processed": self._counts["processed"],
                "dropped": self._counts["dropped"],
                "duplicate": self._counts["duplicate"],
                "queue_depth": len(self._queue),
                "stream_count": len(self._streams),
                "pending_sequences": sum(len(pending) for _, pending in self._streams.values()),
                "excluded_by_reason": dict(self._excluded),
                "updates_emitted": 0,
                "tracker_failed": False,
                "tracker_exhausted": False,
            }

    def state(self):
        raise NotImplementedError("raw ingress restart continuity is not implemented")
