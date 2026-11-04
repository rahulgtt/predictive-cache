"""Bounded private execution targets, separate from demand metadata."""

from __future__ import annotations

import json
from collections import Counter, OrderedDict
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from threading import RLock

from predictive_cache.dispatcher.schema import validate_handle
from predictive_cache.sources.chimera.contract import ChimeraEvent


def _utc(value: datetime) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError("target time must be timezone-aware UTC")
    return value.astimezone(UTC)


@dataclass(frozen=True)
class Target:
    """Private resolver result; retain this exact version for stale feedback."""

    volume_id: str
    file_id: str
    export_id: int
    export_name: str
    target_fh_b64: str = field(repr=False)
    file_size: int | None
    seen_at: datetime


class TargetMap:
    """Thread-safe current-state resolver, not a historical as-of database.

    Handles may leave memory only through private plans or owner-only aggregate
    checkpoints. No method logs events or returns target-bearing error text.
    """

    def __init__(
        self, exports: dict[int, str], *, max_entries: int = 100_000, target_max_age_s: int = 3600
    ):
        if (
            type(max_entries) is not int
            or max_entries < 1
            or type(target_max_age_s) is not int
            or target_max_age_s < 1
        ):
            raise ValueError("invalid target map bounds")
        if not exports or any(
            type(k) is not int
            or not 0 <= k <= 65535
            or not isinstance(v, str)
            or not v
            or len(v) > 128
            for k, v in exports.items()
        ):
            raise ValueError("invalid export allowlist")
        self._exports = dict(exports)
        self.max_entries = max_entries
        self.target_max_age_s = target_max_age_s
        self._entries: OrderedDict[tuple[str, str], tuple[Target, bool]] = OrderedDict()
        self._counts = Counter()
        self._lock = RLock()

    def _prune(self, now: datetime) -> None:
        expired = [
            key
            for key, (entry, _) in self._entries.items()
            if now - entry.seen_at > timedelta(seconds=self.target_max_age_s)
        ]
        for key in expired:
            del self._entries[key]
        self._counts["expired"] += len(expired)

    def update(self, event: ChimeraEvent, seen_at: datetime) -> bool:
        """Use authoritative Controller receipt time; all origins may refresh."""
        try:
            now = _utc(seen_at)
            if not isinstance(event, ChimeraEvent):
                raise ValueError("expected validated Chimera event")
            event = ChimeraEvent.model_validate(event.model_dump(by_alias=True))
            size = event.source_file_size
            if size is not None and not 0 <= size <= 2**64 - 1:
                raise ValueError("invalid target file size")
            handle = validate_handle(event.target_fh)
            name = self._exports[event.export_id]
            target = Target(
                event.volume_id,
                event.file_id,
                event.export_id,
                name,
                handle,
                size,
                now,
            )
        except (ValueError, TypeError, KeyError, OverflowError):
            with self._lock:
                self._counts["rejected"] += 1
            return False
        key = target.volume_id, target.file_id
        with self._lock:
            current = self._entries.get(key)
            if current is not None and target.seen_at <= current[0].seen_at:
                self._counts["old_updates"] += 1
                return False
            self._prune(now)
            self._entries[key] = target, False
            self._entries.move_to_end(key)
            while len(self._entries) > self.max_entries:
                self._entries.popitem(last=False)
                self._counts["evicted"] += 1
            self._counts["updated"] += 1
            return True

    def resolve(self, volume_id: str, file_id: str, t: datetime) -> Target | None:
        now = _utc(t)
        with self._lock:
            self._prune(now)
            key = volume_id, file_id
            current = self._entries.get(key)
            if current is None or current[1] or current[0].seen_at > now:
                self._counts["misses"] += 1
                return None
            self._entries.move_to_end(key)
            self._counts["hits"] += 1
            return current[0]

    def mark_stale(self, target: Target) -> bool:
        """Invalidate only the exact resolved version, never a later refresh."""
        if not isinstance(target, Target):
            return False
        key = target.volume_id, target.file_id
        with self._lock:
            current = self._entries.get(key)
            if current is None or current[0] != target:
                return False
            self._entries[key] = current[0], True
            self._counts["marked_stale"] += 1
            return True

    def stats(self) -> dict[str, int]:
        with self._lock:
            return {**dict(self._counts), "entries": len(self._entries)}

    def state(self) -> bytes:
        """Private checkpoint seam; caller owns safe aggregate persistence."""
        with self._lock:
            rows = [
                {**target.__dict__, "seen_at": target.seen_at.isoformat(), "stale": stale}
                for target, stale in self._entries.values()
            ]
            return json.dumps(
                {
                    "version": 1,
                    "exports": self._exports,
                    "max_entries": self.max_entries,
                    "target_max_age_s": self.target_max_age_s,
                    "entries": rows,
                },
                sort_keys=True,
                allow_nan=False,
            ).encode()

    def restore(self, state: bytes) -> None:
        """Atomically restore matching configuration; reject corrupt snapshots."""
        try:
            document = json.loads(state)
            if set(document) != {
                "version",
                "exports",
                "max_entries",
                "target_max_age_s",
                "entries",
            }:
                raise ValueError
            if (
                document["version"] != 1
                or document["exports"] != {str(k): v for k, v in self._exports.items()}
                or document["max_entries"] != self.max_entries
                or document["target_max_age_s"] != self.target_max_age_s
                or not isinstance(document["entries"], list)
                or len(document["entries"]) > self.max_entries
            ):
                raise ValueError
            restored = OrderedDict()
            for row in document["entries"]:
                if set(row) != {
                    "volume_id",
                    "file_id",
                    "export_id",
                    "export_name",
                    "target_fh_b64",
                    "file_size",
                    "seen_at",
                    "stale",
                }:
                    raise ValueError
                stale = row["stale"]
                if type(stale) is not bool or type(row["export_id"]) is not int:
                    raise ValueError
                if row["export_name"] != self._exports[row["export_id"]]:
                    raise ValueError
                for name, length in (("volume_id", 32), ("file_id", 64)):
                    if (
                        not isinstance(row[name], str)
                        or len(row[name]) != length
                        or any(c not in "0123456789abcdef" for c in row[name])
                    ):
                        raise ValueError
                size = row["file_size"]
                if size is not None and (type(size) is not int or not 0 <= size <= 2**64 - 1):
                    raise ValueError
                target = Target(
                    row["volume_id"],
                    row["file_id"],
                    row["export_id"],
                    row["export_name"],
                    validate_handle(row["target_fh_b64"]),
                    size,
                    _utc(datetime.fromisoformat(row["seen_at"])),
                )
                key = target.volume_id, target.file_id
                if key in restored:
                    raise ValueError
                restored[key] = target, stale
        except (ValueError, TypeError, KeyError, OverflowError, AttributeError):
            raise ValueError("invalid target map checkpoint") from None
        with self._lock:
            self._entries = restored
