"""Incremental handle reconstruction shared by batch and serving."""

from __future__ import annotations

from datetime import timedelta

from predictive_cache.prep.config import CleanConfig


def _distinct_bytes(reads: list[dict]) -> int:
    intervals = sorted((r["offset"], r["offset"] + r["bytes_read"]) for r in reads)
    total = end = 0
    for start, stop in intervals:
        if stop > end:
            total += stop - max(start, end)
            end = stop
    return total


class HandleTracker:
    """Consume kept events ordered by (known_at, event_id)."""

    def __init__(self, cfg: CleanConfig):
        self.cfg = cfg
        self.groups: dict[str, dict] = {}
        self.completed: set[str] = set()
        self.late_reads_after_completion = 0
        self.unexpected_reads_in_summary_mode = 0

    @staticmethod
    def key(event: dict) -> str:
        return event["handle_id"] or f"orphan-{event['event_id']}"

    def push(self, event: dict) -> list[dict]:
        key = self.key(event)
        operation = event["operation"]
        if operation in {"create", "write", "rename", "delete"}:
            return [{"kind": "MetaChanged", "event": event}]
        if key in self.completed:
            if operation == "read":
                self.late_reads_after_completion += 1
            return []
        group = self.groups.setdefault(
            key, {"open": None, "close": None, "reads": [], "last_known": event["known_at"]}
        )
        group["last_known"] = max(group["last_known"], event["known_at"])
        if operation == "open":
            group["open"] = event
            return [{"kind": "OpenObserved", "event": event}]
        if operation == "close":
            group["close"] = event
        elif operation == "read":
            if self.cfg.read_mode == "ranges":
                group["reads"].append(event)
            else:
                self.unexpected_reads_in_summary_mode += 1
        return []

    def _deadline(self, group: dict):
        close = group["close"]
        if close is not None:
            return close["known_at"] + timedelta(seconds=self.cfg.completion_grace_s)
        return group["last_known"] + timedelta(seconds=self.cfg.handle_timeout_s)

    def _complete(self, key: str, group: dict) -> dict | None:
        opened, closed, reads = group["open"], group["close"], group["reads"]
        if opened is None and not reads:
            return None
        first_read = (
            min(reads, key=lambda r: (r["effective_time"], r["event_id"])) if reads else None
        )
        first = opened or first_read
        if first is None:
            return None
        if self.cfg.read_mode == "ranges":
            byte_count = sum(r["bytes_read"] for r in reads)
            distinct = _distinct_bytes(reads)
            inconsistent = (
                closed is not None
                and closed["handle_bytes_read"] is not None
                and closed["handle_bytes_read"] != byte_count
            )
        else:
            byte_count = closed["handle_bytes_read"] if closed is not None else None
            distinct = None
            inconsistent = False
        file_size = first["file_size"]
        full_read = (
            None
            if byte_count is None
            else (distinct if distinct is not None else byte_count) >= 0.9 * file_size
        )
        return {
            "open_id": key,
            "volume_id": first["volume_id"],
            "file_id": first["file_id"],
            "dir_id": first["dir_id"],
            "session_id": first["session_id"],
            "cohort_id": first["cohort_id"],
            "client_id": first["client_id"],
            "app": first["app"],
            "open_time": first["effective_time"],
            "open_known_at": opened["known_at"]
            if opened is not None
            else min(r["known_at"] for r in reads),
            "close_time": closed["effective_time"] if closed is not None else None,
            "close_known_at": closed["known_at"] if closed is not None else None,
            "completed_known_at": self._deadline(group),
            "file_size": file_size,
            "bytes_read": byte_count,
            "distinct_bytes_read": distinct,
            "read_event_count": len(reads),
            "full_read": full_read,
            "consumed": None if byte_count is None else byte_count > 0,
            "open_missing": opened is None,
            "close_missing": closed is None,
            "bytes_inconsistent": inconsistent,
        }

    def advance(self, now) -> list[dict]:
        result = []
        for key, group in sorted(self.groups.items()):
            if self._deadline(group) <= now:
                row = self._complete(key, group)
                if row is not None:
                    result.append({"kind": "OpenCompleted", "row": row})
                    self.completed.add(key)
                self.groups.pop(key)
        return result

    def flush(self) -> list[dict]:
        result = []
        for key, group in sorted(self.groups.items()):
            row = self._complete(key, group)
            if row is not None:
                result.append({"kind": "OpenCompleted", "row": row})
                self.completed.add(key)
        self.groups.clear()
        return result
