"""Bounded, conservative byte-overlap accounting for one experiment.

No target handles, paths, content or source exception text are retained.
"""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from math import isfinite

from predictive_cache.dispatcher.schema import ResultRecord, WarmerTask, warmer_ref
from predictive_cache.semantics.read_demand import ReadDemandEvent


def _utc(value: datetime) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError("feedback time must be UTC")
    return value.astimezone(UTC)


def _union(intervals):
    result = []
    for start, end in sorted(intervals):
        if end <= start:
            continue
        if result and start <= result[-1][1]:
            result[-1] = result[-1][0], max(end, result[-1][1])
        else:
            result.append((start, end))
    return result


def _size(intervals):
    return sum(end - start for start, end in _union(intervals))


def _subtract(left, right):
    pieces = _union(left)
    for start, end in _union(right):
        output = []
        for lo, hi in pieces:
            if hi <= start or lo >= end:
                output.append((lo, hi))
            else:
                if lo < start:
                    output.append((lo, start))
                if hi > end:
                    output.append((end, hi))
        pieces = output
    return pieces


@dataclass(frozen=True)
class _Warm:
    run: str
    volume: str
    file: str
    offset: int
    length: int
    started: datetime
    finished: datetime
    end: datetime
    known: datetime
    status: str
    requested: int
    completed: bool


class UsefulByteFeedback:
    """Use only trusted caller-joined task/results and independently observed reads.

    ``coverage`` and ``invalidate`` consume trusted evidence, not client claims.
    The caller must keep this ledger isolated per declared experiment/version.
    """

    def __init__(
        self,
        *,
        usefulness_s: float = 300,
        max_tasks: int = 10000,
        max_reads: int = 100000,
        max_evidence: int = 10000,
        max_duration_s: float = 86400,
        experiment_id: str,
    ):
        if not isinstance(experiment_id, str) or not experiment_id:
            raise ValueError("experiment identity required")
        if any(type(n) is not int or n < 1 for n in (max_tasks, max_reads, max_evidence)):
            raise ValueError("invalid feedback capacity")
        if any(
            isinstance(n, bool) or not isinstance(n, (int, float)) or not isfinite(n) or n <= 0
            for n in (usefulness_s, max_duration_s)
        ):
            raise ValueError("invalid feedback duration")
        self.experiment_id = experiment_id
        self.usefulness_s = usefulness_s
        self.max_tasks, self.max_reads, self.max_evidence = max_tasks, max_reads, max_evidence
        self.max_duration_s = max_duration_s
        self._tasks: dict[tuple[str, str, str], _Warm] = {}
        self._reads: dict[str, ReadDemandEvent] = {}
        self._coverage = []
        self._invalidations = []
        self._counts = Counter()
        self._loss_since = None
        self._first_known = None
        self._reported_through = None

    def _mark_loss(self, known):
        self._loss_since = known if self._loss_since is None else min(self._loss_since, known)

    def _admit(self, known, size, limit):
        known = _utc(known)
        if self._first_known is None:
            self._first_known = known
        if (
            known < self._first_known
            or (known - self._first_known).total_seconds() > self.max_duration_s
        ):
            self._counts["duration_rejected"] += 1
            self._mark_loss(known)
            return False
        if size >= limit:
            self._counts["capacity_rejected"] += 1
            self._mark_loss(known)
            return False
        return True

    def observe_result(
        self,
        task: WarmerTask,
        result: ResultRecord,
        *,
        known_at: datetime,
        use_until: datetime | None = None,
    ) -> bool:
        """Join one actual dispatcher result to its trusted original task.

        Only exact, terminal-backed completed reads contribute known intervals.
        Other outcomes retain requested-byte accounting, never assumed intervals.
        """
        if (
            not isinstance(task, WarmerTask)
            or type(task.schema_version) is not int
            or task.schema_version != 2
        ):
            raise ValueError("feedback requires a validated task")
        task = WarmerTask(**task.__dict__)
        for value in (task.created_at, task.deadline, task.target_valid_until):
            _utc(value)
        result = ResultRecord.model_validate(result.model_dump())
        known = _utc(known_at)
        finish = _utc(result.finished_at)
        started = _utc(result.started_at)
        if (task.run_id, task.plan_id, task.task_id) != (
            result.run_id,
            result.plan_id,
            result.task_id,
        ):
            raise ValueError("feedback result identity mismatch")
        if result.bytes_requested != task.length or known < finish or started < task.created_at:
            raise ValueError("feedback result range or availability mismatch")
        end = finish + timedelta(seconds=self.usefulness_s)
        if use_until is not None:
            end = min(end, _utc(use_until))
        if end <= finish:
            raise ValueError("feedback window must follow completion")
        terminal = result.warmer_terminal
        completed = (
            result.status == "completed"
            and not result.bytes_read_is_lower_bound
            and result.bytes_read == task.length
            and terminal is not None
            and terminal.execution_started
            and terminal.status == "completed"
            and not terminal.bytes_read_is_lower_bound
            and terminal.bytes_requested == terminal.bytes_read == task.length
            and terminal.plan_ref == warmer_ref(task.plan_id)
            and terminal.task_ref == warmer_ref(task.task_id)
        )
        if result.status == "completed" and not completed:
            raise ValueError("completed feedback requires matching exact execution evidence")
        warm = _Warm(
            task.run_id,
            task.volume_id,
            task.file_id,
            task.offset,
            task.length,
            started,
            finish,
            end,
            known,
            result.status,
            result.bytes_requested,
            completed,
        )
        key = task.run_id, task.plan_id, task.task_id
        existing = self._tasks.get(key)
        if existing is not None:
            # Delivery receipt is not part of immutable execution identity.
            if existing != _Warm(**{**warm.__dict__, "known": existing.known}):
                self._mark_loss(known)
                self._counts["result_conflicts"] += 1
                raise ValueError("conflicting feedback result")
            self._counts["duplicate_results"] += 1
            return False
        if not self._admit(known, len(self._tasks), self.max_tasks):
            return False
        self._tasks[key] = warm
        return True

    def observe_read(self, event: ReadDemandEvent) -> bool:
        event = ReadDemandEvent.model_validate(event.model_dump())
        if not event.eligible_real_demand:
            self._counts["excluded_reads"] += 1
            return False
        if event.event_time > event.known_at:
            raise ValueError("feedback read precedes availability")
        previous = self._reads.get(event.event_id)
        if previous is not None:
            if previous != event:
                self._mark_loss(event.known_at)
                self._counts["read_conflicts"] += 1
                raise ValueError("conflicting feedback read")
            self._counts["duplicate_reads"] += 1
            return False
        if not self._admit(event.known_at, len(self._reads), self.max_reads):
            return False
        if self._reported_through is not None and event.event_time <= self._reported_through:
            self._mark_loss(event.known_at)
            self._counts["late_after_report"] += 1
        self._reads[event.event_id] = event
        return True

    def coverage(
        self, volume_id: str, start: datetime, end: datetime, *, known_at: datetime, complete: bool
    ) -> bool:
        """Record authoritative coverage over [start,end], after that interval ends."""
        start, end, known = _utc(start), _utc(end), _utc(known_at)
        if not start < end <= known or type(complete) is not bool:
            raise ValueError("invalid feedback coverage")
        if not self._admit(
            known, len(self._coverage) + len(self._invalidations), self.max_evidence
        ):
            return False
        self._coverage.append((volume_id, start, end, known, complete))
        return True

    def invalidate(self, volume_id: str, file_id: str, at: datetime, *, known_at: datetime) -> bool:
        """A verified write/replacement makes affected warm windows unobservable."""
        at, known = _utc(at), _utc(known_at)
        if at > known:
            raise ValueError("invalid feedback invalidation time")
        if not self._admit(
            known, len(self._coverage) + len(self._invalidations), self.max_evidence
        ):
            return False
        self._invalidations.append((volume_id, file_id, at, known))
        return True

    def _complete(self, warm, now):
        if (self._loss_since is not None and self._loss_since <= now) or now < warm.end:
            return False
        rows = [row for row in self._coverage if row[0] == warm.volume and row[3] <= now]
        if any(
            not flag and start < warm.end and end > warm.finished for _, start, end, _, flag in rows
        ):
            return False
        cursor = warm.finished
        for _, start, end, _, flag in sorted(rows, key=lambda row: row[1]):
            if flag and start <= cursor:
                cursor = max(cursor, end)
        return cursor >= warm.end

    def report(self, as_of: datetime) -> dict:
        """Recompute a revision as of known evidence; late evidence cannot hide loss."""
        now = _utc(as_of)
        if self._reported_through is not None and now < self._reported_through:
            raise ValueError("feedback reports cannot move backwards")
        previous_report = self._reported_through
        if previous_report is not None:
            for read in self._reads.values():
                if read.event_time <= previous_report < read.known_at <= now:
                    self._mark_loss(read.known_at)
        self._reported_through = now
        groups = {}
        for warm in self._tasks.values():
            if warm.known > now:
                continue
            group = groups.setdefault(
                (warm.run, warm.volume),
                {
                    "requested_bytes": 0,
                    "completed_bytes_sum": 0,
                    "uncertain_requested_bytes": 0,
                    "statuses": Counter(),
                    "files": {},
                },
            )
            group["requested_bytes"] += warm.requested
            group["statuses"][warm.status] += 1
            if not warm.completed:
                if warm.status in {"partial", "failed", "cancelled"}:
                    group["uncertain_requested_bytes"] += warm.requested
                continue
            group["completed_bytes_sum"] += warm.length
            data = group["files"].setdefault(
                warm.file, {"eligible": [], "useful": [], "unknown": []}
            )
            interval = warm.offset, warm.offset + warm.length
            data["eligible"].append(interval)
            invalid = any(
                volume == warm.volume
                and file == warm.file
                and known <= now
                and warm.started <= at <= warm.end
                for volume, file, at, known in self._invalidations
            )
            if invalid:
                data["unknown"].append(interval)
                continue
            intersections = []
            for read in self._reads.values():
                if (
                    read.known_at <= now
                    and read.volume_id == warm.volume
                    and read.file_id == warm.file
                    and warm.finished < read.event_time <= warm.end
                ):
                    lo, hi = (
                        max(interval[0], read.offset),
                        min(interval[1], read.offset + read.bytes_read),
                    )
                    if lo < hi:
                        intersections.append((lo, hi))
            # Compact every warm interval, not after accumulating the task/read
            # Cartesian product. At most evidence-derived disjoint pieces survive.
            data["useful"] = _union(data["useful"] + _union(intersections))
            if not self._complete(warm, now):
                data["unknown"].append(interval)
        rows = []
        for (run, volume), group in sorted(groups.items()):
            eligible = useful = unknown = 0
            for data in group.pop("files").values():
                eligible += _size(data["eligible"])
                useful += _size(data["useful"])
                unknown += _size(_subtract(data["unknown"], data["useful"]))
            rows.append(
                {
                    "run_id": run,
                    "volume_id": volume,
                    **group,
                    "statuses": dict(sorted(group["statuses"].items())),
                    "eligible_completed_unique_bytes": eligible,
                    "useful_bytes": useful,
                    "unknown_bytes": unknown,
                    "unused_bytes": eligible - useful - unknown,
                }
            )
        return {
            "schema_version": 1,
            "experiment_id": self.experiment_id,
            "usefulness_s": self.usefulness_s,
            "as_of": now.isoformat(),
            "complete_accounting": self._loss_since is None or self._loss_since > now,
            "counters": dict(self._counts),
            "runs": rows,
            "evidence": "byte overlap only; no cache residency or latency claim",
        }
