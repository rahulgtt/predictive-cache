"""UTC clocks and bounded-memory metadata replay sources."""

from __future__ import annotations

import json
import sqlite3
import tempfile
import time
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Event, Thread
from typing import Protocol

import pyarrow.parquet as pq

from predictive_cache.datagen.io import canonical_json
from predictive_cache.prep.clean.io import sha256

from .ingest import instant


class EventSource(Protocol):
    def start(self, sink: Callable[[list[dict]], None]) -> None: ...
    def stop(self) -> None: ...


class Clock(Protocol):
    def now(self) -> datetime: ...


class RealClock:
    def now(self) -> datetime:
        return datetime.now(UTC)


class ReplayClock:
    def __init__(self, start: datetime, speed: float = 60):
        if not 0 <= speed < float("inf"):
            raise ValueError("replay speed must be finite and nonnegative")
        self.speed, self._start, self._anchor = speed, instant(start), time.monotonic()

    def now(self) -> datetime:
        return self._start + timedelta(seconds=(time.monotonic() - self._anchor) * self.speed)

    def advance(self, now: datetime) -> None:
        now = instant(now)
        if now < self._start:
            raise ValueError("replay clock cannot move backwards")
        self._start, self._anchor = now, time.monotonic()


def _rows(path: Path, batch_size: int, max_event_bytes: int):
    # The existing dataset wire format and canonical serializer are reused;
    # read_table/read_rows load entire files, so replay uses Arrow's batch API.
    if path.suffix == ".parquet":
        for batch in pq.ParquetFile(path).iter_batches(batch_size=batch_size):
            yield from batch.to_pylist()
    elif path.suffix == ".jsonl":
        with path.open(encoding="utf-8") as stream:
            while line := stream.readline(max_event_bytes + 1):
                if len(line.encode("utf-8")) > max_event_bytes:
                    raise ValueError("replay event exceeds size limit")
                if line.strip():
                    yield json.loads(line)
    else:
        raise ValueError("unsupported replay event format")


class ReplaySource:
    """Replay globally sorted events without retaining the dataset in RAM.

    A temporary SQLite index orders even unsorted partitions. Source files are
    manifest/hash checked and only events/ entries are opened, never truth/ or
    modeled file paths. SQLite's page cap bounds temporary disk use. stop()
    closes/removes the spool, so construct a new source to replay again.
    """

    def __init__(
        self,
        dataset: Path,
        clock: ReplayClock | None = None,
        *,
        batch_size: int = 1000,
        warmup_s: float = 0,
        speed: float = 60,
        max_spool_bytes: int = 4 * 1024**3,
        max_event_bytes: int = 65_536,
    ):
        if batch_size < 1 or max_spool_bytes < 4096 or warmup_s < 0 or max_event_bytes < 1:
            raise ValueError("invalid replay bounds")
        self.dataset = Path(dataset).resolve()
        manifest = json.loads((self.dataset / "manifest.json").read_text())
        if manifest.get("event_schema_version") != 1:
            raise ValueError("unsupported replay event schema")
        self.period_start = instant(manifest["period"]["start"])
        self.period_end = instant(manifest["period"]["end"])
        self.clock = clock or ReplayClock(self.period_start + timedelta(seconds=warmup_s), speed)
        self.batch_size = batch_size
        self._temporary = tempfile.TemporaryDirectory(prefix="pcache-replay-")
        self._database = sqlite3.connect(
            str(Path(self._temporary.name) / "events.sqlite"), check_same_thread=False
        )
        self._database.execute("PRAGMA journal_mode=OFF")
        self._database.execute("PRAGMA temp_store=FILE")
        self._database.execute("PRAGMA cache_size=-2048")
        self._database.execute(f"PRAGMA max_page_count={max_spool_bytes // 4096}")
        self._database.execute(
            "CREATE TABLE events (known TEXT, event_id TEXT, path TEXT, row_idx INTEGER, body TEXT)"
        )
        self._database.execute("CREATE INDEX ordered ON events(known,event_id,path,row_idx)")
        self._stop, self._thread = Event(), None
        self.error = None
        self._closed = False
        try:
            for entry in sorted(manifest["files"], key=lambda item: item["path"]):
                relative = Path(entry["path"])
                if not relative.parts or relative.parts[0] != "events":
                    continue
                artifact = (self.dataset / relative).resolve()
                if relative.is_absolute() or not artifact.is_relative_to(self.dataset):
                    raise ValueError("replay artifact escapes dataset")
                if not artifact.is_file() or sha256(artifact) != entry["sha256"]:
                    raise ValueError("missing or corrupted replay artifact")
                for index, row in enumerate(_rows(artifact, batch_size, max_event_bytes)):
                    if not isinstance(row, dict):
                        raise ValueError("replay event must be an object")
                    known = instant(row.get("ingest_time"))
                    event_id = row.get("event_id")
                    if not isinstance(event_id, str):
                        raise ValueError("replay event must have event_id")
                    encoded = canonical_json(row)
                    if len(encoded.encode("utf-8")) > max_event_bytes:
                        raise ValueError("replay event exceeds size limit")
                    self._database.execute(
                        "INSERT INTO events VALUES (?,?,?,?,?)",
                        (
                            known.isoformat(timespec="microseconds"),
                            event_id,
                            relative.as_posix(),
                            index,
                            encoded,
                        ),
                    )
            self._database.commit()
            self._cursor = self._database.execute(
                "SELECT known,body FROM events ORDER BY known,event_id,path,row_idx"
            )
            self._next = self._cursor.fetchone()
        except BaseException:
            self.stop()
            raise

    @property
    def completed(self) -> bool:
        return self._next is None if not self._closed else True

    @property
    def next_ingest_time(self) -> datetime | None:
        return instant(self._next[0]) if not self._closed and self._next else None

    def emit_ready(self, sink: Callable[[list[dict]], None]) -> int:
        if self._closed:
            raise RuntimeError("replay source is closed")
        count, batch = 0, []
        now = self.clock.now()
        while self._next and instant(self._next[0]) <= now and not self._stop.is_set():
            batch.append(json.loads(self._next[1]))
            self._next = self._cursor.fetchone()
            if len(batch) == self.batch_size:
                sink(batch)
                count += len(batch)
                batch = []
        if batch:
            sink(batch)
            count += len(batch)
        return count

    def start(self, sink: Callable[[list[dict]], None]) -> None:
        if self._closed or self._thread is not None:
            raise RuntimeError("replay source already started or closed")

        def run():
            try:
                while not self._stop.is_set() and not self.completed:
                    self.emit_ready(sink)
                    self._stop.wait(0.01)
            except Exception as exc:
                self.error = exc
                self._stop.set()

        self._thread = Thread(target=run, name="pcache-replay", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=3)
            if self._thread.is_alive():
                raise RuntimeError("replay sink did not stop")
        if not self._closed:
            self._database.close()
            self._temporary.cleanup()
            self._closed = True
