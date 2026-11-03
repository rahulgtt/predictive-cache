# Manual NFSv4.1 range reader

**Implementation in progress; not yet ready for a live pilot.** The CLI
supervisor is implemented, while the NFS adapter is being implemented and
reviewed. The commands and adapter behavior below describe the intended pilot,
not a validated live read.

The experimental reader is independent of the Controller and any model. A
completed NFS read would not prove that data became or remained resident in
CUFS, or that foreground performance improved. Do not use it in production or
against a shared cluster without explicit owner approval.

## Implementation model

Think of the reader as a **supervised, speculative client**. The parent
process validates a single request and controls its lifetime; a separate
child is responsible for NFS protocol operations. Neither process participates
in the foreground read path, but their traffic can still compete with it.

| Layer | Responsibility | Boundary |
| --- | --- | --- |
| CLI ([`cli.py`](../src/predictive_cache/warmer/cli.py)) | Parse and validate the explicit target, byte range, budget, identity and deadline before starting a worker. | Does not discover clusters, resolve stable file identity, or verify operator authorization. |
| Supervisor ([`execution.py`](../src/predictive_cache/warmer/execution.py)) | Start one child, validate its events, log attempted COMPOUND tags, track acknowledged byte counts and stop the child on timeout or interrupt. | Controls this process only; it cannot cancel an RPC already executing on the server. |
| NFS adapter | Establish an NFSv4.1 session and use `pynfs` to issue the read-only request. | **In progress:** protocol behavior and interoperability still need verification. |
| Chimera and CUFS | Execute the server-side NFS operation and its backing filesystem read. | Existing components; this project has not implemented the tag-to-READ/CUFS bridge. |

The intended path is:

```text
operator -> validate target/range/budget -> start child
         -> NFSv4.1 session -> read-only OPEN -> bounded READs -> CLOSE
         -> task result
```

In parallel, before each attempted COMPOUND, the child sends its tag to the
supervisor. The supervisor validates and emits the event, then acknowledges
it to the child; only then may the child continue. This ordering avoids
intentionally submitting a request before its tag has been handed to the
output sink. A log record is still an **attempt**, not proof of network
delivery, server execution, or durable log storage.

### Request validation and limits

The CLI validates the request as a whole before starting the child. In
addition to the limits below, the path must be valid UTF-8, no longer than
4096 encoded bytes, with components no longer than 255 encoded bytes. The
supervisor receives the requested range only after checking it against the
operator-supplied budget; it does not reserve a shared cluster-wide budget.

| Control | Current implementation |
| --- | --- |
| Per invocation | One file range, at most 16 MiB requested; `--max-bytes` must cover the range and cannot exceed that cap. |
| Offset | Unsigned 64-bit offset; the exclusive end of the range cannot exceed 2^64. |
| Concurrency | One child and at most one READ COMPOUND awaiting its response. |
| Chunk | At most 4096 bytes; the supervisor checks that successive requests cover the requested range in order. |
| Attempts | At most 4,160 COMPOUND events per invocation, including setup and cleanup; tags must be unique within the invocation. |
| Deadline | A monotonic deadline of 0.1–300 seconds covers the worker lifetime, subject to scheduling and output-sink responsiveness. |

These are per-process limits, not a rate limiter or a cluster-wide I/O
policy. The server may perform additional backend work for a read, and a
completed read may alter access-time metadata.

### Tags, progress and terminal results

The supervisor accepts a 32-character lowercase hexadecimal tag and a
bounded phase name for each attempted COMPOUND. For a READ, it also validates
the intended offset and count against the original range and previously
acknowledged progress. It rejects duplicate tags, out-of-order ranges, extra
bytes and an attempted continuation after a short read. The worker must report
the count returned by each completed READ; the supervisor accumulates these
counts rather than assuming that a requested number of bytes was returned.

For example, an 8192-byte range beginning at offset 0 is intended to produce
two READ attempts, each with a distinct tag and a count of 4096. If the first
response contains only 1024 bytes, the task stops as partial rather than
claiming 8192 bytes or retrying the remainder. If the process times out
before a response is recorded, the result reports only the bytes already
acknowledged to the supervisor and marks that count as a lower bound.

The adapter must verify that each response echoes its request tag; this is
an interoperability check, not an origin-authentication mechanism. Random
tags contain no path, credential, file handle or data. The proposed
Chimera-side bridge and its loss, replay and privacy semantics remain
unimplemented.

### Failure and process lifetime

The parent uses a separate spawned process because the candidate `pynfs`
client does not expose a reliable per-call connect deadline or cancellation
through its ordinary COMPOUND API. The supervisor checks the deadline while
waiting for worker messages. On timeout, interrupt or an invalid worker
message it terminates the child, then escalates to a kill if necessary.
If the worker cannot be stopped, the result reports that condition rather
than treating it as success.

This boundary prevents the parent from waiting indefinitely on ordinary
client blocking, but it does not undo an in-flight request. It also depends
on the operating system scheduling the supervisor and on a responsive output
sink. A server-side operation may finish after the client exits; there is no
claim of exactly-once execution. Dependency exception messages are not
passed through because they can contain sensitive inputs.

## Prerequisites

- Python 3.12, `uv`, Git, and an authorized Linux environment with network
  access to the target NFSv4.1 server. No kernel mount is used.
- An explicitly approved numeric server IP, port, NFSv4 pseudo-filesystem
  file path, byte range, and AUTH_SYS UID/GID. Confirm that the server permits
  that identity and read before running. AUTH_SYS identity is client-asserted,
  not authenticated origin. Kerberos/GSS is not supported by this pilot.
- The optional `warmer` dependency is the pinned GPL-2.0-only `pynfs`
  protocol test library, installed from Git. Review its licensing obligations
  before redistribution. It is not a hardened production NFS client. The
  same dependency is included in the default development group so the
  repository's normal test selection can exercise the adapter.

From the repository root, install the locked optional dependency:

```bash
uv sync --locked --extra warmer
uv run --locked --extra warmer pcache-warmer --help
```

## Run one authorized range

The following values are **illustrative placeholders**, not an existing
endpoint or permission to access one. Replace every target and identity
with values approved by the cluster owner. The path is relative to the
server's NFSv4 pseudo-root, not necessarily its local filesystem root.

```bash
umask 077
uv run --locked --extra warmer pcache-warmer \
  --host 192.0.2.10 --port 2049 --path /approved-export/example-file \
  --offset 0 --length 8192 --max-bytes 8192 --timeout-seconds 30 \
  --uid 1000 --gid 1000 > warmer-results.jsonl
```

`--host` accepts a numeric IPv4 or IPv6 address without a scope ID; DNS
names are not accepted. The path must be absolute, with nonempty UTF-8
components and no `.` or `..` component. `--port` defaults to 2049.
`--uid` and `--gid` are unsigned 32-bit AUTH_SYS values. Avoid placing
real paths in shared shell history or exposing command lines to other users;
the path is still visible to the operating system and NFS server.

`--offset` is an unsigned 64-bit byte offset. `--length` and `--max-bytes`
are required positive byte counts, each no greater than **16 MiB**, with
length no larger than the budget and the end of the range no greater than
2^64. The timeout must be between 0.1 and 300 seconds. A single invocation
uses at most one in-flight COMPOUND, with one READ of at most 4096 bytes
per COMPOUND. A short read ends the task as partial; the reader does not
retry it to fill the range. Multiple invocations do not share a budget or
concurrency limit.

## Output and stopping

Standard output is JSON Lines. The following **illustrative excerpts** show
the shape, not an observed successful run; setup and cleanup records are
omitted:

```json
{"phase":"read","tag":"0123456789abcdef0123456789abcdef","type":"compound","offset":0,"count":4096}
{"phase":"read","tag":"fedcba9876543210fedcba9876543210","type":"compound","offset":4096,"count":4096}
{"bytes_read":8192,"bytes_read_is_lower_bound":false,"bytes_requested":8192,"reason":"ok","status":"completed","type":"result"}
```

Each `compound` record is emitted **before an attempt** and contains a
fresh random tag; it is not proof the request reached the server. `read`
records also carry the intended offset and count. Successful responses
must echo the tag. Setup and cleanup COMPOUNDs are tagged too. Logs contain
no file contents, credentials, raw file handles or paths, but tags and
ranges are still sensitive operational metadata; restrict their access and
retention.

The terminal result reports only bytes confirmed by responses. `completed`
means the entire requested range was read, not that it remains cached.
`partial` means the range was not fully read, for example at EOF.
`error`, `timeout`, and `cancelled` are not success. When
`bytes_read_is_lower_bound` is true, an already-submitted operation may have
completed without its response being recorded; do not infer exact server
bytes from the reported count. Exit status is 0 for `completed`, 130 for
an interrupt, and 1 for partial, timeout, or error; invalid arguments exit 2.

Press Ctrl+C to cancel. A separate worker process bounds the time spent
waiting for the NFS client, including connection setup. Stopping the process
cannot recall an RPC already received by the server, so server I/O can
finish after timeout or cancellation. The supervisor must be scheduled and
its output sink must remain responsive for timely reporting and stopping.

## What the tests can establish

The supervisor tests in
[`tests/warmer/test_cli.py`](../tests/warmer/test_cli.py) use substitute
workers rather than an NFS server. They exercise argument rejection,
tag-admission ordering, duplicate or invalid events, byte accounting, short
reads, timeout and interrupt handling. These checks show how the parent
reacts to controlled messages; they do **not** show that the NFS adapter sends
the intended wire operations or that Chimera accepts them.

Adapter-level encoding and response tests, a live authorized NFSv4.1
interoperability check, the Chimera/CUFS tag bridge and a controlled cache
experiment are distinct remaining evidence. A successful mock or completed
read cannot replace any of those checks.

## Boundaries and missing evidence

- The operator supplies the current target; stable file identity, a
  path-to-identity resolver, rename protection, allowlists, and a Controller
  contract do not exist. NFS authorization and the operator's approval
  remain necessary. Reads can affect access-time metadata.
- A future Chimera bridge from COMPOUND tag to server READ operation ID and
  CUFS is **not implemented or verified**. Tags are client-controlled,
  copyable, and not authentication. Unmatched events remain `unknown`, not
  foreground demand. No model or evaluation should treat this log alone as
  trustworthy origin.
- This pilot has no bandwidth/rate limit, pressure pause, cache-headroom
  control, global budget, deployment integration, or foreground isolation.
  Even a read-only request can contend with foreground I/O or evict useful
  data. The 16 MiB per-run cap is not a safety guarantee.
- No live CUFS-backed NFS execution or cache-residency/performance claim is
  established by the offline tests. A pilot needs separate approval and
  controlled evidence before any efficacy or safety claim.
