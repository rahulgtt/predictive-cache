# Manual NFSv4.1 range reader

**Experimental implementation; handle-mode live interoperability is
unverified.** One separately authorized path-mode NFSv4.1 read returned
4096 bytes on 2026-11-03. It does not validate handle mode, server-side tag
correlation, cache residency or performance. Any further live pilot requires
separate owner approval.

The [datagen-to-Warmer MVP](experiments/datagen-to-warmer-mvp.md) stops at
Dispatcher `skipped/dry_run` results. Its separately listed one-READ gate is a
future approval checklist, not permission to execute handle-mode NFS I/O.

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
| CLI ([`cli.py`](../src/predictive_cache/warmer/cli.py)) | Validate a path request or a private handle task, its range, budget, identity and deadline before starting a worker. Private task-file I/O runs in a separate, supervised process. | Trusts the task's dispatcher for identity-to-handle binding; does not discover clusters or verify operator authorization. |
| Supervisor ([`execution.py`](../src/predictive_cache/warmer/execution.py)) | Start one child, validate its events, log attempted COMPOUND tags, track acknowledged byte counts and stop the child on timeout or interrupt. | Controls this process only; it cannot cancel an RPC already executing on the server. |
| NFS adapter ([`nfs41.py`](../src/predictive_cache/warmer/nfs41.py)) | Establish an NFSv4.1 session and use `pynfs` to issue the read-only request. | Real XDR requests and responses are exercised offline; handle-mode live interoperability is unverified. |
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
path mode, the path must be valid UTF-8, no longer than 4096 encoded bytes,
with components no longer than 255 encoded bytes. The
supervisor receives the requested range only after checking it against the
operator-supplied budget; it does not reserve a shared cluster-wide budget.

| Control | Current implementation |
| --- | --- |
| Per invocation | One file range, at most 16 MiB requested; `--max-bytes` must cover the range and cannot exceed that cap. |
| Offset | Maximum file offset is 2^64−2; the exclusive end cannot exceed the NFS maximum file length, 2^64−1. |
| Concurrency | One child and at most one READ COMPOUND awaiting its response. |
| Chunk | At most 4096 bytes; the supervisor checks that successive requests cover the requested range in order. |
| Attempts | At most 4,160 COMPOUND events per invocation, including setup and cleanup; tags must be unique within the invocation. |
| Worker memory | The Linux child process has a 256 MiB address-space soft limit, or a lower inherited limit. |
| Deadline | A suspend-aware Linux elapsed-time deadline of 0.1–300 seconds covers validation and worker lifetime. Handle tasks also retain their absolute UTC deadline. Enforcement depends on scheduling and output-sink responsiveness. |

These are per-process limits, not a rate limiter or a cluster-wide I/O
policy. The server may perform additional backend work for a read, and a
completed read may alter access-time metadata. The `pynfs` transport buffers
incoming RPC records without its own record-size bound. The child address-space
limit constrains that process, but does not bound kernel socket buffers,
server work, other invocations or total network traffic.
If the server negotiates a smaller read size, a large range may hit the
COMPOUND-attempt limit before it is fully read; that is an error, not
completion. The adapter reserves up to three attempts for `CLOSE` and
session/client teardown before admitting more work.

### Tags, progress and terminal results

For path mode, the supervisor accepts a 32-character lowercase hexadecimal
tag. For handle mode it accepts exactly
`pc1:w:<plan_ref32hex>:<task_ref32hex>:<nonce32hex>`: 104 ASCII bytes.
Each reference is the first 32 lowercase hex characters of SHA-256 over the
ASCII plan or task ID; the nonce is fresh for each COMPOUND. The `w` role is
a client assertion, not authenticated origin. The supervisor validates the
refs against the dispatched task and accepts a bounded phase name for each
attempted COMPOUND. For a READ, it also validates
the intended offset and count against the original range and previously
acknowledged progress. It rejects duplicate tags, out-of-order ranges, extra
bytes and an attempted continuation after a short read. The worker must report
the count returned by each completed READ; the supervisor accumulates these
counts rather than assuming that a requested number of bytes was returned.

For example, when the negotiated response size permits 4096-byte reads, an
8192-byte range beginning at offset 0 produces two READ attempts, each with a
distinct tag and a count of 4096. If the first response contains only 1024
bytes, the task stops as partial rather than claiming 8192 bytes or retrying
the remainder. If the process times out before a response is recorded, the
result reports only the bytes already acknowledged to the supervisor and
marks that count as a lower bound.

The adapter must verify that each response echoes its request tag; this is
an interoperability check, not an origin-authentication mechanism. Tags
contain no path, credential, file handle or data, although the hashed
identifiers and ranges are sensitive operational metadata. The proposed
Chimera-side bridge and its loss, replay and privacy semantics remain
unimplemented.

### NFSv4.1 operation sequence

The adapter uses the pinned `pynfs` library to encode NFS operations in XDR,
the protocol's binary wire format. A COMPOUND combines NFS operations in one
RPC (remote procedure call); the library adds the session `SEQUENCE`
operation when the call belongs to a session.

1. Send `EXCHANGE_ID` and `CREATE_SESSION`, requesting one fore-channel
   slot, then `RECLAIM_COMPLETE`. The session limits are checked before
   reading.
2. In path mode, resolve the supplied path from `PUTROOTFH`, using `LOOKUP`
   for every component. In handle mode, use the dispatcher's supplied
   opaque wire handle directly, without path lookup or fallback, then use
   `GETATTR` to require a regular file and a size covering the requested
   range. A later concurrent truncation can still cause a short read. Open the
   selected handle with NFSv4.1 `CLAIM_FH`,
   `OPEN4_NOCREATE`, read access, no deny mode and a request for no
   delegation. Temporary file handles and the open state ID stay in memory
   and are not intended for logs.
3. Send `PUTFH` and one `READ` in each COMPOUND. Use the smaller of 4096
   bytes, the remaining range and the negotiated response capacity. Returned
   data is transiently held in process memory to count it; it is not written
   to an output file or securely erased from memory.
4. On the safe cleanup path, send `CLOSE`, `DESTROY_SESSION` and
   `DESTROY_CLIENTID`. A cleanup error is an error result, even if all
   requested bytes were returned. If transport or protocol state is uncertain,
   the adapter may stop without issuing further cleanup COMPOUNDs.

Session establishment and teardown create temporary server-side NFS protocol
state. “Read-only” means the client does not request file creation, file data
writes or file-content modification; it does not mean zero server state or
zero metadata effects. An unanswered OPEN can leave server state that this
client cannot safely close without the response's state ID. A path-mode
`CLAIM_FH` open was exercised in the single live read; direct supplied
handles and their lifetime or behavior across failover remain unverified.
Neither mode verifies that the handle is the operator's intended stable file
identity.

### Failure and process lifetime

The parent uses a separate spawned process because the candidate `pynfs`
client does not expose a reliable per-call connect deadline or cancellation
through its ordinary COMPOUND API. The supervisor checks the deadline while
waiting for worker messages. On timeout, interrupt or an invalid worker
message it terminates the child, then escalates to a kill if necessary.
If the worker cannot be stopped, the result reports that condition rather
than treating it as success. The deadline is checked again after emitting a
tag and before admitting the COMPOUND, and the child checks again after
admission. The UTC task deadline is also checked after a forward wall-clock
step; its remaining lifetime is anchored to the elapsed clock so a backward
step cannot extend it. Linux elapsed time includes system suspend. Scheduling
can still delay the process between its final check and the network call, and
an in-flight RPC cannot be recalled.

On Linux, the child also requests a parent-death signal before it starts
client work. If the supervisor dies, the kernel kills the child; if that
protection cannot be established, the child fails without starting a read.
SIGTERM requests normal cancellation and cleanup, as does Ctrl+C. A forced
supervisor death cannot produce a terminal result or perform NFS cleanup.
The same parent-death and termination controls apply to the separate
task-file validation process, which performs no NFS client operations.
The dispatcher must reset inherited SIGINT and SIGTERM dispositions before
executing the CLI; otherwise a signal sent before the CLI installs its
handlers could be ignored. If cancellation occurs after the supervisor
handoff begins and no terminal result is available, treat the task as
ambiguous rather than claiming that no NFS operation started.

This boundary prevents the parent from waiting indefinitely on ordinary
client blocking, but it does not undo an in-flight request. It also depends
on the operating system scheduling the supervisor and on a responsive output
sink. A server-side operation may finish after the client exits; there is no
claim of exactly-once execution. Dependency exception messages are not
passed through because they can contain sensitive inputs.

## Prerequisites

- Python 3.12, `uv`, Git, and an authorized Linux environment with network
  access to the target NFSv4.1 server. No kernel mount is used.
- An explicitly approved numeric server IP, port, target and byte range,
  and AUTH_SYS UID/GID. Confirm that the server permits
  that identity and read before running. AUTH_SYS identity is client-asserted,
  not authenticated origin. Kerberos/GSS is not supported by this pilot;
  AUTH_SYS does not provide RPC integrity or confidentiality. Use only an
  approved, isolated network and an explicitly authorized server.
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

`uv` creates the Python environment and installs the versions recorded in
`uv.lock`; `uv run` executes the command in that environment. See the
[uv installation guide](https://docs.astral.sh/uv/getting-started/installation/)
if `uv` is not already available.

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

`--host` accepts a specified numeric IPv4 or IPv6 address without a scope ID;
DNS names and unspecified/wildcard addresses are not accepted. The path
must be absolute, with nonempty UTF-8
components and no `.` or `..` component. `--port` defaults to 2049.
`--uid` and `--gid` are unsigned 32-bit AUTH_SYS values. Avoid placing
real paths in shared shell history or exposing command lines to other users;
the path is still visible to the operating system and NFS server.

`--offset` is an unsigned 64-bit byte offset. `--length` and `--max-bytes`
are required positive byte counts, each no greater than **16 MiB**, with
length no larger than the budget and the exclusive end of the range no greater
than 2^64−1. The timeout must be between 0.1 and 300 seconds. A single invocation
uses at most one in-flight COMPOUND, with one READ of at most 4096 bytes
per COMPOUND. A short read ends the task as partial; the reader does not
retry it to fill the range. Multiple invocations do not share a budget or
concurrency limit.

## Run a private handle task

This mode is experimental and has **not** been exercised against a live
server. A trusted dispatcher must obtain the actual NFSv4 **wire file
handle**, bind it to the correct volume, export and file identity, and
provide an owner-only task file. An internal CUFS ID is not a substitute for
a wire handle. The reader cannot check that binding from opaque bytes; file
permissions do not authenticate the dispatcher. Do not use a task from an
untrusted writer or treat these identifiers as server-verified. No handle
resolver, automated dispatcher, replay protection, or deduplication exists.

The private JSON document has exactly the following fields; replace the
placeholder with a real, authorized handle and set a future deadline before
using it:

```json
{
  "schema_version": 2,
  "run_id": "run-example",
  "plan_id": "plan-example",
  "task_id": "task-example",
  "created_at": "2035-01-01T00:00:00Z",
  "volume_id": "volume-example",
  "file_id": "file-example",
  "export_name": "export-example",
  "export_id": "export-id-example",
  "target_fh_b64": "<base64url-wire-handle>",
  "offset": 0,
  "length": 4096,
  "max_bytes": 4096,
  "deadline": "2035-01-01T00:03:00Z",
  "target_valid_until": "2035-01-01T00:05:00Z"
}
```

All fields are required. IDs are
1–128 ASCII characters from letters, digits, `.`, `_`, `:`, and `-`.
The handle is canonical padded or unpadded base64url, decoding to 1–128
bytes. Times use UTC RFC 3339 `Z` form, with at most six fractional digits.
Require `created_at <= now < deadline <= target_valid_until`, and at most
300 seconds from creation to handle expiry. The reader limits execution by
the earlier of the invocation's elapsed-time timeout and the absolute UTC
deadline; it does not restart either clock after loading the task. A trusted
dispatcher must atomically publish committed
tasks into its directory, with non-hidden `.json` names that do not end in
`.tmp.json`; it must not pass a temporary file as a committed task. The
reader validates the filename but cannot prove a past atomic rename occurred.
The reader requires an effective-user-owned
directory with mode 0700, and a regular effective-user-owned task file with
mode 0600, one link, and at most 8192 bytes. It rejects symlink path
components and detects a file changed while being read. File ownership and
permissions do not protect against a malicious process with the same UID;
same-UID forgery is outside this disposable MVP's threat model.
Unknown or duplicate fields, malformed values, expired work, and ranges
outside the budget are rejected before contacting the server.

The CLI executes one explicit task; it does not watch a directory, claim,
move, unlink, retry or deduplicate it. It leaves the input in place and writes
the result to standard output. The trusted dispatcher owns atomic claiming,
deduplication, retries, recovery, global budgets and retention; replaying the
same task manually can issue another read.

For an approved target and a securely created task file:

```bash
umask 077
uv run --locked --extra warmer pcache-warmer \
  --host 192.0.2.10 --port 2049 --task-file /private/authorized-task.json \
  --timeout-seconds 30 --uid 1002 --gid 1002 > warmer-handle-results.jsonl
```

The endpoint and AUTH_SYS identity are trusted dispatcher/operator inputs,
not fields in the task. The values above are illustrative; configure the
authorized service UID/GID, not the originating client's identity. Do not
combine `--task-file` with `--path`, `--offset`,
`--length`, or `--max-bytes`; the task carries the range and per-run budget.
`GETATTR` must report a regular file whose size covers the full range.
`BADHANDLE`, `STALE`, `FHEXPIRED`, and `WRONGSEC` are errors, never triggers
for path lookup or an automatic retry. Endpoint, service identity and
read-only export policy must be configured and enforced by trusted operators
and the server; the reader cannot prove an opaque handle belongs to that
export. Protect the file and command-line
location; the task contains the handle and identifying metadata. A valid
handle can become stale, expire or target an unintended object if the
dispatcher binding is wrong.

## Output and stopping

Standard output is JSON Lines. The following **illustrative excerpts** show
the shape, not an observed successful run; setup and cleanup records are
omitted:

```json
{"phase":"read","tag":"0123456789abcdef0123456789abcdef","type":"compound","offset":0,"count":4096}
{"phase":"read","tag":"fedcba9876543210fedcba9876543210","type":"compound","offset":4096,"count":4096}
{"bytes_read":8192,"bytes_read_is_lower_bound":false,"bytes_requested":8192,"reason":"complete","status":"completed","type":"result"}
```

Each `compound` record is emitted **before an attempt** and contains a
fresh random nonce; it is not proof the request reached the server. `read`
records also carry the intended offset and count. Successful responses
must echo the tag. Setup and cleanup COMPOUNDs are tagged too. Logs contain
no file contents, credentials, raw file handles or paths, but tags and
ranges are still sensitive operational metadata; restrict their access and
retention.

Handle-mode records use the structured tag above, and their terminal result
also includes `plan_ref` and `task_ref`. These refs aid matching only; neither
they nor `w` authenticate the caller. A future server bridge must define and
validate its own provenance policy.

For example, with illustrative references unrelated to the sample task:

```json
{"phase":"read","tag":"pc1:w:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb:0123456789abcdef0123456789abcdef","type":"compound","offset":0,"count":4096}
{"bytes_read":4096,"bytes_read_is_lower_bound":false,"bytes_requested":4096,"plan_ref":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","reason":"complete","status":"completed","task_ref":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","type":"result"}
```

The terminal result reports only bytes confirmed by responses. `completed`
means the entire requested range was read, not that it remains cached.
`partial` means the range was not fully read, for example at EOF.
`error`, `timeout`, and `cancelled` are not success. When
`bytes_read_is_lower_bound` is true, an already-admitted READ may have
completed without its response being recorded, the worker violated its
message protocol, or the worker could not be confirmed stopped. Do not infer
exact server bytes from that count. A definite failure before any READ
reports an exact zero, and a validated, definitive NFS rejection preserves the
exact count from earlier successful READs. An uncached replay reply remains
uncertain. Exit status is 0 for `completed`, 130 for
an interrupt, 143 for SIGTERM cancellation, and 1 for partial, timeout, or
error; invalid arguments exit 2.

If task-file validation times out, is cancelled, or fails before the NFS
worker starts, the CLI emits a distinct record, not a canonical task result:

```json
{"execution_started":false,"reason":"deadline_exceeded","status":"timeout","type":"pre_validation_result"}
```

It omits task references and byte counts because the task may not have been
parsed. The dispatcher may synthesize a canonical result from its trusted
task metadata only when it has confirmed that no NFS COMPOUND was admitted.
If the validation process cannot be confirmed stopped, the reason is
`task_validation_worker_stop_unconfirmed`; the validation process itself
does not perform NFS operations. Argument-parser failures can occur before
this record is emitted and still exit 2.

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

The adapter tests in [`tests/warmer/test_nfs41.py`](../tests/warmer/test_nfs41.py)
exercise error handling and negotiation. The offline
[`tests/warmer/test_protocol_fixture.py`](../tests/warmer/test_protocol_fixture.py)
replaces the RPC transport with an in-memory pipe while using `pynfs` to
encode and decode real NFS XDR. It checks request tags, the AUTH_SYS
credential at that boundary, operation order, read-only OPEN, range READs,
short reads, truncated replies and cleanup. It does not exercise a complete
RPC envelope, a real server or network transport.

The offline XDR fixture also verifies direct-handle `PUTFH` and
`OPEN(CLAIM_FH)`, no path lookup, the 104-byte structured tags, negotiated
response sizing, and representative NFS handle/security errors. These tests
do not establish live direct-handle interoperability. The Chimera/CUFS tag
bridge and a controlled cache experiment remain distinct evidence; a mock or
completed read cannot replace them.

The focused schema-v2 handle contract test additionally starts with a plan
item, writes the exact private Warmer task, runs the real task validator and
supervisor, and uses the XDR fixture to prove the supplied handle and exact
offset/count reach `PUTFH`, `GETATTR`, `OPEN(CLAIM_FH)`, `READ`, `CLOSE`, and
session/client cleanup before an exact terminal result is accepted. This is
offline plumbing evidence only; live handle mode remains unvalidated and
approval-gated.

## Boundaries and missing evidence

- The operator supplies a path or a trusted dispatcher attests a handle and
  identity binding; the reader cannot verify that binding. A stable identity
  resolver, allowlists, and a Controller contract do not exist. NFS
  authorization and the operator's approval remain necessary. Reads can
  affect access-time metadata.
- A future Chimera bridge from COMPOUND tag to server READ operation ID and
  CUFS is **not implemented or verified**. Tags are client-controlled,
  copyable, and not authentication. Unmatched events remain `unknown`, not
  foreground demand. No model or evaluation should treat this log alone as
  trustworthy origin.
- This pilot has no bandwidth/rate limit, pressure pause, cache-headroom
  control, global budget, deployment integration, or foreground isolation.
  Even a read-only request can contend with foreground I/O or evict useful
  data. The 16 MiB per-run cap is not a safety guarantee.
- One path-mode read returned the requested bytes through Chimera; the
  direct-handle mode, event bridge and cache-residency/performance effects
  remain unverified. Further pilots need separate approval and controlled
  evidence before any efficacy or safety claim.
