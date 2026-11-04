"""Offline COMPOUND round trips using the pinned pynfs client and XDR types."""

import re

import pytest

from predictive_cache.warmer import nfs41
from predictive_cache.warmer.execution import execute

const = nfs41.const
xdr = nfs41.xdr
lib = nfs41.nfs4client.nfs4lib

SESSION_ID = b"s" * 16
STATE_ID = xdr.stateid4(7, b"o" * 12)
ROOT_FH = b"root"
DIR_FH = b"directory"
FILE_FH = b"file"
ATTRS = xdr.channel_attrs4(0, 8192, 8192, 8192, 128, 1, [])


class ScriptedPipe:
    """Replace only the RPC pipe; requests and replies remain real XDR bytes."""

    def __init__(
        self,
        reads=(),
        *,
        truncate_read=None,
        failures=None,
        attrs=ATTRS,
        mismatch_read=False,
        file_type=const.NF4REG,
        file_size=65536,
    ):
        self.reads = iter(reads)
        self.truncate_read = truncate_read
        self.failures = failures or {}
        self.attrs = attrs
        self.mismatch_read = mismatch_read
        self.file_type = file_type
        self.file_size = file_size
        self.op_counts = {}
        self.calls = []
        self.response_bytes = []
        self.replies = {}
        self.read_number = 0
        self.getfh_number = 0

    def send_call(self, program, version, procedure, data, cred):
        assert (program, version, procedure) == (const.NFS4_PROGRAM, const.NFS_V4, 1)
        unpacker = lib.FancyNFS4Unpacker(data)
        args = unpacker.unpack_COMPOUND4args()
        unpacker.done()
        self.calls.append((args, cred))
        results = []
        status = const.NFS4_OK
        for op in args.argarray:
            count = self.op_counts.get(op.argop, 0) + 1
            self.op_counts[op.argop] = count
            status = self.failures.get((op.argop, count), const.NFS4_OK)
            results.append(self._result(op) if status == const.NFS4_OK else self._error(op, status))
            if status != const.NFS4_OK:
                break
        packer = lib.FancyNFS4Packer()
        tag = b"wrong" if self.mismatch_read and any(
            op.argop == const.OP_READ for op in args.argarray
        ) else args.tag
        packer.pack_COMPOUND4res(xdr.COMPOUND4res(status, tag, results))
        reply = packer.get_buffer()
        if self.read_number == self.truncate_read:
            reply = reply[:-1]
            self.truncate_read = None
        xid = len(self.calls)
        self.response_bytes.append(reply)
        self.replies[xid] = reply
        return xid

    def listen(self, xid, timeout):
        return None, self.replies.pop(xid)

    def _error(self, op, status):
        constructors = {
            const.OP_GETFH: ("opgetfh", xdr.GETFH4res),
            const.OP_PUTFH: ("opputfh", xdr.PUTFH4res),
            const.OP_GETATTR: ("opgetattr", xdr.GETATTR4res),
            const.OP_OPEN: ("opopen", xdr.OPEN4res),
            const.OP_READ: ("opread", xdr.READ4res),
            const.OP_CLOSE: ("opclose", xdr.CLOSE4res),
            const.OP_DESTROY_CLIENTID: ("opdestroy_clientid", xdr.DESTROY_CLIENTID4res),
        }
        name, result_type = constructors[op.argop]
        return xdr.nfs_resop4(op.argop, **{name: result_type(status)})

    def _result(self, op):
        code = op.argop
        if code == const.OP_EXCHANGE_ID:
            payload = xdr.EXCHANGE_ID4res(
                const.NFS4_OK,
                xdr.EXCHANGE_ID4resok(
                    123,
                    1,
                    0,
                    xdr.state_protect4_r(const.SP4_NONE),
                    xdr.server_owner4(0, b"fixture-server"),
                    b"fixture-scope",
                    [],
                ),
            )
            return xdr.nfs_resop4(code, opexchange_id=payload)
        if code == const.OP_CREATE_SESSION:
            payload = xdr.CREATE_SESSION4res(
                const.NFS4_OK,
                xdr.CREATE_SESSION4resok(SESSION_ID, 1, 0, self.attrs, self.attrs),
            )
            return xdr.nfs_resop4(code, opcreate_session=payload)
        if code == const.OP_SEQUENCE:
            seq = op.opsequence
            payload = xdr.SEQUENCE4res(
                const.NFS4_OK,
                xdr.SEQUENCE4resok(SESSION_ID, seq.sa_sequenceid, seq.sa_slotid, 0, 0, 0),
            )
            return xdr.nfs_resop4(code, opsequence=payload)
        if code == const.OP_GETFH:
            handles = (ROOT_FH, DIR_FH, FILE_FH)
            handle = handles[self.getfh_number]
            self.getfh_number += 1
            return xdr.nfs_resop4(
                code, opgetfh=xdr.GETFH4res(const.NFS4_OK, xdr.GETFH4resok(handle))
            )
        if code == const.OP_GETATTR:
            attributes = {const.FATTR4_TYPE: self.file_type, const.FATTR4_SIZE: self.file_size}
            return xdr.nfs_resop4(
                code, opgetattr=xdr.GETATTR4res(const.NFS4_OK, xdr.GETATTR4resok(attributes))
            )
        if code == const.OP_OPEN:
            payload = xdr.OPEN4resok(
                STATE_ID,
                xdr.change_info4(True, 1, 1),
                0,
                0,
                xdr.open_delegation4(const.OPEN_DELEGATE_NONE),
            )
            return xdr.nfs_resop4(code, opopen=xdr.OPEN4res(const.NFS4_OK, payload))
        if code == const.OP_READ:
            self.read_number += 1
            data, eof = next(self.reads)
            return xdr.nfs_resop4(
                code, opread=xdr.READ4res(const.NFS4_OK, xdr.READ4resok(eof, data))
            )
        if code == const.OP_CLOSE:
            return xdr.nfs_resop4(code, opclose=xdr.CLOSE4res(const.NFS4_OK, STATE_ID))
        simple = {
            const.OP_RECLAIM_COMPLETE: ("opreclaim_complete", xdr.RECLAIM_COMPLETE4res),
            const.OP_PUTROOTFH: ("opputrootfh", xdr.PUTROOTFH4res),
            const.OP_PUTFH: ("opputfh", xdr.PUTFH4res),
            const.OP_LOOKUP: ("oplookup", xdr.LOOKUP4res),
            const.OP_DESTROY_SESSION: ("opdestroy_session", xdr.DESTROY_SESSION4res),
            const.OP_DESTROY_CLIENTID: ("opdestroy_clientid", xdr.DESTROY_CLIENTID4res),
        }
        name, result_type = simple[code]
        return xdr.nfs_resop4(code, **{name: result_type(const.NFS4_OK)})


def make_client(pipe):
    # Avoid NFS4Client.__init__: it starts threads and connects a socket.
    client = nfs41.nfs4client.NFS4Client.__new__(nfs41.nfs4client.NFS4Client)
    client.c1 = pipe
    client.default_prog = const.NFS4_PROGRAM
    client.default_vers = const.NFS_V4
    client.minorversion = 1
    client.tag = b"unused"
    client.summary = None
    client.sessions = {}
    client.prog = const.NFS4_CALLBACK
    client.impl_id = xdr.nfs_impl_id4(b"fixture", b"fixture-client", xdr.nfstime4(0, 0))
    return client


@pytest.fixture
def warm_request():
    return {
        "host": "192.0.2.1",
        "port": 2049,
        "path": "/directory/file",
        "offset": 17,
        "length": 9000,
        "uid": 1001,
        "gid": 1002,
    }


def assert_frames(pipe, events, tag_pattern=r"[0-9a-f]{32}"):
    compounds = [event for event in events if event["type"] == "compound"]
    assert len(compounds) == len(pipe.calls)
    assert len({event["tag"] for event in compounds}) == len(compounds)
    for event, (args, cred) in zip(compounds, pipe.calls, strict=True):
        assert re.fullmatch(tag_pattern, event["tag"])
        assert args.tag == event["tag"].encode("ascii")
        assert args.minorversion == 1
        assert cred.flavor == nfs41.security.AUTH_SYS
        assert (cred.context.uid, cred.context.gid, cred.context.gids) == (1001, 1002, [])
        assert cred.context.machinename == b"pcache"
        auth = cred.sec.make_cred(cred)
        assert auth.flavor == nfs41.security.AUTH_SYS
        decoded_auth = cred.sec.unpack_cred(cred.sec.pack_cred(auth.body))
        assert (decoded_auth.uid, decoded_auth.gid, decoded_auth.gids) == (1001, 1002, [])
        assert decoded_auth.machinename == b"pcache"
        assert all(op.argop not in (const.OP_CREATE, const.OP_WRITE) for op in args.argarray)
    session_ops = [
        args.argarray[0].opsequence
        for args, _ in pipe.calls
        if args.argarray[0].argop == const.OP_SEQUENCE
    ]
    assert [seq.sa_sequenceid for seq in session_ops] == list(range(1, len(session_ops) + 1))
    assert all(seq.sa_sessionid == SESSION_ID and seq.sa_slotid == 0 for seq in session_ops)
    return compounds


def decode_reply(pipe, index):
    unpacker = lib.FancyNFS4Unpacker(pipe.response_bytes[index])
    reply = unpacker.unpack_COMPOUND4res()
    unpacker.done()
    return reply


@pytest.mark.parametrize(
    ("reads", "truncate_read", "expected", "progress"),
    [
        (
            [(b"a" * 4096, False), (b"b" * 4096, False), (b"c" * 808, True)],
            None,
            {"status": "completed", "bytes_read": 9000, "reason": "complete"},
            [4096, 8192, 9000],
        ),
        (
            [(b"a" * 4096, False), (b"b" * 23, True)],
            None,
            {"status": "partial", "bytes_read": 4119, "reason": "eof"},
            [4096, 4119],
        ),
        (
            [(b"a" * 4096, False), (b"b" * 23, False)],
            None,
            {"status": "partial", "bytes_read": 4119, "reason": "short_read"},
            [4096, 4119],
        ),
        (
            [(b"a" * 4096, False), (b"b" * 23, False)],
            2,
            {"status": "error", "bytes_read": 4096, "reason": "transport_error:EOFError"},
            [4096],
        ),
    ],
)
def test_real_client_session_round_trip(warm_request, reads, truncate_read, expected, progress):
    pipe = ScriptedPipe(reads, truncate_read=truncate_read)
    events = []
    result = nfs41._run(make_client(pipe), warm_request, events.append)

    assert result == expected
    assert [event["bytes_read"] for event in events if event["type"] == "progress"] == progress
    compounds = assert_frames(pipe, events)

    prefix = [
        ("exchange_id", [const.OP_EXCHANGE_ID]),
        ("create_session", [const.OP_CREATE_SESSION]),
        ("reclaim_complete", [const.OP_SEQUENCE, const.OP_RECLAIM_COMPLETE]),
        ("lookup", [const.OP_SEQUENCE, const.OP_PUTROOTFH, const.OP_GETFH]),
        ("lookup", [const.OP_SEQUENCE, const.OP_PUTFH, const.OP_LOOKUP, const.OP_GETFH]),
        ("lookup", [const.OP_SEQUENCE, const.OP_PUTFH, const.OP_LOOKUP, const.OP_GETFH]),
        ("open", [const.OP_SEQUENCE, const.OP_PUTFH, const.OP_OPEN]),
    ]
    read_groups = [("read", [const.OP_SEQUENCE, const.OP_PUTFH, const.OP_READ])] * len(reads)
    cleanup = [
        ("close", [const.OP_SEQUENCE, const.OP_PUTFH, const.OP_CLOSE]),
        ("destroy_session", [const.OP_SEQUENCE, const.OP_DESTROY_SESSION]),
        ("destroy_clientid", [const.OP_DESTROY_CLIENTID]),
    ]
    expected_groups = prefix + read_groups + ([] if truncate_read else cleanup)
    assert [
        (event["phase"], [op.argop for op in args.argarray])
        for event, (args, _) in zip(compounds, pipe.calls, strict=True)
    ] == expected_groups
    exchange = pipe.calls[0][0].argarray[0].opexchange_id
    assert exchange.eia_state_protect.spa_how == const.SP4_NONE
    created = pipe.calls[1][0].argarray[0].opcreate_session
    assert (created.csa_clientid, created.csa_sequence, created.csa_flags) == (123, 1, 0)
    assert created.csa_fore_chan_attrs.ca_maxrequests == 1
    lookup = pipe.calls[4][0].argarray
    assert lookup[1].opputfh.object == ROOT_FH
    assert lookup[2].oplookup.objname == b"directory"
    lookup = pipe.calls[5][0].argarray
    assert lookup[1].opputfh.object == DIR_FH
    assert lookup[2].oplookup.objname == b"file"
    open_args = pipe.calls[6][0].argarray
    assert open_args[1].opputfh.object == FILE_FH
    opened = open_args[2].opopen
    assert opened.openhow.opentype == const.OPEN4_NOCREATE
    assert opened.share_access == (
        const.OPEN4_SHARE_ACCESS_READ | const.OPEN4_SHARE_ACCESS_WANT_NO_DELEG
    )
    assert opened.share_deny == const.OPEN4_SHARE_DENY_NONE
    assert opened.claim.claim == const.CLAIM_FH
    for event, (args, _) in zip(compounds, pipe.calls, strict=True):
        if event["phase"] == "read":
            assert args.argarray[1].opputfh.object == FILE_FH
            read = args.argarray[2].opread
            assert (read.stateid.seqid, read.stateid.other) == (STATE_ID.seqid, STATE_ID.other)
            assert (read.offset, read.count) == (event["offset"], event["count"])
    read_events = [event for event in compounds if event["phase"] == "read"]
    assert [(event["offset"], event["count"]) for event in read_events] == [
        (17, 4096),
        (4113, 4096),
        (8209, 808),
    ][: len(reads)]
    if not truncate_read:
        close_args = pipe.calls[-3][0].argarray
        assert close_args[1].opputfh.object == FILE_FH
        closed = close_args[2].opclose.open_stateid
        assert (closed.seqid, closed.other) == (STATE_ID.seqid, STATE_ID.other)
        assert pipe.calls[-2][0].argarray[1].opdestroy_session.dsa_sessionid == SESSION_ID
        assert pipe.calls[-1][0].argarray[0].opdestroy_clientid.dca_clientid == 123


@pytest.mark.parametrize(
    ("failed_op", "occurrence", "phase", "reply_ops"),
    [
        (
            const.OP_GETFH,
            3,
            "lookup",
            [const.OP_SEQUENCE, const.OP_PUTFH, const.OP_LOOKUP, const.OP_GETFH],
        ),
        (
            const.OP_OPEN,
            1,
            "open",
            [const.OP_SEQUENCE, const.OP_PUTFH, const.OP_OPEN],
        ),
    ],
)
def test_wire_path_or_open_failure_preserves_primary_error(
    warm_request, failed_op, occurrence, phase, reply_ops
):
    pipe = ScriptedPipe(
        failures={
            (failed_op, occurrence): const.NFS4ERR_ACCESS,
            (const.OP_DESTROY_CLIENTID, 1): const.NFS4ERR_IO,
        }
    )
    events = []
    result = nfs41._run(make_client(pipe), warm_request, nfs41._CompoundBudget(events.append))
    assert result == {
        "status": "error",
        "bytes_read": 0,
        "reason": f"{phase}_NFS4ERR_ACCESS:cleanup:destroy_clientid_NFS4ERR_IO",
    }
    compounds = assert_frames(pipe, events)
    phases = [event["phase"] for event in compounds]
    assert phases[-3:] == [phase, "destroy_session", "destroy_clientid"]
    assert "read" not in phases and "close" not in phases
    if failed_op == const.OP_GETFH:
        assert "open" not in phases
    reply = decode_reply(pipe, -3)
    assert reply.tag == compounds[-3]["tag"].encode("ascii")
    assert reply.status == const.NFS4ERR_ACCESS
    assert [op.resop for op in reply.resarray] == reply_ops
    assert reply.resarray[-1].status == const.NFS4ERR_ACCESS
    assert decode_reply(pipe, -1).status == const.NFS4ERR_IO


def test_wire_read_and_close_failures_preserve_progress_and_primary_error(warm_request):
    pipe = ScriptedPipe(
        [(b"a" * 4096, False)],
        failures={
            (const.OP_READ, 2): const.NFS4ERR_ACCESS,
            (const.OP_CLOSE, 1): const.NFS4ERR_IO,
        },
    )
    events = []
    result = nfs41._run(make_client(pipe), warm_request, nfs41._CompoundBudget(events.append))
    assert result == {
        "status": "error",
        "bytes_read": 4096,
        "reason": "read_NFS4ERR_ACCESS:cleanup:close_NFS4ERR_IO",
    }
    assert [event["bytes_read"] for event in events if event["type"] == "progress"] == [4096]
    compounds = assert_frames(pipe, events)
    assert [event["phase"] for event in compounds[-5:]] == [
        "read",
        "read",
        "close",
        "destroy_session",
        "destroy_clientid",
    ]
    read_reply = decode_reply(pipe, -4)
    assert read_reply.status == const.NFS4ERR_ACCESS
    assert [op.resop for op in read_reply.resarray] == [
        const.OP_SEQUENCE,
        const.OP_PUTFH,
        const.OP_READ,
    ]
    assert read_reply.resarray[-1].status == const.NFS4ERR_ACCESS
    close_args = pipe.calls[-3][0].argarray
    assert close_args[1].opputfh.object == FILE_FH
    closed = close_args[2].opclose.open_stateid
    assert (closed.seqid, closed.other) == (STATE_ID.seqid, STATE_ID.other)
    close_reply = decode_reply(pipe, -3)
    assert close_reply.status == const.NFS4ERR_IO
    assert close_reply.resarray[-1].resop == const.OP_CLOSE
    assert close_reply.resarray[-1].status == const.NFS4ERR_IO


def test_budget_reserves_wire_cleanup_after_read_progress(monkeypatch, warm_request):
    monkeypatch.setattr(nfs41, "MAX_COMPOUNDS", 11)
    pipe = ScriptedPipe([(b"a" * 4096, False)], failures={(const.OP_CLOSE, 1): const.NFS4ERR_IO})
    events = []
    result = nfs41._run(make_client(pipe), warm_request, nfs41._CompoundBudget(events.append))
    assert result == {
        "status": "error",
        "bytes_read": 4096,
        "reason": "compound_limit_exceeded:cleanup:close_NFS4ERR_IO",
    }
    assert [event["bytes_read"] for event in events if event["type"] == "progress"] == [4096]
    compounds = assert_frames(pipe, events)
    assert len(compounds) == 11
    assert [event["phase"] for event in compounds[-4:]] == [
        "read",
        "close",
        "destroy_session",
        "destroy_clientid",
    ]
    assert decode_reply(pipe, -3).status == const.NFS4ERR_IO


@pytest.fixture
def handle_request(warm_request):
    request = {key: value for key, value in warm_request.items() if key != "path"}
    request.update(file_handle=b"\x00private-handle\xff", plan_ref="a" * 32, task_ref="b" * 32)
    return request


def test_wire_handle_mode_uses_structured_tags_and_never_looks_up_path(handle_request):
    pipe = ScriptedPipe([(b"a" * 4096, False), (b"b" * 4096, False), (b"c" * 808, True)])
    events = []
    result = nfs41._run(make_client(pipe), handle_request, nfs41._CompoundBudget(events.append))

    assert result == {"status": "completed", "bytes_read": 9000, "reason": "complete"}
    compounds = assert_frames(pipe, events, rf"pc1:w:{'a' * 32}:{'b' * 32}:[0-9a-f]{{32}}")
    assert all(len(event["tag"].encode("ascii")) == 104 for event in compounds)
    assert [event["phase"] for event in compounds] == [
        "exchange_id",
        "create_session",
        "reclaim_complete",
        "getattr",
        "open",
        "read",
        "read",
        "read",
        "close",
        "destroy_session",
        "destroy_clientid",
    ]
    for event, (args, _) in zip(compounds, pipe.calls, strict=True):
        codes = [operation.argop for operation in args.argarray]
        assert const.OP_PUTROOTFH not in codes and const.OP_LOOKUP not in codes
        if event["phase"] in {"getattr", "open", "read", "close"}:
            assert args.argarray[1].opputfh.object == handle_request["file_handle"]
        if event["phase"] == "getattr":
            assert args.argarray[2].argop == const.OP_GETATTR
        if event["phase"] == "open":
            opened = args.argarray[2].opopen
            assert opened.claim.claim == const.CLAIM_FH
            assert opened.openhow.opentype == const.OPEN4_NOCREATE
            assert opened.share_access == (
                const.OPEN4_SHARE_ACCESS_READ | const.OPEN4_SHARE_ACCESS_WANT_NO_DELEG
            )
        if event["phase"] == "read":
            read = args.argarray[2].opread
            assert (read.offset, read.count) == (event["offset"], event["count"])
    assert "private-handle" not in repr(events) + repr(result)


@pytest.mark.parametrize(
    "status",
    [const.NFS4ERR_BADHANDLE, const.NFS4ERR_STALE, const.NFS4ERR_FHEXPIRED, const.NFS4ERR_WRONGSEC],
)
def test_wire_handle_failure_never_falls_back_to_lookup(handle_request, status):
    pipe = ScriptedPipe(failures={(const.OP_PUTFH, 1): status})
    events = []
    result = nfs41._run(make_client(pipe), handle_request, nfs41._CompoundBudget(events.append))

    assert result == {
        "status": "error",
        "bytes_read": 0,
        "reason": f"getattr_{const.nfsstat4[status]}",
    }
    compounds = assert_frames(pipe, events, rf"pc1:w:{'a' * 32}:{'b' * 32}:[0-9a-f]{{32}}")
    assert "read" not in [event["phase"] for event in compounds]
    assert all(
        op.argop not in {const.OP_PUTROOTFH, const.OP_LOOKUP, const.OP_READ}
        for args, _ in pipe.calls
        for op in args.argarray
    )


def test_wire_handle_read_tag_mismatch_stops_without_cleanup(handle_request):
    pipe = ScriptedPipe([(b"a" * 4096, False)], mismatch_read=True)
    events = []
    result = nfs41._run(make_client(pipe), handle_request, nfs41._CompoundBudget(events.append))

    assert result == {"status": "error", "bytes_read": 0, "reason": "read_tag_mismatch"}
    assert events[-1]["phase"] == "read"
    assert not any(event["type"] == "progress" for event in events)


def test_wire_handle_structured_tag_reduces_negotiated_read_limit(handle_request):
    handle_request["length"] = 45
    attrs = xdr.channel_attrs4(0, 8192, 252, 8192, 128, 1, [])
    pipe = ScriptedPipe([(b"a" * 44, False), (b"b", True)], attrs=attrs)
    events = []
    result = nfs41._run(make_client(pipe), handle_request, nfs41._CompoundBudget(events.append))

    assert result == {"status": "completed", "bytes_read": 45, "reason": "complete"}
    compounds = assert_frames(pipe, events, rf"pc1:w:{'a' * 32}:{'b' * 32}:[0-9a-f]{{32}}")
    assert [(event["offset"], event["count"]) for event in compounds if event["phase"] == "read"] == [
        (17, 44),
        (61, 1),
    ]


@pytest.mark.parametrize("file_type", [const.NF4DIR, const.NF4LNK, const.NF4CHR])
def test_wire_handle_rejects_nonregular_type_without_open_or_read(handle_request, file_type):
    pipe = ScriptedPipe(file_type=file_type)
    events = []
    result = nfs41._run(make_client(pipe), handle_request, nfs41._CompoundBudget(events.append))

    assert result["status"] == "error"
    assert result["bytes_read"] == 0
    assert all(
        op.argop not in {const.OP_OPEN, const.OP_READ, const.OP_PUTROOTFH, const.OP_LOOKUP}
        for args, _ in pipe.calls
        for op in args.argarray
    )
    assert "private-handle" not in repr(events) + repr(result)


def test_wire_handle_rejects_range_past_reported_size(handle_request):
    pipe = ScriptedPipe(file_size=handle_request["offset"] + handle_request["length"] - 1)
    events = []
    result = nfs41._run(make_client(pipe), handle_request, nfs41._CompoundBudget(events.append))

    assert result["status"] == "error"
    assert result["bytes_read"] == 0
    assert all(
        op.argop not in {const.OP_OPEN, const.OP_READ}
        for args, _ in pipe.calls
        for op in args.argarray
    )


def _wire_error_worker(request, emit):
    pipe = ScriptedPipe(
        [(b"a" * 4096, False)],
        failures={(const.OP_READ, 2): const.NFS4ERR_ACCESS},
    )
    return nfs41._run(make_client(pipe), request, nfs41._CompoundBudget(emit))


def _wire_truncated_reply_worker(request, emit):
    pipe = ScriptedPipe(
        [(b"a" * 4096, False), (b"b" * 4096, False)],
        truncate_read=2,
    )
    return nfs41._run(make_client(pipe), request, nfs41._CompoundBudget(emit))


def _wire_putfh_error_worker(request, emit):
    pipe = ScriptedPipe(
        [(b"a" * 4096, False)],
        failures={(const.OP_PUTFH, 4): const.NFS4ERR_BADHANDLE},
    )
    return nfs41._run(make_client(pipe), request, nfs41._CompoundBudget(emit))


def _wire_uncached_reply_worker(request, emit):
    pipe = ScriptedPipe(
        [(b"a" * 4096, False)],
        failures={(const.OP_PUTFH, 4): const.NFS4ERR_RETRY_UNCACHED_REP},
    )
    return nfs41._run(make_client(pipe), request, nfs41._CompoundBudget(emit))


@pytest.mark.parametrize(
    ("worker", "reason", "lower_bound"),
    [
        (_wire_error_worker, "read_NFS4ERR_ACCESS", False),
        (_wire_putfh_error_worker, "read_NFS4ERR_BADHANDLE", False),
        (_wire_uncached_reply_worker, "read_NFS4ERR_RETRY_UNCACHED_REP", True),
        (_wire_truncated_reply_worker, "transport_error:EOFError", True),
    ],
)
def test_wire_read_failure_certainty_reaches_supervisor(handle_request, worker, reason, lower_bound):
    result = execute(handle_request, 10, lambda _: None, worker=worker)
    assert result["status"] == "error"
    assert result["reason"] == reason
    assert result["bytes_read"] == 4096
    assert result["bytes_read_is_lower_bound"] is lower_bound
