import errno
import logging
import os
import re
import subprocess
import sys
from types import SimpleNamespace

import pytest

from predictive_cache.warmer import limits, nfs41

const = nfs41.const
ops = nfs41._OPS


class FakeSession:
    def __init__(self, server):
        self.server = server
        self.sessionid = b"s" * 16
        self.fore_channel = SimpleNamespace(
            maxoperations=server.maxoperations, maxresponsesize=server.maxresponsesize
        )

    def compound(self, operations, *, tag, max_retries, handle_state_errors):
        assert max_retries == 1
        assert handle_state_errors is False
        return self.server.respond(operations, tag)


class FakeServer:
    def __init__(self, reads=(), failure=None, mismatch=None, maxresponsesize=8192):
        self.reads = iter(reads)
        self.failure = failure
        self.mismatch = mismatch
        self.maxresponsesize = maxresponsesize
        self.maxoperations = 128
        self.calls = []
        self.events = []
        self.credential = None
        self.stopped = False
        self.stop_error = None
        self.connection_error = None
        self.attrs = {const.FATTR4_TYPE: const.NF4REG, const.FATTR4_SIZE: 2**40}

    def emit(self, event):
        self.events.append(event)

    def respond(self, operations, tag):
        event = self.events[-1]
        assert event["type"] == "compound" and event["tag"].encode() == tag
        phase = event["phase"]
        self.calls.append((phase, operations, tag))
        if self.failure == phase:
            return SimpleNamespace(status=const.NFS4ERR_ACCESS, tag=tag, resarray=[])
        if self.mismatch == phase:
            tag = b"wrong"
        result = []
        for operation in operations:
            if operation.argop == const.OP_GETFH:
                result.append(SimpleNamespace(resop=operation.argop, object=b"private-handle"))
            elif operation.argop == const.OP_GETATTR:
                result.append(SimpleNamespace(resop=operation.argop, obj_attributes=self.attrs))
            elif operation.argop == const.OP_OPEN:
                result.append(
                    SimpleNamespace(resop=operation.argop, stateid=nfs41.xdr.stateid4(1, b"x" * 12))
                )
            elif operation.argop == const.OP_READ:
                data, eof = next(self.reads)
                result.append(SimpleNamespace(resop=operation.argop, data=data, eof=eof))
            else:
                result.append(SimpleNamespace(resop=operation.argop))
        return SimpleNamespace(status=const.NFS4_OK, tag=tag, resarray=result)

    def install(self, monkeypatch):
        server = self

        class FakeClient:
            def __init__(self, host, port, minorversion):
                server.client_args = (host, port, minorversion)
                assert self.handle_1(None, None) == (nfs41.rpc.PROC_UNAVAIL, b"")
                logging.getLogger("rpc.thread").info("endpoint %s", host)
                self.impl_id = object()
                self.prog = 0x40000000
                self._alarm = True
                if server.connection_error:
                    raise server.connection_error

            def set_cred(self, cred):
                server.credential = cred

            def compound_async(self, operations, *, tag, version):
                assert version == 1
                self.response = server.respond(operations, tag)
                return 1

            def listen(self, xid):
                return self.response

            def stop(self):
                server.stopped = True
                if server.stop_error:
                    raise server.stop_error

        class FakeRecord:
            def __init__(self, result, client, cred, protection):
                self.clientid = 123
                self.seqid = 1
                server.protection = protection

            def _add_session(self, result):
                return FakeSession(server)

        def fake_session_compound(target, operations, tag, phase):
            response = target.compound(
                operations, tag=tag, max_retries=1, handle_state_errors=False
            )
            if phase != "read":
                return response, {}
            sequence = SimpleNamespace(sa_sessionid=target.sessionid, sa_sequenceid=1, sa_slotid=0)
            seq_result = nfs41.xdr.SEQUENCE4resok(target.sessionid, 1, 0, 0, 0, 0)
            results = [
                nfs41.xdr.nfs_resop4(
                    const.OP_SEQUENCE,
                    opsequence=nfs41.xdr.SEQUENCE4res(const.NFS4_OK, seq_result),
                )
            ]
            for result in response.resarray:
                if isinstance(result, nfs41.xdr.nfs_resop4):
                    results.append(result)
                elif result.resop == const.OP_PUTFH:
                    results.append(
                        nfs41.xdr.nfs_resop4(
                            const.OP_PUTFH, opputfh=nfs41.xdr.PUTFH4res(const.NFS4_OK)
                        )
                    )
                elif result.resop == const.OP_READ:
                    read = nfs41.xdr.READ4resok(result.eof, result.data)
                    results.append(
                        nfs41.xdr.nfs_resop4(
                            const.OP_READ, opread=nfs41.xdr.READ4res(const.NFS4_OK, read)
                        )
                    )
                else:
                    results.append(result)
            return response, {"sequence": sequence, "results": tuple(results)}

        def fake_client_compound(target, operations, tag, phase):
            xid = target.compound_async(operations, tag=tag, version=1)
            return target.listen(xid)

        monkeypatch.setattr(nfs41.nfs4client, "NFS4Client", FakeClient)
        monkeypatch.setattr(nfs41.nfs4client, "ClientRecord", FakeRecord)
        monkeypatch.setattr(nfs41.nfs4client, "SessionRecord", FakeSession)
        monkeypatch.setattr(nfs41, "_session_compound", fake_session_compound)
        monkeypatch.setattr(nfs41, "_client_compound", fake_client_compound)


@pytest.fixture
def warm_request():
    return {
        "host": "192.0.2.1",
        "port": 2049,
        "path": "/private/directory/file",
        "offset": 17,
        "length": 9000,
        "uid": 1001,
        "gid": 1002,
    }


@pytest.fixture
def handle_request(warm_request):
    request = {key: value for key, value in warm_request.items() if key != "path"}
    request.update(
        file_handle=b"\x00sensitive-handle\xff",
        plan_ref="a" * 32,
        task_ref="b" * 32,
        volume_id="opaque-private-volume",
        export_id="opaque-private-export",
        file_id="opaque-private-file",
        plan_id="opaque-private-plan",
        task_id="opaque-private-task",
    )
    return request


def test_full_range_setup_read_and_cleanup(monkeypatch, warm_request, caplog):
    server = FakeServer([(b"a" * 4096, False), (b"b" * 4096, False), (b"c" * 808, True)])
    server.install(monkeypatch)
    with caplog.at_level(logging.INFO):
        result = nfs41.warm(warm_request, server.emit)
    assert result == {"status": "completed", "bytes_read": 9000, "reason": "complete"}
    assert server.client_args == (warm_request["host"], 2049, 1)
    assert server.credential.flavor == nfs41.security.AUTH_SYS
    assert (server.credential.context.uid, server.credential.context.gid) == (1001, 1002)
    assert server.credential.context.gids == []
    assert server.protection.spa_how == const.SP4_NONE
    assert server.stopped
    session_args = next(
        group[0].opcreate_session for phase, group, _ in server.calls if phase == "create_session"
    )
    assert session_args.csa_flags == 0
    assert session_args.csa_fore_chan_attrs.ca_maxrequests == 1
    assert [event["bytes_read"] for event in server.events if event["type"] == "progress"] == [
        4096,
        8192,
        9000,
    ]
    events = [event for event in server.events if event["type"] == "compound"]
    assert [event["phase"] for event in events] == [
        "exchange_id",
        "create_session",
        "reclaim_complete",
        "lookup",
        "lookup",
        "lookup",
        "lookup",
        "open",
        "read",
        "read",
        "read",
        "close",
        "destroy_session",
        "destroy_clientid",
    ]
    assert [(e["offset"], e["count"]) for e in events if e["phase"] == "read"] == [
        (17, 4096),
        (4113, 4096),
        (8209, 808),
    ]
    assert all(re.fullmatch(r"[0-9a-f]{32}", e["tag"]) for e in events)
    assert len({e["tag"] for e in events}) == len(events)
    assert all(re.fullmatch(r"[a-z0-9_-]{1,32}", e["phase"]) for e in events)
    assert all(set(e) == {"type", "tag", "phase"} for e in events if e["phase"] != "read")
    all_ops = [op for _, operations, _ in server.calls for op in operations]
    open_arg = next(op.opopen for op in all_ops if op.argop == const.OP_OPEN)
    assert open_arg.openhow.opentype == const.OPEN4_NOCREATE
    assert open_arg.claim.claim == const.CLAIM_FH
    assert open_arg.share_access == (
        const.OPEN4_SHARE_ACCESS_READ | const.OPEN4_SHARE_ACCESS_WANT_NO_DELEG
    )
    assert open_arg.share_deny == const.OPEN4_SHARE_DENY_NONE
    assert all(op.argop not in (const.OP_WRITE, const.OP_CREATE) for op in all_ops)
    open_ops = next(group for phase, group, _ in server.calls if phase == "open")
    assert [operation.argop for operation in open_ops] == [const.OP_PUTFH, const.OP_OPEN]
    assert all(sum(op.argop == const.OP_READ for op in group) <= 1 for _, group, _ in server.calls)
    serialized = repr(server.events) + repr(result) + caplog.text
    for secret in (warm_request["host"], warm_request["path"], "private-handle"):
        assert secret not in serialized
    untagged = [
        {key: value for key, value in event.items() if key != "tag"} for event in server.events
    ]
    for secret in ("1001", "1002"):
        assert secret not in repr(untagged) + repr(result) + caplog.text


@pytest.mark.parametrize(("eof", "reason"), [(True, "eof"), (False, "short_read")])
def test_short_read_stops_immediately(monkeypatch, warm_request, eof, reason):
    server = FakeServer([(b"a" * 100, eof)])
    server.install(monkeypatch)
    assert nfs41.warm(warm_request, server.emit) == {
        "status": "partial",
        "bytes_read": 100,
        "reason": reason,
    }
    assert sum(phase == "read" for phase, _, _ in server.calls) == 1
    assert {"type": "progress", "bytes_read": 100} in server.events


@pytest.mark.parametrize(
    ("phase", "prior_reads", "status", "bytes_read"),
    [
        ("exchange_id", [], "error", 0),
        ("create_session", [], "error", 0),
        ("reclaim_complete", [], "error", 0),
        ("lookup", [], "error", 0),
        ("open", [], "error", 0),
        ("read", [], "error", 0),
        ("close", [(b"a" * 4096, True)], "error", 4096),
    ],
)
def test_nfs_failure_is_sanitized(
    monkeypatch, warm_request, phase, prior_reads, status, bytes_read
):
    server = FakeServer(prior_reads, failure=phase)
    server.install(monkeypatch)
    result = nfs41.warm(warm_request, server.emit)
    assert result["status"] == "error"
    assert result == {
        "status": status,
        "bytes_read": bytes_read,
        "reason": f"{phase}_NFS4ERR_ACCESS",
    }
    assert not any(event["type"] == "read_rejected" for event in server.events)
    assert server.stopped


@pytest.mark.parametrize(
    "phase", ["exchange_id", "create_session", "lookup", "open", "read", "close"]
)
def test_tag_mismatch_stops_further_compounds(monkeypatch, warm_request, phase):
    server = FakeServer([(b"a" * 4096, True)], mismatch=phase)
    server.install(monkeypatch)
    result = nfs41.warm(warm_request, server.emit)
    assert result["status"] == "error"
    assert result["reason"] == f"{phase}_tag_mismatch"
    assert server.calls[-1][0] == phase


def test_second_read_failure_preserves_actual_progress(monkeypatch, warm_request):
    server = FakeServer([(b"a" * 4096, False)])
    server.install(monkeypatch)
    original = server.respond

    def fail_second(operations, tag):
        if sum(phase == "read" for phase, _, _ in server.calls) == 1:
            server.failure = "read"
        return original(operations, tag)

    monkeypatch.setattr(server, "respond", fail_second)
    assert nfs41.warm(warm_request, server.emit) == {
        "status": "error",
        "bytes_read": 4096,
        "reason": "read_NFS4ERR_ACCESS",
    }
    assert [e["bytes_read"] for e in server.events if e["type"] == "progress"] == [4096]


def _wire_response(status, tag, results):
    packer = nfs41.nfs4client.nfs4lib.FancyNFS4Packer()
    packer.pack_COMPOUND4res(nfs41.xdr.COMPOUND4res(status, tag, results))
    unpacker = nfs41.nfs4client.nfs4lib.FancyNFS4Unpacker(packer.get_buffer())
    response = unpacker.unpack_COMPOUND4res()
    unpacker.done()
    return response


def test_getfh_failure_happens_before_open_and_preserves_primary_error(monkeypatch, warm_request):
    server = FakeServer()
    server.install(monkeypatch)
    original = server.respond

    def fail_final_getfh(operations, tag):
        response = original(operations, tag)
        if sum(phase == "lookup" for phase, _, _ in server.calls) == 4:
            if server.events[-1]["phase"] != "lookup":
                return response
            server.failure = "destroy_clientid"
            xdr = nfs41.xdr
            return _wire_response(
                const.NFS4ERR_ACCESS,
                tag,
                [
                    xdr.nfs_resop4(const.OP_PUTFH, opputfh=xdr.PUTFH4res(const.NFS4_OK)),
                    xdr.nfs_resop4(const.OP_LOOKUP, oplookup=xdr.LOOKUP4res(const.NFS4_OK)),
                    xdr.nfs_resop4(const.OP_GETFH, opgetfh=xdr.GETFH4res(const.NFS4ERR_ACCESS)),
                ],
            )
        return response

    monkeypatch.setattr(server, "respond", fail_final_getfh)
    assert nfs41.warm(warm_request, server.emit) == {
        "status": "error",
        "bytes_read": 0,
        "reason": "lookup_NFS4ERR_ACCESS:cleanup:destroy_clientid_NFS4ERR_ACCESS",
    }
    phases = [phase for phase, _, _ in server.calls]
    assert "open" not in phases and "close" not in phases
    assert phases[-2:] == ["destroy_session", "destroy_clientid"]


def test_wire_open_stateid_is_closed_using_the_already_resolved_handle(monkeypatch, warm_request):
    server = FakeServer([(b"a" * 4096, False)])
    server.install(monkeypatch)
    original = server.respond
    stateid = nfs41.xdr.stateid4(7, b"z" * 12)

    def wire_open(operations, tag):
        response = original(operations, tag)
        if server.events[-1]["phase"] != "open":
            return response
        xdr = nfs41.xdr
        open_result = xdr.OPEN4resok(
            stateid,
            xdr.change_info4(False, 0, 0),
            0,
            0,
            xdr.open_delegation4(const.OPEN_DELEGATE_NONE),
        )
        return _wire_response(
            const.NFS4_OK,
            tag,
            [
                xdr.nfs_resop4(const.OP_PUTFH, opputfh=xdr.PUTFH4res(const.NFS4_OK)),
                xdr.nfs_resop4(const.OP_OPEN, opopen=xdr.OPEN4res(const.NFS4_OK, open_result)),
            ],
        )

    monkeypatch.setattr(server, "respond", wire_open)
    assert nfs41.warm({**warm_request, "length": 4096}, server.emit)["status"] == "completed"
    open_ops = next(operations for phase, operations, _ in server.calls if phase == "open")
    close_ops = next(operations for phase, operations, _ in server.calls if phase == "close")
    assert open_ops[0].opputfh.object == close_ops[0].opputfh.object == b"private-handle"
    assert open_ops[1].opopen.claim.claim == const.CLAIM_FH
    assert close_ops[1].opclose.open_stateid.seqid == 7
    assert close_ops[1].opclose.open_stateid.other == b"z" * 12
    packer = nfs41.nfs4client.nfs4lib.FancyNFS4Packer()
    packer.pack_COMPOUND4args(nfs41.xdr.COMPOUND4args(b"tag", 1, open_ops))
    unpacker = nfs41.nfs4client.nfs4lib.FancyNFS4Unpacker(packer.get_buffer())
    encoded = unpacker.unpack_COMPOUND4args()
    unpacker.done()
    assert encoded.argarray[-1].opopen.claim.claim == const.CLAIM_FH


def test_wire_open_failure_preserves_status_and_releases_session(monkeypatch, warm_request):
    server = FakeServer()
    server.install(monkeypatch)
    original = server.respond

    def deny_open(operations, tag):
        response = original(operations, tag)
        if server.events[-1]["phase"] != "open":
            return response
        xdr = nfs41.xdr
        return _wire_response(
            const.NFS4ERR_ACCESS,
            tag,
            [
                xdr.nfs_resop4(const.OP_PUTFH, opputfh=xdr.PUTFH4res(const.NFS4_OK)),
                xdr.nfs_resop4(const.OP_OPEN, opopen=xdr.OPEN4res(const.NFS4ERR_ACCESS)),
            ],
        )

    monkeypatch.setattr(server, "respond", deny_open)
    assert nfs41.warm(warm_request, server.emit) == {
        "status": "error",
        "bytes_read": 0,
        "reason": "open_NFS4ERR_ACCESS",
    }
    phases = [phase for phase, _, _ in server.calls]
    assert phases[-2:] == ["destroy_session", "destroy_clientid"]
    assert "read" not in phases and "close" not in phases


@pytest.mark.parametrize(("length", "expected_status"), [(4096, "completed"), (8192, "error")])
def test_compound_limit_reserves_all_cleanup_attempts(
    monkeypatch, warm_request, length, expected_status
):
    monkeypatch.setattr(nfs41, "MAX_COMPOUNDS", 12)
    server = FakeServer([(b"a" * 4096, False)])
    server.install(monkeypatch)
    result = nfs41.warm({**warm_request, "length": length}, server.emit)
    assert result == {
        "status": expected_status,
        "bytes_read": 4096,
        "reason": "complete" if expected_status == "completed" else "compound_limit_exceeded",
    }
    compounds = [event for event in server.events if event["type"] == "compound"]
    assert len(compounds) == 12
    assert [event["phase"] for event in compounds[-3:]] == [
        "close",
        "destroy_session",
        "destroy_clientid",
    ]
    assert sum(event["phase"] == "read" for event in compounds) == 1


def test_compound_limit_immediately_after_open_still_closes(monkeypatch, warm_request):
    monkeypatch.setattr(nfs41, "MAX_COMPOUNDS", 11)
    server = FakeServer()
    server.install(monkeypatch)
    assert nfs41.warm(warm_request, server.emit) == {
        "status": "error",
        "bytes_read": 0,
        "reason": "compound_limit_exceeded",
    }
    phases = [event["phase"] for event in server.events if event["type"] == "compound"]
    assert len(phases) == 11
    assert phases[-4:] == ["open", "close", "destroy_session", "destroy_clientid"]


@pytest.mark.parametrize(
    ("limit", "expected_final_phases"),
    [(4, ["exchange_id", "destroy_clientid"]), (8, ["destroy_session", "destroy_clientid"])],
)
def test_compound_limit_during_setup_or_path_walk(
    monkeypatch, warm_request, limit, expected_final_phases
):
    monkeypatch.setattr(nfs41, "MAX_COMPOUNDS", limit)
    server = FakeServer()
    server.install(monkeypatch)
    result = nfs41.warm(warm_request, server.emit)
    assert result == {"status": "error", "bytes_read": 0, "reason": "compound_limit_exceeded"}
    compounds = [event for event in server.events if event["type"] == "compound"]
    assert len(compounds) <= limit
    assert [event["phase"] for event in compounds[-len(expected_final_phases) :]] == (
        expected_final_phases
    )
    assert not any(event["phase"] in ("read", "open", "close") for event in compounds)


def test_compound_limit_remains_primary_if_cleanup_fails(monkeypatch, warm_request):
    monkeypatch.setattr(nfs41, "MAX_COMPOUNDS", 12)
    server = FakeServer([(b"a" * 4096, False)], failure="close")
    server.install(monkeypatch)
    result = nfs41.warm({**warm_request, "length": 8192}, server.emit)
    assert result == {
        "status": "error",
        "bytes_read": 4096,
        "reason": "compound_limit_exceeded:cleanup:close_NFS4ERR_ACCESS",
    }
    assert [phase for phase, _, _ in server.calls][-3:] == [
        "close",
        "destroy_session",
        "destroy_clientid",
    ]


def test_transport_error_is_redacted_and_does_not_issue_cleanup(monkeypatch, warm_request):
    server = FakeServer()
    server.install(monkeypatch)
    original = server.respond

    def disconnect(operations, tag):
        if server.events[-1]["phase"] == "read":
            raise ConnectionResetError(errno.ECONNRESET, "sensitive path and host")
        return original(operations, tag)

    monkeypatch.setattr(server, "respond", disconnect)
    result = nfs41.warm(warm_request, server.emit)
    assert result == {
        "status": "error",
        "bytes_read": 0,
        "reason": f"transport_error:ConnectionResetError:errno:{errno.ECONNRESET}",
    }
    assert server.events[-1]["phase"] == "read"
    assert server.stopped


def test_transport_error_after_progress_preserves_acknowledged_bytes(monkeypatch, warm_request):
    server = FakeServer([(b"a" * 4096, False)])
    server.install(monkeypatch)
    original = server.respond

    def disconnect(operations, tag):
        if sum(phase == "read" for phase, _, _ in server.calls) == 1:
            raise TimeoutError(errno.ETIMEDOUT, "private error details")
        return original(operations, tag)

    monkeypatch.setattr(server, "respond", disconnect)
    result = nfs41.warm(warm_request, server.emit)
    assert result == {
        "status": "error",
        "bytes_read": 4096,
        "reason": f"transport_error:TimeoutError:errno:{errno.ETIMEDOUT}",
    }
    assert [e["bytes_read"] for e in server.events if e["type"] == "progress"] == [4096]
    assert server.events[-1]["phase"] == "read"
    assert not any(e["type"] == "read_rejected" for e in server.events)


def test_protocol_failure_after_progress_preserves_acknowledged_bytes(monkeypatch, warm_request):
    server = FakeServer([(b"a" * 4096, False), (b"b" * 4096, False)])
    server.install(monkeypatch)
    original = server.respond

    def malformed(operations, tag):
        response = original(operations, tag)
        if sum(phase == "read" for phase, _, _ in server.calls) == 2:
            response.resarray[-1].resop = const.OP_WRITE
        return response

    monkeypatch.setattr(server, "respond", malformed)
    result = nfs41.warm(warm_request, server.emit)
    assert result == {"status": "error", "bytes_read": 4096, "reason": "read_invalid_response"}
    assert [e["bytes_read"] for e in server.events if e["type"] == "progress"] == [4096]
    assert server.events[-1]["phase"] == "read"


@pytest.mark.parametrize("after_progress", [False, True])
def test_server_exceeding_count_is_not_acknowledged(monkeypatch, warm_request, after_progress):
    reads = [(b"a" * 4096, False)] if after_progress else []
    reads.append((b"b" * 4097, False))
    server = FakeServer(reads)
    server.install(monkeypatch)
    result = nfs41.warm(warm_request, server.emit)
    acknowledged = 4096 if after_progress else 0
    assert result == {
        "status": "error",
        "bytes_read": acknowledged,
        "reason": "read_invalid_count",
    }
    assert [e["bytes_read"] for e in server.events if e["type"] == "progress"] == (
        [4096] if after_progress else []
    )
    assert server.events[-1]["phase"] == "read"
    assert not any(e["type"] == "read_rejected" for e in server.events)


@pytest.mark.parametrize("phase", ["close", "destroy_session", "destroy_clientid"])
def test_cleanup_failure_after_full_read_is_error(monkeypatch, warm_request, phase):
    server = FakeServer(
        [(b"a" * 4096, False), (b"b" * 4096, False), (b"c" * 808, False)],
        failure=phase,
    )
    server.install(monkeypatch)
    assert nfs41.warm(warm_request, server.emit) == {
        "status": "error",
        "bytes_read": 9000,
        "reason": f"{phase}_NFS4ERR_ACCESS",
    }
    assert [e["bytes_read"] for e in server.events if e["type"] == "progress"] == [
        4096,
        8192,
        9000,
    ]


def test_cleanup_transport_failure_is_actionable_without_private_message(monkeypatch, warm_request):
    server = FakeServer([(b"a" * 4096, False)])
    server.install(monkeypatch)
    original = server.respond

    def disconnect(operations, tag):
        if server.events[-1]["phase"] == "close":
            raise ConnectionResetError(errno.ECONNRESET, "private error details")
        return original(operations, tag)

    monkeypatch.setattr(server, "respond", disconnect)
    result = nfs41.warm({**warm_request, "length": 4096}, server.emit)
    assert result == {
        "status": "error",
        "bytes_read": 4096,
        "reason": f"cleanup_transport_error:ConnectionResetError:errno:{errno.ECONNRESET}",
    }


def test_connection_error_class_and_errno_are_sanitized(monkeypatch, warm_request):
    server = FakeServer()
    server.connection_error = ConnectionRefusedError(errno.ECONNREFUSED, "private error details")
    server.install(monkeypatch)
    assert nfs41.warm(warm_request, server.emit) == {
        "status": "error",
        "bytes_read": 0,
        "reason": f"connection_error:ConnectionRefusedError:errno:{errno.ECONNREFUSED}",
    }
    assert server.stopped
    assert server.events == []


def test_pynfs_transport_override_preserves_connect_errno_without_network(monkeypatch):
    class RefusedSocket:
        closed = False

        def setsockopt(self, *args):
            pass

        def connect(self, address):
            raise ConnectionRefusedError(errno.ECONNREFUSED, "private endpoint")

        def close(self):
            self.closed = True

    connection = RefusedSocket()
    monkeypatch.setattr(nfs41.socket, "getaddrinfo", lambda *args: [(2, 1, 0, "", ("ignored", 0))])
    monkeypatch.setattr(nfs41.socket, "socket", lambda *args: connection)
    with pytest.raises(ConnectionRefusedError) as failure:
        nfs41._connect(object(), ("192.0.2.1", 2049))
    assert connection.closed
    assert failure.value.errno == errno.ECONNREFUSED
    assert nfs41._error_reason("connection_error", failure.value) == (
        f"connection_error:ConnectionRefusedError:errno:{errno.ECONNREFUSED}"
    )


def test_pynfs_transport_override_registers_connected_socket(monkeypatch):
    class ConnectedSocket:
        def setsockopt(self, *args):
            pass

        def connect(self, address):
            self.address = address

        def setblocking(self, value):
            self.blocking = value

    connection = ConnectedSocket()
    monkeypatch.setattr(
        nfs41.socket, "getaddrinfo", lambda *args: [(2, 1, 0, "", ("numeric", 2049))]
    )
    monkeypatch.setattr(nfs41.socket, "socket", lambda *args: connection)
    monkeypatch.setattr(nfs41.rpc, "RpcPipe", lambda sock, alarm: (sock, alarm))
    ready = SimpleNamespace(wait=lambda: None)
    monkeypatch.setattr(nfs41.rpc, "DeferredData", lambda: ready)
    alarms = []
    alarm = SimpleNamespace(buzz=lambda *args: alarms.append(args))
    pipe = nfs41._connect(SimpleNamespace(_alarm=alarm), ("192.0.2.1", 2049))
    assert pipe == (connection, alarm)
    assert connection.address == ("numeric", 2049)
    assert connection.blocking is False
    assert alarms == [(b"\x01", (pipe, ready))]


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (nfs41.rpc.RPCTimeout("private"), "transport_error:RPCTimeout"),
        (nfs41.security.SecError("private"), "transport_error:SecError"),
        (EOFError("private"), "transport_error:EOFError"),
        (OSError("private"), "transport_error:OSError"),
    ],
)
def test_error_reason_uses_safe_fixed_labels(error, expected):
    assert nfs41._error_reason("transport_error", error) == expected


@pytest.mark.parametrize(
    ("reads", "length", "acknowledged"),
    [
        ([(b"a" * 100, True)], 9000, 100),
        ([(b"a" * 4096, False)], 4096, 4096),
    ],
)
def test_stop_failure_is_error_and_retains_progress(
    monkeypatch, warm_request, reads, length, acknowledged
):
    server = FakeServer(reads)
    server.stop_error = RuntimeError("sensitive exception details")
    server.install(monkeypatch)
    result = nfs41.warm({**warm_request, "length": length}, server.emit)
    assert result == {
        "status": "error",
        "bytes_read": acknowledged,
        "reason": "stop_error:RuntimeError",
    }
    assert server.stopped
    assert [e["bytes_read"] for e in server.events if e["type"] == "progress"][-1] == acknowledged


def test_stop_failure_preserves_prior_failure_reason(monkeypatch, warm_request):
    server = FakeServer(failure="open")
    server.stop_error = RuntimeError("private details")
    server.install(monkeypatch)
    assert nfs41.warm(warm_request, server.emit) == {
        "status": "error",
        "bytes_read": 0,
        "reason": "open_NFS4ERR_ACCESS:stop_error:RuntimeError",
    }


def test_long_primary_cleanup_and_stop_reasons_stay_bounded_and_sanitized(
    monkeypatch, warm_request
):
    server = FakeServer([(b"a" * 4096, False), (b"b" * 4096, False)])
    server.stop_error = RuntimeError("private stop details")
    server.install(monkeypatch)
    original = server.respond

    def fail_read_and_cleanup(operations, tag):
        if server.events[-1]["phase"] == "close":
            raise ConnectionAbortedError(2**31 - 1, "private cleanup details")
        response = original(operations, tag)
        if sum(phase == "read" for phase, _, _ in server.calls) == 2:
            response.status = const.NFS4ERR_CONN_NOT_BOUND_TO_SESSION
        return response

    monkeypatch.setattr(server, "respond", fail_read_and_cleanup)
    result = nfs41.warm(warm_request, server.emit)
    assert result["status"] == "error"
    assert result["bytes_read"] == 4096
    assert result["reason"].startswith("read_NFS4ERR_CONN_NOT_BOUND_TO_SESSION:cleanup:")
    assert "cleanup_transport_error" in result["reason"]
    assert result["reason"].endswith(":stop_error")
    assert re.fullmatch(r"[a-zA-Z0-9_.:-]{1,96}", result["reason"])
    assert "private" not in repr(result)


def test_zero_length_never_connects(monkeypatch, warm_request):
    server = FakeServer()
    server.install(monkeypatch)
    assert nfs41.warm({**warm_request, "length": 0}, server.emit) == {
        "status": "completed",
        "bytes_read": 0,
        "reason": "complete",
    }
    assert not server.events and not server.calls


def test_negotiated_response_size_caps_read(monkeypatch, warm_request):
    server = FakeServer([(b"a" * 864, False)], maxresponsesize=1000)
    server.install(monkeypatch)
    result = nfs41.warm({**warm_request, "length": 864}, server.emit)
    assert result["status"] == "completed"
    assert next(e for e in server.events if e.get("phase") == "read")["count"] == 864


def test_shared_read_limit_caps_each_request(monkeypatch, warm_request):
    assert nfs41.READ_BYTES == limits.READ_BYTES
    monkeypatch.setattr(nfs41, "READ_BYTES", 256)
    server = FakeServer([(b"a" * 256, False), (b"b" * 44, False)])
    server.install(monkeypatch)
    result = nfs41.warm({**warm_request, "length": 300}, server.emit)
    assert result == {"status": "completed", "bytes_read": 300, "reason": "complete"}
    assert [e["count"] for e in server.events if e.get("phase") == "read"] == [256, 44]


@pytest.mark.parametrize("handle_length", [1, 128])
def test_handle_mode_tags_wire_handle_and_no_lookup(
    monkeypatch, handle_request, handle_length, caplog
):
    handle = bytes(range(handle_length))
    server = FakeServer([(b"a" * 4096, False), (b"b" * 4096, False), (b"c" * 808, True)])
    server.install(monkeypatch)
    with caplog.at_level(logging.INFO):
        result = nfs41.warm({**handle_request, "file_handle": handle}, server.emit)
    assert result == {"status": "completed", "bytes_read": 9000, "reason": "complete"}
    compounds = [event for event in server.events if event["type"] == "compound"]
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
    pattern = rf"pc1:w:{handle_request['plan_ref']}:{handle_request['task_ref']}:[0-9a-f]{{32}}"
    assert all(
        len(event["tag"]) == 104 and re.fullmatch(pattern, event["tag"]) for event in compounds
    )
    assert len({event["tag"][-32:] for event in compounds}) == len(compounds)
    for phase, operations, tag in server.calls:
        assert all(
            op.argop not in (const.OP_PUTROOTFH, const.OP_LOOKUP, const.OP_GETFH)
            for op in operations
        )
        if phase in ("getattr", "open", "read", "close"):
            assert operations[0].opputfh.object == handle
            packer = nfs41.nfs4client.nfs4lib.FancyNFS4Packer()
            packer.pack_COMPOUND4args(nfs41.xdr.COMPOUND4args(tag, 1, operations))
            unpacker = nfs41.nfs4client.nfs4lib.FancyNFS4Unpacker(packer.get_buffer())
            encoded = unpacker.unpack_COMPOUND4args()
            unpacker.done()
            assert encoded.tag == tag
            assert encoded.argarray[0].opputfh.object == handle
            if phase == "getattr":
                assert encoded.argarray[1].opgetattr.attr_request == (
                    (1 << const.FATTR4_TYPE) | (1 << const.FATTR4_SIZE)
                )
            if phase == "open":
                assert encoded.argarray[1].opopen.claim.claim == const.CLAIM_FH
            if phase == "read":
                event = next(event for event in compounds if event["tag"].encode() == tag)
                assert encoded.argarray[1].opread.offset == event["offset"]
                assert encoded.argarray[1].opread.count == event["count"]
    reads = [(event["offset"], event["count"]) for event in compounds if event["phase"] == "read"]
    assert reads == [
        (17, 4096),
        (4113, 4096),
        (8209, 808),
    ]
    public = repr(server.events) + repr(result) + caplog.text
    assert repr(handle) not in public
    assert handle_request["host"] not in public
    for secret in (
        "opaque-private-volume",
        "opaque-private-export",
        "opaque-private-file",
        "opaque-private-plan",
        "opaque-private-task",
    ):
        assert secret not in public


@pytest.mark.parametrize(
    "phase",
    [
        "exchange_id",
        "create_session",
        "reclaim_complete",
        "getattr",
        "open",
        "read",
        "close",
        "destroy_session",
        "destroy_clientid",
    ],
)
def test_handle_mode_rejects_mismatched_echo(monkeypatch, handle_request, phase):
    server = FakeServer([(b"a" * 4096, True)], mismatch=phase)
    server.install(monkeypatch)
    result = nfs41.warm(handle_request, server.emit)
    assert result["status"] == "error"
    assert result["reason"] == f"{phase}_tag_mismatch"
    assert server.calls[-1][0] == phase
    assert all(
        event["tag"].startswith(f"pc1:w:{handle_request['plan_ref']}:{handle_request['task_ref']}:")
        for event in server.events
        if event["type"] == "compound"
    )


@pytest.mark.parametrize("bad_tag", [None, "unexpected-text", b"pc1:w:truncated"])
def test_handle_mode_rejects_malformed_echo(monkeypatch, handle_request, bad_tag):
    server = FakeServer([(b"a" * 4096, False)])
    server.install(monkeypatch)
    original = server.respond

    def malformed(operations, tag):
        response = original(operations, tag)
        if server.events[-1]["phase"] == "read":
            response.tag = bad_tag
        return response

    monkeypatch.setattr(server, "respond", malformed)
    assert nfs41.warm(handle_request, server.emit) == {
        "status": "error",
        "bytes_read": 0,
        "reason": "read_tag_mismatch",
    }
    assert server.events[-1]["phase"] == "read"


def test_handle_mode_read_and_cleanup_errors_keep_structured_tags(monkeypatch, handle_request):
    server = FakeServer(failure="read")
    server.install(monkeypatch)
    original = server.respond

    def fail_close(operations, tag):
        if server.events[-1]["phase"] == "close":
            server.failure = "close"
        return original(operations, tag)

    monkeypatch.setattr(server, "respond", fail_close)
    result = nfs41.warm(handle_request, server.emit)
    assert result == {
        "status": "error",
        "bytes_read": 0,
        "reason": "read_NFS4ERR_ACCESS:cleanup:close_NFS4ERR_ACCESS",
    }
    assert [phase for phase, _, _ in server.calls][-4:] == [
        "read",
        "close",
        "destroy_session",
        "destroy_clientid",
    ]
    assert all(
        re.fullmatch(
            rf"pc1:w:{handle_request['plan_ref']}:{handle_request['task_ref']}:[0-9a-f]{{32}}",
            event["tag"],
        )
        for event in server.events
        if event["type"] == "compound"
    )


def _assert_getattr_rejected_before_open(server):
    phases = [phase for phase, _, _ in server.calls]
    assert phases[-3:] == ["getattr", "destroy_session", "destroy_clientid"]
    assert not any(phase in ("lookup", "open", "read", "close") for phase in phases)


@pytest.mark.parametrize("file_type", [const.NF4DIR, const.NF4LNK, const.NF4SOCK, const.NF4BLK])
def test_handle_getattr_rejects_nonregular_files(monkeypatch, handle_request, file_type):
    server = FakeServer()
    server.attrs = {const.FATTR4_TYPE: file_type, const.FATTR4_SIZE: 2**40}
    server.install(monkeypatch)
    assert nfs41.warm(handle_request, server.emit) == {
        "status": "error",
        "bytes_read": 0,
        "reason": "getattr_not_regular",
    }
    _assert_getattr_rejected_before_open(server)


@pytest.mark.parametrize(
    "attrs",
    [
        None,
        {},
        {const.FATTR4_TYPE: const.NF4REG},
        {const.FATTR4_SIZE: 12345},
        {const.FATTR4_TYPE: None, const.FATTR4_SIZE: 12345},
        {const.FATTR4_TYPE: True, const.FATTR4_SIZE: 12345},
        {const.FATTR4_TYPE: const.NF4REG, const.FATTR4_SIZE: "12345"},
        {const.FATTR4_TYPE: const.NF4REG, const.FATTR4_SIZE: True},
        {const.FATTR4_TYPE: const.NF4REG, const.FATTR4_SIZE: -1},
        {const.FATTR4_TYPE: const.NF4REG, const.FATTR4_SIZE: 2**64},
        {const.FATTR4_TYPE: const.NF4REG, const.FATTR4_SIZE: 12345, 99: 1},
    ],
)
def test_handle_getattr_rejects_missing_or_malformed_attrs(monkeypatch, handle_request, attrs):
    server = FakeServer()
    server.attrs = attrs
    server.install(monkeypatch)
    assert nfs41.warm(handle_request, server.emit) == {
        "status": "error",
        "bytes_read": 0,
        "reason": "getattr_invalid_attributes",
    }
    _assert_getattr_rejected_before_open(server)


@pytest.mark.parametrize(
    ("size", "status"),
    [(20, "error"), (21, "completed"), (22, "completed")],
)
def test_handle_getattr_checks_requested_range_end(monkeypatch, handle_request, size, status):
    server = FakeServer([(b"abcd", False)])
    server.attrs = {const.FATTR4_TYPE: const.NF4REG, const.FATTR4_SIZE: size}
    server.install(monkeypatch)
    result = nfs41.warm({**handle_request, "offset": 17, "length": 4}, server.emit)
    assert result["status"] == status
    if status == "error":
        assert result["reason"] == "getattr_range_exceeds_size"
        _assert_getattr_rejected_before_open(server)
    else:
        assert result["bytes_read"] == 4
        assert [phase for phase, _, _ in server.calls][-4:] == [
            "read",
            "close",
            "destroy_session",
            "destroy_clientid",
        ]


@pytest.mark.parametrize(
    "status",
    [const.NFS4ERR_BADHANDLE, const.NFS4ERR_STALE, const.NFS4ERR_FHEXPIRED, const.NFS4ERR_WRONGSEC],
)
def test_handle_getattr_wire_errors_fail_closed(monkeypatch, handle_request, status):
    server = FakeServer()
    server.install(monkeypatch)
    original = server.respond

    def fail_getattr(operations, tag):
        response = original(operations, tag)
        if server.events[-1]["phase"] != "getattr":
            return response
        xdr = nfs41.xdr
        return _wire_response(
            status,
            tag,
            [
                xdr.nfs_resop4(const.OP_PUTFH, opputfh=xdr.PUTFH4res(const.NFS4_OK)),
                xdr.nfs_resop4(const.OP_GETATTR, opgetattr=xdr.GETATTR4res(status)),
            ],
        )

    monkeypatch.setattr(server, "respond", fail_getattr)
    result = nfs41.warm(handle_request, server.emit)
    assert result == {
        "status": "error",
        "bytes_read": 0,
        "reason": f"getattr_{const.nfsstat4[status]}",
    }
    _assert_getattr_rejected_before_open(server)
    assert repr(handle_request["file_handle"]) not in repr(result) + repr(server.events)


def test_handle_getattr_decodes_real_pynfs_attribute_response(monkeypatch, handle_request):
    server = FakeServer([(b"abcd", False)])
    server.install(monkeypatch)
    original = server.respond

    def wire_getattr(operations, tag):
        response = original(operations, tag)
        if server.events[-1]["phase"] != "getattr":
            return response
        xdr = nfs41.xdr
        attrs = {const.FATTR4_TYPE: const.NF4REG, const.FATTR4_SIZE: 21}
        return _wire_response(
            const.NFS4_OK,
            tag,
            [
                xdr.nfs_resop4(const.OP_PUTFH, opputfh=xdr.PUTFH4res(const.NFS4_OK)),
                xdr.nfs_resop4(
                    const.OP_GETATTR,
                    opgetattr=xdr.GETATTR4res(const.NFS4_OK, xdr.GETATTR4resok(attrs)),
                ),
            ],
        )

    monkeypatch.setattr(server, "respond", wire_getattr)
    assert nfs41.warm({**handle_request, "offset": 17, "length": 4}, server.emit) == {
        "status": "completed",
        "bytes_read": 4,
        "reason": "complete",
    }
    assert [phase for phase, _, _ in server.calls][-6:] == [
        "getattr",
        "open",
        "read",
        "close",
        "destroy_session",
        "destroy_clientid",
    ]


def test_handle_getattr_truncated_xdr_never_opens_or_reads(monkeypatch, handle_request):
    server = FakeServer()
    server.install(monkeypatch)
    original = server.respond

    def truncated_attrs(operations, tag):
        response = original(operations, tag)
        if server.events[-1]["phase"] != "getattr":
            return response
        xdr = nfs41.xdr
        attrs = xdr.fattr4((1 << const.FATTR4_TYPE) | (1 << const.FATTR4_SIZE), b"\x00")
        return _wire_response(
            const.NFS4_OK,
            tag,
            [
                xdr.nfs_resop4(const.OP_PUTFH, opputfh=xdr.PUTFH4res(const.NFS4_OK)),
                xdr.nfs_resop4(
                    const.OP_GETATTR,
                    opgetattr=xdr.GETATTR4res(const.NFS4_OK, xdr.GETATTR4resok(attrs)),
                ),
            ],
        )

    monkeypatch.setattr(server, "respond", truncated_attrs)
    assert nfs41.warm(handle_request, server.emit) == {
        "status": "error",
        "bytes_read": 0,
        "reason": "transport_error:EOFError",
    }
    assert server.calls[-1][0] == "getattr"
    assert not any(phase in ("open", "read") for phase, _, _ in server.calls)


def test_stale_wire_handle_is_reported_without_echoing_it(monkeypatch, handle_request):
    server = FakeServer()
    server.install(monkeypatch)
    original = server.respond

    def stale_putfh(operations, tag):
        response = original(operations, tag)
        if server.events[-1]["phase"] != "open":
            return response
        xdr = nfs41.xdr
        return _wire_response(
            const.NFS4ERR_STALE,
            tag,
            [xdr.nfs_resop4(const.OP_PUTFH, opputfh=xdr.PUTFH4res(const.NFS4ERR_STALE))],
        )

    monkeypatch.setattr(server, "respond", stale_putfh)
    result = nfs41.warm(handle_request, server.emit)
    assert result == {"status": "error", "bytes_read": 0, "reason": "open_NFS4ERR_STALE"}
    assert [phase for phase, _, _ in server.calls][-3:] == [
        "open",
        "destroy_session",
        "destroy_clientid",
    ]
    assert repr(handle_request["file_handle"]) not in repr(result) + repr(server.events)


def test_handle_mode_compound_limit_reserves_cleanup(monkeypatch, handle_request):
    monkeypatch.setattr(nfs41, "MAX_COMPOUNDS", 9)
    server = FakeServer([(b"a" * 4096, False)])
    server.install(monkeypatch)
    result = nfs41.warm(handle_request, server.emit)
    assert result == {"status": "error", "bytes_read": 4096, "reason": "compound_limit_exceeded"}
    compounds = [event for event in server.events if event["type"] == "compound"]
    assert len(compounds) == 9
    assert [event["phase"] for event in compounds[-3:]] == [
        "close",
        "destroy_session",
        "destroy_clientid",
    ]


@pytest.mark.parametrize(
    "changes",
    [
        {"file_handle": b""},
        {"file_handle": b"x" * 129},
        {"file_handle": "not-bytes"},
        {"plan_ref": "A" * 32},
        {"task_ref": "b" * 31},
        {"plan_ref": "a" * 31 + ":"},
        {"task_ref": None},
    ],
)
def test_invalid_handle_request_fails_before_connection(monkeypatch, handle_request, changes):
    server = FakeServer()
    server.install(monkeypatch)
    result = nfs41.warm({**handle_request, **changes}, server.emit)
    assert result == {"status": "error", "bytes_read": 0, "reason": "invalid_handle_request"}
    assert not hasattr(server, "client_args")
    assert not server.events


@pytest.mark.parametrize(
    ("maxresponse", "length", "expected_status", "expected_count"),
    [
        (208, 4, "error", None),
        (211, 4, "error", None),
        (212, 4, "error", None),
        (247, 4, "error", None),
        (248, 4, "completed", 4),
        (1000, 792, "completed", 792),
    ],
)
def test_structured_tag_negotiated_response_bounds(
    monkeypatch, handle_request, maxresponse, length, expected_status, expected_count
):
    server = FakeServer([(b"a" * length, False)], maxresponsesize=maxresponse)
    server.maxoperations = 3
    server.install(monkeypatch)
    result = nfs41.warm({**handle_request, "length": length}, server.emit)
    assert result["status"] == expected_status
    reads = [event for event in server.events if event.get("phase") == "read"]
    if expected_count is None:
        assert not reads
        assert result["reason"] == "session_response_limit"
    else:
        assert [event["count"] for event in reads] == [expected_count]


@pytest.mark.parametrize("tag_length", [0, 1, 3, 4, 32, 33, 104])
def test_response_overhead_includes_actual_xdr_tag_padding(tag_length):
    xdr = nfs41.xdr
    sequence = xdr.SEQUENCE4res(const.NFS4_OK, xdr.SEQUENCE4resok(b"s" * 16, 1, 0, 0, 0, 0))
    response = xdr.COMPOUND4res(
        const.NFS4_OK,
        b"t" * tag_length,
        [
            xdr.nfs_resop4(const.OP_SEQUENCE, opsequence=sequence),
            xdr.nfs_resop4(const.OP_PUTFH, opputfh=xdr.PUTFH4res(const.NFS4_OK)),
            xdr.nfs_resop4(
                const.OP_READ,
                opread=xdr.READ4res(const.NFS4_OK, xdr.READ4resok(False, b"x" * 40)),
            ),
        ],
    )
    packer = nfs41.nfs4client.nfs4lib.FancyNFS4Packer()
    packer.pack_COMPOUND4res(response)
    assert len(packer.get_buffer()) + 24 - 40 == nfs41._read_response_overhead(tag_length)


def test_minimum_handle_getattr_and_open_responses_fit_negotiated_limit():
    xdr = nfs41.xdr
    sequence = xdr.nfs_resop4(
        const.OP_SEQUENCE,
        opsequence=xdr.SEQUENCE4res(const.NFS4_OK, xdr.SEQUENCE4resok(b"s" * 16, 1, 0, 0, 0, 0)),
    )
    putfh = xdr.nfs_resop4(const.OP_PUTFH, opputfh=xdr.PUTFH4res(const.NFS4_OK))
    attrs = {const.FATTR4_TYPE: const.NF4REG, const.FATTR4_SIZE: 21}
    getattr_op = xdr.nfs_resop4(
        const.OP_GETATTR,
        opgetattr=xdr.GETATTR4res(const.NFS4_OK, xdr.GETATTR4resok(attrs)),
    )
    open_result = xdr.OPEN4resok(
        xdr.stateid4(7, b"o" * 12),
        xdr.change_info4(True, 1, 1),
        0,
        0,
        xdr.open_delegation4(const.OPEN_DELEGATE_NONE),
    )
    open_op = xdr.nfs_resop4(const.OP_OPEN, opopen=xdr.OPEN4res(const.NFS4_OK, open_result))
    sizes = []
    for operation in (getattr_op, open_op):
        packer = nfs41.nfs4client.nfs4lib.FancyNFS4Packer()
        packer.pack_COMPOUND4res(
            xdr.COMPOUND4res(const.NFS4_OK, b"a" * 104, [sequence, putfh, operation])
        )
        sizes.append(len(packer.get_buffer()) + 24)
    assert sizes == [224, 248]
    assert sizes[1] == nfs41._open_response_overhead(104)


def test_handle_mode_direct_run_uses_structured_tags(monkeypatch, handle_request):
    server = FakeServer([(b"a" * 4, False)])
    server.install(monkeypatch)
    client = nfs41.nfs4client.NFS4Client.__new__(nfs41.nfs4client.NFS4Client)
    client.handle_1 = nfs41._reject_callback
    client.__init__(host=handle_request["host"], port=2049, minorversion=1)
    result = nfs41._run(client, {**handle_request, "length": 4}, server.emit)
    assert result == {"status": "completed", "bytes_read": 4, "reason": "complete"}
    assert all(
        event["tag"].startswith(f"pc1:w:{handle_request['plan_ref']}:{handle_request['task_ref']}:")
        for event in server.events
        if event["type"] == "compound"
    )


def test_import_does_not_print_optional_gss_notice():
    completed = subprocess.run(
        [sys.executable, "-c", "import predictive_cache.warmer.nfs41"],
        capture_output=True,
        text=True,
        env=os.environ.copy(),
        check=True,
    )
    assert completed.stdout == ""
    assert completed.stderr == ""


def test_real_pynfs_encodes_supplied_tag_and_read_without_network():
    client = nfs41.nfs4client.NFS4Client.__new__(nfs41.nfs4client.NFS4Client)
    client.c1 = object()
    client.minorversion = 1
    client.default_cred = nfs41.security.AuthSys().init_cred(uid=1, gid=2, gids=[])
    captured = []
    client.send_call = lambda *args: captured.append(args) or 42
    tag = b"0123456789abcdef0123456789abcdef"
    stateid = nfs41.xdr.stateid4(1, b"x" * 12)
    assert client.compound_async([ops.read(stateid, 123, 4096)], tag=tag, version=1) == 42
    unpacker = nfs41.nfs4client.nfs4lib.FancyNFS4Unpacker(captured[0][2])
    compound = unpacker.unpack_COMPOUND4args()
    unpacker.done()
    assert compound.tag == tag
    assert compound.minorversion == 1
    assert len(compound.argarray) == 1
    assert compound.argarray[0].opread.offset == 123
    assert compound.argarray[0].opread.count == 4096


def test_real_client_record_and_session_setup_without_network(monkeypatch, warm_request):
    attrs = nfs41.xdr.channel_attrs4(0, 8192, 8192, 8192, 128, 1, [])
    exchange = SimpleNamespace(
        resop=const.OP_EXCHANGE_ID,
        eir_clientid=123,
        eir_sequenceid=1,
        eir_flags=0,
        eir_state_protect=SimpleNamespace(spr_how=const.SP4_NONE),
    )
    created = SimpleNamespace(
        resop=const.OP_CREATE_SESSION,
        csr_sessionid=b"s" * 16,
        csr_sequence=1,
        csr_fore_chan_attrs=attrs,
        csr_back_chan_attrs=attrs,
    )
    events = []

    class Client:
        impl_id = nfs41.xdr.nfs_impl_id4(b"domain", b"client", nfs41.xdr.nfstime4(0, 0))
        prog = 0x40000000

        def __init__(self):
            self.sessions = {}
            self.results = iter((exchange, created))

        def set_cred(self, cred):
            self.cred = cred

        def compound_async(self, operations, *, tag, version):
            assert tag.decode() == events[-1]["tag"]
            assert version == 1
            self.response = SimpleNamespace(status=0, tag=tag, resarray=[next(self.results)])
            return 1

        def listen(self, xid):
            return self.response

    client = Client()
    monkeypatch.setattr(
        nfs41,
        "_client_compound",
        lambda target, operations, tag, phase: target.listen(
            target.compound_async(operations, tag=tag, version=1)
        ),
    )
    lifecycle = {}
    record, session = nfs41._setup(client, warm_request, events.append, lifecycle)
    assert isinstance(record, nfs41.nfs4client.ClientRecord)
    assert isinstance(session, nfs41.nfs4client.SessionRecord)
    assert client.sessions[session.sessionid] is session
    assert session.cred is client.cred
    assert lifecycle == {"record": record, "session": session}
    assert [event["phase"] for event in events] == ["exchange_id", "create_session"]


@pytest.mark.parametrize("tag_prefix", ["", f"pc1:w:{'a' * 32}:{'b' * 32}:"])
def test_real_session_sends_one_tagged_read_without_retry(tag_prefix):
    session, captured = _wire_read_session(lambda args: _failed_read_reply(args, "read"))
    events = []
    stateid = nfs41.xdr.stateid4(1, b"x" * 12)
    with pytest.raises(nfs41._Failure) as failure:
        nfs41._compound(
            session,
            [ops.putfh(b"private"), ops.read(stateid, 123, 4096)],
            "read",
            events.append,
            tag_prefix=tag_prefix,
            offset=123,
            count=4096,
        )
    assert failure.value.reason == "read_NFS4ERR_ACCESS"
    assert events[1] == {"type": "read_rejected"}
    assert len(captured) == 1
    unpacker = nfs41.nfs4client.nfs4lib.FancyNFS4Unpacker(captured[0][2])
    compound = unpacker.unpack_COMPOUND4args()
    unpacker.done()
    assert compound.tag.decode() == events[0]["tag"]
    assert len(compound.tag) == (104 if tag_prefix else 32)
    assert compound.tag.decode().startswith(tag_prefix)
    assert compound.minorversion == 1
    assert [op.argop for op in compound.argarray] == [
        const.OP_SEQUENCE,
        const.OP_PUTFH,
        const.OP_READ,
    ]
    assert compound.argarray[2].opread.count == events[0]["count"]
    assert compound.argarray[2].opread.offset == events[0]["offset"]


def _wire_read_session(reply):
    client = nfs41.nfs4client.NFS4Client.__new__(nfs41.nfs4client.NFS4Client)
    client.minorversion = 1
    client.summary = None
    client.default_cred = nfs41.security.AuthSys().init_cred(uid=1, gid=2, gids=[])
    captured = []
    client.send_call = lambda *args: captured.append(args) or 42

    class Pipe:
        def listen(self, xid, timeout):
            assert xid == 42
            unpacker = nfs41.nfs4client.nfs4lib.FancyNFS4Unpacker(captured[-1][2])
            args = unpacker.unpack_COMPOUND4args()
            unpacker.done()
            return None, reply(args)

    client.c1 = Pipe()
    attrs = nfs41.xdr.channel_attrs4(0, 8192, 8192, 8192, 128, 1, [])
    csr = SimpleNamespace(
        csr_sessionid=b"s" * 16,
        csr_sequence=1,
        csr_fore_chan_attrs=attrs,
        csr_back_chan_attrs=attrs,
    )
    session = nfs41.nfs4client.SessionRecord(
        csr, SimpleNamespace(c=client, cred=client.default_cred)
    )
    return session, captured


def _failed_read_reply(args, shape, *, tag=None, status=const.NFS4ERR_ACCESS):
    xdr = nfs41.xdr
    sequence = args.argarray[0].opsequence
    seq_status = status if shape in ("sequence", "prior_sequence_error") else const.NFS4_OK
    seq_result = xdr.SEQUENCE4resok(
        b"z" * 16 if shape == "wrong_session" else sequence.sa_sessionid,
        sequence.sa_sequenceid + (shape == "wrong_sequence"),
        sequence.sa_slotid + (shape == "wrong_slot"),
        0,
        0,
        0,
    )
    results = [
        xdr.nfs_resop4(
            const.OP_SEQUENCE,
            opsequence=xdr.SEQUENCE4res(
                seq_status, seq_result if seq_status == const.NFS4_OK else None
            ),
        )
    ]
    putfh_status = (
        const.NFS4ERR_STALE
        if shape == "wrong_last_status"
        else status
        if shape in ("putfh", "prior_putfh_error")
        else const.NFS4_OK
    )
    putfh = xdr.nfs_resop4(const.OP_PUTFH, opputfh=xdr.PUTFH4res(putfh_status))
    read = xdr.nfs_resop4(const.OP_READ, opread=xdr.READ4res(status))
    if shape == "read_with_data":
        read = xdr.nfs_resop4(
            const.OP_READ, opread=xdr.READ4res(const.NFS4_OK, xdr.READ4resok(False, b"secret"))
        )
    if shape not in ("sequence", "missing_result"):
        if shape != "wrong_order":
            results.append(putfh)
        if shape not in ("putfh", "wrong_last_status"):
            results.append(read)
    packer = nfs41.nfs4client.nfs4lib.FancyNFS4Packer()
    packer.pack_COMPOUND4res(xdr.COMPOUND4res(status, args.tag if tag is None else tag, results))
    return packer.get_buffer()


def _successful_read_reply(args, shape, data):
    xdr = nfs41.xdr
    sequence = args.argarray[0].opsequence
    seq_result = xdr.SEQUENCE4resok(
        b"z" * 16 if shape == "wrong_session" else sequence.sa_sessionid,
        sequence.sa_sequenceid + (shape == "wrong_sequence"),
        sequence.sa_slotid + (shape == "wrong_slot"),
        0,
        0,
        0,
    )
    seq_status = const.NFS4ERR_ACCESS if shape == "failed_sequence" else const.NFS4_OK
    putfh_status = const.NFS4ERR_ACCESS if shape == "failed_putfh" else const.NFS4_OK
    read_status = const.NFS4ERR_ACCESS if shape == "failed_read" else const.NFS4_OK
    results = [
        xdr.nfs_resop4(
            const.OP_SEQUENCE,
            opsequence=xdr.SEQUENCE4res(
                seq_status, seq_result if seq_status == const.NFS4_OK else None
            ),
        ),
        xdr.nfs_resop4(const.OP_PUTFH, opputfh=xdr.PUTFH4res(putfh_status)),
        xdr.nfs_resop4(
            const.OP_READ,
            opread=xdr.READ4res(
                read_status, xdr.READ4resok(False, data) if read_status == const.NFS4_OK else None
            ),
        ),
    ]
    packer = nfs41.nfs4client.nfs4lib.FancyNFS4Packer()
    packer.pack_COMPOUND4res(xdr.COMPOUND4res(const.NFS4_OK, args.tag, results))
    return packer.get_buffer()


def _session_reply(args, bad_phase=None, mutation=None):
    xdr = nfs41.xdr
    phase_ops = {
        const.OP_RECLAIM_COMPLETE: "reclaim_complete",
        const.OP_GETATTR: "getattr",
        const.OP_OPEN: "open",
        const.OP_READ: "read",
        const.OP_CLOSE: "close",
        const.OP_DESTROY_SESSION: "destroy_session",
    }
    phase = next((phase_ops[op.argop] for op in args.argarray if op.argop in phase_ops), "lookup")
    corrupt = phase == bad_phase
    seq = args.argarray[0].opsequence
    session_id = b"z" * 16 if corrupt and mutation == "session" else seq.sa_sessionid
    seq_id = seq.sa_sequenceid + (corrupt and mutation == "sequence")
    slot_id = seq.sa_slotid + (corrupt and mutation == "slot")
    seq_ok = xdr.SEQUENCE4resok(session_id, seq_id, slot_id, 0, 0, 0)
    results = [
        xdr.nfs_resop4(const.OP_SEQUENCE, opsequence=xdr.SEQUENCE4res(const.NFS4_OK, seq_ok))
    ]
    simple = {
        const.OP_RECLAIM_COMPLETE: ("opreclaim_complete", xdr.RECLAIM_COMPLETE4res),
        const.OP_PUTROOTFH: ("opputrootfh", xdr.PUTROOTFH4res),
        const.OP_PUTFH: ("opputfh", xdr.PUTFH4res),
        const.OP_LOOKUP: ("oplookup", xdr.LOOKUP4res),
        const.OP_DESTROY_SESSION: ("opdestroy_session", xdr.DESTROY_SESSION4res),
    }
    for op in args.argarray[1:]:
        code = op.argop
        failed = corrupt and (
            (mutation in ("putfh_error", "error_followed_by_success") and code == const.OP_PUTFH)
            or (mutation == "operation_error" and phase_ops.get(code) == phase)
            or (mutation == "error_status_mismatch" and code == const.OP_GETATTR)
        )
        status = (
            const.NFS4ERR_STALE
            if failed and mutation == "error_status_mismatch"
            else const.NFS4ERR_ACCESS
            if failed
            else const.NFS4_OK
        )
        if code in simple:
            name, result_type = simple[code]
            result = xdr.nfs_resop4(code, **{name: result_type(status)})
        elif code == const.OP_GETFH:
            result = xdr.nfs_resop4(code, opgetfh=xdr.GETFH4res(status, xdr.GETFH4resok(b"handle")))
        elif code == const.OP_GETATTR:
            attrs = {const.FATTR4_TYPE: const.NF4REG, const.FATTR4_SIZE: 2**40}
            result = xdr.nfs_resop4(
                code,
                opgetattr=xdr.GETATTR4res(
                    status, xdr.GETATTR4resok(attrs) if status == const.NFS4_OK else None
                ),
            )
        elif code == const.OP_OPEN:
            state = xdr.stateid4(1, b"x" * 12)
            opened = xdr.OPEN4resok(
                state,
                xdr.change_info4(True, 1, 1),
                0,
                0,
                xdr.open_delegation4(const.OPEN_DELEGATE_NONE),
            )
            result = xdr.nfs_resop4(
                code, opopen=xdr.OPEN4res(status, opened if status == const.NFS4_OK else None)
            )
        elif code == const.OP_READ:
            result = xdr.nfs_resop4(
                code,
                opread=xdr.READ4res(
                    status,
                    xdr.READ4resok(False, b"a" * op.opread.count)
                    if status == const.NFS4_OK
                    else None,
                ),
            )
        elif code == const.OP_CLOSE:
            result = xdr.nfs_resop4(
                code,
                opclose=xdr.CLOSE4res(
                    status, xdr.stateid4(1, b"x" * 12) if status == const.NFS4_OK else None
                ),
            )
        else:
            raise AssertionError(f"unexpected test operation: {code}")
        results.append(result)
    if corrupt and mutation == "wrong_order" and len(results) >= 3:
        results[1], results[2] = results[2], results[1]
    overall_status = (
        const.NFS4ERR_ACCESS
        if corrupt and mutation in ("error_followed_by_success", "error_status_mismatch")
        else const.NFS4_OK
    )
    packer = nfs41.nfs4client.nfs4lib.FancyNFS4Packer()
    packer.pack_COMPOUND4res(xdr.COMPOUND4res(overall_status, args.tag, results))
    data = packer.get_buffer()
    return data + b"\x00\x00\x00\x00" if corrupt and mutation == "trailing" else data


@pytest.mark.parametrize("shape", ["putfh", "read"])
def test_read_rejection_proven_by_wire_error(shape):
    session, sent = _wire_read_session(lambda args: _failed_read_reply(args, shape))
    events = []
    with pytest.raises(nfs41._Failure) as failure:
        nfs41._compound(
            session,
            [ops.putfh(b"private-handle"), ops.read(nfs41.xdr.stateid4(1, b"x" * 12), 17, 4)],
            "read",
            events.append,
            offset=17,
            count=4,
        )
    assert failure.value.reason == "read_NFS4ERR_ACCESS"
    assert events == [
        {"type": "compound", "tag": events[0]["tag"], "phase": "read", "offset": 17, "count": 4},
        {"type": "read_rejected"},
    ]
    assert len(sent) == 1
    assert "private-handle" not in repr(events)


@pytest.mark.parametrize(
    ("shape", "status"),
    [
        ("sequence", const.NFS4ERR_ACCESS),
        ("sequence", const.NFS4ERR_RETRY_UNCACHED_REP),
        ("putfh", const.NFS4ERR_RETRY_UNCACHED_REP),
        ("read", const.NFS4ERR_RETRY_UNCACHED_REP),
    ],
)
def test_uncached_or_sequence_error_does_not_confirm_rejection(shape, status):
    session, sent = _wire_read_session(lambda args: _failed_read_reply(args, shape, status=status))
    events = []
    with pytest.raises(nfs41._Failure) as failure:
        nfs41._compound(
            session,
            [ops.putfh(b"private"), ops.read(nfs41.xdr.stateid4(1, b"x" * 12), 17, 4)],
            "read",
            events.append,
            offset=17,
            count=4,
        )
    assert failure.value.reason == f"read_{const.nfsstat4[status]}"
    assert [event["type"] for event in events] == ["compound"]
    assert len(sent) == 1


@pytest.mark.parametrize(
    "shape",
    [
        "missing_result",
        "wrong_order",
        "wrong_last_status",
        "prior_sequence_error",
        "prior_putfh_error",
        "wrong_session",
        "wrong_sequence",
        "wrong_slot",
        "read_with_data",
    ],
)
def test_malformed_wire_error_does_not_confirm_read_rejection(shape):
    session, sent = _wire_read_session(lambda args: _failed_read_reply(args, shape))
    events = []
    with pytest.raises(nfs41._Failure) as failure:
        nfs41._compound(
            session,
            [ops.putfh(b"private"), ops.read(nfs41.xdr.stateid4(1, b"x" * 12), 17, 4)],
            "read",
            events.append,
            offset=17,
            count=4,
        )
    assert failure.value.reason == "read_invalid_response"
    assert [event["type"] for event in events] == ["compound"]
    assert len(sent) == 1


@pytest.mark.parametrize(
    ("bad_reply", "exception"),
    [
        ("tag", nfs41._Failure),
        ("truncated", EOFError),
        ("trailing", nfs41.XDRError),
        ("timeout", TimeoutError),
    ],
)
def test_unverified_read_error_does_not_confirm_rejection(bad_reply, exception):
    def reply(args):
        if bad_reply == "timeout":
            raise TimeoutError("private")
        raw = _failed_read_reply(args, "read", tag=b"wrong" if bad_reply == "tag" else None)
        if bad_reply == "truncated":
            return raw[:-1]
        if bad_reply == "trailing":
            return raw + b"\x00\x00\x00\x00"
        return raw

    session, sent = _wire_read_session(reply)
    events = []
    with pytest.raises(exception) as failure:
        nfs41._compound(
            session,
            [ops.putfh(b"private"), ops.read(nfs41.xdr.stateid4(1, b"x" * 12), 17, 4)],
            "read",
            events.append,
            offset=17,
            count=4,
        )
    if bad_reply == "tag":
        assert failure.value.reason == "read_tag_mismatch"
    assert [event["type"] for event in events] == ["compound"]
    assert len(sent) == 1


def test_read_presend_failure_does_not_emit_rejection():
    session, sent = _wire_read_session(lambda args: _failed_read_reply(args, "read"))
    events = []

    def reject_send(event):
        events.append(event)
        raise nfs41._Failure("compound_limit_exceeded")

    with pytest.raises(nfs41._Failure) as failure:
        nfs41._compound(
            session,
            [ops.putfh(b"private"), ops.read(nfs41.xdr.stateid4(1, b"x" * 12), 17, 4)],
            "read",
            reject_send,
            offset=17,
            count=4,
        )
    assert failure.value.reason == "compound_limit_exceeded"
    assert [event["type"] for event in events] == ["compound"]
    assert not sent


@pytest.mark.parametrize("reply_kind", ["error", "trailing", "timeout"])
def test_read_restores_session_hooks_even_on_failure(reply_kind):
    def reply(args):
        if reply_kind == "timeout":
            raise TimeoutError("private")
        raw = _failed_read_reply(args, "read")
        return raw + b"\x00\x00\x00\x00" if reply_kind == "trailing" else raw

    session, _ = _wire_read_session(reply)

    def original_listen(*args, **kwargs):
        return None

    original_prepare = session._prepare_compound
    session.c.listen = original_listen
    session._prepare_compound = original_prepare
    expected_error = {
        "error": nfs41._Failure,
        "trailing": nfs41.XDRError,
        "timeout": TimeoutError,
    }[reply_kind]
    with pytest.raises(expected_error):
        nfs41._compound(
            session,
            [ops.putfh(b"private"), ops.read(nfs41.xdr.stateid4(1, b"x" * 12), 17, 4)],
            "read",
            lambda event: None,
            offset=17,
            count=4,
        )
    assert session.c.listen is original_listen
    assert session._prepare_compound is original_prepare


@pytest.mark.parametrize(
    ("phase", "mutation", "reason", "bytes_read"),
    [
        ("getattr", "putfh_error", "getattr_invalid_response", 0),
        ("getattr", "operation_error", "getattr_invalid_response", 0),
        ("getattr", "error_followed_by_success", "getattr_invalid_response", 0),
        ("getattr", "error_status_mismatch", "getattr_invalid_response", 0),
        ("getattr", "wrong_order", "getattr_invalid_response", 0),
        ("getattr", "session", "getattr_invalid_response", 0),
        ("getattr", "slot", "getattr_invalid_response", 0),
        ("getattr", "sequence", "getattr_invalid_response", 0),
        ("getattr", "trailing", "invalid_response", 0),
        ("reclaim_complete", "operation_error", "reclaim_complete_invalid_response", 0),
        ("reclaim_complete", "slot", "reclaim_complete_invalid_response", 0),
        ("open", "putfh_error", "open_invalid_response", 0),
        ("open", "operation_error", "open_invalid_response", 0),
        ("open", "session", "open_invalid_response", 0),
        ("close", "operation_error", "close_invalid_response", 4),
        ("close", "sequence", "close_invalid_response", 4),
        ("destroy_session", "operation_error", "destroy_session_invalid_response", 4),
    ],
)
def test_session_reply_integrity_fails_closed_before_state_update(
    monkeypatch, handle_request, phase, mutation, reason, bytes_read
):
    session, sent = _wire_read_session(lambda args: _session_reply(args, phase, mutation))
    record = SimpleNamespace(clientid=123)

    def setup(client, request, emit, lifecycle, tag_prefix):
        lifecycle.update(record=record, session=session)
        return record, session

    original_compound = nfs41._compound

    def compound(target, operations, current_phase, emit, **kwargs):
        if current_phase == "destroy_clientid":
            raise AssertionError("unsafe cleanup after malformed response")
        return original_compound(target, operations, current_phase, emit, **kwargs)

    monkeypatch.setattr(nfs41, "_setup", setup)
    monkeypatch.setattr(nfs41, "_compound", compound)
    events = []
    result = nfs41._run(
        object(), {**handle_request, "length": 4}, nfs41._CompoundBudget(events.append)
    )
    assert result == {"status": "error", "bytes_read": bytes_read, "reason": reason}
    compounds = [event for event in events if event["type"] == "compound"]
    assert compounds[-1]["phase"] == phase
    assert not any(event["type"] == "read_rejected" for event in events)
    if bytes_read == 0:
        assert not any(event["type"] == "progress" for event in events)
        assert not any(event["phase"] == "read" for event in compounds)
    unpacker = nfs41.nfs4client.nfs4lib.FancyNFS4Unpacker(sent[-1][2])
    request = unpacker.unpack_COMPOUND4args()
    unpacker.done()
    attempted_sequence = request.argarray[0].opsequence
    assert session.fore_channel.slots[0].seqid == attempted_sequence.sa_sequenceid - 1


@pytest.mark.parametrize(
    ("phase", "failed_op", "status", "safe"),
    [
        ("reclaim_complete", const.OP_RECLAIM_COMPLETE, const.NFS4ERR_ACCESS, True),
        ("getattr", const.OP_PUTFH, const.NFS4ERR_ACCESS, True),
        ("getattr", const.OP_GETATTR, const.NFS4ERR_ACCESS, True),
        ("open", const.OP_OPEN, const.NFS4ERR_ACCESS, True),
        ("close", const.OP_CLOSE, const.NFS4ERR_ACCESS, True),
        ("destroy_session", const.OP_DESTROY_SESSION, const.NFS4ERR_ACCESS, True),
        ("getattr", const.OP_GETATTR, const.NFS4ERR_RETRY_UNCACHED_REP, False),
    ],
)
def test_valid_nonread_wire_error_is_not_malformed(phase, failed_op, status, safe):
    xdr = nfs41.xdr
    state = xdr.stateid4(1, b"x" * 12)
    open_op = ops.open(
        0,
        const.OPEN4_SHARE_ACCESS_READ | const.OPEN4_SHARE_ACCESS_WANT_NO_DELEG,
        const.OPEN4_SHARE_DENY_NONE,
        xdr.open_owner4(0, b"owner"),
        xdr.openflag4(const.OPEN4_NOCREATE),
        xdr.open_claim4(const.CLAIM_FH),
    )
    phases = {
        "reclaim_complete": [ops.reclaim_complete(False)],
        "getattr": [
            ops.putfh(b"handle"),
            ops.getattr((1 << const.FATTR4_TYPE) | (1 << const.FATTR4_SIZE)),
        ],
        "open": [ops.putfh(b"handle"), open_op],
        "close": [ops.putfh(b"handle"), ops.close(0, state)],
        "destroy_session": [ops.destroy_session(b"s" * 16)],
    }

    def reply(args):
        unpacker = nfs41.nfs4client.nfs4lib.FancyNFS4Unpacker(_session_reply(args))
        response = unpacker.unpack_COMPOUND4res()
        unpacker.done()
        index = next(i for i, result in enumerate(response.resarray) if result.resop == failed_op)
        name, result_type, _, _, _ = nfs41._RESULT_TYPES[failed_op]
        response.resarray[index] = xdr.nfs_resop4(failed_op, **{name: result_type(status)})
        response.resarray = response.resarray[: index + 1]
        response.status = status
        packer = nfs41.nfs4client.nfs4lib.FancyNFS4Packer()
        packer.pack_COMPOUND4res(response)
        return packer.get_buffer()

    session, sent = _wire_read_session(reply)
    events = []
    with pytest.raises(nfs41._Failure) as failure:
        nfs41._compound(session, phases[phase], phase, events.append)
    assert failure.value.reason == f"{phase}_{const.nfsstat4[status]}"
    assert failure.value.safe_to_continue is safe
    assert len(sent) == 1
    assert session.fore_channel.slots[0].seqid == 1
    assert not session.fore_channel.slots[0].inuse


def test_path_mode_real_xdr_session_remains_compatible(monkeypatch, warm_request):
    session, sent = _wire_read_session(_session_reply)
    record = SimpleNamespace(clientid=123)

    def setup(client, request, emit, lifecycle, tag_prefix):
        lifecycle.update(record=record, session=session)
        return record, session

    original_compound = nfs41._compound

    def compound(target, operations, phase, emit, **kwargs):
        if phase == "destroy_clientid":
            return SimpleNamespace(resarray=[])
        return original_compound(target, operations, phase, emit, **kwargs)

    monkeypatch.setattr(nfs41, "_setup", setup)
    monkeypatch.setattr(nfs41, "_compound", compound)
    events = []
    result = nfs41._run(
        object(), {**warm_request, "length": 4}, nfs41._CompoundBudget(events.append)
    )
    assert result == {"status": "completed", "bytes_read": 4, "reason": "complete"}
    operations = []
    for call in sent:
        unpacker = nfs41.nfs4client.nfs4lib.FancyNFS4Unpacker(call[2])
        request = unpacker.unpack_COMPOUND4args()
        unpacker.done()
        operations.extend(op.argop for op in request.argarray)
    assert const.OP_PUTROOTFH in operations
    assert const.OP_LOOKUP in operations
    assert const.OP_GETATTR not in operations
    assert [event for event in events if event["type"] == "progress"] == [
        {"type": "progress", "bytes_read": 4}
    ]


@pytest.mark.parametrize("phase", ["exchange_id", "create_session"])
@pytest.mark.parametrize("mutation", ["operation_error", "wrong_operation", "trailing", "tag"])
def test_bootstrap_reply_integrity_stops_before_new_protocol_state(handle_request, phase, mutation):
    xdr = nfs41.xdr
    attrs = xdr.channel_attrs4(0, 8192, 8192, 8192, 128, 1, [])

    def reply(args):
        code = args.argarray[0].argop
        current_phase = "exchange_id" if code == const.OP_EXCHANGE_ID else "create_session"
        corrupt = phase == current_phase
        status = (
            const.NFS4ERR_ACCESS if corrupt and mutation == "operation_error" else const.NFS4_OK
        )
        if code == const.OP_EXCHANGE_ID:
            body = xdr.EXCHANGE_ID4resok(
                123,
                1,
                0,
                xdr.state_protect4_r(const.SP4_NONE),
                xdr.server_owner4(0, b"fixture"),
                b"scope",
                [],
            )
            result = xdr.nfs_resop4(
                code,
                opexchange_id=xdr.EXCHANGE_ID4res(
                    status, body if status == const.NFS4_OK else None
                ),
            )
        else:
            body = xdr.CREATE_SESSION4resok(b"s" * 16, 1, 0, attrs, attrs)
            result = xdr.nfs_resop4(
                code,
                opcreate_session=xdr.CREATE_SESSION4res(
                    status, body if status == const.NFS4_OK else None
                ),
            )
        if corrupt and mutation == "wrong_operation":
            result = xdr.nfs_resop4(
                const.OP_RECLAIM_COMPLETE,
                opreclaim_complete=xdr.RECLAIM_COMPLETE4res(const.NFS4_OK),
            )
        tag = b"wrong" if corrupt and mutation == "tag" else args.tag
        packer = nfs41.nfs4client.nfs4lib.FancyNFS4Packer()
        packer.pack_COMPOUND4res(xdr.COMPOUND4res(const.NFS4_OK, tag, [result]))
        data = packer.get_buffer()
        return data + b"\x00\x00\x00\x00" if corrupt and mutation == "trailing" else data

    session, sent = _wire_read_session(reply)
    client = session.c
    client.prog = const.NFS4_CALLBACK
    client.impl_id = xdr.nfs_impl_id4(b"fixture", b"fixture", xdr.nfstime4(0, 0))
    client.sessions = {}
    events = []
    result = nfs41._run(client, handle_request, nfs41._CompoundBudget(events.append))
    reason = (
        "invalid_response"
        if mutation == "trailing"
        else f"{phase}_tag_mismatch"
        if mutation == "tag"
        else f"{phase}_invalid_response"
    )
    assert result == {"status": "error", "bytes_read": 0, "reason": reason}
    assert [event["phase"] for event in events] == (
        ["exchange_id"] if phase == "exchange_id" else ["exchange_id", "create_session"]
    )
    assert len(sent) == (1 if phase == "exchange_id" else 2)
    assert not client.sessions


def _bootstrap_success_reply(args):
    xdr = nfs41.xdr
    attrs = xdr.channel_attrs4(0, 8192, 8192, 8192, 128, 1, [])
    code = args.argarray[0].argop
    if code == const.OP_EXCHANGE_ID:
        body = xdr.EXCHANGE_ID4resok(
            123,
            1,
            0,
            xdr.state_protect4_r(const.SP4_NONE),
            xdr.server_owner4(0, b"s"),
            b"s",
            [],
        )
        result = xdr.nfs_resop4(code, opexchange_id=xdr.EXCHANGE_ID4res(const.NFS4_OK, body))
    elif code == const.OP_CREATE_SESSION:
        body = xdr.CREATE_SESSION4resok(b"s" * 16, 1, 0, attrs, attrs)
        result = xdr.nfs_resop4(code, opcreate_session=xdr.CREATE_SESSION4res(const.NFS4_OK, body))
    else:
        assert code == const.OP_DESTROY_CLIENTID
        result = xdr.nfs_resop4(code, opdestroy_clientid=xdr.DESTROY_CLIENTID4res(const.NFS4_OK))
    packer = nfs41.nfs4client.nfs4lib.FancyNFS4Packer()
    packer.pack_COMPOUND4res(xdr.COMPOUND4res(const.NFS4_OK, args.tag, [result]))
    return packer.get_buffer()


def test_real_xdr_bootstrap_and_destroy_clientid_succeed(handle_request):
    xdr = nfs41.xdr
    placeholder, sent = _wire_read_session(_bootstrap_success_reply)
    client = placeholder.c
    client.prog = const.NFS4_CALLBACK
    client.impl_id = xdr.nfs_impl_id4(b"fixture", b"fixture", xdr.nfstime4(0, 0))
    client.sessions = {}
    events = []
    lifecycle = {}
    record, session = nfs41._setup(client, handle_request, events.append, lifecycle)
    assert record.clientid == 123
    assert session.sessionid == b"s" * 16
    assert client.sessions[session.sessionid] is session
    nfs41._compound(
        client, [ops.destroy_clientid(record.clientid)], "destroy_clientid", events.append
    )
    assert [event["phase"] for event in events] == [
        "exchange_id",
        "create_session",
        "destroy_clientid",
    ]
    assert len(sent) == 3


@pytest.mark.parametrize("handle_mode", [False, True])
def test_full_real_xdr_lifecycle_offline(warm_request, handle_request, handle_mode):
    xdr = nfs41.xdr

    def reply(args):
        if args.argarray[0].argop == const.OP_SEQUENCE:
            return _session_reply(args)
        return _bootstrap_success_reply(args)

    placeholder, sent = _wire_read_session(reply)
    client = placeholder.c
    client.prog = const.NFS4_CALLBACK
    client.impl_id = xdr.nfs_impl_id4(b"fixture", b"fixture", xdr.nfstime4(0, 0))
    client.sessions = {}
    events = []
    request = {**(handle_request if handle_mode else warm_request), "length": 4}
    result = nfs41._run(client, request, nfs41._CompoundBudget(events.append))
    assert result == {"status": "completed", "bytes_read": 4, "reason": "complete"}
    phases = [event["phase"] for event in events if event["type"] == "compound"]
    assert phases == (
        ["exchange_id", "create_session", "reclaim_complete"]
        + (["getattr"] if handle_mode else ["lookup"] * 4)
        + ["open", "read", "close", "destroy_session", "destroy_clientid"]
    )
    assert len(sent) == len(phases)


@pytest.mark.parametrize(
    ("mutation", "reason"),
    [
        ("operation_error", "destroy_clientid_invalid_response"),
        ("tag", "destroy_clientid_tag_mismatch"),
        ("trailing", "cleanup_invalid_response"),
    ],
)
def test_destroy_clientid_malformed_reply_preserves_acknowledged_progress(
    handle_request, mutation, reason
):
    xdr = nfs41.xdr

    def reply(args):
        code = args.argarray[0].argop
        if code == const.OP_SEQUENCE:
            return _session_reply(args)
        if code != const.OP_DESTROY_CLIENTID:
            return _bootstrap_success_reply(args)
        status = const.NFS4ERR_ACCESS if mutation == "operation_error" else const.NFS4_OK
        result = xdr.nfs_resop4(code, opdestroy_clientid=xdr.DESTROY_CLIENTID4res(status))
        tag = b"wrong" if mutation == "tag" else args.tag
        packer = nfs41.nfs4client.nfs4lib.FancyNFS4Packer()
        packer.pack_COMPOUND4res(xdr.COMPOUND4res(const.NFS4_OK, tag, [result]))
        data = packer.get_buffer()
        return data + b"\x00\x00\x00\x00" if mutation == "trailing" else data

    placeholder, sent = _wire_read_session(reply)
    client = placeholder.c
    client.prog = const.NFS4_CALLBACK
    client.impl_id = xdr.nfs_impl_id4(b"fixture", b"fixture", xdr.nfstime4(0, 0))
    client.sessions = {}
    events = []
    result = nfs41._run(
        client, {**handle_request, "length": 4}, nfs41._CompoundBudget(events.append)
    )
    assert result == {"status": "error", "bytes_read": 4, "reason": reason}
    compounds = [event for event in events if event["type"] == "compound"]
    assert compounds[-1]["phase"] == "destroy_clientid"
    assert len(sent) == len(compounds)
    assert [event for event in events if event["type"] == "progress"] == [
        {"type": "progress", "bytes_read": 4}
    ]


@pytest.mark.parametrize(
    "shape",
    [
        "failed_putfh",
        "failed_read",
        "wrong_session",
        "wrong_sequence",
        "wrong_slot",
        "failed_sequence",
    ],
)
@pytest.mark.parametrize("after_progress", [False, True])
def test_malformed_read_success_never_acknowledges_data(
    monkeypatch, handle_request, shape, after_progress
):
    xdr = nfs41.xdr
    reads = 0

    def reply(args):
        nonlocal reads
        reads += 1
        current_shape = "valid" if after_progress and reads == 1 else shape
        count = args.argarray[-1].opread.count
        return _successful_read_reply(args, current_shape, b"a" * count)

    session, sent = _wire_read_session(reply)
    record = SimpleNamespace(clientid=123)

    def setup(client, request, emit, lifecycle, tag_prefix):
        lifecycle.update(record=record, session=session)
        return record, session

    original_compound = nfs41._compound

    def fake_other_phases(target, operations, phase, emit, **kwargs):
        if phase == "read":
            return original_compound(target, operations, phase, emit, **kwargs)
        if phase == "getattr":
            return SimpleNamespace(
                resarray=[
                    SimpleNamespace(
                        obj_attributes={const.FATTR4_TYPE: const.NF4REG, const.FATTR4_SIZE: 2**40}
                    )
                ]
            )
        if phase == "open":
            return SimpleNamespace(resarray=[SimpleNamespace(stateid=xdr.stateid4(1, b"x" * 12))])
        return SimpleNamespace(resarray=[])

    monkeypatch.setattr(nfs41, "_setup", setup)
    monkeypatch.setattr(nfs41, "_compound", fake_other_phases)
    events = []
    result = nfs41._run(object(), handle_request, nfs41._CompoundBudget(events.append))
    acknowledged = 4096 if after_progress else 0
    assert result == {
        "status": "error",
        "bytes_read": acknowledged,
        "reason": "read_invalid_response",
    }
    assert [event["bytes_read"] for event in events if event["type"] == "progress"] == (
        [4096] if after_progress else []
    )
    assert [event["type"] for event in events][-1] == "compound"
    assert not any(event["type"] == "read_rejected" for event in events)
    assert len(sent) == (2 if after_progress else 1)


@pytest.mark.parametrize(
    ("status", "rejected"),
    [(const.NFS4ERR_ACCESS, True), (const.NFS4ERR_RETRY_UNCACHED_REP, False)],
)
def test_read_error_after_progress_preserves_bytes_and_rejection_state(
    monkeypatch, handle_request, status, rejected
):
    xdr = nfs41.xdr
    reads = 0

    def reply(args):
        nonlocal reads
        reads += 1
        if reads == 2:
            return _failed_read_reply(args, "read", status=status)
        sequence = args.argarray[0].opsequence
        seq_result = xdr.SEQUENCE4resok(
            sequence.sa_sessionid, sequence.sa_sequenceid, sequence.sa_slotid, 0, 0, 0
        )
        results = [
            xdr.nfs_resop4(
                const.OP_SEQUENCE, opsequence=xdr.SEQUENCE4res(const.NFS4_OK, seq_result)
            ),
            xdr.nfs_resop4(const.OP_PUTFH, opputfh=xdr.PUTFH4res(const.NFS4_OK)),
            xdr.nfs_resop4(
                const.OP_READ,
                opread=xdr.READ4res(const.NFS4_OK, xdr.READ4resok(False, b"a" * 4096)),
            ),
        ]
        packer = nfs41.nfs4client.nfs4lib.FancyNFS4Packer()
        packer.pack_COMPOUND4res(xdr.COMPOUND4res(const.NFS4_OK, args.tag, results))
        return packer.get_buffer()

    session, sent = _wire_read_session(reply)
    record = SimpleNamespace(clientid=123)

    def setup(client, request, emit, lifecycle, tag_prefix):
        lifecycle.update(record=record, session=session)
        return record, session

    original_compound = nfs41._compound

    def fake_other_phases(target, operations, phase, emit, **kwargs):
        if phase == "read":
            return original_compound(target, operations, phase, emit, **kwargs)
        if phase == "getattr":
            return SimpleNamespace(
                resarray=[
                    SimpleNamespace(
                        obj_attributes={const.FATTR4_TYPE: const.NF4REG, const.FATTR4_SIZE: 2**40}
                    )
                ]
            )
        if phase == "open":
            return SimpleNamespace(resarray=[SimpleNamespace(stateid=xdr.stateid4(1, b"x" * 12))])
        return SimpleNamespace(resarray=[])

    monkeypatch.setattr(nfs41, "_setup", setup)
    monkeypatch.setattr(nfs41, "_compound", fake_other_phases)
    events = []
    result = nfs41._run(object(), handle_request, nfs41._CompoundBudget(events.append))
    assert result == {
        "status": "error",
        "bytes_read": 4096,
        "reason": f"read_{const.nfsstat4[status]}",
    }
    assert [event["type"] for event in events] == [
        "compound",
        "progress",
        "compound",
    ] + (["read_rejected"] if rejected else [])
    assert events[1] == {"type": "progress", "bytes_read": 4096}
    if rejected:
        assert events[-1] == {"type": "read_rejected"}
    assert len(sent) == 2
