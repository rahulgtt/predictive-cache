"""A single, sequential NFSv4.1 read-only range attempt.

The caller validates and authorizes the request and owns the process deadline.
COMPOUND events describe send attempts, not server receipt or cache residency.
"""

import logging
import re
import secrets
import socket
import struct
import warnings
from collections.abc import Callable
from contextlib import redirect_stdout
from io import StringIO
from types import MethodType

from .limits import MAX_COMPOUNDS, READ_BYTES

# This pinned library prints an optional-GSS notice at import time.
with redirect_stdout(StringIO()), warnings.catch_warnings():
    warnings.filterwarnings("ignore", message="invalid escape sequence", category=SyntaxWarning)
    import nfs4client
    import nfs_ops
    from rpc import rpc, security
    from xdrdef import nfs4_const as const
    from xdrdef import nfs4_type as xdr
    from xdrlib3 import Error as XDRError

_OPS = nfs_ops.NFS4ops()
# RPC reply header and COMPOUND header, SEQUENCE, PUTFH and READ result,
# excluding the variable-length padded tag itself.
_READ_RESPONSE_FIXED_OVERHEAD = 104
# Minimum successful SEQUENCE + PUTFH + OPEN reply, including the RPC header,
# but excluding the padded tag. Other OPEN results can be larger.
_OPEN_RESPONSE_FIXED_OVERHEAD = 144
_CLEANUP_PHASES = frozenset({"close", "destroy_session", "destroy_clientid"})
_REASON_LIMIT = 96
_STOP_ERROR_MARKER = ":stop_error"
_RESULT_TYPES = {
    const.OP_SEQUENCE: (
        "opsequence",
        xdr.SEQUENCE4res,
        "sr_status",
        "sr_resok4",
        xdr.SEQUENCE4resok,
    ),
    const.OP_EXCHANGE_ID: (
        "opexchange_id",
        xdr.EXCHANGE_ID4res,
        "eir_status",
        "eir_resok4",
        xdr.EXCHANGE_ID4resok,
    ),
    const.OP_CREATE_SESSION: (
        "opcreate_session",
        xdr.CREATE_SESSION4res,
        "csr_status",
        "csr_resok4",
        xdr.CREATE_SESSION4resok,
    ),
    const.OP_RECLAIM_COMPLETE: (
        "opreclaim_complete",
        xdr.RECLAIM_COMPLETE4res,
        "rcr_status",
        None,
        None,
    ),
    const.OP_PUTROOTFH: ("opputrootfh", xdr.PUTROOTFH4res, "status", None, None),
    const.OP_PUTFH: ("opputfh", xdr.PUTFH4res, "status", None, None),
    const.OP_LOOKUP: ("oplookup", xdr.LOOKUP4res, "status", None, None),
    const.OP_GETFH: ("opgetfh", xdr.GETFH4res, "status", "resok4", xdr.GETFH4resok),
    const.OP_GETATTR: ("opgetattr", xdr.GETATTR4res, "status", "resok4", xdr.GETATTR4resok),
    const.OP_OPEN: ("opopen", xdr.OPEN4res, "status", "resok4", xdr.OPEN4resok),
    const.OP_READ: ("opread", xdr.READ4res, "status", "resok4", xdr.READ4resok),
    const.OP_CLOSE: ("opclose", xdr.CLOSE4res, "status", "open_stateid", xdr.stateid4),
    const.OP_DESTROY_SESSION: (
        "opdestroy_session",
        xdr.DESTROY_SESSION4res,
        "dsr_status",
        None,
        None,
    ),
    const.OP_DESTROY_CLIENTID: (
        "opdestroy_clientid",
        xdr.DESTROY_CLIENTID4res,
        "dcr_status",
        None,
        None,
    ),
}


class _Failure(Exception):
    def __init__(self, reason: str, *, safe_to_continue: bool = True):
        self.reason = reason
        self.safe_to_continue = safe_to_continue


class _CompoundBudget:
    def __init__(self, emit):
        self.emit = emit
        self.attempts = 0

    def __call__(self, event):
        if event["type"] == "compound":
            reserved = 0 if event["phase"] in _CLEANUP_PHASES else len(_CLEANUP_PHASES)
            if self.attempts >= MAX_COMPOUNDS - reserved:
                raise _Failure("compound_limit_exceeded")
            self.attempts += 1
        self.emit(event)


def _tag_prefix(request: dict) -> str:
    if "file_handle" not in request:
        return ""
    handle = request["file_handle"]
    plan_ref = request.get("plan_ref")
    task_ref = request.get("task_ref")
    if (
        type(handle) is not bytes
        or not 1 <= len(handle) <= 128
        or not isinstance(plan_ref, str)
        or not re.fullmatch(r"[0-9a-f]{32}", plan_ref)
        or not isinstance(task_ref, str)
        or not re.fullmatch(r"[0-9a-f]{32}", task_ref)
    ):
        raise _Failure("invalid_handle_request")
    return f"pc1:w:{plan_ref}:{task_ref}:"


def _read_response_overhead(tag_length: int) -> int:
    return _READ_RESPONSE_FIXED_OVERHEAD + (tag_length + 3) // 4 * 4


def _open_response_overhead(tag_length: int) -> int:
    return _OPEN_RESPONSE_FIXED_OVERHEAD + (tag_length + 3) // 4 * 4


def _error_reason(category: str, error: Exception) -> str:
    # Use fixed labels rather than an arbitrary exception type name or message.
    for kind in (
        rpc.RPCTimeout,
        rpc.RPCAcceptError,
        rpc.RPCDeniedError,
        rpc.RPCError,
        security.SecError,
        socket.gaierror,
        ConnectionRefusedError,
        ConnectionResetError,
        ConnectionAbortedError,
        BrokenPipeError,
        TimeoutError,
        OSError,
        EOFError,
        RuntimeError,
    ):
        if isinstance(error, kind):
            reason = f"{category}:{kind.__name__}"
            break
    else:
        reason = f"{category}:Error"
    errno = getattr(error, "errno", None)
    if type(errno) is int and -(2**31) <= errno < 2**31:
        reason += f":errno:{errno}"
    return reason


def _status_reason(phase: str, status: int) -> str:
    name = const.nfsstat4.get(status)
    if name is None:
        return f"{phase}_nfs_status_{status}"
    return f"{phase}_{name}"


def _append_cleanup_reason(primary: str, cleanup: str) -> str:
    # Keep room to report a later client.stop() failure without losing the primary.
    prefix = f"{primary}:cleanup:"
    available = max(0, _REASON_LIMIT - len(_STOP_ERROR_MARKER) - len(prefix))
    return prefix + cleanup[:available]


def _reject_callback(data, cred):
    # No backchannel is negotiated; do not run pynfs's traceback-printing handler.
    return rpc.PROC_UNAVAIL, b""


def _connect(client, address, secure=False):
    # pynfs closes failed sockets then uses the last one, masking the real errno.
    last_error = None
    for family, socktype, proto, _, sockaddr in socket.getaddrinfo(*address, 0, socket.SOCK_STREAM):
        connection = None
        try:
            connection = socket.socket(family, socktype, proto)
            connection.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            if secure:
                client.bindsocket(connection)
            connection.connect(sockaddr)
            connection.setblocking(False)
        except OSError as error:
            if connection is not None:
                connection.close()
            last_error = error
            continue
        pipe = rpc.RpcPipe(connection, client._alarm)
        ready = rpc.DeferredData()
        client._alarm.buzz(b"\x01", (pipe, ready))
        ready.wait()
        return pipe
    if last_error is not None:
        raise last_error from None
    raise OSError("no usable address")


def _check_response(response, tag: bytes, phase: str):
    if response is None or response.tag != tag:
        raise _Failure(f"{phase}_tag_mismatch", safe_to_continue=False)
    if response.status != const.NFS4_OK:
        raise _Failure(
            _status_reason(phase, response.status),
            safe_to_continue=response.status != const.NFS4ERR_RETRY_UNCACHED_REP,
        )
    return response


def _unpack_compound(client, xid, pipe=None, timeout=300):
    if pipe is None:
        pipe = client.c1
    _, data = pipe.listen(xid, timeout)
    if data:
        unpacker = nfs4client.nfs4lib.FancyNFS4Unpacker(data)
        data = unpacker.unpack_COMPOUND4res()
        unpacker.done()
    return data


def _validate_compound(response, operations, tag, phase, sequence=None):
    if response is None or response.tag != tag:
        raise _Failure(f"{phase}_tag_mismatch", safe_to_continue=False)
    if type(response.status) is not int or not 0 <= response.status < 2**32:
        raise _Failure(f"{phase}_invalid_response", safe_to_continue=False)
    expected = [operation.argop for operation in operations]
    if sequence is not None:
        expected.insert(0, const.OP_SEQUENCE)
    results = response.resarray
    if not isinstance(results, (list, tuple)) or not 1 <= len(results) <= len(expected):
        raise _Failure(f"{phase}_invalid_response", safe_to_continue=False)
    statuses = []
    for result, op in zip(results, expected, strict=False):
        if not isinstance(result, xdr.nfs_resop4) or result.resop != op or op not in _RESULT_TYPES:
            raise _Failure(f"{phase}_invalid_response", safe_to_continue=False)
        name, result_type, status_name, body_name, body_type = _RESULT_TYPES[op]
        payload = getattr(result, name)
        if not isinstance(payload, result_type):
            raise _Failure(f"{phase}_invalid_response", safe_to_continue=False)
        status = getattr(payload, status_name)
        if type(status) is not int or not 0 <= status < 2**32:
            raise _Failure(f"{phase}_invalid_response", safe_to_continue=False)
        if body_name is not None:
            body = getattr(payload, body_name)
            if (status == const.NFS4_OK and not isinstance(body, body_type)) or (
                status != const.NFS4_OK and body is not None
            ):
                raise _Failure(f"{phase}_invalid_response", safe_to_continue=False)
        statuses.append(status)
    if (
        statuses[-1] != response.status
        or any(status != const.NFS4_OK for status in statuses[:-1])
        or (response.status == const.NFS4_OK and len(results) != len(expected))
    ):
        raise _Failure(f"{phase}_invalid_response", safe_to_continue=False)
    if sequence is not None:
        if statuses[0] != const.NFS4_OK:
            raise _Failure(_status_reason(phase, response.status), safe_to_continue=False)
        if not _valid_sequence(results[0], sequence):
            raise _Failure(f"{phase}_invalid_response", safe_to_continue=False)


def _session_compound(target, operations, tag, phase):
    """Validate the full reply before pynfs updates its SEQUENCE state."""
    client = target.c
    captured = {}
    original_prepare = target._prepare_compound
    missing = object()
    own_listen = client.__dict__.get("listen", missing)
    own_prepare = target.__dict__.get("_prepare_compound", missing)

    def prepare(kwargs):
        slot, sequence = original_prepare(kwargs)
        captured["sequence"] = sequence.opsequence
        return slot, sequence

    def listen(xid, pipe=None, timeout=300):
        response = _unpack_compound(client, xid, pipe, timeout)
        sequence = captured.get("sequence")
        if sequence is None:
            raise _Failure(f"{phase}_invalid_response", safe_to_continue=False)
        _validate_compound(response, operations, tag, phase, sequence)
        captured["results"] = tuple(response.resarray)
        return response

    target._prepare_compound = prepare
    client.listen = listen
    try:
        response = target.compound(operations, tag=tag, max_retries=1, handle_state_errors=False)
    finally:
        if own_listen is missing:
            del client.listen
        else:
            client.listen = own_listen
        if own_prepare is missing:
            del target._prepare_compound
        else:
            target._prepare_compound = own_prepare
    return response, captured


def _client_compound(target, operations, tag, phase):
    xid = target.compound_async(operations, tag=tag, version=1)
    response = _unpack_compound(target, xid)
    _validate_compound(response, operations, tag, phase)
    return response


def _valid_sequence(result, sequence):
    if not isinstance(result, xdr.nfs_resop4) or result.resop != const.OP_SEQUENCE:
        return False
    value = result.opsequence
    if (
        not isinstance(value, xdr.SEQUENCE4res)
        or type(value.sr_status) is not int
        or value.sr_status != const.NFS4_OK
    ):
        return False
    body = value.sr_resok4
    return (
        isinstance(body, xdr.SEQUENCE4resok)
        and body.sr_sessionid == sequence.sa_sessionid
        and body.sr_sequenceid == sequence.sa_sequenceid
        and body.sr_slotid == sequence.sa_slotid
    )


def _is_read_success(response, captured, operations):
    results = captured.get("results")
    sequence = captured.get("sequence")
    if (
        [operation.argop for operation in operations] != [const.OP_PUTFH, const.OP_READ]
        or not isinstance(results, tuple)
        or len(results) != 3
        or sequence is None
        or type(response.status) is not int
        or response.status != const.NFS4_OK
        or not _valid_sequence(results[0], sequence)
    ):
        return False
    putfh, read = results[1:]
    return (
        isinstance(putfh, xdr.nfs_resop4)
        and putfh.resop == const.OP_PUTFH
        and isinstance(putfh.opputfh, xdr.PUTFH4res)
        and type(putfh.opputfh.status) is int
        and putfh.opputfh.status == const.NFS4_OK
        and isinstance(read, xdr.nfs_resop4)
        and read.resop == const.OP_READ
        and isinstance(read.opread, xdr.READ4res)
        and type(read.opread.status) is int
        and read.opread.status == const.NFS4_OK
        and isinstance(read.opread.resok4, xdr.READ4resok)
    )


def _is_read_rejection(response, captured, operations):
    """Recognize only a complete, correctly ordered failed READ prefix."""
    if [operation.argop for operation in operations] != [const.OP_PUTFH, const.OP_READ]:
        return False
    results = captured.get("results")
    sequence = captured.get("sequence")
    # RETRY_UNCACHED_REP does not rule out a previously executed READ.
    if (
        not isinstance(results, tuple)
        or sequence is None
        or not 2 <= len(results) <= 3
        or type(response.status) is not int
        or response.status in (const.NFS4_OK, const.NFS4ERR_RETRY_UNCACHED_REP)
        or not _valid_sequence(results[0], sequence)
    ):
        return False
    expected = (const.OP_PUTFH, const.OP_READ)
    statuses = []
    for result, op in zip(results[1:], expected, strict=False):
        if not isinstance(result, xdr.nfs_resop4) or result.resop != op:
            return False
        if op == const.OP_PUTFH:
            value = result.opputfh
            if not isinstance(value, xdr.PUTFH4res):
                return False
            status = value.status
        else:
            value = result.opread
            if not isinstance(value, xdr.READ4res) or value.resok4 is not None:
                return False
            status = value.status
        if type(status) is not int or not 0 <= status < 2**32:
            return False
        statuses.append(status)
    return all(status == const.NFS4_OK for status in statuses[:-1]) and (
        statuses[-1] == response.status
    )


def _compound(
    target, operations, phase: str, emit: Callable[[dict], None], *, tag_prefix="", **metadata
):
    tag = tag_prefix + secrets.token_hex(16)
    event = {"type": "compound", "tag": tag, "phase": phase, **metadata}
    emit(event)
    captured = {}
    if isinstance(target, nfs4client.SessionRecord):
        response, captured = _session_compound(target, operations, tag.encode("ascii"), phase)
    else:
        response = _client_compound(target, operations, tag.encode("ascii"), phase)
    try:
        response = _check_response(response, tag.encode("ascii"), phase)
    except _Failure:
        if (
            phase == "read"
            and response is not None
            and response.tag == tag.encode("ascii")
            and _is_read_rejection(response, captured, operations)
        ):
            emit({"type": "read_rejected"})
        raise
    if phase == "read" and not _is_read_success(response, captured, operations):
        raise _Failure("read_invalid_response", safe_to_continue=False)
    if len(response.resarray) != len(operations) or any(
        result.resop != operation.argop
        for result, operation in zip(response.resarray, operations, strict=True)
    ):
        raise _Failure(f"{phase}_invalid_response", safe_to_continue=False)
    return response


def _setup(client, request, emit, lifecycle, tag_prefix=""):
    cred = security.AuthSys().init_cred(
        uid=request["uid"], gid=request["gid"], name=b"pcache", gids=[]
    )
    client.set_cred(cred)
    protection = xdr.state_protect4_a(const.SP4_NONE)
    owner = xdr.client_owner4(secrets.token_bytes(8), secrets.token_hex(16).encode("ascii"))
    response = _compound(
        client,
        [_OPS.exchange_id(owner, 0, protection, [client.impl_id])],
        "exchange_id",
        emit,
        tag_prefix=tag_prefix,
    )
    record = nfs4client.ClientRecord(response.resarray[0], client, cred, protection)
    lifecycle["record"] = record
    attrs = xdr.channel_attrs4(0, 8192, 8192, 8192, 128, 1, [])
    response = _compound(
        client,
        [
            _OPS.create_session(
                record.clientid,
                record.seqid,
                0,
                attrs,
                attrs,
                client.prog,
                [xdr.callback_sec_parms4(const.AUTH_NONE)],
            )
        ],
        "create_session",
        emit,
        tag_prefix=tag_prefix,
    )
    session = record._add_session(response.resarray[0])
    lifecycle["session"] = session
    return record, session


def _run(client, request, emit):
    total = 0
    session = None
    record = None
    handle = None
    stateid = None
    safe = True
    result = None
    lifecycle = {}
    try:
        tag_prefix = _tag_prefix(request)
        handle_mode = "file_handle" in request
        record, session = _setup(client, request, emit, lifecycle, tag_prefix)
        channel = session.fore_channel
        if channel.maxoperations < (3 if handle_mode else 4):
            raise _Failure("session_operation_limit")
        # A READ reply includes variable data rounded up to a four-byte boundary.
        tag_length = len(tag_prefix) + 32
        overhead = _read_response_overhead(tag_length)
        read_limit = min(READ_BYTES, (channel.maxresponsesize - overhead) // 4 * 4)
        if read_limit <= 0 or (
            handle_mode and channel.maxresponsesize < _open_response_overhead(tag_length)
        ):
            raise _Failure("session_response_limit")
        _compound(
            session, [_OPS.reclaim_complete(False)], "reclaim_complete", emit, tag_prefix=tag_prefix
        )
        if handle_mode:
            handle = request["file_handle"]
            attrs_response = _compound(
                session,
                [
                    _OPS.putfh(handle),
                    _OPS.getattr((1 << const.FATTR4_TYPE) | (1 << const.FATTR4_SIZE)),
                ],
                "getattr",
                emit,
                tag_prefix=tag_prefix,
            )
            attrs = getattr(attrs_response.resarray[-1], "obj_attributes", None)
            required = {const.FATTR4_TYPE, const.FATTR4_SIZE}
            if (
                type(attrs) is not dict
                or set(attrs) != required
                or type(attrs[const.FATTR4_TYPE]) is not int
                or type(attrs[const.FATTR4_SIZE]) is not int
                or not 0 <= attrs[const.FATTR4_SIZE] < 2**64
            ):
                raise _Failure("getattr_invalid_attributes")
            if attrs[const.FATTR4_TYPE] != const.NF4REG:
                raise _Failure("getattr_not_regular")
            if request["offset"] + request["length"] > attrs[const.FATTR4_SIZE]:
                raise _Failure("getattr_range_exceeds_size")
        else:
            components = request["path"].split("/")[1:]
            if not components or not components[-1]:
                raise _Failure("invalid_path")
            root = _compound(
                session, [_OPS.putrootfh(), _OPS.getfh()], "lookup", emit, tag_prefix=tag_prefix
            )
            parent = root.resarray[-1].object
            for component in components:
                response = _compound(
                    session,
                    [_OPS.putfh(parent), _OPS.lookup(component.encode("utf-8")), _OPS.getfh()],
                    "lookup",
                    emit,
                    tag_prefix=tag_prefix,
                )
                parent = response.resarray[-1].object
            handle = parent
        open_op = _OPS.open(
            0,
            const.OPEN4_SHARE_ACCESS_READ | const.OPEN4_SHARE_ACCESS_WANT_NO_DELEG,
            const.OPEN4_SHARE_DENY_NONE,
            xdr.open_owner4(0, secrets.token_hex(16).encode("ascii")),
            xdr.openflag4(const.OPEN4_NOCREATE),
            xdr.open_claim4(const.CLAIM_FH),
        )
        response = _compound(
            session, [_OPS.putfh(handle), open_op], "open", emit, tag_prefix=tag_prefix
        )
        stateid = response.resarray[-1].stateid
        while total < request["length"]:
            count = min(read_limit, request["length"] - total)
            offset = request["offset"] + total
            response = _compound(
                session,
                [_OPS.putfh(handle), _OPS.read(stateid, offset, count)],
                "read",
                emit,
                tag_prefix=tag_prefix,
                offset=offset,
                count=count,
            )
            read = response.resarray[-1]
            received = len(read.data)
            if received > count:
                raise _Failure("read_invalid_count", safe_to_continue=False)
            total += received
            emit({"type": "progress", "bytes_read": total})
            if total == request["length"]:
                result = {"status": "completed", "bytes_read": total, "reason": "complete"}
                break
            if received < count or read.eof:
                result = {
                    "status": "partial",
                    "bytes_read": total,
                    "reason": "eof" if read.eof else "short_read",
                }
                break
    except _Failure as error:
        safe = error.safe_to_continue
        result = {
            "status": "error",
            "bytes_read": total,
            "reason": error.reason,
        }
    except (rpc.RPCError, security.SecError, OSError, EOFError) as error:
        safe = False
        result = {
            "status": "error",
            "bytes_read": total,
            "reason": _error_reason("transport_error", error),
        }
    except (
        AttributeError,
        IndexError,
        KeyError,
        RuntimeError,
        TypeError,
        ValueError,
        XDRError,
        struct.error,
    ):
        safe = False
        result = {
            "status": "error",
            "bytes_read": total,
            "reason": "invalid_response",
        }
    record = lifecycle.get("record")
    session = lifecycle.get("session")
    if safe and record is not None:
        cleanup_failure = None
        if session is not None and handle is not None and stateid is not None:
            try:
                _compound(
                    session,
                    [_OPS.putfh(handle), _OPS.close(0, stateid)],
                    "close",
                    emit,
                    tag_prefix=tag_prefix,
                )
            except _Failure as error:
                cleanup_failure = error.reason
                safe = error.safe_to_continue
            except (rpc.RPCError, security.SecError, OSError, EOFError) as error:
                cleanup_failure = _error_reason("cleanup_transport_error", error)
                safe = False
            except (
                AttributeError,
                IndexError,
                KeyError,
                RuntimeError,
                TypeError,
                ValueError,
                XDRError,
                struct.error,
            ):
                cleanup_failure = "cleanup_invalid_response"
                safe = False
        try:
            if safe:
                if session is not None:
                    _compound(
                        session,
                        [_OPS.destroy_session(session.sessionid)],
                        "destroy_session",
                        emit,
                        tag_prefix=tag_prefix,
                    )
                _compound(
                    client,
                    [_OPS.destroy_clientid(record.clientid)],
                    "destroy_clientid",
                    emit,
                    tag_prefix=tag_prefix,
                )
        except _Failure as error:
            cleanup_failure = cleanup_failure or error.reason
        except (rpc.RPCError, security.SecError, OSError, EOFError) as error:
            cleanup_failure = cleanup_failure or _error_reason("cleanup_transport_error", error)
        except (
            AttributeError,
            IndexError,
            KeyError,
            RuntimeError,
            TypeError,
            ValueError,
            XDRError,
            struct.error,
        ):
            cleanup_failure = cleanup_failure or "cleanup_invalid_response"
        if cleanup_failure is not None:
            if result is not None and result["status"] == "error":
                result["reason"] = _append_cleanup_reason(result["reason"], cleanup_failure)
            else:
                result = {
                    "status": "error",
                    "bytes_read": total,
                    "reason": cleanup_failure,
                }
    return result


def warm(request: dict, emit: Callable[[dict], None]) -> dict:
    """Attempt a caller-authorized range without retrying any READ."""
    try:
        _tag_prefix(request)
    except _Failure as error:
        return {"status": "error", "bytes_read": 0, "reason": error.reason}
    if request["length"] == 0:
        return {"status": "completed", "bytes_read": 0, "reason": "complete"}
    # pynfs logs connection addresses and potentially full RPC payloads.
    # The adapter runs in a dedicated child, so suppress these library loggers.
    for name in ("rpc.poll", "rpc.thread", "rpc.sec.gss", "nfs.client.cb"):
        logging.getLogger(name).disabled = True
    client = nfs4client.NFS4Client.__new__(nfs4client.NFS4Client)
    client.handle_1 = _reject_callback
    client.connect = MethodType(_connect, client)
    result = {"status": "error", "bytes_read": 0, "reason": "connection_error"}
    try:
        client.__init__(host=request["host"], port=request["port"], minorversion=1)
        result = _run(client, request, _CompoundBudget(emit))
    except (rpc.RPCError, security.SecError, OSError, EOFError) as error:
        result = {
            "status": "error",
            "bytes_read": 0,
            "reason": _error_reason("connection_error", error),
        }
    finally:
        if hasattr(client, "_alarm"):
            try:
                client.stop()
            except Exception as error:  # noqa: BLE001 - child-process cleanup boundary.
                reason = _error_reason("stop_error", error)
                if result["status"] == "error":
                    previous_reason = result["reason"]
                    if len(previous_reason) + len(reason) + 1 > _REASON_LIMIT:
                        reason = "stop_error"
                    reason = f"{previous_reason}:{reason}"
                result = {
                    "status": "error",
                    "bytes_read": result["bytes_read"],
                    "reason": reason,
                }
    return result
