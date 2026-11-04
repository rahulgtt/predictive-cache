"""Owner-only, symlink-resistant file operations for dispatcher state."""

from __future__ import annotations

import ctypes
import errno
import json
import os
import stat
import uuid
from collections.abc import Callable
from contextlib import suppress
from pathlib import Path
from typing import Any

FILE_MODE = 0o600
DIR_MODE = 0o700
DEFAULT_MAX_JSON_BYTES = 4 * 2**20
RENAME_NOREPLACE = 1


class SecureFileError(ValueError):
    """A dispatcher file or directory violated the trusted file contract."""


def _no_symlink_components(path: Path) -> None:
    absolute = path.absolute()
    current = Path(absolute.anchor)
    for component in absolute.parts[1:]:
        current /= component
        try:
            info = os.lstat(current)
        except FileNotFoundError:
            break
        if stat.S_ISLNK(info.st_mode):
            raise SecureFileError(f"symlink path component is not allowed: {current}")


def require_secure_directory(path: Path, *, create: bool = False) -> int:
    _no_symlink_components(path)
    if create:
        path.mkdir(mode=DIR_MODE, parents=False, exist_ok=True)
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(path, flags)
    try:
        info = os.fstat(fd)
        if not stat.S_ISDIR(info.st_mode):
            raise SecureFileError(f"not a directory: {path}")
        if stat.S_IMODE(info.st_mode) != DIR_MODE:
            raise SecureFileError(f"directory mode must be 0700: {path}")
        if info.st_uid != os.getuid():
            raise SecureFileError(f"directory must be owned by uid {os.getuid()}: {path}")
        if info.st_nlink < 1:
            raise SecureFileError(f"directory has an invalid link count: {path}")
        return fd
    except Exception:
        os.close(fd)
        raise


def ensure_child_directory(parent_fd: int, name: str) -> int:
    validate_name(name)
    try:
        os.mkdir(name, DIR_MODE, dir_fd=parent_fd)
        os.fsync(parent_fd)
    except FileExistsError:
        pass
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0) | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(name, flags, dir_fd=parent_fd)
    try:
        info = os.fstat(fd)
        if not stat.S_ISDIR(info.st_mode):
            raise SecureFileError(f"{name} is not a directory")
        if stat.S_IMODE(info.st_mode) != DIR_MODE:
            raise SecureFileError(f"directory mode must be 0700: {name}")
        if info.st_uid != os.getuid():
            raise SecureFileError(f"directory must be owned by uid {os.getuid()}: {name}")
        return fd
    except Exception:
        os.close(fd)
        raise


def validate_name(name: str) -> None:
    if not name or name in {".", ".."} or "/" in name or "\x00" in name:
        raise SecureFileError(f"unsafe file name: {name!r}")


def validate_committed_json_name(name: str) -> None:
    validate_name(name)
    if name.startswith(".") or not name.endswith(".json") or name.endswith(".tmp.json"):
        raise SecureFileError(f"not a committed JSON file name: {name!r}")


def _open_regular_file(
    dir_fd: int,
    name: str,
    *,
    max_bytes: int,
    expected_mode: int = FILE_MODE,
) -> int:
    validate_name(name)
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(name, flags, dir_fd=dir_fd)
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode):
            raise SecureFileError(f"not a regular file: {name}")
        if stat.S_IMODE(info.st_mode) != expected_mode:
            raise SecureFileError(f"file mode must be {expected_mode:04o}: {name}")
        if info.st_uid != os.getuid():
            raise SecureFileError(f"file must be owned by uid {os.getuid()}: {name}")
        if info.st_nlink != 1:
            raise SecureFileError(f"file must have exactly one link: {name}")
        if info.st_size > max_bytes:
            raise SecureFileError(f"file exceeds {max_bytes} bytes: {name}")
        return fd
    except Exception:
        os.close(fd)
        raise


def read_secure_bytes(dir_fd: int, name: str, *, max_bytes: int) -> bytes:
    fd = _open_regular_file(dir_fd, name, max_bytes=max_bytes)
    try:
        chunks: list[bytes] = []
        remaining = max_bytes + 1
        while remaining:
            chunk = os.read(fd, min(remaining, 64 * 1024))
            if not chunk:
                break
            chunks.append(chunk)
            remaining -= len(chunk)
        data = b"".join(chunks)
        if len(data) > max_bytes:
            raise SecureFileError(f"file exceeds {max_bytes} bytes: {name}")
        return data
    finally:
        os.close(fd)


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise SecureFileError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def loads_strict_json(text: str) -> Any:
    return json.loads(text, object_pairs_hook=_reject_duplicate_keys)


def load_secure_json(dir_fd: int, name: str, *, max_bytes: int) -> Any:
    data = read_secure_bytes(dir_fd, name, max_bytes=max_bytes)
    try:
        text = data.decode("utf-8")
        return loads_strict_json(text)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SecureFileError(f"invalid complete UTF-8 JSON in {name}: {exc}") from exc


def _write_all(fd: int, data: bytes) -> None:
    offset = 0
    while offset < len(data):
        offset += os.write(fd, data[offset:])


def atomic_write_bytes(
    dir_fd: int,
    final_name: str,
    data: bytes,
    *,
    no_overwrite: bool,
) -> bool:
    validate_name(final_name)
    temp_name = f".{final_name}.{uuid.uuid4().hex}.tmp"
    fd = os.open(
        temp_name,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
        FILE_MODE,
        dir_fd=dir_fd,
    )
    failure: BaseException | None = None
    try:
        _write_all(fd, data)
        os.fsync(fd)
    except BaseException as exc:
        failure = exc
    try:
        os.close(fd)
    except BaseException as exc:
        if failure is None:
            failure = exc
    if failure is not None:
        with suppress(OSError):
            os.unlink(temp_name, dir_fd=dir_fd)
            os.fsync(dir_fd)
        raise failure

    try:
        if no_overwrite:
            try:
                _renameat2_noreplace(dir_fd, temp_name, dir_fd, final_name)
            except FileExistsError:
                os.unlink(temp_name, dir_fd=dir_fd)
                os.fsync(dir_fd)
                return False
        else:
            os.replace(temp_name, final_name, src_dir_fd=dir_fd, dst_dir_fd=dir_fd)
        os.fsync(dir_fd)
        return True
    except BaseException:
        with suppress(OSError):
            os.unlink(temp_name, dir_fd=dir_fd)
        with suppress(OSError):
            os.fsync(dir_fd)
        raise


def atomic_write_json(
    dir_fd: int,
    final_name: str,
    value: Any,
    *,
    no_overwrite: bool,
) -> bool:
    data = json.dumps(
        value,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8") + b"\n"
    return atomic_write_bytes(dir_fd, final_name, data, no_overwrite=no_overwrite)


def claim_no_replace(
    source_fd: int,
    dest_fd: int,
    name: str,
    *,
    max_bytes: int = DEFAULT_MAX_JSON_BYTES,
) -> bool:
    validate_committed_json_name(name)
    source_file_fd = _open_regular_file(source_fd, name, max_bytes=max_bytes)
    source_info = os.fstat(source_file_fd)
    try:
        try:
            _renameat2_noreplace(source_fd, name, dest_fd, name)
        except FileExistsError:
            return False
        claimed_fd = os.open(
            name,
            os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0),
            dir_fd=dest_fd,
        )
        try:
            claimed_info = os.fstat(claimed_fd)
            if (claimed_info.st_dev, claimed_info.st_ino) != (
                source_info.st_dev,
                source_info.st_ino,
            ):
                raise SecureFileError("plan changed while it was being claimed")
        finally:
            os.close(claimed_fd)
        os.fsync(dest_fd)
        os.fsync(source_fd)
        return True
    finally:
        os.close(source_file_fd)


def _renameat2_noreplace(
    source_fd: int,
    source_name: str,
    dest_fd: int,
    dest_name: str,
) -> None:
    libc = ctypes.CDLL(None, use_errno=True)
    renameat2 = getattr(libc, "renameat2", None)
    if renameat2 is None:
        raise SecureFileError("renameat2(RENAME_NOREPLACE) is unavailable")
    renameat2.argtypes = [
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_uint,
    ]
    renameat2.restype = ctypes.c_int
    result = renameat2(
        source_fd,
        os.fsencode(source_name),
        dest_fd,
        os.fsencode(dest_name),
        RENAME_NOREPLACE,
    )
    if result == 0:
        return
    error = ctypes.get_errno()
    if error == errno.EEXIST:
        raise FileExistsError(error, os.strerror(error), dest_name)
    unsupported = {
        errno.ENOSYS,
        errno.EXDEV,
        errno.EINVAL,
        getattr(errno, "EOPNOTSUPP", errno.ENOTSUP),
        errno.ENOTSUP,
    }
    if error in unsupported:
        raise SecureFileError(
            "renameat2(RENAME_NOREPLACE) is unsupported for the plan and claimed directories"
        )
    raise OSError(error, os.strerror(error), source_name)


def secure_control_exists(root_fd: int, name: str) -> bool:
    validate_name(name)
    try:
        fd = _open_regular_file(root_fd, name, max_bytes=0)
    except FileNotFoundError:
        return False
    else:
        os.close(fd)
        return True


def with_open_fd(fd: int, callback: Callable[[int], Any]) -> Any:
    try:
        return callback(fd)
    finally:
        os.close(fd)


def exclusive_lock(root_fd: int, name: str = "dispatcher.lock") -> int:
    validate_name(name)
    flags = os.O_WRONLY | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0)
    fd = os.open(name, flags, FILE_MODE, dir_fd=root_fd)
    info = os.fstat(fd)
    if not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid():
        os.close(fd)
        raise SecureFileError("dispatcher lock is not an owner-controlled regular file")
    os.fchmod(fd, FILE_MODE)
    try:
        import fcntl

        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except (ImportError, BlockingIOError) as exc:
        os.close(fd)
        if isinstance(exc, BlockingIOError) or getattr(exc, "errno", None) in {
            errno.EACCES,
            errno.EAGAIN,
        }:
            raise SecureFileError("another dispatcher holds the root lock") from exc
        raise SecureFileError("platform does not provide a dispatcher process lock") from exc
    return fd


def open_trusted_executable(path: Path) -> int:
    if not path.is_absolute():
        raise SecureFileError("trusted executable path must be absolute")
    _no_symlink_components(path)
    fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode):
            raise SecureFileError("Warmer executable must be a regular file")
        if info.st_uid not in {0, os.getuid()}:
            raise SecureFileError("Warmer executable must be owned by root or dispatcher uid")
        if info.st_nlink != 1:
            raise SecureFileError("Warmer executable must have exactly one link")
        if info.st_mode & 0o022:
            raise SecureFileError("Warmer executable cannot be group/world writable")
        if not info.st_mode & 0o111:
            raise SecureFileError("Warmer executable is not executable")
        return fd
    except Exception:
        os.close(fd)
        raise
