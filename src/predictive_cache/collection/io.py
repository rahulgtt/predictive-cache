"""Owner-only, atomic evidence IO for the controlled collection harness."""

from __future__ import annotations

import hashlib
import json
import os
import stat
import tempfile
from pathlib import Path
from typing import Iterable, TypeVar

from pydantic import BaseModel

T = TypeVar("T", bound=BaseModel)


def canonical_json(value) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def ensure_private_directory(path: Path) -> Path:
    path = path.absolute()
    if path.exists():
        if path.is_symlink() or not path.is_dir():
            raise ValueError("evidence directory must be a real directory")
        mode = stat.S_IMODE(path.stat().st_mode)
        if mode & 0o077:
            raise PermissionError("evidence directory must be owner-only (0700)")
    else:
        path.mkdir(parents=True, mode=0o700)
    for parent in (path, *path.parents):
        if parent.is_symlink():
            raise ValueError("evidence path must not traverse symlinks")
    return path


def atomic_write_model(path: Path, model: BaseModel, *, immutable: bool = True) -> str:
    parent = ensure_private_directory(path.parent)
    if path.exists() or path.is_symlink():
        if immutable:
            raise FileExistsError(f"immutable evidence already exists: {path}")
        if path.is_symlink() or not path.is_file():
            raise ValueError("evidence target must be a regular file")
    payload = (canonical_json(model.model_dump(mode="json")) + "\n").encode()
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=parent)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "wb", closefd=True) as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        if immutable:
            os.link(temporary, path, follow_symlinks=False)
            os.unlink(temporary)
        else:
            os.replace(temporary, path)
        directory_fd = os.open(parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    except BaseException:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
        raise
    return sha256(path)


def atomic_write_bytes(path: Path, payload: bytes) -> str:
    parent = ensure_private_directory(path.parent)
    if path.exists() or path.is_symlink():
        raise FileExistsError(f"immutable evidence already exists: {path}")
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=parent)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "wb", closefd=True) as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.link(temporary, path, follow_symlinks=False)
        os.unlink(temporary)
        directory_fd = os.open(parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    except BaseException:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
        raise
    return sha256(path)


def append_model(path: Path, model: BaseModel) -> None:
    parent = ensure_private_directory(path.parent)
    flags = os.O_WRONLY | os.O_APPEND
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        fd = os.open(path, flags | os.O_CREAT | os.O_EXCL, 0o600)
        created = True
    except FileExistsError:
        fd = os.open(path, flags)
        created = False
    try:
        mode = stat.S_IMODE(os.fstat(fd).st_mode)
        if mode & 0o077:
            raise PermissionError("evidence file must be owner-only (0600)")
        payload = (canonical_json(model.model_dump(mode="json")) + "\n").encode()
        written = os.write(fd, payload)
        if written != len(payload):
            raise OSError("short evidence write")
        os.fsync(fd)
    finally:
        os.close(fd)
    if created:
        directory_fd = os.open(parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)


def read_models(path: Path, model: type[T]) -> list[T]:
    if path.is_symlink() or not path.is_file():
        raise ValueError("evidence input must be a regular file")
    records: list[T] = []
    with path.open("r", encoding="utf-8") as stream:
        for line_number, line in enumerate(stream, start=1):
            try:
                raw = json.loads(line)
                records.append(model.model_validate(raw))
            except (json.JSONDecodeError, ValueError) as error:
                raise ValueError(f"{path}:{line_number}: invalid evidence record") from error
    return records


def write_jsonl(path: Path, models: Iterable[BaseModel]) -> str:
    parent = ensure_private_directory(path.parent)
    if path.exists() or path.is_symlink():
        raise FileExistsError(f"immutable evidence already exists: {path}")
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=parent)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "wb", closefd=True) as stream:
            for model in models:
                stream.write(
                    (canonical_json(model.model_dump(mode="json")) + "\n").encode()
                )
            stream.flush()
            os.fsync(stream.fileno())
        os.link(temporary, path, follow_symlinks=False)
        os.unlink(temporary)
        directory_fd = os.open(parent, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    except BaseException:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
        raise
    return sha256(path)
