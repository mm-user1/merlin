"""Cooperative process exclusion for one Pattern Lab data root.

One coarse exclusive OS lock guards every public pack-level operation, read and
write alike.  The coordination file ``<data-root>/.pack-lock`` is created once
and then stays in place: it is never unlinked or recreated to release the guard,
so an aborted initial collect leaves a reusable lock-only root behind.

The guard is process-lifetime only.  There is no PID lease, no expiry and no
lock stealing: OS process exit releases it.  Acquisition is always nonblocking,
so a busy root is reported instead of queued, and two independent readers
conflict deliberately.  This module never imports PyArrow or reads market data.
"""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
import os
from pathlib import Path
from typing import Iterator

from . import PatternLabBusyError, PatternLabDataError

LOCK_NAME = ".pack-lock"
# Windows locks a byte range rather than the whole file: use one fixed byte at a
# fixed offset so every participant contends over exactly the same range.
LOCK_OFFSET = 0
LOCK_LENGTH = 1


def lock_path(data_root: Path) -> Path:
    """Return the persistent coordination file inside one data root."""
    return Path(data_root) / LOCK_NAME


def resolve_root_identity(data_root: Path) -> str:
    """Return the canonical ``normcase(resolve())`` identity journalled for a root.

    A root that does not exist yet resolves through its existing parent, so an
    alias in the parent chain is resolved before the first creation.
    """
    path = Path(data_root)
    if path.exists() or path.is_symlink():
        resolved = path.resolve()
    else:
        parent = path.parent
        try:
            resolved = parent.resolve(strict=True) / path.name
        except (FileNotFoundError, OSError) as exc:
            raise PatternLabDataError(
                f"{parent}: the parent directory of the data root does not exist; create it first.",
                error_code="missing_parent_directory",
            ) from exc
    return os.path.normcase(str(resolved))


def _reject_symlink(path: Path, kind: str) -> None:
    if path.is_symlink():
        raise PatternLabDataError(
            f"{path}: the {kind} must not be a symlink.", error_code="unsafe_path"
        )


def _acquire(fd: int, path: Path) -> None:
    """Take the exclusive OS guard without blocking, or report the root as busy."""
    busy = PatternLabBusyError(
        f"{path}: another process holds the Pattern Lab pack lock for this data root. "
        "Wait for that operation or read session to finish; there is no wait option and no "
        "lock stealing."
    )
    if os.name == "nt":  # pragma: no cover - exercised by the Windows commands in the docs
        import msvcrt

        os.lseek(fd, LOCK_OFFSET, os.SEEK_SET)
        try:
            msvcrt.locking(fd, msvcrt.LK_NBLCK, LOCK_LENGTH)
        except OSError as exc:
            raise busy from exc
        return
    import fcntl

    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError as exc:
        raise busy from exc


def _release(fd: int) -> None:
    if os.name == "nt":  # pragma: no cover - Windows branch
        import msvcrt

        os.lseek(fd, LOCK_OFFSET, os.SEEK_SET)
        try:
            msvcrt.locking(fd, msvcrt.LK_UNLCK, LOCK_LENGTH)
        except OSError:
            pass
        return
    import fcntl

    try:
        fcntl.flock(fd, fcntl.LOCK_UN)
    except OSError:
        pass


@dataclass(frozen=True)
class PackGuard:
    """The held exclusion guard for one resolved data root."""

    root: Path
    identity: str
    _fd: int

    def verify_root_unchanged(self) -> None:
        """Fail if the locked root was renamed, moved, deleted or replaced."""
        path = lock_path(self.root)
        try:
            current = os.stat(path)
        except OSError as exc:
            raise PatternLabDataError(
                f"{self.root}: the locked data root disappeared during the operation; restore it "
                "at its original location before recovering.",
                error_code="root_moved",
            ) from exc
        held = os.fstat(self._fd)
        if (current.st_dev, current.st_ino) != (held.st_dev, held.st_ino):
            raise PatternLabDataError(
                f"{self.root}: the locked data root was replaced during the operation; restore the "
                "original directory before recovering.",
                error_code="root_moved",
            )


@contextmanager
def pack_guard(data_root: Path, *, create_root: bool = False) -> Iterator[PackGuard]:
    """Hold the exclusive guard for ``data_root`` for the duration of the block.

    ``create_root`` creates a missing root inside an existing parent; every other
    caller requires the root to exist already.  A read never creates a missing
    root, but it does create or open ``.pack-lock`` inside an existing one,
    because the cooperative API has no shared-lock or bypass mode.
    """
    root = Path(data_root)
    identity = resolve_root_identity(root)
    if create_root and not root.is_dir():
        try:
            root.mkdir(exist_ok=True)
        except OSError as exc:
            raise PatternLabDataError(
                f"{root}: cannot create the data root ({exc}).", error_code="unwritable_root"
            ) from exc
    if not root.is_dir():
        raise PatternLabDataError(
            f"{root}: data root is not an existing directory.", error_code="missing_data_root"
        )

    path = lock_path(root)
    _reject_symlink(path, "pack lock file")
    flags = os.O_RDWR | os.O_CREAT  # never O_TRUNC: an existing lock file is reused as is
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    try:
        fd = os.open(path, flags, 0o644)
    except OSError as exc:
        raise PatternLabDataError(
            f"{path}: cannot open or create the Pattern Lab pack lock ({exc}). Every pack-level "
            "operation, including a read, needs write permission inside the data root; fully "
            "read-only mounted packs are unsupported by this cooperative API.",
            error_code="unwritable_root",
        ) from exc
    try:
        _acquire(fd, path)
    except BaseException:
        os.close(fd)
        raise
    try:
        yield PackGuard(root=root, identity=identity, _fd=fd)
    finally:
        _release(fd)
        os.close(fd)
