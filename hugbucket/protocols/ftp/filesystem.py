"""Virtual FTP filesystem backed by StorageBackend."""

from __future__ import annotations

import errno
import io
import os
import stat
import time
from datetime import datetime

try:
    from pyftpdlib.filesystems import AbstractedFS
except ImportError as exc:  # pragma: no cover - depends on environment
    AbstractedFS = object  # type: ignore[assignment,misc]
    _PYFTPDLIB_IMPORT_ERROR: Exception | None = exc
else:
    _PYFTPDLIB_IMPORT_ERROR = None

from hugbucket.core.backend import StorageBackend
from hugbucket.core.models import BucketFile
from hugbucket.protocols.ftp.runtime import BackendLoopRunner

DIR_MARKER_FILENAME = ".hugbucket_keep"


def _parse_iso_timestamp(ts: str) -> float:
    if not ts:
        return time.time()
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return time.time()


def _normalize(path: str) -> str:
    """Normalize a virtual FTP path and keep it rooted at /."""
    if not path:
        return "/"
    p = path.replace("\\", "/")
    if not p.startswith("/"):
        p = "/" + p

    parts: list[str] = []
    for segment in p.split("/"):
        if segment in ("", "."):
            continue
        if segment == "..":
            if parts:
                parts.pop()
            continue
        parts.append(segment)
    return "/" + "/".join(parts)


def _split_bucket_key(path: str) -> tuple[str, str]:
    norm = _normalize(path)
    if norm == "/":
        return "", ""
    parts = norm.lstrip("/").split("/", 1)
    bucket = parts[0]
    key = parts[1] if len(parts) > 1 else ""
    return bucket, key


def _as_dir_prefix(key: str) -> str:
    key = key.strip("/")
    return f"{key}/" if key else ""


def _stat_result(mode: int, size: int = 0, mtime: float | None = None) -> os.stat_result:
    ts = float(mtime if mtime is not None else time.time())
    return os.stat_result(
        (
            int(mode),
            int(0),
            int(0),
            int(1),
            int(0),
            int(0),
            int(size),
            float(ts),
            float(ts),
            float(ts),
        )
    )


class _UploadBuffer(io.BytesIO):
    """In-memory upload buffer flushed to backend on close."""

    def __init__(
        self,
        runner: BackendLoopRunner,
        backend: StorageBackend,
        bucket: str,
        key: str,
        initial: bytes = b"",
        name: str | None = None,
    ) -> None:
        super().__init__(initial)
        self._runner = runner
        self._backend = backend
        self._bucket = bucket
        self._key = key
        self.name = name or f"/{bucket}/{key}"

    def close(self) -> None:
        if not self.closed:
            data = self.getvalue()
            self._runner.call(self._backend.put_object(self._bucket, self._key, data))
        super().close()


class _DownloadBuffer(io.BytesIO):
    """Read-only bytes buffer with no-op close for pyftpdlib callbacks."""

    def __init__(self, data: bytes, *, name: str) -> None:
        super().__init__(data)
        self.name = name

    def close(self) -> None:
        super().close()


class HugBucketFTPFilesystem(AbstractedFS):
    """pyftpdlib filesystem facade backed by StorageBackend."""

    def __init__(self, root: str, cmd_channel) -> None:  # type: ignore[no-untyped-def]
        if _PYFTPDLIB_IMPORT_ERROR is not None:
            raise RuntimeError(
                "FTP dependencies are missing. Install with project dependencies "
                "(requires pyftpdlib)."
            ) from _PYFTPDLIB_IMPORT_ERROR
        super().__init__(root, cmd_channel)
        runner = getattr(cmd_channel, "backend_runner", None)
        if runner is None:
            raise RuntimeError("FTP handler missing backend_runner")
        self._runner: BackendLoopRunner = runner
        self._backend: StorageBackend = runner.backend

    # ---- path mapping ----

    def ftpnorm(self, ftppath: str) -> str:
        return _normalize(super().ftpnorm(ftppath))

    def ftp2fs(self, ftppath: str) -> str:
        return self.ftpnorm(ftppath)

    def fs2ftp(self, fspath: str) -> str:
        return _normalize(fspath)

    def validpath(self, path: str) -> bool:
        return _normalize(path).startswith("/")

    def realpath(self, path: str) -> str:
        return _normalize(path)

    # ---- internal helpers ----

    def _bucket_exists(self, bucket: str) -> bool:
        return self._runner.call(self._backend.head_bucket(bucket)) is not None

    def _list_pages(
        self,
        bucket: str,
        *,
        prefix: str,
        delimiter: str,
    ) -> tuple[list[BucketFile], list[str]]:
        files: list[BucketFile] = []
        common_prefixes: list[str] = []
        token = ""

        while True:
            page = self._runner.call(
                self._backend.list_objects(
                    bucket,
                    prefix=prefix,
                    delimiter=delimiter,
                    max_keys=1000,
                    continuation_token=token,
                )
            )
            files.extend(page.get("contents", []))
            common_prefixes.extend(page.get("common_prefixes", []))
            if not page.get("is_truncated"):
                break
            token = page.get("next_continuation_token") or ""
            if not token:
                break

        return files, common_prefixes

    def _list_all_files_recursive(self, bucket: str, prefix: str) -> list[BucketFile]:
        files, _ = self._list_pages(bucket, prefix=prefix, delimiter="")
        return files

    # ---- file system methods ----

    def listdir(self, path: str) -> list[str]:
        ftp_path = self.ftpnorm(path)
        if ftp_path == "/":
            buckets = self._runner.call(self._backend.list_buckets())
            return sorted(
                [b.id.split("/")[-1] if "/" in b.id else b.id for b in buckets]
            )

        bucket, key = _split_bucket_key(ftp_path)
        if not bucket or not self._bucket_exists(bucket):
            raise FileNotFoundError(path)

        prefix = _as_dir_prefix(key)
        files, common_prefixes = self._list_pages(
            bucket,
            prefix=prefix,
            delimiter="/",
        )

        names: set[str] = set()
        for item in files:
            rel = item.path[len(prefix) :] if prefix else item.path
            if rel and "/" not in rel:
                names.add(rel)
        for cp in common_prefixes:
            rel = cp[len(prefix) :] if prefix else cp
            rel = rel.strip("/")
            if rel:
                names.add(rel)

        if not names and self.isfile(ftp_path):
            raise NotADirectoryError(path)

        if not names and not self.isdir(ftp_path):
            raise FileNotFoundError(path)

        return sorted(names)

    def stat(self, path: str) -> os.stat_result:
        ftp_path = self.ftpnorm(path)
        if ftp_path == "/":
            return _stat_result(stat.S_IFDIR | 0o755)

        bucket, key = _split_bucket_key(ftp_path)
        if not bucket:
            raise FileNotFoundError(path)

        if key:
            info = self._runner.call(self._backend.head_object(bucket, key))
            if info is not None:
                mtime = _parse_iso_timestamp(info.mtime or info.uploaded_at)
                return _stat_result(stat.S_IFREG | 0o644, size=info.size, mtime=mtime)

        if key == "":
            bucket_info = self._runner.call(self._backend.head_bucket(bucket))
            if bucket_info is not None:
                return _stat_result(stat.S_IFDIR | 0o755)

        if self.isdir(ftp_path):
            return _stat_result(stat.S_IFDIR | 0o755)

        raise FileNotFoundError(path)

    def lstat(self, path: str) -> os.stat_result:
        return self.stat(path)

    def isfile(self, path: str) -> bool:
        ftp_path = self.ftpnorm(path)
        bucket, key = _split_bucket_key(ftp_path)
        if not bucket or not key:
            return False
        return self._runner.call(self._backend.head_object(bucket, key)) is not None

    def isdir(self, path: str) -> bool:
        ftp_path = self.ftpnorm(path)
        if ftp_path == "/":
            return True

        bucket, key = _split_bucket_key(ftp_path)
        if not bucket:
            return False

        if key == "":
            return self._bucket_exists(bucket)

        return self._runner.call(
            self._backend.head_directory(bucket, _as_dir_prefix(key))
        )

    def islink(self, path: str) -> bool:
        return False

    def lexists(self, path: str) -> bool:
        return self.isfile(path) or self.isdir(path)

    def getsize(self, path: str) -> int:
        return self.stat(path).st_size

    def getmtime(self, path: str) -> float:
        return self.stat(path).st_mtime

    def open(self, filename: str, mode: str):  # type: ignore[override]
        ftp_path = self.ftpnorm(filename)
        bucket, key = _split_bucket_key(ftp_path)
        if not bucket or not key:
            raise IsADirectoryError(filename)

        write_mode = any(flag in mode for flag in ("w", "a", "+"))

        if not write_mode:
            data = self._runner.call(self._backend.get_object(bucket, key))
            if data is None:
                raise FileNotFoundError(filename)
            return _DownloadBuffer(data, name=ftp_path)

        existing = b""
        if "a" in mode or ("r" in mode and "+" in mode):
            existing = self._runner.call(self._backend.get_object(bucket, key)) or b""
            if "r" in mode and "+" in mode and existing == b"":
                raise FileNotFoundError(filename)

        handle = _UploadBuffer(
            self._runner,
            self._backend,
            bucket,
            key,
            initial=existing,
            name=ftp_path,
        )
        if "a" in mode:
            handle.seek(0, io.SEEK_END)
        return handle

    def mkstemp(
        self,
        suffix: str = "",
        prefix: str = "",
        dir: str | None = None,
        mode: str = "wb",
    ):
        ftp_dir = self.ftpnorm(dir or self.cwd)
        bucket, key = _split_bucket_key(ftp_dir)
        if not bucket:
            raise PermissionError("Cannot create temporary file in root")

        clean_prefix = prefix.replace("/", "_")
        key_prefix = _as_dir_prefix(key)
        for attempt in range(100):
            unique = f"{int(time.time() * 1_000_000)}-{attempt}"
            temp_key = f"{key_prefix}{clean_prefix}{unique}{suffix}"
            exists = self._runner.call(self._backend.head_object(bucket, temp_key))
            if exists is None:
                handle = _UploadBuffer(
                    self._runner,
                    self._backend,
                    bucket,
                    temp_key,
                    initial=b"",
                    name=f"/{bucket}/{temp_key}",
                )
                if "a" in mode:
                    handle.seek(0, io.SEEK_END)
                return handle

        raise OSError(errno.EEXIST, "No usable unique file name found")

    def chdir(self, path: str) -> None:
        ftp_path = self.ftpnorm(path)
        if not self.isdir(ftp_path):
            raise FileNotFoundError(path)
        self.cwd = ftp_path

    def mkdir(self, path: str) -> None:
        ftp_path = self.ftpnorm(path)
        bucket, key = _split_bucket_key(ftp_path)
        if not bucket:
            raise PermissionError("Cannot create root")

        if self.lexists(ftp_path):
            raise FileExistsError(path)

        if key == "":
            self._runner.call(self._backend.create_bucket(bucket))
            return

        self._runner.call(self._backend.put_object(bucket, _as_dir_prefix(key), b""))

    def rmdir(self, path: str) -> None:
        ftp_path = self.ftpnorm(path)
        bucket, key = _split_bucket_key(ftp_path)
        if not bucket:
            raise PermissionError("Cannot remove root")

        if key == "":
            self._runner.call(self._backend.delete_bucket(bucket))
            return

        prefix = _as_dir_prefix(key)
        files, common_prefixes = self._list_pages(bucket, prefix=prefix, delimiter="/")
        if files or common_prefixes:
            raise OSError("Directory not empty")

        self._runner.call(self._backend.delete_object(bucket, prefix))

    def remove(self, path: str) -> None:
        ftp_path = self.ftpnorm(path)
        bucket, key = _split_bucket_key(ftp_path)
        if not bucket or not key:
            raise IsADirectoryError(path)
        if key.endswith("/") or self.isdir(ftp_path):
            raise IsADirectoryError(path)
        self._runner.call(self._backend.delete_object(bucket, key))

    def rename(self, src: str, dst: str) -> None:
        src_path = self.ftpnorm(src)
        dst_path = self.ftpnorm(dst)
        src_bucket, src_key = _split_bucket_key(src_path)
        dst_bucket, dst_key = _split_bucket_key(dst_path)

        if not src_bucket or not src_key or not dst_bucket or not dst_key:
            raise PermissionError("Renaming bucket roots is not supported")

        if self.isdir(src_path):
            self._rename_directory(src_bucket, src_key, dst_bucket, dst_key)
            return

        self._runner.call(
            self._backend.copy_object(src_bucket, src_key, dst_bucket, dst_key)
        )
        self._runner.call(self._backend.delete_object(src_bucket, src_key))

    def _rename_directory(
        self,
        src_bucket: str,
        src_key: str,
        dst_bucket: str,
        dst_key: str,
    ) -> None:
        src_prefix = _as_dir_prefix(src_key)
        dst_prefix = _as_dir_prefix(dst_key)

        files = self._list_all_files_recursive(src_bucket, src_prefix)
        for item in files:
            rel = item.path[len(src_prefix) :]
            new_key = dst_prefix + rel
            self._runner.call(
                self._backend.copy_object(src_bucket, item.path, dst_bucket, new_key)
            )

        marker_src = src_prefix + DIR_MARKER_FILENAME
        marker_info = self._runner.call(
            self._backend.head_object(src_bucket, marker_src)
        )
        if marker_info is not None:
            marker_dst = dst_prefix + DIR_MARKER_FILENAME
            self._runner.call(
                self._backend.copy_object(src_bucket, marker_src, dst_bucket, marker_dst)
            )

        for item in files:
            self._runner.call(self._backend.delete_object(src_bucket, item.path))

        if marker_info is not None:
            self._runner.call(self._backend.delete_object(src_bucket, marker_src))

    def chmod(self, path: str, mode: int) -> None:
        raise PermissionError("chmod is not supported")

    def readlink(self, path: str) -> str:
        raise OSError("readlink is not supported")
