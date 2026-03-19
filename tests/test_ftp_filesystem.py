"""Unit tests for FTP virtual filesystem backed by StorageBackend."""

from __future__ import annotations

import asyncio
import io
import inspect
from unittest.mock import AsyncMock, MagicMock

import pytest

from hugbucket.core.models import BucketFile, BucketInfo
from hugbucket.protocols.ftp.filesystem import HugBucketFTPFilesystem


class _Runner:
    def __init__(self, backend) -> None:  # type: ignore[no-untyped-def]
        self.backend = backend

    def call(self, awaitable):  # type: ignore[no-untyped-def]
        if inspect.isawaitable(awaitable):
            async def _await_value() -> object:
                return await awaitable

            return asyncio.run(_await_value())
        return awaitable


class _CmdChannel:
    def __init__(self, runner: _Runner) -> None:
        self.backend_runner = runner
        self.use_gmt_times = True
        self.encoding = "utf-8"
        self.unicode_errors = "replace"


def _build_fs(backend: MagicMock) -> HugBucketFTPFilesystem:
    runner = _Runner(backend)
    cmd = _CmdChannel(runner)
    return HugBucketFTPFilesystem("/", cmd)


@pytest.fixture
def backend() -> MagicMock:
    b = MagicMock()
    b.list_buckets = AsyncMock(
        return_value=[
            BucketInfo(id="ns/a", private=False, created_at="", size=0, total_files=0),
            BucketInfo(id="ns/b", private=False, created_at="", size=0, total_files=0),
        ]
    )
    b.head_bucket = AsyncMock(
        side_effect=lambda name: BucketInfo(
            id=f"ns/{name}", private=False, created_at="", size=0, total_files=0
        )
    )
    b.list_objects = AsyncMock(
        return_value={
            "contents": [],
            "common_prefixes": [],
            "is_truncated": False,
            "next_continuation_token": None,
        }
    )
    b.head_directory = AsyncMock(return_value=False)
    b.head_object = AsyncMock(return_value=None)
    b.get_object = AsyncMock(return_value=None)
    b.put_object = AsyncMock(return_value={"ETag": '"x"', "size": 0})
    b.delete_object = AsyncMock()
    b.delete_bucket = AsyncMock()
    b.create_bucket = AsyncMock(return_value="")
    b.copy_object = AsyncMock(return_value={"ETag": '"x"'})
    return b


def test_list_root_returns_bucket_names(backend: MagicMock) -> None:
    fs = _build_fs(backend)
    assert fs.listdir("/") == ["a", "b"]


def test_isdir_root_and_bucket(backend: MagicMock) -> None:
    fs = _build_fs(backend)
    assert fs.isdir("/") is True
    assert fs.isdir("/a") is True


def test_open_read_existing_file(backend: MagicMock) -> None:
    backend.get_object = AsyncMock(return_value=b"hello")
    fs = _build_fs(backend)
    handle = fs.open("/a/hello.txt", "rb")
    assert isinstance(handle, io.BytesIO)
    assert handle.read() == b"hello"
    assert handle.name == "/a/hello.txt"


def test_open_append_mode_starts_at_end(backend: MagicMock) -> None:
    backend.get_object = AsyncMock(return_value=b"ab")
    fs = _build_fs(backend)
    handle = fs.open("/a/new.txt", "ab")
    handle.write(b"cd")
    handle.close()
    backend.put_object.assert_awaited_once_with("a", "new.txt", b"abcd")


def test_open_read_write_requires_existing_file(backend: MagicMock) -> None:
    backend.get_object = AsyncMock(return_value=None)
    fs = _build_fs(backend)
    with pytest.raises(FileNotFoundError):
        fs.open("/a/missing.txt", "r+b")


def test_open_read_write_updates_existing_file(backend: MagicMock) -> None:
    backend.get_object = AsyncMock(return_value=b"abc")
    fs = _build_fs(backend)
    handle = fs.open("/a/file.txt", "r+b")
    assert handle.name == "/a/file.txt"
    handle.seek(1)
    handle.write(b"Z")
    handle.close()
    backend.put_object.assert_awaited_once_with("a", "file.txt", b"aZc")


def test_open_write_flushes_to_backend(backend: MagicMock) -> None:
    fs = _build_fs(backend)
    handle = fs.open("/a/new.txt", "wb")
    handle.write(b"payload")
    handle.close()
    backend.put_object.assert_awaited_once_with("a", "new.txt", b"payload")


def test_mkdir_bucket_calls_create_bucket(backend: MagicMock) -> None:
    backend.head_bucket = AsyncMock(return_value=None)
    fs = _build_fs(backend)
    fs.mkdir("/new-bucket")
    backend.create_bucket.assert_awaited_once_with("new-bucket")


def test_mkdir_directory_calls_marker_put(backend: MagicMock) -> None:
    fs = _build_fs(backend)
    fs.mkdir("/a/folder")
    backend.put_object.assert_awaited_once_with("a", "folder/", b"")


def test_remove_calls_delete_object(backend: MagicMock) -> None:
    fs = _build_fs(backend)
    fs.remove("/a/file.txt")
    backend.delete_object.assert_awaited_once_with("a", "file.txt")


def test_remove_directory_raises_is_a_directory(backend: MagicMock) -> None:
    backend.head_directory = AsyncMock(return_value=True)
    fs = _build_fs(backend)
    with pytest.raises(IsADirectoryError):
        fs.remove("/a/dir")


def test_listdir_file_raises_not_a_directory(backend: MagicMock) -> None:
    backend.head_object = AsyncMock(
        side_effect=lambda bucket, key: BucketFile(type="file", path=key, size=1)
    )
    backend.head_directory = AsyncMock(return_value=False)
    fs = _build_fs(backend)
    with pytest.raises(NotADirectoryError):
        fs.listdir("/a/file.txt")


def test_rename_file_copy_then_delete(backend: MagicMock) -> None:
    backend.head_object = AsyncMock(
        side_effect=lambda bucket, key: BucketFile(type="file", path=key, size=1)
    )
    backend.head_directory = AsyncMock(return_value=False)
    fs = _build_fs(backend)
    fs.rename("/a/src.txt", "/a/dst.txt")
    backend.copy_object.assert_awaited_once_with("a", "src.txt", "a", "dst.txt")
    backend.delete_object.assert_awaited_once_with("a", "src.txt")


def test_rename_directory_moves_all_contents(backend: MagicMock) -> None:
    backend.head_object = AsyncMock(return_value=None)
    backend.head_directory = AsyncMock(return_value=True)

    def _list_objects(bucket, prefix="", delimiter="", max_keys=1000, continuation_token=""):  # type: ignore[no-untyped-def]
        if delimiter:
            return {
                "contents": [],
                "common_prefixes": [],
                "is_truncated": False,
                "next_continuation_token": None,
            }
        return {
            "contents": [
                BucketFile(type="file", path="src/a.txt", size=1),
                BucketFile(type="file", path="src/b.txt", size=1),
            ],
            "common_prefixes": [],
            "is_truncated": False,
            "next_continuation_token": None,
        }

    backend.list_objects = AsyncMock(side_effect=_list_objects)

    fs = _build_fs(backend)
    fs.rename("/a/src", "/a/dst")

    copy_calls = [call.args for call in backend.copy_object.await_args_list]
    delete_calls = [call.args for call in backend.delete_object.await_args_list]
    assert ("a", "src/a.txt", "a", "dst/a.txt") in copy_calls
    assert ("a", "src/b.txt", "a", "dst/b.txt") in copy_calls
    assert ("a", "src/a.txt") in delete_calls
    assert ("a", "src/b.txt") in delete_calls


def test_mkstemp_returns_upload_buffer(backend: MagicMock) -> None:
    backend.head_object = AsyncMock(return_value=None)
    fs = _build_fs(backend)
    handle = fs.mkstemp(prefix="tmp", dir="/a/dir", mode="wb")
    handle.write(b"x")
    name = handle.name
    handle.close()
    assert name.startswith("/a/dir/tmp")
    assert backend.put_object.await_count == 1


def test_mkstemp_in_root_raises_permission_error(backend: MagicMock) -> None:
    fs = _build_fs(backend)
    with pytest.raises(PermissionError):
        fs.mkstemp(prefix="tmp", dir="/", mode="wb")


def test_mkstemp_exhausted_name_space_raises(backend: MagicMock) -> None:
    backend.head_object = AsyncMock(return_value=BucketFile(type="file", path="x", size=1))
    fs = _build_fs(backend)
    with pytest.raises(OSError):
        fs.mkstemp(prefix="tmp", dir="/a/dir", mode="wb")


def test_stat_file_uses_size_and_time(backend: MagicMock) -> None:
    backend.head_object = AsyncMock(
        return_value=BucketFile(
            type="file",
            path="f.bin",
            size=42,
            mtime="2026-01-01T00:00:00Z",
        )
    )
    fs = _build_fs(backend)
    st = fs.stat("/a/f.bin")
    assert st.st_size == 42


def test_stat_missing_raises_file_not_found(backend: MagicMock) -> None:
    backend.head_object = AsyncMock(return_value=None)
    backend.head_directory = AsyncMock(return_value=False)
    fs = _build_fs(backend)
    with pytest.raises(FileNotFoundError):
        fs.stat("/a/missing.bin")


def test_listdir_directory_merges_files_and_common_prefixes(backend: MagicMock) -> None:
    backend.list_objects = AsyncMock(
        return_value={
            "contents": [
                BucketFile(type="file", path="dir/a.txt", size=1),
                BucketFile(type="file", path="dir/b.txt", size=1),
            ],
            "common_prefixes": ["dir/sub/"],
            "is_truncated": False,
            "next_continuation_token": None,
        }
    )
    fs = _build_fs(backend)
    assert fs.listdir("/a/dir") == ["a.txt", "b.txt", "sub"]
