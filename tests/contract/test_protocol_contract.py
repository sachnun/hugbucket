"""Cross-protocol contract tests (S3 and FTP).

These tests verify that different protocol adapters map to the same
StorageBackend semantics.
"""

from __future__ import annotations

import asyncio
import inspect
import io
from unittest.mock import AsyncMock, MagicMock

from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from hugbucket.core.models import BucketFile, BucketInfo
from hugbucket.protocols.ftp.filesystem import HugBucketFTPFilesystem
from hugbucket.s3.server import S3Handler


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


def _build_ftp_fs(backend: MagicMock) -> HugBucketFTPFilesystem:
    return HugBucketFTPFilesystem("/", _CmdChannel(_Runner(backend)))


def _new_backend_mock() -> MagicMock:
    b = MagicMock()
    b.list_buckets = AsyncMock(
        return_value=[
            BucketInfo(id="ns/bucket", private=False, created_at="", size=0, total_files=0)
        ]
    )
    b.head_bucket = AsyncMock(
        side_effect=lambda name: BucketInfo(
            id=f"ns/{name}", private=False, created_at="", size=0, total_files=0
        )
    )
    b.put_object = AsyncMock(return_value={"ETag": '"x"', "size": 0})
    b.delete_object = AsyncMock()
    b.head_directory = AsyncMock(return_value=False)
    b.list_objects = AsyncMock(
        return_value={
            "contents": [],
            "common_prefixes": [],
            "is_truncated": False,
            "next_continuation_token": None,
        }
    )
    b.head_object = AsyncMock(return_value=None)
    b.get_object = AsyncMock(return_value=None)
    b.get_object_stream = AsyncMock(return_value=None)
    b.delete_bucket = AsyncMock()
    b.create_bucket = AsyncMock(return_value="")
    b.copy_object = AsyncMock(return_value={"ETag": '"x"'})
    b.delete_objects = AsyncMock(return_value=([], []))
    b.head_bucket = AsyncMock(
        side_effect=lambda bucket: BucketInfo(
            id=f"ns/{bucket}", private=False, created_at="", size=0, total_files=0
        )
    )
    return b


async def _s3_put_object(backend: MagicMock, path: str, data: bytes) -> int:
    app = web.Application()
    S3Handler(backend).setup_routes(app)
    server = TestServer(app)
    await server.start_server()
    client = TestClient(server)
    await client.start_server()
    try:
        resp = await client.put(path, data=data)
        return resp.status
    finally:
        await client.close()
        await server.close()


async def _s3_delete_object(backend: MagicMock, path: str) -> int:
    app = web.Application()
    S3Handler(backend).setup_routes(app)
    server = TestServer(app)
    await server.start_server()
    client = TestClient(server)
    await client.start_server()
    try:
        resp = await client.delete(path)
        return resp.status
    finally:
        await client.close()
        await server.close()


async def _s3_get_missing(backend: MagicMock, path: str) -> int:
    app = web.Application()
    S3Handler(backend).setup_routes(app)
    server = TestServer(app)
    await server.start_server()
    client = TestClient(server)
    await client.start_server()
    try:
        resp = await client.get(path)
        return resp.status
    finally:
        await client.close()
        await server.close()


def test_write_path_contract_s3_and_ftp() -> None:
    backend = _new_backend_mock()

    status = asyncio.run(_s3_put_object(backend, "/bucket/dir/file.txt", b"abc"))
    assert status == 200
    backend.put_object.assert_awaited_once_with("bucket", "dir/file.txt", b"abc")

    backend.put_object.reset_mock()
    fs = _build_ftp_fs(backend)
    handle = fs.open("/bucket/dir/file.txt", "wb")
    assert isinstance(handle, io.BytesIO)
    handle.write(b"abc")
    handle.close()
    backend.put_object.assert_awaited_once_with("bucket", "dir/file.txt", b"abc")


def test_folder_creation_contract_s3_and_ftp() -> None:
    backend = _new_backend_mock()

    status = asyncio.run(_s3_put_object(backend, "/bucket/folder/", b""))
    assert status == 200
    backend.put_object.assert_awaited_once_with("bucket", "folder/", b"")

    backend.put_object.reset_mock()
    fs = _build_ftp_fs(backend)
    fs.mkdir("/bucket/folder")
    backend.put_object.assert_awaited_once_with("bucket", "folder/", b"")


def test_delete_contract_s3_and_ftp() -> None:
    backend = _new_backend_mock()

    status = asyncio.run(_s3_delete_object(backend, "/bucket/dir/file.txt"))
    assert status == 204
    backend.delete_object.assert_awaited_once_with("bucket", "dir/file.txt")

    backend.delete_object.reset_mock()
    fs = _build_ftp_fs(backend)
    fs.remove("/bucket/dir/file.txt")
    backend.delete_object.assert_awaited_once_with("bucket", "dir/file.txt")


def test_missing_file_contract_s3_and_ftp() -> None:
    backend = _new_backend_mock()
    backend.head_object = AsyncMock(return_value=None)
    backend.get_object = AsyncMock(return_value=None)

    status = asyncio.run(_s3_get_missing(backend, "/bucket/missing.txt"))
    assert status == 404

    fs = _build_ftp_fs(backend)
    try:
        fs.open("/bucket/missing.txt", "rb")
    except FileNotFoundError:
        pass
    else:
        raise AssertionError("FTP open must raise FileNotFoundError for missing file")


def test_list_contract_root_buckets() -> None:
    backend = _new_backend_mock()
    fs = _build_ftp_fs(backend)
    names = fs.listdir("/")
    assert names == ["bucket"]


def test_list_contract_subdir_file_names() -> None:
    backend = _new_backend_mock()
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
    fs = _build_ftp_fs(backend)
    assert fs.listdir("/bucket/dir") == ["a.txt", "b.txt", "sub"]
