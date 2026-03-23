"""Tests for the WebDAV HTTP server using aiohttp test client.

These tests mock the StorageBackend to avoid hitting the live HF API,
focusing on correct WebDAV protocol behavior (RFC 4918).
"""

from __future__ import annotations

import base64
from collections.abc import AsyncIterator
from unittest.mock import AsyncMock, MagicMock
from xml.etree.ElementTree import fromstring

import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient

from hugbucket.core.models import BucketFile, BucketInfo
from hugbucket.protocols.webdav.server import WebDAVHandler


async def _async_chunks(*chunks: bytes) -> AsyncIterator[bytes]:
    """Helper: create an async iterator yielding the given byte chunks."""
    for chunk in chunks:
        yield chunk


@pytest.fixture
def mock_backend() -> MagicMock:
    """Create a mock StorageBackend with async methods."""
    backend = MagicMock()
    backend.list_buckets = AsyncMock(return_value=[])
    backend.create_bucket = AsyncMock(return_value="")
    backend.delete_bucket = AsyncMock()
    backend.head_bucket = AsyncMock(return_value=None)
    backend.put_object = AsyncMock(return_value={"ETag": '"abc123"', "size": 0})
    backend.get_object = AsyncMock(return_value=None)
    backend.get_object_stream = AsyncMock(return_value=None)
    backend.delete_object = AsyncMock()
    backend.delete_objects = AsyncMock(return_value=([], []))
    backend.head_object = AsyncMock(return_value=None)
    backend.head_directory = AsyncMock(return_value=False)
    backend.copy_object = AsyncMock(
        return_value={"ETag": '"abc123"', "LastModified": "2026-01-01T00:00:00Z"}
    )
    backend.list_objects = AsyncMock(
        return_value={
            "contents": [],
            "common_prefixes": [],
            "is_truncated": False,
            "next_continuation_token": None,
        }
    )
    return backend


@pytest.fixture
def app(mock_backend: MagicMock) -> web.Application:
    """Create aiohttp app with WebDAV handler."""
    application = web.Application(client_max_size=16 * 1024 * 1024)
    handler = WebDAVHandler(mock_backend)
    handler.setup_routes(application)
    return application


@pytest.fixture
async def client(aiohttp_client, app: web.Application) -> TestClient:
    """Create test client."""
    return await aiohttp_client(app)


# ── OPTIONS ──────────────────────────────────────────────────────────────


class TestOptions:
    async def test_options_returns_dav_headers(self, client: TestClient) -> None:
        resp = await client.request("OPTIONS", "/")
        assert resp.status == 200
        assert "DAV" in resp.headers
        assert "1" in resp.headers["DAV"]
        assert "Allow" in resp.headers
        assert "PROPFIND" in resp.headers["Allow"]
        assert "MKCOL" in resp.headers["Allow"]

    async def test_options_on_resource(self, client: TestClient) -> None:
        resp = await client.request("OPTIONS", "/bucket/file.txt")
        assert resp.status == 200
        assert "DAV" in resp.headers


# ── PROPFIND ─────────────────────────────────────────────────────────────


class TestPropfind:
    async def test_propfind_root_empty(self, client: TestClient) -> None:
        resp = await client.request("PROPFIND", "/", headers={"Depth": "0"})
        assert resp.status == 207
        body = await resp.text()
        assert "multistatus" in body

    async def test_propfind_root_lists_buckets(
        self, client: TestClient, mock_backend: MagicMock
    ) -> None:
        mock_backend.list_buckets = AsyncMock(
            return_value=[
                BucketInfo(
                    id="ns/photos",
                    private=False,
                    created_at="2026-01-01T00:00:00Z",
                    size=0,
                    total_files=0,
                ),
                BucketInfo(
                    id="ns/docs",
                    private=False,
                    created_at="2026-02-01T00:00:00Z",
                    size=0,
                    total_files=0,
                ),
            ]
        )

        resp = await client.request("PROPFIND", "/", headers={"Depth": "1"})
        assert resp.status == 207
        body = await resp.text()
        assert "photos" in body
        assert "docs" in body
        assert "collection" in body

    async def test_propfind_bucket_contents(
        self, client: TestClient, mock_backend: MagicMock
    ) -> None:
        mock_backend.head_bucket = AsyncMock(
            return_value=BucketInfo(
                id="ns/mybucket",
                private=False,
                created_at="2026-01-01T00:00:00Z",
                size=0,
                total_files=0,
            )
        )
        mock_backend.list_objects = AsyncMock(
            return_value={
                "contents": [
                    BucketFile(
                        type="file",
                        path="file.txt",
                        size=100,
                        xet_hash="abc123def456",
                        mtime="2026-03-01T12:00:00Z",
                    ),
                ],
                "common_prefixes": ["subdir/"],
                "is_truncated": False,
                "next_continuation_token": None,
            }
        )

        resp = await client.request("PROPFIND", "/mybucket/", headers={"Depth": "1"})
        assert resp.status == 207
        body = await resp.text()
        assert "file.txt" in body
        assert "subdir" in body
        assert "100" in body  # content length

    async def test_propfind_file(
        self, client: TestClient, mock_backend: MagicMock
    ) -> None:
        mock_backend.head_object = AsyncMock(
            return_value=BucketFile(
                type="file",
                path="photo.jpg",
                size=50000,
                xet_hash="abc123def456789012345678901234",
                mtime="2026-03-15T10:00:00Z",
            )
        )

        resp = await client.request(
            "PROPFIND", "/mybucket/photo.jpg", headers={"Depth": "0"}
        )
        assert resp.status == 207
        body = await resp.text()
        assert "50000" in body
        assert "photo.jpg" in body

    async def test_propfind_not_found(self, client: TestClient) -> None:
        resp = await client.request(
            "PROPFIND", "/mybucket/missing.txt", headers={"Depth": "0"}
        )
        assert resp.status == 404

    async def test_propfind_directory_without_trailing_slash(
        self, client: TestClient, mock_backend: MagicMock
    ) -> None:
        """PROPFIND on /bucket/dir should find it as a directory."""
        mock_backend.head_object = AsyncMock(return_value=None)
        mock_backend.head_directory = AsyncMock(return_value=True)
        mock_backend.list_objects = AsyncMock(
            return_value={
                "contents": [],
                "common_prefixes": [],
                "is_truncated": False,
                "next_continuation_token": None,
            }
        )

        resp = await client.request(
            "PROPFIND", "/mybucket/mydir", headers={"Depth": "1"}
        )
        assert resp.status == 207


# ── GET ──────────────────────────────────────────────────────────────────


class TestGet:
    async def test_get_file(self, client: TestClient, mock_backend: MagicMock) -> None:
        mock_backend.head_object = AsyncMock(
            return_value=BucketFile(
                type="file",
                path="hello.txt",
                size=5,
                xet_hash="abc123def456789012345678901234",
                mtime="2026-03-15T10:00:00Z",
            )
        )
        mock_backend.get_object_stream = AsyncMock(return_value=_async_chunks(b"hello"))

        resp = await client.get("/mybucket/hello.txt")
        assert resp.status == 200
        data = await resp.read()
        assert data == b"hello"
        assert resp.headers.get("ETag") is not None
        assert resp.headers.get("Content-Length") == "5"

    async def test_get_missing_file(self, client: TestClient) -> None:
        resp = await client.get("/mybucket/missing.txt")
        assert resp.status == 404

    async def test_get_collection_returns_405(self, client: TestClient) -> None:
        resp = await client.get("/")
        # GET on root (no bucket) should return 405
        assert resp.status == 405

    async def test_get_with_range(
        self, client: TestClient, mock_backend: MagicMock
    ) -> None:
        mock_backend.head_object = AsyncMock(
            return_value=BucketFile(
                type="file",
                path="data.bin",
                size=100,
                xet_hash="abc123",
                mtime="2026-03-15T10:00:00Z",
            )
        )
        mock_backend.get_object_stream = AsyncMock(return_value=_async_chunks(b"12345"))

        resp = await client.get("/mybucket/data.bin", headers={"Range": "bytes=0-4"})
        assert resp.status == 206
        assert "Content-Range" in resp.headers


# ── HEAD ─────────────────────────────────────────────────────────────────


class TestHead:
    async def test_head_file(self, client: TestClient, mock_backend: MagicMock) -> None:
        mock_backend.head_object = AsyncMock(
            return_value=BucketFile(
                type="file",
                path="doc.pdf",
                size=1024,
                xet_hash="abc123",
                mtime="2026-03-15T10:00:00Z",
            )
        )

        resp = await client.head("/mybucket/doc.pdf")
        assert resp.status == 200
        assert resp.headers.get("Content-Length") == "1024"

    async def test_head_missing(self, client: TestClient) -> None:
        resp = await client.head("/mybucket/missing.txt")
        assert resp.status == 404

    async def test_head_root(self, client: TestClient) -> None:
        resp = await client.head("/")
        assert resp.status == 200

    async def test_head_bucket(
        self, client: TestClient, mock_backend: MagicMock
    ) -> None:
        mock_backend.head_bucket = AsyncMock(
            return_value=BucketInfo(
                id="ns/mybucket",
                private=False,
                created_at="2026-01-01T00:00:00Z",
                size=0,
                total_files=0,
            )
        )
        resp = await client.head("/mybucket/")
        assert resp.status == 200


# ── PUT ──────────────────────────────────────────────────────────────────


class TestPut:
    async def test_put_file(self, client: TestClient, mock_backend: MagicMock) -> None:
        resp = await client.put("/mybucket/file.txt", data=b"hello world")
        assert resp.status == 201
        mock_backend.put_object.assert_awaited_once_with(
            "mybucket", "file.txt", b"hello world"
        )

    async def test_put_to_root_rejected(self, client: TestClient) -> None:
        resp = await client.put("/", data=b"data")
        assert resp.status == 405

    async def test_put_with_trailing_slash_rejected(self, client: TestClient) -> None:
        resp = await client.put("/mybucket/folder/", data=b"")
        assert resp.status == 405


# ── DELETE ───────────────────────────────────────────────────────────────


class TestDelete:
    async def test_delete_file(
        self, client: TestClient, mock_backend: MagicMock
    ) -> None:
        mock_backend.head_object = AsyncMock(
            return_value=BucketFile(type="file", path="file.txt", size=10)
        )
        resp = await client.delete("/mybucket/file.txt")
        assert resp.status == 204
        mock_backend.delete_object.assert_awaited_once_with("mybucket", "file.txt")

    async def test_delete_bucket(
        self, client: TestClient, mock_backend: MagicMock
    ) -> None:
        resp = await client.delete("/mybucket")
        assert resp.status == 204
        mock_backend.delete_bucket.assert_awaited_once_with("mybucket")

    async def test_delete_root_forbidden(self, client: TestClient) -> None:
        resp = await client.delete("/")
        assert resp.status == 403

    async def test_delete_directory_recursive(
        self, client: TestClient, mock_backend: MagicMock
    ) -> None:
        mock_backend.head_object = AsyncMock(return_value=None)
        mock_backend.list_objects = AsyncMock(
            return_value={
                "contents": [
                    BucketFile(type="file", path="dir/a.txt", size=1),
                    BucketFile(type="file", path="dir/b.txt", size=1),
                ],
                "common_prefixes": [],
                "is_truncated": False,
                "next_continuation_token": None,
            }
        )
        resp = await client.delete("/mybucket/dir/")
        assert resp.status == 204
        mock_backend.delete_objects.assert_awaited_once()


# ── MKCOL ────────────────────────────────────────────────────────────────


class TestMkcol:
    async def test_mkcol_bucket(
        self, client: TestClient, mock_backend: MagicMock
    ) -> None:
        resp = await client.request("MKCOL", "/newbucket")
        assert resp.status == 201
        mock_backend.create_bucket.assert_awaited_once_with("newbucket")

    async def test_mkcol_directory(
        self, client: TestClient, mock_backend: MagicMock
    ) -> None:
        mock_backend.head_bucket = AsyncMock(
            return_value=BucketInfo(
                id="ns/mybucket",
                private=False,
                created_at="",
                size=0,
                total_files=0,
            )
        )
        resp = await client.request("MKCOL", "/mybucket/newdir")
        assert resp.status == 201
        mock_backend.put_object.assert_awaited_once_with("mybucket", "newdir/", b"")

    async def test_mkcol_with_body_rejected(self, client: TestClient) -> None:
        resp = await client.request("MKCOL", "/mybucket/newdir", data=b"<xml/>")
        assert resp.status == 415

    async def test_mkcol_missing_parent_bucket(
        self, client: TestClient, mock_backend: MagicMock
    ) -> None:
        mock_backend.head_bucket = AsyncMock(return_value=None)
        resp = await client.request("MKCOL", "/nonexistent/subdir")
        assert resp.status == 409


# ── COPY ─────────────────────────────────────────────────────────────────


class TestCopy:
    async def test_copy_file(self, client: TestClient, mock_backend: MagicMock) -> None:
        mock_backend.head_object = AsyncMock(
            return_value=BucketFile(type="file", path="src.txt", size=10)
        )
        resp = await client.request(
            "COPY",
            "/mybucket/src.txt",
            headers={"Destination": "/mybucket/dst.txt"},
        )
        assert resp.status == 201
        mock_backend.copy_object.assert_awaited_once_with(
            "mybucket", "src.txt", "mybucket", "dst.txt"
        )

    async def test_copy_missing_destination(self, client: TestClient) -> None:
        resp = await client.request("COPY", "/mybucket/file.txt")
        assert resp.status == 400

    async def test_copy_no_overwrite(
        self, client: TestClient, mock_backend: MagicMock
    ) -> None:
        mock_backend.head_object = AsyncMock(
            return_value=BucketFile(type="file", path="dst.txt", size=10)
        )
        resp = await client.request(
            "COPY",
            "/mybucket/src.txt",
            headers={
                "Destination": "/mybucket/dst.txt",
                "Overwrite": "F",
            },
        )
        assert resp.status == 412


# ── MOVE ─────────────────────────────────────────────────────────────────


class TestMove:
    async def test_move_file(self, client: TestClient, mock_backend: MagicMock) -> None:
        mock_backend.head_object = AsyncMock(
            return_value=BucketFile(type="file", path="old.txt", size=10)
        )
        resp = await client.request(
            "MOVE",
            "/mybucket/old.txt",
            headers={"Destination": "/mybucket/new.txt"},
        )
        assert resp.status == 201
        mock_backend.copy_object.assert_awaited_once_with(
            "mybucket", "old.txt", "mybucket", "new.txt"
        )
        mock_backend.delete_object.assert_awaited_once_with("mybucket", "old.txt")

    async def test_move_missing_destination(self, client: TestClient) -> None:
        resp = await client.request("MOVE", "/mybucket/file.txt")
        assert resp.status == 400


# ── LOCK / UNLOCK ────────────────────────────────────────────────────────


class TestLockUnlock:
    async def test_lock_returns_token(self, client: TestClient) -> None:
        resp = await client.request("LOCK", "/mybucket/file.txt")
        assert resp.status == 200
        assert "Lock-Token" in resp.headers
        body = await resp.text()
        assert "lockdiscovery" in body

    async def test_lock_with_body(self, client: TestClient) -> None:
        lock_body = (
            '<?xml version="1.0" encoding="utf-8"?>'
            '<D:lockinfo xmlns:D="DAV:">'
            "<D:lockscope><D:exclusive/></D:lockscope>"
            "<D:locktype><D:write/></D:locktype>"
            "<D:owner><D:href>user@example.com</D:href></D:owner>"
            "</D:lockinfo>"
        )
        resp = await client.request("LOCK", "/mybucket/file.txt", data=lock_body)
        assert resp.status == 200
        body = await resp.text()
        assert "user@example.com" in body

    async def test_unlock_succeeds(self, client: TestClient) -> None:
        resp = await client.request(
            "UNLOCK",
            "/mybucket/file.txt",
            headers={"Lock-Token": "<opaquelocktoken:fake>"},
        )
        assert resp.status == 204


# ── PROPPATCH ────────────────────────────────────────────────────────────


class TestProppatch:
    async def test_proppatch_accepts_changes(self, client: TestClient) -> None:
        body = (
            '<?xml version="1.0" encoding="utf-8"?>'
            '<D:propertyupdate xmlns:D="DAV:">'
            "<D:set><D:prop>"
            "<D:getlastmodified>Mon, 01 Jan 2026 00:00:00 GMT</D:getlastmodified>"
            "</D:prop></D:set>"
            "</D:propertyupdate>"
        )
        resp = await client.request("PROPPATCH", "/mybucket/file.txt", data=body)
        assert resp.status == 207
        text = await resp.text()
        assert "multistatus" in text
