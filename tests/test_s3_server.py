"""Tests for the S3 HTTP server using aiohttp test client.

These tests mock the Bridge layer to avoid hitting the live HF API,
focusing on correct S3 protocol behavior.
"""

from __future__ import annotations

import hashlib
from unittest.mock import AsyncMock, MagicMock
from xml.etree.ElementTree import fromstring

import pytest
from aiohttp import web
from aiohttp.test_utils import AioHTTPTestCase, TestClient, TestServer

from hugbucket.hub.client import BucketInfo, BucketFile
from hugbucket.s3.server import S3Handler


@pytest.fixture
def mock_bridge() -> MagicMock:
    """Create a mock Bridge with async methods."""
    bridge = MagicMock()
    bridge.list_buckets = AsyncMock(return_value=[])
    bridge.create_bucket = AsyncMock(return_value="")
    bridge.delete_bucket = AsyncMock()
    bridge.head_bucket = AsyncMock(return_value=None)
    bridge.put_object = AsyncMock(return_value={"ETag": '"abc123"', "size": 0})
    bridge.get_object = AsyncMock(return_value=None)
    bridge.delete_object = AsyncMock()
    bridge.delete_objects = AsyncMock(return_value=([], []))
    bridge.head_object = AsyncMock(return_value=None)
    bridge.list_objects = AsyncMock(
        return_value={
            "contents": [],
            "common_prefixes": [],
            "is_truncated": False,
            "next_continuation_token": None,
        }
    )
    return bridge


@pytest.fixture
def app(mock_bridge: MagicMock) -> web.Application:
    """Create aiohttp app with S3 handler."""
    application = web.Application(client_max_size=16 * 1024 * 1024)
    handler = S3Handler(mock_bridge)
    handler.setup_routes(application)
    return application


@pytest.fixture
async def client(aiohttp_client, app: web.Application) -> TestClient:
    """Create test client."""
    return await aiohttp_client(app)


class TestListBuckets:
    async def test_empty_list(self, client: TestClient) -> None:
        resp = await client.get("/")
        assert resp.status == 200
        body = await resp.text()
        assert "ListAllMyBucketsResult" in body

    async def test_with_buckets(
        self, client: TestClient, mock_bridge: MagicMock
    ) -> None:
        mock_bridge.list_buckets.return_value = [
            BucketInfo(
                id="user/my-bucket",
                private=False,
                created_at="2026-01-01T00:00:00Z",
                size=1000,
                total_files=5,
            )
        ]
        resp = await client.get("/")
        assert resp.status == 200
        body = await resp.text()
        assert "my-bucket" in body


class TestBucketOps:
    async def test_create_bucket(
        self, client: TestClient, mock_bridge: MagicMock
    ) -> None:
        resp = await client.put("/my-new-bucket")
        assert resp.status == 200
        mock_bridge.create_bucket.assert_awaited_once_with("my-new-bucket")

    async def test_delete_bucket(
        self, client: TestClient, mock_bridge: MagicMock
    ) -> None:
        resp = await client.delete("/my-bucket")
        assert resp.status == 204
        mock_bridge.delete_bucket.assert_awaited_once_with("my-bucket")

    async def test_head_bucket_found(
        self, client: TestClient, mock_bridge: MagicMock
    ) -> None:
        mock_bridge.head_bucket.return_value = BucketInfo(
            id="user/test", private=False, created_at="", size=0, total_files=0
        )
        resp = await client.head("/test")
        assert resp.status == 200

    async def test_head_bucket_not_found(
        self, client: TestClient, mock_bridge: MagicMock
    ) -> None:
        mock_bridge.head_bucket.return_value = None
        resp = await client.head("/nonexistent")
        assert resp.status == 404


class TestPutObject:
    async def test_put_returns_etag(
        self, client: TestClient, mock_bridge: MagicMock
    ) -> None:
        mock_bridge.put_object.return_value = {"ETag": '"abc123"', "size": 5}
        resp = await client.put("/bucket/key.txt", data=b"hello")
        assert resp.status == 200
        assert resp.headers.get("ETag") == '"abc123"'
        mock_bridge.put_object.assert_awaited_once()


class TestGetObject:
    async def test_get_existing_object(
        self, client: TestClient, mock_bridge: MagicMock
    ) -> None:
        test_data = b"hello world"
        mock_bridge.get_object.return_value = test_data
        mock_bridge.head_object.return_value = BucketFile(
            type="file",
            path="key.txt",
            size=len(test_data),
            xet_hash="a" * 64,
            mtime="2026-01-01T00:00:00Z",
        )
        resp = await client.get("/bucket/key.txt")
        assert resp.status == 200
        body = await resp.read()
        assert body == test_data
        assert "ETag" in resp.headers
        assert "Last-Modified" in resp.headers

    async def test_get_missing_object(
        self, client: TestClient, mock_bridge: MagicMock
    ) -> None:
        mock_bridge.get_object.return_value = None
        resp = await client.get("/bucket/missing.txt")
        assert resp.status == 404
        body = await resp.text()
        assert "NoSuchKey" in body

    async def test_range_request(
        self, client: TestClient, mock_bridge: MagicMock
    ) -> None:
        test_data = b"0123456789"
        mock_bridge.get_object.return_value = test_data
        mock_bridge.head_object.return_value = BucketFile(
            type="file",
            path="data.bin",
            size=10,
            xet_hash="b" * 64,
            mtime="2026-01-01T00:00:00Z",
        )
        resp = await client.get("/bucket/data.bin", headers={"Range": "bytes=2-5"})
        assert resp.status == 206
        body = await resp.read()
        assert body == b"2345"
        assert resp.headers["Content-Range"] == "bytes 2-5/10"

    async def test_range_request_out_of_bounds(
        self, client: TestClient, mock_bridge: MagicMock
    ) -> None:
        test_data = b"short"
        mock_bridge.get_object.return_value = test_data
        mock_bridge.head_object.return_value = BucketFile(
            type="file",
            path="data.bin",
            size=5,
            xet_hash="c" * 64,
            mtime="2026-01-01T00:00:00Z",
        )
        resp = await client.get("/bucket/data.bin", headers={"Range": "bytes=100-200"})
        assert resp.status == 416

    async def test_range_request_suffix(
        self, client: TestClient, mock_bridge: MagicMock
    ) -> None:
        """bytes=5- means from byte 5 to end."""
        test_data = b"0123456789"
        mock_bridge.get_object.return_value = test_data
        mock_bridge.head_object.return_value = BucketFile(
            type="file",
            path="data.bin",
            size=10,
            xet_hash="d" * 64,
            mtime="2026-01-01T00:00:00Z",
        )
        resp = await client.get("/bucket/data.bin", headers={"Range": "bytes=5-"})
        assert resp.status == 206
        body = await resp.read()
        assert body == b"56789"


class TestDeleteObject:
    async def test_delete(self, client: TestClient, mock_bridge: MagicMock) -> None:
        resp = await client.delete("/bucket/key.txt")
        assert resp.status == 204
        mock_bridge.delete_object.assert_awaited_once_with("bucket", "key.txt")


class TestHeadObject:
    async def test_head_found(self, client: TestClient, mock_bridge: MagicMock) -> None:
        mock_bridge.head_object.return_value = BucketFile(
            type="file",
            path="key.txt",
            size=1234,
            xet_hash="e" * 64,
            mtime="2026-01-01T00:00:00Z",
        )
        resp = await client.head("/bucket/key.txt")
        assert resp.status == 200
        assert resp.headers["Content-Length"] == "1234"
        assert "ETag" in resp.headers
        assert "Last-Modified" in resp.headers

    async def test_head_not_found(
        self, client: TestClient, mock_bridge: MagicMock
    ) -> None:
        mock_bridge.head_object.return_value = None
        resp = await client.head("/bucket/missing.txt")
        assert resp.status == 404


class TestListObjectsV2:
    async def test_empty_listing(
        self, client: TestClient, mock_bridge: MagicMock
    ) -> None:
        resp = await client.get("/bucket?list-type=2")
        assert resp.status == 200
        body = await resp.text()
        assert "ListBucketResult" in body

    async def test_with_prefix_and_delimiter(
        self, client: TestClient, mock_bridge: MagicMock
    ) -> None:
        mock_bridge.list_objects.return_value = {
            "contents": [
                BucketFile(
                    type="file",
                    path="dir/file.txt",
                    size=100,
                    xet_hash="f" * 64,
                    mtime="2026-01-01T00:00:00Z",
                )
            ],
            "common_prefixes": ["dir/sub/"],
            "is_truncated": False,
            "next_continuation_token": None,
        }
        resp = await client.get("/bucket?list-type=2&prefix=dir/&delimiter=/")
        assert resp.status == 200
        body = await resp.text()
        assert "dir/file.txt" in body
        assert "dir/sub/" in body


class TestMultipartUpload:
    async def test_full_multipart_flow(
        self, client: TestClient, mock_bridge: MagicMock
    ) -> None:
        """Test initiate → upload parts → complete."""
        # Initiate
        resp = await client.post("/bucket/big-file.bin?uploads")
        assert resp.status == 200
        body = await resp.text()
        root = fromstring(body)
        ns = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
        upload_id = root.find("s3:UploadId", ns).text
        assert upload_id

        # Upload parts
        part1 = b"A" * 5 * 1024 * 1024
        part2 = b"B" * 3 * 1024 * 1024
        etag1 = hashlib.md5(part1).hexdigest()
        etag2 = hashlib.md5(part2).hexdigest()

        resp = await client.put(
            f"/bucket/big-file.bin?partNumber=1&uploadId={upload_id}",
            data=part1,
        )
        assert resp.status == 200
        assert resp.headers["ETag"] == f'"{etag1}"'

        resp = await client.put(
            f"/bucket/big-file.bin?partNumber=2&uploadId={upload_id}",
            data=part2,
        )
        assert resp.status == 200

        # Complete
        complete_xml = f"""<CompleteMultipartUpload>
          <Part><PartNumber>1</PartNumber><ETag>"{etag1}"</ETag></Part>
          <Part><PartNumber>2</PartNumber><ETag>"{etag2}"</ETag></Part>
        </CompleteMultipartUpload>"""

        mock_bridge.put_object.return_value = {
            "ETag": '"combined-etag"',
            "size": len(part1) + len(part2),
        }
        resp = await client.post(
            f"/bucket/big-file.bin?uploadId={upload_id}",
            data=complete_xml.encode(),
        )
        assert resp.status == 200
        body = await resp.text()
        assert "CompleteMultipartUploadResult" in body

        # Verify bridge.put_object was called with concatenated data
        call_args = mock_bridge.put_object.call_args
        assert call_args[0][0] == "bucket"
        assert call_args[0][1] == "big-file.bin"
        assert call_args[0][2] == part1 + part2

    async def test_abort_multipart(
        self, client: TestClient, mock_bridge: MagicMock
    ) -> None:
        # Initiate
        resp = await client.post("/bucket/file.bin?uploads")
        body = await resp.text()
        root = fromstring(body)
        ns = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
        upload_id = root.find("s3:UploadId", ns).text

        # Abort
        resp = await client.delete(f"/bucket/file.bin?uploadId={upload_id}")
        assert resp.status == 204

    async def test_upload_part_nonexistent_upload(self, client: TestClient) -> None:
        resp = await client.put(
            "/bucket/file.bin?partNumber=1&uploadId=nonexistent",
            data=b"data",
        )
        assert resp.status == 404

    async def test_complete_nonexistent_upload(self, client: TestClient) -> None:
        resp = await client.post(
            "/bucket/file.bin?uploadId=nonexistent",
            data=b"<CompleteMultipartUpload/>",
        )
        assert resp.status == 404


class TestDeleteObjects:
    """Tests for S3 multi-object delete (POST /{bucket}?delete)."""

    async def test_delete_multiple_objects(
        self, client: TestClient, mock_bridge: MagicMock
    ) -> None:
        mock_bridge.delete_objects.return_value = (
            ["file1.txt", "file2.txt", "dir/file3.txt"],
            [],
        )
        body = """<?xml version="1.0" encoding="UTF-8"?>
        <Delete>
            <Object><Key>file1.txt</Key></Object>
            <Object><Key>file2.txt</Key></Object>
            <Object><Key>dir/file3.txt</Key></Object>
        </Delete>"""
        resp = await client.post("/my-bucket?delete", data=body.encode())
        assert resp.status == 200
        text = await resp.text()
        assert "DeleteResult" in text
        assert "file1.txt" in text
        assert "file2.txt" in text
        assert "dir/file3.txt" in text
        mock_bridge.delete_objects.assert_awaited_once_with(
            "my-bucket", ["file1.txt", "file2.txt", "dir/file3.txt"]
        )

    async def test_delete_objects_with_errors(
        self, client: TestClient, mock_bridge: MagicMock
    ) -> None:
        mock_bridge.delete_objects.return_value = (
            ["ok.txt"],
            [{"key": "bad.txt", "code": "AccessDenied", "message": "Access Denied"}],
        )
        body = """<Delete>
            <Object><Key>ok.txt</Key></Object>
            <Object><Key>bad.txt</Key></Object>
        </Delete>"""
        resp = await client.post("/my-bucket?delete", data=body.encode())
        assert resp.status == 200
        text = await resp.text()
        assert "ok.txt" in text
        assert "AccessDenied" in text

    async def test_delete_objects_malformed_xml(
        self, client: TestClient, mock_bridge: MagicMock
    ) -> None:
        resp = await client.post("/my-bucket?delete", data=b"not xml")
        assert resp.status == 400
        text = await resp.text()
        assert "MalformedXML" in text

    async def test_delete_objects_no_keys(
        self, client: TestClient, mock_bridge: MagicMock
    ) -> None:
        body = """<Delete></Delete>"""
        resp = await client.post("/my-bucket?delete", data=body.encode())
        assert resp.status == 400
        text = await resp.text()
        assert "MalformedXML" in text

    async def test_delete_objects_with_namespace(
        self, client: TestClient, mock_bridge: MagicMock
    ) -> None:
        """Keys in S3 namespace should be parsed correctly."""
        mock_bridge.delete_objects.return_value = (["a.txt", "b.txt"], [])
        body = """<?xml version="1.0" encoding="UTF-8"?>
        <Delete xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
            <Object><Key>a.txt</Key></Object>
            <Object><Key>b.txt</Key></Object>
        </Delete>"""
        resp = await client.post("/bucket?delete", data=body.encode())
        assert resp.status == 200
        text = await resp.text()
        assert "a.txt" in text
        assert "b.txt" in text
