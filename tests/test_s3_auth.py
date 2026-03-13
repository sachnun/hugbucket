"""Tests for AWS Signature V4 authentication middleware.

Verifies that:
- Requests without credentials are rejected (403) with correct S3 error XML.
- Requests with wrong credentials return the right S3 error code.
- Properly signed requests (header-based) are accepted.
- Error XML format matches real AWS S3 (no xmlns, has HostId, etc.).
"""

from __future__ import annotations

import hashlib
import hmac
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock
from urllib.parse import quote
from xml.etree.ElementTree import fromstring

import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient

from hugbucket.config import Config
from hugbucket.hub.client import BucketFile
from hugbucket.s3.auth import s3_auth_middleware
from hugbucket.s3.server import S3Handler

TEST_ACCESS_KEY = "test-access-key"
TEST_SECRET_KEY = "test-secret-key"
TEST_REGION = "us-east-1"
TEST_SERVICE = "s3"


# ── signing helper (mirrors the server-side verification logic) ──────────


def _hmac_sha256(key: bytes, msg: str) -> bytes:
    return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()


def _sign_request(
    method: str,
    path: str,
    *,
    host: str = "localhost",
    query_string: str = "",
    payload: bytes = b"",
    access_key: str = TEST_ACCESS_KEY,
    secret_key: str = TEST_SECRET_KEY,
    region: str = TEST_REGION,
    extra_headers: dict[str, str] | None = None,
) -> dict[str, str]:
    """Compute AWS SigV4 Authorization header and return all required headers."""
    now = datetime.now(timezone.utc)
    amz_date = now.strftime("%Y%m%dT%H%M%SZ")
    date_stamp = now.strftime("%Y%m%d")
    payload_hash = hashlib.sha256(payload).hexdigest()

    headers: dict[str, str] = {
        "Host": host,
        "x-amz-date": amz_date,
        "x-amz-content-sha256": payload_hash,
    }
    if extra_headers:
        headers.update(extra_headers)

    # Canonical URI
    segments = path.split("/")
    canon_uri = "/".join(quote(seg, safe="-_.~") for seg in segments) or "/"

    # Canonical query string
    if query_string:
        from urllib.parse import unquote as _unquote

        params = []
        for part in query_string.split("&"):
            if "=" in part:
                k, v = part.split("=", 1)
                params.append((_unquote(k), _unquote(v)))
            else:
                params.append((_unquote(part), ""))
        params.sort()
        canon_qs = "&".join(
            f"{quote(k, safe='-_.~')}={quote(v, safe='-_.~')}" for k, v in params
        )
    else:
        canon_qs = ""

    # Signed headers (sorted, lowercase)
    signed_header_names = sorted(k.lower() for k in headers)
    signed_headers_str = ";".join(signed_header_names)

    # Canonical headers
    canon_headers = ""
    for name in signed_header_names:
        for k, v in headers.items():
            if k.lower() == name:
                canon_headers += f"{name}:{' '.join(v.split())}\n"
                break

    canonical_request = "\n".join(
        [method, canon_uri, canon_qs, canon_headers, signed_headers_str, payload_hash]
    )

    credential_scope = f"{date_stamp}/{region}/{TEST_SERVICE}/aws4_request"
    string_to_sign = "\n".join(
        [
            "AWS4-HMAC-SHA256",
            amz_date,
            credential_scope,
            hashlib.sha256(canonical_request.encode()).hexdigest(),
        ]
    )

    k_date = _hmac_sha256(("AWS4" + secret_key).encode("utf-8"), date_stamp)
    k_region = _hmac_sha256(k_date, region)
    k_service = _hmac_sha256(k_region, TEST_SERVICE)
    k_signing = _hmac_sha256(k_service, "aws4_request")
    signature = hmac.new(k_signing, string_to_sign.encode(), hashlib.sha256).hexdigest()

    credential = f"{access_key}/{credential_scope}"
    headers["Authorization"] = (
        f"AWS4-HMAC-SHA256 Credential={credential}, "
        f"SignedHeaders={signed_headers_str}, "
        f"Signature={signature}"
    )
    return headers


# ── XML assertion helpers ────────────────────────────────────────────────


def _parse_error_xml(body: str) -> dict[str, str]:
    """Parse an S3 error XML body into a flat dict of element tag → text."""
    root = fromstring(body)
    return {child.tag: (child.text or "") for child in root}


def _assert_s3_error_structure(body: str) -> dict[str, str]:
    """Assert the XML matches the real AWS S3 error format and return fields."""
    # Must NOT have xmlns (only success responses have it)
    assert 'xmlns="' not in body, "Error XML must not contain xmlns"
    assert body.startswith('<?xml version="1.0" encoding="UTF-8"?>')

    fields = _parse_error_xml(body)

    # AWS always includes these
    assert "Code" in fields
    assert "Message" in fields
    assert "RequestId" in fields
    assert "HostId" in fields

    return fields


# ── fixtures ─────────────────────────────────────────────────────────────


@pytest.fixture
def config() -> Config:
    return Config(
        s3_access_key=TEST_ACCESS_KEY,
        s3_secret_key=TEST_SECRET_KEY,
        region=TEST_REGION,
    )


@pytest.fixture
def mock_bridge() -> MagicMock:
    bridge = MagicMock()
    bridge.list_buckets = AsyncMock(return_value=[])
    bridge.create_bucket = AsyncMock(return_value="")
    bridge.delete_bucket = AsyncMock()
    bridge.head_bucket = AsyncMock(return_value=None)
    bridge.put_object = AsyncMock(return_value={"ETag": '"abc123"', "size": 0})
    bridge.get_object = AsyncMock(return_value=b"hello")
    bridge.delete_object = AsyncMock()
    bridge.head_object = AsyncMock(
        return_value=BucketFile(
            type="file",
            path="key.txt",
            size=5,
            xet_hash="a" * 64,
            mtime="2026-01-01T00:00:00Z",
        )
    )
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
def app(mock_bridge: MagicMock, config: Config) -> web.Application:
    """Create aiohttp app WITH auth middleware."""
    application = web.Application(
        client_max_size=16 * 1024 * 1024,
        middlewares=[s3_auth_middleware],
    )
    application["config"] = config
    handler = S3Handler(mock_bridge)
    handler.setup_routes(application)
    return application


@pytest.fixture
async def client(aiohttp_client, app: web.Application) -> TestClient:
    return await aiohttp_client(app)


# ── tests: error XML format matches real AWS S3 ─────────────────────────


class TestErrorXMLFormat:
    """Verify error responses match the real AWS S3 XML structure."""

    async def test_no_xmlns_on_error(self, client: TestClient) -> None:
        """AWS S3 error XML does NOT carry xmlns (unlike success responses)."""
        resp = await client.get("/")
        body = await resp.text()
        assert 'xmlns="' not in body

    async def test_has_required_elements(self, client: TestClient) -> None:
        """Error must contain Code, Message, RequestId, HostId."""
        resp = await client.get("/")
        body = await resp.text()
        fields = _assert_s3_error_structure(body)
        assert fields["Code"] == "AccessDenied"
        assert fields["Message"] == "Access Denied"

    async def test_has_resource_element(self, client: TestClient) -> None:
        """Error should include the Resource that was accessed."""
        resp = await client.get("/bucket/key.txt")
        body = await resp.text()
        fields = _assert_s3_error_structure(body)
        assert fields.get("Resource") == "/bucket/key.txt"


# ── tests: unauthenticated requests → AccessDenied ──────────────────────


class TestUnauthenticatedRejection:
    """Requests without any AWS credentials must return 403 AccessDenied."""

    async def test_no_auth_get_root(self, client: TestClient) -> None:
        resp = await client.get("/")
        assert resp.status == 403
        fields = _assert_s3_error_structure(await resp.text())
        assert fields["Code"] == "AccessDenied"

    async def test_no_auth_get_object(self, client: TestClient) -> None:
        resp = await client.get("/bucket/key.txt")
        assert resp.status == 403

    async def test_no_auth_put_object(self, client: TestClient) -> None:
        resp = await client.put("/bucket/key.txt", data=b"hello")
        assert resp.status == 403

    async def test_no_auth_delete_object(self, client: TestClient) -> None:
        resp = await client.delete("/bucket/key.txt")
        assert resp.status == 403

    async def test_no_auth_head_object(self, client: TestClient) -> None:
        resp = await client.head("/bucket/key.txt")
        assert resp.status == 403

    async def test_no_auth_list_objects(self, client: TestClient) -> None:
        resp = await client.get("/bucket?list-type=2")
        assert resp.status == 403

    async def test_garbage_auth_header(self, client: TestClient) -> None:
        resp = await client.get("/", headers={"Authorization": "garbage"})
        assert resp.status == 403
        fields = _assert_s3_error_structure(await resp.text())
        assert fields["Code"] == "AccessDenied"

    async def test_wrong_algorithm(self, client: TestClient) -> None:
        resp = await client.get(
            "/",
            headers={
                "Authorization": "AWS4-HMAC-SHA1 Credential=x, SignedHeaders=host, Signature=x"
            },
        )
        assert resp.status == 403


# ── tests: wrong credentials → specific error codes ─────────────────────


class TestWrongCredentials:
    async def test_wrong_access_key_returns_InvalidAccessKeyId(
        self, client: TestClient
    ) -> None:
        """AWS returns InvalidAccessKeyId when the key is unknown."""
        headers = _sign_request("GET", "/", access_key="wrong-key")
        headers.pop("Host", None)
        resp = await client.get("/", headers=headers)
        assert resp.status == 403
        fields = _assert_s3_error_structure(await resp.text())
        assert fields["Code"] == "InvalidAccessKeyId"
        assert "AWSAccessKeyId" in fields
        assert fields["AWSAccessKeyId"] == "wrong-key"

    async def test_wrong_secret_key_returns_SignatureDoesNotMatch(
        self, client: TestClient
    ) -> None:
        """AWS returns SignatureDoesNotMatch when the signature is wrong."""
        headers = _sign_request("GET", "/", secret_key="wrong-secret")
        headers.pop("Host", None)
        resp = await client.get("/", headers=headers)
        assert resp.status == 403
        fields = _assert_s3_error_structure(await resp.text())
        assert fields["Code"] == "SignatureDoesNotMatch"
        assert "AWSAccessKeyId" in fields
        assert "StringToSign" in fields
        assert "SignatureProvided" in fields


# ── tests: properly signed requests are accepted ────────────────────────


class TestValidAuth:
    async def test_signed_get_root(self, client: TestClient) -> None:
        """Signed ListBuckets should return 200."""
        host = f"{client.host}:{client.port}"
        headers = _sign_request("GET", "/", host=host)
        headers.pop("Host", None)
        resp = await client.get("/", headers=headers)
        assert resp.status == 200
        body = await resp.text()
        assert "ListAllMyBucketsResult" in body

    async def test_signed_get_object(self, client: TestClient) -> None:
        host = f"{client.host}:{client.port}"
        headers = _sign_request("GET", "/bucket/key.txt", host=host)
        headers.pop("Host", None)
        resp = await client.get("/bucket/key.txt", headers=headers)
        assert resp.status == 200
        body = await resp.read()
        assert body == b"hello"

    async def test_signed_put_object(self, client: TestClient) -> None:
        host = f"{client.host}:{client.port}"
        payload = b"new data"
        headers = _sign_request("PUT", "/bucket/newkey.txt", host=host, payload=payload)
        headers.pop("Host", None)
        resp = await client.put("/bucket/newkey.txt", data=payload, headers=headers)
        assert resp.status == 200

    async def test_signed_delete_object(self, client: TestClient) -> None:
        host = f"{client.host}:{client.port}"
        headers = _sign_request("DELETE", "/bucket/key.txt", host=host)
        headers.pop("Host", None)
        resp = await client.delete("/bucket/key.txt", headers=headers)
        assert resp.status == 204

    async def test_signed_head_object(self, client: TestClient) -> None:
        host = f"{client.host}:{client.port}"
        headers = _sign_request("HEAD", "/bucket/key.txt", host=host)
        headers.pop("Host", None)
        resp = await client.head("/bucket/key.txt", headers=headers)
        assert resp.status == 200

    async def test_signed_list_objects(self, client: TestClient) -> None:
        host = f"{client.host}:{client.port}"
        headers = _sign_request("GET", "/bucket", host=host, query_string="list-type=2")
        headers.pop("Host", None)
        resp = await client.get("/bucket?list-type=2", headers=headers)
        assert resp.status == 200
