"""End-to-end integration tests for the WebDAV gateway against live HF Hub.

Starts the real hugbucket WebDAV server backed by HF Hub, then exercises the
full WebDAV API through plain HTTP — the same path a real client would take.

Follows the same background-thread pattern as test_boto3.py so that synchronous
urllib calls don't deadlock the server's event loop.

Requires HF_TOKEN env var.
Run with:  HF_TOKEN=hf_xxx uv run pytest -m integration tests/test_webdav_live.py -v
Skip with: uv run pytest -m 'not integration'
"""

from __future__ import annotations

import asyncio
import hashlib
import os
import threading
import time
import urllib.error
import urllib.request
from xml.etree.ElementTree import fromstring

import pytest

from aiohttp import web
from aiohttp.test_utils import TestServer

from hugbucket.config import Config
from hugbucket.bridge import Bridge
from hugbucket.protocols.webdav.app import create_webdav_app


# ── fixtures ─────────────────────────────────────────────────────────────


@pytest.fixture(scope="session")
def webdav_server(hf_token: str):
    """Start a hugbucket WebDAV gateway in a background thread.

    Runs the aiohttp server in a dedicated event loop / thread
    so synchronous urllib calls from tests don't deadlock.
    """
    loop = asyncio.new_event_loop()
    server = None
    bridge = None
    started = threading.Event()

    async def _run():
        nonlocal server, bridge
        config = Config(hf_token=hf_token)
        bridge = Bridge(config=config)
        config.hf_namespace = await bridge.hub.whoami()

        app = create_webdav_app(config=config, backend=bridge)
        # Remove the on_shutdown hook — we close bridge ourselves below.
        app.on_shutdown.clear()

        server = TestServer(app)
        await server.start_server()
        started.set()

        # Keep loop alive until cancelled
        try:
            while True:
                await asyncio.sleep(3600)
        except asyncio.CancelledError:
            pass
        finally:
            await server.close()
            await bridge.close()

    task = loop.create_task(_run())
    thread = threading.Thread(target=loop.run_until_complete, args=(task,), daemon=True)
    thread.start()
    started.wait(timeout=30)

    yield server

    loop.call_soon_threadsafe(task.cancel)
    thread.join(timeout=10)
    loop.close()


@pytest.fixture(scope="session")
def base_url(webdav_server) -> str:
    return f"http://{webdav_server.host}:{webdav_server.port}"


@pytest.fixture(scope="session")
def bucket(base_url):
    """Create a temporary bucket via MKCOL, yield its name, delete on teardown.

    Teardown lists every resource inside the bucket (via PROPFIND) and deletes
    them individually before deleting the bucket itself — so leftover files from
    failed tests never prevent bucket removal.
    """
    name = f"pytest-dav-{int(time.time()) % 100000}"
    req = urllib.request.Request(f"{base_url}/{name}/", method="MKCOL")
    resp = urllib.request.urlopen(req)
    assert resp.status == 201, f"MKCOL bucket failed: {resp.status}"

    yield name

    # ── thorough teardown ────────────────────────────────────────────
    _drain_and_delete_bucket(base_url, name)


# ── helpers ──────────────────────────────────────────────────────────────

NS = {"D": "DAV:"}


def _request(
    base_url: str,
    method: str,
    path: str,
    data: bytes | None = None,
    headers: dict | None = None,
    _retries: int = 4,
):
    """Make an HTTP request and return (status, headers, body).

    Retries transparently on 429 / 500 with exponential backoff so that
    HF Hub rate limits don't cause spurious test failures.
    """
    import time as _time

    url = f"{base_url}{path}"
    for attempt in range(_retries + 1):
        req = urllib.request.Request(url, data=data, method=method)
        if headers:
            for k, v in headers.items():
                req.add_header(k, v)
        if data is not None:
            req.add_header("Content-Length", str(len(data)))
        try:
            resp = urllib.request.urlopen(req)
            return resp.status, dict(resp.headers), resp.read()
        except urllib.error.HTTPError as e:
            code = e.code
            body = e.read()
            if code in (429, 500) and attempt < _retries:
                _time.sleep(2**attempt)
                continue
            return code, dict(e.headers), body
    # unreachable, but keeps mypy happy
    raise RuntimeError("exhausted retries")


def _drain_and_delete_bucket(base_url: str, bucket_name: str) -> None:
    """Delete every resource inside *bucket_name*, then delete the bucket.

    Uses PROPFIND Depth:1 to discover children and deletes them in reverse
    order (files before directories).  Silently ignores errors so that a
    partial cleanup never masks the original test failure.
    """
    try:
        status, _, body = _request(
            base_url, "PROPFIND", f"/{bucket_name}/", headers={"Depth": "1"}
        )
        if status == 207:
            root = fromstring(body)
            hrefs: list[str] = []
            for resp_elem in root.findall("D:response", NS):
                href_elem = resp_elem.find("D:href", NS)
                if href_elem is None or href_elem.text is None:
                    continue
                href = href_elem.text.rstrip("/")
                # Skip the bucket collection itself
                if href == f"/{bucket_name}" or href == f"/{bucket_name}/":
                    continue
                hrefs.append(href_elem.text)

            # Delete longest paths first (deeper children before parents)
            for href in sorted(hrefs, key=len, reverse=True):
                try:
                    _request(base_url, "DELETE", href)
                except Exception:
                    pass
    except Exception:
        pass

    # Now delete the (hopefully empty) bucket
    try:
        _request(base_url, "DELETE", f"/{bucket_name}/")
    except Exception:
        pass


# ── tests ────────────────────────────────────────────────────────────────


@pytest.mark.integration
class TestWebDAVOptions:
    """OPTIONS should advertise DAV compliance."""

    def test_options_root(self, base_url) -> None:
        status, headers, _ = _request(base_url, "OPTIONS", "/")
        assert status == 200
        assert "1" in headers.get("DAV", headers.get("Dav", ""))
        assert "PROPFIND" in headers.get("Allow", "")

    def test_options_resource(self, base_url, bucket) -> None:
        status, headers, _ = _request(base_url, "OPTIONS", f"/{bucket}/")
        assert status == 200
        assert "DAV" in headers or "Dav" in headers


@pytest.mark.integration
class TestWebDAVPropfind:
    """PROPFIND: browsing collections."""

    def test_propfind_root_lists_buckets(self, base_url) -> None:
        status, _, body = _request(base_url, "PROPFIND", "/", headers={"Depth": "1"})
        assert status == 207
        assert b"multistatus" in body

    def test_propfind_bucket(self, base_url, bucket) -> None:
        status, _, _ = _request(
            base_url, "PROPFIND", f"/{bucket}/", headers={"Depth": "1"}
        )
        assert status == 207

    def test_propfind_file(self, base_url, bucket) -> None:
        """PROPFIND on a file returns its properties."""
        data = b"propfind-test"
        key = "propfind-test.txt"

        # Upload
        status, _, _ = _request(base_url, "PUT", f"/{bucket}/{key}", data=data)
        assert status == 201

        # PROPFIND
        status, _, body = _request(
            base_url, "PROPFIND", f"/{bucket}/{key}", headers={"Depth": "0"}
        )
        assert status == 207
        assert b"getcontentlength" in body

        # Cleanup
        _request(base_url, "DELETE", f"/{bucket}/{key}")

    def test_propfind_missing(self, base_url, bucket) -> None:
        status, _, _ = _request(
            base_url,
            "PROPFIND",
            f"/{bucket}/no-such-file-xyz.bin",
            headers={"Depth": "0"},
        )
        assert status == 404


@pytest.mark.integration
class TestWebDAVUploadDownload:
    """PUT -> GET round-trip, verifying byte integrity."""

    def test_small_file_roundtrip(self, base_url, bucket) -> None:
        """1 KB file — single CDC chunk."""
        data = os.urandom(1024)
        key = "small.bin"

        status, _, _ = _request(base_url, "PUT", f"/{bucket}/{key}", data=data)
        assert status == 201

        status, _, downloaded = _request(base_url, "GET", f"/{bucket}/{key}")
        assert status == 200
        assert downloaded == data

        _request(base_url, "DELETE", f"/{bucket}/{key}")

    def test_large_file_roundtrip(self, base_url, bucket) -> None:
        """256 KB file — multiple CDC chunks, exercises chunked streaming."""
        data = os.urandom(256 * 1024)
        key = "big.bin"

        status, _, _ = _request(base_url, "PUT", f"/{bucket}/{key}", data=data)
        assert status == 201

        status, _, downloaded = _request(base_url, "GET", f"/{bucket}/{key}")
        assert status == 200
        assert (
            hashlib.sha256(downloaded).hexdigest() == hashlib.sha256(data).hexdigest()
        )
        assert len(downloaded) == len(data)

        _request(base_url, "DELETE", f"/{bucket}/{key}")

    def test_compressible_data_roundtrip(self, base_url, bucket) -> None:
        """Highly compressible text — guarantees LZ4 compression path."""
        data = b"The quick brown fox jumps over the lazy dog. " * 500
        key = "text.txt"

        status, _, _ = _request(base_url, "PUT", f"/{bucket}/{key}", data=data)
        assert status == 201

        status, _, downloaded = _request(base_url, "GET", f"/{bucket}/{key}")
        assert status == 200
        assert downloaded == data

        _request(base_url, "DELETE", f"/{bucket}/{key}")


@pytest.mark.integration
class TestWebDAVHead:
    """HEAD: file metadata."""

    def test_head_file(self, base_url, bucket) -> None:
        data = os.urandom(5 * 1024)
        key = "head-test.bin"
        _request(base_url, "PUT", f"/{bucket}/{key}", data=data)

        status, headers, _ = _request(base_url, "HEAD", f"/{bucket}/{key}")
        assert status == 200
        assert int(headers["Content-Length"]) == len(data)

        _request(base_url, "DELETE", f"/{bucket}/{key}")

    def test_head_missing(self, base_url, bucket) -> None:
        status, _, _ = _request(base_url, "HEAD", f"/{bucket}/no-such-file-xyz.bin")
        assert status == 404

    def test_head_bucket(self, base_url, bucket) -> None:
        status, _, _ = _request(base_url, "HEAD", f"/{bucket}/")
        assert status == 200


@pytest.mark.integration
class TestWebDAVDelete:
    """DELETE: remove files."""

    def test_delete_file(self, base_url, bucket) -> None:
        _request(base_url, "PUT", f"/{bucket}/to-delete.bin", data=b"bye")

        status, _, _ = _request(base_url, "DELETE", f"/{bucket}/to-delete.bin")
        assert status == 204

        # Confirm it's gone
        status, _, _ = _request(base_url, "HEAD", f"/{bucket}/to-delete.bin")
        assert status == 404

    def test_get_missing_returns_404(self, base_url, bucket) -> None:
        status, _, _ = _request(base_url, "GET", f"/{bucket}/does-not-exist.bin")
        assert status == 404


@pytest.mark.integration
class TestWebDAVMkcol:
    """MKCOL: directory creation."""

    def test_mkcol_creates_directory(self, base_url, bucket) -> None:
        status, _, _ = _request(base_url, "MKCOL", f"/{bucket}/subdir/")
        assert status == 201

        # Verify directory appears in PROPFIND
        status, _, body = _request(
            base_url, "PROPFIND", f"/{bucket}/", headers={"Depth": "1"}
        )
        assert status == 207
        assert b"subdir" in body

        # Clean up
        _request(base_url, "DELETE", f"/{bucket}/subdir/")


@pytest.mark.integration
class TestWebDAVRange:
    """HTTP Range requests via WebDAV GET."""

    def test_byte_range_middle(self, base_url, bucket) -> None:
        """bytes=100-199 — a slice from the middle."""
        data = os.urandom(256 * 1024)
        key = "range-test.bin"
        _request(base_url, "PUT", f"/{bucket}/{key}", data=data)

        status, _, chunk = _request(
            base_url, "GET", f"/{bucket}/{key}", headers={"Range": "bytes=100-199"}
        )
        assert status == 206
        assert len(chunk) == 100
        assert chunk == data[100:200]

        _request(base_url, "DELETE", f"/{bucket}/{key}")

    def test_byte_range_suffix(self, base_url, bucket) -> None:
        """bytes=1000- — from offset to end."""
        data = os.urandom(5 * 1024)
        key = "range-suffix.bin"
        _request(base_url, "PUT", f"/{bucket}/{key}", data=data)

        status, _, chunk = _request(
            base_url, "GET", f"/{bucket}/{key}", headers={"Range": "bytes=1000-"}
        )
        assert status == 206
        assert chunk == data[1000:]

        _request(base_url, "DELETE", f"/{bucket}/{key}")


@pytest.mark.integration
class TestWebDAVCopyMove:
    """COPY and MOVE operations."""

    def test_copy_file(self, base_url, bucket) -> None:
        data = os.urandom(2048)
        src = "copy-src.bin"
        dst = "copy-dst.bin"

        _request(base_url, "PUT", f"/{bucket}/{src}", data=data)

        status, _, _ = _request(
            base_url,
            "COPY",
            f"/{bucket}/{src}",
            headers={"Destination": f"{base_url}/{bucket}/{dst}"},
        )
        assert status == 201

        # Verify destination has the same content
        status, _, downloaded = _request(base_url, "GET", f"/{bucket}/{dst}")
        assert status == 200
        assert downloaded == data

        # Source should still exist
        status, _, _ = _request(base_url, "HEAD", f"/{bucket}/{src}")
        assert status == 200

        _request(base_url, "DELETE", f"/{bucket}/{src}")
        _request(base_url, "DELETE", f"/{bucket}/{dst}")

    def test_move_file(self, base_url, bucket) -> None:
        data = os.urandom(2048)
        src = "move-src.bin"
        dst = "move-dst.bin"

        _request(base_url, "PUT", f"/{bucket}/{src}", data=data)

        status, _, _ = _request(
            base_url,
            "MOVE",
            f"/{bucket}/{src}",
            headers={"Destination": f"{base_url}/{bucket}/{dst}"},
        )
        assert status == 201

        # Destination should have the data
        status, _, downloaded = _request(base_url, "GET", f"/{bucket}/{dst}")
        assert status == 200
        assert downloaded == data

        # Source should be gone
        status, _, _ = _request(base_url, "HEAD", f"/{bucket}/{src}")
        assert status == 404

        _request(base_url, "DELETE", f"/{bucket}/{dst}")


@pytest.mark.integration
class TestWebDAVLockUnlock:
    """LOCK/UNLOCK stubs for client compatibility."""

    def test_lock_returns_token(self, base_url, bucket) -> None:
        status, headers, _ = _request(base_url, "LOCK", f"/{bucket}/locktest.txt")
        assert status == 200
        assert "Lock-Token" in headers

    def test_unlock_succeeds(self, base_url, bucket) -> None:
        status, _, _ = _request(
            base_url,
            "UNLOCK",
            f"/{bucket}/locktest.txt",
            headers={"Lock-Token": "<opaquelocktoken:faketoken>"},
        )
        assert status == 204


@pytest.mark.integration
class TestWebDAVContentType:
    """Content-Type detection based on file extension."""

    def test_text_file(self, base_url, bucket) -> None:
        _request(base_url, "PUT", f"/{bucket}/doc.txt", data=b"hello")

        status, headers, _ = _request(base_url, "GET", f"/{bucket}/doc.txt")
        assert status == 200
        assert "text/plain" in headers.get("Content-Type", "")

        _request(base_url, "DELETE", f"/{bucket}/doc.txt")

    def test_json_file(self, base_url, bucket) -> None:
        _request(base_url, "PUT", f"/{bucket}/data.json", data=b'{"a":1}')

        status, headers, _ = _request(base_url, "GET", f"/{bucket}/data.json")
        assert status == 200
        assert "json" in headers.get("Content-Type", "")

        _request(base_url, "DELETE", f"/{bucket}/data.json")
