"""WebDAV server handler using aiohttp (RFC 4918).

Routes WebDAV requests to the StorageBackend.  Supports:
PROPFIND, PROPPATCH, GET, HEAD, PUT, DELETE, MKCOL, COPY, MOVE,
OPTIONS, LOCK, UNLOCK.

Path mapping: /<bucket>/<key> (same as S3 and FTP adapters).
"""

from __future__ import annotations

import hashlib
import logging
import mimetypes
import uuid
from datetime import datetime, timezone
from urllib.parse import unquote
from xml.etree.ElementTree import fromstring, ParseError

from aiohttp import web

from hugbucket.core.backend import StorageBackend
from hugbucket.core.models import BucketFile
from hugbucket.protocols.webdav.xml_responses import (
    error_xml,
    lock_discovery_xml,
    multistatus_xml,
    propfind_collection,
    propfind_file,
    proppatch_response_xml,
)

logger = logging.getLogger(__name__)

XML_CONTENT = "application/xml"

# Hidden placeholder used by the bridge for empty directories.
DIR_MARKER_FILENAME = ".hugbucket_keep"

# DAV compliance classes advertised in the DAV header.
# Class 1 = basic WebDAV, Class 2 = locking support (stub).
DAV_COMPLIANCE = "1, 2"

ALLOWED_METHODS = (
    "OPTIONS, GET, HEAD, PUT, DELETE, "
    "MKCOL, COPY, MOVE, PROPFIND, PROPPATCH, LOCK, UNLOCK"
)


def _format_last_modified(ts: str | None) -> str:
    """Format an ISO timestamp as RFC 1123 HTTP date."""
    if ts:
        try:
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            return dt.strftime("%a, %d %b %Y %H:%M:%S GMT")
        except Exception:
            pass
    return datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S GMT")


def _parse_path(request: web.Request) -> tuple[str, str]:
    """Extract (bucket, key) from the request path.

    /<bucket>/<key...>  ->  ("bucket", "key...")
    /<bucket>/          ->  ("bucket", "")
    /                   ->  ("", "")
    """
    path = unquote(request.path).lstrip("/")
    if not path:
        return "", ""
    parts = path.split("/", 1)
    bucket = parts[0]
    key = parts[1] if len(parts) > 1 else ""
    return bucket, key


def _dav_error(status: int, message: str) -> web.Response:
    return web.Response(
        status=status,
        content_type=XML_CONTENT,
        body=error_xml(status, message),
    )


def _depth(request: web.Request) -> int:
    """Parse the Depth header (default 1 for PROPFIND)."""
    raw = request.headers.get("Depth", "1")
    if raw == "infinity":
        return 999  # treat as deep
    try:
        return int(raw)
    except ValueError:
        return 1


class WebDAVHandler:
    """Handles WebDAV requests and maps them to StorageBackend calls."""

    def __init__(self, backend: StorageBackend) -> None:
        self.backend = backend

    def setup_routes(self, app: web.Application) -> None:
        app.router.add_route("OPTIONS", "/", self.handle_options)
        app.router.add_route("OPTIONS", "/{path:.*}", self.handle_options)
        app.router.add_route("PROPFIND", "/", self.handle_propfind)
        app.router.add_route("PROPFIND", "/{path:.*}", self.handle_propfind)
        app.router.add_route("PROPPATCH", "/{path:.*}", self.handle_proppatch)
        app.router.add_route("GET", "/{path:.*}", self.handle_get)
        app.router.add_route("HEAD", "/{path:.*}", self.handle_head)
        app.router.add_route("PUT", "/{path:.*}", self.handle_put)
        app.router.add_route("DELETE", "/", self.handle_delete)
        app.router.add_route("DELETE", "/{path:.*}", self.handle_delete)
        app.router.add_route("MKCOL", "/{path:.*}", self.handle_mkcol)
        app.router.add_route("COPY", "/{path:.*}", self.handle_copy)
        app.router.add_route("MOVE", "/{path:.*}", self.handle_move)
        app.router.add_route("LOCK", "/{path:.*}", self.handle_lock)
        app.router.add_route("UNLOCK", "/{path:.*}", self.handle_unlock)

    # ── OPTIONS ──────────────────────────────────────────────────────────

    async def handle_options(self, request: web.Request) -> web.Response:
        return web.Response(
            status=200,
            headers={
                "DAV": DAV_COMPLIANCE,
                "Allow": ALLOWED_METHODS,
                "Content-Length": "0",
                "MS-Author-Via": "DAV",
            },
        )

    # ── PROPFIND ─────────────────────────────────────────────────────────

    async def handle_propfind(self, request: web.Request) -> web.Response:
        """PROPFIND: list properties for a resource or collection.

        Depth: 0 = self only, 1 = self + direct children.
        """
        try:
            bucket, key = _parse_path(request)
            depth = _depth(request)

            # Root: list buckets
            if not bucket:
                return await self._propfind_root(request, depth)

            # Bucket-level (no key, or just trailing slash)
            if not key or key == "":
                return await self._propfind_bucket(request, bucket, depth)

            # Key might be a file or a directory prefix
            # Check if it's a directory (trailing slash or has children)
            if key.endswith("/"):
                return await self._propfind_directory(request, bucket, key, depth)

            # Try as file first
            file_info = await self.backend.head_object(bucket, key)
            if file_info is not None:
                return self._propfind_file_response(request, bucket, key, file_info)

            # Try as directory (no trailing slash)
            is_dir = await self.backend.head_directory(bucket, key + "/")
            if is_dir:
                return await self._propfind_directory(request, bucket, key + "/", depth)

            return _dav_error(404, "Not Found")
        except Exception as e:
            logger.exception("PROPFIND failed")
            return _dav_error(500, str(e))

    async def _propfind_root(self, request: web.Request, depth: int) -> web.Response:
        """PROPFIND on / — list all buckets."""
        responses = [propfind_collection("/", displayname="")]

        if depth >= 1:
            buckets = await self.backend.list_buckets()
            for b in buckets:
                name = b.id.split("/")[-1] if "/" in b.id else b.id
                href = f"/{name}/"
                responses.append(
                    propfind_collection(
                        href,
                        displayname=name,
                        created=b.created_at,
                        modified=b.created_at,
                    )
                )

        body = multistatus_xml(responses)
        return web.Response(status=207, content_type=XML_CONTENT, body=body)

    async def _propfind_bucket(
        self, request: web.Request, bucket: str, depth: int
    ) -> web.Response:
        """PROPFIND on /<bucket>/ — list bucket contents."""
        info = await self.backend.head_bucket(bucket)
        if info is None:
            return _dav_error(404, "Not Found")

        href = f"/{bucket}/"
        responses = [
            propfind_collection(
                href,
                displayname=bucket,
                created=info.created_at,
                modified=info.created_at,
            )
        ]

        if depth >= 1:
            result = await self.backend.list_objects(
                bucket, prefix="", delimiter="/", max_keys=10000
            )
            responses.extend(self._listing_to_responses(bucket, "", result))

        body = multistatus_xml(responses)
        return web.Response(status=207, content_type=XML_CONTENT, body=body)

    async def _propfind_directory(
        self, request: web.Request, bucket: str, prefix: str, depth: int
    ) -> web.Response:
        """PROPFIND on /<bucket>/<prefix>/ — list directory contents."""
        # Normalize: ensure prefix ends with /
        if not prefix.endswith("/"):
            prefix += "/"

        display = prefix.rstrip("/").rsplit("/", 1)[-1] if prefix != "/" else ""
        href = f"/{bucket}/{prefix}"
        responses = [propfind_collection(href, displayname=display)]

        if depth >= 1:
            result = await self.backend.list_objects(
                bucket, prefix=prefix, delimiter="/", max_keys=10000
            )
            responses.extend(self._listing_to_responses(bucket, prefix, result))

        body = multistatus_xml(responses)
        return web.Response(status=207, content_type=XML_CONTENT, body=body)

    def _listing_to_responses(
        self, bucket: str, prefix: str, result: dict
    ) -> list[tuple[str, str]]:
        """Convert a list_objects result to PROPFIND response entries."""
        responses: list[tuple[str, str]] = []

        # Files
        for f in result.get("contents", []):
            # Skip directory markers
            if f.path.endswith("/" + DIR_MARKER_FILENAME):
                continue
            href = f"/{bucket}/{f.path}"
            content_type = mimetypes.guess_type(f.path)[0] or "application/octet-stream"
            etag = f.xet_hash[:32] if f.xet_hash else ""
            responses.append(
                propfind_file(
                    href,
                    size=f.size,
                    content_type=content_type,
                    etag=etag,
                    modified=f.mtime or f.uploaded_at,
                    created=f.uploaded_at,
                    displayname=f.path.rsplit("/", 1)[-1],
                )
            )

        # Sub-directories (common prefixes)
        for cp in result.get("common_prefixes", []):
            display = cp.rstrip("/").rsplit("/", 1)[-1]
            href = f"/{bucket}/{cp}"
            responses.append(propfind_collection(href, displayname=display))

        return responses

    def _propfind_file_response(
        self,
        request: web.Request,
        bucket: str,
        key: str,
        file_info: BucketFile,
    ) -> web.Response:
        """PROPFIND response for a single file."""
        href = f"/{bucket}/{key}"
        content_type = mimetypes.guess_type(key)[0] or "application/octet-stream"
        etag = file_info.xet_hash[:32] if file_info.xet_hash else ""
        responses = [
            propfind_file(
                href,
                size=file_info.size,
                content_type=content_type,
                etag=etag,
                modified=file_info.mtime or file_info.uploaded_at,
                created=file_info.uploaded_at,
                displayname=key.rsplit("/", 1)[-1],
            )
        ]
        body = multistatus_xml(responses)
        return web.Response(status=207, content_type=XML_CONTENT, body=body)

    # ── PROPPATCH ────────────────────────────────────────────────────────

    async def handle_proppatch(self, request: web.Request) -> web.Response:
        """PROPPATCH: accept property changes (stub — no persistence)."""
        try:
            bucket, key = _parse_path(request)
            href = request.path

            body = await request.read()
            prop_names: list[str] = []
            try:
                root = fromstring(body)
                # Extract property names from set/remove elements
                ns = {"D": "DAV:"}
                for prop_el in root.findall(".//D:prop/*", ns):
                    tag = prop_el.tag
                    if tag.startswith("{DAV:}"):
                        prop_names.append(tag[6:])
                    else:
                        prop_names.append(tag)
                # Also try without namespace
                if not prop_names:
                    for prop_el in root.findall(".//prop/*"):
                        prop_names.append(prop_el.tag)
            except ParseError:
                pass

            if not prop_names:
                prop_names = ["getlastmodified"]

            xml_body = proppatch_response_xml(href, prop_names)
            return web.Response(status=207, content_type=XML_CONTENT, body=xml_body)
        except Exception as e:
            logger.exception("PROPPATCH failed")
            return _dav_error(500, str(e))

    # ── GET ──────────────────────────────────────────────────────────────

    async def handle_get(
        self, request: web.Request
    ) -> web.Response | web.StreamResponse:
        """GET: download a file."""
        try:
            bucket, key = _parse_path(request)
            if not bucket or not key:
                return _dav_error(405, "Cannot GET a collection")

            # Remove trailing slash — GET on directories is not typical
            key = key.rstrip("/")

            file_info = await self.backend.head_object(bucket, key)
            if file_info is None:
                return _dav_error(404, "Not Found")

            content_type = mimetypes.guess_type(key)[0] or "application/octet-stream"
            etag = (
                f'"{file_info.xet_hash[:32]}"'
                if file_info.xet_hash
                else f'"{hashlib.md5(b"").hexdigest()}"'
            )
            last_modified = _format_last_modified(
                file_info.mtime or file_info.uploaded_at
            )
            total_size = file_info.size

            # Parse Range header
            byte_range: tuple[int, int] | None = None
            range_header = request.headers.get("Range", "")
            if range_header and range_header.startswith("bytes="):
                range_spec = range_header[6:]
                parts = range_spec.split("-")
                start = int(parts[0]) if parts[0] else 0
                end = int(parts[1]) if parts[1] else total_size - 1
                end = min(end, total_size - 1)
                if start >= total_size or start > end:
                    return web.Response(
                        status=416,
                        headers={"Content-Range": f"bytes */{total_size}"},
                    )
                byte_range = (start, end)

            stream = await self.backend.get_object_stream(
                bucket, key, file_info=file_info, byte_range=byte_range
            )
            if stream is None:
                return _dav_error(404, "Not Found")

            if byte_range is not None:
                start, end = byte_range
                slice_length = end - start + 1
                response = web.StreamResponse(
                    status=206,
                    headers={
                        "ETag": etag,
                        "Content-Length": str(slice_length),
                        "Content-Range": f"bytes {start}-{end}/{total_size}",
                        "Accept-Ranges": "bytes",
                        "Last-Modified": last_modified,
                    },
                )
            else:
                response = web.StreamResponse(
                    status=200,
                    headers={
                        "ETag": etag,
                        "Content-Length": str(total_size),
                        "Accept-Ranges": "bytes",
                        "Last-Modified": last_modified,
                    },
                )

            response.content_type = content_type
            await response.prepare(request)

            async for chunk in stream:
                await response.write(chunk)

            await response.write_eof()
            return response
        except ConnectionResetError, ConnectionError:
            logger.debug("GET: client disconnected for %s", request.path)
            return web.Response(status=499)
        except Exception as e:
            logger.exception("GET failed")
            return _dav_error(500, str(e))

    # ── HEAD ─────────────────────────────────────────────────────────────

    async def handle_head(self, request: web.Request) -> web.Response:
        """HEAD: return file metadata."""
        try:
            bucket, key = _parse_path(request)
            if not bucket:
                return web.Response(
                    status=200,
                    headers={
                        "Content-Type": "httpd/unix-directory",
                        "Content-Length": "0",
                    },
                )

            if not key or key == "":
                info = await self.backend.head_bucket(bucket)
                if info is None:
                    return _dav_error(404, "Not Found")
                return web.Response(
                    status=200,
                    headers={
                        "Content-Type": "httpd/unix-directory",
                        "Content-Length": "0",
                    },
                )

            clean_key = key.rstrip("/")
            file_info = await self.backend.head_object(bucket, clean_key)
            if file_info is not None:
                content_type = (
                    mimetypes.guess_type(clean_key)[0] or "application/octet-stream"
                )
                etag = f'"{file_info.xet_hash[:32]}"' if file_info.xet_hash else '""'
                last_modified = _format_last_modified(
                    file_info.mtime or file_info.uploaded_at
                )
                return web.Response(
                    status=200,
                    headers={
                        "Content-Length": str(file_info.size),
                        "Content-Type": content_type,
                        "ETag": etag,
                        "Last-Modified": last_modified,
                        "Accept-Ranges": "bytes",
                    },
                )

            # Check if it's a directory
            prefix = clean_key + "/"
            is_dir = await self.backend.head_directory(bucket, prefix)
            if is_dir:
                return web.Response(
                    status=200,
                    headers={
                        "Content-Type": "httpd/unix-directory",
                        "Content-Length": "0",
                    },
                )

            return _dav_error(404, "Not Found")
        except Exception as e:
            logger.exception("HEAD failed")
            return _dav_error(500, str(e))

    # ── PUT ──────────────────────────────────────────────────────────────

    async def handle_put(self, request: web.Request) -> web.Response:
        """PUT: upload a file."""
        try:
            bucket, key = _parse_path(request)
            if not bucket or not key:
                return _dav_error(405, "Cannot PUT to a collection root")

            # Reject PUT on collection paths (trailing slash means MKCOL)
            if key.endswith("/"):
                return _dav_error(405, "Use MKCOL to create collections")

            data = await request.read()
            result = await self.backend.put_object(bucket, key, data)

            return web.Response(
                status=201,
                headers={
                    "ETag": result.get("ETag", ""),
                    "Content-Length": "0",
                },
            )
        except Exception as e:
            logger.exception("PUT failed")
            return _dav_error(500, str(e))

    # ── DELETE ───────────────────────────────────────────────────────────

    async def handle_delete(self, request: web.Request) -> web.Response:
        """DELETE: remove a file or collection."""
        try:
            bucket, key = _parse_path(request)
            if not bucket:
                return _dav_error(403, "Cannot delete root")

            # Delete bucket
            if not key or key == "":
                await self.backend.delete_bucket(bucket)
                return web.Response(status=204)

            clean_key = key.rstrip("/")

            # Check if it's a file
            file_info = await self.backend.head_object(bucket, clean_key)
            if file_info is not None:
                await self.backend.delete_object(bucket, clean_key)
                return web.Response(status=204)

            # Try deleting as a directory (delete all contents recursively)
            prefix = clean_key + "/"
            result = await self.backend.list_objects(
                bucket, prefix=prefix, delimiter="", max_keys=10000
            )
            files = result.get("contents", [])
            if files:
                keys_to_delete = [f.path for f in files]
                await self.backend.delete_objects(bucket, keys_to_delete)
                return web.Response(status=204)

            # Also try to delete any directory markers
            await self.backend.delete_object(bucket, prefix)
            return web.Response(status=204)
        except Exception as e:
            logger.exception("DELETE failed")
            return _dav_error(500, str(e))

    # ── MKCOL ────────────────────────────────────────────────────────────

    async def handle_mkcol(self, request: web.Request) -> web.Response:
        """MKCOL: create a collection (directory or bucket)."""
        try:
            bucket, key = _parse_path(request)
            if not bucket:
                return _dav_error(403, "Cannot create root collection")

            # MKCOL with a body is unsupported (RFC 4918 Section 9.3)
            body = await request.read()
            if body:
                return _dav_error(415, "Unsupported Media Type")

            # Create bucket
            if not key or key == "":
                await self.backend.create_bucket(bucket)
                return web.Response(status=201)

            # Create directory inside bucket
            # Ensure the bucket exists
            bucket_info = await self.backend.head_bucket(bucket)
            if bucket_info is None:
                return _dav_error(409, "Conflict — parent bucket does not exist")

            clean_key = key.rstrip("/")
            prefix = clean_key + "/"
            await self.backend.put_object(bucket, prefix, b"")
            return web.Response(status=201)
        except Exception as e:
            logger.exception("MKCOL failed")
            return _dav_error(500, str(e))

    # ── COPY ─────────────────────────────────────────────────────────────

    async def handle_copy(self, request: web.Request) -> web.Response:
        """COPY: copy a resource to a new location."""
        try:
            src_bucket, src_key = _parse_path(request)
            if not src_bucket or not src_key:
                return _dav_error(403, "Cannot copy root or bucket")

            dest_path = self._parse_destination(request)
            if dest_path is None:
                return _dav_error(400, "Missing Destination header")

            dst_bucket, dst_key = self._split_path(dest_path)
            if not dst_bucket or not dst_key:
                return _dav_error(400, "Invalid destination path")

            overwrite = request.headers.get("Overwrite", "T") == "T"

            # Check if destination already exists
            if not overwrite:
                existing = await self.backend.head_object(dst_bucket, dst_key)
                if existing is not None:
                    return _dav_error(412, "Precondition Failed — destination exists")

            src_key = src_key.rstrip("/")
            dst_key = dst_key.rstrip("/")

            # Check if source is a file
            src_info = await self.backend.head_object(src_bucket, src_key)
            if src_info is not None:
                await self.backend.copy_object(src_bucket, src_key, dst_bucket, dst_key)
                return web.Response(status=201)

            # Source might be a directory — copy recursively
            src_prefix = src_key + "/"
            dst_prefix = dst_key + "/"
            result = await self.backend.list_objects(
                src_bucket, prefix=src_prefix, delimiter="", max_keys=10000
            )
            files = result.get("contents", [])
            if not files:
                return _dav_error(404, "Source not found")

            for f in files:
                rel = f.path[len(src_prefix) :]
                new_key = dst_prefix + rel
                await self.backend.copy_object(src_bucket, f.path, dst_bucket, new_key)

            return web.Response(status=201)
        except FileNotFoundError:
            return _dav_error(404, "Source not found")
        except Exception as e:
            logger.exception("COPY failed")
            return _dav_error(500, str(e))

    # ── MOVE ─────────────────────────────────────────────────────────────

    async def handle_move(self, request: web.Request) -> web.Response:
        """MOVE: move a resource (copy + delete source)."""
        try:
            src_bucket, src_key = _parse_path(request)
            if not src_bucket or not src_key:
                return _dav_error(403, "Cannot move root or bucket")

            dest_path = self._parse_destination(request)
            if dest_path is None:
                return _dav_error(400, "Missing Destination header")

            dst_bucket, dst_key = self._split_path(dest_path)
            if not dst_bucket or not dst_key:
                return _dav_error(400, "Invalid destination path")

            overwrite = request.headers.get("Overwrite", "T") == "T"

            if not overwrite:
                existing = await self.backend.head_object(dst_bucket, dst_key)
                if existing is not None:
                    return _dav_error(412, "Precondition Failed — destination exists")

            src_key = src_key.rstrip("/")
            dst_key = dst_key.rstrip("/")

            # Check if source is a file
            src_info = await self.backend.head_object(src_bucket, src_key)
            if src_info is not None:
                await self.backend.copy_object(src_bucket, src_key, dst_bucket, dst_key)
                await self.backend.delete_object(src_bucket, src_key)
                return web.Response(status=201)

            # Source might be a directory — move recursively
            src_prefix = src_key + "/"
            dst_prefix = dst_key + "/"
            result = await self.backend.list_objects(
                src_bucket, prefix=src_prefix, delimiter="", max_keys=10000
            )
            files = result.get("contents", [])
            if not files:
                return _dav_error(404, "Source not found")

            for f in files:
                rel = f.path[len(src_prefix) :]
                new_key = dst_prefix + rel
                await self.backend.copy_object(src_bucket, f.path, dst_bucket, new_key)

            # Delete originals
            keys_to_delete = [f.path for f in files]
            await self.backend.delete_objects(src_bucket, keys_to_delete)

            return web.Response(status=201)
        except FileNotFoundError:
            return _dav_error(404, "Source not found")
        except Exception as e:
            logger.exception("MOVE failed")
            return _dav_error(500, str(e))

    # ── LOCK ─────────────────────────────────────────────────────────────

    async def handle_lock(self, request: web.Request) -> web.Response:
        """LOCK: stub lock for client compatibility.

        HugBucket does not implement true locking.  This stub returns a
        fake lock token so that clients like Windows Explorer and macOS
        Finder can function.
        """
        try:
            token = uuid.uuid4().hex
            owner = ""

            # Try to extract owner from the request body
            body = await request.read()
            if body:
                try:
                    root = fromstring(body)
                    ns = {"D": "DAV:"}
                    owner_el = root.find(".//D:owner/D:href", ns)
                    if owner_el is not None and owner_el.text:
                        owner = owner_el.text
                    else:
                        owner_el = root.find(".//D:owner", ns)
                        if owner_el is not None and owner_el.text:
                            owner = owner_el.text
                except ParseError:
                    pass

            xml_body = lock_discovery_xml(request.path, token, owner)
            return web.Response(
                status=200,
                content_type=XML_CONTENT,
                headers={
                    "Lock-Token": f"<opaquelocktoken:{token}>",
                },
                body=xml_body,
            )
        except Exception as e:
            logger.exception("LOCK failed")
            return _dav_error(500, str(e))

    # ── UNLOCK ───────────────────────────────────────────────────────────

    async def handle_unlock(self, request: web.Request) -> web.Response:
        """UNLOCK: stub — always succeed."""
        return web.Response(status=204)

    # ── Helpers ──────────────────────────────────────────────────────────

    def _parse_destination(self, request: web.Request) -> str | None:
        """Extract and normalize the Destination header value."""
        dest = request.headers.get("Destination", "")
        if not dest:
            return None

        # Destination is an absolute URI — strip the scheme + host.
        # e.g. "http://localhost:8080/bucket/file.txt" -> "/bucket/file.txt"
        if "://" in dest:
            # Find the path after the host
            after_scheme = dest.split("://", 1)[1]
            slash_idx = after_scheme.find("/")
            if slash_idx >= 0:
                dest = after_scheme[slash_idx:]
            else:
                dest = "/"

        return unquote(dest)

    @staticmethod
    def _split_path(path: str) -> tuple[str, str]:
        """Split a path into (bucket, key)."""
        path = path.lstrip("/")
        if not path:
            return "", ""
        parts = path.split("/", 1)
        bucket = parts[0]
        key = parts[1] if len(parts) > 1 else ""
        return bucket, key
