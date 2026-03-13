"""S3-compatible HTTP server using aiohttp.

Routes S3 REST API requests to the Bridge layer.
Supports: ListBuckets, CreateBucket, DeleteBucket, HeadBucket,
          ListObjectsV2, PutObject, GetObject, DeleteObject, DeleteObjects,
          HeadObject.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import mimetypes
import uuid
from datetime import datetime, timezone
from urllib.parse import unquote
from xml.etree.ElementTree import fromstring

from aiohttp import web

from hugbucket.bridge import Bridge
from hugbucket.s3.xml_responses import (
    list_buckets_xml,
    list_objects_v2_xml,
    error_xml,
    delete_result_xml,
    initiate_multipart_upload_xml,
    complete_multipart_upload_xml,
)

logger = logging.getLogger(__name__)

XML_CONTENT = "application/xml"

# Paths commonly requested by browsers/crawlers that are not valid S3 operations.
# Reject these early to avoid unnecessary API calls to HuggingFace.
_IGNORED_PATHS = {"favicon.ico", "robots.txt", "sitemap.xml", ".well-known"}


def _request_id() -> str:
    return uuid.uuid4().hex[:16].upper()


def _format_last_modified(ts: str | None) -> str:
    """Format an ISO timestamp as HTTP Last-Modified (RFC 7231).

    Example: "Wed, 11 Mar 2026 11:44:58 GMT"
    """
    if ts:
        try:
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            return dt.strftime("%a, %d %b %Y %H:%M:%S GMT")
        except Exception:
            pass
    return datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S GMT")


def _s3_error(status: int, code: str, message: str, resource: str = "") -> web.Response:
    return web.Response(
        status=status,
        content_type=XML_CONTENT,
        body=error_xml(code, message, resource, _request_id()),
    )


def _parse_bucket_key(request: web.Request) -> tuple[str, str]:
    """Extract bucket name and object key from request path.

    Path-style: /{bucket}/{key...}
    """
    path = unquote(request.path)
    # Remove leading /
    path = path.lstrip("/")
    if not path:
        return "", ""

    parts = path.split("/", 1)
    bucket = parts[0]
    key = parts[1] if len(parts) > 1 else ""
    return bucket, key


class S3Handler:
    """Handles S3 API requests."""

    def __init__(self, bridge: Bridge) -> None:
        self.bridge = bridge
        # In-memory multipart upload state: upload_id -> {bucket, key, parts: {part_num: bytes}}
        self._multipart_uploads: dict[str, dict] = {}

    def setup_routes(self, app: web.Application) -> None:
        # Service-level
        app.router.add_route("GET", "/", self.handle_list_buckets)

        # Bucket + object operations — catch-all
        app.router.add_route("*", "/{path:.*}", self.handle_request)

    async def handle_request(self, request: web.Request) -> web.Response:
        """Main router: dispatch based on method + path."""
        bucket, key = _parse_bucket_key(request)

        if not bucket:
            return await self.handle_list_buckets(request)

        # Reject browser/crawler paths that are not valid S3 bucket names
        if not key and bucket in _IGNORED_PATHS:
            return _s3_error(
                404,
                "NoSuchBucket",
                f"The specified bucket does not exist.",
                resource=f"/{bucket}",
            )

        # Bucket-level operations (no key)
        if not key:
            query = request.query

            # ListObjectsV2
            if "list-type" in query:
                return await self.handle_list_objects_v2(request, bucket)

            # Multi-object delete: POST /{bucket}?delete
            if request.method == "POST" and "delete" in query:
                return await self.handle_delete_objects(request, bucket)

            # Bucket operations by method
            if request.method == "GET":
                # Default GET on bucket = ListObjectsV2
                return await self.handle_list_objects_v2(request, bucket)
            elif request.method == "PUT":
                return await self.handle_create_bucket(request, bucket)
            elif request.method == "DELETE":
                return await self.handle_delete_bucket(request, bucket)
            elif request.method == "HEAD":
                return await self.handle_head_bucket(request, bucket)
            else:
                return _s3_error(405, "MethodNotAllowed", "Method not allowed")

        # Object-level operations
        query = request.query

        if request.method == "POST":
            if "uploads" in query:
                return await self.handle_initiate_multipart(request, bucket, key)
            elif "uploadId" in query:
                return await self.handle_complete_multipart(request, bucket, key)
            else:
                return _s3_error(400, "InvalidRequest", "Unknown POST operation")
        elif request.method == "GET":
            return await self.handle_get_object(request, bucket, key)
        elif request.method == "PUT":
            if "partNumber" in query and "uploadId" in query:
                return await self.handle_upload_part(request, bucket, key)
            return await self.handle_put_object(request, bucket, key)
        elif request.method == "DELETE":
            if "uploadId" in query:
                return await self.handle_abort_multipart(request, bucket, key)
            return await self.handle_delete_object(request, bucket, key)
        elif request.method == "HEAD":
            return await self.handle_head_object(request, bucket, key)
        else:
            return _s3_error(405, "MethodNotAllowed", "Method not allowed")

    # ---- Bucket operations ----

    async def handle_list_buckets(self, request: web.Request) -> web.Response:
        try:
            buckets = await self.bridge.list_buckets()
            bucket_dicts = [
                {
                    "name": b.id.split("/")[-1] if "/" in b.id else b.id,
                    "creation_date": b.created_at,
                }
                for b in buckets
            ]
            body = list_buckets_xml(bucket_dicts)
            return web.Response(status=200, content_type=XML_CONTENT, body=body)
        except Exception as e:
            logger.exception("ListBuckets failed")
            return _s3_error(500, "InternalError", str(e))

    async def handle_create_bucket(
        self, request: web.Request, bucket: str
    ) -> web.Response:
        try:
            await self.bridge.create_bucket(bucket)
            return web.Response(status=200)
        except Exception as e:
            logger.exception("CreateBucket failed")
            return _s3_error(500, "InternalError", str(e))

    async def handle_delete_bucket(
        self, request: web.Request, bucket: str
    ) -> web.Response:
        try:
            await self.bridge.delete_bucket(bucket)
            return web.Response(status=204)
        except Exception as e:
            logger.exception("DeleteBucket failed")
            return _s3_error(500, "InternalError", str(e))

    async def handle_head_bucket(
        self, request: web.Request, bucket: str
    ) -> web.Response:
        try:
            info = await self.bridge.head_bucket(bucket)
            if info is None:
                return _s3_error(404, "NoSuchBucket", f"Bucket '{bucket}' not found")
            return web.Response(status=200)
        except Exception as e:
            logger.exception("HeadBucket failed")
            return _s3_error(500, "InternalError", str(e))

    async def handle_delete_objects(
        self, request: web.Request, bucket: str
    ) -> web.Response:
        """Handle S3 multi-object delete (POST /{bucket}?delete).

        Parses an XML body with a list of object keys and deletes them
        in a single batch call.
        """
        try:
            body = await request.read()
            try:
                root = fromstring(body)
            except Exception:
                return _s3_error(
                    400, "MalformedXML", "The XML you provided was not well-formed."
                )

            # Parse <Object><Key>...</Key></Object> elements.
            # The root tag may or may not have the S3 namespace.
            ns = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
            keys: list[str] = []
            for obj in root.findall("s3:Object", ns):
                key_el = obj.find("s3:Key", ns)
                if key_el is not None and key_el.text:
                    keys.append(key_el.text)
            # Also try without namespace (common in many clients)
            if not keys:
                for obj in root.findall("Object"):
                    key_el = obj.find("Key")
                    if key_el is not None and key_el.text:
                        keys.append(key_el.text)

            if not keys:
                return _s3_error(
                    400,
                    "MalformedXML",
                    "The XML you provided did not contain any Object/Key elements.",
                )

            deleted, errors = await self.bridge.delete_objects(bucket, keys)

            body_xml = delete_result_xml(deleted, errors or None)
            return web.Response(status=200, content_type=XML_CONTENT, body=body_xml)
        except Exception as e:
            logger.exception("DeleteObjects failed")
            return _s3_error(500, "InternalError", str(e))

    # ---- Object listing ----

    async def handle_list_objects_v2(
        self, request: web.Request, bucket: str
    ) -> web.Response:
        try:
            prefix = request.query.get("prefix", "")
            delimiter = request.query.get("delimiter", "")
            max_keys = int(request.query.get("max-keys", "1000"))
            continuation = request.query.get("continuation-token", "")

            result = await self.bridge.list_objects(
                bucket,
                prefix=prefix,
                delimiter=delimiter,
                max_keys=max_keys,
                continuation_token=continuation,
            )

            contents = [
                {
                    "key": f.path,
                    "size": f.size,
                    "last_modified": f.mtime or f.uploaded_at,
                    "etag": f'"{f.xet_hash[:32]}"' if f.xet_hash else '""',
                }
                for f in result["contents"]
            ]

            body = list_objects_v2_xml(
                bucket=bucket,
                prefix=prefix,
                delimiter=delimiter,
                max_keys=max_keys,
                contents=contents,
                common_prefixes=result["common_prefixes"],
                is_truncated=result["is_truncated"],
                continuation_token=continuation,
                next_continuation_token=result.get("next_continuation_token"),
                key_count=len(contents),
            )
            return web.Response(status=200, content_type=XML_CONTENT, body=body)
        except Exception as e:
            logger.exception("ListObjectsV2 failed")
            return _s3_error(500, "InternalError", str(e))

    # ---- Object operations ----

    async def handle_put_object(
        self, request: web.Request, bucket: str, key: str
    ) -> web.Response:
        try:
            data = await request.read()
            result = await self.bridge.put_object(bucket, key, data)
            return web.Response(
                status=200,
                headers={
                    "ETag": result["ETag"],
                    "Content-Length": "0",
                },
            )
        except Exception as e:
            logger.exception("PutObject failed")
            return _s3_error(500, "InternalError", str(e))

    async def handle_get_object(
        self, request: web.Request, bucket: str, key: str
    ) -> web.Response:
        try:
            # Get metadata first for Last-Modified
            file_info = await self.bridge.head_object(bucket, key)
            data = await self.bridge.get_object(bucket, key)
            if data is None:
                return _s3_error(
                    404,
                    "NoSuchKey",
                    "The specified key does not exist.",
                    resource=f"/{bucket}/{key}",
                )

            etag = (
                f'"{file_info.xet_hash[:32]}"'
                if file_info and file_info.xet_hash
                else f'"{hashlib.md5(data).hexdigest()}"'
            )
            content_type = mimetypes.guess_type(key)[0] or "application/octet-stream"
            last_modified = _format_last_modified(
                file_info.mtime or file_info.uploaded_at if file_info else None
            )

            # Handle Range requests
            range_header = request.headers.get("Range", "")
            if range_header and range_header.startswith("bytes="):
                range_spec = range_header[6:]  # strip "bytes="
                parts = range_spec.split("-")
                total = len(data)
                start = int(parts[0]) if parts[0] else 0
                end = int(parts[1]) if parts[1] else total - 1
                end = min(end, total - 1)

                if start >= total or start > end:
                    return web.Response(
                        status=416,
                        headers={
                            "Content-Range": f"bytes */{total}",
                        },
                    )

                slice_data = data[start : end + 1]
                return web.Response(
                    status=206,
                    body=slice_data,
                    content_type=content_type,
                    headers={
                        "ETag": etag,
                        "Content-Length": str(len(slice_data)),
                        "Content-Range": f"bytes {start}-{end}/{total}",
                        "Accept-Ranges": "bytes",
                        "Last-Modified": last_modified,
                    },
                )

            return web.Response(
                status=200,
                body=data,
                content_type=content_type,
                headers={
                    "ETag": etag,
                    "Content-Length": str(len(data)),
                    "Accept-Ranges": "bytes",
                    "Last-Modified": last_modified,
                },
            )
        except Exception as e:
            logger.exception("GetObject failed")
            return _s3_error(500, "InternalError", str(e))

    async def handle_delete_object(
        self, request: web.Request, bucket: str, key: str
    ) -> web.Response:
        try:
            await self.bridge.delete_object(bucket, key)
            return web.Response(status=204)
        except Exception as e:
            logger.exception("DeleteObject failed")
            return _s3_error(500, "InternalError", str(e))

    async def handle_head_object(
        self, request: web.Request, bucket: str, key: str
    ) -> web.Response:
        try:
            file_info = await self.bridge.head_object(bucket, key)
            if file_info is None:
                return _s3_error(
                    404,
                    "NoSuchKey",
                    "The specified key does not exist.",
                    resource=f"/{bucket}/{key}",
                )

            etag = f'"{file_info.xet_hash[:32]}"' if file_info.xet_hash else '""'
            content_type = mimetypes.guess_type(key)[0] or "application/octet-stream"
            last_modified = _format_last_modified(
                file_info.mtime or file_info.uploaded_at
            )

            return web.Response(
                status=200,
                headers={
                    "Content-Length": str(file_info.size),
                    "ETag": etag,
                    "Content-Type": content_type,
                    "Accept-Ranges": "bytes",
                    "Last-Modified": last_modified,
                },
            )
        except Exception as e:
            logger.exception("HeadObject failed")
            return _s3_error(500, "InternalError", str(e))

    # ---- Multipart upload operations ----

    async def handle_initiate_multipart(
        self, request: web.Request, bucket: str, key: str
    ) -> web.Response:
        """Initiate a multipart upload. Returns an uploadId."""
        try:
            upload_id = uuid.uuid4().hex
            self._multipart_uploads[upload_id] = {
                "bucket": bucket,
                "key": key,
                "parts": {},
            }
            logger.info(
                f"InitiateMultipartUpload: bucket={bucket} key={key} uploadId={upload_id}"
            )
            body = initiate_multipart_upload_xml(bucket, key, upload_id)
            return web.Response(status=200, content_type=XML_CONTENT, body=body)
        except Exception as e:
            logger.exception("InitiateMultipartUpload failed")
            return _s3_error(500, "InternalError", str(e))

    async def handle_upload_part(
        self, request: web.Request, bucket: str, key: str
    ) -> web.Response:
        """Upload a part for a multipart upload."""
        try:
            upload_id = request.query["uploadId"]
            part_number = int(request.query["partNumber"])

            if upload_id not in self._multipart_uploads:
                return _s3_error(
                    404,
                    "NoSuchUpload",
                    "The specified upload does not exist.",
                    resource=f"/{bucket}/{key}",
                )

            data = await request.read()
            etag = f'"{hashlib.md5(data).hexdigest()}"'
            self._multipart_uploads[upload_id]["parts"][part_number] = data

            logger.info(
                f"UploadPart: uploadId={upload_id} part={part_number} "
                f"size={len(data)} etag={etag}"
            )
            return web.Response(
                status=200,
                headers={"ETag": etag},
            )
        except Exception as e:
            logger.exception("UploadPart failed")
            return _s3_error(500, "InternalError", str(e))

    async def handle_complete_multipart(
        self, request: web.Request, bucket: str, key: str
    ) -> web.Response:
        """Complete a multipart upload: concatenate parts and upload as one object."""
        try:
            upload_id = request.query["uploadId"]

            if upload_id not in self._multipart_uploads:
                return _s3_error(
                    404,
                    "NoSuchUpload",
                    "The specified upload does not exist.",
                    resource=f"/{bucket}/{key}",
                )

            upload = self._multipart_uploads.pop(upload_id)
            parts = upload["parts"]

            if not parts:
                return _s3_error(
                    400,
                    "MalformedXML",
                    "You must specify at least one part.",
                )

            # Concatenate parts in order (offload to thread for large payloads)
            sorted_part_nums = sorted(parts.keys())
            data = await asyncio.to_thread(
                lambda: b"".join(parts[n] for n in sorted_part_nums)
            )

            logger.info(
                f"CompleteMultipartUpload: uploadId={upload_id} "
                f"parts={len(parts)} total_size={len(data)}"
            )

            # Upload as regular object
            result = await self.bridge.put_object(bucket, key, data)

            etag = result["ETag"]
            location = f"/{bucket}/{key}"
            body = complete_multipart_upload_xml(location, bucket, key, etag)
            return web.Response(status=200, content_type=XML_CONTENT, body=body)

        except Exception as e:
            logger.exception("CompleteMultipartUpload failed")
            return _s3_error(500, "InternalError", str(e))

    async def handle_abort_multipart(
        self, request: web.Request, bucket: str, key: str
    ) -> web.Response:
        """Abort a multipart upload and discard uploaded parts."""
        try:
            upload_id = request.query["uploadId"]

            if upload_id not in self._multipart_uploads:
                return _s3_error(
                    404,
                    "NoSuchUpload",
                    "The specified upload does not exist.",
                    resource=f"/{bucket}/{key}",
                )

            del self._multipart_uploads[upload_id]
            logger.info(f"AbortMultipartUpload: uploadId={upload_id}")
            return web.Response(status=204)
        except Exception as e:
            logger.exception("AbortMultipartUpload failed")
            return _s3_error(500, "InternalError", str(e))
