"""AWS Signature V4 verification for S3-compatible requests.

Supports both:
1. Authorization header-based authentication (standard S3 API calls)
2. Query-string authentication (presigned URLs)

Error codes returned match real AWS S3 behaviour:
- ``AccessDenied``            — no credentials / expired presigned URL
- ``InvalidAccessKeyId``      — unknown access key
- ``SignatureDoesNotMatch``   — signature mismatch
"""

from __future__ import annotations

import hashlib
import hmac
import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from urllib.parse import quote, unquote

from aiohttp import web

from hugbucket.config import Config
from hugbucket.s3.xml_responses import error_xml

logger = logging.getLogger(__name__)

_ALGORITHM = "AWS4-HMAC-SHA256"


# ── auth error descriptor ───────────────────────────────────────────────


@dataclass
class AuthError:
    """Describes an authentication failure in S3-compatible terms."""

    http_status: int
    code: str
    message: str
    extra: dict[str, str] = field(default_factory=dict)


# ── helpers ──────────────────────────────────────────────────────────────


def _request_id() -> str:
    return uuid.uuid4().hex[:16].upper()


def _hmac_sha256(key: bytes, msg: str) -> bytes:
    return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()


def _derive_signing_key(
    secret_key: str, date_stamp: str, region: str, service: str
) -> bytes:
    """Derive the four-level HMAC signing key."""
    k_date = _hmac_sha256(("AWS4" + secret_key).encode("utf-8"), date_stamp)
    k_region = _hmac_sha256(k_date, region)
    k_service = _hmac_sha256(k_region, service)
    k_signing = _hmac_sha256(k_service, "aws4_request")
    return k_signing


def _canonical_uri(path: str) -> str:
    """Build canonical URI from a *decoded* request path.

    Each path segment is URI-encoded; slashes are preserved.
    S3 uses single encoding (not double).
    """
    if not path or path == "/":
        return "/"
    segments = path.split("/")
    return "/".join(quote(seg, safe="-_.~") for seg in segments) or "/"


def _canonical_query_string(
    raw_query: str,
    exclude_keys: set[str] | None = None,
) -> str:
    """Build the canonical query string from the raw (percent-encoded) QS.

    *exclude_keys* — parameter names to omit (e.g. ``X-Amz-Signature``
    for presigned-URL verification).
    """
    if not raw_query:
        return ""
    exclude = exclude_keys or set()
    params: list[tuple[str, str]] = []
    for part in raw_query.split("&"):
        if not part:
            continue
        if "=" in part:
            k, v = part.split("=", 1)
            dk = unquote(k)
            if dk not in exclude:
                params.append((dk, unquote(v)))
        else:
            dk = unquote(part)
            if dk not in exclude:
                params.append((dk, ""))
    params.sort()
    return "&".join(
        f"{quote(k, safe='-_.~')}={quote(v, safe='-_.~')}" for k, v in params
    )


def _build_canonical_headers(request: web.Request, signed_headers: list[str]) -> str:
    """Return ``name:value\\n`` lines for every signed header."""
    lines: list[str] = []
    for name in signed_headers:
        lname = name.lower()
        if lname == "host":
            val = request.headers.get("Host", request.host)
        else:
            val = request.headers.get(name, "")
        val = " ".join(val.split())  # collapse whitespace
        lines.append(f"{lname}:{val}\n")
    return "".join(lines)


# ── Authorization-header auth ────────────────────────────────────────────


def _parse_auth_header(header: str) -> dict | None:
    """Parse ``Authorization: AWS4-HMAC-SHA256 Credential=…, …`` header."""
    prefix = _ALGORITHM + " "
    if not header.startswith(prefix):
        return None

    fields: dict[str, str] = {}
    for chunk in header[len(prefix) :].split(","):
        chunk = chunk.strip()
        if "=" in chunk:
            k, v = chunk.split("=", 1)
            fields[k.strip()] = v.strip()

    if not all(k in fields for k in ("Credential", "SignedHeaders", "Signature")):
        return None

    cred_parts = fields["Credential"].split("/")
    if len(cred_parts) != 5:
        return None

    return {
        "access_key": cred_parts[0],
        "date_stamp": cred_parts[1],
        "region": cred_parts[2],
        "service": cred_parts[3],
        "credential_scope": "/".join(cred_parts[1:]),
        "signed_headers": sorted(fields["SignedHeaders"].split(";")),
        "signature": fields["Signature"],
    }


def _verify_header_auth(request: web.Request, config: Config) -> AuthError | None:
    """Verify a standard Authorization-header SigV4 request.

    Returns ``None`` on success or an :class:`AuthError` on failure.
    """
    parsed = _parse_auth_header(request.headers.get("Authorization", ""))
    if parsed is None:
        return AuthError(
            403,
            "AccessDenied",
            "Access Denied",
        )

    if parsed["access_key"] != config.s3_access_key:
        return AuthError(
            403,
            "InvalidAccessKeyId",
            "The AWS Access Key Id you provided does not exist in our records.",
            {"AWSAccessKeyId": parsed["access_key"]},
        )

    # ── canonical request ────────────────────────────────────────────
    method = request.method
    canon_uri = _canonical_uri(request.path)
    canon_qs = _canonical_query_string(request.query_string)

    signed_headers = parsed["signed_headers"]
    canonical_headers = _build_canonical_headers(request, signed_headers)
    signed_headers_str = ";".join(signed_headers)

    payload_hash = request.headers.get("x-amz-content-sha256", "UNSIGNED-PAYLOAD")

    canonical_request = "\n".join(
        [
            method,
            canon_uri,
            canon_qs,
            canonical_headers,
            signed_headers_str,
            payload_hash,
        ]
    )

    # ── string to sign ───────────────────────────────────────────────
    amz_date = request.headers.get("x-amz-date", "")
    string_to_sign = "\n".join(
        [
            _ALGORITHM,
            amz_date,
            parsed["credential_scope"],
            hashlib.sha256(canonical_request.encode()).hexdigest(),
        ]
    )

    # ── signature ────────────────────────────────────────────────────
    signing_key = _derive_signing_key(
        config.s3_secret_key,
        parsed["date_stamp"],
        parsed["region"],
        parsed["service"],
    )
    expected = hmac.new(
        signing_key, string_to_sign.encode(), hashlib.sha256
    ).hexdigest()

    if not hmac.compare_digest(expected, parsed["signature"]):
        logger.debug(
            "SigV4 header auth mismatch\n  canonical_request:\n%s\n  string_to_sign:\n%s",
            canonical_request,
            string_to_sign,
        )
        return AuthError(
            403,
            "SignatureDoesNotMatch",
            "The request signature we calculated does not match the signature "
            "you provided. Check your key and signing method.",
            {
                "AWSAccessKeyId": parsed["access_key"],
                "StringToSign": string_to_sign,
                "SignatureProvided": parsed["signature"],
            },
        )

    return None  # success


# ── Query-string (presigned URL) auth ────────────────────────────────────


def _verify_query_auth(request: web.Request, config: Config) -> AuthError | None:
    """Verify a presigned-URL (query-string) SigV4 request.

    Returns ``None`` on success or an :class:`AuthError` on failure.
    """
    query = request.query

    if query.get("X-Amz-Algorithm", "") != _ALGORITHM:
        return AuthError(403, "AccessDenied", "Access Denied")

    credential = query.get("X-Amz-Credential", "")
    amz_date = query.get("X-Amz-Date", "")
    expires = query.get("X-Amz-Expires", "")
    signed_headers_str = query.get("X-Amz-SignedHeaders", "")
    signature = query.get("X-Amz-Signature", "")

    if not all([credential, amz_date, expires, signed_headers_str, signature]):
        return AuthError(403, "AccessDenied", "Access Denied")

    cred_parts = credential.split("/")
    if len(cred_parts) != 5:
        return AuthError(403, "AccessDenied", "Access Denied")

    access_key, date_stamp, region, service, _ = cred_parts
    if access_key != config.s3_access_key:
        return AuthError(
            403,
            "InvalidAccessKeyId",
            "The AWS Access Key Id you provided does not exist in our records.",
            {"AWSAccessKeyId": access_key},
        )

    # ── expiration check ─────────────────────────────────────────────
    try:
        req_time = datetime.strptime(amz_date, "%Y%m%dT%H%M%SZ").replace(
            tzinfo=timezone.utc
        )
        expire_seconds = int(expires)
        now = datetime.now(timezone.utc)
        expiry_time = req_time + timedelta(seconds=expire_seconds)
        if now > expiry_time:
            return AuthError(
                403,
                "AccessDenied",
                "Request has expired",
                {
                    "X-Amz-Expires": expires,
                    "Expires": expiry_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "ServerTime": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
                },
            )
    except ValueError, OverflowError:
        return AuthError(403, "AccessDenied", "Access Denied")

    # ── canonical request ────────────────────────────────────────────
    method = request.method
    canon_uri = _canonical_uri(request.path)
    canon_qs = _canonical_query_string(
        request.query_string, exclude_keys={"X-Amz-Signature"}
    )

    signed_headers = sorted(signed_headers_str.split(";"))
    canonical_headers = _build_canonical_headers(request, signed_headers)
    signed_headers_joined = ";".join(signed_headers)

    # Presigned URLs always use UNSIGNED-PAYLOAD
    payload_hash = "UNSIGNED-PAYLOAD"

    canonical_request = "\n".join(
        [
            method,
            canon_uri,
            canon_qs,
            canonical_headers,
            signed_headers_joined,
            payload_hash,
        ]
    )

    # ── string to sign ───────────────────────────────────────────────
    credential_scope = "/".join(cred_parts[1:])
    string_to_sign = "\n".join(
        [
            _ALGORITHM,
            amz_date,
            credential_scope,
            hashlib.sha256(canonical_request.encode()).hexdigest(),
        ]
    )

    # ── signature ────────────────────────────────────────────────────
    signing_key = _derive_signing_key(config.s3_secret_key, date_stamp, region, service)
    expected = hmac.new(
        signing_key, string_to_sign.encode(), hashlib.sha256
    ).hexdigest()

    if not hmac.compare_digest(expected, signature):
        logger.debug(
            "SigV4 presigned auth mismatch\n  canonical_request:\n%s\n  string_to_sign:\n%s",
            canonical_request,
            string_to_sign,
        )
        return AuthError(
            403,
            "SignatureDoesNotMatch",
            "The request signature we calculated does not match the signature "
            "you provided. Check your key and signing method.",
            {
                "AWSAccessKeyId": access_key,
                "StringToSign": string_to_sign,
                "SignatureProvided": signature,
            },
        )

    return None  # success


# ── public API ───────────────────────────────────────────────────────────


def verify_request(request: web.Request, config: Config) -> AuthError | None:
    """Check AWS SigV4 credentials on *request*.

    Returns ``None`` when the request is authenticated, or an
    :class:`AuthError` describing the failure.
    """
    if request.headers.get("Authorization", ""):
        return _verify_header_auth(request, config)

    if "X-Amz-Algorithm" in request.query:
        return _verify_query_auth(request, config)

    # No credentials provided at all
    return AuthError(403, "AccessDenied", "Access Denied")


@web.middleware
async def s3_auth_middleware(request: web.Request, handler: object) -> web.Response:
    """aiohttp middleware that enforces AWS Signature V4 on every request."""
    config: Config = request.app["config"]

    err = verify_request(request, config)
    if err is not None:
        body = error_xml(
            err.code,
            err.message,
            resource=request.path,
            request_id=_request_id(),
            extra=err.extra or None,
        )
        return web.Response(
            status=err.http_status,
            content_type="application/xml",
            body=body,
        )

    return await handler(request)
