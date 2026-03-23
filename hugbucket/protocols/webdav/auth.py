"""HTTP Basic authentication middleware for WebDAV.

WebDAV clients (macOS Finder, Windows Explorer, cadaver, rclone) use
standard HTTP Basic auth.  This middleware enforces it when the
WEBDAV_USERNAME / WEBDAV_PASSWORD env vars are set.
"""

from __future__ import annotations

import base64
import hmac
import logging
from collections.abc import Awaitable, Callable

from aiohttp import web

from hugbucket.config import Config

logger = logging.getLogger(__name__)


def _parse_basic_auth(header: str) -> tuple[str, str] | None:
    """Extract (username, password) from an Authorization: Basic header."""
    prefix = "Basic "
    if not header.startswith(prefix):
        return None
    try:
        decoded = base64.b64decode(header[len(prefix) :]).decode("utf-8")
    except Exception:
        return None
    if ":" not in decoded:
        return None
    username, password = decoded.split(":", 1)
    return username, password


def _unauthorized_response() -> web.Response:
    """Return a 401 response with WWW-Authenticate challenge."""
    return web.Response(
        status=401,
        headers={"WWW-Authenticate": 'Basic realm="HugBucket WebDAV"'},
        text="Authentication required.",
    )


@web.middleware
async def webdav_auth_middleware(
    request: web.Request,
    handler: Callable[[web.Request], Awaitable[web.StreamResponse]],
) -> web.StreamResponse:
    """aiohttp middleware enforcing HTTP Basic auth for WebDAV."""
    config: Config = request.app["config"]

    # If no credentials configured, allow anonymous access
    if not config.webdav_user and not config.webdav_password:
        return await handler(request)

    auth_header = request.headers.get("Authorization", "")
    if not auth_header:
        return _unauthorized_response()

    creds = _parse_basic_auth(auth_header)
    if creds is None:
        return _unauthorized_response()

    username, password = creds

    # Constant-time comparison to prevent timing attacks
    user_ok = hmac.compare_digest(username, config.webdav_user)
    pass_ok = hmac.compare_digest(password, config.webdav_password)

    if not (user_ok and pass_ok):
        logger.warning("WebDAV auth failed for user %r", username)
        return _unauthorized_response()

    return await handler(request)
