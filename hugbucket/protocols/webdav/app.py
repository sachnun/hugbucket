"""WebDAV app factory and lifecycle wiring."""

from __future__ import annotations

import logging
import sys

from aiohttp import web

from hugbucket.config import Config
from hugbucket.core.backend import StorageBackend
from hugbucket.protocols.webdav.auth import webdav_auth_middleware
from hugbucket.protocols.webdav.server import WebDAVHandler

logger = logging.getLogger(__name__)


def create_webdav_app(
    *,
    config: Config,
    backend: StorageBackend,
    max_upload_bytes: int = 1024 * 1024 * 1024,
) -> web.Application:
    """Create an aiohttp app serving the WebDAV protocol adapter."""
    handler = WebDAVHandler(backend)
    app = web.Application(
        client_max_size=max_upload_bytes,
        middlewares=[webdav_auth_middleware],
    )
    app["config"] = config
    handler.setup_routes(app)

    async def on_startup(app: web.Application) -> None:
        if not config.hf_namespace:
            try:
                config.hf_namespace = await backend.resolve_namespace()
                logger.info("  Resolved HF namespace: %s", config.hf_namespace)
            except Exception as exc:
                logger.error("Failed to resolve HF namespace from token: %s", exc)
                sys.exit(1)

        logger.info(
            "HugBucket WebDAV gateway starting on http://%s:%s",
            config.webdav_host,
            config.webdav_port,
        )
        logger.info("  HF endpoint: %s", config.hf_endpoint)
        logger.info("  HF namespace: %s", config.hf_namespace)
        logger.info("")
        logger.info("Path mapping: /<bucket>/<key>")
        logger.info(
            "Mount in macOS Finder: Go > Connect to Server > http://localhost:%s",
            config.webdav_port,
        )
        logger.info(
            "Mount in Windows Explorer: Map Network Drive > http://localhost:%s",
            config.webdav_port,
        )

    async def on_shutdown(app: web.Application) -> None:
        await backend.close()

    app.on_startup.append(on_startup)
    app.on_shutdown.append(on_shutdown)
    return app
