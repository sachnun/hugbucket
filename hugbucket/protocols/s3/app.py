"""S3 app factory and lifecycle wiring."""

from __future__ import annotations

import logging
import sys

from aiohttp import web

from hugbucket.config import Config
from hugbucket.core.backend import StorageBackend
from hugbucket.s3.auth import s3_auth_middleware
from hugbucket.s3.server import S3Handler

logger = logging.getLogger(__name__)


def create_s3_app(
    *,
    config: Config,
    backend: StorageBackend,
    max_upload_bytes: int = 1024 * 1024 * 1024,
) -> web.Application:
    """Create an aiohttp app serving the S3 protocol adapter."""
    handler = S3Handler(
        backend,
        multipart_upload_ttl=config.multipart_upload_ttl,
    )
    app = web.Application(
        client_max_size=max_upload_bytes,
        middlewares=[s3_auth_middleware],
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
            "HugBucket S3 gateway starting on http://%s:%s",
            config.host,
            config.port,
        )
        logger.info("  HF endpoint: %s", config.hf_endpoint)
        logger.info("  HF namespace: %s", config.hf_namespace)
        logger.info("")
        logger.info("Usage with AWS CLI:")
        logger.info("  aws --endpoint-url http://localhost:%s s3 ls", config.port)

    async def on_shutdown(app: web.Application) -> None:
        await backend.close()

    app.on_startup.append(on_startup)
    app.on_shutdown.append(on_shutdown)
    return app
