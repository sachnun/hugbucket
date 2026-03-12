"""HugBucket entry point."""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys

from aiohttp import web

from hugbucket.config import Config
from hugbucket.bridge import Bridge
from hugbucket.s3.server import S3Handler


def main() -> None:
    parser = argparse.ArgumentParser(
        description="HugBucket: S3 gateway for HF Storage Buckets"
    )
    parser.add_argument(
        "--host", default="0.0.0.0", help="Bind host (default: 0.0.0.0)"
    )
    parser.add_argument(
        "--port", type=int, default=9000, help="Bind port (default: 9000)"
    )
    parser.add_argument(
        "--hf-token", default="", help="HF API token (or set HF_TOKEN env)"
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Enable debug logging"
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    config = Config(
        host=args.host,
        port=args.port,
    )
    if args.hf_token:
        config.hf_token = args.hf_token

    if not config.hf_token:
        logging.error("No HF token provided. Set HF_TOKEN env or pass --hf-token.")
        sys.exit(1)

    bridge = Bridge(config=config)
    handler = S3Handler(bridge)

    app = web.Application(client_max_size=1024 * 1024 * 1024)  # 1 GiB max upload
    handler.setup_routes(app)

    async def on_startup(app: web.Application) -> None:
        # Auto-resolve HF namespace from token if not set
        if not config.hf_namespace:
            try:
                config.hf_namespace = await bridge.hub.whoami()
                logging.info(f"  Resolved HF namespace: {config.hf_namespace}")
            except Exception as e:
                logging.error(f"Failed to resolve HF namespace from token: {e}")
                sys.exit(1)

        logging.info(
            f"HugBucket S3 gateway starting on http://{config.host}:{config.port}"
        )
        logging.info(f"  HF endpoint: {config.hf_endpoint}")
        logging.info(f"  HF namespace: {config.hf_namespace}")
        logging.info("")
        logging.info("Usage with AWS CLI:")
        logging.info(f"  aws --endpoint-url http://localhost:{config.port} s3 ls")

    async def on_shutdown(app: web.Application) -> None:
        await bridge.close()

    app.on_startup.append(on_startup)
    app.on_shutdown.append(on_shutdown)

    web.run_app(app, host=config.host, port=config.port, print=None)


if __name__ == "__main__":
    main()
