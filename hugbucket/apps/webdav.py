"""WebDAV gateway entrypoint.

WebDAV maps paths as ``/<bucket>/<key>``.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys

from aiohttp import web

from hugbucket.config import Config
from hugbucket.protocols.webdav.app import create_webdav_app
from hugbucket.providers.hf.backend import HFStorageBackend


def _require_mode(expected_mode: str) -> None:
    raw_mode = os.environ.get("MODE")
    if raw_mode is None or not raw_mode.strip():
        logging.error("MODE is required. Set MODE=%s.", expected_mode)
        sys.exit(2)

    mode = raw_mode.strip().lower()
    if mode != expected_mode:
        logging.error(
            "Invalid MODE for this entrypoint. Expected MODE=%s, got %r.",
            expected_mode,
            raw_mode,
        )
        sys.exit(2)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="HugBucket: WebDAV gateway for HF Storage Buckets"
    )
    parser.add_argument(
        "--host", default="0.0.0.0", help="Bind host (default: 0.0.0.0)"
    )
    parser.add_argument(
        "--port", type=int, default=8080, help="Bind port (default: 8080)"
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Enable debug logging"
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    config = Config(webdav_host=args.host, webdav_port=args.port)

    _require_mode("webdav")

    if not config.hf_token:
        logging.error("No HF token provided. Set HF_TOKEN env.")
        sys.exit(1)

    if not config.webdav_user and not config.webdav_password:
        logging.warning(
            "WebDAV authentication is disabled (WEBDAV_USERNAME/WEBDAV_PASSWORD empty)."
        )

    backend = HFStorageBackend(config=config)
    app = create_webdav_app(
        config=config,
        backend=backend,
        max_upload_bytes=1024 * 1024 * 1024,
    )
    web.run_app(app, host=config.webdav_host, port=config.webdav_port, print=None)


if __name__ == "__main__":
    main()
