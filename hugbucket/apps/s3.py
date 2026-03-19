"""S3 gateway application entrypoint."""

from __future__ import annotations

import argparse
import logging
import sys

from aiohttp import web

from hugbucket.config import Config
from hugbucket.protocols.s3.app import create_s3_app
from hugbucket.providers.hf.backend import HFStorageBackend


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

    if not config.hf_token:
        logging.error("No HF token provided. Set HF_TOKEN env.")
        sys.exit(1)

    backend = HFStorageBackend(config=config)
    app = create_s3_app(
        config=config,
        backend=backend,
        max_upload_bytes=1024 * 1024 * 1024,
    )
    web.run_app(app, host=config.host, port=config.port, print=None)


if __name__ == "__main__":
    main()
