"""S3 gateway application entrypoint."""

from __future__ import annotations

import argparse
import logging
import os
import sys

from aiohttp import web

from hugbucket.config import Config
from hugbucket.protocols.s3.app import create_s3_app
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


def _mode_for_message() -> str:
    raw_mode = os.environ.get("MODE")
    if raw_mode is None:
        return ""
    return raw_mode.strip().lower()


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

    config = Config(host=args.host, port=args.port)

    _require_mode("s3")

    current_mode = _mode_for_message()

    if not config.hf_token:
        logging.error("No HF token provided. Set HF_TOKEN env.")
        sys.exit(1)

    if current_mode == "s3" and not config.s3_access_key and not config.s3_secret_key:
        logging.warning(
            "S3 authentication is disabled (AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY empty)."
        )

    backend = HFStorageBackend(config=config)
    app = create_s3_app(
        config=config,
        backend=backend,
        max_upload_bytes=1024 * 1024 * 1024,
    )
    web.run_app(app, host=config.host, port=config.port, print=None)


if __name__ == "__main__":
    main()
