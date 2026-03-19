"""FTP gateway entrypoint.

FTP maps paths as ``/<bucket>/<key>``.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys

from hugbucket.config import Config
from hugbucket.protocols.ftp.server import create_ftp_server
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
        description="HugBucket: FTP gateway for HF Storage Buckets"
    )
    parser.add_argument(
        "--host", default="0.0.0.0", help="Bind host (default: 0.0.0.0)"
    )
    parser.add_argument(
        "--port", type=int, default=2121, help="Bind port (default: 2121)"
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
        ftp_host=args.host,
        ftp_port=args.port,
    )

    _require_mode("ftp")

    if not config.hf_token:
        logging.error("No HF token provided. Set HF_TOKEN env.")
        sys.exit(1)

    backend = HFStorageBackend(config=config)
    server, runner = create_ftp_server(config=config, backend=backend)
    try:
        if not config.hf_namespace:
            config.hf_namespace = runner.call(backend.resolve_namespace())

        logging.info(
            "HugBucket FTP gateway starting on %s:%s",
            config.ftp_host,
            config.ftp_port,
        )
        logging.info("  FTP user: %s", config.ftp_user)
        logging.info("  HF endpoint: %s", config.hf_endpoint)
        logging.info("  HF namespace: %s", config.hf_namespace)
        logging.info("Path mapping: /<bucket>/<key>")
        server.serve_forever()
    finally:
        try:
            server.close_all()
        finally:
            runner.close()


if __name__ == "__main__":
    main()
