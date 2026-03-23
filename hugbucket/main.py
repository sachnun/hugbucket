"""Top-level entrypoint with protocol mode selection via ``MODE`` env.

Valid values:

- ``s3``
- ``ftp``
"""

from __future__ import annotations

import os
import sys

VALID_MODES = {"s3", "ftp", "webdav"}


def _normalize_mode(value: str) -> str:
    return value.strip().lower()


def _resolve_mode(env_mode: str | None) -> str:
    if env_mode is None or not env_mode.strip():
        raise ValueError("missing MODE env (expected: s3, ftp, or webdav)")

    mode = _normalize_mode(env_mode)
    if mode not in VALID_MODES:
        raise ValueError(f"invalid MODE '{mode}' (expected: s3, ftp, or webdav)")
    return mode


def _run_s3() -> None:
    from hugbucket.apps.s3 import main as s3_main

    s3_main()


def _run_ftp() -> None:
    from hugbucket.apps.ftp import main as ftp_main

    ftp_main()


def _run_webdav() -> None:
    from hugbucket.apps.webdav import main as webdav_main

    webdav_main()


def main() -> None:
    try:
        mode = _resolve_mode(os.environ.get("MODE"))
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        raise SystemExit(2) from exc

    if mode == "ftp":
        _run_ftp()
        return
    if mode == "webdav":
        _run_webdav()
        return
    _run_s3()


if __name__ == "__main__":
    main()
