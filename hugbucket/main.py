"""Top-level entrypoint — starts the S3-compatible gateway."""

from __future__ import annotations


def main() -> None:
    from hugbucket.apps.s3 import main as s3_main

    s3_main()


if __name__ == "__main__":
    main()
