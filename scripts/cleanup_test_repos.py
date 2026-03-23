#!/usr/bin/env python3
"""Find and delete leftover test buckets on HuggingFace Hub.

Usage:
    HF_TOKEN=hf_xxx python scripts/cleanup_test_repos.py          # dry-run (default)
    HF_TOKEN=hf_xxx python scripts/cleanup_test_repos.py --delete  # actually delete

Scans all buckets owned by the authenticated user and flags any whose
name matches known test-bucket patterns:

    pytest-dav-*   (WebDAV live tests)
    pytest-b3-*    (boto3 / S3 live tests)
    pytest-cd-*    (create-delete bucket test)

Only buckets matching those patterns are touched — everything else is left alone.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import urllib.error
import urllib.request

HF_ENDPOINT = "https://huggingface.co"
# Patterns produced by our test fixtures
TEST_BUCKET_RE = re.compile(r"^pytest-(dav|b3|cd)-\d+$")


def _api(
    method: str,
    url: str,
    token: str,
    data: bytes | None = None,
) -> tuple[int, dict | list | None, dict[str, str]]:
    """Make an authenticated HF API request. Returns (status, json_body, headers)."""
    req = urllib.request.Request(url, method=method, data=data)
    req.add_header("Authorization", f"Bearer {token}")
    if data is not None:
        req.add_header("Content-Type", "application/json")
    try:
        resp = urllib.request.urlopen(req)
        body = resp.read()
        return resp.status, json.loads(body) if body else None, dict(resp.headers)
    except urllib.error.HTTPError as e:
        body = e.read()
        return e.code, json.loads(body) if body else None, dict(e.headers)


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--delete",
        action="store_true",
        help="Actually delete matching buckets (default is dry-run)",
    )
    args = parser.parse_args()

    token = os.environ.get("HF_TOKEN", "")
    if not token:
        print("ERROR: HF_TOKEN environment variable is required", file=sys.stderr)
        sys.exit(1)

    # Who am I?  (uses /api/whoami-v2, same as the codebase)
    status, data, _ = _api("GET", f"{HF_ENDPOINT}/api/whoami-v2", token)
    if status != 200:
        print(f"ERROR: /api/whoami-v2 returned {status}: {data}", file=sys.stderr)
        sys.exit(1)
    username = data["name"]
    print(f"Authenticated as: {username}")

    # List all buckets (paginated via Link header, same as HubClient.list_buckets)
    all_buckets: list[dict] = []
    url: str | None = f"{HF_ENDPOINT}/api/buckets/{username}"
    while url:
        status, items, headers = _api("GET", url, token)
        if status != 200:
            print(f"ERROR: listing buckets returned {status}: {items}", file=sys.stderr)
            sys.exit(1)
        all_buckets.extend(items)
        # Follow pagination via Link header
        url = None
        link = headers.get("Link", "")
        if 'rel="next"' in link:
            # Parse: <https://...>; rel="next"
            for part in link.split(","):
                if 'rel="next"' in part:
                    url = part.split("<")[1].split(">")[0]

    print(f"Found {len(all_buckets)} total bucket(s)")

    matches: list[str] = []
    for bucket in all_buckets:
        bucket_id: str = bucket["id"]  # e.g. "username/pytest-dav-12345"
        name = bucket_id.split("/")[-1]
        if TEST_BUCKET_RE.match(name):
            matches.append(bucket_id)

    if not matches:
        print("No leftover test buckets found. All clean!")
        return

    print(f"\nFound {len(matches)} test bucket(s) to clean up:")
    for bucket_id in sorted(matches):
        print(f"  {bucket_id}")

    if not args.delete:
        print("\nDry-run mode — pass --delete to remove them.")
        return

    print()
    for bucket_id in sorted(matches):
        url = f"{HF_ENDPOINT}/api/buckets/{bucket_id}"
        status, resp_data, _ = _api("DELETE", url, token)
        if status in (200, 204):
            print(f"  DELETED {bucket_id}")
        else:
            print(f"  FAILED  {bucket_id} ({status}: {resp_data})")

    print("\nDone.")


if __name__ == "__main__":
    main()
