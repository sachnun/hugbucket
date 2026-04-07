"""Tests for top-level entrypoint."""

from __future__ import annotations

from unittest.mock import patch

import hugbucket.main as entry


def test_main_calls_s3() -> None:
    with patch("hugbucket.apps.s3.main") as mock_s3:
        entry.main()
        mock_s3.assert_called_once()
