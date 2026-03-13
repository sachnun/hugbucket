"""Unit tests for Bridge layer (mocking Hub + CAS clients)."""

from __future__ import annotations

import hashlib
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from hugbucket.config import Config
from hugbucket.hub.client import BucketFile


@pytest.fixture
def config() -> Config:
    return Config(hf_token="fake-token", hf_namespace="testns")


@pytest.fixture
def mock_hub() -> MagicMock:
    hub = MagicMock()
    hub.batch_files = AsyncMock()
    hub.get_xet_write_token = AsyncMock()
    hub.get_paths_info = AsyncMock(return_value=[])
    hub.close = AsyncMock()
    return hub


@pytest.fixture
def mock_cas() -> MagicMock:
    cas = MagicMock()
    cas.close = AsyncMock()
    return cas


@pytest.fixture
def bridge(config: Config, mock_hub: MagicMock, mock_cas: MagicMock):
    from hugbucket.bridge import Bridge

    b = Bridge(config)
    b.hub = mock_hub
    b.cas = mock_cas
    return b


class TestDirectoryMarkers:
    """Test that folder-creation PUTs (trailing-slash, empty body) are no-ops."""

    async def test_folder_marker_returns_success(self, bridge) -> None:
        result = await bridge.put_object("mybucket", "New Folder/", b"")
        assert "ETag" in result
        expected_etag = f'"{hashlib.md5(b"").hexdigest()}"'
        assert result["ETag"] == expected_etag
        assert result["size"] == 0

    async def test_folder_marker_skips_batch_api(
        self, bridge, mock_hub: MagicMock
    ) -> None:
        """Directory markers must NOT call the Hub batch API."""
        await bridge.put_object("mybucket", "some/dir/", b"")
        mock_hub.batch_files.assert_not_awaited()

    async def test_folder_marker_skips_xet_upload(
        self, bridge, mock_hub: MagicMock, mock_cas: MagicMock
    ) -> None:
        """Directory markers must NOT upload anything to Xet CAS."""
        await bridge.put_object("mybucket", "folder/", b"")
        mock_hub.get_xet_write_token.assert_not_awaited()

    async def test_nested_folder_marker(self, bridge, mock_hub: MagicMock) -> None:
        result = await bridge.put_object("mybucket", "a/b/c/d/", b"")
        assert result["size"] == 0
        mock_hub.batch_files.assert_not_awaited()

    async def test_non_folder_empty_file_still_calls_batch(
        self, bridge, mock_hub: MagicMock
    ) -> None:
        """An empty file WITHOUT trailing slash should still go through batch API."""
        await bridge.put_object("mybucket", "empty.txt", b"")
        mock_hub.batch_files.assert_awaited_once()

    async def test_folder_with_data_is_not_noop(
        self, bridge, mock_hub: MagicMock
    ) -> None:
        """A trailing-slash key WITH data is not a directory marker — it should
        go through the normal upload path (Xet + batch)."""
        mock_hub.get_xet_write_token.return_value = MagicMock(
            endpoint="https://example.com",
            access_token="tok",
            expiration_unix_epoch=9999999999,
        )
        mock_cas = bridge.cas
        mock_cas.upload_xorb = AsyncMock()
        mock_cas.upload_shard = AsyncMock()

        await bridge.put_object("mybucket", "weird-key/", b"some data")
        # Should have uploaded via Xet
        mock_hub.get_xet_write_token.assert_awaited_once()
        mock_hub.batch_files.assert_awaited_once()
