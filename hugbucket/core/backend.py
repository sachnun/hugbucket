"""Protocol-agnostic storage backend interface.

The S3 adapter and future protocol adapters (FTP, WebDAV, etc.)
should depend on this contract instead of concrete HF/Xet details.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator

from hugbucket.core.models import BucketFile, BucketInfo


class StorageBackend(ABC):
    """Backend capabilities required by protocol adapters."""

    @abstractmethod
    async def close(self) -> None:
        """Release backend resources (HTTP sessions, pools, caches)."""

    @abstractmethod
    async def resolve_namespace(self) -> str:
        """Return the effective backend namespace for current credentials."""

    @abstractmethod
    async def list_buckets(self) -> list[BucketInfo]:
        """List all buckets visible to the configured principal."""

    @abstractmethod
    async def create_bucket(self, name: str, private: bool = False) -> str:
        """Create a bucket and return its URL."""

    @abstractmethod
    async def delete_bucket(self, name: str) -> None:
        """Delete a bucket by name."""

    @abstractmethod
    async def head_bucket(self, name: str) -> BucketInfo | None:
        """Return bucket metadata or None when missing."""

    @abstractmethod
    async def put_object(self, bucket: str, key: str, data: bytes) -> dict:
        """Upload an object and return metadata (e.g. ETag)."""

    @abstractmethod
    async def get_object(self, bucket: str, key: str) -> bytes | None:
        """Download object bytes or None when missing."""

    @abstractmethod
    async def get_object_stream(
        self,
        bucket: str,
        key: str,
        file_info: BucketFile | None = None,
        byte_range: tuple[int, int] | None = None,
    ) -> AsyncIterator[bytes] | None:
        """Stream object bytes, optionally constrained by inclusive byte range."""

    @abstractmethod
    async def delete_object(self, bucket: str, key: str) -> None:
        """Delete one object key."""

    @abstractmethod
    async def delete_objects(
        self, bucket: str, keys: list[str]
    ) -> tuple[list[str], list[dict]]:
        """Delete multiple keys, returning (deleted, errors)."""

    @abstractmethod
    async def head_object(self, bucket: str, key: str) -> BucketFile | None:
        """Return object metadata or None when missing."""

    @abstractmethod
    async def head_directory(self, bucket: str, prefix: str) -> bool:
        """Return True if a virtual directory prefix exists."""

    @abstractmethod
    async def copy_object(
        self,
        src_bucket: str,
        src_key: str,
        dst_bucket: str,
        dst_key: str,
    ) -> dict:
        """Copy object metadata/content from source key to destination key."""

    @abstractmethod
    async def list_objects(
        self,
        bucket: str,
        prefix: str = "",
        delimiter: str = "",
        max_keys: int = 1000,
        continuation_token: str = "",
    ) -> dict:
        """List objects with prefix/delimiter pagination semantics."""
