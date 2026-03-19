"""Protocol-agnostic core interfaces for storage backends."""

from hugbucket.core.backend import StorageBackend
from hugbucket.core.models import BucketFile, BucketInfo

__all__ = ["StorageBackend", "BucketFile", "BucketInfo"]
