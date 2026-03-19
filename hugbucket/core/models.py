"""Protocol-agnostic data models shared across adapters/providers."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class BucketInfo:
    id: str
    private: bool
    created_at: str
    size: int
    total_files: int


@dataclass
class BucketFile:
    type: str  # "file" or "directory"
    path: str
    size: int = 0
    xet_hash: str = ""
    mtime: str = ""
    uploaded_at: str = ""
