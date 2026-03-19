"""HF Hub/Xet storage backend.

This module is the provider-facing import path used by protocol adapters.
The concrete implementation currently lives in ``hugbucket.bridge`` for
backward compatibility with existing imports and tests.
"""

from hugbucket.bridge import Bridge, HFStorageBackend

__all__ = ["HFStorageBackend", "Bridge"]
