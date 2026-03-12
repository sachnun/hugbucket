"""Shared fixtures for HugBucket tests."""

from __future__ import annotations

import os

import pytest


def pytest_configure(config: pytest.Config) -> None:
    """Register custom markers."""
    config.addinivalue_line(
        "markers",
        "integration: tests that hit live HF API (deselect with '-m not integration')",
    )


@pytest.fixture
def random_bytes() -> bytes:
    """200 KB of deterministic pseudo-random data (good for CDC tests)."""
    return os.urandom(200 * 1024)


@pytest.fixture
def small_bytes() -> bytes:
    """1 KB payload — fits in a single CDC chunk."""
    return os.urandom(1024)


@pytest.fixture(scope="session")
def hf_token() -> str:
    """HF token from env (required for integration tests)."""
    token = os.environ.get("HF_TOKEN", "")
    if not token:
        pytest.skip("HF_TOKEN not set — skipping integration test")
    return token
