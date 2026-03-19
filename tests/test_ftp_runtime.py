"""Tests for FTP backend loop runner."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from hugbucket.protocols.ftp.runtime import BackendLoopRunner


async def _const(value: str) -> str:
    return value


def test_backend_loop_runner_call_and_close() -> None:
    backend = MagicMock()
    backend.resolve_namespace = AsyncMock(return_value="alice")
    backend.close = AsyncMock(return_value=None)

    runner = BackendLoopRunner(backend)
    try:
        assert runner.call(backend.resolve_namespace()) == "alice"
    finally:
        runner.close()

    backend.close.assert_awaited_once()


def test_backend_loop_runner_close_is_idempotent() -> None:
    backend = MagicMock()
    backend.close = AsyncMock(return_value=None)

    runner = BackendLoopRunner(backend)
    runner.close()
    runner.close()

    assert backend.close.await_count >= 1


def test_backend_loop_runner_call_after_close_raises() -> None:
    backend = MagicMock()
    backend.close = AsyncMock(return_value=None)

    runner = BackendLoopRunner(backend)
    runner.close()

    async def _noop() -> None:
        return None

    with pytest.raises(RuntimeError):
        runner.call(_noop())


def test_backend_loop_runner_with_custom_awaitable() -> None:
    backend = MagicMock()
    backend.close = AsyncMock(return_value=None)

    class _Awaitable:
        def __await__(self):  # type: ignore[no-untyped-def]
            async def _impl() -> str:
                return "ok"

            return _impl().__await__()

    runner = BackendLoopRunner(backend)
    try:
        assert runner.call(_Awaitable()) == "ok"
    finally:
        runner.close()


def test_backend_loop_runner_rejects_call_after_close_without_warning() -> None:
    backend = MagicMock()
    backend.close = AsyncMock(return_value=None)

    runner = BackendLoopRunner(backend)
    runner.close()

    with pytest.raises(RuntimeError):
        runner.call(_const("x"))
