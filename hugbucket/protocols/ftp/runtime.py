"""Runtime helpers for sync FTP code calling async backends."""

from __future__ import annotations

import asyncio
import inspect
from collections.abc import Awaitable, Coroutine
from threading import Thread
from typing import Any, TypeVar
from concurrent.futures import Future

from hugbucket.core.backend import StorageBackend

T = TypeVar("T")


class BackendLoopRunner:
    """Runs backend coroutines on a dedicated event loop thread."""

    def __init__(self, backend: StorageBackend) -> None:
        self.backend = backend
        self._loop = asyncio.new_event_loop()
        self._closed = False
        self._thread = Thread(target=self._run_loop, daemon=True)
        self._thread.start()

    def _run_loop(self) -> None:
        asyncio.set_event_loop(self._loop)
        self._loop.run_forever()

    @staticmethod
    async def _await_value(awaitable: Awaitable[T]) -> T:
        return await awaitable

    def _submit(self, awaitable: Awaitable[T]) -> Future[T]:
        coro: Coroutine[Any, Any, T]
        if inspect.iscoroutine(awaitable):
            coro = awaitable
        else:
            coro = self._await_value(awaitable)
        return asyncio.run_coroutine_threadsafe(coro, self._loop)

    def call(self, awaitable: Awaitable[T]) -> T:
        """Synchronously wait for an awaitable running on the loop."""
        if self._closed:
            if asyncio.iscoroutine(awaitable):
                awaitable.close()
            raise RuntimeError("BackendLoopRunner is closed")
        future = self._submit(awaitable)
        return future.result()

    def close(self) -> None:
        """Close backend and stop loop thread."""
        if self._closed:
            return
        self._closed = True
        try:
            if self._loop.is_running():
                future = self._submit(self.backend.close())
                future.result()
        finally:
            if self._loop.is_running():
                self._loop.call_soon_threadsafe(self._loop.stop)
            self._thread.join(timeout=5)
