"""Tests for FTP app argument wiring and startup flow."""

from __future__ import annotations

import asyncio
import inspect
from unittest.mock import AsyncMock, MagicMock

import pytest

from hugbucket.apps import ftp as ftp_app


def _runner_call(awaitable):  # type: ignore[no-untyped-def]
    if inspect.isawaitable(awaitable):
        async def _await_value() -> object:
            return await awaitable

        return asyncio.run(_await_value())
    return awaitable


def test_ftp_main_starts_server(monkeypatch) -> None:
    server = MagicMock()
    runner = MagicMock()
    backend = MagicMock()
    backend.resolve_namespace = AsyncMock(return_value="alice")
    runner.call.side_effect = _runner_call

    monkeypatch.setattr(
        ftp_app,
        "HFStorageBackend",
        lambda config: backend,
    )
    monkeypatch.setattr(
        ftp_app,
        "create_ftp_server",
        lambda config, backend: (server, runner),
    )
    monkeypatch.setenv("HF_TOKEN", "hf_test")
    monkeypatch.setattr(ftp_app.sys, "argv", ["hugbucket-ftp"])

    ftp_app.main()

    runner.call.assert_called_once()
    call_arg = runner.call.call_args.args[0]
    assert getattr(call_arg, "cr_code", None) is not None
    server.serve_forever.assert_called_once()
    server.close_all.assert_called_once()
    runner.close.assert_called_once()


def test_ftp_main_requires_hf_token(monkeypatch) -> None:
    monkeypatch.delenv("HF_TOKEN", raising=False)
    monkeypatch.setattr(ftp_app.sys, "argv", ["hugbucket-ftp"])
    with pytest.raises(SystemExit) as exc:
        ftp_app.main()
    assert exc.value.code == 1


def test_ftp_main_reads_username_password_from_env(monkeypatch) -> None:
    server = MagicMock()
    runner = MagicMock()
    backend = MagicMock()
    backend.resolve_namespace = AsyncMock(return_value="alice")
    runner.call.side_effect = _runner_call
    seen = {}

    monkeypatch.setattr(
        ftp_app,
        "HFStorageBackend",
        lambda config: backend,
    )

    def _create_server(*, config, backend):  # type: ignore[no-untyped-def]
        seen["user"] = config.ftp_user
        seen["password"] = config.ftp_password
        return server, runner

    monkeypatch.setattr(ftp_app, "create_ftp_server", _create_server)
    monkeypatch.setenv("HF_TOKEN", "hf_test")
    monkeypatch.setenv("FTP_USERNAME", "ftpuser")
    monkeypatch.setenv("FTP_PASSWORD", "ftppass")
    monkeypatch.setattr(ftp_app.sys, "argv", ["hugbucket-ftp"])

    ftp_app.main()
    assert seen["user"] == "ftpuser"
    assert seen["password"] == "ftppass"
