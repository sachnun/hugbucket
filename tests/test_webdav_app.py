"""Tests for WebDAV app entrypoint mode and startup wiring."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from hugbucket.apps import webdav as webdav_app


def test_webdav_main_requires_mode(monkeypatch) -> None:
    monkeypatch.setenv("HF_TOKEN", "hf_test")
    monkeypatch.delenv("MODE", raising=False)
    monkeypatch.setattr(webdav_app.sys, "argv", ["hugbucket-webdav"])

    with pytest.raises(SystemExit) as exc:
        webdav_app.main()

    assert exc.value.code == 2


def test_webdav_main_rejects_wrong_mode(monkeypatch) -> None:
    monkeypatch.setenv("HF_TOKEN", "hf_test")
    monkeypatch.setenv("MODE", "s3")
    monkeypatch.setattr(webdav_app.sys, "argv", ["hugbucket-webdav"])

    with pytest.raises(SystemExit) as exc:
        webdav_app.main()

    assert exc.value.code == 2


def test_webdav_main_requires_hf_token(monkeypatch) -> None:
    monkeypatch.delenv("HF_TOKEN", raising=False)
    monkeypatch.setenv("MODE", "webdav")
    monkeypatch.setattr(webdav_app.sys, "argv", ["hugbucket-webdav"])

    with pytest.raises(SystemExit) as exc:
        webdav_app.main()

    assert exc.value.code == 1


def test_webdav_main_starts_with_webdav_mode(monkeypatch) -> None:
    seen = {}

    class _FakeApp(dict):
        pass

    monkeypatch.setenv("HF_TOKEN", "hf_test")
    monkeypatch.setenv("MODE", "webdav")
    monkeypatch.setattr(webdav_app.sys, "argv", ["hugbucket-webdav"])

    backend = MagicMock()
    monkeypatch.setattr(
        webdav_app,
        "HFStorageBackend",
        lambda config: backend,
    )

    def _create_app(*, config, backend, max_upload_bytes):  # type: ignore[no-untyped-def]
        seen["config"] = config
        seen["backend"] = backend
        seen["max_upload_bytes"] = max_upload_bytes
        return _FakeApp()

    monkeypatch.setattr(webdav_app, "create_webdav_app", _create_app)

    def _run_app(app, host, port, print=None):  # type: ignore[no-untyped-def]
        seen["run"] = (app, host, port, print)

    monkeypatch.setattr(webdav_app.web, "run_app", _run_app)

    webdav_app.main()

    assert seen["backend"] is backend
    assert seen["max_upload_bytes"] == 1024 * 1024 * 1024
    _, host, port, print_fn = seen["run"]
    assert host == "0.0.0.0"
    assert port == 8080
    assert print_fn is None


def test_webdav_main_reads_credentials_from_env(monkeypatch) -> None:
    seen = {}

    class _FakeApp(dict):
        pass

    monkeypatch.setenv("HF_TOKEN", "hf_test")
    monkeypatch.setenv("MODE", "webdav")
    monkeypatch.setenv("WEBDAV_USERNAME", "davuser")
    monkeypatch.setenv("WEBDAV_PASSWORD", "davpass")
    monkeypatch.setattr(webdav_app.sys, "argv", ["hugbucket-webdav"])

    backend = MagicMock()
    monkeypatch.setattr(
        webdav_app,
        "HFStorageBackend",
        lambda config: backend,
    )

    def _create_app(*, config, backend, max_upload_bytes):  # type: ignore[no-untyped-def]
        seen["user"] = config.webdav_user
        seen["password"] = config.webdav_password
        return _FakeApp()

    monkeypatch.setattr(webdav_app, "create_webdav_app", _create_app)
    monkeypatch.setattr(webdav_app.web, "run_app", lambda *a, **kw: None)

    webdav_app.main()
    assert seen["user"] == "davuser"
    assert seen["password"] == "davpass"
