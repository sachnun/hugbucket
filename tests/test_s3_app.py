"""Tests for S3 app entrypoint mode and startup wiring."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from hugbucket.apps import s3 as s3_app


def test_s3_main_requires_mode(monkeypatch) -> None:
    monkeypatch.setenv("HF_TOKEN", "hf_test")
    monkeypatch.delenv("MODE", raising=False)
    monkeypatch.setattr(s3_app.sys, "argv", ["hugbucket-s3"])

    with pytest.raises(SystemExit) as exc:
        s3_app.main()

    assert exc.value.code == 2


def test_s3_main_rejects_wrong_mode(monkeypatch) -> None:
    monkeypatch.setenv("HF_TOKEN", "hf_test")
    monkeypatch.setenv("MODE", "ftp")
    monkeypatch.setattr(s3_app.sys, "argv", ["hugbucket-s3"])

    with pytest.raises(SystemExit) as exc:
        s3_app.main()

    assert exc.value.code == 2


def test_s3_main_starts_with_s3_mode(monkeypatch) -> None:
    seen = {}

    class _FakeApp(dict):
        pass

    monkeypatch.setenv("HF_TOKEN", "hf_test")
    monkeypatch.setenv("MODE", "s3")
    monkeypatch.setattr(s3_app.sys, "argv", ["hugbucket-s3"])

    backend = MagicMock()
    monkeypatch.setattr(
        s3_app,
        "HFStorageBackend",
        lambda config: backend,
    )

    def _create_app(*, config, backend, max_upload_bytes):  # type: ignore[no-untyped-def]
        seen["config"] = config
        seen["backend"] = backend
        seen["max_upload_bytes"] = max_upload_bytes
        return _FakeApp()

    monkeypatch.setattr(s3_app, "create_s3_app", _create_app)

    def _run_app(app, host, port, print=None):  # type: ignore[no-untyped-def]
        seen["run"] = (app, host, port, print)

    monkeypatch.setattr(s3_app.web, "run_app", _run_app)

    s3_app.main()

    assert seen["backend"] is backend
    assert seen["max_upload_bytes"] == 1024 * 1024 * 1024
    _, host, port, print_fn = seen["run"]
    assert host == "0.0.0.0"
    assert port == 9000
    assert print_fn is None
