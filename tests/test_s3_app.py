"""Tests for S3 app entrypoint startup wiring."""

from __future__ import annotations

from unittest.mock import MagicMock

from hugbucket.apps import s3 as s3_app


def test_s3_main_requires_hf_token(monkeypatch) -> None:
    monkeypatch.delenv("HF_TOKEN", raising=False)
    monkeypatch.setattr(s3_app.sys, "argv", ["hugbucket"])

    import pytest

    with pytest.raises(SystemExit) as exc:
        s3_app.main()

    assert exc.value.code == 1


def test_s3_main_starts(monkeypatch) -> None:
    seen = {}

    class _FakeApp(dict):
        pass

    monkeypatch.setenv("HF_TOKEN", "hf_test")
    monkeypatch.setattr(s3_app.sys, "argv", ["hugbucket"])

    backend = MagicMock()
    monkeypatch.setattr(
        s3_app,
        "HFStorageBackend",
        lambda config: backend,
    )

    def _create_app(*, config, backend, max_upload_bytes):
        seen["config"] = config
        seen["backend"] = backend
        seen["max_upload_bytes"] = max_upload_bytes
        return _FakeApp()

    monkeypatch.setattr(s3_app, "create_s3_app", _create_app)

    def _run_app(app, host, port, print=None):
        seen["run"] = (app, host, port, print)

    monkeypatch.setattr(s3_app.web, "run_app", _run_app)

    s3_app.main()

    assert seen["backend"] is backend
    assert seen["max_upload_bytes"] == 1024 * 1024 * 1024
    _, host, port, print_fn = seen["run"]
    assert host == "0.0.0.0"
    assert port == 9000
    assert print_fn is None
