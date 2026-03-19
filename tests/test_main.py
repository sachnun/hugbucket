"""Tests for top-level MODE-based entrypoint routing."""

from __future__ import annotations

import pytest

import hugbucket.main as entry


def test_resolve_mode_uses_env_mode() -> None:
    assert entry._resolve_mode("ftp") == "ftp"
    assert entry._resolve_mode("S3") == "s3"


def test_resolve_mode_trims_whitespace() -> None:
    assert entry._resolve_mode("  ftp  ") == "ftp"


def test_resolve_mode_rejects_missing_env() -> None:
    with pytest.raises(ValueError):
        entry._resolve_mode(None)
    with pytest.raises(ValueError):
        entry._resolve_mode("")
    with pytest.raises(ValueError):
        entry._resolve_mode("   ")


def test_resolve_mode_rejects_invalid_value() -> None:
    with pytest.raises(ValueError):
        entry._resolve_mode("http")


def test_main_routes_to_s3_from_env(monkeypatch) -> None:
    called = {"s3": False}

    def _fake_s3() -> None:
        called["s3"] = True

    monkeypatch.setattr(entry, "_run_s3", _fake_s3)
    monkeypatch.setattr(
        entry,
        "_run_ftp",
        lambda: (_ for _ in ()).throw(AssertionError("unexpected ftp route")),
    )
    monkeypatch.setenv("MODE", "s3")

    entry.main()
    assert called["s3"]


def test_main_routes_to_ftp_from_env(monkeypatch) -> None:
    called = {"ftp": False}

    def _fake_ftp() -> None:
        called["ftp"] = True

    monkeypatch.setattr(entry, "_run_ftp", _fake_ftp)
    monkeypatch.setattr(
        entry,
        "_run_s3",
        lambda: (_ for _ in ()).throw(AssertionError("unexpected s3 route")),
    )
    monkeypatch.setenv("MODE", "ftp")

    entry.main()
    assert called["ftp"]


def test_main_rejects_invalid_mode(monkeypatch) -> None:
    monkeypatch.setenv("MODE", "http")

    with pytest.raises(SystemExit) as exc:
        entry.main()

    assert exc.value.code == 2


def test_main_rejects_missing_mode_env(monkeypatch) -> None:
    monkeypatch.delenv("MODE", raising=False)

    with pytest.raises(SystemExit) as exc:
        entry.main()

    assert exc.value.code == 2
