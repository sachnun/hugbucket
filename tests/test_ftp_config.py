"""Tests for FTP-related configuration defaults and env parsing."""

from __future__ import annotations

from hugbucket.config import Config


def test_ftp_config_defaults() -> None:
    cfg = Config()
    assert cfg.ftp_host == "0.0.0.0"
    assert cfg.ftp_port == 2121
    assert cfg.ftp_user == ""
    assert cfg.ftp_password == ""


def test_port_env_does_not_override_defaults(monkeypatch) -> None:
    monkeypatch.setenv("PORT", "2200")
    cfg = Config()
    assert cfg.port == 9000
    assert cfg.ftp_port == 2121
