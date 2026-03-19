"""Tests for FTP-related configuration defaults and env parsing."""

from __future__ import annotations

from hugbucket.config import Config


def test_ftp_config_defaults() -> None:
    cfg = Config()
    assert cfg.ftp_host == "0.0.0.0"
    assert cfg.ftp_port == 2121
    assert cfg.ftp_user == "hugbucket"
    assert cfg.ftp_password == "hugbucket"
    assert cfg.ftp_passive_min_port == 30000
    assert cfg.ftp_passive_max_port == 30099


def test_ftp_port_env_parsing(monkeypatch) -> None:
    monkeypatch.setenv("FTP_PORT", "2200")
    cfg = Config()
    assert cfg.ftp_port == 2200


def test_ftp_port_env_invalid_falls_back(monkeypatch) -> None:
    monkeypatch.setenv("FTP_PORT", "not-int")
    cfg = Config()
    assert cfg.ftp_port == 2121
