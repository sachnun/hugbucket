"""Tests for FTP server builder."""

from __future__ import annotations

from unittest.mock import MagicMock

from hugbucket.config import Config
from hugbucket.protocols.ftp.server import create_ftp_server


def test_create_ftp_server_returns_server_and_runner(monkeypatch) -> None:
    backend = MagicMock()
    created = {}

    class _FakeAuthorizer:
        def add_user(self, username, password, homedir, perm):  # type: ignore[no-untyped-def]
            created["user"] = (username, password, homedir, perm)

    class _FakeFTPServer:
        def __init__(self, addr, handler):  # type: ignore[no-untyped-def]
            created["addr"] = addr
            created["handler"] = handler

    class _FakeRunner:
        def __init__(self, b):  # type: ignore[no-untyped-def]
            self.backend = b

    class _FakeFTPHandler:
        pass

    monkeypatch.setattr(
        "hugbucket.protocols.ftp.server._load_pyftpdlib",
        lambda: (_FakeAuthorizer, _FakeFTPHandler, _FakeFTPServer),
    )
    monkeypatch.setattr("hugbucket.protocols.ftp.server.BackendLoopRunner", _FakeRunner)

    config = Config(
        ftp_host="127.0.0.1",
        ftp_port=2121,
        ftp_user="u",
        ftp_password="p",
        ftp_passive_min_port=30000,
        ftp_passive_max_port=30010,
    )
    server, runner = create_ftp_server(config=config, backend=backend)

    assert isinstance(server, _FakeFTPServer)
    assert isinstance(runner, _FakeRunner)
    assert created["addr"] == ("127.0.0.1", 2121)
    assert created["user"] == ("u", "p", "/", "elradfmwMT")
    assert created["handler"].backend_runner is runner


def test_create_ftp_server_without_passive_range(monkeypatch) -> None:
    backend = MagicMock()
    created = {}

    class _FakeAuthorizer:
        def add_user(self, username, password, homedir, perm):  # type: ignore[no-untyped-def]
            created["user"] = (username, password, homedir, perm)

    class _FakeFTPServer:
        def __init__(self, addr, handler):  # type: ignore[no-untyped-def]
            created["handler"] = handler

    class _FakeRunner:
        def __init__(self, b):  # type: ignore[no-untyped-def]
            self.backend = b

    class _FakeFTPHandler:
        pass

    monkeypatch.setattr(
        "hugbucket.protocols.ftp.server._load_pyftpdlib",
        lambda: (_FakeAuthorizer, _FakeFTPHandler, _FakeFTPServer),
    )
    monkeypatch.setattr("hugbucket.protocols.ftp.server.BackendLoopRunner", _FakeRunner)

    config = Config(ftp_passive_min_port=0, ftp_passive_max_port=0)
    create_ftp_server(config=config, backend=backend)

    assert not hasattr(created["handler"], "passive_ports")
