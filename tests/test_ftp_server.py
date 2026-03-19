"""Tests for FTP server builder."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from hugbucket.config import Config
from hugbucket.protocols.ftp.server import create_ftp_server


def test_create_ftp_server_returns_server_and_runner(monkeypatch) -> None:
    backend = MagicMock()
    created = {}

    class _FakeAuthorizer:
        def __init__(self) -> None:
            self.user_calls = 0

        def add_user(self, username, password, homedir, perm):  # type: ignore[no-untyped-def]
            self.user_calls += 1
            created["user"] = (username, password, homedir, perm)

        def add_anonymous(self, homedir, perm):  # type: ignore[no-untyped-def]
            created["anonymous"] = (homedir, perm)

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
    assert "anonymous" not in created
    assert created["handler"].backend_runner is runner


def test_create_ftp_server_without_passive_range(monkeypatch) -> None:
    backend = MagicMock()
    created = {}

    class _FakeAuthorizer:
        def __init__(self) -> None:
            self.user_calls = 0

        def add_user(self, username, password, homedir, perm):  # type: ignore[no-untyped-def]
            self.user_calls += 1
            created["user"] = (username, password, homedir, perm)

        def add_anonymous(self, homedir, perm):  # type: ignore[no-untyped-def]
            created["anonymous"] = (homedir, perm)

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

    config = Config(ftp_user="u", ftp_password="p", ftp_passive_min_port=0, ftp_passive_max_port=0)
    create_ftp_server(config=config, backend=backend)

    assert not hasattr(created["handler"], "passive_ports")


def test_create_ftp_server_allows_anonymous_when_auth_empty(monkeypatch) -> None:
    backend = MagicMock()
    created = {}

    class _FakeAuthorizer:
        def add_user(self, username, password, homedir, perm):  # type: ignore[no-untyped-def]
            created["user"] = (username, password, homedir, perm)

        def add_anonymous(self, homedir, perm):  # type: ignore[no-untyped-def]
            created["anonymous"] = (homedir, perm)

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

    config = Config(ftp_user="", ftp_password="")
    create_ftp_server(config=config, backend=backend)

    assert created["anonymous"] == ("/", "elradfmwMT")
    assert "user" not in created


def test_create_ftp_server_rejects_partial_auth_config(monkeypatch) -> None:
    backend = MagicMock()

    class _FakeAuthorizer:
        def add_user(self, username, password, homedir, perm):  # type: ignore[no-untyped-def]
            raise AssertionError("should not be called")

        def add_anonymous(self, homedir, perm):  # type: ignore[no-untyped-def]
            raise AssertionError("should not be called")

    class _FakeFTPServer:
        def __init__(self, addr, handler):  # type: ignore[no-untyped-def]
            self.addr = addr
            self.handler = handler

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

    with pytest.raises(ValueError):
        create_ftp_server(config=Config(ftp_user="user", ftp_password=""), backend=backend)
