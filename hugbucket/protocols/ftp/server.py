"""FTP server builder for HugBucket."""

from __future__ import annotations

import importlib
import logging
from typing import Any

from hugbucket.config import Config
from hugbucket.core.backend import StorageBackend
from hugbucket.protocols.ftp.filesystem import HugBucketFTPFilesystem
from hugbucket.protocols.ftp.runtime import BackendLoopRunner

logger = logging.getLogger(__name__)


def _load_pyftpdlib() -> tuple[type[Any], type[Any], type[Any]]:
    """Load pyftpdlib lazily so module import doesn't hard-fail."""
    try:
        authorizers_mod = importlib.import_module("pyftpdlib.authorizers")
        handlers_mod = importlib.import_module("pyftpdlib.handlers")
        servers_mod = importlib.import_module("pyftpdlib.servers")
        _DummyAuthorizer = getattr(authorizers_mod, "DummyAuthorizer")
        _FTPHandler = getattr(handlers_mod, "FTPHandler")
        _FTPServer = getattr(servers_mod, "FTPServer")
    except ImportError as exc:  # pragma: no cover - depends on environment
        raise RuntimeError(
            "FTP dependencies are missing. Install with project dependencies "
            "(requires pyftpdlib)."
        ) from exc
    return _DummyAuthorizer, _FTPHandler, _FTPServer


def create_ftp_server(
    *,
    config: Config,
    backend: StorageBackend,
) -> tuple[Any, BackendLoopRunner]:
    """Create configured FTPServer and backend loop runner."""
    DummyAuthorizer, FTPHandler, FTPServer = _load_pyftpdlib()

    runner = BackendLoopRunner(backend)

    authorizer = DummyAuthorizer()
    has_user = bool(config.ftp_user)
    has_password = bool(config.ftp_password)
    if has_user and has_password:
        authorizer.add_user(
            config.ftp_user,
            config.ftp_password,
            "/",
            perm="elradfmwMT",
        )
    elif not has_user and not has_password:
        authorizer.add_anonymous("/", perm="elradfmwMT")
        logger.warning("FTP auth disabled: allowing anonymous login")
    else:
        raise ValueError(
            "FTP auth config invalid: set both FTP_USERNAME and FTP_PASSWORD, "
            "or leave both empty for anonymous access."
        )

    def _on_connect(self) -> None:  # type: ignore[no-untyped-def]
        logger.debug("FTP connect from %s", self.remote_ip)

    def _on_disconnect(self) -> None:  # type: ignore[no-untyped-def]
        logger.debug("FTP disconnect from %s", self.remote_ip)

    handler_cls = type("ConfiguredHugBucketFTPHandler", (FTPHandler,), {})
    handler_cls.abstracted_fs = HugBucketFTPFilesystem
    handler_cls.use_sendfile = False
    handler_cls.on_connect = _on_connect
    handler_cls.on_disconnect = _on_disconnect
    handler_cls.authorizer = authorizer
    handler_cls.banner = config.ftp_banner
    handler_cls.backend_runner = runner

    if (
        config.ftp_passive_min_port > 0
        and config.ftp_passive_max_port >= config.ftp_passive_min_port
    ):
        handler_cls.passive_ports = range(
            config.ftp_passive_min_port,
            config.ftp_passive_max_port + 1,
        )

    server = FTPServer((config.ftp_host, config.ftp_port), handler_cls)
    return server, runner
