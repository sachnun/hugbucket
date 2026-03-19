"""Configuration for HugBucket."""

import os
from dataclasses import dataclass, field


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


@dataclass
class Config:
    # S3 gateway settings
    host: str = "0.0.0.0"
    port: int = 9000
    region: str = "us-east-1"

    # FTP gateway settings
    ftp_host: str = field(default_factory=lambda: os.environ.get("FTP_HOST", "0.0.0.0"))
    ftp_port: int = field(default_factory=lambda: _env_int("FTP_PORT", 2121))
    ftp_user: str = field(
        default_factory=lambda: os.environ.get("FTP_USERNAME", "")
    )
    ftp_password: str = field(
        default_factory=lambda: os.environ.get("FTP_PASSWORD", "")
    )
    ftp_banner: str = field(
        default_factory=lambda: os.environ.get("FTP_BANNER", "HugBucket FTP ready")
    )
    ftp_passive_min_port: int = field(
        default_factory=lambda: _env_int("FTP_PASSIVE_MIN_PORT", 30000)
    )
    ftp_passive_max_port: int = field(
        default_factory=lambda: _env_int("FTP_PASSIVE_MAX_PORT", 30099)
    )

    # HF Hub settings
    hf_endpoint: str = "https://huggingface.co"
    hf_token: str = field(default_factory=lambda: os.environ.get("HF_TOKEN", ""))

    # S3 auth — maps to HF token
    s3_access_key: str = field(
        default_factory=lambda: os.environ.get("AWS_ACCESS_KEY_ID", "")
    )
    s3_secret_key: str = field(
        default_factory=lambda: os.environ.get("AWS_SECRET_ACCESS_KEY", "")
    )

    # HF namespace (user or org that owns the buckets)
    # Resolved automatically from HF token via /api/whoami-v2 at startup
    hf_namespace: str = ""

    # Xet CDC settings
    xet_chunk_target: int = 65536  # 64 KiB
    xet_chunk_min: int = 8192  # 8 KiB
    xet_chunk_max: int = 131072  # 128 KiB
    xet_xorb_max_bytes: int = 67108864  # 64 MiB

    # Concurrency / connection-pool settings
    # Total outbound connections shared across all concurrent downloads.
    # 0 = unlimited (no cap on simultaneous outbound connections).
    http_pool_size: int = 0

    # Cache settings
    xorb_cache_max_bytes: int = 512 * 1024 * 1024  # 512 MiB
    recon_cache_max_entries: int = 1024
    recon_cache_ttl: int = 300  # 5 minutes
    file_info_cache_max_entries: int = 256
    file_info_cache_ttl: int = 30  # 30 seconds — short enough for consistency
