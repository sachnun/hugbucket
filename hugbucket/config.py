"""Configuration for HugBucket."""

import os
from dataclasses import dataclass, field


@dataclass
class Config:
    # S3 gateway settings
    host: str = "0.0.0.0"
    port: int = 9000
    region: str = "us-east-1"

    # HF Hub settings
    hf_endpoint: str = field(
        default_factory=lambda: os.environ.get("HF_ENDPOINT", "https://huggingface.co")
    )
    hf_token: str = field(default_factory=lambda: os.environ.get("HF_TOKEN", ""))

    # S3 auth — maps to HF token
    s3_access_key: str = field(
        default_factory=lambda: os.environ.get("AWS_ACCESS_KEY_ID", "hugbucket")
    )
    s3_secret_key: str = field(
        default_factory=lambda: os.environ.get("AWS_SECRET_ACCESS_KEY", "hugbucket")
    )

    # HF namespace (user or org that owns the buckets)
    hf_namespace: str = field(
        default_factory=lambda: os.environ.get("HF_NAMESPACE", "me")
    )

    # Xet CDC settings
    xet_chunk_target: int = 65536  # 64 KiB
    xet_chunk_min: int = 8192  # 8 KiB
    xet_chunk_max: int = 131072  # 128 KiB
    xet_xorb_max_bytes: int = 67108864  # 64 MiB
