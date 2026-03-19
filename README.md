# HugBucket

Multi-protocol gateway for Hugging Face Storage Buckets.

## Quick Start

You'll need [Docker](https://docs.docker.com/get-docker/) to get started.

```bash
docker run -d \
  -p 9000:9000 \
  -p 2121:2121 \
  -p 30000-30099:30000-30099 \
  -e MODE=s3 \
  -e HF_TOKEN=hf_xxxxx \
  -e AWS_ACCESS_KEY_ID=hugbucket \
  -e AWS_SECRET_ACCESS_KEY=hugbucket \
  ghcr.io/sachnun/hugbucket
```

Select adapter mode with env `MODE`:

```bash
# S3
docker run --rm -p 9000:9000 \
  -e MODE=s3 \
  -e HF_TOKEN=hf_xxxxx \
  ghcr.io/sachnun/hugbucket

# FTP
docker run --rm -p 2121:2121 -p 30000-30099:30000-30099 \
  -e MODE=ftp \
  -e HF_TOKEN=hf_xxxxx \
  -e FTP_USERNAME=hugbucket \
  -e FTP_PASSWORD=hugbucket \
  ghcr.io/sachnun/hugbucket

```

## Usage

Use the adapter that matches your selected `MODE`.

S3 mode (AWS CLI):

```bash
aws --endpoint-url http://localhost:9000 s3 ls
aws --endpoint-url http://localhost:9000 s3 cp file.txt s3://my-bucket/file.txt
```

FTP mode (any FTP client):

```bash
ftp localhost 2121
# username: hugbucket
# password: hugbucket
```

You can also run explicit protocol entrypoints (auth from env):

```bash
HF_TOKEN=hf_xxxxx uv run hugbucket-s3
```

FTP entrypoint:

```bash
HF_TOKEN=hf_xxxxx uv run hugbucket-ftp
```

By default FTP uses:

- host/port: `0.0.0.0:2121`
- credentials: `hugbucket` / `hugbucket`
- virtual path mapping: `/<bucket>/<key>`

Configure with env vars:

```bash
HF_TOKEN=hf_xxxxx \
FTP_USERNAME=hugbucket \
FTP_PASSWORD=hugbucket \
uv run hugbucket-ftp
```

Environment variables:

- `MODE` (required: `s3` or `ftp`)
- `HF_TOKEN`, `HF_ENDPOINT`
- `FTP_HOST`, `FTP_PORT`, `FTP_USERNAME`, `FTP_PASSWORD`
- `FTP_BANNER`, `FTP_PASSIVE_MIN_PORT`, `FTP_PASSIVE_MAX_PORT`

`MODE` is required for `uv run hugbucket` and Docker entrypoint usage.

## Development

This project uses [uv](https://docs.astral.sh/uv/) for dependency management.

```bash
uv sync
MODE=s3 HF_TOKEN=hf_xxxxx uv run hugbucket
```

## Internal Layout

The codebase is split by concern to make protocol expansion easier:

- `hugbucket/core`: protocol-agnostic interfaces and shared models
- `hugbucket/providers`: backend providers (currently HF Hub + Xet)
- `hugbucket/protocols`: protocol adapters (S3 + FTP)
- `hugbucket/apps`: runnable app entrypoints per protocol
