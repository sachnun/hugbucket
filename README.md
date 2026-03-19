# HugBucket

Multi-protocol gateway for Hugging Face Storage Buckets.

## Quick Start

You'll need [Docker](https://docs.docker.com/get-docker/) to get started.

#### S3

```bash
docker run -d \
  -p 9000:9000 \
  -e MODE=s3 \
  -e HF_TOKEN=hf_xxxxx \
  -e AWS_ACCESS_KEY_ID=hugbucket \
  -e AWS_SECRET_ACCESS_KEY=hugbucket \
  ghcr.io/sachnun/hugbucket
```

#### FTP

```bash
docker run -d \
  -p 2121:2121 \
  -p 30000-30099:30000-30099 \
  -e MODE=ftp \
  -e HF_TOKEN=hf_xxxxx \
  -e FTP_USERNAME=hugbucket \
  -e FTP_PASSWORD=hugbucket \
  ghcr.io/sachnun/hugbucket
```

## Usage

#### S3 ([AWS CLI](https://aws.amazon.com/cli/))

```bash
aws --endpoint-url http://localhost:9000 s3 ls
aws --endpoint-url http://localhost:9000 s3 cp file.txt s3://my-bucket/file.txt
```

#### FTP

```bash
ftp localhost 2121
# username: hugbucket
# password: hugbucket
```

Path mapping for FTP is `/<bucket>/<key>`.

## Environment Variables

Required:

- `MODE`
- `HF_TOKEN`

Optional:

- `HF_ENDPOINT` (default: `https://huggingface.co`)
- `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` (default: `hugbucket`)
- `FTP_HOST`, `FTP_PORT` (default: `0.0.0.0:2121`)
- `FTP_USERNAME`, `FTP_PASSWORD` (default: `hugbucket` / `hugbucket`)
- `FTP_BANNER` (default: `HugBucket FTP ready`)
- `FTP_PASSIVE_MIN_PORT`, `FTP_PASSIVE_MAX_PORT` (default: `30000` / `30099`)

## Development

This project uses [uv](https://docs.astral.sh/uv/) for dependency management.

```bash
uv sync

MODE=s3 HF_TOKEN=hf_xxxxx uv run hugbucket

# Explicit protocol entrypoints
HF_TOKEN=hf_xxxxx uv run hugbucket-s3
HF_TOKEN=hf_xxxxx uv run hugbucket-ftp
```
