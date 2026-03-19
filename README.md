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
  ghcr.io/sachnun/hugbucket
```

#### FTP

```bash
docker run -d \
  -p 2121:2121 \
  -p 30000-30099:30000-30099 \
  -e MODE=ftp \
  -e HF_TOKEN=hf_xxxxx \
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

| Variable | Default | Description |
| --- | --- | --- |
| `MODE`<sup>*</sup> | - | Protocol selector: `s3` or `ftp` |
| `HF_TOKEN`<sup>*</sup> | - | Hugging Face access token |
| `AWS_ACCESS_KEY_ID` | empty | S3 access key used by clients; leave empty to disable S3 auth |
| `AWS_SECRET_ACCESS_KEY` | empty | S3 secret key used by clients; leave empty to disable S3 auth |
| `FTP_HOST` | `0.0.0.0` | FTP bind host |
| `FTP_PORT` | `2121` | FTP bind port |
| `FTP_USERNAME` | empty | FTP login username; leave empty with `FTP_PASSWORD` for anonymous FTP |
| `FTP_PASSWORD` | empty | FTP login password; leave empty with `FTP_USERNAME` for anonymous FTP |
| `FTP_BANNER` | `HugBucket FTP ready` | FTP server welcome banner |
| `FTP_PASSIVE_MIN_PORT` | `30000` | FTP passive mode range start |
| `FTP_PASSIVE_MAX_PORT` | `30099` | FTP passive mode range end |

## Development

This project uses [uv](https://docs.astral.sh/uv/) for dependency management.

```bash
uv sync

MODE=s3 HF_TOKEN=hf_xxxxx uv run hugbucket

# Explicit protocol entrypoints
HF_TOKEN=hf_xxxxx uv run hugbucket-s3
HF_TOKEN=hf_xxxxx uv run hugbucket-ftp
```
