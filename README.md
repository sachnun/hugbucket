# HugBucket

S3-compatible gateway for Hugging Face Storage Buckets.

## Quick Start

You'll need [Docker](https://docs.docker.com/get-docker/) to get started.

```bash
docker run -d \
  -p 9000:9000 \
  -e HF_TOKEN=hf_xxxxx \
  -e AWS_ACCESS_KEY_ID=hugbucket \
  -e AWS_SECRET_ACCESS_KEY=hugbucket \
  ghcr.io/sachnun/hugbucket
```

## Usage

Interact with the gateway using the [AWS CLI](https://aws.amazon.com/cli/).

```bash
aws --endpoint-url http://localhost:9000 s3 ls
aws --endpoint-url http://localhost:9000 s3 cp file.txt s3://my-bucket/file.txt
```

## Development

This project uses [uv](https://docs.astral.sh/uv/) for dependency management.

```bash
uv sync
uv run hugbucket --hf-token hf_xxxxx
```
