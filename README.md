# HugBucket

S3-compatible gateway for Hugging Face Storage Buckets.

## Quick Start

```bash
docker run -d -p 9000:9000 -e HF_TOKEN=hf_xxxxx ghcr.io/sachnun/hugbucket
```

## Usage

```bash
aws --endpoint-url http://localhost:9000 s3 ls
aws --endpoint-url http://localhost:9000 s3 cp file.txt s3://my-bucket/file.txt
```

## Environment Variables

| Variable | Required |
|---|---|
| `HF_TOKEN` | Yes |
| `AWS_ACCESS_KEY_ID` | No |
| `AWS_SECRET_ACCESS_KEY` | No |

## Development

```bash
uv sync
uv run hugbucket --hf-token hf_xxxxx
```
