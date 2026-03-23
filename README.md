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

#### WebDAV

```bash
docker run -d \
  -p 8080:8080 \
  -e MODE=webdav \
  -e HF_TOKEN=hf_xxxxx \
  -e WEBDAV_USERNAME=hugbucket \
  -e WEBDAV_PASSWORD=hugbucket \
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

#### WebDAV

Mount in your file manager:
- **macOS Finder**: Go > Connect to Server > `http://localhost:8080`
- **Windows Explorer**: Map Network Drive > `http://localhost:8080`
- **Linux (GVFS)**: `davs://localhost:8080` or `dav://localhost:8080`
- **cadaver**: `cadaver http://localhost:8080`
- **rclone**: Configure a WebDAV remote with `http://localhost:8080`

Path mapping for WebDAV is `/<bucket>/<key>`.

## Environment Variables

| Variable | Description |
| --- | --- |
| `MODE`<sup>*</sup> | Run mode (`s3`, `ftp`, or `webdav`) |
| `HF_TOKEN`<sup>*</sup> | Hugging Face token |
| `AWS_ACCESS_KEY_ID` | S3 access key |
| `AWS_SECRET_ACCESS_KEY` | S3 secret key |
| `FTP_USERNAME` | FTP username |
| `FTP_PASSWORD` | FTP password |
| `WEBDAV_USERNAME` | WebDAV username |
| `WEBDAV_PASSWORD` | WebDAV password |

## Development

This project uses [uv](https://docs.astral.sh/uv/) for dependency management.

```bash
uv sync

MODE=s3 HF_TOKEN=hf_xxxxx uv run hugbucket

# Explicit protocol entrypoints
HF_TOKEN=hf_xxxxx uv run hugbucket-s3
HF_TOKEN=hf_xxxxx uv run hugbucket-ftp
HF_TOKEN=hf_xxxxx uv run hugbucket-webdav
```
