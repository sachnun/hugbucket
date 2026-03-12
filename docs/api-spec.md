# Hugging Face Hub - Storage Buckets REST API Specification

> Generated from the official OpenAPI spec at `https://huggingface.co/.well-known/openapi.json`,
> the Python client source (`huggingface_hub` v1.6.0), and the official documentation.
>
> **Base URL:** `https://huggingface.co`

---

## Table of Contents

1. [Overview: How Buckets Differ from Repos](#1-overview-how-buckets-differ-from-repos)
2. [Authentication](#2-authentication)
3. [Bucket CRUD Endpoints](#3-bucket-crud-endpoints)
   - [Create Bucket](#31-create-bucket)
   - [Get Bucket Info](#32-get-bucket-info)
   - [List Buckets](#33-list-buckets)
   - [Delete Bucket](#34-delete-bucket)
   - [Move/Rename Bucket](#35-moverename-bucket)
4. [Bucket File Endpoints](#4-bucket-file-endpoints)
   - [List Bucket Tree](#41-list-bucket-tree)
   - [Get Bucket Paths Info (Batch)](#42-get-bucket-paths-info-batch)
   - [Batch Bucket Files (Add/Delete)](#43-batch-bucket-files-adddelete)
   - [Get Bucket File Metadata (HEAD)](#44-get-bucket-file-metadata-head)
   - [Resolve/Download Bucket File (GET)](#45-resolvedownload-bucket-file-get)
5. [Xet Storage Token Endpoints](#5-xet-storage-token-endpoints)
   - [Get Xet Write Token](#51-get-xet-write-token)
   - [Get Xet Read Token](#52-get-xet-read-token)
6. [Resource Group Endpoints](#6-resource-group-endpoints)
   - [Set Resource Group](#61-set-resource-group)
   - [Get Resource Group](#62-get-resource-group)
7. [Data Structures / Schemas](#7-data-structures--schemas)

---

## 1. Overview: How Buckets Differ from Repos

Buckets are a **distinct resource type** on the Hugging Face Hub, separate from models, datasets, and spaces:

| Feature | Repos (model/dataset/space) | Buckets |
|---|---|---|
| Storage backend | Git + Git-LFS | Xet (content-addressable, S3-like object storage) |
| Version control | Full git history, branches, tags, commits | **No versioning** — mutable, overwrite-in-place |
| API path prefix | `/api/models/`, `/api/datasets/`, `/api/spaces/` | **`/api/buckets/`** (dedicated endpoints) |
| Web URL prefix | `/{namespace}/{repo}` or `/datasets/{ns}/{repo}` | **`/buckets/{namespace}/{bucket_name}`** |
| `hf://` protocol | `hf://datasets/...`, `hf://spaces/...` | **`hf://buckets/{namespace}/{bucket_name}`** |
| Repo type identifier | `"model"`, `"dataset"`, `"space"` | **`"bucket"`** |
| Create endpoint | `POST /api/repos/create` with `type` field | **`POST /api/buckets/{namespace}/{name}`** (dedicated) |
| Delete endpoint | `DELETE /api/repos/{type}/{namespace}/{repo}` | **`DELETE /api/buckets/{namespace}/{bucket_name}`** (dedicated) |
| Move endpoint | `POST /api/repos/move` with `type: "bucket"` | **Shared** with repos |
| File listing | `GET /api/{type}/{ns}/{repo}/tree/{rev}/{path}` | **`GET /api/buckets/{ns}/{name}/tree`** (no revisions) |
| File upload | Git commit API (`POST /api/{type}/{ns}/{repo}/commit/{rev}`) | **`POST /api/buckets/{ns}/{name}/batch`** (NDJSON) |
| File download | `GET /{type}/{ns}/{repo}/resolve/{rev}/{path}` | **`GET /buckets/{ns}/{name}/resolve/{path}`** (no revision) |
| Branches/Tags | Yes | **No** |
| Commits | Yes | **No** |
| Discussions/PRs | Yes (via shared `{repoType}` endpoints) | Via shared endpoints (if supported) |

**Key architectural difference:** Buckets do NOT use the standard `/api/repos/create` endpoint for creation or `/api/repos/{type}` for deletion. They have their own dedicated `/api/buckets/` endpoints. However, they share the `/api/repos/move` endpoint (with `type: "bucket"`) and the Xet token endpoints (with `repoType: "buckets"`).

---

## 2. Authentication

All bucket API endpoints require authentication via a Hugging Face User Access Token.

**Required Header:**
```
Authorization: Bearer hf_xxxxxxxxxxxxxxxxxxxxx
```

The token must have **write** access for create/delete/upload operations and **read** access for listing/downloading.

---

## 3. Bucket CRUD Endpoints

### 3.1 Create Bucket

Creates a new bucket under a namespace.

| Field | Value |
|---|---|
| **Method** | `POST` |
| **URL** | `/api/buckets/{namespace}/{bucket_name}` |
| **Content-Type** | `application/json` |

**Path Parameters:**

| Parameter | Type | Required | Description |
|---|---|---|---|
| `namespace` | string | Yes | Username or organization name. Use `"me"` for the authenticated user. |
| `bucket_name` | string | Yes | Name of the bucket to create. |

**Request Body (JSON):**

```json
{
  "private": false,
  "resourceGroupId": "66670e5163145ca562cb1988"
}
```

| Field | Type | Required | Description |
|---|---|---|---|
| `private` | boolean | No | Whether the bucket is private. Defaults to public (or org default). |
| `resourceGroupId` | string (24-char hex) | No | Enterprise Hub resource group ID. |

**Response (200 OK):**

```json
{
  "url": "https://huggingface.co/buckets/username/my-bucket"
}
```

**Status Codes:**

| Code | Description |
|---|---|
| 200 | Bucket created successfully |
| 401 | Unauthorized — invalid or missing token |
| 403 | Forbidden — no write access to namespace |
| 409 | Conflict — bucket already exists |

---

### 3.2 Get Bucket Info

Get metadata about a specific bucket.

| Field | Value |
|---|---|
| **Method** | `GET` |
| **URL** | `/api/buckets/{namespace}/{bucket_name}` |

**Path Parameters:**

| Parameter | Type | Required | Description |
|---|---|---|---|
| `namespace` | string | Yes | Username or organization name. |
| `bucket_name` | string | Yes | Name of the bucket. |

**Response (200 OK):**

```json
{
  "id": "username/my-bucket",
  "private": false,
  "createdAt": "2026-02-06T17:37:57.000Z",
  "size": 551879671,
  "totalFiles": 12
}
```

| Field | Type | Description |
|---|---|---|
| `id` | string | Full bucket ID (`namespace/name`) |
| `private` | boolean | Visibility of the bucket |
| `createdAt` | string (ISO 8601) | Creation timestamp |
| `size` | integer | Total size of all files in bytes |
| `totalFiles` | integer | Total number of files |

**Status Codes:**

| Code | Description |
|---|---|
| 200 | Success |
| 401 | Unauthorized |
| 404 | Bucket not found or no access |

---

### 3.3 List Buckets

List all buckets in a namespace. Returns paginated results.

| Field | Value |
|---|---|
| **Method** | `GET` |
| **URL** | `/api/buckets/{namespace}` |

**Path Parameters:**

| Parameter | Type | Required | Description |
|---|---|---|---|
| `namespace` | string | Yes | Username or organization name. Use `"me"` for the authenticated user. |

**Pagination:** Uses Link-header based pagination (same as other HF Hub list endpoints). Follow the `Link: <url>; rel="next"` header for subsequent pages.

**Response (200 OK):**

```json
[
  {
    "id": "username/my-bucket",
    "private": false,
    "createdAt": "2026-02-16T15:28:32.000Z",
    "size": 32,
    "totalFiles": 5
  },
  {
    "id": "username/checkpoints",
    "private": false,
    "createdAt": "2026-02-13T10:00:00.000Z",
    "size": 117609095,
    "totalFiles": 700
  }
]
```

Each item has the same schema as [Get Bucket Info](#32-get-bucket-info).

**Status Codes:**

| Code | Description |
|---|---|
| 200 | Success |
| 401 | Unauthorized |

---

### 3.4 Delete Bucket

Permanently delete a bucket and all its contents. **Irreversible.**

| Field | Value |
|---|---|
| **Method** | `DELETE` |
| **URL** | `/api/buckets/{namespace}/{bucket_name}` |

**Path Parameters:**

| Parameter | Type | Required | Description |
|---|---|---|---|
| `namespace` | string | Yes | Username or organization name. |
| `bucket_name` | string | Yes | Name of the bucket. |

**Response:** Empty body on success.

**Status Codes:**

| Code | Description |
|---|---|
| 200 | Bucket deleted successfully |
| 401 | Unauthorized |
| 403 | Forbidden — no write access |
| 404 | Bucket not found |

---

### 3.5 Move/Rename Bucket

Move or rename a bucket. This is a **shared endpoint** with the standard repos API.

| Field | Value |
|---|---|
| **Method** | `POST` |
| **URL** | `/api/repos/move` |
| **Content-Type** | `application/json` |

**Request Body (JSON):**

```json
{
  "fromRepo": "username/old-bucket-name",
  "toRepo": "username/new-bucket-name",
  "type": "bucket"
}
```

| Field | Type | Required | Description |
|---|---|---|---|
| `fromRepo` | string | Yes | Current bucket ID (`namespace/name`). |
| `toRepo` | string | Yes | New bucket ID (`namespace/name`). |
| `type` | string | Yes | Must be `"bucket"`. Enum: `["dataset", "model", "space", "bucket", "kernel"]` |

**Response:** Empty body on success.

**Status Codes:**

| Code | Description |
|---|---|
| 200 | Bucket moved successfully |
| 401 | Unauthorized |
| 403 | Forbidden |
| 404 | Source bucket not found |

---

## 4. Bucket File Endpoints

### 4.1 List Bucket Tree

List files and directories in a bucket, optionally filtered by prefix.

| Field | Value |
|---|---|
| **Method** | `GET` |
| **URL** | `/api/buckets/{namespace}/{bucket_name}/tree` or `/api/buckets/{namespace}/{bucket_name}/tree/{prefix}` |

**Path Parameters:**

| Parameter | Type | Required | Description |
|---|---|---|---|
| `namespace` | string | Yes | Username or organization name. |
| `bucket_name` | string | Yes | Name of the bucket. |
| `prefix` | string | No | URL-encoded path prefix to filter results. |

**Query Parameters:**

| Parameter | Type | Required | Description |
|---|---|---|---|
| `recursive` | boolean | No | If `true`, list all files recursively. If `false`, list top-level entries only (files + directories). Default behavior is recursive (flat file list). |

**Pagination:** Uses Link-header based pagination.

**Response (200 OK):**

```json
[
  {
    "type": "file",
    "path": "models/model.safetensors",
    "size": 2408828,
    "xetHash": "3ed0e9fefe788ddd61d1e26eba67057e9740a064b009256fbafadf6bb95785ca",
    "mtime": "2026-01-15T10:30:00.346Z",
    "uploadedAt": "2026-01-15T10:31:00.000Z"
  },
  {
    "type": "directory",
    "path": "sub",
    "uploadedAt": "2026-01-15T10:30:00.000Z"
  }
]
```

**File entry (`type: "file"`):**

| Field | Type | Description |
|---|---|---|
| `type` | string | Always `"file"` |
| `path` | string | Full path within the bucket |
| `size` | integer | File size in bytes |
| `xetHash` | string | Content-addressable Xet hash |
| `mtime` | string (ISO 8601) | File modification time (as set during upload) |
| `uploadedAt` | string (ISO 8601) \| null | Upload timestamp |

**Directory entry (`type: "directory"`):**

| Field | Type | Description |
|---|---|---|
| `type` | string | Always `"directory"` |
| `path` | string | Directory path |
| `uploadedAt` | string (ISO 8601) \| null | Latest upload timestamp of contents |

**Note:** When `recursive=true` (or the default), the API returns a flat file list. Directories are "virtual" and are inferred client-side by the Python client when `recursive=false`. When `recursive=false`, the API returns both file and directory entries at the top level.

**Status Codes:**

| Code | Description |
|---|---|
| 200 | Success |
| 401 | Unauthorized |
| 404 | Bucket not found |

---

### 4.2 Get Bucket Paths Info (Batch)

Fetch information about specific file paths in a single batch request. Only returns info for paths that exist (missing paths are silently ignored).

| Field | Value |
|---|---|
| **Method** | `POST` |
| **URL** | `/api/buckets/{namespace}/{bucket_name}/paths-info` |
| **Content-Type** | `application/json` |

**Path Parameters:**

| Parameter | Type | Required | Description |
|---|---|---|---|
| `namespace` | string | Yes | Username or organization name. |
| `bucket_name` | string | Yes | Name of the bucket. |

**Request Body (JSON):**

```json
{
  "paths": [
    "file.txt",
    "models/model.safetensors",
    "nonexistent.bin"
  ]
}
```

| Field | Type | Required | Description |
|---|---|---|---|
| `paths` | array of strings | Yes | List of file paths to query. Max recommended batch size: 1000. |

**Response (200 OK):**

```json
[
  {
    "type": "file",
    "path": "file.txt",
    "size": 2379,
    "xetHash": "96e637d9665bd35477b1908a23f2e254edfba0618dbd2d62f90a6baee7d139cf",
    "mtime": "2024-09-25T15:31:02.346Z"
  },
  {
    "type": "file",
    "path": "models/model.safetensors",
    "size": 2408828,
    "xetHash": "3ed0e9fefe788ddd61d1e26eba67057e9740a064b009256fbafadf6bb95785ca",
    "mtime": "2024-09-25T15:31:02.346Z"
  }
]
```

Response items have the same schema as file entries from [List Bucket Tree](#41-list-bucket-tree). Paths that do not exist are omitted from the response.

**Status Codes:**

| Code | Description |
|---|---|
| 200 | Success |
| 401 | Unauthorized |
| 404 | Bucket not found |

---

### 4.3 Batch Bucket Files (Add/Delete)

Add and/or delete files in a bucket in a single request. This is the primary endpoint for file mutations.

**Important:** This is a **non-transactional** operation. If an error occurs mid-batch, some files may have been written or deleted while others have not.

| Field | Value |
|---|---|
| **Method** | `POST` |
| **URL** | `/api/buckets/{namespace}/{bucket_name}/batch` |
| **Content-Type** | `application/x-ndjson` |

**Path Parameters:**

| Parameter | Type | Required | Description |
|---|---|---|---|
| `namespace` | string | Yes | Username or organization name. |
| `bucket_name` | string | Yes | Name of the bucket. |

**Request Body (NDJSON — newline-delimited JSON):**

Each line is a JSON object representing one operation. Two operation types:

**Add file operation:**

```json
{"type": "addFile", "path": "models/model.safetensors", "xetHash": "3ed0e9...", "mtime": 1706123456000}
{"type": "addFile", "path": "config.json", "xetHash": "abc123...", "mtime": 1706123456000, "contentType": "application/json"}
```

| Field | Type | Required | Description |
|---|---|---|---|
| `type` | string | Yes | Must be `"addFile"` |
| `path` | string | Yes | Destination path in the bucket |
| `xetHash` | string | Yes | Xet content hash (obtained after uploading to Xet storage) |
| `mtime` | integer | Yes | Modification time in milliseconds since epoch |
| `contentType` | string | No | MIME type of the file |

**Delete file operation:**

```json
{"type": "deleteFile", "path": "old-model.bin"}
```

| Field | Type | Required | Description |
|---|---|---|---|
| `type` | string | Yes | Must be `"deleteFile"` |
| `path` | string | Yes | Path of the file to delete |

**Full Upload Workflow:**

1. **Get Xet write token:** `GET /api/buckets/{ns}/{name}/xet-write-token` to obtain `casUrl` + `accessToken`
2. **Upload file content to Xet storage** using the `hf_xet` library (upload_files/upload_bytes) — this returns `xetHash` and file size
3. **Register files:** `POST /api/buckets/{ns}/{name}/batch` with the NDJSON payload referencing the `xetHash` values

**Response:** Empty body on success.

**Status Codes:**

| Code | Description |
|---|---|
| 200 | Batch operations completed |
| 401 | Unauthorized |
| 403 | Forbidden — no write access |
| 404 | Bucket not found |

---

### 4.4 Get Bucket File Metadata (HEAD)

Retrieve metadata for a single file without downloading its content. Returns file size and Xet storage information in response headers.

| Field | Value |
|---|---|
| **Method** | `HEAD` |
| **URL** | `/buckets/{namespace}/{bucket_name}/resolve/{path}` |

**Note:** This uses the **web URL** prefix (`/buckets/...`), not the API prefix (`/api/buckets/...`).

**Path Parameters:**

| Parameter | Type | Required | Description |
|---|---|---|---|
| `namespace` | string | Yes | Username or organization name. |
| `bucket_name` | string | Yes | Name of the bucket. |
| `path` | string | Yes | URL-encoded file path within the bucket. |

**Response Headers:**

| Header | Description |
|---|---|
| `Content-Length` | File size in bytes |
| `X-Xet-Cas-Url` | Xet CAS (Content Addressable Storage) URL |
| `X-Xet-Access-Token` | Xet access token for downloading |
| `X-Xet-Expiration` | Token expiration timestamp |
| `X-Xet-Hash` | Xet content hash |
| `X-Xet-Refresh-Route` | Route to refresh the Xet token |

**Status Codes:**

| Code | Description |
|---|---|
| 200 | Success (headers only) |
| 302 | Redirect (follow redirects) |
| 401 | Unauthorized |
| 404 | File or bucket not found |

---

### 4.5 Resolve/Download Bucket File (GET)

Download a file from a bucket. The response may redirect to a Xet-backed URL.

| Field | Value |
|---|---|
| **Method** | `GET` |
| **URL** | `/buckets/{namespace}/{bucket_name}/resolve/{path}` |

**Note:** Same URL as the HEAD endpoint but with GET method. This uses the **web URL** prefix (`/buckets/...`), not the API prefix (`/api/buckets/...`).

**Path Parameters:**

| Parameter | Type | Required | Description |
|---|---|---|---|
| `namespace` | string | Yes | Username or organization name. |
| `bucket_name` | string | Yes | Name of the bucket. |
| `path` | string | Yes | URL-encoded file path within the bucket. |

**Response:** File content (binary), potentially via redirect. Response headers include the same Xet metadata as the HEAD request.

**Efficient Download Workflow (using Xet):**

For large files, the Python client uses the Xet protocol for efficient downloads:
1. `HEAD /buckets/{ns}/{name}/resolve/{path}` — get Xet metadata
2. Use `hf_xet.download_files()` with the Xet hash and connection info

**Status Codes:**

| Code | Description |
|---|---|
| 200 | File content returned |
| 302 | Redirect to storage backend |
| 401 | Unauthorized |
| 404 | File or bucket not found |

---

## 5. Xet Storage Token Endpoints

These endpoints provide authentication tokens for the Xet content-addressable storage backend. They are documented in the OpenAPI spec under the `"buckets"` tag.

### 5.1 Get Xet Write Token

Get a temporary write token for uploading files to Xet storage.

| Field | Value |
|---|---|
| **Method** | `GET` |
| **URL** | `/api/buckets/{namespace}/{bucket_name}/xet-write-token` |

**Note:** In the OpenAPI spec, this is defined as `/api/{repoType}/{namespace}/{repo}/xet-write-token` with `repoType` constrained to `"buckets"`.

**Path Parameters:**

| Parameter | Type | Required | Description |
|---|---|---|---|
| `namespace` | string | Yes | Username or organization name. |
| `bucket_name` | string | Yes | Name of the bucket. |

**Response (200 OK):**

```json
{
  "casUrl": "https://xet-data.huggingface.co",
  "exp": 1706123456,
  "accessToken": "xet_xxxxxxxxxxxxxxx"
}
```

| Field | Type | Description |
|---|---|---|
| `casUrl` | string | Xet Content Addressable Storage endpoint URL |
| `exp` | number | Token expiration as Unix timestamp |
| `accessToken` | string | Xet access token for write operations |

**Status Codes:**

| Code | Description |
|---|---|
| 200 | Token returned |
| 401 | Unauthorized — invalid or missing HF token, or insufficient write permissions |
| 404 | Bucket not found |

---

### 5.2 Get Xet Read Token

Get a temporary read token for downloading files from Xet storage.

| Field | Value |
|---|---|
| **Method** | `GET` |
| **URL** | `/api/buckets/{namespace}/{bucket_name}/xet-read-token` |

**Path Parameters:** Same as [Get Xet Write Token](#51-get-xet-write-token).

**Response (200 OK):**

```json
{
  "casUrl": "https://xet-data.huggingface.co",
  "exp": 1706123456,
  "accessToken": "xet_xxxxxxxxxxxxxxx"
}
```

Same schema as the write token response.

**Status Codes:**

| Code | Description |
|---|---|
| 200 | Token returned |
| 401 | Unauthorized |
| 404 | Bucket not found |

---

## 6. Resource Group Endpoints

These are shared endpoints that work for models, datasets, spaces, AND buckets.

### 6.1 Set Resource Group

Assign a bucket to a resource group (Enterprise Hub only).

| Field | Value |
|---|---|
| **Method** | `POST` |
| **URL** | `/api/buckets/{namespace}/{bucket_name}/resource-group` |
| **Content-Type** | `application/json` |

**Path Parameters:**

| Parameter | Type | Required | Description |
|---|---|---|---|
| `namespace` | string | Yes | Organization name. |
| `bucket_name` | string | Yes | Name of the bucket. |

**Request Body (JSON):**

```json
{
  "resourceGroupId": "66670e5163145ca562cb1988"
}
```

| Field | Type | Required | Description |
|---|---|---|---|
| `resourceGroupId` | string (24-char hex) \| null | Yes | Resource group ID, or `null` to remove from resource group. |

**Status Codes:**

| Code | Description |
|---|---|
| 200 | Resource group updated |
| 401 | Unauthorized |
| 403 | Forbidden |
| 404 | Bucket not found |

---

### 6.2 Get Resource Group

Get the resource group assignment for a bucket.

| Field | Value |
|---|---|
| **Method** | `GET` |
| **URL** | `/api/buckets/{namespace}/{bucket_name}/resource-group` |

**Path Parameters:** Same as [Set Resource Group](#61-set-resource-group).

**Response (200 OK):**

```json
{
  "id": "66670e5163145ca562cb1988",
  "name": "ml-team",
  "numUsers": 5
}
```

| Field | Type | Description |
|---|---|---|
| `id` | string (24-char hex) | Resource group ID |
| `name` | string | Resource group name |
| `numUsers` | number | Number of users in the group (optional) |

**Status Codes:**

| Code | Description |
|---|---|
| 200 | Success |
| 401 | Unauthorized |
| 404 | Bucket not found or no resource group assigned |

---

## 7. Data Structures / Schemas

### BucketInfo

Returned by GET `/api/buckets/{namespace}/{name}` and list endpoints.

```json
{
  "id": "string",
  "private": "boolean",
  "createdAt": "string (ISO 8601)",
  "size": "integer (bytes)",
  "totalFiles": "integer"
}
```

### BucketFile

Returned in tree listing and paths-info responses.

```json
{
  "type": "file",
  "path": "string",
  "size": "integer (bytes)",
  "xetHash": "string (64-char hex)",
  "mtime": "string (ISO 8601) | null",
  "uploadedAt": "string (ISO 8601) | null"
}
```

### BucketFolder

Returned in tree listing (non-recursive mode).

```json
{
  "type": "directory",
  "path": "string",
  "uploadedAt": "string (ISO 8601) | null"
}
```

### BucketUrl

Returned by create bucket.

```json
{
  "url": "https://huggingface.co/buckets/{namespace}/{name}"
}
```

### XetTokenResponse

Returned by Xet token endpoints.

```json
{
  "casUrl": "string (URL)",
  "exp": "number (Unix timestamp)",
  "accessToken": "string"
}
```

---

## Complete Endpoint Summary Table

| # | Method | Path | Description | Auth |
|---|---|---|---|---|
| 1 | `POST` | `/api/buckets/{ns}/{name}` | Create bucket | Write |
| 2 | `GET` | `/api/buckets/{ns}/{name}` | Get bucket info | Read |
| 3 | `GET` | `/api/buckets/{ns}` | List buckets in namespace | Read |
| 4 | `DELETE` | `/api/buckets/{ns}/{name}` | Delete bucket | Write |
| 5 | `POST` | `/api/repos/move` | Move/rename bucket (`type: "bucket"`) | Write |
| 6 | `GET` | `/api/buckets/{ns}/{name}/tree(/{prefix})` | List files (paginated) | Read |
| 7 | `POST` | `/api/buckets/{ns}/{name}/paths-info` | Batch get file info | Read |
| 8 | `POST` | `/api/buckets/{ns}/{name}/batch` | Add/delete files (NDJSON) | Write |
| 9 | `HEAD` | `/buckets/{ns}/{name}/resolve/{path}` | Get file metadata | Read |
| 10 | `GET` | `/buckets/{ns}/{name}/resolve/{path}` | Download file | Read |
| 11 | `GET` | `/api/buckets/{ns}/{name}/xet-write-token` | Get Xet write token | Write |
| 12 | `GET` | `/api/buckets/{ns}/{name}/xet-read-token` | Get Xet read token | Read |
| 13 | `POST` | `/api/buckets/{ns}/{name}/resource-group` | Set resource group | Write |
| 14 | `GET` | `/api/buckets/{ns}/{name}/resource-group` | Get resource group | Read |

---

## Bucket-Specific Features & Limitations

1. **No git operations:** Buckets have no branches, tags, commits, revisions, or pull requests.
2. **No `/api/repos/create`:** Bucket creation uses its own dedicated `POST /api/buckets/{ns}/{name}` endpoint (not the shared `POST /api/repos/create`).
3. **Xet storage only:** All file content is stored via the Xet content-addressable storage backend. Uploads require a two-step process: upload content to Xet, then register the hash via the `/batch` endpoint.
4. **NDJSON batch API:** File mutations use newline-delimited JSON (not multipart form data or standard JSON arrays).
5. **Mutable storage:** Files can be overwritten in place. There is no versioning or history.
6. **Content deduplication:** The Xet backend deduplicates content across files using content-addressable hashing.
7. **Prefix-based listing:** The tree endpoint supports prefix-based filtering (similar to S3 key prefixes), not directory-based navigation.
8. **`hf://buckets/` protocol:** Buckets use `hf://buckets/` as their protocol prefix (vs `hf://datasets/`, `hf://spaces/`, etc.).
