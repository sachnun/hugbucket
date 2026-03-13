"""Bridge: orchestrates S3 operations via HF Hub + Xet CAS.

This is the core translation layer that maps high-level operations
(put object, get object) to the multi-step HF/Xet protocol.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import mimetypes
import time
from dataclasses import dataclass, field

from hugbucket.config import Config
from hugbucket.hub.client import HubClient, BucketInfo, BucketFile, XetConnectionInfo
from hugbucket.xet.cas_client import CASClient
from hugbucket.xet.chunker import Chunk, chunk_data
from hugbucket.xet.hasher import (
    chunk_hash,
    file_hash,
    hash_to_hex,
    verification_hash,
    xorb_hash,
)
from hugbucket.xet.xorb import (
    serialize_xorb,
    deserialize_xorb,
    XORB_MAX_BYTES,
    XorbChunkOffset,
)
from hugbucket.xet.shard import (
    FileInfo,
    FileDataTerm,
    XorbInfo,
    CASChunkInfo,
    build_shard,
)

logger = logging.getLogger(__name__)

# Max chunks per xorb (approx, to stay within 64 MiB serialized)
MAX_CHUNKS_PER_XORB = 1024


@dataclass
class _XorbBatch:
    """Pre-computed xorb ready for upload."""

    xorb_bytes: bytes
    xorb_hash_hex: str
    xorb_info: XorbInfo
    file_term: FileDataTerm
    verification_hash: bytes


@dataclass
class _PreparedUpload:
    """All CPU-bound results needed to complete an upload."""

    file_hash_hex: str
    xorb_batches: list[_XorbBatch]
    file_info: FileInfo
    etag: str


def _prepare_upload(data: bytes) -> _PreparedUpload:
    """CPU-bound upload preparation (chunking, hashing, compression).

    This runs in a thread to avoid blocking the async event loop.
    """
    # Step 1: CDC chunk
    chunks = chunk_data(data)
    logger.info(f"prepare_upload: {len(data)} bytes -> {len(chunks)} chunks")

    # Step 2: Hash all chunks
    c_hashes: list[bytes] = []
    c_sizes: list[int] = []
    for c in chunks:
        c_hashes.append(chunk_hash(c.data))
        c_sizes.append(len(c.data))

    # Step 3: Compute file hash
    f_hash = file_hash(c_hashes, c_sizes)
    f_hash_hex = hash_to_hex(f_hash)

    # Step 4: Group chunks into xorbs, serialize each
    xorb_batches: list[_XorbBatch] = []
    file_terms: list[FileDataTerm] = []
    term_verification_hashes: list[bytes] = []

    xorb_chunks: list[bytes] = []
    xorb_c_hashes: list[bytes] = []
    xorb_c_sizes: list[int] = []
    chunk_start_in_xorb = 0

    def _flush_xorb() -> None:
        nonlocal xorb_chunks, xorb_c_hashes, xorb_c_sizes
        nonlocal chunk_start_in_xorb

        if not xorb_chunks:
            return

        # Serialize (LZ4 compression)
        xorb_bytes, xorb_offsets = serialize_xorb(xorb_chunks)
        x_hash = xorb_hash(xorb_c_hashes, xorb_c_sizes)
        x_hash_hex = hash_to_hex(x_hash)

        # Build CAS info using cumulative uncompressed byte offsets
        cas_chunks: list[CASChunkInfo] = []
        uncompressed_offset = 0
        for i, (ch, cs) in enumerate(zip(xorb_c_hashes, xorb_c_sizes)):
            cas_chunks.append(
                CASChunkInfo(
                    chunk_hash=ch,
                    byte_range_start=uncompressed_offset,
                    unpacked_bytes=cs,
                )
            )
            uncompressed_offset += cs

        xi = XorbInfo(
            xorb_hash=x_hash,
            cas_flags=0,
            chunks=cas_chunks,
            total_bytes_in_xorb=sum(xorb_c_sizes),
            total_bytes_on_disk=len(xorb_bytes),
        )

        ft = FileDataTerm(
            xorb_hash=x_hash,
            cas_flags=0,
            unpacked_bytes=sum(xorb_c_sizes),
            chunk_start=chunk_start_in_xorb,
            chunk_end=chunk_start_in_xorb + len(xorb_chunks),
        )

        v_hash = verification_hash(xorb_c_hashes)

        xorb_batches.append(
            _XorbBatch(
                xorb_bytes=xorb_bytes,
                xorb_hash_hex=x_hash_hex,
                xorb_info=xi,
                file_term=ft,
                verification_hash=v_hash,
            )
        )

        file_terms.append(ft)
        term_verification_hashes.append(v_hash)

        # Reset
        xorb_chunks = []
        xorb_c_hashes = []
        xorb_c_sizes = []
        chunk_start_in_xorb = 0

    for i, c in enumerate(chunks):
        xorb_chunks.append(c.data)
        xorb_c_hashes.append(c_hashes[i])
        xorb_c_sizes.append(c_sizes[i])

        if (
            len(xorb_chunks) >= MAX_CHUNKS_PER_XORB
            or sum(len(d) for d in xorb_chunks) >= XORB_MAX_BYTES // 2
        ):
            _flush_xorb()
            chunk_start_in_xorb = 0

    _flush_xorb()

    # Step 5: Build shard
    fi = FileInfo(
        file_hash=f_hash,
        terms=file_terms,
        verification_hashes=term_verification_hashes,
    )
    # NOTE: shard_bytes is built later after uploads, but since build_shard
    # is also CPU-bound, we do it here too.

    # Step 6: MD5 for ETag
    etag = hashlib.md5(data).hexdigest()

    return _PreparedUpload(
        file_hash_hex=f_hash_hex,
        xorb_batches=xorb_batches,
        file_info=fi,
        etag=etag,
    )


@dataclass
class Bridge:
    """Orchestrates S3 <-> HF Bucket operations."""

    config: Config
    hub: HubClient = field(init=False)
    cas: CASClient = field(init=False)

    def __post_init__(self) -> None:
        self.hub = HubClient(config=self.config)
        self.cas = CASClient()

    async def close(self) -> None:
        await self.hub.close()
        await self.cas.close()

    def _bucket_id(self, bucket_name: str) -> str:
        """Convert S3 bucket name to HF bucket_id (namespace/name)."""
        if "/" in bucket_name:
            return bucket_name
        return f"{self.config.hf_namespace}/{bucket_name}"

    # ---- Bucket operations ----

    async def list_buckets(self) -> list[BucketInfo]:
        return await self.hub.list_buckets()

    async def create_bucket(self, name: str, private: bool = False) -> str:
        return await self.hub.create_bucket(name, private=private)

    async def delete_bucket(self, name: str) -> None:
        await self.hub.delete_bucket(self._bucket_id(name))

    async def head_bucket(self, name: str) -> BucketInfo | None:
        try:
            return await self.hub.get_bucket_info(self._bucket_id(name))
        except Exception:
            return None

    # ---- Object operations ----

    async def put_object(
        self,
        bucket: str,
        key: str,
        data: bytes,
    ) -> dict:
        """Upload an object. Full Xet protocol:
        1. CDC chunk the data
        2. Hash all chunks
        3. Build xorbs from chunks
        4. Upload xorbs to CAS
        5. Build + upload shard
        6. Register file via Hub batch API

        CPU-bound work (chunking, hashing, compression, MD5) is offloaded
        to a thread so the event loop stays responsive during uploads.
        """
        bucket_id = self._bucket_id(bucket)

        # Handle empty files
        if len(data) == 0:
            return await self._put_empty_file(bucket_id, key)

        # Run all CPU-bound work in a thread (chunking, hashing,
        # LZ4 compression, shard building, MD5)
        prepared = await asyncio.to_thread(_prepare_upload, data)
        logger.info(
            f"PUT {key}: {len(data)} bytes -> {len(prepared.xorb_batches)} xorb(s)"
        )

        # Get write token (network I/O, stays on event loop)
        conn = await self.hub.get_xet_write_token(bucket_id)

        # Upload xorbs to CAS (network I/O)
        for batch in prepared.xorb_batches:
            await self.cas.upload_xorb(conn, batch.xorb_hash_hex, batch.xorb_bytes)

        # Build shard (CPU-bound, offload to thread)
        xorb_infos = [b.xorb_info for b in prepared.xorb_batches]
        shard_bytes = await asyncio.to_thread(
            build_shard, [prepared.file_info], xorb_infos
        )
        await self.cas.upload_shard(conn, shard_bytes)

        # Register file with Hub (network I/O)
        content_type = mimetypes.guess_type(key)[0] or "application/octet-stream"
        mtime_ms = int(time.time() * 1000)

        await self.hub.batch_files(
            bucket_id,
            add=[
                {
                    "path": key,
                    "xetHash": prepared.file_hash_hex,
                    "mtime": mtime_ms,
                    "contentType": content_type,
                }
            ],
        )

        return {"ETag": f'"{prepared.etag}"', "size": len(data)}

    async def _put_empty_file(self, bucket_id: str, key: str) -> dict:
        """Handle zero-byte file (no Xet upload needed)."""
        # Empty file still needs a file hash
        c_hash = chunk_hash(b"")
        f_hash = file_hash([c_hash], [0])
        f_hash_hex = hash_to_hex(f_hash)

        content_type = mimetypes.guess_type(key)[0] or "application/octet-stream"
        mtime_ms = int(time.time() * 1000)

        await self.hub.batch_files(
            bucket_id,
            add=[
                {
                    "path": key,
                    "xetHash": f_hash_hex,
                    "mtime": mtime_ms,
                    "contentType": content_type,
                }
            ],
        )
        etag = hashlib.md5(b"").hexdigest()
        return {"ETag": f'"{etag}"', "size": 0}

    async def get_object(
        self,
        bucket: str,
        key: str,
    ) -> bytes | None:
        """Download an object. Full Xet protocol:
        1. Get file metadata (xetHash, size)
        2. Get read token
        3. Get reconstruction from CAS
        4. Fetch xorb ranges from CDN
        5. Decompress + reassemble
        """
        bucket_id = self._bucket_id(bucket)

        # Step 1: Get file info
        files = await self.hub.get_paths_info(bucket_id, [key])
        if not files:
            return None

        file_info = files[0]
        if file_info.size == 0:
            return b""

        # Step 2: Get read token
        conn = await self.hub.get_xet_read_token(bucket_id)

        # Step 3: Get reconstruction
        recon = await self.cas.get_reconstruction(conn, file_info.xet_hash)

        # Step 4+5: Fetch and reassemble
        result_parts: list[bytes] = []
        first_term = True

        for term in recon.terms:
            # Find fetch info for this xorb
            fetches = recon.fetch_info.get(term.hash, [])

            for fetch in fetches:
                # Check if this fetch covers our term's chunk range
                if (
                    fetch.range_start > term.range_end
                    or fetch.range_end < term.range_start
                ):
                    continue

                # Fetch xorb bytes
                xorb_bytes = await self.cas.fetch_xorb_range(fetch)

                # Deserialize xorb
                xorb_chunks = deserialize_xorb(xorb_bytes)

                # Extract chunks for this term
                for ci in range(term.range_start, term.range_end):
                    local_idx = ci - fetch.range_start
                    if 0 <= local_idx < len(xorb_chunks):
                        chunk_bytes = xorb_chunks[local_idx].uncompressed_data

                        # Handle offset for first term
                        if first_term and recon.offset_into_first_range > 0:
                            chunk_bytes = chunk_bytes[recon.offset_into_first_range :]
                            first_term = False
                        else:
                            first_term = False

                        result_parts.append(chunk_bytes)
                break  # Only need one fetch per term

        return b"".join(result_parts)

    async def delete_object(self, bucket: str, key: str) -> None:
        """Delete an object."""
        bucket_id = self._bucket_id(bucket)
        await self.hub.batch_files(bucket_id, delete=[key])

    async def delete_objects(
        self, bucket: str, keys: list[str]
    ) -> tuple[list[str], list[dict]]:
        """Delete multiple objects in a single batch call.

        Returns (deleted_keys, errors) where errors is a list of
        {"key": ..., "code": ..., "message": ...} dicts.
        """
        bucket_id = self._bucket_id(bucket)
        deleted: list[str] = []
        errors: list[dict] = []
        try:
            await self.hub.batch_files(bucket_id, delete=keys)
            deleted = list(keys)
        except Exception as exc:
            logger.exception("delete_objects batch failed")
            # Report every key as failed so the caller can build a proper
            # DeleteResult response.
            for key in keys:
                errors.append(
                    {"key": key, "code": "InternalError", "message": str(exc)}
                )
        return deleted, errors

    async def head_object(self, bucket: str, key: str) -> BucketFile | None:
        """Get object metadata."""
        bucket_id = self._bucket_id(bucket)
        files = await self.hub.get_paths_info(bucket_id, [key])
        return files[0] if files else None

    async def list_objects(
        self,
        bucket: str,
        prefix: str = "",
        delimiter: str = "",
        max_keys: int = 1000,
        continuation_token: str = "",
    ) -> dict:
        """List objects with S3-style prefix/delimiter support.

        Returns dict with keys:
            contents: list of file objects
            common_prefixes: list of prefix strings (when delimiter is used)
            is_truncated: bool
            next_continuation_token: str or None
        """
        bucket_id = self._bucket_id(bucket)

        # Get all files (recursive for prefix filtering)
        all_files = await self.hub.list_bucket_tree(
            bucket_id, prefix=prefix, recursive=True
        )

        # Filter by prefix (Hub should already do this, but be safe)
        filtered = [f for f in all_files if f.path.startswith(prefix)]

        contents: list[BucketFile] = []
        common_prefixes: set[str] = set()

        if delimiter:
            for f in filtered:
                # Get the part after the prefix
                rest = f.path[len(prefix) :]
                delim_pos = rest.find(delimiter)
                if delim_pos >= 0:
                    # This is a "directory" — add as common prefix
                    cp = prefix + rest[: delim_pos + len(delimiter)]
                    common_prefixes.add(cp)
                else:
                    if f.type == "file":
                        contents.append(f)
        else:
            contents = [f for f in filtered if f.type == "file"]

        # Sort by key
        contents.sort(key=lambda f: f.path)
        sorted_prefixes = sorted(common_prefixes)

        # Pagination
        start_idx = 0
        if continuation_token:
            for i, c in enumerate(contents):
                if c.path > continuation_token:
                    start_idx = i
                    break

        truncated = len(contents) > start_idx + max_keys
        page = contents[start_idx : start_idx + max_keys]
        next_token = page[-1].path if truncated and page else None

        return {
            "contents": page,
            "common_prefixes": sorted_prefixes,
            "is_truncated": truncated,
            "next_continuation_token": next_token,
        }
