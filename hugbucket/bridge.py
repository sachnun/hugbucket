"""Bridge: orchestrates S3 operations via HF Hub + Xet CAS.

This is the core translation layer that maps high-level operations
(put object, get object) to the multi-step HF/Xet protocol.
"""

from __future__ import annotations

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
        """
        bucket_id = self._bucket_id(bucket)

        # Handle empty files
        if len(data) == 0:
            return await self._put_empty_file(bucket_id, key)

        # Step 1: CDC chunk
        chunks = chunk_data(data)
        logger.info(f"PUT {key}: {len(data)} bytes -> {len(chunks)} chunks")

        # Step 2: Hash all chunks
        c_hashes: list[bytes] = []
        c_sizes: list[int] = []
        for c in chunks:
            c_hashes.append(chunk_hash(c.data))
            c_sizes.append(len(c.data))

        # Step 3: Compute file hash
        f_hash = file_hash(c_hashes, c_sizes)
        f_hash_hex = hash_to_hex(f_hash)

        # Step 4: Get write token
        conn = await self.hub.get_xet_write_token(bucket_id)

        # Step 5: Group chunks into xorbs and upload
        xorb_infos: list[XorbInfo] = []
        file_terms: list[FileDataTerm] = []
        term_verification_hashes: list[bytes] = []

        xorb_chunks: list[bytes] = []  # raw chunk data
        xorb_c_hashes: list[bytes] = []  # chunk hashes for current xorb
        xorb_c_sizes: list[int] = []  # chunk sizes for current xorb
        chunk_start_in_xorb = 0

        async def _flush_xorb() -> None:
            nonlocal xorb_chunks, xorb_c_hashes, xorb_c_sizes
            nonlocal chunk_start_in_xorb

            if not xorb_chunks:
                return

            # Serialize
            xorb_bytes, xorb_offsets = serialize_xorb(xorb_chunks)
            x_hash = xorb_hash(xorb_c_hashes, xorb_c_sizes)
            x_hash_hex = hash_to_hex(x_hash)

            # Upload to CAS
            await self.cas.upload_xorb(conn, x_hash_hex, xorb_bytes)

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

            xorb_infos.append(
                XorbInfo(
                    xorb_hash=x_hash,
                    cas_flags=0,
                    chunks=cas_chunks,
                    total_bytes_in_xorb=sum(xorb_c_sizes),
                    total_bytes_on_disk=len(xorb_bytes),
                )
            )

            # Build file term
            unpacked = sum(xorb_c_sizes)
            file_terms.append(
                FileDataTerm(
                    xorb_hash=x_hash,
                    cas_flags=0,
                    unpacked_bytes=unpacked,
                    chunk_start=chunk_start_in_xorb,
                    chunk_end=chunk_start_in_xorb + len(xorb_chunks),
                )
            )

            # Verification hash for this term
            v_hash = verification_hash(xorb_c_hashes)
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

            # Flush if too many chunks or too much data
            if (
                len(xorb_chunks) >= MAX_CHUNKS_PER_XORB
                or sum(len(d) for d in xorb_chunks) >= XORB_MAX_BYTES // 2
            ):
                await _flush_xorb()
                chunk_start_in_xorb = 0

        # Flush remaining
        await _flush_xorb()

        # Step 6: Build and upload shard
        fi = FileInfo(
            file_hash=f_hash,
            terms=file_terms,
            verification_hashes=term_verification_hashes,
        )
        shard_bytes = build_shard([fi], xorb_infos)
        await self.cas.upload_shard(conn, shard_bytes)

        # Step 7: Register file with Hub
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

        # Compute ETag (MD5 of content, like S3)
        etag = hashlib.md5(data).hexdigest()
        return {"ETag": f'"{etag}"', "size": len(data)}

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
