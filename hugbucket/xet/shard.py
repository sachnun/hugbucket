"""Xet Shard binary format builder.

Shards carry file reconstruction metadata + xorb metadata for the CAS.

Structure (complete, with footer and lookup tables):
    [Header 48B]
    [File Info Sections... + File Bookend 48B]
    [Xorb Info Sections... + Xorb Bookend 48B]
    [File Lookup Table]
    [Xorb Lookup Table]
    [Chunk Lookup Table]
    [Footer 200B]

All integers are little-endian. All entries are 48 bytes.
"""

from __future__ import annotations

import struct
import time
from dataclasses import dataclass

# Correct magic header tag (32 bytes) from xet-core
# ASCII prefix: "HFRepoMetaData\x00" + 17 magic bytes
MDB_SHARD_HEADER_TAG = bytes(
    [
        0x48,
        0x46,
        0x52,
        0x65,
        0x70,
        0x6F,
        0x4D,
        0x65,
        0x74,
        0x61,
        0x44,
        0x61,
        0x74,
        0x61,
        0x00,
        0x55,
        0x69,
        0x67,
        0x45,
        0x6A,
        0x7B,
        0x81,
        0x57,
        0x83,
        0xA5,
        0xBD,
        0xD9,
        0x5C,
        0xCD,
        0xD1,
        0x4A,
        0xA9,
    ]
)

SHARD_HEADER_VERSION = 2
SHARD_FOOTER_VERSION = 1
ENTRY_SIZE = 48
FOOTER_SIZE = 200

# Bookend marker: 32 bytes of 0xFF + 16 bytes of 0x00
BOOKEND_HASH = b"\xff" * 32
BOOKEND_PAD = b"\x00" * 16

# File flags
FILE_FLAG_VERIFICATION = 0x80000000
FILE_FLAG_METADATA_EXT = 0x40000000


@dataclass
class FileDataTerm:
    """A term in the file reconstruction: a range of chunks in a xorb."""

    xorb_hash: bytes  # 32 bytes
    cas_flags: int  # u32
    unpacked_bytes: int  # u32
    chunk_start: int  # u32
    chunk_end: int  # u32


@dataclass
class CASChunkInfo:
    """Metadata for a single chunk within a xorb."""

    chunk_hash: bytes  # 32 bytes
    byte_range_start: int  # u32 — offset in serialized xorb
    unpacked_bytes: int  # u32


@dataclass
class XorbInfo:
    """Metadata for a xorb."""

    xorb_hash: bytes  # 32 bytes
    cas_flags: int  # u32
    chunks: list[CASChunkInfo]
    total_bytes_in_xorb: int  # total uncompressed size
    total_bytes_on_disk: int  # total serialized xorb size


@dataclass
class FileInfo:
    """File reconstruction info."""

    file_hash: bytes  # 32 bytes
    terms: list[FileDataTerm]
    verification_hashes: list[bytes]  # one per term, 32 bytes each
    sha256: bytes | None = None


def _truncate_hash(h: bytes) -> int:
    """Get truncated hash for lookup table: first 8 bytes as LE u64."""
    return struct.unpack_from("<Q", h, 0)[0]


def build_shard(
    files: list[FileInfo],
    xorbs: list[XorbInfo],
) -> bytes:
    """Build a complete shard binary blob with footer and lookup tables."""
    buf = bytearray()

    # --- Header (48 bytes) ---
    buf.extend(MDB_SHARD_HEADER_TAG)  # 32B magic
    buf.extend(struct.pack("<Q", SHARD_HEADER_VERSION))  # 8B version
    buf.extend(struct.pack("<Q", FOOTER_SIZE))  # 8B footer_size

    file_info_offset = len(buf)

    # --- File Info Sections ---
    # Track entries for lookup table: (truncated_hash, entry_index)
    file_lookups: list[tuple[int, int]] = []
    entry_idx = 0  # counts 48-byte entries from start of file info section

    for fi in files:
        num_entries = len(fi.terms)
        has_verification = len(fi.verification_hashes) == num_entries
        has_sha256 = fi.sha256 is not None

        file_flags = 0
        if has_verification:
            file_flags |= FILE_FLAG_VERIFICATION
        if has_sha256:
            file_flags |= FILE_FLAG_METADATA_EXT

        # Add to lookup table
        file_lookups.append((_truncate_hash(fi.file_hash), entry_idx))

        # FileDataSequenceHeader (48B)
        header = bytearray(48)
        header[0:32] = fi.file_hash
        struct.pack_into("<I", header, 32, file_flags)
        struct.pack_into("<I", header, 36, num_entries)
        # bytes 40-47: unused (zeros)
        buf.extend(header)
        entry_idx += 1

        # FileDataSequenceEntry × N (48B each)
        for term in fi.terms:
            entry = bytearray(48)
            entry[0:32] = term.xorb_hash
            struct.pack_into("<I", entry, 32, term.cas_flags)
            struct.pack_into("<I", entry, 36, term.unpacked_bytes)
            struct.pack_into("<I", entry, 40, term.chunk_start)
            struct.pack_into("<I", entry, 44, term.chunk_end)
            buf.extend(entry)
            entry_idx += 1

        # FileVerificationEntry × N (48B each)
        if has_verification:
            for vh in fi.verification_hashes:
                entry = bytearray(48)
                entry[0:32] = vh
                # bytes 32-47: unused (zeros)
                buf.extend(entry)
                entry_idx += 1

        # FileMetadataExt (48B)
        if has_sha256 and fi.sha256 is not None:
            entry = bytearray(48)
            entry[0:32] = fi.sha256
            buf.extend(entry)
            entry_idx += 1

    # File Bookend (48B)
    bookend = bytearray(48)
    bookend[0:32] = BOOKEND_HASH
    buf.extend(bookend)

    xorb_info_offset = len(buf)

    # --- Xorb Info Sections ---
    xorb_lookups: list[tuple[int, int]] = []
    chunk_lookups: list[
        tuple[int, int, int]
    ] = []  # (truncated_hash, xorb_entry_idx, chunk_offset)
    xorb_entry_idx = 0

    for xi in xorbs:
        num_chunks = len(xi.chunks)

        # Add to xorb lookup
        xorb_lookups.append((_truncate_hash(xi.xorb_hash), xorb_entry_idx))

        # XorbChunkSequenceHeader (48B)
        header = bytearray(48)
        header[0:32] = xi.xorb_hash
        struct.pack_into("<I", header, 32, xi.cas_flags)
        struct.pack_into("<I", header, 36, num_chunks)
        struct.pack_into("<I", header, 40, xi.total_bytes_in_xorb)
        struct.pack_into("<I", header, 44, xi.total_bytes_on_disk)
        buf.extend(header)
        xorb_entry_idx += 1

        # XorbChunkSequenceEntry × N (48B each)
        for ci_idx, ci in enumerate(xi.chunks):
            # Add to chunk lookup
            chunk_lookups.append(
                (
                    _truncate_hash(ci.chunk_hash),
                    xorb_entry_idx - 1,  # index of the xorb header
                    ci_idx,
                )
            )

            entry = bytearray(48)
            entry[0:32] = ci.chunk_hash
            struct.pack_into("<I", entry, 32, ci.byte_range_start)
            struct.pack_into("<I", entry, 36, ci.unpacked_bytes)
            struct.pack_into("<I", entry, 40, 0)  # flags
            struct.pack_into("<I", entry, 44, 0)  # unused
            buf.extend(entry)
            xorb_entry_idx += 1

    # Xorb Bookend (48B)
    bookend = bytearray(48)
    bookend[0:32] = BOOKEND_HASH
    buf.extend(bookend)

    # --- Lookup Tables ---
    # File lookup table (sorted by truncated hash)
    file_lookup_offset = len(buf)
    file_lookups.sort(key=lambda x: x[0])
    for trunc, idx in file_lookups:
        buf.extend(struct.pack("<Q", trunc))
        buf.extend(struct.pack("<I", idx))

    # Xorb lookup table (sorted by truncated hash)
    xorb_lookup_offset = len(buf)
    xorb_lookups.sort(key=lambda x: x[0])
    for trunc, idx in xorb_lookups:
        buf.extend(struct.pack("<Q", trunc))
        buf.extend(struct.pack("<I", idx))

    # Chunk lookup table (sorted by truncated hash)
    chunk_lookup_offset = len(buf)
    chunk_lookups.sort(key=lambda x: x[0])
    for trunc, xorb_idx, chunk_off in chunk_lookups:
        buf.extend(struct.pack("<Q", trunc))
        buf.extend(struct.pack("<I", xorb_idx))
        buf.extend(struct.pack("<I", chunk_off))

    # --- Footer (200 bytes) ---
    footer_offset = len(buf)

    # Compute some stats
    total_stored = sum(xi.total_bytes_on_disk for xi in xorbs)
    total_materialized = sum(sum(ci.unpacked_bytes for ci in xi.chunks) for xi in xorbs)
    total_stored_bytes = total_materialized

    footer = bytearray(FOOTER_SIZE)
    off = 0
    struct.pack_into("<Q", footer, off, SHARD_FOOTER_VERSION)
    off += 8
    struct.pack_into("<Q", footer, off, file_info_offset)
    off += 8
    struct.pack_into("<Q", footer, off, xorb_info_offset)
    off += 8
    struct.pack_into("<Q", footer, off, file_lookup_offset)
    off += 8
    struct.pack_into("<Q", footer, off, len(file_lookups))
    off += 8
    struct.pack_into("<Q", footer, off, xorb_lookup_offset)
    off += 8
    struct.pack_into("<Q", footer, off, len(xorb_lookups))
    off += 8
    struct.pack_into("<Q", footer, off, chunk_lookup_offset)
    off += 8
    struct.pack_into("<Q", footer, off, len(chunk_lookups))
    off += 8
    # chunk_hash_hmac_key (32 bytes of zeros = no HMAC)
    off += 32
    struct.pack_into("<Q", footer, off, int(time.time()))
    off += 8  # creation_timestamp
    struct.pack_into("<Q", footer, off, 0xFFFFFFFFFFFFFFFF)
    off += 8  # key_expiry (no expiry)
    # _buffer (48 bytes of zeros)
    off += 48
    struct.pack_into("<Q", footer, off, total_stored)
    off += 8  # stored_bytes_on_disk
    struct.pack_into("<Q", footer, off, total_materialized)
    off += 8  # materialized_bytes
    struct.pack_into("<Q", footer, off, total_stored_bytes)
    off += 8  # stored_bytes
    struct.pack_into("<Q", footer, off, footer_offset)
    off += 8  # footer_offset

    buf.extend(footer)

    return bytes(buf)
