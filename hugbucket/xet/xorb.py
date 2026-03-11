"""Xorb binary format: serialization and deserialization.

A Xorb is a container of compressed chunks:
    [ChunkHeader 8B][CompressedData][ChunkHeader 8B][CompressedData]...

ChunkHeader (8 bytes):
    - version: 1 byte (currently 0)
    - compressed_size: 3 bytes (LE u24)
    - compression_type: 1 byte (0=None, 1=LZ4, 2=ByteGrouping4+LZ4)
    - uncompressed_size: 3 bytes (LE u24)
"""

from __future__ import annotations

import struct
from dataclasses import dataclass
from enum import IntEnum

import lz4.block


class CompressionType(IntEnum):
    NONE = 0
    LZ4 = 1
    BYTE_GROUPING4_LZ4 = 2


XORB_VERSION = 0
XORB_MAX_BYTES = 64 * 1024 * 1024  # 64 MiB
HEADER_SIZE = 8


@dataclass
class ChunkEntry:
    """A chunk within a xorb."""

    uncompressed_data: bytes
    uncompressed_size: int


def _pack_u24(val: int) -> bytes:
    """Pack an integer as 3 bytes little-endian."""
    return struct.pack("<I", val)[:3]


def _unpack_u24(data: bytes) -> int:
    """Unpack 3 bytes little-endian to integer."""
    return struct.unpack("<I", data + b"\x00")[0]


def _byte_group4_encode(data: bytes) -> bytes:
    """ByteGrouping4 encoding: reorder bytes by position within 4-byte groups.

    [A1,A2,A3,A4, B1,B2,B3,B4, ...] ->
    [A1,B1,..., A2,B2,..., A3,B3,..., A4,B4,...]
    """
    n = len(data)
    full_groups = n // 4
    remainder = n % 4

    result = bytearray(n)
    group_size = full_groups + (1 if remainder > 0 else 0)

    for g in range(full_groups):
        for b in range(4):
            result[b * group_size + g] = data[g * 4 + b]

    # Handle remaining bytes (< 4)
    for r in range(remainder):
        result[r * group_size + full_groups] = data[full_groups * 4 + r]

    return bytes(result)


def _byte_group4_decode(data: bytes, original_size: int) -> bytes:
    """Reverse of ByteGrouping4 encoding."""
    n = original_size
    full_groups = n // 4
    remainder = n % 4

    result = bytearray(n)
    group_size = full_groups + (1 if remainder > 0 else 0)

    for g in range(full_groups):
        for b in range(4):
            result[g * 4 + b] = data[b * group_size + g]

    for r in range(remainder):
        result[full_groups * 4 + r] = data[r * group_size + full_groups]

    return bytes(result)


def _compress_chunk(data: bytes) -> tuple[bytes, CompressionType]:
    """Compress a chunk, trying LZ4 first. Returns (compressed_data, type).

    Strategy: try LZ4 first. If it doesn't shrink, store uncompressed.
    We skip ByteGrouping4+LZ4 for simplicity (it's an optimization for
    structured numeric data).
    """
    compressed = lz4.block.compress(data, store_size=False)
    if len(compressed) < len(data):
        return compressed, CompressionType.LZ4
    return data, CompressionType.NONE


def _decompress_chunk(
    data: bytes,
    compression: CompressionType,
    uncompressed_size: int,
) -> bytes:
    """Decompress a chunk based on compression type."""
    if compression == CompressionType.NONE:
        return data
    elif compression == CompressionType.LZ4:
        return lz4.block.decompress(data, uncompressed_size=uncompressed_size)
    elif compression == CompressionType.BYTE_GROUPING4_LZ4:
        decompressed = lz4.block.decompress(data, uncompressed_size=uncompressed_size)
        return _byte_group4_decode(decompressed, uncompressed_size)
    else:
        raise ValueError(f"Unknown compression type: {compression}")


@dataclass
class XorbChunkOffset:
    """Byte offset of a chunk within the serialized xorb."""

    byte_offset: int  # offset from start of xorb
    compressed_size: int  # size of compressed data (excluding header)


def serialize_xorb(
    chunks: list[bytes],
) -> tuple[bytes, list[XorbChunkOffset]]:
    """Serialize a list of chunk data into xorb binary format.

    Returns (serialized_xorb_bytes, list_of_chunk_offsets).
    """
    parts: list[bytes] = []
    offsets: list[XorbChunkOffset] = []
    current_offset = 0

    for chunk_data in chunks:
        uncompressed_size = len(chunk_data)
        compressed, comp_type = _compress_chunk(chunk_data)
        compressed_size = len(compressed)

        # Record offset of this chunk (points to the header)
        offsets.append(
            XorbChunkOffset(byte_offset=current_offset, compressed_size=compressed_size)
        )

        # Build 8-byte header
        header = bytearray(HEADER_SIZE)
        header[0] = XORB_VERSION
        header[1:4] = _pack_u24(compressed_size)
        header[4] = int(comp_type)
        header[5:8] = _pack_u24(uncompressed_size)

        parts.append(bytes(header))
        parts.append(compressed)
        current_offset += HEADER_SIZE + compressed_size

    result = b"".join(parts)
    if len(result) > XORB_MAX_BYTES:
        raise ValueError(f"Xorb exceeds max size: {len(result)} > {XORB_MAX_BYTES}")
    return result, offsets


def deserialize_xorb(data: bytes) -> list[ChunkEntry]:
    """Deserialize xorb binary data into a list of chunks."""
    chunks: list[ChunkEntry] = []
    offset = 0
    total = len(data)

    while offset < total:
        if offset + HEADER_SIZE > total:
            raise ValueError(f"Truncated xorb header at offset {offset}")

        version = data[offset]
        if version != XORB_VERSION:
            raise ValueError(f"Unknown xorb version: {version}")

        compressed_size = _unpack_u24(data[offset + 1 : offset + 4])
        comp_type = CompressionType(data[offset + 4])
        uncompressed_size = _unpack_u24(data[offset + 5 : offset + 8])

        offset += HEADER_SIZE

        if offset + compressed_size > total:
            raise ValueError(
                f"Truncated xorb chunk data at offset {offset}, "
                f"need {compressed_size} bytes but only {total - offset} remain"
            )

        compressed_data = data[offset : offset + compressed_size]
        offset += compressed_size

        uncompressed = _decompress_chunk(compressed_data, comp_type, uncompressed_size)
        chunks.append(
            ChunkEntry(
                uncompressed_data=uncompressed,
                uncompressed_size=uncompressed_size,
            )
        )

    return chunks
