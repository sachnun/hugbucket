"""Tests for hugbucket.xet.xorb — Xorb binary format."""

from __future__ import annotations

import os

from hugbucket.xet.xorb import (
    serialize_xorb,
    deserialize_xorb,
    _pack_u24,
    _unpack_u24,
    _byte_group4_encode,
    _byte_group4_decode,
    _compress_chunk,
    _decompress_chunk,
    CompressionType,
    XORB_VERSION,
    HEADER_SIZE,
    XORB_MAX_BYTES,
    ChunkEntry,
    XorbChunkOffset,
)


class TestU24:
    """u24 packing/unpacking."""

    def test_round_trip_zero(self) -> None:
        assert _unpack_u24(_pack_u24(0)) == 0

    def test_round_trip_small(self) -> None:
        assert _unpack_u24(_pack_u24(42)) == 42

    def test_round_trip_max(self) -> None:
        # Max u24 = 2^24 - 1 = 16777215
        assert _unpack_u24(_pack_u24(0xFFFFFF)) == 0xFFFFFF

    def test_pack_returns_3_bytes(self) -> None:
        assert len(_pack_u24(1000)) == 3

    def test_le_encoding(self) -> None:
        packed = _pack_u24(0x010203)
        assert packed == bytes([0x03, 0x02, 0x01])


class TestByteGrouping4:
    """ByteGrouping4 encode/decode round-trip."""

    def test_round_trip_aligned(self) -> None:
        """Data length divisible by 4."""
        data = bytes(range(16))
        encoded = _byte_group4_encode(data)
        decoded = _byte_group4_decode(encoded, len(data))
        assert decoded == data

    def test_round_trip_unaligned(self) -> None:
        """Data length NOT divisible by 4."""
        data = bytes(range(13))
        encoded = _byte_group4_encode(data)
        decoded = _byte_group4_decode(encoded, len(data))
        assert decoded == data

    def test_round_trip_random(self) -> None:
        for size in [0, 1, 3, 4, 7, 100, 1023, 4096]:
            data = os.urandom(size)
            encoded = _byte_group4_encode(data)
            decoded = _byte_group4_decode(encoded, len(data))
            assert decoded == data, f"Failed for size {size}"

    def test_encoded_same_length(self) -> None:
        """Encoding is a permutation — same length."""
        data = os.urandom(100)
        assert len(_byte_group4_encode(data)) == len(data)


class TestCompression:
    """Chunk compression/decompression."""

    def test_compressible_data_uses_lz4(self) -> None:
        data = b"\x00" * 10000  # highly compressible
        compressed, comp_type = _compress_chunk(data)
        assert comp_type == CompressionType.LZ4
        assert len(compressed) < len(data)

    def test_incompressible_data_stores_raw(self) -> None:
        data = os.urandom(100)  # random data — hard to compress
        compressed, comp_type = _compress_chunk(data)
        # Might be NONE or LZ4 depending on luck, but should round-trip
        decompressed = _decompress_chunk(compressed, comp_type, len(data))
        assert decompressed == data

    def test_decompress_none(self) -> None:
        data = b"hello"
        assert _decompress_chunk(data, CompressionType.NONE, len(data)) == data

    def test_round_trip_lz4(self) -> None:
        import lz4.block

        data = b"A" * 5000
        compressed = lz4.block.compress(data, store_size=False)
        decompressed = _decompress_chunk(compressed, CompressionType.LZ4, len(data))
        assert decompressed == data


class TestSerializeDeserialize:
    """Xorb serialization round-trip."""

    def test_single_chunk(self) -> None:
        chunks = [b"hello world"]
        xorb_bytes, offsets = serialize_xorb(chunks)
        assert len(offsets) == 1
        assert offsets[0].byte_offset == 0

        entries = deserialize_xorb(xorb_bytes)
        assert len(entries) == 1
        assert entries[0].uncompressed_data == b"hello world"
        assert entries[0].uncompressed_size == 11

    def test_multiple_chunks(self) -> None:
        chunks = [os.urandom(1024) for _ in range(5)]
        xorb_bytes, offsets = serialize_xorb(chunks)
        assert len(offsets) == 5

        entries = deserialize_xorb(xorb_bytes)
        assert len(entries) == 5
        for i, entry in enumerate(entries):
            assert entry.uncompressed_data == chunks[i]

    def test_offsets_are_monotonic(self) -> None:
        chunks = [os.urandom(2048) for _ in range(10)]
        _, offsets = serialize_xorb(chunks)
        for i in range(1, len(offsets)):
            assert offsets[i].byte_offset > offsets[i - 1].byte_offset

    def test_empty_chunk(self) -> None:
        chunks = [b""]
        xorb_bytes, offsets = serialize_xorb(chunks)
        entries = deserialize_xorb(xorb_bytes)
        assert len(entries) == 1
        assert entries[0].uncompressed_data == b""

    def test_large_random_chunks(self) -> None:
        """Round-trip with realistic chunk sizes."""
        chunks = [os.urandom(64 * 1024) for _ in range(8)]
        xorb_bytes, offsets = serialize_xorb(chunks)
        entries = deserialize_xorb(xorb_bytes)
        assert len(entries) == 8
        for i in range(8):
            assert entries[i].uncompressed_data == chunks[i]

    def test_xorb_header_version(self) -> None:
        """First byte of each chunk header should be XORB_VERSION (0)."""
        chunks = [b"data1", b"data2"]
        xorb_bytes, offsets = serialize_xorb(chunks)
        for off in offsets:
            assert xorb_bytes[off.byte_offset] == XORB_VERSION

    def test_header_size_is_8(self) -> None:
        assert HEADER_SIZE == 8

    def test_truncated_header_raises(self) -> None:
        import pytest

        with pytest.raises(ValueError, match="Truncated xorb header"):
            deserialize_xorb(b"\x00" * 5)

    def test_truncated_data_raises(self) -> None:
        import pytest

        # Valid header claiming 1000 bytes of compressed data, but only 2 bytes follow
        import struct

        header = bytearray(8)
        header[0] = 0
        header[1:4] = struct.pack("<I", 1000)[:3]
        header[4] = 0  # NONE
        header[5:8] = struct.pack("<I", 1000)[:3]
        data = bytes(header) + b"\x00\x00"
        with pytest.raises(ValueError, match="Truncated xorb chunk data"):
            deserialize_xorb(data)


class TestSerializeXorbIntegrity:
    """End-to-end: chunk → serialize → deserialize → verify."""

    def test_full_pipeline(self) -> None:
        from hugbucket.xet.chunker import chunk_data

        original = os.urandom(200 * 1024)
        cdc_chunks = chunk_data(original)
        raw_chunks = [c.data for c in cdc_chunks]

        xorb_bytes, offsets = serialize_xorb(raw_chunks)
        entries = deserialize_xorb(xorb_bytes)

        rebuilt = b"".join(e.uncompressed_data for e in entries)
        assert rebuilt == original
