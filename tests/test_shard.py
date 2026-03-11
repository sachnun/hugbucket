"""Tests for hugbucket.xet.shard — Shard binary format builder."""

from __future__ import annotations

import os
import struct

from hugbucket.xet.hasher import chunk_hash, xorb_hash, file_hash, verification_hash
from hugbucket.xet.shard import (
    MDB_SHARD_HEADER_TAG,
    SHARD_HEADER_VERSION,
    SHARD_FOOTER_VERSION,
    ENTRY_SIZE,
    FOOTER_SIZE,
    BOOKEND_HASH,
    FileInfo,
    FileDataTerm,
    XorbInfo,
    CASChunkInfo,
    build_shard,
    _truncate_hash,
)


def _make_test_data() -> tuple[list[FileInfo], list[XorbInfo]]:
    """Create minimal valid shard input data."""
    # Simulate: 2 chunks, 1 xorb, 1 file
    c1_data = os.urandom(1024)
    c2_data = os.urandom(2048)

    c1_hash = chunk_hash(c1_data)
    c2_hash = chunk_hash(c2_data)

    x_hash = xorb_hash([c1_hash, c2_hash], [1024, 2048])
    f_hash = file_hash([c1_hash, c2_hash], [1024, 2048])
    v_hash = verification_hash([c1_hash, c2_hash])

    xorb = XorbInfo(
        xorb_hash=x_hash,
        cas_flags=0,
        chunks=[
            CASChunkInfo(chunk_hash=c1_hash, byte_range_start=0, unpacked_bytes=1024),
            CASChunkInfo(
                chunk_hash=c2_hash, byte_range_start=1024, unpacked_bytes=2048
            ),
        ],
        total_bytes_in_xorb=3072,
        total_bytes_on_disk=3000,
    )

    file_info = FileInfo(
        file_hash=f_hash,
        terms=[
            FileDataTerm(
                xorb_hash=x_hash,
                cas_flags=0,
                unpacked_bytes=3072,
                chunk_start=0,
                chunk_end=2,
            )
        ],
        verification_hashes=[v_hash],
    )

    return [file_info], [xorb]


class TestShardMagic:
    """Header magic bytes validation."""

    def test_magic_length(self) -> None:
        assert len(MDB_SHARD_HEADER_TAG) == 32

    def test_magic_starts_with_ascii(self) -> None:
        assert MDB_SHARD_HEADER_TAG[:14] == b"HFRepoMetaData"
        assert MDB_SHARD_HEADER_TAG[14] == 0x00  # null terminator


class TestTruncateHash:
    """_truncate_hash: first 8 bytes as LE u64."""

    def test_basic(self) -> None:
        h = b"\x01\x00\x00\x00\x00\x00\x00\x00" + b"\x00" * 24
        assert _truncate_hash(h) == 1

    def test_le_order(self) -> None:
        h = struct.pack("<Q", 0xDEADBEEF) + b"\x00" * 24
        assert _truncate_hash(h) == 0xDEADBEEF


class TestBuildShard:
    """build_shard structure validation."""

    def test_starts_with_magic(self) -> None:
        files, xorbs = _make_test_data()
        shard = build_shard(files, xorbs)
        assert shard[:32] == MDB_SHARD_HEADER_TAG

    def test_header_version(self) -> None:
        files, xorbs = _make_test_data()
        shard = build_shard(files, xorbs)
        version = struct.unpack_from("<Q", shard, 32)[0]
        assert version == SHARD_HEADER_VERSION

    def test_header_footer_size(self) -> None:
        files, xorbs = _make_test_data()
        shard = build_shard(files, xorbs)
        footer_size = struct.unpack_from("<Q", shard, 40)[0]
        assert footer_size == FOOTER_SIZE

    def test_ends_with_footer(self) -> None:
        files, xorbs = _make_test_data()
        shard = build_shard(files, xorbs)
        assert len(shard) >= FOOTER_SIZE
        footer = shard[-FOOTER_SIZE:]
        footer_version = struct.unpack_from("<Q", footer, 0)[0]
        assert footer_version == SHARD_FOOTER_VERSION

    def test_footer_offset_matches(self) -> None:
        files, xorbs = _make_test_data()
        shard = build_shard(files, xorbs)
        footer = shard[-FOOTER_SIZE:]
        # footer_offset is at the end of footer (last 8 bytes)
        footer_offset = struct.unpack_from("<Q", footer, FOOTER_SIZE - 8)[0]
        assert footer_offset == len(shard) - FOOTER_SIZE

    def test_file_bookend_present(self) -> None:
        """File info section ends with a bookend (32 bytes of 0xFF)."""
        files, xorbs = _make_test_data()
        shard = build_shard(files, xorbs)
        # Search for bookend hash in the shard
        assert BOOKEND_HASH in shard

    def test_file_lookup_count(self) -> None:
        files, xorbs = _make_test_data()
        shard = build_shard(files, xorbs)
        footer = shard[-FOOTER_SIZE:]
        # file_lookup_count at footer offset 32 (after version + 3 offsets)
        file_lookup_count = struct.unpack_from("<Q", footer, 32)[0]
        assert file_lookup_count == 1  # we have 1 file

    def test_xorb_lookup_count(self) -> None:
        files, xorbs = _make_test_data()
        shard = build_shard(files, xorbs)
        footer = shard[-FOOTER_SIZE:]
        xorb_lookup_count = struct.unpack_from("<Q", footer, 48)[0]
        assert xorb_lookup_count == 1  # we have 1 xorb

    def test_chunk_lookup_count(self) -> None:
        files, xorbs = _make_test_data()
        shard = build_shard(files, xorbs)
        footer = shard[-FOOTER_SIZE:]
        chunk_lookup_count = struct.unpack_from("<Q", footer, 64)[0]
        assert chunk_lookup_count == 2  # we have 2 chunks

    def test_entry_size_48(self) -> None:
        assert ENTRY_SIZE == 48

    def test_empty_shard(self) -> None:
        """Shard with no files and no xorbs."""
        shard = build_shard([], [])
        assert shard[:32] == MDB_SHARD_HEADER_TAG
        assert len(shard) >= 48 + 48 + 48 + FOOTER_SIZE  # header + 2 bookends + footer

    def test_multiple_files(self) -> None:
        """Shard with 2 files sharing the same xorb."""
        c1_hash = chunk_hash(b"chunk1")
        c2_hash = chunk_hash(b"chunk2")
        x_hash = xorb_hash([c1_hash, c2_hash], [6, 6])

        f1_hash = file_hash([c1_hash], [6])
        f2_hash = file_hash([c2_hash], [6])

        v1 = verification_hash([c1_hash])
        v2 = verification_hash([c2_hash])

        xorb = XorbInfo(
            xorb_hash=x_hash,
            cas_flags=0,
            chunks=[
                CASChunkInfo(chunk_hash=c1_hash, byte_range_start=0, unpacked_bytes=6),
                CASChunkInfo(chunk_hash=c2_hash, byte_range_start=6, unpacked_bytes=6),
            ],
            total_bytes_in_xorb=12,
            total_bytes_on_disk=12,
        )

        file1 = FileInfo(
            file_hash=f1_hash,
            terms=[
                FileDataTerm(
                    xorb_hash=x_hash,
                    cas_flags=0,
                    unpacked_bytes=6,
                    chunk_start=0,
                    chunk_end=1,
                )
            ],
            verification_hashes=[v1],
        )
        file2 = FileInfo(
            file_hash=f2_hash,
            terms=[
                FileDataTerm(
                    xorb_hash=x_hash,
                    cas_flags=0,
                    unpacked_bytes=6,
                    chunk_start=1,
                    chunk_end=2,
                )
            ],
            verification_hashes=[v2],
        )

        shard = build_shard([file1, file2], [xorb])
        footer = shard[-FOOTER_SIZE:]
        file_lookup_count = struct.unpack_from("<Q", footer, 32)[0]
        assert file_lookup_count == 2

    def test_shard_with_sha256(self) -> None:
        """File with sha256 metadata extension."""
        files, xorbs = _make_test_data()
        files[0].sha256 = os.urandom(32)
        shard = build_shard(files, xorbs)
        # Should still be valid
        assert shard[:32] == MDB_SHARD_HEADER_TAG

    def test_lookup_tables_sorted(self) -> None:
        """Lookup table entries are sorted by truncated hash."""
        c_hashes = [chunk_hash(f"chunk{i}".encode()) for i in range(5)]
        c_sizes = [10] * 5
        x_hash = xorb_hash(c_hashes, c_sizes)
        f_hash = file_hash(c_hashes, c_sizes)

        xorb = XorbInfo(
            xorb_hash=x_hash,
            cas_flags=0,
            chunks=[
                CASChunkInfo(
                    chunk_hash=c_hashes[i],
                    byte_range_start=i * 10,
                    unpacked_bytes=10,
                )
                for i in range(5)
            ],
            total_bytes_in_xorb=50,
            total_bytes_on_disk=50,
        )

        fi = FileInfo(
            file_hash=f_hash,
            terms=[
                FileDataTerm(
                    xorb_hash=x_hash,
                    cas_flags=0,
                    unpacked_bytes=50,
                    chunk_start=0,
                    chunk_end=5,
                )
            ],
            verification_hashes=[verification_hash(c_hashes)],
        )

        shard = build_shard([fi], [xorb])
        footer = shard[-FOOTER_SIZE:]

        # Read chunk lookup table
        chunk_lookup_offset = struct.unpack_from("<Q", footer, 56)[0]
        chunk_lookup_count = struct.unpack_from("<Q", footer, 64)[0]
        assert chunk_lookup_count == 5

        # Verify sorted order
        prev_trunc = 0
        for i in range(chunk_lookup_count):
            off = (
                chunk_lookup_offset + i * 16
            )  # 8 (hash) + 4 (xorb_idx) + 4 (chunk_off)
            trunc = struct.unpack_from("<Q", shard, off)[0]
            assert trunc >= prev_trunc, f"Chunk lookup not sorted at index {i}"
            prev_trunc = trunc
