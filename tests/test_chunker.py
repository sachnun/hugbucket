"""Tests for hugbucket.xet.chunker — Gearhash CDC."""

from __future__ import annotations

import os

from hugbucket.xet.chunker import (
    Chunk,
    chunk_data,
    GEAR_TABLE,
    GEAR_MASK,
    MIN_CHUNK_SIZE,
    MAX_CHUNK_SIZE,
)


class TestGearTable:
    """Sanity checks on the Gear lookup table."""

    def test_table_length(self) -> None:
        assert len(GEAR_TABLE) == 256

    def test_all_entries_are_u64(self) -> None:
        for i, v in enumerate(GEAR_TABLE):
            assert 0 <= v < 2**64, f"GEAR_TABLE[{i}] out of u64 range: {v}"

    def test_no_duplicates(self) -> None:
        # Gear tables should have all unique entries
        assert len(set(GEAR_TABLE)) == 256


class TestChunkDataEmpty:
    """Edge case: empty input."""

    def test_empty_returns_empty(self) -> None:
        assert chunk_data(b"") == []


class TestChunkDataSmall:
    """Data smaller than min_chunk_size → single chunk."""

    def test_tiny_data(self) -> None:
        data = b"hello"
        chunks = chunk_data(data)
        assert len(chunks) == 1
        assert chunks[0].data == data
        assert chunks[0].offset == 0

    def test_exactly_min_size(self) -> None:
        data = os.urandom(MIN_CHUNK_SIZE)
        chunks = chunk_data(data)
        assert len(chunks) == 1
        assert chunks[0].data == data

    def test_just_under_min_size(self) -> None:
        data = os.urandom(MIN_CHUNK_SIZE - 1)
        chunks = chunk_data(data)
        assert len(chunks) == 1
        assert chunks[0].data == data


class TestChunkDataDeterminism:
    """CDC must be deterministic: same input → same chunks."""

    def test_same_input_same_chunks(self) -> None:
        data = os.urandom(200 * 1024)
        c1 = chunk_data(data)
        c2 = chunk_data(data)
        assert len(c1) == len(c2)
        for a, b in zip(c1, c2):
            assert a.offset == b.offset
            assert a.data == b.data


class TestChunkDataLarge:
    """Multi-chunk payloads."""

    def test_200kb_produces_multiple_chunks(self) -> None:
        data = os.urandom(200 * 1024)
        chunks = chunk_data(data)
        assert len(chunks) > 1

    def test_no_data_loss(self) -> None:
        """Concatenation of all chunks must equal original data."""
        data = os.urandom(500 * 1024)
        chunks = chunk_data(data)
        rebuilt = b"".join(c.data for c in chunks)
        assert rebuilt == data

    def test_offsets_are_contiguous(self) -> None:
        data = os.urandom(300 * 1024)
        chunks = chunk_data(data)
        expected_offset = 0
        for c in chunks:
            assert c.offset == expected_offset
            expected_offset += len(c.data)
        assert expected_offset == len(data)

    def test_1mb_data(self) -> None:
        data = os.urandom(1024 * 1024)
        chunks = chunk_data(data)
        assert len(chunks) > 1
        rebuilt = b"".join(c.data for c in chunks)
        assert rebuilt == data


class TestChunkSizeConstraints:
    """Min/max chunk size enforcement."""

    def test_no_chunk_exceeds_max(self) -> None:
        data = os.urandom(500 * 1024)
        chunks = chunk_data(data)
        for c in chunks:
            assert len(c.data) <= MAX_CHUNK_SIZE

    def test_interior_chunks_at_least_min(self) -> None:
        """All chunks except possibly the last must be >= min_size."""
        data = os.urandom(500 * 1024)
        chunks = chunk_data(data)
        for c in chunks[:-1]:
            assert len(c.data) >= MIN_CHUNK_SIZE

    def test_custom_min_max(self) -> None:
        data = os.urandom(100 * 1024)
        chunks = chunk_data(data, min_size=4096, max_size=16384)
        for c in chunks[:-1]:
            assert len(c.data) >= 4096
        for c in chunks:
            assert len(c.data) <= 16384
        rebuilt = b"".join(c.data for c in chunks)
        assert rebuilt == data


class TestChunkDataMemoryview:
    """chunk_data should accept memoryview too."""

    def test_memoryview_input(self) -> None:
        data = os.urandom(50 * 1024)
        mv = memoryview(data)
        chunks = chunk_data(mv)
        rebuilt = b"".join(c.data for c in chunks)
        assert rebuilt == data
