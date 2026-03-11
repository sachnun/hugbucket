"""Tests for hugbucket.xet.hasher — Blake3 hashing and Merkle tree."""

from __future__ import annotations

import os
import struct

from hugbucket.xet.hasher import (
    DATA_KEY,
    INTERNAL_NODE_KEY,
    FILE_KEY,
    VERIFICATION_KEY,
    chunk_hash,
    xorb_hash,
    file_hash,
    verification_hash,
    hash_to_hex,
    hex_to_hash,
    _merkle_root,
    _hash_last_u64,
    _next_merge_cut,
    _merged_hash,
)


class TestKeys:
    """Key constants sanity checks."""

    def test_data_key_length(self) -> None:
        assert len(DATA_KEY) == 32

    def test_internal_node_key_length(self) -> None:
        assert len(INTERNAL_NODE_KEY) == 32

    def test_file_key_is_zeros(self) -> None:
        assert FILE_KEY == b"\x00" * 32

    def test_verification_key_length(self) -> None:
        assert len(VERIFICATION_KEY) == 32

    def test_all_keys_distinct(self) -> None:
        keys = {DATA_KEY, INTERNAL_NODE_KEY, FILE_KEY, VERIFICATION_KEY}
        assert len(keys) == 4


class TestChunkHash:
    """chunk_hash basic tests."""

    def test_returns_32_bytes(self) -> None:
        h = chunk_hash(b"hello world")
        assert len(h) == 32

    def test_deterministic(self) -> None:
        data = os.urandom(1024)
        assert chunk_hash(data) == chunk_hash(data)

    def test_different_inputs_different_hashes(self) -> None:
        assert chunk_hash(b"aaa") != chunk_hash(b"bbb")

    def test_empty_input(self) -> None:
        h = chunk_hash(b"")
        assert len(h) == 32


class TestHashToHex:
    """hash_to_hex / hex_to_hash encoding."""

    def test_round_trip(self) -> None:
        h = os.urandom(32)
        assert hex_to_hash(hash_to_hex(h)) == h

    def test_output_length(self) -> None:
        h = os.urandom(32)
        assert len(hash_to_hex(h)) == 64  # 4 groups × 16 hex chars

    def test_le_byte_reversal(self) -> None:
        """First 8 bytes [0,1,2,3,4,5,6,7] → LE u64 → hex '0706050403020100'."""
        h = bytes(range(32))
        hex_str = hash_to_hex(h)
        # First group: bytes 0-7 as LE u64
        first_group = hex_str[:16]
        assert first_group == "0706050403020100"

    def test_known_value(self) -> None:
        # All zeros → "0000000000000000" × 4
        h = b"\x00" * 32
        assert hash_to_hex(h) == "0" * 64

    def test_all_ff(self) -> None:
        h = b"\xff" * 32
        assert hash_to_hex(h) == "f" * 64

    def test_hex_to_hash_inverse(self) -> None:
        hex_str = "0706050403020100" * 4
        h = hex_to_hash(hex_str)
        # First 8 bytes should be [0,1,2,3,4,5,6,7] (reversed LE)
        assert h[:8] == bytes(range(8))

    def test_invalid_length_raises(self) -> None:
        import pytest

        with pytest.raises(AssertionError):
            hash_to_hex(b"\x00" * 16)
        with pytest.raises(AssertionError):
            hex_to_hash("0000")


class TestHashLastU64:
    """_hash_last_u64 — used for Merkle tree branching."""

    def test_extracts_last_8_bytes(self) -> None:
        h = b"\x00" * 24 + b"\x01\x00\x00\x00\x00\x00\x00\x00"
        assert _hash_last_u64(h) == 1

    def test_le_order(self) -> None:
        h = b"\x00" * 24 + struct.pack("<Q", 0xDEADBEEF)
        assert _hash_last_u64(h) == 0xDEADBEEF


class TestNextMergeCut:
    """Variable-branching Merkle tree cut determination."""

    def test_single_entry(self) -> None:
        entry = (os.urandom(32), 100)
        assert _next_merge_cut([entry]) == 1

    def test_two_entries(self) -> None:
        entries = [(os.urandom(32), 100), (os.urandom(32), 200)]
        assert _next_merge_cut(entries) == 2

    def test_max_9_children(self) -> None:
        """Even without hitting mod 4 == 0, should cap at 9."""
        # Create entries where no hash has last_u64 % 4 == 0 at positions 2..8
        entries = []
        for _ in range(20):
            # Force last u64 to NOT be divisible by 4
            h = bytearray(os.urandom(32))
            val = struct.unpack_from("<Q", h, 24)[0]
            if val % 4 == 0:
                val += 1
            struct.pack_into("<Q", h, 24, val)
            entries.append((bytes(h), 100))
        cut = _next_merge_cut(entries)
        assert cut <= 9

    def test_early_cut_on_mod4(self) -> None:
        """Entry at index 2 with last_u64 % 4 == 0 → cut at 3."""
        entries = []
        for _ in range(2):
            h = bytearray(os.urandom(32))
            val = struct.unpack_from("<Q", h, 24)[0]
            if val % 4 == 0:
                val += 1  # make NOT divisible
            struct.pack_into("<Q", h, 24, val)
            entries.append((bytes(h), 100))
        # Third entry: force last_u64 % 4 == 0
        h = bytearray(os.urandom(32))
        struct.pack_into("<Q", h, 24, 8)  # 8 % 4 == 0
        entries.append((bytes(h), 100))
        # More entries
        for _ in range(5):
            entries.append((os.urandom(32), 100))

        cut = _next_merge_cut(entries)
        assert cut == 3


class TestMerkleRoot:
    """Merkle tree root computation."""

    def test_zero_chunks(self) -> None:
        root = _merkle_root([], [])
        assert len(root) == 32

    def test_single_chunk(self) -> None:
        """Single chunk → root IS the chunk hash."""
        h = chunk_hash(b"data")
        root = _merkle_root([h], [4])
        assert root == h

    def test_two_chunks(self) -> None:
        h1 = chunk_hash(b"chunk1")
        h2 = chunk_hash(b"chunk2")
        root = _merkle_root([h1, h2], [6, 6])
        assert len(root) == 32
        # Two entries → merged in one step
        assert root != h1
        assert root != h2

    def test_many_chunks_deterministic(self) -> None:
        hashes = [chunk_hash(os.urandom(1024)) for _ in range(50)]
        sizes = [1024] * 50
        r1 = _merkle_root(hashes, sizes)
        r2 = _merkle_root(hashes, sizes)
        assert r1 == r2

    def test_different_inputs_different_roots(self) -> None:
        h1 = [chunk_hash(b"a")]
        h2 = [chunk_hash(b"b")]
        r1 = _merkle_root(h1, [1])
        r2 = _merkle_root(h2, [1])
        assert r1 != r2

    def test_six_chunks(self) -> None:
        """Regression: 6+ chunks triggered variable-branching bug in earlier code."""
        hashes = [chunk_hash(f"chunk{i}".encode()) for i in range(6)]
        sizes = [100] * 6
        root = _merkle_root(hashes, sizes)
        assert len(root) == 32
        # Should be deterministic
        assert root == _merkle_root(hashes, sizes)

    def test_large_chunk_count(self) -> None:
        """Test with 100 chunks — exercises multiple tree levels."""
        hashes = [chunk_hash(os.urandom(64)) for _ in range(100)]
        sizes = [64] * 100
        root = _merkle_root(hashes, sizes)
        assert len(root) == 32


class TestXorbHash:
    """xorb_hash is just _merkle_root."""

    def test_matches_merkle_root(self) -> None:
        hashes = [chunk_hash(b"x"), chunk_hash(b"y")]
        sizes = [1, 1]
        assert xorb_hash(hashes, sizes) == _merkle_root(hashes, sizes)


class TestFileHash:
    """file_hash = merkle_root + extra FILE_KEY hash."""

    def test_different_from_merkle_root(self) -> None:
        hashes = [chunk_hash(b"data")]
        sizes = [4]
        fh = file_hash(hashes, sizes)
        mr = _merkle_root(hashes, sizes)
        # file_hash applies one more keyed hash on top
        assert fh != mr
        assert len(fh) == 32

    def test_deterministic(self) -> None:
        hashes = [chunk_hash(b"a"), chunk_hash(b"b")]
        sizes = [1, 1]
        assert file_hash(hashes, sizes) == file_hash(hashes, sizes)


class TestVerificationHash:
    """verification_hash = Blake3(concat chunk hashes, key=VERIFICATION_KEY)."""

    def test_returns_32_bytes(self) -> None:
        hashes = [chunk_hash(b"a")]
        assert len(verification_hash(hashes)) == 32

    def test_deterministic(self) -> None:
        hashes = [chunk_hash(b"x"), chunk_hash(b"y")]
        assert verification_hash(hashes) == verification_hash(hashes)

    def test_order_matters(self) -> None:
        h1, h2 = chunk_hash(b"a"), chunk_hash(b"b")
        assert verification_hash([h1, h2]) != verification_hash([h2, h1])
