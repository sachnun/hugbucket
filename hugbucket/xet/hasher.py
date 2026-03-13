"""Blake3 keyed hashing for Xet protocol.

Implements chunk hashing, xorb hashing (Merkle tree), file hashing,
and the non-standard hash-to-string encoding (LE u64 groups).
"""

from __future__ import annotations

import struct

import blake3

# Blake3 keyed hash keys (32 bytes each) from xet-core
DATA_KEY = bytes(
    [
        102,
        151,
        245,
        119,
        91,
        149,
        80,
        222,
        49,
        53,
        203,
        172,
        165,
        151,
        24,
        28,
        157,
        228,
        33,
        16,
        155,
        235,
        43,
        88,
        180,
        208,
        176,
        75,
        147,
        173,
        242,
        41,
    ]
)

INTERNAL_NODE_KEY = bytes(
    [
        1,
        126,
        197,
        199,
        165,
        71,
        41,
        150,
        253,
        148,
        102,
        102,
        180,
        138,
        2,
        230,
        93,
        221,
        83,
        111,
        55,
        199,
        109,
        210,
        248,
        99,
        82,
        230,
        74,
        83,
        113,
        63,
    ]
)

FILE_KEY = bytes(32)  # 32 zero bytes for the final file hash step

VERIFICATION_KEY = bytes(
    [
        127,
        24,
        87,
        214,
        206,
        86,
        237,
        102,
        18,
        127,
        249,
        19,
        231,
        165,
        195,
        243,
        164,
        205,
        38,
        213,
        181,
        219,
        73,
        230,
        65,
        36,
        152,
        127,
        40,
        251,
        148,
        195,
    ]
)


def chunk_hash(data: bytes) -> bytes:
    """Hash a single chunk's data. Returns 32 bytes."""
    return blake3.blake3(data, key=DATA_KEY).digest()


# Mean branching factor for the aggregated hash tree (from xet-core)
_MEAN_BRANCHING_FACTOR = 4
_MAX_CHILDREN = 2 * _MEAN_BRANCHING_FACTOR + 1  # 9


def _hash_last_u64(h: bytes) -> int:
    """Get bytes 24-31 of a 32-byte hash as LE u64.

    This is used by next_merge_cut to determine tree branching points.
    Matches xet-core's MerkleHash Rem<u64> impl (self[3].to_le() % rhs).
    """
    return struct.unpack_from("<Q", h, 24)[0]


def _next_merge_cut(entries: list[tuple[bytes, int]]) -> int:
    """Determine how many entries to merge into one parent node.

    Implements xet-core's aggregated_hashes::next_merge_cut:
    - If 2 or fewer entries, merge all
    - Otherwise, check entries[2..min(9, n)]: if the hash at index i
      has last_u64 % 4 == 0, cut at i+1
    - If no cut found, merge up to min(9, n)
    """
    n = len(entries)
    if n <= 2:
        return n

    end = min(_MAX_CHILDREN, n)
    for i in range(2, end):
        h = entries[i][0]  # the 32-byte hash
        if _hash_last_u64(h) % _MEAN_BRANCHING_FACTOR == 0:
            return i + 1
    return end


def _merged_hash(entries: list[tuple[bytes, int]]) -> tuple[bytes, int]:
    """Merge a group of (hash, size) entries into a single parent.

    Format: concat("{hash_to_hex(h)} : {s}\n" for each (h, s))
    Then blake3 keyed hash with INTERNAL_NODE_KEY.
    """
    parts: list[bytes] = []
    total_size = 0
    for h, s in entries:
        h_hex = hash_to_hex(h)
        parts.append(f"{h_hex} : {s}\n".encode("ascii"))
        total_size += s
    content = b"".join(parts)
    new_hash = blake3.blake3(content, key=INTERNAL_NODE_KEY).digest()
    return new_hash, total_size


def _merkle_root(chunk_hashes: list[bytes], chunk_sizes: list[int]) -> bytes:
    """Compute Merkle tree root from chunk hashes and sizes.

    Uses xet-core's aggregated hash tree with variable branching:
    - Mean branching factor of 4, max 9 children per node
    - Cut points determined by hash values (last u64 mod 4 == 0)
    - Single chunk: root = the chunk hash itself (no internal node)
    - Zero chunks: hash of empty data

    The tree is built iteratively: each round collapses entries using
    next_merge_cut to determine group sizes, until one entry remains.
    """
    assert len(chunk_hashes) == len(chunk_sizes)

    if len(chunk_hashes) == 0:
        return blake3.blake3(b"", key=DATA_KEY).digest()

    if len(chunk_hashes) == 1:
        return chunk_hashes[0]

    # Build initial entries list
    entries: list[tuple[bytes, int]] = list(zip(chunk_hashes, chunk_sizes))

    # Iteratively collapse until one entry remains
    while len(entries) > 1:
        new_entries: list[tuple[bytes, int]] = []
        idx = 0
        while idx < len(entries):
            remaining = entries[idx:]
            cut = _next_merge_cut(remaining)
            group = entries[idx : idx + cut]
            new_entries.append(_merged_hash(group))
            idx += cut
        entries = new_entries

    return entries[0][0]


def xorb_hash(chunk_hashes: list[bytes], chunk_sizes: list[int]) -> bytes:
    """Compute xorb hash (Merkle root of its chunks). Returns 32 bytes."""
    return _merkle_root(chunk_hashes, chunk_sizes)


def file_hash(chunk_hashes: list[bytes], chunk_sizes: list[int]) -> bytes:
    """Compute file hash.

    1. Compute Merkle root over ALL file chunks
    2. Apply one more Blake3 keyed hash with FILE_KEY (32 zero bytes)
    """
    root = _merkle_root(chunk_hashes, chunk_sizes)
    return blake3.blake3(root, key=FILE_KEY).digest()


def verification_hash(chunk_hashes: list[bytes]) -> bytes:
    """Compute term verification hash.

    Concatenate raw 32-byte chunk hashes, then Blake3 keyed with VERIFICATION_KEY.
    """
    content = b"".join(chunk_hashes)
    return blake3.blake3(content, key=VERIFICATION_KEY).digest()


def hash_to_hex(h: bytes) -> str:
    """Convert a 32-byte hash to Xet's non-standard hex encoding.

    For every 8-byte group (indices 0-7, 8-15, 16-23, 24-31),
    reverse the byte order within each group (treat as LE u64),
    then hex-encode each group zero-padded to 16 hex chars.

    Example: bytes [0,1,2,3,4,5,6,7,...] -> "0706050403020100..."
    """
    assert len(h) == 32, f"Expected 32 bytes, got {len(h)}"
    parts: list[str] = []
    for i in range(0, 32, 8):
        # Read 8 bytes as little-endian u64, then format as hex
        val = struct.unpack_from("<Q", h, i)[0]
        parts.append(f"{val:016x}")
    return "".join(parts)


def hex_to_hash(s: str) -> bytes:
    """Reverse of hash_to_hex: convert Xet hex string back to 32 bytes."""
    assert len(s) == 64, f"Expected 64 hex chars, got {len(s)}"
    result = bytearray(32)
    for i in range(4):
        val = int(s[i * 16 : (i + 1) * 16], 16)
        struct.pack_into("<Q", result, i * 8, val)
    return bytes(result)
