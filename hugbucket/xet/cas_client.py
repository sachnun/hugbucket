"""Async client for Xet CAS (Content-Addressable Storage) API."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

import aiohttp

from hugbucket.hub.client import XetConnectionInfo

logger = logging.getLogger(__name__)


@dataclass
class ReconstructionTerm:
    """A term in a file reconstruction: a range of chunks in a xorb."""

    hash: str  # xorb hash (hex)
    unpacked_length: int  # total bytes
    range_start: int  # first chunk index
    range_end: int  # one past last chunk index


@dataclass
class FetchRange:
    """Info for fetching part of a xorb from the transfer CDN."""

    range_start: int
    range_end: int
    url: str
    url_range_start: int
    url_range_end: int


@dataclass
class Reconstruction:
    """File reconstruction info from the CAS."""

    offset_into_first_range: int
    terms: list[ReconstructionTerm]
    fetch_info: dict[str, list[FetchRange]]  # xorb_hash -> fetch ranges


@dataclass
class CASClient:
    """Async client for Xet CAS endpoints."""

    _session: aiohttp.ClientSession | None = field(default=None, repr=False)

    async def _ensure_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(raise_for_status=False)
        return self._session

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    @staticmethod
    def _auth_headers(conn: XetConnectionInfo) -> dict[str, str]:
        return {"Authorization": f"Bearer {conn.access_token}"}

    # ---- Upload ----

    async def upload_xorb(
        self,
        conn: XetConnectionInfo,
        xorb_hash: str,
        xorb_data: bytes,
    ) -> bool:
        """Upload a xorb to CAS. Returns True if newly inserted."""
        session = await self._ensure_session()
        url = f"{conn.cas_url}/v1/xorbs/default/{xorb_hash}"
        async with session.post(
            url,
            data=xorb_data,
            headers={
                **self._auth_headers(conn),
                "Content-Type": "application/octet-stream",
            },
        ) as resp:
            if resp.status >= 400:
                body = await resp.text()
                logger.error(
                    f"CAS upload_xorb failed: {resp.status} {body} "
                    f"url={url} xorb_size={len(xorb_data)}"
                )
                resp.raise_for_status()
            data = await resp.json()
            return data.get("was_inserted", False)

    async def upload_shard(
        self,
        conn: XetConnectionInfo,
        shard_data: bytes,
    ) -> int:
        """Upload a shard to CAS. Returns result code (0 or 1)."""
        session = await self._ensure_session()
        url = f"{conn.cas_url}/v1/shards"
        async with session.post(
            url,
            data=shard_data,
            headers={
                **self._auth_headers(conn),
                "Content-Type": "application/octet-stream",
            },
        ) as resp:
            if resp.status >= 400:
                body = await resp.text()
                logger.error(f"CAS upload_shard failed: {resp.status} {body}")
                resp.raise_for_status()
            data = await resp.json()
            return data.get("result", -1)

    # ---- Download ----

    async def get_reconstruction(
        self,
        conn: XetConnectionInfo,
        file_id: str,
    ) -> Reconstruction:
        """Get file reconstruction info from CAS."""
        session = await self._ensure_session()
        url = f"{conn.cas_url}/v1/reconstructions/{file_id}"
        async with session.get(url, headers=self._auth_headers(conn)) as resp:
            resp.raise_for_status()
            data = await resp.json()

        terms = [
            ReconstructionTerm(
                hash=t["hash"],
                unpacked_length=t["unpacked_length"],
                range_start=t["range"]["start"],
                range_end=t["range"]["end"],
            )
            for t in data["terms"]
        ]

        fetch_info: dict[str, list[FetchRange]] = {}
        for xorb_hash, ranges in data.get("fetch_info", {}).items():
            fetch_info[xorb_hash] = [
                FetchRange(
                    range_start=r["range"]["start"],
                    range_end=r["range"]["end"],
                    url=r["url"],
                    url_range_start=r["url_range"]["start"],
                    url_range_end=r["url_range"]["end"],
                )
                for r in ranges
            ]

        return Reconstruction(
            offset_into_first_range=data.get("offset_into_first_range", 0),
            terms=terms,
            fetch_info=fetch_info,
        )

    async def fetch_xorb_range(
        self,
        fetch: FetchRange,
    ) -> bytes:
        """Fetch xorb bytes from the transfer CDN using presigned URL + range."""
        session = await self._ensure_session()
        headers = {
            "Range": f"bytes={fetch.url_range_start}-{fetch.url_range_end}",
        }
        async with session.get(fetch.url, headers=headers) as resp:
            # Accept 200 or 206 (partial content)
            if resp.status not in (200, 206):
                resp.raise_for_status()
            return await resp.read()

    # ---- Dedup query ----

    async def query_global_dedup(
        self,
        conn: XetConnectionInfo,
        chunk_hash: str,
    ) -> bytes | None:
        """Query global dedup. Returns shard bytes or None if no match."""
        session = await self._ensure_session()
        url = f"{conn.cas_url}/v1/chunks/default-merkledb/{chunk_hash}"
        async with session.get(url, headers=self._auth_headers(conn)) as resp:
            if resp.status == 404:
                return None
            resp.raise_for_status()
            return await resp.read()
