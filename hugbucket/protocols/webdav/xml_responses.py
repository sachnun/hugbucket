"""WebDAV XML response builders (RFC 4918).

Generates well-formed DAV: namespace XML for PROPFIND, PROPPATCH,
error responses, and lock discovery stubs.
"""

from __future__ import annotations

import html
from datetime import datetime, timezone
from urllib.parse import quote


DAV_NS = "DAV:"
XML_HEADER = '<?xml version="1.0" encoding="utf-8"?>\n'


def _escape(text: str) -> str:
    """Escape text for safe XML embedding."""
    return html.escape(str(text), quote=True)


def _href(path: str) -> str:
    """Encode a path as a DAV href (percent-encoded, preserving slashes)."""
    parts = path.split("/")
    return "/".join(quote(p, safe="") for p in parts)


def _format_rfc1123(ts: str | None) -> str:
    """Format an ISO timestamp as RFC 1123 (HTTP date).

    Example: "Mon, 23 Mar 2026 12:00:00 GMT"
    """
    if ts:
        try:
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            return dt.strftime("%a, %d %b %Y %H:%M:%S GMT")
        except Exception:
            pass
    return datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S GMT")


def _format_iso8601(ts: str | None) -> str:
    """Format an ISO timestamp for creationdate property."""
    if ts:
        try:
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            return dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        except Exception:
            pass
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _propstat_block(props_xml: str, status: str = "HTTP/1.1 200 OK") -> str:
    """Wrap property XML in a <D:propstat> block."""
    return (
        "<D:propstat>"
        f"<D:prop>{props_xml}</D:prop>"
        f"<D:status>{status}</D:status>"
        "</D:propstat>"
    )


def _collection_props(
    *,
    displayname: str = "",
    created: str | None = None,
    modified: str | None = None,
) -> str:
    """Property XML for a collection (directory/bucket)."""
    props = "<D:resourcetype><D:collection/></D:resourcetype>"
    if displayname:
        props += f"<D:displayname>{_escape(displayname)}</D:displayname>"
    props += f"<D:getlastmodified>{_format_rfc1123(modified)}</D:getlastmodified>"
    props += f"<D:creationdate>{_format_iso8601(created)}</D:creationdate>"
    return props


def _file_props(
    *,
    size: int = 0,
    content_type: str = "application/octet-stream",
    etag: str = "",
    modified: str | None = None,
    created: str | None = None,
    displayname: str = "",
) -> str:
    """Property XML for a non-collection resource (file)."""
    props = "<D:resourcetype/>"
    if displayname:
        props += f"<D:displayname>{_escape(displayname)}</D:displayname>"
    props += f"<D:getcontentlength>{size}</D:getcontentlength>"
    props += f"<D:getcontenttype>{_escape(content_type)}</D:getcontenttype>"
    if etag:
        props += f"<D:getetag>&quot;{_escape(etag)}&quot;</D:getetag>"
    props += f"<D:getlastmodified>{_format_rfc1123(modified)}</D:getlastmodified>"
    props += f"<D:creationdate>{_format_iso8601(created)}</D:creationdate>"
    return props


# ── Public response builders ────────────────────────────────────────────


def multistatus_xml(responses: list[tuple[str, str]]) -> str:
    """Build a 207 Multi-Status response body.

    Each item in *responses* is (href, propstat_xml).
    """
    body = XML_HEADER
    body += '<D:multistatus xmlns:D="DAV:">'
    for href, propstat in responses:
        body += f"<D:response><D:href>{_href(href)}</D:href>{propstat}</D:response>"
    body += "</D:multistatus>"
    return body


def propfind_collection(
    href: str,
    *,
    displayname: str = "",
    created: str | None = None,
    modified: str | None = None,
) -> tuple[str, str]:
    """Return (href, propstat_xml) for a collection resource."""
    props = _collection_props(
        displayname=displayname, created=created, modified=modified
    )
    return href, _propstat_block(props)


def propfind_file(
    href: str,
    *,
    size: int = 0,
    content_type: str = "application/octet-stream",
    etag: str = "",
    modified: str | None = None,
    created: str | None = None,
    displayname: str = "",
) -> tuple[str, str]:
    """Return (href, propstat_xml) for a non-collection resource."""
    props = _file_props(
        size=size,
        content_type=content_type,
        etag=etag,
        modified=modified,
        created=created,
        displayname=displayname,
    )
    return href, _propstat_block(props)


def error_xml(status_code: int, message: str) -> str:
    """Build a simple DAV error response body."""
    body = XML_HEADER
    body += '<D:error xmlns:D="DAV:">'
    body += f"<D:status>HTTP/1.1 {status_code} {message}</D:status>"
    body += "</D:error>"
    return body


def proppatch_response_xml(href: str, prop_names: list[str]) -> str:
    """Build a PROPPATCH response accepting all property changes.

    We accept the request but don't actually persist dead properties since
    the HF backend doesn't support arbitrary metadata.  Returning 200 for
    each property keeps clients (Windows Explorer, macOS Finder) happy.
    """
    props = "".join(f"<D:{name}/>" for name in prop_names)
    propstat = _propstat_block(props)
    body = XML_HEADER
    body += '<D:multistatus xmlns:D="DAV:">'
    body += f"<D:response><D:href>{_href(href)}</D:href>{propstat}</D:response>"
    body += "</D:multistatus>"
    return body


def lock_discovery_xml(href: str, token: str, owner: str = "") -> str:
    """Build a fake LOCK response for client compatibility.

    HugBucket does not implement true locking; this stub satisfies clients
    like Windows Explorer that require a lock token before writing.
    """
    owner_xml = f"<D:owner><D:href>{_escape(owner)}</D:href></D:owner>" if owner else ""
    body = XML_HEADER
    body += '<D:prop xmlns:D="DAV:">'
    body += "<D:lockdiscovery><D:activelock>"
    body += "<D:locktype><D:write/></D:locktype>"
    body += "<D:lockscope><D:exclusive/></D:lockscope>"
    body += "<D:depth>infinity</D:depth>"
    body += owner_xml
    body += f"<D:timeout>Second-3600</D:timeout>"
    body += f"<D:locktoken><D:href>opaquelocktoken:{_escape(token)}</D:locktoken>"
    body += "</D:activelock></D:lockdiscovery>"
    body += "</D:prop>"
    return body
