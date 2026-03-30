#!/usr/bin/env python3
"""
coda_mcp v2 — Full-featured MCP server for the Coda.io API.

Transport : stdio (local)
Auth      : CODA_API_TOKEN env var

Coverage (v2):
  Docs        — list, get, create, delete
  Pages       — list, get, create, update, delete, export content
  Tables      — list, get
  Columns     — list, get, delete
  Rows        — fetch (paginated), get, upsert (bulk), insert (bulk),
                update (single), delete (bulk by IDs), clear (query-based)
  Formulas    — list named formulas, get named formula
  Controls    — list, get
  Buttons     — push button on a row
  Automations — trigger automation rule
  Permissions — list, add, delete
  Analytics   — doc analytics, row analytics

Design rules:
  - NEVER upsert row-by-row. Always use coda_upsert_rows (batches 100/call).
  - Always paginate — tables can have thousands of rows.
  - Always use IDs, not display names.
  - Respect Retry-After on 429.
"""

import json
import os
import time
from typing import Any, Optional

import httpx
from mcp.server.fastmcp import FastMCP

# ---------------------------------------------------------------------------
# Server init
# ---------------------------------------------------------------------------

mcp = FastMCP(
    "coda_mcp",
    instructions=(
        "Full Coda.io API coverage: docs, pages, tables, columns, rows, formulas, "
        "controls, buttons, automations, permissions, analytics. "
        "Always use coda_upsert_rows for bulk writes (100 rows/call). "
        "Never loop coda_update_row for >5 rows. Use column IDs, not names."
    ),
)

BASE_URL = "https://coda.io/apis/v1"


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def _headers() -> dict:
    token = os.environ.get("CODA_API_TOKEN", "")
    if not token:
        raise RuntimeError("CODA_API_TOKEN environment variable is not set.")
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }


def _request(method: str, path: str, **kwargs) -> httpx.Response:
    """Core request with exponential back-off on 429."""
    url = path if path.startswith("http") else f"{BASE_URL}{path}"
    for attempt in range(8):
        r = httpx.request(method, url, headers=_headers(), timeout=60, **kwargs)
        if r.status_code == 429:
            wait = int(r.headers.get("Retry-After", "10")) + 2
            time.sleep(wait)
            continue
        return r
    raise RuntimeError(f"{method} {path} failed after 8 retries (persistent 429)")


def _get(path: str, params: Optional[dict] = None) -> dict:
    r = _request("GET", path, params=params or {})
    r.raise_for_status()
    return r.json()


def _post(path: str, payload: dict) -> dict:
    r = _request("POST", path, json=payload)
    if r.status_code not in (200, 201, 202):
        r.raise_for_status()
    return r.json()


def _put(path: str, payload: dict) -> dict:
    r = _request("PUT", path, json=payload)
    if r.status_code not in (200, 202):
        r.raise_for_status()
    return r.json()


def _patch(path: str, payload: dict) -> dict:
    r = _request("PATCH", path, json=payload)
    if r.status_code not in (200, 202):
        r.raise_for_status()
    return r.json()


def _delete(path: str, payload: Optional[dict] = None) -> Any:
    kwargs = {"json": payload} if payload else {}
    r = _request("DELETE", path, **kwargs)
    if r.status_code == 204:
        return {}
    if r.status_code not in (200, 202):
        r.raise_for_status()
    try:
        return r.json()
    except Exception:
        return {}


def _paginate(path: str, params: dict, limit: int = 0) -> list:
    """Fetch all items across pages, respecting optional hard limit."""
    items = []
    page_token: Optional[str] = None

    while True:
        p = dict(params)
        if page_token:
            p["pageToken"] = page_token

        data = _get(path, p)
        batch = data.get("items", [])
        items.extend(batch)

        if limit and len(items) >= limit:
            return items[:limit]

        page_token = data.get("nextPageToken")
        if not page_token:
            break
        time.sleep(0.3)

    return items


# ---------------------------------------------------------------------------
# DOCS
# ---------------------------------------------------------------------------

@mcp.tool(name="coda_list_docs", annotations={"readOnlyHint": True})
def coda_list_docs(query: str = "", limit: int = 100) -> str:
    """
    List accessible Coda docs.

    Args:
        query: Optional search string to filter docs by name.
        limit: Max docs to return (0 = all, default 100).

    Returns:
        JSON list of docs: [{id, name, browserLink, createdAt, updatedAt, owner}]
    """
    params: dict = {"limit": min(limit or 200, 200)}
    if query:
        params["query"] = query

    items = _paginate("/docs", params, limit=limit)
    result = [
        {
            "id": d["id"],
            "name": d["name"],
            "browserLink": d.get("browserLink", ""),
            "createdAt": d.get("createdAt", ""),
            "updatedAt": d.get("updatedAt", ""),
            "owner": d.get("owner", ""),
        }
        for d in items
    ]
    return json.dumps(result, ensure_ascii=False, indent=2)


@mcp.tool(name="coda_get_doc", annotations={"readOnlyHint": True})
def coda_get_doc(doc_id: str) -> str:
    """
    Get metadata for a specific Coda doc.

    Args:
        doc_id: The doc ID (e.g. "AAMBGrHiFm").

    Returns:
        JSON doc metadata including name, browserLink, icon, stats.
    """
    return json.dumps(_get(f"/docs/{doc_id}"), ensure_ascii=False, indent=2)


@mcp.tool(name="coda_create_doc", annotations={"readOnlyHint": False})
def coda_create_doc(title: str, source_doc_id: str = "", timezone: str = "") -> str:
    """
    Create a new Coda doc (optionally by copying an existing one).

    Args:
        title:         Title of the new doc.
        source_doc_id: Optional doc ID to copy from.
        timezone:      Optional IANA timezone string (e.g. "America/New_York").

    Returns:
        JSON with the new doc's id, name, browserLink.
    """
    payload: dict = {"title": title}
    if source_doc_id:
        payload["sourceDoc"] = source_doc_id
    if timezone:
        payload["timezone"] = timezone
    data = _post("/docs", payload)
    return json.dumps(data, ensure_ascii=False, indent=2)


@mcp.tool(name="coda_delete_doc", annotations={"readOnlyHint": False, "destructiveHint": True})
def coda_delete_doc(doc_id: str) -> str:
    """
    ⚠️ PERMANENTLY delete a Coda doc. This cannot be undone.

    Args:
        doc_id: The doc ID.

    Returns:
        JSON confirmation.
    """
    data = _delete(f"/docs/{doc_id}")
    return json.dumps({"deleted": doc_id, "response": data}, ensure_ascii=False, indent=2)


# ---------------------------------------------------------------------------
# PAGES
# ---------------------------------------------------------------------------

@mcp.tool(name="coda_list_pages", annotations={"readOnlyHint": True})
def coda_list_pages(doc_id: str) -> str:
    """
    List all pages (and sub-pages) in a Coda doc.

    Args:
        doc_id: The doc ID.

    Returns:
        JSON list of pages: [{id, name, subtitle, browserLink, isHidden, contentType, parent}]
    """
    items = _paginate(f"/docs/{doc_id}/pages", {"limit": 100})
    result = [
        {
            "id": p["id"],
            "name": p["name"],
            "subtitle": p.get("subtitle", ""),
            "browserLink": p.get("browserLink", ""),
            "isHidden": p.get("isHidden", False),
            "contentType": p.get("contentType", ""),
            "parent": p.get("parent", {}).get("id", "") if p.get("parent") else "",
        }
        for p in items
    ]
    return json.dumps(result, ensure_ascii=False, indent=2)


@mcp.tool(name="coda_get_page", annotations={"readOnlyHint": True})
def coda_get_page(doc_id: str, page_id: str) -> str:
    """
    Get metadata for a specific page.

    Args:
        doc_id:  The doc ID.
        page_id: The page ID or page URL name.

    Returns:
        JSON page metadata.
    """
    return json.dumps(_get(f"/docs/{doc_id}/pages/{page_id}"), ensure_ascii=False, indent=2)


@mcp.tool(name="coda_create_page", annotations={"readOnlyHint": False})
def coda_create_page(
    doc_id: str,
    name: str,
    subtitle: str = "",
    parent_page_id: str = "",
    page_content: str = "",
    content_type: str = "canvas",
) -> str:
    """
    Create a new page in a Coda doc.

    Args:
        doc_id:         The doc ID.
        name:           Page title.
        subtitle:       Optional subtitle.
        parent_page_id: Optional parent page ID (for sub-pages).
        page_content:   Optional initial content (Markdown supported for canvas pages).
        content_type:   "canvas" (default) or "embed".

    Returns:
        JSON with new page id, name, browserLink.
    """
    payload: dict = {"name": name, "contentType": content_type}
    if subtitle:
        payload["subtitle"] = subtitle
    if parent_page_id:
        payload["parentPageId"] = parent_page_id
    if page_content:
        payload["pageContent"] = {"type": "canvas", "canvasContent": {"format": "markdown", "content": page_content}}
    data = _post(f"/docs/{doc_id}/pages", payload)
    return json.dumps(data, ensure_ascii=False, indent=2)


@mcp.tool(name="coda_update_page", annotations={"readOnlyHint": False})
def coda_update_page(
    doc_id: str,
    page_id: str,
    name: str = "",
    subtitle: str = "",
    is_hidden: Optional[bool] = None,
) -> str:
    """
    Update a page's name, subtitle, or visibility.

    Args:
        doc_id:    The doc ID.
        page_id:   The page ID.
        name:      New page title (leave blank to keep current).
        subtitle:  New subtitle (leave blank to keep current).
        is_hidden: Set True to hide, False to show (None = no change).

    Returns:
        JSON updated page metadata.
    """
    payload: dict = {}
    if name:
        payload["name"] = name
    if subtitle:
        payload["subtitle"] = subtitle
    if is_hidden is not None:
        payload["isHidden"] = is_hidden
    data = _put(f"/docs/{doc_id}/pages/{page_id}", payload)
    return json.dumps(data, ensure_ascii=False, indent=2)


@mcp.tool(name="coda_delete_page", annotations={"readOnlyHint": False, "destructiveHint": True})
def coda_delete_page(doc_id: str, page_id: str) -> str:
    """
    ⚠️ Delete a page from a Coda doc.

    Args:
        doc_id:  The doc ID.
        page_id: The page ID.

    Returns:
        JSON confirmation.
    """
    data = _delete(f"/docs/{doc_id}/pages/{page_id}")
    return json.dumps({"deleted": page_id, "response": data}, ensure_ascii=False, indent=2)


@mcp.tool(name="coda_export_page_content", annotations={"readOnlyHint": True})
def coda_export_page_content(doc_id: str, page_id: str, output_format: str = "markdown") -> str:
    """
    Export the content of a canvas page as Markdown, HTML, or plain text.

    This kicks off an async export job and polls until complete.

    Args:
        doc_id:        The doc ID.
        page_id:       The page ID.
        output_format: "markdown" (default), "html", or "pdf".

    Returns:
        JSON with {"format": ..., "downloadLink": ..., "content": ...} for markdown/html,
        or {"format": "pdf", "downloadLink": "..."} for PDF (download separately).
    """
    payload = {"outputFormat": output_format}
    job = _post(f"/docs/{doc_id}/pages/{page_id}/export", payload)
    request_id = job.get("id", "")
    if not request_id:
        return json.dumps(job, ensure_ascii=False, indent=2)

    # Poll for completion
    for _ in range(30):
        time.sleep(2)
        status = _get(f"/docs/{doc_id}/pages/{page_id}/export/{request_id}")
        if status.get("status") == "complete":
            link = status.get("downloadLink", "")
            result: dict = {"format": output_format, "downloadLink": link}
            if output_format in ("markdown", "html") and link:
                try:
                    r = httpx.get(link, timeout=30)
                    result["content"] = r.text
                except Exception:
                    pass
            return json.dumps(result, ensure_ascii=False, indent=2)
        if status.get("status") == "failed":
            return json.dumps({"error": "Export failed", "details": status}, ensure_ascii=False, indent=2)

    return json.dumps({"error": "Export timed out after 60s", "last_status": status}, ensure_ascii=False, indent=2)


# ---------------------------------------------------------------------------
# TABLES
# ---------------------------------------------------------------------------

@mcp.tool(name="coda_list_tables", annotations={"readOnlyHint": True})
def coda_list_tables(doc_id: str, table_type: str = "") -> str:
    """
    List all tables and views in a Coda doc.

    Args:
        doc_id:     The doc ID.
        table_type: Filter by type: "" (all), "table", "view", "childTable".

    Returns:
        JSON list: [{id, name, tableType, rowCount, createdAt, updatedAt}]
    """
    params: dict = {"limit": 100}
    if table_type:
        params["tableTypes"] = table_type
    items = _paginate(f"/docs/{doc_id}/tables", params)
    result = [
        {
            "id": t["id"],
            "name": t["name"],
            "tableType": t.get("tableType", ""),
            "rowCount": t.get("rowCount", 0),
            "createdAt": t.get("createdAt", ""),
            "updatedAt": t.get("updatedAt", ""),
        }
        for t in items
    ]
    return json.dumps(result, ensure_ascii=False, indent=2)


@mcp.tool(name="coda_get_table", annotations={"readOnlyHint": True})
def coda_get_table(doc_id: str, table_id: str) -> str:
    """
    Get metadata for a specific table.

    Args:
        doc_id:   The doc ID.
        table_id: The table ID or name.

    Returns:
        JSON table metadata including rowCount, columns summary, filter, sort info.
    """
    return json.dumps(_get(f"/docs/{doc_id}/tables/{table_id}"), ensure_ascii=False, indent=2)


# ---------------------------------------------------------------------------
# COLUMNS
# ---------------------------------------------------------------------------

@mcp.tool(name="coda_list_columns", annotations={"readOnlyHint": True})
def coda_list_columns(doc_id: str, table_id: str) -> str:
    """
    List all columns in a Coda table with their stable IDs and format types.

    Args:
        doc_id:   The doc ID.
        table_id: The table ID.

    Returns:
        JSON list: [{id, name, format, display, calculated}]
        Use column IDs (not names) in all write operations.
    """
    items = _paginate(f"/docs/{doc_id}/tables/{table_id}/columns", {"limit": 200})
    result = [
        {
            "id": c["id"],
            "name": c["name"],
            "format": c.get("format", {}).get("type", ""),
            "display": c.get("display", False),
            "calculated": c.get("calculated", False),
        }
        for c in items
    ]
    return json.dumps(result, ensure_ascii=False, indent=2)


@mcp.tool(name="coda_get_column", annotations={"readOnlyHint": True})
def coda_get_column(doc_id: str, table_id: str, column_id: str) -> str:
    """
    Get detailed metadata for a specific column, including its formula if calculated.

    Args:
        doc_id:    The doc ID.
        table_id:  The table ID.
        column_id: The column ID or name.

    Returns:
        JSON column detail.
    """
    return json.dumps(
        _get(f"/docs/{doc_id}/tables/{table_id}/columns/{column_id}"),
        ensure_ascii=False, indent=2,
    )


@mcp.tool(name="coda_delete_column", annotations={"readOnlyHint": False, "destructiveHint": True})
def coda_delete_column(doc_id: str, table_id: str, column_id: str) -> str:
    """
    ⚠️ Permanently delete a column and all its data.

    Args:
        doc_id:    The doc ID.
        table_id:  The table ID.
        column_id: The column ID.

    Returns:
        JSON confirmation.
    """
    data = _delete(f"/docs/{doc_id}/tables/{table_id}/columns/{column_id}")
    return json.dumps({"deleted": column_id, "response": data}, ensure_ascii=False, indent=2)


# ---------------------------------------------------------------------------
# ROWS — read
# ---------------------------------------------------------------------------

@mcp.tool(name="coda_fetch_rows", annotations={"readOnlyHint": True})
def coda_fetch_rows(
    doc_id: str,
    table_id: str,
    value_format: str = "simpleWithArrays",
    query: str = "",
    sort_by: str = "",
    visible_only: bool = False,
    limit: int = 0,
) -> str:
    """
    Fetch rows from a Coda table with automatic pagination.

    Args:
        doc_id:       The doc ID.
        table_id:     The table ID.
        value_format: "simple" | "simpleWithArrays" (default) | "rich".
                      "rich" returns objects with id+name for lookups.
        query:        Optional server-side search string (filters on display values).
        sort_by:      Sort order: "" (none), "natural", "id", "createdAt".
        visible_only: If True, only return columns visible in the table view.
        limit:        Max rows to return (0 = all rows).
                      ⚠️ Large tables may be slow — use limit for samples.

    Returns:
        JSON: {"total": N, "rows": [{id, name, index, values: {colId: val, ...}}]}
    """
    params: dict = {
        "limit": 500,
        "valueFormat": value_format,
        "useColumnNames": False,
    }
    if query:
        params["query"] = query
    if sort_by:
        params["sortBy"] = sort_by
    if visible_only:
        params["visibleOnly"] = "true"

    rows = _paginate(f"/docs/{doc_id}/tables/{table_id}/rows", params, limit=limit)
    return json.dumps({"total": len(rows), "rows": rows}, ensure_ascii=False, indent=2)


@mcp.tool(name="coda_get_row", annotations={"readOnlyHint": True})
def coda_get_row(
    doc_id: str,
    table_id: str,
    row_id: str,
    value_format: str = "simpleWithArrays",
) -> str:
    """
    Fetch a single row by its Coda row ID.

    Args:
        doc_id:       The doc ID.
        table_id:     The table ID.
        row_id:       The row ID (e.g. "i-abc123") or display value of the display column.
        value_format: "simple" | "simpleWithArrays" | "rich".

    Returns:
        JSON row object with id, name, values keyed by column ID.
    """
    data = _get(
        f"/docs/{doc_id}/tables/{table_id}/rows/{row_id}",
        {"valueFormat": value_format, "useColumnNames": False},
    )
    return json.dumps(data, ensure_ascii=False, indent=2)


# ---------------------------------------------------------------------------
# ROWS — write
# ---------------------------------------------------------------------------

@mcp.tool(
    name="coda_upsert_rows",
    annotations={"readOnlyHint": False, "destructiveHint": False, "idempotentHint": True},
)
def coda_upsert_rows(
    doc_id: str,
    table_id: str,
    rows: list[dict],
    key_column_ids: list[str],
    batch_size: int = 100,
    delay_between_batches: float = 6.0,
) -> str:
    """
    *** PREFERRED bulk write method. ***
    Upsert rows into a Coda table using keyColumns (insert or update by key).

    Sends ≤100 rows per POST /rows?keyColumns=… call. Rows matching an existing
    key are updated; non-matching rows are inserted. Never call coda_update_row
    in a loop for >5 rows — use this instead.

    Args:
        doc_id:                The doc ID.
        table_id:              The table ID.
        rows:                  List of row dicts, each with a "cells" key:
                               [{"cells": [{"column": "c-colId", "value": "..."}]}]
        key_column_ids:        Column IDs to match on for upsert.
                               E.g. ["c-3inXE8XQBX"]
        batch_size:            Rows per API call (max 100).
        delay_between_batches: Seconds between batches (default 6s).

    Returns:
        JSON: {"submitted": N, "batches": B, "errors": [...]}
    """
    batch_size = min(batch_size, 100)
    total = len(rows)
    batches_sent = 0
    errors = []

    for i in range(0, total, batch_size):
        batch = rows[i: i + batch_size]
        try:
            _post(f"/docs/{doc_id}/tables/{table_id}/rows", {
                "rows": batch,
                "keyColumns": key_column_ids,
            })
            batches_sent += 1
        except Exception as e:
            errors.append({"batch_start": i, "error": str(e)})

        if i + batch_size < total:
            time.sleep(delay_between_batches)

    return json.dumps({"submitted": total, "batches": batches_sent, "errors": errors}, ensure_ascii=False, indent=2)


@mcp.tool(
    name="coda_insert_rows",
    annotations={"readOnlyHint": False, "destructiveHint": False, "idempotentHint": False},
)
def coda_insert_rows(
    doc_id: str,
    table_id: str,
    rows: list[dict],
    batch_size: int = 100,
    delay_between_batches: float = 6.0,
) -> str:
    """
    Insert new rows (always creates new, no deduplication).

    Use coda_upsert_rows when you want update-or-insert semantics.

    Args:
        doc_id:    The doc ID.
        table_id:  The table ID.
        rows:      [{"cells": [{"column": "c-colId", "value": "..."}]}]
        batch_size:            Rows per API call (max 100).
        delay_between_batches: Sleep between batches.

    Returns:
        JSON: {"submitted": N, "batches": B, "errors": [...]}
    """
    batch_size = min(batch_size, 100)
    total = len(rows)
    batches_sent = 0
    errors = []

    for i in range(0, total, batch_size):
        batch = rows[i: i + batch_size]
        try:
            _post(f"/docs/{doc_id}/tables/{table_id}/rows", {"rows": batch})
            batches_sent += 1
        except Exception as e:
            errors.append({"batch_start": i, "error": str(e)})

        if i + batch_size < total:
            time.sleep(delay_between_batches)

    return json.dumps({"submitted": total, "batches": batches_sent, "errors": errors}, ensure_ascii=False, indent=2)


@mcp.tool(
    name="coda_update_row",
    annotations={"readOnlyHint": False, "destructiveHint": False, "idempotentHint": True},
)
def coda_update_row(
    doc_id: str,
    table_id: str,
    row_id: str,
    cells: list[dict],
) -> str:
    """
    Update a single row by its Coda row ID.

    ⚠️ Do NOT call in a loop for >5 rows — use coda_upsert_rows instead.

    Args:
        doc_id:   The doc ID.
        table_id: The table ID.
        row_id:   The row ID (e.g. "i-abc123").
        cells:    [{"column": "c-colId", "value": "..."}]

    Returns:
        JSON confirmation.
    """
    data = _put(f"/docs/{doc_id}/tables/{table_id}/rows/{row_id}", {"row": {"cells": cells}})
    return json.dumps(data, ensure_ascii=False, indent=2)


@mcp.tool(
    name="coda_delete_rows",
    annotations={"readOnlyHint": False, "destructiveHint": True, "idempotentHint": True},
)
def coda_delete_rows(
    doc_id: str,
    table_id: str,
    row_ids: list[str],
) -> str:
    """
    Bulk-delete rows by their Coda row IDs (uses the batch DELETE endpoint).

    Args:
        doc_id:   The doc ID.
        table_id: The table ID.
        row_ids:  List of row IDs to delete (e.g. ["i-abc123", "i-def456"]).
                  Up to 100 IDs per call; this tool auto-batches.

    Returns:
        JSON: {"deleted": N, "errors": [...]}
    """
    batch_size = 100
    deleted = 0
    errors = []

    for i in range(0, len(row_ids), batch_size):
        batch = row_ids[i: i + batch_size]
        try:
            _delete(f"/docs/{doc_id}/tables/{table_id}/rows", payload={"rowIds": batch})
            deleted += len(batch)
        except Exception as e:
            errors.append({"batch_start": i, "error": str(e)})
        if i + batch_size < len(row_ids):
            time.sleep(1)

    return json.dumps({"deleted": deleted, "errors": errors}, ensure_ascii=False, indent=2)


@mcp.tool(
    name="coda_delete_rows_by_query",
    annotations={"readOnlyHint": False, "destructiveHint": True, "idempotentHint": False},
)
def coda_delete_rows_by_query(
    doc_id: str,
    table_id: str,
    query: str,
    dry_run: bool = True,
) -> str:
    """
    Delete all rows matching a search query.

    ⚠️ Use dry_run=True first to preview what will be deleted.

    Args:
        doc_id:   The doc ID.
        table_id: The table ID.
        query:    Server-side search string (filters on display values).
        dry_run:  If True (default), only return matching rows without deleting.

    Returns:
        If dry_run=True: JSON list of matching rows.
        If dry_run=False: JSON: {"deleted": N, "errors": [...]}
    """
    params = {"limit": 500, "query": query, "useColumnNames": False, "valueFormat": "simple"}
    rows = _paginate(f"/docs/{doc_id}/tables/{table_id}/rows", params)
    ids = [r["id"] for r in rows]

    if dry_run:
        return json.dumps({"dry_run": True, "matches": len(ids), "row_ids": ids}, ensure_ascii=False, indent=2)

    # Actually delete
    batch_size = 100
    deleted = 0
    errors = []
    for i in range(0, len(ids), batch_size):
        batch = ids[i: i + batch_size]
        try:
            _delete(f"/docs/{doc_id}/tables/{table_id}/rows", payload={"rowIds": batch})
            deleted += len(batch)
        except Exception as e:
            errors.append({"batch_start": i, "error": str(e)})
        if i + batch_size < len(ids):
            time.sleep(1)

    return json.dumps({"deleted": deleted, "total_matched": len(ids), "errors": errors}, ensure_ascii=False, indent=2)


# ---------------------------------------------------------------------------
# BUTTONS
# ---------------------------------------------------------------------------

@mcp.tool(name="coda_push_button", annotations={"readOnlyHint": False})
def coda_push_button(
    doc_id: str,
    table_id: str,
    row_id: str,
    column_id: str,
) -> str:
    """
    Push (trigger) a button in a specific cell.

    Args:
        doc_id:    The doc ID.
        table_id:  The table ID.
        row_id:    The row ID.
        column_id: The column ID of the button column.

    Returns:
        JSON confirmation from Coda.
    """
    data = _post(
        f"/docs/{doc_id}/tables/{table_id}/rows/{row_id}/buttons/{column_id}",
        {},
    )
    return json.dumps(data, ensure_ascii=False, indent=2)


# ---------------------------------------------------------------------------
# FORMULAS
# ---------------------------------------------------------------------------

@mcp.tool(name="coda_list_formulas", annotations={"readOnlyHint": True})
def coda_list_formulas(doc_id: str) -> str:
    """
    List all named formulas in a Coda doc.

    Args:
        doc_id: The doc ID.

    Returns:
        JSON list of formulas: [{id, name, type}]
    """
    items = _paginate(f"/docs/{doc_id}/formulas", {"limit": 100})
    return json.dumps(items, ensure_ascii=False, indent=2)


@mcp.tool(name="coda_get_formula", annotations={"readOnlyHint": True})
def coda_get_formula(doc_id: str, formula_id: str) -> str:
    """
    Get a specific named formula and its current value.

    Args:
        doc_id:     The doc ID.
        formula_id: The formula ID or name.

    Returns:
        JSON formula detail including current value.
    """
    return json.dumps(
        _get(f"/docs/{doc_id}/formulas/{formula_id}"),
        ensure_ascii=False, indent=2,
    )


# ---------------------------------------------------------------------------
# CONTROLS
# ---------------------------------------------------------------------------

@mcp.tool(name="coda_list_controls", annotations={"readOnlyHint": True})
def coda_list_controls(doc_id: str) -> str:
    """
    List all controls (sliders, date pickers, dropdowns, etc.) in a Coda doc.

    Args:
        doc_id: The doc ID.

    Returns:
        JSON list of controls: [{id, name, controlType, current_value}]
    """
    items = _paginate(f"/docs/{doc_id}/controls", {"limit": 100})
    return json.dumps(items, ensure_ascii=False, indent=2)


@mcp.tool(name="coda_get_control", annotations={"readOnlyHint": True})
def coda_get_control(doc_id: str, control_id: str) -> str:
    """
    Get a specific control and its current value.

    Args:
        doc_id:     The doc ID.
        control_id: The control ID or name.

    Returns:
        JSON control detail including current value and allowed values.
    """
    return json.dumps(
        _get(f"/docs/{doc_id}/controls/{control_id}"),
        ensure_ascii=False, indent=2,
    )


# ---------------------------------------------------------------------------
# AUTOMATIONS
# ---------------------------------------------------------------------------

@mcp.tool(name="coda_trigger_automation", annotations={"readOnlyHint": False})
def coda_trigger_automation(doc_id: str, rule_id: str) -> str:
    """
    Manually trigger an automation rule in a Coda doc.

    The rule must have "When manually triggered" set as its trigger in Coda.

    Args:
        doc_id:  The doc ID.
        rule_id: The automation rule ID (find with the Coda UI or from the doc URL).

    Returns:
        JSON confirmation.
    """
    data = _post(f"/docs/{doc_id}/hooks/automation/{rule_id}", {})
    return json.dumps(data, ensure_ascii=False, indent=2)


# ---------------------------------------------------------------------------
# PERMISSIONS (ACL)
# ---------------------------------------------------------------------------

@mcp.tool(name="coda_list_permissions", annotations={"readOnlyHint": True})
def coda_list_permissions(doc_id: str) -> str:
    """
    List all sharing permissions on a Coda doc.

    Args:
        doc_id: The doc ID.

    Returns:
        JSON list of permissions: [{id, access, principal: {type, email/teamId/...}}]
    """
    data = _get(f"/docs/{doc_id}/acl/permissions", {"limit": 100})
    return json.dumps(data, ensure_ascii=False, indent=2)


@mcp.tool(name="coda_add_permission", annotations={"readOnlyHint": False})
def coda_add_permission(
    doc_id: str,
    access_level: str,
    principal_type: str,
    principal_email: str = "",
    suppress_email: bool = True,
) -> str:
    """
    Grant access to a Coda doc.

    Args:
        doc_id:          The doc ID.
        access_level:    "readonly", "write", or "comment".
        principal_type:  "email", "anyone" (public link), or "domain".
        principal_email: Email address (required when principal_type = "email").
        suppress_email:  If True, don't send invitation email (default True).

    Returns:
        JSON confirmation.
    """
    principal: dict = {"type": principal_type}
    if principal_email:
        principal["email"] = principal_email

    payload = {
        "access": access_level,
        "principal": principal,
        "suppressEmail": suppress_email,
    }
    data = _post(f"/docs/{doc_id}/acl/permissions", payload)
    return json.dumps(data, ensure_ascii=False, indent=2)


@mcp.tool(name="coda_delete_permission", annotations={"readOnlyHint": False, "destructiveHint": True})
def coda_delete_permission(doc_id: str, permission_id: str) -> str:
    """
    Remove a sharing permission from a Coda doc.

    Args:
        doc_id:        The doc ID.
        permission_id: The permission ID (from coda_list_permissions).

    Returns:
        JSON confirmation.
    """
    data = _delete(f"/docs/{doc_id}/acl/permissions/{permission_id}")
    return json.dumps({"deleted": permission_id, "response": data}, ensure_ascii=False, indent=2)


# ---------------------------------------------------------------------------
# ANALYTICS
# ---------------------------------------------------------------------------

@mcp.tool(name="coda_get_doc_analytics", annotations={"readOnlyHint": True})
def coda_get_doc_analytics(doc_id: str, since: str = "", until: str = "") -> str:
    """
    Get view/edit analytics for a Coda doc.

    Args:
        doc_id: The doc ID.
        since:  Start date in ISO 8601 (e.g. "2024-01-01"). Optional.
        until:  End date in ISO 8601. Optional.

    Returns:
        JSON analytics data with daily view/edit counts.
    """
    params: dict = {"isPublisher": False}
    if since:
        params["sinceDate"] = since
    if until:
        params["untilDate"] = until
    data = _get(f"/analytics/docs/{doc_id}/pages", params)
    return json.dumps(data, ensure_ascii=False, indent=2)


@mcp.tool(name="coda_get_row_analytics", annotations={"readOnlyHint": True})
def coda_get_row_analytics(
    doc_id: str,
    table_id: str,
    since: str = "",
    until: str = "",
    limit: int = 100,
) -> str:
    """
    Get row-level analytics (views per row) for a Coda table.

    Args:
        doc_id:   The doc ID.
        table_id: The table ID.
        since:    Start date in ISO 8601. Optional.
        until:    End date in ISO 8601. Optional.
        limit:    Max rows to return (default 100).

    Returns:
        JSON list of rows with their view counts.
    """
    params: dict = {"limit": min(limit, 500)}
    if since:
        params["sinceDate"] = since
    if until:
        params["untilDate"] = until
    items = _paginate(f"/analytics/docs/{doc_id}/tables/{table_id}/rows", params, limit=limit)
    return json.dumps({"total": len(items), "rows": items}, ensure_ascii=False, indent=2)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
