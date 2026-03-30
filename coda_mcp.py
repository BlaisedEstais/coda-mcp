#!/usr/bin/env python3
"""
coda_mcp — MCP server for the Coda.io API.

Transport : stdio (local)
Auth      : CODA_API_TOKEN env var

Key design rules (from hard-won experience):
  - NEVER upsert row-by-row (rate-limit hell).
    Always use coda_upsert_rows which batches 100 rows per POST /rows?keyColumns=...
  - Always paginate — tables can have thousands of rows.
  - Always use column/table IDs, not display names (names drift; IDs are stable).
  - Respect Retry-After on 429s.

Install:
  pip install mcp httpx
  export CODA_API_TOKEN=your-token-here
  # then register as an MCP stdio server in claude_desktop_config.json / .mcp.json
"""

import asyncio
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
        "Tools for reading and writing Coda.io docs, tables and rows. "
        "Always prefer coda_upsert_rows for bulk writes — it batches 100 rows per "
        "request using keyColumns. Never iterate row-by-row with coda_update_row "
        "when writing more than ~5 rows; use coda_upsert_rows instead."
    ),
)

BASE_URL = "https://coda.io/apis/v1"


# ---------------------------------------------------------------------------
# HTTP client helpers
# ---------------------------------------------------------------------------

def _headers() -> dict:
    token = os.environ.get("CODA_API_TOKEN", "")
    if not token:
        raise RuntimeError("CODA_API_TOKEN environment variable is not set.")
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }


def _get(path: str, params: Optional[dict] = None) -> dict:
    """Synchronous GET with retry on 429."""
    for attempt in range(8):
        r = httpx.get(
            f"{BASE_URL}{path}",
            headers=_headers(),
            params=params or {},
            timeout=60,
        )
        if r.status_code == 429:
            wait = int(r.headers.get("Retry-After", "10")) + 2
            time.sleep(wait)
            continue
        r.raise_for_status()
        return r.json()
    raise RuntimeError(f"GET {path} failed after retries (last 429)")


def _post(path: str, payload: dict) -> dict:
    """Synchronous POST with retry on 429."""
    for attempt in range(8):
        r = httpx.post(
            f"{BASE_URL}{path}",
            headers=_headers(),
            json=payload,
            timeout=60,
        )
        if r.status_code == 429:
            wait = int(r.headers.get("Retry-After", "10")) + 2
            time.sleep(wait)
            continue
        if r.status_code in (200, 202):
            return r.json()
        r.raise_for_status()
    raise RuntimeError(f"POST {path} failed after retries (last 429)")


def _put(path: str, payload: dict) -> dict:
    for attempt in range(8):
        r = httpx.put(
            f"{BASE_URL}{path}",
            headers=_headers(),
            json=payload,
            timeout=60,
        )
        if r.status_code == 429:
            wait = int(r.headers.get("Retry-After", "10")) + 2
            time.sleep(wait)
            continue
        if r.status_code in (200, 202):
            return r.json()
        r.raise_for_status()
    raise RuntimeError(f"PUT {path} failed after retries (last 429)")


def _delete(path: str) -> dict:
    for attempt in range(8):
        r = httpx.delete(
            f"{BASE_URL}{path}",
            headers=_headers(),
            timeout=60,
        )
        if r.status_code == 429:
            wait = int(r.headers.get("Retry-After", "10")) + 2
            time.sleep(wait)
            continue
        if r.status_code in (200, 202):
            return r.json()
        r.raise_for_status()
    raise RuntimeError(f"DELETE {path} failed after retries (last 429)")


# ---------------------------------------------------------------------------
# Docs
# ---------------------------------------------------------------------------

@mcp.tool(
    name="coda_list_docs",
    annotations={"readOnlyHint": True},
)
def coda_list_docs(query: str = "") -> str:
    """
    List accessible Coda docs.

    Args:
        query: Optional search string to filter docs by name.

    Returns:
        JSON list of docs with id, name, browserLink.
    """
    params = {"limit": 50}
    if query:
        params["query"] = query
    data = _get("/docs", params)
    docs = [
        {"id": d["id"], "name": d["name"], "browserLink": d.get("browserLink", "")}
        for d in data.get("items", [])
    ]
    return json.dumps(docs, ensure_ascii=False, indent=2)


@mcp.tool(
    name="coda_get_doc",
    annotations={"readOnlyHint": True},
)
def coda_get_doc(doc_id: str) -> str:
    """
    Get metadata for a specific Coda doc.

    Args:
        doc_id: The doc ID (e.g. "AAMBGrHiFm").

    Returns:
        JSON with doc metadata.
    """
    data = _get(f"/docs/{doc_id}")
    return json.dumps(data, ensure_ascii=False, indent=2)


# ---------------------------------------------------------------------------
# Tables & columns
# ---------------------------------------------------------------------------

@mcp.tool(
    name="coda_list_tables",
    annotations={"readOnlyHint": True},
)
def coda_list_tables(doc_id: str) -> str:
    """
    List all tables (and views) in a Coda doc.

    Args:
        doc_id: The doc ID.

    Returns:
        JSON list of tables with id, name, tableType, rowCount.
    """
    data = _get(f"/docs/{doc_id}/tables", {"limit": 100})
    tables = [
        {
            "id": t["id"],
            "name": t["name"],
            "tableType": t.get("tableType", ""),
            "rowCount": t.get("rowCount", 0),
        }
        for t in data.get("items", [])
    ]
    return json.dumps(tables, ensure_ascii=False, indent=2)


@mcp.tool(
    name="coda_list_columns",
    annotations={"readOnlyHint": True},
)
def coda_list_columns(doc_id: str, table_id: str) -> str:
    """
    List columns in a Coda table, including their stable IDs.

    Args:
        doc_id:   The doc ID.
        table_id: The table ID (e.g. "grid-qwNymJesmG").

    Returns:
        JSON list of columns with id, name, format type.
        Use column IDs (not names) in all write operations — names can change, IDs are stable.
    """
    data = _get(f"/docs/{doc_id}/tables/{table_id}/columns", {"limit": 200})
    cols = [
        {
            "id": c["id"],
            "name": c["name"],
            "format": c.get("format", {}).get("type", ""),
        }
        for c in data.get("items", [])
    ]
    return json.dumps(cols, ensure_ascii=False, indent=2)


# ---------------------------------------------------------------------------
# Rows — read
# ---------------------------------------------------------------------------

@mcp.tool(
    name="coda_fetch_rows",
    annotations={"readOnlyHint": True},
)
def coda_fetch_rows(
    doc_id: str,
    table_id: str,
    value_format: str = "simpleWithArrays",
    query: str = "",
    limit: int = 0,
) -> str:
    """
    Fetch rows from a Coda table with automatic pagination.

    This function handles multi-page results transparently.
    Use value_format="rich" if you need resolved lookup labels alongside IDs.

    Args:
        doc_id:       The doc ID.
        table_id:     The table ID.
        value_format: "simple" | "simpleWithArrays" (default) | "rich".
                      "simpleWithArrays" returns arrays for multi-select / lookup columns.
                      "rich" returns objects with id + name for lookup columns.
        query:        Optional server-side search string (filters on display values).
        limit:        Max rows to return (0 = all). Fetching all rows in large tables
                      can take many seconds — use a limit if you only need a sample.

    Returns:
        JSON object with {"total": N, "rows": [...]}
        Each row has "id", "name", "index", and "values" (keyed by column ID).
    """
    all_rows: list = []
    page_token: Optional[str] = None
    per_page = 500

    while True:
        params: dict = {
            "limit": per_page,
            "valueFormat": value_format,
            "useColumnNames": False,
        }
        if page_token:
            params["pageToken"] = page_token
        if query:
            params["query"] = query

        data = _get(f"/docs/{doc_id}/tables/{table_id}/rows", params)
        items = data.get("items", [])
        all_rows.extend(items)

        if limit and len(all_rows) >= limit:
            all_rows = all_rows[:limit]
            break

        page_token = data.get("nextPageToken")
        if not page_token:
            break
        time.sleep(0.5)  # be polite

    return json.dumps(
        {"total": len(all_rows), "rows": all_rows},
        ensure_ascii=False,
        indent=2,
    )


@mcp.tool(
    name="coda_get_row",
    annotations={"readOnlyHint": True},
)
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
        row_id:       The row ID (e.g. "i-abc123").
        value_format: "simple" | "simpleWithArrays" | "rich".

    Returns:
        JSON row object with id, name, values (keyed by column ID).
    """
    data = _get(
        f"/docs/{doc_id}/tables/{table_id}/rows/{row_id}",
        {"valueFormat": value_format, "useColumnNames": False},
    )
    return json.dumps(data, ensure_ascii=False, indent=2)


# ---------------------------------------------------------------------------
# Rows — write
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
    Bulk upsert rows into a Coda table using keyColumns.

    *** THIS IS THE CORRECT WAY TO WRITE AT SCALE. ***
    It sends up to 100 rows per API call (POST /rows with keyColumns).
    Each row that matches an existing key is updated; non-matching rows are inserted.

    DO NOT use coda_update_row in a loop for bulk operations — that hits rate limits
    hard and can make a 1000-row update take hours instead of minutes.

    Args:
        doc_id:                The doc ID.
        table_id:              The table ID.
        rows:                  List of row dicts. Each dict must have a "cells" key:
                               [{"column": "c-colId", "value": "..."}, ...]
                               Example:
                               [
                                 {"cells": [
                                   {"column": "c-3inXE8XQBX", "value": "DOSSIER1&COT1"},
                                   {"column": "c-K3rq-fpzbK", "value": "Some text"}
                                 ]}
                               ]
        key_column_ids:        List of column IDs to match on (upsert keys).
                               E.g. ["c-3inXE8XQBX"] to upsert on IDENTIFIANT UNIQUE COMPLET.
        batch_size:            Rows per API call (max 100, default 100).
        delay_between_batches: Seconds to sleep between batches (default 6s).
                               Increase if you see 429s; 6s is safe for most workloads.

    Returns:
        JSON summary: {"submitted": N, "batches": B, "errors": [...]}
    """
    batch_size = min(batch_size, 100)
    total = len(rows)
    batches_sent = 0
    errors = []

    for i in range(0, total, batch_size):
        batch = rows[i : i + batch_size]
        payload = {
            "rows": batch,
            "keyColumns": key_column_ids,
        }
        try:
            _post(f"/docs/{doc_id}/tables/{table_id}/rows", payload)
            batches_sent += 1
        except Exception as e:
            errors.append({"batch_start": i, "error": str(e)})

        if i + batch_size < total:
            time.sleep(delay_between_batches)

    return json.dumps(
        {
            "submitted": total,
            "batches": batches_sent,
            "errors": errors,
        },
        ensure_ascii=False,
        indent=2,
    )


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
    Insert new rows into a Coda table (no upsert key matching — always creates new rows).

    Use this when you know the rows don't exist yet and you don't need deduplication.
    For upsert semantics, use coda_upsert_rows instead.

    Args:
        doc_id:    The doc ID.
        table_id:  The table ID.
        rows:      List of row dicts, each with a "cells" key:
                   [{"cells": [{"column": "c-colId", "value": "..."}, ...]}]
        batch_size:            Rows per API call (max 100).
        delay_between_batches: Sleep seconds between batches.

    Returns:
        JSON summary: {"submitted": N, "batches": B, "errors": [...]}
    """
    batch_size = min(batch_size, 100)
    total = len(rows)
    batches_sent = 0
    errors = []

    for i in range(0, total, batch_size):
        batch = rows[i : i + batch_size]
        payload = {"rows": batch}
        try:
            _post(f"/docs/{doc_id}/tables/{table_id}/rows", payload)
            batches_sent += 1
        except Exception as e:
            errors.append({"batch_start": i, "error": str(e)})

        if i + batch_size < total:
            time.sleep(delay_between_batches)

    return json.dumps(
        {"submitted": total, "batches": batches_sent, "errors": errors},
        ensure_ascii=False,
        indent=2,
    )


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

    ⚠️  DO NOT call this in a loop for more than ~5 rows.
    For bulk updates, use coda_upsert_rows which batches 100 rows per call.

    Args:
        doc_id:   The doc ID.
        table_id: The table ID.
        row_id:   The row ID (e.g. "i-abc123").
        cells:    List of cell updates: [{"column": "c-colId", "value": "..."}, ...]

    Returns:
        JSON confirmation from Coda.
    """
    payload = {"row": {"cells": cells}}
    data = _put(f"/docs/{doc_id}/tables/{table_id}/rows/{row_id}", payload)
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
    Delete rows by their Coda row IDs.

    Deletes are sent one at a time (Coda API does not support bulk delete).
    Throttled automatically on 429.

    Args:
        doc_id:   The doc ID.
        table_id: The table ID.
        row_ids:  List of row IDs to delete (e.g. ["i-abc123", "i-def456"]).

    Returns:
        JSON summary: {"deleted": N, "errors": [...]}
    """
    deleted = 0
    errors = []
    for rid in row_ids:
        try:
            _delete(f"/docs/{doc_id}/tables/{table_id}/rows/{rid}")
            deleted += 1
        except Exception as e:
            errors.append({"row_id": rid, "error": str(e)})
        time.sleep(0.3)  # ~3 deletes/sec to stay under limits

    return json.dumps(
        {"deleted": deleted, "errors": errors},
        ensure_ascii=False,
        indent=2,
    )


# ---------------------------------------------------------------------------
# Formulas / resolve
# ---------------------------------------------------------------------------

@mcp.tool(
    name="coda_resolve_formula",
    annotations={"readOnlyHint": True},
)
def coda_resolve_formula(doc_id: str, formula: str) -> str:
    """
    Evaluate a Coda formula and return the result.

    Useful for resolving computed column values or checking doc-level formulas.

    Args:
        doc_id:  The doc ID.
        formula: A valid Coda formula string (e.g. "Tables()[1].RowCount()").

    Returns:
        JSON with the formula result.
    """
    data = _get(f"/docs/{doc_id}/formulas", {"formula": formula})
    return json.dumps(data, ensure_ascii=False, indent=2)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
