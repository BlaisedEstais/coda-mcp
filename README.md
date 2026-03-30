# coda-mcp

MCP server for the [Coda.io](https://coda.io) API. Gives any MCP-compatible client (Claude, Cowork, Claude Code…) direct read/write access to Coda tables.

Built from real production patterns — specifically avoids the row-by-row upsert trap that turns a 1000-row write into a multi-hour rate-limited nightmare.

---

## Quick install

```bash
# 1. Install dependencies
pip install mcp httpx

# 2. Set your API token (get it at coda.io/account → API settings)
export CODA_API_TOKEN=your-token-here

# 3. Test the server starts
python coda_mcp.py
# Should hang silently waiting on stdin — that's correct for stdio transport.
# Ctrl-C to exit.
```

---

## Register with Claude / Cowork

### Claude Code (`~/.claude/claude_code_config.json` or `.mcp.json` in your project)

```json
{
  "mcpServers": {
    "coda": {
      "command": "python",
      "args": ["/absolute/path/to/coda_mcp.py"],
      "env": {
        "CODA_API_TOKEN": "your-token-here"
      }
    }
  }
}
```

### Claude Desktop (`~/Library/Application Support/Claude/claude_desktop_config.json`)

Same format as above — just use the absolute path to `coda_mcp.py`.

---

## Available tools

| Tool | Purpose |
|---|---|
| `coda_list_docs` | List accessible docs (with optional name filter) |
| `coda_get_doc` | Get metadata for a specific doc |
| `coda_list_tables` | List all tables/views in a doc (with row counts) |
| `coda_list_columns` | List columns **with stable IDs** — always use IDs in writes, never display names |
| `coda_fetch_rows` | Fetch rows with auto-pagination. Supports `value_format=rich` for resolved lookups |
| `coda_get_row` | Fetch a single row by ID |
| `coda_upsert_rows` | **⭐ THE bulk write tool.** Batches 100 rows/call, uses `keyColumns` for upsert matching |
| `coda_insert_rows` | Insert new rows in bulk (no dedup) |
| `coda_update_row` | Update a single row by ID (for ≤5 rows only) |
| `coda_delete_rows` | Delete rows by ID list |
| `coda_resolve_formula` | Evaluate a Coda formula |

---

## The upsert rule — read this

Coda's API has two ways to write rows:

**❌ Row-by-row (slow — don't use for bulk):**
```
PUT /docs/{docId}/tables/{tableId}/rows/{rowId}   ← one call per row
```
With thousands of rows, this burns through rate limits in seconds and your job runs for hours.

**✅ Bulk upsert (fast — always use this):**
```
POST /docs/{docId}/tables/{tableId}/rows
  { "rows": [...up to 100 rows...], "keyColumns": ["c-colId"] }
```
100 rows per call, matched and upserted by the key columns you specify. A 5000-row job that would take hours row-by-row takes ~5 minutes with batches.

`coda_upsert_rows` always uses the bulk approach. The `key_column_ids` parameter tells Coda which column(s) to match on — typically `IDENTIFIANT UNIQUE COMPLET` (`c-3inXE8XQBX` in the Zita doc) for letter-table upserts.

---

## Example: upsert 3 rows in the Zita Lettres table

```python
# Ask Claude / call the tool directly:
coda_upsert_rows(
    doc_id="AAMBGrHiFm",
    table_id="grid-qwNymJesmG",
    rows=[
        {"cells": [
            {"column": "c-3inXE8XQBX", "value": "DOSSIER1&COT1"},
            {"column": "c-K3rq-fpzbK", "value": "Paris, Rome"}
        ]},
        {"cells": [
            {"column": "c-3inXE8XQBX", "value": "DOSSIER1&COT2"},
            {"column": "c-K3rq-fpzbK", "value": "Vienne"}
        ]},
    ],
    key_column_ids=["c-3inXE8XQBX"],
)
```

---

## Rate limits & tuning

- Default `delay_between_batches=6.0` seconds is safe for sustained workloads.
- For a one-shot burst (< 500 rows), you can lower to 3–4s.
- 429s are retried automatically using the `Retry-After` header.
- `coda_delete_rows` throttles at ~3 deletions/second (Coda doesn't support bulk delete).

---

## Publishing / sharing

This server follows the MCP stdio spec and has no external dependencies beyond `mcp` + `httpx`. To share it:

- **GitHub** — push as a public repo and others can `pip install` from it.
- **PyPI** — wrap in a `pyproject.toml` with `coda-mcp` as the package name, then `pip install coda-mcp`.
- **MCP registry** — submit to [mcp.so](https://mcp.so) or the Anthropic MCP directory once published on PyPI/GitHub.

The server is generic (no Zita-specific constants hardcoded) — doc IDs, table IDs, and column IDs are all passed as parameters.

---

## Notes on value_format

| Format | Best for |
|---|---|
| `simple` | Plain values, fastest |
| `simpleWithArrays` | Multi-select / lookup columns (returns arrays) — **use this as default** |
| `rich` | Returns `{id, name}` objects for lookup columns — use when you need both the row ID and the display label |
