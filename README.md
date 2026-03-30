# coda-mcp v2

Full-featured **MCP server** for the [Coda.io API](https://coda.io/developers/apis/v1), designed for use with Claude Desktop and any MCP client.

## Features (v2 — 33 tools)

| Category | Tools |
|---|---|
| **Docs** | `list_docs`, `get_doc`, `create_doc`, `delete_doc` |
| **Pages** | `list_pages`, `get_page`, `create_page`, `update_page`, `delete_page`, `export_page_content` |
| **Tables** | `list_tables`, `get_table` |
| **Columns** | `list_columns`, `get_column`, `delete_column` |
| **Rows** | `fetch_rows`, `get_row`, `upsert_rows`, `insert_rows`, `update_row`, `delete_rows`, `delete_rows_by_query` |
| **Buttons** | `push_button` |
| **Formulas** | `list_formulas`, `get_formula` |
| **Controls** | `list_controls`, `get_control` |
| **Automations** | `trigger_automation` |
| **Permissions** | `list_permissions`, `add_permission`, `delete_permission` |
| **Analytics** | `get_doc_analytics`, `get_row_analytics` |

## Key design principles

- **Never write row-by-row.** `coda_upsert_rows` batches 100 rows per POST — safe for thousands of rows.
- **Always paginate.** All read tools fetch all pages automatically.
- **Always use column IDs**, not display names (names drift; IDs are stable).
- **429 back-off.** All requests retry with `Retry-After` respect.

## Install

```bash
pip install mcp httpx
```

Or with pipx:

```bash
pipx install git+https://github.com/btdestais/coda-mcp
```

## Claude Desktop setup

Add to `~/Library/Application Support/Claude/claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "coda": {
      "command": "python3",
      "args": ["/path/to/coda_mcp.py"],
      "env": {
        "CODA_API_TOKEN": "your-token-here"
      }
    }
  }
}
```

Get your API token at: https://coda.io/account → **API tokens**

## Quick example (bulk upsert)

```python
# Via Claude / MCP:
coda_upsert_rows(
    doc_id="AAMBGrHiFm",
    table_id="grid-abc123",
    rows=[
        {"cells": [
            {"column": "c-name", "value": "Alice"},
            {"column": "c-score", "value": 98}
        ]},
        {"cells": [
            {"column": "c-name", "value": "Bob"},
            {"column": "c-score", "value": 75}
        ]},
    ],
    key_column_ids=["c-name"]  # upsert key
)
```

## What's new in v2

- **Pages**: full CRUD + content export (Markdown / HTML / PDF)
- **Docs**: create & delete
- **Tables**: `get_table` metadata
- **Columns**: `get_column`, `delete_column`
- **Rows**: bulk delete endpoint (no more one-by-one), `delete_rows_by_query` with dry-run
- **Buttons**: `push_button` to trigger button columns
- **Formulas**: `list_formulas`, `get_formula`
- **Controls**: `list_controls`, `get_control`
- **Automations**: `trigger_automation`
- **Permissions**: full ACL management
- **Analytics**: doc analytics + row analytics
- **Better HTTP client**: unified `_request()` with exponential back-off on all methods

## License

MIT
