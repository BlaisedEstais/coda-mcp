# Agent instructions — coda-mcp

Public MCP server for the Coda.io API (33 tools), in a single file, `coda_mcp.py`. User-facing documentation: [README.md](README.md).

## Working on it
- Keep the design principles of the README: batch writes (100 rows per POST, never row by row), paginate every read, address columns by ID, honour `Retry-After` on 429.
- The API token comes from the `CODA_API_TOKEN` environment variable. Never commit a token, a doc ID from a private workspace or a real row of data (this repository is public).
- Destructive tools (`delete_doc`, `delete_page`, `delete_rows`, `delete_rows_by_query`, `delete_permission`) must stay explicit in their names and descriptions so an agent's guardrail can recognise them.
- Check syntax with `python3 -m py_compile coda_mcp.py` before committing; test against a throwaway Coda doc, never a real one.
- Commits in English, short imperative titles. This repository is only pulled automatically on the maintainer's Mac: commit and push your own changes.
