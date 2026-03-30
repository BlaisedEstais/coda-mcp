#!/usr/bin/env bash
# coda-mcp installer
# Usage: bash install.sh
# Run from anywhere — it auto-detects its own location.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPT_PATH="$SCRIPT_DIR/coda_mcp.py"
CONFIG_PATH="$HOME/Library/Application Support/Claude/claude_desktop_config.json"

echo "╔══════════════════════════════════════╗"
echo "║         coda-mcp installer           ║"
echo "╚══════════════════════════════════════╝"
echo ""

# ── 1. Python deps ────────────────────────────────────────────────────────────
echo "▶ Installing Python dependencies (mcp, httpx)..."
pip3 install -q mcp httpx
echo "  ✓ Done"
echo ""

# ── 2. Coda API token ─────────────────────────────────────────────────────────
if [ -n "${CODA_API_TOKEN:-}" ]; then
  TOKEN="$CODA_API_TOKEN"
  echo "  ✓ Using CODA_API_TOKEN from environment"
else
  echo "▶ Enter your Coda API token (coda.io/account → API settings):"
  read -r -s TOKEN
  echo ""
  if [ -z "$TOKEN" ]; then
    echo "  ✗ No token provided. Aborting."
    exit 1
  fi
  echo "  ✓ Token received"
fi
echo ""

# ── 3. Claude Desktop config ──────────────────────────────────────────────────
echo "▶ Updating Claude Desktop config at:"
echo "  $CONFIG_PATH"
mkdir -p "$(dirname "$CONFIG_PATH")"

# Read existing config or start fresh
if [ -f "$CONFIG_PATH" ]; then
  EXISTING=$(cat "$CONFIG_PATH")
else
  EXISTING='{}'
fi

# Inject the coda entry using Python (handles existing mcpServers gracefully)
python3 - <<PYEOF
import json, sys

config_path = """$CONFIG_PATH"""
script_path = """$SCRIPT_PATH"""
token = """$TOKEN"""
existing_raw = '''$EXISTING'''

try:
    config = json.loads(existing_raw)
except json.JSONDecodeError:
    config = {}

config.setdefault("mcpServers", {})
config["mcpServers"]["coda"] = {
    "command": "python3",
    "args": [script_path],
    "env": {
        "CODA_API_TOKEN": token
    }
}

with open(config_path, "w") as f:
    json.dump(config, f, indent=2)

print("  ✓ Config written")
PYEOF

echo ""
echo "╔══════════════════════════════════════╗"
echo "║          Installation complete!      ║"
echo "╚══════════════════════════════════════╝"
echo ""
echo "  Next step: restart Claude Desktop."
echo "  The 'coda' MCP will appear automatically."
echo ""
echo "  Tools available:"
echo "    coda_list_docs       coda_list_tables    coda_list_columns"
echo "    coda_fetch_rows      coda_get_row        coda_upsert_rows"
echo "    coda_insert_rows     coda_update_row     coda_delete_rows"
echo "    coda_resolve_formula coda_get_doc"
echo ""
echo "  ⚠  Token is stored in Claude Desktop config (plaintext)."
echo "     Treat that file like a .env — don't commit it."
