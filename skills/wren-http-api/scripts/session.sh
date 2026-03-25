#!/usr/bin/env bash
# Initialize a Wren MCP JSON-RPC session and run a health check.
# Usage: bash session.sh [BASE_URL]
# Example: bash session.sh http://localhost:9000/mcp
set -euo pipefail

BASE_URL="${1:-http://localhost:9000/mcp}"
CURL_OPTS=(--silent --show-error --fail-with-body --connect-timeout 5 --max-time 30)
HEADERS=(-H "Content-Type: application/json" -H "Accept: application/json, text/event-stream")

echo "Initializing session at $BASE_URL ..."

# Step 1: Initialize and capture session ID
INIT_RESPONSE=$(curl "${CURL_OPTS[@]}" -D - "$BASE_URL" \
  "${HEADERS[@]}" \
  -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-03-26","capabilities":{},"clientInfo":{"name":"wren-http-skill","version":"1.0"}}}')

SESSION_ID=$(printf '%s\n' "$INIT_RESPONSE" \
  | awk -F': *' 'tolower($1)=="mcp-session-id"{print $2; exit}' \
  | tr -d '\r')

if [ -z "$SESSION_ID" ]; then
  echo "ERROR: Failed to obtain session ID. Server response:" >&2
  echo "$INIT_RESPONSE" >&2
  exit 1
fi

echo "Session ID: $SESSION_ID"

# Step 2: Complete handshake
curl "${CURL_OPTS[@]}" "$BASE_URL" \
  "${HEADERS[@]}" \
  -H "Mcp-Session-Id: $SESSION_ID" \
  -d '{"jsonrpc":"2.0","method":"notifications/initialized"}' > /dev/null

echo "Handshake complete."

# Step 3: Health check
echo ""
echo "Running health_check ..."
HEALTH_RESPONSE=$(curl "${CURL_OPTS[@]}" "$BASE_URL" \
  "${HEADERS[@]}" \
  -H "Mcp-Session-Id: $SESSION_ID" \
  -d '{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"health_check","arguments":{}}}')

RESULT=$(printf '%s\n' "$HEALTH_RESPONSE" \
  | sed -n 's/^data: //p' \
  | tail -n 1)

if [ -z "$RESULT" ]; then
  echo "WARNING: No data received from health_check. Raw response:" >&2
  echo "$HEALTH_RESPONSE" >&2
  exit 1
fi

if command -v python3 &>/dev/null; then
  echo "$RESULT" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['result']['content'][0]['text'])"
elif command -v jq &>/dev/null; then
  echo "$RESULT" | jq -r '.result.content[0].text'
else
  echo "$RESULT"
fi

echo ""
echo "Session is ready. Export for subsequent calls:"
echo "  export MCP_SESSION_ID=$SESSION_ID"
echo "  export MCP_BASE_URL=$BASE_URL"
