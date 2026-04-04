# Response Format & Error Handling

## SSE Response Format

The MCP server uses **Server-Sent Events (SSE)** for streaming responses. Each HTTP response contains one or more SSE events:

```
event: message
data: {"jsonrpc":"2.0","id":3,"result":{"content":[{"type":"text","text":"..."}]}}
```

### Parsing responses

1. Look for lines starting with `data: `
2. Parse the JSON after the `data: ` prefix
3. Extract `result.content[0].text` for the tool output
4. Optionally use `result.structuredContent.result` for pre-parsed structured data

Shell one-liner:

```bash
curl -s ... | grep '^data: ' | sed 's/^data: //' | jq '.result.content[0].text'
```

Python example:

```python
import httpx

resp = httpx.post(url, headers=headers, json=payload)
for line in resp.text.splitlines():
    if line.startswith("data: "):
        data = json.loads(line[6:])
        text = data["result"]["content"][0]["text"]
```

---

## Error Handling

### JSON-RPC protocol errors

If the JSON-RPC request itself is malformed, the response contains an `error` object instead of `result`:

```json
{
  "jsonrpc": "2.0",
  "id": 3,
  "error": {
    "code": -32602,
    "message": "Invalid params: ..."
  }
}
```

**Common error codes:**

| Code | Meaning |
|------|---------|
| `-32700` | Parse error — invalid JSON |
| `-32600` | Invalid request — missing required fields |
| `-32601` | Method not found |
| `-32602` | Invalid params |
| `-32603` | Internal error |

### Tool-level errors

Tools may return errors as text content with HTTP 200. Check the `isError` field or look for the `ERROR:` prefix:

```json
{
  "result": {
    "content": [{ "type": "text", "text": "ERROR: File not found: /bad/path.json" }],
    "isError": true
  }
}
```

### Read-only mode errors

Tools blocked by read-only mode (`deploy`, `deploy_manifest`, `list_remote_tables`, `list_remote_constraints`) return:

```json
{
  "result": {
    "content": [{ "type": "text", "text": "ERROR: 'deploy' is disabled because read-only mode is active. Toggle it off in the Wren Engine Web UI." }],
    "isError": true
  }
}
```

### Missing or expired session

If the `Mcp-Session-Id` header is missing or the session has expired, you will get an HTTP 400 response. Re-initialize the session to recover.
