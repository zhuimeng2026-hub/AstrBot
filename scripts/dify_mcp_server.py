"""
MCP server that exposes Dify knowledge base as a tool.
Connects to Dify chat API and returns responses.
"""

import json
import os
import sys
from urllib.request import Request, urlopen

DIFY_BASE = os.environ.get("DIFY_BASE_URL", "http://lanes.ymxt.top:3000")
DIFY_KEY = os.environ.get("DIFY_API_KEY", "app-aMB5cKJpDqNxR3tV7yW8eU2hL4fG9bZ1")
PROXY = os.environ.get("HTTP_PROXY", os.environ.get("http_proxy", ""))


def dify_chat(query: str) -> str:
    """Send a query to Dify and return the answer."""
    url = f"{DIFY_BASE}/v1/chat-messages"
    headers = {
        "Authorization": f"Bearer {DIFY_KEY}",
        "Content-Type": "application/json",
    }
    body = json.dumps({
        "inputs": {},
        "query": query,
        "response_mode": "blocking",
        "user": "hermes-mcp",
    }).encode()

    req = Request(url, data=body, headers=headers, method="POST")
    try:
        with urlopen(req, timeout=60) as resp:
            data = json.loads(resp.read())
            return data.get("answer", "No answer returned.")
    except Exception as e:
        return f"Error querying Dify: {e}"


def handle_request(request):
    """Handle a single JSON-RPC request."""
    method = request.get("method")
    req_id = request.get("id")

    if method == "initialize":
        return {
            "jsonrpc": "2.0",
            "id": req_id,
            "result": {
                "protocolVersion": "2024-11-05",
                "capabilities": {"tools": {}},
                "serverInfo": {"name": "dify-kb", "version": "1.0.0"},
            },
        }

    if method == "tools/list":
        return {
            "jsonrpc": "2.0",
            "id": req_id,
            "result": {
                "tools": [
                    {
                        "name": "query_dify_kb",
                        "description": "Query the Dify knowledge base (抖音小店电商客服知识库). Use this for questions about Douyin store operations, customer service, orders, shipping, refunds, and platform rules.",
                        "inputSchema": {
                            "type": "object",
                            "properties": {
                                "query": {
                                    "type": "string",
                                    "description": "The question to ask the Dify knowledge base",
                                }
                            },
                            "required": ["query"],
                        },
                    }
                ]
            },
        }

    if method == "tools/call":
        args = request.get("params", {}).get("arguments", {})
        query = args.get("query", "")
        if not query:
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"content": [{"type": "text", "text": "Error: query is required"}], "isError": True},
            }
        answer = dify_chat(query)
        return {
            "jsonrpc": "2.0",
            "id": req_id,
            "result": {"content": [{"type": "text", "text": answer}]},
        }

    return {"jsonrpc": "2.0", "id": req_id, "error": {"code": -32601, "message": f"Unknown method: {method}"}}


def main():
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            request = json.loads(line)
            response = handle_request(request)
            print(json.dumps(response), flush=True)
        except json.JSONDecodeError:
            print(json.dumps({"jsonrpc": "2.0", "error": {"code": -32700, "message": "Parse error"}}), flush=True)


if __name__ == "__main__":
    main()
