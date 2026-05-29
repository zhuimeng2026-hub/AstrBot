"""
OpenAI-compatible API bridge for OpenClaw.
Wraps `openclaw agent` CLI as a /v1/chat/completions endpoint.
"""

import asyncio
import json
import os
import sys
import time
import uuid
from http.server import HTTPServer, BaseHTTPRequestHandler

PORT = int(os.environ.get("OPENCLAW_BRIDGE_PORT", "8643"))
OPENCLAW_BIN = os.environ.get("OPENCLAW_BIN", "/usr/bin/openclaw")
SESSION_MAP = {}  # track session IDs per conversation


def call_openclaw_agent(message: str, session_id: str | None = None, agent_id: str = "main") -> str:
    """Call openclaw agent CLI and return the response text."""
    cmd = [OPENCLAW_BIN, "agent", "--agent", agent_id, "--message", message, "--json"]
    if session_id:
        cmd.extend(["--session-id", session_id])
    cmd.extend(["--timeout", "120"])

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=130)
        # OpenClaw outputs JSON to stderr, not stdout
        output = result.stderr or result.stdout
        # Parse the full output as a stream of JSON objects
        # OpenClaw outputs multiple JSON objects; find the one with "payloads"
        decoder = json.JSONDecoder()
        idx = 0
        while idx < len(output):
            # Skip whitespace
            while idx < len(output) and output[idx] in " \t\n\r":
                idx += 1
            if idx >= len(output):
                break
            if output[idx] != "{":
                idx += 1
                continue
            try:
                obj, end = decoder.raw_decode(output, idx)
                if "payloads" in obj and isinstance(obj["payloads"], list) and obj["payloads"]:
                    texts = [p["text"] for p in obj["payloads"] if isinstance(p, dict) and p.get("text")]
                    if texts:
                        return "\n".join(texts)
                idx = end
            except json.JSONDecodeError:
                idx += 1
        return "OpenClaw returned no usable response"
        # Fallback: return raw output
        return output.strip()[-500:] if output.strip() else "OpenClaw returned no response"
    except subprocess.TimeoutExpired:
        return "OpenClaw agent timed out"
    except Exception as e:
        return f"OpenClaw bridge error: {e}"


import subprocess


class OpenClawBridgeHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/v1/models":
            self.send_json({
                "object": "list",
                "data": [{
                    "id": "openclaw-agent",
                    "object": "model",
                    "created": int(time.time()),
                    "owned_by": "openclaw",
                }]
            })
        elif self.path == "/health":
            self.send_json({"ok": True})
        else:
            self.send_error(404)

    def do_POST(self):
        if self.path == "/v1/chat/completions":
            self.handle_chat_completions()
        else:
            self.send_error(404)

    def handle_chat_completions(self):
        try:
            body = self.read_body()
            messages = body.get("messages", [])
            model = body.get("model", "openclaw-agent")
            stream = body.get("stream", False)

            # Extract user message (last user message)
            user_msg = ""
            for msg in reversed(messages):
                if msg.get("role") == "user":
                    content = msg.get("content", "")
                    if isinstance(content, list):
                        user_msg = " ".join(p.get("text", "") for p in content if p.get("type") == "text")
                    else:
                        user_msg = content
                    break

            if not user_msg:
                self.send_error(400, "No user message found")
                return

            # Derive session from messages hash for continuity
            session_key = str(hash(json.dumps(messages[:3])))[:16]
            session_id = SESSION_MAP.get(session_key)

            # Call OpenClaw
            reply = call_openclaw_agent(user_msg, session_id)

            # Store session for continuity
            if not session_id:
                SESSION_MAP[session_key] = str(uuid.uuid4())[:8]

            if stream:
                self.send_stream_response(reply, model)
            else:
                self.send_json({
                    "id": f"chatcmpl-{uuid.uuid4().hex[:12]}",
                    "object": "chat.completion",
                    "created": int(time.time()),
                    "model": model,
                    "choices": [{
                        "index": 0,
                        "message": {"role": "assistant", "content": reply},
                        "finish_reason": "stop"
                    }],
                    "usage": {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}
                })
        except Exception as e:
            self.send_error(500, str(e))

    def send_stream_response(self, reply: str, model: str):
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()

        # Send as single chunk
        chunk = {
            "id": f"chatcmpl-{uuid.uuid4().hex[:12]}",
            "object": "chat.completion.chunk",
            "created": int(time.time()),
            "model": model,
            "choices": [{"index": 0, "delta": {"role": "assistant", "content": reply}, "finish_reason": None}]
        }
        self.wfile.write(f"data: {json.dumps(chunk)}\n\n".encode())
        self.wfile.write(b"data: [DONE]\n\n")
        self.wfile.flush()

    def read_body(self):
        length = int(self.headers.get("Content-Length", 0))
        return json.loads(self.rfile.read(length))

    def send_json(self, data):
        body = json.dumps(data).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def send_error(self, code, message=""):
        body = json.dumps({"error": {"message": message, "code": code}}).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        pass  # Suppress default logging


def main():
    server = HTTPServer(("127.0.0.1", PORT), OpenClawBridgeHandler)
    print(f"OpenClaw bridge listening on http://127.0.0.1:{PORT}")
    server.serve_forever()


if __name__ == "__main__":
    main()
