"""
OpenAI-compatible API bridge for OpenClaw.
Routes simple text to AIKey (newapi) directly,
complex/tool requests to openclaw agent.
"""

import asyncio
import json
import os
import sys
import time
import uuid
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.request import Request, urlopen

PORT = int(os.environ.get("OPENCLAW_BRIDGE_PORT", "8643"))
OPENCLAW_BIN = os.environ.get("OPENCLAW_BIN", "/usr/bin/openclaw")

# AIKey (newapi) for simple text passthrough
AIKEY_BASE = os.environ.get("AIKEY_BASE", "https://aikey.aixifs.com/v1")
AIKEY_KEY = os.environ.get("AIKEY_KEY", "t7npV6raGbd2f4HOMR4RRi0gsK2MbvPWk5TMs4i8Q9eJ80cG")
AIKEY_MODEL = os.environ.get("AIKEY_MODEL", "mimo-v2.5")
AIKEY_VISION_MODEL = os.environ.get("AIKEY_VISION_MODEL", "mimo-v2.5")

SESSION_MAP = {}

# Keywords that indicate OpenClaw tools are needed
TOOL_KEYWORDS = ["快递", "物流", "单号", "库存", "查询", "订单", "跟踪", "追踪",
                 "express", "tracking", "inventory", "order", "ship"]


def needs_openclaw(message: str) -> bool:
    """Check if message needs OpenClaw tools."""
    return any(kw in message for kw in TOOL_KEYWORDS)


def call_aikey(messages: list, model: str = AIKEY_MODEL) -> str:
    """Call AIKey (newapi) directly for simple text."""
    url = f"{AIKEY_BASE}/chat/completions"
    headers = {
        "Authorization": f"Bearer {AIKEY_KEY}",
        "Content-Type": "application/json",
    }
    # Strip image_url parts from history to avoid 404 on non-vision models
    cleaned = []
    for msg in messages:
        content = msg.get("content")
        if isinstance(content, list):
            text_parts = [p for p in content if p.get("type") == "text"]
            if text_parts:
                cleaned.append({**msg, "content": text_parts})
            else:
                cleaned.append({**msg, "content": [{"type": "text", "text": "[图片]"}]})
        else:
            cleaned.append(msg)
    messages = cleaned

    body = json.dumps({
        "model": model,
        "messages": messages,
    }).encode()

    print(f"[bridge] call_aikey: model={model}, msg_count={len(messages)}, body_size={len(body)} bytes", flush=True)
    req = Request(url, data=body, headers=headers, method="POST")
    try:
        with urlopen(req, timeout=60) as resp:
            data = json.loads(resp.read())
            return data["choices"][0]["message"]["content"]
    except Exception as e:
        detail = ""
        if hasattr(e, 'fp') and e.fp:
            try:
                detail = e.fp.read().decode('utf-8', errors='replace')[:500]
            except Exception:
                pass
        print(f"[bridge] call_aikey error: {e}, detail={detail}", flush=True)
        return f"AIKey error: {e}"


def call_openclaw_agent(message: str, session_id: str | None = None, agent_id: str = "main") -> str:
    """Call openclaw agent CLI and return the response text."""
    # Clean stale lock files before each call
    import glob as _glob
    for lock in _glob.glob(f"/root/.openclaw/agents/{agent_id}/sessions/*.lock"):
        try:
            os.remove(lock)
        except OSError:
            pass

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

            # Extract user message and check for images
            user_msg = ""
            has_images = False
            for msg in reversed(messages):
                if msg.get("role") == "user":
                    content = msg.get("content", "")
                    if isinstance(content, list):
                        user_msg = " ".join(p.get("text", "") for p in content if p.get("type") == "text")
                        has_images = any(p.get("type") == "image_url" for p in content)
                    else:
                        user_msg = content
                    break

            print(f"[bridge] model={model} has_images={has_images} user_msg={user_msg[:80]!r}", flush=True)

            if not user_msg and not has_images:
                self.send_error(400, "No user message found")
                return

            # Route: images → AIKey vision model, simple text → AIKey, tool-needed → OpenClaw
            if has_images:
                print(f"[bridge] 路由到视觉模型: {AIKEY_VISION_MODEL}", flush=True)
                reply = call_aikey(messages, model=AIKEY_VISION_MODEL)
            elif needs_openclaw(user_msg):
                session_key = str(hash(json.dumps(messages[:3])))[:16]
                session_id = SESSION_MAP.get(session_key)
                reply = call_openclaw_agent(user_msg, session_id)
                if not session_id:
                    SESSION_MAP[session_key] = str(uuid.uuid4())[:8]
            else:
                reply = call_aikey(messages)

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
