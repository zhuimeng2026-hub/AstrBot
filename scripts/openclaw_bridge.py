"""
OpenAI-compatible API bridge for OpenClaw.
Routes messages to specialized OpenClaw agents based on content type.

For image messages, saves base64 images to temp files and passes local file
paths using openclaw's native [Image: source: /path] format. This avoids the
Linux MAX_ARG_STRLEN (128KB) CLI limit while keeping full image quality.
"""

import base64
import json
import os
import subprocess
import tempfile
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PORT = int(os.environ.get("OPENCLAW_BRIDGE_PORT", "8643"))
OPENCLAW_BIN = os.environ.get("OPENCLAW_BIN", "/usr/bin/openclaw")

# Agent routing
AGENT_TEXT = os.environ.get("AGENT_TEXT", "text-agent")
AGENT_IMAGE = os.environ.get("AGENT_IMAGE", "image-agent")
AGENT_VOICE = os.environ.get("AGENT_VOICE", "voice-agent")
AGENT_TOOL = os.environ.get("AGENT_TOOL", "main")

SESSION_MAP = {}
SESSION_MAP_LOCK = threading.Lock()

# Keywords that indicate OpenClaw tools are needed
TOOL_KEYWORDS = [
    "快递",
    "物流",
    "单号",
    "库存",
    "查询",
    "订单",
    "跟踪",
    "追踪",
    "express",
    "tracking",
    "inventory",
    "order",
    "ship",
]


def needs_tools(message: str) -> bool:
    """Check if message needs OpenClaw tools."""
    return any(kw in message for kw in TOOL_KEYWORDS)


def save_data_url_to_file(data_url: str) -> str | None:
    """Save a data:image/... base64 URL to a temp file, return file path."""
    try:
        header, b64data = data_url.split(",", 1)
        mime = header.split(";")[0].split(":")[1]
        ext = "." + mime.split("/")[1].replace("jpeg", "jpg")
        raw = base64.b64decode(b64data)
        fd, path = tempfile.mkstemp(suffix=ext, dir="/tmp")
        os.write(fd, raw)
        os.close(fd)
        return path
    except Exception:
        return None


def call_openclaw_agent(
    message: str, session_id: str | None = None, agent_id: str = "main"
) -> str:
    """Call openclaw agent CLI and return the response text."""
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
        output = result.stderr or result.stdout
        decoder = json.JSONDecoder()
        idx = 0
        while idx < len(output):
            while idx < len(output) and output[idx] in " \t\n\r":
                idx += 1
            if idx >= len(output):
                break
            if output[idx] != "{":
                idx += 1
                continue
            try:
                obj, end = decoder.raw_decode(output, idx)
                if (
                    "payloads" in obj
                    and isinstance(obj["payloads"], list)
                    and obj["payloads"]
                ):
                    texts = [
                        p["text"]
                        for p in obj["payloads"]
                        if isinstance(p, dict) and p.get("text")
                    ]
                    if texts:
                        return "\n".join(texts)
                idx = end
            except json.JSONDecodeError:
                idx += 1
        return "OpenClaw returned no usable response"
    except subprocess.TimeoutExpired:
        return "OpenClaw agent timed out"
    except Exception as e:
        return f"OpenClaw bridge error: {e}"


def extract_message_info(messages: list) -> tuple[str, bool, list[str], str]:
    """Extract user message text, image presence, image data URLs, and prior context."""
    user_msg = ""
    has_images = False
    image_data_urls = []
    prior_context_parts = []

    # Collect all user messages in order
    user_messages = [msg for msg in messages if msg.get("role") == "user"]

    if not user_messages:
        return "", False, [], ""

    # Process the LAST user message (the current one) for routing
    last_msg = user_messages[-1]
    content = last_msg.get("content", "")
    if isinstance(content, list):
        text_parts = [p.get("text", "") for p in content if p.get("type") == "text"]
        user_msg = " ".join(text_parts)
        for p in content:
            if p.get("type") == "image_url":
                has_images = True
                url = p.get("image_url", {}).get("url", "")
                if url:
                    image_data_urls.append(url)
    else:
        user_msg = content

    # Build prior context from ALL earlier user messages
    has_prior_image = False
    for msg in user_messages[:-1]:
        content = msg.get("content", "")
        if isinstance(content, list):
            for p in content:
                if p.get("type") == "text":
                    text = p.get("text", "").strip()
                    if text:
                        prior_context_parts.append(text)
                elif p.get("type") == "image_url":
                    has_prior_image = True
        elif isinstance(content, str) and content.strip():
            prior_context_parts.append(content.strip())

    if has_prior_image:
        prior_context_parts.append("[此前用户发送了图片]")

    prior_context = "\n".join(prior_context_parts) if prior_context_parts else ""
    return user_msg, has_images, image_data_urls, prior_context


class OpenClawBridgeHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/v1/models":
            self.send_json(
                {
                    "object": "list",
                    "data": [
                        {
                            "id": "openclaw-agent",
                            "object": "model",
                            "created": int(time.time()),
                            "owned_by": "openclaw",
                        }
                    ],
                }
            )
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
        temp_files = []
        try:
            body = self.read_body()
            messages = body.get("messages", [])
            model = body.get("model", "openclaw-agent")
            stream = body.get("stream", False)

            user_msg, has_images, image_data_urls, prior_context = extract_message_info(
                messages
            )

            if not user_msg and not has_images:
                self.send_error(400, "No user message found")
                return

            # Build agent message with prior context for text/tool routes
            def _with_context(msg: str) -> str:
                if prior_context:
                    return f"[对话历史]\n{prior_context}\n[当前消息]\n{msg}"
                return msg

            # Route by content type to specialized agents
            if has_images:
                # Save base64 images to temp files and use openclaw's native
                # [Image: source: /path] format. This avoids the Linux
                # MAX_ARG_STRLEN (128KB) CLI limit with zero quality loss.
                img_refs = []
                for url in image_data_urls:
                    path = save_data_url_to_file(url)
                    if path:
                        img_refs.append(f"[Image: source: {path}]")
                        temp_files.append(path)

                if img_refs:
                    ref_str = " ".join(img_refs)
                    agent_msg = f"{user_msg}\n{ref_str}" if user_msg else ref_str
                else:
                    agent_msg = user_msg or "Please describe this image"
                agent_id = AGENT_IMAGE
            elif needs_tools(user_msg):
                agent_id = AGENT_TOOL
                agent_msg = _with_context(user_msg)
            else:
                agent_id = AGENT_TEXT
                agent_msg = _with_context(user_msg)

            # Use first user message as stable session key
            first_user_msg = next(
                (m for m in messages if m.get("role") == "user"), None
            )
            if first_user_msg:
                session_key = str(hash(json.dumps(first_user_msg.get("content", ""))))[
                    :16
                ]
            else:
                session_key = str(hash(json.dumps(messages[:3])))[:16]
            with SESSION_MAP_LOCK:
                session_id = SESSION_MAP.get(session_key)
            print(
                f"[bridge] agent={agent_id} has_images={has_images} msg={user_msg[:80]!r}",
                flush=True,
            )

            reply = call_openclaw_agent(agent_msg, session_id, agent_id)

            if not session_id:
                with SESSION_MAP_LOCK:
                    SESSION_MAP[session_key] = str(uuid.uuid4())[:8]

            if stream:
                self.send_stream_response(reply, model)
            else:
                self.send_json(
                    {
                        "id": f"chatcmpl-{uuid.uuid4().hex[:12]}",
                        "object": "chat.completion",
                        "created": int(time.time()),
                        "model": model,
                        "choices": [
                            {
                                "index": 0,
                                "message": {"role": "assistant", "content": reply},
                                "finish_reason": "stop",
                            }
                        ],
                        "usage": {
                            "prompt_tokens": 0,
                            "completion_tokens": 0,
                            "total_tokens": 0,
                        },
                    }
                )
        except Exception as e:
            self.send_error(500, str(e))
        finally:
            for f in temp_files:
                try:
                    os.unlink(f)
                except OSError:
                    pass

    def send_stream_response(self, reply: str, model: str):
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        chunk = {
            "id": f"chatcmpl-{uuid.uuid4().hex[:12]}",
            "object": "chat.completion.chunk",
            "created": int(time.time()),
            "model": model,
            "choices": [
                {
                    "index": 0,
                    "delta": {"role": "assistant", "content": reply},
                    "finish_reason": None,
                }
            ],
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
        pass


def main():
    server = ThreadingHTTPServer(("127.0.0.1", PORT), OpenClawBridgeHandler)
    print(f"OpenClaw bridge listening on http://127.0.0.1:{PORT}")
    print(
        f"  text-agent={AGENT_TEXT}  image-agent={AGENT_IMAGE}  voice-agent={AGENT_VOICE}  tool-agent={AGENT_TOOL}"
    )
    server.serve_forever()


if __name__ == "__main__":
    main()
