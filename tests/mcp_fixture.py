"""Minimal real MCP wire server for integration tests (HTTP and stdio).

Deliberately rejects tools/list: execution must use the platform-supplied schema.
"""

import json
import os
import socketserver
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


def reply(message):
    method, params = message["method"], message.get("params", {})
    if "id" not in message:
        return None
    if method == "initialize":
        result = {
            "protocolVersion": "2025-11-25",
            "capabilities": {"tools": {}},
            "serverInfo": {"name": "fixture", "version": "1"},
        }
    elif method == "tools/call":
        if params["name"] == "reject":
            return {
                "jsonrpc": "2.0",
                "id": message["id"],
                "error": {"code": -32602, "message": "bad"},
            }
        arguments = params.get("arguments", {})
        result = {
            "content": [{"type": "text", "text": str(arguments.get("value", "ok"))}],
            "structuredContent": {"value": arguments.get("value", "ok")},
            "isError": params["name"] == "error",
        }
    else:
        return {
            "jsonrpc": "2.0",
            "id": message["id"],
            "error": {"code": -32601, "message": "Unexpected discovery"},
        }
    return {"jsonrpc": "2.0", "id": message["id"], "result": result}


def progress(message):
    token = message.get("params", {}).get("_meta", {}).get("progressToken")
    if token is None:
        return None
    return {
        "jsonrpc": "2.0",
        "method": "notifications/progress",
        "params": {"progressToken": token, "progress": 1, "total": 2, "message": "working"},
    }


class HttpFixture:
    def __init__(self):
        self.requests = []
        self.called = threading.Event()
        self.release = threading.Event()
        fixture = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args):
                pass

            def record(self, body=None):
                fixture.requests.append((self.command, dict(self.headers), body))

            def send(self, status, body=None, content_type="application/json"):
                data = b"" if body is None else body
                self.send_response(status)
                self.send_header("Content-Type", content_type)
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                try:
                    self.wfile.write(data)
                except (BrokenPipeError, ConnectionResetError):
                    pass

            def do_GET(self):
                self.record()
                self.send(405)

            def do_DELETE(self):
                self.record()
                self.send(204)

            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                self.record(body)
                result = reply(body)
                if body["method"] == "initialize":
                    data = json.dumps(result).encode()
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Mcp-Session-Id", "fixture-session")
                    self.send_header("Content-Length", str(len(data)))
                    self.end_headers()
                    self.wfile.write(data)
                    return
                if body["method"] == "tools/call":
                    fixture.called.set()
                    if body["params"]["name"] == "wait":
                        fixture.release.wait(5)
                    elif body["params"]["name"] in ("drop", "drop_after"):
                        if body["params"]["name"] == "drop_after":
                            fixture.release.wait(5)
                        self.close_connection = True
                        return
                    update = progress(body)
                    events = [update, result] if update else [result]
                    payload = "".join(f"event: message\ndata: {json.dumps(e)}\n\n" for e in events)
                    self.send(200, payload.encode(), "text/event-stream")
                    return
                self.send(
                    202 if result is None else 200, json.dumps(result).encode() if result else None
                )

        class Server(ThreadingHTTPServer):
            request_queue_size = 32

            def server_bind(self):
                # Avoid reverse DNS in a loopback-only fixture.
                socketserver.TCPServer.server_bind(self)
                self.server_name = "localhost"
                self.server_port = self.server_address[1]

        self.server = Server(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.url = f"http://127.0.0.1:{self.server.server_port}/mcp"

    def close(self):
        self.release.set()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()


def stdio():
    pid_file = os.environ.get("MCP_TEST_PID")
    if pid_file:
        Path(pid_file).write_text(str(os.getpid()))
    for line in sys.stdin:
        message = json.loads(line)
        if log := os.environ.get("MCP_TEST_LOG"):
            with open(log, "a") as file:
                file.write(json.dumps(message) + "\n")
        if message["method"] == "tools/call":
            if message["params"]["name"] == "wait":
                time.sleep(30)
            if update := progress(message):
                print(json.dumps(update), flush=True)
        if result := reply(message):
            print(json.dumps(result), flush=True)


if __name__ == "__main__":
    stdio()
