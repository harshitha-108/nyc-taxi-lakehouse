"""Loopback HTTP API for the Power BI custom chat visual."""

from __future__ import annotations

import argparse
import hmac
import json
import os
import secrets
import threading
import time
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import urlparse

from chatbot.data import SourceError, source_from_env
from chatbot.engine import ChatEngine, InvalidRequest

DEFAULT_ALLOWED_ORIGINS = frozenset({"https://ms-pbi.pbi.microsoft.com"})
ACCESS_CODE_HEADER = "X-City-Pulse-Code"
PUBLIC_RATE_LIMIT = 30
PUBLIC_RATE_WINDOW_SECONDS = 60


def auth_mode_from_env() -> str:
    mode = os.environ.get("CHAT_AUTH_MODE", "code").strip().casefold()
    if mode not in {"code", "local_public"}:
        raise ValueError("CHAT_AUTH_MODE must be code or local_public")
    if mode == "local_public" and os.environ.get("CHAT_DATA_SOURCE", "csv").casefold() != "csv":
        raise ValueError("Code-free local mode can read only exported CSV marts")
    return mode


def allowed_origins_from_env() -> frozenset[str]:
    raw = os.environ.get("CHAT_ALLOWED_ORIGINS")
    if raw is None:
        return DEFAULT_ALLOWED_ORIGINS
    origins = frozenset(value.strip() for value in raw.split(",") if value.strip())
    for origin in origins:
        parsed = urlparse(origin)
        if origin != "null" and (parsed.scheme not in {"http", "https"} or
                                 not parsed.netloc or parsed.path or parsed.params or
                                 parsed.query or parsed.fragment or parsed.username or
                                 parsed.password):
            raise ValueError("CHAT_ALLOWED_ORIGINS must contain exact web origins")
    return origins


class ChatHandler(BaseHTTPRequestHandler):
    engine: ChatEngine
    allowed_origins: frozenset[str] = DEFAULT_ALLOWED_ORIGINS
    access_code: str = ""
    auth_mode: str = "code"
    public_request_times: deque[float] = deque()
    public_rate_lock = threading.Lock()
    protocol_version = "HTTP/1.1"

    def _origin_allowed(self) -> bool:
        origin = self.headers.get("Origin")
        if origin and origin not in self.allowed_origins:
            referer = self.headers.get("Referer", "")
            parsed = urlparse(referer)
            referrer_origin = f"{parsed.scheme}://{parsed.netloc}" if parsed.netloc else "none"
            self.log_message("rejected browser origin %r; referrer origin %r; fetch-site %r",
                             origin, referrer_origin, self.headers.get("Sec-Fetch-Site"))
            self.close_connection = True
            self._json(403, {"answer": "", "evidence": [],
                             "error": "Origin not allowed; check CHAT_ALLOWED_ORIGINS"})
            return False
        return True

    def _json(self, status: int, payload: dict[str, Any]) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("Vary", "Origin")
        origin = self.headers.get("Origin")
        if origin in self.allowed_origins:
            self.send_header("Access-Control-Allow-Origin", origin)
            self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
            self.send_header("Access-Control-Allow-Headers",
                             f"Content-Type, {ACCESS_CODE_HEADER}")
            self.send_header("Access-Control-Allow-Private-Network", "true")
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self) -> None:  # noqa: N802
        if not self._origin_allowed():
            return
        if self.path not in {"/chat", "/health"}:
            self._json(404, {"error": "Not found"})
        else:
            self._json(200, {})

    def do_GET(self) -> None:  # noqa: N802
        if not self._origin_allowed():
            return
        if self.path == "/health":
            health = {**self.engine.source.health(),
                      "auth_required": self.auth_mode == "code"}
            self._json(200 if health["status"] == "ready" else 503, health)
        else:
            self._json(404, {"error": "Not found"})

    def do_POST(self) -> None:  # noqa: N802
        if not self._origin_allowed():
            return
        if self.path != "/chat":
            self._json(404, {"answer": "", "evidence": [], "error": "Not found"})
            return
        if self.auth_mode == "code":
            provided_code = self.headers.get(ACCESS_CODE_HEADER, "")
            if (not provided_code or not provided_code.isascii() or not self.access_code
                    or not hmac.compare_digest(provided_code, self.access_code)):
                self.close_connection = True
                self._json(401, {"answer": "", "evidence": [],
                                 "error": "Access code required or incorrect"})
                return
        else:
            now = time.monotonic()
            with self.public_rate_lock:
                while (self.public_request_times and
                       now - self.public_request_times[0] >= PUBLIC_RATE_WINDOW_SECONDS):
                    self.public_request_times.popleft()
                limited = len(self.public_request_times) >= PUBLIC_RATE_LIMIT
                if not limited:
                    self.public_request_times.append(now)
            if limited:
                self._json(429, {"answer": "", "evidence": [],
                                 "error": "Too many local chat requests; try again shortly"})
                return
        try:
            size = int(self.headers.get("Content-Length", "0"))
            if not 0 < size <= 16_384:
                raise InvalidRequest("JSON request must be at most 16 KB")
            if "application/json" not in self.headers.get("Content-Type", ""):
                raise InvalidRequest("Content-Type must be application/json")
            payload = json.loads(self.rfile.read(size))
            if not isinstance(payload, dict):
                raise InvalidRequest("JSON request must be an object")
            answer = self.engine.answer(payload)
        except (InvalidRequest, ValueError, json.JSONDecodeError) as exc:
            self._json(400, {"answer": "", "evidence": [], "error": str(exc)})
        except SourceError as exc:
            self._json(503, {"answer": "", "evidence": [], "error": str(exc)})
        else:
            self._json(200, answer)

    def log_message(self, format: str, *args: object) -> None:
        # The standard handler logs paths only; question text and DB credentials stay out of logs.
        super().log_message(format, *args)


def make_server(port: int = 8765) -> ThreadingHTTPServer:
    """Power BI Desktop calls only a host-local endpoint."""
    mode = auth_mode_from_env()
    source = source_from_env()
    handler = type("LocalChatHandler", (ChatHandler,),
                   {"engine": ChatEngine(source),
                    "allowed_origins": allowed_origins_from_env(),
                    "auth_mode": mode,
                    "access_code": secrets.token_urlsafe(12) if mode == "code" else "",
                    "public_request_times": deque(),
                    "public_rate_lock": threading.Lock()})
    return ThreadingHTTPServer(("127.0.0.1", port), handler)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the local Power BI taxi chatbot")
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args()
    if not 1 <= args.port <= 65535:
        parser.error("port must be 1–65535")
    server = make_server(args.port)
    print(f"NYC taxi chat listening on http://127.0.0.1:{args.port}", flush=True)
    if server.RequestHandlerClass.auth_mode == "code":
        print(f"Paste this access code into the Power BI chat: "
              f"{server.RequestHandlerClass.access_code}", flush=True)
    else:
        print("Code-free local mode: only read-only exported taxi CSV aggregates are served.",
              flush=True)
    try:
        server.serve_forever(poll_interval=0.2)
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
