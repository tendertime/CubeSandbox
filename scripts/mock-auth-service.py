#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# Copyright (C) 2026 Tencent. All rights reserved.

"""Minimal auth callback mock for CubeAPI.

This server implements the callback contract used by CubeAPI auth middleware:

- POST /verify
- Allow requests by returning HTTP 200
- Deny requests by returning HTTP 401 or 403

Credential rules are configured with environment variables:

- MOCK_AUTH_KEYS: comma-separated list of full-access keys
- MOCK_READONLY_KEYS: comma-separated list of read-only keys
- MOCK_AUTH_PORT: listen port, default 8081
- MOCK_AUTH_HOST: listen host, default 127.0.0.1

Read-only keys are allowed only for GET and HEAD requests. All other keys are
rejected. If MOCK_AUTH_KEYS is empty, the server accepts any non-empty credential.
"""

from __future__ import annotations

import argparse
import json
import os
from email.message import Message
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse


READ_METHODS = {"GET", "HEAD"}


def parse_keys(value: str | None) -> set[str]:
    if not value:
        return set()
    return {item.strip() for item in value.split(",") if item.strip()}


def extract_credential(headers: Message) -> str:
    auth = headers.get("Authorization", "")
    if auth.startswith("Bearer "):
        token = auth.removeprefix("Bearer ").strip()
        if token:
            return token

    api_key = headers.get("X-API-Key", "").strip()
    return api_key


def decide_access(
    key: str,
    method: str,
    path: str,
    full_access_keys: set[str],
    readonly_keys: set[str],
    allow_any_non_empty: bool,
) -> tuple[bool, int, str]:
    if not key:
        return False, 401, "missing credential"

    if full_access_keys:
        if key in full_access_keys:
            return True, 200, "full access"
    elif allow_any_non_empty:
        return True, 200, "allow any non-empty credential"

    if key in readonly_keys:
        if path.rstrip("/").endswith("/terminal"):
            return False, 403, "read-only key cannot open a terminal"
        if method in READ_METHODS:
            return True, 200, "read-only access"
        return False, 403, "read-only key cannot use write methods"

    return False, 401, "unknown credential"


class MockAuthHandler(BaseHTTPRequestHandler):
    server_version = "cube-mock-auth/1.0"

    def do_GET(self) -> None:  # noqa: N802
        if self.path == "/health":
            self._send_json(200, {"ok": True})
            return
        self._send_json(404, {"ok": False, "error": "not found"})

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        if parsed.path != "/verify":
            self._send_json(404, {"ok": False, "error": "not found"})
            return

        full_access_keys = self.server.full_access_keys
        readonly_keys = self.server.readonly_keys
        key = extract_credential(self.headers)
        method = self.headers.get("X-Request-Method", self.command).upper()
        path = self.headers.get("X-Request-Path", "")

        allowed, status, reason = decide_access(
            key,
            method,
            path,
            full_access_keys,
            readonly_keys,
            self.server.allow_any_non_empty,
        )
        payload = {
            "ok": allowed,
            "reason": reason,
            "path": path,
            "method": method,
        }
        self._send_json(status, payload)

    def log_message(self, fmt: str, *args: object) -> None:
        print(
            json.dumps(
                {
                    "event": "request",
                    "method": self.command,
                    "path": self.path,
                    "message": fmt % args,
                },
                ensure_ascii=False,
            )
        )

    def _send_json(self, status: int, payload: dict[str, object]) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


class MockAuthServer(ThreadingHTTPServer):
    def __init__(self, server_address: tuple[str, int], handler_class):
        super().__init__(server_address, handler_class)
        self.full_access_keys = parse_keys(os.environ.get("MOCK_AUTH_KEYS"))
        self.readonly_keys = parse_keys(os.environ.get("MOCK_READONLY_KEYS"))
        self.allow_any_non_empty = not self.full_access_keys and not self.readonly_keys


def main() -> int:
    parser = argparse.ArgumentParser(description="CubeAPI auth callback mock")
    parser.add_argument(
        "--host",
        default=os.environ.get("MOCK_AUTH_HOST", "127.0.0.1"),
        help="listen host (default: 127.0.0.1 or MOCK_AUTH_HOST)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.environ.get("MOCK_AUTH_PORT", "8081")),
        help="listen port (default: 8081 or MOCK_AUTH_PORT)",
    )
    args = parser.parse_args()

    server = MockAuthServer((args.host, args.port), MockAuthHandler)
    print(
        json.dumps(
            {
                "event": "start",
                "host": args.host,
                "port": args.port,
                "verify_url": f"http://{args.host}:{args.port}/verify",
                "health_url": f"http://{args.host}:{args.port}/health",
                "full_access_keys": len(server.full_access_keys),
                "readonly_keys": len(server.readonly_keys),
            },
            ensure_ascii=False,
        )
    )

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
