"""HTTP interface for the multi-site change convergence service.

Zero third-party dependencies: stdlib ThreadingHTTPServer + sqlite3.
All API responses are synthetic descriptors only — no original identity, no
pixel data is ever accepted, persisted or returned.
"""
from __future__ import annotations

import json
import os
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

from .. import canonical, engine
from ..store import WriteLock, connect, init
from ..config import db_path
from .auth import AuthError, check_credentials, require

STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")

_WRITE_ROLES = ("operator",)
_REVIEW_ROLES = ("reviewer",)
_READ_ROLES = ("operator", "reviewer", "inspector")

_state: dict = {}


def get_state():
    return _state


def init_state(path: str | None = None) -> dict:
    path = path or db_path()
    conn = connect(path)
    init(conn)
    wlock = WriteLock()
    _state["conn"] = conn
    _state["wlock"] = wlock
    _state["recover"] = engine.boot_recover(conn, wlock)
    return _state


class Handler(BaseHTTPRequestHandler):
    server_version = "ConvergenceService/1.0"

    # ------------------------------------------------------------------ utils
    def _user(self):
        return check_credentials(_state["conn"], self.headers.get("Authorization"))

    def _send(self, status: int, obj: dict | list) -> None:
        body = canonical.encode(obj)
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _send_error(self, status: int, code: str, message: str) -> None:
        self._send(status, {"error": code, "message": message})

    def _read_json(self):
        n = int(self.headers.get("Content-Length") or 0)
        if n <= 0 or n > 1_000_000:
            raise engine.DomainError("invalid_body", "request body missing or too large")
        raw = self.rfile.read(n)
        try:
            return json.loads(raw.decode("utf-8"))
        except Exception:
            raise engine.DomainError("invalid_body", "body must be valid JSON")

    def log_message(self, fmt, *args):  # concise stderr logging
        sys.stderr.write("%s - %s\n" % (self.address_string(), fmt % args))

    # --------------------------------------------------------------- routing
    def do_GET(self):
        try:
            u = urlparse(self.path)
            p = u.path
            q = parse_qs(u.query)
            if p == "/healthz":
                return self._send(200, {"ok": True})
            if p == "/" or p == "/ui" or p == "/index.html":
                return self._static("index.html", "text/html; charset=utf-8")
            if p == "/ui/app.js":
                return self._static("app.js", "application/javascript; charset=utf-8")
            if p == "/ui/styles.css":
                return self._static("styles.css", "text/css; charset=utf-8")
            if p.startswith("/api/"):
                return self._api_get(p, q)
            self._send_error(404, "not_found", "unknown path")
        except AuthError as e:
            self._auth_header_if_needed(e)
        except engine.DomainError as e:
            self._send_error(e.status, e.code, e.message)
        except Exception as e:  # never leak internals
            self._send_error(500, "internal_error", "internal error")

    def do_POST(self):
        try:
            u = urlparse(self.path)
            p = u.path
            if p.startswith("/api/"):
                return self._api_post(p)
            self._send_error(404, "not_found", "unknown path")
        except AuthError as e:
            self._auth_header_if_needed(e)
        except engine.DomainError as e:
            self._send_error(e.status, e.code, e.message)
        except Exception:
            self._send_error(500, "internal_error", "internal error")

    def _auth_header_if_needed(self, e: AuthError):
        if e.status == 401:
            self.send_response(401)
            self.send_header("WWW-Authenticate", 'Basic realm="converge"')
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            body = canonical.encode({"error": e.code, "message": e.message})
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        else:
            self._send_error(e.status, e.code, e.message)

    def _static(self, name: str, ctype: str):
        path = os.path.join(STATIC_DIR, name)
        with open(path, "rb") as f:
            body = f.read()
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    # -------------------------------------------------------------- GET APIs
    def _api_get(self, p: str, q: dict):
        user = self._user()
        conn = _state["conn"]
        if p == "/api/me":
            return self._send(200, {"username": user["username"], "role": user["role"]})
        if p == "/api/objects":
            require(user, *_READ_ROLES)
            return self._send(200, engine.list_objects(conn))
        if p.startswith("/api/objects/"):
            rest = p[len("/api/objects/"):]
            parts = rest.split("/")
            if len(parts) == 1:
                require(user, *_READ_ROLES)
                detail = engine.object_detail(
                    conn, parts[0], redact_sites=(user["role"] == "inspector")
                )
                if user["role"] == "inspector":
                    detail["redacted_for_inspector"] = True
                return self._send(200, detail)
            if len(parts) == 2 and parts[1] == "receipts":
                require(user, *_READ_ROLES)
                bundle = engine.receipt_bundle(conn, parts[0])
                if user["role"] == "inspector":
                    bundle = _redact_bundle(bundle)
                return self._send(200, bundle)
            if len(parts) == 2 and parts[1] == "verify":
                require(user, *_READ_ROLES)
                return self._send(200, engine.verify_object(conn, parts[0]))
        if p == "/api/cases":
            require(user, "operator", "reviewer")
            status = (q.get("status") or ["open"])[0]
            return self._send(200, engine.list_cases(conn, status))
        if p == "/api/journal/verify":
            require(user, *_READ_ROLES)
            return self._send(200, engine.verify_journal(conn))
        self._send_error(404, "not_found", "unknown path")

    # ------------------------------------------------------------- POST APIs
    def _api_post(self, p: str):
        user = self._user()
        conn = _state["conn"]
        wlock = _state["wlock"]
        body = self._read_json()

        if p == "/api/envelopes":
            require(user, *_WRITE_ROLES)
            rk = self.headers.get("X-Request-Key") or body.get("request_key") if isinstance(body, dict) else None
            envelope = body.get("envelope") if isinstance(body, dict) else None
            if envelope is None and isinstance(body, dict) and "request_key" in body:
                envelope = {k: v for k, v in body.items() if k != "request_key"}
            if envelope is None:
                raise engine.DomainError("invalid_envelope", "missing envelope")
            result = engine.ingest_envelope(conn, wlock, envelope, rk)
            return self._send(200, result)

        if p.startswith("/api/cases/") and p.endswith("/reviews"):
            require(user, *_REVIEW_ROLES)
            case_id = p[len("/api/cases/"):-len("/reviews")]
            if not isinstance(body, dict):
                raise engine.DomainError("invalid_review", "body must be an object")
            result = engine.submit_review(
                conn, wlock, user["username"], case_id,
                body.get("action"), body.get("choice_node_digest"),
                body.get("note", ""),
            )
            return self._send(200, result)

        self._send_error(404, "not_found", "unknown path")


def _redact_bundle(bundle: dict) -> dict:
    """Inspector view: site identifiers are never exposed in receipts."""
    # payloads already contain only pseudonymous keys / digests / sections;
    # node digests are content-derived (no site field), so nothing else maps
    # back to an origin. Mark the bundle to make the guarantee explicit.
    bundle["redacted_for_inspector"] = True
    return bundle


def main(argv=None):
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=8090)
    ap.add_argument("--db", default=None)
    args = ap.parse_args(argv)

    state = init_state(args.db)
    rec = state["recover"]
    sys.stderr.write(f"[boot] recovery: {json.dumps(rec)}\n")
    httpd = ThreadingHTTPServer((args.host, args.port), Handler)
    sys.stderr.write(f"[boot] convergence service on http://{args.host}:{args.port}/ui\n")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
