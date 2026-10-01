"""HTTP Basic authentication against the local users table + role gates."""
from __future__ import annotations

import base64
import sqlite3
from typing import Optional

from .. import canonical


class AuthError(Exception):
    def __init__(self, status: int, code: str, message: str):
        self.status = status
        self.code = code
        self.message = message


def check_credentials(conn: sqlite3.Connection, header: Optional[str]) -> dict:
    if not header or not header.startswith("Basic "):
        raise AuthError(401, "unauthorized", "credentials required")
    try:
        raw = base64.b64decode(header[6:], validate=True).decode("utf-8")
        username, password = raw.split(":", 1)
    except Exception:
        raise AuthError(401, "unauthorized", "malformed credentials")
    u = conn.execute("SELECT * FROM users WHERE username=?", (username,)).fetchone()
    if u is None:
        raise AuthError(401, "unauthorized", "invalid credentials")
    if canonical.digest({"salt": u["salt"], "password": password}) != u["pw_hash"]:
        raise AuthError(401, "unauthorized", "invalid credentials")
    return {"username": u["username"], "role": u["role"]}


def require(user: dict, *roles: str) -> None:
    if user["role"] not in roles:
        raise AuthError(403, "forbidden",
                        f"role '{user['role']}' may not perform this action")
