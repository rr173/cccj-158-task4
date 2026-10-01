"""SQLite persistence layer.

Design notes
------------
* Every schema transition lives in one schema version; opening a database
  migrates it forward.
* A global write lock is taken by the service layer (BEGIN IMMEDIATE) so all
  multi-statement transitions (ingest + pump, review + pump, ...) are
  serialised per process; unique constraints add the second line of defence.
* No raw identity or pixel content is ever accepted or stored: the API layer
  enforces a strict envelope schema before anything reaches these tables.
"""
from __future__ import annotations

import os
import sqlite3
import threading
import time
from typing import Optional

SCHEMA = [
    """
    CREATE TABLE IF NOT EXISTS meta (
        key   TEXT PRIMARY KEY,
        value TEXT NOT NULL
    )""",
    """
    CREATE TABLE IF NOT EXISTS users (
        username TEXT PRIMARY KEY,
        pw_hash  TEXT NOT NULL,
        salt     TEXT NOT NULL,
        role     TEXT NOT NULL CHECK (role IN ('operator','reviewer','inspector'))
    )""",
    # Hash-chained audit journal; verified on every boot and on demand.
    """
    CREATE TABLE IF NOT EXISTS journal (
        seq       INTEGER PRIMARY KEY AUTOINCREMENT,
        event     TEXT NOT NULL,
        payload   TEXT NOT NULL,
        prev_hash TEXT NOT NULL,
        entry_hash TEXT NOT NULL
    )""",
    # Idempotency / request-key ledger.
    """
    CREATE TABLE IF NOT EXISTS requests (
        request_key TEXT PRIMARY KEY,
        object_key  TEXT NOT NULL,
        body_hash   TEXT NOT NULL,
        node_digest TEXT,
        result_json TEXT NOT NULL,
        status      TEXT NOT NULL CHECK (status IN ('seen','processed')),
        created_ts  TEXT NOT NULL
    )""",
    # Reconstructed directed version graph.
    """
    CREATE TABLE IF NOT EXISTS nodes (
        node_digest TEXT PRIMARY KEY,
        object_key  TEXT NOT NULL,
        site_id     TEXT NOT NULL,
        site_seq    INTEGER NOT NULL,
        prev_digest TEXT,
        regions     TEXT NOT NULL,           -- canonical JSON list
        body_hash   TEXT NOT NULL,
        change_digest TEXT NOT NULL,
        status      TEXT NOT NULL CHECK (status IN
                    ('waiting','active','incorporated','deferred')),
        waiting_reason TEXT,
        decision_id TEXT,
        first_request_key TEXT NOT NULL,
        created_ts  TEXT NOT NULL,
        UNIQUE (object_key, site_id, site_seq)
    )""",
    "CREATE INDEX IF NOT EXISTS idx_nodes_object ON nodes(object_key)",
    """
    CREATE TABLE IF NOT EXISTS objects (
        object_key  TEXT PRIMARY KEY,
        created_ts  TEXT NOT NULL
    )""",
    """
    CREATE TABLE IF NOT EXISTS decisions (
        decision_id    TEXT PRIMARY KEY,
        object_key     TEXT NOT NULL,
        version_no     INTEGER NOT NULL,
        kind           TEXT NOT NULL CHECK (kind IN ('merge','conflict')),
        claims_json    TEXT NOT NULL,        -- sorted atomic claims
        state_digest   TEXT NOT NULL,
        parent_digest  TEXT NOT NULL,        -- prior decision (or GENESIS)
        case_id        TEXT,
        created_ts     TEXT NOT NULL,
        UNIQUE (object_key, version_no)
    )""",
    # One current decision per object — single source of truth for "current".
    """
    CREATE TABLE IF NOT EXISTS object_current (
        object_key  TEXT PRIMARY KEY REFERENCES objects(object_key),
        decision_id TEXT NOT NULL REFERENCES decisions(decision_id)
    )""",
    """
    CREATE TABLE IF NOT EXISTS conflict_cases (
        case_id        TEXT PRIMARY KEY,
        object_key     TEXT NOT NULL,
        opened_decision_id TEXT NOT NULL,
        status         TEXT NOT NULL CHECK (status IN ('open','rejected','resolved')),
        step           INTEGER NOT NULL DEFAULT 1,  -- next required approval
        choice_node_digest TEXT,
        claims_json    TEXT NOT NULL,        -- frozen conflicting batch
        region         TEXT NOT NULL,        -- first differing atomic segment
        resolved_decision_id TEXT,
        created_ts     TEXT NOT NULL,
        updated_ts     TEXT NOT NULL
    )""",
    """
    CREATE TABLE IF NOT EXISTS reviews (
        id        INTEGER PRIMARY KEY AUTOINCREMENT,
        case_id   TEXT NOT NULL REFERENCES conflict_cases(case_id),
        reviewer  TEXT NOT NULL,
        step      INTEGER NOT NULL,
        action    TEXT NOT NULL CHECK (action IN ('approve','reject')),
        choice_node_digest TEXT,
        note      TEXT NOT NULL DEFAULT '',
        created_ts TEXT NOT NULL
    )""",
    # Verifiable receipt chain, one link per decision.
    """
    CREATE TABLE IF NOT EXISTS receipts (
        receipt_id   TEXT PRIMARY KEY,
        object_key   TEXT NOT NULL,
        decision_id  TEXT NOT NULL,
        version_no   INTEGER NOT NULL,
        seq          INTEGER NOT NULL,        -- per-object 1..n
        payload_json TEXT NOT NULL,
        receipt_hash TEXT NOT NULL,
        prev_receipt_hash TEXT NOT NULL,
        UNIQUE (object_key, seq),
        UNIQUE (decision_id)
    )""",
]


def now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def connect(path: str) -> sqlite3.Connection:
    new = not os.path.exists(path)
    if new:
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    conn = sqlite3.connect(path, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA synchronous=FULL")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def init(conn: sqlite3.Connection) -> None:
    """Create schema (idempotent) and seed built-in users."""
    cur = conn.execute("PRAGMA user_version")
    ver = cur.fetchone()[0]
    if ver == 0:
        for stmt in SCHEMA:
            conn.execute(stmt)
        _seed_users(conn)
        conn.execute("PRAGMA user_version=1")
        conn.commit()


def _seed_users(conn: sqlite3.Connection) -> None:
    # Local demo accounts only; production deployments should front this with
    # an IdP. Passwords are stored salted+hashed, never in clear text.
    from .canonical import digest

    seeds = [
        ("operator", "operator-pw", "operator"),
        ("reviewer-a", "review-a-pw", "reviewer"),
        ("reviewer-b", "review-b-pw", "reviewer"),
        ("inspector", "inspector-pw", "inspector"),
    ]
    for username, pw, role in seeds:
        salt = digest({"seed": username, "kind": "salt"})[:16]
        pw_hash = digest({"salt": salt, "password": pw})
        conn.execute(
            "INSERT OR IGNORE INTO users(username, pw_hash, salt, role) VALUES (?,?,?,?)",
            (username, pw_hash, salt, role),
        )


class WriteLock:
    """Process-wide serializer for multi-statement write transactions."""

    def __init__(self) -> None:
        self._lock = threading.RLock()

    def __enter__(self):
        self._lock.acquire()
        return self

    def __exit__(self, *exc):
        self._lock.release()
