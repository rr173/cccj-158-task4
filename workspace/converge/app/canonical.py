"""Deterministic canonical JSON encoding and content hashing.

The convergence service derives every node digest, state digest and receipt
hash from canonical serialization so that the same logical payload always
produces the same hash byte-for-byte, regardless of dict insertion order.
"""
from __future__ import annotations

import hashlib
import json
from typing import Any

Canonical = Any


def encode(obj: Canonical) -> bytes:
    """Serialize *obj* deterministically: sorted keys, compact separators."""
    return json.dumps(
        obj, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False
    ).encode("utf-8")


def digest(obj: Canonical) -> str:
    return hashlib.sha256(encode(obj)).hexdigest()


def hash_chain(prev_hash: str, payload: Canonical) -> str:
    return digest({"prev": prev_hash, "payload": payload})


GENESIS = "0" * 64
