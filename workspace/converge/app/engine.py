"""Core convergence engine.

The service reconstructs a directed version graph per replacement-object key.

Node lifecycle
    waiting       predecessor unknown or not yet settled (held envelope)
      -> active   all ancestors are incorporated; eligible for convergence
      -> incorporated   folded into a current decision
      -> deferred       frozen into an open conflict case awaiting two
                        different human reviewers

Per object there is exactly one current decision row (object_current) and one
hash-chained receipt per decision. A conflicting batch also produces a
decision (kind=conflict) which is the current determination until two
reviewers, in order, resolve it; the resolving merge then becomes current.
"""
from __future__ import annotations

import json
import os
import sqlite3
from typing import Any, Optional

from . import canonical
from .canonical import GENESIS, digest, hash_chain
from .store import WriteLock, now_iso

# Limit on pump iterations per call so a pathological graph cannot spin forever.
MAX_PUMP_ITER = 10000


class DomainError(Exception):
    def __init__(self, code: str, message: str, status: int = 400):
        super().__init__(message)
        self.code = code
        self.message = message
        self.status = status


# --------------------------------------------------------------------------- #
# validation
# --------------------------------------------------------------------------- #

_ROLES_INPUT_FIELDS = {
    "object_key",
    "site_id",
    "site_seq",
    "prev_digest",
    "regions",
    "change_digest",
}
_REGION_LIMIT = 1_000_000  # abstract coordinate space upper bound


def validate_envelope(raw: Any) -> dict:
    """Strict schema check.

    Only synthetic descriptors are accepted: keys, hashes and integer section
    coordinates. Free-form text, identifiers or pixel payloads are rejected
    outright (unexpected field -> error), so neither a response nor persisted
    state can ever carry original identity or actual pixels.
    """
    if not isinstance(raw, dict):
        raise DomainError("invalid_envelope", "envelope must be a JSON object")
    extra = set(raw) - _ROLES_INPUT_FIELDS
    if extra:
        raise DomainError(
            "invalid_envelope", f"unexpected fields: {sorted(extra)}"
        )

    def _nonempty_str(v: Any, name: str, maxlen: int = 200) -> str:
        if not isinstance(v, str) or not v or len(v) > maxlen:
            raise DomainError("invalid_envelope", f"{name} must be a non-empty string")
        return v

    def _hex(v: Any, name: str, length: int) -> str:
        s = _nonempty_str(v, name, length)
        int(s, 16)  # raises ValueError -> invalid
        if len(s) != length:
            raise ValueError
        return s.lower()

    obj = _nonempty_str(raw.get("object_key"), "object_key")
    site = _nonempty_str(raw.get("site_id"), "site_id", 64)
    seq = raw.get("site_seq")
    if not isinstance(seq, int) or isinstance(seq, bool) or seq < 0 or seq > 2**53:
        raise DomainError("invalid_envelope", "site_seq must be a non-negative integer")

    prev = raw.get("prev_digest")
    if prev is not None:
        try:
            prev = _hex(prev, "prev_digest", 64)
        except (ValueError, DomainError):
            raise DomainError("invalid_envelope", "prev_digest must be a 64-hex digest")

    regions_raw = raw.get("regions")
    if not isinstance(regions_raw, list) or not regions_raw:
        raise DomainError("invalid_envelope", "regions must be a non-empty list")
    regions: list[list[int]] = []
    for r in regions_raw:
        if (
            not isinstance(r, dict)
            or set(r) != {"start", "end"}
            or not all(isinstance(v, int) and not isinstance(v, bool) for v in r.values())
        ):
            raise DomainError("invalid_envelope", "each region needs integer start/end")
        s, e = r["start"], r["end"]
        if not (0 <= s < e <= _REGION_LIMIT):
            raise DomainError("invalid_envelope", "region must satisfy 0<=start<end")
        regions.append([s, e])
    regions.sort()
    for (s1, e1), (s2, e2) in zip(regions, regions[1:]):
        if s2 < e1:
            raise DomainError("invalid_envelope", "regions of one change must not overlap")

    ch = raw.get("change_digest")
    try:
        ch = _hex(ch, "change_digest", 64)
    except (ValueError, DomainError):
        raise DomainError("invalid_envelope", "change_digest must be a 64-hex digest")

    return {
        "object_key": obj,
        "site_id": site,
        "site_seq": seq,
        "prev_digest": prev,
        "regions": regions,
        "change_digest": ch,
    }


def body_hash_of(env: dict) -> str:
    """Hash of the reportable body (request_key excluded by design)."""
    return digest(
        {
            "object_key": env["object_key"],
            "site_id": env["site_id"],
            "site_seq": env["site_seq"],
            "prev_digest": env["prev_digest"],
            "regions": env["regions"],
            "change_digest": env["change_digest"],
        }
    )


def node_digest_of(env: dict, body_hash: str) -> str:
    return digest(
        {
            "object_key": env["object_key"],
            "site_id": env["site_id"],
            "site_seq": env["site_seq"],
            "prev_digest": env["prev_digest"],
            "regions": env["regions"],
            "body_hash": body_hash,
        }
    )


# --------------------------------------------------------------------------- #
# journal (hash chained)
# --------------------------------------------------------------------------- #


def append_journal(conn: sqlite3.Connection, event: str, payload: dict) -> None:
    enc = canonical.encode(payload).decode("utf-8")
    row = conn.execute("SELECT seq, entry_hash FROM journal ORDER BY seq DESC LIMIT 1").fetchone()
    seq = (row["seq"] + 1) if row else 1
    prev = row["entry_hash"] if row else GENESIS
    entry_hash = hash_chain(prev, {"event": event, "payload": payload})
    conn.execute(
        "INSERT INTO journal(seq,event,payload,prev_hash,entry_hash) VALUES (?,?,?,?,?)",
        (seq, event, enc, prev, entry_hash),
    )


def verify_journal(conn: sqlite3.Connection) -> dict:
    rows = conn.execute("SELECT seq,event,payload,prev_hash,entry_hash FROM journal ORDER BY seq").fetchall()
    prev = GENESIS
    for i, r in enumerate(rows, start=1):
        if r["seq"] != i:
            raise DomainError("journal_chain_broken", f"journal gap at seq {r['seq']}", 409)
        if r["prev_hash"] != prev:
            raise DomainError(
                "journal_chain_broken", f"journal prev-link mismatch at seq {i}", 409
            )
        payload = json.loads(r["payload"])
        if hash_chain(prev, {"event": r["event"], "payload": payload}) != r["entry_hash"]:
            raise DomainError("journal_chain_broken", f"journal entry hash mismatch at seq {i}", 409)
        prev = r["entry_hash"]
    return {"ok": True, "entries": len(rows), "tail": prev}


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #


def _regions_of(node: sqlite3.Row) -> list[list[int]]:
    return json.loads(node["regions"])


def _current_row(conn: sqlite3.Connection, object_key: str) -> Optional[sqlite3.Row]:
    return conn.execute(
        """SELECT d.* FROM decisions d JOIN object_current c ON c.decision_id=d.decision_id
           WHERE c.object_key=?""",
        (object_key,),
    ).fetchone()


def _current_claims(conn: sqlite3.Connection, object_key: str) -> list[list]:
    row = _current_row(conn, object_key)
    return json.loads(row["claims_json"]) if row else []


def _open_case(conn: sqlite3.Connection, object_key: str) -> Optional[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM conflict_cases WHERE object_key=? AND status='open' ORDER BY created_ts,case_id LIMIT 1",
        (object_key,),
    ).fetchone()


def _node_sort_key(n: sqlite3.Row):
    return (n["site_id"], n["site_seq"], n["node_digest"])


def _promote_waiting(conn: sqlite3.Connection, object_key: str) -> int:
    """waiting -> active whenever the predecessor chain is incorporated."""
    moved = 0
    for n in conn.execute(
        "SELECT * FROM nodes WHERE object_key=? AND status='waiting'", (object_key,)
    ).fetchall():
        prev = n["prev_digest"]
        reason = None
        if prev is None:
            status = "active"
        else:
            p = conn.execute("SELECT status FROM nodes WHERE node_digest=? AND object_key=?",
                             (prev, object_key)).fetchone()
            if p is None:
                status, reason = "waiting", f"missing_ancestor:{prev}"
            elif p["status"] != "incorporated":
                status, reason = "waiting", f"ancestor_not_settled:{prev}"
            else:
                status = "active"
        if status == "active":
            conn.execute("UPDATE nodes SET status='active', waiting_reason=NULL WHERE node_digest=?",
                         (n["node_digest"],))
            moved += 1
        elif reason != n["waiting_reason"]:
            conn.execute("UPDATE nodes SET waiting_reason=? WHERE node_digest=?",
                         (reason, n["node_digest"]))
    return moved


# --------------------------------------------------------------------------- #
# convergence planning (deterministic)
# --------------------------------------------------------------------------- #


def _atomic_segments(current_claims: list[list], candidates: list[sqlite3.Row]):
    """Split the union of incumbent + candidate regions into atomic segments."""
    bounds = {0, _REGION_LIMIT}
    for s, e, _ in current_claims:
        bounds.add(s)
        bounds.add(e)
    for n in candidates:
        for s, e in _regions_of(n):
            bounds.add(s)
            bounds.add(e)
    pts = sorted(b for b in bounds if 0 <= b <= _REGION_LIMIT)
    segments = [[pts[i], pts[i + 1]] for i in range(len(pts) - 1) if pts[i] < pts[i + 1]]
    return segments


def _plan(current_claims: list[list], candidates: list[sqlite3.Row]) -> dict:
    """Return {clean, claims|conflict_region, per_segment} for an eligible batch."""
    segments = _atomic_segments(current_claims, candidates)
    incumbent = {(s, e): h for s, e, h in current_claims}

    # node -> covered atomic segments (with its own change digest)
    coverage: dict[str, dict[tuple, str]] = {}
    for n in candidates:
        cover: dict[tuple, str] = {}
        for rs, re_ in _regions_of(n):
            for s, e in segments:
                if rs <= s and e <= re_:
                    cover[(s, e)] = n["change_digest"]
        coverage[n["node_digest"]] = cover

    final_atoms: list[list] = []
    conflict_region = None
    conflict_nodes: set[str] = set()
    for s, e in segments:
        digests = {}  # digest -> node digests claiming it on this segment
        for nd, cover in coverage.items():
            if (s, e) in cover:
                digests.setdefault(cover[(s, e)], []).append(nd)
        inc = incumbent.get((s, e))
        if inc is not None:
            digests.setdefault(inc, []).append(None)  # incumbent side
        if not digests:
            continue  # gap nobody covers
        if len(digests) > 1 and conflict_region is None:
            conflict_region = [s, e]
            for nodes in digests.values():
                conflict_nodes.update(x for x in nodes if x)
        # deterministic per-segment digest regardless of conflict (used only
        # after human choice; for a clean batch there is a single digest)
        chosen = sorted(digests)[0]
        final_atoms.append([s, e, chosen])

    if conflict_region is not None:
        return {"clean": False, "conflict_region": conflict_region,
                "conflict_nodes": sorted(conflict_nodes), "segments": segments}

    # coalesce adjacent segments carrying the same digest
    merged: list[list] = []
    for s, e, h in final_atoms:
        if merged and merged[-1][1] == s and merged[-1][2] == h:
            merged[-1][1] = e
        else:
            merged.append([s, e, h])
    return {"clean": True, "claims": merged}


def _freeze_batch(candidates: list[sqlite3.Row], plan: dict, has_incumbent: bool) -> list[list]:
    """Snapshot [start,end,change_digest,node_digest] for a conflict case.

    The synthetic node digest ``__incumbent__`` represents keeping the current
    determination on the disputed section.
    """
    freeze = []
    if has_incumbent:
        s, e = plan["conflict_region"]
        freeze.append([s, e, "__incumbent__", "__incumbent__"])
    for n in sorted(candidates, key=_node_sort_key):
        for rs, re_ in _regions_of(n):
            freeze.append([rs, re_, n["change_digest"], n["node_digest"]])
    freeze.sort(key=lambda x: (x[0], x[1], x[3]))
    return freeze


# --------------------------------------------------------------------------- #
# pump
# --------------------------------------------------------------------------- #


def _pump_object(conn: sqlite3.Connection, object_key: str) -> None:
    """Drive one object as far as possible: held -> merged or conflict."""
    for _ in range(MAX_PUMP_ITER):
        _promote_waiting(conn, object_key)
        if _open_case(conn, object_key) is not None:
            return  # human gate; active late branches wait in line

        candidates = conn.execute(
            """SELECT * FROM nodes
               WHERE object_key=? AND status='active'
                 AND (prev_digest IS NULL
                      OR EXISTS (SELECT 1 FROM nodes p
                                 WHERE p.node_digest=nodes.prev_digest
                                   AND p.object_key=nodes.object_key
                                   AND p.status='incorporated'))""",
            (object_key,),
        ).fetchall()
        if not candidates:
            return
        candidates = sorted(candidates, key=_node_sort_key)
        cur = _current_row(conn, object_key)
        current_claims = json.loads(cur["claims_json"]) if cur else []
        plan = _plan(current_claims, candidates)

        if not plan["clean"]:
            _open_conflict(conn, object_key, candidates, plan, cur, current_claims,
                           has_incumbent=bool(current_claims))
            return
        _apply_merge(conn, object_key, candidates, plan["claims"], cur, kind="merge", case_id=None)
    raise DomainError("pump_limit", "convergence pump iteration limit exceeded", 500)


def _next_version(conn: sqlite3.Connection, object_key: str) -> int:
    row = conn.execute(
        "SELECT COALESCE(MAX(version_no),0)+1 AS v FROM decisions WHERE object_key=?",
        (object_key,),
    ).fetchone()
    return row["v"]


def _write_decision_and_receipt(
    conn, object_key, version_no, kind, claims, included_nodes, parent_digest, case_id
) -> tuple[str, str, str]:
    ts = now_iso()
    state_digest = digest({"claims": claims})
    decision_record = {
        "object_key": object_key,
        "version_no": version_no,
        "kind": kind,
        "claims": claims,
        "state_digest": state_digest,
        "parent_digest": parent_digest,
        "included_nodes": included_nodes,
        "case_id": case_id,
    }
    decision_id = "dec_" + digest(decision_record)[:24]
    existing = conn.execute("SELECT decision_id FROM decisions WHERE decision_id=?",
                            (decision_id,)).fetchone()
    if existing is None:
        conn.execute(
            """INSERT INTO decisions(decision_id,object_key,version_no,kind,claims_json,
                                     state_digest,parent_digest,case_id,created_ts)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (decision_id, object_key, version_no, kind, canonical.encode(claims).decode(),
             state_digest, parent_digest, case_id, ts),
        )

    seq_row = conn.execute("SELECT COALESCE(MAX(seq),0)+1 AS s FROM receipts WHERE object_key=?",
                           (object_key,)).fetchone()
    seq = seq_row["s"]
    # Receipt payload is inherently redacted: only pseudonym keys, section
    # coordinates and content digests — never site identity or pixels.
    payload = {
        "receipt_version": 1,
        "object_key": object_key,
        "version_no": version_no,
        "decision_id": decision_id,
        "kind": kind,
        "state_digest": state_digest,
        "claims": claims,
        "included_nodes": included_nodes,
        "parent_digest": parent_digest,
        "case_id": case_id,
        "created_ts": ts,
    }
    prev_r = conn.execute(
        "SELECT receipt_hash FROM receipts WHERE object_key=? ORDER BY seq DESC LIMIT 1",
        (object_key,),
    ).fetchone()
    prev_hash = prev_r["receipt_hash"] if prev_r else GENESIS
    receipt_hash = hash_chain(prev_hash, payload)
    receipt_id = "rcp_" + receipt_hash[:24]
    conn.execute(
        """INSERT INTO receipts(receipt_id,object_key,decision_id,version_no,seq,
                                payload_json,receipt_hash,prev_receipt_hash)
           VALUES (?,?,?,?,?,?,?,?)""",
        (receipt_id, object_key, decision_id, version_no, seq,
         canonical.encode(payload).decode(), receipt_hash, prev_hash),
    )
    conn.execute(
        """INSERT INTO object_current(object_key,decision_id) VALUES(?,?)
           ON CONFLICT(object_key) DO UPDATE SET decision_id=excluded.decision_id""",
        (object_key, decision_id),
    )
    return decision_id, receipt_id, receipt_hash


def _apply_merge(conn, object_key, candidates, claims, cur, kind, case_id) -> None:
    version_no = _next_version(conn, object_key)
    parent_digest = cur["state_digest"] if cur else GENESIS
    included = [n["node_digest"] for n in candidates]
    decision_id, receipt_id, _ = _write_decision_and_receipt(
        conn, object_key, version_no, kind, claims, included, parent_digest, case_id
    )
    conn.executemany(
        "UPDATE nodes SET status='incorporated', waiting_reason=NULL, decision_id=? WHERE node_digest=?",
        [(decision_id, n["node_digest"]) for n in candidates],
    )
    append_journal(conn, "decision.issued", {
        "object_key": object_key, "version_no": version_no, "decision_id": decision_id,
        "receipt_id": receipt_id, "kind": kind, "state_digest": digest({"claims": claims}),
        "included_nodes": included, "case_id": case_id,
    })
    # Fault injection: die AFTER rows are written but BEFORE commit, so the
    # transaction is rolled back and restart must resume exactly once.
    if kind == "merge" and os.environ.get("CONVERGE_FAULT") == "crash_before_commit":
        import sys
        print(f"[fault] simulated crash mid-commit on {object_key} v{version_no}", file=sys.stderr)
        sys.stderr.flush()
        os._exit(2)


def _open_conflict(conn, object_key, candidates, plan, cur, current_claims, has_incumbent) -> None:
    version_no = _next_version(conn, object_key)
    parent_digest = cur["state_digest"] if cur else GENESIS
    freeze = _freeze_batch(candidates, plan, has_incumbent)
    # The conflict decision records the current determination: branches
    # disagree on a section and the object is gated for two-person review.
    decision_id, receipt_id, _ = _write_decision_and_receipt(
        conn, object_key, version_no, "conflict", current_claims,
        [], parent_digest, case_id=None,
    )
    case_id = "case_" + digest({
        "object_key": object_key, "decision_id": decision_id,
        "region": plan["conflict_region"], "nodes": plan["conflict_nodes"],
    })[:20]
    ts = now_iso()
    conn.execute(
        """INSERT INTO conflict_cases(case_id,object_key,opened_decision_id,status,step,
                                      choice_node_digest,claims_json,region,created_ts,updated_ts)
           VALUES (?,?,?,'open',1,NULL,?,?,?,?)""",
        (case_id, object_key, decision_id, canonical.encode(freeze).decode(),
         canonical.encode(plan["conflict_region"]).decode(), ts, ts),
    )
    conn.execute("UPDATE decisions SET case_id=? WHERE decision_id=?", (case_id, decision_id))
    conn.executemany(
        "UPDATE nodes SET status='deferred', waiting_reason=? WHERE node_digest=?",
        [(f"conflict:{case_id}", n["node_digest"]) for n in candidates],
    )
    append_journal(conn, "conflict.opened", {
        "object_key": object_key, "version_no": version_no, "decision_id": decision_id,
        "receipt_id": receipt_id, "case_id": case_id,
        "region": plan["conflict_region"], "nodes": plan["conflict_nodes"],
    })


# --------------------------------------------------------------------------- #
# ingest
# --------------------------------------------------------------------------- #


def ingest_envelope(conn: sqlite3.Connection, wlock: WriteLock, raw: Any, request_key: str):
    with wlock:
        try:
            conn.execute("BEGIN IMMEDIATE")
            return _ingest_tx(conn, raw, request_key)
        except Exception:
            conn.rollback()
            raise
        finally:
            _end_tx(conn)


def _end_tx(conn) -> None:
    try:
        conn.commit()
    except sqlite3.OperationalError:
        pass


def _ingest_tx(conn, raw, request_key) -> dict:
    if not isinstance(request_key, str) or not request_key or len(request_key) > 200:
        raise DomainError("invalid_request_key", "request_key must be a non-empty string")
    verify_journal(conn)  # refuse to write on a tampered audit chain

    env = validate_envelope(raw)
    body_hash = body_hash_of(env)
    node_digest = node_digest_of(env, body_hash)
    object_key = env["object_key"]

    # 1) request-key idempotency ledger
    existing_req = conn.execute("SELECT * FROM requests WHERE request_key=?", (request_key,)).fetchone()
    replay = False
    if existing_req:
        if existing_req["body_hash"] != body_hash:
            raise DomainError(
                "request_key_body_conflict",
                "request key was already used with a different body", 409,
            )
        result = json.loads(existing_req["result_json"])
        result["replay"] = existing_req["status"] == "processed"
        return result

    # Never extend an object whose receipt chain has been tampered with.
    if conn.execute("SELECT 1 FROM objects WHERE object_key=?", (object_key,)).fetchone():
        assert_object_intact(conn, object_key)

    # 2) site-local monotonic sequence
    row = conn.execute(
        "SELECT MAX(site_seq) AS m FROM nodes WHERE object_key=? AND site_id=?",
        (object_key, env["site_id"]),
    ).fetchone()
    if row["m"] is not None and env["site_seq"] < row["m"]:
        raise DomainError("site_seq_regressed",
                          f"site_seq must be strictly increasing for site {env['site_id']}", 409)

    # 3) object / node
    if conn.execute("SELECT 1 FROM objects WHERE object_key=?", (object_key,)).fetchone() is None:
        conn.execute("INSERT INTO objects(object_key,created_ts) VALUES(?,?)",
                     (object_key, now_iso()))
        append_journal(conn, "object.created", {"object_key": object_key})

    existing_node = conn.execute("SELECT * FROM nodes WHERE node_digest=?", (node_digest,)).fetchone()
    if existing_node is None:
        # same (object, site, seq) but different content -> hard conflict
        clash = conn.execute(
            "SELECT node_digest FROM nodes WHERE object_key=? AND site_id=? AND site_seq=?",
            (object_key, env["site_id"], env["site_seq"]),
        ).fetchone()
        if clash is not None:
            raise DomainError("site_seq_conflict",
                              "a different change already used this site_seq", 409)

        prev = env["prev_digest"]
        if prev is None:
            status, reason = "active", None
        else:
            p = conn.execute("SELECT status FROM nodes WHERE node_digest=? AND object_key=?",
                             (prev, object_key)).fetchone()
            if p is None:
                status, reason = "waiting", f"missing_ancestor:{prev}"
            elif p["status"] != "incorporated":
                status, reason = "waiting", f"ancestor_not_settled:{prev}"
            else:
                status, reason = "active", None
        conn.execute(
            """INSERT INTO nodes(node_digest,object_key,site_id,site_seq,prev_digest,regions,
                                 body_hash,change_digest,status,waiting_reason,decision_id,
                                 first_request_key,created_ts)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (node_digest, object_key, env["site_id"], env["site_seq"], prev,
             canonical.encode(env["regions"]).decode(), body_hash, env["change_digest"],
             status, reason, None, request_key, now_iso()),
        )
        append_journal(conn, "envelope.received", {
            "object_key": object_key, "node_digest": node_digest,
            "site_id": env["site_id"], "site_seq": env["site_seq"],
            "prev_digest": prev, "status": status,
        })
        node_was_new = True
    else:
        node_was_new = False

    conn.execute(
        """INSERT INTO requests(request_key,object_key,body_hash,node_digest,result_json,status,created_ts)
           VALUES (?,?,?,?,?,'seen',?)""",
        (request_key, object_key, body_hash, node_digest, "{}", now_iso()),
    )

    # 4) converge as far as possible (may settle this node and descendants)
    _pump_object(conn, object_key)

    result = _snapshot_result(conn, object_key, node_digest)
    result["replay"] = False
    conn.execute("UPDATE requests SET status='processed', result_json=? WHERE request_key=?",
                 (canonical.encode(result).decode(), request_key))
    return result


def _snapshot_result(conn, object_key: str, node_digest: str) -> dict:
    n = conn.execute("SELECT * FROM nodes WHERE node_digest=?", (node_digest,)).fetchone()
    cur = _current_row(conn, object_key)
    result = {
        "object_key": object_key,
        "node_digest": node_digest,
        "node_status": n["status"],
        "current_decision_id": cur["decision_id"] if cur else None,
        "current_version_no": cur["version_no"] if cur else None,
    }
    if n["status"] == "waiting":
        result["waiting_reason"] = n["waiting_reason"]
    if n["status"] == "deferred":
        result["waiting_reason"] = n["waiting_reason"]
        case_id = n["waiting_reason"].split(":", 1)[1]
        result["case_id"] = case_id
    if n["status"] == "incorporated":
        r = conn.execute("SELECT receipt_id FROM receipts WHERE decision_id=?",
                         (n["decision_id"],)).fetchone()
        result["decision_id"] = n["decision_id"]
        result["receipt_id"] = r["receipt_id"] if r else None
    return result


# --------------------------------------------------------------------------- #
# two-person adjudication
# --------------------------------------------------------------------------- #


def submit_review(conn, wlock, reviewer: str, case_id: str, action: str,
                  choice_node_digest: Optional[str], note: str) -> dict:
    with wlock:
        try:
            conn.execute("BEGIN IMMEDIATE")
            verify_journal(conn)
            case = conn.execute("SELECT * FROM conflict_cases WHERE case_id=?", (case_id,)).fetchone()
            if case is None:
                raise DomainError("case_not_found", "unknown conflict case", 404)
            if case["status"] != "open":
                raise DomainError("case_not_open", f"case already {case['status']}", 409)
            assert_object_intact(conn, case["object_key"])
            if action not in ("approve", "reject"):
                raise DomainError("invalid_review", "action must be approve/reject")

            step = case["step"]
            prior = conn.execute(
                "SELECT reviewer FROM reviews WHERE case_id=? AND action='approve' ORDER BY id",
                (case_id,),
            ).fetchall()
            prior_reviewers = {r["reviewer"] for r in prior}
            if reviewer in prior_reviewers:
                raise DomainError(
                    "reviewer_must_differ",
                    "the two approvals must come from two different reviewers", 409,
                )

            if action == "reject":
                conn.execute(
                    """INSERT INTO reviews(case_id,reviewer,step,action,choice_node_digest,note,created_ts)
                       VALUES (?,?,?,'reject',?,?,?)""",
                    (case_id, reviewer, step, choice_node_digest, note or "", now_iso()),
                )
                append_journal(conn, "conflict.objection",
                               {"case_id": case_id, "object_key": case["object_key"],
                                "reviewer": reviewer})
                result = {"case_id": case_id, "status": "open", "step": step,
                          "note": "objection recorded; case stays open for adjudication"}
                conn.commit()
                return result

            freeze = json.loads(case["claims_json"])
            frozen_nodes = {row[3] for row in freeze}
            if step == 1:
                if not choice_node_digest or choice_node_digest not in frozen_nodes:
                    raise DomainError(
                        "invalid_choice",
                        "step 1 approval must select a node digest from the conflicting batch",
                    )
                conn.execute(
                    """INSERT INTO reviews(case_id,reviewer,step,action,choice_node_digest,note,created_ts)
                       VALUES (?,? ,1,'approve',?,?,?)""",
                    (case_id, reviewer, choice_node_digest, note or "", now_iso()),
                )
                conn.execute(
                    "UPDATE conflict_cases SET step=2, choice_node_digest=?, updated_ts=? WHERE case_id=?",
                    (choice_node_digest, now_iso(), case_id),
                )
                append_journal(conn, "conflict.approval1",
                               {"case_id": case_id, "object_key": case["object_key"],
                                "choice_node_digest": choice_node_digest})
                result = {"case_id": case_id, "status": "open", "step": 2}
                conn.commit()
                return result

            # step 2: second, different reviewer confirms
            winner = case["choice_node_digest"]
            conn.execute(
                """INSERT INTO reviews(case_id,reviewer,step,action,choice_node_digest,note,created_ts)
                   VALUES (?,?,2,'approve',?,?,?)""",
                (case_id, reviewer, winner, note or "", now_iso()),
            )
            decision_id = _resolve_case(conn, case, winner)
            conn.commit()
            return {"case_id": case_id, "status": "resolved",
                    "resolved_decision_id": decision_id}
        except Exception:
            conn.rollback()
            raise


def _resolve_case(conn, case, winner: str) -> str:
    object_key = case["object_key"]
    opened = conn.execute("SELECT * FROM decisions WHERE decision_id=?",
                          (case["opened_decision_id"],)).fetchone()
    base_claims = json.loads(opened["claims_json"])

    # frozen deferred batch, in deterministic order; the winner applies last
    freeze = json.loads(case["claims_json"])
    batch_rows = []
    seen = set()
    for rs, re_, ch, nd in freeze:
        if nd in seen or nd == "__incumbent__":
            continue
        seen.add(nd)
        row = conn.execute("SELECT * FROM nodes WHERE node_digest=?", (nd,)).fetchone()
        batch_rows.append(row)
    batch_rows.sort(key=_node_sort_key)
    ordered = list(batch_rows)
    winner_is_incumbent = winner == "__incumbent__"
    winner_row = None
    if not winner_is_incumbent:
        winner_row = conn.execute("SELECT * FROM nodes WHERE node_digest=?", (winner,)).fetchone()
        ordered = [n for n in ordered if n["node_digest"] != winner]
        ordered.append(winner_row)

    # Rebuild atomic claims applying the adjudicated choice.
    segments = _atomic_segments(base_claims, ordered)
    incumbent = {(s, e): h for s, e, h in base_claims}
    winner_digest = winner_row["change_digest"] if winner_row is not None else None
    atoms = []
    for s, e in segments:
        inc = incumbent.get((s, e))
        covering = [(n, n["change_digest"]) for n in ordered
                    if any(rs <= s and e <= re_ for rs, re_ in _regions_of(n))]
        distinct = {d for _, d in covering}
        if inc is not None:
            distinct.add(inc)
        if not distinct:
            continue
        if len(distinct) == 1:
            # unambiguous section: a candidate's content (winner ordered last),
            # else the incumbent content
            chosen = covering[-1][1] if covering else inc
        elif winner_is_incumbent:
            # human chose to keep the current determination on disputed sections
            chosen = inc if inc is not None else covering[-1][1]
        elif winner_row is not None and any(rs <= s and e <= re_
                                            for rs, re_ in _regions_of(winner_row)):
            chosen = winner_digest
        else:
            # disputed section the winning node does not cover: keep incumbent
            # if one exists, otherwise the deterministic order decides
            chosen = inc if inc is not None else covering[-1][1]
        atoms.append([s, e, chosen])
    merged: list[list] = []
    for s, e, h in atoms:
        if merged and merged[-1][1] == s and merged[-1][2] == h:
            merged[-1][1] = e
        else:
            merged.append([s, e, h])

    decision_id, receipt_id, _ = _write_decision_and_receipt(
        conn, object_key, _next_version(conn, object_key), "merge", merged,
        [n["node_digest"] for n in ordered], opened["state_digest"], case_id=case["case_id"],
    )
    conn.executemany(
        "UPDATE nodes SET status='incorporated', waiting_reason=NULL, decision_id=? WHERE node_digest=?",
        [(decision_id, n["node_digest"]) for n in ordered],
    )
    ts = now_iso()
    conn.execute(
        """UPDATE conflict_cases SET status='resolved', resolved_decision_id=?, updated_ts=?
           WHERE case_id=?""",
        (decision_id, ts, case["case_id"]),
    )
    append_journal(conn, "conflict.resolved", {
        "case_id": case["case_id"], "object_key": object_key,
        "decision_id": decision_id, "receipt_id": receipt_id,
        "choice_node_digest": winner,
    })
    # New branches that arrived while the case was open converge now, and may
    # themselves open a fresh conflict.
    _pump_object(conn, object_key)
    return decision_id


# --------------------------------------------------------------------------- #
# receipt verification
# --------------------------------------------------------------------------- #


def verify_object(conn, object_key: str) -> dict:
    obj = conn.execute("SELECT 1 FROM objects WHERE object_key=?", (object_key,)).fetchone()
    if obj is None:
        raise DomainError("object_not_found", "unknown object", 404)
    rows = conn.execute(
        "SELECT * FROM receipts WHERE object_key=? ORDER BY seq", (object_key,)
    ).fetchall()
    prev = GENESIS
    last_state = GENESIS
    for i, r in enumerate(rows, start=1):
        if r["seq"] != i:
            raise DomainError("receipt_chain_broken",
                              f"receipt sequence gap: expected {i}, got {r['seq']}", 409)
        payload = json.loads(r["payload_json"])
        if r["prev_receipt_hash"] != prev:
            raise DomainError("receipt_chain_broken",
                              f"receipt {r['receipt_id']}: previous-link mismatch", 409)
        if hash_chain(prev, payload) != r["receipt_hash"]:
            raise DomainError("receipt_chain_broken",
                              f"receipt {r['receipt_id']}: hash mismatch (chain tampered)", 409)
        # cross-check receipt payload against the authoritative decisions table
        d = conn.execute("SELECT * FROM decisions WHERE decision_id=?",
                         (r["decision_id"],)).fetchone()
        if d is None:
            raise DomainError("receipt_chain_broken",
                              f"receipt {r['receipt_id']}: decision missing", 409)
        if payload["state_digest"] != d["state_digest"]:
            raise DomainError("receipt_chain_broken",
                              f"receipt {r['receipt_id']}: state digest disagrees with decision",
                              409)
        if digest({"claims": json.loads(d["claims_json"])}) != d["state_digest"]:
            raise DomainError("receipt_chain_broken",
                              f"decision {d['decision_id']}: stored claims disagree with digest",
                              409)
        if payload["parent_digest"] != last_state and i > 1:
            raise DomainError("receipt_chain_broken",
                              f"receipt {r['receipt_id']}: parent state digest mismatch", 409)
        prev = r["receipt_hash"]
        last_state = payload["state_digest"]

    cur = _current_row(conn, object_key)
    if cur is not None:
        if not rows or rows[-1]["decision_id"] != cur["decision_id"]:
            raise DomainError("receipt_chain_broken",
                              "current decision is not anchored at the receipt chain tail", 409)
        if digest({"claims": json.loads(cur["claims_json"])}) != cur["state_digest"]:
            raise DomainError("receipt_chain_broken", "current decision digest mismatch", 409)
    return {"ok": True, "object_key": object_key, "receipts": len(rows),
            "tail_receipt_hash": prev, "current_state_digest": last_state}


def assert_object_intact(conn, object_key: str) -> None:
    """Called before extending an object: never build on a tampered chain."""
    verify_object(conn, object_key)


# --------------------------------------------------------------------------- #
# boot recovery
# --------------------------------------------------------------------------- #


def boot_recover(conn, wlock: WriteLock) -> dict:
    """Resume everything from durable state after a crash/restart."""
    journal_ok = verify_journal(conn)
    with wlock:
        conn.execute("BEGIN IMMEDIATE")
        try:
            # finalize any in-flight request markers left by a dead process
            # Resolve request markers left by a process that died mid-write:
            # if the node survived (earlier tx), record its final state;
            # otherwise drop the marker so a client retry is reprocessed.
            stale = conn.execute(
                "SELECT request_key, node_digest, object_key FROM requests WHERE status='seen'"
            ).fetchall()
            for r in stale:
                n = conn.execute("SELECT status FROM nodes WHERE node_digest=?",
                                 (r["node_digest"],)).fetchone()
                if n is not None:
                    _pump_object(conn, r["object_key"])
                    result = _snapshot_result(conn, r["object_key"], r["node_digest"])
                    result["replay"] = False
                    conn.execute(
                        "UPDATE requests SET status='processed', result_json=? WHERE request_key=?",
                        (canonical.encode(result).decode(), r["request_key"]))
                else:
                    conn.execute("DELETE FROM requests WHERE request_key=?", (r["request_key"],))
            objects = [r["object_key"] for r in conn.execute("SELECT object_key FROM objects").fetchall()]
            waited = 0
            for ok_ in objects:
                try:
                    verify_object(conn, ok_)  # never converge on a tampered chain
                except DomainError:
                    continue
                _pump_object(conn, ok_)
                waited += conn.execute(
                    "SELECT COUNT(*) c FROM nodes WHERE object_key=? AND status='waiting'",
                    (ok_,),
                ).fetchone()["c"]
            if stale or objects:
                append_journal(conn, "boot.recovered",
                               {"objects": len(objects), "stale_requests": len(stale),
                                "still_waiting": waited})
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    # report (do not block boot on) any tampered receipt chains; writes to the
    # affected objects fail deterministically via assert_object_intact
    broken = []
    for r in conn.execute("SELECT object_key FROM objects").fetchall():
        try:
            verify_object(conn, r["object_key"])
        except DomainError as e:
            broken.append({"object_key": r["object_key"], "error": e.code})
    return {"journal": journal_ok, "objects": len(objects or []),
            "broken_receipt_chains": broken}


# --------------------------------------------------------------------------- #
# queries
# --------------------------------------------------------------------------- #


def list_objects(conn) -> list[dict]:
    out = []
    for r in conn.execute("SELECT object_key, created_ts FROM objects ORDER BY created_ts,object_key").fetchall():
        ok = r["object_key"]
        cur = _current_row(conn, ok)
        case = _open_case(conn, ok)
        counts = {s: conn.execute("SELECT COUNT(*) c FROM nodes WHERE object_key=? AND status=?",
                                  (ok, s)).fetchone()["c"]
                  for s in ("waiting", "active", "deferred", "incorporated")}
        out.append({
            "object_key": ok,
            "current_version_no": cur["version_no"] if cur else 0,
            "current_decision_id": cur["decision_id"] if cur else None,
            "current_kind": cur["kind"] if cur else None,
            "state": "conflict" if case else ("waiting" if counts["waiting"] else "current"),
            "open_case_id": case["case_id"] if case else None,
            "node_counts": counts,
        })
    return out


def object_detail(conn, object_key: str, redact_sites: bool = False) -> dict:
    if conn.execute("SELECT 1 FROM objects WHERE object_key=?", (object_key,)).fetchone() is None:
        raise DomainError("object_not_found", "unknown object", 404)
    cur = _current_row(conn, object_key)
    case = _open_case(conn, object_key)
    nodes = conn.execute("SELECT * FROM nodes WHERE object_key=? ORDER BY created_ts,node_digest",
                         (object_key,)).fetchall()
    site_ids = sorted({n["site_id"] for n in nodes})
    alias = {s: f"site-{i+1:02d}" for i, s in enumerate(site_ids)}

    node_out = []
    for n in nodes:
        node_out.append({
            "node_digest": n["node_digest"],
            "site_id": alias[n["site_id"]] if redact_sites else n["site_id"],
            "site_seq": n["site_seq"],
            "prev_digest": n["prev_digest"],
            "regions": json.loads(n["regions"]),
            "change_digest": n["change_digest"],
            "status": n["status"],
            "waiting_reason": n["waiting_reason"],
            "decision_id": n["decision_id"],
        })

    decisions = []
    for d in conn.execute(
        "SELECT * FROM decisions WHERE object_key=? ORDER BY version_no", (object_key,)
    ).fetchall():
        r = conn.execute("SELECT receipt_id,receipt_hash FROM receipts WHERE decision_id=?",
                         (d["decision_id"],)).fetchone()
        decisions.append({
            "decision_id": d["decision_id"],
            "version_no": d["version_no"],
            "kind": d["kind"],
            "state_digest": d["state_digest"],
            "claims": json.loads(d["claims_json"]),
            "case_id": d["case_id"],
            "receipt_id": r["receipt_id"] if r else None,
            "receipt_hash": r["receipt_hash"] if r else None,
        })

    case_out = None
    if case is not None:
        reviews = conn.execute("SELECT reviewer,step,action,choice_node_digest,note,created_ts FROM reviews WHERE case_id=? ORDER BY id",
                               (case["case_id"],)).fetchall()
        case_out = {
            "case_id": case["case_id"],
            "status": case["status"],
            "step": case["step"],
            "region": json.loads(case["region"]),
            "frozen_batch": json.loads(case["claims_json"]),
            "choice_node_digest": case["choice_node_digest"],
            "reviews": [dict(x) for x in reviews],
        }
    return {
        "object_key": object_key,
        "current_decision_id": cur["decision_id"] if cur else None,
        "current_version_no": cur["version_no"] if cur else None,
        "current_state_digest": cur["state_digest"] if cur else None,
        "nodes": node_out,
        "decisions": decisions,
        "open_case": case_out,
    }


def list_cases(conn, status: str = "open") -> list[dict]:
    if status not in ("open", "resolved", "rejected", "all"):
        status = "open"
    q = "SELECT * FROM conflict_cases"
    args: list = []
    if status != "all":
        q += " WHERE status=?"
        args.append(status)
    q += " ORDER BY created_ts,case_id"
    out = []
    for c in conn.execute(q, args).fetchall():
        out.append({
            "case_id": c["case_id"],
            "object_key": c["object_key"],
            "status": c["status"],
            "step": c["step"],
            "region": json.loads(c["region"]),
            "choice_node_digest": c["choice_node_digest"],
            "frozen_batch": json.loads(c["claims_json"]),
            "resolved_decision_id": c["resolved_decision_id"],
        })
    return out


def receipt_bundle(conn, object_key: str) -> dict:
    if conn.execute("SELECT 1 FROM objects WHERE object_key=?", (object_key,)).fetchone() is None:
        raise DomainError("object_not_found", "unknown object", 404)
    receipts = []
    for r in conn.execute(
        "SELECT seq,version_no,receipt_id,receipt_hash,prev_receipt_hash,payload_json FROM receipts WHERE object_key=? ORDER BY seq",
        (object_key,),
    ).fetchall():
        receipts.append({
            "seq": r["seq"],
            "version_no": r["version_no"],
            "receipt_id": r["receipt_id"],
            "receipt_hash": r["receipt_hash"],
            "prev_receipt_hash": r["prev_receipt_hash"],
            "payload": json.loads(r["payload_json"]),
        })
    return {
        "bundle": "convergence-receipt-chain/v1",
        "object_key": object_key,
        "note": "redacted: only pseudonymous keys, section coordinates and content digests",
        "receipts": receipts,
    }
