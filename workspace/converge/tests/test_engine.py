"""Engine-level tests: graph rebuild, held envelopes, deterministic merge,
two-person adjudication, receipt chains, tamper detection, crash recovery."""
import json
import os
import sqlite3
import sys
import tempfile
import threading
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app import engine  # noqa: E402
from app.engine import body_hash_of, node_digest_of, validate_envelope  # noqa: E402
from app.store import WriteLock, connect, init  # noqa: E402
from tests.helpers import H_A, H_B, H_C, env  # noqa: E402


class EngineCase(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp(prefix="conv-eng-")
        self.conn = connect(os.path.join(self.dir, "t.db"))
        init(self.conn)
        self.wl = WriteLock()

    def tearDown(self):
        self.conn.close()

    def put(self, e, key):
        return engine.ingest_envelope(self.conn, self.wl, e, key)

    def digest_of(self, raw):
        v = validate_envelope(raw)
        return node_digest_of(v, body_hash_of(v))

    # 1. normal deterministic merge of mutually exclusive siblings ----------
    def test_clean_merge_disjoint_siblings(self):
        root = env("O", "s1", 0, None, [(0, 10)], H_A)
        r0 = self.put(root, "k0")
        self.assertEqual(r0["node_status"], "incorporated")
        d0 = r0["node_digest"]
        self.put(env("O", "s2", 0, d0, [(10, 20)], H_B), "k1")
        r2 = self.put(env("O", "s3", 0, d0, [(20, 30)], H_C), "k2")
        self.assertEqual(r2["current_version_no"], 3)
        detail = engine.object_detail(self.conn, "O")
        claims = detail["decisions"][-1]["claims"]
        self.assertEqual(claims, [[0, 10, H_A], [10, 20, H_B], [20, 30, H_C]])
        # exactly one current decision
        cur = self.conn.execute("SELECT COUNT(*) c FROM object_current").fetchone()["c"]
        self.assertEqual(cur, 1)
        # receipt chain verifies
        self.assertTrue(engine.verify_object(self.conn, "O")["ok"])

    def test_merge_order_deterministic_regardless_of_arrival(self):
        # same graph, opposite arrival order -> same final state digest
        finals = []
        for order in (("s2", "s3"), ("s3", "s2")):
            db = os.path.join(self.dir, "ord" + order[0])
            c = connect(db)
            init(c)
            wl = WriteLock()
            r0 = engine.ingest_envelope(c, wl, env("O", "s1", 0, None, [(0, 10)], H_A), "k0")
            d0 = r0["node_digest"]
            payloads = {
                "s2": env("O", "s2", 0, d0, [(10, 20)], H_B),
                "s3": env("O", "s3", 0, d0, [(20, 30)], H_C),
            }
            for i, s in enumerate(order):
                engine.ingest_envelope(c, wl, payloads[s], f"k-{s}")
            finals.append(engine.object_detail(c, "O")["current_state_digest"])
            c.close()
        self.assertEqual(finals[0], finals[1])

    # 2. late ancestor: held then auto-resumed ------------------------------
    def test_late_ancestor_held_and_resumed(self):
        parent_raw = env("O2", "s1", 0, None, [(0, 5)], H_B)
        pd = self.digest_of(parent_raw)
        # child arrives first with a reference to a node the server can derive
        child = env("O2", "s2", 0, pd, [(5, 10)], H_A)
        r1 = self.put(child, "c1")
        self.assertEqual(r1["node_status"], "waiting")
        self.assertIn("missing_ancestor", r1["waiting_reason"])
        # parent lands -> child auto-continues within the same call
        r2 = self.put(parent_raw, "c2")
        detail = engine.object_detail(self.conn, "O2")
        statuses = {n["site_id"]: n["status"] for n in detail["nodes"]}
        self.assertEqual(statuses, {"s1": "incorporated", "s2": "incorporated"})
        self.assertEqual(detail["current_version_no"], 2)

    def test_cascading_held_chain(self):
        g_raw = env("O3", "s1", 0, None, [(0, 5)], H_A)
        gd = self.digest_of(g_raw)
        m_raw = env("O3", "s2", 0, gd, [(5, 10)], H_B)
        md = self.digest_of(m_raw)
        r = self.put(env("O3", "s3", 0, md, [(10, 15)], H_C), "leaf")
        self.assertEqual(r["node_status"], "waiting")
        self.put(g_raw, "g")
        # middle still missing -> leaf remains waiting, never crosses ancestor
        detail = engine.object_detail(self.conn, "O3")
        self.assertEqual({n["site_id"]: n["status"] for n in detail["nodes"]},
                         {"s3": "waiting", "s1": "incorporated"})
        self.put(m_raw, "m")
        detail = engine.object_detail(self.conn, "O3")
        self.assertTrue(all(n["status"] == "incorporated" for n in detail["nodes"]))

    # 3. request-key semantics ----------------------------------------------
    def test_same_key_same_body_returns_original_result(self):
        r1 = self.put(env("O4", "s1", 0, None, [(0, 5)], H_A), "dup")
        r2 = self.put(env("O4", "s1", 0, None, [(0, 5)], H_A), "dup")
        self.assertTrue(r2["replay"])
        self.assertEqual(r1["decision_id"], r2["decision_id"])
        self.assertEqual(r1["receipt_id"], r2["receipt_id"])

    def test_same_key_different_body_rejected(self):
        self.put(env("O4", "s1", 0, None, [(0, 5)], H_A), "key")
        with self.assertRaises(engine.DomainError) as cm:
            self.put(env("O4", "s1", 0, None, [(0, 5)], H_B), "key")
        self.assertEqual(cm.exception.code, "request_key_body_conflict")
        self.assertEqual(cm.exception.status, 409)

    # 4. same-section conflict + two-person adjudication --------------------
    def test_same_region_conflict_and_two_person_review(self):
        r0 = self.put(env("C", "s1", 0, None, [(0, 10)], H_A), "c0")
        self.put(env("C", "s2", 0, r0["node_digest"], [(0, 10)], H_B), "c1")
        case = engine.list_cases(self.conn)[0]
        self.assertEqual(case["status"], "open")
        frozen = engine.object_detail(self.conn, "C")["open_case"]["frozen_batch"]
        winner = next(row[3] for row in frozen if row[3] != "__incumbent__")

        # a single approval does not resolve
        a1 = engine.submit_review(self.conn, self.wl, "reviewer-a",
                                  case["case_id"], "approve", winner, "")
        self.assertEqual(a1["step"], 2)
        # same reviewer cannot do both steps
        with self.assertRaises(engine.DomainError) as cm:
            engine.submit_review(self.conn, self.wl, "reviewer-a",
                                 case["case_id"], "approve", winner, "")
        self.assertEqual(cm.exception.code, "reviewer_must_differ")
        # second, different reviewer resolves
        res = engine.submit_review(self.conn, self.wl, "reviewer-b",
                                   case["case_id"], "approve", winner, "")
        self.assertEqual(res["status"], "resolved")
        detail = engine.object_detail(self.conn, "C")
        self.assertEqual(detail["decisions"][-1]["kind"], "merge")
        self.assertEqual(detail["decisions"][-1]["claims"], [[0, 10, H_B]])
        # chain: root -> conflict -> resolving merge
        kinds = [d["kind"] for d in detail["decisions"]]
        self.assertEqual(kinds, ["merge", "conflict", "merge"])
        self.assertTrue(engine.verify_object(self.conn, "C")["ok"])

    def test_objection_keeps_case_open(self):
        r0 = self.put(env("D", "s1", 0, None, [(0, 10)], H_A), "d0")
        self.put(env("D", "s2", 0, r0["node_digest"], [(0, 10)], H_B), "d1")
        cid = engine.list_cases(self.conn)[0]["case_id"]
        frozen = engine.object_detail(self.conn, "D")["open_case"]["frozen_batch"]
        winner = next(row[3] for row in frozen if row[3] != "__incumbent__")
        out = engine.submit_review(self.conn, self.wl, "reviewer-a", cid, "reject", winner, "异议")
        self.assertEqual(out["status"], "open")
        self.assertEqual(engine.list_cases(self.conn)[0]["step"], 1)

    def test_conflict_resolution_keep_incumbent(self):
        # incumbent covers [0,10); sibling proposes different content on [0,10)
        # and new content on disjoint [10,20); reviewers choose to keep the
        # incumbent on the disputed section — sibling still contributes [10,20)
        r0 = self.put(env("G", "s1", 0, None, [(0, 10)], H_A), "g0")
        self.put(env("G", "s2", 0, r0["node_digest"], [(0, 20)], H_B), "g1")
        cid = engine.list_cases(self.conn)[0]["case_id"]
        engine.submit_review(self.conn, self.wl, "reviewer-a", cid,
                             "approve", "__incumbent__", "")
        engine.submit_review(self.conn, self.wl, "reviewer-b", cid,
                             "approve", "__incumbent__", "")
        detail = engine.object_detail(self.conn, "G")
        self.assertEqual(detail["decisions"][-1]["claims"],
                         [[0, 10, H_A], [10, 20, H_B]])
        self.assertTrue(all(n["status"] == "incorporated" for n in detail["nodes"]))

    def test_new_branch_while_case_open_waits_then_converges(self):
        r0 = self.put(env("E", "s1", 0, None, [(0, 10)], H_A), "e0")
        self.put(env("E", "s2", 0, r0["node_digest"], [(0, 10)], H_B), "e1")
        # a disjoint branch arrives while the conflict is open
        r3 = self.put(env("E", "s3", 0, r0["node_digest"], [(10, 20)], H_C), "e2")
        self.assertEqual(r3["node_status"], "active")
        cid = engine.list_cases(self.conn)[0]["case_id"]
        frozen = engine.object_detail(self.conn, "E")["open_case"]["frozen_batch"]
        winner = next(row[3] for row in frozen if row[3] != "__incumbent__")
        engine.submit_review(self.conn, self.wl, "reviewer-a", cid, "approve", winner, "")
        engine.submit_review(self.conn, self.wl, "reviewer-b", cid, "approve", winner, "")
        detail = engine.object_detail(self.conn, "E")
        self.assertEqual(detail["decisions"][-1]["claims"],
                         [[0, 10, H_B], [10, 20, H_C]])
        self.assertTrue(all(n["status"] == "incorporated" for n in detail["nodes"]))

    # 5. concurrent unique decision -----------------------------------------
    def test_concurrent_ingest_single_current_decision(self):
        errors = []

        def worker(i):
            try:
                engine.ingest_envelope(
                    self.conn, self.wl,
                    env("CC", f"s{i}", 0, None, [(i * 10, i * 10 + 5)], H_A),
                    f"k-{i}",
                )
            except Exception as e:  # noqa: BLE001
                errors.append(e)
        threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(errors, [])
        detail = engine.object_detail(self.conn, "CC")
        # one chain, all nodes incorporated, deterministic union of sections
        self.assertTrue(all(n["status"] == "incorporated" for n in detail["nodes"]))
        cur = self.conn.execute("SELECT COUNT(*) c FROM object_current WHERE object_key='CC'").fetchone()
        self.assertEqual(cur["c"], 1)
        self.assertTrue(engine.verify_object(self.conn, "CC")["ok"])

    def test_concurrent_same_request_key_single_ledger_row(self):
        results = []

        def worker():
            results.append(engine.ingest_envelope(
                self.conn, self.wl, env("RR", "s1", 0, None, [(0, 5)], H_A), "same-key"))
        ts = [threading.Thread(target=worker) for _ in range(6)]
        for t in ts:
            t.start()
        for t in ts:
            t.join()
        ids = {r["decision_id"] for r in results}
        self.assertEqual(len(ids), 1)
        n = self.conn.execute("SELECT COUNT(*) c FROM requests WHERE request_key='same-key'").fetchone()
        self.assertEqual(n["c"], 1)
        n2 = self.conn.execute("SELECT COUNT(*) c FROM receipts").fetchone()
        self.assertEqual(n2["c"], 1)

    # 6. crash recovery ------------------------------------------------------
    def test_crash_before_commit_resumes_without_duplicate_decision(self):
        os.environ["CONVERGE_FAULT"] = "crash_before_commit"
        try:
            pid = os.fork()
            if pid == 0:
                try:
                    engine.ingest_envelope(self.conn, self.wl,
                                           env("F", "s1", 0, None, [(0, 5)], H_A), "f0")
                except Exception:
                    pass
                os._exit(0)
            _, status = os.waitpid(pid, 0)
            self.assertTrue(os.WIFEXITED(status))
        finally:
            os.environ.pop("CONVERGE_FAULT", None)

        # The forked child shared the connection; reopen the database file to
        # mimic a process restart.
        self.conn.close()
        self.conn = connect(os.path.join(self.dir, "t.db"))
        rec = engine.boot_recover(self.conn, self.wl)
        self.assertEqual(rec["broken_receipt_chains"], [])
        # nothing half-published before restart...
        self.assertEqual(self.conn.execute("SELECT COUNT(*) c FROM decisions").fetchone()["c"], 0)
        # ...and replay of the same request key resumes exactly once
        r = engine.ingest_envelope(self.conn, self.wl,
                                   env("F", "s1", 0, None, [(0, 5)], H_A), "f0")
        self.assertEqual(r["node_status"], "incorporated")
        self.assertEqual(self.conn.execute("SELECT COUNT(*) c FROM decisions").fetchone()["c"], 1)
        self.assertEqual(self.conn.execute("SELECT COUNT(*) c FROM receipts").fetchone()["c"], 1)

    # 7. receipt tamper -> deterministic error -------------------------------
    def test_receipt_chain_tamper_detected(self):
        self.put(env("T", "s1", 0, None, [(0, 5)], H_A), "t0")
        self.assertTrue(engine.verify_object(self.conn, "T")["ok"])

        row = self.conn.execute("SELECT payload_json FROM receipts WHERE object_key='T'").fetchone()
        payload = json.loads(row["payload_json"])
        payload["claims"][0][2] = H_B  # mutate claimed content
        self.conn.execute("UPDATE receipts SET payload_json=? WHERE object_key='T'",
                          (json.dumps(payload, sort_keys=True),))
        self.conn.commit()
        with self.assertRaises(engine.DomainError) as cm:
            engine.verify_object(self.conn, "T")
        self.assertEqual(cm.exception.code, "receipt_chain_broken")
        self.assertEqual(cm.exception.status, 409)

    def test_tampered_chain_blocks_new_decision(self):
        r0 = self.put(env("U", "s1", 0, None, [(0, 5)], H_A), "u0")
        self.conn.execute("UPDATE receipts SET receipt_hash=? WHERE object_key='U'",
                          ("f" * 64,))
        self.conn.commit()
        with self.assertRaises(engine.DomainError) as cm:
            self.put(env("U", "s2", 0, r0["node_digest"], [(5, 9)], H_B), "u1")
        self.assertEqual(cm.exception.code, "receipt_chain_broken")

    def test_journal_tamper_detected(self):
        self.put(env("V", "s1", 0, None, [(0, 5)], H_A), "v0")
        self.assertTrue(engine.verify_journal(self.conn)["ok"])
        self.conn.execute("UPDATE journal SET event='forged' WHERE seq=1")
        self.conn.commit()
        with self.assertRaises(engine.DomainError) as cm:
            engine.verify_journal(self.conn)
        self.assertEqual(cm.exception.code, "journal_chain_broken")

    # 8. strict schema: no identity/pixels can enter ------------------------
    def test_unexpected_fields_rejected(self):
        for bad in [
            {**env("X", "s1", 0, None, [(0, 5)], H_A), "patient_name": "John"},
            {**env("X", "s1", 0, None, [(0, 5)], H_A), "pixels": "base64..."},
        ]:
            with self.assertRaises(engine.DomainError) as cm:
                self.put(bad, "x")
            self.assertEqual(cm.exception.code, "invalid_envelope")

    def test_overlapping_regions_within_envelope_rejected(self):
        with self.assertRaises(engine.DomainError):
            self.put(env("X", "s1", 0, None, [(0, 10), (5, 15)], H_A), "x")

    def test_site_seq_regression_rejected(self):
        self.put(env("S", "s1", 2, None, [(0, 5)], H_A), "s2")
        with self.assertRaises(engine.DomainError) as cm:
            self.put(env("S", "s1", 1, None, [(5, 9)], H_B), "s1")
        self.assertEqual(cm.exception.code, "site_seq_regressed")

    # 9. recovery from durable state marks held envelopes -------------------
    def test_boot_recovers_survived_node_under_seen_request(self):
        # Simulate a crash boundary: node + object + journal committed, but the
        # request ledger was left at 'seen'. Boot must finalize it without
        # duplicating any decision.
        from app import canonical
        r = self.put(env("BOOT", "s1", 0, None, [(0, 5)], H_A), "boot-1")
        self.assertEqual(r["node_status"], "incorporated")
        # fabricate the durable state of a second, disjoint envelope interrupted
        raw = env("BOOT", "s2", 0, r["node_digest"], [(5, 9)], H_B)
        v = validate_envelope(raw)
        bh = body_hash_of(v)
        nd = node_digest_of(v, bh)
        self.conn.execute(
            """INSERT INTO nodes(node_digest,object_key,site_id,site_seq,prev_digest,regions,
                                body_hash,change_digest,status,waiting_reason,decision_id,
                                first_request_key,created_ts)
               VALUES (?,?,?,?,?,?,?,?,'active',NULL,NULL,?,?)""",
            (nd, "BOOT", "s2", 0, r["node_digest"],
             canonical.encode(v["regions"]).decode(), bh, H_B,
             "boot-2", "2026-01-01T00:00:00Z"),
        )
        self.conn.execute(
            """INSERT INTO requests(request_key,object_key,body_hash,node_digest,result_json,status,created_ts)
               VALUES ('boot-2','BOOT',?,?,'{}','seen','2026-01-01T00:00:00Z')""",
            (bh, nd),
        )
        self.conn.commit()
        self.conn.close()
        self.conn = connect(os.path.join(self.dir, "t.db"))
        engine.boot_recover(self.conn, self.wl)
        marker = self.conn.execute(
            "SELECT status FROM requests WHERE request_key='boot-2'").fetchone()["status"]
        self.assertEqual(marker, "processed")
        self.assertEqual(self.conn.execute(
            "SELECT COUNT(*) c FROM decisions WHERE object_key='BOOT'").fetchone()["c"], 2)
        # a client retry returns the recovered original result, not a new one
        again = engine.ingest_envelope(self.conn, self.wl, raw, "boot-2")
        self.assertTrue(again["replay"])
        self.assertEqual(self.conn.execute(
            "SELECT COUNT(*) c FROM decisions WHERE object_key='BOOT'").fetchone()["c"], 2)

    def test_boot_recover_resumes_pending(self):
        parent_raw = env("W", "s1", 0, None, [(0, 5)], H_A)
        pd = self.digest_of(parent_raw)
        self.put(env("W", "s2", 0, pd, [(5, 9)], H_B), "w1")
        self.conn.close()
        self.conn = connect(os.path.join(self.dir, "t.db"))
        rec = engine.boot_recover(self.conn, self.wl)
        self.assertTrue(rec["journal"]["ok"])
        # still waiting (parent genuinely absent)
        detail = engine.object_detail(self.conn, "W")
        self.assertEqual(detail["nodes"][0]["status"], "waiting")
        # deliver parent after restart -> auto resume
        engine.ingest_envelope(self.conn, self.wl, parent_raw, "w0")
        detail = engine.object_detail(self.conn, "W")
        self.assertTrue(all(n["status"] == "incorporated" for n in detail["nodes"]))


if __name__ == "__main__":
    unittest.main(verbosity=2)
