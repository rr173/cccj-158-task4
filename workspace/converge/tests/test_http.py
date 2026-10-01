"""End-to-end tests against the real HTTP service.

Covers the scenarios required for acceptance:
clean merge, late-ancestor recovery, request-key/body conflict,
same-section conflict with two different reviewers, concurrent unique
decision, crash recovery, authorization rejection and tampered receipt
download verification.
"""
import json
import os
import shutil
import signal
import socket
import sqlite3
import subprocess
import sys
import tempfile
import time
import unittest
import urllib.request
from concurrent.futures import ThreadPoolExecutor

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from tests.helpers import H_A, H_B, H_C, Client, env  # noqa: E402


def free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


class ServiceCase(unittest.TestCase):
    home = None
    port = None
    proc = None
    client = None

    @classmethod
    def setUpClass(cls):
        cls.home = tempfile.mkdtemp(prefix="conv-http-")
        cls.port = free_port()
        env_vars = dict(os.environ, CONVERGE_HOME=cls.home)
        cls.proc = subprocess.Popen(
            [sys.executable, os.path.join(ROOT, "scripts", "run_server.py"),
             "--port", str(cls.port), "--host", "127.0.0.1"],
            cwd=ROOT, env=env_vars,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
        base = f"http://127.0.0.1:{cls.port}"
        for _ in range(50):
            try:
                urllib.request.urlopen(base + "/healthz", timeout=1)
                break
            except Exception:
                time.sleep(0.1)
        else:
            cls.proc.kill()
            out, err = cls.proc.communicate(timeout=5)
            raise RuntimeError("server did not start:\n" + err.decode())
        cls.client = Client(base)

    @classmethod
    def tearDownClass(cls):
        if cls.proc and cls.proc.poll() is None:
            cls.proc.send_signal(signal.SIGINT)
            try:
                cls.proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                cls.proc.kill()
        shutil.rmtree(cls.home, ignore_errors=True)

    def db(self):
        path = os.path.join(self.home, "convergence.db")
        c = sqlite3.connect(path, timeout=10)
        c.row_factory = sqlite3.Row
        return c

    # ------------------------------------------------------------ scenarios
    def test_01_health_and_auth(self):
        status, body = self.client.call("GET", "/healthz")
        self.assertEqual(status, 200)
        status, _ = self.client.call("GET", "/api/objects")
        self.assertEqual(status, 401)
        status, me = self.client.call("GET", "/api/me", user="operator")
        self.assertEqual(status, 200)
        self.assertEqual(me["role"], "operator")
        status, jv = self.client.call("GET", "/api/journal/verify", user="inspector")
        self.assertEqual(status, 200)
        self.assertTrue(jv["ok"])

    def test_02_clean_merge_over_http(self):
        c = self.client
        s, r0 = c.post_env(env("http-obj", "s1", 0, None, [(0, 10)], H_A), "h-k0")
        self.assertEqual(s, 200)
        self.assertEqual(r0["node_status"], "incorporated")
        d0 = r0["node_digest"]
        s, r1 = c.post_env(env("http-obj", "s2", 0, d0, [(10, 20)], H_B), "h-k1")
        self.assertEqual(s, 200)
        self.assertEqual(r1["current_version_no"], 2)
        s, r2 = c.post_env(env("http-obj", "s3", 0, d0, [(20, 30)], H_C), "h-k2")
        self.assertEqual(r2["current_version_no"], 3)
        s, detail = c.call("GET", "/api/objects/http-obj", user="operator")
        self.assertEqual(detail["decisions"][-1]["claims"],
                         [[0, 10, H_A], [10, 20, H_B], [20, 30, H_C]])
        # graph contains held-reason fields and three nodes
        self.assertEqual(len(detail["nodes"]), 3)

    def test_03_late_ancestor_recovery(self):
        # derive the future parent digest deterministically
        sys.path.insert(0, ROOT)
        from app.engine import body_hash_of, node_digest_of, validate_envelope
        parent = env("late-obj", "s1", 0, None, [(0, 5)], H_A)
        pd = node_digest_of(validate_envelope(parent),
                            body_hash_of(validate_envelope(parent)))
        s, r = self.client.post_env(env("late-obj", "s2", 0, pd, [(5, 9)], H_B), "late-1")
        self.assertEqual(r["node_status"], "waiting")
        self.assertIn("missing_ancestor", r["waiting_reason"])
        s, _ = self.client.post_env(parent, "late-0")
        s, detail = self.client.call("GET", "/api/objects/late-obj", user="operator")
        self.assertTrue(all(n["status"] == "incorporated" for n in detail["nodes"]))

    def test_04_request_key_conflicts_and_replay(self):
        c = self.client
        body = env("rk-obj", "s1", 0, None, [(0, 5)], H_A)
        s, r1 = c.post_env(body, "same-rk")
        self.assertEqual(s, 200)
        s, r2 = c.post_env(body, "same-rk")
        self.assertTrue(r2["replay"])
        self.assertEqual(r1["decision_id"], r2["decision_id"])
        s, err = c.post_env({**body, "change_digest": H_B}, "same-rk")
        self.assertEqual(s, 409)
        self.assertEqual(err["error"], "request_key_body_conflict")

    def test_05_same_region_conflict_two_reviewers_over_http(self):
        c = self.client
        s, r0 = c.post_env(env("conf-obj", "s1", 0, None, [(0, 10)], H_A), "cf0")
        s, r1 = c.post_env(env("conf-obj", "s2", 0, r0["node_digest"], [(0, 10)], H_B), "cf1")
        self.assertEqual(r1["node_status"], "deferred")
        s, cases = c.call("GET", "/api/cases?status=open", user="reviewer-a")
        self.assertEqual(s, 200)
        case = next(x for x in cases if x["object_key"] == "conf-obj")
        s, detail = c.call("GET", "/api/objects/conf-obj", user="reviewer-a")
        frozen = detail["open_case"]["frozen_batch"]
        winner = next(row[3] for row in frozen if row[3] != "__incumbent__")

        # operator cannot adjudicate
        s, err = c.review(case["case_id"], "operator", "approve", winner)
        self.assertEqual(s, 403)
        # inspector cannot adjudicate either
        s, err = c.review(case["case_id"], "inspector", "approve", winner)
        self.assertEqual(s, 403)
        # first reviewer
        s, step = c.review(case["case_id"], "reviewer-a", "approve", winner)
        self.assertEqual(step["step"], 2)
        # same reviewer rejected at step 2
        s, err = c.review(case["case_id"], "reviewer-a", "approve", winner)
        self.assertEqual(s, 409)
        self.assertEqual(err["error"], "reviewer_must_differ")
        # second, different reviewer
        s, done = c.review(case["case_id"], "reviewer-b", "approve", winner)
        self.assertEqual(done["status"], "resolved")
        s, detail = c.call("GET", "/api/objects/conf-obj", user="operator")
        self.assertEqual(detail["decisions"][-1]["claims"], [[0, 10, H_B]])

    def test_06_concurrent_unique_decision(self):
        c = self.client

        def post(i):
            return c.post_env(env("concurrent-obj", f"site-{i:02d}", 0, None,
                                  [(i * 4, i * 4 + 2)], H_A), f"cc-{i}")

        with ThreadPoolExecutor(max_workers=10) as ex:
            results = list(ex.map(post, range(10)))
        self.assertTrue(all(s == 200 for s, _ in results))
        conn = self.db()
        n_cur = conn.execute(
            "SELECT COUNT(*) c FROM object_current WHERE object_key='concurrent-obj'").fetchone()["c"]
        self.assertEqual(n_cur, 1)
        n_inc = conn.execute(
            "SELECT COUNT(*) c FROM nodes WHERE object_key='concurrent-obj' AND status='incorporated'"
        ).fetchone()["c"]
        self.assertEqual(n_inc, 10)
        s, ver = c.call("GET", "/api/objects/concurrent-obj/verify", user="operator")
        self.assertEqual(s, 200)
        self.assertTrue(ver["ok"])
        conn.close()

    def test_07_concurrent_same_request_key(self):
        c = self.client

        def post(_):
            return c.post_env(env("rkrace-obj", "s1", 0, None, [(0, 5)], H_A), "race-key")

        with ThreadPoolExecutor(max_workers=8) as ex:
            results = list(ex.map(post, range(8)))
        statuses = [s for s, _ in results]
        self.assertTrue(all(s == 200 for s in statuses), statuses)
        decisions = {r["decision_id"] for _, r in results if "decision_id" in r}
        self.assertEqual(len(decisions), 1)
        conn = self.db()
        self.assertEqual(conn.execute(
            "SELECT COUNT(*) c FROM requests WHERE request_key='race-key'").fetchone()["c"], 1)
        self.assertEqual(conn.execute(
            "SELECT COUNT(*) c FROM receipts WHERE object_key='rkrace-obj'").fetchone()["c"], 1)
        conn.close()

    def test_08_receipts_redacted_and_verifiable_for_inspector(self):
        c = self.client
        c.post_env(env("redact-obj", "secret-site-name", 0, None, [(0, 10)], H_A), "rd0")
        s, bundle = c.call("GET", "/api/objects/redact-obj/receipts", user="inspector")
        self.assertEqual(s, 200)
        self.assertTrue(bundle["redacted_for_inspector"])
        blob = json.dumps(bundle)
        self.assertNotIn("secret-site-name", blob)
        s, detail = c.call("GET", "/api/objects/redact-obj", user="inspector")
        self.assertTrue(detail["redacted_for_inspector"])
        self.assertTrue(all(not n["site_id"].startswith("secret") for n in detail["nodes"]))
        s, ver = c.call("GET", "/api/objects/redact-obj/verify", user="inspector")
        self.assertEqual(ver["ok"], True)
        # inspector cannot submit envelopes
        s, err = c.call("POST", "/api/envelopes", user="inspector", body={"envelope":
                        env("x", "s", 0, None, [(0, 1)], H_A)},
                        headers={"X-Request-Key": "z"})
        self.assertEqual(s, 403)

    def test_09_tampered_receipt_download_gives_deterministic_error(self):
        c = self.client
        c.post_env(env("tamper-obj", "s1", 0, None, [(0, 5)], H_A), "tp0")
        # simulate storage tampering directly (test-only)
        conn = self.db()
        conn.execute("UPDATE receipts SET receipt_hash=? WHERE object_key='tamper-obj'",
                     ("1" * 64,))
        conn.commit()
        conn.close()
        s, err = c.call("GET", "/api/objects/tamper-obj/verify", user="inspector")
        self.assertEqual(s, 409)
        self.assertEqual(err["error"], "receipt_chain_broken")
        # a valid next envelope extending the object is refused deterministically
        s0, detail = c.call("GET", "/api/objects/tamper-obj", user="operator")
        real_prev = detail["nodes"][0]["node_digest"]
        s, err2 = c.post_env(env("tamper-obj", "s2", 0, real_prev, [(5, 8)], H_B), "tp1")
        self.assertEqual(s, 409)
        self.assertEqual(err2["error"], "receipt_chain_broken")

    def test_10_strict_schema_blocks_identity_and_pixels(self):
        c = self.client
        bad = {**env("strict-obj", "s1", 0, None, [(0, 5)], H_A),
               "patient_name": "张三", "pixel_data": "BASE64=="}
        s, err = c.post_env(bad, "strict-1")
        self.assertEqual(s, 400)
        self.assertEqual(err["error"], "invalid_envelope")

    def test_11_ui_static_is_data_free(self):
        with urllib.request.urlopen(f"http://127.0.0.1:{self.port}/ui") as r:
            body = r.read().decode()
            self.assertEqual(r.status, 200)
        # the page shell never embeds business data
        self.assertNotIn("patient_name", body)
        self.assertIn("多站点变更汇合", body)


class CrashRecoveryCase(unittest.TestCase):
    """Start a server with the crash fault armed; the first merge kills the
    process mid-commit. Restart on the same database and resume."""

    def test_crash_then_restart_resumes_exactly_once(self):
        home = tempfile.mkdtemp(prefix="conv-crash-")
        port = free_port()
        base = f"http://127.0.0.1:{port}"

        def start(fault):
            e = dict(os.environ, CONVERGE_HOME=home)
            if fault:
                e["CONVERGE_FAULT"] = "crash_before_commit"
            return subprocess.Popen(
                [sys.executable, os.path.join(ROOT, "scripts", "run_server.py"),
                 "--port", str(port)], cwd=ROOT, env=e,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        proc = start(True)
        try:
            for _ in range(50):
                try:
                    urllib.request.urlopen(base + "/healthz", timeout=1)
                    break
                except Exception:
                    time.sleep(0.1)
            c = Client(base)
            try:
                c.post_env(env("crash-obj", "s1", 0, None, [(0, 5)], H_A), "cr-k0")
            except Exception:
                pass  # the process exits mid-commit by design; connection drops
            # process should have died via os._exit(2)
            time.sleep(0.5)
            self.assertNotEqual(proc.poll(), None)
        finally:
            if proc.poll() is None:
                proc.kill()
            proc.wait(timeout=5)

        # no half decision durably committed
        db = sqlite3.connect(os.path.join(home, "convergence.db"))
        self.assertEqual(db.execute("SELECT COUNT(*) c FROM decisions").fetchone()[0], 0)
        db.close()

        # restart clean
        proc2 = start(False)
        try:
            for _ in range(50):
                try:
                    urllib.request.urlopen(base + "/healthz", timeout=1)
                    break
                except Exception:
                    time.sleep(0.1)
            c = Client(base)
            s, r = c.post_env(env("crash-obj", "s1", 0, None, [(0, 5)], H_A), "cr-k0")
            self.assertEqual(s, 200)
            self.assertEqual(r["node_status"], "incorporated")
            db = sqlite3.connect(os.path.join(home, "convergence.db"))
            self.assertEqual(db.execute("SELECT COUNT(*) c FROM decisions").fetchone()[0], 1)
            self.assertEqual(db.execute("SELECT COUNT(*) c FROM receipts").fetchone()[0], 1)
            db.close()
        finally:
            if proc2.poll() is None:
                proc2.send_signal(signal.SIGINT)
                proc2.wait(timeout=5)
        shutil.rmtree(home, ignore_errors=True)


if __name__ == "__main__":
    unittest.main(verbosity=2)
