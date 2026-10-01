"""Shared test helpers: fresh database and HTTP client."""
import base64
import json
import os
import shutil
import tempfile
import urllib.error
import urllib.request


H_A = "a" * 64
H_B = "b" * 64
H_C = "c" * 64
H_D = "d" * 64

ACCOUNTS = {
    "operator": ("operator", "operator-pw"),
    "reviewer-a": ("reviewer-a", "review-a-pw"),
    "reviewer-b": ("reviewer-b", "review-b-pw"),
    "inspector": ("inspector", "inspector-pw"),
}


def fresh_home():
    d = tempfile.mkdtemp(prefix="conv-test-")
    os.environ["CONVERGE_HOME"] = d
    return d


def cleanup(d):
    shutil.rmtree(d, ignore_errors=True)


def env(object_key, site, seq, prev, regions, change):
    return {
        "object_key": object_key,
        "site_id": site,
        "site_seq": seq,
        "prev_digest": prev,
        "regions": [{"start": s, "end": e} for s, e in regions],
        "change_digest": change,
    }


def basic(user):
    u, p = ACCOUNTS[user]
    return "Basic " + base64.b64encode(f"{u}:{p}".encode()).decode()


class Client:
    def __init__(self, base):
        self.base = base

    def call(self, method, path, user=None, body=None, headers=None, raw=False):
        h = {"Cache-Control": "no-store"}
        if user:
            h["Authorization"] = basic(user)
        if headers:
            h.update(headers)
        data = None
        if body is not None:
            data = body if raw else json.dumps(body).encode()
            h.setdefault("Content-Type", "application/json")
        req = urllib.request.Request(self.base + path, data=data, headers=h, method=method)
        try:
            with urllib.request.urlopen(req, timeout=10) as r:
                payload = r.read()
                return r.status, (json.loads(payload) if payload else None)
        except urllib.error.HTTPError as e:
            payload = e.read()
            try:
                return e.code, json.loads(payload)
            except Exception:
                return e.code, {"raw": payload.decode("utf-8", "replace")}

    def post_env(self, envelope, request_key, user="operator"):
        return self.call("POST", "/api/envelopes", user=user,
                         body={"envelope": envelope},
                         headers={"X-Request-Key": request_key})

    def review(self, case_id, user, action, choice=None, note=""):
        return self.call("POST", f"/api/cases/{case_id}/reviews", user=user,
                         body={"action": action, "choice_node_digest": choice, "note": note})
