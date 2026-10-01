import fs from 'node:fs';
import { Wal } from './wal.mjs';
import { Mutex } from './mutex.mjs';
import { canonical, digestOf, sign } from './crypto-util.mjs';

export class ApiError extends Error {
  constructor(status, code, message, details = undefined) {
    super(message);
    this.status = status;
    this.code = code;
    this.details = details;
  }
}

const T_ENVELOPE = 1;
const T_DECISION = 2;
const T_REVIEW = 3;

const TOKEN_RE = /^[A-Za-z0-9:._-]{1,128}$/;
const HEX64_RE = /^[0-9a-f]{64}$/;

function nowIso() {
  return new Date().toISOString();
}

/**
 * The convergence store. All public mutating methods take the process-wide mutex;
 * reads are synchronous snapshots taken on the same event-loop turn after a lock
 * acquisition (server reads lock too, so they never observe a half-written state).
 */
export class ConvergenceStore {
  constructor(config) {
    this.config = config;
    this.wal = new Wal(config.walPath);
    this.lock = new Mutex();

    this.nodes = new Map(); // digest -> node
    this.objects = new Map(); // objectKey -> object state
    this.siteSeqs = new Map(); // `${objectKey}|${site}|${seq}` -> node digest
    this.requestIndex = new Map(); // requestKey -> { body_hash, node_digest }
    this.receipts = new Map(); // receipt hash -> receipt
    this.reviews = new Map(); // caseId -> review state (rebuilt / live)
    this.receiptSeq = new Map(); // objectKey -> last seq

    this._waiters = new Map(); // objectKey -> [resolve]
    this._timers = new Map(); // objectKey -> timeout
    this._settleGen = new Map(); // objectKey -> generation, stale callbacks no-op
  }

  // ---------------------------------------------------------------- opening --

  open() {
    this.wal.open();
    const { events, truncatedBytes } = this.wal.readAll();
    if (truncatedBytes > 0) {
      // Torn tail from a crash mid-write: drop it before appending new frames.
      const total = fs.statSync(this.config.walPath).size;
      this.wal.truncate(total - truncatedBytes);
    }
    for (const ev of events) this._replay(ev.type, ev.payload);
    this._rebuildDerived();
    this._recoverSettlements();
  }

  close() {
    for (const t of this._timers.values()) clearTimeout(t);
    this._timers.clear();
    this.wal.close();
  }

  // ---------------------------------------------------------------- replay --

  _replay(type, payloadBuf) {
    const ev = JSON.parse(payloadBuf.toString('utf8'));
    if (type === T_ENVELOPE) {
      this._indexEnvelope(ev);
    } else if (type === T_DECISION) {
      this.nodes.set(ev.node.digest, ev.node);
      this.receipts.set(ev.receipt.hash, ev.receipt);
      this.receiptSeq.set(ev.receipt.object_key, ev.receipt.seq);
      const obj = this._obj(ev.node.object_key);
      obj.currentNode = ev.node.digest;
      obj.currentReceipt = ev.receipt.hash;
      if (ev.node.decision_kind === 'adjudicate') {
        const cs = this.reviews.get(ev.node.case_id);
        if (cs) cs.resolved = true;
      }
    } else if (type === T_REVIEW) {
      let cs = this.reviews.get(ev.case_id);
      if (!cs) {
        cs = {
          caseId: ev.case_id,
          objectKey: ev.object_key,
          disputed: ev.disputed,
          candidates: ev.candidates,
          parents: ev.parents,
          baseContributions: ev.base_contributions,
          reviewers: [],
          choices: null,
          resolved: false,
        };
        this.reviews.set(ev.case_id, cs);
      }
      cs.reviewers.push({ reviewer_id: ev.reviewer_id, reviewer_name: ev.reviewer_name, choices: ev.choices });
      if (cs.reviewers.length === 1) cs.choices = ev.choices;
    }
  }

  _indexEnvelope(ev) {
    const node = {
      digest: ev.digest,
      kind: 'change',
      object_key: ev.object_key,
      site: ev.site,
      site_seq: ev.site_seq,
      region: ev.coverage_region,
      change_digest: ev.change_digest,
      predecessors: ev.predecessor_digests,
      request_key: ev.request_key,
    };
    this.nodes.set(node.digest, node);
    this.siteSeqs.set(`${node.object_key}|${node.site}|${node.site_seq}`, node.digest);
    this.requestIndex.set(node.request_key, { body_hash: ev.body_hash, node_digest: node.digest });
  }

  _rebuildDerived() {
    this.objects.clear();
    for (const node of this.nodes.values()) this._obj(node.object_key);
    // Restore each object's current-decision pointers from its highest-seq
    // receipt (the object map was just recreated empty during WAL replay).
    for (const obj of this.objects.values()) {
      let latest = null;
      for (const r of this.receipts.values()) {
        if (r.object_key === obj.key && (!latest || r.seq > latest.seq)) latest = r;
      }
      if (latest) {
        obj.currentReceipt = latest.hash;
        obj.currentNode = latest.decision_node;
      }
    }
  }

  _obj(key) {
    let o = this.objects.get(key);
    if (!o) {
      o = {
        key,
        currentNode: null,
        currentReceipt: null,
        hasOpenConflict: false,
        openCaseId: null,
      };
      this.objects.set(key, o);
    }
    return o;
  }

  // ------------------------------------------------------- graph derivation --

  _childrenIndex() {
    const children = new Map();
    for (const node of this.nodes.values()) {
      const parents = node.kind === 'change' ? node.predecessors : node.parents;
      for (const p of parents) {
        if (!this.nodes.has(p)) continue; // suspended predecessor
        if (!children.has(p)) children.set(p, []);
        children.get(p).push(node.digest);
      }
    }
    return children;
  }

  _missingPreds(node) {
    return node.kind === 'change' ? node.predecessors.filter((p) => !this.nodes.has(p)) : [];
  }

  /** Compute live tips and suspended envelopes for one object. */
  _objectTips(key, children) {
    const mine = [...this.nodes.values()].filter((n) => n.object_key === key);
    const suspended = [];
    const live = [];
    for (const n of mine) {
      const missing = this._missingPreds(n);
      if (missing.length) suspended.push({ digest: n.digest, missing });
      else live.push(n);
    }
    const tips = live.filter((n) => !(children.get(n.digest) || []).some((c) => this.nodes.get(c)?.object_key === key)).map((n) => n.digest);
    return { tips: this._sortNodeDigests(tips), suspended };
  }

  _sortNodeDigests(digests) {
    return [...digests].sort((a, b) => {
      const x = this.nodes.get(a);
      const y = this.nodes.get(b);
      const kx = x.kind === 'change' ? `0|${x.site}|${String(x.site_seq).padStart(12, '0')}|${x.digest}` : `1|${x.digest}`;
      const ky = y.kind === 'change' ? `0|${y.site}|${String(y.site_seq).padStart(12, '0')}|${y.digest}` : `1|${y.digest}`;
      return kx < ky ? -1 : kx > ky ? 1 : 0;
    });
  }

  _coverage(node) {
    if (node.kind === 'change') return [{ region: node.region, change_digest: node.change_digest, tip: node.digest }];
    return node.contributions.map((c) => ({ ...c, tip: node.digest }));
  }

  // ------------------------------------------------------------- validation --

  _validateEnvelope(body) {
    if (!body || typeof body !== 'object' || Array.isArray(body)) {
      throw new ApiError(400, 'BAD_REQUEST', 'envelope must be a JSON object');
    }
    const allowed = ['object_key', 'predecessor_digests', 'coverage_region', 'change_digest', 'site', 'site_seq', 'request_key'];
    for (const k of Object.keys(body)) {
      if (!allowed.includes(k)) throw new ApiError(400, 'BAD_REQUEST', `unknown field ${k}`);
    }
    const str = (name) => {
      const v = body[name];
      if (typeof v !== 'string' || !TOKEN_RE.test(v)) throw new ApiError(400, 'BAD_REQUEST', `${name} must be a 1-128 char token`);
      return v;
    };
    const hex = (name) => {
      const v = body[name];
      if (typeof v !== 'string' || !HEX64_RE.test(v)) throw new ApiError(400, 'BAD_REQUEST', `${name} must be 64 hex chars`);
      return v;
    };
    const object_key = str('object_key');
    const coverage_region = str('coverage_region');
    const change_digest = hex('change_digest');
    const site = str('site');
    const request_key = str('request_key');
    const site_seq = body.site_seq;
    if (!Number.isInteger(site_seq) || site_seq < 0) throw new ApiError(400, 'BAD_REQUEST', 'site_seq must be a non-negative integer');
    let preds = body.predecessor_digests;
    if (!Array.isArray(preds)) throw new ApiError(400, 'BAD_REQUEST', 'predecessor_digests must be an array');
    preds = preds.map((p) => {
      if (typeof p !== 'string' || !HEX64_RE.test(p)) throw new ApiError(400, 'BAD_REQUEST', 'predecessor digests must be 64 hex chars');
      return p;
    });
    if (new Set(preds).size !== preds.length) throw new ApiError(400, 'BAD_REQUEST', 'duplicate predecessor digests');
    preds = [...preds].sort();
    return { object_key, predecessor_digests: preds, coverage_region, change_digest, site, site_seq, request_key };
  }

  envelopeDigestInput(env) {
    return {
      object_key: env.object_key,
      predecessor_digests: [...env.predecessor_digests].sort(),
      coverage_region: env.coverage_region,
      change_digest: env.change_digest,
      site: env.site,
      site_seq: env.site_seq,
    };
  }

  // ------------------------------------------------------------ submission --

  async submitEnvelope(rawBody) {
    const prepared = await this.lock.runExclusive(() => this._acceptEnvelopeLocked(rawBody));
    if (prepared.immediate) return prepared.result;
    return this._awaitSettle(prepared.objectKey, prepared.nodeDigest, prepared.requestKey);
  }

  async previewEnvelopeDigest(rawBody) {
    const env = this._validateEnvelope(rawBody);
    return { node_digest: digestOf(this.envelopeDigestInput(env)) };
  }

  _acceptEnvelopeLocked(rawBody) {
    const env = this._validateEnvelope(rawBody);
    const bodyHash = digestOf(canonical(env));

    // Idempotency authority is the request key.
    const seen = this.requestIndex.get(env.request_key);
    if (seen) {
      if (seen.body_hash !== bodyHash) {
        throw new ApiError(409, 'REQUEST_KEY_BODY_CONFLICT', 'same request_key was already used with a different body', {
          request_key: env.request_key,
        });
      }
      return { immediate: true, result: { ...this._resultFor(env.object_key, seen.node_digest), replayed: true, request_key: env.request_key } };
    }

    const nodeDigest = digestOf(this.envelopeDigestInput(env));

    // Site monotonic sequence: a (object, site, seq) triple binds exactly one payload.
    const seqKey = `${env.object_key}|${env.site}|${env.site_seq}`;
    const priorAtSeq = this.siteSeqs.get(seqKey);
    if (priorAtSeq && priorAtSeq !== nodeDigest) {
      throw new ApiError(409, 'SITE_SEQ_CONFLICT', 'site already reported a different envelope for this sequence', {
        site: env.site,
        site_seq: env.site_seq,
      });
    }
    // The envelope event is the durable request-key binding; re-reporting the
    // same content under a new key appends the binding, not a second node.
    const ev = { ...env, digest: nodeDigest, body_hash: bodyHash };
    this.wal.append(T_ENVELOPE, JSON.stringify(ev));
    this._indexEnvelope(ev);
    this._obj(env.object_key); // register even if the envelope will suspend

    const missing = env.predecessor_digests.filter((p) => !this.nodes.has(p));
    if (missing.length) {
      return {
        immediate: true,
        result: {
          status: 'suspended',
          request_key: env.request_key,
          node_digest: nodeDigest,
          object_key: env.object_key,
          missing_predecessors: missing,
          replayed: false,
        },
      };
    }

    this._cascadeResolution(env.object_key);
    const obj = this._obj(env.object_key);
    if (obj.hasOpenConflict) {
      return { immediate: true, result: { ...this._resultFor(env.object_key, nodeDigest), replayed: false, request_key: env.request_key } };
    }
    return { immediate: false, objectKey: env.object_key, nodeDigest, requestKey: env.request_key };
  }

  /** After a live node arrives, activate any envelopes whose ancestry is now complete. */
  _cascadeResolution(seedObjectKey) {
    const affected = new Set([seedObjectKey]);
    let changed = true;
    while (changed) {
      changed = false;
      for (const node of this.nodes.values()) {
        if (node.kind !== 'change' || node._resolved) continue;
        if (this._missingPreds(node).length === 0) {
          node._resolved = true;
          affected.add(node.object_key);
          changed = true;
        }
      }
    }
    for (const key of affected) this._scheduleSettle(key);
  }

  // -------------------------------------------------------------- settling --

  /**
   * (Re)arm the debounced settle for an object. Each arriving envelope bumps a
   * generation counter and resets the quiet-period window. A burst of
   * concurrent/out-of-order reports therefore collapses into exactly one
   * convergence pass and one decision; stale timer callbacks self-cancel.
   */
  _scheduleSettle(objectKey) {
    const obj = this._obj(objectKey);
    if (obj.hasOpenConflict) return;
    const gen = (this._settleGen.get(objectKey) || 0) + 1;
    this._settleGen.set(objectKey, gen);
    const existing = this._timers.get(objectKey);
    if (existing) clearTimeout(existing);
    const timer = setTimeout(() => {
      this._timers.delete(objectKey);
      if (this._settleGen.get(objectKey) !== gen) return; // superseded
      this.lock.runExclusive(() => {
        if (this._settleGen.get(objectKey) !== gen) return;
        this._settleObject(objectKey);
      }).catch(() => {});
    }, this.config.settleDelayMs);
    if (existing) this._timers.set(objectKey, timer);
    else this._timers.set(objectKey, timer);
  }

  /**
   * Wait OUTSIDE the lock for the scheduled settle of this object to run.
   * Re-reading state on the wake turn (no lock held; the settle itself ran under
   * the lock) is safe because JS reads of plain values are atomic on this turn.
   */
  _awaitSettle(objectKey, nodeDigest, requestKey) {
    return new Promise((resolve) => {
      const finish = () => resolve({ ...this._resultFor(objectKey, nodeDigest), replayed: false, request_key: requestKey });
      const list = this._waiters.get(objectKey) || [];
      list.push(finish);
      this._waiters.set(objectKey, list);
      // Safety bound; the settle timer is the normal wake path.
      setTimeout(() => {
        const cur = this._waiters.get(objectKey);
        if (!cur || !cur.includes(finish)) return;
        this._waiters.set(objectKey, cur.filter((w) => w !== finish));
        finish();
      }, Math.max(5000, this.config.settleDelayMs * 20));
    });
  }

  _wake(objectKey) {
    const list = this._waiters.get(objectKey);
    if (!list) return;
    this._waiters.delete(objectKey);
    for (const w of list) w();
  }

  _regionMap(coverages) {
    const byRegion = new Map();
    for (const { cov } of coverages) {
      for (const c of cov) {
        if (!byRegion.has(c.region)) byRegion.set(c.region, c.change_digest);
      }
    }
    return byRegion;
  }

  /**
   * Deterministic redundancy reduction. A tip is redundant if the UNION of all
   * OTHER tips already covers every region it covers with the same digest —
   * this drops (a) duplicate siblings and (b) raw changes that a newer merge
   * node already subsumes (the restart case). Ties resolve by the canonical
   * tip order so identical siblings never all disappear.
   */
  _essentialTips(coverages) {
    const redundant = new Set();
    for (let i = 0; i < coverages.length; i++) {
      const cur = coverages[i];
      if (cur.cov.length === 0) { redundant.add(cur.tip); continue; }
      const coveredByOthers = cur.cov.every((c) =>
        coverages.some((o, j) =>
          j !== i && !redundant.has(o.tip) &&
          o.cov.some((x) => x.region === c.region && x.change_digest === c.change_digest)),
      );
      if (coveredByOthers) redundant.add(cur.tip);
    }
    return coverages.map((c) => c.tip).filter((t) => !redundant.has(t));
  }

  _essentialTips(coverages) {
    // Pass 1: exact duplicates. A tip equal to a single earlier tip is
    // redundant; iterating in canonical tip order always keeps the first copy,
    // so a set of identical siblings collapses to exactly one.
    const redundant = new Set();
    for (let i = 0; i < coverages.length; i++) {
      const cur = coverages[i];
      if (cur.cov.length === 0) { redundant.add(cur.tip); continue; }
      const equalToAnother = coverages.some((o, j) =>
        j < i && !redundant.has(o.tip) && cur.cov.length === o.cov.length &&
        cur.cov.every((c) => o.cov.some((x) => x.region === c.region && x.change_digest === c.change_digest)));
      if (equalToAnother) redundant.add(cur.tip);
    }
    // Pass 2: strict containment by a single other surviving tip. A merge node
    // therefore subsumes every raw change it synthesised (the restart case),
    // while it can never be contained by a single narrower change.
    const alive = coverages.filter((c) => !redundant.has(c.tip));
    for (const cur of alive) {
      const containedByOne = alive.some((o) =>
        o.tip !== cur.tip &&
        o.cov.length >= cur.cov.length &&
        cur.cov.every((c) => o.cov.some((x) => x.region === c.region && x.change_digest === c.change_digest)));
      if (containedByOne) redundant.add(cur.tip);
    }
    return coverages.map((c) => c.tip).filter((t) => !redundant.has(t));
  }

  _disputedRegions(coverages) {
    const byRegion = new Map();
    for (const { tip, cov } of coverages) {
      for (const c of cov) {
        if (!byRegion.has(c.region)) byRegion.set(c.region, []);
        byRegion.get(c.region).push({ tip, change_digest: c.change_digest });
      }
    }
    const disputed = [];
    const candidates = [];
    for (const region of [...byRegion.keys()].sort()) {
      const entries = byRegion.get(region);
      const distinct = new Set(entries.map((e) => e.change_digest));
      if (distinct.size > 1) {
        disputed.push(region);
        candidates.push(...entries.map((e) => ({ region, tip: e.tip, change_digest: e.change_digest })));
      }
    }
    candidates.sort((a, b) => {
      const k = `${a.region}|${a.change_digest}|${a.tip}`;
      const l = `${b.region}|${b.change_digest}|${b.tip}`;
      return k < l ? -1 : 1;
    });
    return { disputed, candidates };
  }

  /** One deterministic convergence pass for an object. */
  _settleObject(objectKey) {
    const obj = this._obj(objectKey);
    if (obj.hasOpenConflict) {
      this._wake(objectKey);
      return;
    }
    const children = this._childrenIndex();
    const { tips, suspended } = this._objectTips(objectKey, children);
    if (suspended.length || tips.length === 0) {
      this._wake(objectKey);
      return;
    }

    let coverages = tips.map((d) => ({ tip: d, cov: this._coverage(this.nodes.get(d)) }));

    // Drop duplicate siblings and raw changes already contained in an existing
    // decision BEFORE testing for collisions. Otherwise a freshly emitted merge
    // would immediately re-conflict with the raw changes it synthesised (this
    // matters on the post-decision settle turn and during recovery).
    const essentialTips = this._essentialTips(coverages);
    if (essentialTips.length !== coverages.length) {
      const keep = new Set(essentialTips);
      coverages = coverages.filter((c) => keep.has(c.tip));
    }
    const { disputed, candidates } = this._disputedRegions(coverages);

    if (disputed.length) {
      this._openConflict(obj, essentialTips, disputed, candidates, coverages);
      this._wake(objectKey);
      return;
    }

    const regionMap = this._regionMap(coverages);
    const contributions = [...regionMap.entries()]
      .sort((a, b) => (a[0] < b[0] ? -1 : 1))
      .map(([region, change_digest]) => ({ region, change_digest }));

    // Content-based idempotence: if the persisted current decision already
    // describes exactly this region->digest map, there is nothing new to
    // decide. Comparing content (not node identity / parent set) is what makes
    // recovery safe: the same convergent content reached via different parent
    // bookkeeping must not yield a second receipt.
    if (obj.currentNode) {
      const current = this.nodes.get(obj.currentNode);
      const currentCov = new Map((current?.contributions || []).map((c) => [c.region, c.change_digest]));
      const sameContent = currentCov.size === contributions.length &&
        contributions.every((c) => currentCov.get(c.region) === c.change_digest);
      if (sameContent) {
        this._wake(objectKey);
        return;
      }
    }

    const multipleSiblings = essentialTips.length > 1;
    const decisionKind = obj.currentReceipt ? (multipleSiblings ? 'merge' : 'extend') : multipleSiblings ? 'merge' : 'accept';
    this._emitDecision(objectKey, essentialTips, contributions, decisionKind, null);
    this._wake(objectKey);
  }

  _openConflict(obj, tips, disputed, candidates, coverages) {
    const parents = this._sortNodeDigests(tips);
    const baseContributions = [];
    const seenRegion = new Set();
    for (const { cov } of coverages) {
      for (const c of cov) {
        if (!seenRegion.has(c.region)) {
          seenRegion.add(c.region);
          baseContributions.push({ region: c.region, change_digest: c.change_digest });
        }
      }
    }
    baseContributions.sort((a, b) => (a.region < b.region ? -1 : 1));
    const caseId = this._caseIdFor(obj.key, disputed, candidates);

    const existing = this.reviews.get(caseId);
    if (!existing || existing.resolved) {
      this.reviews.set(caseId, {
        caseId,
        objectKey: obj.key,
        disputed,
        candidates: candidates.map((c) => ({ ...c })).sort((a, b) => {
          const k = `${a.region}|${a.change_digest}|${a.tip}`;
          const l = `${b.region}|${b.change_digest}|${b.tip}`;
          return k < l ? -1 : 1;
        }),
        parents,
        baseContributions,
        reviewers: [],
        choices: null,
        resolved: false,
      });
    }
    obj.hasOpenConflict = true;
    obj.openCaseId = caseId;
  }

  // ------------------------------------------------------------- decisions --

  _emitDecision(objectKey, parents, contributions, decisionKind, adjudication) {
    const obj = this._obj(objectKey);
    const nodeInput = {
      object_key: objectKey,
      parents,
      contributions,
      decision_kind: decisionKind,
      ...(adjudication ? { case_id: adjudication.caseId, reviewers: adjudication.reviewerNames } : {}),
    };
    const node = {
      digest: digestOf(nodeInput),
      kind: 'merge',
      ...nodeInput,
    };
    if (this.nodes.has(node.digest)) {
      // Deterministic replay-safe dedup: same convergence already emitted.
      obj.currentNode = node.digest;
      obj.currentReceipt = this._receiptByNode(node.digest)?.hash ?? obj.currentReceipt;
      return node;
    }

    const seq = (this.receiptSeq.get(objectKey) || 0) + 1;
    const receiptFields = {
      seq,
      object_key: objectKey,
      decision_kind: decisionKind,
      decision_node: node.digest,
      parents,
      contributions,
      ...(adjudication
        ? { case_id: adjudication.caseId, reviewers: adjudication.reviewerNames }
        : {}),
      prev_receipt: obj.currentReceipt,
      emitted_at: nowIso(),
    };
    const hash = digestOf(receiptFields);
    const receipt = { ...receiptFields, hash, sig: sign(this.config.receiptSecret, receiptFields) };

    this.wal.append(T_DECISION, JSON.stringify({ node, receipt }));
    this.nodes.set(node.digest, node);
    this.receipts.set(hash, receipt);
    this.receiptSeq.set(objectKey, seq);
    obj.currentNode = node.digest;
    obj.currentReceipt = hash;
    if (adjudication) {
      const cs = this.reviews.get(adjudication.caseId);
      if (cs) cs.resolved = true;
      obj.hasOpenConflict = false;
      obj.openCaseId = null;
    }
    return node;
  }

  _receiptByNode(nodeDigest) {
    for (const r of this.receipts.values()) if (r.decision_node === nodeDigest) return r;
    return null;
  }

  // --------------------------------------------------------------- reviews --

  async submitReview(caseId, reviewerId, reviewerName, rawChoices) {
    return this.lock.runExclusive(() => this._submitReviewLocked(caseId, reviewerId, reviewerName, rawChoices));
  }

  _validateChoices(cs, rawChoices) {
    if (!rawChoices || typeof rawChoices !== 'object' || Array.isArray(rawChoices)) {
      throw new ApiError(400, 'BAD_REQUEST', 'choices must be an object region->change_digest');
    }
    const keys = Object.keys(rawChoices).sort();
    const want = [...cs.disputed].sort();
    if (canonical(keys) !== canonical(want)) {
      throw new ApiError(400, 'BAD_REQUEST', 'choices must cover exactly the disputed regions', { expected: want });
    }
    for (const region of want) {
      const choice = rawChoices[region];
      const ok = cs.candidates.some((c) => c.region === region && c.change_digest === choice);
      if (!ok) throw new ApiError(400, 'BAD_REQUEST', `choice for ${region} is not a candidate change_digest`);
    }
    const out = {};
    for (const region of want) out[region] = rawChoices[region];
    return out;
  }

  _submitReviewLocked(caseId, reviewerId, reviewerName, rawChoices) {
    const cs = this.reviews.get(caseId);
    if (!cs || cs.resolved) throw new ApiError(404, 'CASE_NOT_FOUND', 'no open conflict with this case id');
    if (cs.objectKey && !this.objects.has(cs.objectKey)) throw new ApiError(404, 'CASE_NOT_FOUND', 'object missing');

    if (cs.reviewers.some((r) => r.reviewer_id === reviewerId)) {
      throw new ApiError(403, 'REVIEWER_MUST_DIFFER', 'the same reviewer may not record both decisions');
    }
    if (cs.reviewers.length >= 2) throw new ApiError(409, 'CASE_ALREADY_DECIDED', 'case already has two reviews');

    const choices = this._validateChoices(cs, rawChoices);

    const isFirst = cs.reviewers.length === 0;

    if (isFirst) {
      const ev = {
        case_id: caseId,
        object_key: cs.objectKey,
        reviewer_id: reviewerId,
        reviewer_name: reviewerName,
        choices,
        disputed: cs.disputed,
        candidates: cs.candidates,
        parents: cs.parents,
        base_contributions: cs.baseContributions,
      };
      this.wal.append(T_REVIEW, JSON.stringify(ev));
      cs.reviewers.push({ reviewer_id: reviewerId, reviewer_name: reviewerName, choices });
      cs.choices = choices;
      return { status: 'awaiting_second_review', case_id: caseId, object_key: cs.objectKey };
    }

    // Second opinion: compare BEFORE appending. A dissenting second opinion is
    // rejected atomically (nothing persisted) so a later matching pair can
    // adjudicate and the WAL never carries an ambiguous triple.
    const first = cs.reviewers[0];
    if (canonical(first.choices) !== canonical(choices)) {
      throw new ApiError(409, 'REVIEW_MISMATCH', 'second reviewer disagrees with the first; case remains open', {
        first_reviewer: first.reviewer_name,
        first_choices: first.choices,
      });
    }
    const ev = {
      case_id: caseId,
      object_key: cs.objectKey,
      reviewer_id: reviewerId,
      reviewer_name: reviewerName,
      choices,
    };
    this.wal.append(T_REVIEW, JSON.stringify(ev));
    cs.reviewers.push({ reviewer_id: reviewerId, reviewer_name: reviewerName, choices });
    const node = this._finalizeAdjudication(cs, choices, [
      { reviewer_id: first.reviewer_id, reviewer_name: first.reviewer_name },
      { reviewer_id: reviewerId, reviewer_name: reviewerName },
    ]);
    if (!node) {
      // Reviews matched, but a branch arriving concurrently opened a new case.
      const obj = this._obj(cs.objectKey);
      return { status: 'new_conflict_opened', case_id: obj.openCaseId, object_key: cs.objectKey };
    }
    return { status: 'adjudicated', case_id: caseId, object_key: cs.objectKey, current_receipt: this._obj(cs.objectKey).currentReceipt };
  }

  /**
   * Apply a matched pair of reviews against the CURRENT live tips, not the
   * snapshot from when the case opened. Branches that arrived while the case
   * was open are folded in; a genuinely new disagreement (a third digest for a
   * disputed region, or a fresh collision elsewhere) opens a new case instead
   * of emitting a decision.
   */
  _finalizeAdjudication(cs, choices, reviewers) {
    const children = this._childrenIndex();
    const { tips, suspended } = this._objectTips(cs.objectKey, children);
    const coverages = tips.map((d) => ({ tip: d, cov: this._coverage(this.nodes.get(d)) }));

    // Drop every unchosen candidate contribution of a disputed region. A tip
    // whose coverage becomes empty (the rejected side of a same-region split)
    // drops out entirely so it cannot reappear as a live tip afterwards.
    const kept = [];
    for (const { tip, cov } of coverages) {
      const remaining = cov.filter((c) => !cs.disputed.includes(c.region) || c.change_digest === choices[c.region]);
      if (remaining.length) kept.push({ tip, cov: remaining });
    }
    const { disputed, candidates } = this._disputedRegions(kept);

    if (disputed.length || suspended.length) {
      // Adjudication is stale: the live graph still contains a disagreement or
      // awaits an ancestor. Close the old case and open one for the live state.
      cs.resolved = true;
      const obj = this._obj(cs.objectKey);
      if (!suspended.length) {
        this._openConflict(obj, tips, disputed, candidates, kept);
      }
      return null;
    }

    // Same redundancy reduction as the normal settle pass, plus union.
    const regionMap = this._regionMap(kept);
    const contributions = [...regionMap.entries()]
      .sort((a, b) => (a[0] < b[0] ? -1 : 1))
      .map(([region, change_digest]) => ({ region, change_digest }));

    // Graph parents are EVERY current live tip, including the rejected side of a
    // split: the decision supersedes it (so it must stop being a live tip) and
    // the auditable version graph records that it was adjudicated away. Only the
    // winning digests survive in `contributions`.
    const graphParents = this._sortNodeDigests(tips);
    return this._emitDecision(
      cs.objectKey,
      graphParents,
      contributions,
      'adjudicate',
      { caseId: cs.caseId, reviewerNames: reviewers.map((r) => r.reviewer_name) },
    );
  }

  _caseIdFor(objectKey, disputed, candidates) {
    return digestOf({
      object_key: objectKey,
      disputed,
      candidates: candidates.map((c) => `${c.region}:${c.change_digest}@${c.tip}`),
    });
  }

  // -------------------------------------------------------------- recovery --

  /**
   * Re-run convergence after WAL replay. Deterministic: an emitted decision
   * dedupes on node digest; a crash between the second review append and the
   * decision append is completed here exactly once. No settle timer survives a
   * restart, so this pass replaces it.
   */
  _recoverSettlements() {
    for (const node of this.nodes.values()) {
      if (node.kind === 'change' && this._missingPreds(node).length === 0) node._resolved = true;
    }

    let progressed = true;
    let guard = 0;
    while (progressed && guard++ < 100000) {
      progressed = false;
      for (const key of [...this.objects.keys()].sort()) {
        const obj = this._obj(key);
        const children = this._childrenIndex();
        const { tips, suspended } = this._objectTips(key, children);
        if (suspended.length || tips.length === 0) continue;

        let coverages = tips.map((d) => ({ tip: d, cov: this._coverage(this.nodes.get(d)) }));
        // Collapse duplicates/contained tips before collision analysis, matching
        // the live settle path, so an emitted merge cannot re-conflict with the
        // raw changes it contains.
        const essential = this._essentialTips(coverages);
        if (essential.length !== coverages.length) {
          const keep = new Set(essential);
          coverages = coverages.filter((c) => keep.has(c.tip));
        }
        const { disputed, candidates } = this._disputedRegions(coverages);

        if (!disputed.length) {
          if (obj.hasOpenConflict) {
            obj.hasOpenConflict = false;
            obj.openCaseId = null;
          }
          const before = obj.currentNode;
          this._settleObject(key);
          if (obj.currentNode !== before) progressed = true;
          continue;
        }

        const caseId = this._caseIdFor(key, disputed, candidates);
        const stored = this.reviews.get(caseId);
        if (stored && stored.reviewers.length >= 2 && !stored.resolved) {
          // Crash window: second review durable, decision not yet.
          const [first, second] = stored.reviewers;
          if (canonical(first.choices) === canonical(second.choices)) {
            const node = this._finalizeAdjudication(stored, first.choices, [
              { reviewer_id: first.reviewer_id, reviewer_name: first.reviewer_name },
              { reviewer_id: second.reviewer_id, reviewer_name: second.reviewer_name },
            ]);
            if (node) {
              // All adjudicated tips (rejected side included) are now parents of
              // the decision, so they stop being live tips. _emitDecision clears
              // the conflict flags; a genuinely new concurrent branch would have
              // made _finalizeAdjudication return null and reopen a case.
              progressed = true;
            }
            continue;
          }
        }
        // Re-mark an open conflict (zero or one recorded review).
        if (stored) {
          obj.hasOpenConflict = true;
          obj.openCaseId = caseId;
        } else {
          this._openConflict(obj, tips, disputed, candidates, coverages);
        }
      }
    }
  }

  // ------------------------------------------------------------------ views --

  _resultFor(objectKey, nodeDigest) {
    const obj = this.objects.get(objectKey);
    const children = this._childrenIndex();
    const { tips, suspended } = obj ? this._objectTips(objectKey, children) : { tips: [], suspended: [] };
    const out = {
      object_key: objectKey,
      node_digest: nodeDigest,
      status: 'unknown',
      tips,
      suspended_count: suspended.length,
    };
    if (suspended.some((s) => s.digest === nodeDigest)) {
      out.status = 'suspended';
      out.missing_predecessors = suspended.find((s) => s.digest === nodeDigest).missing;
    } else if (obj?.hasOpenConflict) {
      out.status = 'conflict_open';
      out.case_id = obj.openCaseId;
    } else if (obj?.currentReceipt) {
      out.status = 'settled';
      out.current_receipt = obj.currentReceipt;
      out.current_decision_node = obj.currentNode;
    } else {
      out.status = 'pending';
    }
    return out;
  }

  async listObjects() {
    return this.lock.runExclusive(() => {
      const children = this._childrenIndex();
      return [...this.objects.keys()].sort().map((key) => {
        const obj = this._obj(key);
        const { tips, suspended } = this._objectTips(key, children);
        return {
          object_key: key,
          status: obj.hasOpenConflict ? 'conflict' : suspended.length ? 'suspended' : obj.currentReceipt ? 'settled' : 'pending',
          tips,
          suspended,
          current_receipt: obj.currentReceipt,
          open_case: obj.openCaseId,
        };
      });
    });
  }

  async getObject(key) {
    return this.lock.runExclusive(() => {
      if (!this.objects.has(key)) throw new ApiError(404, 'OBJECT_NOT_FOUND', 'unknown object');
      const children = this._childrenIndex();
      const { tips, suspended } = this._objectTips(key, children);
      const obj = this._obj(key);
      const nodes = [...this.nodes.values()]
        .filter((n) => n.object_key === key)
        .map((n) =>
          n.kind === 'change'
            ? {
                digest: n.digest,
                kind: 'change',
                site: n.site,
                site_seq: n.site_seq,
                coverage_region: n.region,
                change_digest: n.change_digest,
                predecessors: n.predecessors,
                request_key: n.request_key,
                suspended: this._missingPreds(n).length > 0,
              }
            : {
                digest: n.digest,
                kind: 'merge',
                decision_kind: n.decision_kind,
                parents: n.parents,
                contributions: n.contributions,
                case_id: n.case_id,
                reviewers: n.reviewers,
              },
        )
        .sort((a, b) => (a.digest < b.digest ? -1 : 1));
      const edges = [];
      for (const n of nodes) {
        const parents = n.kind === 'change' ? n.predecessors : n.parents;
        for (const p of parents) edges.push({ from: p, to: n.digest, live: this.nodes.has(p) });
      }
      return {
        object_key: key,
        status: obj.hasOpenConflict ? 'conflict' : suspended.length ? 'suspended' : obj.currentReceipt ? 'settled' : 'pending',
        nodes,
        edges,
        tips,
        suspended,
        current: obj.currentReceipt ? this._publicReceipt(this.receipts.get(obj.currentReceipt)) : null,
        open_case: obj.openCaseId ? this._caseView(obj.openCaseId) : null,
      };
    });
  }

  async listCases() {
    return this.lock.runExclusive(() => {
      const children = this._childrenIndex();
      const out = [];
      for (const cs of this.reviews.values()) {
        if (cs.resolved) continue;
        const obj = this.objects.get(cs.objectKey);
        if (!obj || !obj.hasOpenConflict) continue;
        out.push(this._caseView(cs.caseId, children));
      }
      out.sort((a, b) => (a.case_id < b.case_id ? -1 : 1));
      return out;
    });
  }

  async getCase(caseId) {
    return this.lock.runExclusive(() => {
      const view = this._caseView(caseId);
      const obj = view && this.objects.get(view.object_key);
      if (!view || !obj || !obj.hasOpenConflict) {
        throw new ApiError(404, 'CASE_NOT_FOUND', 'no open conflict with this case id');
      }
      return view;
    });
  }

  _caseView(caseId, children = this._childrenIndex()) {
    const cs = this.reviews.get(caseId);
    if (!cs) return null;
    return {
      case_id: cs.caseId,
      object_key: cs.objectKey,
      disputed_regions: cs.disputed,
      candidates: cs.candidates,
      reviews: cs.reviewers.map((r) => ({ reviewer_id: r.reviewer_id, reviewer_name: r.reviewer_name, choices: r.choices })),
      awaiting: Math.max(0, 2 - cs.reviewers.length),
      tips: this._objectTips(cs.objectKey, children).tips,
    };
  }

  // -------------------------------------------------------------- receipts --

  _publicReceipt(r) {
    if (!r) return null;
    const { ...pub } = r;
    return pub;
  }

  async listReceipts(objectKey = null) {
    return this.lock.runExclusive(() => {
      const rows = [...this.receipts.values()]
        .filter((r) => !objectKey || r.object_key === objectKey)
        .sort((a, b) => (a.object_key < b.object_key ? -1 : a.object_key > b.object_key ? 1 : a.seq - b.seq));
      // Desensitised receipt: no request key, station id or sequence; only
      // pseudonymous digests, regions (synthetic coordinates) and hashes.
      return rows.map((r) => ({
        seq: r.seq,
        object_key: r.object_key,
        decision_kind: r.decision_kind,
        decision_node: r.decision_node,
        parents: r.parents,
        contributions: r.contributions,
        ...(r.case_id ? { case_id: r.case_id, reviewers: r.reviewers } : {}),
        prev_receipt: r.prev_receipt,
        emitted_at: r.emitted_at,
        hash: r.hash,
        sig: r.sig,
      }));
    });
  }

  async getReceipt(hash) {
    return this.lock.runExclusive(() => {
      const r = this.receipts.get(hash);
      if (!r) throw new ApiError(404, 'RECEIPT_NOT_FOUND', 'unknown receipt hash');
      return this._publicReceipt(r);
    });
  }

  receiptChainForObject(objectKey) {
    return [...this.receipts.values()]
      .filter((r) => r.object_key === objectKey)
      .sort((a, b) => a.seq - b.seq);
  }

  /**
   * Verify a (possibly externally supplied / tampered) receipt chain entry.
   * Returns {ok:true} or throws a deterministic error code.
   */
  async verifyReceipt(receipt) {
    return this.lock.runExclusive(() => this._verifyReceiptLocked(receipt));
  }

  _verifyReceiptLocked(receipt) {
    if (!receipt || typeof receipt !== 'object') throw new ApiError(400, 'RECEIPT_MALFORMED', 'receipt must be an object');
    const fields = [
      'seq', 'object_key', 'decision_kind', 'decision_node', 'parents', 'contributions',
      'prev_receipt', 'emitted_at',
    ];
    for (const f of fields) if (!(f in receipt)) throw new ApiError(422, 'RECEIPT_MALFORMED', `missing field ${f}`);

    const stored = this.receipts.get(receipt.hash);
    const fieldsForHash = {
      seq: receipt.seq,
      object_key: receipt.object_key,
      decision_kind: receipt.decision_kind,
      decision_node: receipt.decision_node,
      parents: receipt.parents,
      contributions: receipt.contributions,
      ...(receipt.case_id ? { case_id: receipt.case_id, reviewers: receipt.reviewers } : {}),
      prev_receipt: receipt.prev_receipt,
      emitted_at: receipt.emitted_at,
    };

    // 1) Content hash: any edit to a covered field breaks this.
    const recomputed = digestOf(fieldsForHash);
    if (recomputed !== receipt.hash) {
      throw new ApiError(422, 'RECEIPT_HASH_MISMATCH', 'receipt hash does not match its contents');
    }
    // 2) Server signature over the same fields.
    const expectedSig = sign(this.config.receiptSecret, fieldsForHash);
    if (expectedSig !== receipt.sig) {
      throw new ApiError(422, 'RECEIPT_SIG_INVALID', 'receipt signature is invalid');
    }
    // 3) The receipt must be one this server emitted.
    if (!stored) {
      throw new ApiError(422, 'RECEIPT_UNKNOWN', 'receipt not found in the persisted chain');
    }
    if (stored.sig !== receipt.sig || stored.hash !== receipt.hash) {
      throw new ApiError(422, 'RECEIPT_HASH_MISMATCH', 'submitted receipt differs from stored receipt');
    }
    // 4) Chain linkage.
    if (receipt.prev_receipt !== stored.prev_receipt) {
      throw new ApiError(422, 'RECEIPT_CHAIN_BROKEN', 'prev_receipt linkage altered');
    }
    if (receipt.prev_receipt) {
      const prev = this.receipts.get(receipt.prev_receipt);
      if (!prev) throw new ApiError(422, 'RECEIPT_CHAIN_BROKEN', 'previous receipt missing');
      if (prev.object_key !== receipt.object_key || prev.seq !== receipt.seq - 1) {
        throw new ApiError(422, 'RECEIPT_CHAIN_BROKEN', 'previous receipt is not the in-order predecessor');
      }
    }
    return { ok: true, object_key: receipt.object_key, seq: receipt.seq, decision_node: receipt.decision_node };
  }
}
