'use strict';

const $ = (id) => document.getElementById(id);
const state = {
  token: localStorage.getItem('converge-token') || '',
  role: localStorage.getItem('converge-role') || '',
  selectedObject: null,
  selectedCase: null,
};

const ROLE_FROM_TOKEN = {
  'operator-token': 'operator',
  'reviewer1-token': 'reviewer',
  'reviewer2-token': 'reviewer',
  'inspector-token': 'inspector',
};

function short(h, n = 10) { return typeof h === 'string' ? h.slice(0, n) : h; }
function esc(s) { return String(s).replace(/[&<>"]/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;' }[c])); }

function toast(message, kind = '') {
  const box = document.createElement('div');
  if (kind) box.className = kind === 'ok' ? 'okbox' : 'errbox';
  box.textContent = message;
  $('toast').appendChild(box);
  setTimeout(() => box.remove(), 6000);
}

async function api(method, path, body) {
  const res = await fetch(path, {
    method,
    headers: {
      'Authorization': `Bearer ${state.token}`,
      ...(body !== undefined ? { 'Content-Type': 'application/json' } : {}),
    },
    body: body !== undefined ? JSON.stringify(body) : undefined,
  });
  const text = await res.text();
  let data = null;
  try { data = text ? JSON.parse(text) : null; } catch { data = { raw: text }; }
  if (!res.ok) {
    const err = new Error((data && data.error && (data.error.code + ': ' + data.error.message)) || `HTTP ${res.status}`);
    err.status = res.status;
    err.payload = data;
    throw err;
  }
  return data;
}

function digestInput(s) {
  // Small helper: hash arbitrary text client-side isn't needed; the server only
  // accepts 64-hex digests, so we derive via SubtleCrypto SHA-256.
  return crypto.subtle.digest('SHA-256', new TextEncoder().encode(s)).then((b) =>
    [...new Uint8Array(b)].map((x) => x.toString(16).padStart(2, '0')).join(''));
}

// ------------------------------------------------------------------ session --

function applyRole() {
  $('rolepill').textContent = state.role ? `角色：${state.role}` : '未认证';
  const isOp = state.role === 'operator';
  const isRev = state.role === 'reviewer';
  const isInsp = state.role === 'inspector';
  $('submitPanel').hidden = !isOp;
  $('casesPanel').hidden = !isRev;
  $('receiptsPanel').hidden = !isOp && !isRev && !isInsp;
  $('objectsHint').hidden = !isInsp;
  $('objectsTable').style.display = isInsp ? 'none' : '';
  if (isInsp) $('detailPanel').hidden = true;
  refresh();
}

$('tokenSelect').addEventListener('change', (e) => {
  const t = e.target.value;
  if (!t) return;
  state.token = t;
  state.role = ROLE_FROM_TOKEN[t] || 'unknown';
  localStorage.setItem('converge-token', state.token);
  localStorage.setItem('converge-role', state.role);
  applyRole();
});
$('tokenInput').addEventListener('change', (e) => {
  state.token = e.target.value.trim();
  state.role = '';
  // Infer role from the objects endpoint permission.
  api('GET', '/api/objects').then(() => {
    state.role = state.token.startsWith('reviewer') ? 'reviewer' : 'operator';
  }).catch(async () => {
    state.role = 'inspector';
    try { await api('GET', '/api/receipts'); } catch { state.role = ''; }
  }).finally(() => {
    localStorage.setItem('converge-token', state.token);
    localStorage.setItem('converge-role', state.role);
    applyRole();
  });
});
$('refreshBtn').addEventListener('click', refresh);

// ----------------------------------------------------------------- envelope --

$('genDigest').addEventListener('click', async () => {
  const seed = Math.random().toString(36) + Date.now();
  $('f_change').value = await digestInput(seed);
});
$('genReq').addEventListener('click', () => {
  $('f_req').value = 'req-' + Math.random().toString(36).slice(2, 12) + Date.now().toString(36);
});

function readEnvelope() {
  const preds = $('f_preds').value.split('\n').map((s) => s.trim()).filter(Boolean);
  return {
    object_key: $('f_object').value.trim(),
    site: $('f_site').value.trim(),
    site_seq: Number($('f_seq').value),
    coverage_region: $('f_region').value.trim(),
    change_digest: $('f_change').value.trim(),
    predecessor_digests: preds,
    request_key: $('f_req').value.trim(),
  };
}

$('submitBtn').addEventListener('click', async () => {
  const pre = $('submitResult');
  pre.hidden = false;
  try {
    const r = await api('POST', '/api/envelopes', readEnvelope());
    pre.textContent = JSON.stringify(r, null, 2);
    toast(r.status === 'suspended' ? '信封已挂起：等待前驱' : '上报完成：' + r.status, 'ok');
    loadObjects();
  } catch (e) {
    pre.textContent = JSON.stringify(e.payload || e.message, null, 2);
    toast(e.message, 'err');
  }
});
$('previewDigest').addEventListener('click', async () => {
  try {
    const r = await api('POST', '/api/envelopes/digest', readEnvelope());
    $('submitResult').hidden = false;
    $('submitResult').textContent = JSON.stringify(r, null, 2);
  } catch (e) { toast(e.message, 'err'); }
});

// ------------------------------------------------------------------ objects --

async function loadObjects() {
  if (state.role === 'inspector') return;
  const tbody = $('objectsTable').querySelector('tbody');
  try {
    const { objects } = await api('GET', '/api/objects');
    if (!objects.length) {
      tbody.innerHTML = '<tr><td colspan="4" class="muted">尚无对象</td></tr>';
      return;
    }
    tbody.innerHTML = objects.map((o) => `
      <tr class="clickable" data-key="${esc(o.object_key)}">
        <td class="mono">${esc(o.object_key)}</td>
        <td><span class="tag ${esc(o.status)}">${esc(o.status)}</span>${o.open_case ? `<div class="muted mono" style="margin-top:3px">${esc(short(o.open_case, 14))}</div>` : ''}</td>
        <td class="mono">${o.current_receipt ? esc(short(o.current_receipt, 14)) : '<span class="muted">—</span>'}</td>
        <td>${o.suspended.length ? o.suspended.map((s) => `<div class="warn mono">${esc(short(s.digest, 8))} ← 缺 ${s.missing.length}</div>`).join('') : '<span class="muted">—</span>'}</td>
      </tr>`).join('');
    tbody.querySelectorAll('tr.clickable').forEach((tr) =>
      tr.addEventListener('click', () => showObject(tr.dataset.key)));
    if (state.selectedObject) showObject(state.selectedObject);
  } catch (e) {
    tbody.innerHTML = `<tr><td colspan="4" class="err">${esc(e.message)}</td></tr>`;
  }
}

async function showObject(key) {
  state.selectedObject = key;
  const panel = $('detailPanel');
  panel.hidden = false;
  $('detailTitle').textContent = '对象 ' + key;
  try {
    const d = await api('GET', '/api/objects/' + encodeURIComponent(key));
    drawGraph(d);
    $('suspInfo').innerHTML = d.suspended.length
      ? d.suspended.map((s) => `<div class="warn">挂起节点 ${esc(short(s.digest))}：缺失前驱 ${s.missing.map((m) => esc(short(m))).join(', ')}</div>`).join('')
      : '无挂起信封';
    $('detailBody').innerHTML = renderDetail(d);
  } catch (e) {
    $('detailBody').innerHTML = `<p class="err">${esc(e.message)}</p>`;
  }
}

function renderDetail(d) {
  const parts = [];
  if (d.current) {
    parts.push(`<div class="candidate"><b>当前决定</b> <span class="tag settled">${esc(d.current.decision_kind)}</span>
      <div class="mono muted">receipt #${d.current.seq} ${esc(short(d.current.hash))}</div>
      <pre>${esc(JSON.stringify(d.current.contributions, null, 2))}</pre></div>`);
  }
  if (d.open_case) {
    parts.push(`<div class="candidate"><b>开放冲突</b> <span class="tag conflict">${esc(d.open_case.case_id.slice(0, 14))}</span>
      <div>争议区段：${d.open_case.disputed_regions.map(esc).join(', ')}</div>
      <div class="muted">已录复核：${d.open_case.reviews.length}/2</div></div>`);
  }
  parts.push('<table><thead><tr><th>节点</th><th>类型</th><th>说明</th></tr></thead><tbody>' +
    d.nodes.map((n) => {
      const desc = n.kind === 'change'
        ? `site=<b>${esc(n.site)}</b> #${n.site_seq} 区段 ${esc(n.region)}<br><span class="muted">变更 ${esc(short(n.change_digest))}</span>${n.suspended ? '<br><span class="warn">挂起</span>' : ''}`
        : `${esc(n.decision_kind)}<br><span class="muted">${n.contributions.map((c) => esc(c.region)).join(', ')}</span>${n.reviewers ? '<br>复核：' + n.reviewers.map(esc).join(' / ') : ''}`;
      return `<tr><td class="mono">${esc(short(n.digest, 12))}</td><td>${n.kind}</td><td>${desc}</td></tr>`;
    }).join('') + '</tbody></table>');
  return parts.join('');
}

// Simple layered DAG layout: change/merge nodes in topological columns.
function drawGraph(d) {
  const svg = $('graphSvg');
  const W = svg.clientWidth || 640, H = 420;
  const byDigest = new Map(d.nodes.map((n) => [n.digest, n]));
  const depth = new Map();
  const computeDepth = (dig, visiting = new Set()) => {
    if (depth.has(dig)) return depth.get(dig);
    if (visiting.has(dig)) return 0;
    visiting.add(dig);
    const n = byDigest.get(dig);
    const parents = n ? (n.kind === 'change' ? n.predecessors : n.parents) : [];
    const liveParents = parents.filter((p) => byDigest.has(p));
    const dep = liveParents.length ? 1 + Math.max(...liveParents.map((p) => computeDepth(p, visiting))) : 0;
    depth.set(dig, dep);
    return dep;
  };
  d.nodes.forEach((n) => computeDepth(n.digest));

  const cols = new Map();
  for (const n of d.nodes) {
    const dep = depth.get(n.digest) || 0;
    if (!cols.has(dep)) cols.set(dep, []);
    cols.get(dep).push(n);
  }
  const maxCol = Math.max(1, cols.size);
  const colW = Math.max(150, (W - 80) / maxCol);
  const pos = new Map();
  [...cols.keys()].sort((a, b) => a - b).forEach((dep) => {
    const list = cols.get(dep);
    const rowH = Math.min(80, (H - 40) / Math.max(1, list.length));
    list.forEach((n, i) => pos.set(n.digest, { x: 30 + dep * colW, y: 20 + i * rowH, n }));
  });

  const NS = 'http://www.w3.org/2000/svg';
  svg.innerHTML = '';
  svg.setAttribute('viewBox', `0 0 ${W} ${H}`);

  // Edges first.
  for (const e of d.edges) {
    if (!e.live) continue;
    const a = pos.get(e.from), b = pos.get(e.to);
    if (!a || !b) continue;
    const path = document.createElementNS(NS, 'path');
    path.setAttribute('class', 'edge');
    path.setAttribute('d', `M ${a.x + 118} ${a.y + 22} C ${a.x + colW * 0.6} ${a.y + 22}, ${b.x + colW * 0.4} ${b.y + 22}, ${b.x} ${b.y + 22}`);
    svg.appendChild(path);
  }
  // Missing predecessor markers.
  for (const s of d.suspended) {
    const b = pos.get(s.digest);
    if (!b) continue;
    for (const m of s.missing) {
      const line = document.createElementNS(NS, 'path');
      line.setAttribute('class', 'edge missing');
      line.setAttribute('d', `M ${b.x - 40} ${b.y + 22} L ${b.x} ${b.y + 22}`);
      svg.appendChild(line);
      const t = document.createElementNS(NS, 'text');
      t.setAttribute('class', 'node-sub');
      t.setAttribute('x', b.x - 40); t.setAttribute('y', b.y + 12);
      t.textContent = '缺 ' + short(m, 6);
      svg.appendChild(t);
    }
  }
  // Nodes.
  for (const [dig, p] of pos) {
    const n = p.n;
    const g = document.createElementNS(NS, 'g');
    const r = document.createElementNS(NS, 'rect');
    r.setAttribute('class', `node-rect ${n.kind}${n.suspended ? ' susp' : ''}`);
    r.setAttribute('x', p.x); r.setAttribute('y', p.y); r.setAttribute('width', 124); r.setAttribute('height', 44); r.setAttribute('rx', 7);
    g.appendChild(r);
    const t1 = document.createElementNS(NS, 'text');
    t1.setAttribute('class', 'node-text');
    t1.setAttribute('x', p.x + 7); t1.setAttribute('y', p.y + 17);
    t1.textContent = (n.kind === 'change' ? '◦ ' : '◆ ') + short(dig, 9);
    g.appendChild(t1);
    const t2 = document.createElementNS(NS, 'text');
    t2.setAttribute('class', 'node-sub');
    t2.setAttribute('x', p.x + 7); t2.setAttribute('y', p.y + 34);
    t2.textContent = n.kind === 'change' ? `${n.site}#${n.site_seq} ${n.region}` : n.decision_kind;
    g.appendChild(t2);
    svg.appendChild(g);
  }
}

// -------------------------------------------------------------------- cases --

async function loadCases() {
  if (state.role !== 'reviewer') return;
  try {
    const { cases } = await api('GET', '/api/cases');
    if (!cases.length) { $('casesList').innerHTML = '<p class="muted">当前无待判定冲突。</p>'; return; }
    $('casesList').innerHTML = cases.map((c) => `
      <div class="candidate" data-case="${esc(c.case_id)}">
        <b>${esc(c.object_key)}</b> <span class="tag conflict">冲突</span>
        <div class="mono muted">case ${esc(short(c.case_id, 16))}</div>
        <div>争议区段：${c.disputed_regions.map(esc).join(', ')}</div>
        <div class="muted">已录复核 ${2 - c.awaiting}/2</div>
        <div class="choices"></div>
      </div>`).join('');
    document.querySelectorAll('#casesList .candidate').forEach(async (el) => {
      const id = el.dataset.case;
      const full = await api('GET', '/api/cases/' + encodeURIComponent(id));
      const box = el.querySelector('.choices');
      box.innerHTML = full.disputed_regions.map((region) => `
        <label>区段 <b>${esc(region)}</b>
          <select data-region="${esc(region)}">
            <option value="">— 选择保留的变更摘要 —</option>
            ${[...new Set(full.candidates.filter((c) => c.region === region).map((c) => c.change_digest))]
              .map((dig) => `<option value="${esc(dig)}">${esc(short(dig, 20))}</option>`).join('')}
          </select>
        </label>`).join('') +
        (full.reviews.length ? `<div class="muted">一审（${esc(full.reviews[0].reviewer_name)}）：${esc(JSON.stringify(full.reviews[0].choices))}</div>` : '') +
        '<button class="ghost" style="margin-top:8px">提交复核</button>';
      box.querySelector('button').addEventListener('click', async () => {
        const choices = {};
        box.querySelectorAll('select').forEach((s) => { if (s.value) choices[s.dataset.region] = s.value; });
        try {
          const r = await api('POST', '/api/reviews/' + encodeURIComponent(id), { choices });
          toast('复核结果：' + r.status, r.status === 'adjudicated' ? 'ok' : '');
          loadCases(); loadObjects();
        } catch (e) {
          toast(e.message + (e.payload?.error?.details ? ' ' + JSON.stringify(e.payload.error.details) : ''), 'err');
        }
      });
    });
  } catch (e) {
    $('casesList').innerHTML = `<p class="err">${esc(e.message)}</p>`;
  }
}

// ----------------------------------------------------------------- receipts --

async function loadReceipts() {
  const key = $('receiptFilter').value.trim();
  try {
    const { receipts } = await api('GET', '/api/receipts' + (key ? '?object_key=' + encodeURIComponent(key) : ''));
    $('receiptsList').innerHTML = receipts.length ? `
      <table><thead><tr><th>#</th><th>对象</th><th>类型</th><th>回执链哈希</th></tr></thead><tbody>
      ${receipts.map((r) => `<tr class="clickable" data-h="${esc(r.hash)}">
        <td>${r.seq}</td><td class="mono">${esc(r.object_key)}</td><td>${esc(r.decision_kind)}</td>
        <td class="mono">${esc(short(r.hash, 18))}${r.prev_receipt ? '<div class="muted">↑ ' + esc(short(r.prev_receipt, 14)) + '</div>' : '<div class="muted">链首</div>'}</td>
      </tr>`).join('')}
      </tbody></table>
      ${key ? `<div style="margin-top:8px"><a href="/api/objects/${encodeURIComponent(key)}/receipts/download" id="dlLink">下载该对象脱敏收据链（JSON）</a></div>` : ''}
    ` : '<p class="muted">无收据。</p>';
    document.querySelectorAll('#receiptsList tr.clickable').forEach((tr) =>
      tr.addEventListener('click', async () => {
        const r = await api('GET', '/api/receipts/' + encodeURIComponent(tr.dataset.h));
        const pre = $('receiptVerify');
        pre.hidden = false;
        pre.textContent = JSON.stringify(r, null, 2);
      }));
    const dl = $('dlLink');
    if (dl) dl.addEventListener('click', (e) => {
      // Ensure the download is authenticated.
      e.preventDefault();
      fetch(dl.href, { headers: { Authorization: `Bearer ${state.token}` } })
        .then((x) => x.blob()).then((blob) => {
          const a = document.createElement('a');
          a.href = URL.createObjectURL(blob);
          a.download = `receipts-${key}.json`;
          a.click();
        });
    });
  } catch (e) {
    $('receiptsList').innerHTML = `<p class="err">${esc(e.message)}</p>`;
  }
}
$('loadReceipts').addEventListener('click', loadReceipts);
$('verifyModeBtn').addEventListener('click', () => {
  const pre = $('receiptVerify');
  pre.hidden = false;
  pre.contentEditable = 'true';
  pre.textContent = pre.textContent || '// 粘贴一条收据 JSON，然后点“校验”';
  const btn = document.createElement('button');
  btn.textContent = '校验此收据';
  btn.className = 'ghost';
  btn.style.marginTop = '6px';
  btn.onclick = async () => {
    try {
      const receipt = JSON.parse(pre.textContent.replace(/^\/\/.*$/gm, ''));
      const r = await api('POST', '/api/receipts/verify', { receipt });
      toast('校验通过：' + JSON.stringify(r), 'ok');
    } catch (e) { toast('校验失败：' + e.message, 'err'); }
  };
  if (pre.dataset.verifyBtn === '1') return;
  pre.dataset.verifyBtn = '1';
  pre.after(btn);
});

// ------------------------------------------------------------------- boot ---

function refresh() {
  if (!state.token) return;
  loadObjects();
  loadCases();
  loadReceipts();
}
if (state.token) {
  $('tokenSelect').value = state.token.startsWith('reviewer') || ROLE_FROM_TOKEN[state.token] ? state.token : '';
  applyRole();
}
