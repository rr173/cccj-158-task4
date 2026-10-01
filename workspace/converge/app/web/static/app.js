/* Multi-site change convergence console.
 * Static page only: every state comes from the server (fetch before render).
 * Credentials live in sessionStorage for the current tab only. */
"use strict";

const $ = (id) => document.getElementById(id);
let auth = null;          // {user, pass}
let me = null;            // {username, role}
let selectedObject = null;
let pollTimer = null;

// ------------------------------------------------------------------ session
function saveSession() {
  try { sessionStorage.setItem("conv_auth", JSON.stringify(auth)); } catch (e) {}
}
function clearSession() {
  auth = null; me = null;
  try { sessionStorage.removeItem("conv_auth"); } catch (e) {}
  $("who").textContent = "";
  $("logoutBtn").hidden = true;
  $("loginBtn").disabled = false;
  document.querySelectorAll(".panel").forEach((p) => (p.hidden = true));
}
try { auth = JSON.parse(sessionStorage.getItem("conv_auth") || "null"); } catch (e) { auth = null; }

async function api(path, opts = {}) {
  const headers = Object.assign(
    { "Authorization": "Basic " + btoa(auth.user + ":" + auth.pass),
      "Cache-Control": "no-store" },
    opts.headers || {}
  );
  if (opts.body !== undefined) headers["Content-Type"] = "application/json";
  const res = await fetch(path, Object.assign({}, opts, { headers }));
  const text = await res.text();
  let data = null;
  try { data = text ? JSON.parse(text) : null; } catch (e) { data = { raw: text }; }
  if (res.status === 401) { clearSession(); throw new Error("未授权（401），请重新登录"); }
  if (!res.ok) throw new Error(`${res.status} ${data && data.error ? data.error : res.statusText}` +
                               (data && data.message ? "：" + data.message : ""));
  return data;
}

async function login() {
  auth = { user: $("user").value.trim(), pass: $("pass").value };
  try {
    me = await api("/api/me");
    saveSession();
    $("who").textContent = `${me.username} · ${roleName(me.role)}`;
    $("logoutBtn").hidden = false;
    showRoleTabs();
  } catch (e) {
    alert(e.message);
    clearSession();
  }
}

function roleName(r) {
  return { operator: "操作员", reviewer: "复核员", inspector: "监察员" }[r] || r;
}

function showRoleTabs() {
  const allowed = {
    operator: ["report", "objects"],
    reviewer: ["objects", "review"],
    inspector: ["objects", "receipts"],
  }[me.role] || [];
  document.querySelectorAll(".tab").forEach((b) => {
    b.style.display = allowed.includes(b.dataset.tab) ? "" : "none";
  });
  switchTab(allowed[0]);
}

function switchTab(name) {
  document.querySelectorAll(".tab").forEach((b) => b.classList.toggle("active", b.dataset.tab === name));
  document.querySelectorAll(".panel").forEach((p) => (p.hidden = p.id !== "tab-" + name));
  if (pollTimer) { clearInterval(pollTimer); pollTimer = null; }
  if (name === "objects") { loadObjects(); pollTimer = setInterval(loadObjects, 5000); }
  if (name === "review") { loadCases(); pollTimer = setInterval(loadCases, 5000); }
}

// ------------------------------------------------------------------ report
function readRegions() {
  const lines = $("f_regions").value.split("\n").map((s) => s.trim()).filter(Boolean);
  return lines.map((line) => {
    const parts = line.split(",").map((x) => parseInt(x.trim(), 10));
    if (parts.length !== 2 || parts.some(isNaN)) throw new Error("区段格式应为 start,end");
    return { start: parts[0], end: parts[1] };
  });
}

async function submitEnvelope() {
  $("reportOut").textContent = "提交中…";
  const prev = $("f_prev").value.trim();
  const envelope = {
    object_key: $("f_obj").value.trim(),
    site_id: $("f_site").value.trim(),
    site_seq: parseInt($("f_seq").value, 10),
    prev_digest: prev ? prev : null,
    regions: readRegions(),
    change_digest: $("f_change").value.trim(),
  };
  try {
    const r = await api("/api/envelopes", {
      method: "POST",
      headers: { "X-Request-Key": $("f_rk").value.trim() },
      body: JSON.stringify({ envelope }),
    });
    $("reportOut").textContent = JSON.stringify(r, null, 2);
  } catch (e) { $("reportOut").textContent = "拒绝：" + e.message; }
}

// ------------------------------------------------------------------ objects
async function loadObjects() {
  try {
    const objs = await api("/api/objects");
    const tb = $("objTable").querySelector("tbody");
    tb.innerHTML = "";
    objs.forEach((o) => {
      const tr = document.createElement("tr");
      tr.className = "clickable" + (o.object_key === selectedObject ? " selected" : "");
      const pending =
        (o.node_counts.waiting ? `挂起 ${o.node_counts.waiting} ` : "") +
        (o.node_counts.deferred ? `待判 ${o.node_counts.deferred}` : "");
      tr.innerHTML = `<td class="mono">${esc(o.object_key)}</td>
        <td>v${o.current_version_no} <span class="small">${o.current_kind || ""}</span></td>
        <td><span class="badge ${o.state}">${o.state}</span></td>
        <td>${esc(pending || "—")}</td>`;
      tr.onclick = () => { selectedObject = o.object_key; loadObjects(); };
      tb.appendChild(tr);
    });
    if (selectedObject) await loadDetail(selectedObject);
  } catch (e) { console.warn(e); }
}

async function loadDetail(objectKey) {
  const d = await api("/api/objects/" + encodeURIComponent(objectKey));
  $("detailMeta").innerHTML =
    `<b class="mono">${esc(d.object_key)}</b> · 当前 v${d.current_version_no || 0} · ` +
    `state_digest <span class="mono">${short(d.current_state_digest)}</span>` +
    (d.redacted_for_inspector ? ' · <span class="badge current">已脱敏（站点→伪名）</span>' : "");
  drawGraph(d);
  const rows = d.nodes.map((n) =>
    `<tr class="${n.status}">
       <td>${short(n.node_digest)}</td><td>${esc(n.site_id)}</td><td>${n.site_seq}</td>
       <td>${short(n.prev_digest)}</td><td>${n.regions.map((r) => r.join("-")).join(",")}</td>
       <td>${n.status}${n.waiting_reason ? " (" + esc(n.waiting_reason.split(":")[0]) + ")" : ""}</td>
     </tr>`).join("");
  $("detailNodes").innerHTML =
    `<table><thead><tr><th>节点</th><th>站点</th><th>序号</th><th>前驱</th><th>区段</th><th>状态</th></tr></thead>
     <tbody>${rows}</tbody></table>`;
}

function drawGraph(d) {
  const svg = $("graph");
  svg.innerHTML = "";
  const ns = "http://www.w3.org/2000/svg";
  const defs = document.createElementNS(ns, "defs");
  defs.innerHTML = '<marker id="arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0,0 L10,5 L0,10 z" fill="#8aa0b6"/></marker>';
  svg.appendChild(defs);

  const byId = Object.fromEntries(d.nodes.map((n) => [n.node_digest, n]));
  const depth = {};
  const getDepth = (id, stack = new Set()) => {
    if (depth[id] !== undefined) return depth[id];
    const n = byId[id];
    if (!n || !n.prev_digest || stack.has(id)) return (depth[id] = 0);
    stack.add(id);
    depth[id] = getDepth(n.prev_digest, stack) + 1;
    stack.delete(id);
    return depth[id];
  };
  d.nodes.forEach((n) => getDepth(n.node_digest));
  const cols = {};
  d.nodes.forEach((n) => {
    const k = depth[n.node_digest];
    cols[k] = (cols[k] || 0) + 1;
  });
  const colRow = {};
  const counts = {};
  const W = 150, H = 58;
  const pos = {};
  d.nodes.slice().sort((a, b) => depth[a.node_digest] - depth[b.node_digest] || a.site_id.localeCompare(b.site_id))
    .forEach((n) => {
      const k = depth[n.node_digest];
      const row = counts[k] || 0;
      counts[k] = row + 1;
      pos[n.node_digest] = { x: 30 + k * W, y: 24 + row * H };
    });

  // edges
  d.nodes.forEach((n) => {
    if (n.prev_digest && pos[n.prev_digest]) {
      const a = pos[n.prev_digest], b = pos[n.node_digest];
      const p = document.createElementNS(ns, "path");
      p.setAttribute("class", "edge");
      p.setAttribute("d", `M${a.x + 120},${a.y + 16} C${a.x + 140},${a.y + 16} ${b.x - 20},${b.y + 16} ${b.x},${b.y + 16}`);
      if (byId[n.prev_digest] && byId[n.prev_digest].status !== "incorporated") {
        p.setAttribute("stroke-dasharray", "4 3");
      }
      svg.appendChild(p);
    }
  });
  // nodes
  d.nodes.forEach((n) => {
    const { x, y } = pos[n.node_digest];
    const g = document.createElementNS(ns, "g");
    const r = document.createElementNS(ns, "rect");
    r.setAttribute("class", "node-rect " + n.status);
    r.setAttribute("x", x); r.setAttribute("y", y); r.setAttribute("width", 122); r.setAttribute("height", 34); r.setAttribute("rx", 6);
    const t1 = document.createElementNS(ns, "text");
    t1.setAttribute("class", "node-text"); t1.setAttribute("x", x + 6); t1.setAttribute("y", y + 14);
    t1.textContent = short(n.node_digest) + " " + n.status;
    const t2 = document.createElementNS(ns, "text");
    t2.setAttribute("class", "node-text"); t2.setAttribute("x", x + 6); t2.setAttribute("y", y + 28);
    t2.textContent = (d.redacted_for_inspector ? "site-*" : n.site_id) + "#" + n.site_seq;
    g.appendChild(r); g.appendChild(t1); g.appendChild(t2);
    const title = document.createElementNS(ns, "title");
    title.textContent = `${n.node_digest}\n${n.status}${n.waiting_reason ? " — " + n.waiting_reason : ""}`;
    g.appendChild(title);
    svg.appendChild(g);
  });
}

// ------------------------------------------------------------------ review
async function loadCases() {
  try {
    const cases = await api("/api/cases?status=open");
    const box = $("cases");
    box.innerHTML = cases.length ? "" : '<p class="muted">当前没有待判冲突。</p>';
    for (const c of cases) box.appendChild(renderCase(c));
  } catch (e) { $("cases").innerHTML = '<p class="muted">' + esc(e.message) + "</p>"; }
}

function renderCase(c) {
  const card = document.createElement("div");
  card.className = "case-card";
  const options = c.frozen_batch.map((row) => {
    const [s, e, ch, nd] = row;
    const label = nd === "__incumbent__"
      ? "保留当前（现任）"
      : `节点 ${short(nd)} · 区段[${s},${e}) · 摘要 ${short(ch)}`;
    return `<label class="mono"><input type="radio" name="ch_${c.case_id}" value="${esc(nd)}" ${c.choice_node_digest === nd ? "checked" : ""}> ${esc(label)}</label>`;
  }).join("<br>");
  card.innerHTML =
    `<h3>${esc(c.case_id)} · 对象 <span class="mono">${esc(c.object_key)}</span> · 第 ${c.step} 步</h3>
     <div class="small">分歧区段 [${c.region[0]},${c.region[1]}) —— 同区段内容不同，需两名不同复核员依次批准同一选择。</div>
     <div class="row">${options}</div>
     <div class="row">
       <input id="note_${c.case_id}" placeholder="判定备注（可选）" style="max-width:280px">
       <button data-act="approve">${c.step === 1 ? "复核员 1 批准选择" : "复核员 2 确认（不同人）"}</button>
       <button data-act="reject" class="ghost">异议（保留打开）</button>
     </div>
     <pre class="out" id="out_${c.case_id}" style="min-height:0;max-height:120px"></pre>`;
  card.querySelectorAll("button").forEach((b) => {
    b.onclick = async () => {
      const choice = card.querySelector(`input[name="ch_${c.case_id}"]:checked`);
      const note = card.querySelector(`#note_${c.case_id}`).value;
      const out = card.querySelector(`#out_${c.case_id}`);
      out.textContent = "提交中…";
      try {
        const r = await api(`/api/cases/${encodeURIComponent(c.case_id)}/reviews`, {
          method: "POST",
          body: JSON.stringify({ action: b.dataset.act, choice_node_digest: choice ? choice.value : null, note }),
        });
        out.textContent = JSON.stringify(r, null, 2);
        loadCases();
      } catch (e) { out.textContent = "拒绝：" + e.message; }
    };
  });
  return card;
}

// ------------------------------------------------------------------ receipts
async function receiptBundle() {
  const key = $("rcpObj").value.trim();
  return api("/api/objects/" + encodeURIComponent(key) + "/receipts");
}
async function loadReceipts() {
  try { $("rcpOut").textContent = JSON.stringify(await receiptBundle(), null, 2); }
  catch (e) { $("rcpOut").textContent = "拒绝：" + e.message; }
}
function downloadReceipts() {
  receiptBundle().then((b) => {
    const blob = new Blob([JSON.stringify(b, null, 2)], { type: "application/json" });
    const a = document.createElement("a");
    a.href = URL.createObjectURL(blob);
    a.download = `receipts-${b.object_key}.json`;
    a.click();
    URL.revokeObjectURL(a.href);
  }).catch((e) => alert(e.message));
}
async function verifyReceipts() {
  const key = $("rcpObj").value.trim();
  try { $("rcpOut").textContent = JSON.stringify(
    await api("/api/objects/" + encodeURIComponent(key) + "/verify"), null, 2); }
  catch (e) { $("rcpOut").textContent = "确定错误：" + e.message; }
}

// ------------------------------------------------------------------ helpers
function short(h) { return h ? h.slice(0, 10) + "…" : "∅"; }
function esc(s) { return String(s == null ? "" : s).replace(/[&<>"]/g, (c) =>
  ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c])); }

// ------------------------------------------------------------------ wire-up
$("loginBtn").onclick = login;
$("pass").addEventListener("keydown", (e) => { if (e.key === "Enter") login(); });
$("logoutBtn").onclick = clearSession;
document.querySelectorAll(".tab").forEach((b) => b.onclick = () => switchTab(b.dataset.tab));
$("submitEnv").onclick = submitEnvelope;
$("refreshObjs").onclick = loadObjects;
$("refreshCases").onclick = loadCases;
$("loadRcp").onclick = loadReceipts;
$("dlRcp").onclick = downloadReceipts;
$("verifyRcp").onclick = verifyReceipts;

if (auth) login();
