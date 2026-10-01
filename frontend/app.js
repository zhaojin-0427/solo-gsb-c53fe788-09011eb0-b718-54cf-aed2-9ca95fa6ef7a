"use strict";
/* 多人应急演练平台 — 原生 JS 前端（无构建步骤）
 *
 * hash 路由：
 *   #/                         首页：主持人 / 参与者入口
 *   #/host                     主持人控制台（剧本、会话、联合演练）
 *   #/host/session/<sid>       某会话的主持台（实时）
 *   #/host/joints              联合演练列表
 *   #/host/joint/<id|new>      联合演练编排编辑器 / 实时监控台
 *   #/join/<sid>[?invite=...]  参与者输入邀请码
 *   #/play/<sid>               参与者控制台（实时）
 *   #/replay/<sid>             确定性回放
 */

const app = document.getElementById("app");
const nav = document.getElementById("nav");
const navContext = document.getElementById("nav-context");
document.getElementById("nav-home").addEventListener("click", (e) => {
  e.preventDefault();
  location.hash = "#/";
});

// --------------------------------------------------------------------------- //
// utils
// --------------------------------------------------------------------------- //
const HOST_KEY = () => localStorage.getItem("host_key") || "";
const credKey = (sid) => `pt_${sid}`;
const getCred = (sid) => {
  try { return JSON.parse(localStorage.getItem(credKey(sid)) || "null"); }
  catch { return null; }
};
const setCred = (sid, c) => localStorage.setItem(credKey(sid), JSON.stringify(c));

const esc = (s) =>
  String(s ?? "").replace(/[&<>"']/g, (c) =>
    ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c])
  );

function toast(msg, kind = "") {
  const t = document.getElementById("toast");
  t.textContent = msg;
  t.className = `toast ${kind}`;
  clearTimeout(toast._tm);
  toast._tm = setTimeout(() => t.classList.add("hidden"), 3200);
}

function el(html) {
  const wrap = document.createElement("div");
  wrap.innerHTML = html.trim();
  return wrap.firstElementChild;
}

// Append a <tr> built from an HTML string. A bare <tr> cannot be parsed via a
// <div> wrapper (the parser fosters its cells out and drops the row), so the
// row must be created through the table API and populated via innerHTML.
function addRow(tbody, html) {
  const tr = tbody.insertRow(-1);
  tr.innerHTML = html;
  return tr;
}

async function api(path, opts = {}) {
  const headers = Object.assign({ "Content-Type": "application/json" }, opts.headers || {});
  if (opts.host) headers["X-Host-Key"] = HOST_KEY();
  if (opts.token) headers["Authorization"] = `Bearer ${opts.token}`;
  const res = await fetch(path, {
    method: opts.method || "GET",
    headers,
    body: opts.body ? JSON.stringify(opts.body) : undefined,
  });
  let data = null;
  try { data = await res.json(); } catch { /* empty body */ }
  if (!res.ok) {
    const err = new Error((data && data.detail) || `请求失败 (${res.status})`);
    err.status = res.status;
    throw err;
  }
  return data;
}

function copyText(text) {
  navigator.clipboard?.writeText(text).then(
    () => toast("已复制到剪贴板", "ok"),
    () => toast("复制失败")
  );
}

function fmtTime(ts) {
  if (ts == null) return "—";
  return new Date(ts * 1000).toLocaleString("zh-CN", { hour12: false });
}
function fmtClock(ts) {
  if (ts == null) return "--:--:--";
  return new Date(ts * 1000).toLocaleTimeString("zh-CN", { hour12: false });
}
function roleName(snap, rid) {
  const r = (snap.roles || []).find((x) => x.id === rid);
  if (r) return r.name;
  const p = (snap.participants || []).find((x) => x.role_id === rid);
  return p ? p.role_name : rid;
}
function statusChip(status) {
  return ({
    created: ["未开始", "warn"],
    running: ["进行中", "ok"],
    ended: ["已结束", "danger"],
  }[status] || [status, ""]);
}

// --------------------------------------------------------------------------- //
// live WebSocket with auto reconnect
// --------------------------------------------------------------------------- //
class Live {
  constructor(sessionId, viewer, handlers) {
    this.sessionId = sessionId;
    this.viewer = viewer;             // {host:'key'} or {token:'...'}
    this.onSnapshot = handlers.onSnapshot || (() => {});
    this.onResult = handlers.onResult || (() => {});
    this.onStatus = handlers.onStatus || (() => {});
    this.closedByUser = false;
    this.retry = 0;
    this.serverOffset = 0;
    this.connect();
  }
  url() {
    const proto = location.protocol === "https:" ? "wss" : "ws";
    const q = this.viewer.host
      ? `host=${encodeURIComponent(this.viewer.host)}`
      : `token=${encodeURIComponent(this.viewer.token)}`;
    return `${proto}://${location.host}/ws/${this.sessionId}?${q}`;
  }
  connect() {
    const ws = new WebSocket(this.url());
    this.ws = ws;
    this.onStatus("connecting");
    ws.onopen = () => this.onStatus("open");
    ws.onmessage = (ev) => {
      const msg = JSON.parse(ev.data);
      if (msg.type === "snapshot") {
        this.retry = 0;
        this.serverOffset = (msg.snapshot.server_time || 0) - Date.now() / 1000;
        this.onSnapshot(msg.snapshot);
      } else if (msg.type === "pong") {
        this.serverOffset = msg.server_time - Date.now() / 1000;
      } else if (msg.type === "submit_result") {
        this.onResult(msg);
      } else if (msg.type === "error") {
        toast(msg.detail || "操作被拒绝", "error");
        if (msg.code === "auth") this.close();
      }
    };
    ws.onclose = () => {
      if (this.closedByUser) return;
      this.retry = Math.min(this.retry + 1, 6);
      const delay = Math.min(500 * 2 ** this.retry, 10000);
      this.onStatus("reconnecting", delay);
      setTimeout(() => { if (!this.closedByUser) this.connect(); }, delay);
    };
  }
  send(obj) {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify(obj));
      return true;
    }
    toast("连接中断，正在重连…", "error");
    return false;
  }
  now() { return Date.now() / 1000 + this.serverOffset; }
  close() { this.closedByUser = true; try { this.ws.close(); } catch {} }
}

// --------------------------------------------------------------------------- //
// router
// --------------------------------------------------------------------------- //
let intervals = [];
let liveHandle = null;
function clearTimers() {
  intervals.forEach(clearInterval);
  intervals = [];
}
function teardownLive() {
  if (liveHandle) { liveHandle.close(); liveHandle = null; }
}

function route() {
  teardownLive();
  clearTimers();
  const raw = (location.hash || "#/").replace(/^#\/?/, "");
  const [path, query] = raw.split("?");
  const params = new URLSearchParams(query || "");
  const parts = path.split("/").filter(Boolean);
  nav.classList.toggle("hidden", parts.length === 0);
  navContext.textContent = "";

  if (parts[0] === "host" && parts[1] === "session" && parts[2]) {
    navContext.textContent = `主持人 · 会话 ${parts[2]}`;
    return hostSessionPage(parts[2]);
  }
  if (parts[0] === "host" && parts[1] === "joint" && parts[2]) {
    navContext.textContent = `主持人 · 联合演练编排`;
    return jointEditPage(parts[2]);
  }
  if (parts[0] === "host" && parts[1] === "joint-review" && parts[2]) {
    navContext.textContent = "主持人 · 联演因果时序复盘";
    return jointReviewPage(parts[2]);
  }
  if (parts[0] === "host" && parts[1] === "joints") {
    navContext.textContent = "主持人 · 联合演练";
    return jointsPage();
  }
  if (parts[0] === "host" && parts[1] === "drill" && parts[2]) {
    navContext.textContent = `主持人 · 剧本版本 ${parts[2]}`;
    return drillVersionPage(parts[2]);
  }
  if (parts[0] === "host") { navContext.textContent = "主持人控制台"; return hostPage(); }
  if (parts[0] === "join" && parts[1]) return joinPage(parts[1], params);
  if (parts[0] === "play" && parts[1]) return playPage(parts[1]);
  if (parts[0] === "replay" && parts[1]) return replayPage(parts[1]);
  return homePage();
}
window.addEventListener("hashchange", route);

// --------------------------------------------------------------------------- //
// home
// --------------------------------------------------------------------------- //
function homePage() {
  app.innerHTML = "";
  app.appendChild(el(`
    <div class="card">
      <h2>主持人入口</h2>
      <p class="muted">管理 YAML 剧本、创建会话、实时结算、回放与导出全量记录。</p>
      <label>主持人密钥（HOST_KEY）</label>
      <input type="password" id="hk" value="${esc(HOST_KEY())}" placeholder="默认 host-1234" />
      <div class="row" style="margin-top:14px">
        <button id="go-host">进入主持人控制台</button>
      </div>
    </div>`));
  app.appendChild(el(`
    <div class="card">
      <h2>参与者入口</h2>
      <p class="muted">凭主持人发放的会话编号与角色邀请码加入；只能看到本角色的信息。</p>
      <div class="row">
        <input type="text" id="sid" placeholder="会话编号（8 位）" style="max-width:280px" />
        <button id="go-join" class="ghost">加入演练</button>
      </div>
    </div>`));
  document.getElementById("go-host").onclick = () => {
    localStorage.setItem("host_key", document.getElementById("hk").value.trim());
    location.hash = "#/host";
  };
  document.getElementById("go-join").onclick = () => {
    const sid = document.getElementById("sid").value.trim();
    if (!sid) return toast("请输入会话编号");
    location.hash = `#/join/${encodeURIComponent(sid)}`;
  };
}

// --------------------------------------------------------------------------- //
// host: drills & sessions
// --------------------------------------------------------------------------- //
async function hostPage() {
  app.innerHTML = `<div class="card"><h2>主持人控制台</h2><p class="muted">加载中…</p></div>`;
  let drills, sessions;
  try {
    [drills, sessions] = await Promise.all([
      api("/api/drills", { host: true }),
      api("/api/sessions", { host: true }),
    ]);
  } catch (e) {
    app.innerHTML = "";
    app.appendChild(el(`<div class="card"><h2>主持人控制台</h2>
      <p class="chip danger">${esc(e.message)}</p>
      <p class="muted small">请返回首页检查主持人密钥。</p></div>`));
    return;
  }

  const card = el(`<div class="card">
    <h2>演练剧本（YAML）</h2>
    <div id="drill-list"></div>
    <h3>上传 / 编辑剧本（生成草稿版本）</h3>
    <p class="muted small" id="base-hint">
      保存只生成不可变<strong>草稿</strong>并计算内容摘要；草稿发布后才能用于新会话。
      若已存在发布版，编辑器会自动以<strong>当前发布版</strong>为基准（乐观锁：
      期间他人发布了新版本时发布会被拒绝）。
    </p>
    <textarea id="yaml" placeholder="粘贴 YAML 剧本（顶层 id 需保证唯一）"></textarea>
    <div class="row" style="margin-top:10px">
      <button id="upload">保存为草稿版本</button>
      <span class="muted small" id="upload-state"></span>
    </div>
  </div>`);

  const list = card.querySelector("#drill-list");
  if (!drills.length) {
    list.innerHTML = `<p class="muted">暂无剧本。将 YAML 放入 <code>drills/</code> 目录，或在下方粘贴上传（首次保存为草稿，发布后生效）。</p>`;
  } else {
    for (const d of drills) {
      const pub = d.published_version;
      const draft = d.draft_version;
      const row = el(`<div class="box">
        <div class="row"><strong>${esc(d.name)}</strong>
          <span class="chip mono">${esc(d.id)}</span>
          ${pub ? `<span class="chip ok">已发布 v${pub}</span>`
                : '<span class="chip warn">未发布</span>'}
          ${draft ? `<span class="chip ${pub ? "warn" : "host"}">草稿 v${draft}</span>` : ""}
          <span class="spacer"></span>
          <button class="ghost" data-act="versions">版本管理</button>
          ${pub ? `<button data-act="create">创建会话（v${pub}）</button>`
                : '<button disabled title="发布草稿后才能创建会话">不可创建会话</button>'}
        </div>
        <div class="muted small">${esc(d.description || "")}</div></div>`);
      row.querySelector('[data-act="versions"]').onclick = () => {
        location.hash = `#/host/drill/${encodeURIComponent(d.id)}`;
      };
      const createBtn = row.querySelector('[data-act="create"]');
      if (createBtn) {
        createBtn.onclick = async (ev) => {
          try {
            const r = await api(`/api/sessions?drill_id=${encodeURIComponent(d.id)}`,
              { method: "POST", host: true });
            toast(`会话 ${r.session_id} 已创建（绑定 v${r.drill_version}）`, "ok");
            location.hash = `#/host/session/${r.session_id}`;
          } catch (e2) { toast(e2.message, "error"); }
        };
      }
      list.appendChild(row);
    }
  }
  card.querySelector("#upload").onclick = async () => {
    const state = card.querySelector("#upload-state");
    // Resolve the base version from the parsed YAML's id at click time, so the
    // optimistic-lock check always reflects the freshest drill list.
    let yamlText = card.querySelector("#yaml").value;
    let drillId = null;
    try {
      // Cheap best-effort id read for base lookup; server validates for real.
      drillId = (yamlText.match(/^\s*id\s*:\s*['"]?([\w.-]+)/m) || [])[1];
    } catch { drillId = null; }
    let baseVersion = null;
    if (drillId) {
      const match = drills.find((x) => x.id === drillId);
      baseVersion = match ? match.published_version : null;
    }
    state.textContent = "保存中…";
    try {
      const r = await api("/api/drills", {
        method: "POST", host: true, body: { yaml: yamlText, base_version: baseVersion },
      });
      if (r.reused) {
        toast(`内容未变化，复用 v${r.version}（${r.digest_short}）`, "ok");
      } else {
        toast(`草稿 v${r.version} 已保存（摘要 ${r.digest_short}）`, "ok");
      }
      location.hash = `#/host/drill/${encodeURIComponent(r.id)}`;
    } catch (e) {
      state.textContent = "";
      toast(e.message, "error");
    }
  };

  const sc = el(`<div class="card"><h2>会话</h2><div id="session-list"></div></div>`);
  const sl = sc.querySelector("#session-list");
  if (!sessions.length) {
    sl.innerHTML = `<p class="muted">暂无会话。</p>`;
  } else {
    for (const s of sessions) {
      const [label, cls] = statusChip(s.status);
      sl.appendChild(el(`<div class="box">
        <div class="row">
          <strong class="mono">${esc(s.id)}</strong>
          <span class="chip ${cls}">${label}</span>
          <span class="muted small">${esc(s.drill_name || s.drill_id)}</span>
          <span class="chip mono" title="会话创建时固化的剧本版本与内容摘要">
            v${esc(s.drill_version)} · ${esc(s.digest_short || "")}
          </span>
          <span class="spacer"></span>
          <a href="#/host/session/${esc(s.id)}"><button class="ghost">主持台</button></a>
          <a href="#/replay/${esc(s.id)}"><button class="ghost">回放</button></a>
        </div></div>`));
    }
  }

  app.innerHTML = "";

  const jc = el(`<div class="card">
    <div class="row">
      <h2 style="margin:0">🔗 联合演练编排</h2>
      <span class="spacer"></span>
      <a href="#/host/joints"><button class="ghost">进入联合演练模块</button></a>
    </div>
    <p class="muted small" style="margin-bottom:0">
      把多个<strong>独立会话</strong>编入同一场联演，为各会话阶段配置有向依赖：
      上游阶段达到指定结局（跳转结局或某角色的选择）后，向下游会话的指定阶段
      <strong>追加一次性联演注入</strong>并按该会话角色权限推送；多个前置分支
      <strong>全部满足</strong>才释放。保存时校验依赖无环，会话/阶段/结局有效；
      各会话保留各自独立的剧本版本与事件记录。
    </p>
  </div>`);

  app.append(card, jc, sc);
}

// --------------------------------------------------------------------------- //
// host: joint exercise orchestration ("联合演练编排")
// --------------------------------------------------------------------------- //
async function jointsPage() {
  app.innerHTML = `<div class="card"><h2>🔗 联合演练</h2><p class="muted">加载中…</p></div>`;
  let joints, sessions;
  try {
    [joints, sessions] = await Promise.all([
      api("/api/joints", { host: true }),
      api("/api/sessions", { host: true }),
    ]);
  } catch (e) {
    app.innerHTML = `<div class="card"><h2>联合演练</h2>
      <p class="chip danger">${esc(e.message)}</p>
      <a href="#/host"><button class="ghost">返回控制台</button></a></div>`;
    return;
  }
  const root = el(`<div>
    <div class="card">
      <div class="row">
        <h2 style="margin:0">联合演练</h2>
        <span class="spacer"></span>
        <a href="#/host"><button class="ghost">← 返回控制台</button></a>
        <a href="#/host/joint/new"><button>＋ 编排新的联合演练</button></a>
      </div>
      <p class="muted small" style="margin-bottom:0">
        一场联演包含多个独立会话；编排定义“某会话的某阶段达到指定结局”后，
        向另一个会话的指定阶段<strong>一次性追加注入</strong>（AND 汇合）。
        注入写入目标会话自己的事件日志（新版本号），各会话的剧本版本与事件记录互不影响。
      </p>
    </div>
    <div class="card" id="joint-list"></div>
    <div class="card" id="session-picker"></div>
  </div>`);

  const list = root.querySelector("#joint-list");
  if (!joints.length) {
    list.innerHTML = `<p class="muted">还没有联合演练。先在主持人控制台为<strong>已发布剧本</strong>创建多个会话，再点右上方“编排新的联合演练”。</p>`;
  } else {
    for (const j of joints) {
      const row = el(`<div class="box">
        <div class="row">
          <strong>${esc(j.name)}</strong>
          <span class="chip mono">${esc(j.id)}</span>
          <span class="chip ok">已投递 ${j.released_count}</span>
          <span class="chip warn">等待 ${j.waiting_count}</span>
          <span class="chip danger">错过 ${j.missed_count}</span>
          <span class="muted small">${j.member_count} 个会话 · ${j.gate_count} 条编排</span>
          <span class="spacer"></span>
          <a href="#/host/joint-review/${esc(j.id)}"><button class="ghost">⏱ 因果时序复盘</button></a>
          <a href="#/host/joint/${esc(j.id)}"><button class="ghost">打开编排 / 监控台</button></a>
        </div>
        <div class="muted small">${esc(j.description || "")}</div>
      </div>`);
      list.appendChild(row);
    }
  }

  const picker = root.querySelector("#session-picker");
  picker.innerHTML = `<h2>可选会话（各会话绑定各自的剧本版本）</h2>`;
  if (!sessions.length) {
    picker.appendChild(el(`<p class="muted small">暂无会话。</p>`));
  } else {
    const tbl = el(`<table><thead><tr>
      <th>会话</th><th>剧本（绑定版本）</th><th>状态</th></tr></thead><tbody></tbody></table>`);
    for (const s of sessions) {
      const [label, cls] = statusChip(s.status);
      addRow(tbl.querySelector("tbody"), `
        <td class="mono">${esc(s.id)}</td>
        <td>${esc(s.drill_name || s.drill_id)}
          <span class="chip mono">v${esc(s.drill_version)} · ${esc(s.digest_short || "")}</span></td>
        <td><span class="chip ${cls}">${label}</span></td>`);
    }
    picker.appendChild(tbl);
  }

  app.innerHTML = "";
  app.appendChild(root);
}

// --------------------------------------------------------------------------- //
// joint editor + live monitor (#/host/joint/new | #/host/joint/<id>)
// --------------------------------------------------------------------------- //
async function jointEditPage(jid) {
  const isNew = jid === "new";
  app.innerHTML = `<div class="card"><h2>${isNew ? "编排新的联合演练" : "联合演练编排"}</h2>
    <p class="muted">加载中…</p></div>`;

  let sessions = [];
  let scripts = {};            // session_id -> pinned script reference
  let detail = null;           // existing joint detail (or null)
  try {
    sessions = await api("/api/sessions", { host: true });
    if (!isNew) detail = await api(`/api/joints/${encodeURIComponent(jid)}`, { host: true });
  } catch (e) {
    app.innerHTML = `<div class="card"><h2>联合演练编排</h2>
      <p class="chip danger">${esc(e.message)}</p>
      <a href="#/host/joints"><button class="ghost">返回列表</button></a></div>`;
    return;
  }
  if (!isNew) jid = detail.id;

  // Pinned scripts are fetched lazily when a session is checked, so a long
  // session list does not mean dozens of requests on page load.
  async function scriptFor(sid) {
    if (!scripts[sid]) scripts[sid] = await api(`/api/sessions/${sid}/script`, { host: true });
    return scripts[sid];
  }

  const root = el(`<div>
    <div class="card">
      <div class="row">
        <h2 style="margin:0">${isNew ? "编排新的联合演练" : esc(detail.name)}</h2>
        ${detail ? `<span class="chip mono">${esc(detail.id)}</span>` : ""}
        <span class="spacer"></span>
        <a href="#/host/joints"><button class="ghost">← 返回列表</button></button></a>
        ${detail ? '<a href="#/host/joint-review/' + esc(detail.id) + '"><button class="ghost">⏱ 因果时序复盘</button></a>' : ""}
        ${detail ? '<button class="danger" id="delete-joint">删除联演</button>' : ""}
      </div>
      <label>联演名称</label>
      <input type="text" id="j-name" value="${detail ? esc(detail.name) : ""}" placeholder="如：厂区间联合处置演练" />
      <label>说明</label>
      <input type="text" id="j-desc" value="${detail ? esc(detail.description || "") : ""}" />
    </div>
    <div class="card">
      <h2>1. 选择参演会话（独立会话，可来自不同剧本/版本）</h2>
      <div id="member-box" class="joint-members"></div>
      <p class="muted small" style="margin-bottom:0">
        每个会话保留其创建时固化的剧本版本与摘要；编排里引用的阶段、结局、角色
        均按各会话<strong>绑定版本</strong>校验。
      </p>
    </div>
    <div class="card">
      <div class="row">
        <h2 style="margin:0">2. 依赖编排（有向、无环；AND 汇合）</h2>
        <span class="spacer"></span>
        <button id="add-gate" class="ghost">＋ 新增一条注入编排</button>
      </div>
      <div id="gates"></div>
      <p class="muted small">
        每条编排：当<strong>所有前置分支</strong>观察到指定结局（阶段结算跳转，
        或某角色的确定选项）且目标会话正处在目标阶段时，向该阶段
        <strong>一次性追加注入</strong>；目标阶段先结算则记为“已错过”，不补投。
      </p>
    </div>
    <div class="card">
      <div class="row">
        <button id="save-joint">${isNew ? "保存并开始监控" : "保存修改"}</button>
        <span id="save-state" class="muted small"></span>
      </div>
      <p class="muted small" style="margin-bottom:0">
        保存前服务端校验：会话/阶段/结局/角色有效、分支不重复、依赖无环。
        已投递或已错过的编排不可修改/删除（保留追溯）；未释放的编排可自由编辑。
      </p>
    </div>
    ${detail ? `<div class="card"><h2>3. 主持台：分支等待状态与阻塞原因</h2>
      <div id="monitor"><p class="muted small">载入中…</p></div>
      <p class="muted small" style="margin-bottom:0">本页每 2 秒自动刷新分支满足、
        释放/错过状态；注入投递的实时推送同时出现在目标会话参与者界面上。</p>
    </div>` : ""}
  </div>`);
  app.innerHTML = "";
  app.appendChild(root);

  // ---- members ------------------------------------------------------------
  const memberBox = root.querySelector("#member-box");
  const selected = new Set(detail ? detail.members.map((m) => m.session_id) : []);
  const tbl = el(`<table><thead><tr>
    <th>参演</th><th>会话</th><th>剧本（绑定版本）</th><th>状态</th>
  </tr></thead><tbody></tbody></table>`);
  for (const s of sessions) {
    const [label, cls] = statusChip(s.status);
    addRow(tbl.querySelector("tbody"), `
      <td><input type="checkbox" data-sid="${esc(s.id)}" ${selected.has(s.id) ? "checked" : ""}/></td>
      <td class="mono">${esc(s.id)}
        <a href="#/host/session/${esc(s.id)}" target="_blank"><button class="ghost" style="padding:2px 8px">主持台↗</button></a></td>
      <td>${esc(s.drill_name || s.drill_id)}
        <span class="chip mono">v${esc(s.drill_version)} · ${esc(s.digest_short || "")}</span></td>
      <td><span class="chip ${cls}">${label}</span></td>`);
  }
  memberBox.appendChild(tbl);
  if (!sessions.length) {
    memberBox.appendChild(el(`<p class="chip warn">请先创建至少一个会话。</p>`));
  }
  const memberChecks = () => [...memberBox.querySelectorAll("input[type=checkbox]:checked")].map((c) => c.dataset.sid);

  // ---- gates --------------------------------------------------------------
  const gatesBox = root.querySelector("#gates");
  let gateSeq = 0;
  const frozenGates = new Set();  // released/missed gates: immutable in the editor

  function memberSession(sid) {
    return sessions.find((s) => s.id === sid);
  }

  function stageOptionsHtml(script, sel) {
    return (script?.stages || []).map(
      (st) => `<option value="${esc(st.id)}" ${st.id === sel ? "selected" : ""}>${esc(st.title)} (${esc(st.id)})</option>`
    ).join("");
  }

  function branchRow(prefill, scriptsReady) {
    const p = prefill || {};
    const row = el(`<div class="branch-row box">
      <div class="row">
        <label style="margin:0">上游会话
          <select class="b-session"></select></label>
        <label style="margin:0">上游阶段
          <select class="b-stage"></select></label>
        <label style="margin:0">结局类型
          <select class="b-kind">
            <option value="next_stage">结算跳转</option>
            <option value="choice">角色选择</option>
          </select></label>
        <label class="b-role-wrap" style="margin:0;display:none">角色
          <select class="b-role"></select></label>
        <label class="b-value-wrap" style="margin:0">结局值
          <select class="b-value"></select></label>
        <button class="ghost b-del" type="button">删除分支</button>
      </div>
      <p class="muted small b-hint" style="margin:6px 0 0"></p>
    </div>`);
    const sSel = row.querySelector(".b-session");
    const stSel = row.querySelector(".b-stage");
    const kSel = row.querySelector(".b-kind");
    const rWrap = row.querySelector(".b-role-wrap");
    const rSel = row.querySelector(".b-role");
    const vWrap = row.querySelector(".b-value-wrap");
    const vSel = row.querySelector(".b-value");
    const hint = row.querySelector(".b-hint");

    for (const sid of memberChecks()) {
      const s = memberSession(sid);
      sSel.appendChild(el(`<option value="${esc(sid)}">${esc(sid)} · ${esc(s.drill_name || s.drill_id)}</option>`));
    }
    sSel.value = p.session_id || sSel.value;

    async function refreshStages(keep) {
      stSel.innerHTML = "";
      const sid = sSel.value;
      if (!sid) return;
      const sc = await scriptFor(sid);
      stSel.innerHTML = stageOptionsHtml(sc, keep);
      if (!keep) stSel.value = (sc.stages[0] || {}).id || "";
      refreshKindRows();
    }
    async function refreshKindRows() {
      const sid = sSel.value;
      const sc = sid ? await scriptFor(sid) : null;
      const stage = (sc?.stages || []).find((x) => x.id === stSel.value);
      const kind = kSel.value;
      rWrap.style.display = kind === "choice" ? "" : "none";
      if (kind === "choice") {
        rSel.innerHTML = (stage?.deciding_roles || []).map(
          (r) => `<option value="${esc(r.role_id)}">${esc(r.role_name)} (${esc(r.role_id)})</option>`
        ).join("");
        if (p.role_id && (stage?.deciding_roles || []).some((r) => r.role_id === p.role_id)) rSel.value = p.role_id;
      }
      const stageHint = stage
        ? `上游阶段「${stage.title}」` : "上游阶段（该会话绑定剧本中的阶段）";
      if (kind === "next_stage") {
        // 跳转结局: 该阶段可解析到的 next 目标集合 + 结束（value=""）
        const targets = new Set();
        for (const r of stage?.deciding_roles || []) for (const o of r.options) {
          // options may carry next in the raw definition; script endpoint omits
          // it, so we only offer transitions/default targets below.
        }
        vSel.innerHTML = `<option value="">（演练结束 / next 为空）</option>`;
        for (const st2 of sc?.stages || []) {
          vSel.appendChild(el(`<option value="${esc(st2.id)}">跳转：${esc(st2.title)} (${esc(st2.id)})</option>`));
        }
        if (p.value !== undefined) vSel.value = p.value;
        hint.textContent = `${stageHint} 结算后解析出的下一阶段等于所选结局，分支即满足（超时/其他分支不匹配则不满足）。`;
      } else {
        const role = stage?.deciding_roles.find((r) => r.role_id === rSel.value);
        vSel.innerHTML = (role?.options || []).map(
          (o) => `<option value="${esc(o.id)}">${esc(o.text)} (${esc(o.id)})</option>`
        ).join("");
        if (p.value && (role?.options || []).some((o) => o.id === p.value)) vSel.value = p.value;
        hint.textContent = `${stageHint} 的所选角色在结算记录中确定提交了该选项（超时不满足）。`;
      }
    }
    sSel.onchange = () => { p.session_id = sSel.value; refreshStages(); };
    stSel.onchange = refreshKindRows;
    kSel.onchange = refreshKindRows;
    rSel.onchange = refreshKindRows;
    row.querySelector(".b-del").onclick = () => {
      const branches = row.parentElement;
      row.remove();
      if (!branches.children.length) branches.appendChild(emptyBranchHint());
    };
    (async () => {
      if (p.session_id) await refreshStages(p.stage_id);
      else await refreshStages();
      if (p.kind) kSel.value = p.kind;
      await refreshKindRows();
      if (p.role_id) rSel.value = p.role_id;
      if (p.value !== undefined) vSel.value = p.value;
    })();
    row._collect = () => {
      const out = {
        session_id: sSel.value,
        stage_id: stSel.value,
        kind: kSel.value,
      };
      if (kSel.value === "choice") out.role_id = rSel.value;
      out.value = vSel.value;
      return out;
    };
    return row;
  }
  function emptyBranchHint() {
    return el(`<p class="muted small empty-hint">至少一个前置分支；点右侧添加。</p>`);
  }

  function gateCard(prefill, frozen) {
    gateSeq += 1;
    const g = prefill || {};
    const card = el(`<div class="gate box ${frozen ? "frozen" : ""}">
      <div class="row">
        <strong>注入编排 ${frozen ? "（已锁定，不可修改）" : ""}</strong>
        <input type="hidden" class="g-id" value="${esc(g.id || `gate${gateSeq}`)}" />
        <span class="chip mono g-id-chip">${esc(g.id || `gate${gateSeq}`)}</span>
        ${frozen ? `<span class="chip ${g.status === "released" ? "ok" : "danger"}">${g.status === "released" ? "已投递" : "已错过"}</span>` : ""}
        <span class="spacer"></span>
        ${frozen ? "" : '<button class="ghost g-del" type="button">删除该编排</button>'}
      </div>
      <div class="row">
        <label style="margin:0">目标会话
          <select class="g-session"></select></label>
        <label style="margin:0">目标阶段（注入追加到此阶段）
          <select class="g-stage"></select></label>
      </div>
      <label>注入标题</label>
      <input type="text" class="g-title" value="${esc(g.title || "")}" placeholder="如：友邻厂区请求联动关阀" />
      <label>公开注入内容（目标会话全员可见）</label>
      <textarea class="g-public" rows="2" ${frozen ? "disabled" : ""}>${esc(g.public || "")}</textarea>`);
    const viewsHost = el(`<div class="g-views"></div>`);
    card.appendChild(el(`<label>按角色追加的私有注入（仅该角色可见，可留空）</label>`));
    card.appendChild(viewsHost);
    const branchesHead = el(`<div class="row" style="margin-top:8px">
      <strong>前置分支（全部满足才释放）</strong><span class="spacer"></span>
      ${frozen ? "" : '<button class="ghost b-add" type="button">＋ 添加前置分支</button>'}
    </div>`);
    card.appendChild(branchesHead);
    const branches = el(`<div class="branches"></div>`);
    card.appendChild(branches);

    const sSel = card.querySelector(".g-session");
    const stSel = card.querySelector(".g-stage");
    for (const sid of memberChecks()) {
      const s = memberSession(sid);
      sSel.appendChild(el(`<option value="${esc(sid)}">${esc(sid)} · ${esc(s.drill_name || s.drill_id)}</option>`));
    }
    sSel.value = g.target_session || sSel.value;

    function renderViewInputs(sc, keepValues) {
      viewsHost.innerHTML = "";
      for (const r of sc?.roles || []) {
        const val = keepValues && keepValues[r.id] ? keepValues[r.id] : "";
        viewsHost.appendChild(el(`<label class="small">仅 ${esc(r.name)} (${esc(r.id)}) 可见
          <textarea rows="2" class="g-view" data-role="${esc(r.id)}" ${frozen ? "disabled" : ""}>${esc(val)}</textarea></label>`));
      }
    }

    async function refreshTarget(keepStage) {
      stSel.innerHTML = "";
      const sc = await scriptFor(sSel.value);
      stSel.innerHTML = stageOptionsHtml(sc, keepStage);
      if (!keepStage) stSel.value = (sc.stages[0] || {}).id || "";
      renderViewInputs(sc, g.views || {});
    }
    sSel.onchange = () => refreshTarget();

    if (!frozen) {
      card.querySelector(".g-del").onclick = () => card.remove();
      card.querySelector(".b-add").onclick = () => {
        branches.querySelector(".empty-hint")?.remove();
        branches.appendChild(branchRow(null));
      };
    }
    const prefs = g.prerequisites || [];
    if (!prefs.length) branches.appendChild(emptyBranchHint());
    for (const p of prefs) branches.appendChild(branchRow(p, true));

    (async () => {
      await refreshTarget(g.target_stage);
      if (g.target_stage) stSel.value = g.target_stage;
      if (frozen) {
        sSel.disabled = stSel.disabled = true;
      }
    })();

    card._collect = () => ({
      id: card.querySelector(".g-id").value,
      target_session: sSel.value,
      target_stage: stSel.value,
      title: card.querySelector(".g-title").value,
      public: card.querySelector(".g-public").value,
      views: Object.fromEntries(
        [...card.querySelectorAll(".g-view")].map((t) => [t.dataset.role, t.value]).filter(([, v]) => v)
      ),
      prerequisites: [...branches.querySelectorAll(".branch-row")].map((r) => r._collect()),
    });
    if (frozen) {
      // Frozen gates are audit anchors: always submit the server-provided
      // definition verbatim, independent of DOM state.
      card._collect = () => ({
        id: g.id,
        target_session: g.target_session,
        target_stage: g.target_stage,
        title: g.title,
        public: g.public,
        views: g.views || {},
        prerequisites: (g.prerequisites || []).map((p) => ({
          session_id: p.session_id,
          stage_id: p.stage_id,
          kind: p.kind,
          role_id: p.role_id || "",
          value: p.value || "",
        })),
      });
    }
    return card;
  }

  for (const gg of detail?.gates || []) {
    const frozen = gg.status !== "waiting";
    if (frozen) frozenGates.add(gg.id);
    gatesBox.appendChild(gateCard(gg, frozen));
  }
  if (!detail) gatesBox.appendChild(el(`<p class="muted small" id="no-gate-hint">还没有编排，点上方“＋ 新增一条注入编排”。</p>`));
  root.querySelector("#add-gate").onclick = () => {
    root.querySelector("#no-gate-hint")?.remove();
    gatesBox.appendChild(gateCard(null, false));
  };

  // ---- save ---------------------------------------------------------------
  root.querySelector("#save-joint").onclick = async () => {
    const state = root.querySelector("#save-state");
    const payload = {
      name: root.querySelector("#j-name").value.trim(),
      description: root.querySelector("#j-desc").value,
      members: memberChecks(),
      gates: [...gatesBox.querySelectorAll(".gate")].map((c) => c._collect()),
    };
    if (!payload.name) return toast("请填写联演名称", "error");
    if (!payload.members.length) return toast("请至少选择一个参演会话", "error");
    if (!payload.gates.length) return toast("请至少添加一条注入编排", "error");
    for (const gx of payload.gates) {
      if (!gx.target_session || !gx.target_stage) return toast("每条编排都需要目标会话与目标阶段", "error");
      if (!gx.prerequisites.length) return toast(`编排 ${gx.id} 至少需要一个前置分支`, "error");
      for (const b of payload.gates.flatMap((g) => g.prerequisites)) {
        if (!b.session_id || !b.stage_id) return toast("前置分支需要上游会话与阶段", "error");
      }
    }
    state.textContent = "校验并保存中…";
    try {
      let saved;
      if (isNew) {
        saved = await api("/api/joints", { method: "POST", host: true, body: payload });
        toast("联合演练已保存，开始监控", "ok");
        location.hash = `#/host/joint/${saved.id}`;
      } else {
        saved = await api(`/api/joints/${encodeURIComponent(jid)}`, { method: "PUT", host: true, body: payload });
        toast("联演修改已保存", "ok");
        jointEditPage(jid);
      }
    } catch (e) {
      state.textContent = "";
      toast(e.message, "error");
    }
  };

  if (detail) {
    root.querySelector("#delete-joint").onclick = async () => {
      if (!confirm("删除该联合演练？已投递/已错过的联演不允许删除。")) return;
      try {
        await api(`/api/joints/${encodeURIComponent(jid)}`, { method: "DELETE", host: true });
        toast("已删除", "ok");
        location.hash = "#/host/joints";
      } catch (e) { toast(e.message, "error"); }
    };
    const monitor = root.querySelector("#monitor");
    async function poll() {
      try {
        const d = await api(`/api/joints/${encodeURIComponent(jid)}`, { host: true });
        renderMonitor(monitor, d);
      } catch { /* transient poll error: keep last render */ }
    }
    intervals.push(setInterval(poll, 2000));
    poll();
  }
}

function renderMonitor(box, j) {
  const memberMap = Object.fromEntries(j.members.map((m) => [m.session_id, m]));
  const summary = el(`<div class="row" style="margin-bottom:10px">
    <span class="chip ok">已投递 ${j.gates.filter((g) => g.status === "released").length}</span>
    <span class="chip warn">等待 ${j.gates.filter((g) => g.status === "waiting").length}</span>
    <span class="chip danger">错过 ${j.gates.filter((g) => g.status === "missed").length}</span>
  </div>`);
  const membersHtml = j.members.map((m) => `
    <div class="box">
      <div class="row">
        <strong class="mono">${esc(m.session_id)}</strong>
        <span class="muted small">${esc(m.drill_name)} <span class="chip mono">v${esc(m.drill_version)} · ${esc(m.digest_short)}</span></span>
        <span class="spacer"></span>
        <span class="chip ${m.status === "running" ? "ok" : m.status === "ended" ? "danger" : "warn"}">
          ${({ created: "未开始", running: "进行中", ended: "已结束" })[m.status] || m.status}</span>
        <a href="#/host/session/${esc(m.session_id)}" target="_blank"><button class="ghost" style="padding:3px 10px">主持台↗</button></a>
      </div>
      <div class="muted small">${m.current_stage
        ? `当前阶段：${esc(m.current_stage_title || m.current_stage)}`
        : "当前无开放阶段"} · 已结算阶段：${
          (m.sealed_stages || []).map((s) => `<code>${esc(s)}</code>`).join(" ") || "—"}</div>
    </div>`).join("");
  const membersCard = el(`<div><h3>参演会话</h3>${membersHtml}</div>`);

  const gatesHtml = j.gates.map((g, i) => {
    const statusChip = g.status === "released"
      ? '<span class="chip ok">已投递</span>'
      : g.status === "missed"
        ? '<span class="chip danger">已错过（窗口关闭，不补投）</span>'
        : g.ready
          ? '<span class="chip host">前置已齐 · 等待目标阶段开放</span>'
          : '<span class="chip warn">分支等待中</span>';
    const branches = g.prerequisites.map((p) => `
      <li class="${p.satisfied ? "branch-met" : "branch-waiting"}">
        <span>${p.satisfied ? "✅" : "⏳"}</span>
        <span>${esc(p.label)}</span>
        ${p.sealed ? `<span class="muted small">（v${p.sealed.version} 已达成）</span>` : ""}
      </li>`).join("");
    return `<div class="box gate-mon ${g.status}">
      <div class="row">
        <strong>#${i + 1} ${esc(g.title)}</strong>
        <span class="chip mono">${esc(g.id)}</span>
        ${statusChip}
        <span class="spacer"></span>
        <span class="muted small">→ 会话 <code>${esc(g.target_session)}</code> 阶段「${esc(g.target_stage_title)}」</span>
      </div>
      <ul class="branch-list">${branches}</ul>
      ${g.block_reason ? `<p class="block-reason">⛔ ${esc(g.block_reason)}</p>` : ""}
      ${g.status === "released"
        ? `<p class="muted small">已于 v${esc(g.event_version)} 作为 <code>joint_injection</code> 事件追加到目标会话事件日志（各会话独立记录）。</p>` : ""}
    </div>`;
  }).join("");
  box.innerHTML = "";
  box.append(summary, membersCard, el(`<h3>注入编排与依赖状态</h3>`), el(gatesHtml || '<p class="muted small">无编排。</p>'));
}

// --------------------------------------------------------------------------- //
// host: 联演因果时序复盘 (#/host/joint-review/<id>)
// --------------------------------------------------------------------------- //
const REVIEW_STATUS = {
  delivered: ["已投递", "ok"],
  missed: ["已错过", "danger"],
  awaiting_target: ["目标未开放", "host"],
  unsatisfied: ["尚未满足", "warn"],
};

async function jointReviewPage(jid) {
  app.innerHTML = `<div class="card"><h2>⏱ 联演因果时序复盘</h2><p class="muted">加载联演与成员会话…</p></div>`;
  let detail;
  try {
    detail = await api(`/api/joints/${encodeURIComponent(jid)}`, { host: true });
  } catch (e) {
    app.innerHTML = `<div class="card"><h2>联演因果时序复盘</h2>
      <p class="chip danger">${esc(e.message)}</p>
      <a href="#/host/joints"><button class="ghost">返回联演列表</button></a></div>`;
    return;
  }
  jid = detail.id;

  // The latest event tip per member (for cutoff/prefill defaults).
  const latestTip = Math.max(...detail.members.map((m) => m.last_event_at || 0), 0);

  const root = el(`<div>
    <div class="card">
      <div class="row">
        <h2 style="margin:0">⏱ ${esc(detail.name)}</h2>
        <span class="chip mono">${esc(detail.id)}</span>
        <span class="spacer"></span>
        <a href="#/host/joint/${esc(detail.id)}"><button class="ghost">← 返回监控台</button></a>
        <a href="#/host/joints"><button class="ghost">联演列表</button></a>
      </div>
      <p class="muted small">${esc(detail.description || "")}</p>
      <p class="muted small" style="margin-bottom:0">
        系统读取各成员会话<strong>冻结剧本版本</strong>与<strong>不可变事件日志</strong>，
        按事件时间、会话顺序号、版本/事件序号稳定合并；标出前置结局、分支命中、
        目标阶段开放、联演注入与阶段结算；沿分支链汇总关键路径与瓶颈。
      </p>
    </div>

    <div class="card">
      <h2>1. 选择复盘截至时刻</h2>
      <div class="row">
        <label class="small"><input type="radio" name="rvmode" value="as_of" checked />
          <strong>统一复盘截止时刻</strong></label>
        <label class="small"><input type="radio" name="rvmode" value="prefix" />
          <strong>各成员会话事件版本前缀</strong></label>
      </div>

      <div id="mode-asof">
        <label class="small">统一截止时刻（浏览器本地时区；仅统计严格早于该时刻的整版本提交，
          同秒与迟到事件不会计入，保证重复生成一致）</label>
        <div class="row">
          <input type="datetime-local" step="1" id="asof-input" style="max-width:260px" />
          <button class="ghost" id="asof-now">取最新事件之后</button>
          <span class="muted small mono" id="asof-epoch"></span>
        </div>
      </div>

      <div id="mode-prefix" hidden>
        <label class="small">为每个成员会话指定冻结的事件版本号前缀（0 = 仅创建后状态；
          前缀外的注入/结算不计入，可还原“当时能知道什么”）</label>
        <div id="prefix-box" class="joint-members"></div>
      </div>

      <div class="row" style="margin-top:10px">
        <button id="run-review">生成截至时刻报告</button>
        <span id="review-state" class="muted small"></span>
      </div>
    </div>

    <div id="report-slot"></div>
  </div>`);
  app.innerHTML = "";
  app.appendChild(root);

  // ---- mode toggle / inputs ----------------------------------------------
  const asofBox = root.querySelector("#mode-asof");
  const prefixBox = root.querySelector("#mode-prefix");
  root.querySelectorAll('input[name=rvmode]').forEach((r) => {
    r.onchange = () => {
      const mode = root.querySelector('input[name=rvmode]:checked').value;
      asofBox.hidden = mode !== "as_of";
      prefixBox.hidden = mode !== "prefix";
    };
  });

  // datetime-local <-> epoch helper
  const asofInput = root.querySelector("#asof-input");
  const epochLabel = root.querySelector("#asof-epoch");
  function epochFromInput() {
    const v = asofInput.value;
    if (!v) return null;
    // datetime-local has no tz suffix; Date interprets it as local time.
    const t = new Date(v).getTime() / 1000;
    return Number.isFinite(t) ? t : null;
  }
  function syncEpoch() {
    const t = epochFromInput();
    epochLabel.textContent = t == null ? "" : `POSIX ${t} 秒`;
  }
  asofInput.onchange = syncEpoch;
  asofInput.oninput = syncEpoch;
  root.querySelector("#asof-now").onclick = () => {
    const t = (latestTip || Date.now() / 1000) + 1;
    const d = new Date(t * 1000);
    const pad = (n) => String(n).padStart(2, "0");
    asofInput.value = `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`
      + `T${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
    syncEpoch();
  };
  // default: just after the latest event
  root.querySelector("#asof-now").click();

  // prefix inputs (one per member, default latest version)
  const pBox = root.querySelector("#prefix-box");
  const tbl = el(`<table><thead><tr>
    <th>会话顺序号</th><th>会话</th><th>剧本（冻结版本）</th><th>事件版本前缀</th>
  </tr></thead><tbody></tbody></table>`);
  detail.members.forEach((m, i) => {
    // Build rows via the table API: el() wraps markup in a <div>, whose
    // innerHTML parser would drop a bare <tr> and keep only its cells.
    const tr = tbl.querySelector("tbody").insertRow(-1);
    tr.innerHTML = `
      <td class="mono">#${i}</td>
      <td class="mono">${esc(m.session_id)} · ${esc(m.drill_name)}</td>
      <td><span class="chip mono">v${esc(m.drill_version)} · ${esc(m.digest_short)}</span>
        <span class="chip ${m.status === "running" ? "ok" : m.status === "ended" ? "danger" : "warn"}">
          ${({ created: "未开始", running: "进行中", ended: "已结束" })[m.status] || m.status}</span></td>
      <td><input type="number" min="0" step="1" class="prefix-in" data-sid="${esc(m.session_id)}"
         value="${m.last_version ?? 0}" style="max-width:110px" />
        <span class="muted small">/ ${m.last_version ?? 0}</span></td>`;
  });
  pBox.appendChild(tbl);

  let currentReport = null;
  function downloadReport() {
    if (!currentReport) { toast("请先生成复盘报告", "error"); return; }
    const blob = new Blob([JSON.stringify(currentReport, null, 2)], { type: "application/json" });
    const a = document.createElement("a");
    a.href = URL.createObjectURL(blob);
    a.download = `joint-review-${currentReport.joint_id}-${currentReport.mode}`
      + `-${currentReport.mode === "as_of" ? Math.floor(currentReport.as_of) : "prefix"}.json`;
    a.click();
    URL.revokeObjectURL(a.href);
    toast("报告 JSON 已下载", "ok");
  }

  // ---- run report ---------------------------------------------------------
  root.querySelector("#run-review").onclick = async () => {
    const state = root.querySelector("#review-state");
    const mode = root.querySelector('input[name=rvmode]:checked').value;
    let body;
    if (mode === "as_of") {
      const asOf = epochFromInput();
      if (asOf == null) { state.textContent = "请选择有效的截止时刻"; return; }
      body = { mode: "as_of", as_of: asOf };
    } else {
      const prefixes = {};
      for (const inp of pBox.querySelectorAll(".prefix-in")) {
        const v = Number(inp.value);
        if (!Number.isInteger(v) || v < 0) {
          state.textContent = `会话 ${inp.dataset.sid} 的前缀需是非负整数`;
          return;
        }
        prefixes[inp.dataset.sid] = v;
      }
      body = { mode: "prefix", prefixes };
    }
    state.textContent = "生成中…";
    try {
      currentReport = await api(`/api/joints/${encodeURIComponent(jid)}/postmortem`,
        { method: "POST", host: true, body });
      renderReviewReport(root.querySelector("#report-slot"), currentReport, downloadReport);
      state.textContent = "已生成（同一截至时刻重复生成结果一致）";
    } catch (e) {
      state.textContent = "";
      toast(e.message, "error");
    }
  };
}

// ---- review report rendering + host filters ------------------------------- //
function renderReviewReport(slot, rep, onDownload) {
  const s = rep.summary;
  const root = el(`<div>
    <div class="card">
      <div class="row">
        <h2 style="margin:0">2. 截至时刻报告</h2>
        <span class="chip ${rep.mode === "as_of" ? "host" : ""}">
          ${rep.mode === "as_of" ? "统一截止时刻" : "成员版本前缀"}</span>
        ${rep.mode === "as_of" ? `<span class="chip mono">${fmtTime(rep.as_of)}</span>` : ""}
        ${s.evidence_partial ? `<span class="chip warn">${s.evidence_partial} 条编排证据不完整</span>` : ""}
        <span class="spacer"></span>
        <button id="download-json">⬇ 下载报告 JSON</button>
      </div>
      <p class="muted small">${esc(rep.horizon_label)}</p>
      <div class="row" style="margin-top:8px">
        <span class="chip ok">已投递 ${s.delivered}</span>
        <span class="chip host">目标未开放 ${s.awaiting_target}</span>
        <span class="chip warn">尚未满足 ${s.unsatisfied}</span>
        <span class="chip danger">已错过 ${s.missed}</span>
        <span class="muted small">共 ${s.total} 条编排 · 合并事件 ${s.timeline_events} 个</span>
      </div>
      <div class="muted small" style="margin-top:6px">
        已投递注入汇合后等待合计 <b>${s.total_delivered_wait_seconds ?? 0}</b> 秒；
        最大汇合后等待 <b>${s.max_wait_seconds ?? "—"}</b> 秒；
        AND 汇合耗时合计 <b>${s.total_assembly_seconds ?? 0}</b> 秒。
      </div>
    </div>

    <div class="card">
      <h2>主持台筛选</h2>
      <div class="row">
        <label class="small">编排状态
          <select id="f-status">
            <option value="">全部</option>
            <option value="delivered">已投递</option>
            <option value="awaiting_target">目标未开放</option>
            <option value="unsatisfied">尚未满足</option>
            <option value="missed">已错过</option>
          </select></label>
        <label class="small">成员会话
          <select id="f-member"><option value="">全部</option>
            ${rep.members.map((m) => `<option value="${esc(m.session_id)}">
              #${m.ord} ${esc(m.session_id)} · ${esc(m.drill_name)}</option>`).join("")}
          </select></label>
        <label class="small">标注类型
          <select id="f-mark">
            <option value="">全部事件</option>
            <option value="prereq_outcome">前置结局</option>
            <option value="branch_hit">分支命中</option>
            <option value="branch_miss">分支不符</option>
            <option value="target_open">目标阶段开放</option>
            <option value="target_close">目标阶段结算</option>
            <option value="injection">联演注入</option>
            <option value="stage_settlement">阶段结算</option>
            <option value="session_end">会话结束</option>
          </select></label>
        <input type="text" id="f-text" placeholder="按编排/会话/引用关键字过滤" style="max-width:240px" />
        <span class="spacer"></span>
        <span id="f-count" class="muted small"></span>
      </div>
    </div>

    <div class="card" id="cp-card"><h2>3. 关键路径与瓶颈</h2><div id="cp-body"></div></div>
    <div class="card"><h2>4. 成员会话前缀</h2><div id="members-body"></div></div>
    <div class="card"><h2>5. 注入编排逐条复盘</h2><div id="gates-body"></div></div>
    <div class="card"><h2>6. 统一合并时间轴（因果时序）</h2>
      <p class="muted small">排序键：事件时间 → 会话顺序号 → 会话版本号 → 事件序号（稳定合并）。</p>
      <div id="timeline-body"></div>
    </div>
  </div>`);

  // download: the exact JSON the server produced (re-fetch-free, same bytes)
  root.querySelector("#download-json").onclick = () => onDownload();

  // critical path
  const cp = rep.critical_path;
  const cpBody = root.querySelector("#cp-body");
  if (cp.gates && cp.gates.length) {
    cpBody.appendChild(el(`<div class="box">
      <div class="row"><strong>关键路径（沿分支链累计等待）：</strong>
        ${cp.gates.map((g, i) =>
          `<span class="chip ${REVIEW_STATUS[g.status][1]}">${esc(g.title)}</span>`
          + (i < cp.gates.length - 1 ? '<span class="muted">→</span>' : "")).join("")}
        <span class="spacer"></span><span class="chip mono">累计 ${cp.total_wait_seconds ?? 0} 秒</span>
      </div>
      <table><thead><tr><th>编排</th><th>状态</th><th>汇合后等待(秒)</th>
        <th>AND 汇合(秒)</th><th>截至时等待中(秒)</th><th>瓶颈分支</th></tr></thead><tbody>
      ${cp.gates.map((g) => `<tr>
        <td>${esc(g.title)} <code>${esc(g.gate_id)}</code></td>
        <td>${esc(REVIEW_STATUS[g.status][0])}</td>
        <td class="mono">${g.wait_seconds ?? "—"}</td>
        <td class="mono">${g.assembly_seconds ?? "—"}</td>
        <td class="mono">${g.age_seconds ?? "—"}</td>
        <td>${g.bottleneck_branch ? esc(g.bottleneck_branch.label)
          + ` <code>${esc(g.bottleneck_branch.event_ref?.ref || "")}</code>` : "—"}</td>
      </tr>`).join("")}
      </tbody></table>
    </div>`));
  } else {
    cpBody.appendChild(el(`<p class="muted small">无已产生等待的链路。</p>`));
  }
  if (rep.bottleneck_gates && rep.bottleneck_gates.length) {
    cpBody.appendChild(el(`<div><strong>瓶颈编排排序：</strong>
      <ol class="small">${rep.bottleneck_gates.map((b) =>
        `<li><span class="chip ${REVIEW_STATUS[b.status][1]}">${esc(REVIEW_STATUS[b.status][0])}</span>
         ${esc(b.title)} <code>${esc(b.gate_id)}</code> — 权重 <b class="mono">${b.weight_seconds}</b> 秒
         （汇合 ${b.assembly_seconds ?? 0}s${b.wait_seconds != null ? `，汇合后 ${b.wait_seconds}s` : ""}）</li>`).join("")}
      </ol></div>`));
  }

  // members prefix
  root.querySelector("#members-body").innerHTML = `<table><thead><tr>
    <th>#</th><th>会话</th><th>剧本冻结版本</th><th>状态</th><th>当前阶段</th>
    <th>纳入版本</th><th>最新版本</th><th>纳入事件</th>
  </tr></thead><tbody>
  ${rep.members.map((m) => `<tr>
    <td class="mono">#${m.ord}</td>
    <td class="mono">${esc(m.session_id)}<div class="muted small">${esc(m.drill_name)}</div></td>
    <td><span class="chip mono">v${esc(m.drill_version)} · ${esc(m.digest_short)}</span></td>
    <td>${({ created: "未开始", running: "进行中", ended: "已结束" })[m.status] || m.status}</td>
    <td>${esc(m.current_stage_title || "—")}</td>
    <td class="mono">v${m.include_version}${m.include_version < m.latest_version
      ? ' <span class="chip warn">截断</span>' : ""}</td>
    <td class="mono">v${m.latest_version}</td>
    <td class="mono">${m.events_included}</td>
  </tr>`).join("")}</tbody></table>`;

  // gates
  const gatesBody = root.querySelector("#gates-body");
  rep.gates.forEach((g, i) => gatesBody.appendChild(reviewGateCard(g, i)));

  // timeline
  const tlBody = root.querySelector("#timeline-body");
  const tlRows = rep.timeline.map((e) => reviewTimelineRow(e, rep));
  tlBody.appendChild(el(`<div id="tl-list">${tlRows.map((r) => r.outerHTML).join("")}</div>`));

  // ---- filters ------------------------------------------------------------
  const fStatus = root.querySelector("#f-status");
  const fMember = root.querySelector("#f-member");
  const fMark = root.querySelector("#f-mark");
  const fText = root.querySelector("#f-text");
  const fCount = root.querySelector("#f-count");

  function applyFilters() {
    const st = fStatus.value, mem = fMember.value, mk = fMark.value;
    const q = fText.value.trim().toLowerCase();
    let gShown = 0;
    gatesBody.querySelectorAll(".review-gate").forEach((card) => {
      const ok = (!st || card.dataset.status === st)
        && (!mem || card.dataset.members.split(",").includes(mem))
        && (!q || card.dataset.text.includes(q));
      card.hidden = !ok;
      if (ok) gShown++;
    });
    let tShown = 0;
    tlBody.querySelectorAll(".tl-row").forEach((row) => {
      const marks = JSON.parse(row.dataset.marks || "[]");
      const ok = (!mem || row.dataset.session === mem)
        && (!mk || marks.includes(mk))
        && (!st || (row.dataset.gates ? JSON.parse(row.dataset.gates).some(
              (gg) => gg.status === st) : false))
        && (!q || row.dataset.text.includes(q));
      row.hidden = !ok;
      if (ok) tShown++;
    });
    fCount.textContent = `显示 ${gShown}/${rep.gates.length} 条编排 · ${tShown}/${rep.timeline.length} 个事件`;
  }
  [fStatus, fMember, fMark].forEach((x) => x.onchange = applyFilters);
  fText.oninput = applyFilters;
  applyFilters();

  slot.innerHTML = "";
  slot.appendChild(root);
}

function reviewGateCard(g, i) {
  const [sLabel, sCls] = REVIEW_STATUS[g.status] || [g.status, ""];
  const members = new Set([g.target_session, ...g.prerequisites.map((p) => p.session_id)]);
  const branches = g.prerequisites.map((p) => `
    <li class="${p.satisfied ? "branch-met" : "branch-waiting"}">
      <span>${p.satisfied ? "✅" : "⏳"}</span>
      <span>${esc(p.label)}</span>
      ${p.satisfied
        ? `<span class="muted small">命中于 ${fmtClock(p.satisfied_at)}
           <code>${esc(p.event_ref?.ref || "")}</code></span>`
        : (p.outcome_match === false
          ? `<span class="chip danger">结局不符</span>
             <span class="muted small">实际 next=<code>${esc(String(p.actual?.next))}</code>
             ${p.actual?.choice !== undefined ? `选择=<code>${esc(String(p.actual.choice))}</code>` : ""}
             <code>${esc(p.event_ref?.ref || "")}</code></span>`
          : '<span class="muted small">上游阶段尚未结算</span>')}
    </li>`).join("");
  const blockers = (g.active_blockers || []).map((b) =>
    `<li class="branch-waiting">⛔ ${esc(b.label)}${b.sealed_mismatch ? "（结局已确定不符）" : ""}
     <code>${esc(b.event_ref?.ref || "")}</code></li>`).join("");
  const card = el(`<div class="box review-gate ${esc(g.status)}" data-status="${esc(g.status)}"
      data-members="${esc([...members].join(","))}">
    <div class="row">
      <strong>#${i + 1} ${esc(g.title)}</strong>
      <span class="chip mono">${esc(g.id)}</span>
      <span class="chip ${sCls}">${sLabel}</span>
      ${g.evidence_partial ? '<span class="chip warn" title="目标前缀含注入，但授权前置在前缀之外">证据不完整</span>' : ""}
      <span class="spacer"></span>
      <span class="muted small">→ <code>${esc(g.target_session)}</code>
        阶段「${esc(g.target_stage_title)}」</span>
    </div>
    <ul class="branch-list">${branches}</ul>
    <div class="row small" style="margin-top:6px">
      <span class="chip mono">汇合后等待 ${g.wait_seconds ?? "—"} 秒</span>
      <span class="chip mono">AND 汇合 ${g.assembly_seconds ?? "—"} 秒</span>
      ${g.age_seconds != null ? `<span class="chip mono">截至时已等待 ${g.age_seconds} 秒</span>` : ""}
      <span class="spacer"></span>
      ${g.delivered_at ? `<span class="muted small">投递 ${fmtTime(g.delivered_at)}
        <code>${esc(g.injection_ref?.ref || "")}</code></span>` : ""}
      ${g.closed_at ? `<span class="muted small">窗口关闭 ${fmtTime(g.closed_at)}
        <code>${esc(g.close_ref?.ref || "")}</code></span>` : ""}
    </div>
    ${g.bottleneck_branch ? `<p class="muted small" style="margin:6px 0 0">
      汇合瓶颈分支：${esc(g.bottleneck_branch.label)}
      <code>${esc(g.bottleneck_branch.event_ref?.ref || "")}</code>
      ${g.bottleneck_branch.earlier_branch_lead_seconds.length
        ? `（其他分支提前 ${g.bottleneck_branch.earlier_branch_lead_seconds.join("、")} 秒）` : ""}
    </p>` : ""}
    ${blockers ? `<ul class="branch-list small">${blockers}</ul>` : ""}
    ${g.block_reason ? `<p class="block-reason">⛔ ${esc(g.block_reason)}</p>` : ""}
    ${g.critical_path && g.critical_path.gate_ids.length > 1 ? `<p class="muted small" style="margin-bottom:0">
      沿分支链关键路径：${g.critical_path.gate_ids.map((x) => `<code>${esc(x)}</code>`).join(" → ")}
      （累计 ${g.critical_path.cumulative_wait_seconds} 秒）</p>` : ""}
  </div>`);
  card.dataset.text = card.textContent.toLowerCase();
  return card;
}

function reviewTimelineRow(e, rep) {
  const markHtml = (e.marks || []).map((m) => {
    if (m.kind === "stage_settlement" || m.kind === "session_end") return "";
    const g = m.gate_id ? rep.gates.find((x) => x.id === m.gate_id) : null;
    const cls = {
      branch_hit: "ok", branch_miss: "danger", injection: "host",
      target_open: "", target_close: "warn", prereq_outcome: "",
    }[m.kind] || "";
    const kindLabel = {
      prereq_outcome: "前置结局", branch_hit: "分支命中", branch_miss: "分支不符",
      target_open: "目标开放", target_close: "目标结算", injection: "注入",
    }[m.kind] || m.kind;
    return `<span class="chip ${cls}">${kindLabel}${g ? " · " + esc(g.title) : ""}</span>`;
  }).join("");
  const member = rep.members[e.member_ord];
  const gateMarks = (e.marks || []).filter((m) => m.gate_id)
    .map((m) => rep.gates.find((x) => x.id === m.gate_id)).filter(Boolean);
  const row = el(`<div class="tl-row ${esc(e.type)}">
    <div class="row">
      <span class="mono small">${fmtTime(e.at)}</span>
      <span class="chip mono small" title="${esc(e.session_id)}">#${e.member_ord} ${esc(e.session_id.slice(0, 4))}…</span>
      <span class="muted small">${esc(member ? member.drill_name : "")}</span>
      <code class="small">v${e.version}.${e.seq} ${esc(e.type)}</code>
      <span class="spacer"></span><span class="small">${esc(e.label)}</span>
    </div>
    ${markHtml ? `<div class="tl-marks">${markHtml} <code class="muted small">${esc(e.event_ref)}</code></div>`
      : `<div class="tl-marks muted small"><code>${esc(e.event_ref)}</code></div>`}
  </div>`);
  row.dataset.session = e.session_id;
  row.dataset.marks = JSON.stringify((e.marks || []).map((m) => m.kind));
  row.dataset.gates = JSON.stringify(gateMarks.map((g) => ({ id: g.id, status: g.status })));
  row.dataset.text = row.textContent.toLowerCase();
  return row;
}

// --------------------------------------------------------------------------- //
// host: script versions & release management
// --------------------------------------------------------------------------- //
const STATUS_CHIP = {
  published: ["已发布", "ok"],
  draft: ["草稿", "warn"],
  history: ["历史版本", ""],
  superseded: ["已废弃草稿", ""],
};

async function drillVersionPage(drillId) {
  app.innerHTML = `<div class="card"><h2>剧本版本管理 <span class="mono">${esc(drillId)}</span></h2>
    <p class="muted">加载中…</p></div>`;

  let versions;
  try {
    versions = await api(`/api/drills/${encodeURIComponent(drillId)}/versions`, { host: true });
  } catch (e) {
    app.innerHTML = "";
    app.appendChild(el(`<div class="card"><h2>剧本版本管理</h2>
      <p class="chip danger">${esc(e.message)}</p>
      <a href="#/host"><button class="ghost">返回主持人控制台</button></a></div>`));
    return;
  }

  const published = versions.find((v) => v.status === "published");
  const draft = versions.find((v) => v.status === "draft");
  const selectable = [...versions].reverse();  // ascending for <select>

  const root = el(`<div>
    <div class="card">
      <div class="row">
        <h2 style="margin:0">版本概览</h2>
        <span class="spacer"></span>
        <a href="#/host"><button class="ghost">← 返回控制台</button></a>
      </div>
      <div class="row" style="margin-top:10px">
        ${published
          ? `<span class="chip ok">当前发布版 v${published.version}</span>
             <span class="chip mono" title="SHA-256 内容摘要">${esc(published.digest_short)}</span>`
          : '<span class="chip warn">尚无发布版（必须发布后才能创建会话）</span>'}
        ${draft
          ? `<span class="chip warn">开放草稿 v${draft.version}（基于 v${draft.base_version ?? "—"}）</span>
             <span class="chip mono">${esc(draft.digest_short)}</span>`
          : '<span class="muted small">当前无草稿</span>'}
      </div>
      <p class="muted small" style="margin-bottom:0">
        每个版本一经生成即<strong>不可变</strong>：保存为草稿 → 校对差异 → 发布；
        发布会校验基准版本未被他人更新，失败不会改变当前发布版。
        已创建/进行中的会话永久绑定其创建时的版本，后续编辑不影响其快照、分支与回放。
      </p>
    </div>
    <div class="card" id="editor-card">
      <h2>编辑草稿</h2>
      <div class="row">
        <button class="ghost" id="load-published">载入当前发布版 YAML</button>
        ${draft ? '<button class="ghost" id="load-draft">载入当前草稿 YAML</button>' : ""}
      </div>
      <textarea id="yaml-editor" placeholder="从上方载入 YAML，或直接粘贴新内容"></textarea>
      <div class="row" style="margin-top:8px">
        <button id="save-draft">保存为新草稿版本</button>
        <span class="muted small">基准版本：<b id="base-label">v${published ? published.version : "（新剧本）"}</b></span>
      </div>
    </div>
    <div class="card" id="diff-card">
      <h2>结构化差异对比</h2>
      <div class="row">
        <label style="margin:0">旧版
          <select id="diff-from"></select></label>
        <label style="margin:0">新版
          <select id="diff-to"></select></label>
        <button id="run-diff">对比</button>
      </div>
      <div id="diff-out" style="margin-top:12px"></div>
    </div>
    <div class="card">
      <h2>版本列表</h2>
      <div id="version-list"></div>
    </div>
    <div class="card" id="yaml-view" hidden></div>
  </div>`);

  const fillSelect = (sel, defVer) => {
    sel.innerHTML = selectable.map((v) =>
      `<option value="${v.version}" ${v.version === defVer ? "selected" : ""}>v${v.version} · ${esc(v.digest_short)} · ${esc(v.status)}</option>`
    ).join("");
  };
  fillSelect(root.querySelector("#diff-from"), published ? published.version : null);
  fillSelect(root.querySelector("#diff-to"), draft ? draft.version : (published ? published.version : null));

  // editor wiring
  const editor = root.querySelector("#yaml-editor");
  const setEditor = (text) => { editor.value = text; toast("YAML 已载入编辑器", "ok"); };
  root.querySelector("#load-published").onclick = async () => {
    if (!published) return toast("尚无发布版");
    const v = await api(`/api/drills/${drillId}/versions/${published.version}`, { host: true });
    setEditor(v.yaml);
  };
  const loadDraftBtn = root.querySelector("#load-draft");
  if (loadDraftBtn) loadDraftBtn.onclick = async () => {
    const v = await api(`/api/drills/${drillId}/versions/${draft.version}`, { host: true });
    setEditor(v.yaml);
  };
  root.querySelector("#save-draft").onclick = async () => {
    const base = published ? published.version : null;
    try {
      const res = await api("/api/drills", {
        method: "POST", host: true,
        body: { yaml: editor.value, base_version: base },
      });
      toast(res.reused
        ? `内容未变化，复用 v${res.version}`
        : `草稿 v${res.version} 已保存（${res.digest_short}）`, "ok");
      drillVersionPage(drillId);
    } catch (e) { toast(e.message, "error"); }
  };

  // diff wiring
  root.querySelector("#run-diff").onclick = async () => {
    const from = Number(root.querySelector("#diff-from").value);
    const to = Number(root.querySelector("#diff-to").value);
    const out = root.querySelector("#diff-out");
    if (from === to) { out.innerHTML = '<p class="muted small">同一版本无差异。</p>'; return; }
    out.innerHTML = '<p class="muted small">对比中…</p>';
    try {
      const diff = await api(
        `/api/drills/${encodeURIComponent(drillId)}/diff?from=${from}&to=${to}`,
        { host: true });
      out.innerHTML = "";
      out.appendChild(renderDiff(diff, from, to));
    } catch (e) { out.innerHTML = `<p class="chip danger">${esc(e.message)}</p>`; }
  };

  // version list
  const vl = root.querySelector("#version-list");
  for (const v of versions) {
    const [slabel, scls] = STATUS_CHIP[v.status] || [v.status, ""];
    const row = el(`<div class="box version-row">
      <div class="row">
        <strong class="mono">v${v.version}</strong>
        <span class="chip ${scls}">${slabel}</span>
        <span class="chip mono" title="SHA-256">${esc(v.digest_short)}</span>
        <span class="muted small">基准 v${v.base_version ?? "—"}</span>
        <span class="spacer"></span>
        <button class="ghost" data-act="yaml">查看 YAML</button>
        <button class="ghost" data-act="diff-prev">与上一版对比</button>
        ${v.status === "history" || v.status === "published" || v.status === "superseded"
          ? '<button class="ghost" data-act="copy">复制为新草稿</button>' : ""}
        ${v.status === "draft" ? '<button data-act="publish">▶ 发布此草稿</button>' : ""}
      </div>
      <div class="muted small">
        创建 ${fmtTime(v.created_at)}${v.published_at ? ` · 发布 ${fmtTime(v.published_at)}` : ""}
      </div>
    </div>`);

    row.querySelector('[data-act="yaml"]').onclick = () => showYaml(drillId, v, root);
    const prevBtn = row.querySelector('[data-act="diff-prev"]');
    prevBtn.onclick = async () => {
      const earlier = versions.find((x) => x.version === v.version - 1);
      if (!earlier) return toast("v1 没有上一版本可对比");
      root.querySelector("#diff-from").value = earlier.version;
      root.querySelector("#diff-to").value = v.version;
      root.querySelector("#run-diff").click();
      document.getElementById("diff-card").scrollIntoView({ behavior: "smooth" });
    };
    const copyBtn = row.querySelector('[data-act="copy"]');
    if (copyBtn) copyBtn.onclick = async () => {
      if (!confirm(`把 v${v.version} 的内容复制为新的可编辑草稿？（仍需发布后才生效）`)) return;
      try {
        const base = published ? published.version : null;
        const res = await api(`/api/drills/${encodeURIComponent(drillId)}/copy-draft`, {
          method: "POST", host: true,
          body: { source_version: v.version, base_version: base },
        });
        toast(res.reused
          ? `相同草稿已存在（v${res.version}）`
          : `已从 v${v.version} 复制为草稿 v${res.version}`, "ok");
        drillVersionPage(drillId);
      } catch (e) { toast(e.message, "error"); }
    };
    const pubBtn = row.querySelector('[data-act="publish"]');
    if (pubBtn) pubBtn.onclick = async () => {
      const base = v.base_version;
      if (!confirm(`确认发布草稿 v${v.version}（基于 v${base ?? "—"}）？发布后新会话将使用此版本。`)) return;
      try {
        await api(`/api/drills/${encodeURIComponent(drillId)}/versions/${v.version}/publish`, {
          method: "POST", host: true, body: { base_version: base },
        });
        toast(`v${v.version} 已发布`, "ok");
        drillVersionPage(drillId);
      } catch (e) {
        toast(`${e.message}。请刷新版本列表、重新复制/编辑后再发布。`, "error");
      }
    };
    vl.appendChild(row);
  }

  app.innerHTML = "";
  app.appendChild(root);
  if (draft) root.querySelector("#run-diff").click();
}

function showYaml(drillId, v, root) {
  const card = root.querySelector("#yaml-view");
  card.hidden = false;
  card.innerHTML = `<h2>v${v.version} 的 YAML（${esc(v.status)}）</h2>
    <pre class="yaml-pre">${esc(v.yaml || "（无原始 YAML）")}</pre>
    <div class="row"><button class="ghost" id="hide-yaml">收起</button></div>`;
  card.querySelector("#hide-yaml").onclick = () => { card.hidden = true; };
  card.scrollIntoView({ behavior: "smooth" });
}

// ---- structured diff rendering ----
function renderDiff(diff, fromV, toV) {
  const wrap = el(`<div>
    <div class="row">
      <strong>v${fromV} → v${toV}</strong>
      ${diff.has_changes
        ? `<span class="chip warn">有结构变化</span>`
        : '<span class="chip ok">内容相同（无结构差异）</span>'}
    </div>
    <div id="diff-body" class="diff-body"></div>
  </div>`);
  const body = wrap.querySelector("#diff-body");
  if (!diff.has_changes) return wrap;

  const addOldNew = (f) =>
    `<span class="diff-old">${esc(fmtVal(f.old))}</span>
     <span class="diff-arrow">→</span>
     <span class="diff-new">${esc(fmtVal(f.new))}</span>`;

  if (diff.meta.length) {
    body.appendChild(el(`<div class="diff-section">
      <h4>剧本属性</h4>
      ${diff.meta.map((f) => `<div class="diff-line"><b>${esc(f.label)}</b>：${addOldNew(f)}</div>`).join("")}
    </div>`));
  }

  const roles = diff.roles;
  if (roles.added.length || roles.removed.length || roles.changed.length) {
    body.appendChild(el(`<div class="diff-section"><h4>角色</h4>
      ${roles.added.map((r) => `<div class="diff-line diff-add">＋ 新增角色 ${esc(r.name)} <code>${esc(r.id)}</code>（邀请码 ${esc(r.invite)}）</div>`).join("")}
      ${roles.removed.map((r) => `<div class="diff-line diff-remove">－ 删除角色 ${esc(r.name)} <code>${esc(r.id)}</code></div>`).join("")}
      ${roles.changed.map((r) => `<div class="diff-line">✎ 角色 ${esc(r.name)}：
        ${r.fields.map((f) => `${esc(f.label)} ${addOldNew(f)}`).join("；")}</div>`).join("")}
    </div>`));
  }

  const stages = diff.stages;
  if (stages.added.length) {
    body.appendChild(el(`<div class="diff-section"><h4>新增阶段</h4>
      ${stages.added.map((s) => `<div class="diff-line diff-add">＋ ${esc(s.title)} <code>${esc(s.id)}</code></div>`).join("")}
    </div>`));
  }
  if (stages.removed.length) {
    body.appendChild(el(`<div class="diff-section"><h4>删除阶段</h4>
      ${stages.removed.map((s) => `<div class="diff-line diff-remove">－ ${esc(s.title)} <code>${esc(s.id)}</code></div>`).join("")}
    </div>`));
  }
  for (const s of stages.changed) {
    const sec = el(`<div class="diff-section">
      <h4>✎ 阶段 ${esc(s.title)} <code>${esc(s.id)}</code></h4></div>`);
    for (const f of s.fields) {
      sec.appendChild(el(`<div class="diff-line"><b>${esc(f.label)}</b>：${addOldNew(f)}</div>`));
    }
    // views
    for (const x of s.views.added) sec.appendChild(el(`<div class="diff-line diff-add">＋ 新增 ${esc(x.role_name)} 可见信息：${esc(x.text)}</div>`));
    for (const x of s.views.removed) sec.appendChild(el(`<div class="diff-line diff-remove">－ 删除 ${esc(x.role_name)} 可见信息：${esc(x.text)}</div>`));
    for (const x of s.views.changed) sec.appendChild(el(
      `<div class="diff-line">✎ ${esc(x.role_name)} 可见信息：
        <span class="diff-old">${esc(x.old)}</span><span class="diff-arrow">→</span>
        <span class="diff-new">${esc(x.new)}</span></div>`));
    // options
    for (const r of s.options.added_roles) {
      sec.appendChild(el(`<div class="diff-line diff-add">＋ 新增决策角色 ${esc(r.role_name)}：
        ${r.options.map((o) => `<code>${esc(o.id)}</code> ${esc(o.text)}${o.next ? `→${esc(o.next)}` : ""}`).join("；")}</div>`));
    }
    for (const r of s.options.removed_roles) {
      sec.appendChild(el(`<div class="diff-line diff-remove">－ 删除决策角色 ${esc(r.role_name)}</div>`));
    }
    for (const r of s.options.changed) {
      for (const o of r.added) sec.appendChild(el(`<div class="diff-line diff-add">＋ ${esc(r.role_name)} 新增选项 <code>${esc(o.id)}</code> ${esc(o.text)}${o.next ? ` → ${esc(o.next)}` : ""}</div>`));
      for (const o of r.removed) sec.appendChild(el(`<div class="diff-line diff-remove">－ ${esc(r.role_name)} 删除选项 <code>${esc(o.id)}</code> ${esc(o.text)}</div>`));
      for (const o of r.changed) sec.appendChild(el(`<div class="diff-line">✎ ${esc(r.role_name)} 选项 <code>${esc(o.id)}</code>：
        ${o.fields.map((f) => `${esc(f.label)} ${addOldNew(f)}`).join("；")}</div>`));
    }
    // transitions
    for (const t of s.transitions.added) sec.appendChild(el(
      `<div class="diff-line diff-add">＋ 新规则 当 ${fmtWhen(t.when)} → ${fmtNext(t.next)}</div>`));
    for (const t of s.transitions.removed) sec.appendChild(el(
      `<div class="diff-line diff-remove">－ 删规则 当 ${fmtWhen(t.when)} → ${fmtNext(t.next)}</div>`));
    for (const t of s.transitions.changed) sec.appendChild(el(
      `<div class="diff-line">✎ 规则 当 ${fmtWhen(t.new.when)}：跳转
        <span class="diff-old">${fmtNext(t.old.next)}</span><span class="diff-arrow">→</span>
        <span class="diff-new">${fmtNext(t.new.next)}</span></div>`));
    body.appendChild(sec);
  }
  return wrap;
}

function fmtVal(v) {
  if (v === null) return "（无 / 结束）";
  if (v === undefined) return "—";
  if (typeof v === "object") return JSON.stringify(v);
  return String(v);
}
function fmtWhen(when) {
  return Object.entries(when || {}).map(([k, v2]) => `${k}=${v2}`).join(" 且 ") || "（无条件）";
}
function fmtNext(n) { return n ? `<code>${esc(n)}</code>` : "（演练结束）"; }

// --------------------------------------------------------------------------- //
// host live console
// --------------------------------------------------------------------------- //
function hostSessionPage(sid) {
  app.innerHTML = `<div class="card"><h2>主持台 <span class="mono">${esc(sid)}</span></h2>
    <p id="connline" class="muted">正在建立实时连接…</p></div>`;
  let snapRef = null;

  liveHandle = new Live(sid, { host: HOST_KEY() }, {
    onStatus: (s, delay) => {
      const line = document.getElementById("connline");
      if (!line) return;
      if (s === "reconnecting") {
        line.innerHTML = `<span class="conn off">● 断开，${(delay / 1000).toFixed(0)}s 后自动重连…</span>`;
      } else if (s === "connecting") {
        line.innerHTML = `<span class="muted">● 连接中…</span>`;
      }
    },
    onSnapshot: (snap) => { snapRef = snap; renderHost(snap); },
  });

  function renderHost(snap) {
    clearTimers();
    const s = snap.session;
    const [lbl, cls] = statusChip(s.status);
    const banner = {
      created: ["wait", "会话尚未开始。参与者凭邀请码加入；就绪后开始演练。"],
      running: ["live", "演练进行中：服务端正向各客户端推送按角色裁剪的快照。"],
      ended: ["ended", `演练已结束（${esc(s.end_reason_label || s.end_reason || "")}）。可回放或导出记录。`],
    }[s.status];

    const root = el(`<div>
      <div class="banner ${banner[0]}">${banner[1]}</div>
      <div class="card">
        <div class="row">
          <h2 style="margin:0">控制台</h2>
          <span class="chip ${cls}">${lbl}</span>
          <span class="spacer"></span>
          <span class="conn on">● 已连接</span>
        </div>
        <div class="row" id="buttons" style="margin-top:12px"></div>
        <p class="muted small" style="margin-top:10px">
          会话单调版本号：<b class="mono">v${s.version}</b> ·
          绑定剧本版本：<b class="mono">v${esc(snap.drill.version)}</b>
          <span class="chip mono" title="SHA-256 内容摘要（会话创建时固化）">${esc(snap.drill.digest_short)}</span> ·
          服务端时间：<span id="srvclock"></span>
        </p>
      </div>
      <div class="card" id="people"></div>
      <div class="card" id="joint-panel" hidden>
        <div class="row"><h2 style="margin:0">🔗 联合演练状态</h2>
          <span class="spacer"></span>
          <a id="joint-link" href="#" target="_blank"><button class="ghost">打开联演监控台↗</button></a>
        </div>
        <div id="joint-body"></div>
      </div>
      <div class="card" id="stages"><h2>阶段（含各角色隐藏信息与联演注入）</h2></div>
      <div class="card" id="exports"></div>
    </div>`);

    const btns = root.querySelector("#buttons");
    if (s.status === "created") {
      const b = el(`<button>▶ 开始演练</button>`);
      b.onclick = () => liveHandle.send({ type: "start" });
      btns.appendChild(b);
    }
    if (s.status === "running") {
      const b1 = el(`<button class="warn">⏭ 立即结算当前阶段（未提交按超时）</button>`);
      b1.onclick = () => liveHandle.send({ type: "settle" });
      const b2 = el(`<button class="danger">■ 结束整场演练</button>`);
      b2.onclick = () => {
        if (confirm("确定结束整场演练？当前未提交的角色将按超时结算。"))
          liveHandle.send({ type: "end" });
      };
      btns.append(b1, b2);
    }
    btns.appendChild(el(`<a href="#/replay/${esc(sid)}"><button class="ghost">↺ 确定性回放</button></a>`));
    btns.appendChild(el(`<a href="#/host/drill/${encodeURIComponent(snap.drill.id)}"><button class="ghost">剧本版本管理</button></a>`));

    // people
    const people = root.querySelector("#people");
    people.innerHTML = `<h2>角色、邀请码与参与者</h2>`;
    const joined = Object.fromEntries(snap.participants.map((p) => [p.role_id, p]));
    const table = el(`<table><thead><tr>
      <th>角色</th><th>邀请码</th><th>参与者</th><th>连接</th><th>邀请链接</th></tr></thead><tbody></tbody></table>`);
    for (const r of snap.roles) {
      const p = joined[r.id];
      const link = `${location.origin}/#/join/${sid}?invite=${encodeURIComponent(r.invite)}`;
      const tr = addRow(table.querySelector("tbody"), `
        <td>${esc(r.name)}</td>
        <td class="mono">${esc(r.invite)}</td>
        <td>${p ? esc(p.display_name) : '<span class="muted">未加入</span>'}</td>
        <td>${p ? (p.online ? '<span class="conn on">●在线</span>' : '<span class="conn off">○离线</span>') : ""}</td>
        <td><div class="copybox"><code>${esc(link)}</code><button class="ghost">复制</button></div></td>`);
      tr.querySelector("button").onclick = () => copyText(link);
    }
    people.appendChild(table);

    // stages
    const stagesCard = root.querySelector("#stages");
    const curFirst = [...snap.history].sort((a, b) =>
      (a.stage_id === s.current_stage ? -1 : b.stage_id === s.current_stage ? 1 : 0));
    for (const stg of curFirst) stagesCard.appendChild(hostStageBlock(stg, snap));

    // exports
    const ex = root.querySelector("#exports");
    ex.innerHTML = `<h2>记录导出</h2>
      <p class="muted small">主持人全量记录包含隐藏信息、全部决定与事件日志，并标注本会话固化的
        <b>剧本版本 v${esc(snap.drill.version)}（${esc(snap.drill.digest_short)}）</b>；
        个人记录由参与者用本人令牌导出，已剔除其他角色的任何隐藏信息。</p>
      <div class="row">
        <a href="/api/sessions/${sid}/record?fmt=md" target="_blank"><button>全量记录 Markdown</button></a>
        <a href="/api/sessions/${sid}/record?fmt=json" target="_blank"><button class="ghost">全量记录 JSON</button></a>
      </div>`;

    app.innerHTML = "";
    app.appendChild(root);

    loadHostJointPanel(sid);

    const tick = () => {
      const clock = document.getElementById("srvclock");
      if (clock) clock.textContent = fmtClock(liveHandle.now());
    };
    intervals.push(setInterval(tick, 500));
    tick();
  }

  // Joint status panel embedded in a session's host console. Polls the joint
// read model (the live push channel stays per-session) and renders branch
// waiting state and blocking reasons.
async function loadHostJointPanel(sid) {
  try {
    const js = await api(`/api/sessions/${encodeURIComponent(sid)}/joints`, { host: true });
    if (!js || !js.length) return;
    const panel = document.getElementById("joint-panel");
    const body = document.getElementById("joint-body");
    if (!panel || !body) return;
    panel.hidden = false;
    document.getElementById("joint-link").href = `#/host/joint/${js[0].id}`;
    body.innerHTML = "";
    for (const j of js) {
      const box = el(`<div class="box">
        <div class="row"><strong>🔗 ${esc(j.name)}</strong>
          <span class="chip mono">${esc(j.id)}</span><span class="spacer"></span>
          <a href="#/host/joint/${esc(j.id)}" target="_blank"><button class="ghost" style="padding:3px 10px">完整监控台↗</button></a>
        </div>
        <div class="joint-mini"></div>
      </div>`);
      const mini = box.querySelector(".joint-mini");
      for (const g of j.gates) {
        const here = g.target_session === sid;
        const cls = g.status === "released" ? "ok" : g.status === "missed" ? "danger"
          : g.ready ? "host" : "warn";
        const label = g.status === "released" ? "已投递"
          : g.status === "missed" ? "已错过"
          : g.ready ? "前置已齐·等待阶段" : "等待分支";
        const waitList = g.prerequisites.filter((p) => !p.satisfied).map((p) => p.label);
        mini.appendChild(el(`<div class="branch-line">
          <span class="chip ${cls}">${label}</span>
          <strong>${esc(g.title)}</strong>
          <span class="muted small">${here ? "→ 注入到本会话阶段「" + esc(g.target_stage_title) + "」"
            : `→ 会话 ${esc(g.target_session)} 阶段「${esc(g.target_stage_title)}」`}</span>
          ${g.block_reason ? `<div class="block-reason">⛔ ${esc(g.block_reason)}</div>` : ""}
          ${waitList.length ? `<ul class="branch-list small">${waitList.map((t) => `<li class="branch-waiting">⏳ ${esc(t)}</li>`).join("")}</ul>` : ""}
        </div>`));
      }
      body.appendChild(box);
    }
  } catch { /* session not part of any joint: panel stays hidden */ }
}

function hostStageBlock(stg, snap) {
    const s = snap.session;
    const isCurrent = stg.stage_id === s.current_stage;
    const cls = isCurrent ? "current" : stg.kind === "sealed" ? "sealed" : "";
    const node = el(`<div class="stage ${cls}">
      <div class="row">
        <span class="stage-title">${esc(stg.title)}</span>
        <span class="chip mono">${esc(stg.stage_id)}</span>
        ${isCurrent ? '<span class="chip ok">当前阶段</span>' : ""}
        ${stg.kind === "sealed" ? `<span class="chip">${esc(stg.reason_label)}</span>` : ""}
        ${!stg.reached ? '<span class="chip warn">未到达</span>' : ""}
        <span class="spacer"></span>
        ${isCurrent && stg.duration ? '<span class="timer" id="host-timer">--</span>' : ""}
      </div>
      <div class="box public"><div class="box-label">公开信息（全员可见）</div>${esc(stg.public)}</div>
      <div class="views"></div>
      <div class="opts"></div>
      ${stg.kind === "sealed" ? `<div class="muted small">下一阶段：<b>${esc(stg.next || "（结束）")}</b></div>` : ""}
    </div>`);

    for (const [rid, text] of Object.entries(stg.views || {})) {
      node.querySelector(".views").appendChild(el(
        `<div class="box private"><div class="box-label">仅 ${esc(roleName(snap, rid))} 可见</div>${esc(text)}</div>`));
    }

    const injHost = node.querySelector(".inj-slot") || node.appendChild(el(`<div class="inj-slot"></div>`));
    for (const inj of stg.injections || []) {
      const block = el(`<div class="box injection">
        <div class="row"><span class="chip host">🔗 联演注入 · v${esc(inj.version)}</span>
          <strong>${esc(inj.title || "（无标题）")}</strong></div>
        <div class="box public"><div class="box-label">公开注入（全员可见）</div>${esc(inj.public)}</div>
        <div class="inj-views"></div>
      </div>`);
      for (const [rid, text] of Object.entries(inj.views || {})) {
        block.querySelector(".inj-views").appendChild(el(
          `<div class="box private"><div class="box-label">仅 ${esc(roleName(snap, rid))} 可见</div>${esc(text)}</div>`));
      }
      injHost.appendChild(block);
    }

    for (const rid of stg.deciding_roles || []) {
      const choice = stg.choices?.[rid];
      let stateHtml;
      if (!stg.reached) stateHtml = '<span class="muted">—</span>';
      else if (stg.kind === "sealed" && !choice) stateHtml = '<span class="chip danger">超时未提交</span>';
      else if (choice) stateHtml = `<span class="chip ok">已提交：${esc(choice.label || choice.id)}</span>`;
      else stateHtml = '<span class="chip warn">待提交</span>';

      const optsHtml = (stg.options?.[rid] || []).map((o) => `
        <div class="option ${choice && choice.id === o.id ? "chosen" : "disabled"}">
          <span class="oid">${esc(o.id)}</span>${esc(o.text)}
          ${o.next ? `<span class="muted small"> → ${esc(o.next)}</span>` : ""}
        </div>`).join("");

      node.querySelector(".opts").appendChild(el(`<div class="box">
        <div class="row"><strong>${esc(roleName(snap, rid))}</strong>${stateHtml}</div>
        ${optsHtml}
      </div>`));
    }

    if (isCurrent) {
      const cd = snap.current_detail;
      node.appendChild(el(`<p class="muted small">
        决策角色 ${cd.deciding_roles.length} 人，已提交 ${cd.submitted_roles.length} 人。
        全员提交即自动结算；到达截止时刻未提交者按超时进入条件分支。</p>`));
      if (stg.duration) {
        const bar = el(`<div class="timerbar" id="host-bar"><div style="width:100%"></div></div>`);
        node.appendChild(bar);
      }
    }
    return node;
  }

  // host countdown refresh
  intervals.push(setInterval(() => loadHostJointPanel(sid), 3000));
  setInterval(() => {
    if (!snapRef || snapRef.session.status !== "running") return;
    const s = snapRef.session;
    const t = document.getElementById("host-timer");
    const bar = document.getElementById("host-bar");
    if (t && s.deadline) {
      const remain = s.deadline - liveHandle.now();
      t.textContent = remain > 0 ? `${Math.ceil(remain)} 秒` : "结算中…";
      t.classList.toggle("urgent", remain <= 10);
      if (bar) {
        const cur = snapRef.history.find((x) => x.stage_id === s.current_stage);
        const pct = cur?.duration ? Math.max(0, remain / cur.duration) * 100 : 0;
        bar.firstElementChild.style.width = `${pct}%`;
        bar.classList.toggle("urgent", remain <= 10);
      }
    }
  }, 300);
}

// --------------------------------------------------------------------------- //
// join
// --------------------------------------------------------------------------- //
function joinPage(sid, params) {
  const existing = getCred(sid);
  app.innerHTML = "";
  const card = el(`<div class="card">
    <h2>加入演练 <span class="mono">${esc(sid)}</span></h2>
    <p class="muted">请输入主持人向您提供的角色邀请码。加入后该角色即与您绑定。</p>
    <label>角色邀请码</label>
    <input type="text" id="invite" value="${esc(params.get("invite") || "")}" placeholder="如 CMD-2026" />
    <label>您的姓名（可选，默认使用角色名）</label>
    <input type="text" id="name" placeholder="姓名 / 工号" />
    <div class="row" style="margin-top:14px">
      <button id="do-join">加入</button>
      ${existing ? `<button class="ghost" id="resume">以「${esc(existing.role_name)}」重新进入</button>` : ""}
    </div>
    <p id="jerr" class="chip danger hidden" style="margin-top:12px"></p>
  </div>`);
  app.appendChild(card);

  const showErr = (m) => {
    const p = card.querySelector("#jerr");
    p.textContent = m;
    p.classList.remove("hidden");
  };
  card.querySelector("#do-join").onclick = async () => {
    const invite = card.querySelector("#invite").value.trim();
    const name = card.querySelector("#name").value.trim();
    if (!invite) return showErr("请输入邀请码");
    try {
      const r = await api(`/api/sessions/${encodeURIComponent(sid)}/join`, {
        method: "POST", body: { invite, name },
      });
      setCred(sid, {
        token: r.token, role_id: r.role_id, role_name: r.role_name,
        display_name: r.display_name,
      });
      toast(`已加入：${r.role_name}`, "ok");
      location.hash = `#/play/${sid}`;
    } catch (e) { showErr(e.message); }
  };
  const resume = card.querySelector("#resume");
  if (resume) resume.onclick = () => { location.hash = `#/play/${sid}`; };
}

// --------------------------------------------------------------------------- //
// participant live console
// --------------------------------------------------------------------------- //
function playPage(sid) {
  const cred = getCred(sid);
  if (!cred) { location.hash = `#/join/${sid}`; return; }

  app.innerHTML = `<div class="card"><h2>演练控制台</h2>
    <p id="connline" class="muted">正在建立实时连接…</p></div>`;
  let snapRef = null;

  liveHandle = new Live(sid, { token: cred.token }, {
    onStatus: (s, delay) => {
      const line = document.getElementById("connline");
      if (!line) return;
      line.innerHTML = s === "reconnecting"
        ? `<span class="conn off">● 断线，${(delay / 1000).toFixed(0)}s 后自动重连（提交不会丢失）…</span>`
        : `<span class="muted">● 连接中…</span>`;
    },
    onSnapshot: (snap) => { snapRef = snap; renderPlay(snap); },
    onResult: (r) => {
      if (r.duplicate) toast("已提交过，本次为重复请求（幂等返回）", "ok");
      else if (r.accepted) toast("选择已提交", "ok");
      else if (r.reason === "deadline") toast("到达截止时刻，本阶段已结算", "error");
    },
  });

  function renderPlay(snap) {
    clearTimers();
    const s = snap.session;
    const me = snap.me;
    const [lbl] = statusChip(s.status);

    const root = el(`<div>
      <div class="card">
        <div class="row">
          <h2 style="margin:0">${esc(snap.drill.name)}</h2>
          <span class="spacer"></span>
          <span class="chip host">${esc(me.role_name)}</span>
          <span class="conn on">● 已连接</span>
        </div>
        <p class="muted small" style="margin:6px 0 0">
          会话 <span class="mono">${esc(s.id)}</span> · 状态：${lbl} ·
          会话版本 <b class="mono">v${s.version}</b> ·
          剧本 <b class="mono">v${esc(snap.drill.version)}</b>
          <span class="chip mono" title="内容摘要（创建时固化，后续编辑不影响本会话）">${esc(snap.drill.digest_short)}</span>
          · 服务端时间 <span id="srvclock"></span>
        </p>
      </div>
      <div id="banner-slot"></div>
      <div class="card" id="current"></div>
      <div class="card" id="history"><h2>已发生的阶段</h2></div>
      <div class="card" id="exports"></div>
    </div>`);

    const bannerSlot = root.querySelector("#banner-slot");
    if (s.status === "created") {
      bannerSlot.appendChild(el(`<div class="banner wait">演练尚未开始，请等待主持人开始。</div>`));
    } else if (s.status === "ended") {
      bannerSlot.appendChild(el(`<div class="banner ended">
        演练已结束（${esc(s.end_reason_label || "")}）。可在下方导出您的个人记录。</div>`));
    } else {
      bannerSlot.appendChild(el(`<div class="banner live">演练进行中。</div>`));
    }

    // current stage (the last block is current when kind == current)
    const curCard = root.querySelector("#current");
    const current = snap.history.find((x) => x.kind === "current");
    if (s.status === "running" && current) {
      curCard.innerHTML = "";
      curCard.appendChild(roleStageBlock(current, snap, true));
    } else {
      curCard.innerHTML = s.status === "created"
        ? "<h2>等待开始…</h2>"
        : "<h2>当前无开放阶段</h2>";
    }

    const hist = root.querySelector("#history");
    const sealed = snap.history.filter((x) => x.kind === "sealed");
    if (!sealed.length) {
      hist.innerHTML = `<h2>已发生的阶段</h2><p class="muted small">暂无。</p>`;
    } else {
      for (const stg of sealed) hist.appendChild(roleStageBlock(stg, snap, false));
    }

    // exports (visible always, most useful after end)
    const ex = root.querySelector("#exports");
    const tk = encodeURIComponent(cred.token);
    ex.innerHTML = `<h2>我的个人记录</h2>
      <p class="muted small">仅含公开信息与您本人的可见内容和选择，绝不含其他角色的隐藏信息。</p>
      <div class="row">
        <a href="/api/sessions/${sid}/record?fmt=md&token=${tk}" target="_blank"><button>导出 Markdown</button></a>
        <a href="/api/sessions/${sid}/record?fmt=json&token=${tk}" target="_blank"><button class="ghost">导出 JSON</button></a>
        <a href="#/replay/${sid}"><button class="ghost">我的回放</button></a>
        <span class="spacer"></span>
        <button class="ghost" id="leave">清除本地登录信息</button>
      </div>`;
    ex.querySelector("#leave").onclick = () => {
      localStorage.removeItem(credKey(sid));
      location.hash = "#/";
    };

    app.innerHTML = "";
    app.appendChild(root);

    const tick = () => {
      const clock = document.getElementById("srvclock");
      if (clock) clock.textContent = fmtClock(liveHandle.now());
      const t = document.getElementById("role-timer");
      const bar = document.getElementById("role-bar");
      if (t && snapRef.session.deadline) {
        const remain = snapRef.session.deadline - liveHandle.now();
        t.textContent = remain > 0 ? `剩余 ${Math.ceil(remain)} 秒` : "正在结算…";
        t.classList.toggle("urgent", remain <= 10);
        if (bar) {
          const dur = current?.duration || 0;
          bar.firstElementChild.style.width = `${dur ? Math.max(0, remain / dur) * 100 : 0}%`;
          bar.classList.toggle("urgent", remain <= 10);
        }
      }
    };
    intervals.push(setInterval(tick, 250));
    tick();
  }

  function roleStageBlock(stg, snap, isCurrent) {
    const node = el(`<div class="stage ${isCurrent ? "current" : "sealed"}">
      <div class="row">
        <span class="stage-title">${esc(stg.title)}</span>
        ${isCurrent ? '<span class="chip ok">当前阶段</span>' : ""}
        ${!isCurrent ? `<span class="chip">${esc(stg.reason_label)}</span>` : ""}
        <span class="spacer"></span>
        ${isCurrent && stg.duration ? '<span class="timer" id="role-timer">--</span>' : ""}
      </div>
      ${isCurrent && stg.duration ? '<div class="timerbar" id="role-bar"><div style="width:100%"></div></div>' : ""}
      <div class="box public"><div class="box-label">公开信息</div>${esc(stg.public)}</div>
      ${stg.private ? `<div class="box private"><div class="box-label">仅您可见</div>${esc(stg.private)}</div>` : ""}
      <div class="decision"></div>
      ${!isCurrent ? `<div class="muted small">阶段后续：<b>${esc(stg.next ? "进入下一阶段" : "演练结束")}</b></div>` : ""}
    </div>`);

    const dec = node.querySelector(".decision");
    if (stg.deciding) {
      const chosen = stg.my_choice;
      if (isCurrent) {
        if (chosen) {
          dec.appendChild(el(`<p><span class="chip ok">您已提交：${esc(stg.my_choice_label || chosen)}</span></p>`));
          dec.appendChild(el(`<p class="muted small">提交不可更改。如因断线重复发送，服务器幂等处理。</p>`));
        } else {
          dec.appendChild(el(`<p class="muted small">请在限时内选择（截止后未选按超时处理）：</p>`));
        }
      } else if (stg.timed_out) {
        dec.appendChild(el(`<p><span class="chip danger">您未在截止前提交（超时）</span></p>`));
      } else if (chosen) {
        dec.appendChild(el(`<p><span class="chip ok">您的决定：${esc(stg.my_choice_label || chosen)}</span></p>`));
      }

      for (const o of stg.options || []) {
        const btn = el(`<button class="option ${chosen === o.id ? "chosen" : ""} ${chosen ? "disabled" : ""}">
          <span class="oid">${esc(o.id)}</span>${esc(o.text)}</button>`);
        if (!chosen && isCurrent) {
          btn.onclick = () => {
            if (!confirm(`确认选择「${o.text}」？提交后不可更改。`)) return;
            liveHandle.send({
              type: "submit",
              choice_id: o.id,
              client_stage: stg.stage_id,
            });
            btn.disabled = true;
            toast("已发送，等待服务器确认…");
          };
        }
        dec.appendChild(btn);
      }
    } else if (isCurrent) {
      dec.appendChild(el(`<p class="muted small">本阶段您无需决策，请等待其他角色提交或主持人结算。</p>`));
    }

    for (const inj of stg.injections || []) {
      node.appendChild(el(`<div class="box injection">
        <div class="row"><span class="chip host">🔗 联演注入</span>
          <strong>${esc(inj.title || "（无标题）")}</strong></div>
        <div class="box public"><div class="box-label">联演公开信息</div>${esc(inj.public)}</div>
        ${inj.private ? `<div class="box private"><div class="box-label">联演注入 · 仅您可见</div>${esc(inj.private)}</div>` : ""}
      </div>`));
    }
    return node;
  }
}

// --------------------------------------------------------------------------- //
// deterministic replay
// --------------------------------------------------------------------------- //
async function replayPage(sid) {
  app.innerHTML = `<div class="card"><h2>确定性回放 <span class="mono">${esc(sid)}</span></h2>
    <p class="muted">正在加载事件日志…</p></div>`;

  const cred = getCred(sid);
  let data = null;
  let asRole = false;
  try {
    data = await api(`/api/sessions/${sid}/replay`, { host: true });
  } catch (e) {
    if (cred) {
      try {
        data = await fetch(`/api/sessions/${sid}/replay?token=${encodeURIComponent(cred.token)}`).then((r) => {
          if (!r.ok) throw new Error("无权重放");
          return r.json();
        });
        asRole = true;
      } catch { /* fallthrough */ }
    }
    if (!data) {
      app.innerHTML = "";
      app.appendChild(el(`<div class="card"><h2>回放</h2>
        <p class="chip danger">${esc(e.message)}</p>
        <p class="muted small">主持人请在首页输入正确的 HOST_KEY；参与者请先从邀请链接加入。</p></div>`));
      return;
    }
  }

  const frames = data.frames;
  let idx = frames.length - 1;

  const card = el(`<div class="card">
    <div class="row">
      <h2 style="margin:0">${esc(data.drill_name)}</h2>
      <span class="chip ${asRole ? "warn" : "host"}">${asRole ? "个人视角回放（已按角色裁剪）" : "主持人视角回放（全量）"}</span>
      <span class="spacer"></span>
      <span class="chip mono" title="会话创建时固化的剧本版本与内容摘要">
        剧本 v${esc(data.drill_version)} · ${esc((data.drill_sha256 || "").slice(0, 12))}
      </span>
      <span class="muted small">会话最终版本 v${data.final_version}</span>
    </div>
    <p class="muted small" style="margin:6px 0 12px">
      每个会话版本号对应一次原子提交。事件日志不可变，任意版本的快照都可被确定性重建；
      本回放始终使用会话绑定的剧本版本，不受后续剧本编辑影响。
    </p>
    <div id="frame"></div>
  </div>
  <div id="replay-bar">
    <button class="ghost" id="prev">◀</button>
    <input type="range" id="slider" min="0" max="${frames.length - 1}" value="${idx}" />
    <button class="ghost" id="next">▶</button>
    <span class="mono small" id="vlabel">v${idx}</span>
  </div>`);

  app.innerHTML = "";
  app.appendChild(card);

  function renderFrame() {
    const f = frames[idx];
    const wrap = card.querySelector("#frame");
    const changes = f.changes.length
      ? f.changes.map((c) => `<div class="logline">· ${fmtTime(c.at)} ${esc(c.label || c.type)}</div>`).join("")
      : '<div class="logline muted">· （会话已创建，尚未开始）</div>';

    const snap = f.snapshot;
    const s = snap.session;
    const hostStage = (stg) => {
      const choices = (stg.deciding_roles || []).map((rid) => {
        const c = stg.choices?.[rid];
        const text = !stg.reached ? "—"
          : stg.kind === "sealed" && !c ? "⏱ 超时"
          : c ? esc(c.label || c.id) : "待提交";
        return `<li>${esc(roleName(snap, rid))}：${text}</li>`;
      }).join("");
      const inj = (stg.injections || []).map((i) =>
        `<li>🔗 联演注入 v${esc(i.version)}：${esc(i.title)} — ${esc(i.public)}</li>`).join("");
      return `<div class="stage ${stg.kind}">
        <div class="stage-title">${esc(stg.title)} <span class="chip mono">${esc(stg.stage_id)}</span></div>
        <div class="box public">${esc(stg.public)}</div>
        ${choices ? `<ul class="small" style="margin:6px 0">${choices}</ul>` : ""}
        ${inj ? `<ul class="small" style="margin:6px 0">${inj}</ul>` : ""}
      </div>`;
    };
    const roleStage = (stg) => {
      let mine = "";
      if (stg.deciding) {
        mine = stg.timed_out ? "⏱ 超时未提交"
          : stg.my_choice ? esc(stg.my_choice_label || stg.my_choice) : "未提交";
      }
      const inj = (stg.injections || []).map((i) =>
        `<div class="box injection" style="margin:6px 0"><b>🔗 联演注入：${esc(i.title)}</b>
         <div>${esc(i.public)}</div>${i.private ? `<div>仅您可见：${esc(i.private)}</div>` : ""}</div>`
      ).join("");
      return `<div class="stage ${stg.kind}">
        <div class="stage-title">${esc(stg.title)}</div>
        <div class="box public">${esc(stg.public)}</div>
        ${stg.private ? `<div class="box private">${esc(stg.private)}</div>` : ""}
        ${mine ? `<div class="small">我的决定：${mine}</div>` : ""}
        ${inj}
      </div>`;
    };
    const stagesHtml = (snap.history || [])
      .map((stg) => (snap.kind === "host" ? hostStage(stg) : roleStage(stg)))
      .join("");

    wrap.innerHTML = `
      <h3>v${f.version} 的事件</h3>${changes}
      <h3 style="margin-top:16px">该版本时刻的${snap.kind === "host" ? "全量" : "角色"}快照
        <span class="chip ${s.status === "ended" ? "danger" : s.status === "running" ? "ok" : "warn"}">
          ${esc(statusChip(s.status)[0])}</span></h3>
      ${stagesHtml || '<p class="muted small">无</p>'}`;
    card.querySelector("#vlabel").textContent = `v${f.version} / v${data.final_version}`;
    card.querySelector("#slider").value = idx;
  }

  card.querySelector("#slider").oninput = (e) => { idx = Number(e.target.value); renderFrame(); };
  card.querySelector("#prev").onclick = () => { idx = Math.max(0, idx - 1); renderFrame(); };
  card.querySelector("#next").onclick = () => { idx = Math.min(frames.length - 1, idx + 1); renderFrame(); };
  renderFrame();
}

// boot
route();
