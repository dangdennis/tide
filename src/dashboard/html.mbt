// Embedded single-page dashboard UI.
// The entire HTML/CSS/JS is stored as a string constant so the binary
// needs no external assets — just serve this from GET /.

///|
pub const DASHBOARD_HTML : String =
  #|<!DOCTYPE html>
  #|<html lang="en">
  #|<head>
  #|<meta charset="UTF-8">
  #|<meta name="viewport" content="width=device-width, initial-scale=1.0">
  #|<title>tide</title>
  #|<style>
  #|*{box-sizing:border-box;margin:0;padding:0}
  #|body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#0f172a;color:#e2e8f0;min-height:100vh}
  #|header{background:#1e293b;border-bottom:1px solid #334155;padding:14px 24px;display:flex;align-items:center;gap:16px}
  #|header h1{font-size:1.2rem;font-weight:700;color:#38bdf8;letter-spacing:.5px}
  #|.queues{display:flex;gap:8px;margin-left:auto}
  #|.queue-btn{background:transparent;border:1px solid #475569;color:#94a3b8;padding:5px 14px;border-radius:6px;cursor:pointer;font-size:.8rem;transition:.15s}
  #|.queue-btn.active,.queue-btn:hover{background:#3b82f6;border-color:#3b82f6;color:#fff}
  #|.auto-refresh{margin-left:8px;display:flex;align-items:center;gap:6px;font-size:.8rem;color:#64748b}
  #|.auto-refresh input{cursor:pointer}
  #|.stats{display:flex;gap:12px;padding:16px 24px;background:#1e293b;border-bottom:1px solid #334155;flex-wrap:wrap}
  #|.stat{padding:8px 16px;border-radius:8px;cursor:pointer;font-size:.85rem;font-weight:600;transition:.15s;border:2px solid transparent}
  #|.stat:hover{filter:brightness(1.15)}
  #|.stat.active{border-color:#fff4}
  #|.stat span{display:block;font-size:1.4rem;font-weight:700;line-height:1.2}
  #|.s-available{background:#1d4ed8;color:#bfdbfe}
  #|.s-scheduled{background:#6d28d9;color:#ddd6fe}
  #|.s-executing{background:#b45309;color:#fde68a}
  #|.s-completed{background:#065f46;color:#a7f3d0}
  #|.s-retryable{background:#b45309;color:#fed7aa}
  #|.s-discarded{background:#991b1b;color:#fecaca}
  #|.s-cancelled{background:#374151;color:#d1d5db}
  #|main{display:flex;height:calc(100vh - 120px)}
  #|.job-list{flex:1;overflow-y:auto;padding:16px 24px}
  #|table{width:100%;border-collapse:collapse;font-size:.85rem}
  #|th{text-align:left;padding:8px 12px;color:#64748b;font-weight:600;border-bottom:1px solid #334155;white-space:nowrap}
  #|td{padding:9px 12px;border-bottom:1px solid #1e293b;vertical-align:top}
  #|tr:hover td{background:#1e293b;cursor:pointer}
  #|.badge{display:inline-block;padding:2px 8px;border-radius:99px;font-size:.75rem;font-weight:600}
  #|.tag{display:inline-block;padding:1px 6px;border-radius:4px;font-size:.72rem;background:#334155;color:#94a3b8;margin-right:3px}
  #|.mono{font-family:'SF Mono',Menlo,monospace;font-size:.8rem;color:#94a3b8}
  #|.empty{text-align:center;padding:60px;color:#475569}
  #|.panel{width:420px;background:#1e293b;border-left:1px solid #334155;overflow-y:auto;padding:20px;display:none;flex-shrink:0}
  #|.panel.open{display:block}
  #|.panel h2{font-size:1rem;font-weight:700;margin-bottom:16px;color:#e2e8f0}
  #|.panel .row{display:flex;margin-bottom:10px;gap:8px}
  #|.panel .label{color:#64748b;font-size:.8rem;min-width:110px;flex-shrink:0}
  #|.panel .val{font-size:.82rem;word-break:break-all}
  #|.panel pre{background:#0f172a;border-radius:6px;padding:10px;font-size:.78rem;white-space:pre-wrap;word-break:break-all;color:#7dd3fc;max-height:180px;overflow-y:auto}
  #|.panel .err-item{background:#450a0a;border-radius:4px;padding:6px 10px;margin-bottom:6px;font-size:.78rem;color:#fca5a5}
  #|.actions{display:flex;gap:8px;margin-top:20px;flex-wrap:wrap}
  #|.btn{padding:7px 16px;border-radius:6px;border:none;cursor:pointer;font-size:.82rem;font-weight:600;transition:.15s}
  #|.btn:hover{filter:brightness(1.15)}
  #|.btn-retry{background:#1d4ed8;color:#fff}
  #|.btn-cancel{background:#b45309;color:#fff}
  #|.btn-delete{background:#7f1d1d;color:#fff}
  #|.btn-close{background:#334155;color:#e2e8f0;margin-left:auto}
  #|.loading{opacity:.4;pointer-events:none}
  #|</style>
  #|</head>
  #|<body>
  #|<header>
  #|  <h1>&#9741; tide</h1>
  #|  <div class="queues" id="queueTabs"></div>
  #|  <label class="auto-refresh"><input type="checkbox" id="autoRefresh" checked> auto-refresh</label>
  #|</header>
  #|<div class="stats" id="statsBar"></div>
  #|<main>
  #|  <div class="job-list" id="jobList"><div class="empty">select a queue</div></div>
  #|  <div class="panel" id="panel"></div>
  #|</main>
  #|<script>
  #|const STATE_COLORS = {
  #|  available:'s-available', scheduled:'s-scheduled', executing:'s-executing',
  #|  completed:'s-completed', retryable:'s-retryable', discarded:'s-discarded', cancelled:'s-cancelled'
  #|};
  #|let currentQueue = null;
  #|let currentState = 'available';
  #|let refreshTimer = null;
  #|
  #|async function loadQueues() {
  #|  const res = await fetch('/api/queues');
  #|  const data = await res.json();
  #|  const tabs = document.getElementById('queueTabs');
  #|  tabs.innerHTML = '';
  #|  data.forEach(q => {
  #|    const b = document.createElement('button');
  #|    b.className = 'queue-btn' + (q.queue === currentQueue ? ' active' : '');
  #|    b.textContent = q.queue;
  #|    b.onclick = () => selectQueue(q.queue);
  #|    tabs.appendChild(b);
  #|  });
  #|  if (!currentQueue && data.length > 0) selectQueue(data[0].queue);
  #|  else if (currentQueue) renderStats(data.find(q => q.queue === currentQueue));
  #|}
  #|
  #|function renderStats(q) {
  #|  if (!q) return;
  #|  const bar = document.getElementById('statsBar');
  #|  const states = [
  #|    ['available', q.available], ['scheduled', q.scheduled], ['executing', q.executing]
  #|  ];
  #|  bar.innerHTML = states.map(([s, n]) =>
  #|    '<div class="stat ' + STATE_COLORS[s] + (s === currentState ? ' active' : '') + '" onclick="selectState(\'' + s + '\')">' +
  #|    '<span>' + n + '</span>' + s + '</div>'
  #|  ).join('');
  #|  ['completed','retryable','discarded','cancelled'].forEach(s => {
  #|    const div = document.createElement('div');
  #|    div.className = 'stat ' + STATE_COLORS[s] + (s === currentState ? ' active' : '');
  #|    div.onclick = () => selectState(s);
  #|    div.innerHTML = '<span>?</span>' + s;
  #|    bar.appendChild(div);
  #|  });
  #|}
  #|
  #|function selectQueue(q) {
  #|  currentQueue = q;
  #|  document.querySelectorAll('.queue-btn').forEach(b => b.classList.toggle('active', b.textContent === q));
  #|  loadStats();
  #|  loadJobs();
  #|}
  #|
  #|function selectState(s) {
  #|  currentState = s;
  #|  loadJobs();
  #|  document.querySelectorAll('.stat').forEach(el => {
  #|    el.classList.toggle('active', el.textContent.trim().endsWith(s));
  #|  });
  #|}
  #|
  #|async function loadStats() {
  #|  if (!currentQueue) return;
  #|  const res = await fetch('/api/queues');
  #|  const data = await res.json();
  #|  renderStats(data.find(q => q.queue === currentQueue));
  #|}
  #|
  #|async function loadJobs() {
  #|  if (!currentQueue) return;
  #|  const list = document.getElementById('jobList');
  #|  list.classList.add('loading');
  #|  try {
  #|    const url = '/api/jobs?queue=' + encodeURIComponent(currentQueue) + '&state=' + currentState + '&limit=100&offset=0';
  #|    const res = await fetch(url);
  #|    const jobs = await res.json();
  #|    renderTable(jobs);
  #|  } finally { list.classList.remove('loading'); }
  #|}
  #|
  #|function renderTable(jobs) {
  #|  const list = document.getElementById('jobList');
  #|  if (jobs.length === 0) { list.innerHTML = '<div class="empty">no jobs</div>'; return; }
  #|  let html = '<table><thead><tr><th>worker</th><th>state</th><th>attempt</th><th>priority</th><th>tags</th><th>inserted</th></tr></thead><tbody>';
  #|  jobs.forEach(j => {
  #|    const tags = (j.tags || []).map(t => '<span class="tag">' + esc(t) + '</span>').join('');
  #|    html += '<tr onclick="openJob(\'' + j.id + '\')">' +
  #|      '<td>' + esc(j.worker) + '</td>' +
  #|      '<td><span class="badge ' + STATE_COLORS[j.state] + '">' + esc(j.state) + '</span></td>' +
  #|      '<td>' + j.attempt + '/' + j.max_attempts + '</td>' +
  #|      '<td>' + j.priority + '</td>' +
  #|      '<td>' + tags + '</td>' +
  #|      '<td class="mono">' + relTime(j.inserted_at) + '</td>' +
  #|      '</tr>';
  #|  });
  #|  html += '</tbody></table>';
  #|  list.innerHTML = html;
  #|}
  #|
  #|async function openJob(id) {
  #|  const res = await fetch('/api/jobs/' + id);
  #|  if (!res.ok) return;
  #|  const j = await res.json();
  #|  const panel = document.getElementById('panel');
  #|  const canRetry = j.state === 'discarded' || j.state === 'cancelled';
  #|  const canCancel = j.state === 'available' || j.state === 'scheduled' || j.state === 'retryable';
  #|  const isTerminal = ['completed','discarded','cancelled'].includes(j.state);
  #|  const errors = Array.isArray(j.errors) ? j.errors : [];
  #|  let errHtml = errors.length ? errors.map(e => '<div class="err-item">' + esc(e) + '</div>').join('') : '<span style="color:#475569">none</span>';
  #|  panel.innerHTML =
  #|    '<div class="row"><div class="label">ID</div><div class="val mono">' + esc(j.id) + '</div></div>' +
  #|    '<div class="row"><div class="label">Worker</div><div class="val">' + esc(j.worker) + '</div></div>' +
  #|    '<div class="row"><div class="label">Queue</div><div class="val">' + esc(j.queue) + '</div></div>' +
  #|    '<div class="row"><div class="label">State</div><div class="val"><span class="badge ' + STATE_COLORS[j.state] + '">' + esc(j.state) + '</span></div></div>' +
  #|    '<div class="row"><div class="label">Attempt</div><div class="val">' + j.attempt + ' / ' + j.max_attempts + '</div></div>' +
  #|    '<div class="row"><div class="label">Priority</div><div class="val">' + j.priority + '</div></div>' +
  #|    '<div class="row"><div class="label">Inserted</div><div class="val mono">' + fmtTime(j.inserted_at) + '</div></div>' +
  #|    (j.attempted_at ? '<div class="row"><div class="label">Attempted</div><div class="val mono">' + fmtTime(j.attempted_at) + '</div></div>' : '') +
  #|    (j.completed_at ? '<div class="row"><div class="label">Completed</div><div class="val mono">' + fmtTime(j.completed_at) + '</div></div>' : '') +
  #|    (j.discarded_at ? '<div class="row"><div class="label">Discarded</div><div class="val mono">' + fmtTime(j.discarded_at) + '</div></div>' : '') +
  #|    (j.cancelled_at ? '<div class="row"><div class="label">Cancelled</div><div class="val mono">' + fmtTime(j.cancelled_at) + '</div></div>' : '') +
  #|    (j.tags && j.tags.length ? '<div class="row"><div class="label">Tags</div><div class="val">' + j.tags.map(t => '<span class="tag">' + esc(t) + '</span>').join(' ') + '</div></div>' : '') +
  #|    (j.attempted_by && j.attempted_by.length ? '<div class="row"><div class="label">Attempted by</div><div class="val mono">' + esc(j.attempted_by.join(', ')) + '</div></div>' : '') +
  #|    '<div class="label" style="margin-top:12px;margin-bottom:6px">Args</div><pre>' + esc(j.args || '{}') + '</pre>' +
  #|    '<div class="label" style="margin-top:12px;margin-bottom:6px">Errors</div>' + errHtml +
  #|    '<div class="actions">' +
  #|    (canRetry ? '<button class="btn btn-retry" onclick="doAction(\'' + id + '\',\'retry\')">Retry</button>' : '') +
  #|    (canCancel ? '<button class="btn btn-cancel" onclick="doAction(\'' + id + '\',\'cancel\')">Cancel</button>' : '') +
  #|    (isTerminal ? '<button class="btn btn-delete" onclick="doAction(\'' + id + '\',\'delete\')">Delete</button>' : '') +
  #|    '<button class="btn btn-close" onclick="closePanel()">Close</button>' +
  #|    '</div>';
  #|  panel.classList.add('open');
  #|}
  #|
  #|async function doAction(id, action) {
  #|  const method = action === 'delete' ? 'DELETE' : 'POST';
  #|  const url = action === 'delete' ? '/api/jobs/' + id : '/api/jobs/' + id + '/' + action;
  #|  await fetch(url, { method });
  #|  closePanel();
  #|  loadStats();
  #|  loadJobs();
  #|}
  #|
  #|function closePanel() { document.getElementById('panel').classList.remove('open'); }
  #|
  #|function esc(s) {
  #|  return String(s || '').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
  #|}
  #|
  #|function fmtTime(ms) {
  #|  if (!ms) return '';
  #|  return new Date(Number(ms)).toISOString().replace('T',' ').slice(0,19);
  #|}
  #|
  #|function relTime(ms) {
  #|  if (!ms) return '';
  #|  const s = Math.floor((Date.now() - Number(ms)) / 1000);
  #|  if (s < 60) return s + 's ago';
  #|  if (s < 3600) return Math.floor(s/60) + 'm ago';
  #|  if (s < 86400) return Math.floor(s/3600) + 'h ago';
  #|  return Math.floor(s/86400) + 'd ago';
  #|}
  #|
  #|function startRefresh() {
  #|  if (refreshTimer) clearInterval(refreshTimer);
  #|  if (document.getElementById('autoRefresh').checked) {
  #|    refreshTimer = setInterval(() => { loadStats(); loadJobs(); }, 5000);
  #|  }
  #|}
  #|
  #|document.getElementById('autoRefresh').onchange = startRefresh;
  #|loadQueues();
  #|startRefresh();
  #|</script>
  #|</body>
  #|</html>
