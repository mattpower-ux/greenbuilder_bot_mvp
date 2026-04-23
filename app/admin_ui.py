from __future__ import annotations

HTML = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>Green Builder Bot Editor Console</title>
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <style>
    body { font-family: Arial, sans-serif; margin: 0; background: #f8fafc; color: #0f172a; }
    header { background: #0f766e; color: white; padding: 16px 20px; display: flex; align-items: center; justify-content: space-between; gap: 12px; }
    .header-title { font-weight: 700; }
    .header-actions { display: flex; align-items: center; gap: 10px; flex-wrap: wrap; }
    .header-btn { background: #ffffff; color: #0f766e; border: 0; border-radius: 10px; padding: 9px 12px; cursor: pointer; font-weight: 700; }
    .header-btn.secondary { background: rgba(255,255,255,.18); color: #ffffff; border: 1px solid rgba(255,255,255,.55); }
    #rebuild-status { color: #ffffff; font-size: 13px; max-width: 320px; }
    main { max-width: 1100px; margin: 0 auto; padding: 20px; }
    .grid { display: grid; gap: 18px; grid-template-columns: 1.2fr .8fr; }
    .card { background: white; border-radius: 14px; box-shadow: 0 8px 24px rgba(15,23,42,.08); padding: 16px; }
    textarea, input, select { width: 100%; padding: 10px; border: 1px solid #cbd5e1; border-radius: 10px; box-sizing: border-box; }
    textarea { min-height: 120px; }
    button { background: #0f766e; color: white; border: 0; border-radius: 10px; padding: 10px 14px; cursor: pointer; }
    .log { border-top: 1px solid #e2e8f0; padding: 12px 0; }
    .muted { color: #475569; font-size: 13px; }
    .pill { display: inline-block; padding: 4px 8px; background: #e2e8f0; border-radius: 999px; font-size: 12px; margin-right: 6px; }
    .row { display: grid; gap: 8px; grid-template-columns: 1fr 1fr; }
    .notice { background: #ecfeff; border: 1px solid #a5f3fc; padding: 10px 12px; border-radius: 10px; margin-bottom: 12px; }
    @media (max-width: 760px) {
      header { align-items: flex-start; flex-direction: column; }
      .grid { grid-template-columns: 1fr; }
      .header-actions { width: 100%; }
    }
  </style>
</head>
<body>
<header>
  <div class="header-title">Green Builder Bot Editor Console</div>
  <div class="header-actions">
    <button id="rebuild-index-btn" class="header-btn">Rebuild Index</button>
    <button id="check-rebuild-status-btn" class="header-btn secondary">Check Status</button>
    <span id="rebuild-status">Index status: idle</span>
  </div>
</header>

<main>
  <div class="notice" id="prefillNotice" style="display:none"></div>
  <div class="grid">
    <section class="card">
      <h2>Recent chatbot answers</h2>
      <div id="logs"></div>
    </section>
    <aside class="card">
      <h2>Create or update a correction</h2>
      <div class="muted">Use exact match for one question, contains for a recurring topic, or regex for advanced patterns.</div>
      <div style="height:10px"></div>
      <label>Question pattern</label>
      <input id="question_pattern" />
      <div style="height:10px"></div>
      <div class="row">
        <div>
          <label>Match type</label>
          <select id="match_type"><option value="exact">exact</option><option value="contains">contains</option><option value="regex">regex</option></select>
        </div>
        <div>
          <label>Editor name</label>
          <input id="editor_name" placeholder="Editor initials or name" />
        </div>
      </div>
      <div style="height:10px"></div>
      <label>Corrected answer</label>
      <textarea id="answer_override"></textarea>
      <div style="height:10px"></div>
      <label>Optional note</label>
      <input id="editor_note" placeholder="Why this correction exists" />
      <div style="height:12px"></div>
      <button id="saveBtn">Save correction</button>
      <div id="status" class="muted" style="margin-top:10px"></div>
      <hr style="margin:18px 0;border:none;border-top:1px solid #e2e8f0" />
      <h3>Active corrections</h3>
      <div id="corrections"></div>
    </aside>
  </div>
</main>

<script>
function escapeHtml(s) {
  return (s || '').replace(/[&<>"]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));
}

async function rebuildIndex() {
  const statusEl = document.getElementById("rebuild-status");
  statusEl.textContent = "Starting rebuild...";

  try {
    const res = await fetch("/api/admin/rebuild-index", {
      method: "POST",
      credentials: "same-origin"
    });

    const data = await res.json();
    statusEl.textContent = data.message || "Rebuild started.";
    pollRebuildStatus();
  } catch (err) {
    statusEl.textContent = "Error starting rebuild: " + err.message;
  }
}

async function checkRebuildStatus() {
  const statusEl = document.getElementById("rebuild-status");

  try {
    const res = await fetch("/api/admin/rebuild-index-status", {
      method: "GET",
      credentials: "same-origin"
    });

    const data = await res.json();

    if (data.status === "running") {
      statusEl.textContent = "Index rebuild is running...";
    } else if (data.status === "completed") {
      statusEl.textContent = "Index rebuild completed.";
    } else if (data.status === "failed") {
      statusEl.textContent = "Index rebuild failed: " + (data.error || "Unknown error");
    } else {
      statusEl.textContent = "Index status: idle";
    }
  } catch (err) {
    statusEl.textContent = "Error checking status: " + err.message;
  }
}

function pollRebuildStatus() {
  const interval = setInterval(async () => {
    await checkRebuildStatus();
    const text = document.getElementById("rebuild-status").textContent || "";
    if (text.includes("completed") || text.includes("failed") || text.includes("idle")) {
      clearInterval(interval);
    }
  }, 5000);
}

function applyPrefillFromUrl() {
  const params = new URLSearchParams(window.location.search);
  const q = params.get('q') || '';
  const a = params.get('a') || '';
  const note = params.get('note') || '';
  if (q) {
    document.getElementById('question_pattern').value = q;
    document.getElementById('match_type').value = 'exact';
  }
  if (a) {
    document.getElementById('answer_override').value = a;
  }
  if (note) {
    document.getElementById('editor_note').value = note;
  }
  if (q || a) {
    const el = document.getElementById('prefillNotice');
    el.style.display = 'block';
    el.textContent = 'This form was pre-filled from the Fix this answer button. Review the text, edit it, and click Save correction.';
  }
}

async function loadLogs() {
  const res = await fetch('/api/admin/logs');
  const data = await res.json();
  const el = document.getElementById('logs');
  el.innerHTML = '';
  (data.logs || []).forEach((item) => {
    const div = document.createElement('div');
    div.className = 'log';
    div.innerHTML = `
      <div><strong>Q:</strong> ${escapeHtml(item.question || '')}</div>
      <div style="margin-top:6px"><strong>A:</strong> ${escapeHtml((item.answer || '').slice(0, 450))}</div>
      <div style="margin-top:8px" class="muted">${escapeHtml(item.created_at || '')}</div>
      <div style="margin-top:8px">
        ${(item.public_sources || []).map(src => `<span class="pill">${escapeHtml(src.title || 'Source')}</span>`).join('')}
        ${item.private_archive_used ? '<span class="pill">private archive used</span>' : ''}
      </div>
      <div style="margin-top:8px"><button data-q="${encodeURIComponent(item.question || '')}" data-a="${encodeURIComponent(item.answer || '')}" class="useBtn">Use as template</button></div>
    `;
    el.appendChild(div);
  });
  document.querySelectorAll('.useBtn').forEach(btn => {
    btn.addEventListener('click', () => {
      document.getElementById('question_pattern').value = decodeURIComponent(btn.dataset.q || '');
      document.getElementById('answer_override').value = decodeURIComponent(btn.dataset.a || '');
      document.getElementById('match_type').value = 'exact';
      window.scrollTo({ top: 0, behavior: 'smooth' });
    });
  });
}

async function loadCorrections() {
  const res = await fetch('/api/admin/corrections');
  const data = await res.json();
  const el = document.getElementById('corrections');
  el.innerHTML = '';
  (data.corrections || []).forEach((item) => {
    const div = document.createElement('div');
    div.className = 'log';
    div.innerHTML = `<div><strong>${escapeHtml(item.match_type)}:</strong> ${escapeHtml(item.question_pattern || '')}</div><div class="muted" style="margin-top:6px">${escapeHtml((item.answer_override || '').slice(0, 220))}</div>`;
    el.appendChild(div);
  });
}

document.getElementById('saveBtn').addEventListener('click', async () => {
  const payload = {
    question_pattern: document.getElementById('question_pattern').value,
    match_type: document.getElementById('match_type').value,
    answer_override: document.getElementById('answer_override').value,
    editor_name: document.getElementById('editor_name').value,
    editor_note: document.getElementById('editor_note').value,
  };
  const res = await fetch('/api/admin/corrections', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload)
  });
  const data = await res.json();
  document.getElementById('status').textContent = data.message || 'Saved';
  loadCorrections();
});

document.getElementById("rebuild-index-btn").addEventListener("click", rebuildIndex);
document.getElementById("check-rebuild-status-btn").addEventListener("click", checkRebuildStatus);

applyPrefillFromUrl();
loadLogs();
loadCorrections();
checkRebuildStatus();
</script>
</body>
</html>
"""
