from __future__ import annotations

HTML = r"""
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

    .upload-box { margin-top: 14px; padding: 12px; border: 1px dashed #99f6e4; border-radius: 12px; background: #f0fdfa; }
    .upload-row { display: flex; gap: 8px; align-items: center; flex-wrap: wrap; }
    .upload-row input[type="file"] { flex: 1; min-width: 220px; background: #fff; }
    #upload-status { margin-top: 8px; font-size: 13px; color: #475569; }
    .progress-wrap { margin-top: 10px; background: #ccfbf1; border-radius: 999px; overflow: hidden; height: 14px; border: 1px solid #99f6e4; }
    .progress-bar { width: 0%; height: 100%; background: #0f766e; transition: width .3s ease; }
    .file-dashboard { margin-top: 12px; display: grid; gap: 10px; grid-template-columns: repeat(4, 1fr); }
    .file-col { background: #ffffff; border: 1px solid #dbeafe; border-radius: 10px; padding: 10px; min-height: 80px; }
    .file-col h4 { margin: 0 0 6px; font-size: 14px; }
    .file-item { border-top: 1px solid #e2e8f0; padding: 6px 0; font-size: 12px; overflow-wrap: anywhere; }
    .disk-line { margin-top: 8px; font-size: 12px; color: #475569; }
    @media (max-width: 900px) { .file-dashboard { grid-template-columns: 1fr 1fr; } }
    @media (max-width: 560px) { .file-dashboard { grid-template-columns: 1fr; } }
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

  <section class="card upload-box">
    <h2 style="margin-top:0">Upload magazine PDF(s)</h2>
    <div class="muted">Uploads PDF files safely to <strong>/data/pdf_inbox</strong>. They will not be ingested until you click <strong>Ingest PDFs</strong>.</div>
    <div style="height:10px"></div>
    <div class="upload-row">
      <input type="file" id="magazine-file" accept="application/pdf" multiple />
      <button id="upload-magazine-btn">Upload PDF</button>
      <button id="ingest-pdf-inbox-btn" type="button">Ingest PDFs</button>
      <button id="check-ingest-status-btn" type="button">Check Ingest Status</button>
    </div>
    <div id="upload-status">No file uploaded yet. Safe upload mode is ON.</div>
    <div class="progress-wrap" aria-label="PDF ingest progress"><div id="ingest-progress-bar" class="progress-bar"></div></div>
    <div id="disk-status" class="disk-line">Disk status not checked yet.</div>
    <div style="margin-top:10px; padding:10px; border:1px solid #fde68a; border-radius:10px; background:#fffbeb;">
      <strong>Clean unused PDFs</strong>
      <div class="muted" style="margin-top:4px">Preview and delete PDFs in <strong>/data/magazines</strong> only if they are not indexed and not waiting in the inbox. Indexed PDFs are kept so chatbot download links continue to work.</div>
      <div style="height:8px"></div>
      <button id="preview-unused-pdfs-btn" type="button">Preview Unused PDFs</button>
      <button id="clean-unused-pdfs-btn" type="button">Delete Previewed Unused PDFs</button>
      <div id="cleanup-status" class="muted" style="margin-top:8px">No cleanup preview yet.</div>
      <div id="cleanup-list" class="muted" style="margin-top:8px"></div>
    </div>
    <div class="file-dashboard">
      <div class="file-col"><h4>Inbox</h4><div id="inbox-files" class="muted">Loading...</div></div>
      <div class="file-col"><h4>Processing</h4><div id="processing-files" class="muted">Loading...</div></div>
      <div class="file-col"><h4>Done</h4><div id="done-files" class="muted">Loading...</div></div>
      <div class="file-col"><h4>Failed</h4><div id="failed-files" class="muted">Loading...</div></div>
    </div>
  </section>
  <div style="height:18px"></div>
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

async function fetchJson(url, options = {}) {
  const res = await fetch(url, {
    credentials: "same-origin",
    ...options
  });

  const rawText = await res.text();
  let data = {};

  try {
    data = rawText ? JSON.parse(rawText) : {};
  } catch (e) {
    throw new Error(`Server did not return JSON (${res.status}): ${rawText.slice(0, 400)}`);
  }

  return { res, data, rawText };
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

    const rawText = await res.text();
    let data = {};

    try {
      data = JSON.parse(rawText);
    } catch (e) {
      statusEl.textContent = "Ingest status failed. Server did not return JSON: " + rawText.slice(0, 400);
      return;
    }

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
refreshPDFDashboard();
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


function renderFileList(elId, files, emptyText) {
  const el = document.getElementById(elId);
  const list = files || [];
  if (!list.length) {
    el.innerHTML = `<span class="muted">${escapeHtml(emptyText)}</span>`;
    return;
  }
  el.innerHTML = list.map(file => `
    <div class="file-item">
      <strong>${escapeHtml(file.name || '')}</strong><br />
      <span class="muted">${escapeHtml(String(file.size_mb ?? ''))} MB</span>
    </div>
  `).join('');
}

function updateProgressBar(status) {
  const bar = document.getElementById("ingest-progress-bar");
  const processed = Number(status?.processed || 0);
  const total = Number(status?.total || 0);
  const pct = total > 0 ? Math.min(100, Math.round((processed / total) * 100)) : 0;
  bar.style.width = pct + "%";
}

async function refreshPDFDashboard() {
  try {
    const res = await fetch("/admin/pdf-inbox-status", {
      method: "GET",
      credentials: "same-origin"
    });
    const data = await res.json();

    renderFileList("inbox-files", data.inbox, "No PDFs waiting.");
    renderFileList("processing-files", data.processing, "Nothing processing.");
    renderFileList("done-files", data.done, "No done markers yet.");
    renderFileList("failed-files", data.failed, "No failures.");

    if (data.disk) {
      document.getElementById("disk-status").textContent =
        `Disk: ${data.disk.free_gb} GB free of ${data.disk.total_gb} GB on /data`;
    }

    updateProgressBar(data.status || {});
  } catch (err) {
    document.getElementById("disk-status").textContent = "Error loading PDF dashboard: " + err.message;
  }
}

async function uploadMagazinePDF() {
  const fileInput = document.getElementById("magazine-file");
  const statusEl = document.getElementById("upload-status");
  const files = fileInput.files;

  if (!files || files.length === 0) {
    statusEl.textContent = "Choose one or more PDFs first.";
    return;
  }

  const formData = new FormData();

  for (const file of files) {
    if (!file.name.toLowerCase().endsWith(".pdf")) {
      statusEl.textContent = "Only PDF files are allowed.";
      return;
    }
    formData.append("files", file);
  }

  statusEl.textContent = "Uploading " + files.length + " PDF(s)...";

  try {
    const res = await fetch("/admin/upload-magazine", {
      method: "POST",
      credentials: "same-origin",
      body: formData
    });

    const rawText = await res.text();
    let data = {};

    try {
      data = JSON.parse(rawText);
    } catch (e) {
      statusEl.textContent = "Upload failed. Server did not return JSON: " + rawText.slice(0, 400);
      return;
    }

    if (!res.ok || data.ok === false) {
      statusEl.textContent = "Upload failed: " + (data.error || data.detail || data.message || "Unknown error");
      return;
    }

    statusEl.textContent = data.message || "Upload complete. PDFs are waiting safely in /data/pdf_inbox.";
    fileInput.value = "";
    refreshPDFDashboard();
  } catch (err) {
    statusEl.textContent = "Upload error: " + err.message;
  }
}

async function ingestPDFInbox() {
  const statusEl = document.getElementById("upload-status");
  statusEl.textContent = "Starting PDF ingest from /data/pdf_inbox...";

  try {
    const res = await fetch("/admin/ingest-pdf-inbox", {
      method: "POST",
      credentials: "same-origin"
    });

    const rawText = await res.text();
    let data = {};

    try {
      data = JSON.parse(rawText);
    } catch (e) {
      statusEl.textContent = "Ingest failed. Server did not return JSON: " + rawText.slice(0, 400);
      return;
    }

    if (!res.ok || data.ok === false) {
      statusEl.textContent = "Ingest failed: " + (data.error || data.detail || data.message || "Unknown error");
      return;
    }

    statusEl.textContent = data.message || "PDF ingest started.";
    refreshPDFDashboard();
    pollMagazineIngestStatus();
  } catch (err) {
    statusEl.textContent = "Ingest error: " + err.message;
  }
}

async function checkMagazineIngestStatus() {
  const statusEl = document.getElementById("upload-status");

  try {
    const res = await fetch("/admin/magazine-ingest-status", {
      method: "GET",
      credentials: "same-origin"
    });

    const data = await res.json();
    updateProgressBar(data || {});

    if (data.status === "running") {
      statusEl.textContent = `${data.message || "Ingest running"} — ${data.processed || 0}/${data.total || 0} processed`;
    } else if (data.status === "completed") {
      statusEl.textContent = data.message || "Magazine ingest completed.";
    } else if (data.status === "completed_with_errors") {
      statusEl.textContent = data.message || "Magazine ingest completed with errors.";
    } else {
      statusEl.textContent = data.message || "No magazine ingest is running.";
    }
  } catch (err) {
    statusEl.textContent = "Error checking ingest status: " + err.message;
  }
}

function pollMagazineIngestStatus() {
  const interval = setInterval(async () => {
    await checkMagazineIngestStatus();
    await refreshPDFDashboard();
    const text = document.getElementById("upload-status").textContent || "";

    if (
      text.includes("completed") ||
      text.includes("errors") ||
      text.includes("No magazine ingest")
    ) {
      clearInterval(interval);
    }
  }, 10000);
}

async function previewUnusedPDFs() {
  const statusEl = document.getElementById("cleanup-status");
  const listEl = document.getElementById("cleanup-list");
  statusEl.textContent = "Checking for unused PDFs...";
  listEl.innerHTML = "";

  try {
    const { res, data } = await fetchJson("/admin/unused-pdf-preview", {
      method: "GET"
    });

    if (!res.ok || data.ok === false) {
      statusEl.textContent = "Preview failed: " + (data.error || data.detail || data.message || "Unknown error");
      return;
    }

    const unused = data.unused || [];
    statusEl.textContent = data.message || `Found ${unused.length} unused PDF(s).`;

    if (!unused.length) {
      listEl.innerHTML = "<span class='muted'>Nothing to delete.</span>";
      return;
    }

    listEl.innerHTML = unused.map(file => `
      <div class="file-item">
        <strong>${escapeHtml(file.name || '')}</strong><br />
        <span class="muted">${escapeHtml(String(file.size_mb ?? ''))} MB</span>
      </div>
    `).join('');
  } catch (err) {
    statusEl.textContent = "Preview error: " + err.message;
  }
}

async function cleanUnusedPDFs() {
  const statusEl = document.getElementById("cleanup-status");
  const listEl = document.getElementById("cleanup-list");

  const ok = window.confirm(
    "Delete unused PDFs from /data/magazines? This will NOT delete indexed PDFs used by the chatbot and will NOT delete PDFs waiting in /data/pdf_inbox."
  );
  if (!ok) {
    statusEl.textContent = "Cleanup cancelled.";
    return;
  }

  statusEl.textContent = "Deleting unused PDFs...";

  try {
    const { res, data } = await fetchJson("/admin/clean-unused-pdfs", {
      method: "POST"
    });

    if (!res.ok || data.ok === false) {
      statusEl.textContent = "Cleanup failed: " + (data.error || data.detail || data.message || "Unknown error");
      return;
    }

    statusEl.textContent = data.message || "Cleanup complete.";
    listEl.innerHTML = "";
    refreshPDFDashboard();
  } catch (err) {
    statusEl.textContent = "Cleanup error: " + err.message;
  }
}

function bindClick(id, fn) {
  const el = document.getElementById(id);
  if (el) el.addEventListener("click", fn);
}

bindClick('saveBtn', async () => {
  const payload = {
    question_pattern: document.getElementById('question_pattern').value,
    match_type: document.getElementById('match_type').value,
    answer_override: document.getElementById('answer_override').value,
    editor_name: document.getElementById('editor_name').value,
    editor_note: document.getElementById('editor_note').value,
  };
  const res = await fetch('/api/admin/corrections', {
    method: 'POST',
    credentials: 'same-origin',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload)
  });
  const data = await res.json();
  document.getElementById('status').textContent = data.message || 'Saved';
  loadCorrections();
});

bindClick("upload-magazine-btn", uploadMagazinePDF);
bindClick("ingest-pdf-inbox-btn", ingestPDFInbox);
bindClick("check-ingest-status-btn", checkMagazineIngestStatus);
bindClick("preview-unused-pdfs-btn", previewUnusedPDFs);
bindClick("clean-unused-pdfs-btn", cleanUnusedPDFs);
bindClick("rebuild-index-btn", rebuildIndex);
bindClick("check-rebuild-status-btn", checkRebuildStatus);

applyPrefillFromUrl();
loadLogs();
loadCorrections();
checkRebuildStatus();
refreshPDFDashboard();
</script>
</body>
</html>
"""
