(function () {
  const currentScript = document.currentScript;
  const apiBase = (currentScript?.dataset.apiBase || '').replace(/\/$/, '');
  const title = currentScript?.dataset.chatbotTitle || 'Ask Green Builder';
  const editorTools = (currentScript?.dataset.editorTools || '').toLowerCase() === 'true' || new URLSearchParams(window.location.search).get('gbmEditor') === '1';
  if (!apiBase) {
    console.error('Green Builder widget requires data-api-base.');
    return;
  }

  const root = document.createElement('div');
  root.id = 'gbm-chatbot-root';
  document.body.appendChild(root);

  root.innerHTML = `
    <style>
      #gbm-chatbot-root * { box-sizing: border-box; font-family: Arial, sans-serif; }
      .gbm-launcher {
        position: fixed; bottom: 20px; right: 20px; z-index: 999999;
        background: #0f766e; color: #fff; border: 0; border-radius: 999px;
        padding: 14px 18px; cursor: pointer; box-shadow: 0 8px 24px rgba(0,0,0,.2);
      }
      .gbm-panel {
        position: fixed; bottom: 80px; right: 20px; width: 380px; max-width: calc(100vw - 24px);
        height: 560px; background: #fff; border-radius: 16px; box-shadow: 0 16px 48px rgba(0,0,0,.18);
        display: none; flex-direction: column; overflow: hidden; z-index: 999999;
        border: 1px solid rgba(0,0,0,.08);
      }
      .gbm-header { background: #0f766e; color: white; padding: 14px 16px; font-weight: 700; }
      .gbm-messages { flex: 1; overflow-y: auto; padding: 14px; background: #f8fafc; }
      .gbm-msg { margin-bottom: 12px; padding: 10px 12px; border-radius: 12px; line-height: 1.4; white-space: pre-wrap; }
      .gbm-user { background: #dbeafe; margin-left: 36px; }
      .gbm-bot { background: #ffffff; border: 1px solid #e2e8f0; margin-right: 18px; }
      .gbm-sources { margin-top: 8px; font-size: 12px; }
      .gbm-sources a { color: #0f766e; text-decoration: none; display: block; margin-top: 4px; }
      .gbm-tools { margin-top: 8px; display: flex; gap: 8px; flex-wrap: wrap; }
      .gbm-tool-btn { border: 1px solid #cbd5e1; background: #fff; border-radius: 999px; padding: 4px 8px; font-size: 12px; cursor: pointer; }
      .gbm-input-row { display: flex; gap: 8px; padding: 12px; border-top: 1px solid #e5e7eb; background: white; }
      .gbm-input { flex: 1; border: 1px solid #cbd5e1; border-radius: 12px; padding: 10px 12px; }
      .gbm-send { background: #0f766e; color: white; border: 0; border-radius: 12px; padding: 10px 14px; cursor: pointer; }
    </style>
    <button class="gbm-launcher">${title}</button>
    <div class="gbm-panel">
      <div class="gbm-header">${title}</div>
      <div class="gbm-messages">
        <div class="gbm-msg gbm-bot">Ask a question about Green Builder Media coverage, projects, research, or articles.</div>
      </div>
      <div class="gbm-input-row">
        <input class="gbm-input" type="text" placeholder="Ask about a topic, trend, or article..." />
        <button class="gbm-send">Send</button>
      </div>
    </div>
  `;

  const launcher = root.querySelector('.gbm-launcher');
  const panel = root.querySelector('.gbm-panel');
  const messages = root.querySelector('.gbm-messages');
  const input = root.querySelector('.gbm-input');
  const send = root.querySelector('.gbm-send');
  let lastQuestion = '';
  let lastAnswer = '';

  launcher.addEventListener('click', () => {
    panel.style.display = panel.style.display === 'flex' ? 'none' : 'flex';
  });

  function openCorrectionPage() {
    const url = new URL(apiBase + '/admin');
    url.searchParams.set('q', lastQuestion || '');
    url.searchParams.set('a', lastAnswer || '');
    url.searchParams.set('note', 'Created from Fix this answer button in site widget.');
    window.open(url.toString(), '_blank', 'noopener');
  }

  function addMessage(text, who, sources, options) {
    const el = document.createElement('div');
    el.className = 'gbm-msg ' + (who === 'user' ? 'gbm-user' : 'gbm-bot');
    el.textContent = text;

    if (Array.isArray(sources) && sources.length) {
      const s = document.createElement('div');
      s.className = 'gbm-sources';
      const label = document.createElement('div');
      label.textContent = 'Sources:';
      s.appendChild(label);
      sources.slice(0, 4).forEach((src) => {
        const a = document.createElement('a');
        a.href = src.url;
        a.target = '_blank';
        a.rel = 'noopener noreferrer';
        a.textContent = src.title + (src.published_at ? ' (' + src.published_at.slice(0, 10) + ')' : '');
        s.appendChild(a);
      });
      el.appendChild(s);
    }

    if (who === 'bot' && editorTools && !(options && options.hideTools)) {
      const tools = document.createElement('div');
      tools.className = 'gbm-tools';
      const fixBtn = document.createElement('button');
      fixBtn.className = 'gbm-tool-btn';
      fixBtn.textContent = 'Fix this answer';
      fixBtn.addEventListener('click', openCorrectionPage);
      tools.appendChild(fixBtn);
      el.appendChild(tools);
    }

    messages.appendChild(el);
    messages.scrollTop = messages.scrollHeight;
  }

  async function ask() {
    const question = input.value.trim();
    if (!question) return;
    lastQuestion = question;
    addMessage(question, 'user');
    input.value = '';

    try {
      const res = await fetch(apiBase + '/chat', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ question })
      });
      const data = await res.json();
      lastAnswer = data.answer || 'No answer returned.';
      addMessage(lastAnswer, 'bot', data.sources || []);
    } catch (err) {
      lastAnswer = 'The Green Builder assistant is temporarily unavailable.';
      addMessage(lastAnswer, 'bot');
    }
  }

  send.addEventListener('click', ask);
  input.addEventListener('keydown', function (e) {
    if (e.key === 'Enter') ask();
  });
})();
