document.addEventListener('DOMContentLoaded', function () {
  function toggleMenu(btnId, menuId) {
    const btn = document.getElementById(btnId);
    const menu = document.getElementById(menuId);
    btn.addEventListener('click', () => {
      menu.classList.toggle('d-none');
    });
  }

  toggleMenu('btn-source', 'menu-source');
  toggleMenu('btn-ingest', 'menu-ingest');
  toggleMenu('btn-sink', 'menu-sink');

  // source config input removed for demo; no dynamic placeholders
  // sink-config removed: no extra sink inputs shown for Kafka

  document.getElementById('apply-btn').addEventListener('click', async () => {
    const source = document.querySelector('input[name="source"]:checked');
    const ingest = document.querySelector('input[name="ingest"]:checked');
    const sink = document.querySelector('input[name="sink"]:checked');

    const payload = {
      source: source ? source.value : null,
      ingest: ingest ? ingest.value : null,
      sink: sink ? sink.value : null,
    };
    try {
      const res = await fetch('/api/generate', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      });
      if (!res.ok) throw new Error('HTTP ' + res.status);
      const data = await res.json();
      const codeBlock = document.getElementById('code-block');
      // set plain text then highlight
      codeBlock.textContent = data.code || '# sẽ bổ sung sau';
      if (window.hljs) hljs.highlightElement(codeBlock);
    } catch (err) {
      const codeBlock = document.getElementById('code-block');
      codeBlock.textContent = '# Lỗi khi sinh mã mẫu: ' + err.message;
      if (window.hljs) hljs.highlightElement(codeBlock);
    }
  });

  // FAQ management (client-side, persisted in localStorage)
  const faqKey = 'dataflow_demo_faqs_v1';
  function loadFaqs() {
    try {
      const raw = localStorage.getItem(faqKey) || '[]';
      return JSON.parse(raw);
    } catch (e) {
      return [];
    }
  }

  function saveFaqs(list) {
    localStorage.setItem(faqKey, JSON.stringify(list));
  }

  function renderFaqs() {
    const container = document.getElementById('faq-list');
    if (!container) return;
    container.innerHTML = '';
    const faqs = loadFaqs();
    if (faqs.length === 0) {
      container.innerHTML = '<div class="text-muted">Chưa có mục nào. Nhấn "Thêm câu hỏi" để tạo.</div>';
      return;
    }
    faqs.forEach((f, idx) => {
      const q = document.createElement('div');
      q.className = 'mb-2';
      const btn = document.createElement('button');
      btn.className = 'btn btn-link p-0';
      btn.textContent = f.question;
      btn.addEventListener('click', () => {
        const ans = document.getElementById('faq-answer-' + idx);
        if (ans) ans.classList.toggle('d-none');
      });
      const ans = document.createElement('div');
      ans.id = 'faq-answer-' + idx;
      ans.className = 'ms-2 mt-1 small d-none';
      ans.textContent = f.answer;
      q.appendChild(btn);
      q.appendChild(ans);
      container.appendChild(q);
    });
  }

  document.getElementById('add-faq').addEventListener('click', () => {
    const question = prompt('Nhập câu hỏi (ngắn):');
    if (!question) return;
    const answer = prompt('Nhập câu trả lời:');
    if (answer === null) return;
    const faqs = loadFaqs();
    faqs.push({ question: question.trim(), answer: answer.trim() });
    saveFaqs(faqs);
    renderFaqs();
  });

  // initial render
  renderFaqs();
});
