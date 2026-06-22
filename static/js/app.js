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
  // sink config: show placeholder when Streaming (Kafka) selected
  function updateSinkConfigUI(value) {
    const cfg = document.getElementById('sink-config');
    const input = document.getElementById('sink-config-input');
    if (!value || !value.toLowerCase().includes('kafka')) {
      cfg.classList.add('d-none');
      if (input) input.placeholder = '';
      return;
    }
    cfg.classList.remove('d-none');
    if (input) input.placeholder = 'broker1:9092,broker2:9092 (ví dụ)';
  }

  document.querySelectorAll('input[name="sink"]').forEach(r => {
    r.addEventListener('change', (e) => updateSinkConfigUI(e.target.value));
  });

  document.getElementById('apply-btn').addEventListener('click', async () => {
    const source = document.querySelector('input[name="source"]:checked');
    const ingest = document.querySelector('input[name="ingest"]:checked');
    const sink = document.querySelector('input[name="sink"]:checked');

    const payload = {
      source: source ? source.value : null,
      ingest: ingest ? ingest.value : null,
      sink: sink ? sink.value : null,
      sink_config: document.getElementById('sink-config-input') ? document.getElementById('sink-config-input').value : null,
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
});
