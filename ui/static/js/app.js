async function createTag() {
  const form = document.getElementById('create-tag-form');
  if (!form) return;
  const data = Object.fromEntries(new FormData(form).entries());
  const res = await fetch('/api/classifications/tags', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(data),
  });
  if (res.ok) {
    window.location.reload();
  } else {
    alert('Failed to save tag');
  }
}

async function assignTags(sourceDb, sourceTable, sourcePk, inputId) {
  const input = document.getElementById(inputId);
  if (!input) return;
  const tags = input.value
    .split(',')
    .map((s) => s.trim())
    .filter(Boolean);

  if (tags.length === 0) {
    alert('Please input at least one tag.');
    return;
  }

  const res = await fetch('/api/classifications/assign', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      source_db: sourceDb,
      source_table: sourceTable,
      source_pk: String(sourcePk),
      tag_names: tags,
    }),
  });

  if (res.ok) {
    input.value = '';
    alert('Tags assigned.');
    return;
  }
  alert('Failed to assign tags.');
}

function openTextModal(rawText, columnName) {
  const modal = document.getElementById('text-modal');
  const title = document.getElementById('text-modal-title');
  const body = document.getElementById('text-modal-body');
  if (!modal || !title || !body) return;

  title.textContent = `Column: ${columnName}`;
  const rendered = _formatTextForDisplay(rawText);

  body.textContent = rendered;
  modal.classList.add('open');
}

function _unescapeCommonText(text) {
  return String(text || '')
    .replace(/\\\\r\\\\n/g, '\n')
    .replace(/\\\\n/g, '\n')
    .replace(/\\\\t/g, '\t')
    .replace(/\\r\\n/g, '\n')
    .replace(/\\n/g, '\n')
    .replace(/\\t/g, '\t')
    .replace(/\\"/g, '"');
}

function _decodeNestedJsonLike(value, depth = 0) {
  if (depth > 5) return value;
  if (value === null || value === undefined) return value;
  if (Array.isArray(value)) return value.map((v) => _decodeNestedJsonLike(v, depth + 1));
  if (typeof value === 'object') {
    const out = {};
    Object.entries(value).forEach(([k, v]) => {
      out[String(k)] = _decodeNestedJsonLike(v, depth + 1);
    });
    return out;
  }
  if (typeof value !== 'string') return value;

  const raw = String(value).trim();
  if (!raw) return '';
  const looksLikeJson = (raw.startsWith('{') && raw.endsWith('}')) || (raw.startsWith('[') && raw.endsWith(']'));
  if (looksLikeJson) {
    try {
      return _decodeNestedJsonLike(JSON.parse(raw), depth + 1);
    } catch (_) {
      // ignore
    }
  }
  const unescaped = _unescapeCommonText(raw);
  if (unescaped !== raw) {
    const u = unescaped.trim();
    const uLooksLikeJson = (u.startsWith('{') && u.endsWith('}')) || (u.startsWith('[') && u.endsWith(']'));
    if (uLooksLikeJson) {
      try {
        return _decodeNestedJsonLike(JSON.parse(u), depth + 1);
      } catch (_) {
        // ignore
      }
    }
  }
  return unescaped;
}

function _extractBalancedJson(text, startIdx) {
  if (startIdx < 0 || startIdx >= text.length) return null;
  const open = text[startIdx];
  if (open !== '[' && open !== '{') return null;
  const close = open === '[' ? ']' : '}';
  let depth = 0;
  let inString = false;
  let escaped = false;
  for (let i = startIdx; i < text.length; i += 1) {
    const ch = text[i];
    if (inString) {
      if (escaped) {
        escaped = false;
      } else if (ch === '\\') {
        escaped = true;
      } else if (ch === '"') {
        inString = false;
      }
      continue;
    }
    if (ch === '"') {
      inString = true;
      continue;
    }
    if (ch === open) depth += 1;
    if (ch === close) {
      depth -= 1;
      if (depth === 0) return text.slice(startIdx, i + 1);
    }
  }
  return null;
}

function _prettyPromptEmbeddedJson(text) {
  let out = String(text || '');
  const keys = ['candidates=', 'candidate_predictions=', 'predictions=', 'features='];
  keys.forEach((key) => {
    const idx = out.indexOf(key);
    if (idx < 0) return;
    let j = idx + key.length;
    while (j < out.length && /\s/.test(out[j])) j += 1;
    const segment = _extractBalancedJson(out, j);
    if (!segment) return;
    try {
      const parsed = _decodeNestedJsonLike(JSON.parse(segment));
      const pretty = JSON.stringify(parsed, null, 2);
      out = `${out.slice(0, idx)}${key}\n${pretty}${out.slice(j + segment.length)}`;
    } catch (_) {
      // ignore
    }
  });
  return out;
}

function _formatTextForDisplay(rawText) {
  const decoded = _decodeNestedJsonLike(rawText);
  const normalized = _normalizeDisplayValue(decoded);
  if (Array.isArray(normalized) || (normalized && typeof normalized === 'object')) {
    return _renderStructuredText(normalized);
  }
  return String(normalized ?? '');
}

function _normalizeDisplayValue(value) {
  if (value === null || value === undefined) return value;
  if (Array.isArray(value)) return value.map((v) => _normalizeDisplayValue(v));
  if (typeof value === 'object') {
    const out = {};
    Object.entries(value).forEach(([k, v]) => {
      out[String(k)] = _normalizeDisplayValue(v);
    });
    return out;
  }
  if (typeof value !== 'string') return value;
  const unescaped = _unescapeCommonText(value);
  return _prettyPromptEmbeddedJson(unescaped);
}

function _indent(level) {
  return '  '.repeat(Math.max(0, level));
}

function _renderScalar(value) {
  if (value === null) return 'null';
  if (value === undefined) return 'undefined';
  if (typeof value === 'boolean' || typeof value === 'number') return String(value);
  return String(value);
}

function _renderStructuredText(value, level = 0) {
  if (Array.isArray(value)) {
    if (value.length === 0) return `${_indent(level)}[]`;
    const lines = [];
    value.forEach((item) => {
      if (Array.isArray(item) || (item && typeof item === 'object')) {
        lines.push(`${_indent(level)}-`);
        lines.push(_renderStructuredText(item, level + 1));
      } else {
        const scalar = _renderScalar(item);
        if (scalar.includes('\n')) {
          lines.push(`${_indent(level)}-|`);
          scalar.split('\n').forEach((line) => lines.push(`${_indent(level + 1)}${line}`));
        } else {
          lines.push(`${_indent(level)}- ${scalar}`);
        }
      }
    });
    return lines.join('\n');
  }

  if (value && typeof value === 'object') {
    const entries = Object.entries(value);
    if (entries.length === 0) return `${_indent(level)}{}`;
    const lines = [];
    entries.forEach(([k, v]) => {
      if (Array.isArray(v) || (v && typeof v === 'object')) {
        lines.push(`${_indent(level)}${k}:`);
        lines.push(_renderStructuredText(v, level + 1));
      } else {
        const scalar = _renderScalar(v);
        if (scalar.includes('\n')) {
          lines.push(`${_indent(level)}${k}: |`);
          scalar.split('\n').forEach((line) => lines.push(`${_indent(level + 1)}${line}`));
        } else {
          lines.push(`${_indent(level)}${k}: ${scalar}`);
        }
      }
    });
    return lines.join('\n');
  }

  return `${_indent(level)}${_renderScalar(value)}`;
}

function openTextModalFromCell(button, columnName) {
  if (!button) return;
  const wrapper = button.closest('.cell-preview');
  if (!wrapper) return;
  const holder = wrapper.querySelector('.hidden-fulltext');
  const text = holder ? holder.value : '';
  openTextModal(text, columnName);
}

function closeTextModal() {
  const modal = document.getElementById('text-modal');
  if (!modal) return;
  modal.classList.remove('open');
}

async function viewFullMaterialDocFromDetail(sourceName, materialType, sourceKind, workflowRunId, roundIndex) {
  const src = String(sourceName || '').trim();
  if (!src) {
    alert('Missing source_name');
    return;
  }
  const params = new URLSearchParams();
  params.set('source_name', src);
  if (String(materialType || '').trim()) params.set('material_type', String(materialType || '').trim());
  if (String(sourceKind || '').trim()) params.set('source_kind', String(sourceKind || '').trim());
  if (String(workflowRunId || '').trim()) params.set('workflow_run_id', String(workflowRunId || '').trim());
  if (Number.isFinite(Number(roundIndex))) params.set('round_index', String(Number(roundIndex)));

  const res = await fetch(`/api/material-data/docs/full?${params.toString()}`);
  if (!res.ok) {
    alert('Failed to load full document.');
    return;
  }
  const data = await res.json();
  openTextModal(data.full_text || '', `Full Doc: ${src}`);
}

async function copyModalText() {
  const body = document.getElementById('text-modal-body');
  if (!body) return;
  try {
    await navigator.clipboard.writeText(body.textContent || '');
  } catch (_) {
    // ignore clipboard failure
  }
}

function reloadViewerTables() {
  const db = document.getElementById('viewer-db');
  if (!db) return;
  const url = new URL('/viewer', window.location.origin);
  url.searchParams.set('db', db.value);
  window.location.href = url.toString();
}

async function handleViewerCascade(source) {
  const form = document.querySelector('.viewer-form');
  if (!form) return;
  const dbSel = document.getElementById('viewer-db');
  const tableSel = document.getElementById('viewer-table');
  if (!dbSel || !tableSel) return;

  // table/db change: use full page load to refresh visible filter controls.
  if (source === 'table' || source === 'db') {
    const url = new URL('/viewer', window.location.origin);
    const data = new FormData(form);
    for (const [k, v] of data.entries()) {
      url.searchParams.set(k, String(v));
    }
    window.location.href = url.toString();
    return;
  }

  const stepSel = document.getElementById('viewer-step');
  const agentSel = document.getElementById('viewer-agent');
  const toolSel = document.getElementById('viewer-tool');
  const statusSel = document.getElementById('viewer-status');
  const eventTypeSel = document.getElementById('viewer-event-type');
  const decisionSel = document.getElementById('viewer-decision');
  const shouldStopSel = document.getElementById('viewer-should-stop');
  const successSel = document.getElementById('viewer-success');
  const materialTypeSel = document.getElementById('viewer-material-type');
  const traceInput = document.getElementById('viewer-trace-id');
  const sessionInput = document.getElementById('viewer-session-id');
  const runInput = document.getElementById('viewer-run-id');

  const params = new URLSearchParams({
    db: dbSel.value,
    table: tableSel.value,
    trace_id: traceInput ? traceInput.value : '',
    session_id: sessionInput ? sessionInput.value : '',
    run_id: runInput ? runInput.value : '',
    material_type: materialTypeSel ? materialTypeSel.value : '',
    step_name: stepSel ? stepSel.value : '',
    agent_name: agentSel ? agentSel.value : '',
    event_type: eventTypeSel ? eventTypeSel.value : '',
    decision: decisionSel ? decisionSel.value : '',
    should_stop: shouldStopSel ? shouldStopSel.value : '',
    success: successSel ? successSel.value : '',
  });
  const res = await fetch(`/api/viewer/filter-options?${params.toString()}`);
  if (res.ok) {
    const data = await res.json();
    const prevStep = stepSel ? stepSel.value : '';
    const prevAgent = agentSel ? agentSel.value : '';
    const prevTool = toolSel ? toolSel.value : '';
    const prevStatus = statusSel ? statusSel.value : '';
    const prevEventType = eventTypeSel ? eventTypeSel.value : '';
    const prevDecision = decisionSel ? decisionSel.value : '';
    const prevShouldStop = shouldStopSel ? shouldStopSel.value : '';
    _setSelectOptions(stepSel, data.step_names, prevStep);
    _setSelectOptions(agentSel, data.agent_names, prevAgent);
    _setSelectOptions(toolSel, data.tool_names, prevTool);
    _setSelectOptions(statusSel, data.statuses, prevStatus);
    _setSelectOptions(eventTypeSel, data.event_types, prevEventType);
    _setSelectOptions(decisionSel, data.decisions, prevDecision);
    _setSelectOptions(shouldStopSel, data.should_stop_values, prevShouldStop);

    // Auto drill-down similar to tool-trace for faster定位
    if (source === 'step' && agentSel && agentSel.value === '' && Array.isArray(data.agent_names) && data.agent_names.length > 0) {
      agentSel.value = data.agent_names[0];
    }
    if ((source === 'step' || source === 'agent') && toolSel && toolSel.value === '' && Array.isArray(data.tool_names) && data.tool_names.length > 0) {
      toolSel.value = data.tool_names[0];
    }

    if (source === 'step' && agentSel && agentSel.value) {
      const params2 = new URLSearchParams({
        db: dbSel.value,
        table: tableSel.value,
        trace_id: traceInput ? traceInput.value : '',
        session_id: sessionInput ? sessionInput.value : '',
        run_id: runInput ? runInput.value : '',
        material_type: materialTypeSel ? materialTypeSel.value : '',
        step_name: stepSel ? stepSel.value : '',
        agent_name: agentSel.value,
        event_type: eventTypeSel ? eventTypeSel.value : '',
        decision: decisionSel ? decisionSel.value : '',
        should_stop: shouldStopSel ? shouldStopSel.value : '',
        success: successSel ? successSel.value : '',
      });
      const res2 = await fetch(`/api/viewer/filter-options?${params2.toString()}`);
      if (res2.ok) {
        const data2 = await res2.json();
        _setSelectOptions(toolSel, data2.tool_names, toolSel ? toolSel.value : '');
        if (toolSel && toolSel.value === '' && Array.isArray(data2.tool_names) && data2.tool_names.length > 0) {
          toolSel.value = data2.tool_names[0];
        }
      }
    }
  }

  if (window.htmx) {
    window.htmx.trigger(form, 'submit');
  } else {
    form.requestSubmit();
  }
}

function setSortOrder(order) {
  const input = document.getElementById('sort-order-input');
  const form = document.querySelector('.viewer-form');
  if (!input || !form) return;
  input.value = order === 'asc' ? 'asc' : 'desc';
  if (window.htmx) {
    window.htmx.trigger(form, 'submit');
  } else {
    form.requestSubmit();
  }
}

function setToolTraceSort(order) {
  const input = document.getElementById('trace-sort-order');
  const form = document.querySelector('.trace-form');
  if (!input || !form) return;
  input.value = order === 'asc' ? 'asc' : 'desc';
  if (window.htmx) {
    window.htmx.trigger(form, 'submit');
  } else {
    form.requestSubmit();
  }
}

function _setSelectOptions(selectEl, values, currentValue) {
  if (!selectEl) return;
  const safeValues = Array.isArray(values) ? values : [];
  const currentValid = currentValue && safeValues.includes(currentValue);
  const selected = currentValid ? currentValue : '';
  const options = ['<option value="">all</option>', ...safeValues.map((v) => `<option value="${v}">${v}</option>`)];
  selectEl.innerHTML = options.join('');
  selectEl.value = selected;
}

async function handleToolTraceCascade(source) {
  const form = document.querySelector('.trace-form');
  if (!form) return;

  const sessionInput = form.querySelector('input[name="session_id"]');
  const stepSel = document.getElementById('trace-step');
  const toolSel = document.getElementById('trace-tool');
  const successSel = form.querySelector('select[name="success"]');
  if (source === 'step') {
    if (toolSel) toolSel.value = '';
  }

  const params = new URLSearchParams({
    session_id: sessionInput ? sessionInput.value : '',
    step_name: stepSel ? stepSel.value : '',
    success: successSel ? successSel.value : '',
  });

  const res = await fetch(`/api/tool-trace/filter-options?${params.toString()}`);
  if (!res.ok) return;
  const data = await res.json();

  const prevStep = stepSel ? stepSel.value : '';
  const prevTool = toolSel ? toolSel.value : '';

  _setSelectOptions(stepSel, data.step_names, prevStep);
  _setSelectOptions(toolSel, data.tool_names, prevTool);

  if (window.htmx) {
    window.htmx.trigger(form, 'submit');
  } else {
    form.requestSubmit();
  }
}

function _viewerQueryParams() {
  const form = document.querySelector('.viewer-form');
  const params = new URLSearchParams();
  if (!form) return params;
  const data = new FormData(form);
  for (const [k, v] of data.entries()) {
    params.set(k, String(v));
  }
  return params;
}

async function _refreshViewerPanels() {
  const results = document.getElementById('viewer-results');
  const hasViewerForm = Boolean(document.querySelector('.viewer-form'));
  const params = _viewerQueryParams();
  const query = params.toString();

  if (results && hasViewerForm) {
    const res = await fetch(`/partials/viewer-results?${query}`);
    if (res.ok) results.innerHTML = await res.text();
  }
  window.location.reload();
}

async function batchDeleteSelected(sourceDb, sourceTable, keyCol) {
  const checks = Array.from(document.querySelectorAll('.record-check:checked'));
  const keyValues = checks.map((el) => String(el.value));
  if (keyValues.length === 0) {
    alert('Please select records first.');
    return;
  }
  const res = await fetch('/api/records/batch-delete', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      source_db: sourceDb,
      source_table: sourceTable,
      key_col: keyCol,
      key_values: keyValues,
    }),
  });
  if (!res.ok) {
    alert('Batch delete failed.');
    return;
  }
  await _refreshViewerPanels();
}

async function restoreSelected() {
  const checks = Array.from(document.querySelectorAll('.recycle-check:checked'));
  const ids = checks.map((el) => Number(el.value));
  if (ids.length === 0) {
    alert('Please select recycle records first.');
    return;
  }
  const res = await fetch('/api/records/restore', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ recycle_ids: ids }),
  });
  if (!res.ok) {
    alert('Restore failed.');
    return;
  }
  await _refreshViewerPanels();
}

function setExplorerSortOrder(order) {
  const input = document.getElementById('explorer-sort-order');
  if (!input) return;
  input.value = order === 'asc' ? 'asc' : 'desc';
  const form = input.closest('form');
  if (form) form.submit();
}

function toggleExplorerSelectAll(checked) {
  document.querySelectorAll('.record-check').forEach((el) => {
    el.checked = Boolean(checked);
  });
}

function toggleRecycleSelectAll(checked) {
  document.querySelectorAll('.recycle-check').forEach((el) => {
    el.checked = Boolean(checked);
  });
}

async function purgeSelectedRecycle() {
  const checks = Array.from(document.querySelectorAll('.recycle-check:checked'));
  const ids = checks.map((el) => Number(el.value)).filter((x) => Number.isInteger(x) && x > 0);
  if (ids.length === 0) {
    alert('Please select recycle records first.');
    return;
  }
  if (!confirm(`Permanently delete ${ids.length} selected recycle records?`)) return;
  const res = await fetch('/api/records/purge', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ recycle_ids: ids }),
  });
  if (!res.ok) {
    alert('Permanent delete failed.');
    return;
  }
  await _refreshViewerPanels();
}

async function purgeAllRecycle() {
  if (!confirm('Permanently clear all un-restored records in recycle bin?')) return;
  const res = await fetch('/api/records/purge-all', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({}),
  });
  if (!res.ok) {
    alert('Clear recycle bin failed.');
    return;
  }
  await _refreshViewerPanels();
}

document.body.addEventListener('htmx:beforeRequest', (evt) => {
  const target = evt.detail.target;
  if (target) target.classList.add('loading');
});

document.body.addEventListener('htmx:afterRequest', (evt) => {
  const target = evt.detail.target;
  if (target) target.classList.remove('loading');
});
