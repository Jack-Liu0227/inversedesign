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
      workflow_run_id: runInput ? runInput.value : '',
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

    // Auto drill-down similar to tool-trace for faster瀹氫綅
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
      workflow_run_id: runInput ? runInput.value : '',
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

function _escapeHtmlAttr(value) {
  return String(value ?? '')
    .replace(/&/g, '&amp;')
    .replace(/"/g, '&quot;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');
}

async function handleToolTraceCascade(source) {
  const form = document.querySelector('.trace-form');
  if (!form) return;

  const sessionInput = form.querySelector('input[name="session_id"]');
  const runInput = form.querySelector('input[name="workflow_run_id"]');
  const stepSel = document.getElementById('trace-step');
  const toolSel = document.getElementById('trace-tool');
  const successSel = form.querySelector('select[name="success"]');
  if (source === 'step') {
    if (toolSel) toolSel.value = '';
  }

  const params = new URLSearchParams({
    session_id: sessionInput ? sessionInput.value : '',
      workflow_run_id: runInput ? runInput.value : '',
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

async function deleteAcrossDatabases(defaultFilterCol, defaultFilterValue) {
  const cleanupFilterCol = document.getElementById('cleanup-filter-col');
  const viewerRunInput = document.getElementById('viewer-run-id');
  const explorerRunInput = document.getElementById('explorer-run-id');

  const filterCol = String(
    defaultFilterCol
      || (cleanupFilterCol ? cleanupFilterCol.value : '')
      || 'id'
  ).trim();

  const selectedValues = getCleanupSelectedValues();
  const fallbackSingleValue = String(
    defaultFilterValue
      || (viewerRunInput ? viewerRunInput.value : '')
      || (explorerRunInput ? explorerRunInput.value : '')
      || ''
  ).trim();
  const filterValues = selectedValues.length > 0 ? selectedValues : (fallbackSingleValue ? [fallbackSingleValue] : []);

  if (!filterCol || filterValues.length === 0) {
    alert('Please select identifier type and at least one value first.');
    return;
  }

  let previewData = cleanupSelectionMatchesPreview(filterCol, filterValues) ? cleanupPreviewState : null;
  if (!previewData) {
    try {
      previewData = await fetchCleanupPreview(filterCol, filterValues);
      renderCleanupPreview(previewData);
    } catch (error) {
      alert(error?.message || 'Preview request failed.');
      return;
    }
  }

  const totalMatches = Number(previewData?.total_matches || 0);
  const matchedTables = Number(previewData?.matched_tables || 0);
  if (totalMatches <= 0) {
    alert('No matched rows were found for the current selection. Nothing will be deleted.');
    return;
  }

  const summary = summarizeCleanupSelection(filterValues);
  if (!confirm(`Delete ${totalMatches} row(s) from ${matchedTables} table(s) for ${filterCol}: ${summary}?`)) return;

  const deleteBtn = document.getElementById('cleanup-delete-btn');
  if (deleteBtn) deleteBtn.disabled = true;
  try {
    const res = await fetch('/api/records/delete-across-databases', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        filter_col: filterCol,
        filter_values: filterValues,
      }),
    });
    if (!res.ok) {
      alert(`Delete across databases failed for ${filterCol}.`);
      return;
    }
    const data = await res.json().catch(() => ({}));
    const deleted = Number(data?.deleted || 0);
    const detailCount = Array.isArray(data?.details) ? data.details.length : 0;
    const errorCount = Array.isArray(data?.errors) ? data.errors.length : 0;
    alert(`Deleted ${deleted} row(s) in ${detailCount} table(s). Errors: ${errorCount}.`);

    if (window.location.pathname === '/record-cleanup') {
      cleanupSelectionState = [];
      syncCleanupSummary();
      renderCleanupPreview({
        filter_col: filterCol,
        filter_values: [],
        total_matches: 0,
        matched_tables: 0,
        scanned_tables: Number(previewData?.scanned_tables || 0),
        details: [],
        errors: [],
      });
      await refreshCleanupValueOptions();
      return;
    }
    await _refreshViewerPanels();
  } finally {
    if (deleteBtn) deleteBtn.disabled = getCleanupSelectedValues().length === 0;
  }
}

async function deleteAcrossDatabasesFromCleanup() {
  await deleteAcrossDatabases('', '');
}

function toggleViewerSelectAll(checked) {
  const panel = document.getElementById('viewer-results');
  if (!panel) return;
  panel.querySelectorAll('.record-check').forEach((el) => {
    el.checked = Boolean(checked);
  });
}

function syncViewerSelectAllState() {
  const panel = document.getElementById('viewer-results');
  const master = document.getElementById('viewer-select-all');
  if (!panel || !master) return;
  const all = Array.from(panel.querySelectorAll('.record-check'));
  const selected = all.filter((el) => el.checked);
  if (all.length === 0) {
    master.checked = false;
    master.indeterminate = false;
    return;
  }
  if (selected.length === 0) {
    master.checked = false;
    master.indeterminate = false;
    return;
  }
  if (selected.length === all.length) {
    master.checked = true;
    master.indeterminate = false;
    return;
  }
  master.checked = false;
  master.indeterminate = true;
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

function setExplorerSortBy(sortBy) {
  const select = document.getElementById('explorer-sort-by');
  if (!select) return;
  select.value = String(sortBy || '');
  const form = select.closest('form');
  if (form) form.submit();
}

function autoDetectExplorerRunId() {
  const codeInput = document.getElementById('explorer-code-filter');
  const runInput = document.getElementById('explorer-run-id');
  if (!codeInput || !runInput) return;
  if (String(runInput.value || '').trim()) return;
  const raw = String(codeInput.value || '').trim();
  if (!raw) return;
  const uuidLike = /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
  if (uuidLike.test(raw)) {
    runInput.value = raw;
  }
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

const cleanupFilterPlaceholders = {
  id: 'Search primary IDs',
  workflow_id: 'Search workflow IDs',
  session_id: 'Search session IDs',
  trace_id: 'Search trace IDs',
};

let cleanupValueRefreshTimer = null;
let cleanupPreviewState = null;
let cleanupSuggestionValues = [];
let cleanupSelectionState = [];

function cleanupSelectionMatchesPreview(filterCol, values) {
  if (!cleanupPreviewState) return false;
  const previewValues = Array.isArray(cleanupPreviewState.filter_values) ? cleanupPreviewState.filter_values : [];
  if (String(cleanupPreviewState.filter_col || '') !== String(filterCol || '')) return false;
  if (previewValues.length !== values.length) return false;
  return previewValues.every((value, index) => value === values[index]);
}

function getCleanupSelectedValues() {
  return [...cleanupSelectionState];
}

function summarizeCleanupSelection(values) {
  if (!Array.isArray(values) || values.length === 0) return 'None selected';
  if (values.length === 1) return values[0];
  const head = values.slice(0, 2).join(', ');
  const extra = values.length > 2 ? ` +${values.length - 2}` : '';
  return `${head}${extra}`;
}

function syncCleanupPlaceholder() {
  const filterColEl = document.getElementById('cleanup-filter-col');
  const filterQueryEl = document.getElementById('cleanup-filter-query');
  if (!filterColEl || !filterQueryEl) return;
  const filterCol = String(filterColEl.value || 'id').trim();
  filterQueryEl.placeholder = cleanupFilterPlaceholders[filterCol] || 'Search values';
}

function removeCleanupSelection(valueToRemove) {
  const target = String(valueToRemove || '').trim();
  if (!target) return;
  cleanupSelectionState = cleanupSelectionState.filter((item) => item !== target);
  const checkbox = document.querySelector(`#cleanup-filter-value input[type="checkbox"][value="${CSS.escape(target)}"]`);
  if (checkbox) checkbox.checked = false;
  syncCleanupSummary();
}

function renderCleanupSelectionChips() {
  const chipsEl = document.getElementById('cleanup-selected-chips');
  if (!chipsEl) return;
  const values = getCleanupSelectedValues();
  if (values.length === 0) {
    chipsEl.innerHTML = '<span class="cleanup-chip cleanup-chip-empty">No values selected yet.</span>';
    return;
  }
  chipsEl.innerHTML = values.map((value) => {
    const safe = _escapeHtmlAttr(value);
    return `
      <button type="button" class="cleanup-chip" onclick="removeCleanupSelection('${safe}')">
        <span>${safe}</span>
        <span class="cleanup-chip-x">x</span>
      </button>`;
  }).join('');
}

function syncCleanupSummary() {
  const selectedValues = getCleanupSelectedValues();
  const selectedValueEl = document.getElementById('cleanup-selected-value');
  const deleteBtn = document.getElementById('cleanup-delete-btn');
  const previewBtn = document.getElementById('cleanup-preview-btn');
  const summaryText = summarizeCleanupSelection(selectedValues);
  if (selectedValueEl) selectedValueEl.textContent = summaryText;
  if (deleteBtn) deleteBtn.disabled = selectedValues.length === 0;
  if (previewBtn) previewBtn.disabled = selectedValues.length === 0;
  renderCleanupSelectionChips();
}

function renderCleanupValueOptions(values) {
  cleanupSuggestionValues = Array.isArray(values) ? values.map((item) => String(item || '').trim()).filter(Boolean) : [];
  const listEl = document.getElementById('cleanup-filter-value');
  if (!listEl) return;
  const selected = new Set(cleanupSelectionState);
  if (cleanupSuggestionValues.length === 0) {
    listEl.innerHTML = '<div class="cleanup-picker-empty">No values found for this identifier type.</div>';
    syncCleanupSummary();
    return;
  }
  listEl.innerHTML = cleanupSuggestionValues.map((value) => {
    const checked = selected.has(value) ? ' checked' : '';
    const safeValue = _escapeHtmlAttr(value);
    return `
      <label class="cleanup-option-row">
        <input type="checkbox" value="${safeValue}"${checked} onchange="handleCleanupValueToggle(this)" />
        <span>${safeValue}</span>
      </label>`;
  }).join('');
  syncCleanupSummary();
}

function handleCleanupValueToggle(input) {
  if (!input) return;
  const value = String(input.value || '').trim();
  if (!value) return;
  const current = new Set(cleanupSelectionState);
  if (input.checked) current.add(value);
  else current.delete(value);
  cleanupSelectionState = [...current];
  syncCleanupSummary();
}

function clearCleanupSelection() {
  cleanupSelectionState = [];
  document.querySelectorAll('#cleanup-filter-value input[type="checkbox"]').forEach((item) => {
    item.checked = false;
  });
  syncCleanupSummary();
}

function selectAllVisibleCleanupValues() {
  const current = new Set(cleanupSelectionState);
  cleanupSuggestionValues.forEach((value) => current.add(value));
  cleanupSelectionState = [...current];
  document.querySelectorAll('#cleanup-filter-value input[type="checkbox"]').forEach((item) => {
    item.checked = true;
  });
  syncCleanupSummary();
}

function renderCleanupPreview(preview) {
  cleanupPreviewState = preview || null;
  const totalMatchesEl = document.getElementById('cleanup-total-matches');
  const matchedTablesEl = document.getElementById('cleanup-matched-tables');
  const scannedTablesEl = document.getElementById('cleanup-scanned-tables');
  const previewColEl = document.getElementById('cleanup-preview-col');
  const previewValuesEl = document.getElementById('cleanup-preview-values');
  const previewBadgeEl = document.getElementById('cleanup-preview-badge');
  const previewListEl = document.getElementById('cleanup-preview-list');
  const errorBoxEl = document.getElementById('cleanup-error-box');

  const details = Array.isArray(preview?.details) ? preview.details : [];
  const errors = Array.isArray(preview?.errors) ? preview.errors : [];
  const values = Array.isArray(preview?.filter_values) ? preview.filter_values : [];

  if (totalMatchesEl) totalMatchesEl.textContent = String(Number(preview?.total_matches || 0));
  if (matchedTablesEl) matchedTablesEl.textContent = String(Number(preview?.matched_tables || 0));
  if (scannedTablesEl) scannedTablesEl.textContent = String(Number(preview?.scanned_tables || 0));
  if (previewColEl) previewColEl.textContent = String(preview?.filter_col || 'id');
  if (previewValuesEl) previewValuesEl.textContent = values.length > 0 ? summarizeCleanupSelection(values) : 'N/A';
  if (previewBadgeEl) previewBadgeEl.textContent = errors.length > 0 ? `${errors.length} errors` : (details.length > 0 ? 'Preview Ready' : 'No Match');

  if (previewListEl) {
    if (values.length === 0) {
      previewListEl.innerHTML = '<div class="empty-state">Choose an identifier type, open the value picker, select one or more values, then preview the matches.</div>';
    } else if (details.length === 0) {
      previewListEl.innerHTML = '<div class="empty-state">No matched records were found for the current selection.</div>';
    } else {
      previewListEl.innerHTML = details.map((item) => {
        const db = _escapeHtmlAttr(item?.db || '');
        const table = _escapeHtmlAttr(item?.table || '');
        const matched = Number(item?.matched || 0);
        const matchedColumn = _escapeHtmlAttr(item?.matched_column || '');
        const keyCol = _escapeHtmlAttr(item?.key_col || '');
        const filterValue = _escapeHtmlAttr(item?.filter_value || '');
        const samples = Array.isArray(item?.sample_keys) && item.sample_keys.length > 0
          ? item.sample_keys.map((sample) => _escapeHtmlAttr(sample)).join(', ')
          : 'none';
        return `
          <article class="cleanup-match-card">
            <header>
              <div>
                <strong>${db}</strong>
                <span>${table}</span>
              </div>
              <div class="cleanup-match-total">${matched} row(s)</div>
            </header>
            <div class="cleanup-match-meta">
              <span>matched by: ${matchedColumn}</span>
              <span>key: ${keyCol}</span>
              <span>value: ${filterValue}</span>
              <span>samples: ${samples}</span>
            </div>
          </article>`;
      }).join('');
    }
  }

  if (errorBoxEl) {
    if (errors.length === 0) {
      errorBoxEl.style.display = 'none';
      errorBoxEl.innerHTML = '';
    } else {
      errorBoxEl.style.display = 'grid';
      errorBoxEl.innerHTML = errors.map((err) => {
        const db = _escapeHtmlAttr(err?.db || '');
        const table = _escapeHtmlAttr(err?.table || '');
        const message = _escapeHtmlAttr(err?.error || 'unknown error');
        return `<div><code>${db}</code> / <code>${table}</code>: ${message}</div>`;
      }).join('');
    }
  }
}

async function refreshCleanupValueOptions() {
  const filterColEl = document.getElementById('cleanup-filter-col');
  const filterQueryEl = document.getElementById('cleanup-filter-query');
  if (!filterColEl || !filterQueryEl) return;

  const filterCol = String(filterColEl.value || '').trim();
  const query = String(filterQueryEl.value || '').trim();
  if (!filterCol) {
    renderCleanupValueOptions([]);
    return;
  }

  try {
    const params = new URLSearchParams({
      filter_col: filterCol,
      q: query,
      limit: query ? '160' : '100',
    });
    const res = await fetch(`/api/records/cross-db-suggestions?${params.toString()}`);
    if (!res.ok) return;
    const data = await res.json().catch(() => ({}));
    renderCleanupValueOptions(Array.isArray(data?.values) ? data.values : []);
  } catch (_) {
    // ignore suggestion failures
  }
}

function debouncedRefreshCleanupValueOptions() {
  if (cleanupValueRefreshTimer) window.clearTimeout(cleanupValueRefreshTimer);
  cleanupValueRefreshTimer = window.setTimeout(() => {
    refreshCleanupValueOptions();
  }, 140);
}

function handleCleanupFilterTypeChange() {
  const queryEl = document.getElementById('cleanup-filter-query');
  syncCleanupPlaceholder();
  cleanupSelectionState = [];
  if (queryEl) {
    queryEl.value = '';
    queryEl.focus();
  }
  renderCleanupValueOptions([]);
  renderCleanupPreview({
    filter_col: String(document.getElementById('cleanup-filter-col')?.value || 'id'),
    filter_values: [],
    total_matches: 0,
    matched_tables: 0,
    scanned_tables: 0,
    details: [],
    errors: [],
  });
  refreshCleanupValueOptions();
}

async function fetchCleanupPreview(filterCol, filterValues) {
  const res = await fetch('/api/records/cross-db-preview-batch', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ filter_col: filterCol, filter_values: filterValues }),
  });
  if (!res.ok) throw new Error('Preview request failed.');
  return await res.json().catch(() => ({}));
}

async function previewCleanupSelection() {
  const filterColEl = document.getElementById('cleanup-filter-col');
  const previewBtn = document.getElementById('cleanup-preview-btn');
  if (!filterColEl) return;
  const filterCol = String(filterColEl.value || 'id').trim();
  const filterValues = getCleanupSelectedValues();
  if (filterValues.length === 0) {
    alert('Select at least one identifier value first.');
    return;
  }

  if (previewBtn) previewBtn.disabled = true;
  try {
    const data = await fetchCleanupPreview(filterCol, filterValues);
    renderCleanupPreview(data);
  } catch (error) {
    alert(error?.message || 'Preview request failed.');
  } finally {
    if (previewBtn) previewBtn.disabled = false;
    syncCleanupSummary();
  }
}

document.addEventListener('DOMContentLoaded', () => {
  const cleanupFilterCol = document.getElementById('cleanup-filter-col');
  const initialStateEl = document.getElementById('cleanup-initial-state');
  let initialState = null;
  if (initialStateEl) {
    try {
      initialState = JSON.parse(initialStateEl.textContent || '{}');
    } catch (_) {
      initialState = null;
    }
  }

  if (cleanupFilterCol) {
    cleanupFilterCol.addEventListener('change', handleCleanupFilterTypeChange);
    cleanupSelectionState = Array.isArray(initialState?.selected_values)
      ? initialState.selected_values.map((item) => String(item || '').trim()).filter(Boolean)
      : [];
    syncCleanupPlaceholder();
    renderCleanupValueOptions(Array.isArray(initialState?.suggestions) ? initialState.suggestions : []);
    renderCleanupPreview(initialState?.preview || {
      filter_col: String(cleanupFilterCol.value || 'id'),
      filter_values: cleanupSelectionState,
      total_matches: 0,
      matched_tables: 0,
      scanned_tables: 0,
      details: [],
      errors: [],
    });
    syncCleanupSummary();
    refreshCleanupValueOptions();
  }
});


