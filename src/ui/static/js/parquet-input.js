/* Typed source selection. Durations are values, never column positions in storage. */
(function (root) {
  'use strict';
  const defaults = [5, 15, 30, 60, 120, 240, 1440];
  function parseDuration(text) {
    const match = /^(\d+)(m|h|D|d)$/.exec(String(text).trim());
    if (!match) throw new Error('Use a positive integer followed by m, h or D (for example 90m or 3D).');
    const minutes = Number(match[1]) * ({ m: 1, h: 60, D: 1440, d: 1440 }[match[2]]);
    if (!Number.isSafeInteger(minutes) || minutes <= 0 || minutes % 5 || minutes > 153722867) {
      throw new Error('Timeframe must be a representable positive multiple of 5 minutes.');
    }
    return minutes;
  }
  function label(minutes) {
    if (minutes % 1440 === 0) return `${minutes / 1440}D`;
    if (minutes % 60 === 0) return `${minutes / 60}h`;
    return `${minutes}m`;
  }
  function validSource(s) {
    return Boolean(s && s.type === 'parquet' && s.version === 1
      && typeof s.pack_root === 'string' && /^(?:[A-Za-z]:[\\/]|\\\\|\/)/.test(s.pack_root)
      && /^[A-Z0-9][A-Z0-9.-]*_[A-Z0-9][A-Z0-9.-]*$/.test(s.instrument_id)
      && Number.isSafeInteger(s.timeframe_minutes) && s.timeframe_minutes > 0
      && s.timeframe_minutes % 5 === 0 && s.timeframe_minutes <= 153722867);
  }
  function sourceLabel(s) { return `${s.instrument_id}, ${label(s.timeframe_minutes)}`; }
  function identity(s) { return `${s.pack_root.replace(/\\/g, '/')}|${s.instrument_id}|${s.timeframe_minutes}`; }
  class Selection {
    constructor(durations = defaults) {
      this.durations = [...durations]; this.invalid = new Map(); this.drafts = new Map(); this.reset([]);
    }
    reset(instruments) {
      this.instruments = instruments; this.selected = new Set(); this.anchor = 0;
      this.checked = instruments.map(() => this.durations.map(() => false));
      this.invalid.clear();
      this.drafts.clear();
    }
    commit(column, text) {
      try {
        const value = parseDuration(text);
        if (this.durations.some((n, i) => i !== column && n === value)) throw new Error('Timeframes must be distinct.');
        this.durations[column] = value; this.invalid.delete(column); this.drafts.delete(column); return value;
      } catch (error) { this.drafts.set(column, text); this.invalid.set(column, error.message); throw error; }
    }
    cancel(column) { this.invalid.delete(column); this.drafts.delete(column); return label(this.durations[column]); }
    bulk(column) {
      const all = this.checked.length > 0 && this.checked.every(row => row[column]);
      this.checked.forEach(row => { row[column] = !all; });
    }
    sources(packRoot) {
      if (this.invalid.size) throw new Error('Fix or cancel the invalid timeframe before adding.');
      if (!this.selected.size) throw new Error('Select at least one instrument row.');
      const sources = [];
      [...this.selected].sort((a, b) => a - b).forEach(i => {
        if (!this.checked[i].some(Boolean)) throw new Error(`${this.instruments[i].instrument_id}: select at least one timeframe.`);
        this.checked[i].forEach((checked, col) => {
          if (checked) sources.push({ type: 'parquet', version: 1, pack_root: packRoot,
            instrument_id: this.instruments[i].instrument_id, timeframe_minutes: this.durations[col] });
        });
      });
      return sources;
    }
  }
  const api = { parseDuration, label, validSource, sourceLabel, identity, Selection };
  if (typeof module !== 'undefined') module.exports = api;
  if (!root.document) return;
  const saved = (() => { try { return JSON.parse(sessionStorage.getItem('merlinParquetInput') || '{}'); } catch (_) { return {}; } })();
  const model = new Selection(Array.isArray(saved.durations) && saved.durations.length === 7 ? saved.durations : defaults);
  const state = { mode: false, paths: { csv: '', parquet: saved.path || '' }, sources: saved.sources || [], sequence: 0, pack: null, metadata: new Map(), relocation: null };
  function persist() { sessionStorage.setItem('merlinParquetInput', JSON.stringify({ durations: model.durations, path: state.paths.parquet, sources: state.sources })); }
  const el = id => document.getElementById(id);
  function setMode(on) {
    state.sequence++;
    const input = el('csvDirectory');
    if (input && state.mode !== Boolean(on)) {
      state.paths[state.mode ? 'parquet' : 'csv'] = input.value;
      input.value = state.paths[on ? 'parquet' : 'csv'];
    }
    state.mode = Boolean(on);
    if (el('useParquet')) el('useParquet').checked = state.mode;
    const directoryLabel = document.querySelector('label[for="csvDirectory"]');
    if (directoryLabel) directoryLabel.textContent = state.mode ? 'Parquet data directory' : 'CSV data directory';
    if (el('dataDirectoryHelp')) el('dataDirectoryHelp').textContent = state.mode
      ? 'Open a pack, select instruments and check timeframes. Use Ctrl/Shift-click for multi-select.'
      : 'Select files from the directory browser. Use Ctrl/Shift-click for multi-select.';
    const title = document.querySelector('.selected-files-title');
    if (title) title.textContent = state.mode ? 'Selected Parquet sources' : 'Selected CSV files';
    if (el('parquetCoverageDetails')) el('parquetCoverageDetails').hidden = !state.mode;
    if (typeof renderSelectedFiles === 'function') renderSelectedFiles([]);
    if (state.mode) refreshSelectedMetadata();
  }
  function renderSelected() {
    const list = el('selectedFilesList'); if (!list) return;
    el('selectedFilesWrapper').style.display = state.sources.length ? 'block' : 'none';
    list.replaceChildren();
    state.sources.forEach((source, i) => {
      const li = document.createElement('li'); li.textContent = sourceLabel(source);
      const remove = document.createElement('button'); remove.type = 'button'; remove.textContent = '\u00d7';
      remove.className = 'parquet-source-remove';
      remove.title = `Remove ${sourceLabel(source)}`; remove.setAttribute('aria-label', remove.title);
      remove.addEventListener('click', () => { state.sources.splice(i, 1); persist(); renderSelected(); });
      li.appendChild(remove); list.appendChild(li);
    });
    const instruments = new Set(state.sources.map(s => `${s.pack_root}|${s.instrument_id}`)).size;
    el('selectedCsvCount').textContent = `· ${instruments} instruments · ${state.sources.length} sources`;
    el('clearSelectedCsvBtn').disabled = state.sources.length === 0;
    summary();
  }
  function summary() {
    const warmup = Number(el('warmupBars')?.value || 1000);
    const startDate = el('startDate')?.value;
    const endDate = el('endDate')?.value;
    const start = startDate ? startDate + (el('startTime')?.value ? 'T' + el('startTime').value + 'Z' : '') : '';
    const end = endDate ? endDate + (el('endTime')?.value ? 'T' + el('endTime').value + 'Z' : '') : '';
    const filter = Boolean(el('dateFilter')?.checked);
    const help = 'Parquet requires Backtester V2. Metadata is advisory; execution rechecks candles. Opens use the UTC epoch grid; a multi-day bar may close after End. 1000 daily warmup bars need roughly 2.7 years.';
    const describe = (instrument, minutes) => {
      if (!instrument) return 'Metadata unavailable; refresh this pack in the picker.';
      const step = minutes * 60000;
      const parse = text => Date.parse(/^\d{4}-\d{2}-\d{2}$/.test(text) ? text + 'T00:00:00Z' : text);
      const first = Math.ceil(parse(instrument.first_open_utc) / step) * step;
      const closure = parse(instrument.verification.closed_before_utc);
      const last = Math.floor(Math.min(parse(instrument.coverage_end_utc), closure) / step) * step - step;
      const chosenStart = filter && start ? Math.ceil(parse(start) / step) * step : first;
      const endInstant = filter && end ? parse(end) + (/^\d{4}-\d{2}-\d{2}$/.test(end) ? 86400000 - 1 : 0) : last;
      const chosenEnd = Math.floor(endInstant / step) * step;
      const count = filter && start ? warmup : 0;
      const iso = value => Number.isFinite(value) && Math.abs(value) <= 8640000000000000 ? new Date(value).toISOString().replace('T', ' ').replace(':00.000Z', '') : 'unavailable';
      return `${instrument.instrument_id}, ${label(minutes)}\nOpens: ${iso(chosenStart)} – ${iso(chosenEnd)} UTC. Warmup: ${count} bars from ${iso(chosenStart - count * step)} UTC.\nCoverage: ${iso(parse(instrument.first_open_utc))} – ${iso(parse(instrument.coverage_end_utc))} UTC; closed before ${iso(closure)} UTC. Pack-wide missing base bars: ${instrument.missing_bar_count}.`;
    };
    const lines = [help];
    model.selected.forEach(i => model.checked[i].forEach((checked, col) => { if (checked) lines.push(describe(model.instruments[i], model.durations[col])); }));
    if (el('parquetCoverage')) el('parquetCoverage').textContent = lines.join('\n');
    const selected = state.sources.map(source => describe(state.metadata.get(source.pack_root)?.instruments.find(i => i.instrument_id === source.instrument_id), source.timeframe_minutes));
    if (el('parquetSelectedCoverage')) el('parquetSelectedCoverage').textContent = [help, ...selected].join('\n\n');
  }
  async function refreshSelectedMetadata() {
    const sequence = state.sequence;
    for (const path of new Set(state.sources.map(s => s.pack_root))) {
      if (state.metadata.has(path)) continue;
      try {
        const response = await fetch(`/api/parquet/browse?path=${encodeURIComponent(path)}`);
        const payload = await response.json();
        if (sequence !== state.sequence || !state.mode) return;
        if (!response.ok) throw new Error(payload.error);
        if (payload.pack) state.metadata.set(path, payload.pack);
      } catch (error) {
        if (el('parquetSelectedCoverage') && sequence === state.sequence) el('parquetSelectedCoverage').textContent = error.message;
        return;
      }
    }
    summary();
  }
  function renderGrid() {
    const host = el('parquetGrid'); host.replaceChildren();
    const table = document.createElement('table'); table.className = 'parquet-table';
    const head = document.createElement('tr'); const name = document.createElement('th'); name.textContent = 'Instrument'; head.appendChild(name);
    model.durations.forEach((minutes, col) => {
      const th = document.createElement('th');
      const bulk = document.createElement('input'); bulk.type = 'checkbox'; bulk.title = `Toggle ${label(minutes)} for every instrument`;
      const checked = model.checked.filter(row => row[col]).length;
      bulk.checked = checked > 0 && checked === model.checked.length; bulk.indeterminate = checked > 0 && checked < model.checked.length;
      bulk.addEventListener('change', () => { model.bulk(col); renderGrid(); });
      const input = document.createElement('input'); input.type = 'text'; input.value = model.drafts.get(col) ?? label(minutes); input.className = 'parquet-timeframe';
      if (model.invalid.has(col)) input.setAttribute('aria-invalid', 'true');
      input.setAttribute('aria-label', `Timeframe column ${col + 1}`);
      input.addEventListener('focus', () => input.select());
      const commit = () => {
        try { model.commit(col, input.value); input.value = label(model.durations[col]); input.removeAttribute('aria-invalid'); showCsvBrowserError(''); persist(); summary(); }
        catch (error) { input.setAttribute('aria-invalid', 'true'); showCsvBrowserError(error.message); }
      };
      input.addEventListener('input', () => { model.invalid.set(col, 'Uncommitted timeframe'); model.drafts.set(col, input.value); });
      input.addEventListener('blur', commit);
      input.addEventListener('keydown', event => {
        if (event.key === 'Enter') { event.preventDefault(); commit(); }
        if (event.key === 'Escape') { event.preventDefault(); event.stopPropagation(); input.value = model.cancel(col); input.removeAttribute('aria-invalid'); showCsvBrowserError(''); }
      });
      th.append(bulk, input); head.appendChild(th);
    });
    table.appendChild(head);
    model.instruments.forEach((instrument, i) => {
      const row = document.createElement('tr'); row.tabIndex = 0; row.classList.toggle('selected', model.selected.has(i));
      row.setAttribute('aria-selected', String(model.selected.has(i)));
      const select = event => {
        if (event.shiftKey) {
          if (!event.ctrlKey) model.selected.clear();
          for (let n = Math.min(model.anchor, i); n <= Math.max(model.anchor, i); n++) model.selected.add(n);
        } else if (event.ctrlKey || event.metaKey) {
          if (model.selected.has(i)) model.selected.delete(i); else model.selected.add(i);
          model.anchor = i;
        } else { model.selected.clear(); model.selected.add(i); model.anchor = i; }
        table.querySelectorAll('tr[tabindex]').forEach((r, n) => { r.classList.toggle('selected', model.selected.has(n)); r.setAttribute('aria-selected', String(model.selected.has(n))); });
        summary();
      };
      row.addEventListener('click', select);
      row.addEventListener('keydown', event => {
        if (event.target !== row) return;
        if (event.key === ' ' || event.key === 'Enter') { event.preventDefault(); select(event); }
        if (event.key === 'ArrowDown' || event.key === 'ArrowUp') {
          event.preventDefault(); const next = table.querySelectorAll('tr[tabindex]')[i + (event.key === 'ArrowDown' ? 1 : -1)];
          if (next) { next.focus(); if (event.shiftKey) next.dispatchEvent(new MouseEvent('click', { bubbles: true, shiftKey: true, ctrlKey: event.ctrlKey })); }
        }
      });
      const title = document.createElement('td'); title.textContent = instrument.instrument_id; row.appendChild(title);
      model.durations.forEach((_, col) => {
        const cell = document.createElement('td'); const check = document.createElement('input'); check.type = 'checkbox'; check.checked = model.checked[i][col];
        check.setAttribute('aria-label', `${instrument.instrument_id} ${label(model.durations[col])}`);
        check.addEventListener('click', e => e.stopPropagation());
        check.addEventListener('change', () => { model.checked[i][col] = check.checked; summary(); const bulk = head.children[col + 1].firstChild; const n = model.checked.filter(r => r[col]).length; bulk.checked = n === model.checked.length; bulk.indeterminate = n > 0 && n < model.checked.length; });
        cell.appendChild(check); row.appendChild(cell);
      });
      table.appendChild(row);
    });
    host.appendChild(table); summary();
  }
  async function load(path) {
    const sequence = ++state.sequence; showCsvBrowserError('');
    state.pack = null; model.reset([]);
    el('parquetGrid').hidden = true;
    el('parquetCoverage').hidden = true;
    el('csvBrowserAddBtn').disabled = true;
    try {
      const response = await fetch(`/api/parquet/browse?path=${encodeURIComponent(path || '')}`);
      const payload = await response.json(); if (!response.ok) throw new Error(payload.error);
      if (sequence !== state.sequence || (!state.mode && !state.packChoice)) return;
      csvBrowserState.currentPath = payload.current_path; csvBrowserState.entries = payload.entries;
      el('csvBrowserPath').value = payload.current_path;
      if (!state.packChoice) state.paths.parquet = payload.current_path;
      state.pack = payload.pack;
      if (payload.pack) state.metadata.set(payload.current_path, payload.pack);
      renderCsvBrowserEntries(payload.entries);
      if (!state.packChoice) { model.reset(payload.pack?.instruments || []); renderGrid(); }
      el('csvBrowserList').hidden = Boolean(payload.pack);
      el('parquetGrid').hidden = !payload.pack || Boolean(state.packChoice);
      el('parquetCoverage').hidden = !payload.pack || Boolean(state.packChoice);
      el('csvBrowserOpenBtn').disabled = Boolean(payload.pack);
      el('csvBrowserAddBtn').disabled = !payload.pack;
      el('csvBrowserHelp').textContent = payload.pack
        ? (state.packChoice ? 'Use this pack, then choose one instrument in the filter editor.' : 'Select instruments with click, Ctrl/Shift-click or the keyboard, check timeframes, then Add Selected. Edit a timeframe label directly.')
        : payload.entries.length
          ? 'Open a pack directory containing manifest.json. Instruments appear when the pack is opened.'
          : 'No pack manifest or subfolders here. Use Up to return to the pack root; internal ohlcv folders are not selectable sources.';
      persist();
    } catch (error) { if (sequence === state.sequence) showCsvBrowserError(error.message); }
  }
  async function add() {
    try {
      if (!state.pack) throw new Error('Open a valid pack directory before adding or locating a source.');
      if (state.packChoice) {
        state.packChoice(csvBrowserState.currentPath, state.pack);
        closeCsvBrowserModal(); return;
      }
      if (state.relocation) {
        const response = await fetch(`/api/studies/${encodeURIComponent(state.relocation.study)}/locate-parquet-pack`, {
          method: 'POST', headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ packRoot: csvBrowserState.currentPath, testId: state.relocation.testId, dependency: state.relocation.dependency })
        });
        const result = await response.json(); if (!response.ok) throw new Error(result.error);
        window.location.href = '/results?study=' + encodeURIComponent(state.relocation.study);
        return;
      }
      const added = model.sources(csvBrowserState.currentPath);
      const unique = new Map([...state.sources, ...added].map(s => [identity(s), s]));
      state.sources = [...unique.values()]; el('csvDirectory').value = state.paths.parquet;
      persist(); renderSelected(); closeCsvBrowserModal();
    } catch (error) { showCsvBrowserError(error.message); }
  }
  function restore(sources) {
    if (sources.every(s => s.type === 'path')) {
      setMode(false);
      const paths = sources.map(s => s.path);
      const parent = getPathParentDirectory(paths[0]);
      if (parent) { state.paths.csv = parent; el('csvDirectory').value = parent; }
      setSelectedCsvPaths(paths); persist(); return;
    }
    if (!sources.every(validSource)) throw new Error('Mixed or invalid source selections cannot be edited in Start. Queue sources were preserved.');
    state.sources = sources.map(s => ({ ...s })); state.paths.parquet = sources[0]?.pack_root || '';
    setMode(true); el('csvDirectory').value = state.paths.parquet; persist(); renderSelected();
  }
  root.ParquetInput = { ...api, state, model, setMode, renderSelected, load, add, restore, summary,
    active: () => state.mode || Boolean(state.packChoice),
    choosePack: async (path, callback) => {
      state.packChoice = callback;
      await openCsvBrowserModal();
      if (path) await load(path);
      el('csvBrowserAddBtn').textContent = 'Use pack';
    },
    sources: () => state.sources.map(s => ({ ...s })),
    close: () => { state.sequence++; state.packChoice = null; el('csvBrowserAddBtn').textContent = 'Add Selected'; model.invalid.clear(); model.drafts.clear(); },
    clear: () => { state.sources = []; persist(); renderSelected(); } };
  document.addEventListener('DOMContentLoaded', () => {
    state.paths.csv = el('csvDirectory')?.value || '';
    el('useParquet')?.addEventListener('change', event => setMode(event.target.checked));
    ['start', 'end', 'startDate', 'endDate', 'startTime', 'endTime', 'dateFilter', 'warmupBars'].forEach(id => el(id)?.addEventListener('change', summary));
    const query = new URLSearchParams(window.location.search);
    if (query.has('locatePack')) {
      state.relocation = { study: query.get('locatePack'), testId: query.get('testId') ? Number(query.get('testId')) : null, dependency: query.get('dependency') || 'target' };
      state.paths.parquet = query.get('packRoot') || '';
      setMode(true);
      el('csvBrowserAddBtn').textContent = 'Verify and use pack';
      setTimeout(() => openCsvBrowserModal(), 0);
    }
  });
})(typeof window !== 'undefined' ? window : globalThis);
