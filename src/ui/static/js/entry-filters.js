(function (root) {
  'use strict';
  const off = () => ({ version: 1, enabled: false, items: [] });
  const copy = value => JSON.parse(JSON.stringify(value));
  const stable = value => JSON.stringify(value, (_, v) => v && typeof v === 'object' && !Array.isArray(v)
    ? Object.keys(v).sort().reduce((o, k) => { o[k] = v[k]; return o; }, {}) : v);
  const exactKeys = (value, keys) => value && !Array.isArray(value) && typeof value === 'object'
    && Object.keys(value).every(k => keys.includes(k));
  function locator(value) {
    if (typeof value !== 'string' || !value.trim()) throw Error('Choose an absolute source pack directory.');
    const windows = /^[a-z]:[\\/]/i.test(value) || /^\\\\/.test(value) || /^\/\/[^/]/.test(value);
    let path = windows ? value.replace(/\\/g, '/').toLowerCase() : value;
    if (!windows && !path.startsWith('/')) throw Error('Source directory must be absolute.');
    const prefix = windows && !/^[a-z]:/i.test(path) ? '//' : path.startsWith('/') ? '/' : '';
    const parts = [];
    const anchor = windows ? (prefix === '//' ? 2 : 1) : 0;
    path.split('/').forEach(part => { if (!part || part === '.') return; if (part === '..') { if (parts.length > anchor) parts.pop(); } else parts.push(part); });
    return prefix + parts.join('/') + (windows && prefix === '' && parts.length === 1 ? '/' : '');
  }
  function duration(value) {
    const match = String(value).trim().match(/^(\d+)(m|h|d)$/i);
    if (!match) throw Error('Timeframe needs an integer and m, h or D.');
    const minutes = Number(match[1]) * ({ m: 1, h: 60, d: 1440 }[match[2].toLowerCase()]);
    if (!Number.isSafeInteger(minutes) || minutes <= 0 || minutes % 5 || minutes > 153722867) throw Error('Timeframe must be a supported positive multiple of 5m.');
    return minutes;
  }
  const tf = n => typeof n !== 'number' ? String(n ?? '') : n % 1440 === 0 ? `${n / 1440}D` : n % 60 === 0 ? `${n / 60}h` : `${n}m`;
  function effective(editor) {
    if (editor === undefined) return [];
    if (!exactKeys(editor, ['version', 'enabled', 'items']) || editor.version !== 1 || typeof editor.enabled !== 'boolean' || !Array.isArray(editor.items)) throw Error('Unsupported Entry Filters configuration.');
    const ids = new Set(), values = new Map();
    editor.items.forEach(item => {
      if (!item || typeof item.id !== 'string' || !item.id.trim() || ids.has(item.id)) throw Error('Filter IDs must be unique.');
      ids.add(item.id);
      if (!editor.enabled) return;
      if (typeof item.enabled !== 'boolean') throw Error('Invalid filter enabled state.');
      if (!item.enabled) return;
      if (!exactKeys(item, ['id', 'type', 'version', 'enabled', 'parameters', 'source']) || item.type !== 'candleDirection' || ![1, 2].includes(item.version)) throw Error('Unsupported active filter method or version.');
      if (!exactKeys(item.parameters, item.version === 1 ? ['confirmationBars'] : ['confirmationBars', 'breakBars']) || !Number.isSafeInteger(item.parameters.confirmationBars) || item.parameters.confirmationBars < 2) throw Error('Trend Confirm Bars must be an integer >= 2.');
      if (item.version === 2 && (item.parameters.confirmationBars > 6 || !Number.isInteger(item.parameters.breakBars)
          || item.parameters.breakBars < 1 || item.parameters.breakBars > 4 || item.parameters.breakBars >= item.parameters.confirmationBars)) throw Error('v2 requires Confirm Bars 2..6 and Break Bars 1..4, strictly below Confirm Bars.');
      const s = item.source;
      if (!exactKeys(s, ['type', 'version', 'pack_root', 'instrument_id', 'timeframe_minutes']) || s.type !== 'parquet' || s.version !== 1
          || typeof s.instrument_id !== 'string' || !/^[A-Z0-9]+_[A-Z0-9][A-Z0-9._-]*$/.test(s.instrument_id)) throw Error('Choose a valid Parquet instrument.');
      const normalized = { type: item.type, version: item.version, parameters: copy(item.parameters),
        source: { ...s, pack_root: locator(s.pack_root), timeframe_minutes: duration(`${s.timeframe_minutes}m`) } };
      values.set(stable(normalized), normalized);
    });
    return [...values.keys()].sort().map(key => values.get(key));
  }
  function describe(items, detailed = false) {
    if (!items.length) return 'Filter Off';
    return items.map(item => `F06${detailed ? ` · Candle Direction v${item.version}` : item.version === 2 ? ' v2' : ''} · ${item.source.instrument_id} · ${tf(item.source.timeframe_minutes)} · ${item.version === 2 || detailed ? `N ${item.parameters.confirmationBars} / break ${item.parameters.breakBars ?? 1}` : `${item.parameters.confirmationBars} bars`}${detailed ? '\n' + item.source.pack_root : ''}`).join('\n');
  }
  function stamp(value) {
    if (typeof value !== 'string') throw Error('Invalid timestamp');
    const fraction = value.match(/\.(\d+)(?=Z$|[+-]\d\d:\d\d$)/);
    if (fraction && fraction[1].length > 9) throw Error('Invalid timestamp precision');
    const ms = Date.parse(fraction ? value.replace(fraction[0], '') : value);
    if (!Number.isFinite(ms)) throw Error('Invalid timestamp');
    const ns = BigInt(ms) * 1000000n + BigInt((fraction?.[1] || '').padEnd(9, '0'));
    if (ns <= -9223372036854775808n || ns > 9223372036854775807n) throw Error('Timestamp out of range');
    return ns;
  }
  const floor = (n, d) => n / d - (n % d < 0n ? 1n : 0n);
  function saved(config = {}) {
    try {
      if (!('entryFilters' in config) && !('entry_filter_inputs' in config)) return { key: 'off', text: 'Filter Off', details: 'Filter Off' };
      if (!('entryFilters' in config)) throw Error('Missing settings');
      const items = effective(config.entryFilters), pin = config.entry_filter_inputs;
      if (!pin || pin.version !== 1) throw Error('Missing input facts');
      if (!items.length) {
        if (stable(pin) !== stable({ version: 1, state: 'off' })) throw Error('Off conflict');
      } else {
        if (pin.state !== 'filtered' || stable(pin.effective) !== stable(items) || !Array.isArray(pin.sources) || pin.sources.length !== items.length) throw Error('Input conflict');
        pin.sources.forEach((source, i) => {
          const item = items[i], step = BigInt(item.source.timeframe_minutes) * 60000000000n;
          const rule = item.version === 2 && item.parameters.breakBars > 1 ? 'declared_source_origin' : 'bounded_prefix';
          if (stable(source.descriptor) !== stable(item) || source.policy !== `closed_candle_direction_v${item.version}`
              || source.fingerprint_version !== 1 || !/^[a-f0-9]{64}$/.test(source.input_fingerprint)
              || !source.bounds || source.bounds.origin !== source.bounds.start
              || source.bounds.priorBars !== Math.max(28, items[i].parameters.confirmationBars - 1)
              || !(stamp(source.bounds.start) < stamp(source.bounds.end))
              || stamp(source.bounds.start) % step !== 0n || stamp(source.bounds.end) % step !== 0n
              || (item.version === 2 ? source.originRule !== rule : 'originRule' in source)
              || !source.instrument || typeof source.instrument !== 'object' || Array.isArray(source.instrument)) throw Error('Invalid source facts');
          const target = config.data_input;
          if (item.version === 2 && !target) throw Error('Missing target input');
          if (target) {
            const targetStep = BigInt(duration(`${target.source.timeframe_minutes}m`)) * 60000000000n;
            const start = floor(stamp(target.bounds.start) + targetStep, step) * step - BigInt(source.bounds.priorBars + 1) * step;
            const end = floor(stamp(target.bounds.end), step) * step;
            if (start <= -9223372036854775808n || start >= end || end > 9223372036854775807n
                || stamp(source.bounds.end) !== end
                || (rule === 'declared_source_origin' ? stamp(source.bounds.start) > start : stamp(source.bounds.start) !== start)) throw Error('Filter bounds conflict with target input');
          }
        });
      }
      return { key: items.length ? stable(items) : 'off', text: describe(items), details: describe(items, true) };
    } catch (_) { return { key: null, text: 'Unavailable', details: 'Entry Filters unavailable: missing, conflicting or unsupported saved context.' }; }
  }
  function consensus(studies) {
    if (!studies.length) return '?';
    const values = studies.map(s => s.entry_filters || saved(s.config_json || {}));
    if (values.some(v => v.key === null)) return 'Unavailable';
    return values.every(v => v.key === values[0].key) ? values[0].text : 'Mixed';
  }
  function population({ studies, context, focusedId, mode, sets, focusedSetId, checkedSetIds }) {
    const map = new Map(studies.map(s => [String(s.study_id), s]));
    if (focusedId && map.has(String(focusedId))) return [map.get(String(focusedId))];
    if (mode === 'setFocus' || mode === 'setCheckboxes') {
      const chosen = mode === 'setFocus' ? sets.filter(s => String(s.id) === String(focusedSetId))
        : sets.filter(s => [...checkedSetIds].some(id => String(id) === String(s.id)));
      return [...new Set(chosen.flatMap(s => s.study_ids || []).map(String))].filter(id => map.has(id)).map(id => map.get(id));
    }
    return context;
  }
  const api = { off, stable, locator, duration, effective, saved, consensus, population, describe };
  if (typeof module !== 'undefined') module.exports = api;
  root.EntryFilters = api;
  if (!root.document) return;
  let editor = off(), sequence = 0;
  const opened = new Set(), metadata = new Map();
  const host = () => document.getElementById('entryFiltersEditor');
  const supported = () => root.currentStrategyConfig?.entryFilterSupport?.version === 1
    && root.currentStrategyConfig.entryFilterSupport.methods?.includes('candleDirection');
  const node = (tag, text) => { const n = document.createElement(tag); if (tag === 'button') n.className = 'btn-secondary'; if (text !== undefined) n.textContent = text; return n; };
  function changed() { root.resetGridPreviewState?.(); root.scheduleGridPreviewUpdate?.(); }
  function errorText(message) { const el = document.getElementById('entryFiltersError'); if (el) el.textContent = message; }
  async function loadPack(item, path) {
    const response = await fetch(`/api/parquet/browse?path=${encodeURIComponent(path)}`);
    const value = await response.json();
    if (!response.ok || !value.pack) throw Error(value.error || 'Choose a directory containing a pack manifest.');
    if (!editor.items?.includes(item) || item.source.pack_root !== path) return;
    metadata.set(path, value.pack); render();
  }
  function render() {
    const container = host(); if (!container) return;
    container.replaceChildren();
    const heading = node('div'); heading.className = 'entry-filter-heading';
    const master = node('input'); master.type = 'checkbox'; master.id = 'entryFiltersEnabled'; master.checked = editor?.enabled === true;
    const label = node('label', ' Entry Filters'); label.htmlFor = master.id;
    master.onchange = () => { if (editor && typeof editor === 'object') editor.enabled = master.checked; render(); changed(); };
    const add = node('button', '+ Add Filter'); add.type = 'button'; add.disabled = !supported() || !editor?.enabled;
    add.onclick = () => { const item = { id: `filter-${Date.now()}-${++sequence}`, type: 'candleDirection', version: 2, enabled: true,
      parameters: { confirmationBars: 2, breakBars: 1 }, source: { type: 'parquet', version: 1, pack_root: '', instrument_id: 'OKX_BTC-USDT-SWAP', timeframe_minutes: 240 } };
      editor.items.push(item); opened.add(item.id); render(); changed(); };
    heading.append(master, label, add); container.append(heading);
    if (!supported()) container.append(node('small', 'This strategy does not support entry filters. Turn Entry Filters Off to launch.'));
    const error = node('div'); error.id = 'entryFiltersError'; error.className = 'csv-browser-error'; container.append(error);
    if (!editor || !Array.isArray(editor.items)) { error.textContent = 'Unsupported saved configuration: ' + JSON.stringify(editor); return; }
    if (!editor.enabled) return;
    editor.items.forEach(item => {
      if (!item || typeof item !== 'object' || Array.isArray(item)) {
        const invalid = node('div', 'Unsupported saved filter: ' + JSON.stringify(item));
        const remove = node('button', 'Remove'); remove.type = 'button'; remove.disabled = !supported();
        remove.onclick = () => { editor.items = editor.items.filter(i => i !== item); render(); changed(); };
        invalid.append(remove); container.append(invalid); return;
      }
      const card = node('details'); card.className = 'entry-filter-item'; card.open = opened.has(item.id);
      card.ontoggle = () => { if (card.open) opened.add(item.id); else opened.delete(item.id); };
      const summary = node('summary');
      const toggle = node('input'); toggle.type = 'checkbox'; toggle.checked = item.enabled === true; toggle.disabled = !supported();
      toggle.onclick = event => event.stopPropagation(); toggle.onchange = () => { item.enabled = toggle.checked; changed(); };
      let title = item.type === 'candleDirection' ? `F06 · ${item.source?.instrument_id || 'Choose instrument'} · ${tf(item.source?.timeframe_minutes || 240)} · N ${item.parameters?.confirmationBars ?? 2} / break ${item.parameters?.breakBars ?? 1} (v${item.version})` : `Unsupported filter · ${item.type || '?'}`;
      const remove = node('button', 'Remove'); remove.type = 'button'; remove.disabled = !supported();
      remove.onclick = event => { event.preventDefault(); editor.items = editor.items.filter(i => i !== item); render(); changed(); };
      summary.append(toggle, node('span', title), remove); card.append(summary);
      const controls = node('fieldset'); controls.disabled = !supported();
      if (item.type !== 'candleDirection' || ![1, 2].includes(item.version) || !item.source || !item.parameters) {
        controls.append(node('pre', JSON.stringify(item, null, 2)));
      } else {
        const path = node('input'); path.type = 'text'; path.value = item.source.pack_root; path.setAttribute('aria-label', 'Source pack');
        path.onchange = () => { item.source.pack_root = path.value; changed(); loadPack(item, path.value).catch(e => errorText(e.message)); };
        const browse = node('button', 'Browse'); browse.type = 'button'; browse.onclick = () => root.ParquetInput.choosePack(path.value, (rootPath, pack) => {
          item.source.pack_root = rootPath; metadata.set(rootPath, pack); render(); changed(); });
        const packLine = node('div'); packLine.className = 'entry-filter-source'; packLine.append(path, browse);
        controls.append(node('label', 'Source pack'), packLine);
        const instrument = node('select'); instrument.setAttribute('aria-label', 'Filter instrument');
        const pack = metadata.get(item.source.pack_root);
        const ids = (pack?.instruments || []).map(i => i.instrument_id);
        if (!ids.includes(item.source.instrument_id)) ids.unshift(item.source.instrument_id);
        ids.forEach(id => { const option = node('option', id); option.value = id; instrument.append(option); });
        instrument.value = item.source.instrument_id;
        instrument.onchange = () => { item.source.instrument_id = instrument.value; changed(); };
        controls.append(node('label', 'Instrument'), instrument);
        const line = node('div'); line.className = 'entry-filter-parameters';
        const timeframe = node('input'); timeframe.value = tf(item.source.timeframe_minutes); timeframe.setAttribute('aria-label', 'Filter timeframe');
        timeframe.onchange = () => { try { item.source.timeframe_minutes = duration(timeframe.value); timeframe.setCustomValidity(''); changed(); } catch (e) { item.source.timeframe_minutes = timeframe.value; timeframe.setCustomValidity(e.message); errorText(e.message); } };
        const count = node('input'); count.type = 'number'; count.min = '2'; count.step = '1'; count.value = item.parameters.confirmationBars; count.setAttribute('aria-label', 'Trend Confirm Bars');
        if (item.version === 2) count.max = '6';
        count.onchange = () => { item.parameters.confirmationBars = Number(count.value); changed(); };
        const opposite = node('input'); opposite.type = 'number'; opposite.min = '1'; opposite.max = '4'; opposite.step = '1'; opposite.value = item.parameters.breakBars ?? 1; opposite.setAttribute('aria-label', 'Trend Break Bars');
        opposite.onchange = () => {
          const n = item.parameters.confirmationBars, m = Number(opposite.value);
          if (item.version === 1 && (!Number.isInteger(n) || n < 2 || n > 6 || !Number.isInteger(m) || m < 1 || m > 4 || m >= n)) {
            opposite.value = 1; errorText('Choose Confirm Bars 2..6 and Break Bars 1..4 below Confirm Bars before converting v1 to v2.'); return;
          }
          item.version = 2; item.parameters.breakBars = m; render(); changed();
        };
        for (const [title, input] of [['Timeframe', timeframe], ['Trend Confirm Bars', count], ['Trend Break Bars', opposite]]) {
          const field = node('label'); field.append(node('span', title), input); line.append(field);
        }
        controls.append(line);
        controls.append(node('small', 'Closed ordinary candles; entries only.'));
        if (pack) { const info = pack.instruments.find(i => i.instrument_id === item.source.instrument_id);
          controls.append(node('small', info ? `Coverage: ${info.first_open_utc} — ${info.coverage_end_utc} (UTC)` : 'Instrument absent from this pack.'));
        }
      }
      card.append(controls); container.append(card);
    });
  }
  function dependencies(config, reset = false) {
    const select = document.getElementById('parquetDependency'); if (!select) return;
    const previous = reset ? 'target' : select.value; select.replaceChildren();
    const sources = config?.entry_filter_inputs?.sources;
    const roots = [...new Set((Array.isArray(sources) ? sources : []).map(p => p?.descriptor?.source?.pack_root).filter(Boolean))];
    [['target', 'Trading target'], ...roots.map(path => [path, `Filter source · ${path}`])].forEach(([value, text]) => {
      const option = node('option', text); option.value = value; select.append(option);
    });
    select.value = previous === 'target' || roots.includes(previous) ? previous : 'target'; select.hidden = !roots.length;
  }
  Object.assign(api, { render, dependencies, restore: value => { editor = copy(value === undefined ? off() : value); opened.clear(); render(); },
    collect: () => { const items = effective(editor); if (items.length && !supported()) throw Error('Entry Filters are active for an unsupported strategy. Turn them Off.'); return copy(editor); } });
  document.addEventListener('DOMContentLoaded', render);
})(typeof window !== 'undefined' ? window : globalThis);
