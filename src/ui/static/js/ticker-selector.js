(function () {
  const defaults = {version: 1, enabled: false, method: 'meanBarRangePct', methodVersion: 1,
    topN: 8, rebalanceMonths: 1, evaluateUnselected: false, params: {lookbackMonths: 4, timeframeMinutes: 30}};
  let retained = structuredClone(defaults);
  const el = id => document.getElementById(id);
  function capable() {
    const c = window.currentStrategyConfig;
    return c?.engine === 'v2' && c?.execution?.entryAdmission === 'risk_bracket_v1' && c?.execution?.boundary === 'strict_close';
  }
  function sync() {
    const root = el('tickerSelectorEditor');
    if (!root) return;
    root.hidden = !capable() && !retained.enabled;
    const enabled = el('enableTickerSelector').checked;
    el('tickerSelectorSettings').hidden = !enabled;
    el('tickerSelectorError').textContent = enabled && !capable()
      ? 'Ticker Selector requires a V2 pack-based risk-bracket strategy. Turn it Off or choose a supported strategy.' : '';
    root.querySelectorAll('input, select').forEach(control => { control.disabled = control.id !== 'enableTickerSelector' && !capable(); });
  }
  function read() {
    if (!el('enableTickerSelector')) return structuredClone(defaults);
    const text = el('selectorTimeframe').value.trim();
    const match = /^(\d+)(m|h|d)$/i.exec(text);
    const minutes = match ? Number(match[1]) * ({m:1,h:60,d:1440}[match[2].toLowerCase()]) : NaN;
    return {...retained, enabled: el('enableTickerSelector').checked, topN: Number(el('selectorTopN').value),
      rebalanceMonths: Number(el('selectorRebalance').value), evaluateUnselected: el('selectorUnselected').checked,
      params: {...retained.params, lookbackMonths: Number(el('selectorLookback').value), timeframeMinutes: minutes}};
  }
  function restore(value) {
    retained = structuredClone(value || defaults);
    if (!el('enableTickerSelector')) return;
    el('enableTickerSelector').checked = !!retained.enabled;
    el('selectorTopN').value = retained.topN ?? 8;
    el('selectorRebalance').value = retained.rebalanceMonths ?? 1;
    el('selectorUnselected').checked = !!retained.evaluateUnselected;
    el('selectorLookback').value = retained.params?.lookbackMonths ?? 4;
    el('selectorTimeframe').value = `${retained.params?.timeframeMinutes ?? 30}m`;
    sync();
  }
  async function execute(item, signal, runId) {
    const form = new FormData();
    form.append('strategy', item.strategyId);
    form.append('config', JSON.stringify(item.config));
    form.append('tickerSelector', JSON.stringify(item.tickerSelector));
    form.append('sources', JSON.stringify(item.sources));
    form.append('warmupBars', String(item.warmupBars));
    form.append('runId', runId);
    if (item.index) form.append('queueNumber', String(item.index));
    if (item.dbTarget) form.append('dbTarget', item.dbTarget);
    if (item.mode === 'wfa') {
      form.append('wf_enabled', 'true');
      appendQueueWfaPeriodFields(form, item);
      form.append('wf_adaptive_mode', item.wfa?.adaptiveMode ? 'true' : 'false');
      form.append('wf_store_top_n_trials', String(item.wfa?.storeTopNTrials ?? 50));
    }
    const response = await fetch('/api/portfolio', {method:'POST', body:form, signal});
    const data = await response.json();
    if (!response.ok) throw new Error(data.error || 'Portfolio failed.');
    return data;
  }
  window.TickerSelector = {read, restore, sync, execute};
  document.addEventListener('DOMContentLoaded', () => {
    restore();
    el('tickerSelectorEditor')?.addEventListener('change', () => { retained = read(); sync(); });
    el('strategySelect')?.addEventListener('change', sync);
  });
})();
