/* Saved portfolio scopes extend the existing table and chart; Python owns math. */
(function () {
  let current = null, outerChecks = null, rows = [], key = '', epoch = 0, curveEpoch = 0;
  let showUnselected = false, hasUnselected = false, lastState = null, lastCallbacks = null, restoredChecks = null;
  const el = id => document.getElementById(id);
  const escape = value => String(value ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const percent = value => value == null ? 'N/A' : `${Number(value).toFixed(1)}%`;
  async function get(url, options) {
    const response = await fetch(url, options), value = await response.json();
    if (!response.ok) throw new Error(value.error || 'Saved portfolio is unavailable.');
    return value;
  }
  function context(state) {
    const scope = window.AnalyticsSets.getPortfolioScope();
    if (scope) return {scope, portfolios:[]};
    if (window.AnalyticsSets.getViewMode() !== 'setCheckboxes') return null;
    const portfolios = window.AnalyticsSets.getCheckedPortfolioPeriods();
    return portfolios.length ? {scope:null, portfolios} : null;
  }
  function setsChanged(state, event) {
    const next = context(state);
    if (next?.scope) restoredChecks = null;
    if (next?.scope && !current?.scope && outerChecks === null) outerChecks = new Set(state.checkedStudyIds);
    if (!next?.scope && current?.scope && outerChecks !== null) {
      state.checkedStudyIds = outerChecks; outerChecks = null;
      restoredChecks = new Set(state.checkedStudyIds);
      if (event) event.syncCheckedStudyIds = null;
    }
    if (next || current) {
      ['studyEquityRequestToken','setEquityRequestToken','allStudiesEquityRequestToken','portfolioRequestToken','focusedWindowBoundariesRequestToken'].forEach(k=>state[k]++);
      if (event) event.syncCheckedStudyIds = null;
      state.focusedStudyId = null;
    }
    current = next;
    if (!next) { key = ''; rows = []; epoch++; curveEpoch++; }
  }
  function drawTable(state, callbacks) {
    const toggle = el('portfolioShowUnselected');
    if (toggle) { toggle.closest('label').hidden = !hasUnselected; toggle.checked = showUnselected; }
    const displayed = rows.filter(row => !current.scope?.period || (showUnselected && hasUnselected) || row.portfolioRank != null);
    const visibleIds = new Set(displayed.map(r=>r.study_id));
    state.checkedStudyIds = new Set([...state.checkedStudyIds].filter(id=>visibleIds.has(id)));
    if (!visibleIds.has(state.focusedStudyId)) state.focusedStudyId = null;
    window.AnalyticsTable.renderTable(displayed, state.checkedStudyIds, callbacks.selection, {
      filters:{}, visibleStudyIds:null, autoSelect:false, groupDatesEnabled:false, sortState:state.sortState,
      onSortChange:callbacks.sort, onFocusToggle:callbacks.focus, onViewChange:callbacks.view,
      focusedStudyId:state.focusedStudyId
    });
    state.checkedStudyIds = new Set(window.AnalyticsTable.getCheckedStudyIds());
    state.orderedStudyIds = window.AnalyticsTable.getOrderedStudyIds();
    visuals(state);
  }
  function renderTable(state, callbacks) {
    if (!current) return false;
    lastState = state; lastCallbacks = callbacks;
    const ordinary = current.scope ? [] : state.sets.filter(s=>s.kind !== 'portfolio' && state.checkedSetIds.has(s.id)).flatMap(s=>s.study_ids);
    const nextKey = JSON.stringify([state.dbName,current,ordinary]);
    if (nextKey === key) { drawTable(state, callbacks); return true; }
    key = nextKey;
    const token = ++epoch;
    ++curveEpoch; rows = []; hasUnselected = false; state.focusedStudyId = null;
    drawTable(state, callbacks);
    Promise.all(current.scope
      ? [get(`/api/portfolios/${encodeURIComponent(current.scope.id)}${current.scope.period ? '?period='+current.scope.period : ''}`)]
      : current.portfolios.map(p=>get(`/api/portfolios/${encodeURIComponent(p.id)}`)))
      .then(values => {
        if (token !== epoch) return;
        if (current.scope) rows = values[0].rows;
        else rows = [...state.studies.filter(s=>ordinary.includes(s.study_id)),
          ...values.flatMap((v,i)=>v.periodRows.filter(row=>current.portfolios[i].periods.includes(row.portfolioSelection.periods[0])))];
        hasUnselected = !!current.scope?.period && values[0].selector.evaluateUnselected === true
          && rows.some(row=>row.portfolioRank == null);
        state.checkedStudyIds = restoredChecks || new Set(rows.filter(r=>!r.unavailable && (!current.scope?.period || r.portfolioRank != null)).map(r=>r.study_id));
        restoredChecks = null;
        const settings = el('portfolioSelectorSettings');
        if (settings) {
          settings.hidden = false;
          const configs = values.map(v=>JSON.stringify(v.selector)), cfg = values[0].selector;
          const entries = configs.every(v=>v===configs[0]) ? [
            ['Method', 'X — Mean Candle Range'], ['Top N', cfg.topN],
            ['Rebalance Every', `${cfg.rebalanceMonths} month(s)`],
            ['Evaluate Unselected', cfg.evaluateUnselected ? 'On' : 'Off'],
            ['Lookback', `${cfg.params.lookbackMonths} month(s)`],
            ['Ranking Timeframe', `${cfg.params.timeframeMinutes}m`]
          ] : [['Settings', 'Mixed']];
          settings.querySelector('.settings-list').innerHTML = entries.map(([label,value])=>
            `<div class="setting-item"><span class="key">${escape(label)}</span><span class="val">${escape(value)}</span></div>`).join('');
        }
        drawTable(state, callbacks);
      }).catch(error => { if (token === epoch) clear(error.message); });
    return true;
  }
  function clear(message='No data to display') {
    window.AnalyticsEquity?.renderChart([],[]);
    const title = el('analyticsChartTitle'); if (title) title.textContent = message;
    cards({});
  }
  function cards(data) {
    const target = el('analyticsSummaryRow'); if (!target) return;
    const entries = current?.scope ? [
      ['ANN.P%',percent(data.ann_profit_pct)],['NET PROFIT',percent(data.profit_pct)],['MAX DRAWDOWN',percent(data.max_drawdown_pct)],
      ['TOTAL TRADES',data.total_trades == null ? 'N/A' : `${data.winning_trades ?? 0}/${data.total_trades}`],
      ['WFE',percent(data.wfe)],['OOS WINS',percent(data.oos_win_rate)],['OOS PROFIT (MED)',percent(data.oos_profit_median)],
      ['OOS WIN RATE (MED)',percent(data.oos_wr_median)]] : [
      ['Portfolio Ann.P%',percent(data.ann_profit_pct)],['Portfolio Profit',percent(data.profit_pct)],['Portfolio MaxDD',percent(data.max_drawdown_pct)],
      ['Total Trades',data.total_trades ?? 'N/A'],['Profitable',percent(data.profitable_pct)],['Avg OOS Wins',percent(data.oos_win_rate)],
      ['Avg WFE',percent(data.wfe)],['Avg OOS P(med)',percent(data.oos_profit_median)]];
    target.innerHTML = entries.map(([label,value])=>`<div class="summary-card"><div class="value">${escape(value)}</div><div class="label">${label}</div></div>`).join('')
      + (window.EntryDiagnostics?.card(data.entry_diagnostics) || '');
  }
  function visuals(state) {
    if (!current) {
      if (el('portfolioSelectorSettings')) el('portfolioSelectorSettings').hidden = true;
      if (el('portfolioShowUnselected')) el('portfolioShowUnselected').closest('label').hidden = true;
      return false;
    }
    const focus = rows.find(r=>r.study_id===state.focusedStudyId);
    const selected = focus ? [focus] : rows.filter(r=>state.checkedStudyIds.has(r.study_id));
    const token = ++curveEpoch;
    clear(focus?.unavailable ? (focus.reason || 'No saved result') : 'Loading…');
    if (focus?.unavailable || !selected.length) { clear(focus?.reason || 'No data to display'); return true; }
    const payload = {studyIds:selected.filter(r=>!r.portfolio_id).map(r=>r.study_id),
      portfolios:selected.filter(r=>r.portfolio_id).map(r=>({id:r.portfolio_id,selection:r.portfolioSelection}))};
    get('/api/portfolios/equity',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(payload)})
      .then(data=>{
        if (token!==curveEpoch) return;
        cards(data);
        window.AnalyticsEquity.renderChart(data.curve || [],data.timestamps || []);
        const title = el('analyticsChartTitle');
        if (title) title.textContent = focus?.study_name || current.scope?.name || 'Selected studies';
        const meta = el('analyticsChartMeta'); if (meta) meta.textContent = data.depleted ? 'Depleted return index' : '';
      }).catch(error=>{if(token===curveEpoch) clear(error.message);});
    return true;
  }
  function setShowUnselected(value) {
    showUnselected = !!value;
    if (lastState && current) drawTable(lastState,lastCallbacks);
  }
  function describe(state) {
    const focused = state.sets.find(s=>s.id === state.focusedSetId);
    const population = focused ? [focused] : window.AnalyticsSets.getViewMode() === 'setCheckboxes'
      ? state.sets.filter(s=>state.checkedSetIds.has(s.id)) : [];
    const descriptions = population.map(s=>s.selector?.enabled
      ? `X · Top ${s.selector.topN} · Every ${s.selector.rebalanceMonths}m · ${s.selector.params.lookbackMonths}m / ${s.selector.params.timeframeMinutes}m` : 'Off');
    return new Set(descriptions).size > 1 ? 'Mixed' : descriptions[0] || 'Off';
  }
  function reset() { current = null; outerChecks = null; restoredChecks = null; rows = []; hasUnselected = false; key = ''; epoch++; curveEpoch++; }
  window.AnalyticsPortfolio = {setsChanged,renderTable,visuals,active:()=>!!current,rows:()=>rows,setShowUnselected,describe,reset};
  document.addEventListener('DOMContentLoaded',()=>el('portfolioShowUnselected')?.addEventListener('change',e=>setShowUnselected(e.target.checked)));
})();
