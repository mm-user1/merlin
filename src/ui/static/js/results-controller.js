let studiesListCache = [];
let studiesSortByNameActive = false;

function inferPostProcessSource(trials, key) {
  const values = new Set();
  (trials || []).forEach((trial) => {
    const value = trial ? trial[key] : null;
    if (value) values.add(value);
  });
  if (values.size === 1) {
    return Array.from(values)[0];
  }
  return null;
}

function buildRankMapFromKey(trials, rankKey) {
  const map = {};
  (trials || []).forEach((trial) => {
    if (!trial) return;
    const rank = trial[rankKey];
    if (rank !== null && rank !== undefined) {
      map[trial.trial_number] = rank;
    }
  });
  return map;
}

function parseTimeframeToMinutes(value) {
  const token = String(value || '').trim().toLowerCase();
  if (!token) return Number.POSITIVE_INFINITY;
  const compact = token.replace(/\s+/g, '');
  let match = compact.match(/^(\d+)(m|min|mins|minute|minutes)?$/);
  if (match) return Number(match[1]);
  match = compact.match(/^(\d+)(h|hr|hrs|hour|hours)$/);
  if (match) return Number(match[1]) * 60;
  match = compact.match(/^(\d+)(d|day|days)$/);
  if (match) return Number(match[1]) * 1440;
  match = compact.match(/^(\d+)(w|week|weeks)$/);
  if (match) return Number(match[1]) * 10080;
  return Number.POSITIVE_INFINITY;
}

function isAbsoluteFilesystemPath(path) {
  const value = String(path || '').trim();
  if (!value) return false;
  if (/^[A-Za-z]:[\\/]/.test(value)) return true; // Windows drive path
  if (/^\\\\[^\\]/.test(value)) return true; // UNC path
  if (value.startsWith('/')) return true; // POSIX path
  return false;
}

function extractDatasetPrefixFromStudy(study) {
  const csvFileName = String(study?.csv_file_name || '').trim();
  const source = csvFileName || String(study?.study_name || '').trim();
  if (!source) return '';
  const stem = source.replace(/\.[^.]+$/, '');
  const withoutStrategyPrefix = stem.replace(/^[^_]+_/, '');
  const dateMatch = withoutStrategyPrefix.match(/\b\d{4}[.\-/]\d{2}[.\-/]\d{2}\b/);
  if (!dateMatch || dateMatch.index === undefined) return withoutStrategyPrefix.trim();
  return withoutStrategyPrefix.slice(0, dateMatch.index).trim();
}

function extractStudySortIdentity(study) {
  const datasetPrefix = extractDatasetPrefixFromStudy(study);
  const commaIndex = datasetPrefix.indexOf(',');
  let symbolPart = datasetPrefix;
  let timeframePart = '';
  if (commaIndex >= 0) {
    symbolPart = datasetPrefix.slice(0, commaIndex).trim();
    timeframePart = datasetPrefix.slice(commaIndex + 1).trim().split(/\s+/)[0] || '';
  } else {
    const parts = datasetPrefix.split(/\s+/).filter(Boolean);
    if (parts.length > 1) {
      timeframePart = parts[parts.length - 1];
      symbolPart = parts.slice(0, -1).join(' ');
    }
  }
  const symbolTokens = symbolPart.split('_').filter(Boolean);
  const ticker = (symbolTokens[symbolTokens.length - 1] || symbolPart || '').toLowerCase();
  const timeframeRaw = String(timeframePart || '').trim().toLowerCase();
  return {
    ticker,
    timeframeRaw,
    timeframeMinutes: parseTimeframeToMinutes(timeframeRaw),
  };
}

function compareStudiesByTickerAndTimeframe(left, right) {
  const a = extractStudySortIdentity(left);
  const b = extractStudySortIdentity(right);

  const tickerCmp = a.ticker.localeCompare(b.ticker, undefined, { sensitivity: 'base' });
  if (tickerCmp !== 0) return tickerCmp;

  const aFinite = Number.isFinite(a.timeframeMinutes);
  const bFinite = Number.isFinite(b.timeframeMinutes);
  if (aFinite && bFinite && a.timeframeMinutes !== b.timeframeMinutes) {
    return a.timeframeMinutes - b.timeframeMinutes;
  }
  if (aFinite !== bFinite) return aFinite ? -1 : 1;

  const timeframeCmp = a.timeframeRaw.localeCompare(b.timeframeRaw, undefined, {
    numeric: true,
    sensitivity: 'base',
  });
  if (timeframeCmp !== 0) return timeframeCmp;

  const aName = String(left?.study_name || '');
  const bName = String(right?.study_name || '');
  return aName.localeCompare(bName, undefined, { numeric: true, sensitivity: 'base' });
}

function getStudiesForRender(studies) {
  const rows = Array.isArray(studies) ? studies.slice() : [];
  if (!studiesSortByNameActive) return rows;
  const enriched = rows.map((study, index) => ({ study, index }));
  enriched.sort((a, b) => {
    const cmp = compareStudiesByTickerAndTimeframe(a.study, b.study);
    return cmp !== 0 ? cmp : a.index - b.index;
  });
  return enriched.map((item) => item.study);
}

function updateTabsVisibility() {
  const tabs = document.querySelectorAll('.tab-btn');
  const dsrTab = document.querySelector('.tab-btn[data-tab="dsr"]');
  const forwardTab = document.querySelector('.tab-btn[data-tab="forward_test"]');
  const stressTab = document.querySelector('.tab-btn[data-tab="stress_test"]');
  const oosTab = document.querySelector('.tab-btn[data-tab="oos_test"]');
  const manualTab = document.querySelector('.tab-btn[data-tab="manual_tests"]');
  const tabsContainer = document.getElementById('resultsTabs');
  const manualBtn = document.getElementById('manualTestBtn');

  if (ResultsState.mode !== 'optuna') {
    if (tabsContainer) tabsContainer.style.display = 'none';
    if (manualBtn) manualBtn.style.display = 'none';
    return;
  }

  if (tabsContainer) tabsContainer.style.display = 'flex';

  const hasDsr = ResultsState.dsr.enabled && ResultsState.dsr.trials.length > 0;
  const hasForwardTest = ResultsState.forwardTest.enabled && ResultsState.forwardTest.trials.length > 0;
  const hasStressTest = ResultsState.stressTest.enabled && ResultsState.stressTest.trials.length > 0;
  const hasOosTest = ResultsState.oosTest.enabled && ResultsState.oosTest.trials.length > 0;
  const hasManualTests = ResultsState.manualTests.length > 0;

  if (dsrTab) dsrTab.style.display = hasDsr ? 'inline-flex' : 'none';
  if (forwardTab) forwardTab.style.display = hasForwardTest ? 'inline-flex' : 'none';
  if (stressTab) stressTab.style.display = hasStressTest ? 'inline-flex' : 'none';
  if (oosTab) oosTab.style.display = hasOosTest ? 'inline-flex' : 'none';
  if (manualTab) manualTab.style.display = hasManualTests ? 'inline-flex' : 'none';
  if (manualBtn) {
    manualBtn.style.display = ['optuna', 'dsr', 'forward_test', 'stress_test'].includes(ResultsState.activeTab)
      ? 'inline-flex'
      : 'none';
  }

  if (!hasDsr && ResultsState.activeTab === 'dsr') {
    ResultsState.activeTab = 'optuna';
  }
  if (!hasForwardTest && ResultsState.activeTab === 'forward_test') {
    ResultsState.activeTab = 'optuna';
  }
  if (!hasStressTest && ResultsState.activeTab === 'stress_test') {
    ResultsState.activeTab = 'optuna';
  }
  if (!hasOosTest && ResultsState.activeTab === 'oos_test') {
    ResultsState.activeTab = 'optuna';
  }
  if (!hasManualTests && ResultsState.activeTab === 'manual_tests') {
    ResultsState.activeTab = 'optuna';
  }

  tabs.forEach((tab) => {
    const tabId = tab.dataset.tab;
    tab.classList.toggle('active', tabId === ResultsState.activeTab);
  });
}

function setTableExpanded(expanded) {
  const scroll = document.querySelector('.table-scroll');
  const toggle = document.getElementById('tableExpandToggle');
  if (!scroll || !toggle) return;
  scroll.classList.toggle('expanded', expanded);
  toggle.dataset.expanded = expanded ? '1' : '0';
  toggle.classList.toggle('expanded', expanded);
}

function setTableExpandVisibility() {
  const wrapper = document.querySelector('.table-expand');
  const scroll = document.querySelector('.table-scroll');
  if (!wrapper) return;
  const show = ResultsState.mode !== 'wfa';
  wrapper.style.display = show ? 'flex' : 'none';
  if (!show) setTableExpanded(false);
  if (scroll) {
    scroll.classList.toggle('wfa-tall', ResultsState.mode === 'wfa');
  }
}

function bindTableExpandToggle() {
  const toggle = document.getElementById('tableExpandToggle');
  if (!toggle) return;
  toggle.addEventListener('click', () => {
    const expanded = toggle.dataset.expanded === '1';
    setTableExpanded(!expanded);
  });
  setTableExpanded(false);
}

async function activateTab(tabId) {
  ResultsState.activeTab = tabId;
  ResultsState.selectedRowId = null;
  updateTabsVisibility();
  if (tabId === 'manual_tests') {
    await ensureManualTestSelection();
  }
  refreshResultsView();
}

function renderStudiesList(studies) {
  const listEl = document.querySelector('.studies-list');
  if (!listEl) return;
  const selectedStudies = Array.isArray(ResultsState.selectedStudies) ? ResultsState.selectedStudies : [];
  if (!Array.isArray(ResultsState.selectedStudies)) {
    ResultsState.selectedStudies = selectedStudies;
  }
  listEl.innerHTML = '';

  if (!studies || !studies.length) {
    const empty = document.createElement('div');
    empty.className = 'study-item';
    empty.textContent = 'No saved studies yet.';
    listEl.appendChild(empty);
    return;
  }

  studies.forEach((study) => {
    const studyId = study.study_id;
    const studyName = study.study_name || '';
    const item = document.createElement('div');
    item.className = 'study-item';
    item.dataset.studyId = String(studyId);
    item.dataset.studyName = studyName;

    if (studyId === ResultsState.studyId && !ResultsState.multiSelect) {
      item.classList.add('selected');
    }
    if (ResultsState.multiSelect && selectedStudies.includes(studyId)) {
      item.classList.add('selected');
    }

    const name = document.createElement('span');
    name.className = 'study-name';
    name.textContent = studyName;
    item.appendChild(name);

    item.addEventListener('click', (event) => {
      if (ResultsState.multiSelect) {
        event.preventDefault();
        toggleStudySelection(studyId);
      } else {
        openStudy(studyId);
      }
    });

    listEl.appendChild(item);
  });

  applyStudiesFilter();
}

function syncStudiesManagerControls() {
  const selectBtn = document.getElementById('studySelectBtn');
  if (selectBtn) {
    selectBtn.textContent = ResultsState.multiSelect ? 'Cancel' : 'Select';
    selectBtn.classList.toggle('active', ResultsState.multiSelect);
  }

  const filterBtn = document.getElementById('studyFilterBtn');
  if (filterBtn) {
    filterBtn.classList.toggle('active', ResultsState.filterActive);
  }

  const sortBtn = document.getElementById('studySortNameBtn');
  if (sortBtn) {
    sortBtn.disabled = !ResultsState.filterActive;
    sortBtn.classList.toggle('active', ResultsState.filterActive && studiesSortByNameActive);
  }

  const filterRow = document.getElementById('studyFilterRow');
  if (filterRow) {
    filterRow.hidden = !ResultsState.filterActive;
  }

  const filterText = typeof ResultsState.filterText === 'string' ? ResultsState.filterText : '';
  if (ResultsState.filterText !== filterText) {
    ResultsState.filterText = filterText;
  }
  const filterInput = document.getElementById('studyFilterInput');
  if (filterInput && filterInput.value !== filterText) {
    filterInput.value = filterText;
  }
}

function updateStudiesFilterEmptyState(listEl, show) {
  if (!listEl) return;
  let emptyState = listEl.querySelector('.study-filter-empty');
  if (!show) {
    if (emptyState) emptyState.remove();
    return;
  }
  if (!emptyState) {
    emptyState = document.createElement('div');
    emptyState.className = 'study-item study-filter-empty';
    emptyState.textContent = 'No matching studies.';
  }
  if (emptyState.parentElement !== listEl) {
    listEl.appendChild(emptyState);
  }
}

function applyStudiesFilter() {
  const listEl = document.querySelector('.studies-list');
  if (!listEl) return;
  const items = Array.from(listEl.querySelectorAll('.study-item[data-study-id]'));
  const filterText = (ResultsState.filterText || '').trim().toLowerCase();
  const shouldFilter = ResultsState.filterActive && filterText.length > 0;
  const visibleIds = new Set();
  let visibleCount = 0;

  items.forEach((item) => {
    const studyId = item.dataset.studyId || '';
    const studyName = (item.dataset.studyName || '').toLowerCase();
    const match = !shouldFilter || studyName.includes(filterText);
    item.style.display = match ? '' : 'none';
    if (match) {
      visibleCount += 1;
      if (studyId) visibleIds.add(studyId);
    }
  });

  const selectedStudies = Array.isArray(ResultsState.selectedStudies) ? ResultsState.selectedStudies : [];
  if (!Array.isArray(ResultsState.selectedStudies)) {
    ResultsState.selectedStudies = selectedStudies;
  }
  if (ResultsState.multiSelect && selectedStudies.length) {
    const pruned = selectedStudies.filter((studyId) => visibleIds.has(String(studyId)));
    if (pruned.length !== selectedStudies.length) {
      ResultsState.selectedStudies = pruned;
    }
    const selectedIds = new Set((ResultsState.selectedStudies || []).map((studyId) => String(studyId)));
    items.forEach((item) => {
      const studyId = item.dataset.studyId || '';
      item.classList.toggle('selected', selectedIds.has(studyId));
    });
  }

  updateStudiesFilterEmptyState(listEl, shouldFilter && items.length > 0 && visibleCount === 0);
}

function getVisibleStudyIds() {
  const visibleIds = new Set();
  document.querySelectorAll('.studies-list .study-item[data-study-id]').forEach((item) => {
    if (item.style.display === 'none') return;
    const studyId = item.dataset.studyId;
    if (studyId) visibleIds.add(studyId);
  });
  return visibleIds;
}

function getVisibleStudyItems() {
  return Array.from(document.querySelectorAll('.studies-list .study-item[data-study-id]'))
    .filter((item) => item.style.display !== 'none');
}

function isTypingElement(element) {
  if (!element) return false;
  if (element.isContentEditable) return true;
  const tagName = element.tagName ? element.tagName.toLowerCase() : '';
  return tagName === 'input' || tagName === 'textarea' || tagName === 'select';
}

function hasOpenModal() {
  return Boolean(document.querySelector('.modal-overlay.show'));
}

function isStudiesManagerClosed() {
  const manager = document.querySelector('.studies-manager');
  if (!manager) return true;
  const collapsible = manager.closest('.collapsible');
  return Boolean(collapsible && !collapsible.classList.contains('open'));
}

function resolveStudyArrowTarget(direction) {
  if (ResultsState.multiSelect) return null;
  const currentStudyId = String(ResultsState.studyId || '');
  if (!currentStudyId) return null;
  const items = getVisibleStudyItems();
  if (!items.length) return null;
  const currentIndex = items.findIndex((item) => (item.dataset.studyId || '') === currentStudyId);
  if (currentIndex < 0) return null;
  const targetIndex = (currentIndex + direction + items.length) % items.length;
  const target = items[targetIndex];
  const targetStudyId = target ? (target.dataset.studyId || '') : '';
  if (!targetStudyId || targetStudyId === currentStudyId) return null;
  return { studyId: targetStudyId, item: target };
}

async function navigateStudyByArrow(target) {
  if (!target || studiesKeyboardNavigationInProgress) return;
  studiesKeyboardNavigationInProgress = true;
  try {
    await openStudy(target.studyId);
    const selectedItem = getVisibleStudyItems()
      .find((item) => (item.dataset.studyId || '') === String(target.studyId));
    if (selectedItem) {
      selectedItem.scrollIntoView({ block: 'nearest' });
    }
  } finally {
    studiesKeyboardNavigationInProgress = false;
  }
}

function bindStudiesKeyboardNavigation() {
  if (studiesKeyboardNavigationBound) return;
  studiesKeyboardNavigationBound = true;
  document.addEventListener('keydown', (event) => {
    if (event.defaultPrevented) return;
    if (event.altKey || event.ctrlKey || event.metaKey || event.shiftKey) return;
    if (event.key !== 'ArrowDown' && event.key !== 'ArrowUp') return;
    if (studiesKeyboardNavigationInProgress) return;
    if (hasOpenModal()) return;
    if (isStudiesManagerClosed()) return;
    if (isTypingElement(document.activeElement)) return;
    const direction = event.key === 'ArrowDown' ? 1 : -1;
    const target = resolveStudyArrowTarget(direction);
    if (!target) return;
    event.preventDefault();
    void navigateStudyByArrow(target);
  });
}

async function loadStudiesList() {
  try {
    const data = await fetchStudiesList();
    studiesListCache = Array.isArray(data.studies) ? data.studies : [];
    renderStudiesList(getStudiesForRender(studiesListCache));
    syncStudiesManagerControls();
    return studiesListCache;
  } catch (error) {
    console.warn('Failed to load studies list', error);
    studiesListCache = [];
    renderStudiesList([]);
    syncStudiesManagerControls();
    return [];
  }
}

function rerenderStudiesListFromCache() {
  renderStudiesList(getStudiesForRender(studiesListCache));
  syncStudiesManagerControls();
}

function resetForDbSwitch() {
  ResultsState.studyId = '';
  ResultsState.studyName = '';
  ResultsState.studyCreatedAt = '';
  ResultsState.status = 'idle';
  ResultsState.mode = 'optuna';
  ResultsState.strategy = {};
  ResultsState.strategyId = '';
  ResultsState.dataset = {};
  ResultsState.optuna = {};
  ResultsState.wfa = {};
  ResultsState.summary = {};
  ResultsState.results = [];
  ResultsState.dsr = {
    enabled: false,
    topK: null,
    trials: [],
    nTrials: null,
    meanSharpe: null,
    varSharpe: null
  };
  ResultsState.forwardTest = {
    enabled: false,
    trials: [],
    startDate: '',
    endDate: '',
    periodDays: null,
    sortMetric: 'profit_degradation',
    source: 'optuna'
  };
  ResultsState.stressTest = {
    enabled: false,
    topK: null,
    trials: [],
    sortMetric: 'profit_retention',
    failureThreshold: 0.7,
    avgProfitRetention: null,
    avgRomadRetention: null,
    avgCombinedFailureRate: null,
    candidatesSkippedBadBase: 0,
    candidatesSkippedNoParams: 0,
    candidatesInsufficientData: 0,
    source: 'optuna'
  };
  ResultsState.oosTest = {
    enabled: false,
    topK: null,
    periodDays: null,
    startDate: '',
    endDate: '',
    source: '',
    trials: []
  };
  ResultsState.manualTests = [];
  ResultsState.activeManualTest = null;
  ResultsState.manualTestResults = [];
  ResultsState.activeTab = 'optuna';
  ResultsState.stitched_oos = {};
  ResultsState.dataPath = '';
  ResultsState.selectedRowId = null;
  ResultsState.multiSelect = false;
  ResultsState.selectedStudies = [];
  ResultsState.wfaSelection = null;
  updateStoredState({
    status: 'idle',
    mode: 'optuna',
    study_id: '',
    studyId: '',
    study_name: '',
    studyName: '',
    summary: {}
  });
  window.history.replaceState({}, '', window.location.pathname);
  syncStudiesManagerControls();
}

async function loadDatabasesList() {
  const container = document.querySelector('.database-list');
  if (!container) return;
  try {
    const data = await fetchDatabasesList();
    renderDatabasesList(data.databases || []);
  } catch (error) {
    console.warn('Failed to load databases', error);
    container.innerHTML = '';
  }
}

let databaseSwitchInProgress = false;
let studiesKeyboardNavigationBound = false;
let studiesKeyboardNavigationInProgress = false;

function renderDatabasesList(databases) {
  const container = document.querySelector('.database-list');
  if (!container) return;
  container.innerHTML = '';

  if (!databases || !databases.length) {
    const empty = document.createElement('div');
    empty.className = 'study-item';
    empty.textContent = 'No database files found.';
    container.appendChild(empty);
    return;
  }

  databases.forEach((db) => {
    const item = document.createElement('div');
    const classes = ['study-item'];
    if (db.active) classes.push('db-active', 'selected');
    item.className = classes.join(' ');
    item.textContent = db.name;
    item.dataset.dbName = db.name;
    item.addEventListener('click', async () => {
      selectDatabaseItem(item);
      if (db.active || databaseSwitchInProgress) return;
      databaseSwitchInProgress = true;
      try {
        await switchDatabase(db.name);
      } finally {
        databaseSwitchInProgress = false;
      }
    });
    container.appendChild(item);
  });
}

function selectDatabaseItem(item) {
  document.querySelectorAll('.database-list .study-item').forEach((el) => {
    el.classList.remove('selected');
  });
  item.classList.add('selected');
}

async function switchDatabase(filename) {
  try {
    await switchDatabaseRequest(filename);
    resetForDbSwitch();
    refreshResultsView();
    await loadStudiesList();
    await loadDatabasesList();
  } catch (error) {
    await loadDatabasesList();
    alert(error.message || 'Failed to switch database.');
  }
}

function buildStitchedFromWindows(windows) {
  const stitched = [];
  const stitchedTimestamps = [];
  const windowIds = [];
  let currentBalance = 100.0;
  let timestampsValid = true;

  (windows || []).forEach((window, index) => {
    const equity = window.oos_equity_curve || [];
    if (!equity.length) return;

    const timestamps = Array.isArray(window.oos_timestamps) ? window.oos_timestamps : [];
    const hasTimestamps = timestamps.length >= equity.length;

    const startEquity = equity[0] || 100.0;
    const startIdx = index === 0 ? 0 : 1;
    const windowId = window.window_number || window.window_id || index + 1;

    for (let i = startIdx; i < equity.length; i += 1) {
      const pctChange = (equity[i] / startEquity) - 1.0;
      const newBalance = currentBalance * (1.0 + pctChange);
      stitched.push(newBalance);
      windowIds.push(windowId);
      if (timestampsValid) {
        if (hasTimestamps) {
          stitchedTimestamps.push(timestamps[i]);
        } else {
          timestampsValid = false;
        }
      }
    }

    if (stitched.length) {
      currentBalance = stitched[stitched.length - 1];
    }
  });

  const timestamps = timestampsValid && stitchedTimestamps.length === stitched.length
    ? stitchedTimestamps
    : [];
  return { equity_curve: stitched, window_ids: windowIds, timestamps };
}

function toFiniteNumber(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function calculateMedian(values) {
  if (!Array.isArray(values) || !values.length) return null;
  const sorted = values
    .map((value) => Number(value))
    .filter((value) => Number.isFinite(value))
    .sort((a, b) => a - b);
  if (!sorted.length) return null;
  const mid = Math.floor(sorted.length / 2);
  if (sorted.length % 2 === 1) return sorted[mid];
  return (sorted[mid - 1] + sorted[mid]) / 2;
}

function deriveWindowWinningTrades(windowData) {
  const totalRaw = toFiniteNumber(windowData?.oos_total_trades);
  if (totalRaw === null) return null;
  const total = Math.max(0, Math.round(totalRaw));
  const directWins = toFiniteNumber(windowData?.oos_winning_trades);
  if (directWins !== null) {
    return Math.min(Math.max(0, Math.round(directWins)), total);
  }
  const winRate = toFiniteNumber(windowData?.oos_win_rate);
  if (winRate === null) return null;
  const derived = Math.round((total * winRate) / 100);
  return Math.min(Math.max(derived, 0), total);
}

function buildWindowAggregates(windows) {
  const rows = Array.isArray(windows) ? windows : [];
  let totalTrades = 0;
  let totalTradesKnown = true;
  let winningTrades = 0;
  let winningTradesKnown = true;
  let profitableWindows = 0;
  const profitValues = [];
  const tradeWinRateValues = [];

  rows.forEach((windowData) => {
    const netProfit = toFiniteNumber(windowData?.oos_net_profit_pct);
    if (netProfit !== null) {
      profitValues.push(netProfit);
      if (netProfit > 0) profitableWindows += 1;
    }

    const tradeWinRate = toFiniteNumber(windowData?.oos_win_rate);
    if (tradeWinRate !== null) tradeWinRateValues.push(tradeWinRate);

    const trades = toFiniteNumber(windowData?.oos_total_trades);
    if (trades === null) {
      totalTradesKnown = false;
    } else {
      totalTrades += Math.max(0, Math.round(trades));
    }

    const wins = deriveWindowWinningTrades(windowData);
    if (wins === null) {
      winningTradesKnown = false;
    } else {
      winningTrades += wins;
    }
  });

  const totalWindows = rows.length;
  const profitableWindowsPct = totalWindows > 0
    ? (profitableWindows / totalWindows) * 100
    : 0;

  return {
    totalTrades: totalTradesKnown ? totalTrades : null,
    winningTrades: winningTradesKnown ? winningTrades : null,
    profitableWindows,
    totalWindows,
    profitableWindowsPct,
    medianWindowProfit: calculateMedian(profitValues),
    medianWindowWr: calculateMedian(tradeWinRateValues)
  };
}

function calculateSummaryFromEquity(equityCurve) {
  if (!equityCurve || !equityCurve.length) {
    return { final_net_profit_pct: 0, max_drawdown_pct: 0 };
  }
  const finalValue = equityCurve[equityCurve.length - 1];
  const finalNetProfitPct = (finalValue / 100.0 - 1.0) * 100.0;

  let peak = equityCurve[0];
  let maxDd = 0;
  equityCurve.forEach((value) => {
    if (value > peak) peak = value;
    if (peak > 0) {
      const dd = (peak - value) / peak * 100.0;
      if (dd > maxDd) maxDd = dd;
    }
  });

  return { final_net_profit_pct: finalNetProfitPct, max_drawdown_pct: maxDd };
}

async function applyStudyPayload(data) {
  const study = data.study || {};
  ResultsState.studyId = study.study_id || ResultsState.studyId;
  ResultsState.studyName = study.study_name || ResultsState.studyName;
  ResultsState.studyCreatedAt = study.completed_at || study.created_at || ResultsState.studyCreatedAt;
  ResultsState.mode = study.optimization_mode || ResultsState.mode;
  ResultsState.status = study.status || (study.completed_at ? 'completed' : ResultsState.status);
  ResultsState.strategyId = study.strategy_id || ResultsState.strategyId;
  ResultsState.dataset = { label: study.csv_file_name || '' };
  ResultsState.dataPath = study.csv_file_path || ResultsState.dataPath;

  const config = study.config_json || {};
  const configWfa = config.wfa || {};
  const consistencySegments = typeof getStudyConsistencySegments === 'function'
    ? getStudyConsistencySegments(study)
    : null;
  const isPeriodDaysForSegments = study.is_period_days ?? configWfa.is_period_days ?? config.is_period_days ?? null;
  const ftPeriodDaysForSegments = study.ft_period_days ?? null;
  const oosPeriodDaysForSegments = study.oos_test_period_days ?? null;
  const ftConsistencySegments = typeof deriveAutoConsistencySegments === 'function'
    ? deriveAutoConsistencySegments(isPeriodDaysForSegments, consistencySegments, ftPeriodDaysForSegments)
    : null;
  const oosConsistencySegments = typeof deriveAutoConsistencySegments === 'function'
    ? deriveAutoConsistencySegments(isPeriodDaysForSegments, consistencySegments, oosPeriodDaysForSegments)
    : null;
  const adaptiveModeRaw = study.adaptive_mode ?? configWfa.adaptive_mode ?? config.adaptive_mode;
  ResultsState.wfa = {
    postProcess: config.postProcess || {},
    isPeriodDays: study.is_period_days ?? configWfa.is_period_days ?? config.is_period_days ?? null,
    oosPeriodDays: configWfa.oos_period_days ?? config.oos_period_days ?? null,
    storeTopNTrials: configWfa.store_top_n_trials ?? null,
    adaptiveMode: adaptiveModeRaw === undefined || adaptiveModeRaw === null
      ? null
      : Boolean(adaptiveModeRaw),
    maxOosPeriodDays: study.max_oos_period_days ?? configWfa.max_oos_period_days ?? config.max_oos_period_days ?? null,
    minOosTrades: study.min_oos_trades ?? configWfa.min_oos_trades ?? config.min_oos_trades ?? null,
    checkIntervalTrades: study.check_interval_trades ?? configWfa.check_interval_trades ?? config.check_interval_trades ?? null,
    cusumThreshold: study.cusum_threshold ?? configWfa.cusum_threshold ?? config.cusum_threshold ?? null,
    ddThresholdMultiplier: study.dd_threshold_multiplier ?? configWfa.dd_threshold_multiplier ?? config.dd_threshold_multiplier ?? null,
    inactivityMultiplier: study.inactivity_multiplier ?? configWfa.inactivity_multiplier ?? config.inactivity_multiplier ?? null,
    runTimeSeconds: study.optimization_time_seconds ?? null
  };
  ResultsState.fixedParams = config.fixed_params || ResultsState.fixedParams;
  ResultsState.dateFilter = Boolean(ResultsState.fixedParams.dateFilter ?? ResultsState.dateFilter);
  ResultsState.start = ResultsState.fixedParams.start || ResultsState.start;
  ResultsState.end = ResultsState.fixedParams.end || ResultsState.end;
  ResultsState.consistencySegments = consistencySegments;

  if (ResultsState.mode === 'wfa') {
    ResultsState.results = data.windows || [];
  } else {
    ResultsState.results = (data.trials || []).map((trial) => ({
      ...trial,
      consistency_segments_used: trial.consistency_segments_used ?? consistencySegments,
      ft_consistency_segments_used: trial.ft_consistency_segments_used ?? ftConsistencySegments,
      oos_test_consistency_segments_used:
        trial.oos_test_consistency_segments_used ?? oosConsistencySegments,
    }));
  }
  ResultsState.selectedRowId = null;

  ResultsState.forwardTest.enabled = Boolean(study.ft_enabled);
  ResultsState.forwardTest.startDate = study.ft_start_date || '';
  ResultsState.forwardTest.endDate = study.ft_end_date || '';
  ResultsState.forwardTest.periodDays = study.ft_period_days ?? null;
  ResultsState.forwardTest.sortMetric = study.ft_sort_metric || 'profit_degradation';
  ResultsState.forwardTest.trials = (ResultsState.results || [])
    .filter((trial) => trial.ft_rank !== null && trial.ft_rank !== undefined);
  ResultsState.forwardTest.trials.sort((a, b) => (a.ft_rank || 0) - (b.ft_rank || 0));

  const dsrTrials = (ResultsState.results || [])
    .filter((trial) => trial.dsr_rank !== null && trial.dsr_rank !== undefined);
  dsrTrials.sort((a, b) => (a.dsr_rank || 0) - (b.dsr_rank || 0));
  ResultsState.dsr = {
    enabled: Boolean(study.dsr_enabled),
    topK: study.dsr_top_k ?? null,
    trials: dsrTrials,
    nTrials: study.dsr_n_trials ?? null,
    meanSharpe: study.dsr_mean_sharpe ?? null,
    varSharpe: study.dsr_var_sharpe ?? null
  };
  ResultsState.forwardTest.source = study.ft_source
    || inferPostProcessSource(data.trials || [], 'ft_source')
    || 'optuna';

  const stTrials = (ResultsState.results || [])
    .filter((trial) => trial.st_rank !== null && trial.st_rank !== undefined);
  stTrials.sort((a, b) => (a.st_rank || 0) - (b.st_rank || 0));
  ResultsState.stressTest = {
    enabled: Boolean(study.st_enabled),
    topK: study.st_top_k ?? null,
    trials: stTrials,
    sortMetric: study.st_sort_metric || 'profit_retention',
    failureThreshold: study.st_failure_threshold ?? 0.7,
    avgProfitRetention: study.st_avg_profit_retention ?? null,
    avgRomadRetention: study.st_avg_romad_retention ?? null,
    avgCombinedFailureRate: study.st_avg_combined_failure_rate ?? null,
    candidatesSkippedBadBase: study.st_candidates_skipped_bad_base ?? 0,
    candidatesSkippedNoParams: study.st_candidates_skipped_no_params ?? 0,
    candidatesInsufficientData: study.st_candidates_insufficient_data ?? 0,
    source: study.st_source
      || inferPostProcessSource(data.trials || [], 'st_source')
      || 'optuna'
  };

  const oosTrials = (ResultsState.results || []).filter(
    (trial) => trial.oos_test_source_rank !== null && trial.oos_test_source_rank !== undefined
  );
  oosTrials.sort((a, b) => (a.oos_test_source_rank || 0) - (b.oos_test_source_rank || 0));
  ResultsState.oosTest = {
    enabled: Boolean(study.oos_test_enabled),
    topK: study.oos_test_top_k ?? null,
    periodDays: study.oos_test_period_days ?? null,
    startDate: study.oos_test_start_date || '',
    endDate: study.oos_test_end_date || '',
    source: study.oos_test_source_module
      || inferPostProcessSource(data.trials || [], 'oos_test_source')
      || '',
    trials: oosTrials
  };

  ResultsState.manualTests = data.manual_tests || [];
  ResultsState.activeManualTest = null;
  ResultsState.manualTestResults = [];

  if (ResultsState.mode === 'optuna') {
    const hasDsr = ResultsState.dsr.enabled && ResultsState.dsr.trials.length > 0;
    const hasForward = ResultsState.forwardTest.enabled && ResultsState.forwardTest.trials.length > 0;
    const hasStress = ResultsState.stressTest.enabled && ResultsState.stressTest.trials.length > 0;
    const hasOos = ResultsState.oosTest.enabled && ResultsState.oosTest.trials.length > 0;
    const hasManual = ResultsState.manualTests.length > 0;
    if (ResultsState.activeTab === 'dsr' && !hasDsr) {
      ResultsState.activeTab = 'optuna';
    }
    if (ResultsState.activeTab === 'forward_test' && !hasForward) {
      ResultsState.activeTab = 'optuna';
    }
    if (ResultsState.activeTab === 'stress_test' && !hasStress) {
      ResultsState.activeTab = 'optuna';
    }
    if (ResultsState.activeTab === 'oos_test' && !hasOos) {
      ResultsState.activeTab = 'optuna';
    }
    if (ResultsState.activeTab === 'manual_tests' && !hasManual) {
      ResultsState.activeTab = 'optuna';
    }
    if (!ResultsState.activeTab) {
      ResultsState.activeTab = 'optuna';
    }
  }

  if (ResultsState.mode === 'wfa') {
    const storedStitched = data.stitched_oos || null;
    const aggregates = buildWindowAggregates(ResultsState.results || []);
    const totalTrades = aggregates.totalTrades;
    const winningTrades = aggregates.winningTrades;
    const profitableWindows = aggregates.profitableWindows;
    const totalWindows = aggregates.totalWindows;
    const winRate = aggregates.profitableWindowsPct;
    const medianWindowProfit = aggregates.medianWindowProfit;
    const medianWindowWr = aggregates.medianWindowWr;

    if (storedStitched && Array.isArray(storedStitched.equity_curve) && storedStitched.equity_curve.length) {
      const fallbackSummary = calculateSummaryFromEquity(storedStitched.equity_curve);
      ResultsState.stitched_oos = {
        final_net_profit_pct: storedStitched.final_net_profit_pct ?? fallbackSummary.final_net_profit_pct,
        max_drawdown_pct: storedStitched.max_drawdown_pct ?? fallbackSummary.max_drawdown_pct,
        total_trades: storedStitched.total_trades ?? study.stitched_oos_total_trades ?? totalTrades,
        winning_trades: storedStitched.winning_trades ?? study.stitched_oos_winning_trades ?? winningTrades,
        wfe: storedStitched.wfe ?? study.best_value ?? 0,
        oos_win_rate: storedStitched.oos_win_rate ?? study.stitched_oos_win_rate ?? winRate,
        profitable_windows: storedStitched.profitable_windows ?? study.profitable_windows ?? profitableWindows,
        total_windows: storedStitched.total_windows ?? study.total_windows ?? totalWindows,
        median_window_profit: storedStitched.median_window_profit ?? study.median_window_profit ?? medianWindowProfit,
        median_window_wr: storedStitched.median_window_wr ?? study.median_window_wr ?? medianWindowWr,
        worst_window_profit: storedStitched.worst_window_profit ?? study.worst_window_profit ?? null,
        worst_window_dd: storedStitched.worst_window_dd ?? study.worst_window_dd ?? null,
        equity_curve: storedStitched.equity_curve,
        timestamps: storedStitched.timestamps || [],
        window_ids: storedStitched.window_ids || []
      };
    } else {
      const stitched = buildStitchedFromWindows(ResultsState.results);
      const summary = calculateSummaryFromEquity(stitched.equity_curve);
      ResultsState.stitched_oos = {
        final_net_profit_pct: summary.final_net_profit_pct,
        max_drawdown_pct: summary.max_drawdown_pct,
        total_trades: study.stitched_oos_total_trades ?? totalTrades,
        winning_trades: study.stitched_oos_winning_trades ?? winningTrades,
        wfe: study.best_value ?? 0,
        oos_win_rate: study.stitched_oos_win_rate ?? winRate,
        profitable_windows: study.profitable_windows ?? profitableWindows,
        total_windows: study.total_windows ?? totalWindows,
        median_window_profit: study.median_window_profit ?? medianWindowProfit,
        median_window_wr: study.median_window_wr ?? medianWindowWr,
        worst_window_profit: study.worst_window_profit ?? null,
        worst_window_dd: study.worst_window_dd ?? null,
        equity_curve: stitched.equity_curve,
        timestamps: stitched.timestamps || [],
        window_ids: stitched.window_ids
      };
    }
  }

  if (ResultsState.strategyId) {
    try {
      const strategyConfig = await fetchStrategyConfig(ResultsState.strategyId);
      ResultsState.strategyConfig = strategyConfig || {};
      ResultsState.strategy = {
        name: strategyConfig.name || ResultsState.strategyId,
        version: strategyConfig.version || ''
      };
    } catch (error) {
      console.warn('Failed to load strategy config', error);
    }
  }

  const optunaConfig = config.optuna_config || {};
  const primaryObjective = study.primary_objective
    ?? optunaConfig.primary_objective
    ?? config.primary_objective
    ?? null;
  const objectives = study.objectives ?? optunaConfig.objectives ?? study.objectives_json ?? [];
  const constraints = study.constraints ?? optunaConfig.constraints ?? study.constraints_json ?? [];
  const sanitizeEnabledRaw = optunaConfig.sanitize_enabled ?? study.sanitize_enabled;
  const sanitizeEnabled = sanitizeEnabledRaw === undefined || sanitizeEnabledRaw === null
    ? null
    : Boolean(sanitizeEnabledRaw);
  const sanitizeThresholdRaw = optunaConfig.sanitize_trades_threshold ?? study.sanitize_trades_threshold;
  const sanitizeThreshold = sanitizeThresholdRaw === undefined || sanitizeThresholdRaw === null
    ? null
    : sanitizeThresholdRaw;
  const filterMinProfitRaw = study.filter_min_profit ?? optunaConfig.filter_min_profit;
  const filterMinProfit = filterMinProfitRaw === undefined || filterMinProfitRaw === null
    ? false
    : Boolean(filterMinProfitRaw);
  const minProfitThresholdRaw = study.min_profit_threshold ?? optunaConfig.min_profit_threshold;
  const minProfitThreshold = minProfitThresholdRaw === undefined || minProfitThresholdRaw === null
    ? null
    : minProfitThresholdRaw;
  const scoreConfig = study.score_config_json || optunaConfig.score_config || {};
  const scoreFilterRaw = scoreConfig ? scoreConfig.filter_enabled : null;
  const scoreFilterEnabled = scoreFilterRaw === undefined || scoreFilterRaw === null
    ? false
    : Boolean(scoreFilterRaw);
  const scoreThresholdRaw = scoreConfig ? scoreConfig.min_score_threshold : null;
  const scoreThreshold = scoreThresholdRaw === undefined || scoreThresholdRaw === null
    ? null
    : scoreThresholdRaw;
  ResultsState.optuna = {
    objectives,
    primaryObjective,
    constraints,
    budgetMode: optunaConfig.budget_mode ?? study.budget_mode ?? null,
    nTrials: optunaConfig.n_trials ?? study.n_trials ?? null,
    timeLimit: optunaConfig.time_limit ?? study.time_limit ?? null,
    convergence: optunaConfig.convergence_patience ?? study.convergence_patience ?? null,
    sampler: (optunaConfig.sampler_config && optunaConfig.sampler_config.sampler_type)
      || optunaConfig.sampler_type
      || optunaConfig.sampler
      || config.sampler_type
      || study.sampler_type
      || null,
    pruner: optunaConfig.pruner ?? null,
    warmupTrials: optunaConfig.warmup_trials
      ?? config.n_startup_trials
      ?? (optunaConfig.sampler_config ? optunaConfig.sampler_config.n_startup_trials : null)
      ?? null,
    coverageMode: Boolean(
      optunaConfig.coverage_mode
      ?? config.coverage_mode
      ?? false
    ),
    workers: config.worker_processes ?? null,
    sanitizeEnabled,
    sanitizeTradesThreshold: sanitizeThreshold,
    filterMinProfit,
    minProfitThreshold,
    scoreFilterEnabled,
    scoreThreshold,
    optimizationTimeSeconds: study.optimization_time_seconds ?? null
  };

  updateResultsHeader();
}

async function openStudy(studyId) {
  if (!studyId) return;
  try {
    const data = await fetchStudyDetails(studyId);
    ResultsState.studyId = studyId;
    ResultsState.studyName = data.study?.study_name || ResultsState.studyName;

    if (!data.csv_exists) {
      showMissingCsvDialog(studyId, data.study?.csv_file_path || '', data.study?.csv_file_name || '');
      return;
    }

    await applyStudyPayload(data);
    if (ResultsState.activeTab === 'manual_tests') {
      await ensureManualTestSelection();
    }
    setQueryStudyId(studyId);
    await loadStudiesList();
    refreshResultsView();
  } catch (error) {
    console.warn('Failed to open study', error);
  }
}

const MissingCsvState = {
  studyId: '',
  originalPath: '',
  originalName: ''
};

function showMissingCsvDialog(studyId, originalPath, originalName) {
  MissingCsvState.studyId = studyId;
  MissingCsvState.originalPath = originalPath || '';
  MissingCsvState.originalName = originalName || '';

  const modal = document.getElementById('missingCsvModal');
  const pathEl = document.getElementById('missingCsvPath');
  const nameEl = document.getElementById('missingCsvName');
  if (pathEl) pathEl.textContent = MissingCsvState.originalPath || 'Unknown path';
  if (nameEl) nameEl.textContent = MissingCsvState.originalName || 'Unknown file';
  if (modal) modal.classList.add('show');
}

function hideMissingCsvDialog() {
  const modal = document.getElementById('missingCsvModal');
  const pathInput = document.getElementById('missingCsvInput');
  if (modal) modal.classList.remove('show');
  if (pathInput) pathInput.value = '';
  MissingCsvState.studyId = '';
  MissingCsvState.originalPath = '';
  MissingCsvState.originalName = '';
}

function bindMissingCsvDialog() {
  const modal = document.getElementById('missingCsvModal');
  const cancelBtn = document.getElementById('missingCsvCancel');
  const updateBtn = document.getElementById('missingCsvUpdate');
  const pathInput = document.getElementById('missingCsvInput');

  if (cancelBtn) {
    cancelBtn.addEventListener('click', () => {
      hideMissingCsvDialog();
    });
  }

  if (updateBtn) {
    updateBtn.addEventListener('click', async () => {
      if (!MissingCsvState.studyId) return;
      const formData = new FormData();
      const csvPath = pathInput ? pathInput.value.trim() : '';
      if (!csvPath) {
        alert('Provide an absolute CSV path.');
        return;
      }
      if (!isAbsoluteFilesystemPath(csvPath)) {
        alert('CSV path must be absolute.');
        return;
      }
      formData.append('csvPath', csvPath);

      try {
        const response = await updateStudyCsvPathRequest(MissingCsvState.studyId, formData);
        if (response.warnings && response.warnings.length) {
          alert(`CSV updated with warnings:\n- ${response.warnings.join('\n- ')}`);
        }
        hideMissingCsvDialog();
        await openStudy(MissingCsvState.studyId);
      } catch (error) {
        alert(error.message || 'Failed to update CSV path.');
      }
    });
  }

  if (modal) {
    modal.addEventListener('click', (event) => {
      if (event.target === modal) {
        hideMissingCsvDialog();
      }
    });
  }
}

async function refreshManualTestsList() {
  if (!ResultsState.studyId) return;
  try {
    const data = await fetchManualTestsList(ResultsState.studyId);
    ResultsState.manualTests = data.tests || [];
  } catch (error) {
    ResultsState.manualTests = [];
  }
}

async function loadManualTestResultsById(testId) {
  if (!ResultsState.studyId || !testId) return;
  try {
    const data = await fetchManualTestResults(ResultsState.studyId, testId);
    ResultsState.activeManualTest = {
      id: data.id,
      source_tab: data.source_tab,
      config: data.results_json?.config || null
    };
    ResultsState.manualTestResults = data.results_json?.results || [];
  } catch (error) {
    ResultsState.activeManualTest = null;
    ResultsState.manualTestResults = [];
  }
}

async function ensureManualTestSelection() {
  if (!ResultsState.manualTests.length) {
    ResultsState.activeManualTest = null;
    ResultsState.manualTestResults = [];
    return;
  }
  const activeId = ResultsState.activeManualTest?.id;
  const exists = ResultsState.manualTests.some((test) => test.id === activeId);
  if (activeId && exists) {
    await loadManualTestResultsById(activeId);
    return;
  }
  await loadManualTestResultsById(ResultsState.manualTests[0].id);
}

function getTrialsForActiveTab() {
  if (ResultsState.activeTab === 'forward_test') {
    return ResultsState.forwardTest.trials || [];
  }
  if (ResultsState.activeTab === 'dsr') {
    return ResultsState.dsr.trials || [];
  }
  if (ResultsState.activeTab === 'stress_test') {
    return ResultsState.stressTest.trials || [];
  }
  return ResultsState.results || [];
}

function bindTabs() {
  document.querySelectorAll('.tab-btn').forEach((tab) => {
    tab.addEventListener('click', async () => {
      await activateTab(tab.dataset.tab);
    });
  });
}

async function fetchEquityCurve(result, options = null) {
  if (!ResultsState.dataPath) {
    return null;
  }
  const params = { ...(ResultsState.fixedParams || {}), ...(result.params || {}) };
  let start = ResultsState.start;
  let end = ResultsState.end;
  let dateFilter = typeof ResultsState.dateFilter === 'boolean' ? ResultsState.dateFilter : false;

  if (options && options.start && options.end) {
    start = options.start;
    end = options.end;
    dateFilter = true;
  }

  if (start) params.start = start;
  if (end) params.end = end;
  params.dateFilter = dateFilter;

  const formData = new FormData();
  formData.append('strategy', ResultsState.strategyId || ResultsState.strategy.id || '');
  formData.append('warmupBars', String(ResultsState.warmupBars || 1000));
  formData.append('csvPath', ResultsState.dataPath);
  formData.append('payload', JSON.stringify(params));

  const response = await fetch('/api/backtest', {
    method: 'POST',
    body: formData
  });
  if (!response.ok) {
    return null;
  }
  const data = await response.json();
  if (!data || !data.metrics) return null;
  const equity = data.metrics.equity_curve || data.metrics.balance_curve || [];
  const timestamps = data.metrics.timestamps || [];
  return { equity, timestamps };
}

function refreshResultsView() {
  updateStatusBadge(ResultsState.status || 'idle');
  updateSidebarSettings();
  updateResultsHeader();
  updateTabsVisibility();
  setTableExpandVisibility();

  const progressLabel = document.getElementById('progressLabel');
  const progressPercent = document.getElementById('progressPercent');
  if (progressLabel) progressLabel.textContent = 'Trial - / -';
  if (progressPercent) progressPercent.textContent = '0%';

  if (ResultsState.mode === 'wfa') {
    setComparisonLine('');
    updateTableHeader('Stitched OOS', '', getWfaStitchedPeriodLabel(ResultsState.results || []));
    const summary = ResultsState.stitched_oos || ResultsState.summary || {};
    displaySummaryCards(summary);
    if (window.WFAResultsUI) {
      WFAResultsUI.resetState();
      WFAResultsUI.renderWFAResultsTable(
        ResultsState.results || [],
        summary
      );
    } else {
      renderWFATable(ResultsState.results || []);
    }
    let boundaries = [];
    if (typeof calculateWindowBoundariesByDate === 'function') {
      boundaries = calculateWindowBoundariesByDate(ResultsState.results || [], summary.timestamps || []);
    } else if (typeof calculateWindowBoundaries === 'function') {
      boundaries = calculateWindowBoundaries(ResultsState.results || [], summary);
    }
    renderEquityChart(summary.equity_curve || [], boundaries, summary.timestamps || [], { useTimeScale: true });
    renderWindowIndicators(ResultsState.summary?.total_windows || ResultsState.results?.length || 0);
  } else {
    setComparisonLine('');
    const summaryRow = document.querySelector('.summary-row');
    if (summaryRow) summaryRow.style.display = 'none';
    const periodLabel = getActivePeriodLabel();
    if (ResultsState.activeTab === 'forward_test') {
      const sortLabel = formatSortMetricLabel(ResultsState.forwardTest.sortMetric) || 'FT results';
      updateTableHeader('Forward Test', `Sorted by ${sortLabel}`, periodLabel);
      renderForwardTestTable(ResultsState.forwardTest.trials || []);
    } else if (ResultsState.activeTab === 'stress_test') {
      const sortLabel = formatSortMetricLabel(ResultsState.stressTest.sortMetric) || 'retention';
      updateTableHeader('Stress Test', `Sorted by ${sortLabel}`, periodLabel);
      renderStressTestTable(ResultsState.stressTest.trials || []);
    } else if (ResultsState.activeTab === 'oos_test') {
      const sourceLabel = formatSourceLabel(ResultsState.oosTest.source);
      const subtitle = sourceLabel ? `Source: ${sourceLabel}` : 'Source: -';
      updateTableHeader('OOS Test', subtitle, periodLabel);
      renderOosTestTable(ResultsState.oosTest.trials || []);
    } else if (ResultsState.activeTab === 'dsr') {
      updateTableHeader('DSR', 'Sorted by DSR probability', periodLabel);
      renderDsrTable(ResultsState.dsr.trials || []);
    } else if (ResultsState.activeTab === 'manual_tests') {
      const sourceLabel = formatSourceLabel(ResultsState.activeManualTest?.source_tab);
      const subtitle = sourceLabel ? `Source: ${sourceLabel}` : 'Source: -';
      updateTableHeader('Test Results', subtitle, periodLabel);
      renderManualTestTable(ResultsState.manualTestResults || []);
    } else {
      updateTableHeader('Optuna IS', getOptunaSortSubtitle(), periodLabel);
      renderOptunaTable(ResultsState.results || []);
    }
    renderManualTestControls();
  }
}

function bindCollapsibles() {
  document.querySelectorAll('.collapsible-header').forEach((header) => {
    header.addEventListener('click', () => {
      const parent = header.parentElement;
      if (parent) parent.classList.toggle('open');
    });
  });
}

function bindStudiesManager() {
  const selectBtn = document.getElementById('studySelectBtn');
  const deleteBtn = document.getElementById('studyDeleteBtn');
  const filterBtn = document.getElementById('studyFilterBtn');
  const sortNameBtn = document.getElementById('studySortNameBtn');
  const filterInput = document.getElementById('studyFilterInput');

  syncStudiesManagerControls();

  if (selectBtn) {
    selectBtn.addEventListener('click', () => {
      ResultsState.multiSelect = !ResultsState.multiSelect;
      if (!ResultsState.multiSelect) {
        ResultsState.selectedStudies = [];
      }
      syncStudiesManagerControls();
      loadStudiesList();
    });
  }

  if (filterBtn && filterInput) {
    filterBtn.addEventListener('click', () => {
      ResultsState.filterActive = !ResultsState.filterActive;
      if (!ResultsState.filterActive) {
        ResultsState.filterText = '';
        studiesSortByNameActive = false;
      }
      rerenderStudiesListFromCache();
      if (ResultsState.filterActive) {
        filterInput.focus();
      }
    });

    filterInput.addEventListener('input', () => {
      ResultsState.filterText = filterInput.value || '';
      applyStudiesFilter();
    });
  }

  if (sortNameBtn && filterInput) {
    sortNameBtn.addEventListener('click', () => {
      if (!ResultsState.filterActive) return;
      studiesSortByNameActive = !studiesSortByNameActive;
      rerenderStudiesListFromCache();
      filterInput.focus();
    });
  }

  if (deleteBtn) {
    deleteBtn.addEventListener('click', async () => {
      const visibleStudyIds = getVisibleStudyIds();
      const selectedStudies = Array.isArray(ResultsState.selectedStudies) ? ResultsState.selectedStudies : [];
      if (!Array.isArray(ResultsState.selectedStudies)) {
        ResultsState.selectedStudies = selectedStudies;
      }
      const selected = ResultsState.multiSelect
        ? selectedStudies.filter((studyId) => visibleStudyIds.has(String(studyId)))
        : (ResultsState.studyId ? [ResultsState.studyId] : []);
      if (!selected.length) {
        alert('Select a study first.');
        return;
      }
      const confirmed = window.confirm(
        selected.length > 1
          ? `Delete ${selected.length} studies? This cannot be undone.`
          : 'Delete this study? This cannot be undone.'
      );
      if (!confirmed) return;
      try {
        for (const studyId of selected) {
          await deleteStudyRequest(studyId);
        }
        ResultsState.studyId = '';
        ResultsState.studyName = '';
        ResultsState.results = [];
        ResultsState.selectedStudies = [];
        refreshResultsView();
        await loadStudiesList();
      } catch (error) {
        alert(error.message || 'Failed to delete study.');
      }
    });
  }

  bindStudiesKeyboardNavigation();
}

function toggleStudySelection(studyId) {
  const selectedStudies = Array.isArray(ResultsState.selectedStudies) ? ResultsState.selectedStudies : [];
  const selected = new Set(selectedStudies);
  if (selected.has(studyId)) {
    selected.delete(studyId);
  } else {
    selected.add(studyId);
  }
  ResultsState.selectedStudies = Array.from(selected);
  loadStudiesList();
}

function openManualTestModal() {
  const modal = document.getElementById('manualTestModal');
  const selectedLabel = document.getElementById('manualSelectedLabel');
  const dataPath = document.getElementById('manualDataPath');
  const dataOriginal = document.getElementById('manualDataOriginal');
  if (selectedLabel) {
    selectedLabel.textContent = ResultsState.selectedRowId
      ? `Trial #${ResultsState.selectedRowId}`
      : 'Trial # -';
  }
  if (dataPath && dataOriginal) {
    dataPath.disabled = dataOriginal.checked;
  }
  if (modal) modal.classList.add('show');
}

function closeManualTestModal() {
  const modal = document.getElementById('manualTestModal');
  if (modal) modal.classList.remove('show');
}

function bindManualDataSourceToggle() {
  const dataOriginal = document.getElementById('manualDataOriginal');
  const dataNew = document.getElementById('manualDataNew');
  const dataPath = document.getElementById('manualDataPath');
  if (!dataPath) return;
  const sync = () => {
    dataPath.disabled = dataOriginal && dataOriginal.checked;
  };
  if (dataOriginal) dataOriginal.addEventListener('change', sync);
  if (dataNew) dataNew.addEventListener('change', sync);
  sync();
}

function getManualTrialNumbers() {
  const topMode = document.getElementById('manualTrialTop');
  const topInput = document.getElementById('manualTopK');
  const selectedMode = document.getElementById('manualTrialSelected');

  if (selectedMode && selectedMode.checked) {
    if (!ResultsState.selectedRowId) return [];
    return [ResultsState.selectedRowId];
  }

  const topK = topInput ? Number(topInput.value) : 0;
  const normalized = Number.isFinite(topK) ? Math.max(1, Math.round(topK)) : 1;
  const trials = getTrialsForActiveTab();
  return trials.slice(0, normalized).map((trial) => trial.trial_number);
}

async function runManualTestFromModal() {
  const dataOriginal = document.getElementById('manualDataOriginal');
  const dataPathInput = document.getElementById('manualDataPath');
  const startInput = document.getElementById('manualStartDate');
  const endInput = document.getElementById('manualEndDate');

  const dataSource = dataOriginal && dataOriginal.checked ? 'original_csv' : 'new_csv';
  const startDate = startInput ? startInput.value.trim() : '';
  const endDate = endInput ? endInput.value.trim() : '';

  const trialNumbers = getManualTrialNumbers();
  if (!trialNumbers.length) {
    alert('Select at least one trial.');
    return;
  }

  let csvPath = null;
  if (dataSource === 'new_csv') {
    csvPath = dataPathInput ? dataPathInput.value.trim() : '';
    if (!csvPath) {
      alert('Provide an absolute CSV path for the manual test.');
      return;
    }
    if (!isAbsoluteFilesystemPath(csvPath)) {
      alert('CSV path must be absolute.');
      return;
    }
  }

  let sourceTab = 'optuna';
  if (ResultsState.activeTab === 'forward_test') {
    sourceTab = 'forward_test';
  } else if (ResultsState.activeTab === 'dsr') {
    sourceTab = 'dsr';
  } else if (ResultsState.activeTab === 'stress_test') {
    sourceTab = 'stress_test';
  }

  const payload = {
    dataSource,
    csvPath,
    startDate,
    endDate,
    trialNumbers,
    sourceTab
  };

  try {
    await runManualTestRequest(ResultsState.studyId, payload);
    await refreshManualTestsList();
    ResultsState.activeTab = 'manual_tests';
    await ensureManualTestSelection();
    updateTabsVisibility();
    renderManualTestControls();
    refreshResultsView();
    closeManualTestModal();
  } catch (error) {
    alert(error.message || 'Manual test failed.');
  }
}

function bindEventHandlers() {
  const cancelBtn = document.querySelector('.control-btn.cancel');
  if (cancelBtn) {
    cancelBtn.addEventListener('click', async () => {
      const runId = String(ResultsState.run_id || ResultsState.runId || '').trim();
      try {
        await cancelOptimizationRequest(runId);
      } catch (error) {
        console.warn('Cancel request failed', error);
      }
      ResultsState.status = 'cancelled';
      updateStatusBadge('cancelled');
      const controlPayload = { action: 'cancel', at: Date.now() };
      if (runId) {
        controlPayload.run_id = runId;
      }
      localStorage.setItem(OPT_CONTROL_KEY, JSON.stringify(controlPayload));
      updateStoredState({ status: 'cancelled', run_id: runId || '' });
    });
  }

  const pauseBtn = document.querySelector('.control-btn.pause');
  if (pauseBtn) {
    pauseBtn.addEventListener('click', () => {
      alert('Pause functionality coming soon');
    });
  }

  const stopBtn = document.querySelector('.control-btn.stop');
  if (stopBtn) {
    stopBtn.addEventListener('click', () => {
      alert('Stop functionality coming soon');
    });
  }

  const downloadBtn = document.getElementById('downloadTradesBtn');
  if (downloadBtn) {
    downloadBtn.addEventListener('click', async () => {
      if (!ResultsState.studyId) {
        alert('Select a study first.');
        return;
      }
      let endpoint = null;
      let requestOptions = { method: 'POST' };
      if (ResultsState.mode === 'wfa') {
        const selection = ResultsState.wfaSelection || {};
        if (selection.windowNumber) {
          endpoint = `/api/studies/${encodeURIComponent(ResultsState.studyId)}/wfa/windows/${selection.windowNumber}/trades`;
          requestOptions = {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
              moduleType: selection.moduleType,
              trialNumber: selection.trialNumber,
              period: selection.period
            })
          };
        } else {
          endpoint = `/api/studies/${encodeURIComponent(ResultsState.studyId)}/wfa/trades`;
        }
      } else {
        const activeTab = ResultsState.activeTab || 'optuna';
        if (activeTab === 'forward_test') {
          if (!ResultsState.selectedRowId) {
            alert('Select a trial in the table.');
            return;
          }
          endpoint = `/api/studies/${encodeURIComponent(ResultsState.studyId)}/trials/${ResultsState.selectedRowId}/ft-trades`;
        } else if (activeTab === 'oos_test') {
          if (!ResultsState.selectedRowId) {
            alert('Select a trial in the table.');
            return;
          }
          endpoint = `/api/studies/${encodeURIComponent(ResultsState.studyId)}/trials/${ResultsState.selectedRowId}/oos-trades`;
        } else if (activeTab === 'manual_tests') {
          if (!ResultsState.activeManualTest || !ResultsState.activeManualTest.id) {
            alert('Select a manual test first.');
            return;
          }
          if (!ResultsState.selectedRowId) {
            alert('Select a trial in the table.');
            return;
          }
          endpoint = `/api/studies/${encodeURIComponent(ResultsState.studyId)}/tests/${ResultsState.activeManualTest.id}/trials/${ResultsState.selectedRowId}/mt-trades`;
        } else {
          if (!ResultsState.selectedRowId) {
            alert('Select a trial in the table.');
            return;
          }
          endpoint = `/api/studies/${encodeURIComponent(ResultsState.studyId)}/trials/${ResultsState.selectedRowId}/trades`;
        }
      }
      try {
        const response = await fetch(endpoint, requestOptions);
        if (!response.ok) {
          const message = await response.text();
          throw new Error(message || 'Trade export failed.');
        }
        const blob = await response.blob();
        const url = window.URL.createObjectURL(blob);
        const link = document.createElement('a');
        link.href = url;
        const disposition = response.headers.get('Content-Disposition');
        let filename = `trades_${Date.now()}.csv`;
        if (disposition) {
          const match = disposition.match(/filename="?([^";]+)"?/i);
          if (match && match[1]) {
            filename = match[1];
          }
        }
        link.download = filename;
        document.body.appendChild(link);
        link.click();
        link.remove();
        window.URL.revokeObjectURL(url);
      } catch (error) {
        alert(error.message || 'Trade export failed.');
      }
    });
  }

  const manualBtn = document.getElementById('manualTestBtn');
  if (manualBtn) {
    manualBtn.addEventListener('click', () => {
      if (!ResultsState.studyId) {
        alert('Select a study first.');
        return;
      }
      openManualTestModal();
    });
  }

  const manualCancel = document.getElementById('manualTestCancel');
  if (manualCancel) {
    manualCancel.addEventListener('click', () => {
      closeManualTestModal();
    });
  }

  const manualRun = document.getElementById('manualTestRun');
  if (manualRun) {
    manualRun.addEventListener('click', async () => {
      await runManualTestFromModal();
    });
  }

  const manualSelect = document.getElementById('manualTestSelect');
  if (manualSelect) {
    manualSelect.addEventListener('change', async () => {
      const testId = manualSelect.value;
      await loadManualTestResultsById(testId);
      renderManualTestControls();
      refreshResultsView();
    });
  }

  const manualDelete = document.getElementById('manualTestDelete');
  if (manualDelete) {
    manualDelete.addEventListener('click', async () => {
      const testId = ResultsState.activeManualTest?.id;
      if (!testId) {
        alert('Select a test to delete.');
        return;
      }
      const confirmed = window.confirm('Delete this manual test? This cannot be undone.');
      if (!confirmed) return;
      try {
        await deleteManualTestRequest(ResultsState.studyId, testId);
        await refreshManualTestsList();
        ResultsState.activeManualTest = null;
        ResultsState.manualTestResults = [];
        await ensureManualTestSelection();
        renderManualTestControls();
        refreshResultsView();
      } catch (error) {
        alert(error.message || 'Failed to delete manual test.');
      }
    });
  }
}

async function hydrateFromServer() {
  try {
    const data = await fetchOptimizationStatus();
    if (!data || !data.status) return;

    const stored = readStoredState();
    const storedUpdated = stored && stored.updated_at ? Date.parse(stored.updated_at) : 0;
    const serverUpdated = data.updated_at ? Date.parse(data.updated_at) : 0;
    const shouldApply = !stored || !storedUpdated || (serverUpdated && serverUpdated >= storedUpdated);

      if (shouldApply) {
        applyState(data);
        if (data.study_id) {
          await openStudy(data.study_id);
        } else {
          refreshResultsView();
        }
      }
  } catch (error) {
    return;
  }
}

function initResultsPage() {
  bindCollapsibles();
  bindStudiesManager();
  bindEventHandlers();
  bindMissingCsvDialog();
  bindTabs();
  bindManualDataSourceToggle();
  bindTableExpandToggle();

  const stored = readStoredState();
  if (stored) {
    applyState(stored);
    refreshResultsView();
  }

  loadStudiesList().then((studies) => {
    const urlStudyId = getQueryStudyId();
    if (urlStudyId) {
      openStudy(urlStudyId);
      return;
    }
    if (stored && (stored.study_id || stored.studyId)) {
      openStudy(stored.study_id || stored.studyId);
      return;
    }
    if (studies && studies.length && studies[0].study_id) {
      openStudy(studies[0].study_id);
    }
  });
  loadDatabasesList();

  hydrateFromServer();

  window.addEventListener('storage', handleStorageUpdate);
}

// Minimal MD5 implementation (ASCII only)

document.addEventListener('DOMContentLoaded', initResultsPage);
