(function () {
  const DEFAULT_SORT_STATE = {
    sortColumn: null,
    sortDirection: null,
    sortClickCount: 0,
  };

  const SORT_LABELS = {
    study_name: 'Study Name',
    ann_profit_pct: 'Ann.P%',
    profit_pct: 'Profit%',
    max_dd_pct: 'MaxDD%',
    total_trades: 'Trades',
    wfe_pct: 'WFE%',
    profitable_windows_pct: 'OOS Wins',
    median_window_profit: 'OOS P(med)',
    median_window_wr: 'OOS WR(med)',
  };

  const AnalyticsState = {
    dbName: '',
    studies: [],
    researchInfo: {},
    checkedStudyIds: new Set(),
    orderedStudyIds: [],
    dbSwitchInProgress: false,
    filters: {
      strategy: null,
      symbol: null,
      tf: null,
      wfa: null,
      isOos: null,
    },
    autoSelect: false,
    groupDatesEnabled: true,
    sortState: { ...DEFAULT_SORT_STATE },
    filtersInitialized: false,
    focusedStudyId: null,
    sets: [],
    allStudiesMetrics: null,
    focusedSetId: null,
    checkedSetIds: new Set(),
    setViewMode: 'allStudies',
    setMoveMode: false,
    filterContextEpoch: 0,
    filterContextSignature: null,
    portfolioData: null,
    portfolioSelectionKey: null,
    portfolioPendingKey: null,
    portfolioDebounceTimer: null,
    portfolioAbortController: null,
    portfolioRequestToken: 0,
    focusedWindowBoundariesByStudyId: new Map(),
    focusedWindowBoundariesPendingStudyId: null,
    focusedWindowBoundariesAbortController: null,
    focusedWindowBoundariesRequestToken: 0,
    studyEquityByStudyId: new Map(),
    studyEquityPendingStudyId: null,
    studyEquityAbortController: null,
    studyEquityRequestToken: 0,
    setEquityBySetId: new Map(),
    setEquityPendingSetId: null,
    setEquityAbortController: null,
    setEquityRequestToken: 0,
    allStudiesEquity: null,
    allStudiesEquityPending: false,
    allStudiesEquityAbortController: null,
    allStudiesEquityRequestToken: 0,
  };

  const EMPTY_FILTERS = {
    strategy: null,
    symbol: null,
    tf: null,
    wfa: null,
    isOos: null,
  };

  const MISSING_TEXT = '-';
  const OBJECTIVE_LABELS = {
    net_profit_pct: 'Net Profit %',
    max_drawdown_pct: 'Max DD %',
    sharpe_ratio: 'Sharpe Ratio',
    sortino_ratio: 'Sortino Ratio',
    romad: 'RoMaD',
    profit_factor: 'Profit Factor',
    win_rate: 'Win Rate %',
    max_consecutive_losses: 'Max CL',
    sqn: 'SQN',
    ulcer_index: 'Ulcer Index',
    consistency_score: 'Consistency',
    total_trades: 'Total Trades',
    composite_score: 'Composite Score',
  };
  const CONSTRAINT_OPERATORS = {
    total_trades: '>=',
    net_profit_pct: '>=',
    max_drawdown_pct: '<=',
    sharpe_ratio: '>=',
    sortino_ratio: '>=',
    romad: '>=',
    profit_factor: '>=',
    win_rate: '>=',
    max_consecutive_losses: '<=',
    sqn: '>=',
    ulcer_index: '<=',
    consistency_score: '>=',
  };
  const SORT_METRIC_LABELS = {
    profit_degradation: 'Profit Degradation',
    ft_romad: 'FT RoMaD',
    profit_retention: 'Profit Retention',
    romad_retention: 'RoMaD Retention',
    combined_score: 'Combined Score',
  };

  function toFiniteNumber(value) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }

  function toNonNegativeInteger(value) {
    const parsed = toFiniteNumber(value);
    if (parsed === null) return 0;
    return Math.max(0, Math.round(parsed));
  }

  function average(values) {
    const finite = values
      .map((value) => toFiniteNumber(value))
      .filter((value) => value !== null);
    if (!finite.length) return null;
    const sum = finite.reduce((acc, value) => acc + value, 0);
    return sum / finite.length;
  }

  function formatSignedPercent(value, digits) {
    const parsed = toFiniteNumber(value);
    if (parsed === null) return MISSING_TEXT;
    if (parsed === 0) return `0.${'0'.repeat(digits)}%`;
    const sign = parsed > 0 ? '+' : '-';
    return `${sign}${Math.abs(parsed).toFixed(digits)}%`;
  }

  function formatNegativePercent(value, digits) {
    const parsed = toFiniteNumber(value);
    if (parsed === null) return MISSING_TEXT;
    return `-${Math.abs(parsed).toFixed(digits)}%`;
  }

  function formatUnsignedPercent(value, digits) {
    const parsed = toFiniteNumber(value);
    if (parsed === null) return MISSING_TEXT;
    return `${parsed.toFixed(digits)}%`;
  }

  function formatInteger(value) {
    const parsed = toFiniteNumber(value);
    if (parsed === null) return MISSING_TEXT;
    return String(Math.max(0, Math.round(parsed)));
  }

  function isMissingValue(value) {
    if (value === null || value === undefined) return true;
    if (typeof value === 'string') return value.trim() === '';
    return false;
  }

  function displayValue(value) {
    return isMissingValue(value) ? MISSING_TEXT : String(value);
  }

  function setChartSubtitle(text) {
    const subtitleEl = document.getElementById('analyticsChartSubtitle');
    if (!subtitleEl) return;
    const content = String(text || '').trim();
    subtitleEl.textContent = content;
    subtitleEl.hidden = content.length === 0;
  }

  function setChartWarning(text) {
    const warningEl = document.getElementById('analyticsChartWarning');
    if (!warningEl) return;
    const content = String(text || '').trim();
    warningEl.textContent = content;
    warningEl.hidden = content.length === 0;
  }

  function clearChartMeta() {
    setChartSubtitle('');
    setChartWarning('');
  }

  function computeAnnualizedProfitDisplay(study) {
    if (window.AnalyticsTable && typeof window.AnalyticsTable.computeAnnualizedProfitMetrics === 'function') {
      const metrics = window.AnalyticsTable.computeAnnualizedProfitMetrics(study || {});
      const ann = toFiniteNumber(metrics?.annProfitPct);
      const spanDays = toFiniteNumber(metrics?.oosSpanDays);
      if (ann === null) {
        if (spanDays !== null && spanDays > 0 && spanDays <= 30) {
          return {
            text: 'N/A',
            className: '',
            tooltip: `OOS period too short for meaningful annualization (${Math.round(spanDays)} days)`,
          };
        }
        return { text: 'N/A', className: '', tooltip: '' };
      }
      const className = ann >= 0 ? 'positive' : 'negative';
      if (spanDays !== null && spanDays >= 31 && spanDays < 90) {
        return {
          text: `${formatSignedPercent(ann, 1)}*`,
          className,
          tooltip: `Short OOS period (${Math.round(spanDays)} days) - annualized value may be misleading`,
        };
      }
      return { text: formatSignedPercent(ann, 1), className, tooltip: '' };
    }
    return { text: 'N/A', className: '', tooltip: '' };
  }

  function showMessage(message) {
    const messageEl = document.getElementById('analyticsMessage');
    if (!messageEl) return;
    const text = String(message || '').trim();
    if (!text) {
      messageEl.hidden = true;
      messageEl.textContent = '';
      return;
    }
    messageEl.hidden = false;
    messageEl.textContent = text;
  }

  function getStudyMap() {
    const map = new Map();
    AnalyticsState.studies.forEach((study) => {
      map.set(String(study.study_id || ''), study);
    });
    return map;
  }

  function cloneAnalyticsCurvePayload(payload) {
    const curve = Array.isArray(payload?.curve) ? payload.curve.slice() : [];
    const timestamps = Array.isArray(payload?.timestamps) ? payload.timestamps.slice() : [];
    return {
      curve,
      timestamps,
      return_profile: payload?.return_profile && typeof payload.return_profile === 'object'
        ? payload.return_profile
        : null,
      profit_pct: toFiniteNumber(payload?.profit_pct),
      max_drawdown_pct: toFiniteNumber(payload?.max_drawdown_pct),
      ann_profit_pct: toFiniteNumber(payload?.ann_profit_pct),
      overlap_days: toFiniteNumber(payload?.overlap_days),
      overlap_days_exact: toFiniteNumber(payload?.overlap_days_exact),
      studies_used: toFiniteNumber(payload?.studies_used),
      studies_excluded: toFiniteNumber(payload?.studies_excluded),
      selected_count: toFiniteNumber(payload?.selected_count),
      warning: String(payload?.warning || '').trim(),
      has_curve: curve.length > 0 && curve.length === timestamps.length,
    };
  }

  function cloneStudyEquityPayload(payload) {
    const curve = Array.isArray(payload?.curve) ? payload.curve.slice() : [];
    const timestamps = Array.isArray(payload?.timestamps) ? payload.timestamps.slice() : [];
    const pointCount = toNonNegativeInteger(payload?.point_count);
    return {
      curve,
      timestamps,
      point_count: pointCount,
      has_equity_curve: Boolean(payload?.has_equity_curve) && curve.length === timestamps.length,
      warning: String(payload?.warning || '').trim(),
    };
  }

  function normalizeStudyIdList(studyIds) {
    return (Array.isArray(studyIds) ? studyIds : [])
      .map((studyId) => String(studyId || '').trim())
      .filter(Boolean)
      .sort((left, right) => left.localeCompare(right, undefined, { numeric: true, sensitivity: 'base' }));
  }

  function studyIdListsMatch(leftIds, rightIds) {
    const left = normalizeStudyIdList(leftIds);
    const right = normalizeStudyIdList(rightIds);
    if (left.length !== right.length) return false;
    return left.every((studyId, index) => studyId === right[index]);
  }

  function cloneFilters(filters) {
    const source = filters || EMPTY_FILTERS;
    return {
      strategy: source.strategy instanceof Set ? new Set(source.strategy) : null,
      symbol: source.symbol instanceof Set ? new Set(source.symbol) : null,
      tf: source.tf instanceof Set ? new Set(source.tf) : null,
      wfa: source.wfa instanceof Set ? new Set(source.wfa) : null,
      isOos: source.isOos instanceof Set ? new Set(source.isOos) : null,
    };
  }

  function isTypingElement(element) {
    if (!element) return false;
    if (element.isContentEditable) return true;
    const tagName = element.tagName ? element.tagName.toLowerCase() : '';
    return tagName === 'input' || tagName === 'textarea' || tagName === 'select';
  }

  function hasOpenAnalyticsMenu() {
    return Boolean(
      document.querySelector('.analytics-filter.open')
      || document.querySelector('#analyticsSetUpdateMenu:not([hidden])')
      || document.querySelector('#analyticsSetColorMenu:not([hidden])')
    );
  }

  function getVisibleOrderedStudyIds() {
    if (!window.AnalyticsTable || typeof window.AnalyticsTable.getVisibleStudyIds !== 'function') {
      return AnalyticsState.orderedStudyIds.slice();
    }
    return window.AnalyticsTable.getVisibleStudyIds()
      .map((studyId) => String(studyId || '').trim())
      .filter(Boolean);
  }

  function setAllStudiesChecked(checked) {
    if (!window.AnalyticsTable || typeof window.AnalyticsTable.setAllChecked !== 'function') return;
    window.AnalyticsTable.setAllChecked(Boolean(checked));
  }

  function deselectAllStudies() {
    setAllStudiesChecked(false);
  }

  function scrollStudyIntoView(studyId) {
    if (!window.AnalyticsTable || typeof window.AnalyticsTable.scrollStudyIntoView !== 'function') return;
    window.AnalyticsTable.scrollStudyIntoView(studyId);
  }

  function scrollSetIntoView(setId) {
    if (!window.AnalyticsSets || typeof window.AnalyticsSets.scrollSetIntoView !== 'function') return;
    window.AnalyticsSets.scrollSetIntoView(setId);
  }

  function getVisibleStudyCount() {
    return getVisibleOrderedStudyIds().length;
  }

  function getTotalWfaStudyCount() {
    const researchCount = toFiniteNumber(AnalyticsState.researchInfo?.wfa_studies);
    if (researchCount !== null) {
      return Math.max(0, Math.round(researchCount));
    }
    return AnalyticsState.studies.length;
  }

  function formatHeaderCount(value) {
    return toNonNegativeInteger(value).toLocaleString('en-US');
  }

  function filtersEqual(left, right) {
    const keys = Object.keys(EMPTY_FILTERS);
    return keys.every((key) => {
      const leftSet = left?.[key];
      const rightSet = right?.[key];
      const leftIsSet = leftSet instanceof Set;
      const rightIsSet = rightSet instanceof Set;
      if (leftIsSet !== rightIsSet) return false;
      if (!leftIsSet) return true;
      if (leftSet.size !== rightSet.size) return false;
      for (const value of leftSet) {
        if (!rightSet.has(value)) return false;
      }
      return true;
    });
  }

  function syncSetStateFromModule() {
    if (!window.AnalyticsSets) return;
    const previousSets = AnalyticsState.sets;
    const nextSets = typeof window.AnalyticsSets.getSets === 'function'
      ? window.AnalyticsSets.getSets()
      : [];
    const nextSetIds = new Set(
      nextSets
        .map((setItem) => toNonNegativeInteger(setItem?.id))
        .filter((setId) => setId > 0)
    );
    previousSets.forEach((setItem) => {
      const setId = toNonNegativeInteger(setItem?.id);
      if (setId > 0 && !nextSetIds.has(setId)) {
        AnalyticsState.setEquityBySetId.delete(setId);
        if (AnalyticsState.setEquityPendingSetId === setId) {
          cancelFocusedSetEquityFetch();
        }
      }
    });
    nextSets.forEach((setItem) => {
      const setId = toNonNegativeInteger(setItem?.id);
      if (setId <= 0) return;
      const previous = previousSets.find((item) => toNonNegativeInteger(item?.id) === setId) || null;
      if (!previous) return;
      if (!studyIdListsMatch(previous.study_ids, setItem.study_ids)) {
        AnalyticsState.setEquityBySetId.delete(setId);
        if (AnalyticsState.setEquityPendingSetId === setId) {
          cancelFocusedSetEquityFetch();
        }
      }
    });

    AnalyticsState.sets = nextSets;
    AnalyticsState.allStudiesMetrics = typeof window.AnalyticsSets.getAllMetrics === 'function'
      ? window.AnalyticsSets.getAllMetrics()
      : null;
    AnalyticsState.focusedSetId = typeof window.AnalyticsSets.getFocusedSetId === 'function'
      ? window.AnalyticsSets.getFocusedSetId()
      : null;
    AnalyticsState.checkedSetIds = typeof window.AnalyticsSets.getCheckedSetIds === 'function'
      ? new Set(Array.from(window.AnalyticsSets.getCheckedSetIds()))
      : new Set();
    AnalyticsState.setViewMode = typeof window.AnalyticsSets.getViewMode === 'function'
      ? String(window.AnalyticsSets.getViewMode() || 'allStudies')
      : 'allStudies';
    AnalyticsState.setMoveMode = Boolean(
      typeof window.AnalyticsSets.isMoveMode === 'function' && window.AnalyticsSets.isMoveMode()
    );
  }

  function getSetVisibleStudyIds() {
    if (!window.AnalyticsSets || typeof window.AnalyticsSets.getViewMode !== 'function') return null;
    if (window.AnalyticsSets.getViewMode() === 'allStudies') return null;
    if (typeof window.AnalyticsSets.getVisibleStudyIds !== 'function') return new Set();
    const ids = window.AnalyticsSets.getVisibleStudyIds();
    if (ids === null) return null;
    return ids instanceof Set ? new Set(ids) : new Set();
  }

  function getStudiesForFilterContext(setVisibleStudyIds) {
    if (!(setVisibleStudyIds instanceof Set)) {
      return AnalyticsState.studies.slice();
    }
    const map = getStudyMap();
    return Array.from(setVisibleStudyIds)
      .map((studyId) => map.get(String(studyId || '').trim()))
      .filter(Boolean);
  }

  function buildFilterContextSignature(setVisibleStudyIds) {
    const epochPrefix = `epoch:${AnalyticsState.filterContextEpoch}`;
    if (!(setVisibleStudyIds instanceof Set)) {
      return `${epochPrefix}|all`;
    }
    const normalized = Array.from(setVisibleStudyIds)
      .map((studyId) => String(studyId || '').trim())
      .filter(Boolean)
      .sort((left, right) => left.localeCompare(right, undefined, { numeric: true, sensitivity: 'base' }));
    return `${epochPrefix}|subset:${normalized.join('|')}`;
  }

  function refreshFiltersForCurrentContext(setVisibleStudyIds, options = {}) {
    if (!window.AnalyticsFilters) return;
    const force = options.force === true;
    const signature = buildFilterContextSignature(setVisibleStudyIds);
    if (!force && AnalyticsState.filterContextSignature === signature) {
      return;
    }
    AnalyticsState.filterContextSignature = signature;

    const contextStudies = getStudiesForFilterContext(setVisibleStudyIds);
    window.AnalyticsFilters.updateStudies(contextStudies, { emitChange: false });
    const nextFilters = cloneFilters(window.AnalyticsFilters.getFilters());
    if (!filtersEqual(AnalyticsState.filters, nextFilters)) {
      AnalyticsState.filters = nextFilters;
    }
  }

  function handleSetsStateChange(eventPayload) {
    syncSetStateFromModule();

    const syncIds = eventPayload?.syncCheckedStudyIds;
    if (syncIds instanceof Set) {
      AnalyticsState.checkedStudyIds = new Set(Array.from(syncIds));
    } else if (Array.isArray(syncIds)) {
      AnalyticsState.checkedStudyIds = new Set(syncIds.map((id) => String(id || '').trim()).filter(Boolean));
    }

    renderTableWithCurrentState();
  }

  function getSelectedStudies() {
    const map = getStudyMap();
    return Array.from(AnalyticsState.checkedStudyIds)
      .map((studyId) => map.get(studyId))
      .filter(Boolean);
  }

  function getSelectedStudyIds() {
    return getSelectedStudies()
      .map((study) => String(study?.study_id || '').trim())
      .filter(Boolean)
      .sort((left, right) => left.localeCompare(right, undefined, { numeric: true, sensitivity: 'base' }));
  }

  function getAllStudyIds() {
    return normalizeStudyIdList(AnalyticsState.studies.map((study) => study?.study_id));
  }

  function isAllStudiesSelection(studyIds) {
    const selectedIds = Array.isArray(studyIds) ? studyIds : getSelectedStudyIds();
    return studyIdListsMatch(selectedIds, getAllStudyIds());
  }

  function buildSelectionKey(studyIds) {
    return Array.isArray(studyIds) && studyIds.length ? studyIds.join('|') : '';
  }

  function cancelPortfolioFetches() {
    if (AnalyticsState.portfolioDebounceTimer !== null) {
      window.clearTimeout(AnalyticsState.portfolioDebounceTimer);
      AnalyticsState.portfolioDebounceTimer = null;
    }
    if (AnalyticsState.portfolioAbortController) {
      AnalyticsState.portfolioAbortController.abort();
      AnalyticsState.portfolioAbortController = null;
    }
    AnalyticsState.portfolioPendingKey = null;
    AnalyticsState.portfolioRequestToken += 1;
  }

  function clearPortfolioState() {
    cancelPortfolioFetches();
    AnalyticsState.portfolioData = null;
    AnalyticsState.portfolioSelectionKey = null;
  }

  function cancelFocusedWindowBoundariesFetch() {
    if (AnalyticsState.focusedWindowBoundariesAbortController) {
      AnalyticsState.focusedWindowBoundariesAbortController.abort();
      AnalyticsState.focusedWindowBoundariesAbortController = null;
    }
    AnalyticsState.focusedWindowBoundariesPendingStudyId = null;
    AnalyticsState.focusedWindowBoundariesRequestToken += 1;
  }

  function clearFocusedWindowBoundariesState() {
    cancelFocusedWindowBoundariesFetch();
    AnalyticsState.focusedWindowBoundariesByStudyId = new Map();
  }

  function getFocusedWindowBoundaries(studyId) {
    const normalizedStudyId = String(studyId || '').trim();
    if (!normalizedStudyId) return null;
    if (!AnalyticsState.focusedWindowBoundariesByStudyId.has(normalizedStudyId)) return null;
    const boundaries = AnalyticsState.focusedWindowBoundariesByStudyId.get(normalizedStudyId);
    return Array.isArray(boundaries) ? boundaries : [];
  }

  function normalizeWindowBoundaries(boundaries) {
    if (!Array.isArray(boundaries)) return [];
    return boundaries
      .map((item, index) => {
        const time = String(item?.time || item?.timestamp || item?.date || '').trim();
        if (!time) return null;
        const windowNumber = toFiniteNumber(item?.window_number);
        const normalizedNumber = windowNumber === null ? null : Math.max(1, Math.round(windowNumber));
        const label = String(item?.label || '').trim()
          || (normalizedNumber !== null ? `W${normalizedNumber}` : `W${index + 1}`);
        return {
          time,
          window_id: item?.window_id || null,
          window_number: normalizedNumber,
          label,
        };
      })
      .filter(Boolean);
  }

  function ensureFocusedWindowBoundaries(study) {
    if (!study || typeof fetchAnalyticsStudyWindowBoundariesRequest !== 'function') return;
    const studyId = String(study.study_id || '').trim();
    if (!studyId) return;
    if (AnalyticsState.focusedWindowBoundariesByStudyId.has(studyId)) return;
    if (AnalyticsState.focusedWindowBoundariesPendingStudyId === studyId) return;

    cancelFocusedWindowBoundariesFetch();
    AnalyticsState.focusedWindowBoundariesPendingStudyId = studyId;

    const requestToken = AnalyticsState.focusedWindowBoundariesRequestToken + 1;
    AnalyticsState.focusedWindowBoundariesRequestToken = requestToken;

    const controller = new AbortController();
    AnalyticsState.focusedWindowBoundariesAbortController = controller;

    fetchAnalyticsStudyWindowBoundariesRequest(studyId, controller.signal)
      .then((payload) => {
        if (requestToken !== AnalyticsState.focusedWindowBoundariesRequestToken) return;
        if (AnalyticsState.focusedWindowBoundariesPendingStudyId !== studyId) return;
        const boundaries = normalizeWindowBoundaries(payload?.boundaries);
        AnalyticsState.focusedWindowBoundariesByStudyId.set(studyId, boundaries);
      })
      .catch((error) => {
        if (controller.signal.aborted || error?.name === 'AbortError') {
          return;
        }
        if (requestToken !== AnalyticsState.focusedWindowBoundariesRequestToken) return;
        if (AnalyticsState.focusedWindowBoundariesPendingStudyId !== studyId) return;
        AnalyticsState.focusedWindowBoundariesByStudyId.set(studyId, []);
        console.warn('Failed to load analytics focused window boundaries', error);
      })
      .finally(() => {
        if (requestToken !== AnalyticsState.focusedWindowBoundariesRequestToken) return;
        if (AnalyticsState.focusedWindowBoundariesPendingStudyId !== studyId) return;
        AnalyticsState.focusedWindowBoundariesPendingStudyId = null;
        AnalyticsState.focusedWindowBoundariesAbortController = null;
        if (String(AnalyticsState.focusedStudyId || '') === studyId) {
          renderSelectedStudyChart();
        }
      });
  }

  function cancelFocusedStudyEquityFetch() {
    if (AnalyticsState.studyEquityAbortController) {
      AnalyticsState.studyEquityAbortController.abort();
      AnalyticsState.studyEquityAbortController = null;
    }
    AnalyticsState.studyEquityPendingStudyId = null;
    AnalyticsState.studyEquityRequestToken += 1;
  }

  function clearFocusedStudyEquityState() {
    cancelFocusedStudyEquityFetch();
    AnalyticsState.studyEquityByStudyId = new Map();
  }

  function getCachedStudyEquity(studyId) {
    const normalizedStudyId = String(studyId || '').trim();
    if (!normalizedStudyId) return null;
    return AnalyticsState.studyEquityByStudyId.get(normalizedStudyId) || null;
  }

  function ensureStudyEquity(study) {
    if (!study || typeof fetchAnalyticsStudyEquityRequest !== 'function') return;
    const studyId = String(study.study_id || '').trim();
    if (!studyId || !study?.has_equity_curve) return;
    if (AnalyticsState.studyEquityByStudyId.has(studyId)) return;
    if (AnalyticsState.studyEquityPendingStudyId === studyId) return;

    cancelFocusedStudyEquityFetch();
    AnalyticsState.studyEquityPendingStudyId = studyId;

    const requestToken = AnalyticsState.studyEquityRequestToken + 1;
    AnalyticsState.studyEquityRequestToken = requestToken;

    const controller = new AbortController();
    AnalyticsState.studyEquityAbortController = controller;

    fetchAnalyticsStudyEquityRequest(studyId, controller.signal)
      .then((payload) => {
        if (requestToken !== AnalyticsState.studyEquityRequestToken) return;
        if (AnalyticsState.studyEquityPendingStudyId !== studyId) return;
        AnalyticsState.studyEquityByStudyId.set(studyId, cloneStudyEquityPayload(payload));
      })
      .catch((error) => {
        if (controller.signal.aborted || error?.name === 'AbortError') {
          return;
        }
        if (requestToken !== AnalyticsState.studyEquityRequestToken) return;
        if (AnalyticsState.studyEquityPendingStudyId !== studyId) return;
        AnalyticsState.studyEquityByStudyId.set(
          studyId,
          cloneStudyEquityPayload({
            curve: [],
            timestamps: [],
            point_count: 0,
            has_equity_curve: false,
            warning: error?.message || 'Failed to load stitched OOS equity.',
          })
        );
        console.warn('Failed to load analytics study equity', error);
      })
      .finally(() => {
        if (requestToken !== AnalyticsState.studyEquityRequestToken) return;
        if (AnalyticsState.studyEquityPendingStudyId !== studyId) return;
        AnalyticsState.studyEquityPendingStudyId = null;
        AnalyticsState.studyEquityAbortController = null;
        renderSelectedStudyChart();
      });
  }

  function cancelFocusedSetEquityFetch() {
    if (AnalyticsState.setEquityAbortController) {
      AnalyticsState.setEquityAbortController.abort();
      AnalyticsState.setEquityAbortController = null;
    }
    AnalyticsState.setEquityPendingSetId = null;
    AnalyticsState.setEquityRequestToken += 1;
  }

  function clearFocusedSetEquityState() {
    cancelFocusedSetEquityFetch();
    AnalyticsState.setEquityBySetId = new Map();
  }

  function getCachedSetEquity(setId) {
    const normalizedSetId = toNonNegativeInteger(setId);
    if (normalizedSetId <= 0) return null;
    return AnalyticsState.setEquityBySetId.get(normalizedSetId) || null;
  }

  function ensureFocusedSetEquity(setId) {
    if (typeof fetchAnalyticsSetEquityRequest !== 'function') return;
    const normalizedSetId = toNonNegativeInteger(setId);
    if (normalizedSetId <= 0) return;
    if (AnalyticsState.setEquityBySetId.has(normalizedSetId)) return;
    if (AnalyticsState.setEquityPendingSetId === normalizedSetId) return;

    const focusedSet = AnalyticsState.sets.find((setItem) => toNonNegativeInteger(setItem?.id) === normalizedSetId);
    const selectedCount = Array.isArray(focusedSet?.study_ids) ? focusedSet.study_ids.length : 0;

    cancelFocusedSetEquityFetch();
    AnalyticsState.setEquityPendingSetId = normalizedSetId;

    const requestToken = AnalyticsState.setEquityRequestToken + 1;
    AnalyticsState.setEquityRequestToken = requestToken;

    const controller = new AbortController();
    AnalyticsState.setEquityAbortController = controller;

    fetchAnalyticsSetEquityRequest(normalizedSetId, controller.signal)
      .then((payload) => {
        if (requestToken !== AnalyticsState.setEquityRequestToken) return;
        if (AnalyticsState.setEquityPendingSetId !== normalizedSetId) return;
        AnalyticsState.setEquityBySetId.set(normalizedSetId, cloneAnalyticsCurvePayload(payload));
      })
      .catch((error) => {
        if (controller.signal.aborted || error?.name === 'AbortError') {
          return;
        }
        if (requestToken !== AnalyticsState.setEquityRequestToken) return;
        if (AnalyticsState.setEquityPendingSetId !== normalizedSetId) return;
        AnalyticsState.setEquityBySetId.set(
          normalizedSetId,
          cloneAnalyticsCurvePayload({
            curve: [],
            timestamps: [],
            profit_pct: null,
            max_drawdown_pct: null,
            ann_profit_pct: null,
            overlap_days: 0,
            overlap_days_exact: 0.0,
            studies_used: 0,
            studies_excluded: selectedCount,
            selected_count: selectedCount,
            return_profile: null,
            warning: error?.message || 'Failed to load set equity.',
          })
        );
        console.warn('Failed to load analytics set equity', error);
      })
      .finally(() => {
        if (requestToken !== AnalyticsState.setEquityRequestToken) return;
        if (AnalyticsState.setEquityPendingSetId !== normalizedSetId) return;
        AnalyticsState.setEquityPendingSetId = null;
        AnalyticsState.setEquityAbortController = null;
        renderSummaryCards();
        renderSelectedStudyChart();
      });
  }

  function cancelAllStudiesEquityFetch() {
    if (AnalyticsState.allStudiesEquityAbortController) {
      AnalyticsState.allStudiesEquityAbortController.abort();
      AnalyticsState.allStudiesEquityAbortController = null;
    }
    AnalyticsState.allStudiesEquityPending = false;
    AnalyticsState.allStudiesEquityRequestToken += 1;
  }

  function clearAllStudiesEquityState() {
    cancelAllStudiesEquityFetch();
    AnalyticsState.allStudiesEquity = null;
  }

  function getCurrentAllStudiesData() {
    return AnalyticsState.allStudiesEquity || AnalyticsState.allStudiesMetrics || null;
  }

  function ensureAllStudiesEquity() {
    if (typeof fetchAnalyticsAllStudiesEquityRequest !== 'function') return;
    if (AnalyticsState.allStudiesEquity) return;
    if (AnalyticsState.allStudiesEquityPending) return;

    const selectedCount = getAllStudyIds().length;
    AnalyticsState.allStudiesEquityPending = true;

    const requestToken = AnalyticsState.allStudiesEquityRequestToken + 1;
    AnalyticsState.allStudiesEquityRequestToken = requestToken;

    const controller = new AbortController();
    AnalyticsState.allStudiesEquityAbortController = controller;

    fetchAnalyticsAllStudiesEquityRequest(controller.signal)
      .then((payload) => {
        if (requestToken !== AnalyticsState.allStudiesEquityRequestToken) return;
        if (!AnalyticsState.allStudiesEquityPending) return;
        AnalyticsState.allStudiesEquity = cloneAnalyticsCurvePayload(payload);
      })
      .catch((error) => {
        if (controller.signal.aborted || error?.name === 'AbortError') {
          return;
        }
        if (requestToken !== AnalyticsState.allStudiesEquityRequestToken) return;
        if (!AnalyticsState.allStudiesEquityPending) return;
        AnalyticsState.allStudiesEquity = cloneAnalyticsCurvePayload({
          curve: [],
          timestamps: [],
          profit_pct: null,
          max_drawdown_pct: null,
          ann_profit_pct: null,
          overlap_days: 0,
          overlap_days_exact: 0.0,
          studies_used: 0,
          studies_excluded: selectedCount,
          selected_count: selectedCount,
          return_profile: null,
          warning: error?.message || 'Failed to load all-studies equity.',
        });
        console.warn('Failed to load all-studies analytics equity', error);
      })
      .finally(() => {
        if (requestToken !== AnalyticsState.allStudiesEquityRequestToken) return;
        AnalyticsState.allStudiesEquityPending = false;
        AnalyticsState.allStudiesEquityAbortController = null;
        renderSummaryCards();
        renderSelectedStudyChart();
      });
  }

  function ensurePortfolioDataForSelection() {
    const focusedStudy = getFocusedStudy();
    const focusedSet = getFocusedSet();

    if (focusedStudy) {
      clearPortfolioState();
      cancelFocusedSetEquityFetch();
      cancelAllStudiesEquityFetch();
      return;
    }

    if (focusedSet) {
      clearPortfolioState();
      cancelAllStudiesEquityFetch();
      ensureFocusedSetEquity(focusedSet.id);
      return;
    }

    cancelFocusedSetEquityFetch();

    const studyIds = getSelectedStudyIds();
    if (studyIds.length < 2) {
      clearPortfolioState();
      cancelAllStudiesEquityFetch();
      return;
    }

    if (isAllStudiesSelection(studyIds)) {
      clearPortfolioState();
      ensureAllStudiesEquity();
      return;
    }

    cancelAllStudiesEquityFetch();

    const selectionKey = buildSelectionKey(studyIds);
    if (AnalyticsState.portfolioSelectionKey !== selectionKey) {
      cancelPortfolioFetches();
      AnalyticsState.portfolioSelectionKey = selectionKey;
      AnalyticsState.portfolioData = null;
    }

    if (AnalyticsState.portfolioData && AnalyticsState.portfolioSelectionKey === selectionKey) {
      return;
    }
    if (AnalyticsState.portfolioPendingKey === selectionKey) {
      return;
    }

    cancelPortfolioFetches();
    AnalyticsState.portfolioPendingKey = selectionKey;
    AnalyticsState.portfolioDebounceTimer = window.setTimeout(async () => {
      const requestToken = AnalyticsState.portfolioRequestToken + 1;
      AnalyticsState.portfolioRequestToken = requestToken;
      AnalyticsState.portfolioDebounceTimer = null;

      const controller = new AbortController();
      AnalyticsState.portfolioAbortController = controller;
      try {
        const payload = await fetchAnalyticsEquityRequest(studyIds, controller.signal);
        if (requestToken !== AnalyticsState.portfolioRequestToken) return;
        if (AnalyticsState.portfolioSelectionKey !== selectionKey) return;
        AnalyticsState.portfolioPendingKey = null;
        AnalyticsState.portfolioAbortController = null;
        AnalyticsState.portfolioData = cloneAnalyticsCurvePayload(payload);
      } catch (error) {
        if (controller.signal.aborted || error?.name === 'AbortError') {
          return;
        }
        if (requestToken !== AnalyticsState.portfolioRequestToken) return;
        AnalyticsState.portfolioPendingKey = null;
        AnalyticsState.portfolioAbortController = null;
        AnalyticsState.portfolioData = cloneAnalyticsCurvePayload({
          curve: null,
          timestamps: null,
          profit_pct: null,
          max_drawdown_pct: null,
          ann_profit_pct: null,
          overlap_days: 0,
          overlap_days_exact: 0.0,
          studies_used: 0,
          studies_excluded: studyIds.length,
          return_profile: null,
          warning: error?.message || 'Failed to aggregate portfolio equity.',
        });
      }
      updateVisualsForSelection();
    }, 300);
  }

  function getCurrentPortfolioData() {
    const studyIds = getSelectedStudyIds();
    if (studyIds.length < 2) return null;
    const currentKey = buildSelectionKey(studyIds);
    if (AnalyticsState.portfolioSelectionKey !== currentKey) return null;
    return AnalyticsState.portfolioData;
  }

  function formatObjectiveLabel(name) {
    const key = String(name || '').trim();
    return OBJECTIVE_LABELS[key] || key || MISSING_TEXT;
  }

  function formatTitleFromKey(value) {
    const safe = String(value || '').trim();
    if (!safe) return '';
    return safe
      .split(/[_\s-]+/)
      .filter(Boolean)
      .map((token) => token.charAt(0).toUpperCase() + token.slice(1))
      .join(' ');
  }

  function formatSortMetricLabel(metric) {
    const key = String(metric || '').trim().toLowerCase();
    return SORT_METRIC_LABELS[key] || formatTitleFromKey(key) || MISSING_TEXT;
  }

  function formatCompactPostProcessSortMetricLabel(metric) {
    const normalized = String(metric || '').trim().toLowerCase();
    const compactLabels = {
      profit_degradation: 'Profit Deg',
      profit_retention: 'Profit Ret',
      romad_retention: 'RoMaD Ret',
    };
    return compactLabels[normalized] || formatSortMetricLabel(normalized) || MISSING_TEXT;
  }

  function formatPercentWithOptionalSign(value, digits = 1) {
    const number = toFiniteNumber(value);
    if (number === null) return MISSING_TEXT;
    const sign = number > 0 ? '+' : (number < 0 ? '-' : '');
    return `${sign}${Math.abs(number).toFixed(digits)}%`;
  }

  function formatPostProcessActionLabel(action) {
    const normalized = String(action || '').trim().toLowerCase();
    if (normalized === 'cooldown_reoptimize') return 'CD + Re-opt';
    if (normalized === 'no_trade') return 'No Trade';
    return formatTitleFromKey(normalized) || MISSING_TEXT;
  }

  function buildPostProcessSettingsRows(settings, isWfaStudy) {
    const config = settings && typeof settings === 'object' ? settings : {};
    const rows = [];
    if (config.ft_enabled) {
      const ftParts = [
        `${config.ft_period_days ?? MISSING_TEXT}d`,
        `Top ${config.ft_top_k ?? MISSING_TEXT}`,
        `Sort: ${formatCompactPostProcessSortMetricLabel(config.ft_sort_metric)}`,
        `Threshold: ${formatPercentWithOptionalSign(config.ft_threshold_pct, 1)}`,
      ];
      if (isWfaStudy) {
        const rejectAction = String(config.ft_reject_action || '').trim().toLowerCase();
        ftParts.push(`Policy: ${formatPostProcessActionLabel(rejectAction)}`);
        if (rejectAction === 'cooldown_reoptimize') {
          ftParts.push(`CD ${config.ft_reject_cooldown_days ?? MISSING_TEXT}d`);
          ftParts.push(`Retry ${config.ft_reject_max_attempts ?? MISSING_TEXT}`);
          ftParts.push(`Min OOS ${config.ft_reject_min_remaining_oos_days ?? MISSING_TEXT}d`);
        }
      }
      rows.push({
        key: 'Forward Test',
        val: ftParts.join(', '),
      });
    }
    if (config.dsr_enabled) {
      rows.push({
        key: 'DSR',
        val: `Top ${config.dsr_top_k ?? MISSING_TEXT}`,
      });
    }
    if (config.st_enabled) {
      const failureThresholdRaw = toFiniteNumber(config.st_failure_threshold);
      const failureThresholdPct = failureThresholdRaw === null
        ? MISSING_TEXT
        : `${((failureThresholdRaw > 1 ? failureThresholdRaw : failureThresholdRaw * 100)).toFixed(1)}%`;
      rows.push({
        key: 'Stress Test',
        val: `Top ${config.st_top_k ?? MISSING_TEXT}, Failure: ${failureThresholdPct}, Sort: ${formatCompactPostProcessSortMetricLabel(config.st_sort_metric)}`,
      });
    }
    return rows;
  }

  function formatObjectivesList(objectives) {
    if (!Array.isArray(objectives) || !objectives.length) return MISSING_TEXT;
    return objectives.map((item) => formatObjectiveLabel(item)).join(', ');
  }

  function formatConstraintsSummary(constraints) {
    if (!Array.isArray(constraints) || !constraints.length) return 'None';
    const enabled = constraints.filter((item) => item && item.enabled);
    if (!enabled.length) return 'None';
    return enabled.map((item) => {
      const metric = String(item.metric || '').trim();
      const operator = CONSTRAINT_OPERATORS[metric] || '';
      const threshold = item.threshold !== undefined && item.threshold !== null ? item.threshold : '-';
      return `${formatObjectiveLabel(metric)}${operator ? ` ${operator}` : ''} ${threshold}`;
    }).join(', ');
  }

  function formatBudgetLabel(settings) {
    const mode = String(settings?.budget_mode || '').trim().toLowerCase();
    if (!mode) return MISSING_TEXT;
    if (mode === 'trials') {
      const nTrials = toFiniteNumber(settings?.n_trials);
      return `${nTrials === null ? 0 : Math.max(0, Math.round(nTrials))} trials`;
    }
    if (mode === 'time') {
      const timeLimit = toFiniteNumber(settings?.time_limit);
      const minutes = timeLimit === null ? 0 : Math.round(timeLimit / 60);
      return `${Math.max(0, minutes)} min`;
    }
    if (mode === 'convergence') {
      const patience = toFiniteNumber(settings?.convergence_patience);
      return `No improvement ${patience === null ? 0 : Math.max(0, Math.round(patience))} trials`;
    }
    return MISSING_TEXT;
  }

  function formatDuration(seconds) {
    const totalSeconds = toFiniteNumber(seconds);
    if (totalSeconds === null || totalSeconds < 0) return '';
    const rounded = Math.round(totalSeconds);
    const hours = Math.floor(rounded / 3600);
    const minutes = Math.floor((rounded % 3600) / 60);
    const secs = rounded % 60;
    if (hours > 0) return `${hours}h ${minutes}m ${secs}s`;
    if (minutes > 0) return `${minutes}m ${secs}s`;
    return `${secs}s`;
  }

  function formatSanitizeLabel(settings) {
    const sanitizeEnabledRaw = settings?.sanitize_enabled;
    const sanitizeEnabled = sanitizeEnabledRaw === undefined || sanitizeEnabledRaw === null
      ? null
      : Boolean(sanitizeEnabledRaw);
    const sanitizeThresholdRaw = settings?.sanitize_trades_threshold;
    const sanitizeThreshold = toFiniteNumber(sanitizeThresholdRaw) === null
      ? 0
      : Math.max(0, Math.round(Number(sanitizeThresholdRaw)));
    if (sanitizeEnabled === true) return `On (<= ${sanitizeThreshold})`;
    if (sanitizeEnabled === false) return 'Off';
    return MISSING_TEXT;
  }

  function formatInitialLabel(settings) {
    const warmupRaw = settings?.warmup_trials;
    const warmup = toFiniteNumber(warmupRaw);
    if (warmup === null) return MISSING_TEXT;
    const initialValue = String(Math.max(0, Math.round(Number(warmup))));
    const coverageModeRaw = settings?.coverage_mode;
    const coverageMode = coverageModeRaw === undefined || coverageModeRaw === null
      ? null
      : Boolean(coverageModeRaw);
    return coverageMode === true ? `${initialValue} (coverage)` : initialValue;
  }

  function formatFilterLabel(settings) {
    const filterMinProfitRaw = settings?.filter_min_profit;
    const filterMinProfit = filterMinProfitRaw === undefined || filterMinProfitRaw === null
      ? false
      : Boolean(filterMinProfitRaw);
    const minProfitThresholdRaw = settings?.min_profit_threshold;
    const minProfitThreshold = toFiniteNumber(minProfitThresholdRaw) === null
      ? null
      : Math.max(0, Math.round(Number(minProfitThresholdRaw)));

    const scoreFilterEnabledRaw = settings?.score_filter_enabled;
    const scoreFilterEnabled = scoreFilterEnabledRaw === undefined || scoreFilterEnabledRaw === null
      ? false
      : Boolean(scoreFilterEnabledRaw);
    const scoreThresholdRaw = settings?.score_min_threshold;
    const scoreThreshold = toFiniteNumber(scoreThresholdRaw) === null
      ? null
      : Math.max(0, Math.round(Number(scoreThresholdRaw)));

    const filterParts = [];
    if (filterMinProfit) {
      filterParts.push(`Net Profit = ${minProfitThreshold !== null ? minProfitThreshold : 0}`);
    }
    if (scoreFilterEnabled) {
      filterParts.push(`Score = ${scoreThreshold !== null ? scoreThreshold : 0}`);
    }
    return filterParts.length ? filterParts.join(', ') : 'Off';
  }

  function computeRunTimeSeconds(study, wfaSettings) {
    const explicitRuntime = toFiniteNumber(wfaSettings?.run_time_seconds);
    if (explicitRuntime !== null) {
      return Math.max(0, Math.round(explicitRuntime));
    }

    const createdEpoch = toFiniteNumber(study?.created_at_epoch);
    const completedEpoch = toFiniteNumber(study?.completed_at_epoch);
    if (createdEpoch !== null && completedEpoch !== null && completedEpoch >= createdEpoch) {
      return Math.round(completedEpoch - createdEpoch);
    }

    const createdAt = Date.parse(String(study?.created_at || '').trim());
    const completedAt = Date.parse(String(study?.completed_at || '').trim());
    if (Number.isFinite(createdAt) && Number.isFinite(completedAt) && completedAt >= createdAt) {
      return Math.round((completedAt - createdAt) / 1000);
    }

    return null;
  }

  function renderSettingsList(container, rows) {
    if (!container) return;
    container.innerHTML = '';
    (rows || []).forEach((row) => {
      const item = document.createElement('div');
      item.className = 'setting-item';

      const key = document.createElement('span');
      key.className = 'key';
      key.textContent = String(row.key || '');

      const val = document.createElement('span');
      val.className = 'val';
      val.textContent = displayValue(row.val);

      item.appendChild(key);
      item.appendChild(val);
      container.appendChild(item);
    });
  }

  function hideFocusSidebar() {
    const optunaSection = document.getElementById('analytics-optuna-section');
    const postProcessSection = document.getElementById('analytics-post-process-section');
    const wfaSection = document.getElementById('analytics-wfa-section');
    if (optunaSection) optunaSection.style.display = 'none';
    if (postProcessSection) postProcessSection.style.display = 'none';
    if (wfaSection) wfaSection.style.display = 'none';
  }

  function renderFocusedSidebar(study) {
    const optunaSection = document.getElementById('analytics-optuna-section');
    const postProcessSection = document.getElementById('analytics-post-process-section');
    const wfaSection = document.getElementById('analytics-wfa-section');
    const optunaContainer = document.getElementById('analyticsOptunaSettings');
    const postProcessContainer = document.getElementById('analyticsPostProcessSettings');
    const wfaContainer = document.getElementById('analyticsWfaSettings');
    if (!optunaSection || !postProcessSection || !wfaSection || !optunaContainer || !postProcessContainer || !wfaContainer) return;

    const optunaSettings = study?.optuna_settings || {};
    const postProcessSettings = study?.post_process_settings || {};
    const wfaSettings = study?.wfa_settings || {};
    const enablePruning = optunaSettings.enable_pruning === null || optunaSettings.enable_pruning === undefined
      ? null
      : Boolean(optunaSettings.enable_pruning);
    const prunerValue = enablePruning === false
      ? '-'
      : (String(optunaSettings.pruner || '').trim() || (enablePruning ? 'On' : MISSING_TEXT));

    const optunaRows = [
      { key: 'Objectives', val: formatObjectivesList(optunaSettings.objectives) },
      {
        key: 'Primary',
        val: optunaSettings.primary_objective ? formatObjectiveLabel(optunaSettings.primary_objective) : MISSING_TEXT,
      },
      { key: 'Constraints', val: formatConstraintsSummary(optunaSettings.constraints) },
      { key: 'Budget', val: formatBudgetLabel(optunaSettings) },
      {
        key: 'Sampler',
        val: String(optunaSettings.sampler_type || '').trim()
          ? String(optunaSettings.sampler_type).toUpperCase()
          : MISSING_TEXT,
      },
      { key: 'Pruner', val: prunerValue },
      { key: 'Initial', val: formatInitialLabel(optunaSettings) },
      { key: 'Sanitize Trades', val: formatSanitizeLabel(optunaSettings) },
      { key: 'Filter', val: formatFilterLabel(optunaSettings) },
      {
        key: 'Workers',
        val: toFiniteNumber(optunaSettings.workers) === null
          ? MISSING_TEXT
          : String(Math.max(0, Math.round(Number(optunaSettings.workers)))),
      },
    ];
    renderSettingsList(optunaContainer, optunaRows);

    const adaptiveModeRaw = wfaSettings.adaptive_mode;
    const adaptiveMode = adaptiveModeRaw === null || adaptiveModeRaw === undefined
      ? null
      : Boolean(adaptiveModeRaw);
    const cooldownEnabledRaw = wfaSettings.cooldown_enabled;
    const cooldownEnabled = cooldownEnabledRaw === null || cooldownEnabledRaw === undefined
      ? null
      : Boolean(cooldownEnabledRaw);
    const wfaRows = [
      {
        key: 'IS (days)',
        val: toFiniteNumber(wfaSettings.is_period_days) === null
          ? MISSING_TEXT
          : String(Math.max(0, Math.round(Number(wfaSettings.is_period_days)))),
      },
      {
        key: 'OOS (days)',
        val: toFiniteNumber(wfaSettings.oos_period_days) === null
          ? MISSING_TEXT
          : String(Math.max(0, Math.round(Number(wfaSettings.oos_period_days)))),
      },
      { key: 'Adaptive', val: adaptiveMode === null ? MISSING_TEXT : (adaptiveMode ? 'On' : 'Off') },
    ];
    if (adaptiveMode === true) {
      if (cooldownEnabled) {
        wfaRows.push({
          key: 'Cooldown (days)',
          val: toFiniteNumber(wfaSettings.cooldown_days) === null
            ? '15d'
            : `${Math.max(1, Math.round(Number(wfaSettings.cooldown_days)))}d`,
        });
      }
      wfaRows.push(
        {
          key: 'Max OOS (days)',
          val: toFiniteNumber(wfaSettings.max_oos_period_days) === null
            ? MISSING_TEXT
            : String(Math.max(0, Math.round(Number(wfaSettings.max_oos_period_days)))),
        },
        {
          key: 'Min OOS Trades',
          val: toFiniteNumber(wfaSettings.min_oos_trades) === null
            ? MISSING_TEXT
            : String(Math.max(0, Math.round(Number(wfaSettings.min_oos_trades)))),
        },
        {
          key: 'Check Interval',
          val: toFiniteNumber(wfaSettings.check_interval_trades) === null
            ? MISSING_TEXT
            : String(Math.max(0, Math.round(Number(wfaSettings.check_interval_trades)))),
        },
        {
          key: 'CUSUM Threshold',
          val: toFiniteNumber(wfaSettings.cusum_threshold) === null
            ? MISSING_TEXT
            : Number(wfaSettings.cusum_threshold).toFixed(2),
        },
        {
          key: 'DD Multiplier',
          val: toFiniteNumber(wfaSettings.dd_threshold_multiplier) === null
            ? MISSING_TEXT
            : Number(wfaSettings.dd_threshold_multiplier).toFixed(2),
        },
        {
          key: 'Inactivity Mult.',
          val: toFiniteNumber(wfaSettings.inactivity_multiplier) === null
            ? MISSING_TEXT
            : Number(wfaSettings.inactivity_multiplier).toFixed(2),
        }
      );
    }
    const runTimeSeconds = computeRunTimeSeconds(study, wfaSettings);
    wfaRows.push({
      key: 'WFA Run Time',
      val: formatDuration(runTimeSeconds) || MISSING_TEXT,
    });
    renderSettingsList(wfaContainer, wfaRows);

    const postProcessRows = buildPostProcessSettingsRows(
      postProcessSettings,
      String(study?.optimization_mode || '').trim().toLowerCase() === 'wfa'
    );
    renderSettingsList(postProcessContainer, postProcessRows);

    optunaSection.style.display = '';
    postProcessSection.style.display = postProcessRows.length ? '' : 'none';
    wfaSection.style.display = '';
  }

  function getFocusedStudy() {
    const focusedId = String(AnalyticsState.focusedStudyId || '');
    if (!focusedId) return null;
    return getStudyMap().get(focusedId) || null;
  }

  function getFocusedSet() {
    const focusedSetId = toNonNegativeInteger(AnalyticsState.focusedSetId);
    if (focusedSetId <= 0) return null;
    return AnalyticsState.sets.find((setItem) => toNonNegativeInteger(setItem?.id) === focusedSetId) || null;
  }

  function renderFocusedCards(study) {
    const container = document.getElementById('analyticsSummaryRow');
    if (!container || !study) return;

    const annDisplay = computeAnnualizedProfitDisplay(study);
    const netProfit = toFiniteNumber(study.profit_pct);
    const maxDrawdown = toFiniteNumber(study.max_dd_pct);
    const totalTradesRaw = toFiniteNumber(study.total_trades);
    const winningTradesRaw = toFiniteNumber(study.winning_trades);
    const totalTrades = totalTradesRaw === null ? null : Math.max(0, Math.round(totalTradesRaw));
    let winningTrades = winningTradesRaw === null ? null : Math.max(0, Math.round(winningTradesRaw));
    if (winningTrades !== null && totalTrades !== null) {
      winningTrades = Math.min(winningTrades, totalTrades);
    } else if (winningTrades === null && totalTrades === 0) {
      winningTrades = 0;
    }
    const totalTradesText = totalTrades !== null
      ? `${winningTrades !== null ? winningTrades : 'N/A'}/${totalTrades}`
      : (winningTrades !== null ? `${winningTrades}/N/A` : 'N/A');

    const profitableWindowsRaw = toFiniteNumber(study.profitable_windows);
    const totalWindowsRaw = toFiniteNumber(study.total_windows);
    const profitableWindows = profitableWindowsRaw === null ? null : Math.max(0, Math.round(profitableWindowsRaw));
    const totalWindows = totalWindowsRaw === null ? null : Math.max(0, Math.round(totalWindowsRaw));
    const oosWinsPctRaw = toFiniteNumber(study.profitable_windows_pct);
    const oosWinsPct = oosWinsPctRaw !== null
      ? oosWinsPctRaw
      : (profitableWindows !== null && totalWindows !== null && totalWindows > 0
        ? (profitableWindows / totalWindows) * 100
        : 0);
    const oosWinsText = (profitableWindows !== null && totalWindows !== null)
      ? `${Math.min(profitableWindows, totalWindows)}/${totalWindows} (${Math.round(totalWindows > 0 ? oosWinsPct : 0)}%)`
      : (oosWinsPctRaw !== null ? `${Math.round(oosWinsPctRaw)}%` : 'N/A');

    const wfe = toFiniteNumber(study.wfe_pct);
    const medianProfit = toFiniteNumber(study.median_window_profit);
    const medianWr = toFiniteNumber(study.median_window_wr);

    const netClass = netProfit === null ? '' : (netProfit >= 0 ? 'positive' : 'negative');
    const medianProfitClass = medianProfit === null ? '' : (medianProfit >= 0 ? 'positive' : 'negative');
    const annTitle = annDisplay.tooltip ? ` title="${annDisplay.tooltip}"` : '';

    container.innerHTML = `
      <div class="summary-card"${annTitle}>
        <div class="value ${annDisplay.className}">${annDisplay.text}</div>
        <div class="label">ANN.P%</div>
      </div>
      <div class="summary-card">
        <div class="value ${netClass}">${formatSignedPercent(netProfit, 2)}</div>
        <div class="label">NET PROFIT</div>
      </div>
      <div class="summary-card">
        <div class="value negative">${formatNegativePercent(maxDrawdown, 2)}</div>
        <div class="label">MAX DRAWDOWN</div>
      </div>
      <div class="summary-card">
        <div class="value">${totalTradesText}</div>
        <div class="label">TOTAL TRADES</div>
      </div>
      <div class="summary-card">
        <div class="value">${wfe === null ? 'N/A' : `${wfe.toFixed(1)}%`}</div>
        <div class="label">WFE</div>
      </div>
      <div class="summary-card">
        <div class="value">${oosWinsText}</div>
        <div class="label">OOS WINS</div>
      </div>
      <div class="summary-card">
        <div class="value ${medianProfitClass}">${formatSignedPercent(medianProfit, 1)}</div>
        <div class="label">OOS PROFIT (MED)</div>
      </div>
      <div class="summary-card">
        <div class="value">${formatUnsignedPercent(medianWr, 1)}</div>
        <div class="label">OOS WIN RATE (MED)</div>
      </div>
    `;
  }

  function computePortfolioTailMetrics(selected) {
    const totalTrades = selected.reduce(
      (acc, study) => acc + Math.max(0, Math.round(toFiniteNumber(study?.total_trades) || 0)),
      0
    );
    const profitableCount = selected.reduce((acc, study) => {
      const profit = toFiniteNumber(study?.profit_pct);
      return acc + (profit !== null && profit > 0 ? 1 : 0);
    }, 0);
    const profitablePct = selected.length > 0 ? Math.round((profitableCount / selected.length) * 100) : 0;

    return {
      totalTrades,
      profitableText: `${profitableCount}/${selected.length} (${profitablePct}%)`,
      avgOosWins: average(selected.map((study) => study?.profitable_windows_pct)),
      avgWfe: average(selected.map((study) => study?.wfe_pct)),
      avgOosProfitMed: average(selected.map((study) => study?.median_window_profit)),
    };
  }

  function renderEmptySummaryCards(container) {
    container.innerHTML = `
      <div class="summary-card"><div class="value">-</div><div class="label">Portfolio Ann.P%</div></div>
      <div class="summary-card"><div class="value">-</div><div class="label">Portfolio Profit</div></div>
      <div class="summary-card"><div class="value">-</div><div class="label">Portfolio MaxDD</div></div>
      <div class="summary-card"><div class="value">-</div><div class="label">Total Trades</div></div>
      <div class="summary-card"><div class="value">-</div><div class="label">Profitable</div></div>
      <div class="summary-card"><div class="value">-</div><div class="label">Avg OOS Wins</div></div>
      <div class="summary-card"><div class="value">-</div><div class="label">Avg WFE</div></div>
      <div class="summary-card"><div class="value">-</div><div class="label">Avg OOS P(med)</div></div>
    `;
  }

  function renderSummaryCards() {
    const container = document.getElementById('analyticsSummaryRow');
    if (!container) return;

    const focusedStudy = getFocusedStudy();
    if (focusedStudy) {
      renderFocusedCards(focusedStudy);
      return;
    }

    const selected = getSelectedStudies();
    if (!selected.length) {
      renderEmptySummaryCards(container);
      return;
    }

    const tail = computePortfolioTailMetrics(selected);
    const selectedStudyIds = getSelectedStudyIds();
    const focusedSet = getFocusedSet();
    const avgOosProfitClass = tail.avgOosProfitMed === null
      ? ''
      : (tail.avgOosProfitMed >= 0 ? 'positive' : 'negative');

    let portfolioProfit = null;
    let portfolioAnn = null;
    let portfolioMaxDd = null;
    let portfolioAnnClass = '';
    let annTooltip = '';
    let loadingPrimaryMetrics = false;

    if (focusedSet) {
      const focusedSetData = getCachedSetEquity(focusedSet.id) || focusedSet.metrics || null;
      loadingPrimaryMetrics = !focusedSetData && AnalyticsState.setEquityPendingSetId === focusedSet.id;
      portfolioProfit = toFiniteNumber(focusedSetData?.profit_pct);
      portfolioAnn = toFiniteNumber(focusedSetData?.ann_profit_pct);
      portfolioMaxDd = toFiniteNumber(focusedSetData?.max_drawdown_pct);

      const overlapDaysExact = toFiniteNumber(focusedSetData?.overlap_days_exact);
      if (portfolioAnn !== null && overlapDaysExact !== null && overlapDaysExact >= 31 && overlapDaysExact < 90) {
        annTooltip = `Short overlap period (${Math.round(overlapDaysExact)} days) - annualized value may be misleading`;
      }
    } else if (selected.length === 1) {
      const study = selected[0];
      const annDisplay = computeAnnualizedProfitDisplay(study);
      portfolioProfit = toFiniteNumber(study?.profit_pct);
      portfolioAnn = annDisplay.text;
      portfolioAnnClass = annDisplay.className || '';
      portfolioMaxDd = toFiniteNumber(study?.max_dd_pct);
      annTooltip = annDisplay.tooltip || '';
    } else {
      const usingAllStudiesCache = isAllStudiesSelection(selectedStudyIds);
      const key = buildSelectionKey(selectedStudyIds);
      const portfolio = usingAllStudiesCache
        ? getCurrentAllStudiesData()
        : getCurrentPortfolioData();
      loadingPrimaryMetrics = usingAllStudiesCache
        ? (!portfolio && AnalyticsState.allStudiesEquityPending)
        : (!portfolio && AnalyticsState.portfolioPendingKey === key);
      portfolioProfit = toFiniteNumber(portfolio?.profit_pct);
      portfolioAnn = toFiniteNumber(portfolio?.ann_profit_pct);
      portfolioMaxDd = toFiniteNumber(portfolio?.max_drawdown_pct);

      const overlapDaysExact = toFiniteNumber(portfolio?.overlap_days_exact);
      if (portfolioAnn !== null && overlapDaysExact !== null && overlapDaysExact >= 31 && overlapDaysExact < 90) {
        annTooltip = `Short overlap period (${Math.round(overlapDaysExact)} days) - annualized value may be misleading`;
      }
    }

    const profitClass = portfolioProfit === null ? '' : (portfolioProfit >= 0 ? 'positive' : 'negative');
    const annClass = typeof portfolioAnn === 'number'
      ? (portfolioAnn >= 0 ? 'positive' : 'negative')
      : portfolioAnnClass;
    const annTitleAttr = annTooltip ? ` title="${annTooltip}"` : '';
    const annText = typeof portfolioAnn === 'number'
      ? formatSignedPercent(portfolioAnn, 1)
      : (typeof portfolioAnn === 'string' ? portfolioAnn : (loadingPrimaryMetrics ? '...' : 'N/A'));
    const profitText = loadingPrimaryMetrics && portfolioProfit === null ? '...' : formatSignedPercent(portfolioProfit, 1);
    const maxDdText = loadingPrimaryMetrics && portfolioMaxDd === null ? '...' : formatNegativePercent(portfolioMaxDd, 1);

    container.innerHTML = `
      <div class="summary-card"${annTitleAttr}>
        <div class="value ${annClass}">${annText}</div>
        <div class="label">Portfolio Ann.P%</div>
      </div>
      <div class="summary-card">
        <div class="value ${profitClass}">${profitText}</div>
        <div class="label">Portfolio Profit</div>
      </div>
      <div class="summary-card">
        <div class="value negative">${maxDdText}</div>
        <div class="label">Portfolio MaxDD</div>
      </div>
      <div class="summary-card">
        <div class="value">${formatInteger(tail.totalTrades)}</div>
        <div class="label">Total Trades</div>
      </div>
      <div class="summary-card">
        <div class="value">${tail.profitableText}</div>
        <div class="label">Profitable</div>
      </div>
      <div class="summary-card">
        <div class="value">${formatUnsignedPercent(tail.avgOosWins, 1)}</div>
        <div class="label">Avg OOS Wins</div>
      </div>
      <div class="summary-card">
        <div class="value">${formatUnsignedPercent(tail.avgWfe, 1)}</div>
        <div class="label">Avg WFE</div>
      </div>
      <div class="summary-card">
        <div class="value ${avgOosProfitClass}">${formatSignedPercent(tail.avgOosProfitMed, 1)}</div>
        <div class="label">Avg OOS P(med)</div>
      </div>
    `;
  }

  function renderChartTitle(study) {
    const titleEl = document.getElementById('analyticsChartTitle');
    if (!titleEl) return;
    titleEl.textContent = '';

    if (!study) {
      titleEl.textContent = 'Stitched OOS Equity';
      return;
    }

    if (String(AnalyticsState.focusedStudyId || '') === String(study.study_id || '')) {
      const fullStudyName = String(study.study_name || '').trim();
      const fallbackTitle = [study.symbol || '-', study.tf || '-'].join(' ').trim();
      titleEl.textContent = `Stitched OOS Equity - ${fullStudyName || fallbackTitle}`;
      return;
    }

    const visibleStudyIds = getVisibleOrderedStudyIds();
    const rowNumber = Math.max(1, visibleStudyIds.indexOf(String(study.study_id || '')) + 1);
    const symbol = study.symbol || '-';
    const tf = study.tf || '-';
    titleEl.appendChild(document.createTextNode(`Stitched OOS Equity - #${rowNumber} ${symbol} ${tf}`));
  }

  function renderPortfolioChartTitle(selectedCount, portfolioData) {
    const titleEl = document.getElementById('analyticsChartTitle');
    if (!titleEl) return;
    titleEl.textContent = '';

    const overlapDays = Math.max(0, Math.round(toFiniteNumber(portfolioData?.overlap_days) || 0));
    const overlapText = overlapDays > 0 ? `, ${overlapDays} days` : '';
    titleEl.appendChild(
      document.createTextNode(`Portfolio Equity (${selectedCount} studies${overlapText})`)
    );

    const warning = String(portfolioData?.warning || '').trim();
    if (warning) {
      const indicator = document.createElement('span');
      indicator.className = 'chart-title-indicator';
      indicator.textContent = ' [!]';
      indicator.title = warning;
      indicator.setAttribute('aria-label', warning);
      titleEl.appendChild(indicator);
    }
  }

  function renderSetChartTitle(setItem, portfolioData) {
    const titleEl = document.getElementById('analyticsChartTitle');
    if (!titleEl) return;
    titleEl.textContent = '';

    const overlapDays = Math.max(0, Math.round(toFiniteNumber(portfolioData?.overlap_days) || 0));
    const overlapText = overlapDays > 0 ? `, ${overlapDays} days` : '';
    const studyCount = Array.isArray(setItem?.study_ids) ? setItem.study_ids.length : 0;
    const setName = String(setItem?.name || 'Unnamed Set').trim() || 'Unnamed Set';
    titleEl.appendChild(
      document.createTextNode(`Set Equity - ${setName} (${studyCount} studies${overlapText})`)
    );

    const warning = String(portfolioData?.warning || '').trim();
    if (warning) {
      const indicator = document.createElement('span');
      indicator.className = 'chart-title-indicator';
      indicator.textContent = ' [!]';
      indicator.title = warning;
      indicator.setAttribute('aria-label', warning);
      titleEl.appendChild(indicator);
    }
  }

  function renderPortfolioChartMeta(totalSelected, portfolioData) {
    const warning = String(portfolioData?.warning || '').trim();
    setChartWarning(warning);

    if (!portfolioData) {
      setChartSubtitle('');
      return;
    }

    const used = Math.max(0, Math.round(toFiniteNumber(portfolioData?.studies_used) || 0));
    if (used >= 0 && used < totalSelected) {
      setChartSubtitle(`${used} of ${totalSelected} studies used`);
      return;
    }
    setChartSubtitle('');
  }

  function getPrimaryCheckedStudy() {
    const map = getStudyMap();
    let selectedId = null;
    for (const studyId of getVisibleOrderedStudyIds()) {
      if (AnalyticsState.checkedStudyIds.has(studyId)) {
        selectedId = studyId;
        break;
      }
    }
    if (!selectedId) {
      selectedId = Array.from(AnalyticsState.checkedStudyIds)[0] || null;
    }
    return selectedId ? map.get(selectedId) || null : null;
  }

  function renderSelectedStudyChart() {
    const focusedStudy = getFocusedStudy();
    if (focusedStudy) {
      clearChartMeta();
      renderChartTitle(focusedStudy);
      if (!focusedStudy.has_equity_curve) {
        window.AnalyticsEquity.renderEmpty('No stitched OOS equity data for selected study');
        return;
      }
      const studyEquity = getCachedStudyEquity(focusedStudy.study_id);
      const focusedBoundaries = getFocusedWindowBoundaries(focusedStudy.study_id) || [];
      ensureFocusedWindowBoundaries(focusedStudy);
      if (!studyEquity) {
        ensureStudyEquity(focusedStudy);
        window.AnalyticsEquity.renderEmpty('Loading stitched OOS equity...');
        return;
      }
      if (!studyEquity.has_equity_curve) {
        window.AnalyticsEquity.renderEmpty(
          studyEquity.warning || 'No stitched OOS equity data for selected study'
        );
        return;
      }
      window.AnalyticsEquity.renderChart(
        studyEquity.curve || [],
        studyEquity.timestamps || [],
        { windowBoundaries: focusedBoundaries }
      );
      return;
    }
    if (AnalyticsState.focusedWindowBoundariesPendingStudyId) {
      cancelFocusedWindowBoundariesFetch();
    }

    const focusedSet = getFocusedSet();
    if (focusedSet) {
      const setData = getCachedSetEquity(focusedSet.id);
      clearChartMeta();
      renderSetChartTitle(focusedSet, setData || focusedSet.metrics);
      renderPortfolioChartMeta(focusedSet.study_ids.length, setData || focusedSet.metrics);

      if (!setData) {
        ensureFocusedSetEquity(focusedSet.id);
        window.AnalyticsEquity.renderEmpty('Loading set equity...');
        return;
      }

      const curve = Array.isArray(setData.curve) ? setData.curve : [];
      const timestamps = Array.isArray(setData.timestamps) ? setData.timestamps : [];
      if (!curve.length || curve.length !== timestamps.length) {
        const emptyMessage = focusedSet.study_ids.length
          ? (setData.warning || 'No overlapping equity data to display')
          : 'No studies in selected set';
        window.AnalyticsEquity.renderEmpty(emptyMessage);
        return;
      }

      window.AnalyticsEquity.renderChart(curve, timestamps, {
        returnProfile: setData?.return_profile || null,
      });
      return;
    }

    const selected = getSelectedStudies();
    if (!selected.length) {
      clearChartMeta();
      renderChartTitle(null);
      window.AnalyticsEquity.renderEmpty('No data to display');
      return;
    }

    if (selected.length === 1) {
      clearChartMeta();
      const singleStudy = getPrimaryCheckedStudy() || selected[0];
      renderChartTitle(singleStudy);
      if (!singleStudy?.has_equity_curve) {
        window.AnalyticsEquity.renderEmpty('No stitched OOS equity data for selected study');
        return;
      }
      const singleStudyEquity = getCachedStudyEquity(singleStudy.study_id);
      if (!singleStudyEquity) {
        ensureStudyEquity(singleStudy);
        window.AnalyticsEquity.renderEmpty('Loading stitched OOS equity...');
        return;
      }
      if (!singleStudyEquity.has_equity_curve) {
        window.AnalyticsEquity.renderEmpty(
          singleStudyEquity.warning || 'No stitched OOS equity data for selected study'
        );
        return;
      }
      window.AnalyticsEquity.renderChart(singleStudyEquity.curve || [], singleStudyEquity.timestamps || []);
      return;
    }

    const selectedStudyIds = getSelectedStudyIds();
    if (isAllStudiesSelection(selectedStudyIds)) {
      const allStudiesData = getCurrentAllStudiesData();
      clearChartMeta();
      renderPortfolioChartTitle(selected.length, allStudiesData);
      renderPortfolioChartMeta(selected.length, allStudiesData);

      if (!AnalyticsState.allStudiesEquity) {
        ensureAllStudiesEquity();
        window.AnalyticsEquity.renderEmpty('Loading portfolio equity...');
        return;
      }

      const curve = Array.isArray(AnalyticsState.allStudiesEquity.curve)
        ? AnalyticsState.allStudiesEquity.curve
        : [];
      const timestamps = Array.isArray(AnalyticsState.allStudiesEquity.timestamps)
        ? AnalyticsState.allStudiesEquity.timestamps
        : [];
      if (!curve.length || curve.length !== timestamps.length) {
        window.AnalyticsEquity.renderEmpty(
          String(allStudiesData?.warning || '').trim() || 'No overlapping equity data to display'
        );
        return;
      }

      window.AnalyticsEquity.renderChart(curve, timestamps, {
        returnProfile: AnalyticsState.allStudiesEquity?.return_profile || null,
      });
      return;
    }

    const selectionKey = buildSelectionKey(selectedStudyIds);
    const isLoading = AnalyticsState.portfolioPendingKey === selectionKey && !getCurrentPortfolioData();
    const portfolio = getCurrentPortfolioData();
    clearChartMeta();
    renderPortfolioChartTitle(selected.length, portfolio);
    renderPortfolioChartMeta(selected.length, portfolio);

    if (isLoading) {
      window.AnalyticsEquity.renderEmpty('Loading portfolio equity...');
      return;
    }

    if (!portfolio) {
      window.AnalyticsEquity.renderEmpty('No data to display');
      return;
    }

    const curve = Array.isArray(portfolio.curve) ? portfolio.curve : [];
    const timestamps = Array.isArray(portfolio.timestamps) ? portfolio.timestamps : [];
    if (!curve.length || curve.length !== timestamps.length) {
      window.AnalyticsEquity.renderEmpty('No overlapping equity data to display');
      return;
    }

    window.AnalyticsEquity.renderChart(curve, timestamps, {
      returnProfile: portfolio?.return_profile || null,
    });
  }

  function updateVisualsForSelection() {
    renderTableHeaderMeta();
    if (window.AnalyticsSets && typeof window.AnalyticsSets.setFocusedStudyId === 'function') {
      window.AnalyticsSets.setFocusedStudyId(AnalyticsState.focusedStudyId);
    }
    const focusedStudy = getFocusedStudy();
    if (focusedStudy) {
      renderFocusedSidebar(focusedStudy);
    } else {
      hideFocusSidebar();
    }
    ensurePortfolioDataForSelection();
    renderSummaryCards();
    renderSelectedStudyChart();
  }

  function buildResearchInfoRows(info) {
    const symbols = Array.isArray(info.symbols) ? info.symbols : [];
    const strategies = Array.isArray(info.strategies) ? info.strategies : [];
    const timeframes = Array.isArray(info.timeframes) ? info.timeframes : [];
    const wfaModes = Array.isArray(info.wfa_modes) ? info.wfa_modes : [];
    const isOosPeriods = Array.isArray(info.is_oos_periods) ? info.is_oos_periods : [];
    const dataPeriods = Array.isArray(info.data_periods) ? info.data_periods : [];

    return [
      { key: 'Studies', val: `${info.total_studies || 0} total (${info.wfa_studies || 0} WFA)` },
      { key: 'Strategies', val: strategies.length ? strategies.join(', ') : MISSING_TEXT },
      { key: 'Symbols', val: symbols.length ? `${symbols.length} tickers` : MISSING_TEXT },
      { key: 'Timeframes', val: timeframes.length ? timeframes.join(', ') : MISSING_TEXT },
      { key: 'WFA Mode', val: wfaModes.length ? wfaModes.join(', ') : MISSING_TEXT },
      { key: 'IS / OOS', val: isOosPeriods.length ? isOosPeriods.join(', ') : MISSING_TEXT },
      { key: 'Data Periods', val: `${dataPeriods.length} periods` },
    ];
  }

  function renderResearchInfo() {
    const container = document.getElementById('analyticsResearchInfo');
    if (!container) return;
    const rows = buildResearchInfoRows(AnalyticsState.researchInfo || {});
    container.innerHTML = '';

    rows.forEach((row) => {
      const item = document.createElement('div');
      item.className = 'setting-item';

      const key = document.createElement('span');
      key.className = 'key';
      key.textContent = String(row.key || '');

      const val = document.createElement('span');
      val.className = 'val';
      val.textContent = displayValue(row.val);

      item.appendChild(key);
      item.appendChild(val);
      container.appendChild(item);
    });
  }

  function renderTableSubtitle() {
    const subtitle = document.getElementById('analyticsTableSubtitle');
    if (!subtitle) return;

    const sortColumn = AnalyticsState.sortState.sortColumn;
    const sortDirection = AnalyticsState.sortState.sortDirection;
    if (!sortColumn || !sortDirection) {
      subtitle.textContent = 'Sorted by date added (newest first)';
      return;
    }

    const label = SORT_LABELS[sortColumn] || sortColumn;
    const arrow = sortDirection === 'asc' ? '▲' : '▼';
    subtitle.textContent = `Sorted by ${label} ${arrow}`;
  }

  function getFocusedStudySetMembershipMeta() {
    const focusedStudyId = String(AnalyticsState.focusedStudyId || '').trim();
    if (!focusedStudyId) return null;

    const visibleSetIds = window.AnalyticsSets && typeof window.AnalyticsSets.getVisibleSetIds === 'function'
      ? new Set(
        window.AnalyticsSets.getVisibleSetIds()
          .map((setId) => toNonNegativeInteger(setId))
          .filter((setId) => setId > 0)
      )
      : null;

    let total = 0;
    let visible = 0;
    (AnalyticsState.sets || []).forEach((setItem) => {
      if (!Array.isArray(setItem?.study_ids) || !setItem.study_ids.includes(focusedStudyId)) {
        return;
      }
      total += 1;
      const setId = toNonNegativeInteger(setItem?.id);
      if (visibleSetIds === null || (setId > 0 && visibleSetIds.has(setId))) {
        visible += 1;
      }
    });

    return { visible, total };
  }

  function renderTableHeaderMeta() {
    const meta = document.getElementById('analyticsTableHeaderMeta');
    if (!meta) return;

    const checkedCount = AnalyticsState.checkedStudyIds.size;
    const shownCount = getVisibleStudyCount();
    const totalCount = getTotalWfaStudyCount();
    const focusedStudySetMembership = getFocusedStudySetMembershipMeta();
    const inSetsMetric = focusedStudySetMembership
      ? `
      <span class="analytics-table-header-metric">
        <span class="analytics-table-header-value">${
          focusedStudySetMembership.visible < focusedStudySetMembership.total
            ? `${formatHeaderCount(focusedStudySetMembership.visible)}/${formatHeaderCount(focusedStudySetMembership.total)}`
            : formatHeaderCount(focusedStudySetMembership.total)
        }</span>
        <span class="analytics-table-header-label">${
          focusedStudySetMembership.total === 1 ? 'set' : 'sets'
        }</span>
      </span>
      <span class="analytics-table-header-separator">|</span>
    `
      : '';

    meta.innerHTML = `
      ${inSetsMetric}
      <span class="analytics-table-header-metric">
        <span class="analytics-table-header-value">${formatHeaderCount(checkedCount)}</span>
        <span class="analytics-table-header-label">checked</span>
      </span>
      <span class="analytics-table-header-separator">|</span>
      <span class="analytics-table-header-metric">
        <span class="analytics-table-header-value">${formatHeaderCount(shownCount)}</span>
        <span class="analytics-table-header-label">shown</span>
      </span>
      <span class="analytics-table-header-separator">/</span>
      <span class="analytics-table-header-metric">
        <span class="analytics-table-header-value">${formatHeaderCount(totalCount)}</span>
        <span class="analytics-table-header-label">total</span>
      </span>
    `;
  }

  function renderDatabasesList(databases) {
    const container = document.getElementById('analyticsDbList');
    if (!container) return;

    container.innerHTML = '';
    if (!Array.isArray(databases) || !databases.length) {
      container.innerHTML = '<div class="study-item">No database files found.</div>';
      return;
    }

    databases.forEach((db) => {
      const item = document.createElement('div');
      item.className = db.active ? 'study-item selected' : 'study-item';
      item.textContent = db.name;
      item.dataset.dbName = db.name;
      item.addEventListener('click', async () => {
        if (AnalyticsState.dbSwitchInProgress || db.active) return;
        AnalyticsState.dbSwitchInProgress = true;
        try {
          await switchDatabaseRequest(db.name);
          AnalyticsState.checkedStudyIds = new Set();
          AnalyticsState.sortState = { ...DEFAULT_SORT_STATE };
          AnalyticsState.focusedStudyId = null;
          AnalyticsState.focusedSetId = null;
          AnalyticsState.checkedSetIds = new Set();
          AnalyticsState.setViewMode = 'allStudies';
          clearPortfolioState();
          await Promise.all([loadDatabases(), loadSummary()]);
        } catch (error) {
          alert(error.message || 'Failed to switch database.');
        } finally {
          AnalyticsState.dbSwitchInProgress = false;
        }
      });
      container.appendChild(item);
    });
  }

  function onTableSelectionChange(checkedSet) {
    AnalyticsState.checkedStudyIds = new Set(checkedSet || []);
    if (window.AnalyticsSets && typeof window.AnalyticsSets.updateCheckedStudyIds === 'function') {
      window.AnalyticsSets.updateCheckedStudyIds(AnalyticsState.checkedStudyIds);
    }
    updateVisualsForSelection();
  }

  function clearFocus() {
    if (!AnalyticsState.focusedStudyId) return;
    AnalyticsState.focusedStudyId = null;
    cancelFocusedWindowBoundariesFetch();
    if (window.AnalyticsTable && typeof window.AnalyticsTable.setFocusedStudyId === 'function') {
      window.AnalyticsTable.setFocusedStudyId(null);
    }
    updateVisualsForSelection();
  }

  function setFocus(studyId) {
    const normalized = String(studyId || '').trim();
    if (!normalized) {
      clearFocus();
      return;
    }
    if (AnalyticsState.focusedStudyId === normalized) {
      clearFocus();
      return;
    }
    AnalyticsState.focusedStudyId = normalized;
    if (window.AnalyticsTable && typeof window.AnalyticsTable.setFocusedStudyId === 'function') {
      window.AnalyticsTable.setFocusedStudyId(normalized);
    }
    updateVisualsForSelection();
  }

  function moveFocusedStudy(direction) {
    const step = Number(direction);
    if (!Number.isInteger(step) || step === 0 || !AnalyticsState.focusedStudyId) return false;
    const visibleStudyIds = getVisibleOrderedStudyIds();
    if (!visibleStudyIds.length) return false;
    const currentIndex = visibleStudyIds.indexOf(String(AnalyticsState.focusedStudyId || ''));
    if (currentIndex < 0) return false;
    const targetIndex = currentIndex + step;
    if (targetIndex < 0 || targetIndex >= visibleStudyIds.length) return false;
    const targetStudyId = visibleStudyIds[targetIndex];
    if (!targetStudyId || targetStudyId === AnalyticsState.focusedStudyId) return false;
    setFocus(targetStudyId);
    scrollStudyIntoView(targetStudyId);
    return true;
  }

  function moveFocusedSet(direction) {
    const step = Number(direction);
    if (!Number.isInteger(step) || step === 0 || AnalyticsState.focusedSetId === null) return false;
    const orderedSetIds = (window.AnalyticsSets && typeof window.AnalyticsSets.getVisibleSetIds === 'function'
      ? window.AnalyticsSets.getVisibleSetIds()
      : AnalyticsState.sets.map((setItem) => Number(setItem?.id)))
      .map((setId) => Number(setId))
      .filter((setId) => Number.isInteger(setId) && setId > 0);
    if (!orderedSetIds.length) return false;
    const currentIndex = orderedSetIds.indexOf(Number(AnalyticsState.focusedSetId));
    if (currentIndex < 0) return false;
    const targetIndex = currentIndex + step;
    if (targetIndex < 0 || targetIndex >= orderedSetIds.length) return false;
    const targetSetId = orderedSetIds[targetIndex];
    if (!Number.isInteger(targetSetId) || targetSetId === Number(AnalyticsState.focusedSetId)) return false;
    if (!window.AnalyticsSets || typeof window.AnalyticsSets.setFocusedSetId !== 'function') return false;
    window.AnalyticsSets.setFocusedSetId(targetSetId);
    syncSetStateFromModule();
    scrollSetIntoView(targetSetId);
    return true;
  }

  function onTableFocusToggle(studyId) {
    setFocus(studyId);
  }

  function onTableSortChange(sortState) {
    AnalyticsState.sortState = {
      sortColumn: sortState?.sortColumn || null,
      sortDirection: sortState?.sortDirection || null,
      sortClickCount: Number(sortState?.sortClickCount || 0),
    };
    renderTableSubtitle();
    AnalyticsState.orderedStudyIds = window.AnalyticsTable.getOrderedStudyIds();
    updateVisualsForSelection();
  }

  function syncTableStateFromModule(options) {
    if (!window.AnalyticsTable) return;

    AnalyticsState.orderedStudyIds = window.AnalyticsTable.getOrderedStudyIds();
    const visibleStudyIds = typeof window.AnalyticsTable.getVisibleStudyIds === 'function'
      ? new Set(window.AnalyticsTable.getVisibleStudyIds())
      : new Set();

    const nextFocusedStudyId = String(options?.focusedStudyId || '').trim();
    if (nextFocusedStudyId) {
      AnalyticsState.focusedStudyId = nextFocusedStudyId;
    } else if (AnalyticsState.focusedStudyId && !visibleStudyIds.has(AnalyticsState.focusedStudyId)) {
      AnalyticsState.focusedStudyId = null;
    }

    if (typeof window.AnalyticsTable.setFocusedStudyId === 'function') {
      window.AnalyticsTable.setFocusedStudyId(AnalyticsState.focusedStudyId);
    }
  }

  function onTableViewChange(focusedStudyId) {
    syncTableStateFromModule({ focusedStudyId });
    updateVisualsForSelection();
  }

  function renderTableWithCurrentState() {
    const setVisibleStudyIds = getSetVisibleStudyIds();
    refreshFiltersForCurrentContext(setVisibleStudyIds);

    window.AnalyticsTable.renderTable(
      AnalyticsState.studies,
      AnalyticsState.checkedStudyIds,
      onTableSelectionChange,
      {
        filters: AnalyticsState.filters,
        visibleStudyIds: setVisibleStudyIds,
        autoSelect: AnalyticsState.autoSelect,
        groupDatesEnabled: AnalyticsState.groupDatesEnabled,
        sortState: AnalyticsState.sortState,
        onSortChange: onTableSortChange,
        onFocusToggle: onTableFocusToggle,
        onViewChange: onTableViewChange,
        focusedStudyId: AnalyticsState.focusedStudyId,
      }
    );

    AnalyticsState.checkedStudyIds = new Set(window.AnalyticsTable.getCheckedStudyIds());
    syncTableStateFromModule();
    if (window.AnalyticsSets && typeof window.AnalyticsSets.updateCheckedStudyIds === 'function') {
      window.AnalyticsSets.updateCheckedStudyIds(AnalyticsState.checkedStudyIds);
    }
    updateVisualsForSelection();
  }

  function handleFiltersChanged(nextFilters) {
    AnalyticsState.filters = cloneFilters(nextFilters || EMPTY_FILTERS);
    renderTableWithCurrentState();
  }

  function bindTableToggles() {
    const autoSelectInput = document.getElementById('analyticsAutoSelect');
    if (autoSelectInput) {
      autoSelectInput.checked = AnalyticsState.autoSelect;
      autoSelectInput.addEventListener('change', () => {
        AnalyticsState.autoSelect = Boolean(autoSelectInput.checked);
        renderTableWithCurrentState();
      });
    }

    const groupDatesInput = document.getElementById('analyticsGroupDates');
    if (groupDatesInput) {
      groupDatesInput.checked = AnalyticsState.groupDatesEnabled;
      groupDatesInput.addEventListener('change', () => {
        AnalyticsState.groupDatesEnabled = Boolean(groupDatesInput.checked);
        renderTableWithCurrentState();
      });
    }
  }

  function initSetsModule() {
    if (!window.AnalyticsSets) return;
    window.AnalyticsSets.init({
      studies: AnalyticsState.studies,
      checkedStudyIds: AnalyticsState.checkedStudyIds,
      onStateChange: handleSetsStateChange,
    });
    if (typeof window.AnalyticsSets.setFocusedStudyId === 'function') {
      window.AnalyticsSets.setFocusedStudyId(AnalyticsState.focusedStudyId);
    }
    syncSetStateFromModule();
  }

  async function loadDatabases() {
    const payload = await fetchDatabasesList();
    renderDatabasesList(payload.databases || []);
  }

  function ensureFiltersInitialized() {
    if (!window.AnalyticsFilters) return;
    if (!AnalyticsState.filtersInitialized) {
      window.AnalyticsFilters.init({
        studies: AnalyticsState.studies,
        onChange: handleFiltersChanged,
      });
      AnalyticsState.filtersInitialized = true;
      return;
    }
    window.AnalyticsFilters.updateStudies(AnalyticsState.studies, { emitChange: false });
  }

  async function loadSummary() {
    const response = await fetch('/api/analytics/summary');
    if (!response.ok) {
      const payload = await response.json().catch(() => ({}));
      throw new Error(payload.error || 'Failed to load analytics summary.');
    }
    const data = await response.json();

    AnalyticsState.dbName = String(data.db_name || '');
    AnalyticsState.studies = Array.isArray(data.studies) ? data.studies : [];
    AnalyticsState.researchInfo = data.research_info || {};
    AnalyticsState.checkedStudyIds = new Set();
    AnalyticsState.focusedStudyId = null;
    AnalyticsState.focusedSetId = null;
    AnalyticsState.checkedSetIds = new Set();
    AnalyticsState.setViewMode = 'allStudies';
    AnalyticsState.setMoveMode = false;
    AnalyticsState.allStudiesMetrics = null;
    AnalyticsState.filterContextEpoch += 1;
    AnalyticsState.filterContextSignature = null;
    clearPortfolioState();
    clearFocusedWindowBoundariesState();
    clearFocusedStudyEquityState();
    clearFocusedSetEquityState();
    clearAllStudiesEquityState();

    renderResearchInfo();
    showMessage(AnalyticsState.researchInfo.message || '');

    ensureFiltersInitialized();
    AnalyticsState.filters = window.AnalyticsFilters
      ? cloneFilters(window.AnalyticsFilters.getFilters())
      : cloneFilters(EMPTY_FILTERS);

    if (window.AnalyticsSets) {
      if (typeof window.AnalyticsSets.updateStudies === 'function') {
        window.AnalyticsSets.updateStudies(AnalyticsState.studies);
      }
      if (typeof window.AnalyticsSets.loadSets === 'function') {
        await window.AnalyticsSets.loadSets({ preserveSelection: false, emitState: false });
      }
      if (typeof window.AnalyticsSets.updateCheckedStudyIds === 'function') {
        window.AnalyticsSets.updateCheckedStudyIds(AnalyticsState.checkedStudyIds);
      }
      syncSetStateFromModule();
    }

    renderTableSubtitle();
    renderTableWithCurrentState();
  }

  function bindCollapsibleHeaders() {
    const headers = document.querySelectorAll('.sidebar .collapsible-header');
    headers.forEach((header) => {
      header.addEventListener('click', () => {
        const root = header.closest('.collapsible');
        if (!root) return;
        root.classList.toggle('open');
      });
    });
  }

  function bindSelectionButtons() {
    const selectAllBtn = document.getElementById('analyticsSelectAllBtn');
    const deselectAllBtn = document.getElementById('analyticsDeselectAllBtn');

    if (selectAllBtn) {
      selectAllBtn.addEventListener('click', () => {
        setAllStudiesChecked(true);
      });
    }
    if (deselectAllBtn) {
      deselectAllBtn.addEventListener('click', () => {
        deselectAllStudies();
      });
    }
  }

  function bindFocusHotkeys() {
    document.addEventListener('keydown', (event) => {
      if (event.defaultPrevented) return;

      if (event.key === 'Escape') {
        if (AnalyticsState.focusedStudyId) {
          event.preventDefault();
          clearFocus();
          return;
        }

        if (window.AnalyticsSets && typeof window.AnalyticsSets.isMoveMode === 'function'
            && window.AnalyticsSets.isMoveMode()) {
          event.preventDefault();
          if (typeof window.AnalyticsSets.cancelMoveMode === 'function') {
            window.AnalyticsSets.cancelMoveMode();
            syncSetStateFromModule();
            renderTableWithCurrentState();
          }
          return;
        }

        if (window.AnalyticsSets && typeof window.AnalyticsSets.handleEscapeFromSetFocus === 'function') {
          const consumed = window.AnalyticsSets.handleEscapeFromSetFocus();
          if (consumed) {
            event.preventDefault();
            syncSetStateFromModule();
            return;
          }
        }
        return;
      }

      if (event.ctrlKey && event.altKey && !event.shiftKey && !event.metaKey && event.code === 'KeyD') {
        if (isTypingElement(document.activeElement)) return;
        event.preventDefault();
        deselectAllStudies();
        return;
      }

      if (event.altKey || event.ctrlKey || event.metaKey || event.shiftKey) return;
      if (event.key !== 'ArrowUp' && event.key !== 'ArrowDown') return;
      if (isTypingElement(document.activeElement) || hasOpenAnalyticsMenu()) return;

      if (window.AnalyticsSets && typeof window.AnalyticsSets.isMoveMode === 'function'
          && window.AnalyticsSets.isMoveMode()) {
        return;
      }

      const direction = event.key === 'ArrowDown' ? 1 : -1;
      if (AnalyticsState.focusedStudyId) {
        if (moveFocusedStudy(direction)) {
          event.preventDefault();
        }
        return;
      }

      if (AnalyticsState.focusedSetId !== null && moveFocusedSet(direction)) {
        event.preventDefault();
      }
    });
  }

  async function initAnalyticsPage() {
    bindCollapsibleHeaders();
    bindSelectionButtons();
    bindTableToggles();
    initSetsModule();
    bindFocusHotkeys();
    try {
      await Promise.all([loadDatabases(), loadSummary()]);
    } catch (error) {
      showMessage(error.message || 'Failed to initialize analytics page.');
      window.AnalyticsEquity.renderEmpty('No data to display');
    }
  }

  document.addEventListener('DOMContentLoaded', initAnalyticsPage);
})();
