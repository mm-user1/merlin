(function () {
  const VIEW_MODES = {
    ALL: 'allStudies',
    FOCUS: 'setFocus',
    CHECKBOXES: 'setCheckboxes',
  };

  const SET_COLOR_OPTIONS = [
    { token: null, label: 'Default' },
    { token: 'blue', label: 'Blue' },
    { token: 'teal', label: 'Teal' },
    { token: 'mint', label: 'Mint' },
    { token: 'olive', label: 'Olive' },
    { token: 'sand', label: 'Sand' },
    { token: 'amber', label: 'Amber' },
    { token: 'rose', label: 'Rose' },
    { token: 'lavender', label: 'Lavender' },
  ];
  const SET_COLOR_TOKENS = new Set(
    SET_COLOR_OPTIONS.map((option) => option.token).filter(Boolean)
  );
  const TABLE_HEIGHT_MIN = 0;
  const TABLE_HEIGHT_MAX = 2;
  const TABLE_HEIGHT_OPTIONS = [
    { id: 'analyticsSetsHeightMinBtn', label: 'Min', level: 0, ariaLabel: 'Set minimum sets table height' },
    { id: 'analyticsSetsHeightMedBtn', label: 'Med', level: 1, ariaLabel: 'Set medium sets table height' },
    { id: 'analyticsSetsHeightMaxBtn', label: 'Max', level: 2, ariaLabel: 'Set maximum sets table height' },
  ];
  const SET_SORT_META = {
    set_name: {
      label: 'Set Name',
      directionLabels: { asc: 'A-Z', desc: 'Z-A' },
    },
    ann_profit_pct: {
      label: 'Ann.P%',
      directionLabels: { asc: 'lowest first', desc: 'highest first' },
    },
    profit_pct: {
      label: 'Profit%',
      directionLabels: { asc: 'lowest first', desc: 'highest first' },
    },
    max_drawdown_pct: {
      label: 'MaxDD%',
      directionLabels: { asc: 'lowest first', desc: 'highest first' },
    },
    profitable_pct: {
      label: 'Profitable',
      directionLabels: { asc: 'lowest first', desc: 'highest first' },
    },
    wfe_pct: {
      label: 'WFE%',
      directionLabels: { asc: 'lowest first', desc: 'highest first' },
    },
    oos_wins_pct: {
      label: 'OOS Wins',
      directionLabels: { asc: 'lowest first', desc: 'highest first' },
    },
    consistency: {
      label: 'Consist',
      directionLabels: { asc: 'lowest first', desc: 'highest first' },
    },
  };

  const state = {
    studies: [],
    studyMap: new Map(),
    focusedStudyId: null,
    sets: [],
    allMetrics: null,
    focusedSetId: null,
    checkedSetIds: new Set(),
    viewMode: VIEW_MODES.ALL,
    checkedStudyIds: new Set(),
    forceAllStudies: false,
    batchMode: false,
    batchSelectedSetIds: new Set(),
    batchAnchorSetId: null,
    moveMode: false,
    moveOriginalOrder: [],
    moveSelectionIds: [],
    moveInsertionIndex: 0,
    rangeAnchorSetId: null,
    rangeAnchorChecked: null,
    panelOpen: true,
    panelTouched: false,
    tableHeightLevel: TABLE_HEIGHT_MIN,
    updateMenuOpen: false,
    colorMenuOpen: false,
    compareMarkers: new Map(),
    onStateChange: null,
    onCompareToggle: null,
    bound: false,
  };

  function escapeHtml(value) {
    return String(value ?? '')
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#39;');
  }

  function toFiniteNumber(value) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }

  function average(values) {
    const finite = values
      .map((value) => toFiniteNumber(value))
      .filter((value) => value !== null);
    if (!finite.length) return null;
    return finite.reduce((acc, value) => acc + value, 0) / finite.length;
  }

  function normalizeSetId(raw) {
    const parsed = Number(raw);
    if (!Number.isInteger(parsed) || parsed <= 0) return null;
    return parsed;
  }

  function normalizeColorToken(raw) {
    const token = String(raw || '').trim().toLowerCase();
    if (!token) return null;
    return SET_COLOR_TOKENS.has(token) ? token : null;
  }

  function normalizeTableHeightLevel(raw) {
    const parsed = Number(raw);
    if (!Number.isInteger(parsed)) return TABLE_HEIGHT_MIN;
    return Math.max(TABLE_HEIGHT_MIN, Math.min(TABLE_HEIGHT_MAX, parsed));
  }

  function cloneCheckedSetIds() {
    return new Set(Array.from(state.checkedSetIds));
  }

  function cloneBatchSelectedSetIds() {
    return new Set(Array.from(state.batchSelectedSetIds));
  }

  function cloneCompareMarkers(compareMarkers) {
    const next = new Map();
    if (!(compareMarkers instanceof Map)) return next;

    compareMarkers.forEach((marker, rawSetId) => {
      const setId = normalizeSetId(rawSetId);
      if (setId === null || !marker || typeof marker !== 'object') return;

      const color = String(marker.color || '').trim();
      if (!color) return;

      const slotIndex = Number.isInteger(marker.slotIndex)
        ? marker.slotIndex
        : Number.parseInt(marker.slotIndex, 10);
      next.set(setId, {
        color,
        slotIndex: Number.isInteger(slotIndex) ? slotIndex : null,
      });
    });

    return next;
  }

  function cloneMetrics(rawMetrics) {
    if (!rawMetrics || typeof rawMetrics !== 'object') return null;
    return {
      ann_profit_pct: toFiniteNumber(rawMetrics.ann_profit_pct),
      profit_pct: toFiniteNumber(rawMetrics.profit_pct),
      max_drawdown_pct: toFiniteNumber(rawMetrics.max_drawdown_pct),
      consistency_full: toFiniteNumber(rawMetrics.consistency_full),
      consistency_recent: toFiniteNumber(rawMetrics.consistency_recent),
      overlap_days: toFiniteNumber(rawMetrics.overlap_days),
      overlap_days_exact: toFiniteNumber(rawMetrics.overlap_days_exact),
      studies_used: toFiniteNumber(rawMetrics.studies_used),
      studies_excluded: toFiniteNumber(rawMetrics.studies_excluded),
      selected_count: toFiniteNumber(rawMetrics.selected_count),
      warning: rawMetrics.warning == null ? null : String(rawMetrics.warning),
      computed_at: rawMetrics.computed_at || null,
      has_curve: Boolean(rawMetrics.has_curve),
      curve_point_count: toFiniteNumber(rawMetrics.curve_point_count),
    };
  }

  function cloneSetList(rawSets) {
    if (!Array.isArray(rawSets)) return [];
    return rawSets
      .map((setItem) => {
        const id = normalizeSetId(setItem?.id);
        if (id === null) return null;
        const studyIds = Array.isArray(setItem?.study_ids)
          ? setItem.study_ids.map((value) => String(value || '').trim()).filter(Boolean)
          : [];
        return {
          id,
          name: String(setItem?.name || '').trim(),
          sort_order: Number.isFinite(Number(setItem?.sort_order)) ? Number(setItem.sort_order) : 0,
          created_at: setItem?.created_at || null,
          color_token: normalizeColorToken(setItem?.color_token),
          study_ids: studyIds,
          metrics: cloneMetrics(setItem?.metrics),
        };
      })
      .filter(Boolean)
      .sort((left, right) => {
        if (left.sort_order !== right.sort_order) return left.sort_order - right.sort_order;
        return left.id - right.id;
      });
  }

  function getSetById(setId) {
    const normalized = normalizeSetId(setId);
    if (normalized === null) return null;
    return state.sets.find((item) => item.id === normalized) || null;
  }

  function hasSet(setId) {
    return Boolean(getSetById(setId));
  }

  function getAllSetIds() {
    return state.sets.map((item) => item.id);
  }

  function getActiveSetSortState() {
    if (!window.AnalyticsSetsView || typeof window.AnalyticsSetsView.getSortState !== 'function') {
      return { sortColumn: null, sortDirection: null, sortClickCount: 0 };
    }
    const sortState = window.AnalyticsSetsView.getSortState();
    return {
      sortColumn: sortState?.sortColumn || null,
      sortDirection: sortState?.sortDirection || null,
      sortClickCount: Number(sortState?.sortClickCount || 0),
    };
  }

  function hasActiveSetSort() {
    return Boolean(
      window.AnalyticsSetsView
      && typeof window.AnalyticsSetsView.hasActiveSort === 'function'
      && window.AnalyticsSetsView.hasActiveSort()
    );
  }

  function hasActiveSetFilters() {
    return Boolean(
      window.AnalyticsSetsView
      && typeof window.AnalyticsSetsView.hasActiveFilters === 'function'
      && window.AnalyticsSetsView.hasActiveFilters()
    );
  }

  function compareNumbersWithNulls(left, right, sortDirection) {
    const leftValue = toFiniteNumber(left);
    const rightValue = toFiniteNumber(right);
    if (leftValue === null && rightValue === null) return 0;
    if (leftValue === null) return 1;
    if (rightValue === null) return -1;
    if (leftValue === rightValue) return 0;
    if (sortDirection === 'asc') return leftValue - rightValue;
    return rightValue - leftValue;
  }

  function compareTextValues(left, right, sortDirection) {
    const leftValue = String(left || '');
    const rightValue = String(right || '');
    const cmp = leftValue.localeCompare(rightValue, undefined, { numeric: true, sensitivity: 'base' });
    return sortDirection === 'asc' ? cmp : -cmp;
  }

  function buildVisibleSetRows() {
    const visibleIds = !window.AnalyticsSetsView || typeof window.AnalyticsSetsView.getVisibleSetIds !== 'function'
      ? new Set(getAllSetIds())
      : new Set(
        window.AnalyticsSetsView.getVisibleSetIds()
          .map((setId) => normalizeSetId(setId))
          .filter((setId) => setId !== null && hasSet(setId))
      );
    const rows = state.sets
      .map((setItem, index) => {
        const normalizedId = normalizeSetId(setItem?.id);
        if (normalizedId === null || !visibleIds.has(normalizedId)) return null;
        const setStudies = resolveStudiesForSet(setItem);
        const metrics = computeMetrics(setStudies, setItem.metrics);
        return {
          setItem,
          metrics,
          defaultIndex: index,
        };
      })
      .filter(Boolean);

    const sortState = getActiveSetSortState();
    const hasSort = !state.moveMode && Boolean(sortState.sortColumn && sortState.sortDirection);
    if (!hasSort) return rows;

    return rows.slice().sort((leftRow, rightRow) => {
      const sortColumn = sortState.sortColumn;
      const sortDirection = sortState.sortDirection;
      let cmp = 0;

      if (sortColumn === 'set_name') {
        cmp = compareTextValues(leftRow.setItem.name, rightRow.setItem.name, sortDirection);
      } else if (sortColumn === 'ann_profit_pct') {
        cmp = compareNumbersWithNulls(leftRow.metrics.annProfitPct, rightRow.metrics.annProfitPct, sortDirection);
      } else if (sortColumn === 'profit_pct') {
        cmp = compareNumbersWithNulls(leftRow.metrics.profitPct, rightRow.metrics.profitPct, sortDirection);
      } else if (sortColumn === 'max_drawdown_pct') {
        cmp = compareNumbersWithNulls(leftRow.metrics.maxDdPct, rightRow.metrics.maxDdPct, sortDirection);
      } else if (sortColumn === 'profitable_pct') {
        cmp = compareNumbersWithNulls(leftRow.metrics.profitablePct, rightRow.metrics.profitablePct, sortDirection);
        if (cmp === 0) {
          cmp = compareNumbersWithNulls(
            leftRow.metrics.profitableCount,
            rightRow.metrics.profitableCount,
            sortDirection
          );
        }
      } else if (sortColumn === 'wfe_pct') {
        cmp = compareNumbersWithNulls(leftRow.metrics.wfePct, rightRow.metrics.wfePct, sortDirection);
      } else if (sortColumn === 'oos_wins_pct') {
        cmp = compareNumbersWithNulls(leftRow.metrics.oosWinsPct, rightRow.metrics.oosWinsPct, sortDirection);
      } else if (sortColumn === 'consistency') {
        cmp = compareNumbersWithNulls(
          leftRow.metrics.consistencyRecent,
          rightRow.metrics.consistencyRecent,
          sortDirection
        );
        if (cmp === 0) {
          cmp = compareNumbersWithNulls(
            leftRow.metrics.consistencyFull,
            rightRow.metrics.consistencyFull,
            sortDirection
          );
        }
      }

      if (cmp !== 0) return cmp;
      return leftRow.defaultIndex - rightRow.defaultIndex;
    });
  }

  function getVisibleOrderedSetIds() {
    return buildVisibleSetRows().map((row) => row.setItem.id);
  }

  function getVisibleSetRows() {
    return buildVisibleSetRows();
  }

  function getVisibleSetIdSet() {
    return new Set(getVisibleOrderedSetIds());
  }

  function getNormalizedFocusedStudyId() {
    const normalized = String(state.focusedStudyId || '').trim();
    return normalized || null;
  }

  function getFocusedStudyMembershipInfo() {
    const focusedStudyId = getNormalizedFocusedStudyId();
    if (!focusedStudyId) {
      return {
        focusedStudyId: null,
        totalCount: 0,
        visibleCount: 0,
        matchingSetIds: new Set(),
      };
    }

    const visibleSetIds = getVisibleSetIdSet();
    const matchingSetIds = new Set();
    let visibleCount = 0;

    state.sets.forEach((setItem) => {
      if (!Array.isArray(setItem?.study_ids) || !setItem.study_ids.includes(focusedStudyId)) {
        return;
      }
      matchingSetIds.add(setItem.id);
      if (visibleSetIds.has(setItem.id)) {
        visibleCount += 1;
      }
    });

    return {
      focusedStudyId,
      totalCount: matchingSetIds.size,
      visibleCount,
      matchingSetIds,
    };
  }

  function applyFocusedStudyMembershipClasses() {
    const membership = getFocusedStudyMembershipInfo();
    const { tableWrap } = getDom();
    if (!tableWrap) return;

    tableWrap.querySelectorAll('tr.analytics-set-row').forEach((row) => {
      const setId = normalizeSetId(row.dataset.setId || '');
      const shouldHighlight = setId !== null
        && membership.matchingSetIds.has(setId)
        && !row.classList.contains('analytics-set-focused')
        && !row.classList.contains('analytics-set-batch-selected')
        && !row.classList.contains('analytics-set-moving');
      row.classList.toggle('analytics-set-study-match', shouldHighlight);
    });
  }

  function applyCompareMarkers() {
    const { tableWrap } = getDom();
    if (!tableWrap) return;

    tableWrap.querySelectorAll('tr.analytics-set-row').forEach((row) => {
      const setId = normalizeSetId(row.dataset.setId || '');
      const marker = setId === null ? null : (state.compareMarkers.get(setId) || null);
      const isFocused = setId !== null && state.focusedSetId === setId;

      row.classList.toggle('analytics-set-compare', !isFocused && Boolean(marker));

      if (!isFocused && marker?.color) {
        row.style.setProperty('--analytics-compare-marker-color', marker.color);
      } else {
        row.style.removeProperty('--analytics-compare-marker-color');
      }

      if (!isFocused && Number.isInteger(marker?.slotIndex)) {
        row.setAttribute('data-compare-slot', String(marker.slotIndex + 1));
      } else {
        row.removeAttribute('data-compare-slot');
      }
    });
  }

  function getInteractiveSetIds() {
    return getVisibleOrderedSetIds();
  }

  function hasActiveViewControls() {
    return Boolean(
      window.AnalyticsSetsView
      && typeof window.AnalyticsSetsView.hasActiveControls === 'function'
      && window.AnalyticsSetsView.hasActiveControls()
    );
  }

  function describeActiveSetSort() {
    const sortState = getActiveSetSortState();
    if (!sortState.sortColumn || !sortState.sortDirection) return '';
    const meta = SET_SORT_META[sortState.sortColumn];
    if (!meta) return '';
    const directionLabel = meta.directionLabels?.[sortState.sortDirection]
      || (sortState.sortDirection === 'asc' ? 'ascending' : 'descending');
    return `Sorted by ${meta.label} (${directionLabel})`;
  }

  function getMoveDisabledHintText() {
    const hasFilters = hasActiveSetFilters();
    const hasSort = hasActiveSetSort();
    if (hasFilters && hasSort) return 'Clear set filters or reset sorting before moving rows.';
    if (hasSort) return 'Reset set sorting before moving rows.';
    if (hasFilters) return 'Clear set filters before moving rows.';
    return '';
  }

  function renderHeaderControls() {
    if (!window.AnalyticsSetsView || typeof window.AnalyticsSetsView.renderControls !== 'function') return;
    window.AnalyticsSetsView.renderControls({ disabled: state.moveMode });
  }

  function buildSyncCheckedStudyIdsForCurrentView() {
    if (state.viewMode === VIEW_MODES.FOCUS) {
      const focused = getSetById(state.focusedSetId);
      return Array.from(new Set((focused?.study_ids || []).slice()));
    }
    if (state.viewMode === VIEW_MODES.CHECKBOXES) {
      const unionIds = computeVisibleStudyIds();
      return Array.from(unionIds || []);
    }
    return null;
  }

  function syncSelectionToVisibleSets() {
    const visibleIds = getVisibleSetIdSet();
    let changed = false;

    if (state.focusedSetId !== null && !visibleIds.has(state.focusedSetId)) {
      state.focusedSetId = null;
      state.forceAllStudies = false;
      changed = true;
    }

    const nextChecked = new Set(
      Array.from(state.checkedSetIds).filter((setId) => visibleIds.has(setId))
    );
    if (nextChecked.size !== state.checkedSetIds.size) {
      state.checkedSetIds = nextChecked;
      changed = true;
    }

    const nextBatchSelected = new Set(
      Array.from(state.batchSelectedSetIds).filter((setId) => visibleIds.has(setId))
    );
    if (nextBatchSelected.size !== state.batchSelectedSetIds.size) {
      state.batchSelectedSetIds = nextBatchSelected;
      changed = true;
    }

    if (state.batchAnchorSetId !== null && !visibleIds.has(state.batchAnchorSetId)) {
      state.batchAnchorSetId = null;
      changed = true;
    }
    if (state.rangeAnchorSetId !== null && !visibleIds.has(state.rangeAnchorSetId)) {
      state.rangeAnchorSetId = null;
      state.rangeAnchorChecked = null;
      changed = true;
    }

    if (state.batchMode && state.batchSelectedSetIds.size === 0) {
      state.batchMode = false;
      changed = true;
    }

    resolveViewMode();
    return changed;
  }

  function getCheckedSetArray() {
    return Array.from(state.checkedSetIds)
      .map((setId) => getSetById(setId))
      .filter(Boolean);
  }

  function clearBatchSelection() {
    state.batchSelectedSetIds = new Set();
    state.batchAnchorSetId = null;
  }

  function getOrderedBatchSelectedSetIds() {
    if (!(state.batchSelectedSetIds instanceof Set) || state.batchSelectedSetIds.size === 0) {
      return [];
    }
    return getInteractiveSetIds()
      .filter((setId) => state.batchSelectedSetIds.has(setId));
  }

  function getActiveActionSetIds() {
    if (state.batchMode) {
      return getOrderedBatchSelectedSetIds();
    }
    return state.focusedSetId !== null ? [state.focusedSetId] : [];
  }

  function getCommonColorToken(setIds) {
    const orderedIds = Array.isArray(setIds) ? setIds : [];
    if (!orderedIds.length) return undefined;
    let common = null;
    let initialized = false;
    for (const setId of orderedIds) {
      const setItem = getSetById(setId);
      if (!setItem) continue;
      const token = normalizeColorToken(setItem.color_token);
      if (!initialized) {
        common = token;
        initialized = true;
        continue;
      }
      if (token !== common) {
        return undefined;
      }
    }
    return initialized ? common : undefined;
  }

  function resolveViewMode() {
    if (state.focusedSetId !== null && !hasSet(state.focusedSetId)) {
      state.focusedSetId = null;
    }
    state.checkedSetIds = new Set(Array.from(state.checkedSetIds).filter((setId) => hasSet(setId)));

    if (state.focusedSetId !== null) {
      state.viewMode = VIEW_MODES.FOCUS;
      return;
    }
    if (state.forceAllStudies) {
      state.viewMode = VIEW_MODES.ALL;
      return;
    }
    if (state.checkedSetIds.size > 0) {
      state.viewMode = VIEW_MODES.CHECKBOXES;
      return;
    }
    state.viewMode = VIEW_MODES.ALL;
  }

  function computeVisibleStudyIds() {
    if (state.viewMode === VIEW_MODES.ALL) return null;
    const ids = new Set();

    if (state.viewMode === VIEW_MODES.FOCUS) {
      const focused = getSetById(state.focusedSetId);
      if (!focused) return new Set();
      focused.study_ids.forEach((studyId) => ids.add(studyId));
      return ids;
    }

    getCheckedSetArray().forEach((setItem) => {
      setItem.study_ids.forEach((studyId) => ids.add(studyId));
    });
    return ids;
  }

  function resolveStudiesForSet(setItem) {
    if (!setItem) return [];
    const studies = [];
    setItem.study_ids.forEach((studyId) => {
      const study = state.studyMap.get(String(studyId || ''));
      if (study) studies.push(study);
    });
    return studies;
  }

  function computeNonCurveMetrics(studies) {
    const list = Array.isArray(studies) ? studies : [];
    if (!list.length) {
      return {
        profitableText: '0/0 (0%)',
        profitableCount: 0,
        profitableTotal: 0,
        profitablePct: 0,
        wfePct: null,
        oosWinsPct: null,
      };
    }

    const profitableCount = list.reduce((acc, study) => {
      const profit = toFiniteNumber(study?.profit_pct);
      return acc + (profit !== null && profit > 0 ? 1 : 0);
    }, 0);
    const profitablePct = list.length > 0 ? Math.round((profitableCount / list.length) * 100) : 0;
    const profitableText = `${profitableCount}/${list.length} (${profitablePct}%)`;

    const wfePct = average(list.map((study) => study?.wfe_pct));
    const oosWinsPct = average(list.map((study) => study?.profitable_windows_pct));

    return {
      profitableText,
      profitableCount,
      profitableTotal: list.length,
      profitablePct,
      wfePct,
      oosWinsPct,
    };
  }

  function computeMetrics(studies, curveMetrics) {
    const list = Array.isArray(studies) ? studies : [];
    const nonCurve = computeNonCurveMetrics(list);
    const curve = curveMetrics && typeof curveMetrics === 'object' ? curveMetrics : null;

    return {
      annProfitPct: toFiniteNumber(curve?.ann_profit_pct),
      profitPct: toFiniteNumber(curve?.profit_pct),
      maxDdPct: toFiniteNumber(curve?.max_drawdown_pct),
      consistencyFull: toFiniteNumber(curve?.consistency_full),
      consistencyRecent: toFiniteNumber(curve?.consistency_recent),
      profitableText: nonCurve.profitableText,
      profitableCount: nonCurve.profitableCount,
      profitableTotal: nonCurve.profitableTotal,
      profitablePct: nonCurve.profitablePct,
      wfePct: nonCurve.wfePct,
      oosWinsPct: nonCurve.oosWinsPct,
    };
  }

  function formatSignedPercent(value, digits = 1) {
    const parsed = toFiniteNumber(value);
    if (parsed === null) return 'N/A';
    if (parsed === 0) return `0.${'0'.repeat(digits)}%`;
    const sign = parsed > 0 ? '+' : '-';
    return `${sign}${Math.abs(parsed).toFixed(digits)}%`;
  }

  function formatNegativePercent(value, digits = 1) {
    const parsed = toFiniteNumber(value);
    if (parsed === null) return 'N/A';
    return `-${Math.abs(parsed).toFixed(digits)}%`;
  }

  function formatUnsignedPercent(value, digits = 1) {
    const parsed = toFiniteNumber(value);
    if (parsed === null) return 'N/A';
    return `${parsed.toFixed(digits)}%`;
  }

  function getSignedClass(value) {
    const parsed = toFiniteNumber(value);
    if (parsed === null) return '';
    return parsed >= 0 ? 'val-positive' : 'val-negative';
  }

  function getMaxDdClass(value) {
    const parsed = toFiniteNumber(value);
    if (parsed === null) return '';
    return Math.abs(parsed) > 40 ? 'val-negative' : '';
  }

  function formatConsistencyValue(value, digits = 2) {
    const parsed = toFiniteNumber(value);
    if (parsed === null) return 'N/A';
    return parsed.toFixed(digits);
  }

  function getRecentConsistencyClass(value) {
    const parsed = toFiniteNumber(value);
    if (parsed === null) return '';
    if (parsed < -0.2) return 'val-negative';
    if (parsed > 0) return 'val-positive';
    return '';
  }

  function getFullConsistencyClass(value) {
    const parsed = toFiniteNumber(value);
    if (parsed === null) return '';
    if (parsed < 0) return 'val-negative';
    if (parsed > 0.8) return 'val-positive';
    return '';
  }

  function renderConsistencyPair(recentValue, fullValue, digits = 2) {
    const recent = toFiniteNumber(recentValue);
    const full = toFiniteNumber(fullValue);
    if (recent === null && full === null) return 'N/A';

    const recentClass = getRecentConsistencyClass(recent);
    const fullClass = getFullConsistencyClass(full);
    return [
      `<span class="${recentClass}">${escapeHtml(formatConsistencyValue(recent, digits))}</span>`,
      '<span>/</span>',
      `<span class="${fullClass}">${escapeHtml(formatConsistencyValue(full, digits))}</span>`,
    ].join('');
  }

  function updateStudyMap() {
    state.studyMap = new Map();
    state.studies.forEach((study) => {
      const studyId = String(study?.study_id || '').trim();
      if (studyId) state.studyMap.set(studyId, study);
    });
  }

  function emitStateChange(meta = {}) {
    if (typeof state.onStateChange !== 'function') return;
    state.onStateChange({
      ...meta,
      focusedSetId: state.focusedSetId,
      checkedSetIds: cloneCheckedSetIds(),
      viewMode: state.viewMode,
    });
  }

  function getDom() {
    return {
      section: document.getElementById('analyticsSetsSection'),
      root: document.getElementById('analytics-sets-collapsible'),
      header: document.getElementById('analyticsSetsHeader'),
      summary: document.getElementById('analyticsSetsSummary'),
      tableWrap: document.getElementById('analyticsSetsTableWrap'),
      actions: document.getElementById('analyticsSetsActions'),
    };
  }

  function findSetRow(setId) {
    const normalized = normalizeSetId(setId);
    if (normalized === null) return null;
    const { tableWrap } = getDom();
    if (!tableWrap) return null;
    return Array.from(tableWrap.querySelectorAll('tr.analytics-set-row'))
      .find((row) => normalizeSetId(row.dataset.setId || '') === normalized) || null;
  }

  function isEventInsideElement(event, element) {
    if (!event || !element) return false;
    const target = event.target instanceof Node ? event.target : null;
    if (!target) return false;
    if (element.contains(target)) return true;
    if (typeof event.composedPath === 'function') {
      const path = event.composedPath();
      if (Array.isArray(path) && path.includes(element)) return true;
    }
    return false;
  }

  function clearTextSelection() {
    const selection = window.getSelection ? window.getSelection() : null;
    if (selection && typeof selection.removeAllRanges === 'function') {
      selection.removeAllRanges();
    }
  }

  function closeColorMenu() {
    state.colorMenuOpen = false;
  }

  function closeTransientMenus() {
    state.updateMenuOpen = false;
    closeColorMenu();
  }

  function clearMoveState() {
    state.moveMode = false;
    state.moveOriginalOrder = [];
    state.moveSelectionIds = [];
    state.moveInsertionIndex = 0;
  }

  function getMoveSelectionSet() {
    return new Set(Array.isArray(state.moveSelectionIds) ? state.moveSelectionIds : []);
  }

  function getMoveableSelectionIds() {
    if (state.batchMode) {
      return getOrderedBatchSelectedSetIds();
    }
    return state.focusedSetId !== null ? [state.focusedSetId] : [];
  }

  function buildReorderedSetList(selectionIds, insertionIndex) {
    const orderedSelection = Array.isArray(selectionIds)
      ? selectionIds
        .map((setId) => getSetById(setId))
        .filter(Boolean)
      : [];
    if (!orderedSelection.length) return state.sets.slice();

    const selectedIdSet = new Set(orderedSelection.map((setItem) => setItem.id));
    const unselected = state.sets.filter((setItem) => !selectedIdSet.has(setItem.id));
    const boundedIndex = Math.max(0, Math.min(unselected.length, Number(insertionIndex) || 0));
    return [
      ...unselected.slice(0, boundedIndex),
      ...orderedSelection,
      ...unselected.slice(boundedIndex),
    ];
  }

  function getInitialMoveInsertionIndex(selectionIds) {
    const selectedIdSet = new Set(Array.isArray(selectionIds) ? selectionIds : []);
    let insertionIndex = 0;
    for (const setItem of state.sets) {
      if (selectedIdSet.has(setItem.id)) {
        break;
      }
      insertionIndex += 1;
    }
    return insertionIndex;
  }

  function renderTableHeight() {
    const { tableWrap } = getDom();
    if (!tableWrap) return;
    tableWrap.dataset.heightLevel = String(normalizeTableHeightLevel(state.tableHeightLevel));
  }

  function focusMoveKeyboardTarget() {
    const { tableWrap } = getDom();
    if (!(tableWrap instanceof HTMLElement)) return;
    tableWrap.tabIndex = -1;
    tableWrap.focus({ preventScroll: true });
  }

  function setPanelOpen(open) {
    state.panelOpen = Boolean(open);
    state.panelTouched = true;
    const { root } = getDom();
    if (!root) return;
    root.classList.toggle('open', state.panelOpen);
  }

  function moveSelectionToIndex(targetIndex) {
    if (!state.moveMode) return;
    const selectionIds = Array.isArray(state.moveSelectionIds) ? state.moveSelectionIds : [];
    if (!selectionIds.length) return;

    const unselectedCount = Math.max(0, state.sets.length - selectionIds.length);
    const boundedTarget = Math.max(0, Math.min(unselectedCount, Number(targetIndex) || 0));
    if (boundedTarget === state.moveInsertionIndex) return;

    state.moveInsertionIndex = boundedTarget;
    state.sets = buildReorderedSetList(selectionIds, boundedTarget);
    render();
  }

  function startMoveMode() {
    if (state.moveMode || hasActiveViewControls()) return;
    const selectionIds = getMoveableSelectionIds();
    if (!selectionIds.length) return;

    closeTransientMenus();
    state.moveMode = true;
    state.moveOriginalOrder = state.sets.map((setItem) => setItem.id);
    state.moveSelectionIds = selectionIds.slice();
    state.moveInsertionIndex = getInitialMoveInsertionIndex(selectionIds);
    render();
    if (typeof window.requestAnimationFrame === 'function') {
      window.requestAnimationFrame(focusMoveKeyboardTarget);
    } else {
      window.setTimeout(focusMoveKeyboardTarget, 0);
    }
  }

  async function confirmMoveMode() {
    if (!state.moveMode) return;
    const newOrder = state.sets.map((setItem) => setItem.id);
    const oldOrder = state.moveOriginalOrder.slice();
    const changed = oldOrder.length === newOrder.length
      ? oldOrder.some((value, index) => value !== newOrder[index])
      : true;

    if (!changed) {
      clearMoveState();
      render();
      return;
    }

    try {
      await reorderAnalyticsSetsRequest(newOrder);
      clearMoveState();
      state.sets.forEach((setItem, index) => {
        setItem.sort_order = index;
      });
      render();
      emitStateChange({ reason: 'moveConfirm' });
    } catch (error) {
      window.alert(error?.message || 'Failed to reorder sets.');
    }
  }

  function cancelMoveMode() {
    if (!state.moveMode) return;
    const byId = new Map(state.sets.map((setItem) => [setItem.id, setItem]));
    const restored = state.moveOriginalOrder
      .map((setId) => byId.get(setId))
      .filter(Boolean);
    if (restored.length === state.sets.length) {
      state.sets = restored;
    }
    clearMoveState();
    render();
  }

  function setCheckedSet(setId, checked) {
    const normalized = normalizeSetId(setId);
    if (normalized === null || !hasSet(normalized)) return;
    if (checked) state.checkedSetIds.add(normalized);
    else state.checkedSetIds.delete(normalized);
  }

  function applyRangeSelection(targetSetId) {
    if (state.rangeAnchorSetId === null || typeof state.rangeAnchorChecked !== 'boolean') return false;
    const ids = getInteractiveSetIds();
    const anchorIndex = ids.indexOf(state.rangeAnchorSetId);
    const targetIndex = ids.indexOf(targetSetId);
    if (anchorIndex < 0 || targetIndex < 0) return false;

    const [start, end] = anchorIndex <= targetIndex
      ? [anchorIndex, targetIndex]
      : [targetIndex, anchorIndex];
    for (let index = start; index <= end; index += 1) {
      setCheckedSet(ids[index], state.rangeAnchorChecked);
    }
    return true;
  }

  function rememberRangeAnchor(setId, checked) {
    state.rangeAnchorSetId = normalizeSetId(setId);
    state.rangeAnchorChecked = Boolean(checked);
  }

  function clearFocusedSetForModeChange(reasonAll, reasonCheckboxes) {
    if (state.focusedSetId === null) return false;
    closeTransientMenus();
    state.focusedSetId = null;
    state.forceAllStudies = false;
    resolveViewMode();
    render();

    if (state.viewMode === VIEW_MODES.CHECKBOXES) {
      const unionIds = computeVisibleStudyIds();
      emitStateChange({
        reason: reasonCheckboxes,
        syncCheckedStudyIds: Array.from(unionIds || []),
      });
    } else {
      emitStateChange({ reason: reasonAll, syncCheckedStudyIds: null });
    }
    return true;
  }

  function enterBatchMode() {
    if (state.moveMode || state.batchMode || !getInteractiveSetIds().length) return;
    closeTransientMenus();
    const seedSetId = state.focusedSetId;
    if (seedSetId !== null) {
      clearFocusedSetForModeChange('setFocusClearedForBatchToAll', 'setFocusClearedForBatchToCheckboxes');
    }
    state.batchMode = true;
    clearBatchSelection();
    if (seedSetId !== null && hasSet(seedSetId)) {
      state.batchSelectedSetIds = new Set([seedSetId]);
      state.batchAnchorSetId = seedSetId;
    }
    render();
  }

  function exitBatchMode() {
    if (!state.batchMode) return false;
    closeTransientMenus();
    clearBatchSelection();
    state.batchMode = false;
    render();
    return true;
  }

  function applyBatchRangeSelection(targetSetId) {
    const target = normalizeSetId(targetSetId);
    if (target === null || state.batchAnchorSetId === null) return false;

    const ids = getInteractiveSetIds();
    const anchorIndex = ids.indexOf(state.batchAnchorSetId);
    const targetIndex = ids.indexOf(target);
    if (anchorIndex < 0 || targetIndex < 0) return false;

    const [start, end] = anchorIndex <= targetIndex
      ? [anchorIndex, targetIndex]
      : [targetIndex, anchorIndex];
    state.batchSelectedSetIds = new Set(ids.slice(start, end + 1));
    return true;
  }

  function handleBatchSelection(setId, event) {
    if (state.moveMode || !state.batchMode) return;
    closeTransientMenus();
    const normalized = normalizeSetId(setId);
    if (normalized === null || !hasSet(normalized)) return;

    if (event.shiftKey) {
      if (!applyBatchRangeSelection(normalized)) {
        state.batchSelectedSetIds = new Set([normalized]);
      }
      state.batchAnchorSetId = normalized;
      render();
      return;
    }

    if (event.ctrlKey || event.metaKey) {
      const next = cloneBatchSelectedSetIds();
      if (next.has(normalized)) next.delete(normalized);
      else next.add(normalized);
      state.batchSelectedSetIds = next;
      state.batchAnchorSetId = normalized;
      render();
      return;
    }

    state.batchSelectedSetIds = new Set([normalized]);
    state.batchAnchorSetId = normalized;
    render();
  }

  function handleAllStudiesClick() {
    if (state.moveMode || state.batchMode) return;
    closeTransientMenus();
    const previousMode = state.viewMode;
    state.focusedSetId = null;
    state.forceAllStudies = true;
    resolveViewMode();
    render();
    if (previousMode !== state.viewMode) {
      emitStateChange({ reason: 'allStudiesClick', syncCheckedStudyIds: null });
    }
  }

  function handleCheckboxToggle(setId, event) {
    if (state.moveMode) return;
    closeTransientMenus();
    const normalized = normalizeSetId(setId);
    if (normalized === null) return;
    const focused = state.focusedSetId;
    state.forceAllStudies = false;
    const wasChecked = state.checkedSetIds.has(normalized);

    if (event.ctrlKey) {
      const next = !wasChecked;
      getInteractiveSetIds().forEach((id) => setCheckedSet(id, next));
      rememberRangeAnchor(normalized, next);
    } else if (event.shiftKey) {
      if (!applyRangeSelection(normalized)) {
        const next = !wasChecked;
        setCheckedSet(normalized, next);
        rememberRangeAnchor(normalized, next);
      }
    } else {
      const next = !wasChecked;
      setCheckedSet(normalized, next);
      rememberRangeAnchor(normalized, next);
    }

    const prevMode = state.viewMode;
    resolveViewMode();
    render();

    if (focused !== null) {
      emitStateChange({ reason: 'setCheckboxWhileFocus', syncCheckedStudyIds: null });
      return;
    }

    if (state.viewMode === VIEW_MODES.CHECKBOXES) {
      const unionIds = computeVisibleStudyIds();
      emitStateChange({
        reason: 'setCheckboxesChanged',
        syncCheckedStudyIds: Array.from(unionIds || []),
      });
      return;
    }

    if (prevMode !== state.viewMode) {
      emitStateChange({ reason: 'setCheckboxesCleared', syncCheckedStudyIds: null });
    }
  }

  function toggleFocus(setId) {
    if (state.moveMode || state.batchMode) return;
    closeTransientMenus();
    const normalized = normalizeSetId(setId);
    if (normalized === null || !hasSet(normalized)) return;

    if (state.focusedSetId === normalized) {
      clearFocusedSetForModeChange('setFocusClearedToAll', 'setFocusClearedToCheckboxes');
      return;
    }

    state.focusedSetId = normalized;
    state.forceAllStudies = false;
    resolveViewMode();
    render();
    const focusedSet = getSetById(normalized);
    emitStateChange({
      reason: 'setFocused',
      syncCheckedStudyIds: Array.from(new Set((focusedSet?.study_ids || []).slice())),
    });
  }

  function handleViewControlsChange() {
    if (state.moveMode) return;
    closeTransientMenus();
    const selectionChanged = syncSelectionToVisibleSets();
    render();
    if (!selectionChanged) return;
    emitStateChange({
      reason: 'setViewControlsChanged',
      syncCheckedStudyIds: buildSyncCheckedStudyIdsForCurrentView(),
    });
  }

  async function handleSaveSet() {
    const selectedStudyIds = Array.from(state.checkedStudyIds);
    if (!selectedStudyIds.length) return;

    const nameRaw = window.prompt('Enter set name:', '');
    if (nameRaw === null) return;
    const name = String(nameRaw || '').trim();
    if (!name) {
      window.alert('Set name cannot be empty.');
      return;
    }

    try {
      await createAnalyticsSetRequest(name, selectedStudyIds);
      await loadSets({ preserveSelection: true, emitState: true });
    } catch (error) {
      window.alert(error?.message || 'Failed to save set.');
    }
  }

  async function handleRenameSet() {
    if (state.batchMode) return;
    const focused = getSetById(state.focusedSetId);
    if (!focused) return;
    const nameRaw = window.prompt('Enter new set name:', focused.name);
    if (nameRaw === null) return;
    const name = String(nameRaw || '').trim();
    if (!name) {
      window.alert('Set name cannot be empty.');
      return;
    }

    try {
      await updateAnalyticsSetRequest(focused.id, { name });
      await loadSets({ preserveSelection: true, emitState: true, preferredFocusId: focused.id });
    } catch (error) {
      window.alert(error?.message || 'Failed to rename set.');
    }
  }

  async function handleDeleteSet() {
    const targetIds = getActiveActionSetIds();
    if (!targetIds.length) return;
    const targetSets = targetIds
      .map((setId) => getSetById(setId))
      .filter(Boolean);
    if (!targetSets.length) return;

    const confirmed = targetSets.length === 1
      ? window.confirm(`Delete set "${targetSets[0].name}"?`)
      : window.confirm(`Delete ${targetSets.length} selected sets? This cannot be undone.`);
    if (!confirmed) return;

    try {
      closeTransientMenus();
      if (targetSets.length === 1) {
        await deleteAnalyticsSetRequest(targetSets[0].id);
      } else {
        await bulkDeleteAnalyticsSetsRequest(targetIds);
      }
      if (state.batchMode) {
        clearBatchSelection();
      }
      await loadSets({ preserveSelection: true, emitState: false, preferredFocusId: null });
      resolveViewMode();
      render();
      if (state.viewMode === VIEW_MODES.CHECKBOXES) {
        const unionIds = computeVisibleStudyIds();
        emitStateChange({
          reason: 'setDeletedToCheckboxes',
          syncCheckedStudyIds: Array.from(unionIds || []),
        });
      } else {
        emitStateChange({ reason: 'setDeletedToAll', syncCheckedStudyIds: null });
      }
    } catch (error) {
      window.alert(error?.message || 'Failed to delete set.');
    }
  }

  async function updateSetMembers(setItem, preferredFocusId) {
    if (!setItem) return;
    const selectedStudyIds = Array.from(state.checkedStudyIds);
    if (!selectedStudyIds.length) {
      window.alert('Select at least one study before updating a set.');
      return;
    }

    const confirmed = window.confirm(`Update "${setItem.name}" with current selected studies only?`);
    if (!confirmed) return;

    try {
      closeTransientMenus();
      await updateAnalyticsSetRequest(setItem.id, { study_ids: selectedStudyIds });
      await loadSets({ preserveSelection: true, emitState: true, preferredFocusId });
    } catch (error) {
      window.alert(error?.message || 'Failed to update set members.');
    }
  }

  async function handleUpdateCurrentSet() {
    const focused = getSetById(state.focusedSetId);
    if (!focused) return;
    await updateSetMembers(focused, focused.id);
  }

  async function handleDropdownUpdateSet(setIdRaw) {
    const setId = normalizeSetId(setIdRaw);
    if (setId === null) return;
    const setItem = getSetById(setId);
    if (!setItem) return;
    await updateSetMembers(setItem, null);
  }

  function renderColorMenuItems(currentToken) {
    return SET_COLOR_OPTIONS.map((option) => {
      const token = option.token;
      const selected = token === currentToken;
      const swatchAttr = token ? ` data-color-token="${token}"` : '';
      return `
        <button class="analytics-set-color-item${selected ? ' is-selected' : ''}" type="button"
                data-set-color-token="${token || ''}">
          <span class="analytics-set-color-swatch"${swatchAttr}></span>
          <span class="analytics-set-color-item-label">${escapeHtml(option.label)}</span>
          <span class="analytics-set-color-item-check" aria-hidden="true">${selected ? '&#10003;' : ''}</span>
        </button>
      `;
    }).join('');
  }

  async function handleSetColorChange(colorTokenRaw) {
    const targetIds = getActiveActionSetIds();
    if (!targetIds.length) return;
    const nextToken = normalizeColorToken(colorTokenRaw);
    const currentToken = getCommonColorToken(targetIds);
    if (targetIds.length === 1 && nextToken === currentToken) {
      closeColorMenu();
      renderActions();
      return;
    }

    try {
      closeColorMenu();
      if (targetIds.length === 1) {
        await updateAnalyticsSetRequest(targetIds[0], { color_token: nextToken });
      } else {
        await bulkUpdateAnalyticsSetColorRequest(targetIds, nextToken);
      }
      const preferredFocusId = state.batchMode ? null : targetIds[0];
      await loadSets({ preserveSelection: true, emitState: true, preferredFocusId });
    } catch (error) {
      window.alert(error?.message || 'Failed to update set color.');
    }
  }

  function renderActions() {
    const { actions } = getDom();
    if (!actions) return;

    const focused = getSetById(state.focusedSetId);
    const hasSets = state.sets.length > 0;
    const visibleSetCount = getVisibleOrderedSetIds().length;
    const hasCheckedStudies = state.checkedStudyIds.size > 0;
    const activeSetIds = getActiveActionSetIds();
    const hasActionTarget = activeSetIds.length > 0;
    const hasBlockingViewControls = hasActiveViewControls();
    const moveDisabledHintText = getMoveDisabledHintText();
    const canToggleBatch = visibleSetCount > 0 && !state.moveMode;
    const canMove = hasActionTarget && !state.moveMode && !hasBlockingViewControls;
    const canRename = !state.batchMode && Boolean(focused) && !state.moveMode;
    const canDelete = hasActionTarget && !state.moveMode;
    const canColor = hasActionTarget && !state.moveMode;
    const canUpdate = hasSets && hasCheckedStudies && !state.moveMode;
    const canSave = hasCheckedStudies && !state.moveMode;
    const currentHeightLevel = normalizeTableHeightLevel(state.tableHeightLevel);
    const updateLabel = !state.batchMode && focused ? 'Update Current Set' : 'Update Set &#9662;';
    const currentColorToken = getCommonColorToken(activeSetIds);
    const moveHint = state.moveMode
      ? `<span class="hint">Move mode active. Enter = save, Esc = cancel.</span>`
      : (moveDisabledHintText
        ? `<span class="hint">${escapeHtml(moveDisabledHintText)}</span>`
        : '');
    const heightButtons = TABLE_HEIGHT_OPTIONS.map((option) => {
      const isActive = currentHeightLevel === option.level;
      const isDisabled = state.moveMode || isActive;
      return `
          <button class="sel-btn analytics-sets-height-btn${isActive ? ' analytics-sets-height-btn-active' : ''}"
                  id="${option.id}" type="button"
                  aria-label="${option.ariaLabel}"
                  aria-pressed="${isActive ? 'true' : 'false'}"${isDisabled ? ' disabled' : ''}>${option.label}</button>
      `;
    }).join('');

    if (!canUpdate && state.updateMenuOpen) {
      state.updateMenuOpen = false;
    }
    if (!canColor && state.colorMenuOpen) {
      closeColorMenu();
    }

    actions.innerHTML = `
      <div class="analytics-sets-actions-left">
        <button class="sel-btn${state.batchMode ? ' active' : ''}" id="analyticsSetBatchBtn"
                type="button" aria-pressed="${state.batchMode ? 'true' : 'false'}"
                ${canToggleBatch ? '' : ' disabled'}>Batch</button>
        <button class="sel-btn" id="analyticsSetMoveBtn" type="button"
                ${canMove ? '' : ' disabled'}
                ${moveDisabledHintText ? `title="${escapeHtml(moveDisabledHintText)}"` : ''}>Move</button>
        <button class="sel-btn" id="analyticsSetRenameBtn" type="button"${canRename ? '' : ' disabled'}>Rename</button>
        <button class="sel-btn" id="analyticsSetDeleteBtn" type="button"${canDelete ? '' : ' disabled'}>Delete</button>
        <div class="analytics-set-color-wrap" id="analyticsSetColorWrap">
          <button class="sel-btn" id="analyticsSetColorBtn" type="button"
                  aria-haspopup="menu" aria-expanded="${canColor && state.colorMenuOpen ? 'true' : 'false'}"
                  ${canColor ? '' : ' disabled'}>Color &#9662;</button>
          <div class="analytics-set-color-menu" id="analyticsSetColorMenu"${canColor && state.colorMenuOpen ? '' : ' hidden'}>
            ${renderColorMenuItems(currentColorToken)}
          </div>
        </div>
        ${moveHint}
      </div>
      <div class="analytics-sets-actions-center">
        <div class="analytics-sets-height-controls">
          ${heightButtons}
        </div>
      </div>
      <div class="analytics-sets-actions-right">
        <div class="analytics-set-update-wrap" id="analyticsSetUpdateWrap">
          <button class="sel-btn analytics-update-set-btn" id="analyticsSetUpdateBtn"
                  type="button" aria-haspopup="${!state.batchMode && focused ? 'false' : 'menu'}"
                  aria-expanded="${canUpdate && state.updateMenuOpen ? 'true' : 'false'}"
                  ${canUpdate ? '' : ' disabled'}>${updateLabel}</button>
          <div class="analytics-set-update-menu" id="analyticsSetUpdateMenu"${canUpdate && state.updateMenuOpen ? '' : ' hidden'}></div>
        </div>
        <button class="sel-btn analytics-save-set-btn" id="analyticsSaveSetBtn"
                type="button"${canSave ? '' : ' disabled'}>+ Save Set</button>
      </div>
    `;

    const batchBtn = document.getElementById('analyticsSetBatchBtn');
    const moveBtn = document.getElementById('analyticsSetMoveBtn');
    const renameBtn = document.getElementById('analyticsSetRenameBtn');
    const deleteBtn = document.getElementById('analyticsSetDeleteBtn');
    const colorBtn = document.getElementById('analyticsSetColorBtn');
    const colorMenu = document.getElementById('analyticsSetColorMenu');
    const updateBtn = document.getElementById('analyticsSetUpdateBtn');
    const updateMenu = document.getElementById('analyticsSetUpdateMenu');
    const saveBtn = document.getElementById('analyticsSaveSetBtn');
    const heightButtonsByLevel = TABLE_HEIGHT_OPTIONS
      .map((option) => ({
        level: option.level,
        button: document.getElementById(option.id),
      }))
      .filter((entry) => entry.button);

    if (batchBtn) {
      batchBtn.addEventListener('click', () => {
        if (state.batchMode) {
          exitBatchMode();
        } else {
          enterBatchMode();
        }
      });
    }
    if (moveBtn) moveBtn.addEventListener('click', startMoveMode);
    if (renameBtn) renameBtn.addEventListener('click', handleRenameSet);
    if (deleteBtn) deleteBtn.addEventListener('click', handleDeleteSet);
    if (colorBtn) {
      colorBtn.addEventListener('click', (event) => {
        event.preventDefault();
        event.stopPropagation();
        if (!canColor) return;
        state.colorMenuOpen = !state.colorMenuOpen;
        renderActions();
      });
    }
    if (colorMenu) {
      colorMenu.querySelectorAll('[data-set-color-token]').forEach((button) => {
        button.addEventListener('click', async (event) => {
          event.preventDefault();
          event.stopPropagation();
          await handleSetColorChange(button.getAttribute('data-set-color-token'));
        });
      });
    }
    if (updateBtn) {
      updateBtn.addEventListener('click', (event) => {
        event.preventDefault();
        event.stopPropagation();
        if (!canUpdate) return;
        if (!state.batchMode && focused) {
          handleUpdateCurrentSet();
          return;
        }
        state.updateMenuOpen = !state.updateMenuOpen;
        renderActions();
      });
    }
    if (updateMenu && canUpdate && state.updateMenuOpen) {
      const menuItems = state.sets
        .map((setItem) => (
          `<button class="analytics-set-update-item" type="button" data-update-set-id="${setItem.id}">`
          + `${escapeHtml(setItem.name)} (${setItem.study_ids.length})`
          + '</button>'
        ))
        .join('');
      updateMenu.innerHTML = menuItems || '<div class="analytics-set-update-empty">No sets available.</div>';
      updateMenu.querySelectorAll('[data-update-set-id]').forEach((button) => {
        button.addEventListener('click', async (event) => {
          event.preventDefault();
          event.stopPropagation();
          const setIdRaw = button.getAttribute('data-update-set-id');
          await handleDropdownUpdateSet(setIdRaw);
        });
      });
    }
    if (saveBtn) {
      saveBtn.addEventListener('click', (event) => {
        event.preventDefault();
        event.stopPropagation();
        if (!canSave) return;
        handleSaveSet();
      });
    }
    heightButtonsByLevel.forEach(({ level, button }) => {
      button.addEventListener('click', () => {
        state.tableHeightLevel = normalizeTableHeightLevel(level);
        render();
      });
    });
  }

  function renderSummaryText() {
    const { summary } = getDom();
    if (!summary) return;
    summary.classList.remove('align-left');
    const visibleCount = getVisibleOrderedSetIds().length;
    if (state.moveMode && state.moveSelectionIds.length > 0) {
      summary.textContent = `Moving: ${state.moveSelectionIds.length} selected`;
      return;
    }
    if (state.batchMode) {
      summary.textContent = `Batch: ${state.batchSelectedSetIds.size} selected`;
      return;
    }
    if (state.focusedSetId !== null) {
      const focused = getSetById(state.focusedSetId);
      if (focused) {
        summary.classList.add('align-left');
        summary.textContent = `Focused: ${focused.name} (${focused.study_ids.length})`;
        return;
      }
    }
    if (state.viewMode === VIEW_MODES.CHECKBOXES && state.checkedSetIds.size > 0) {
      summary.textContent = `Checked sets: ${state.checkedSetIds.size}`;
      return;
    }
    if (hasActiveSetFilters() && state.sets.length > 0) {
      const sortDescription = describeActiveSetSort();
      summary.textContent = sortDescription
        ? `Shown: ${visibleCount}/${state.sets.length} | ${sortDescription}`
        : `Shown: ${visibleCount}/${state.sets.length}`;
      return;
    }
    if (hasActiveSetSort() && state.sets.length > 0) {
      summary.textContent = describeActiveSetSort();
      return;
    }
    if (hasActiveViewControls() && state.sets.length > 0) {
      summary.textContent = `Shown: ${visibleCount}/${state.sets.length}`;
      return;
    }
    summary.textContent = '';
  }

  function renderTable() {
    const { tableWrap } = getDom();
    if (!tableWrap) return;
    renderTableHeight();

    const allMetrics = computeMetrics(state.studies, state.allMetrics);
    const moveSelectionSet = getMoveSelectionSet();
    const visibleRows = getVisibleSetRows();
    const focusedStudyMembership = getFocusedStudyMembershipInfo();
    const sortState = getActiveSetSortState();
    const rows = [];
    rows.push(`
      <tr class="analytics-set-all-row" data-all-studies="1">
        <td class="col-check"></td>
        <td title="All WFA studies in active database">All Studies</td>
        <td class="${getSignedClass(allMetrics.annProfitPct)}">${escapeHtml(formatSignedPercent(allMetrics.annProfitPct, 1))}</td>
        <td class="${getSignedClass(allMetrics.profitPct)}">${escapeHtml(formatSignedPercent(allMetrics.profitPct, 1))}</td>
        <td class="${getMaxDdClass(allMetrics.maxDdPct)}">${escapeHtml(formatNegativePercent(allMetrics.maxDdPct, 1))}</td>
        <td>${escapeHtml(allMetrics.profitableText)}</td>
        <td>${escapeHtml(formatUnsignedPercent(allMetrics.wfePct, 1))}</td>
        <td>${escapeHtml(formatUnsignedPercent(allMetrics.oosWinsPct, 1))}</td>
        <td title="Recent / Full signed R² for the aggregated portfolio curve">${renderConsistencyPair(allMetrics.consistencyRecent, allMetrics.consistencyFull, 2)}</td>
      </tr>
    `);

    if (!visibleRows.length) {
      rows.push(`
        <tr class="analytics-sets-empty-row">
          <td class="col-check"></td>
          <td colspan="8">No study sets match the current filters.</td>
        </tr>
      `);
    }

    visibleRows.forEach(({ setItem, metrics }) => {
      const checked = state.checkedSetIds.has(setItem.id) ? ' checked' : '';
      const focusedClass = state.focusedSetId === setItem.id ? ' analytics-set-focused' : '';
      const batchSelectedClass = state.batchMode && state.batchSelectedSetIds.has(setItem.id)
        ? ' analytics-set-batch-selected'
        : '';
      const movingClass = state.moveMode && moveSelectionSet.has(setItem.id) ? ' analytics-set-moving' : '';
      const focusedStudyMatchClass = !focusedClass && !batchSelectedClass && !movingClass
        && focusedStudyMembership.matchingSetIds.has(setItem.id)
        ? ' analytics-set-study-match'
        : '';
      const colorTokenAttr = setItem.color_token ? ` data-color-token="${setItem.color_token}"` : '';
      const encodedName = escapeHtml(setItem.name || `Set ${setItem.id}`);
      rows.push(`
        <tr class="analytics-set-row${focusedClass}${batchSelectedClass}${movingClass}${focusedStudyMatchClass}" data-set-id="${setItem.id}"${colorTokenAttr}>
          <td class="col-check"><input type="checkbox" class="analytics-set-check" data-set-id="${setItem.id}"${checked} /></td>
          <td title="${encodedName}">${encodedName} (${setItem.study_ids.length})</td>
          <td class="${getSignedClass(metrics.annProfitPct)}">${escapeHtml(formatSignedPercent(metrics.annProfitPct, 1))}</td>
          <td class="${getSignedClass(metrics.profitPct)}">${escapeHtml(formatSignedPercent(metrics.profitPct, 1))}</td>
          <td class="${getMaxDdClass(metrics.maxDdPct)}">${escapeHtml(formatNegativePercent(metrics.maxDdPct, 1))}</td>
          <td>${escapeHtml(metrics.profitableText)}</td>
          <td>${escapeHtml(formatUnsignedPercent(metrics.wfePct, 1))}</td>
          <td>${escapeHtml(formatUnsignedPercent(metrics.oosWinsPct, 1))}</td>
          <td title="Recent / Full signed R² for the aggregated portfolio curve">${renderConsistencyPair(metrics.consistencyRecent, metrics.consistencyFull, 2)}</td>
        </tr>
      `);
    });

    tableWrap.innerHTML = `
      <table class="analytics-sets-table">
        <thead>
          <tr>
            <th class="col-check"></th>
            <th class="analytics-sortable${sortState.sortColumn === 'set_name' ? ` sort-active sort-${sortState.sortDirection}` : ''}" data-sort-key="set_name">
              <span class="sort-label">Set Name</span><span class="sort-arrow">${sortState.sortColumn === 'set_name' ? (sortState.sortDirection === 'asc' ? '▲' : '▼') : '↕'}</span>
            </th>
            <th class="analytics-sortable${sortState.sortColumn === 'ann_profit_pct' ? ` sort-active sort-${sortState.sortDirection}` : ''}" data-sort-key="ann_profit_pct">
              <span class="sort-label">Ann.P%</span><span class="sort-arrow">${sortState.sortColumn === 'ann_profit_pct' ? (sortState.sortDirection === 'asc' ? '▲' : '▼') : '↕'}</span>
            </th>
            <th class="analytics-sortable${sortState.sortColumn === 'profit_pct' ? ` sort-active sort-${sortState.sortDirection}` : ''}" data-sort-key="profit_pct">
              <span class="sort-label">Profit%</span><span class="sort-arrow">${sortState.sortColumn === 'profit_pct' ? (sortState.sortDirection === 'asc' ? '▲' : '▼') : '↕'}</span>
            </th>
            <th class="analytics-sortable${sortState.sortColumn === 'max_drawdown_pct' ? ` sort-active sort-${sortState.sortDirection}` : ''}" data-sort-key="max_drawdown_pct">
              <span class="sort-label">MaxDD%</span><span class="sort-arrow">${sortState.sortColumn === 'max_drawdown_pct' ? (sortState.sortDirection === 'asc' ? '▲' : '▼') : '↕'}</span>
            </th>
            <th class="analytics-sortable${sortState.sortColumn === 'profitable_pct' ? ` sort-active sort-${sortState.sortDirection}` : ''}" data-sort-key="profitable_pct">
              <span class="sort-label">Profitable</span><span class="sort-arrow">${sortState.sortColumn === 'profitable_pct' ? (sortState.sortDirection === 'asc' ? '▲' : '▼') : '↕'}</span>
            </th>
            <th class="analytics-sortable${sortState.sortColumn === 'wfe_pct' ? ` sort-active sort-${sortState.sortDirection}` : ''}" data-sort-key="wfe_pct">
              <span class="sort-label">WFE%</span><span class="sort-arrow">${sortState.sortColumn === 'wfe_pct' ? (sortState.sortDirection === 'asc' ? '▲' : '▼') : '↕'}</span>
            </th>
            <th class="analytics-sortable${sortState.sortColumn === 'oos_wins_pct' ? ` sort-active sort-${sortState.sortDirection}` : ''}" data-sort-key="oos_wins_pct">
              <span class="sort-label">OOS Wins</span><span class="sort-arrow">${sortState.sortColumn === 'oos_wins_pct' ? (sortState.sortDirection === 'asc' ? '▲' : '▼') : '↕'}</span>
            </th>
            <th class="analytics-sortable${sortState.sortColumn === 'consistency' ? ` sort-active sort-${sortState.sortDirection}` : ''}" data-sort-key="consistency" title="Recent / Full signed R² for the aggregated portfolio curve">
              <span class="sort-label">Consist</span><span class="sort-arrow">${sortState.sortColumn === 'consistency' ? (sortState.sortDirection === 'asc' ? '▲' : '▼') : '↕'}</span>
            </th>
          </tr>
        </thead>
        <tbody>${rows.join('')}</tbody>
      </table>
    `;

    tableWrap.querySelectorAll('thead th.analytics-sortable').forEach((header) => {
      header.addEventListener('click', (event) => {
        event.preventDefault();
        event.stopPropagation();
        if (state.moveMode) return;
        const sortKey = header.dataset.sortKey || '';
        if (!sortKey || !window.AnalyticsSetsView || typeof window.AnalyticsSetsView.cycleSortForColumn !== 'function') {
          return;
        }
        window.AnalyticsSetsView.cycleSortForColumn(sortKey);
      });
    });

    const allRow = tableWrap.querySelector('tr[data-all-studies="1"]');
    if (allRow) {
      allRow.addEventListener('click', (event) => {
        event.preventDefault();
        handleAllStudiesClick();
      });
    }

    tableWrap.querySelectorAll('tr.analytics-set-row').forEach((row) => {
      const setId = normalizeSetId(row.dataset.setId || '');
      if (setId === null) return;

      const checkbox = row.querySelector('input.analytics-set-check');
      if (checkbox) {
        checkbox.addEventListener('click', (event) => {
          event.preventDefault();
          event.stopPropagation();
          const isCompareShortcut = event.ctrlKey && event.altKey && !event.shiftKey && !event.metaKey;
          if (isCompareShortcut) {
            if (typeof state.onCompareToggle === 'function') {
              state.onCompareToggle(setId);
            }
            return;
          }
          if (event.altKey) {
            toggleFocus(setId);
            return;
          }
          handleCheckboxToggle(setId, event);
        });
      }

      row.addEventListener('click', (event) => {
        if (event.target && event.target.closest('input.analytics-set-check')) return;
        event.preventDefault();
        if (state.batchMode) {
          handleBatchSelection(setId, event);
          return;
        }
        const isCompareShortcut = event.ctrlKey && event.altKey && !event.shiftKey && !event.metaKey;
        if (isCompareShortcut) {
          if (typeof state.onCompareToggle === 'function') {
            state.onCompareToggle(setId);
          }
          return;
        }
        if (event.altKey) {
          toggleFocus(setId);
          return;
        }
        handleCheckboxToggle(setId, event);
        });
    });

    applyFocusedStudyMembershipClasses();
    applyCompareMarkers();
  }

  function renderHeader() {
    const { root } = getDom();
    if (!root) return;
    root.classList.toggle('open', state.panelOpen);
    renderHeaderControls();
    return;

    const hasSets = state.sets.length > 0;
    const focusedSet = getSetById(state.focusedSetId);
    const isFocused = Boolean(focusedSet);
    const canUpdate = hasSets && state.checkedStudyIds.size > 0 && !state.moveMode;

    if (updateWrap) {
      updateWrap.style.display = hasSets ? '' : 'none';
    }

    if (!hasSets || isFocused || !canUpdate) {
      state.updateMenuOpen = false;
    }
    if (!isFocused) {
      closeColorMenu();
    }

    if (updateBtn) {
      updateBtn.disabled = !canUpdate;
      updateBtn.textContent = isFocused ? 'Update Current Set' : 'Update Set ▼';
      updateBtn.setAttribute('aria-haspopup', isFocused ? 'false' : 'menu');
      updateBtn.setAttribute('aria-expanded', !isFocused && state.updateMenuOpen ? 'true' : 'false');
    }

    if (updateMenu) {
      if (!isFocused && canUpdate && state.updateMenuOpen) {
        const menuItems = state.sets
          .map((setItem) => (
            `<button class="analytics-set-update-item" type="button" data-update-set-id="${setItem.id}">`
            + `${escapeHtml(setItem.name)} (${setItem.study_ids.length})`
            + '</button>'
          ))
          .join('');
        updateMenu.innerHTML = menuItems || '<div class="analytics-set-update-empty">No sets available.</div>';
        updateMenu.hidden = false;
        updateMenu.querySelectorAll('[data-update-set-id]').forEach((button) => {
          button.addEventListener('click', async (event) => {
            event.preventDefault();
            event.stopPropagation();
            const setIdRaw = button.getAttribute('data-update-set-id');
            await handleDropdownUpdateSet(setIdRaw);
          });
        });
      } else {
        updateMenu.hidden = true;
        updateMenu.innerHTML = '';
      }
    }

    if (saveBtn) {
      saveBtn.style.display = state.checkedStudyIds.size > 0 ? '' : 'none';
    }
  }

  function render() {
    renderHeader();
    renderSummaryText();
    renderTable();
    renderActions();
  }

  function syncFromLoadedSets(newSets, options = {}) {
    const preserveSelection = options.preserveSelection !== false;
    const preferredFocusId = options.preferredFocusId;
    const prevChecked = cloneCheckedSetIds();
    const prevFocused = state.focusedSetId;
    const prevForceAll = state.forceAllStudies;
    const prevBatchSelected = cloneBatchSelectedSetIds();
    const prevBatchAnchor = state.batchAnchorSetId;
    const prevBatchMode = state.batchMode;

    state.sets = cloneSetList(newSets);
    if (window.AnalyticsSetsView && typeof window.AnalyticsSetsView.updateSets === 'function') {
      window.AnalyticsSetsView.updateSets(state.sets, { emitChange: false });
    }
    state.forceAllStudies = preserveSelection ? prevForceAll : false;
    const availableIds = new Set(state.sets.map((setItem) => setItem.id));

    if (preserveSelection) {
      state.checkedSetIds = new Set(Array.from(prevChecked).filter((setId) => availableIds.has(setId)));
      if (preferredFocusId === null) {
        state.focusedSetId = null;
      } else if (preferredFocusId !== undefined) {
        const preferred = normalizeSetId(preferredFocusId);
        state.focusedSetId = preferred !== null && availableIds.has(preferred) ? preferred : null;
      } else {
        state.focusedSetId = prevFocused !== null && availableIds.has(prevFocused) ? prevFocused : null;
      }
      state.batchSelectedSetIds = new Set(
        Array.from(prevBatchSelected).filter((setId) => availableIds.has(setId))
      );
      state.batchAnchorSetId = prevBatchAnchor !== null && availableIds.has(prevBatchAnchor)
        ? prevBatchAnchor
        : null;
      state.batchMode = prevBatchMode && state.sets.length > 0;
    } else {
      state.checkedSetIds = new Set();
      state.focusedSetId = null;
      clearBatchSelection();
      state.batchMode = false;
    }

    resolveViewMode();
    syncSelectionToVisibleSets();
    if (!state.panelTouched) {
      state.panelOpen = state.sets.length > 0;
    }
    if (!state.sets.length) {
      clearBatchSelection();
      state.batchMode = false;
      closeTransientMenus();
      clearMoveState();
    }
  }

  async function loadSets(options = {}) {
    const payload = await fetchAnalyticsSetsRequest();
    state.allMetrics = cloneMetrics(payload?.all_metrics);
    syncFromLoadedSets(payload?.sets || [], options);
    render();
    if (options.emitState !== false) {
      emitStateChange({
        reason: 'setsLoaded',
        syncCheckedStudyIds: buildSyncCheckedStudyIdsForCurrentView(),
      });
    }
  }

  function updateStudies(studies) {
    state.allMetrics = null;
    state.studies = Array.isArray(studies) ? studies.slice() : [];
    updateStudyMap();
    render();
  }

  function updateCheckedStudyIds(checkedStudyIds) {
    state.checkedStudyIds = new Set(Array.from(checkedStudyIds || []));
    renderActions();
  }

  function setFocusedStudyId(studyId) {
    state.focusedStudyId = String(studyId || '').trim() || null;
    renderSummaryText();
    applyFocusedStudyMembershipClasses();
  }

  function handleEscapeFromSetFocus() {
    return clearFocusedSetForModeChange('setFocusEscToAll', 'setFocusEscToCheckboxes');
  }

  function bindEventsOnce() {
    if (state.bound) return;
    const {
      section,
      header,
    } = getDom();
    if (!section || !header) return;

    section.addEventListener('mousedown', (event) => {
      const target = event.target instanceof Element ? event.target : null;
      if (!target || !event.shiftKey) return;
      const row = target.closest('tr.analytics-set-row');
      if (!row) return;
      event.preventDefault();
      clearTextSelection();
    });

    header.addEventListener('click', (event) => {
      setPanelOpen(!state.panelOpen);
      render();
    });

    document.addEventListener('keydown', (event) => {
      if (state.colorMenuOpen && event.key === 'Escape') {
        event.preventDefault();
        closeColorMenu();
        renderActions();
        return;
      }

      if (state.updateMenuOpen && event.key === 'Escape') {
        event.preventDefault();
        state.updateMenuOpen = false;
        renderActions();
        return;
      }

      if (state.batchMode && event.key === 'Escape' && !state.moveMode) {
        event.preventDefault();
        exitBatchMode();
        return;
      }

      if (!state.moveMode || event.defaultPrevented) return;
      if (event.key === 'Enter') {
        event.preventDefault();
        confirmMoveMode();
        return;
      }
      if (event.key === 'ArrowUp') {
        event.preventDefault();
        moveSelectionToIndex(state.moveInsertionIndex - 1);
        return;
      }
      if (event.key === 'ArrowDown') {
        event.preventDefault();
        moveSelectionToIndex(state.moveInsertionIndex + 1);
        return;
      }
      if (event.key === 'PageUp') {
        event.preventDefault();
        moveSelectionToIndex(state.moveInsertionIndex - 3);
        return;
      }
      if (event.key === 'PageDown') {
        event.preventDefault();
        moveSelectionToIndex(state.moveInsertionIndex + 3);
        return;
      }
      if (event.key === 'Home') {
        event.preventDefault();
        moveSelectionToIndex(0);
        return;
      }
      if (event.key === 'End') {
        event.preventDefault();
        moveSelectionToIndex(Math.max(0, state.sets.length - state.moveSelectionIds.length));
      }
    });

    document.addEventListener('pointerdown', (event) => {
      const updateWrap = document.getElementById('analyticsSetUpdateWrap');
      if (state.updateMenuOpen && updateWrap && !isEventInsideElement(event, updateWrap)) {
        state.updateMenuOpen = false;
        renderActions();
      }

      const colorWrap = document.getElementById('analyticsSetColorWrap');
      if (state.colorMenuOpen && colorWrap && !isEventInsideElement(event, colorWrap)) {
        closeColorMenu();
        renderActions();
      }

      if (!state.moveMode) return;
      if (isEventInsideElement(event, section)) return;
      cancelMoveMode();
    });

    state.bound = true;
  }

  function init(options = {}) {
    state.onStateChange = typeof options.onStateChange === 'function' ? options.onStateChange : null;
    state.onCompareToggle = typeof options.onCompareToggle === 'function' ? options.onCompareToggle : null;
    state.checkedStudyIds = new Set(Array.from(options.checkedStudyIds || []));
    state.forceAllStudies = false;
    state.panelTouched = false;
    state.panelOpen = true;
    state.tableHeightLevel = TABLE_HEIGHT_MIN;
    state.updateMenuOpen = false;
    state.colorMenuOpen = false;
    state.compareMarkers = new Map();
    state.batchMode = false;
    clearBatchSelection();
    clearMoveState();
    state.rangeAnchorSetId = null;
    state.rangeAnchorChecked = null;
    if (window.AnalyticsSetsView && typeof window.AnalyticsSetsView.init === 'function') {
      window.AnalyticsSetsView.init({ onChange: handleViewControlsChange });
      if (typeof window.AnalyticsSetsView.updateSets === 'function') {
        window.AnalyticsSetsView.updateSets(state.sets, { emitChange: false });
      }
    }
    updateStudies(options.studies || []);
    bindEventsOnce();
    render();
  }

  function getVisibleStudyIds() {
    const visible = computeVisibleStudyIds();
    if (visible === null) return null;
    return new Set(Array.from(visible));
  }

  function getCheckedSetIds() {
    return cloneCheckedSetIds();
  }

  function getFocusedSetId() {
    return state.focusedSetId;
  }

  function getViewMode() {
    return state.viewMode;
  }

  function clearFocus() {
    if (state.focusedSetId === null) return;
    closeTransientMenus();
    state.focusedSetId = null;
    state.forceAllStudies = false;
    resolveViewMode();
    render();
    emitStateChange({ reason: 'setFocusCleared', syncCheckedStudyIds: null });
  }

  function setFocusedSetId(setId, options = {}) {
    if (state.batchMode) return;
    const normalized = normalizeSetId(setId);
    if (normalized === null) {
      if (state.focusedSetId === null) return;
      closeTransientMenus();
      state.focusedSetId = null;
      state.forceAllStudies = false;
      resolveViewMode();
      render();
      if (options.emitState !== false) {
        emitStateChange({ reason: 'setFocusedExternalClear', syncCheckedStudyIds: null });
      }
      return;
    }

    if (!hasSet(normalized) || !getVisibleSetIdSet().has(normalized)) return;
    closeTransientMenus();
    state.focusedSetId = normalized;
    state.forceAllStudies = false;
    resolveViewMode();
    render();

    if (options.emitState === false) return;
    const focusedSet = getSetById(normalized);
    emitStateChange({
      reason: 'setFocusedExternal',
      syncCheckedStudyIds: Array.from(new Set((focusedSet?.study_ids || []).slice())),
    });
  }

  function isMoveMode() {
    return state.moveMode;
  }

  function getSets() {
    return state.sets.map((setItem) => ({
      id: setItem.id,
      name: setItem.name,
      sort_order: setItem.sort_order,
      created_at: setItem.created_at,
      color_token: setItem.color_token,
      study_ids: setItem.study_ids.slice(),
      metrics: setItem.metrics ? { ...setItem.metrics } : null,
    }));
  }

  function getAllMetrics() {
    return state.allMetrics ? { ...state.allMetrics } : null;
  }

  function getVisibleSetIds() {
    return getVisibleOrderedSetIds().slice();
  }

  function scrollSetIntoView(setId) {
    const row = findSetRow(setId);
    if (!row) return;
    const container = row.closest('.analytics-sets-table-wrap');
    if (!(container instanceof HTMLElement)) return;

    const rowRect = row.getBoundingClientRect();
    const containerRect = container.getBoundingClientRect();
    if (rowRect.top < containerRect.top) {
      container.scrollTop -= containerRect.top - rowRect.top;
      return;
    }
    if (rowRect.bottom > containerRect.bottom) {
      container.scrollTop += rowRect.bottom - containerRect.bottom;
    }
  }

  function setCompareMarkers(compareMarkers) {
    state.compareMarkers = cloneCompareMarkers(compareMarkers);
    applyCompareMarkers();
  }

  window.AnalyticsSets = {
    init,
    updateStudies,
    updateCheckedStudyIds,
    setFocusedStudyId,
    loadSets,
    getFocusedSetId,
    getCheckedSetIds,
    getVisibleStudyIds,
    getViewMode,
    setFocusedSetId,
    clearFocus,
    handleEscapeFromSetFocus,
    cancelMoveMode,
    isMoveMode,
    getSets,
    getVisibleSetIds,
    getAllMetrics,
    scrollSetIntoView,
    setCompareMarkers,
  };
})();
