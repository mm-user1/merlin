(function () {
  const MS_PER_DAY = 24 * 60 * 60 * 1000;

  const FILTER_TO_FIELD = {
    strategy: 'strategy',
    symbol: 'symbol',
    tf: 'tf',
    wfa: 'wfa_mode',
    isOos: 'is_oos',
  };

  const SORT_META = {
    study_name: { label: 'Study Name', bestDirection: 'asc' },
    ann_profit_pct: { label: 'Ann.P%', bestDirection: 'desc' },
    profit_pct: { label: 'Profit%', bestDirection: 'desc' },
    max_dd_pct: { label: 'MaxDD%', bestDirection: 'asc' },
    total_trades: { label: 'Trades', bestDirection: 'desc' },
    wfe_pct: { label: 'WFE%', bestDirection: 'desc' },
    profitable_windows_pct: { label: 'OOS Wins', bestDirection: 'desc' },
    median_window_profit: { label: 'OOS P(med)', bestDirection: 'desc' },
    median_window_wr: { label: 'OOS WR(med)', bestDirection: 'desc' },
  };

  const tableState = {
    studies: [],
    checkedSet: new Set(),
    visibleSet: new Set(),
    visibleStudyIds: null,
    orderedStudyIds: [],
    sortState: {
      sortColumn: null,
      sortDirection: null,
      sortClickCount: 0,
    },
    filters: {
      strategy: null,
      symbol: null,
      tf: null,
      wfa: null,
      isOos: null,
    },
    autoSelect: false,
    groupDatesEnabled: true,
    collapsedGroups: new Set(),
    onSelectionChange: null,
    onSortChange: null,
    onFocusToggle: null,
    onViewChange: null,
    rangeAnchorStudyId: null,
    rangeAnchorChecked: null,
    focusedStudyId: null,
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

  function parseTimestampMs(value) {
    if (!value) return null;
    const parsed = Date.parse(value);
    return Number.isFinite(parsed) ? parsed : null;
  }

  function parseDateFlexible(value) {
    const token = String(value || '').trim();
    if (!token) return null;
    const normalized = token.replace(/\./g, '-');
    const parsed = Date.parse(`${normalized}T00:00:00Z`);
    if (!Number.isFinite(parsed)) return null;
    return new Date(parsed);
  }

  function periodDays(start, end) {
    const startDate = parseDateFlexible(start);
    const endDate = parseDateFlexible(end);
    if (!startDate || !endDate) return null;
    const diff = Math.floor((endDate.getTime() - startDate.getTime()) / (24 * 60 * 60 * 1000));
    return Math.max(0, diff);
  }

  function normalizeTfToken(value) {
    const token = String(value || '').trim();
    if (!token) return '';

    const numeric = token.match(/^(\d+)$/);
    if (numeric) {
      const minutes = Number(numeric[1]);
      if (!Number.isFinite(minutes)) return token;
      if (minutes >= 1440 && minutes % 1440 === 0) return `${minutes / 1440}D`;
      if (minutes >= 60 && minutes % 60 === 0) return `${minutes / 60}h`;
      return `${minutes}m`;
    }

    const lower = token.toLowerCase();
    const withM = lower.match(/^(\d+)m$/);
    if (withM) {
      const minutes = Number(withM[1]);
      if (minutes >= 1440 && minutes % 1440 === 0) return `${minutes / 1440}D`;
      if (minutes >= 60 && minutes % 60 === 0) return `${minutes / 60}h`;
      return `${minutes}m`;
    }

    if (/^\d+h$/i.test(token)) return token.toLowerCase();
    if (/^\d+d$/i.test(token)) return `${token.slice(0, -1)}D`;
    if (/^\d+w$/i.test(token)) return token.toLowerCase();
    return token;
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

  function extractCounterSuffix(value) {
    const text = String(value || '').trim();
    const match = text.match(/\s\((\d+)\)\s*$/);
    if (!match || match.index === undefined) {
      return { base: text, counter: null };
    }
    return {
      base: text.slice(0, match.index).trim(),
      counter: Number(match[1]),
    };
  }

  function parseStudyNameDisplayIdentity(value) {
    const text = String(value || '').trim();
    if (!text) return { symbol: '', tf: '' };

    const commaIndex = text.lastIndexOf(',');
    if (commaIndex >= 0) {
      const symbol = text.slice(0, commaIndex).trim();
      const tfToken = text.slice(commaIndex + 1).trim().split(/\s+/)[0] || '';
      return { symbol, tf: normalizeTfToken(tfToken) };
    }

    const parts = text.split(/\s+/).filter(Boolean);
    if (parts.length > 1) {
      const tfToken = parts[parts.length - 1];
      const symbol = parts.slice(0, -1).join(' ');
      return { symbol, tf: normalizeTfToken(tfToken) };
    }

    return { symbol: text, tf: '' };
  }

  function buildStudyNameSortIdentity(study, studyNameDisplay) {
    const displayName = String(studyNameDisplay || '').trim();
    const counterInfo = extractCounterSuffix(displayName);
    const parsedDisplay = parseStudyNameDisplayIdentity(counterInfo.base);
    const symbol = String(study?.symbol || '').trim() || parsedDisplay.symbol;
    const tfToken = normalizeTfToken(study?.tf || '') || parsedDisplay.tf;
    const timeframeRaw = String(tfToken || '').trim().toLowerCase();
    const counter = Number.isFinite(counterInfo.counter) ? counterInfo.counter : null;

    return {
      ticker: symbol.toLowerCase(),
      timeframeRaw,
      timeframeMinutes: parseTimeframeToMinutes(timeframeRaw),
      counterRank: counter === null ? 0 : 1,
      counter,
      displayName,
    };
  }

  function fallbackStudyName(study) {
    const symbol = String(study?.symbol || '').trim();
    const tf = normalizeTfToken(study?.tf || '');
    if (symbol && tf) return `${symbol}, ${tf}`;
    if (symbol) return symbol;
    if (tf) return tf;
    return 'Unknown';
  }

  function buildDisplayStudyName(study) {
    const rawName = String(study?.study_name || '').trim();
    if (!rawName) return fallbackStudyName(study);

    let working = rawName;
    let counterSuffix = '';

    const counterMatch = working.match(/\s\((\d+)\)\s*$/);
    if (counterMatch) {
      counterSuffix = ` (${counterMatch[1]})`;
      working = working.slice(0, counterMatch.index).trim();
    }

    working = working.replace(/_(WFA|OPT)\s*$/i, '').trim();
    working = working.replace(
      /\s+\d{4}[.\-/]\d{2}[.\-/]\d{2}\s*-\s*\d{4}[.\-/]\d{2}[.\-/]\d{2}\s*$/i,
      ''
    ).trim();
    working = working.replace(/^S\d{2}_/i, '').trim();

    const commaIndex = working.lastIndexOf(',');
    if (commaIndex >= 0) {
      const left = working.slice(0, commaIndex + 1);
      const right = working.slice(commaIndex + 1).trim();
      if (right) {
        working = `${left} ${normalizeTfToken(right)}`.replace(/\s+/g, ' ').trim();
      }
    }

    if (!working) {
      working = fallbackStudyName(study);
    }
    return `${working}${counterSuffix}`.trim();
  }

  function withDerivedFields(study) {
    const createdEpoch = toFiniteNumber(study?.created_at_epoch);
    const completedEpoch = toFiniteNumber(study?.completed_at_epoch);
    const createdMs = createdEpoch === null ? parseTimestampMs(study?.created_at) : createdEpoch * 1000;
    const completedMs = completedEpoch === null ? parseTimestampMs(study?.completed_at) : completedEpoch * 1000;
    const studyNameDisplay = buildDisplayStudyName(study);
    const annMetrics = computeAnnualizedProfitMetrics(study);
    return {
      ...study,
      _study_name_display: studyNameDisplay,
      _study_name_sort: buildStudyNameSortIdentity(study, studyNameDisplay),
      _created_ms: createdMs,
      _completed_ms: completedMs,
      _default_order_ms: createdMs === null ? completedMs : createdMs,
      ann_profit_pct: annMetrics.annProfitPct,
      _oos_span_days: annMetrics.oosSpanDays,
    };
  }

  function compareDefaultRows(leftStudy, rightStudy) {
    const leftTs = leftStudy?._default_order_ms;
    const rightTs = rightStudy?._default_order_ms;

    if (leftTs === null && rightTs !== null) return 1;
    if (leftTs !== null && rightTs === null) return -1;
    if (leftTs !== null && rightTs !== null && leftTs !== rightTs) return rightTs - leftTs;

    const leftId = String(leftStudy?.study_id || '');
    const rightId = String(rightStudy?.study_id || '');
    return rightId.localeCompare(leftId, undefined, { numeric: true, sensitivity: 'base' });
  }

  function compareNumbersWithNulls(leftValue, rightValue, direction) {
    const left = toFiniteNumber(leftValue);
    const right = toFiniteNumber(rightValue);
    if (left === null && right === null) return 0;
    if (left === null) return 1;
    if (right === null) return -1;
    if (left === right) return 0;
    return direction === 'asc' ? left - right : right - left;
  }

  function compareStudyNameRows(leftStudy, rightStudy) {
    const left = leftStudy?._study_name_sort || buildStudyNameSortIdentity(leftStudy, leftStudy?._study_name_display);
    const right = rightStudy?._study_name_sort || buildStudyNameSortIdentity(rightStudy, rightStudy?._study_name_display);

    const tickerCmp = String(left.ticker || '').localeCompare(String(right.ticker || ''), undefined, {
      sensitivity: 'base',
    });
    if (tickerCmp !== 0) return tickerCmp;

    const leftFinite = Number.isFinite(left.timeframeMinutes);
    const rightFinite = Number.isFinite(right.timeframeMinutes);
    if (leftFinite && rightFinite && left.timeframeMinutes !== right.timeframeMinutes) {
      return left.timeframeMinutes - right.timeframeMinutes;
    }
    if (leftFinite !== rightFinite) return leftFinite ? -1 : 1;

    const timeframeCmp = String(left.timeframeRaw || '').localeCompare(String(right.timeframeRaw || ''), undefined, {
      numeric: true,
      sensitivity: 'base',
    });
    if (timeframeCmp !== 0) return timeframeCmp;

    if (left.counterRank !== right.counterRank) {
      return left.counterRank - right.counterRank;
    }
    if (left.counterRank === 1 && right.counterRank === 1 && left.counter !== right.counter) {
      return Number(left.counter) - Number(right.counter);
    }

    return String(left.displayName || '').localeCompare(String(right.displayName || ''), undefined, {
      numeric: true,
      sensitivity: 'base',
    });
  }

  function compareBySortColumn(leftStudy, rightStudy, sortColumn, sortDirection) {
    if (!sortColumn || !sortDirection) {
      return compareDefaultRows(leftStudy, rightStudy);
    }

    if (sortColumn === 'study_name') {
      const cmp = compareStudyNameRows(leftStudy, rightStudy);
      if (cmp !== 0) return sortDirection === 'asc' ? cmp : -cmp;
      return compareDefaultRows(leftStudy, rightStudy);
    }

    const cmp = compareNumbersWithNulls(leftStudy?.[sortColumn], rightStudy?.[sortColumn], sortDirection);
    if (cmp !== 0) return cmp;
    return compareDefaultRows(leftStudy, rightStudy);
  }

  function normalizeFilters(filters) {
    const next = {};
    Object.keys(FILTER_TO_FIELD).forEach((key) => {
      const current = filters?.[key];
      next[key] = current instanceof Set ? new Set(current) : null;
    });
    return next;
  }

  function normalizeSortState(sortState) {
    const sortColumn = sortState?.sortColumn ?? null;
    const sortDirection = sortState?.sortDirection ?? null;
    const clickCount = Number(sortState?.sortClickCount || 0);
    if (!sortColumn || !sortDirection || clickCount <= 0) {
      return {
        sortColumn: null,
        sortDirection: null,
        sortClickCount: 0,
      };
    }
    return {
      sortColumn,
      sortDirection,
      sortClickCount: clickCount,
    };
  }

  function normalizeVisibleStudyIds(visibleStudyIds) {
    if (!(visibleStudyIds instanceof Set)) return null;
    const normalized = new Set();
    visibleStudyIds.forEach((studyId) => {
      const value = String(studyId || '').trim();
      if (value) normalized.add(value);
    });
    return normalized;
  }

  function cloneSortState() {
    return {
      sortColumn: tableState.sortState.sortColumn,
      sortDirection: tableState.sortState.sortDirection,
      sortClickCount: tableState.sortState.sortClickCount,
    };
  }

  function setsEqual(leftSet, rightSet) {
    if (leftSet.size !== rightSet.size) return false;
    for (const value of leftSet) {
      if (!rightSet.has(value)) return false;
    }
    return true;
  }

  function matchesFilters(study) {
    return Object.keys(FILTER_TO_FIELD).every((filterKey) => {
      const selected = tableState.filters[filterKey];
      if (!(selected instanceof Set)) return true;
      const field = FILTER_TO_FIELD[filterKey];
      const value = String(study?.[field] ?? '').trim();
      return selected.has(value);
    });
  }

  function getTableElements() {
    const table = document.getElementById('analyticsSummaryTable');
    const tbody = document.getElementById('analyticsSummaryTableBody');
    const headerCheck = document.getElementById('analyticsHeaderCheck');
    return { table, tbody, headerCheck };
  }

  function getRowCheckboxes() {
    const { table } = getTableElements();
    if (!table) return [];
    return Array.from(table.querySelectorAll('tbody .analytics-row-check'));
  }

  function getStudyRows() {
    const { table } = getTableElements();
    if (!table) return [];
    return Array.from(table.querySelectorAll('tbody tr.analytics-study-row'));
  }

  function findStudyRow(studyId) {
    const normalized = String(studyId || '').trim();
    if (!normalized) return null;
    return getStudyRows().find((row) => decodeStudyId(row.dataset.studyId || '') === normalized) || null;
  }

  function findStudyRecord(studyId) {
    const normalized = String(studyId || '').trim();
    if (!normalized) return null;
    return tableState.studies.find((study) => String(study?.study_id || '') === normalized) || null;
  }

  function encodeStudyId(studyId) {
    return encodeURIComponent(String(studyId || ''));
  }

  function buildGroupId(start, end) {
    return encodeURIComponent(`${String(start || '').trim()}||${String(end || '').trim()}`);
  }

  function getStudyGroupId(study) {
    return buildGroupId(study?.dataset_start_date, study?.dataset_end_date);
  }

  function isElementVisible(element) {
    if (!element) return false;
    return element.style.display !== 'none';
  }

  function isStudyRowEligible(row) {
    return row instanceof HTMLTableRowElement && row.dataset.eligible === '1';
  }

  function isStudyRowVisible(row) {
    return isStudyRowEligible(row) && row.dataset.visible === '1' && isElementVisible(row);
  }

  function getEligibleRowCheckboxes() {
    return getRowCheckboxes().filter((checkbox) => isStudyRowEligible(checkbox.closest('tr.analytics-study-row')));
  }

  function getVisibleRowCheckboxes() {
    return getRowCheckboxes().filter((checkbox) => isStudyRowVisible(checkbox.closest('tr.analytics-study-row')));
  }

  function getGroupCheckboxes() {
    const { table } = getTableElements();
    if (!table) return [];
    return Array.from(table.querySelectorAll('tbody .analytics-group-check'));
  }

  function decodeStudyId(encodedId) {
    const value = String(encodedId || '');
    if (!value) return '';
    try {
      return decodeURIComponent(value);
    } catch (_error) {
      return value;
    }
  }

  function getCheckedStudyIds() {
    return getRowCheckboxes()
      .filter((checkbox) => checkbox.checked)
      .map((checkbox) => decodeStudyId(checkbox.dataset.studyId || ''))
      .filter(Boolean);
  }

  function getEligibleRowCheckboxesForGroup(groupId) {
    return getEligibleRowCheckboxes().filter((checkbox) => checkbox.dataset.group === groupId);
  }

  function rememberRangeAnchor(studyId, checked) {
    if (!studyId || typeof checked !== 'boolean') return;
    tableState.rangeAnchorStudyId = studyId;
    tableState.rangeAnchorChecked = checked;
  }

  function commitSelectionState() {
    syncHierarchyCheckboxes();
    tableState.checkedSet = new Set(getCheckedStudyIds());
    applyFocusedRowClass();
    notifySelectionChanged();
  }

  function setVisibleChecked(checked) {
    const next = Boolean(checked);
    getVisibleRowCheckboxes().forEach((checkbox) => {
      checkbox.checked = next;
    });
  }

  function applyRangeSelection(anchorStudyId, targetStudyId, checked) {
    if (!anchorStudyId || !targetStudyId || typeof checked !== 'boolean') return false;

    const visibleCheckboxes = getVisibleRowCheckboxes();
    const visibleIds = visibleCheckboxes.map((checkbox) => decodeStudyId(checkbox.dataset.studyId || ''));
    const anchorIndex = visibleIds.indexOf(anchorStudyId);
    const targetIndex = visibleIds.indexOf(targetStudyId);

    if (anchorIndex < 0 || targetIndex < 0) return false;

    const [start, end] = anchorIndex <= targetIndex
      ? [anchorIndex, targetIndex]
      : [targetIndex, anchorIndex];

    for (let index = start; index <= end; index += 1) {
      visibleCheckboxes[index].checked = checked;
    }
    return true;
  }

  function handleStudyRowToggle(row, shiftKey) {
    if (!(row instanceof HTMLTableRowElement)) return;
    const checkbox = row.querySelector('.analytics-row-check');
    if (!(checkbox instanceof HTMLInputElement) || checkbox.type !== 'checkbox') return;

    const studyId = decodeStudyId(checkbox.dataset.studyId || '');
    if (!studyId) return;
    const wasChecked = tableState.checkedSet.has(studyId);

    const hasAnchor = Boolean(tableState.rangeAnchorStudyId) && typeof tableState.rangeAnchorChecked === 'boolean';
    if (shiftKey && hasAnchor) {
      const applied = applyRangeSelection(
        tableState.rangeAnchorStudyId,
        studyId,
        tableState.rangeAnchorChecked
      );
      if (!applied) {
        const nextChecked = !wasChecked;
        checkbox.checked = nextChecked;
        rememberRangeAnchor(studyId, nextChecked);
      }
    } else {
      const nextChecked = !wasChecked;
      checkbox.checked = nextChecked;
      rememberRangeAnchor(studyId, nextChecked);
    }

    commitSelectionState();
  }

  function handleRowCheckboxChange(checkbox) {
    if (!(checkbox instanceof HTMLInputElement) || checkbox.type !== 'checkbox') return;
    const studyId = decodeStudyId(checkbox.dataset.studyId || '');
    if (studyId) {
      rememberRangeAnchor(studyId, Boolean(checkbox.checked));
    }
    commitSelectionState();
  }

  function clearTextSelection() {
    const selection = window.getSelection ? window.getSelection() : null;
    if (!selection) return;
    if (selection.type === 'Range') {
      selection.removeAllRanges();
    }
  }

  function updateRowSelectionClasses() {
    getStudyRows().forEach((row) => {
      row.classList.remove('selected');
    });
  }

  function updateGroupVisibility() {
    getGroupCheckboxes().forEach((groupCheckbox) => {
      const group = groupCheckbox.dataset.group || '';
      const groupRow = groupCheckbox.closest('tr.analytics-group-row');
      if (groupRow) {
        groupRow.style.display = getEligibleRowCheckboxesForGroup(group).length ? '' : 'none';
      }
    });
  }

  function renumberVisibleRows() {
    let counter = 1;
    getStudyRows().forEach((row) => {
      const numberCell = row.querySelector('.analytics-row-number');
      if (!numberCell) return;
      if (isElementVisible(row)) {
        numberCell.textContent = String(counter);
        counter += 1;
      } else {
        numberCell.textContent = '';
      }
    });
  }

  function syncHierarchyCheckboxes() {
    const { headerCheck } = getTableElements();

    getGroupCheckboxes().forEach((groupCheckbox) => {
      const groupKey = groupCheckbox.dataset.group || '';
      const children = getEligibleRowCheckboxesForGroup(groupKey);

      if (!children.length) {
        groupCheckbox.checked = false;
        groupCheckbox.indeterminate = false;
        return;
      }

      const checkedCount = children.filter((checkbox) => checkbox.checked).length;
      groupCheckbox.checked = checkedCount === children.length;
      groupCheckbox.indeterminate = checkedCount > 0 && checkedCount < children.length;
    });

    if (headerCheck) {
      const visibleRows = getVisibleRowCheckboxes();
      const checkedCount = visibleRows.filter((checkbox) => checkbox.checked).length;
      headerCheck.checked = visibleRows.length > 0 && checkedCount === visibleRows.length;
      headerCheck.indeterminate = checkedCount > 0 && checkedCount < visibleRows.length;
    }

    updateRowSelectionClasses();
  }

  function applyFocusedRowClass() {
    const focused = String(tableState.focusedStudyId || '');
    getStudyRows().forEach((row) => {
      row.classList.remove('analytics-focused');
      if (!focused) return;
      const rowStudyId = decodeStudyId(row.dataset.studyId || '');
      if (rowStudyId === focused) {
        row.classList.add('analytics-focused');
      }
    });
  }

  function notifySelectionChanged() {
    if (typeof tableState.onSelectionChange !== 'function') return;
    tableState.onSelectionChange(new Set(getCheckedStudyIds()));
  }

  function notifySortChanged() {
    if (typeof tableState.onSortChange !== 'function') return;
    tableState.onSortChange(cloneSortState());
  }

  function notifyViewChanged() {
    if (typeof tableState.onViewChange !== 'function') return;
    tableState.onViewChange(tableState.focusedStudyId);
  }

  function updateSortHeaders() {
    const { table } = getTableElements();
    if (!table) return;

    const activeColumn = tableState.sortState.sortColumn;
    const activeDirection = tableState.sortState.sortDirection;

    Array.from(table.querySelectorAll('thead th.analytics-sortable')).forEach((header) => {
      const key = header.dataset.sortKey || '';
      const arrow = header.querySelector('.sort-arrow');
      const active = key && key === activeColumn && activeDirection;

      header.classList.toggle('sort-active', Boolean(active));
      header.classList.toggle('sort-asc', active && activeDirection === 'asc');
      header.classList.toggle('sort-desc', active && activeDirection === 'desc');
      if (arrow) {
        arrow.textContent = active ? (activeDirection === 'asc' ? '▲' : '▼') : '↕';
      }
    });
  }

  function cycleSortForColumn(sortKey) {
    const current = tableState.sortState;

    if (current.sortColumn !== sortKey || !current.sortColumn) {
      const bestDirection = SORT_META[sortKey]?.bestDirection || 'desc';
      tableState.sortState = {
        sortColumn: sortKey,
        sortDirection: bestDirection,
        sortClickCount: 1,
      };
      return;
    }

    if (current.sortClickCount === 1) {
      tableState.sortState = {
        sortColumn: sortKey,
        sortDirection: current.sortDirection === 'asc' ? 'desc' : 'asc',
        sortClickCount: 2,
      };
      return;
    }

    tableState.sortState = {
      sortColumn: null,
      sortDirection: null,
      sortClickCount: 0,
    };
  }

  function isStudyEligible(study) {
    const studyId = String(study?.study_id || '');
    const visibleBySet = !(tableState.visibleStudyIds instanceof Set) || tableState.visibleStudyIds.has(studyId);
    return visibleBySet && matchesFilters(study);
  }

  function buildGroupedStudies(preparedStudies) {
    const groupsMap = new Map();
    preparedStudies.forEach((study) => {
      const start = String(study.dataset_start_date || '').trim();
      const end = String(study.dataset_end_date || '').trim();
      const groupId = getStudyGroupId(study);
      if (!groupsMap.has(groupId)) {
        groupsMap.set(groupId, {
          groupId,
          start,
          end,
          key: `${start}||${end}`,
          studies: [],
        });
      }
      groupsMap.get(groupId).studies.push(study);
    });

    const groups = Array.from(groupsMap.values());
    const availableGroupIds = new Set(groups.map((group) => group.groupId));
    tableState.collapsedGroups = new Set(
      Array.from(tableState.collapsedGroups).filter((groupId) => availableGroupIds.has(groupId))
    );

    groups.forEach((group) => {
      group.defaultSortedStudies = group.studies.slice().sort(compareDefaultRows);
      group.newestStudy = group.defaultSortedStudies[0] || null;
    });

    groups.sort((left, right) => {
      const baseCmp = compareDefaultRows(left.newestStudy, right.newestStudy);
      if (baseCmp !== 0) return baseCmp;
      return String(left.key).localeCompare(String(right.key), undefined, {
        numeric: true,
        sensitivity: 'base',
      });
    });

    const sortColumn = tableState.sortState.sortColumn;
    const sortDirection = tableState.sortState.sortDirection;
    groups.forEach((group) => {
      const orderedStudies = sortColumn
        ? group.studies.slice().sort((left, right) => compareBySortColumn(left, right, sortColumn, sortDirection))
        : group.defaultSortedStudies.slice();

      group.isCollapsed = tableState.collapsedGroups.has(group.groupId);
      group.orderedStudies = orderedStudies;
      group.eligibleStudies = orderedStudies.filter((study) => isStudyEligible(study));
      group.eligibleStudyIds = new Set(group.eligibleStudies.map((study) => String(study.study_id || '')));
    });

    return groups;
  }

  function buildFlatStudies(preparedStudies) {
    const sortColumn = tableState.sortState.sortColumn;
    const sortDirection = tableState.sortState.sortDirection;
    const orderedStudies = sortColumn
      ? preparedStudies.slice().sort((left, right) => compareBySortColumn(left, right, sortColumn, sortDirection))
      : preparedStudies.slice().sort(compareDefaultRows);

    return orderedStudies.map((study) => ({
      study,
      groupId: getStudyGroupId(study),
      eligible: isStudyEligible(study),
    }));
  }

  function buildStudyRowHtml(study, groupId, eligible, visible, nextChecked) {
    const studyId = String(study.study_id || '');
    const encodedStudyId = encodeStudyId(studyId);
    const checked = tableState.autoSelect ? eligible : tableState.checkedSet.has(studyId);
    const styleHidden = visible ? '' : ' style="display:none;"';

    if (checked) nextChecked.add(studyId);
    if (visible) {
      tableState.visibleSet.add(studyId);
    }
    tableState.orderedStudyIds.push(studyId);

    const profitText = escapeHtml(formatSignedPercentValue(study.profit_pct, 1));
    const maxDdText = escapeHtml(formatNegativePercentValue(study.max_dd_pct, 1));
    const annProfitCell = formatAnnualizedProfitCell(study);
    const annProfitText = escapeHtml(annProfitCell.text);
    const annProfitTitleAttr = annProfitCell.tooltip
      ? ` title="${escapeHtml(annProfitCell.tooltip)}"`
      : '';
    const wfeRaw = toFiniteNumber(study.wfe_pct);
    const wfeText = escapeHtml(wfeRaw === null ? 'N/A' : `${wfeRaw.toFixed(1)}%`);
    const oosProfitText = escapeHtml(formatSignedPercentValue(study.median_window_profit, 1));
    const oosWrRaw = toFiniteNumber(study.median_window_wr);
    const oosWrText = escapeHtml(oosWrRaw === null ? 'N/A' : `${oosWrRaw.toFixed(1)}%`);

    const profitClass = (toFiniteNumber(study.profit_pct) || 0) >= 0 ? 'val-positive' : 'val-negative';
    const maxDdValue = toFiniteNumber(study.max_dd_pct);
    const maxDdClass = maxDdValue !== null && Math.abs(maxDdValue) > 40 ? 'val-negative' : '';
    const oosProfitClass = (toFiniteNumber(study.median_window_profit) || 0) >= 0 ? 'val-positive' : 'val-negative';

    const strategyText = escapeHtml(study.strategy || 'Unknown');
    const studyNameText = escapeHtml(study._study_name_display || fallbackStudyName(study));
    const studyNameTitle = escapeHtml(study.study_name || '');
    const wfaModeText = escapeHtml(study.wfa_mode || 'Unknown');
    const isOosText = escapeHtml(study.is_oos || 'N/A');
    const totalTradesText = escapeHtml(formatInteger(study.total_trades, '0'));
    const oosWinsText = escapeHtml(formatOosWins(study));

    return `
      <tr
        class="clickable analytics-study-row"
        data-group="${groupId}"
        data-study-id="${encodedStudyId}"
        data-eligible="${eligible ? '1' : '0'}"
        data-visible="${visible ? '1' : '0'}"${styleHidden}
      >
        <td class="col-check">
          <input
            type="checkbox"
            class="analytics-row-check"
            data-group="${groupId}"
            data-study-id="${encodedStudyId}"
            ${checked ? 'checked' : ''}
          />
        </td>
        <td class="analytics-row-number"></td>
        <td>${strategyText}</td>
        <td title="${studyNameTitle}">${studyNameText}</td>
        <td>${wfaModeText}</td>
        <td>${isOosText}</td>
        <td class="${annProfitCell.className || ''}"${annProfitTitleAttr}>${annProfitText}</td>
        <td class="${profitClass}">${profitText}</td>
        <td class="${maxDdClass}">${maxDdText}</td>
        <td>${totalTradesText}</td>
        <td>${wfeText}</td>
        <td>${oosWinsText}</td>
        <td class="${oosProfitClass}">${oosProfitText}</td>
        <td>${oosWrText}</td>
      </tr>
    `;
  }

  function buildGroupRowHtml(group) {
    const days = periodDays(group.start, group.end);
    const daysText = days === null ? '?' : String(days);
    const groupStart = escapeHtml(group.start || 'Unknown');
    const groupEnd = escapeHtml(group.end || 'Unknown');
    const toggleIcon = group.isCollapsed ? '&#9656;' : '&#9662;';
    const groupHiddenStyle = group.eligibleStudies.length ? '' : ' style="display:none;"';
    const collapsedClass = group.isCollapsed ? ' collapsed' : '';

    return `
      <tr
        class="group-row analytics-group-row${collapsedClass}"
        data-group="${group.groupId}"
        aria-expanded="${group.isCollapsed ? 'false' : 'true'}"${groupHiddenStyle}
      >
        <td class="col-check"><input type="checkbox" class="analytics-group-check" data-group="${group.groupId}" /></td>
        <td colspan="13">
          <div class="group-label">
            <span class="group-toggle-icon" aria-hidden="true">${toggleIcon}</span>
            <span class="group-dates">${groupStart} &mdash; ${groupEnd}</span>
            <span class="group-duration">(${daysText} days)</span>
            <span class="group-count">${group.eligibleStudies.length} studies</span>
          </div>
        </td>
      </tr>
    `;
  }

  function renderTableBody() {
    const { tbody } = getTableElements();
    if (!tbody) return;

    const beforeChecked = new Set(tableState.checkedSet);
    const preparedStudies = tableState.studies.map(withDerivedFields);
    tableState.orderedStudyIds = [];
    tableState.visibleSet = new Set();

    if (!preparedStudies.length) {
      tbody.innerHTML = `
        <tr>
          <td colspan="14" class="analytics-empty-cell">No WFA studies found in this database.</td>
        </tr>
      `;
      updateSortHeaders();
      return;
    }

    const html = [];
    const nextChecked = new Set();

    if (tableState.groupDatesEnabled) {
      const groups = buildGroupedStudies(preparedStudies);
      groups.forEach((group) => {
        html.push(buildGroupRowHtml(group));
        group.orderedStudies.forEach((study) => {
          const eligible = group.eligibleStudyIds.has(String(study.study_id || ''));
          const visible = eligible && !group.isCollapsed;
          html.push(buildStudyRowHtml(study, group.groupId, eligible, visible, nextChecked));
        });
      });
    } else {
      buildFlatStudies(preparedStudies).forEach((entry) => {
        html.push(buildStudyRowHtml(entry.study, entry.groupId, entry.eligible, entry.eligible, nextChecked));
      });
    }

    tbody.innerHTML = html.join('');

    tableState.checkedSet = nextChecked;
    updateGroupVisibility();
    renumberVisibleRows();
    syncHierarchyCheckboxes();
    updateSortHeaders();
    applyFocusedRowClass();

    const afterChecked = new Set(getCheckedStudyIds());
    tableState.checkedSet = afterChecked;
    if (!setsEqual(beforeChecked, afterChecked)) {
      notifySelectionChanged();
    }
  }

  function formatSignedPercentValue(value, digits) {
    const parsed = toFiniteNumber(value);
    if (parsed === null) return 'N/A';
    if (parsed === 0) return `0.${'0'.repeat(digits)}%`;
    const sign = parsed > 0 ? '+' : '-';
    return `${sign}${Math.abs(parsed).toFixed(digits)}%`;
  }

  function formatNegativePercentValue(value, digits) {
    const parsed = toFiniteNumber(value);
    if (parsed === null) return 'N/A';
    return `-${Math.abs(parsed).toFixed(digits)}%`;
  }

  function formatInteger(value, fallback) {
    const parsed = toFiniteNumber(value);
    if (parsed === null) return fallback;
    return String(Math.max(0, Math.round(parsed)));
  }

  function formatOosWins(study) {
    const profitable = Math.max(0, Math.round(toFiniteNumber(study.profitable_windows) || 0));
    const total = Math.max(0, Math.round(toFiniteNumber(study.total_windows) || 0));
    const pct = toFiniteNumber(study.profitable_windows_pct);
    if (total <= 0) {
      const pctText = pct === null ? 0 : Math.round(pct);
      return `0/0 (${pctText}%)`;
    }
    const bounded = Math.min(profitable, total);
    const computedPct = Math.round((bounded / total) * 100);
    return `${bounded}/${total} (${computedPct}%)`;
  }

  function bindEventsOnce() {
    if (tableState.bound) return;
    const { table } = getTableElements();
    if (!table) return;

    table.addEventListener('mousedown', (event) => {
      const target = event.target instanceof Element ? event.target : null;
      if (!target) return;
      const row = target.closest('tr.analytics-study-row');
      if (!row || !event.shiftKey) return;
      event.preventDefault();
      clearTextSelection();
    });

    table.addEventListener('change', (event) => {
      const target = event.target;
      if (!(target instanceof HTMLInputElement) || target.type !== 'checkbox') return;

      if (target.id === 'analyticsHeaderCheck') {
        getVisibleRowCheckboxes().forEach((checkbox) => {
          checkbox.checked = target.checked;
        });
      } else if (target.classList.contains('analytics-group-check')) {
        const group = target.dataset.group || '';
        getEligibleRowCheckboxesForGroup(group)
          .forEach((checkbox) => {
            checkbox.checked = target.checked;
          });
      } else if (target.classList.contains('analytics-row-check')) {
        handleRowCheckboxChange(target);
        return;
      }

      commitSelectionState();
    });

    table.addEventListener('click', (event) => {
      const target = event.target instanceof Element ? event.target : null;
      if (!target) return;

      const header = target.closest('th.analytics-sortable');
      if (header) {
        const sortKey = header.dataset.sortKey || '';
        if (!SORT_META[sortKey]) return;

        tableState.checkedSet = new Set(getCheckedStudyIds());
        cycleSortForColumn(sortKey);
        renderTableBody();
        notifySortChanged();
        return;
      }

      const groupRow = target.closest('tr.analytics-group-row');
      if (groupRow) {
        if (target.closest('input.analytics-group-check')) {
          return;
        }

        if (!tableState.groupDatesEnabled) return;

        const groupId = String(groupRow.dataset.group || '').trim();
        if (!groupId || !getEligibleRowCheckboxesForGroup(groupId).length) return;

        const nextCollapsed = !tableState.collapsedGroups.has(groupId);
        if (nextCollapsed) {
          tableState.collapsedGroups.add(groupId);
          const focusedStudy = findStudyRecord(tableState.focusedStudyId);
          if (focusedStudy && getStudyGroupId(focusedStudy) === groupId) {
            tableState.focusedStudyId = null;
          }
        } else {
          tableState.collapsedGroups.delete(groupId);
        }

        renderTableBody();
        notifyViewChanged();
        return;
      }

      const row = target.closest('tr.analytics-study-row');
      if (!row) return;

      const rowCheckbox = row.querySelector('.analytics-row-check');
      if (!(rowCheckbox instanceof HTMLInputElement) || rowCheckbox.type !== 'checkbox') return;
      const studyId = decodeStudyId(rowCheckbox.dataset.studyId || '');
      const clickedRowCheckbox = Boolean(target.closest('input.analytics-row-check'));

      if (event.altKey) {
        event.preventDefault();
        if (studyId && typeof tableState.onFocusToggle === 'function') {
          tableState.onFocusToggle(studyId);
        }
        return;
      }

      if (clickedRowCheckbox && !event.ctrlKey && !event.shiftKey) {
        // Allow native checkbox toggle; change handler will sync state.
        return;
      }

      if (event.ctrlKey) {
        if (clickedRowCheckbox) {
          event.preventDefault();
        }

        const nextChecked = !tableState.checkedSet.has(studyId);
        setVisibleChecked(nextChecked);
        if (studyId) {
          rememberRangeAnchor(studyId, nextChecked);
        }
        commitSelectionState();
        return;
      }

      if (clickedRowCheckbox) {
        // Keep checkbox clicks and row clicks on the same deterministic toggle path.
        event.preventDefault();
      }
      handleStudyRowToggle(row, Boolean(event.shiftKey));
      if (event.shiftKey) {
        clearTextSelection();
      }
    });

    tableState.bound = true;
  }

  function renderTable(studies, checkedStudyIds, onSelectionChange, options) {
    const opts = options || {};

    tableState.studies = Array.isArray(studies) ? studies.slice() : [];
    tableState.checkedSet = new Set(Array.from(checkedStudyIds || []));
    tableState.onSelectionChange = onSelectionChange;
    tableState.onSortChange = typeof opts.onSortChange === 'function' ? opts.onSortChange : null;
    tableState.onFocusToggle = typeof opts.onFocusToggle === 'function' ? opts.onFocusToggle : null;
    tableState.onViewChange = typeof opts.onViewChange === 'function' ? opts.onViewChange : null;
    tableState.filters = normalizeFilters(opts.filters);
    tableState.visibleStudyIds = normalizeVisibleStudyIds(opts.visibleStudyIds);
    tableState.autoSelect = Boolean(opts.autoSelect);
    tableState.groupDatesEnabled = opts.groupDatesEnabled !== false;
    tableState.sortState = normalizeSortState(opts.sortState);
    tableState.focusedStudyId = String(opts.focusedStudyId || '') || null;

    bindEventsOnce();
    renderTableBody();
  }

  function setAllChecked(checked) {
    getRowCheckboxes().forEach((checkbox) => {
      checkbox.checked = Boolean(checked);
    });
    syncHierarchyCheckboxes();
    tableState.checkedSet = new Set(getCheckedStudyIds());
    notifySelectionChanged();
  }

  function getOrderedStudyIds() {
    return tableState.orderedStudyIds.slice();
  }

  function getSortState() {
    return cloneSortState();
  }

  function setFocusedStudyId(studyId) {
    const normalized = String(studyId || '').trim();
    tableState.focusedStudyId = normalized || null;
    applyFocusedRowClass();
  }

  function getVisibleStudyIds() {
    return Array.from(tableState.visibleSet);
  }

  function scrollStudyIntoView(studyId) {
    const row = findStudyRow(studyId);
    if (!row || !isElementVisible(row)) return;
    const container = row.closest('.analytics-table-scroll');
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

  function computeAnnualizedProfitMetrics(study) {
    let oosSpanDays = toFiniteNumber(study?.oos_span_days_exact);

    if (oosSpanDays === null) {
      const firstMs = parseTimestampMs(study?.equity_start_ts);
      const lastMs = parseTimestampMs(study?.equity_end_ts);
      if (firstMs !== null && lastMs !== null) {
        oosSpanDays = (lastMs - firstMs) / MS_PER_DAY;
      }
    }

    if (oosSpanDays === null) {
      const timestamps = Array.isArray(study?.equity_timestamps) ? study.equity_timestamps : [];
      if (timestamps.length >= 2) {
        const firstMs = parseTimestampMs(timestamps[0]);
        const lastMs = parseTimestampMs(timestamps[timestamps.length - 1]);
        if (firstMs !== null && lastMs !== null) {
          oosSpanDays = (lastMs - firstMs) / MS_PER_DAY;
        }
      }
    }

    if (oosSpanDays === null || !Number.isFinite(oosSpanDays) || oosSpanDays <= 0) {
      return { annProfitPct: null, oosSpanDays };
    }

    const profitPct = toFiniteNumber(study?.profit_pct);
    if (profitPct === null) {
      return { annProfitPct: null, oosSpanDays };
    }

    if (oosSpanDays <= 30) {
      return { annProfitPct: null, oosSpanDays };
    }

    const returnMultiple = 1 + (profitPct / 100);
    if (returnMultiple <= 0) {
      return { annProfitPct: null, oosSpanDays };
    }

    const annProfitPct = (Math.pow(returnMultiple, 365 / oosSpanDays) - 1) * 100;
    if (!Number.isFinite(annProfitPct)) {
      return { annProfitPct: null, oosSpanDays };
    }

    return { annProfitPct, oosSpanDays };
  }

  function formatAnnualizedProfitCell(study) {
    const annProfitPct = toFiniteNumber(study?.ann_profit_pct);
    const oosSpanDays = toFiniteNumber(study?._oos_span_days);

    if (annProfitPct === null) {
      if (oosSpanDays !== null && oosSpanDays > 0 && oosSpanDays <= 30) {
        return {
          text: 'N/A',
          className: '',
          tooltip: `OOS period too short for meaningful annualization (${Math.round(oosSpanDays)} days)`,
        };
      }
      return { text: 'N/A', className: '', tooltip: '' };
    }

    const className = annProfitPct >= 0 ? 'val-positive' : 'val-negative';
    if (oosSpanDays !== null && oosSpanDays >= 31 && oosSpanDays < 90) {
      return {
        text: `${formatSignedPercentValue(annProfitPct, 1)}*`,
        className,
        tooltip: `Short OOS period (${Math.round(oosSpanDays)} days) - annualized value may be misleading`,
      };
    }
    return { text: formatSignedPercentValue(annProfitPct, 1), className, tooltip: '' };
  }

  window.AnalyticsTable = {
    renderTable,
    setAllChecked,
    getCheckedStudyIds,
    getOrderedStudyIds,
    getSortState,
    setFocusedStudyId,
    getVisibleStudyIds,
    scrollStudyIntoView,
    encodeStudyId,
    computeAnnualizedProfitMetrics,
  };
})();
