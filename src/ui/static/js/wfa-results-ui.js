(function () {
  const MODULE_ORDER = ['optuna_is', 'dsr', 'forward_test', 'stress_test'];
  const MODULE_LABELS = {
    optuna_is: 'Optuna IS',
    dsr: 'DSR',
    forward_test: 'Forward Test',
    stress_test: 'Stress Test'
  };

  const WFAState = {
    expandedWindows: new Set(),
    windowTrials: {},
    activeTab: {}
  };

  function formatNumber(value, digits = 2) {
    if (value === null || value === undefined || Number.isNaN(Number(value))) {
      return '-';
    }
    const num = Number(value);
    if (!Number.isFinite(num)) {
      return num > 0 ? 'Inf' : '-Inf';
    }
    return num.toFixed(digits);
  }

  function formatPct(value, digits = 2) {
    if (value === null || value === undefined || Number.isNaN(Number(value))) {
      return '-';
    }
    const num = Number(value);
    const prefix = num >= 0 ? '+' : '';
    return `${prefix}${num.toFixed(digits)}%`;
  }

  function formatInt(value) {
    if (value === null || value === undefined || Number.isNaN(Number(value))) {
      return '-';
    }
    return String(parseInt(value, 10));
  }

  function renderBadge(type, value) {
    if (value === null || value === undefined) return '';
    if (type === 'pareto') {
      return value ? '<span class="dot dot-pareto"></span>' : '';
    }
    if (type === 'constraints') {
      if (value === true) return '<span class="dot dot-ok"></span>';
      if (value === false) return '<span class="dot dot-fail"></span>';
      return '';
    }
    return '';
  }

  function normalizeModules(availableModules) {
    if (!Array.isArray(availableModules)) return [];
    const set = new Set(availableModules.map((m) => String(m)));
    return MODULE_ORDER.filter((key) => set.has(key));
  }

  function hasConstraintFlags(items) {
    return (items || []).some((item) => item && item.constraints_satisfied !== undefined && item.constraints_satisfied !== null);
  }

  function getOptunaMeta(context = {}) {
    const objectives = window.ResultsState?.optuna?.objectives || [];
    const constraints = window.ResultsState?.optuna?.constraints || [];
    const hasConstraintConfig = Array.isArray(constraints)
      ? constraints.some((c) => c && c.enabled)
      : false;
    const hasConstraintData = hasConstraintFlags(context.windows) || hasConstraintFlags(context.trials);
    return { objectives, hasConstraints: hasConstraintConfig || hasConstraintData };
  }

  function buildOptunaRow(trial, objectives, hasConstraints) {
    if (!window.OptunaResultsUI) return null;
    const temp = document.createElement('tbody');
    temp.innerHTML = window.OptunaResultsUI.renderTrialRow(trial, objectives, { hasConstraints }).trim();
    return temp.firstElementChild || null;
  }

  function getStudyId() {
    const state = window.ResultsState || {};
    return state.studyId || state.study_id || (state.study && state.study.study_id) || '';
  }

  function displayStitchedEquity() {
    const stitched = window.ResultsState?.stitched_oos;
    if (!stitched?.equity_curve) return;
    if (typeof renderEquityChart !== 'function') return;
    const windows = window.ResultsState?.results || [];
    let boundaries = [];
    if (typeof calculateWindowBoundariesByDate === 'function') {
      boundaries = calculateWindowBoundariesByDate(windows, stitched.timestamps || []);
    } else if (typeof calculateWindowBoundaries === 'function') {
      boundaries = calculateWindowBoundaries(windows, stitched);
    }
    renderEquityChart(stitched.equity_curve, boundaries, stitched.timestamps || [], { useTimeScale: true });
  }

  function setWfaSelection(selection) {
    if (!window.ResultsState) return;
    window.ResultsState.wfaSelection = selection;
  }

  function selectRow(row) {
    if (!row) return;
    const scope = row.closest('.table-card') || document;
    scope.querySelectorAll('.data-table tr.selected').forEach((node) => {
      if (node !== row) node.classList.remove('selected');
    });
    row.classList.add('selected');
  }

  function getWindowData(windowNumber) {
    const windows = window.ResultsState?.results || [];
    const target = Number(windowNumber);
    if (!Number.isFinite(target)) return null;
    return windows.find((item, idx) => {
      const value = item?.window_number ?? item?.window_id ?? (idx + 1);
      return Number(value) === target;
    }) || null;
  }

  function deriveDaysFromIsoRange(startIso, endIso) {
    if (!startIso || !endIso) return null;
    const startTs = Date.parse(startIso);
    const endTs = Date.parse(endIso);
    if (!Number.isFinite(startTs) || !Number.isFinite(endTs) || endTs <= startTs) {
      return null;
    }
    return (endTs - startTs) / 86400000;
  }

  function enrichModuleConsistencySegments(modules, windowData) {
    const source = modules && typeof modules === 'object' ? modules : {};
    const isSegments = windowData?.is_consistency_segments_used
      ?? window.ResultsState?.consistencySegments
      ?? null;
    const isPeriodDays = window.ResultsState?.wfa?.isPeriodDays ?? null;
    const oosDays = windowData?.oos_actual_days
      ?? deriveDaysFromIsoRange(windowData?.oos_start_date, windowData?.oos_end_date);
    const ftDays = deriveDaysFromIsoRange(windowData?.ft_start_date, windowData?.ft_end_date)
      ?? window.ResultsState?.forwardTest?.periodDays
      ?? null;
    const deriveAuto = typeof deriveAutoConsistencySegments === 'function'
      ? deriveAutoConsistencySegments
      : null;
    const oosSegments = deriveAuto ? deriveAuto(isPeriodDays, isSegments, oosDays) : null;
    const ftSegments = deriveAuto ? deriveAuto(isPeriodDays, isSegments, ftDays) : null;

    const mapped = {};
    Object.entries(source).forEach(([moduleType, trials]) => {
      mapped[moduleType] = (Array.isArray(trials) ? trials : []).map((trial) => {
        if (!trial || typeof trial !== 'object') return trial;
        let segments = trial.consistency_segments_used ?? null;
        if (segments == null) {
          if (moduleType === 'forward_test') {
            segments = ftSegments;
          } else if (moduleType === 'optuna_is' || moduleType === 'dsr' || moduleType === 'stress_test') {
            segments = isSegments;
          } else if (moduleType === 'oos') {
            segments = oosSegments;
          }
        }
        if (segments == null) return trial;
        return { ...trial, consistency_segments_used: segments };
      });
    });
    return mapped;
  }

  function findBoundaryIndex(timestamps, boundaryDate) {
    if (!Array.isArray(timestamps) || !boundaryDate) return null;
    const boundaryTime = Date.parse(boundaryDate);
    if (!Number.isFinite(boundaryTime)) return null;
    for (let i = 0; i < timestamps.length; i += 1) {
      const t = Date.parse(timestamps[i]);
      if (Number.isFinite(t) && t >= boundaryTime) {
        return i;
      }
    }
    return null;
  }

  function getIsOosBoundary(windowNumber, timestamps) {
    if (!Array.isArray(timestamps) || timestamps.length < 2) return [];
    const windowData = getWindowData(windowNumber);
    if (!windowData) return [];
    const boundaryDate = windowData.oos_start_date || windowData.is_end_date;
    const index = findBoundaryIndex(timestamps, boundaryDate);
    if (!Number.isFinite(index) || index <= 0 || index >= timestamps.length - 1) {
      return [];
    }
    return [{ index }];
  }

  async function generateWindowEquity(windowNumber, period) {
    const studyId = getStudyId();
    if (!studyId) return;

    try {
      const response = await fetch(`/api/studies/${encodeURIComponent(studyId)}/wfa/windows/${windowNumber}/equity`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ period })
      });

      if (!response.ok) {
        throw new Error(`Failed to generate equity: ${response.status}`);
      }

      const data = await response.json();
      if (typeof renderEquityChart === 'function') {
        const timestamps = data.timestamps || [];
        const boundaries = period === 'both'
          ? getIsOosBoundary(windowNumber, timestamps)
          : [];
        renderEquityChart(data.equity_curve || [], boundaries, timestamps);
      }
    } catch (error) {
      console.error('Error generating window equity:', error);
    }
  }

  async function generateTrialEquity(windowNumber, moduleType, trialNumber, period) {
    const studyId = getStudyId();
    if (!studyId) return;

    try {
      const response = await fetch(`/api/studies/${encodeURIComponent(studyId)}/wfa/windows/${windowNumber}/equity`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ moduleType, trialNumber, period })
      });

      if (!response.ok) {
        throw new Error(`Failed to generate equity: ${response.status}`);
      }

      const data = await response.json();
      if (typeof renderEquityChart === 'function') {
        renderEquityChart(data.equity_curve || [], [], data.timestamps || []);
      }
    } catch (error) {
      console.error('Error generating trial equity:', error);
    }
  }

  function defaultPeriodForModule(moduleType) {
    switch (moduleType) {
      case 'forward_test':
        return 'ft';
      case 'optuna_is':
      case 'dsr':
      case 'stress_test':
        return 'optuna_is';
      default:
        return 'is';
    }
  }

  function formatWfaRankCell(rank, sourceRank) {
    const baseRank = Number(rank);
    if (!Number.isFinite(baseRank)) return '';
    if (sourceRank === null || sourceRank === undefined) {
      return typeof formatRankCell === 'function'
        ? formatRankCell(baseRank, null)
        : String(baseRank);
    }
    const source = Number(sourceRank);
    const delta = Number.isFinite(source) ? source - baseRank : null;
    if (typeof formatRankCell === 'function') {
      return formatRankCell(baseRank, delta);
    }
    return String(baseRank);
  }

  function getRankDelta(trial) {
    const baseRank = Number(trial?.module_rank);
    const sourceRank = Number(trial?.source_rank);
    if (!Number.isFinite(baseRank) || !Number.isFinite(sourceRank)) return null;
    return sourceRank - baseRank;
  }

  function getModuleSourceLabel(windowNumber, moduleType) {
    const cached = WFAState.windowTrials[windowNumber];
    const modules = cached?.modules || {};
    if (moduleType === 'forward_test') {
      return (modules.dsr && modules.dsr.length) ? 'DSR' : 'Optuna';
    }
    if (moduleType === 'stress_test') {
      if (modules.forward_test && modules.forward_test.length) return 'FT';
      if (modules.dsr && modules.dsr.length) return 'DSR';
      return 'Optuna';
    }
    return '';
  }

  function buildWfaComparisonLine(trial, moduleType, windowNumber) {
    if (typeof formatSigned !== 'function') return '';
    const metrics = trial?.module_metrics || {};
    if (moduleType === 'dsr') {
      const rankDelta = getRankDelta(trial);
      const rankLine = rankDelta !== null ? `Rank: ${formatSigned(rankDelta, 0)}` : null;
      const dsrValue = Number(metrics.dsr_probability);
      const dsrLabel = Number.isFinite(dsrValue) ? dsrValue.toFixed(3) : 'N/A';
      const luckValue = Number(metrics.dsr_luck_share_pct);
      const luckLabel = Number.isFinite(luckValue) ? `${luckValue.toFixed(1)}%` : 'N/A';
      return [rankLine, `DSR: ${dsrLabel}`, `Luck: ${luckLabel}`].filter(Boolean).join(' | ');
    }
    if (moduleType === 'forward_test') {
      const rankDelta = getRankDelta(trial);
      const sourceLabel = getModuleSourceLabel(windowNumber, moduleType) || 'Optuna';
      const rankLine = rankDelta !== null
        ? `Rank: ${formatSigned(rankDelta, 0)} (vs ${sourceLabel})`
        : null;
      const line = [
        rankLine,
        `Profit Deg: ${formatSigned(metrics.profit_degradation || 0, 2)}`,
        `Max DD: ${formatSigned(metrics.max_dd_change || 0, 2, '%')}`,
        `ROMAD: ${formatSigned(metrics.romad_change || 0, 2)}`,
        `Sharpe: ${formatSigned(metrics.sharpe_change || 0, 2)}`,
        `PF: ${formatSigned(metrics.pf_change || 0, 2)}`
      ].filter(Boolean).join(' | ');
      return line;
    }
    if (moduleType === 'stress_test') {
      const status = trial?.status;
      if (status === 'skipped_bad_base') {
        const baseProfit = Number(trial?.net_profit_pct || 0);
        return `Status: Bad Base (profit <= 0%) | Base Profit: ${baseProfit.toFixed(1)}%`;
      }
      if (status === 'insufficient_data') {
        const totalPerturbations = Number(metrics.total_perturbations || 0);
        const combinedFailures = Number(metrics.combined_failure_count || 0);
        const validNeighbors = totalPerturbations - combinedFailures;
        return `Status: Insufficient Data (${validNeighbors} valid neighbors, minimum 4 required) | Profit Ret: N/A | RoMaD Ret: N/A`;
      }
      if (status === 'skipped_no_params') {
        return 'Status: No Testable Parameters (strategy has only categorical params)';
      }
      const rankDelta = getRankDelta(trial);
      const sourceLabel = getModuleSourceLabel(windowNumber, moduleType) || 'Optuna';
      const rankLine = rankDelta !== null
        ? `Rank: ${formatSigned(rankDelta, 0)} (vs ${sourceLabel})`
        : null;
      const profitRet = metrics.profit_retention;
      const profitRetLabel = profitRet !== null && profitRet !== undefined
        ? `${(profitRet * 100).toFixed(1)}%`
        : 'N/A';
      const romadRet = metrics.romad_retention;
      const romadRetLabel = romadRet !== null && romadRet !== undefined
        ? `${(romadRet * 100).toFixed(1)}%`
        : 'N/A';
      const failRate = metrics.combined_failure_rate;
      const failRateLabel = failRate !== null && failRate !== undefined
        ? `${(failRate * 100).toFixed(1)}%`
        : 'N/A';
      const romadValid = metrics.romad_failure_rate !== null && metrics.romad_failure_rate !== undefined;
      const failRateType = romadValid ? 'Fail' : 'Fail (profit)';
      const sensParam = metrics.most_sensitive_param || null;
      const sensLine = sensParam ? `Sens: ${sensParam}` : null;
      return [
        rankLine,
        `Profit Ret: ${profitRetLabel}`,
        `RoMaD Ret: ${romadRetLabel}`,
        `${failRateType}: ${failRateLabel}`,
        sensLine
      ].filter(Boolean).join(' | ');
    }
    return '';
  }

  function getWfaModulePeriodLabel(windowData, moduleType) {
    if (typeof buildPeriodLabel !== 'function') return 'Period: N/A';
    if (!windowData) return 'Period: N/A';
    if (moduleType === 'forward_test') {
      return buildPeriodLabel(windowData.ft_start_date, windowData.ft_end_date);
    }
    const start = windowData.optimization_start_date || windowData.is_start_date;
    const end = windowData.optimization_end_date || windowData.is_end_date;
    return buildPeriodLabel(start, end);
  }

  function getWfaModuleSortSubtitle(moduleType) {
    if (moduleType === 'optuna_is') {
      return typeof getOptunaSortSubtitle === 'function' ? getOptunaSortSubtitle() : 'Sorted by objectives';
    }
    if (moduleType === 'dsr') {
      return 'Sorted by DSR probability';
    }
    if (moduleType === 'forward_test') {
      const sortMetric = window.ResultsState?.wfa?.postProcess?.sortMetric
        || window.ResultsState?.forwardTest?.sortMetric
        || 'profit_degradation';
      const label = typeof formatSortMetricLabel === 'function'
        ? formatSortMetricLabel(sortMetric)
        : '';
      return `Sorted by ${label || 'FT results'}`;
    }
    if (moduleType === 'stress_test') {
      const sortMetric = window.ResultsState?.wfa?.postProcess?.stressTest?.sortMetric
        || window.ResultsState?.stressTest?.sortMetric
        || 'profit_retention';
      const label = typeof formatSortMetricLabel === 'function'
        ? formatSortMetricLabel(sortMetric)
        : '';
      return `Sorted by ${label || 'retention'}`;
    }
    return '';
  }

  function updateWfaTableHeader(windowData, moduleType) {
    if (typeof updateTableHeader !== 'function') return;
    const title = MODULE_LABELS[moduleType] || moduleType || '';
    const periodLabel = getWfaModulePeriodLabel(windowData, moduleType);
    const subtitle = getWfaModuleSortSubtitle(moduleType);
    updateTableHeader(title, subtitle, periodLabel);
  }

  function renderModuleTrialsTable(trials, moduleType, windowNumber) {
    if (!trials || !trials.length) {
      return '<div class="no-data">No trials available for this module.</div>';
    }

    const { objectives, hasConstraints } = getOptunaMeta({ trials });

    const rows = trials.map((trial, idx) => {
      const moduleRank = trial.module_rank || idx + 1;
      const supportsRankDelta = moduleType === 'dsr'
        || moduleType === 'forward_test'
        || moduleType === 'stress_test';
      const sourceRank = supportsRankDelta ? trial.source_rank : null;
      const rankHtml = formatWfaRankCell(moduleRank, sourceRank);
      if (!window.OptunaResultsUI) {
        const netClass = Number(trial.net_profit_pct || 0) >= 0 ? 'val-positive' : 'val-negative';
        const ddClass = Number(trial.max_drawdown_pct || 0) >= 0 ? 'val-negative' : 'val-positive';
        const score = trial.composite_score ?? trial.score;
        return `
          <tr data-trial-number="${trial.trial_number}" data-module-type="${moduleType}" data-window-number="${windowNumber}">
            <td>${rankHtml}</td>
            <td class="param-hash">${trial.param_id || '-'}</td>
            <td>${renderBadge('pareto', trial.is_pareto_optimal)}</td>
            ${hasConstraints ? `<td>${renderBadge('constraints', trial.constraints_satisfied)}</td>` : ''}
            <td>${formatNumber(trial.win_rate, 1)}</td>
            <td class="${netClass}">${formatPct(trial.net_profit_pct, 2)}</td>
            <td class="${ddClass}">-${Math.abs(Number(trial.max_drawdown_pct || 0)).toFixed(2)}%</td>
            <td>${formatInt(trial.total_trades)}</td>
            <td>${formatInt(trial.max_consecutive_losses)}</td>
            <td>${score !== undefined && score !== null ? formatNumber(score, 2) : '-'}</td>
          </tr>
        `;
      }

      const normalized = { ...trial };
      if (normalized.score === undefined || normalized.score === null) {
        normalized.score = normalized.composite_score ?? null;
      }
      const row = buildOptunaRow(normalized, objectives, hasConstraints);
      if (!row) return '';

      row.classList.add('clickable');
      row.dataset.trialNumber = trial.trial_number ?? '';
      row.dataset.moduleType = moduleType;
      row.dataset.windowNumber = windowNumber;

      const rankCell = row.querySelector('.rank');
      if (rankCell) {
        rankCell.innerHTML = rankHtml;
      }
      const hashCell = row.querySelector('.param-hash');
      if (hashCell) {
        hashCell.textContent = trial.param_id || '-';
      }
      return row.outerHTML;
    }).join('');

    const header = window.OptunaResultsUI
      ? window.OptunaResultsUI.buildTrialTableHeaders(objectives, hasConstraints)
      : `
        <th>#</th>
        <th>Param ID</th>
        <th>P</th>
        ${hasConstraints ? '<th>C</th>' : ''}
        <th>WR%</th>
        <th>Net%</th>
        <th>DD%</th>
        <th>Trades</th>
        <th>MaxCL</th>
        <th>Score</th>
      `;

    return `
      <table class="data-table wfa-module-table">
        <thead>
          <tr>${header}</tr>
        </thead>
        <tbody>${rows}</tbody>
      </table>
    `;
  }

  function renderModuleTabs(windowNumber, availableModules) {
    const modules = normalizeModules(availableModules);
    if (!modules.length) {
      return '<div class="no-data">No module data available.</div>';
    }

    const active = WFAState.activeTab[windowNumber] || modules[0];
    WFAState.activeTab[windowNumber] = active;

    const tabButtons = modules.map((moduleType) => {
      const label = MODULE_LABELS[moduleType] || moduleType;
      const activeClass = moduleType === active ? 'active' : '';
      return `<button class="tab-btn wfa-tab-btn ${activeClass}" data-window-number="${windowNumber}" data-module="${moduleType}">${label}</button>`;
    }).join('');

    return `
      <div class="wfa-modules-container">
        <div class="wfa-module-tabs">${tabButtons}</div>
        <div class="wfa-tab-content" id="wfa-tab-content-${windowNumber}"></div>
      </div>
    `;
  }

  function renderActiveModuleTable(windowNumber, moduleType) {
    const cached = WFAState.windowTrials[windowNumber];
    if (!cached?.loaded) return;

    const trials = cached.modules[moduleType] || [];
    const contentContainer = document.getElementById(`wfa-tab-content-${windowNumber}`);
      if (contentContainer) {
        contentContainer.innerHTML = renderModuleTrialsTable(trials, moduleType, windowNumber);
        contentContainer.querySelectorAll('tbody tr').forEach((row) => {
          row.addEventListener('click', async () => {
            selectRow(row);
            const trialNumber = parseInt(row.dataset.trialNumber, 10);
            const period = defaultPeriodForModule(moduleType);
            const trial = trials.find((item) => Number(item.trial_number) === trialNumber);
            if (trial) {
              showParameterDetails({ ...trial, param_id: trial.param_id });
            }
            if (typeof setComparisonLine === 'function') {
              const line = trial ? buildWfaComparisonLine(trial, moduleType, windowNumber) : '';
              setComparisonLine(line);
            }
            setWfaSelection({ windowNumber, moduleType, trialNumber, period });
            await generateTrialEquity(windowNumber, moduleType, trialNumber, period);
          });
        });
      }
  }

  async function loadWindowTrials(windowNumber, windowId, availableModules) {
    const cached = WFAState.windowTrials[windowNumber];
    if (cached?.loaded) {
      return;
    }

    const studyId = getStudyId();
    if (!studyId) {
      const container = document.getElementById(`wfa-window-expand-${windowNumber}`);
      if (container) {
        container.innerHTML = '<div class="error">Select a study to load window details.</div>';
      }
      return;
    }

    WFAState.windowTrials[windowNumber] = { loaded: false, modules: {} };

    try {
      const response = await fetch(`/api/studies/${encodeURIComponent(studyId)}/wfa/windows/${windowNumber}`);
      if (!response.ok) {
        throw new Error(`Failed to load window details: ${response.status}`);
      }
      const data = await response.json();
      const modules = enrichModuleConsistencySegments(data.modules || {}, data.window || {});
      const available = normalizeModules(data.window?.available_modules || availableModules || []);

      WFAState.windowTrials[windowNumber] = {
        loaded: true,
        modules,
        window: data.window || {},
        availableModules: available
      };

      if (!WFAState.activeTab[windowNumber]) {
        WFAState.activeTab[windowNumber] = available[0] || 'optuna_is';
      }

      const container = document.getElementById(`wfa-window-expand-${windowNumber}`);
      if (container) {
        container.innerHTML = renderModuleTabs(windowNumber, available);
        container.querySelectorAll('.wfa-tab-btn').forEach((btn) => {
          btn.addEventListener('click', () => {
            const moduleType = btn.dataset.module;
            WFAState.activeTab[windowNumber] = moduleType;
            container.querySelectorAll('.wfa-tab-btn').forEach((node) => {
              node.classList.toggle('active', node.dataset.module === moduleType);
            });
            if (typeof setComparisonLine === 'function') {
              setComparisonLine('');
            }
            updateWfaTableHeader(WFAState.windowTrials[windowNumber]?.window || {}, moduleType);
            renderActiveModuleTable(windowNumber, moduleType);
          });
        });
        renderActiveModuleTable(windowNumber, WFAState.activeTab[windowNumber]);
      }
    } catch (error) {
      console.error('Failed to load WFA window trials:', error);
      const container = document.getElementById(`wfa-window-expand-${windowNumber}`);
      if (container) {
        container.innerHTML = '<div class="error">Failed to load window details.</div>';
      }
    }
  }

  function renderWFAResultsTable(windows, summary) {
    const tbody = document.querySelector('.data-table tbody');
    if (!tbody) return;

    const { objectives, hasConstraints } = getOptunaMeta({ windows });

    const thead = document.querySelector('.data-table thead tr');
    if (thead) {
      const header = window.OptunaResultsUI
        ? window.OptunaResultsUI.buildTrialTableHeaders(objectives, hasConstraints)
        : `
          <th>#</th>
          <th>Param ID</th>
          <th>P</th>
          ${hasConstraints ? '<th>C</th>' : ''}
          <th>WR%</th>
          <th>Net%</th>
          <th>DD%</th>
          <th>Trades</th>
          <th>MaxCL</th>
          <th>Score</th>
        `;
      thead.innerHTML = `<th class="col-expand"></th>${header}`;
    }

    const totalColumns = thead ? thead.querySelectorAll('th').length : 0;

    tbody.innerHTML = '';

    const stitchedSummary = summary?.stitched_oos || {};
    const stitchedNet = summary?.stitched_oos_net_profit_pct
      ?? stitchedSummary.final_net_profit_pct
      ?? summary?.final_net_profit_pct
      ?? summary?.net_profit_pct
      ?? null;
    const stitchedDd = summary?.stitched_oos_max_drawdown_pct
      ?? stitchedSummary.max_drawdown_pct
      ?? summary?.max_drawdown_pct
      ?? null;
    const stitchedTrades = summary?.stitched_oos_total_trades
      ?? stitchedSummary.total_trades
      ?? summary?.total_trades
      ?? null;
    const stitchedWinRate = summary?.stitched_oos_win_rate
      ?? stitchedSummary.oos_win_rate
      ?? summary?.oos_win_rate
      ?? summary?.win_rate
      ?? null;

    const stitchedTrial = {
      trial_number: '',
      win_rate: stitchedWinRate,
      net_profit_pct: stitchedNet,
      max_drawdown_pct: stitchedDd,
      total_trades: stitchedTrades,
      max_consecutive_losses: null,
      score: null,
      romad: null,
      sharpe_ratio: null,
      profit_factor: null,
      ulcer_index: null,
      sqn: null,
      consistency_score: null,
      objective_values: []
    };

    const stitchedRow = buildOptunaRow(stitchedTrial, objectives, hasConstraints) || document.createElement('tr');
    stitchedRow.classList.add('wfa-stitched-row', 'clickable');
    stitchedRow.insertAdjacentHTML('afterbegin', '<td class="col-expand"></td>');

    const stitchedRankCell = stitchedRow.querySelector('.rank');
    if (stitchedRankCell) stitchedRankCell.textContent = '-';
    const stitchedHashCell = stitchedRow.querySelector('.param-hash');
    if (stitchedHashCell) stitchedHashCell.textContent = 'Stitched OOS';

    const stitchedCells = stitchedRow.querySelectorAll('td');
    const pIndex = 3;
    if (stitchedCells[pIndex]) stitchedCells[pIndex].innerHTML = '';
    if (hasConstraints && stitchedCells[pIndex + 1]) stitchedCells[pIndex + 1].innerHTML = '';

    stitchedRow.addEventListener('click', () => {
      selectRow(stitchedRow);
      setWfaSelection({ type: 'stitched' });
      if (typeof setComparisonLine === 'function') {
        setComparisonLine('');
      }
      if (typeof updateTableHeader === 'function' && typeof getWfaStitchedPeriodLabel === 'function') {
        updateTableHeader('Stitched OOS', '', getWfaStitchedPeriodLabel(window.ResultsState?.results || []));
      }
      displayStitchedEquity();
    });
    tbody.appendChild(stitchedRow);

    (windows || []).forEach((window, index) => {
      const windowNumberRaw = window.window_number ?? window.window_id ?? index + 1;
      const windowNumber = Number.isFinite(Number(windowNumberRaw))
        ? parseInt(windowNumberRaw, 10)
        : index + 1;
      const availableModules = window.available_modules || [];
      const actualDaysRaw = window.oos_actual_days;
      const hasActualDays = actualDaysRaw !== null
        && actualDaysRaw !== undefined
        && String(actualDaysRaw).trim() !== ''
        && Number.isFinite(Number(actualDaysRaw));
      const actualDays = hasActualDays
        ? `${Math.round(Number(actualDaysRaw))}d`
        : null;
      const triggerType = String(window.trigger_type || '').toLowerCase();
      const triggerLabels = {
        cusum: 'CUSUM',
        drawdown: 'DD',
        inactivity: 'INACTIVE',
        max_period: 'MAX'
      };
      const triggerLabel = triggerLabels[triggerType] || '';
      const triggerBadge = triggerLabel
        ? `<span class="wfa-trigger-badge wfa-trigger-${triggerType}">${triggerLabel}</span>`
        : '';
      const adaptiveModeRaw = window.ResultsState?.wfa?.adaptiveMode ?? window.ResultsState?.wfa?.adaptive_mode;
      const adaptiveMode = adaptiveModeRaw === null || adaptiveModeRaw === undefined
        ? null
        : Boolean(adaptiveModeRaw);
      const hasAdaptiveMeta = Boolean(triggerLabel || actualDays);
      const showAdaptiveMeta = adaptiveMode === true || (adaptiveMode === null && hasAdaptiveMeta);
      const adaptiveSuffix = showAdaptiveMeta && hasAdaptiveMeta
        ? ` (${actualDays || '-'}) ${triggerBadge}`
        : '';

      const headerRow = document.createElement('tr');
      headerRow.className = 'wfa-window-header';
      headerRow.innerHTML = `
        <td class="col-expand"><span class="expand-toggle" data-window-number="${windowNumber}">&#9654;</span></td>
        <td colspan="${Math.max(1, totalColumns - 1)}">Window ${windowNumber} | IS: ${window.is_start_date || '-'} - ${window.is_end_date || '-'} | OOS: ${window.oos_start_date || '-'} - ${window.oos_end_date || '-'}${adaptiveSuffix}</td>
      `;
      headerRow.addEventListener('click', () => {
        setWfaSelection({ windowNumber, period: 'both' });
        if (typeof setComparisonLine === 'function') {
          setComparisonLine('');
        }
        generateWindowEquity(windowNumber, 'both');
      });
      tbody.appendChild(headerRow);

      const isSegments = window.is_consistency_segments_used
        ?? ResultsState.consistencySegments
        ?? null;
      let oosDays = window.oos_actual_days;
      if ((oosDays === null || oosDays === undefined) && window.oos_start_date && window.oos_end_date) {
        const startTs = Date.parse(window.oos_start_date);
        const endTs = Date.parse(window.oos_end_date);
        if (Number.isFinite(startTs) && Number.isFinite(endTs) && endTs > startTs) {
          oosDays = (endTs - startTs) / 86400000;
        }
      }
      const oosSegments = window.oos_consistency_segments_used
        ?? (typeof deriveAutoConsistencySegments === 'function'
          ? deriveAutoConsistencySegments(
            ResultsState.wfa?.isPeriodDays,
            isSegments,
            oosDays
          )
          : null);

      const isTrial = {
        trial_number: window.is_best_trial_number ?? '',
        win_rate: window.is_win_rate,
        net_profit_pct: window.is_net_profit_pct,
        max_drawdown_pct: window.is_max_drawdown_pct,
        total_trades: window.is_total_trades,
        max_consecutive_losses: window.is_max_consecutive_losses,
        score: window.is_composite_score,
        romad: window.is_romad,
        sharpe_ratio: window.is_sharpe_ratio,
        profit_factor: window.is_profit_factor,
        ulcer_index: window.is_ulcer_index,
        sqn: window.is_sqn,
        consistency_score: window.is_consistency_score,
        consistency_segments_used: isSegments,
        is_pareto_optimal: window.is_pareto_optimal,
        constraints_satisfied: window.constraints_satisfied,
        objective_values: []
      };

      const isRow = buildOptunaRow(isTrial, objectives, hasConstraints) || document.createElement('tr');
      isRow.classList.add('wfa-window-row', 'clickable');
      isRow.insertAdjacentHTML('afterbegin', '<td class="col-expand"></td>');

      const isRankCell = isRow.querySelector('.rank');
      if (isRankCell) isRankCell.textContent = windowNumber;
      const isHashCell = isRow.querySelector('.param-hash');
      if (isHashCell) isHashCell.textContent = window.param_id || '-';

      isRow.addEventListener('click', async () => {
        selectRow(isRow);
        showParameterDetails(window);
        setWfaSelection({ windowNumber, period: 'is' });
        if (typeof setComparisonLine === 'function') {
          setComparisonLine('');
        }
        await generateWindowEquity(windowNumber, 'is');
      });
      tbody.appendChild(isRow);

      const oosTrial = {
        trial_number: '',
        win_rate: window.oos_win_rate,
        net_profit_pct: window.oos_net_profit_pct,
        max_drawdown_pct: window.oos_max_drawdown_pct,
        total_trades: window.oos_total_trades,
        max_consecutive_losses: window.oos_max_consecutive_losses,
        score: null,
        romad: window.oos_romad,
        sharpe_ratio: window.oos_sharpe_ratio,
        profit_factor: window.oos_profit_factor,
        ulcer_index: window.oos_ulcer_index,
        sqn: window.oos_sqn,
        consistency_score: window.oos_consistency_score,
        consistency_segments_used: oosSegments,
        is_pareto_optimal: window.is_pareto_optimal,
        constraints_satisfied: window.constraints_satisfied,
        objective_values: []
      };

      const oosRow = buildOptunaRow(oosTrial, objectives, hasConstraints) || document.createElement('tr');
      oosRow.classList.add('wfa-window-row', 'clickable');
      oosRow.insertAdjacentHTML('afterbegin', '<td class="col-expand"></td>');

      const oosRankCell = oosRow.querySelector('.rank');
      if (oosRankCell) oosRankCell.textContent = '';
      const oosHashCell = oosRow.querySelector('.param-hash');
      if (oosHashCell) oosHashCell.textContent = window.param_id || '-';

      oosRow.addEventListener('click', async () => {
        selectRow(oosRow);
        showParameterDetails(window);
        setWfaSelection({ windowNumber, period: 'oos' });
        if (typeof setComparisonLine === 'function') {
          setComparisonLine('');
        }
        await generateWindowEquity(windowNumber, 'oos');
      });
      tbody.appendChild(oosRow);

      const expandRow = document.createElement('tr');
      expandRow.className = 'wfa-window-expand hidden';
      expandRow.innerHTML = `
        <td colspan="${Math.max(1, totalColumns)}">
          <div id="wfa-window-expand-${windowNumber}" class="wfa-window-expand-content"></div>
        </td>
      `;
      tbody.appendChild(expandRow);

      const toggle = headerRow.querySelector('.expand-toggle');
      if (toggle) {
        toggle.addEventListener('click', async (event) => {
          event.stopPropagation();
          const isExpanded = WFAState.expandedWindows.has(windowNumber);
          if (isExpanded) {
            WFAState.expandedWindows.delete(windowNumber);
            expandRow.classList.add('hidden');
            toggle.innerHTML = '&#9654;';
          } else {
            WFAState.expandedWindows.add(windowNumber);
            expandRow.classList.remove('hidden');
            toggle.innerHTML = '&#9660;';
            await loadWindowTrials(windowNumber, window.window_id, availableModules);
            const container = document.getElementById(`wfa-window-expand-${windowNumber}`);
            if (container && !container.innerHTML.trim()) {
              container.innerHTML = renderModuleTabs(windowNumber, availableModules);
            }
          }
        });
      }
    });
  }

  window.WFAResultsUI = {
    renderWFAResultsTable,
    loadWindowTrials,
    resetState: function () {
      WFAState.expandedWindows.clear();
      WFAState.windowTrials = {};
      WFAState.activeTab = {};
    }
  };
})();
