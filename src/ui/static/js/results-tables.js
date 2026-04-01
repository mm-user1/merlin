function updateResultsHeader() {
  const header = document.querySelector('.results-header h2');
  if (!header) return;
  const runDateLabel = formatDateLabel(ResultsState.studyCreatedAt);
  if (ResultsState.studyName) {
    header.textContent = runDateLabel
      ? `${ResultsState.studyName} · ${runDateLabel}`
      : ResultsState.studyName;
  } else {
    header.textContent = runDateLabel
      ? `Optimization Results · ${runDateLabel}`
      : 'Optimization Results';
  }
}

function updateTableHeader(title, subtitle, periodLabel) {
  const titleEl = document.getElementById('resultsTableTitle');
  const subtitleEl = document.getElementById('resultsTableSubtitle');
  const safeTitle = title || '';
  const safePeriod = periodLabel ? ` · ${periodLabel}` : '';
  if (titleEl) titleEl.textContent = `${safeTitle}${safePeriod}`.trim();
  if (subtitleEl) subtitleEl.textContent = subtitle || '';
}

function setComparisonLine(text) {
  const line = document.getElementById('comparisonLine');
  if (!line) return;
  if (text) {
    line.textContent = text;
    line.style.display = 'flex';
  } else {
    line.textContent = '';
    line.style.display = 'none';
  }
}

function renderManualTestControls() {
  const controls = document.getElementById('testResultsControls');
  const select = document.getElementById('manualTestSelect');
  const baseline = document.getElementById('manualTestBaselineLabel');

  if (!controls || !select) return;

  if (ResultsState.activeTab !== 'manual_tests') {
    controls.style.display = 'none';
    return;
  }

  controls.style.display = 'flex';
  select.innerHTML = '';

  ResultsState.manualTests.forEach((test) => {
    const option = document.createElement('option');
    const dateLabel = test.created_at ? new Date(test.created_at).toLocaleString() : 'Unknown';
    const name = test.test_name ? ` - ${test.test_name}` : '';
    option.value = test.id;
    option.textContent = `#${test.id}${name} (${dateLabel})`;
    if (ResultsState.activeManualTest && ResultsState.activeManualTest.id === test.id) {
      option.selected = true;
    }
    select.appendChild(option);
  });

  if (baseline) {
    baseline.textContent = '';
    baseline.style.display = 'none';
  }
}

function updateStatusBadge(status) {
  const badge = document.getElementById('statusBadge');
  if (!badge) return;
  badge.classList.remove('running', 'paused', 'cancelled');
  if (status === 'running') {
    badge.classList.add('running');
    badge.innerHTML = '<span class="status-dot"></span>Running';
    return;
  }
  if (status === 'paused') {
    badge.classList.add('paused');
    badge.textContent = 'Paused';
    return;
  }
  if (status === 'cancelled') {
    badge.classList.add('cancelled');
    badge.textContent = 'Cancelled';
    return;
  }
  badge.textContent = status ? status.charAt(0).toUpperCase() + status.slice(1) : 'Idle';
}

function setText(id, value) {
  const el = document.getElementById(id);
  if (el) el.textContent = value ?? '-';
}

function setElementVisible(id, visible) {
  const el = document.getElementById(id);
  if (el) el.style.display = visible ? 'block' : 'none';
}

function setSettingRowVisible(rowId, visible) {
  const el = document.getElementById(rowId);
  if (el) el.style.display = visible ? '' : 'none';
}

function setAdaptiveWfaRowsVisible(visible) {
  setSettingRowVisible('wfa-cooldown-row', visible);
  setSettingRowVisible('wfa-max-oos-row', visible);
  setSettingRowVisible('wfa-min-trades-row', visible);
  setSettingRowVisible('wfa-check-interval-row', visible);
  setSettingRowVisible('wfa-cusum-row', visible);
  setSettingRowVisible('wfa-dd-mult-row', visible);
  setSettingRowVisible('wfa-inactivity-row', visible);
}

function setAdaptiveCooldownRowVisible(visible) {
  setSettingRowVisible('wfa-cooldown-row', visible);
}

function renderOptunaTable(results) {
  const tbody = document.querySelector('.data-table tbody');
  if (!tbody) return;

  tbody.innerHTML = '';

  const thead = document.querySelector('.data-table thead tr');
  const objectives = ResultsState.optuna.objectives || [];
  const constraints = ResultsState.optuna.constraints || [];
  const hasConstraints = constraints.some((c) => c && c.enabled);
  if (thead && window.OptunaResultsUI) {
    thead.innerHTML = window.OptunaResultsUI.buildTrialTableHeaders(objectives, hasConstraints);
  }

  const list = results || [];
  list.forEach((result, index) => {
    const temp = document.createElement('tbody');
    if (window.OptunaResultsUI) {
      temp.innerHTML = window.OptunaResultsUI.renderTrialRow(result, objectives, { hasConstraints }).trim();
    }
    const row = temp.firstElementChild || document.createElement('tr');
    row.className = 'clickable';
    row.dataset.index = index;
    const trialNumber = result.trial_number ?? (index + 1);
    row.dataset.trialNumber = trialNumber;

    const paramId = result.param_id
      || createParamId(result.params || {}, ResultsState.strategyConfig, ResultsState.fixedParams);

    const rankCell = row.querySelector('.rank');
    if (rankCell) rankCell.textContent = index + 1;
    const hashCell = row.querySelector('.param-hash');
    if (hashCell) hashCell.textContent = paramId;

      row.addEventListener('click', async () => {
        selectTableRow(index, trialNumber);
        await showParameterDetails({ ...result, param_id: paramId });
        setComparisonLine('');
        if (result.equity_curve && result.equity_curve.length) {
          renderEquityChart(result.equity_curve, null, result.timestamps);
          return;
        }
        const payload = await fetchEquityCurve(result);
        if (payload && payload.equity && payload.equity.length) {
          renderEquityChart(payload.equity, null, payload.timestamps);
        }
    });

    tbody.appendChild(row);
  });
}

function renderForwardTestTable(results) {
  const tbody = document.querySelector('.data-table tbody');
  if (!tbody) return;

  tbody.innerHTML = '';

  const thead = document.querySelector('.data-table thead tr');
  const objectives = ResultsState.optuna.objectives || [];
  const constraints = ResultsState.optuna.constraints || [];
  const hasConstraints = constraints.some((c) => c && c.enabled);
  if (thead && window.OptunaResultsUI) {
    thead.innerHTML = window.OptunaResultsUI.buildTrialTableHeaders(objectives, hasConstraints);
  }

  const ftSource = ResultsState.forwardTest?.source || 'optuna';
  let sourceRankMap = {};
  if (ftSource === 'dsr') {
    sourceRankMap = buildRankMapFromKey(ResultsState.results || [], 'dsr_rank');
  } else {
    (ResultsState.results || []).forEach((trial, idx) => {
      if (trial.trial_number !== undefined) {
        sourceRankMap[trial.trial_number] = idx + 1;
      }
    });
  }

  (results || []).forEach((trial, index) => {
    const mapped = {
      ...trial,
      net_profit_pct: trial.ft_net_profit_pct,
      max_drawdown_pct: trial.ft_max_drawdown_pct,
      total_trades: trial.ft_total_trades,
      win_rate: trial.ft_win_rate,
      max_consecutive_losses: trial.ft_max_consecutive_losses,
      sharpe_ratio: trial.ft_sharpe_ratio,
      sortino_ratio: trial.ft_sortino_ratio,
      romad: trial.ft_romad,
      profit_factor: trial.ft_profit_factor,
      ulcer_index: trial.ft_ulcer_index,
      sqn: trial.ft_sqn,
      consistency_score: trial.ft_consistency_score,
      score: null
    };

    const temp = document.createElement('tbody');
    if (window.OptunaResultsUI) {
      temp.innerHTML = window.OptunaResultsUI.renderTrialRow(mapped, objectives, { hasConstraints }).trim();
    }
    const row = temp.firstElementChild || document.createElement('tr');
    row.className = 'clickable';
    row.dataset.index = index;
    const trialNumber = trial.trial_number ?? (index + 1);
    row.dataset.trialNumber = trialNumber;

    const paramId = trial.param_id
      || createParamId(trial.params || {}, ResultsState.strategyConfig, ResultsState.fixedParams);

    const rankCell = row.querySelector('.rank');
    const displayedRank = trial.ft_rank || index + 1;
    const sourceRank = sourceRankMap[trialNumber];
    const rankChange = sourceRank != null ? sourceRank - displayedRank : null;
    if (rankCell) rankCell.innerHTML = formatRankCell(displayedRank, rankChange);
    const hashCell = row.querySelector('.param-hash');
    if (hashCell) hashCell.textContent = paramId;

    row.addEventListener('click', async () => {
      selectTableRow(index, trialNumber);
      await showParameterDetails({ ...trial, param_id: paramId });

      const comparison = window.PostProcessUI
        ? window.PostProcessUI.buildComparisonMetrics(trial)
        : null;
      const rankSourceLabel = ftSource === 'dsr' ? 'DSR' : 'Optuna';

      if (comparison) {
        const line = [
          rankChange !== null ? `Rank: ${formatSigned(rankChange, 0)} (vs ${rankSourceLabel})` : null,
          `Profit Deg: ${formatSigned(comparison.profit_degradation || 0, 2)}`,
          `Max DD: ${formatSigned(comparison.max_dd_change || 0, 2, '%')}`,
          `ROMAD: ${formatSigned(comparison.romad_change || 0, 2)}`,
          `Sharpe: ${formatSigned(comparison.sharpe_change || 0, 2)}`,
          `PF: ${formatSigned(comparison.pf_change || 0, 2)}`
        ].filter(Boolean).join(' | ');
        setComparisonLine(line);
      }

      const equity = await fetchEquityCurve(trial, {
        start: ResultsState.forwardTest.startDate,
        end: ResultsState.forwardTest.endDate
      });
      if (equity && equity.equity && equity.equity.length) {
        renderEquityChart(equity.equity, null, equity.timestamps);
      }
    });

    tbody.appendChild(row);
  });
}

function renderDsrTable(results) {
  const tbody = document.querySelector('.data-table tbody');
  if (!tbody) return;

  tbody.innerHTML = '';

  const thead = document.querySelector('.data-table thead tr');
  const objectives = ResultsState.optuna.objectives || [];
  const constraints = ResultsState.optuna.constraints || [];
  const hasConstraints = constraints.some((c) => c && c.enabled);
  if (thead && window.OptunaResultsUI) {
    thead.innerHTML = window.OptunaResultsUI.buildTrialTableHeaders(objectives, hasConstraints);
  }

  const optunaRankMap = {};
  (ResultsState.results || []).forEach((trial, idx) => {
    if (trial.trial_number !== undefined) {
      optunaRankMap[trial.trial_number] = idx + 1;
    }
  });

  (results || []).forEach((trial, index) => {
    const temp = document.createElement('tbody');
    if (window.OptunaResultsUI) {
      temp.innerHTML = window.OptunaResultsUI.renderTrialRow(trial, objectives, { hasConstraints }).trim();
    }
    const row = temp.firstElementChild || document.createElement('tr');
    row.className = 'clickable';
    row.dataset.index = index;
    const trialNumber = trial.trial_number ?? (index + 1);
    row.dataset.trialNumber = trialNumber;

    const paramId = trial.param_id
      || createParamId(trial.params || {}, ResultsState.strategyConfig, ResultsState.fixedParams);

    const rankCell = row.querySelector('.rank');
    const dsrRank = trial.dsr_rank || index + 1;
    const optunaRank = optunaRankMap[trialNumber];
    const rankDelta = optunaRank != null ? (optunaRank - dsrRank) : null;
    if (rankCell) rankCell.innerHTML = formatRankCell(dsrRank, rankDelta);
    const hashCell = row.querySelector('.param-hash');
    if (hashCell) hashCell.textContent = paramId;

    row.addEventListener('click', async () => {
      selectTableRow(index, trialNumber);
      await showParameterDetails({ ...trial, param_id: paramId });

      const rankLine = rankDelta !== null ? `Rank: ${formatSigned(rankDelta, 0)}` : null;

      const dsrValue = Number(trial.dsr_probability);
      const dsrLabel = Number.isFinite(dsrValue) ? dsrValue.toFixed(3) : 'N/A';
      const luckValue = Number(trial.dsr_luck_share_pct);
      const luckLabel = Number.isFinite(luckValue) ? `${luckValue.toFixed(1)}%` : 'N/A';

      const line = [
        rankLine,
        `DSR: ${dsrLabel}`,
        `Luck: ${luckLabel}`
      ].filter(Boolean).join(' | ');
      setComparisonLine(line);

      const equity = await fetchEquityCurve(trial);
      if (equity && equity.equity && equity.equity.length) {
        renderEquityChart(equity.equity, null, equity.timestamps);
      }
    });

    tbody.appendChild(row);
  });
}

function renderStressTestTable(results) {
  const tbody = document.querySelector('.data-table tbody');
  if (!tbody) return;

  tbody.innerHTML = '';

  const thead = document.querySelector('.data-table thead tr');
  const objectives = ResultsState.optuna.objectives || [];
  const constraints = ResultsState.optuna.constraints || [];
  const hasConstraints = constraints.some((c) => c && c.enabled);
  if (thead && window.OptunaResultsUI) {
    thead.innerHTML = window.OptunaResultsUI.buildTrialTableHeaders(objectives, hasConstraints);
  }

  const stSource = ResultsState.stressTest?.source || 'optuna';
  let sourceRankMap = {};
  if (stSource === 'ft') {
    sourceRankMap = buildRankMapFromKey(ResultsState.results || [], 'ft_rank');
  } else if (stSource === 'dsr') {
    sourceRankMap = buildRankMapFromKey(ResultsState.results || [], 'dsr_rank');
  } else {
    (ResultsState.results || []).forEach((trial, idx) => {
      if (trial.trial_number !== undefined) {
        sourceRankMap[trial.trial_number] = idx + 1;
      }
    });
  }

  (results || []).forEach((trial, index) => {
    const temp = document.createElement('tbody');
    if (window.OptunaResultsUI) {
      temp.innerHTML = window.OptunaResultsUI.renderTrialRow(trial, objectives, { hasConstraints }).trim();
    }
    const row = temp.firstElementChild || document.createElement('tr');
    row.className = 'clickable';
    row.dataset.index = index;
    const trialNumber = trial.trial_number ?? (index + 1);
    row.dataset.trialNumber = trialNumber;

    const paramId = trial.param_id
      || createParamId(trial.params || {}, ResultsState.strategyConfig, ResultsState.fixedParams);

    const stRank = trial.st_rank || index + 1;
    const rankCell = row.querySelector('.rank');
    const sourceRank = sourceRankMap[trialNumber];
    const rankDelta = sourceRank != null ? (sourceRank - stRank) : null;
    if (rankCell) rankCell.innerHTML = formatRankCell(stRank, rankDelta);
    const hashCell = row.querySelector('.param-hash');
    if (hashCell) {
      hashCell.textContent = paramId;
      if (trial.st_status && trial.st_status !== 'ok') {
        hashCell.classList.add('param-hash-warning');
      }
    }

    row.addEventListener('click', async () => {
      selectTableRow(index, trialNumber);
      await showParameterDetails({ ...trial, param_id: paramId });

      const rankSourceLabel = stSource === 'ft' ? 'FT' : (stSource === 'dsr' ? 'DSR' : 'Optuna');

      if (trial.st_status === 'skipped_bad_base') {
        const baseProfit = Number((trial.base_net_profit_pct ?? trial.net_profit_pct) || 0);
        const line = `Status: Bad Base (profit <= 0%) | Base Profit: ${baseProfit.toFixed(1)}%`;
        setComparisonLine(line);
      } else if (trial.st_status === 'insufficient_data') {
        const totalPerturbations = Number(trial.total_perturbations || 0);
        const combinedFailures = Number(trial.combined_failure_count || 0);
        const validNeighbors = totalPerturbations - combinedFailures;
        const line = `Status: Insufficient Data (${validNeighbors} valid neighbors, minimum 4 required) | Profit Ret: N/A | RoMaD Ret: N/A`;
        setComparisonLine(line);
      } else if (trial.st_status === 'skipped_no_params') {
        const line = 'Status: No Testable Parameters (strategy has only categorical params)';
        setComparisonLine(line);
      } else {
        const rankLine = rankDelta !== null ? `Rank: ${formatSigned(rankDelta, 0)} (vs ${rankSourceLabel})` : null;

        const profitRet = trial.profit_retention;
        const profitRetLabel = profitRet !== null && profitRet !== undefined
          ? `${(profitRet * 100).toFixed(1)}%`
          : 'N/A';

        const romadRet = trial.romad_retention;
        const romadRetLabel = romadRet !== null && romadRet !== undefined
          ? `${(romadRet * 100).toFixed(1)}%`
          : 'N/A';

        const failRate = trial.combined_failure_rate;
        const failRateLabel = failRate !== null && failRate !== undefined
          ? `${(failRate * 100).toFixed(1)}%`
          : 'N/A';

        const romadValid = trial.romad_failure_rate !== null && trial.romad_failure_rate !== undefined;
        const failRateType = romadValid ? 'Fail' : 'Fail (profit)';

        const sensParam = trial.most_sensitive_param || null;
        const sensLine = sensParam ? `Sens: ${sensParam}` : null;

        const line = [
          rankLine,
          `Profit Ret: ${profitRetLabel}`,
          `RoMaD Ret: ${romadRetLabel}`,
          `${failRateType}: ${failRateLabel}`,
          sensLine
        ].filter(Boolean).join(' | ');
        setComparisonLine(line);
      }

      const equity = await fetchEquityCurve(trial);
      if (equity && equity.equity && equity.equity.length) {
        renderEquityChart(equity.equity, null, equity.timestamps);
      }
    });

    tbody.appendChild(row);
  });
}

function renderOosTestTable(results) {
  const tbody = document.querySelector('.data-table tbody');
  if (!tbody) return;

  tbody.innerHTML = '';

  const thead = document.querySelector('.data-table thead tr');
  const objectives = ResultsState.optuna.objectives || [];
  const constraints = ResultsState.optuna.constraints || [];
  const hasConstraints = constraints.some((c) => c && c.enabled);
  if (thead && window.OptunaResultsUI) {
    thead.innerHTML = window.OptunaResultsUI.buildTrialTableHeaders(objectives, hasConstraints);
  }

  (results || []).forEach((trial, index) => {
    const source = trial.oos_test_source || ResultsState.oosTest.source || '';
    const baseMetrics = source === 'forward_test'
      ? {
        net_profit_pct: trial.ft_net_profit_pct,
        max_drawdown_pct: trial.ft_max_drawdown_pct,
        romad: trial.ft_romad,
        sharpe_ratio: trial.ft_sharpe_ratio,
        profit_factor: trial.ft_profit_factor
      }
      : {
        net_profit_pct: trial.net_profit_pct,
        max_drawdown_pct: trial.max_drawdown_pct,
        romad: trial.romad,
        sharpe_ratio: trial.sharpe_ratio,
        profit_factor: trial.profit_factor
      };

    const metrics = {
      net_profit_pct: trial.oos_test_net_profit_pct,
      max_drawdown_pct: trial.oos_test_max_drawdown_pct,
      total_trades: trial.oos_test_total_trades,
      win_rate: trial.oos_test_win_rate,
      max_consecutive_losses: trial.oos_test_max_consecutive_losses,
      sharpe_ratio: trial.oos_test_sharpe_ratio,
      sortino_ratio: trial.oos_test_sortino_ratio,
      romad: trial.oos_test_romad,
      profit_factor: trial.oos_test_profit_factor,
      ulcer_index: trial.oos_test_ulcer_index,
      sqn: trial.oos_test_sqn,
      consistency_score: trial.oos_test_consistency_score,
      score: null
    };
    const mapped = { ...trial, ...metrics };

    const temp = document.createElement('tbody');
    if (window.OptunaResultsUI) {
      temp.innerHTML = window.OptunaResultsUI.renderTrialRow(mapped, objectives, { hasConstraints }).trim();
    }
    const row = temp.firstElementChild || document.createElement('tr');
    row.className = 'clickable';
    row.dataset.index = index;
    row.dataset.trialNumber = trial.trial_number;

    const paramId = trial.param_id
      || createParamId(trial.params || {}, ResultsState.strategyConfig, ResultsState.fixedParams);

    const rankCell = row.querySelector('.rank');
    if (rankCell) rankCell.textContent = trial.oos_test_source_rank || index + 1;
    const hashCell = row.querySelector('.param-hash');
    if (hashCell) hashCell.textContent = paramId;

    row.addEventListener('click', async () => {
      selectTableRow(index, trial.trial_number);
      await showParameterDetails({ ...trial, param_id: paramId });

      const profitDeg = trial.oos_test_profit_degradation;
      const maxDdChange = Number(metrics.max_drawdown_pct || 0) - Number(baseMetrics.max_drawdown_pct || 0);
      const romadChange = Number(metrics.romad || 0) - Number(baseMetrics.romad || 0);
      const sharpeChange = Number(metrics.sharpe_ratio || 0) - Number(baseMetrics.sharpe_ratio || 0);
      const pfChange = Number(metrics.profit_factor || 0) - Number(baseMetrics.profit_factor || 0);

      const line = [
        profitDeg !== null && profitDeg !== undefined ? `Profit Deg: ${formatSigned(profitDeg, 2)}` : null,
        `Max DD: ${formatSigned(maxDdChange || 0, 2, '%')}`,
        `ROMAD: ${formatSigned(romadChange || 0, 2)}`,
        `Sharpe: ${formatSigned(sharpeChange || 0, 2)}`,
        `PF: ${formatSigned(pfChange || 0, 2)}`
      ].filter(Boolean).join(' | ');
      setComparisonLine(line);

      if (ResultsState.oosTest.startDate && ResultsState.oosTest.endDate) {
        const equity = await fetchEquityCurve(trial, {
          start: ResultsState.oosTest.startDate,
          end: ResultsState.oosTest.endDate
        });
        if (equity && equity.equity && equity.equity.length) {
          renderEquityChart(equity.equity, null, equity.timestamps);
        }
      }
    });

    tbody.appendChild(row);
  });
}

function renderManualTestTable(results) {
  const tbody = document.querySelector('.data-table tbody');
  if (!tbody) return;

  tbody.innerHTML = '';

  const thead = document.querySelector('.data-table thead tr');
  const objectives = ResultsState.optuna.objectives || [];
  const constraints = ResultsState.optuna.constraints || [];
  const hasConstraints = constraints.some((c) => c && c.enabled);
  if (thead && window.OptunaResultsUI) {
    thead.innerHTML = window.OptunaResultsUI.buildTrialTableHeaders(objectives, hasConstraints);
  }

  const trialMap = {};
  (ResultsState.results || []).forEach((trial) => {
    if (trial.trial_number !== undefined) {
      trialMap[trial.trial_number] = trial;
    }
  });

  (results || []).forEach((entry, index) => {
    const trialNumber = entry.trial_number;
    const baseTrial = trialMap[trialNumber] || {};
    const metrics = entry.test_metrics || {};
    const mapped = {
      ...baseTrial,
      net_profit_pct: metrics.net_profit_pct,
      max_drawdown_pct: metrics.max_drawdown_pct,
      total_trades: metrics.total_trades,
      win_rate: metrics.win_rate,
      max_consecutive_losses: metrics.max_consecutive_losses,
      sharpe_ratio: metrics.sharpe_ratio,
      sortino_ratio: metrics.sortino_ratio,
      romad: metrics.romad,
      profit_factor: metrics.profit_factor,
      ulcer_index: metrics.ulcer_index,
      sqn: metrics.sqn,
      consistency_score: metrics.consistency_score,
      score: null
    };

    const temp = document.createElement('tbody');
    if (window.OptunaResultsUI) {
      temp.innerHTML = window.OptunaResultsUI.renderTrialRow(mapped, objectives, { hasConstraints }).trim();
    }
    const row = temp.firstElementChild || document.createElement('tr');
    row.className = 'clickable';
    row.dataset.index = index;
    row.dataset.trialNumber = trialNumber;

    const paramId = baseTrial.param_id
      || createParamId(baseTrial.params || {}, ResultsState.strategyConfig, ResultsState.fixedParams);

    const rankCell = row.querySelector('.rank');
    if (rankCell) rankCell.textContent = index + 1;
    const hashCell = row.querySelector('.param-hash');
    if (hashCell) hashCell.textContent = paramId;

    row.addEventListener('click', async () => {
      selectTableRow(index, trialNumber);
      await showParameterDetails({ ...baseTrial, param_id: paramId });

      const comparison = entry.comparison || {};
      const line = [
        `Profit Deg: ${formatSigned(comparison.profit_degradation || 0, 2)}`,
        `Max DD: ${formatSigned(comparison.max_dd_change || 0, 2, '%')}`,
        `ROMAD: ${formatSigned(comparison.romad_change || 0, 2)}`,
        `Sharpe: ${formatSigned(comparison.sharpe_change || 0, 2)}`,
        `PF: ${formatSigned(comparison.pf_change || 0, 2)}`
      ].filter(Boolean).join(' | ');
      setComparisonLine(line);

      if (ResultsState.activeManualTest && ResultsState.activeManualTest.config) {
        const config = ResultsState.activeManualTest.config;
        const equity = await fetchEquityCurve(baseTrial, {
          start: config.start_date,
          end: config.end_date
        });
        if (equity && equity.equity && equity.equity.length) {
          renderEquityChart(equity.equity, null, equity.timestamps);
        }
      }
    });

    tbody.appendChild(row);
  });
}

function renderWFATable(windows) {
  const tbody = document.querySelector('.data-table tbody');
  if (!tbody) return;

  tbody.innerHTML = '';

  const thead = document.querySelector('.data-table thead tr');
  if (thead) {
    thead.innerHTML = `
      <th>#</th>
      <th>Param ID</th>
      <th>IS Profit %</th>
      <th>OOS Profit %</th>
      <th>IS Trades</th>
      <th>OOS Trades</th>
      <th>OOS DD %</th>
    `;
  }

  (windows || []).forEach((window, index) => {
    const row = document.createElement('tr');
    row.className = 'clickable';
    row.dataset.index = index;
    row.dataset.windowNumber = window.window_number || index + 1;

    row.innerHTML = `
      <td class="rank">${window.window_number || index + 1}</td>
      <td class="param-hash">${window.param_id}</td>
      <td class="${window.is_net_profit_pct >= 0 ? 'val-positive' : 'val-negative'}">
        ${window.is_net_profit_pct >= 0 ? '+' : ''}${Number(window.is_net_profit_pct || 0).toFixed(2)}%
      </td>
      <td class="${window.oos_net_profit_pct >= 0 ? 'val-positive' : 'val-negative'}">
        ${window.oos_net_profit_pct >= 0 ? '+' : ''}${Number(window.oos_net_profit_pct || 0).toFixed(2)}%
      </td>
      <td>${window.is_total_trades ?? '-'}</td>
      <td>${window.oos_total_trades ?? '-'}</td>
      <td class="val-negative">-${Math.abs(Number(window.oos_max_drawdown_pct || 0)).toFixed(2)}%</td>
    `;

      row.addEventListener('click', async () => {
        const windowNumber = window.window_number || window.window_id || index + 1;
        selectTableRow(index, windowNumber);
        await showParameterDetails(window);
        setComparisonLine('');
      });

    tbody.appendChild(row);
  });
}

function selectTableRow(index, rowId) {
  document.querySelectorAll('.data-table tr.clickable').forEach((row) => {
    row.classList.remove('selected');
  });
  const rows = document.querySelectorAll('.data-table tr.clickable');
  if (rows[index]) {
    rows[index].classList.add('selected');
  }
  ResultsState.selectedRowId = rowId;
}

async function showParameterDetails(result) {
  const section = document.getElementById('paramDetailsSection');
  const title = document.getElementById('paramDetailsTitle');
  const content = document.getElementById('paramDetailsContent');

  if (!section || !content) return;

  if ((!ResultsState.strategyConfig || !ResultsState.strategyConfig.parameters) && ResultsState.strategyId) {
    try {
      ResultsState.strategyConfig = await fetchStrategyConfig(ResultsState.strategyId);
    } catch (error) {
      console.warn('Failed to load strategy config for parameter ordering', error);
    }
  }

  const label = result.param_id
    || createParamId(result.params || result.best_params || {}, ResultsState.strategyConfig, ResultsState.fixedParams);
  if (title) {
    title.textContent = `Parameters: ${label}`;
  }

  const params = result.params || result.best_params || {};
  content.innerHTML = '';

  const orderedKeys = getParamDisplayOrder(params, ResultsState.strategyConfig);
  const paramDefs = ResultsState.strategyConfig?.parameters || {};
  const groupOrder = ResultsState.strategyConfig?.group_order || [];
  const groups = {};

  orderedKeys.forEach((key) => {
    if (['dateFilter', 'start', 'end'].includes(key)) return;
    const def = paramDefs[key];
    const group = (def && def.group) || 'Other';
    if (!groups[group]) {
      groups[group] = [];
    }
    groups[group].push({ key, value: params[key], def });
  });

  const orderedGroups = [];
  groupOrder.forEach((group) => {
    if (groups[group] && groups[group].length) {
      orderedGroups.push(group);
    }
  });
  Object.keys(groups).forEach((group) => {
    if (!orderedGroups.includes(group)) {
      orderedGroups.push(group);
    }
  });

  orderedGroups.forEach((group) => {
    const items = groups[group] || [];
    if (!items.length) return;
    const card = document.createElement('div');
    card.className = 'param-group-card';

    const header = document.createElement('div');
    header.className = 'param-group-title';
    header.textContent = group;
    card.appendChild(header);

    items.forEach(({ key, value, def }) => {
      const formattedValue = formatParamValue(value);
      const labelText = (def && def.label) ? def.label : formatParamName(key);
      const optimized = def && def.optimize && def.optimize.enabled === true;
      const item = document.createElement('div');
      item.className = `param-item${optimized ? '' : ' param-fixed'}`;

      const nameEl = document.createElement('span');
      nameEl.className = 'param-item-name';
      nameEl.textContent = labelText;

      const valueEl = document.createElement('span');
      valueEl.className = 'param-item-value';
      valueEl.textContent = formattedValue;

      const copyHandler = () => {
        copyParamValue(formattedValue);
        highlightParamItem(item);
      };
      nameEl.addEventListener('click', copyHandler);
      valueEl.addEventListener('click', copyHandler);

      item.appendChild(nameEl);
      item.appendChild(valueEl);
      card.appendChild(item);
    });

    content.appendChild(card);
  });

  section.classList.add('show');
}

function getParamDisplayOrder(params, strategyConfig) {
  const configParams = strategyConfig?.parameters || {};
  const configKeys = strategyConfig?.parameter_order || Object.keys(configParams);
  const paramsKeys = Object.keys(params || {});
  const ordered = [];
  const seen = new Set();

  configKeys.forEach((key) => {
    if (Object.prototype.hasOwnProperty.call(params, key)) {
      ordered.push(key);
      seen.add(key);
    }
  });

  paramsKeys.forEach((key) => {
    if (!seen.has(key)) {
      ordered.push(key);
    }
  });

  return ordered;
}

function copyParamValue(value) {
  const text = value === undefined || value === null ? '' : String(value);
  if (!text) return;
  if (navigator.clipboard && navigator.clipboard.writeText) {
    navigator.clipboard.writeText(text).catch(() => {});
    return;
  }
  const temp = document.createElement('textarea');
  temp.value = text;
  temp.setAttribute('readonly', '');
  temp.style.position = 'absolute';
  temp.style.left = '-9999px';
  document.body.appendChild(temp);
  temp.select();
  try {
    document.execCommand('copy');
  } catch (error) {
    // ignore
  }
  document.body.removeChild(temp);
}

function highlightParamItem(item) {
  if (!item) return;
  item.classList.add('copied');
  window.setTimeout(() => {
    item.classList.remove('copied');
  }, 800);
}

function renderEquityChart(equityData, windowBoundaries = null, timestamps = null, options = null) {
  const svg = document.querySelector('.chart-svg');
  const axis = document.getElementById('equityAxis');
  if (!svg || !equityData || equityData.length === 0) return;

  const width = 800;
  const height = 260;
  const padding = 20;
  const useTimeScaleRequested = Boolean(options && options.useTimeScale);
  const hasTimestamps = Array.isArray(timestamps) && timestamps.length === equityData.length;
  const pointCount = equityData.length;
  const denom = Math.max(1, pointCount - 1);
  let useTimeScale = false;
  let timeStart = null;
  let timeEnd = null;

  if (useTimeScaleRequested && hasTimestamps) {
    const start = Date.parse(timestamps[0]);
    const end = Date.parse(timestamps[timestamps.length - 1]);
    if (Number.isFinite(start) && Number.isFinite(end) && end > start) {
      useTimeScale = true;
      timeStart = start;
      timeEnd = end;
    }
  }

  const getIndexRatio = (index) => {
    return index / denom;
  };

  const getTimeRatio = (timeValue) => {
    if (!useTimeScale) return null;
    const t = Date.parse(timeValue);
    if (!Number.isFinite(t)) return null;
    const ratio = (t - timeStart) / (timeEnd - timeStart);
    return Math.min(1, Math.max(0, ratio));
  };

  const getRatioForIndex = (index) => {
    if (useTimeScale) {
      const ratio = getTimeRatio(timestamps[index]);
      if (ratio !== null) return ratio;
    }
    return getIndexRatio(index);
  };

  const getRatioForBoundary = (boundary) => {
    if (useTimeScale) {
      let timeValue = boundary ? (boundary.time || boundary.timestamp || boundary.date) : null;
      if (!timeValue && boundary && boundary.index !== undefined && boundary.index !== null) {
        if (hasTimestamps && boundary.index >= 0 && boundary.index < timestamps.length) {
          timeValue = timestamps[boundary.index];
        }
      }
      const ratio = getTimeRatio(timeValue);
      if (ratio !== null) return ratio;
    }
    if (!boundary || boundary.index === undefined || boundary.index === null) return null;
    return getIndexRatio(boundary.index);
  };

  const baseValue = 100.0;
  const minValue = Math.min(...equityData, baseValue);
  const maxValue = Math.max(...equityData, baseValue);
  const valueRange = maxValue - minValue || 1;

  svg.innerHTML = '';
  if (axis) {
    axis.innerHTML = '';
  }

  const bg = document.createElementNS('http://www.w3.org/2000/svg', 'rect');
  bg.setAttribute('width', '100%');
  bg.setAttribute('height', '100%');
  bg.setAttribute('fill', '#fafafa');
  svg.appendChild(bg);

  const baseY = height - padding - ((baseValue - minValue) / valueRange) * (height - 2 * padding);
  const baseLine = document.createElementNS('http://www.w3.org/2000/svg', 'line');
  baseLine.setAttribute('x1', 0);
  baseLine.setAttribute('y1', baseY);
  baseLine.setAttribute('x2', width);
  baseLine.setAttribute('y2', baseY);
  baseLine.setAttribute('stroke', '#c8c8c8');
  baseLine.setAttribute('stroke-width', '1');
  baseLine.setAttribute('stroke-dasharray', '3 4');
  svg.appendChild(baseLine);

  if (windowBoundaries && windowBoundaries.length > 0) {
    const boundaryPositions = windowBoundaries.map((boundary) => {
      const ratio = getRatioForBoundary(boundary);
      if (ratio === null) return null;
      return ratio * width;
    });

    windowBoundaries.forEach((boundary, index) => {
      const x = boundaryPositions[index];
      if (x === null) return;
      const line = document.createElementNS('http://www.w3.org/2000/svg', 'line');
      line.setAttribute('x1', x);
      line.setAttribute('y1', 0);
      line.setAttribute('x2', x);
      line.setAttribute('y2', height);
      line.setAttribute('stroke', '#a9c9ff');
      line.setAttribute('stroke-width', '2');
      line.setAttribute('stroke-dasharray', '6 4');
      svg.appendChild(line);

      if (boundary.windowId !== undefined && boundary.windowId !== null) {
        const nextX = boundaryPositions[index + 1];
        const labelX = nextX !== null && nextX !== undefined
          ? (x + nextX) / 2
          : x + 40;

        const text = document.createElementNS('http://www.w3.org/2000/svg', 'text');
        text.setAttribute('x', labelX);
        text.setAttribute('y', 20);
        text.setAttribute('font-size', '10');
        text.setAttribute('fill', '#999');
        text.setAttribute('text-anchor', 'middle');
        text.textContent = `W${index + 1}`;
        svg.appendChild(text);
      }
    });
  }

  if (Array.isArray(timestamps) && timestamps.length === equityData.length && axis) {
    const startDate = new Date(timestamps[0]);
    const endDate = new Date(timestamps[timestamps.length - 1]);
    if (!Number.isNaN(startDate.getTime()) && !Number.isNaN(endDate.getTime())) {
      const tickCount = Math.min(5, equityData.length);
      if (tickCount >= 2) {
        for (let i = 0; i < tickCount; i += 1) {
          const ratio = tickCount === 1 ? 0 : i / (tickCount - 1);
          let tickDate = null;
          let xPct = 0;
          let x = 0;
          if (useTimeScale) {
            const tickTime = timeStart + ratio * (timeEnd - timeStart);
            tickDate = new Date(tickTime);
            xPct = ratio * 100;
            x = ratio * width;
          } else {
            const index = Math.round(ratio * (equityData.length - 1));
            tickDate = new Date(timestamps[index]);
            xPct = (index / (equityData.length - 1)) * 100;
            x = (index / (equityData.length - 1)) * width;
          }
          if (!tickDate || Number.isNaN(tickDate.getTime())) continue;

          const grid = document.createElementNS('http://www.w3.org/2000/svg', 'line');
          grid.setAttribute('x1', x);
          grid.setAttribute('y1', 0);
          grid.setAttribute('x2', x);
          grid.setAttribute('y2', height);
          grid.setAttribute('stroke', '#e3e3e3');
          grid.setAttribute('stroke-width', '1');
          svg.appendChild(grid);

          const m = String(tickDate.getUTCMonth() + 1).padStart(2, '0');
          const d = String(tickDate.getUTCDate()).padStart(2, '0');
          const label = `${m}.${d}`;

          const text = document.createElement('div');
          let labelClass = 'chart-axis-label';
          if (i === 0) {
            labelClass += ' start';
          } else if (i === tickCount - 1) {
            labelClass += ' end';
          }
          text.className = labelClass;
          text.style.left = `${xPct}%`;
          text.textContent = label;
          axis.appendChild(text);
        }
      }
    }
  }

  const points = equityData.map((value, index) => {
    const x = getRatioForIndex(index) * width;
    const y = height - padding - ((value - minValue) / valueRange) * (height - 2 * padding);
    return `${x},${y}`;
  }).join(' ');

  const polyline = document.createElementNS('http://www.w3.org/2000/svg', 'polyline');
  polyline.setAttribute('points', points);
  polyline.setAttribute('fill', 'none');
  polyline.setAttribute('stroke', '#4a90e2');
  polyline.setAttribute('stroke-width', '1.5');
  svg.appendChild(polyline);
}

function calculateWindowBoundaries(windows, stitchedOOS) {
  if (!windows || !stitchedOOS || !stitchedOOS.window_ids) return [];
  const boundaries = [];
  let lastWindowId = null;
  stitchedOOS.window_ids.forEach((windowId, index) => {
    if (windowId !== lastWindowId) {
      boundaries.push({ index, windowId });
      lastWindowId = windowId;
    }
  });
  return boundaries;
}

function calculateWindowBoundariesByDate(windows, timestamps) {
  if (!windows || !windows.length || !Array.isArray(timestamps) || !timestamps.length) return [];
  const boundaries = [];
  windows.forEach((window, index) => {
    const windowId = window.window_number || window.window_id || index + 1;
    const boundaryDate = window.oos_start_date || window.oos_start || window.is_end_date;
    if (!boundaryDate) return;
    boundaries.push({ time: boundaryDate, windowId });
  });
  return boundaries;
}

function getFiniteNumber(value) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

function formatSignedPercent(value, digits = 2) {
  const number = getFiniteNumber(value);
  if (number === null) return 'N/A';
  const sign = number > 0 ? '+' : (number < 0 ? '-' : '');
  return `${sign}${Math.abs(number).toFixed(digits)}%`;
}

function formatNegativePercent(value, digits = 2) {
  const number = getFiniteNumber(value);
  if (number === null) return 'N/A';
  return `-${Math.abs(number).toFixed(digits)}%`;
}

function formatUnsignedPercent(value, digits = 1) {
  const number = getFiniteNumber(value);
  if (number === null) return 'N/A';
  return `${number.toFixed(digits)}%`;
}

function displaySummaryCards(stitchedOOS) {
  const container = document.querySelector('.summary-row');
  if (!container) return;

  const netProfit = getFiniteNumber(stitchedOOS.final_net_profit_pct);
  const maxDrawdown = getFiniteNumber(stitchedOOS.max_drawdown_pct);
  const wfe = getFiniteNumber(stitchedOOS.wfe);
  const netClass = netProfit === null ? '' : (netProfit >= 0 ? 'positive' : 'negative');

  const totalTradesRaw = getFiniteNumber(stitchedOOS.total_trades);
  const totalTrades = totalTradesRaw === null ? null : Math.max(0, Math.round(totalTradesRaw));
  const winningTradesRaw = getFiniteNumber(stitchedOOS.winning_trades);
  let winningTrades = winningTradesRaw === null ? null : Math.max(0, Math.round(winningTradesRaw));
  if (winningTrades !== null && totalTrades !== null) {
    winningTrades = Math.min(winningTrades, totalTrades);
  } else if (winningTrades === null && totalTrades === 0) {
    winningTrades = 0;
  }
  const totalTradesText = totalTrades !== null
    ? `${winningTrades !== null ? winningTrades : 'N/A'}/${totalTrades}`
    : (winningTrades !== null ? `${winningTrades}/N/A` : 'N/A');

  const profitableWindowsRaw = getFiniteNumber(stitchedOOS.profitable_windows);
  const totalWindowsRaw = getFiniteNumber(stitchedOOS.total_windows);
  const profitableWindows = profitableWindowsRaw === null ? null : Math.max(0, Math.round(profitableWindowsRaw));
  const totalWindows = totalWindowsRaw === null ? null : Math.max(0, Math.round(totalWindowsRaw));
  const oosWinsPctRaw = getFiniteNumber(stitchedOOS.oos_win_rate);
  const oosWinsPct = oosWinsPctRaw !== null
    ? oosWinsPctRaw
    : (profitableWindows !== null && totalWindows !== null && totalWindows > 0
      ? (profitableWindows / totalWindows) * 100
      : 0);
  const oosWinsText = (profitableWindows !== null && totalWindows !== null)
    ? `${Math.min(profitableWindows, totalWindows)}/${totalWindows} (${Math.round(totalWindows > 0 ? oosWinsPct : 0)}%)`
    : (oosWinsPctRaw !== null ? `${Math.round(oosWinsPctRaw)}%` : 'N/A');

  const medianWindowProfit = getFiniteNumber(stitchedOOS.median_window_profit);
  const medianWindowWr = getFiniteNumber(stitchedOOS.median_window_wr);
  const medianProfitClass = medianWindowProfit === null ? '' : (medianWindowProfit >= 0 ? 'positive' : 'negative');

  container.innerHTML = `
    <div class="summary-card">
      <div class="value ${netClass}">
        ${formatSignedPercent(netProfit, 2)}
      </div>
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
      <div class="value ${medianProfitClass}">
        ${formatSignedPercent(medianWindowProfit, 1)}
      </div>
      <div class="label">OOS PROFIT (MED)</div>
    </div>
    <div class="summary-card">
      <div class="value">${formatUnsignedPercent(medianWindowWr, 1)}</div>
      <div class="label">OOS WIN RATE (MED)</div>
    </div>
  `;

  container.style.display = 'grid';
}

function formatPercentWithOptionalSign(value, digits = 1) {
  const number = getFiniteNumber(value);
  if (number === null) return '-';
  const sign = number > 0 ? '+' : (number < 0 ? '-' : '');
  return `${sign}${Math.abs(number).toFixed(digits)}%`;
}

function formatPostProcessActionLabel(action) {
  const normalized = String(action || '').trim().toLowerCase();
  if (normalized === 'cooldown_reoptimize') return 'CD + Re-opt';
  if (normalized === 'no_trade') return 'No Trade';
  return formatTitleFromKey(normalized) || '-';
}

function formatCompactPostProcessSortMetricLabel(metric) {
  const normalized = String(metric || '').trim().toLowerCase();
  const compactLabels = {
    profit_degradation: 'Profit Deg',
    profit_retention: 'Profit Ret',
    romad_retention: 'RoMaD Ret'
  };
  return compactLabels[normalized] || formatSortMetricLabel(normalized) || '-';
}

function renderSidebarSettingsList(containerId, rows) {
  const container = document.getElementById(containerId);
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
    val.textContent = String(row.val ?? '-');

    item.appendChild(key);
    item.appendChild(val);
    container.appendChild(item);
  });
}

function buildPostProcessSettingsRows(postProcessConfig, isWfaMode) {
  const config = postProcessConfig && typeof postProcessConfig === 'object'
    ? postProcessConfig
    : {};
  const rows = [];
  const stressConfig = config.stressTest && typeof config.stressTest === 'object'
    ? config.stressTest
    : {};

  if (config.enabled) {
    const ftParts = [
      `${config.ftPeriodDays ?? '-'}d`,
      `Top ${config.topK ?? '-'}`,
      `Sort: ${formatCompactPostProcessSortMetricLabel(config.sortMetric)}`,
      `Threshold: ${formatPercentWithOptionalSign(config.ftThresholdPct, 1)}`,
    ];
    if (isWfaMode) {
      const rejectAction = String(config.ftRejectAction || '').trim().toLowerCase();
      ftParts.push(`Policy: ${formatPostProcessActionLabel(rejectAction)}`);
      if (rejectAction === 'cooldown_reoptimize') {
        ftParts.push(`CD ${config.ftRejectCooldownDays ?? '-'}d`);
        ftParts.push(`Retry ${config.ftRejectMaxAttempts ?? '-'}`);
        ftParts.push(`Min OOS ${config.ftRejectMinRemainingOosDays ?? '-'}d`);
      }
    }
    rows.push({
      key: 'Forward Test',
      val: ftParts.join(', '),
    });
  }

  if (config.dsrEnabled) {
    rows.push({
      key: 'DSR',
      val: `Top ${config.dsrTopK ?? '-'}`,
    });
  }

  if (stressConfig.enabled) {
    const failureThresholdRaw = getFiniteNumber(stressConfig.failureThreshold);
    const failureThresholdPct = failureThresholdRaw === null
      ? '-'
      : `${((failureThresholdRaw > 1 ? failureThresholdRaw : failureThresholdRaw * 100)).toFixed(1)}%`;
    rows.push({
      key: 'Stress Test',
      val: `Top ${stressConfig.topK ?? '-'}, Failure: ${failureThresholdPct}, Sort: ${formatCompactPostProcessSortMetricLabel(stressConfig.sortMetric)}`,
    });
  }

  return rows;
}

function updateSidebarSettings() {
  setText('optuna-objectives', formatObjectivesList(ResultsState.optuna.objectives || []));
  setText('optuna-primary', ResultsState.optuna.primaryObjective ? formatObjectiveLabel(ResultsState.optuna.primaryObjective) : '-');
  setText('optuna-constraints', formatConstraintsSummary(ResultsState.optuna.constraints || []));
  const budgetMode = ResultsState.optuna.budgetMode || '';
  let budgetLabel = '-';
  if (budgetMode === 'trials') {
    budgetLabel = `${ResultsState.optuna.nTrials || 0} trials`;
  } else if (budgetMode === 'time') {
    const minutes = Math.round((ResultsState.optuna.timeLimit || 0) / 60);
    budgetLabel = `${minutes} min`;
  } else if (budgetMode === 'convergence') {
    budgetLabel = `No improvement ${ResultsState.optuna.convergence || 0} trials`;
  }
  setText('optuna-budget', budgetLabel);
  setText('optuna-sampler', (ResultsState.optuna.sampler || '').toUpperCase() || '-');
  setText('optuna-pruner', ResultsState.optuna.pruner ? ResultsState.optuna.pruner : '-');
  const warmupTrials = ResultsState.optuna.warmupTrials;
  const coverageMode = Boolean(ResultsState.optuna.coverageMode);
  if (warmupTrials !== null && warmupTrials !== undefined) {
    const initialValue = Number.isFinite(Number(warmupTrials))
      ? String(Math.max(0, Math.round(Number(warmupTrials))))
      : String(warmupTrials);
    setText('optuna-initial', coverageMode ? `${initialValue} (coverage)` : initialValue);
  } else {
    setText('optuna-initial', '-');
  }
  const sanitizeEnabled = ResultsState.optuna.sanitizeEnabled;
  const sanitizeThresholdRaw = ResultsState.optuna.sanitizeTradesThreshold;
  const sanitizeThreshold = Number.isFinite(Number(sanitizeThresholdRaw))
    ? Math.max(0, Math.round(Number(sanitizeThresholdRaw)))
    : 0;
  let sanitizeLabel = '-';
  if (sanitizeEnabled === true) {
    sanitizeLabel = `On (<= ${sanitizeThreshold})`;
  } else if (sanitizeEnabled === false) {
    sanitizeLabel = 'Off';
  }
  setText('optuna-sanitize', sanitizeLabel);
  const filterMinProfit = ResultsState.optuna.filterMinProfit;
  const minProfitThresholdRaw = ResultsState.optuna.minProfitThreshold;
  const minProfitThreshold = Number.isFinite(Number(minProfitThresholdRaw))
    ? Math.max(0, Math.round(Number(minProfitThresholdRaw)))
    : null;
  const scoreFilterEnabled = ResultsState.optuna.scoreFilterEnabled;
  const scoreThresholdRaw = ResultsState.optuna.scoreThreshold;
  const scoreThreshold = Number.isFinite(Number(scoreThresholdRaw))
    ? Math.max(0, Math.round(Number(scoreThresholdRaw)))
    : null;
  const filterParts = [];
  if (filterMinProfit) {
    filterParts.push(`Net Profit = ${minProfitThreshold !== null ? minProfitThreshold : 0}`);
  }
  if (scoreFilterEnabled) {
    filterParts.push(`Score = ${scoreThreshold !== null ? scoreThreshold : 0}`);
  }
  const filterLabel = filterParts.length ? filterParts.join(', ') : 'Off';
  setText('optuna-filter', filterLabel);
  setText('optuna-workers', ResultsState.optuna.workers ?? '-');
  const optimizationTime = ResultsState.optuna.optimizationTimeSeconds;
  const timeLabel = ResultsState.mode === 'wfa' ? '-' : (formatDuration(optimizationTime) || '-');
  setText('optuna-time', timeLabel);

  const postProcessRows = buildPostProcessSettingsRows(ResultsState.postProcess, ResultsState.mode === 'wfa');
  renderSidebarSettingsList('post-process-settings-list', postProcessRows);
  setElementVisible('post-process-settings-section', postProcessRows.length > 0);

  if (ResultsState.mode === 'wfa') {
    setElementVisible('wfa-progress-section', true);
    setElementVisible('wfa-settings-section', true);
    setText('wfa-is-days', ResultsState.wfa.isPeriodDays ?? ResultsState.wfa.is_period_days ?? '-');
    setText('wfa-oos-days', ResultsState.wfa.oosPeriodDays ?? ResultsState.wfa.oos_period_days ?? '-');
    const adaptiveModeRaw = ResultsState.wfa.adaptiveMode ?? ResultsState.wfa.adaptive_mode;
    const adaptiveModeLabel = adaptiveModeRaw === null || adaptiveModeRaw === undefined
      ? '-'
      : (Boolean(adaptiveModeRaw) ? 'On' : 'Off');
    const cooldownEnabledRaw = ResultsState.wfa.cooldownEnabled ?? ResultsState.wfa.cooldown_enabled;
    const cooldownEnabled = cooldownEnabledRaw === null || cooldownEnabledRaw === undefined
      ? null
      : Boolean(cooldownEnabledRaw);
    const cooldownDays = ResultsState.wfa.cooldownDays ?? ResultsState.wfa.cooldown_days;
    setText('wfa-adaptive-mode', adaptiveModeLabel);
    setAdaptiveWfaRowsVisible(Boolean(adaptiveModeRaw));
    setAdaptiveCooldownRowVisible(Boolean(adaptiveModeRaw) && Boolean(cooldownEnabled));
    setText(
      'wfa-cooldown-days',
      cooldownEnabled
        ? `${Math.max(1, Math.round(Number(cooldownDays || 15)))}d`
        : '-'
    );
    setText('wfa-max-oos-days', ResultsState.wfa.maxOosPeriodDays ?? ResultsState.wfa.max_oos_period_days ?? '-');
    setText('wfa-min-trades', ResultsState.wfa.minOosTrades ?? ResultsState.wfa.min_oos_trades ?? '-');
    setText('wfa-check-interval', ResultsState.wfa.checkIntervalTrades ?? ResultsState.wfa.check_interval_trades ?? '-');
    setText(
      'wfa-cusum-h',
      (ResultsState.wfa.cusumThreshold ?? ResultsState.wfa.cusum_threshold) !== null
      && (ResultsState.wfa.cusumThreshold ?? ResultsState.wfa.cusum_threshold) !== undefined
        ? Number(ResultsState.wfa.cusumThreshold ?? ResultsState.wfa.cusum_threshold).toFixed(2)
        : '-'
    );
    setText(
      'wfa-dd-mult',
      (ResultsState.wfa.ddThresholdMultiplier ?? ResultsState.wfa.dd_threshold_multiplier) !== null
      && (ResultsState.wfa.ddThresholdMultiplier ?? ResultsState.wfa.dd_threshold_multiplier) !== undefined
        ? Number(ResultsState.wfa.ddThresholdMultiplier ?? ResultsState.wfa.dd_threshold_multiplier).toFixed(2)
        : '-'
    );
    setText(
      'wfa-inactivity-mult',
      (ResultsState.wfa.inactivityMultiplier ?? ResultsState.wfa.inactivity_multiplier) !== null
      && (ResultsState.wfa.inactivityMultiplier ?? ResultsState.wfa.inactivity_multiplier) !== undefined
        ? Number(ResultsState.wfa.inactivityMultiplier ?? ResultsState.wfa.inactivity_multiplier).toFixed(2)
        : '-'
    );
    setText('wfa-run-time', formatDuration(ResultsState.wfa.runTimeSeconds) || '-');
  } else {
    setElementVisible('wfa-progress-section', false);
    setElementVisible('wfa-settings-section', false);
    setAdaptiveWfaRowsVisible(true);
    setAdaptiveCooldownRowVisible(true);
  }

  setText('strategy-name', ResultsState.strategy.name || ResultsState.strategyId || '-');
  setText('strategy-version', ResultsState.strategy.version || '-');
  setText('strategy-dataset', ResultsState.dataset.label || '-');
}

function renderWindowIndicators(total) {
  const container = document.querySelector('.window-indicator');
  if (!container) return;
  container.innerHTML = '';
  const count = total || 0;
  for (let i = 0; i < count; i += 1) {
    const dot = document.createElement('div');
    dot.className = 'window-dot completed';
    container.appendChild(dot);
  }
}
