(function () {
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
    composite_score: 'Composite Score'
  };

  function formatObjectiveLabel(key) {
    return OBJECTIVE_LABELS[key] || key;
  }

  function formatNumber(value, digits = 2) {
    if (value === null || value === undefined || Number.isNaN(Number(value))) {
      if (value === 'inf' || value === '+inf' || value === 'Infinity') return 'Inf';
      if (value === '-inf' || value === '-Infinity') return '-Inf';
      if (value === 'nan' || value === 'NaN') return 'NaN';
      return 'N/A';
    }
    const num = Number(value);
    if (!Number.isFinite(num)) {
      return num > 0 ? 'Inf' : '-Inf';
    }
    return num.toFixed(digits);
  }

  function buildTrialTableHeaders(objectives, hasConstraints) {
    const columns = [];
    columns.push('<th>#</th>');
    columns.push('<th>Param ID</th>');
    columns.push('<th>P</th>');
    if (hasConstraints) {
      columns.push('<th>C</th>');
    }

    const objectiveList = Array.isArray(objectives) ? objectives : [];
    const objectiveSet = new Set(objectiveList);
    const rawMetricColumns = new Set([
      'win_rate',
      'net_profit_pct',
      'max_drawdown_pct',
      'max_consecutive_losses',
      'romad',
      'sharpe_ratio',
      'profit_factor',
      'ulcer_index',
      'sqn',
      'consistency_score'
    ]);
    objectiveList.forEach((objective) => {
      if (rawMetricColumns.has(objective)) return;
      columns.push(`<th>${formatObjectiveLabel(objective)}</th>`);
    });

    columns.push('<th>WR %</th>');
    columns.push('<th>Net Profit %</th>');
    columns.push('<th>Max DD %</th>');
    columns.push('<th>Trades</th>');
    columns.push('<th>Max CL</th>');
    columns.push('<th>Score</th>');
    columns.push('<th>RoMaD</th>');
    columns.push('<th>Sharpe</th>');
    columns.push('<th>PF</th>');
    columns.push('<th>Ulcer</th>');
    columns.push('<th>SQN</th>');
    columns.push('<th>Consist</th>');
    return columns.join('');
  }

  function renderTrialRow(trial, objectives, flags) {
    const objectiveList = Array.isArray(objectives) ? objectives : [];
    const objectiveSet = new Set(objectiveList);
    const hasConstraints = Boolean(flags && flags.hasConstraints);
    const isPareto = Boolean(trial.is_pareto_optimal);
    const rawConstraint = trial.constraints_satisfied;
    const hasConstraintValue = rawConstraint !== null && rawConstraint !== undefined;
    const constraintState = hasConstraintValue ? Boolean(rawConstraint) : null;

    const paretoBadge = isPareto
      ? '<span class="dot dot-pareto"></span>'
      : '';

    let constraintBadge = '';
    if (hasConstraints) {
      if (constraintState === true) {
        constraintBadge = '<span class="dot dot-ok"></span>';
      } else if (constraintState === false) {
        constraintBadge = '<span class="dot dot-fail"></span>';
      }
    }

    const objectiveValues = Array.isArray(trial.objective_values) ? trial.objective_values : [];
    const rawMetricColumns = new Set([
      'win_rate',
      'net_profit_pct',
      'max_drawdown_pct',
      'max_consecutive_losses',
      'romad',
      'sharpe_ratio',
      'profit_factor',
      'ulcer_index',
      'sqn',
      'consistency_score'
    ]);
    const objectiveCells = objectiveList
      .map((objective, idx) => ({ objective, idx }))
      .filter(({ objective }) => !rawMetricColumns.has(objective))
      .map(({ objective, idx }) => {
        const value = objectiveValues[idx];
        const isPercent = objective.includes('pct') || objective === 'win_rate';
        const formatted = formatNumber(value, isPercent ? 2 : 3);
        return `<td>${formatted}${isPercent && formatted !== 'N/A' ? '%' : ''}</td>`;
      });

    const winRate = trial.win_rate;
    const winRateFormatted = formatNumber(winRate, 2);
    const winRateCell = `<td>${winRateFormatted}${winRateFormatted !== 'N/A' ? '%' : ''}</td>`;
    const netProfit = Number(trial.net_profit_pct || 0);
    const netProfitCell = `<td class="${netProfit >= 0 ? 'val-positive' : 'val-negative'}">${netProfit >= 0 ? '+' : ''}${formatNumber(netProfit, 2)}%</td>`;
    const maxDd = Math.abs(Number(trial.max_drawdown_pct || 0));
    const maxDdCell = `<td class="val-negative">-${formatNumber(maxDd, 2)}%</td>`;
    const maxClCell = `<td>${trial.max_consecutive_losses ?? '-'}</td>`;

    const scoreValue = trial.score !== undefined && trial.score !== null
      ? Number(trial.score)
      : null;
    const romad = trial.romad;
    const sharpe = trial.sharpe_ratio;
    const pf = trial.profit_factor;
    const ulcer = trial.ulcer_index;
    const sqn = trial.sqn;
    const consistency = trial.consistency_score;
    const consistencyDisplay = typeof window.formatConsistency === 'function'
      ? window.formatConsistency(consistency, trial.consistency_segments_used)
      : `${formatNumber(consistency, 1)}${consistency !== null && consistency !== undefined ? '%' : ''}`;

    return `
      <tr class="clickable" data-trial-number="${trial.trial_number ?? ''}">
        <td class="rank"></td>
        <td class="param-hash"></td>
        <td>${paretoBadge}</td>
        ${hasConstraints ? `<td>${constraintBadge}</td>` : ''}
        ${objectiveCells.join('')}
        ${winRateCell}
        ${netProfitCell}
        ${maxDdCell}
        <td>${trial.total_trades ?? '-'}</td>
        ${maxClCell}
        <td>${scoreValue !== null ? formatNumber(scoreValue, 1) : 'N/A'}</td>
        <td>${formatNumber(romad, 3)}</td>
        <td>${formatNumber(sharpe, 3)}</td>
        <td>${formatNumber(pf, 3)}</td>
        <td>${formatNumber(ulcer, 2)}</td>
        <td>${formatNumber(sqn, 3)}</td>
        <td>${consistencyDisplay}</td>
      </tr>
    `;
  }

  window.OptunaResultsUI = {
    buildTrialTableHeaders,
    renderTrialRow
  };
})();
