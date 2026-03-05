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

const SORT_METRIC_LABELS = {
  profit_degradation: 'Profit Degradation',
  ft_romad: 'FT RoMaD',
  profit_retention: 'Profit Retention',
  romad_retention: 'RoMaD Retention'
};

const SOURCE_LABELS = {
  optuna: 'Optuna IS',
  dsr: 'DSR',
  forward_test: 'Forward Test',
  stress_test: 'Stress Test',
  oos_test: 'OOS Test',
  manual_tests: 'Manual Test'
};

const TOKEN_LABELS = {
  ft: 'FT',
  st: 'ST',
  is: 'IS',
  oos: 'OOS',
  dsr: 'DSR',
  romad: 'RoMaD',
  pf: 'PF',
  sqn: 'SQN',
  dd: 'DD',
  pnl: 'PnL'
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
  consistency_score: '>='
};

function formatSigned(value, digits = 2, suffix = '') {
  const num = Number(value);
  if (!Number.isFinite(num)) return 'N/A';
  const sign = num > 0 ? '+' : '';
  return `${sign}${num.toFixed(digits)}${suffix}`;
}

function normalizeConsistencySegments(value) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return null;
  const rounded = Math.round(parsed);
  if (rounded < 2) return null;
  return rounded;
}

function deriveAutoConsistencySegments(isPeriodDays, isSegments, targetPeriodDays) {
  const isDays = Number(isPeriodDays);
  const targetDays = Number(targetPeriodDays);
  const normalizedIsSegments = normalizeConsistencySegments(isSegments);
  if (!Number.isFinite(isDays) || !Number.isFinite(targetDays) || !normalizedIsSegments) {
    return null;
  }
  if (isDays <= 0 || targetDays <= 0) return null;
  const segmentDays = isDays / normalizedIsSegments;
  if (!Number.isFinite(segmentDays) || segmentDays <= 0) return null;
  const derived = Math.round(targetDays / segmentDays);
  return derived >= 2 ? derived : null;
}

function getStudyConsistencySegments(study) {
  const config = (study && study.config_json) || {};
  const value = config.consistencySegments
    ?? config.consistency_segments
    ?? null;
  return normalizeConsistencySegments(value);
}

function formatConsistency(value, segments) {
  const num = Number(value);
  if (!Number.isFinite(num)) return 'N/A';
  const normalizedSegments = normalizeConsistencySegments(segments);
  if (!normalizedSegments) {
    return `${num.toFixed(1)}%`;
  }
  return `${num.toFixed(2)}/${normalizedSegments}`;
}

function formatRankCell(rank, delta) {
  const baseRank = Number(rank);
  if (!Number.isFinite(baseRank)) return '';
  const change = Number(delta);
  if (!Number.isFinite(change) || change === 0) {
    return `<span class="rank-base">${baseRank}</span>`;
  }
  const direction = change > 0 ? 'up' : 'down';
  const magnitude = Math.abs(Math.round(change));
  const deltaLabel = change > 0 ? `+${magnitude}` : `-${magnitude}`;
  return `<span class="rank-base">${baseRank}</span><span class="rank-delta ${direction}">${deltaLabel}</span>`;
}

function formatDateLabel(value) {
  if (!value) return '';
  const text = String(value).trim();
  if (!text) return '';
  const match = text.match(/(\d{4})[.\-/](\d{2})[.\-/](\d{2})/);
  if (!match) return '';
  return `${match[1]}.${match[2]}.${match[3]}`;
}

function formatDuration(seconds) {
  const totalSeconds = Number(seconds);
  if (!Number.isFinite(totalSeconds) || totalSeconds < 0) return '';
  const rounded = Math.round(totalSeconds);
  const hours = Math.floor(rounded / 3600);
  const minutes = Math.floor((rounded % 3600) / 60);
  const secs = rounded % 60;
  if (hours > 0) return `${hours}h ${minutes}m ${secs}s`;
  if (minutes > 0) return `${minutes}m ${secs}s`;
  return `${secs}s`;
}

function buildPeriodLabel(startDate, endDate) {
  const startLabel = formatDateLabel(startDate);
  const endLabel = formatDateLabel(endDate);
  if (startLabel && endLabel) {
    return `${startLabel}-${endLabel}`;
  }
  return 'Period: N/A';
}

function getWfaStitchedPeriodLabel(windows) {
  let minTime = null;
  let maxTime = null;
  let minLabel = '';
  let maxLabel = '';
  (windows || []).forEach((window) => {
    const start = window?.oos_start_date;
    const end = window?.oos_end_date;
    const startTime = Date.parse(start);
    const endTime = Date.parse(end);
    if (Number.isFinite(startTime) && (minTime === null || startTime < minTime)) {
      minTime = startTime;
      minLabel = start || '';
    }
    if (Number.isFinite(endTime) && (maxTime === null || endTime > maxTime)) {
      maxTime = endTime;
      maxLabel = end || '';
    }
  });
  if (minLabel && maxLabel) {
    return buildPeriodLabel(minLabel, maxLabel);
  }
  return 'Period: N/A';
}

function getActivePeriodLabel() {
  if (ResultsState.activeTab === 'forward_test') {
    return buildPeriodLabel(ResultsState.forwardTest.startDate, ResultsState.forwardTest.endDate);
  }
  if (ResultsState.activeTab === 'oos_test') {
    return buildPeriodLabel(ResultsState.oosTest.startDate, ResultsState.oosTest.endDate);
  }
  if (ResultsState.activeTab === 'manual_tests') {
    const config = ResultsState.activeManualTest?.config || {};
    return buildPeriodLabel(config.start_date, config.end_date);
  }
  return buildPeriodLabel(ResultsState.start, ResultsState.end);
}

function formatObjectiveLabel(name) {
  return OBJECTIVE_LABELS[name] || name;
}

function formatTitleToken(token) {
  const safe = String(token || '').trim();
  if (!safe) return '';
  const lower = safe.toLowerCase();
  if (TOKEN_LABELS[lower]) return TOKEN_LABELS[lower];
  return lower.charAt(0).toUpperCase() + lower.slice(1);
}

function formatTitleFromKey(key) {
  const safe = String(key || '').trim();
  if (!safe) return '';
  return safe
    .split(/[_\s-]+/)
    .filter(Boolean)
    .map(formatTitleToken)
    .join(' ');
}

function formatSortMetricLabel(metric) {
  const safe = String(metric || '').trim().toLowerCase();
  if (!safe) return '';
  return SORT_METRIC_LABELS[safe] || formatTitleFromKey(safe);
}

function formatSourceLabel(source) {
  const safe = String(source || '').trim().toLowerCase();
  if (!safe) return '';
  return SOURCE_LABELS[safe] || formatTitleFromKey(safe);
}

function formatObjectivesList(objectives) {
  if (!objectives || !objectives.length) return '-';
  return objectives.map((obj) => formatObjectiveLabel(obj)).join(', ');
}

function formatConstraintsSummary(constraints) {
  const enabled = (constraints || []).filter((c) => c && c.enabled);
  if (!enabled.length) return 'None';
  return enabled.map((c) => {
    const operator = CONSTRAINT_OPERATORS[c.metric] || '';
    const threshold = c.threshold !== undefined && c.threshold !== null ? c.threshold : '-';
    return `${formatObjectiveLabel(c.metric)} ${operator} ${threshold}`;
  }).join(', ');
}

function getOptunaSortSubtitle() {
  const objectives = ResultsState.optuna.objectives || [];
  if (!objectives.length) return 'Sorted by objectives';
  const primary = ResultsState.optuna.primaryObjective || null;
  const mainObjective = objectives.length > 1 ? (primary || objectives[0]) : objectives[0];
  const label = mainObjective ? formatObjectiveLabel(mainObjective) : '';
  if (!label) return 'Sorted by objectives';
  if (objectives.length > 1) {
    return `Sorted by Primary Objective: ${label}`;
  }
  return `Sorted by Objective: ${label}`;
}

function formatParamName(name) {
  return name.replace(/([A-Z])/g, ' $1').replace(/^./, (s) => s.toUpperCase());
}

function formatParamValue(value) {
  if (typeof value === 'number') {
    return Number.isInteger(value) ? value : value.toFixed(4);
  }
  return value;
}

function createParamId(params, strategyConfig, fixedParams) {
  const merged = { ...(fixedParams || {}), ...(params || {}) };
  const paramStr = stableStringify(merged);
  const hash = md5(paramStr).slice(0, 8);
  const configParams = (strategyConfig && strategyConfig.parameters) || {};
  const optimizable = [];
  Object.entries(configParams).forEach(([name, spec]) => {
    if (spec && spec.optimize && spec.optimize.enabled) {
      optimizable.push(name);
    }
  });
    const preferredPairs = [
      ['maType', 'maLength'],
      ['maType3', 'maLength3'],
      ['maType2', 'maLength2']
    ];
    let labelKeys = null;
    preferredPairs.some((pair) => {
      const hasPair = pair.every((key) => Object.prototype.hasOwnProperty.call(merged, key));
      if (hasPair) {
        labelKeys = pair;
        return true;
      }
      return false;
    });
    if (!labelKeys) {
      labelKeys = optimizable.slice(0, 2);
    }
  const labelParts = labelKeys.map((key) => {
    const value = Object.prototype.hasOwnProperty.call(merged, key) ? merged[key] : '?';
    return String(value);
  });
  if (labelParts.length) {
    return `${labelParts.join(' ')}_${hash}`;
  }
  return hash;
}

function stableStringify(obj) {
  const keys = Object.keys(obj || {}).sort();
  const ordered = {};
  keys.forEach((key) => {
    ordered[key] = obj[key];
  });
  return JSON.stringify(ordered);
}

function md5(string) {
  function rotateLeft(value, shift) {
    return (value << shift) | (value >>> (32 - shift));
  }
  function addUnsigned(x, y) {
    const x8 = (x & 0x80000000);
    const y8 = (y & 0x80000000);
    const x4 = (x & 0x40000000);
    const y4 = (y & 0x40000000);
    const result = (x & 0x3fffffff) + (y & 0x3fffffff);
    if (x4 & y4) return (result ^ 0x80000000 ^ x8 ^ y8);
    if (x4 | y4) {
      if (result & 0x40000000) return (result ^ 0xc0000000 ^ x8 ^ y8);
      return (result ^ 0x40000000 ^ x8 ^ y8);
    }
    return (result ^ x8 ^ y8);
  }
  function f(x, y, z) { return (x & y) | ((~x) & z); }
  function g(x, y, z) { return (x & z) | (y & (~z)); }
  function h(x, y, z) { return x ^ y ^ z; }
  function i(x, y, z) { return y ^ (x | (~z)); }
  function ff(a, b, c, d, x, s, ac) {
    a = addUnsigned(a, addUnsigned(addUnsigned(f(b, c, d), x), ac));
    return addUnsigned(rotateLeft(a, s), b);
  }
  function gg(a, b, c, d, x, s, ac) {
    a = addUnsigned(a, addUnsigned(addUnsigned(g(b, c, d), x), ac));
    return addUnsigned(rotateLeft(a, s), b);
  }
  function hh(a, b, c, d, x, s, ac) {
    a = addUnsigned(a, addUnsigned(addUnsigned(h(b, c, d), x), ac));
    return addUnsigned(rotateLeft(a, s), b);
  }
  function ii(a, b, c, d, x, s, ac) {
    a = addUnsigned(a, addUnsigned(addUnsigned(i(b, c, d), x), ac));
    return addUnsigned(rotateLeft(a, s), b);
  }
  function convertToWordArray(str) {
    const wordCount = ((str.length + 8) >> 6) + 1;
    const wordArray = new Array(wordCount * 16).fill(0);
    let bytePos = 0;
    for (; bytePos < str.length; bytePos += 1) {
      wordArray[bytePos >> 2] |= str.charCodeAt(bytePos) << ((bytePos % 4) * 8);
    }
    wordArray[bytePos >> 2] |= 0x80 << ((bytePos % 4) * 8);
    wordArray[wordCount * 16 - 2] = str.length * 8;
    return wordArray;
  }
  function wordToHex(value) {
    let hex = '';
    for (let i = 0; i <= 3; i += 1) {
      const byte = (value >> (i * 8)) & 255;
      const temp = `0${byte.toString(16)}`;
      hex += temp.substr(temp.length - 2, 2);
    }
    return hex;
  }

  let a = 0x67452301;
  let b = 0xefcdab89;
  let c = 0x98badcfe;
  let d = 0x10325476;

  const x = convertToWordArray(string);

  for (let k = 0; k < x.length; k += 16) {
    const aa = a;
    const bb = b;
    const cc = c;
    const dd = d;

    a = ff(a, b, c, d, x[k + 0], 7, 0xd76aa478);
    d = ff(d, a, b, c, x[k + 1], 12, 0xe8c7b756);
    c = ff(c, d, a, b, x[k + 2], 17, 0x242070db);
    b = ff(b, c, d, a, x[k + 3], 22, 0xc1bdceee);
    a = ff(a, b, c, d, x[k + 4], 7, 0xf57c0faf);
    d = ff(d, a, b, c, x[k + 5], 12, 0x4787c62a);
    c = ff(c, d, a, b, x[k + 6], 17, 0xa8304613);
    b = ff(b, c, d, a, x[k + 7], 22, 0xfd469501);
    a = ff(a, b, c, d, x[k + 8], 7, 0x698098d8);
    d = ff(d, a, b, c, x[k + 9], 12, 0x8b44f7af);
    c = ff(c, d, a, b, x[k + 10], 17, 0xffff5bb1);
    b = ff(b, c, d, a, x[k + 11], 22, 0x895cd7be);
    a = ff(a, b, c, d, x[k + 12], 7, 0x6b901122);
    d = ff(d, a, b, c, x[k + 13], 12, 0xfd987193);
    c = ff(c, d, a, b, x[k + 14], 17, 0xa679438e);
    b = ff(b, c, d, a, x[k + 15], 22, 0x49b40821);

    a = gg(a, b, c, d, x[k + 1], 5, 0xf61e2562);
    d = gg(d, a, b, c, x[k + 6], 9, 0xc040b340);
    c = gg(c, d, a, b, x[k + 11], 14, 0x265e5a51);
    b = gg(b, c, d, a, x[k + 0], 20, 0xe9b6c7aa);
    a = gg(a, b, c, d, x[k + 5], 5, 0xd62f105d);
    d = gg(d, a, b, c, x[k + 10], 9, 0x02441453);
    c = gg(c, d, a, b, x[k + 15], 14, 0xd8a1e681);
    b = gg(b, c, d, a, x[k + 4], 20, 0xe7d3fbc8);
    a = gg(a, b, c, d, x[k + 9], 5, 0x21e1cde6);
    d = gg(d, a, b, c, x[k + 14], 9, 0xc33707d6);
    c = gg(c, d, a, b, x[k + 3], 14, 0xf4d50d87);
    b = gg(b, c, d, a, x[k + 8], 20, 0x455a14ed);
    a = gg(a, b, c, d, x[k + 13], 5, 0xa9e3e905);
    d = gg(d, a, b, c, x[k + 2], 9, 0xfcefa3f8);
    c = gg(c, d, a, b, x[k + 7], 14, 0x676f02d9);
    b = gg(b, c, d, a, x[k + 12], 20, 0x8d2a4c8a);

    a = hh(a, b, c, d, x[k + 5], 4, 0xfffa3942);
    d = hh(d, a, b, c, x[k + 8], 11, 0x8771f681);
    c = hh(c, d, a, b, x[k + 11], 16, 0x6d9d6122);
    b = hh(b, c, d, a, x[k + 14], 23, 0xfde5380c);
    a = hh(a, b, c, d, x[k + 1], 4, 0xa4beea44);
    d = hh(d, a, b, c, x[k + 4], 11, 0x4bdecfa9);
    c = hh(c, d, a, b, x[k + 7], 16, 0xf6bb4b60);
    b = hh(b, c, d, a, x[k + 10], 23, 0xbebfbc70);
    a = hh(a, b, c, d, x[k + 13], 4, 0x289b7ec6);
    d = hh(d, a, b, c, x[k + 0], 11, 0xeaa127fa);
    c = hh(c, d, a, b, x[k + 3], 16, 0xd4ef3085);
    b = hh(b, c, d, a, x[k + 6], 23, 0x04881d05);
    a = hh(a, b, c, d, x[k + 9], 4, 0xd9d4d039);
    d = hh(d, a, b, c, x[k + 12], 11, 0xe6db99e5);
    c = hh(c, d, a, b, x[k + 15], 16, 0x1fa27cf8);
    b = hh(b, c, d, a, x[k + 2], 23, 0xc4ac5665);

    a = ii(a, b, c, d, x[k + 0], 6, 0xf4292244);
    d = ii(d, a, b, c, x[k + 7], 10, 0x432aff97);
    c = ii(c, d, a, b, x[k + 14], 15, 0xab9423a7);
    b = ii(b, c, d, a, x[k + 5], 21, 0xfc93a039);
    a = ii(a, b, c, d, x[k + 12], 6, 0x655b59c3);
    d = ii(d, a, b, c, x[k + 3], 10, 0x8f0ccc92);
    c = ii(c, d, a, b, x[k + 10], 15, 0xffeff47d);
    b = ii(b, c, d, a, x[k + 1], 21, 0x85845dd1);
    a = ii(a, b, c, d, x[k + 8], 6, 0x6fa87e4f);
    d = ii(d, a, b, c, x[k + 15], 10, 0xfe2ce6e0);
    c = ii(c, d, a, b, x[k + 6], 15, 0xa3014314);
    b = ii(b, c, d, a, x[k + 13], 21, 0x4e0811a1);
    a = ii(a, b, c, d, x[k + 4], 6, 0xf7537e82);
    d = ii(d, a, b, c, x[k + 11], 10, 0xbd3af235);
    c = ii(c, d, a, b, x[k + 2], 15, 0x2ad7d2bb);
    b = ii(b, c, d, a, x[k + 9], 21, 0xeb86d391);

    a = addUnsigned(a, aa);
    b = addUnsigned(b, bb);
    c = addUnsigned(c, cc);
    d = addUnsigned(d, dd);
  }

  return (wordToHex(a) + wordToHex(b) + wordToHex(c) + wordToHex(d)).toLowerCase();
}
