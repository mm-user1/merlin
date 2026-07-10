/**
 * UI event handlers and form management.
 * Dependencies: utils.js, api.js, strategy-config.js, presets.js
 */

const SCORE_METRICS = ['romad', 'sharpe', 'pf', 'ulcer', 'sqn', 'consistency'];
const SCORE_DEFAULT_THRESHOLD = 60;
const SCORE_DEFAULT_WEIGHTS = {
  romad: 0.25,
  sharpe: 0.20,
  pf: 0.20,
  ulcer: 0.15,
  sqn: 0.10,
  consistency: 0.10
};
const SCORE_DEFAULT_ENABLED = {
  romad: true,
  sharpe: true,
  pf: true,
  ulcer: true,
  sqn: true,
  consistency: true
};
const SCORE_DEFAULT_INVERT = {
  ulcer: true
};
const SCORE_DEFAULT_BOUNDS = {
  romad: { min: 0, max: 10 },
  sharpe: { min: -1, max: 3 },
  pf: { min: 0, max: 5 },
  ulcer: { min: 0, max: 20 },
  sqn: { min: -2, max: 7 },
  consistency: { min: -1, max: 1 }
};
const GRID_SUPPORTED_OBJECTIVES = new Set([
  'net_profit_pct',
  'max_drawdown_pct',
  'romad',
  'profit_factor',
  'win_rate'
]);
const GRID_SUPPORTED_SLOW_OBJECTIVES = new Set([
  ...GRID_SUPPORTED_OBJECTIVES,
  'sharpe_ratio',
  'sortino_ratio',
  'sqn',
  'ulcer_index',
  'consistency_score'
]);
const GRID_OBJECTIVE_LABELS = {
  net_profit_pct: 'Net Profit %',
  max_drawdown_pct: 'Max DD %',
  sharpe_ratio: 'Sharpe Ratio',
  sortino_ratio: 'Sortino Ratio',
  romad: 'RoMaD',
  profit_factor: 'Profit Factor',
  win_rate: 'Win Rate %',
  sqn: 'SQN',
  ulcer_index: 'Ulcer Index',
  consistency_score: 'Consistency'
};
const GRID_SUPPORTED_CONSTRAINTS = new Set([
  'total_trades',
  'net_profit_pct',
  'max_drawdown_pct',
  'romad',
  'profit_factor',
  'win_rate',
  'max_consecutive_losses'
]);
let gridPreviewTimer = null;
let gridPreviewSeq = 0;

function normalizeScoreBounds(rawBounds = {}) {
  const normalized = {};

  SCORE_METRICS.forEach((metric) => {
    const defaults = SCORE_DEFAULT_BOUNDS[metric];
    const source = rawBounds && typeof rawBounds === 'object' ? rawBounds[metric] : null;
    let min = Number(source?.min);
    let max = Number(source?.max);

    if (!Number.isFinite(min)) min = defaults.min;
    if (!Number.isFinite(max)) max = defaults.max;

    if (metric === 'consistency' && min === 0 && max === 100) {
      min = defaults.min;
      max = defaults.max;
    }

    normalized[metric] = { min, max };
  });

  return normalized;
}

const OPT_STATE_KEY = 'merlinOptimizationState';
const OPT_CONTROL_KEY = 'merlinOptimizationControl';
let optimizationAbortController = null;

function saveOptimizationState(state) {
  try {
    const payload = JSON.stringify(state || {});
    sessionStorage.setItem(OPT_STATE_KEY, payload);
    localStorage.setItem(OPT_STATE_KEY, payload);
  } catch (error) {
    console.warn('Failed to store optimization state', error);
  }
}

function loadOptimizationState() {
  const fromSession = sessionStorage.getItem(OPT_STATE_KEY);
  const fromLocal = localStorage.getItem(OPT_STATE_KEY);
  const raw = fromSession || fromLocal;
  if (!raw) return null;
  try {
    return JSON.parse(raw);
  } catch (error) {
    return null;
  }
}

function updateOptimizationState(patch) {
  const current = loadOptimizationState() || {};
  const updated = { ...current, ...patch };
  saveOptimizationState(updated);
  return updated;
}

function generateOptimizationRunId(prefix = 'run') {
  const normalizedPrefix = String(prefix || 'run').replace(/[^A-Za-z0-9_-]+/g, '') || 'run';
  return normalizedPrefix + '_' + Date.now() + '_' + Math.random().toString(36).slice(2, 10);
}

function setCurrentOptimizationRunId(runId) {
  const normalizedRunId = String(runId || '').trim();
  window.activeOptimizationRunId = normalizedRunId;
  updateOptimizationState({ run_id: normalizedRunId });
}

async function cancelCurrentRunBestEffort(runId) {
  const normalizedRunId = String(runId || '').trim();
  if (!normalizedRunId || typeof cancelOptimizationRequest !== 'function') {
    return;
  }
  try {
    await cancelOptimizationRequest(normalizedRunId);
  } catch (error) {
    console.warn('Cancel request failed', error);
  }
}

function openResultsPage() {
  try {
    window.open('/results', '_blank', 'noopener');
  } catch (error) {
    window.location.href = '/results';
  }
}

function getStrategySummary() {
  const config = window.currentStrategyConfig || {};
  return {
    id: window.currentStrategyId || '',
    name: config.name || '',
    version: config.version || '',
    description: config.description || ''
  };
}

function isAbsoluteFilesystemPath(path) {
  const value = String(path || '').trim();
  if (!value) return false;
  if (/^[A-Za-z]:[\\/]/.test(value)) return true; // Windows drive path
  if (/^\\\\[^\\]/.test(value)) return true; // UNC path
  if (value.startsWith('/')) return true; // POSIX path
  return false;
}

function normalizeSelectedCsvPaths(paths) {
  const items = Array.isArray(paths) ? paths : [];
  const unique = [];
  const seen = new Set();
  items.forEach((item) => {
    const value = String(item || '').trim();
    if (!value) return;
    const key = value.toLowerCase();
    if (seen.has(key)) return;
    seen.add(key);
    unique.push(value);
  });
  return unique;
}

function getSelectedCsvPaths() {
  if (Array.isArray(window.selectedCsvPaths) && window.selectedCsvPaths.length) {
    return normalizeSelectedCsvPaths(window.selectedCsvPaths);
  }
  const fallback = String(window.selectedCsvPath || '').trim();
  return fallback ? [fallback] : [];
}

function setSelectedCsvPaths(paths) {
  const normalized = normalizeSelectedCsvPaths(paths);
  window.selectedCsvPaths = normalized;
  window.selectedCsvPath = normalized[0] || '';
  if (!window.uiState || typeof window.uiState !== 'object') {
    window.uiState = {};
  }
  window.uiState.csvPath = window.selectedCsvPath;
  renderSelectedFiles([]);
  if (typeof syncQueueAutoCreateSetUi === 'function') {
    syncQueueAutoCreateSetUi();
  }
}

const csvBrowserState = {
  currentPath: '',
  entries: []
};

function csvBrowserElements() {
  return {
    modal: document.getElementById('csvBrowserModal'),
    pathInput: document.getElementById('csvBrowserPath'),
    list: document.getElementById('csvBrowserList'),
    error: document.getElementById('csvBrowserError'),
    rootInput: document.getElementById('csvDirectory'),
    upBtn: document.getElementById('csvBrowserUpBtn'),
    openBtn: document.getElementById('csvBrowserOpenBtn'),
    refreshBtn: document.getElementById('csvBrowserRefreshBtn'),
    cancelBtn: document.getElementById('csvBrowserCancelBtn'),
    addBtn: document.getElementById('csvBrowserAddBtn')
  };
}

function showCsvBrowserError(message) {
  const { error } = csvBrowserElements();
  if (!error) return;
  const text = String(message || '').trim();
  if (!text) {
    error.textContent = '';
    error.style.display = 'none';
    return;
  }
  error.textContent = text;
  error.style.display = 'block';
}

function renderCsvBrowserEntries(entries) {
  const { list } = csvBrowserElements();
  if (!list) return;
  list.innerHTML = '';
  const fragment = document.createDocumentFragment();
  entries.forEach((entry) => {
    const option = document.createElement('option');
    option.value = entry.path;
    option.dataset.kind = entry.kind;
    option.textContent = entry.kind === 'dir'
      ? `[DIR] ${entry.name}`
      : `      ${entry.name}`;
    fragment.appendChild(option);
  });
  list.appendChild(fragment);
}

async function loadCsvBrowserDirectory(path) {
  const { pathInput } = csvBrowserElements();
  const targetPath = String(path || '').trim();
  showCsvBrowserError('');
  try {
    const payload = await browseCsvDirectoryRequest(targetPath);
    csvBrowserState.currentPath = payload.current_path || '';
    csvBrowserState.entries = Array.isArray(payload.entries) ? payload.entries : [];
    if (pathInput) {
      pathInput.value = csvBrowserState.currentPath;
    }
    renderCsvBrowserEntries(csvBrowserState.entries);
  } catch (error) {
    showCsvBrowserError(error.message || 'Failed to load directory.');
  }
}

function closeCsvBrowserModal() {
  const { modal } = csvBrowserElements();
  if (!modal) return;
  modal.classList.remove('show');
  modal.setAttribute('aria-hidden', 'true');
}

async function openCsvBrowserModal() {
  const { modal, rootInput } = csvBrowserElements();
  if (!modal) return;
  const rootPath = String(rootInput?.value || '').trim();
  await loadCsvBrowserDirectory(rootPath);
  modal.classList.add('show');
  modal.setAttribute('aria-hidden', 'false');
}

async function openSelectedCsvBrowserDirectory() {
  const { list } = csvBrowserElements();
  if (!list) return;
  const selected = Array.from(list.selectedOptions || []);
  if (selected.length !== 1) {
    showCsvBrowserError('Select exactly one folder to open.');
    return;
  }
  const option = selected[0];
  if (option.dataset.kind !== 'dir') {
    showCsvBrowserError('Selected entry is not a folder.');
    return;
  }
  await loadCsvBrowserDirectory(option.value);
}

async function moveCsvBrowserUp() {
  const current = csvBrowserState.currentPath;
  if (!current) return;
  const lastSlash = Math.max(current.lastIndexOf('\\'), current.lastIndexOf('/'));
  if (lastSlash <= 2) {
    await loadCsvBrowserDirectory(current);
    return;
  }
  const parent = current.slice(0, lastSlash);
  await loadCsvBrowserDirectory(parent);
}

function addSelectedCsvFilesFromBrowser() {
  const { list, rootInput } = csvBrowserElements();
  if (!list) return;
  const selected = Array.from(list.selectedOptions || []);
  const filePaths = selected
    .filter((item) => item.dataset.kind === 'file')
    .map((item) => String(item.value || '').trim())
    .filter(Boolean);

  if (!filePaths.length) {
    showCsvBrowserError('Select at least one CSV file.');
    return;
  }

  const merged = normalizeSelectedCsvPaths([...getSelectedCsvPaths(), ...filePaths]);
  setSelectedCsvPaths(merged);
  if (rootInput && csvBrowserState.currentPath) {
    rootInput.value = csvBrowserState.currentPath;
  }
  closeCsvBrowserModal();
}

function bindCsvBrowserControls() {
  const { modal, list, upBtn, openBtn, refreshBtn, cancelBtn, addBtn } = csvBrowserElements();
  if (!modal || !list || !upBtn || !openBtn || !refreshBtn || !cancelBtn || !addBtn) {
    return;
  }

  if (modal.dataset.bound === '1') {
    return;
  }
  modal.dataset.bound = '1';

  upBtn.addEventListener('click', moveCsvBrowserUp);
  openBtn.addEventListener('click', openSelectedCsvBrowserDirectory);
  refreshBtn.addEventListener('click', () => loadCsvBrowserDirectory(csvBrowserState.currentPath));
  cancelBtn.addEventListener('click', closeCsvBrowserModal);
  addBtn.addEventListener('click', addSelectedCsvFilesFromBrowser);

  list.addEventListener('dblclick', (event) => {
    const target = event.target;
    if (!target || target.tagName !== 'OPTION') return;
    if (target.dataset.kind === 'dir') {
      loadCsvBrowserDirectory(target.value);
    }
  });

  modal.addEventListener('click', (event) => {
    if (event.target === modal) {
      closeCsvBrowserModal();
    }
  });

  document.addEventListener('keydown', (event) => {
    if (event.key === 'Escape' && modal.classList.contains('show')) {
      closeCsvBrowserModal();
    }
  });
}

function getDatasetLabel() {
  const selectedPaths = getSelectedCsvPaths();
  if (selectedPaths.length === 1) return selectedPaths[0];
  if (selectedPaths.length > 1) return `${selectedPaths.length} CSV files selected`;
  return '';
}

function parseCsvLine(line) {
  const values = [];
  let current = '';
  let inQuotes = false;
  for (let i = 0; i < line.length; i += 1) {
    const ch = line[i];
    if (ch === '"') {
      if (inQuotes && line[i + 1] === '"') {
        current += '"';
        i += 1;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }
    if (ch === ',' && !inQuotes) {
      values.push(current);
      current = '';
      continue;
    }
    current += ch;
  }
  values.push(current);
  return values;
}

function parseOptunaCsv(csvText, strategyConfig) {
  const results = [];
  if (!csvText) return results;

  const lines = csvText.split(/\r?\n/);
  let headerIndex = -1;
  for (let i = 0; i < lines.length; i += 1) {
    const line = lines[i].trim();
    if (!line) continue;
    if (line.includes('Net Profit%') && line.includes('Max DD%')) {
      headerIndex = i;
      break;
    }
  }
  if (headerIndex === -1) return results;

  const header = parseCsvLine(lines[headerIndex]);
  const paramLabelMap = {};
  const paramsConfig = (strategyConfig && strategyConfig.parameters) || {};
  Object.entries(paramsConfig).forEach(([name, def]) => {
    const label = (def && def.label) ? String(def.label) : name;
    paramLabelMap[label] = name;
    paramLabelMap[name] = name;
  });

  const metricMap = {
    'Net Profit%': 'net_profit_pct',
    'Max DD%': 'max_drawdown_pct',
    'Trades': 'total_trades',
    'Score': 'score',
    'RoMaD': 'romad',
    'Sharpe': 'sharpe_ratio',
    'PF': 'profit_factor',
    'Ulcer': 'ulcer_index',
    'SQN': 'sqn',
    'Consist': 'consistency_score'
  };

  for (let i = headerIndex + 1; i < lines.length; i += 1) {
    const line = lines[i];
    if (!line || !line.trim()) break;
    const values = parseCsvLine(line);
    const params = {};
    const metrics = {};
    header.forEach((col, idx) => {
      const value = values[idx] ?? '';
      const trimmed = String(value).trim();
      if (metricMap[col]) {
        const metricKey = metricMap[col];
        const cleaned = trimmed.replace('%', '');
        const num = Number(cleaned);
        metrics[metricKey] = Number.isFinite(num) ? num : trimmed;
        return;
      }
      const paramName = paramLabelMap[col];
      if (!paramName) return;
      const def = paramsConfig[paramName] || {};
      if (def.type === 'int') {
        const num = Number(trimmed);
        params[paramName] = Number.isFinite(num) ? Math.round(num) : trimmed;
      } else if (def.type === 'float') {
        const num = Number(trimmed);
        params[paramName] = Number.isFinite(num) ? num : trimmed;
      } else if (def.type === 'bool') {
        params[paramName] = trimmed.toLowerCase() === 'true';
      } else {
        params[paramName] = trimmed;
      }
    });
    results.push({ params, ...metrics });
  }

  return results;
}

async function refreshOptimizationStateFromServer() {
  try {
    const state = await fetchOptimizationStatus();
    if (state && state.status) {
      saveOptimizationState(state);
      return state;
    }
  } catch (error) {
    console.warn('Failed to refresh optimization status from server', error);
  }
  return null;
}

function toggleWFSettings() {
  const wfToggle = document.getElementById('enableWF');
  const wfSettings = document.getElementById('wfSettings');
  const adaptiveToggle = document.getElementById('enableAdaptiveWF');
  if (!wfToggle || !wfSettings) {
    return;
  }
  if (wfToggle.disabled) {
    wfSettings.style.display = 'none';
    if (adaptiveToggle) {
      adaptiveToggle.disabled = true;
      adaptiveToggle.checked = false;
    }
    toggleAdaptiveWFSettings();
    if (typeof syncQueueAutoCreateSetUi === 'function') {
      syncQueueAutoCreateSetUi();
    }
    return;
  }
  wfSettings.style.display = wfToggle.checked ? 'block' : 'none';
  if (adaptiveToggle) {
    adaptiveToggle.disabled = !wfToggle.checked;
    if (!wfToggle.checked) {
      adaptiveToggle.checked = false;
    }
  }
  toggleAdaptiveWFSettings();
  if (typeof syncQueueAutoCreateSetUi === 'function') {
    syncQueueAutoCreateSetUi();
  }
}

window.toggleWFSettings = toggleWFSettings;

function toggleAdaptiveWFSettings() {
  const wfToggle = document.getElementById('enableWF');
  const adaptiveToggle = document.getElementById('enableAdaptiveWF');
  const adaptiveSettings = document.getElementById('adaptiveWFSettings');
  const oosInput = document.getElementById('wfOosPeriodDays');
  const cooldownToggle = document.getElementById('wfCooldownEnabled');
  const cooldownDaysInput = document.getElementById('wfCooldownDays');
  if (!adaptiveToggle || !adaptiveSettings || !oosInput) {
    return;
  }

  const enabled = Boolean(
    wfToggle
    && wfToggle.checked
    && !wfToggle.disabled
    && adaptiveToggle.checked
    && !adaptiveToggle.disabled
  );
  adaptiveSettings.style.display = enabled ? 'block' : 'none';
  oosInput.disabled = enabled;
  if (cooldownToggle) {
    cooldownToggle.disabled = !enabled;
    if (!enabled) {
      cooldownToggle.checked = false;
    }
  }
  if (cooldownDaysInput) {
    cooldownDaysInput.disabled = !enabled || !(cooldownToggle && cooldownToggle.checked);
  }
}

window.toggleAdaptiveWFSettings = toggleAdaptiveWFSettings;

function syncBudgetInputs() {
  const budgetModeRadios = document.querySelectorAll('input[name="budgetMode"]');
  const optunaTrials = document.getElementById('optunaTrials');
  const optunaTimeLimit = document.getElementById('optunaTimeLimit');
  const optunaConvergence = document.getElementById('optunaConvergence');

  if (!budgetModeRadios || !budgetModeRadios.length) {
    return;
  }

  const selected = Array.from(budgetModeRadios).find((radio) => radio.checked)?.value || 'trials';

  if (optunaTrials) optunaTrials.disabled = selected !== 'trials';
  if (optunaTimeLimit) optunaTimeLimit.disabled = selected !== 'time';
  if (optunaConvergence) optunaConvergence.disabled = selected !== 'convergence';
}

function getMinProfitElements() {
  return {
    checkbox: document.getElementById('minProfitFilter'),
    input: document.getElementById('minProfitThreshold'),
    group: document.getElementById('minProfitFilterGroup')
  };
}

function getScoreElements() {
  return {
    checkbox: document.getElementById('scoreFilter'),
    input: document.getElementById('scoreThreshold'),
    group: document.getElementById('scoreFilterGroup')
  };
}

function syncMinProfitFilterUI() {
  const { checkbox, input, group } = getMinProfitElements();
  if (!checkbox || !input) return;

  const isChecked = Boolean(checkbox.checked);
  input.disabled = !isChecked;
  if (group) {
    group.classList.toggle('active', isChecked);
  }
}

function syncScoreFilterUI() {
  const { checkbox, input, group } = getScoreElements();
  if (!checkbox || !input) return;

  const isChecked = Boolean(checkbox.checked);
  input.disabled = !isChecked;
  if (group) {
    group.classList.toggle('active', isChecked);
  }
}

function readScoreUIState() {
  const { checkbox, input } = getScoreElements();
  const weights = {};
  const enabled = {};

  SCORE_METRICS.forEach((metric) => {
    const metricCheckbox = document.getElementById(`metric-${metric}`);
    const weightInput = document.getElementById(`weight-${metric}`);
    enabled[metric] = Boolean(metricCheckbox && metricCheckbox.checked);
    const rawWeight = weightInput ? Number(weightInput.value) : NaN;
    const fallback = SCORE_DEFAULT_WEIGHTS[metric] ?? 0;
    const parsedWeight = Number.isFinite(rawWeight) ? rawWeight : fallback;
    weights[metric] = Math.min(1, Math.max(0, parsedWeight));
  });

  const invertCheckbox = document.getElementById('invert-ulcer');
  const invert = {
    ulcer: Boolean(invertCheckbox && invertCheckbox.checked)
  };

  const thresholdRaw = input ? Number(input.value) : NaN;
  const threshold = Number.isFinite(thresholdRaw)
    ? Math.min(100, Math.max(0, thresholdRaw))
    : SCORE_DEFAULT_THRESHOLD;

  const bounds = {};
  SCORE_METRICS.forEach((metric) => {
    const defaults = SCORE_DEFAULT_BOUNDS[metric] || { min: 0, max: 100 };
    const minInput = document.getElementById(`bound-min-${metric}`);
    const maxInput = document.getElementById(`bound-max-${metric}`);
    const minRaw = minInput ? Number(minInput.value) : NaN;
    const maxRaw = maxInput ? Number(maxInput.value) : NaN;
    bounds[metric] = {
      min: Number.isFinite(minRaw) ? minRaw : defaults.min,
      max: Number.isFinite(maxRaw) ? maxRaw : defaults.max
    };
  });

  return {
    scoreFilterEnabled: Boolean(checkbox && checkbox.checked),
    scoreThreshold: threshold,
    scoreWeights: weights,
    scoreEnabledMetrics: enabled,
    scoreInvertMetrics: invert,
    scoreMetricBounds: bounds
  };
}

function applyScoreSettings(settings = {}) {
  const filterCheckbox = document.getElementById('scoreFilter');
  const thresholdInput = document.getElementById('scoreThreshold');

  const defaultScoreConfig = window.defaults?.scoreConfig || {};
  const baseBounds = normalizeScoreBounds({
    ...SCORE_DEFAULT_BOUNDS,
    ...(defaultScoreConfig.metric_bounds || {})
  });
  const effectiveWeights = {
    ...SCORE_DEFAULT_WEIGHTS,
    ...(defaultScoreConfig.weights || {}),
    ...(settings.scoreWeights || {})
  };
  const effectiveEnabled = {
    ...SCORE_DEFAULT_ENABLED,
    ...(defaultScoreConfig.enabled_metrics || {}),
    ...(settings.scoreEnabledMetrics || {})
  };
  const effectiveInvert = {
    ...SCORE_DEFAULT_INVERT,
    ...(defaultScoreConfig.invert_metrics || {}),
    ...(settings.scoreInvertMetrics || {})
  };
  const effectiveBounds = normalizeScoreBounds({
    ...baseBounds,
    ...(settings.scoreMetricBounds || {})
  });

  const filterEnabled = Object.prototype.hasOwnProperty.call(settings, 'scoreFilterEnabled')
    ? Boolean(settings.scoreFilterEnabled)
    : Boolean(defaultScoreConfig.filter_enabled);
  const thresholdValue = Object.prototype.hasOwnProperty.call(settings, 'scoreThreshold')
    ? Number(settings.scoreThreshold)
    : Number(defaultScoreConfig.min_score_threshold);

  if (filterCheckbox) {
    filterCheckbox.checked = filterEnabled;
  }
  if (thresholdInput) {
    const safeValue = Number.isFinite(thresholdValue)
      ? Math.min(100, Math.max(0, thresholdValue))
      : SCORE_DEFAULT_THRESHOLD;
    thresholdInput.value = safeValue;
  }

  SCORE_METRICS.forEach((metric) => {
    const metricCheckbox = document.getElementById(`metric-${metric}`);
    const weightInput = document.getElementById(`weight-${metric}`);
    if (metricCheckbox) {
      metricCheckbox.checked = Boolean(effectiveEnabled[metric]);
    }
    if (weightInput) {
      const weightValue = Number.isFinite(Number(effectiveWeights[metric]))
        ? Math.min(1, Math.max(0, Number(effectiveWeights[metric])))
        : SCORE_DEFAULT_WEIGHTS[metric];
      weightInput.value = weightValue;
    }
  });

  const invertCheckbox = document.getElementById('invert-ulcer');
  if (invertCheckbox) {
    invertCheckbox.checked = Boolean(effectiveInvert.ulcer);
  }

  SCORE_METRICS.forEach((metric) => {
    const bounds = effectiveBounds[metric] || {};
    const base = baseBounds[metric] || { min: 0, max: 100 };
    const minValue = Number.isFinite(Number(bounds.min)) ? Number(bounds.min) : base.min;
    const maxValue = Number.isFinite(Number(bounds.max)) ? Number(bounds.max) : base.max;
    const minInput = document.getElementById(`bound-min-${metric}`);
    const maxInput = document.getElementById(`bound-max-${metric}`);
    if (minInput) minInput.value = minValue;
    if (maxInput) maxInput.value = maxValue;
  });

  syncScoreFilterUI();
  updateScoreFormulaPreview();
}

function updateScoreFormulaPreview() {
  const previewEl = document.getElementById('formulaPreview');
  if (!previewEl) return;

  const state = readScoreUIState();
  const enabledWeights = SCORE_METRICS
    .filter((metric) => state.scoreEnabledMetrics[metric] && state.scoreWeights[metric] > 0)
    .map((metric) => {
      const labelMap = {
        romad: 'RoMaD',
        sharpe: 'Sharpe Ratio',
        pf: 'Profit Factor',
        ulcer: 'Ulcer Index',
        sqn: 'SQN',
        consistency: 'Consistency Score'
      };
      const label = labelMap[metric] || metric;
      const weight = state.scoreWeights[metric];
      return `${weight.toFixed(2)}?-${label}`;
    });

  if (!enabledWeights.length) {
    previewEl.textContent = 'Score disabled (no metrics enabled).';
    return;
  }
  previewEl.textContent = `Score = ${enabledWeights.join(' + ')}`;
}

function collectScoreConfig() {
  const state = readScoreUIState();
  const config = {
    filter_enabled: state.scoreFilterEnabled,
    min_score_threshold: state.scoreThreshold,
    weights: {},
    enabled_metrics: {},
    invert_metrics: {},
    normalization_method: 'minmax',
    metric_bounds: state.scoreMetricBounds
  };

  SCORE_METRICS.forEach((metric) => {
    config.enabled_metrics[metric] = Boolean(state.scoreEnabledMetrics[metric]);
    const normalizedWeight = Math.min(1, Math.max(0, state.scoreWeights[metric]));
    config.weights[metric] = config.enabled_metrics[metric] ? normalizedWeight : 0;
  });

  if (state.scoreInvertMetrics.ulcer) {
    config.invert_metrics.ulcer = true;
  }

  return config;
}

function collectDynamicBacktestParams() {
  const params = {};
  const container = document.getElementById('backtestParamsContent');

  if (!container || !window.currentStrategyConfig || !window.currentStrategyConfig.parameters) {
    return params;
  }

  Object.entries(window.currentStrategyConfig.parameters).forEach(([name, def]) => {
    const input = document.getElementById(`backtest_${name}`);
    if (!input) return;

    if (input.type === 'checkbox') {
      params[name] = Boolean(input.checked);
    } else if (input.type === 'number') {
      const value = parseFloat(input.value);
      const fallback = Object.prototype.hasOwnProperty.call(def, 'default') ? def.default : 0;
      params[name] = Number.isFinite(value) ? value : fallback;
    } else {
      params[name] = input.value;
    }
  });

  return params;
}

function applyDynamicBacktestParams(params) {
  if (!params || typeof params !== 'object') return;
  if (!window.currentStrategyConfig || !window.currentStrategyConfig.parameters) return;

  Object.entries(window.currentStrategyConfig.parameters).forEach(([name]) => {
    if (!Object.prototype.hasOwnProperty.call(params, name)) return;

    const input = document.getElementById(`backtest_${name}`);
    if (!input) return;

    const value = params[name];

    if (input.type === 'checkbox') {
      input.checked = Boolean(value);
    } else if (input.type === 'number') {
      input.value = value;
    } else if (input.tagName === 'SELECT') {
      input.value = value;
    } else {
      input.value = value;
    }
  });
}

function gatherFormState() {
  const start = composeDateTime(
    document.getElementById('startDate').value,
    document.getElementById('startTime').value
  );
  const end = composeDateTime(
    document.getElementById('endDate').value,
    document.getElementById('endTime').value
  );

  const dynamicParams = collectDynamicBacktestParams();

  const payload = {
    ...dynamicParams,
    dateFilter: document.getElementById('dateFilter').checked,
    start,
    end
  };

  return { start, end, payload };
}
function getBacktestParamValue(paramName, paramDef = {}, dynamicParams = {}) {
  if (Object.prototype.hasOwnProperty.call(dynamicParams, paramName)) {
    return dynamicParams[paramName];
  }

  const input = document.getElementById(`backtest_${paramName}`);
  if (input) {
    if (input.type === 'checkbox') {
      return Boolean(input.checked);
    }
    if (input.type === 'number') {
      const value = Number(input.value);
      if (Number.isFinite(value)) return value;
    }
    return input.value;
  }

  if (Object.prototype.hasOwnProperty.call(paramDef, 'default')) {
    return paramDef.default;
  }

  return null;
}

function getWorkerProcessesValue() {
  const workerInput = document.getElementById('workerProcesses');
  let workerProcesses = window.defaults.workerProcesses;
  if (workerInput) {
    const rawValue = Number(workerInput.value);
    if (Number.isFinite(rawValue)) {
      workerProcesses = rawValue;
    }
  }
  return Math.round(Math.min(32, Math.max(1, workerProcesses)));
}

function getOptimizerMode() {
  const selected = document.querySelector('input[name="optimizerMode"]:checked');
  return selected && selected.value === 'grid' ? 'grid' : 'optuna';
}

function parseCompactCount(rawValue) {
  const text = String(rawValue ?? '').trim();
  const match = text.match(/^(\d+(?:\.\d+)?)([kKmMbB]?)$/);
  if (!match) return null;
  const number = Number(match[1]);
  if (!Number.isFinite(number)) return null;
  const suffix = match[2].toLowerCase();
  const multiplier = suffix === 'k' ? 1000 : (suffix === 'm' ? 1000000 : (suffix === 'b' ? 1000000000 : 1));
  const value = Math.round(number * multiplier);
  return value > 0 ? value : null;
}

function formatCompactCount(value) {
  const number = Number(value);
  if (!Number.isFinite(number)) return '-';
  const absValue = Math.abs(number);
  const units = [
    ['B', 1000000000],
    ['M', 1000000],
    ['k', 1000]
  ];
  for (const [suffix, scale] of units) {
    if (absValue >= scale) {
      const scaled = number / scale;
      if (Math.abs(scaled) >= 100) return `${scaled.toFixed(0)}${suffix}`;
      if (Math.abs(scaled) >= 10) return `${scaled.toFixed(1).replace(/\.0$/, '')}${suffix}`;
      return `${scaled.toFixed(2).replace(/\.?0+$/, '')}${suffix}`;
    }
  }
  return String(Math.round(number));
}

function getGridBudgetValue() {
  const input = document.getElementById('gridBudget');
  return parseCompactCount(input?.value || '200k') || 200000;
}

function getSelectedObjectiveKeys() {
  if (getOptimizerMode() === 'grid') {
    return collectGridObjectiveSelection('fast').objectives;
  }
  if (window.OptunaUI && typeof window.OptunaUI.collectObjectives === 'function') {
    return window.OptunaUI.collectObjectives().objectives || [];
  }
  return Array.from(document.querySelectorAll('.objective-checkbox'))
    .filter((cb) => cb.checked)
    .map((cb) => cb.dataset.objective);
}

function getGridObjectiveElements(kind) {
  const selector = kind === 'slow' ? '.grid-slow-objective-checkbox' : '.grid-fast-objective-checkbox';
  return Array.from(document.querySelectorAll(selector));
}

function getGridPrimarySelect(kind) {
  return document.getElementById(kind === 'slow' ? 'gridSlowPrimaryObjective' : 'gridFastPrimaryObjective');
}

function getGridPrimaryRow(kind) {
  return document.getElementById(kind === 'slow' ? 'gridSlowPrimaryObjectiveRow' : 'gridFastPrimaryObjectiveRow');
}

function collectGridObjectiveSelection(kind = 'fast') {
  const checkboxes = getGridObjectiveElements(kind);
  const objectives = checkboxes
    .filter((checkbox) => checkbox.checked && !checkbox.disabled)
    .map((checkbox) => checkbox.dataset.objective)
    .filter(Boolean);
  const primarySelect = getGridPrimarySelect(kind);
  const primary = objectives.length > 1 && primarySelect ? primarySelect.value : null;
  return { objectives, primary_objective: primary };
}

function updateGridObjectiveSelection(kind = 'fast') {
  const checkboxes = getGridObjectiveElements(kind);
  if (!checkboxes.length) return;

  const enabled = kind !== 'slow' || Boolean(document.getElementById('gridSlowRefinementEnabled')?.checked);
  const supported = kind === 'slow' ? GRID_SUPPORTED_SLOW_OBJECTIVES : GRID_SUPPORTED_OBJECTIVES;
  const selected = [];

  checkboxes.forEach((checkbox) => {
    const objective = checkbox.dataset.objective;
    const unsupported = !supported.has(objective);
    checkbox.disabled = !enabled || unsupported;
    if (unsupported || (!enabled && kind === 'slow')) {
      checkbox.title = unsupported ? 'This objective is not supported for Grid.' : '';
    } else {
      checkbox.title = '';
    }
    if (checkbox.checked && !checkbox.disabled) {
      selected.push(objective);
    }
    const item = checkbox.closest('.objective-item');
    if (item) {
      item.classList.toggle('disabled', checkbox.disabled);
      item.title = checkbox.title || '';
    }
  });

  if (!selected.length) {
    const firstAvailable = checkboxes.find((checkbox) => !checkbox.disabled);
    if (firstAvailable) {
      firstAvailable.checked = true;
      selected.push(firstAvailable.dataset.objective);
    }
  }

  const row = getGridPrimaryRow(kind);
  const select = getGridPrimarySelect(kind);
  if (row && select) {
    if (enabled && selected.length > 1) {
      row.style.display = 'flex';
      const previous = select.value;
      select.innerHTML = '';
      selected.forEach((objective) => {
        const option = document.createElement('option');
        option.value = objective;
        option.textContent = GRID_OBJECTIVE_LABELS[objective] || objective;
        select.appendChild(option);
      });
      select.value = selected.includes(previous) ? previous : selected[0];
    } else {
      row.style.display = 'none';
      select.innerHTML = '';
    }
  }
}

function syncGridObjectiveUi() {
  updateGridObjectiveSelection('fast');
  updateGridObjectiveSelection('slow');
}

function getEnabledGridMetadata() {
  const metadata = window.currentStrategyConfig?.grid_optimizer || {};
  const supported = Boolean(metadata.supported);
  const numbaAvailable = metadata.numba_available !== false;
  return {
    ...metadata,
    profile: metadata.profile || 'sampled_by_mode',
    modes: Array.isArray(metadata.modes) ? metadata.modes : [],
    available: Boolean(metadata.available ?? (supported && numbaAvailable)),
    reason: metadata.reason || (supported ? '' : 'No fast Grid backend is available.')
  };
}

function isFullEnumerationProfile(profile) {
  return profile === 'full_enumeration' || profile === 'full_enumeration_v2';
}

function getSelectedGridModes() {
  return Array.from(document.querySelectorAll('input[name="gridEnabledMode"]'))
    .filter((checkbox) => checkbox.checked)
    .map((checkbox) => String(checkbox.value || '').trim())
    .filter(Boolean);
}

function syncGridProfileUi() {
  const metadata = getEnabledGridMetadata();
  const fullEnumeration = isFullEnumerationProfile(metadata.profile);
  const modeSection = document.getElementById('gridProfileModesSection');
  const modeContainer = document.getElementById('gridEnabledModes');
  const budgetRow = document.getElementById('gridBudgetRow');
  const seedRow = document.getElementById('gridSeedRow');
  const allocationSection = document.getElementById('gridAllocationSection');
  const samplingInput = document.getElementById('gridSamplingMethod');
  const diversityLabel = document.getElementById('gridDiversityMaxLabel');

  if (budgetRow) budgetRow.style.display = fullEnumeration ? 'none' : 'flex';
  if (seedRow) seedRow.style.display = fullEnumeration ? 'none' : 'flex';
  if (allocationSection) allocationSection.style.display = fullEnumeration ? 'none' : 'block';
  if (samplingInput) samplingInput.value = fullEnumeration ? 'Full enumeration' : 'LHS by mode';
  if (diversityLabel) {
    diversityLabel.textContent = fullEnumeration
      ? 'Max per diversity group'
      : 'Max per MA group';
  }

  if (modeSection) modeSection.style.display = fullEnumeration ? 'block' : 'none';
  if (!modeContainer) return;
  const profileKey = `${window.currentStrategyId || ''}:${metadata.profile}`;
  if (modeContainer.dataset.profileKey === profileKey) return;
  modeContainer.dataset.profileKey = profileKey;
  modeContainer.innerHTML = '';
  metadata.modes.forEach((mode) => {
    const modeId = String(mode?.id || '').trim();
    if (!modeId) return;
    const label = document.createElement('label');
    label.className = 'optimizer-mode-option';
    const checkbox = document.createElement('input');
    checkbox.type = 'checkbox';
    checkbox.name = 'gridEnabledMode';
    checkbox.id = `gridEnabledMode-${modeId}`;
    checkbox.value = modeId;
    checkbox.checked = mode.default_enabled !== false;
    checkbox.addEventListener('change', () => {
      if (!getSelectedGridModes().length) {
        checkbox.checked = true;
        setGridPreviewError('At least one Grid mode must remain enabled.');
      }
      scheduleGridPreviewUpdate();
    });
    label.appendChild(checkbox);
    label.appendChild(document.createTextNode(` ${mode.label || modeId}`));
    modeContainer.appendChild(label);
  });
}

function readSelectedOptimizerOptionValues(paramName) {
  return Array.from(
    document.querySelectorAll(
      `input.select-option-checkbox[data-param-name="${paramName}"]:not([data-option-value="__ALL__"])`
    )
  )
    .filter((cb) => cb.checked)
    .map((cb) => cb.dataset.optionValue);
}

function parseBoolOptionValue(rawValue) {
  if (rawValue === true || rawValue === false) return rawValue;
  const normalized = String(rawValue ?? '').trim().toLowerCase();
  if (normalized === 'true' || normalized === '1' || normalized === 'yes' || normalized === 'on') {
    return true;
  }
  if (normalized === 'false' || normalized === '0' || normalized === 'no' || normalized === 'off') {
    return false;
  }
  return null;
}

function validateOptimizerForm(config) {
  const params = config?.parameters || {};
  const errors = [];
  let enabledCount = 0;
  const optimizerMode = getOptimizerMode();

  getOptimizerParamElements().forEach(({ name, checkbox, fromInput, toInput, stepInput, def }) => {
    const paramDef = def || params[name] || {};
    const paramType = paramDef.type || 'float';
    const label = paramDef.label || name;
    const enabled = Boolean(checkbox && checkbox.checked);

    if (!enabled) return;

    enabledCount += 1;

    if (
      paramType === 'select'
      || paramType === 'options'
      || paramType === 'bool'
      || paramType === 'boolean'
    ) {
      const selectedOptions = readSelectedOptimizerOptionValues(name);
      if (!selectedOptions.length) {
        errors.push(`${label}: select at least one option to optimize.`);
      }
      return;
    }

    const fromVal = Number(fromInput?.value);
    const toVal = Number(toInput?.value);
    const stepVal = Number(stepInput?.value);

    if (!Number.isFinite(fromVal) || !Number.isFinite(toVal) || !Number.isFinite(stepVal)) {
      errors.push(`${label}: enter valid numeric values for range and step.`);
      return;
    }

    if (stepVal <= 0) {
      errors.push(`${label}: step must be greater than 0.`);
    }
    if (fromVal > toVal || (optimizerMode !== 'grid' && fromVal === toVal)) {
      errors.push(`${label}: from must be ${optimizerMode === 'grid' ? 'less than or equal to' : 'less than'} to.`);
    }

    const minBound = paramDef.optimize?.min ?? paramDef.min;
    const maxBound = paramDef.optimize?.max ?? paramDef.max;

    if (minBound !== undefined && fromVal < minBound) {
      errors.push(`${label}: from below minimum (${minBound}).`);
    }
    if (maxBound !== undefined && toVal > maxBound) {
      errors.push(`${label}: to above maximum (${maxBound}).`);
    }
  });

  const gridMetadata = getEnabledGridMetadata();
  if (
    enabledCount === 0
    && !(optimizerMode === 'grid' && isFullEnumerationProfile(gridMetadata.profile))
  ) {
    errors.push('Enable at least one parameter to optimize.');
  }

  if (optimizerMode === 'grid') {
    const gridMeta = getEnabledGridMetadata();
    if (!gridMeta.available) {
      errors.push(gridMeta.reason || 'Grid mode is unavailable for this strategy.');
    }
    if (isFullEnumerationProfile(gridMeta.profile) && !getSelectedGridModes().length) {
      errors.push('Enable at least one Grid mode.');
    }

    const fastSelection = collectGridObjectiveSelection('fast');
    if (!fastSelection.objectives.length) {
      errors.push('Select at least one Grid fast objective.');
    }
    fastSelection.objectives.forEach((objective) => {
      if (!GRID_SUPPORTED_OBJECTIVES.has(objective)) {
        errors.push(
          objective === 'composite_score'
            ? 'Composite Score is not supported in Grid v1.'
            : `${objective}: objective is not supported in Grid v1.`
        );
      }
    });
    if (
      fastSelection.objectives.length > 1
      && !fastSelection.objectives.includes(fastSelection.primary_objective)
    ) {
      errors.push('Primary Grid fast objective must be selected.');
    }

    const slowEnabled = Boolean(document.getElementById('gridSlowRefinementEnabled')?.checked);
    const slowSelection = collectGridObjectiveSelection('slow');
    if (slowEnabled) {
      if (!slowSelection.objectives.length) {
        errors.push('Select at least one Grid slow objective when slow refinement is enabled.');
      }
      slowSelection.objectives.forEach((objective) => {
        if (!GRID_SUPPORTED_SLOW_OBJECTIVES.has(objective)) {
          errors.push(`${objective}: objective is not supported for Grid slow refinement.`);
        }
      });
      if (
        slowSelection.objectives.length > 1
        && !slowSelection.objectives.includes(slowSelection.primary_objective)
      ) {
        errors.push('Primary Grid slow objective must be selected.');
      }
    }

    const constraints = window.OptunaUI ? window.OptunaUI.collectConstraints() : [];
    constraints.filter((item) => item && item.enabled).forEach((item) => {
      if (!GRID_SUPPORTED_CONSTRAINTS.has(item.metric)) {
        errors.push(`${item.metric}: constraint is not supported in Grid v1.`);
      }
    });

    if (window.currentStrategyId === 's03_reversal_v10') {
      const maTypeOptions = readSelectedOptimizerOptionValues('maType3');
      const selectedMaTypes = maTypeOptions.map((value) => String(value || '').trim().toUpperCase());
      if (selectedMaTypes.includes('VWAP')) {
        errors.push('VWAP is not supported in S03 Grid mode.');
      }
    }
  }

  return errors;
}

function collectOptimizerParams() {
  const ranges = {};
  const params = window.currentStrategyConfig?.parameters || {};

  Object.entries(params).forEach(([paramName, paramDef]) => {
    const checkbox = document.getElementById(`opt-${paramName}`);
    if (!checkbox || !checkbox.checked) return;

    const paramType = paramDef.type || 'float';

    if (
      paramType === 'select'
      || paramType === 'options'
      || paramType === 'bool'
      || paramType === 'boolean'
    ) {
      const selectedOptions = readSelectedOptimizerOptionValues(paramName);
      if (!selectedOptions.length) return;

      if (paramType === 'bool' || paramType === 'boolean') {
        const boolValues = [];
        selectedOptions.forEach((value) => {
          const parsed = parseBoolOptionValue(value);
          if (parsed === null || boolValues.includes(parsed)) return;
          boolValues.push(parsed);
        });
        if (boolValues.length > 0) {
          ranges[paramName] = {
            type: 'select',
            values: boolValues
          };
        }
      } else {
        ranges[paramName] = {
          type: 'select',
          values: selectedOptions
        };
      }
      return;
    }

    const fromInput = document.getElementById(`opt-${paramName}-from`);
    const toInput = document.getElementById(`opt-${paramName}-to`);
    const stepInput = document.getElementById(`opt-${paramName}-step`);

    if (fromInput && toInput && stepInput) {
      const fromValue = parseFloat(fromInput.value);
      const toValue = parseFloat(toInput.value);
      const stepValue = parseFloat(stepInput.value);

      if (isNaN(fromValue) || isNaN(toValue) || isNaN(stepValue)) {
        console.warn(`Invalid values for parameter ${paramName}, skipping`);
        return;
      }

      if (fromValue >= toValue) {
        console.warn(`From >= To for parameter ${paramName}, skipping`);
        return;
      }

      if (stepValue <= 0) {
        console.warn(`Invalid step for parameter ${paramName}, skipping`);
        return;
      }

      ranges[paramName] = [fromValue, toValue, stepValue];
    }
  });

  return ranges;
}

function buildOptimizationConfig(state, optimizerMode = 'optuna') {
  const enabledParams = {};
  const paramRanges = {};
  const paramTypes = {};
  const fixedParams = {
    dateFilter: state.payload.dateFilter,
    start: state.start,
    end: state.end
  };

  const { checkbox: minProfitCheckbox, input: minProfitInput } = getMinProfitElements();
  const filterEnabled = Boolean(minProfitCheckbox && minProfitCheckbox.checked);
  let minProfitThreshold = 0;
  if (minProfitInput) {
    const parsedValue = Number(minProfitInput.value);
    if (Number.isFinite(parsedValue)) {
      minProfitThreshold = Math.min(99000, Math.max(0, parsedValue));
    }
  }

  const dynamicParams = collectDynamicBacktestParams();
  const paramsDef = window.currentStrategyConfig?.parameters || {};
  const optimizableNames = new Set();

  Object.entries(paramsDef).forEach(([name, def]) => {
    paramTypes[name] = def.type || 'float';
  });

  getOptimizerParamElements().forEach(({ name, checkbox, fromInput, toInput, stepInput, def }) => {
    optimizableNames.add(name);
    const paramDef = def || {};
    const paramType = paramDef.type || 'float';
    const isChecked = Boolean(checkbox && checkbox.checked);
    enabledParams[name] = isChecked;

    if (isChecked) {
      if (
        paramType === 'select'
        || paramType === 'options'
        || paramType === 'bool'
        || paramType === 'boolean'
      ) {
        const selectedOptions = readSelectedOptimizerOptionValues(name);
        if (selectedOptions.length > 0) {
          if (paramType === 'bool' || paramType === 'boolean') {
            const boolValues = [];
            selectedOptions.forEach((value) => {
              const parsed = parseBoolOptionValue(value);
              if (parsed === null || boolValues.includes(parsed)) return;
              boolValues.push(parsed);
            });
            if (boolValues.length > 0) {
              paramRanges[name] = {
                type: 'select',
                values: boolValues
              };
            }
          } else {
            paramRanges[name] = {
              type: 'select',
              values: selectedOptions
            };
          }
        }
      } else if (fromInput && toInput && stepInput) {
        const fromValue = Number(fromInput.value);
        const toValue = Number(toInput.value);
        const stepValue = Math.abs(Number(stepInput.value));
        if (
          Number.isFinite(fromValue) &&
          Number.isFinite(toValue) &&
          Number.isFinite(stepValue) &&
          stepValue > 0 &&
          (fromValue < toValue || (optimizerMode === 'grid' && fromValue === toValue))
        ) {
          paramRanges[name] = [fromValue, toValue, stepValue];
        }
      }
    } else {
      fixedParams[name] = getBacktestParamValue(name, paramDef, dynamicParams);
    }
  });

  Object.entries(paramsDef).forEach(([name, def]) => {
    if (optimizableNames.has(name)) return;
    fixedParams[name] = getBacktestParamValue(name, def, dynamicParams);
  });

  const workerProcesses = getWorkerProcessesValue();

  const riskPerTrade = getBacktestParamValue('riskPerTrade', paramsDef.riskPerTrade, dynamicParams) || 0;
  const contractSize = getBacktestParamValue('contractSize', paramsDef.contractSize, dynamicParams) || 0;
  const commissionRate = getBacktestParamValue('commissionPct', paramsDef.commissionPct, dynamicParams);

  return {
    enabled_params: enabledParams,
    param_ranges: paramRanges,
    fixed_params: fixedParams,
    param_types: paramTypes,
    risk_per_trade_pct: Number(riskPerTrade) || 0,
    contract_size: Number(contractSize) || 0,
    commission_rate: commissionRate !== undefined ? Number(commissionRate) || 0 : 0.0005,
    worker_processes: workerProcesses,
    filter_min_profit: filterEnabled,
    min_profit_threshold: minProfitThreshold,
    score_config: collectScoreConfig(),
    detailed_log: Boolean(document.getElementById('detailedLog')?.checked),
    trials_log: Boolean(document.getElementById('trialsLog')?.checked),
    optimization_mode: optimizerMode === 'grid' ? 'grid' : 'optuna'
  };
}

function buildOptunaConfig(state) {
  const baseConfig = buildOptimizationConfig(state, 'optuna');
  const budgetModeRadios = document.querySelectorAll('input[name="budgetMode"]');
  const optunaTrials = document.getElementById('optunaTrials');
  const optunaTimeLimit = document.getElementById('optunaTimeLimit');
  const optunaConvergence = document.getElementById('optunaConvergence');
  const optunaPruning = document.getElementById('optunaPruning');
  const optunaSampler = document.getElementById('optunaSampler');
  const optunaPruner = document.getElementById('optunaPruner');
  const optunaWarmupTrials = document.getElementById('optunaWarmupTrials');
  const optunaCoverageMode = document.getElementById('optunaCoverageMode');
  const dispatcherBatchResultProcessing = document.getElementById('dispatcherBatchResultProcessing');
  const softDuplicateCycleLimitEnabled = document.getElementById('softDuplicateCycleLimitEnabled');
  const dispatcherDuplicateCycleLimit = document.getElementById('dispatcherDuplicateCycleLimit');
  const nsgaPopulation = document.getElementById('nsgaPopulationSize');
  const nsgaCrossover = document.getElementById('nsgaCrossoverProb');
  const nsgaMutation = document.getElementById('nsgaMutationProb');
  const nsgaSwapping = document.getElementById('nsgaSwappingProb');

  const selectedBudget = Array.from(budgetModeRadios).find((radio) => radio.checked)?.value || 'trials';
  const trialsValue = Number(optunaTrials?.value);
  const timeLimitMinutes = Number(optunaTimeLimit?.value);
  const convergenceValue = Number(optunaConvergence?.value);
  const warmupValue = Number(optunaWarmupTrials?.value);
  const duplicateCycleLimitRaw = dispatcherDuplicateCycleLimit?.value;
  const duplicateCycleLimitValue = duplicateCycleLimitRaw === '' || duplicateCycleLimitRaw === undefined
    ? Number.NaN
    : Number(duplicateCycleLimitRaw);
  const populationValue = Number(nsgaPopulation?.value);
  const crossoverValue = Number(nsgaCrossover?.value);
  const mutationRaw = nsgaMutation?.value;
  const mutationValue = mutationRaw === '' || mutationRaw === undefined ? null : Number(mutationRaw);
  const swappingValue = Number(nsgaSwapping?.value);

  const normalizedTrials = Number.isFinite(trialsValue) ? Math.max(10, Math.min(10000, Math.round(trialsValue))) : 500;
  const normalizedMinutes = Number.isFinite(timeLimitMinutes) ? Math.max(1, Math.round(timeLimitMinutes)) : 60;
  const normalizedConvergence = Number.isFinite(convergenceValue)
    ? Math.max(10, Math.min(500, Math.round(convergenceValue)))
    : 50;
  const normalizedWarmup = Number.isFinite(warmupValue) ? Math.max(0, Math.min(50000, Math.round(warmupValue))) : 20;
  const normalizedDuplicateCycleLimit = Number.isFinite(duplicateCycleLimitValue)
    ? Math.max(1, Math.min(1000, Math.round(duplicateCycleLimitValue)))
    : 18;
  const normalizedPopulation = Number.isFinite(populationValue) ? Math.max(2, Math.min(1000, Math.round(populationValue))) : 50;
  const normalizedCrossover = Number.isFinite(crossoverValue) ? Math.max(0, Math.min(1, crossoverValue)) : 0.9;
  const normalizedMutation = Number.isFinite(mutationValue) ? Math.max(0, Math.min(1, mutationValue)) : null;
  const normalizedSwapping = Number.isFinite(swappingValue) ? Math.max(0, Math.min(1, swappingValue)) : 0.5;

  const objectiveConfig = window.OptunaUI
    ? window.OptunaUI.collectObjectives()
    : { objectives: ['net_profit_pct'], primary_objective: null };
  const constraints = window.OptunaUI ? window.OptunaUI.collectConstraints() : [];
  const sanitizeConfig = window.OptunaUI
    ? window.OptunaUI.collectSanitizeConfig()
    : { sanitize_enabled: true, sanitize_trades_threshold: 0 };
  const selectedObjectives = objectiveConfig.objectives || [];
  const postProcessConfig = window.PostProcessUI
    ? window.PostProcessUI.collectConfig()
    : { enabled: false, ftPeriodDays: 30, topK: 10, sortMetric: 'profit_degradation' };
  const oosTestConfig = window.OosTestUI
    ? window.OosTestUI.collectConfig()
    : { enabled: false, periodDays: 30, topK: 20 };

  return {
    ...baseConfig,
    optimization_mode: 'optuna',
    optuna_budget_mode: selectedBudget,
    optuna_n_trials: normalizedTrials,
    optuna_time_limit: normalizedMinutes * 60,
    optuna_convergence: normalizedConvergence,
    optuna_enable_pruning: selectedObjectives.length > 1 ? false : Boolean(optunaPruning && optunaPruning.checked),
    sampler: optunaSampler ? optunaSampler.value : 'tpe',
    optuna_pruner: optunaPruner ? optunaPruner.value : 'median',
    n_startup_trials: normalizedWarmup,
    coverage_mode: Boolean(optunaCoverageMode && optunaCoverageMode.checked),
    dispatcher_batch_result_processing: Boolean(
      dispatcherBatchResultProcessing ? dispatcherBatchResultProcessing.checked : true
    ),
    dispatcher_soft_duplicate_cycle_limit_enabled: Boolean(
      softDuplicateCycleLimitEnabled ? softDuplicateCycleLimitEnabled.checked : true
    ),
    dispatcher_duplicate_cycle_limit: normalizedDuplicateCycleLimit,
    objectives: selectedObjectives,
    primary_objective: objectiveConfig.primary_objective,
    constraints,
    sanitize_enabled: sanitizeConfig.sanitize_enabled,
    sanitize_trades_threshold: sanitizeConfig.sanitize_trades_threshold,
    population_size: normalizedPopulation,
    crossover_prob: normalizedCrossover,
    mutation_prob: normalizedMutation,
    swapping_prob: normalizedSwapping,
    postProcess: postProcessConfig,
    oosTest: oosTestConfig
  };
}

function buildGridConfig(state) {
  const optunaCompatibleConfig = buildOptunaConfig(state);
  const gridBaseConfig = buildOptimizationConfig(state, 'grid');
  const metadata = getEnabledGridMetadata();
  const fullEnumeration = isFullEnumerationProfile(metadata.profile);
  const previewBudget = Number(window.lastGridPreview?.actual_budget);
  const budget = fullEnumeration && Number.isFinite(previewBudget) && previewBudget > 0
    ? previewBudget
    : getGridBudgetValue();
  const seedRaw = Number(document.getElementById('gridSeed')?.value);
  const topRaw = Number(document.getElementById('gridTopCandidates')?.value);
  const minQuotaRaw = Number(document.getElementById('gridMinQuota')?.value);
  const diversityMaxRaw = Number(document.getElementById('gridDiversityMaxPerGroup')?.value);
  const allocationMethod = Array.from(document.querySelectorAll('input[name="gridAllocationMethod"]'))
    .find((radio) => radio.checked)?.value || 'auto_sqrt_space';
  const fastObjectiveConfig = collectGridObjectiveSelection('fast');
  const slowObjectiveConfig = collectGridObjectiveSelection('slow');
  const slowRefinementEnabled = Boolean(document.getElementById('gridSlowRefinementEnabled')?.checked);

  const config = {
    ...optunaCompatibleConfig,
    ...gridBaseConfig,
    optimization_mode: 'grid',
    objectives: fastObjectiveConfig.objectives,
    primary_objective: fastObjectiveConfig.primary_objective,
    grid_budget: budget,
    grid_seed: Number.isFinite(seedRaw) ? Math.max(0, Math.round(seedRaw)) : 42,
    grid_top_candidates: Number.isFinite(topRaw) ? Math.max(1, Math.min(500, Math.round(topRaw))) : 10,
    grid_enabled_modes: fullEnumeration ? getSelectedGridModes() : [],
    grid_allocation_method: allocationMethod,
    grid_min_quota: Number.isFinite(minQuotaRaw) ? Math.max(0, Math.min(0.33, minQuotaRaw)) : 0.10,
    grid_manual_percents: {
      cc_only: Number(document.getElementById('gridManualCc')?.value) || 0,
      tbands_only: Number(document.getElementById('gridManualTbands')?.value) || 0,
      both: Number(document.getElementById('gridManualBoth')?.value) || 0
    },
    grid_diversity_enabled: Boolean(document.getElementById('gridDiversityEnabled')?.checked),
    grid_diversity_max_per_group: Number.isFinite(diversityMaxRaw)
      ? Math.max(1, Math.min(50, Math.round(diversityMaxRaw)))
      : 2,
    grid_strict_validation: Boolean(document.getElementById('gridStrictValidation')?.checked),
    grid_fast_objectives: fastObjectiveConfig.objectives,
    grid_fast_primary_objective: fastObjectiveConfig.primary_objective,
    grid_slow_refinement_enabled: slowRefinementEnabled,
    grid_slow_objectives: slowObjectiveConfig.objectives,
    grid_slow_primary_objective: slowObjectiveConfig.primary_objective
  };

  return config;
}

function buildCurrentOptimizerConfig(state) {
  return getOptimizerMode() === 'grid' ? buildGridConfig(state) : buildOptunaConfig(state);
}
function clearWFResults() {
  const wfStatusEl = document.getElementById('wfStatus');
  if (wfStatusEl) {
    wfStatusEl.textContent = '';
  }
}

function appendDatabaseTargetToFormData(formData) {
  const dbTarget = document.getElementById('dbTarget')?.value || '';
  if (!dbTarget) return;
  formData.append('dbTarget', dbTarget);
}

function getDatabaseTargetValidationError() {
  const dbTarget = document.getElementById('dbTarget')?.value || '';
  if (dbTarget === 'new') {
    return 'Please create and select a database in "Database Target" before running optimization or Walk-Forward.';
  }
  return '';
}

async function runWalkForward({ sources, state }) {
  const wfStatusEl = document.getElementById('wfStatus');

  if (!sources.length) {
    if (wfStatusEl) {
      wfStatusEl.textContent = 'Please select a CSV file before running Walk-Forward.';
    }
    return;
  }

  if (!window.currentStrategyId) {
    if (wfStatusEl) {
      wfStatusEl.textContent = 'Please select a strategy before running Walk-Forward.';
    }
    return;
  }

  const validationErrors = validateOptimizerForm(window.currentStrategyConfig);
  if (validationErrors.length) {
    if (wfStatusEl) {
      wfStatusEl.textContent = `Validation errors:\n${validationErrors.join('\n')}`;
    }
    return;
  }

  const dbTargetError = getDatabaseTargetValidationError();
  if (dbTargetError) {
    if (wfStatusEl) {
      wfStatusEl.textContent = dbTargetError;
    }
    return;
  }

  const totalSources = sources.length;
  const config = buildCurrentOptimizerConfig(state);
  const optimizerMode = config.optimization_mode === 'grid' ? 'grid' : 'optuna';
  const hasEnabledParams = Object.values(config.enabled_params || {}).some(Boolean);
  if (!hasEnabledParams) {
    if (wfStatusEl) {
      wfStatusEl.textContent = 'Please enable at least one parameter to optimize before running Walk-Forward.';
    }
    return;
  }

  const wfIsPeriodDays = document.getElementById('wfIsPeriodDays').value;
  const wfOosPeriodDays = document.getElementById('wfOosPeriodDays').value;
  const wfStoreTopNTrials = document.getElementById('wfStoreTopNTrials')?.value || '50';
  const wfAdaptiveMode = Boolean(document.getElementById('enableAdaptiveWF')?.checked);
  const wfCooldownEnabled = wfAdaptiveMode && Boolean(document.getElementById('wfCooldownEnabled')?.checked);
  const wfCooldownDays = document.getElementById('wfCooldownDays')?.value || '15';
  const wfMaxOosPeriodDays = document.getElementById('wfMaxOosPeriodDays')?.value || '90';
  const wfMinOosTrades = document.getElementById('wfMinOosTrades')?.value || '5';
  const wfCheckIntervalTrades = document.getElementById('wfCheckIntervalTrades')?.value || '3';
  const wfCusumThreshold = document.getElementById('wfCusumThreshold')?.value || '5.0';
  const wfDdThresholdMultiplier = document.getElementById('wfDdThresholdMultiplier')?.value || '1.5';
  const wfInactivityMultiplier = document.getElementById('wfInactivityMultiplier')?.value || '5.0';
  const warmupValue = document.getElementById('warmupBars')?.value || '1000';
  const strategySummary = getStrategySummary();

  optimizationAbortController = new AbortController();
  window.optimizationAbortController = optimizationAbortController;
  saveOptimizationState({
    status: 'running',
    mode: 'wfa',
    run_id: '',
    strategy: strategySummary,
    dataset: {
      label: getDatasetLabel()
    },
    warmupBars: Number(warmupValue) || 1000,
    dateFilter: state.payload.dateFilter,
    start: state.start,
    end: state.end,
    optuna: {
      objectives: config.objectives,
      primaryObjective: config.primary_objective,
      budgetMode: config.optuna_budget_mode,
      nTrials: config.optuna_n_trials,
      timeLimit: config.optuna_time_limit,
      convergence: config.optuna_convergence,
      sampler: config.sampler,
      pruner: config.optuna_pruner,
      workers: config.worker_processes
    },
    grid: optimizerMode === 'grid' ? {
      budget: config.grid_budget,
      seed: config.grid_seed,
      topCandidates: config.grid_top_candidates,
      allocationMethod: config.grid_allocation_method,
      fastObjectives: config.grid_fast_objectives,
      fastPrimaryObjective: config.grid_fast_primary_objective,
      slowRefinementEnabled: config.grid_slow_refinement_enabled,
      slowObjectives: config.grid_slow_objectives,
      slowPrimaryObjective: config.grid_slow_primary_objective
    } : null,
    wfa: {
      isPeriodDays: Number(wfIsPeriodDays),
      oosPeriodDays: Number(wfOosPeriodDays),
      storeTopNTrials: Number(wfStoreTopNTrials),
      adaptiveMode: wfAdaptiveMode,
      cooldownEnabled: wfCooldownEnabled,
      cooldownDays: Number(wfCooldownDays),
      maxOosPeriodDays: Number(wfMaxOosPeriodDays),
      minOosTrades: Number(wfMinOosTrades),
      checkIntervalTrades: Number(wfCheckIntervalTrades),
      cusumThreshold: Number(wfCusumThreshold),
      ddThresholdMultiplier: Number(wfDdThresholdMultiplier),
      inactivityMultiplier: Number(wfInactivityMultiplier)
    },
    fixedParams: clonePreset(config.fixed_params || {}),
    strategyConfig: clonePreset(window.currentStrategyConfig || {})
  });
  openResultsPage();

  const statusMessages = new Array(totalSources).fill('');
  const updateStatus = (index, message) => {
    statusMessages[index] = message;
    if (wfStatusEl) {
      wfStatusEl.textContent = statusMessages.filter(Boolean).join('\n');
    }
  };

  if (wfStatusEl) {
    wfStatusEl.textContent = '';
  }

  const errors = [];
  let successCount = 0;
  let lastSuccessfulData = null;
  let inFlightRunId = '';

  for (let index = 0; index < totalSources; index += 1) {
    const source = sources[index];
    const sourcePath = String(source?.path || '').trim();
    const sourceName = sourcePath || ('source_' + (index + 1));
    const sourceNumber = index + 1;
    const fileLabel = `Processing source ${sourceNumber} of ${totalSources}: ${sourceName}`;

    updateStatus(index, `${fileLabel} - running Walk-Forward...`);

    if (!isAbsoluteFilesystemPath(sourcePath)) {
      const message = 'CSV path must be absolute.';
      errors.push({ file: sourceName, message });
      updateStatus(index, `Error: Source ${sourceNumber} of ${totalSources} (${sourceName}) failed: ${message}`);
      continue;
    }

    const formData = new FormData();
    formData.append('strategy', window.currentStrategyId);
    formData.append('warmupBars', warmupValue);
    formData.append('csvPath', sourcePath);

    formData.append('config', JSON.stringify(config));
    formData.append('wf_is_period_days', wfIsPeriodDays);
    formData.append('wf_oos_period_days', wfOosPeriodDays);
    formData.append('wf_store_top_n_trials', wfStoreTopNTrials);
    formData.append('wf_adaptive_mode', wfAdaptiveMode ? 'true' : 'false');
    formData.append('wf_cooldown_enabled', wfCooldownEnabled ? 'true' : 'false');
    formData.append('wf_cooldown_days', wfCooldownDays);
    formData.append('wf_max_oos_period_days', wfMaxOosPeriodDays);
    formData.append('wf_min_oos_trades', wfMinOosTrades);
    formData.append('wf_check_interval_trades', wfCheckIntervalTrades);
    formData.append('wf_cusum_threshold', wfCusumThreshold);
    formData.append('wf_dd_threshold_multiplier', wfDdThresholdMultiplier);
    formData.append('wf_inactivity_multiplier', wfInactivityMultiplier);
    inFlightRunId = generateOptimizationRunId('wfa');
    formData.append('runId', inFlightRunId);
    setCurrentOptimizationRunId(inFlightRunId);
    appendDatabaseTargetToFormData(formData);
    try {
      const data = await runWalkForwardRequest(formData, optimizationAbortController.signal);

      updateStatus(index, `Success: Source ${sourceNumber} of ${totalSources} (${sourceName}) completed successfully.`);
      successCount += 1;
      lastSuccessfulData = data;
      inFlightRunId = '';
      setCurrentOptimizationRunId('');
    } catch (err) {
      if (err && err.name === 'AbortError') {
        await cancelCurrentRunBestEffort(inFlightRunId);
        inFlightRunId = '';
        setCurrentOptimizationRunId('');
        updateStatus(index, `Cancelled: Source ${sourceNumber} of ${totalSources} (${sourceName}).`);
        updateOptimizationState({ status: 'cancelled', mode: 'wfa', run_id: '' });
        break;
      }
      const message = err && err.message ? err.message : 'Walk-Forward failed.';
      console.error(`Walk-Forward failed for source ${sourceName}`, err);
      errors.push({ file: sourceName, message });
      updateStatus(index, `Error: Source ${sourceNumber} of ${totalSources} (${sourceName}) failed: ${message}`);
      inFlightRunId = '';
      setCurrentOptimizationRunId('');
    }
  }

  if (successCount === totalSources) {
    const summaryMsg = totalSources === 1
      ? 'Success: Walk-Forward completed successfully.'
      : `Success: All ${totalSources} files processed successfully.`;

    if (wfStatusEl) {
      wfStatusEl.textContent = summaryMsg + '\n\n' + statusMessages.filter(Boolean).join('\n');
    }

    if (totalSources === 1 && lastSuccessfulData) {
      // Results are available in the Results page; keep main page status only.
    }

    if (lastSuccessfulData) {
      updateOptimizationState({
        status: 'completed',
        mode: 'wfa',
        run_id: '',
        study_id: lastSuccessfulData.study_id || '',
        summary: lastSuccessfulData.summary || {},
        dataPath: lastSuccessfulData.data_path || '',
        strategyId: lastSuccessfulData.strategy_id || strategySummary.id || ''
      });
    }
  } else if (successCount > 0) {
    const summaryMsg = `Partial: ${successCount} of ${totalSources} files processed successfully, ${errors.length} failed.`;
    if (wfStatusEl) {
      wfStatusEl.textContent = summaryMsg + '\n\n' + statusMessages.filter(Boolean).join('\n');
    }

    if (successCount === 1 && lastSuccessfulData) {
      updateOptimizationState({
        status: 'completed',
        mode: 'wfa',
        run_id: '',
        study_id: lastSuccessfulData.study_id || '',
        summary: lastSuccessfulData.summary || {},
        dataPath: lastSuccessfulData.data_path || '',
        strategyId: lastSuccessfulData.strategy_id || strategySummary.id || ''
      });
    }
  } else {
    const summaryMsg = `Error: All ${totalSources} files failed.`;
    if (wfStatusEl) {
      wfStatusEl.textContent = summaryMsg + '\n\n' + statusMessages.filter(Boolean).join('\n');
    }
    updateOptimizationState({
      status: 'error',
      mode: 'wfa',
      run_id: '',
      error: 'All walk-forward runs failed.'
    });
  }
  setCurrentOptimizationRunId('');
}

function buildBacktestRequestFormData(csvPath, payload) {
  const formData = new FormData();
  formData.append('strategy', window.currentStrategyId);
  const warmupInput = document.getElementById('warmupBars');
  formData.append('warmupBars', warmupInput ? warmupInput.value : '1000');
  if (csvPath) {
    formData.append('csvPath', csvPath);
  }
  formData.append('payload', JSON.stringify(payload));
  return formData;
}

async function triggerDownloadFromResponse(response, fallbackFilename) {
  const blob = await response.blob();
  const url = window.URL.createObjectURL(blob);
  const link = document.createElement('a');
  link.href = url;

  let filename = fallbackFilename;
  const disposition = response.headers.get('Content-Disposition');
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
}

async function executeBacktestRun({ event = null, downloadTrades = false } = {}) {
  if (event && typeof event.preventDefault === 'function') {
    event.preventDefault();
  }
  const resultsEl = document.getElementById('results');
  const errorEl = document.getElementById('error');
  if (!resultsEl || !errorEl) {
    return;
  }

  errorEl.style.display = 'none';
  resultsEl.classList.remove('ready');

  const selectedPaths = getSelectedCsvPaths();
  const primaryPath = selectedPaths.length ? selectedPaths[0] : '';

  if (!primaryPath) {
    errorEl.textContent = 'Please select a CSV file before running.';
    errorEl.style.display = 'block';
    return;
  }

  if (!isAbsoluteFilesystemPath(primaryPath)) {
    errorEl.textContent = 'CSV path must be absolute.';
    errorEl.style.display = 'block';
    return;
  }

  const state = gatherFormState();
  if (!state.start || !state.end) {
    errorEl.textContent = 'Please fill in start and end dates.';
    errorEl.style.display = 'block';
    return;
  }

  if (!window.currentStrategyId) {
    errorEl.textContent = 'Please select a strategy before running.';
    errorEl.style.display = 'block';
    return;
  }

  const combinations = [{}];
  if (!combinations.length) {
    errorEl.textContent = 'No parameter combinations to run.';
    errorEl.style.display = 'block';
    return;
  }

  resultsEl.textContent = 'Running calculation...';
  resultsEl.classList.add('loading');

  const aggregatedResults = [];
  renderSelectedFiles([]);

  for (let index = 0; index < combinations.length; index += 1) {
    const combo = combinations[index];
    const payload = { ...state.payload, ...combo };

    const formData = buildBacktestRequestFormData(primaryPath, payload);

    resultsEl.textContent = `Running calculation... (${index + 1}/${combinations.length})`;

    try {
      const data = await runBacktestRequest(formData);
      aggregatedResults.push(formatResultBlock(index + 1, combinations.length, payload, data));
    } catch (err) {
      resultsEl.textContent = 'An error occurred.';
      resultsEl.classList.remove('loading');
      errorEl.textContent = err.message;
      errorEl.style.display = 'block';
      return;
    }

    if (downloadTrades) {
      try {
        const tradesResponse = await downloadBacktestTradesRequest(
          buildBacktestRequestFormData(primaryPath, payload)
        );
        await triggerDownloadFromResponse(
          tradesResponse,
          `backtest_trades_${Date.now()}.csv`
        );
      } catch (err) {
        resultsEl.textContent = aggregatedResults.join('\n\n');
        resultsEl.classList.remove('loading');
        resultsEl.classList.add('ready');
        errorEl.textContent = err.message || 'Backtest trade export failed.';
        errorEl.style.display = 'block';
        return;
      }
    }
  }

  resultsEl.textContent = aggregatedResults.join('\n\n');
  resultsEl.classList.remove('loading');
  resultsEl.classList.add('ready');
}

async function runBacktest(event) {
  await executeBacktestRun({ event, downloadTrades: false });
}

async function runBacktestAndDownloadTrades(event) {
  await executeBacktestRun({ event, downloadTrades: true });
}

function syncGridBudgetHelp({ normalizeInput = false } = {}) {
  const input = document.getElementById('gridBudget');
  const help = document.getElementById('gridBudgetHelp');
  if (!input || !help) return;
  const parsed = parseCompactCount(input.value);
  if (!parsed) {
    help.textContent = 'Enter a positive count, for example 200k.';
    return;
  }
  if (normalizeInput) {
    input.value = formatCompactCount(parsed);
  }
  help.textContent = `${parsed.toLocaleString('en-US')} candidates`;
}

function syncGridAllocationUi() {
  if (isFullEnumerationProfile(getEnabledGridMetadata().profile)) return;
  const method = Array.from(document.querySelectorAll('input[name="gridAllocationMethod"]'))
    .find((radio) => radio.checked)?.value || 'auto_sqrt_space';
  const minQuotaRow = document.getElementById('gridMinQuotaRow');
  const manual = document.getElementById('gridManualAllocation');
  if (minQuotaRow) {
    minQuotaRow.style.display = method === 'auto_sqrt_space' ? 'flex' : 'none';
  }
  if (manual) {
    manual.style.display = method === 'manual' ? 'flex' : 'none';
  }
}

function updateGridPreviewDom(preview) {
  window.lastGridPreview = preview || null;
  const summary = document.getElementById('gridPreviewSummary');
  const rowsEl = document.getElementById('gridPreviewRows');
  const errorEl = document.getElementById('gridPreviewError');
  if (errorEl) {
    errorEl.style.display = 'none';
    errorEl.textContent = '';
  }
  if (summary) {
    const total = preview?.total_space_label || '-';
    const budget = preview?.actual_budget_label || preview?.requested_budget_label || '-';
    const coverage = preview?.coverage_label || '-';
    summary.textContent = isFullEnumerationProfile(preview?.profile)
      ? `Parameter space: ${total} semantic combinations. Coverage: ${coverage}. Method: Full enumeration.`
      : `Parameter space: ${total} semantic combinations. Grid budget: ${budget} candidates, ${coverage} coverage.`;
  }
  if (!rowsEl) return;
  rowsEl.innerHTML = '';
  const modes = Array.isArray(preview?.modes) ? preview.modes : [];
  if (!modes.length) {
    rowsEl.innerHTML = '<tr><td colspan="5">No Grid preview available.</td></tr>';
    return;
  }
  modes.forEach((mode) => {
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td>${mode.label || mode.mode || '-'}</td>
      <td>${mode.space_label || mode.space_size || '-'}</td>
      <td>${mode.budget_label || mode.budget || '-'}</td>
      <td>${mode.coverage_label || '-'}</td>
      <td>${mode.generation || '-'}</td>
    `;
    rowsEl.appendChild(tr);
  });
}

function setGridPreviewError(message) {
  const errorEl = document.getElementById('gridPreviewError');
  const summary = document.getElementById('gridPreviewSummary');
  const rowsEl = document.getElementById('gridPreviewRows');
  if (summary) summary.textContent = 'Parameter space: -';
  if (rowsEl) rowsEl.innerHTML = '<tr><td colspan="5">Preview unavailable.</td></tr>';
  if (errorEl) {
    errorEl.textContent = message || 'Grid preview failed.';
    errorEl.style.display = 'block';
  }
}

function scheduleGridPreviewUpdate() {
  if (getOptimizerMode() !== 'grid') return;
  if (gridPreviewTimer) {
    window.clearTimeout(gridPreviewTimer);
  }
  gridPreviewTimer = window.setTimeout(updateGridPreview, 250);
}

async function updateGridPreview() {
  if (getOptimizerMode() !== 'grid') return;
  if (typeof fetchGridPreviewRequest !== 'function') return;
  const seq = ++gridPreviewSeq;
  try {
    const state = gatherFormState();
    const config = buildGridConfig(state);
    const warmupBars = Number(document.getElementById('warmupBars')?.value) || 1000;
    const preview = await fetchGridPreviewRequest(config, window.currentStrategyId, warmupBars);
    if (seq !== gridPreviewSeq) return;
    updateGridPreviewDom(preview);
  } catch (error) {
    if (seq !== gridPreviewSeq) return;
    setGridPreviewError(error?.message || 'Grid preview failed.');
  }
}

function syncGridParameterOptions() {
  const isGrid = getOptimizerMode() === 'grid';
  const isS03Grid = isGrid && window.currentStrategyId === 's03_reversal_v10';
  const vwapOptions = Array.from(
    document.querySelectorAll('input.select-option-checkbox[data-param-name="maType3"][data-option-value="VWAP"]')
  );
  vwapOptions.forEach((checkbox) => {
    if (isS03Grid) {
      checkbox.checked = false;
      checkbox.disabled = true;
      checkbox.title = 'VWAP is not supported in S03 Grid mode.';
      const wrapper = checkbox.closest('label');
      if (wrapper) wrapper.title = checkbox.title;
    } else {
      checkbox.disabled = false;
      checkbox.title = '';
      const wrapper = checkbox.closest('label');
      if (wrapper) wrapper.title = '';
    }
  });

  const allMa = document.getElementById('opt-maType3-all');
  if (allMa && isS03Grid && vwapOptions.length) {
    const individual = Array.from(
      document.querySelectorAll('input.select-option-checkbox[data-param-name="maType3"]:not([data-option-value="__ALL__"])')
    ).filter((checkbox) => !checkbox.disabled);
    allMa.checked = individual.length > 0 && individual.every((checkbox) => checkbox.checked);
  }
}

function syncGridObjectiveAndConstraintUi() {
  const isGrid = getOptimizerMode() === 'grid';
  document.querySelectorAll('.objective-checkbox').forEach((checkbox) => {
    const objective = checkbox.dataset.objective;
    const unsupported = isGrid && !GRID_SUPPORTED_OBJECTIVES.has(objective);
    if (unsupported) {
      checkbox.checked = false;
      checkbox.disabled = true;
      checkbox.title = objective === 'composite_score'
        ? 'Composite Score is not supported in Grid v1'
        : 'This objective is not supported in Grid v1';
    } else if (!isGrid) {
      checkbox.disabled = false;
      checkbox.title = '';
    }
    const item = checkbox.closest('.objective-item');
    if (item) {
      item.title = checkbox.title || '';
      item.classList.toggle('disabled', unsupported);
    }
  });

  document.querySelectorAll('.constraint-row').forEach((row) => {
    const checkbox = row.querySelector('.constraint-checkbox');
    const input = row.querySelector('.constraint-input');
    const metric = checkbox ? checkbox.dataset.constraintMetric : '';
    const unsupported = isGrid && !GRID_SUPPORTED_CONSTRAINTS.has(metric);
    if (checkbox) {
      if (unsupported) checkbox.checked = false;
      checkbox.disabled = unsupported;
      checkbox.title = unsupported ? 'This constraint is not supported in Grid v1' : '';
    }
    if (input) {
      input.disabled = unsupported;
    }
    row.classList.toggle('disabled', unsupported);
    row.title = unsupported ? 'This constraint is not supported in Grid v1' : '';
  });
}

function syncOptimizerModeUI() {
  const mode = getOptimizerMode();
  const isGrid = mode === 'grid';
  const gridSettings = document.getElementById('gridSettings');
  const optunaSettings = document.getElementById('optunaSettings');
  const gridRadio = document.getElementById('optimizerModeGrid');
  const gridHelp = document.getElementById('gridModeHelp');
  const gridMeta = getEnabledGridMetadata();

  if (gridRadio) {
    gridRadio.disabled = !gridMeta.available;
    if (!gridMeta.available && gridRadio.checked) {
      document.getElementById('optimizerModeOptuna').checked = true;
    }
  }

  const effectiveGrid = getOptimizerMode() === 'grid';
  if (gridHelp) {
    gridHelp.textContent = gridMeta.available ? '' : gridMeta.reason;
  }
  if (gridSettings) {
    gridSettings.style.display = effectiveGrid ? 'block' : 'none';
    if (effectiveGrid) gridSettings.classList.add('open');
  }
  if (optunaSettings) {
    optunaSettings.style.display = effectiveGrid ? 'none' : 'block';
  }

  syncGridBudgetHelp();
  syncGridProfileUi();
  syncGridAllocationUi();
  syncGridParameterOptions();
  syncGridObjectiveUi();
  syncGridObjectiveAndConstraintUi();

  if (window.OptunaUI && typeof window.OptunaUI.updateObjectiveSelection === 'function') {
    window.OptunaUI.updateObjectiveSelection();
    syncGridObjectiveUi();
    syncGridObjectiveAndConstraintUi();
  }

  if (effectiveGrid) {
    scheduleGridPreviewUpdate();
  }
}

async function submitOptimization(event) {

  event.preventDefault();

  if (typeof isQueueRunning === 'function' && isQueueRunning()) {
    if (typeof requestQueueStopAfterCurrent === 'function') {
      requestQueueStopAfterCurrent();
    } else if (window.optimizationAbortController) {
      window.optimizationAbortController.abort();
    }
    return;
  }

  const queueLoaded = typeof isQueueLoaded === 'function' ? isQueueLoaded() : true;
  const queue = queueLoaded && typeof loadQueue === 'function' ? loadQueue() : { items: [] };
  const queuePendingCount = queueLoaded && typeof getQueuePendingCount === 'function'
    ? getQueuePendingCount()
    : queue.items.length;
  if (queueLoaded && queuePendingCount > 0 && typeof runQueue === 'function') {
    await runQueue();
    return;
  }

  const optimizerResultsEl = document.getElementById('optimizerResults');
  const progressContainer = document.getElementById('optimizerProgress');
  const optunaProgress = document.getElementById('optunaProgress');
  const optunaProgressFill = document.getElementById('optunaProgressFill');
  const optunaProgressText = document.getElementById('optunaProgressText');
  const optunaBestTrial = document.getElementById('optunaBestTrial');
  const optunaCurrentTrial = document.getElementById('optunaCurrentTrial');
  const optunaEta = document.getElementById('optunaEta');

  const selectedPaths = getSelectedCsvPaths();
  const invalidPath = selectedPaths.find((path) => !isAbsoluteFilesystemPath(path));
  if (invalidPath) {
    optimizerResultsEl.textContent = `CSV path must be absolute: ${invalidPath}`;
    optimizerResultsEl.classList.remove('ready');
    optimizerResultsEl.style.display = 'block';
    return;
  }
  const sources = selectedPaths.map((path) => ({ path }));
  if (!sources.length) {
    optimizerResultsEl.textContent = 'Please select at least one CSV file before running optimization.';
    optimizerResultsEl.classList.remove('ready');
    optimizerResultsEl.style.display = 'block';
    return;
  }

  const state = gatherFormState();

  if (!window.currentStrategyId) {
    optimizerResultsEl.textContent = 'Please select a strategy before running optimization.';
    optimizerResultsEl.classList.remove('ready');
    optimizerResultsEl.style.display = 'block';
    return;
  }

  const validationErrors = validateOptimizerForm(window.currentStrategyConfig);
  if (validationErrors.length) {
    optimizerResultsEl.textContent = `Validation errors:\n\n${validationErrors.join('\n')}`;
    optimizerResultsEl.classList.remove('ready');
    optimizerResultsEl.style.display = 'block';
    return;
  }

  const wfToggle = document.getElementById('enableWF');
  const wfEnabled = Boolean(wfToggle && wfToggle.checked && !wfToggle.disabled);

  if (!state.start || !state.end) {
    optimizerResultsEl.textContent = 'Please specify both start and end dates before optimization.';
    optimizerResultsEl.classList.remove('ready');
    optimizerResultsEl.style.display = 'block';
    return;
  }

  if (wfEnabled) {
    clearWFResults();
    await runWalkForward({ sources, state });
    return;
  }

  const dbTargetError = getDatabaseTargetValidationError();
  if (dbTargetError) {
    optimizerResultsEl.textContent = dbTargetError;
    optimizerResultsEl.classList.remove('ready');
    optimizerResultsEl.classList.remove('loading');
    optimizerResultsEl.style.display = 'block';
    return;
  }

  renderSelectedFiles([]);

  const config = buildCurrentOptimizerConfig(state);
  const optimizerMode = config.optimization_mode === 'grid' ? 'grid' : 'optuna';
  const hasEnabledParams = Object.values(config.enabled_params || {}).some(Boolean);
  if (!hasEnabledParams) {
    optimizerResultsEl.textContent = 'Please enable at least one parameter to optimize.';
    optimizerResultsEl.classList.remove('ready');
    optimizerResultsEl.classList.remove('loading');
    optimizerResultsEl.style.display = 'block';
    return;
  }

  const warmupValue = document.getElementById('warmupBars')?.value || '1000';
  const strategySummary = getStrategySummary();

  optimizationAbortController = new AbortController();
  window.optimizationAbortController = optimizationAbortController;
  saveOptimizationState({
    status: 'running',
    mode: optimizerMode,
    run_id: '',
    strategy: strategySummary,
    dataset: {
      label: getDatasetLabel()
    },
    warmupBars: Number(warmupValue) || 1000,
    dateFilter: state.payload.dateFilter,
    start: state.start,
    end: state.end,
    optuna: {
      objectives: config.objectives,
      primaryObjective: config.primary_objective,
      budgetMode: config.optuna_budget_mode,
      nTrials: config.optuna_n_trials,
      timeLimit: config.optuna_time_limit,
      convergence: config.optuna_convergence,
      sampler: config.sampler,
      pruner: config.optuna_pruner,
      workers: config.worker_processes,
      sanitizeEnabled: config.sanitize_enabled,
      sanitizeTradesThreshold: config.sanitize_trades_threshold
    },
    grid: optimizerMode === 'grid' ? {
      budget: config.grid_budget,
      seed: config.grid_seed,
      topCandidates: config.grid_top_candidates,
      allocationMethod: config.grid_allocation_method
    } : null,
    fixedParams: clonePreset(config.fixed_params || {}),
    strategyConfig: clonePreset(window.currentStrategyConfig || {})
  });
  openResultsPage();
  const totalSources = sources.length;
  const statusMessages = new Array(totalSources).fill('');
  const updateStatus = (index, message) => {
    statusMessages[index] = message;
    optimizerResultsEl.textContent = statusMessages.filter(Boolean).join('\n');
  };

  optimizerResultsEl.textContent = '';
  optimizerResultsEl.classList.add('loading');
  optimizerResultsEl.classList.remove('ready');
  optimizerResultsEl.style.display = 'block';

  progressContainer.style.display = 'block';
  if (optunaProgress) {
    optunaProgress.style.display = 'block';
  }
  if (optunaProgressFill) {
    optunaProgressFill.style.width = '0%';
  }
  if (optunaProgressText) {
    if (optimizerMode === 'grid') {
      optunaProgressText.textContent = `Grid candidates: 0 / ${Number(config.grid_budget || 0).toLocaleString('en-US')}`;
    } else if (config.optuna_budget_mode === 'trials') {
      optunaProgressText.textContent = `Trial: 0 / ${config.optuna_n_trials.toLocaleString('en-US')} (0%)`;
    } else if (config.optuna_budget_mode === 'time') {
      const minutes = Math.round(config.optuna_time_limit / 60);
      optunaProgressText.textContent = `Time budget: ${minutes} min`;
    } else {
      optunaProgressText.textContent = 'Waiting for convergence threshold...';
    }
  }
  if (optunaBestTrial) {
    optunaBestTrial.textContent = optimizerMode === 'grid' ? 'Waiting for Grid candidates...' : 'Waiting for first trial...';
  }
  if (optunaCurrentTrial) {
    optunaCurrentTrial.textContent = optimizerMode === 'grid' ? 'Current candidate: -' : 'Current trial: -';
  }
  if (optunaEta) {
    optunaEta.textContent = 'Est. time remaining: -';
  }

  const errors = [];
  let successCount = 0;
  let lastStudyId = '';
  let lastSummary = null;
  let lastDataPath = '';
  let inFlightRunId = '';
  const optunaBudgetMode = config.optuna_budget_mode;
  const plannedTrials = optunaBudgetMode === 'trials' ? config.optuna_n_trials : null;
  const plannedGridCandidates = optimizerMode === 'grid' ? Number(config.grid_budget || 0) : null;

  for (let index = 0; index < totalSources; index += 1) {
    const source = sources[index];
    const sourcePath = String(source?.path || '').trim();
    const sourceName = sourcePath || ('source_' + (index + 1));
    const sourceNumber = index + 1;
    const fileLabel = `Processing source ${sourceNumber} of ${totalSources}: ${sourceName}`;

    updateStatus(index, `${fileLabel} - processing...`);
    if (optunaProgressText) {
      if (plannedGridCandidates) {
        optunaProgressText.textContent = `Grid candidates: 0 / ${plannedGridCandidates.toLocaleString('en-US')} - running...`;
      } else if (plannedTrials) {
        optunaProgressText.textContent = `Trial: 0 / ${plannedTrials.toLocaleString('en-US')} (0%)`;
      } else if (optunaBudgetMode === 'time') {
        const minutes = Math.round(config.optuna_time_limit / 60);
        optunaProgressText.textContent = `Time budget: ${minutes} min - running...`;
      } else {
        optunaProgressText.textContent = `Running ${optimizerMode === 'grid' ? 'Grid' : 'Optuna'} optimization...`;
      }
    }
    if (optunaProgressFill) {
      optunaProgressFill.style.width = '0%';
    }

    const formData = new FormData();
    formData.append('strategy', window.currentStrategyId);
    formData.append('warmupBars', warmupValue);
    if (!isAbsoluteFilesystemPath(sourcePath)) {
      const message = 'CSV path must be absolute.';
      errors.push({ file: sourceName, message });
      updateStatus(index, `Error: Source ${sourceNumber} of ${totalSources} (${sourceName}) failed: ${message}`);
      continue;
    }
    formData.append('csvPath', sourcePath);
    formData.append('config', JSON.stringify(config));
    inFlightRunId = generateOptimizationRunId('opt');
    formData.append('runId', inFlightRunId);
    setCurrentOptimizationRunId(inFlightRunId);
    appendDatabaseTargetToFormData(formData);

    try {
      const data = await runOptimizationRequest(formData, optimizationAbortController.signal);
      lastStudyId = data.study_id || '';
      lastSummary = data.summary || null;
      lastDataPath = data.data_path || lastDataPath;

      if (optunaProgressFill) {
        optunaProgressFill.style.width = '100%';
      }
      if (optunaProgressText) {
        if (plannedGridCandidates) {
          optunaProgressText.textContent = `Grid candidates: ${plannedGridCandidates.toLocaleString('en-US')} / ${plannedGridCandidates.toLocaleString('en-US')} (100%)`;
        } else if (plannedTrials) {
          optunaProgressText.textContent = `Trial: ${plannedTrials.toLocaleString('en-US')} / ${plannedTrials.toLocaleString('en-US')} (100%)`;
        } else {
          optunaProgressText.textContent = `${optimizerMode === 'grid' ? 'Grid' : 'Optuna'} optimization completed.`;
        }
      }
      if (optunaBestTrial) {
        optunaBestTrial.textContent = 'Optimization completed. Results saved to Studies Manager.';
      }

      updateStatus(index, `Success: Source ${sourceNumber} of ${totalSources} (${sourceName}) processed successfully.`);
      successCount += 1;
      inFlightRunId = '';
      setCurrentOptimizationRunId('');
    } catch (err) {
      if (err && err.name === 'AbortError') {
        await cancelCurrentRunBestEffort(inFlightRunId);
        inFlightRunId = '';
        setCurrentOptimizationRunId('');
        updateStatus(index, `Cancelled: Source ${sourceNumber} of ${totalSources} (${sourceName}).`);
        updateOptimizationState({ status: 'cancelled', mode: optimizerMode, run_id: '' });
        break;
      }
      const message = err && err.message ? err.message : 'Optimization failed.';
      console.error(`Optimization failed for source ${sourceName}`, err);
      errors.push({ file: sourceName, message });

      if (optunaProgressFill) {
        optunaProgressFill.style.width = '0%';
      }
      if (optunaProgressText) {
        optunaProgressText.textContent = `Error: ${message}`;
      }

      updateStatus(index, `Error: Source ${sourceNumber} of ${totalSources} (${sourceName}) failed: ${message}`);
      inFlightRunId = '';
      setCurrentOptimizationRunId('');
    }
  }

  optimizerResultsEl.classList.remove('loading');
  if (successCount > 0) {
    optimizerResultsEl.classList.add('ready');
  } else {
    optimizerResultsEl.classList.remove('ready');
  }

  const summaryMessages = statusMessages.filter(Boolean);
  if (successCount === totalSources) {
    summaryMessages.push(`Optimization complete! All ${totalSources} data source(s) processed successfully.`);
    const serverState = await refreshOptimizationStateFromServer();
    if (!serverState) {
      updateOptimizationState({
        status: 'completed',
        mode: optimizerMode,
        run_id: '',
        study_id: lastStudyId,
        summary: lastSummary || {},
        dataPath: lastDataPath,
        strategyId: strategySummary.id || ''
      });
    }
  } else if (successCount > 0) {
    summaryMessages.push(
      `Optimization finished with ${successCount} successful data source(s) and ${errors.length} error(s).`
    );
    const serverState = await refreshOptimizationStateFromServer();
    if (!serverState) {
      updateOptimizationState({
        status: 'completed',
        mode: optimizerMode,
        run_id: '',
        study_id: lastStudyId,
        summary: lastSummary || {},
        dataPath: lastDataPath,
        strategyId: strategySummary.id || ''
      });
    }
  } else {
    summaryMessages.push('Optimization failed for all selected data sources. See error details above.');
    updateOptimizationState({
      status: 'error',
      mode: optimizerMode,
      run_id: '',
      error: 'Optimization failed for all selected data sources.'
    });
  }
  setCurrentOptimizationRunId('');
  optimizerResultsEl.textContent = summaryMessages.join('\n');
}

function bindOptimizerInputs() {
  const paramCheckboxes = document.querySelectorAll('.opt-param-toggle');

  paramCheckboxes.forEach((checkbox) => {
    checkbox.removeEventListener('change', handleOptimizerCheckboxChange);
    checkbox.addEventListener('change', handleOptimizerCheckboxChange);
    handleOptimizerCheckboxChange.call(checkbox);
  });

  document.querySelectorAll('.select-option-checkbox').forEach((checkbox) => {
    if (checkbox.dataset.gridPreviewBound === '1') return;
    checkbox.dataset.gridPreviewBound = '1';
    checkbox.addEventListener('change', () => {
      syncGridParameterOptions();
      scheduleGridPreviewUpdate();
    });
  });

  document.querySelectorAll('input[id^="opt-"][id$="-from"], input[id^="opt-"][id$="-to"], input[id^="opt-"][id$="-step"]').forEach((input) => {
    if (input.dataset.gridPreviewBound === '1') return;
    input.dataset.gridPreviewBound = '1';
    input.addEventListener('input', scheduleGridPreviewUpdate);
    input.addEventListener('change', scheduleGridPreviewUpdate);
  });

  if (window.OptunaUI && typeof window.OptunaUI.updateCoverageInfo === 'function') {
    window.OptunaUI.updateCoverageInfo();
  }
  syncOptimizerModeUI();
}

function handleOptimizerCheckboxChange() {
  const paramName = this.dataset.paramName || this.id.replace('opt-', '');
  const row = this.closest('.opt-row');
  const fromInput = document.getElementById(`opt-${paramName}-from`);
  const toInput = document.getElementById(`opt-${paramName}-to`);
  const stepInput = document.getElementById(`opt-${paramName}-step`);
  const selectOptions = row
    ? row.querySelectorAll(`input.select-option-checkbox[data-param-name="${paramName}"]`)
    : document.querySelectorAll(`input.select-option-checkbox[data-param-name="${paramName}"]`);

  const disabled = !this.checked;

  if (fromInput) fromInput.disabled = disabled;
  if (toInput) toInput.disabled = disabled;
  if (stepInput) stepInput.disabled = disabled;
  if (selectOptions && selectOptions.length) {
    selectOptions.forEach((optionCheckbox) => {
      optionCheckbox.disabled = disabled;
    });
  }

  if (row) {
    if (disabled) {
      row.classList.add('disabled');
    } else {
      row.classList.remove('disabled');
    }
  }
  syncGridParameterOptions();
  scheduleGridPreviewUpdate();
}

function bindMinProfitFilterControl() {
  const { checkbox, input } = getMinProfitElements();
  if (!checkbox || !input) {
    return;
  }

  checkbox.addEventListener('change', syncMinProfitFilterUI);
  syncMinProfitFilterUI();
}

function bindScoreControls() {
  const { checkbox, input } = getScoreElements();
  if (checkbox) {
    checkbox.addEventListener('change', () => {
      syncScoreFilterUI();
      updateScoreFormulaPreview();
    });
  }
  if (input) {
    input.addEventListener('input', updateScoreFormulaPreview);
  }

  SCORE_METRICS.forEach((metric) => {
    const metricCheckbox = document.getElementById(`metric-${metric}`);
    const weightInput = document.getElementById(`weight-${metric}`);
    if (metricCheckbox) {
      metricCheckbox.addEventListener('change', updateScoreFormulaPreview);
    }
    if (weightInput) {
      weightInput.addEventListener('input', updateScoreFormulaPreview);
    }
  });

  const invertCheckbox = document.getElementById('invert-ulcer');
  if (invertCheckbox) {
    invertCheckbox.addEventListener('change', updateScoreFormulaPreview);
  }

  const resetButton = document.getElementById('resetScoreBtn');
  if (resetButton) {
    resetButton.addEventListener('click', () => {
      const defaultsConfig = window.defaults?.scoreConfig || {};
      const mergedBounds = normalizeScoreBounds({
        ...SCORE_DEFAULT_BOUNDS,
        ...(defaultsConfig.metric_bounds || {})
      });
      applyScoreSettings({
        scoreFilterEnabled: Boolean(defaultsConfig.filter_enabled),
        scoreThreshold: defaultsConfig.min_score_threshold ?? SCORE_DEFAULT_THRESHOLD,
        scoreWeights: clonePreset({ ...SCORE_DEFAULT_WEIGHTS, ...(defaultsConfig.weights || {}) }),
        scoreEnabledMetrics: clonePreset({ ...SCORE_DEFAULT_ENABLED, ...(defaultsConfig.enabled_metrics || {}) }),
        scoreInvertMetrics: clonePreset({ ...SCORE_DEFAULT_INVERT, ...(defaultsConfig.invert_metrics || {}) }),
        scoreMetricBounds: clonePreset(mergedBounds)
      });
      updateScoreFormulaPreview();
    });
  }

  applyScoreSettings();
  syncScoreFilterUI();
  updateScoreFormulaPreview();
}

function bindOptunaUiControls() {
  if (!window.OptunaUI) return;
  const checkboxes = document.querySelectorAll('.objective-checkbox');
  checkboxes.forEach((checkbox) => {
    checkbox.addEventListener('change', () => {
      window.OptunaUI.updateObjectiveSelection();
      syncOptimizerModeUI();
    });
  });
  const sampler = document.getElementById('optunaSampler');
  if (sampler) {
    sampler.addEventListener('change', window.OptunaUI.toggleNsgaSettings);
  }
  window.OptunaUI.initSanitizeControls();
  if (typeof window.OptunaUI.initCoverageInfo === 'function') {
    window.OptunaUI.initCoverageInfo();
  }
  if (typeof window.OptunaUI.initDispatcherControls === 'function') {
    window.OptunaUI.initDispatcherControls();
  }
  window.OptunaUI.updateObjectiveSelection();
  window.OptunaUI.toggleNsgaSettings();
  bindGridControls();
  syncOptimizerModeUI();
}

function bindGridControls() {
  document.querySelectorAll('input[name="optimizerMode"]').forEach((radio) => {
    if (radio.dataset.gridBound === '1') return;
    radio.dataset.gridBound = '1';
    radio.addEventListener('change', syncOptimizerModeUI);
  });

  document.querySelectorAll('input[name="gridAllocationMethod"]').forEach((radio) => {
    if (radio.dataset.gridBound === '1') return;
    radio.dataset.gridBound = '1';
    radio.addEventListener('change', () => {
      syncGridAllocationUi();
      scheduleGridPreviewUpdate();
    });
  });

  [
    'gridSeed',
    'gridTopCandidates',
    'gridMinQuota',
    'gridManualCc',
    'gridManualTbands',
    'gridManualBoth',
    'gridDiversityEnabled',
    'gridDiversityMaxPerGroup',
    'gridStrictValidation',
    'gridSlowRefinementEnabled'
  ].forEach((id) => {
    const element = document.getElementById(id);
    if (!element || element.dataset.gridBound === '1') return;
    element.dataset.gridBound = '1';
    element.addEventListener('input', scheduleGridPreviewUpdate);
    element.addEventListener('change', scheduleGridPreviewUpdate);
  });

  const budgetInput = document.getElementById('gridBudget');
  if (budgetInput && budgetInput.dataset.gridBound !== '1') {
    budgetInput.dataset.gridBound = '1';
    budgetInput.addEventListener('input', () => {
      syncGridBudgetHelp();
      scheduleGridPreviewUpdate();
    });
    budgetInput.addEventListener('blur', () => {
      syncGridBudgetHelp({ normalizeInput: true });
      scheduleGridPreviewUpdate();
    });
  }

  const slowToggle = document.getElementById('gridSlowRefinementEnabled');
  if (slowToggle && slowToggle.dataset.gridSlowToggleBound !== '1') {
    slowToggle.dataset.gridSlowToggleBound = '1';
    slowToggle.addEventListener('change', () => {
      syncGridObjectiveUi();
      scheduleGridPreviewUpdate();
    });
  }

  document.querySelectorAll('.grid-fast-objective-checkbox, .grid-slow-objective-checkbox').forEach((checkbox) => {
    if (!checkbox || checkbox.dataset.gridObjectiveBound === '1') return;
    checkbox.dataset.gridObjectiveBound = '1';
    checkbox.addEventListener('change', () => {
      syncGridObjectiveUi();
      scheduleGridPreviewUpdate();
    });
  });

  ['gridFastPrimaryObjective', 'gridSlowPrimaryObjective'].forEach((id) => {
    const select = document.getElementById(id);
    if (!select || select.dataset.gridObjectiveBound === '1') return;
    select.dataset.gridObjectiveBound = '1';
    select.addEventListener('change', scheduleGridPreviewUpdate);
  });

  syncGridBudgetHelp();
  syncGridAllocationUi();
  syncGridObjectiveUi();
}

function setCheckboxGroup(group, selectedTypes) {
  void group;
  void selectedTypes;
}

function collectSelectedTypes(group) {
  void group;
  return [];
}

function syncAllToggle(group) {
  void group;
}

function bindMASelectors() {
  // MA selector UI removed; nothing to bind.
}
