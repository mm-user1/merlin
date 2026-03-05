const OPT_STATE_KEY = 'merlinOptimizationState';
const OPT_CONTROL_KEY = 'merlinOptimizationControl';
const VOLATILE_RESULTS_KEYS = ['filterActive', 'filterText'];

const ResultsState = {
  status: 'idle',
  mode: 'optuna',
  studyId: '',
  studyName: '',
  studyCreatedAt: '',
  strategy: {},
  strategyId: '',
  dataset: {},
  warmupBars: 1000,
  dateFilter: false,
  start: '',
  end: '',
  consistencySegments: null,
  fixedParams: {},
  strategyConfig: {},
  optuna: {},
  wfa: {},
  summary: {},
  results: [],
  dsr: {
    enabled: false,
    topK: null,
    trials: [],
    nTrials: null,
    meanSharpe: null,
    varSharpe: null
  },
  forwardTest: {
    enabled: false,
    trials: [],
    startDate: '',
    endDate: '',
    periodDays: null,
    sortMetric: 'profit_degradation',
    source: 'optuna'
  },
  stressTest: {
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
  },
  oosTest: {
    enabled: false,
    topK: null,
    periodDays: null,
    startDate: '',
    endDate: '',
    source: '',
    trials: []
  },
  manualTests: [],
  activeManualTest: null,
  manualTestResults: [],
  activeTab: 'optuna',
  stitched_oos: {},
  dataPath: '',
  selectedRowId: null,
  multiSelect: false,
  selectedStudies: [],
  filterActive: false,
  filterText: ''
};

if (typeof window !== 'undefined') {
  window.ResultsState = ResultsState;
}

function readStoredState() {
  const raw = sessionStorage.getItem(OPT_STATE_KEY) || localStorage.getItem(OPT_STATE_KEY);
  if (!raw) return null;
  try {
    return JSON.parse(raw);
  } catch (error) {
    return null;
  }
}

function applyState(state) {
  if (!state) return;
  const localFilterActive = ResultsState.filterActive;
  const localFilterText = ResultsState.filterText;
  Object.assign(ResultsState, state);
  ResultsState.filterActive = localFilterActive;
  ResultsState.filterText = localFilterText;
  ResultsState.status = state.status || 'idle';
  ResultsState.mode = state.mode || 'optuna';
  ResultsState.studyId = state.studyId || state.study_id || ResultsState.studyId;
  ResultsState.studyName = state.studyName || state.study_name || ResultsState.studyName;
  ResultsState.strategy = state.strategy || ResultsState.strategy;
  ResultsState.strategyId = state.strategyId || state.strategy_id || ResultsState.strategyId;
  ResultsState.dataset = state.dataset || ResultsState.dataset;
  ResultsState.dataPath = state.dataPath || state.data_path || ResultsState.dataPath;
  ResultsState.warmupBars = state.warmupBars || state.warmup_bars || ResultsState.warmupBars;
  ResultsState.fixedParams = state.fixedParams || ResultsState.fixedParams;
  if ((!ResultsState.fixedParams || Object.keys(ResultsState.fixedParams).length === 0) && state.config) {
    ResultsState.fixedParams = state.config.fixed_params || ResultsState.fixedParams;
  }
  ResultsState.strategyConfig = state.strategyConfig || ResultsState.strategyConfig;
  ResultsState.optuna = state.optuna || ResultsState.optuna;
  ResultsState.wfa = state.wfa || ResultsState.wfa;
  ResultsState.summary = state.summary || ResultsState.summary;
  ResultsState.results = state.results || state.windows || ResultsState.results;
  ResultsState.stitched_oos = state.stitched_oos || ResultsState.stitched_oos;
  ResultsState.dateFilter = Boolean(state.dateFilter ?? ResultsState.dateFilter);
  ResultsState.start = state.start || ResultsState.start;
  ResultsState.end = state.end || ResultsState.end;

  if (!ResultsState.stitched_oos || !ResultsState.stitched_oos.equity_curve) {
    const summary = state.summary || {};
    if (summary && summary.stitched_oos_net_profit_pct !== undefined) {
      ResultsState.stitched_oos = {
        final_net_profit_pct: summary.stitched_oos_net_profit_pct,
        max_drawdown_pct: summary.stitched_oos_max_drawdown_pct,
        total_trades: summary.stitched_oos_total_trades,
        wfe: summary.wfe,
        oos_win_rate: summary.oos_win_rate,
        equity_curve: [],
        timestamps: [],
        window_ids: []
      };
    }
  }
}

function getQueryStudyId() {
  const params = new URLSearchParams(window.location.search);
  return params.get('study') || '';
}

function setQueryStudyId(studyId) {
  if (!studyId) return;
  const params = new URLSearchParams(window.location.search);
  params.set('study', studyId);
  const newUrl = `${window.location.pathname}?${params.toString()}`;
  window.history.replaceState({}, '', newUrl);
}

function updateStoredState(patch) {
  const current = readStoredState() || {};
  VOLATILE_RESULTS_KEYS.forEach((key) => {
    delete current[key];
  });
  const safePatch = { ...(patch || {}) };
  VOLATILE_RESULTS_KEYS.forEach((key) => {
    delete safePatch[key];
  });
  const updated = { ...current, ...safePatch };
  try {
    const raw = JSON.stringify(updated);
    sessionStorage.setItem(OPT_STATE_KEY, raw);
    localStorage.setItem(OPT_STATE_KEY, raw);
  } catch (error) {
    return;
  }
}

function handleStorageUpdate(event) {
  if (event.key !== OPT_STATE_KEY) return;
  if (!event.newValue) return;
  try {
    const state = JSON.parse(event.newValue);
    applyState(state);
    if (state.study_id || state.studyId) {
      openStudy(state.study_id || state.studyId);
    } else {
      refreshResultsView();
    }
  } catch (error) {
    return;
  }
}
