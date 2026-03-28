/**
 * API communication functions for Strategy Backtester & Optimizer.
 * Dependencies: utils.js
 */

async function fetchStrategies() {
  const response = await fetch('/api/strategies');
  if (!response.ok) {
    throw new Error(`Failed to fetch strategies: ${response.status}`);
  }
  return response.json();
}

async function fetchStrategyConfig(strategyId) {
  const response = await fetch(`/api/strategy/${strategyId}/config`);
  if (!response.ok) {
    throw new Error(`Server returned ${response.status}: ${response.statusText}`);
  }
  const config = await response.json();

  if (!config || typeof config !== 'object') {
    throw new Error('Invalid config format');
  }

  if (!config.parameters || typeof config.parameters !== 'object') {
    throw new Error('Missing parameters in config');
  }

  return config;
}

async function runBacktestRequest(formData) {
  const response = await fetch('/api/backtest', {
    method: 'POST',
    body: formData
  });

  if (!response.ok) {
    const message = await response.text();
    throw new Error(message || 'Backtest request failed.');
  }

  return response.json();
}

async function downloadBacktestTradesRequest(formData) {
  const response = await fetch('/api/backtest/trades', {
    method: 'POST',
    body: formData
  });

  if (!response.ok) {
    const message = await response.text();
    throw new Error(message || 'Backtest trade export failed.');
  }

  return response;
}

async function runOptimizationRequest(formData, signal = null) {
  const response = await fetch('/api/optimize', {
    method: 'POST',
    body: formData,
    signal: signal || undefined
  });

  if (!response.ok) {
    const message = await response.text();
    throw new Error(message || 'Optimization request failed.');
  }

  const data = await response.json();
  if (data && data.status === 'cancelled') {
    const error = new Error('Optimization cancelled.');
    error.name = 'AbortError';
    throw error;
  }
  return data;
}

async function runWalkForwardRequest(formData, signal = null) {
  const response = await fetch('/api/walkforward', {
    method: 'POST',
    body: formData,
    signal: signal || undefined
  });

  const data = await response.json();
  if (data && data.status === 'cancelled') {
    const error = new Error('Walk-Forward request cancelled.');
    error.name = 'AbortError';
    throw error;
  }

  if (!response.ok || data.status !== 'success') {
    const message = data && data.error ? data.error : 'Walk-Forward request failed.';
    throw new Error(message);
  }

  return data;
}

async function fetchOptimizationStatus() {
  const response = await fetch('/api/optimization/status');
  if (!response.ok) {
    throw new Error(`Status request failed: ${response.status}`);
  }
  return response.json();
}

async function fetchQueueStateRequest() {
  const response = await fetch('/api/queue');
  if (!response.ok) {
    const payload = await response.json().catch(() => ({}));
    throw new Error(payload.error || 'Failed to load queue state.');
  }
  return response.json();
}

async function saveQueueStateRequest(queueState) {
  const response = await fetch('/api/queue', {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(queueState || {})
  });
  if (!response.ok) {
    const payload = await response.json().catch(() => ({}));
    throw new Error(payload.error || 'Failed to save queue state.');
  }
  return response.json();
}

async function clearQueueStateRequest() {
  const response = await fetch('/api/queue', {
    method: 'DELETE'
  });
  if (!response.ok) {
    const payload = await response.json().catch(() => ({}));
    throw new Error(payload.error || 'Failed to clear queue state.');
  }
  return response.json();
}

async function fetchStudiesList() {
  const response = await fetch('/api/studies');
  if (!response.ok) {
    throw new Error(`Failed to fetch studies: ${response.status}`);
  }
  return response.json();
}

async function browseCsvDirectoryRequest(path = '') {
  const params = new URLSearchParams();
  if (path) {
    params.set('path', path);
  }
  const url = params.toString() ? `/api/csv/browse?${params.toString()}` : '/api/csv/browse';
  const response = await fetch(url);
  if (!response.ok) {
    let message = 'Failed to browse CSV directory.';
    try {
      const payload = await response.json();
      if (payload && payload.error) {
        message = payload.error;
      }
    } catch (error) {
      const fallback = await response.text();
      if (fallback) {
        message = fallback;
      }
    }
    throw new Error(message);
  }
  return response.json();
}

async function fetchStudyDetails(studyId) {
  const response = await fetch(`/api/studies/${encodeURIComponent(studyId)}`);
  if (!response.ok) {
    throw new Error(`Failed to fetch study: ${response.status}`);
  }
  return response.json();
}

async function deleteStudyRequest(studyId) {
  const response = await fetch(`/api/studies/${encodeURIComponent(studyId)}`, {
    method: 'DELETE'
  });
  if (!response.ok) {
    const message = await response.text();
    throw new Error(message || 'Failed to delete study.');
  }
}

async function updateStudyCsvPathRequest(studyId, formData) {
  const response = await fetch(`/api/studies/${encodeURIComponent(studyId)}/update-csv-path`, {
    method: 'POST',
    body: formData
  });
  if (!response.ok) {
    const message = await response.text();
    throw new Error(message || 'Failed to update CSV path.');
  }
  return response.json();
}

async function runManualTestRequest(studyId, payload) {
  const response = await fetch(`/api/studies/${encodeURIComponent(studyId)}/test`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload)
  });
  if (!response.ok) {
    const message = await response.text();
    throw new Error(message || 'Manual test failed.');
  }
  return response.json();
}

async function fetchManualTestsList(studyId) {
  const response = await fetch(`/api/studies/${encodeURIComponent(studyId)}/tests`);
  if (!response.ok) {
    const message = await response.text();
    throw new Error(message || 'Failed to fetch manual tests.');
  }
  return response.json();
}

async function fetchManualTestResults(studyId, testId) {
  const response = await fetch(`/api/studies/${encodeURIComponent(studyId)}/tests/${encodeURIComponent(testId)}`);
  if (!response.ok) {
    const message = await response.text();
    throw new Error(message || 'Failed to fetch manual test.');
  }
  return response.json();
}

async function deleteManualTestRequest(studyId, testId) {
  const response = await fetch(`/api/studies/${encodeURIComponent(studyId)}/tests/${encodeURIComponent(testId)}`, {
    method: 'DELETE'
  });
  if (!response.ok) {
    const message = await response.text();
    throw new Error(message || 'Failed to delete manual test.');
  }
}

async function cancelOptimizationRequest(runId = '') {
  const normalizedRunId = String(runId || '').trim();
  const endpoint = normalizedRunId
    ? ('/api/optimization/cancel?run_id=' + encodeURIComponent(normalizedRunId))
    : '/api/optimization/cancel';
  const response = await fetch(endpoint, { method: 'POST' });
  if (!response.ok) {
    throw new Error(`Cancel request failed: ${response.status}`);
  }
  return response.json();
}

async function fetchPresetsList() {
  const response = await fetch('/api/presets');
  if (!response.ok) {
    const message = await response.text();
    throw new Error(message || 'Failed to fetch presets.');
  }
  return response.json();
}

async function loadPresetRequest(name) {
  const response = await fetch(`/api/presets/${encodeURIComponent(name)}`);
  if (!response.ok) {
    const message = await response.text();
    throw new Error(message || 'Failed to load preset.');
  }
  return response.json();
}

async function savePresetRequest(name, values) {
  const response = await fetch('/api/presets', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ name, values })
  });
  if (!response.ok) {
    const message = await response.text();
    throw new Error(message || 'Failed to save preset.');
  }
  return response.json();
}

async function overwritePresetRequest(name, values) {
  const response = await fetch(`/api/presets/${encodeURIComponent(name)}`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ values })
  });
  if (!response.ok) {
    const message = await response.text();
    throw new Error(message || 'Failed to overwrite preset.');
  }
  return response.json();
}

async function saveDefaultsRequest(values) {
  const response = await fetch('/api/presets/defaults', {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ values })
  });
  if (!response.ok) {
    const message = await response.text();
    throw new Error(message || 'Failed to save defaults.');
  }
  return response.json();
}

async function deletePresetRequest(name) {
  const response = await fetch(`/api/presets/${encodeURIComponent(name)}`, {
    method: 'DELETE'
  });
  if (!response.ok) {
    const message = await response.text();
    throw new Error(message || 'Failed to delete preset.');
  }
}

async function importPresetFromCsvRequest(file) {
  const formData = new FormData();
  formData.append('file', file, file.name);

  const response = await fetch('/api/presets/import-csv', {
    method: 'POST',
    body: formData
  });

  if (!response.ok) {
    const message = await response.text();
    throw new Error(message || 'Failed to import settings from CSV.');
  }

  return response.json();
}

async function fetchDatabasesList() {
  const response = await fetch('/api/databases');
  if (!response.ok) {
    const message = await response.text();
    throw new Error(message || 'Failed to fetch databases.');
  }
  return response.json();
}

async function switchDatabaseRequest(filename) {
  const response = await fetch('/api/databases/active', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ filename })
  });
  if (!response.ok) {
    const payload = await response.json().catch(() => ({}));
    throw new Error(payload.error || 'Failed to switch database.');
  }
  return response.json();
}

async function createDatabaseRequest(label) {
  const response = await fetch('/api/databases', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ label })
  });
  if (!response.ok) {
    const payload = await response.json().catch(() => ({}));
    throw new Error(payload.error || 'Failed to create database.');
  }
  return response.json();
}

async function fetchAnalyticsSetsRequest() {
  const response = await fetch('/api/analytics/sets');
  if (!response.ok) {
    const payload = await response.json().catch(() => ({}));
    throw new Error(payload.error || 'Failed to load study sets.');
  }
  return response.json();
}

async function fetchAnalyticsAllStudiesEquityRequest(signal = null) {
  const response = await fetch('/api/analytics/all-studies/equity', {
    signal: signal || undefined
  });
  if (!response.ok) {
    const payload = await response.json().catch(() => ({}));
    throw new Error(payload.error || 'Failed to load all-studies analytics equity.');
  }
  return response.json();
}

async function fetchAnalyticsSetEquityRequest(setId, signal = null) {
  const response = await fetch(`/api/analytics/sets/${encodeURIComponent(setId)}/equity`, {
    signal: signal || undefined
  });
  if (!response.ok) {
    const payload = await response.json().catch(() => ({}));
    throw new Error(payload.error || 'Failed to load analytics set equity.');
  }
  return response.json();
}

async function fetchAnalyticsStudyEquityRequest(studyId, signal = null) {
  const response = await fetch(`/api/analytics/studies/${encodeURIComponent(studyId)}/equity`, {
    signal: signal || undefined
  });
  if (!response.ok) {
    const payload = await response.json().catch(() => ({}));
    throw new Error(payload.error || 'Failed to load analytics study equity.');
  }
  return response.json();
}

async function fetchAnalyticsEquityRequest(studyIds, signal = null) {
  const response = await fetch('/api/analytics/equity', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ study_ids: Array.isArray(studyIds) ? studyIds : [] }),
    signal: signal || undefined
  });

  if (!response.ok) {
    const payload = await response.json().catch(() => ({}));
    throw new Error(payload.error || 'Failed to aggregate analytics equity.');
  }
  return response.json();
}

async function fetchAnalyticsEquityBatchRequest(groups, signal = null) {
  const response = await fetch('/api/analytics/equity/batch', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ groups: Array.isArray(groups) ? groups : [] }),
    signal: signal || undefined
  });

  if (!response.ok) {
    const payload = await response.json().catch(() => ({}));
    throw new Error(payload.error || 'Failed to aggregate analytics equity batch.');
  }
  return response.json();
}

async function fetchAnalyticsStudyWindowBoundariesRequest(studyId, signal = null) {
  const response = await fetch(`/api/analytics/studies/${encodeURIComponent(studyId)}/window-boundaries`, {
    signal: signal || undefined
  });
  if (!response.ok) {
    const payload = await response.json().catch(() => ({}));
    throw new Error(payload.error || 'Failed to load analytics window boundaries.');
  }
  return response.json();
}

async function createAnalyticsSetRequest(name, studyIds, options = {}) {
  const payload = {
    name,
    study_ids: Array.isArray(studyIds) ? studyIds : []
  };
  if (Object.prototype.hasOwnProperty.call(options || {}, 'colorToken')) {
    payload.color_token = options.colorToken ?? null;
  }
  const response = await fetch('/api/analytics/sets', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload)
  });
  if (!response.ok) {
    const payload = await response.json().catch(() => ({}));
    throw new Error(payload.error || 'Failed to create study set.');
  }
  return response.json();
}

async function updateAnalyticsSetRequest(setId, payload) {
  const response = await fetch(`/api/analytics/sets/${encodeURIComponent(setId)}`, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload || {})
  });
  if (!response.ok) {
    const body = await response.json().catch(() => ({}));
    throw new Error(body.error || 'Failed to update study set.');
  }
  return response.json();
}

async function deleteAnalyticsSetRequest(setId) {
  const response = await fetch(`/api/analytics/sets/${encodeURIComponent(setId)}`, {
    method: 'DELETE'
  });
  if (!response.ok) {
    const body = await response.json().catch(() => ({}));
    throw new Error(body.error || 'Failed to delete study set.');
  }
  return response.json();
}

async function bulkUpdateAnalyticsSetColorRequest(setIds, colorToken) {
  const response = await fetch('/api/analytics/sets/bulk-color', {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      set_ids: Array.isArray(setIds) ? setIds : [],
      color_token: colorToken ?? null,
    })
  });
  if (!response.ok) {
    const body = await response.json().catch(() => ({}));
    throw new Error(body.error || 'Failed to update study set colors.');
  }
  return response.json();
}

async function bulkDeleteAnalyticsSetsRequest(setIds) {
  const response = await fetch('/api/analytics/sets/bulk-delete', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ set_ids: Array.isArray(setIds) ? setIds : [] })
  });
  if (!response.ok) {
    const body = await response.json().catch(() => ({}));
    throw new Error(body.error || 'Failed to delete study sets.');
  }
  return response.json();
}

async function reorderAnalyticsSetsRequest(order) {
  const response = await fetch('/api/analytics/sets/reorder', {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ order: Array.isArray(order) ? order : [] })
  });
  if (!response.ok) {
    const body = await response.json().catch(() => ({}));
    throw new Error(body.error || 'Failed to reorder study sets.');
  }
  return response.json();
}
