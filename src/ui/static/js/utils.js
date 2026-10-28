/**
 * Utility functions for the Strategy Backtester & Optimizer UI.
 * No dependencies on other modules.
 */

function clonePreset(data) {
  try {
    return JSON.parse(JSON.stringify(data || {}));
  } catch (error) {
    return {};
  }
}

function parseNumber(value, fallback = 0) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function formatMetric(value, digits = 2) {
  if (typeof value === 'number' && Number.isFinite(value)) {
    return value.toFixed(digits);
  }
  if (value === undefined || value === null) {
    return 'N/A';
  }
  return value;
}

function formatResultBlock(index, total, payload, data) {
  const metrics = data.metrics || {};
  return [
    `#${index}/${total}`,
    `Net Profit %: ${formatMetric(metrics.net_profit_pct)}`,
    `Max Drawdown %: ${formatMetric(metrics.max_drawdown_pct)}`,
    `Total Trades: ${metrics.total_trades ?? 'N/A'}`
  ].join('\n');
}

function composeDateTime(datePart, timePart) {
  const date = (datePart || '').trim();
  const time = (timePart || '').trim();
  if (!date) {
    return '';
  }
  const normalizedTime = time || '00:00';
  return `${date}T${normalizedTime}`;
}

function composeISOTimestamp(dateInput, timeInput) {
  const date = (dateInput || '').trim();
  const time = (timeInput || '').trim();
  if (!date) {
    return '';
  }
  const normalizedTime = time || '00:00';
  return `${date}T${normalizedTime}`;
}

function parseISOTimestamp(isoString) {
  const raw = typeof isoString === 'string' ? isoString.trim() : '';
  if (!raw) {
    return { date: '', time: '' };
  }
  const [datePart, timePart] = raw.split('T');
  const date = datePart || '';
  const time = timePart ? timePart.slice(0, 5) : '00:00';
  return { date, time };
}

function setCheckboxValue(id, checked) {
  const element = document.getElementById(id);
  if (!element) {
    return;
  }
  element.checked = Boolean(checked);
}

function setInputValue(id, value) {
  const element = document.getElementById(id);
  if (!element) {
    return;
  }
  if (value === undefined || value === null) {
    element.value = '';
  } else {
    element.value = value;
  }
}

function showErrorMessage(message) {
  const errorEl = document.getElementById('error');
  if (!errorEl) {
    return;
  }
  errorEl.textContent = message;
  errorEl.style.display = message ? 'block' : 'none';
}

function clearErrorMessage() {
  showErrorMessage('');
}

function showResultsMessage(message) {
  const resultsEl = document.getElementById('results');
  if (!resultsEl) {
    return;
  }
  resultsEl.textContent = message;
  resultsEl.classList.remove('loading');
  if (message) {
    resultsEl.classList.add('ready');
  } else {
    resultsEl.classList.remove('ready');
  }
}

function splitCsvPathSegments(path) {
  return String(path || '')
    .split(/[\\/]+/)
    .filter(Boolean);
}

function buildCsvPathDisplayEntries(paths) {
  const items = (Array.isArray(paths) ? paths : [])
    .map((rawPath) => {
      const fullPath = String(rawPath || '').trim();
      if (!fullPath) return null;
      const segments = splitCsvPathSegments(fullPath);
      const fileName = segments.length ? segments[segments.length - 1] : fullPath;
      const parents = segments.slice(0, -1);
      return { fullPath, fileName, parents };
    })
    .filter(Boolean);

  if (!items.length) {
    return [];
  }

  const groups = new Map();
  items.forEach((item, idx) => {
    const key = String(item.fileName || '').toLowerCase();
    if (!groups.has(key)) {
      groups.set(key, []);
    }
    groups.get(key).push(idx);
  });

  const labels = new Array(items.length).fill('');
  groups.forEach((indexes) => {
    if (indexes.length === 1) {
      const index = indexes[0];
      labels[index] = items[index].fileName || items[index].fullPath;
      return;
    }

    const maxDepth = indexes.reduce((maxValue, index) => {
      return Math.max(maxValue, items[index].parents.length);
    }, 0);

    let depth = 1;
    while (depth <= maxDepth) {
      const seen = new Set();
      let unique = true;
      indexes.forEach((index) => {
        const tail = items[index].parents.slice(-depth).join('\\') || '.';
        const key = tail.toLowerCase();
        if (seen.has(key)) {
          unique = false;
          return;
        }
        seen.add(key);
      });
      if (unique) {
        break;
      }
      depth += 1;
    }

    const collisionCounter = new Map();
    indexes.forEach((index) => {
      const tail = items[index].parents.slice(-depth).join('\\') || '.';
      const candidate = `${items[index].fileName} [${tail}]`;
      const candidateKey = candidate.toLowerCase();
      const counter = (collisionCounter.get(candidateKey) || 0) + 1;
      collisionCounter.set(candidateKey, counter);
      labels[index] = counter === 1 ? candidate : `${candidate} #${counter}`;
    });
  });

  return items.map((item, idx) => {
    return {
      label: labels[idx] || item.fileName || item.fullPath,
      title: item.fullPath
    };
  });
}

function renderSelectedFiles(files) {
  if (window.ParquetInput?.state.mode) return window.ParquetInput.renderSelected();
  const selectedFilesWrapper = document.getElementById('selectedFilesWrapper');
  const selectedFilesList = document.getElementById('selectedFilesList');
  const clearSelectedCsvBtn = document.getElementById('clearSelectedCsvBtn');
  const selectedCsvCount = document.getElementById('selectedCsvCount');

  if (!selectedFilesWrapper || !selectedFilesList) {
    return;
  }

  const entries = [];
  if (files && files.length) {
    files.forEach((file) => {
      entries.push({
        label: file.name,
        title: file.name
      });
    });
  } else if (Array.isArray(window.selectedCsvPaths) && window.selectedCsvPaths.length) {
    entries.push(...buildCsvPathDisplayEntries(window.selectedCsvPaths));
  } else if (window.selectedCsvPath) {
    entries.push(...buildCsvPathDisplayEntries([window.selectedCsvPath]));
  }

  selectedFilesList.innerHTML = '';
  if (selectedCsvCount) {
    selectedCsvCount.textContent = `· ${entries.length} selected`;
  }
  if (clearSelectedCsvBtn) {
    clearSelectedCsvBtn.disabled = entries.length === 0;
  }
  if (!entries.length) {
    selectedFilesWrapper.style.display = 'none';
    return;
  }

  const fragment = document.createDocumentFragment();
  entries.forEach((entry) => {
    const item = document.createElement('li');
    item.textContent = entry.label;
    if (entry.title) {
      item.title = entry.title;
    }
    fragment.appendChild(item);
  });

  selectedFilesList.appendChild(fragment);
  selectedFilesWrapper.style.display = 'block';
}
