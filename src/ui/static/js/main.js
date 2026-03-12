/**
 * Main entry: binds DOM events and initializes UI state.
 */

async function loadDatabasesList({ preserveSelection = false } = {}) {
  const select = document.getElementById('dbTarget');
  if (!select || typeof fetchDatabasesList !== 'function') return;

  const refreshBtn = document.getElementById('dbTargetRefreshBtn');
  const previousValue = preserveSelection ? select.value : '';

  try {
    if (refreshBtn) {
      refreshBtn.disabled = true;
    }

    const data = await fetchDatabasesList();
    const databases = Array.isArray(data.databases) ? data.databases : [];
    select.querySelectorAll('option:not([value="new"])').forEach((opt) => opt.remove());

    databases.forEach((db) => {
      const option = document.createElement('option');
      option.value = db.name;
      option.textContent = db.name;
      select.appendChild(option);
    });

    let nextValue = 'new';
    const hasPrevious = preserveSelection
      && previousValue
      && databases.some((db) => db.name === previousValue);
    if (hasPrevious) {
      nextValue = previousValue;
    } else if (databases.length) {
      const activeDb = databases.find((db) => db.active);
      nextValue = activeDb ? activeDb.name : databases[0].name;
    }
    select.value = nextValue;
  } catch (error) {
    console.warn('Failed to load database list', error);
  } finally {
    if (refreshBtn) {
      refreshBtn.disabled = false;
    }
  }

  toggleDbLabelVisibility();
}

function toggleDbLabelVisibility() {
  const select = document.getElementById('dbTarget');
  const labelGroup = document.getElementById('dbLabelGroup');
  if (!select || !labelGroup) return;
  labelGroup.style.display = select.value === 'new' ? 'flex' : 'none';
}

async function createAndSelectDatabase() {
  const select = document.getElementById('dbTarget');
  const labelInput = document.getElementById('dbLabel');
  const createBtn = document.getElementById('dbCreateBtn');
  const refreshBtn = document.getElementById('dbTargetRefreshBtn');
  if (!select || !labelInput || !createBtn || typeof createDatabaseRequest !== 'function') return;

  select.value = 'new';
  toggleDbLabelVisibility();
  const label = labelInput.value.trim();

  try {
    createBtn.disabled = true;
    if (refreshBtn) refreshBtn.disabled = true;

    const payload = await createDatabaseRequest(label);
    await loadDatabasesList({ preserveSelection: false });

    const createdName = payload && payload.filename ? String(payload.filename) : '';
    if (createdName) {
      select.value = createdName;
      toggleDbLabelVisibility();
      if (typeof showResultsMessage === 'function') {
        showResultsMessage(`Database selected: ${createdName}`);
      }
    }
  } catch (error) {
    if (typeof showErrorMessage === 'function') {
      showErrorMessage(error?.message || 'Failed to create database.');
    } else {
      alert(error?.message || 'Failed to create database.');
    }
  } finally {
    createBtn.disabled = false;
    if (refreshBtn) refreshBtn.disabled = false;
  }
}

document.addEventListener('DOMContentLoaded', async () => {
  await loadStrategiesList();
  await loadDatabasesList();

  const resultsNav = document.querySelector('.nav-tab[data-nav="results"]');
  if (resultsNav) {
    resultsNav.addEventListener('click', (event) => {
      const raw = sessionStorage.getItem('merlinOptimizationState')
        || localStorage.getItem('merlinOptimizationState');
      if (!raw) return;
      try {
        const state = JSON.parse(raw);
        if (state && state.status === 'running') {
          event.preventDefault();
          window.open('/results', '_blank', 'noopener');
        }
      } catch (error) {
        return;
      }
    });
  }

  document.querySelectorAll('.collapsible').forEach((collapsible) => {
    const header = collapsible.querySelector('.collapsible-header');
    if (!header) return;
    header.addEventListener('click', () => {
      collapsible.classList.toggle('open');
    });
  });

  const linkToScoreConfig = document.getElementById('linkToScoreConfig');
  if (linkToScoreConfig) {
    linkToScoreConfig.addEventListener('click', (event) => {
      event.preventDefault();
      const scoreConfigCollapsible = document.getElementById('scoreConfigCollapsible');
      if (!scoreConfigCollapsible) return;
      if (!scoreConfigCollapsible.classList.contains('open')) {
        scoreConfigCollapsible.classList.add('open');
      }
      scoreConfigCollapsible.scrollIntoView({ behavior: 'smooth', block: 'center' });
      scoreConfigCollapsible.style.outline = '3px solid #90caf9';
      window.setTimeout(() => {
        scoreConfigCollapsible.style.outline = '';
      }, 2000);
    });
  }

  const budgetModeRadios = document.querySelectorAll('input[name="budgetMode"]');
  budgetModeRadios.forEach((radio) => {
    radio.addEventListener('change', syncBudgetInputs);
  });
  syncBudgetInputs();
  toggleWFSettings();
  if (typeof toggleAdaptiveWFSettings === 'function') {
    toggleAdaptiveWFSettings();
  }

  const dbTargetSelect = document.getElementById('dbTarget');
  if (dbTargetSelect) {
    dbTargetSelect.addEventListener('change', toggleDbLabelVisibility);
  }

  const dbTargetRefreshBtn = document.getElementById('dbTargetRefreshBtn');
  if (dbTargetRefreshBtn) {
    dbTargetRefreshBtn.addEventListener('click', async () => {
      await loadDatabasesList({ preserveSelection: true });
    });
  }

  const dbCreateBtn = document.getElementById('dbCreateBtn');
  if (dbCreateBtn) {
    dbCreateBtn.addEventListener('click', createAndSelectDatabase);
  }

  const dbLabelInput = document.getElementById('dbLabel');
  if (dbLabelInput) {
    dbLabelInput.addEventListener('keydown', async (event) => {
      if (event.key !== 'Enter') return;
      event.preventDefault();
      await createAndSelectDatabase();
    });
  }

  if (typeof initQueue === 'function') {
    try {
      await initQueue();
    } catch (error) {
      if (typeof showQueueError === 'function') {
        showQueueError(error?.message || 'Failed to initialize queue.');
      }
    }
  }

  const loadQueueBtn = document.getElementById('loadQueueBtn');
  if (loadQueueBtn) {
    loadQueueBtn.addEventListener('click', async () => {
      try {
        if (typeof loadQueueUi === 'function') {
          await loadQueueUi();
        } else if (typeof attachQueueUiIfNeeded === 'function') {
          await attachQueueUiIfNeeded();
        }
      } catch (error) {
        if (typeof showQueueError === 'function') {
          showQueueError(error?.message || 'Failed to load queue.');
        }
      }
    });
  }

  const addToQueueBtn = document.getElementById('addToQueueBtn');
  if (addToQueueBtn && typeof collectQueueItem === 'function' && typeof addToQueue === 'function') {
    addToQueueBtn.addEventListener('click', async () => {
      try {
        if (typeof loadQueueUi === 'function') {
          await loadQueueUi();
        } else if (typeof attachQueueUiIfNeeded === 'function') {
          await attachQueueUiIfNeeded();
        }
        const item = collectQueueItem();
        if (item) {
          await addToQueue(item);
        }
      } catch (error) {
        if (typeof showQueueError === 'function') {
          showQueueError(error?.message || 'Failed to add item to queue.');
        }
      }
    });
  }

  const clearQueueBtn = document.getElementById('clearQueueBtn');
  if (clearQueueBtn) {
    clearQueueBtn.addEventListener('click', async () => {
      try {
        if (typeof handleQueueClearAction === 'function') {
          await handleQueueClearAction();
        } else if (typeof clearQueue === 'function' && window.confirm('Clear all items from the queue?')) {
          await clearQueue();
        }
      } catch (error) {
        if (typeof showQueueError === 'function') {
          showQueueError(error?.message || 'Failed to clear queue.');
        }
      }
    });
  }

  if (typeof bindCsvBrowserControls === 'function') {
    bindCsvBrowserControls();
  }

  const chooseCsvBtnEl = document.getElementById('chooseCsvBtn');
  if (chooseCsvBtnEl && typeof openCsvBrowserModal === 'function') {
    chooseCsvBtnEl.addEventListener('click', () => {
      openCsvBrowserModal();
    });
  }

  const csvDirectoryEl = document.getElementById('csvDirectory');
  if (csvDirectoryEl && typeof openCsvBrowserModal === 'function') {
    csvDirectoryEl.addEventListener('keydown', (event) => {
      if (event.key === 'Enter') {
        event.preventDefault();
        openCsvBrowserModal();
      }
    });
  }

  const clearSelectedCsvBtnEl = document.getElementById('clearSelectedCsvBtn');
  if (clearSelectedCsvBtnEl && typeof setSelectedCsvPaths === 'function') {
    clearSelectedCsvBtnEl.addEventListener('click', () => {
      setSelectedCsvPaths([]);
    });
  }

  const presetToggleEl = document.getElementById('presetToggle');
  const presetMenuEl = document.getElementById('presetMenu');
  const presetDropdownEl = document.getElementById('presetDropdown');
  const presetImportInput = document.getElementById('presetImportInput');

  if (presetToggleEl) {
    presetToggleEl.addEventListener('click', (event) => {
      event.stopPropagation();
      togglePresetMenu();
    });
  }

  if (presetMenuEl) {
    presetMenuEl.addEventListener('click', (event) => {
      event.stopPropagation();
      const actionButton = event.target.closest('.preset-action');
      if (!actionButton) return;

      const action = actionButton.dataset.action;
      if (action === 'apply-defaults') {
        handleApplyDefaults();
      } else if (action === 'save-as') {
        handleSaveAsPreset();
      } else if (action === 'import') {
        if (presetImportInput) {
          presetImportInput.value = '';
          presetImportInput.click();
        }
      }
    });
  }

  if (presetImportInput) {
    presetImportInput.addEventListener('change', async (event) => {
      const file = event.target.files && event.target.files[0];
      if (!file) {
        presetImportInput.value = '';
        closePresetMenu();
        return;
      }
      try {
        const data = await importPresetFromCsvRequest(file);
        applyPresetValues(data?.values || {}, { clearResults: false });
        const appliedKeys = Array.from(new Set(data?.applied || []));
        const appliedLabels = appliedKeys.map((key) => formatPresetLabel(key));
        const message = appliedLabels.length
          ? `Imported parameters: ${appliedLabels.join(', ')}.`
          : 'CSV import did not change any settings.';
        showResultsMessage(message);
        clearErrorMessage();
      } catch (error) {
        showErrorMessage(error.message || 'Failed to import settings from CSV');
      } finally {
        presetImportInput.value = '';
        closePresetMenu();
      }
    });
  }

  document.addEventListener('click', (event) => {
    if (
      presetDropdownEl &&
      presetDropdownEl.classList.contains('open') &&
      !presetDropdownEl.contains(event.target)
    ) {
      closePresetMenu();
    }
  });

  document.addEventListener('keydown', (event) => {
    if (event.key === 'Escape') {
      closePresetMenu();
    }
  });

  const tradesBtn = document.getElementById('tradesBtn');
  if (tradesBtn) {
    tradesBtn.addEventListener('click', runBacktestAndDownloadTrades);
  }

  const backtestForm = document.getElementById('backtestForm');
  if (backtestForm) {
    backtestForm.addEventListener('submit', runBacktest);
  }

  const optimizerForm = document.getElementById('optimizerForm');
  if (optimizerForm) {
    optimizerForm.addEventListener('submit', submitOptimization);
  }

  bindOptimizerInputs();
  bindMinProfitFilterControl();
  bindScoreControls();
  bindOptunaUiControls();
  bindMASelectors();
  if (window.PostProcessUI && typeof window.PostProcessUI.bind === 'function') {
    window.PostProcessUI.bind();
  }
  if (window.OosTestUI && typeof window.OosTestUI.bind === 'function') {
    window.OosTestUI.bind();
  }

  // Dataset Timeline Preview - bind listeners and initial render.
  if (typeof window.updateDatasetPreview === 'function') {
    const triggerDatasetPreviewUpdate = () => {
      if (typeof window.requestAnimationFrame === 'function') {
        window.requestAnimationFrame(() => window.updateDatasetPreview());
      } else {
        window.setTimeout(() => window.updateDatasetPreview(), 0);
      }
    };

    const previewTriggerIds = [
      'dateFilter', 'startDate', 'endDate',
      'enableWF', 'enableAdaptiveWF', 'wfIsPeriodDays', 'wfOosPeriodDays',
      'enablePostProcess', 'ftPeriodDays',
      'enableOosTest', 'oosPeriodDays'
    ];

    previewTriggerIds.forEach((id) => {
      const element = document.getElementById(id);
      if (!element) return;
      const eventType = element.type === 'checkbox'
        ? 'change'
        : (element.type === 'number' ? 'input' : 'change');
      element.addEventListener(eventType, triggerDatasetPreviewUpdate);
    });

    triggerDatasetPreviewUpdate();
  }

  await initializePresets();
});

window.addEventListener('storage', (event) => {
  if (event.key !== 'merlinOptimizationControl') return;
  if (!event.newValue) return;
  try {
    const payload = JSON.parse(event.newValue);
    if (payload && payload.action === 'cancel') {
      if (window.optimizationAbortController) {
        window.optimizationAbortController.abort();
      }
    }
  } catch (error) {
    return;
  }
});
