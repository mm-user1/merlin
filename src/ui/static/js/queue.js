/**
 * Run queue management for optimization / walk-forward execution.
 * Dependencies: utils.js, api.js, ui-handlers.js, strategy-config.js, presets.js
 */

const CONSTRAINT_LE_METRICS = ['max_drawdown_pct', 'max_consecutive_losses', 'ulcer_index'];
const LEGACY_QUEUE_STORAGE_KEY = 'merlinRunQueue';
const LEGACY_QUEUE_RUNTIME_STORAGE_KEY = 'merlinQueueRuntime';

let queueRunning = false;
let queueStopRequested = false;
let queueUiLoaded = false;
let queueStateLoaded = false;
let queueStateLoadPromise = null;
let queueItemLoadRequestId = 0;
let queueUiEventsBound = false;
let queueFocusedItemId = '';
let queueBatchMode = false;
let queueBatchAnchorItemId = '';
let queueBatchSelectedItemIds = new Set();
let queueMoveMode = false;
let queueMoveOriginalPendingOrder = [];
let queueMoveSelectionIds = [];
let queueMoveInsertionIndex = 0;
let queueState = {
  items: [],
  nextIndex: 1,
  runtime: { active: false, updatedAt: 0 }
};

function isAbsoluteFilesystemPath(path) {
  const value = String(path || '').trim();
  if (!value) return false;
  if (/^[A-Za-z]:[\\/]/.test(value)) return true; // Windows drive path
  if (/^\\\\[^\\]/.test(value)) return true; // UNC path
  if (value.startsWith('/')) return true; // POSIX path
  return false;
}

function getPathFileName(path) {
  return String(path || '').split(/[/\\]/).pop() || '';
}

function isTypingElement(element) {
  if (!element) return false;
  if (element.isContentEditable) return true;
  const tagName = element.tagName ? element.tagName.toLowerCase() : '';
  return tagName === 'input' || tagName === 'textarea' || tagName === 'select';
}

function uniqueStringValues(values) {
  const items = Array.isArray(values) ? values : [];
  const unique = [];
  const seen = new Set();
  items.forEach((value) => {
    const normalized = String(value || '').trim();
    if (!normalized || seen.has(normalized)) return;
    seen.add(normalized);
    unique.push(normalized);
  });
  return unique;
}

function normalizeQueueStudySetStatus(rawStatus) {
  const normalized = String(rawStatus || '').trim().toLowerCase();
  if (normalized === 'created' || normalized === 'skipped' || normalized === 'error') {
    return normalized;
  }
  return '';
}

function generateOptimizationRunId(prefix = 'run') {
  const normalizedPrefix = String(prefix || 'run').replace(/[^A-Za-z0-9_-]+/g, '') || 'run';
  return normalizedPrefix + '_' + Date.now() + '_' + Math.random().toString(36).slice(2, 10);
}

function setActiveOptimizationRunId(runId) {
  const normalizedRunId = String(runId || '').trim();
  window.activeOptimizationRunId = normalizedRunId;
  if (typeof updateOptimizationState === 'function') {
    updateOptimizationState({ run_id: normalizedRunId });
  }
}

function buildSourceDisplayLabel(source, fallbackIndex = 0) {
  if (!source || typeof source !== 'object') return 'source_' + (fallbackIndex + 1);
  return getPathFileName(source.path) || String(source.path || '').trim() || ('source_' + (fallbackIndex + 1));
}

function buildSourceModeLabel(source) {
  void source;
  return 'PATH';
}

function normalizeQueueSource(rawSource, fallbackIndex) {
  let path = '';
  if (typeof rawSource === 'string') {
    path = rawSource;
  } else if (rawSource && typeof rawSource === 'object') {
    path = rawSource.path || rawSource.csvPath || '';
  } else {
    return null;
  }

  const normalizedPath = String(path || '').trim();
  if (!normalizedPath) return null;
  if (!isAbsoluteFilesystemPath(normalizedPath)) return null;
  return { type: 'path', path: normalizedPath };
}

function getQueueSources(item) {
  if (!item || typeof item !== 'object') return [];

  const rawSources = Array.isArray(item.sources) ? item.sources : [];

  const sources = [];
  rawSources.forEach((source, index) => {
    const normalized = normalizeQueueSource(source, index);
    if (normalized) {
      sources.push(normalized);
    }
  });
  return sources;
}

function cloneSourcesForStorage(sources) {
  return (Array.isArray(sources) ? sources : [])
    .map((source, index) => normalizeQueueSource(source, index))
    .filter(Boolean)
    .map((source) => {
      return {
        type: 'path',
        path: source.path
      };
    });
}

function normalizeQueueFinalState(rawFinalState) {
  const normalized = String(rawFinalState || '').trim().toLowerCase();
  if (normalized === 'completed' || normalized === 'failed') {
    return normalized;
  }
  return '';
}

function normalizeQueueStudySet(rawStudySet, item) {
  const source = rawStudySet && typeof rawStudySet === 'object' ? rawStudySet : {};
  const createdSetIdRaw = Number(source.createdSetId);
  const normalizedStatus = normalizeQueueStudySetStatus(source.status);
  const configured = Boolean(
    source.configured
    || source.autoCreate
    || uniqueStringValues(source.completedStudyIds).length
    || (Number.isInteger(createdSetIdRaw) && createdSetIdRaw > 0)
    || String(source.createdSetName || '').trim()
    || normalizedStatus
    || String(source.error || '').trim()
    || String(source.lastUpdatedAt || '').trim()
  );
  return {
    configured,
    autoCreate: Boolean(source.autoCreate),
    completedStudyIds: uniqueStringValues(source.completedStudyIds),
    createdSetId: Number.isInteger(createdSetIdRaw) && createdSetIdRaw > 0 ? createdSetIdRaw : null,
    createdSetName: String(source.createdSetName || '').trim(),
    status: normalizedStatus,
    error: String(source.error || '').trim(),
    lastUpdatedAt: String(source.lastUpdatedAt || '').trim(),
  };
}

function isQueueItemFinalized(item) {
  return normalizeQueueFinalState(item?.finalState) !== '';
}

function countQueuePendingItems(queueLike) {
  const queue = queueLike && typeof queueLike === 'object' ? queueLike : { items: [] };
  const items = Array.isArray(queue.items) ? queue.items : [];
  return items.filter((item) => !isQueueItemFinalized(item)).length;
}

function findNextPendingQueueItem(queueLike) {
  const queue = queueLike && typeof queueLike === 'object' ? queueLike : { items: [] };
  const items = Array.isArray(queue.items) ? queue.items : [];
  return items.find((item) => !isQueueItemFinalized(item)) || null;
}

function getQueuePendingCount() {
  return countQueuePendingItems(loadQueue());
}

function normalizeQueueItem(raw, fallbackIndex) {
  if (!raw || typeof raw !== 'object') return null;

  const sources = getQueueSources(raw);
  if (!sources.length) return null;

  const indexRaw = Number(raw.index);
  const index = Number.isFinite(indexRaw) && indexRaw > 0
    ? Math.round(indexRaw)
    : Math.max(1, fallbackIndex);

  const item = {
    ...raw,
    index,
    sources: cloneSourcesForStorage(sources)
  };

  const cursorRaw = Number(raw.sourceCursor);
  item.sourceCursor = Number.isFinite(cursorRaw)
    ? Math.max(0, Math.min(item.sources.length, Math.floor(cursorRaw)))
    : 0;

  const successRaw = Number(raw.successCount);
  item.successCount = Number.isFinite(successRaw) ? Math.max(0, Math.floor(successRaw)) : 0;

  const failureRaw = Number(raw.failureCount);
  item.failureCount = Number.isFinite(failureRaw) ? Math.max(0, Math.floor(failureRaw)) : 0;

  item.finalState = normalizeQueueFinalState(raw.finalState);
  if (!item.finalState && item.sourceCursor >= item.sources.length) {
    if (item.successCount > 0) {
      item.finalState = 'completed';
    } else if (item.failureCount > 0) {
      item.finalState = 'failed';
    }
  }

  if (typeof item.label !== 'string' || !item.label.trim()) {
    item.label = generateQueueLabel(item);
  }

  item.studySet = normalizeQueueStudySet(raw.studySet, item);

  return item;
}

function computeQueueNextIndex(items, candidateNextIndex) {
  const normalizedItems = Array.isArray(items) ? items : [];
  if (normalizedItems.length === 0) {
    return 1;
  }

  const maxIndex = normalizedItems.reduce((acc, item) => Math.max(acc, Number(item.index) || 0), 0);
  const candidate = Number(candidateNextIndex);
  if (Number.isFinite(candidate) && candidate > maxIndex) {
    return Math.floor(candidate);
  }
  return Math.max(1, maxIndex + 1);
}

function cloneQueueState(rawState) {
  return JSON.parse(JSON.stringify(rawState || {
    items: [],
    nextIndex: 1,
    runtime: { active: false, updatedAt: 0 }
  }));
}

function normalizeQueueRuntimeState(rawRuntime) {
  if (!rawRuntime || typeof rawRuntime !== 'object') {
    return { active: false, updatedAt: 0 };
  }
  const updatedAtRaw = Number(rawRuntime.updatedAt);
  return {
    active: Boolean(rawRuntime.active),
    updatedAt: Number.isFinite(updatedAtRaw) ? Math.max(0, Math.floor(updatedAtRaw)) : 0
  };
}

function normalizeQueueState(rawQueue) {
  const parsed = rawQueue && typeof rawQueue === 'object' ? rawQueue : {};
  const rawItems = Array.isArray(parsed.items) ? parsed.items : [];
  const items = [];
  rawItems.forEach((item, idx) => {
    const normalized = normalizeQueueItem(item, idx + 1);
    if (normalized) items.push(normalized);
  });

  const nextIndex = computeQueueNextIndex(items, parsed.nextIndex);
  const runtime = normalizeQueueRuntimeState(parsed.runtime);
  if (!items.length || countQueuePendingItems({ items }) === 0) {
    runtime.active = false;
    runtime.updatedAt = 0;
  }

  return {
    items,
    nextIndex,
    runtime
  };
}

function applyQueueState(rawQueue) {
  queueState = normalizeQueueState(rawQueue);
  queueStateLoaded = true;
  return loadQueue();
}

function loadQueue() {
  return cloneQueueState(queueState);
}

async function reloadQueueStateFromServer() {
  if (typeof fetchQueueStateRequest !== 'function') {
    return applyQueueState(null);
  }
  const payload = await fetchQueueStateRequest();
  return applyQueueState(payload);
}

async function persistQueueState() {
  if (typeof saveQueueStateRequest !== 'function') {
    queueStateLoaded = true;
    return loadQueue();
  }
  const payload = await saveQueueStateRequest(queueState);
  return applyQueueState(payload);
}

async function clearQueueStateFromServer() {
  if (typeof clearQueueStateRequest !== 'function') {
    return applyQueueState(null);
  }
  const payload = await clearQueueStateRequest();
  return applyQueueState(payload);
}

function readLegacyQueueState() {
  try {
    const queueRaw = localStorage.getItem(LEGACY_QUEUE_STORAGE_KEY);
    const runtimeRaw = localStorage.getItem(LEGACY_QUEUE_RUNTIME_STORAGE_KEY);
    const parsedQueue = queueRaw ? JSON.parse(queueRaw) : {};
    const parsedRuntime = runtimeRaw ? JSON.parse(runtimeRaw) : {};
    return normalizeQueueState({
      ...parsedQueue,
      runtime: parsedRuntime
    });
  } catch (_error) {
    return normalizeQueueState(null);
  }
}

function clearLegacyQueueState() {
  try {
    localStorage.removeItem(LEGACY_QUEUE_STORAGE_KEY);
    localStorage.removeItem(LEGACY_QUEUE_RUNTIME_STORAGE_KEY);
  } catch (_error) {
    return;
  }
}

async function migrateLegacyQueueStateIfNeeded() {
  const current = loadQueue();
  if (current.items.length > 0) {
    clearLegacyQueueState();
    return;
  }

  const legacy = readLegacyQueueState();
  if (!legacy.items.length) {
    clearLegacyQueueState();
    return;
  }

  queueState = cloneQueueState(legacy);
  queueStateLoaded = true;
  try {
    await persistQueueState();
    clearLegacyQueueState();
  } catch (error) {
    console.warn('Failed to migrate legacy queue state to server storage', error);
  }
}

async function ensureQueueStateLoaded() {
  if (queueStateLoaded) {
    return loadQueue();
  }
  if (queueStateLoadPromise) {
    await queueStateLoadPromise;
    return loadQueue();
  }

  queueStateLoadPromise = (async () => {
    await reloadQueueStateFromServer();
    await migrateLegacyQueueStateIfNeeded();
  })()
    .catch((error) => {
      console.warn('Failed to load queue state from server', error);
      if (!queueStateLoaded) {
        applyQueueState(null);
      }
    })
    .finally(() => {
      queueStateLoadPromise = null;
    });

  await queueStateLoadPromise;
  return loadQueue();
}

function hasPersistedQueueItems() {
  return loadQueue().items.length > 0;
}

function isQueueLoaded() {
  return queueUiLoaded;
}

function getQueueForUi() {
  if (!queueUiLoaded && !queueRunning) {
    return { items: [], nextIndex: 1, runtime: { active: false, updatedAt: 0 } };
  }
  return loadQueue();
}

function loadQueueRuntimeState() {
  const runtime = loadQueue().runtime;
  return normalizeQueueRuntimeState(runtime);
}

function getQueueItemById(itemId, queueLike = null) {
  const queue = queueLike && typeof queueLike === 'object' ? queueLike : loadQueue();
  const normalizedId = String(itemId || '').trim();
  if (!normalizedId) return null;
  return (Array.isArray(queue.items) ? queue.items : [])
    .find((item) => String(item?.id || '').trim() === normalizedId) || null;
}

function isQueueItemPending(item) {
  return Boolean(item) && !isQueueItemFinalized(item);
}

function getPendingQueueItemIds(queueLike = null) {
  const queue = queueLike && typeof queueLike === 'object' ? queueLike : loadQueue();
  return (Array.isArray(queue.items) ? queue.items : [])
    .filter((item) => isQueueItemPending(item))
    .map((item) => String(item.id || '').trim())
    .filter(Boolean);
}

function getOrderedQueueBatchSelectedIds(queueLike = null) {
  const queue = queueLike && typeof queueLike === 'object' ? queueLike : loadQueue();
  const selected = queueBatchSelectedItemIds instanceof Set ? queueBatchSelectedItemIds : new Set();
  return (Array.isArray(queue.items) ? queue.items : [])
    .map((item) => String(item?.id || '').trim())
    .filter((itemId) => itemId && selected.has(itemId));
}

function clearQueueFocus() {
  queueFocusedItemId = '';
}

function setQueueFocusedItemId(itemId) {
  const normalizedId = String(itemId || '').trim();
  if (!normalizedId || queueBatchMode) {
    queueFocusedItemId = '';
    return;
  }
  const item = getQueueItemById(normalizedId);
  queueFocusedItemId = item ? normalizedId : '';
}

function clearQueueBatchSelection() {
  queueBatchSelectedItemIds = new Set();
  queueBatchAnchorItemId = '';
}

function clearQueueMoveState() {
  queueMoveMode = false;
  queueMoveOriginalPendingOrder = [];
  queueMoveSelectionIds = [];
  queueMoveInsertionIndex = 0;
}

function getQueueMoveSelectionSet() {
  return new Set(Array.isArray(queueMoveSelectionIds) ? queueMoveSelectionIds : []);
}

function syncQueueUiEphemeralState(queueLike = null) {
  const queue = queueLike && typeof queueLike === 'object' ? queueLike : loadQueue();
  const itemIds = new Set((Array.isArray(queue.items) ? queue.items : [])
    .map((item) => String(item?.id || '').trim())
    .filter(Boolean));

  if (!itemIds.has(queueFocusedItemId)) {
    queueFocusedItemId = '';
  }

  queueBatchSelectedItemIds = new Set(
    Array.from(queueBatchSelectedItemIds || []).filter((itemId) => itemIds.has(String(itemId || '').trim()))
  );
  if (!itemIds.has(queueBatchAnchorItemId)) {
    queueBatchAnchorItemId = '';
  }
  const pendingIds = getPendingQueueItemIds(queue);
  const pendingSet = new Set(pendingIds);
  if (queueMoveMode) {
    const selectionIds = (Array.isArray(queueMoveSelectionIds) ? queueMoveSelectionIds : [])
      .map((itemId) => String(itemId || '').trim())
      .filter((itemId) => pendingSet.has(itemId));
    if (!selectionIds.length) {
      clearQueueMoveState();
      return;
    }
    const originalPendingOrder = Array.isArray(queueMoveOriginalPendingOrder)
      ? queueMoveOriginalPendingOrder.map((itemId) => String(itemId || '').trim()).filter(Boolean)
      : [];
    if (
      originalPendingOrder.length !== pendingIds.length
      || originalPendingOrder.some((itemId) => !pendingSet.has(itemId))
    ) {
      clearQueueMoveState();
      return;
    }
    queueMoveSelectionIds = pendingIds.filter((itemId) => selectionIds.includes(itemId));
    const unselectedCount = Math.max(0, pendingIds.length - queueMoveSelectionIds.length);
    queueMoveInsertionIndex = Math.max(0, Math.min(unselectedCount, Number(queueMoveInsertionIndex) || 0));
  }

  if (!queue.items.length) {
    clearQueueFocus();
    clearQueueBatchSelection();
    queueBatchMode = false;
    clearQueueMoveState();
  }
}

function buildReorderedPendingIds(pendingIds, selectionIds, insertionIndex) {
  const orderedPending = Array.isArray(pendingIds) ? pendingIds.slice() : [];
  const orderedSelection = Array.isArray(selectionIds)
    ? selectionIds.filter((itemId) => orderedPending.includes(itemId))
    : [];
  if (!orderedSelection.length) return orderedPending;

  const selectedSet = new Set(orderedSelection);
  const unselected = orderedPending.filter((itemId) => !selectedSet.has(itemId));
  const boundedIndex = Math.max(0, Math.min(unselected.length, Number(insertionIndex) || 0));
  return [
    ...unselected.slice(0, boundedIndex),
    ...orderedSelection,
    ...unselected.slice(boundedIndex),
  ];
}

function applyPendingOrderToQueueItems(items, reorderedPendingIds) {
  const queueItems = Array.isArray(items) ? items.slice() : [];
  const pendingIds = Array.isArray(reorderedPendingIds) ? reorderedPendingIds.slice() : [];
  let pendingCursor = 0;

  return queueItems.map((item) => {
    if (!isQueueItemPending(item)) return item;
    const nextPendingId = pendingIds[pendingCursor];
    pendingCursor += 1;
    if (!nextPendingId) return item;
    return queueItems.find((candidate) => String(candidate?.id || '').trim() === nextPendingId) || item;
  });
}

function getInitialQueueMoveInsertionIndex(queueLike, selectionIds) {
  const pendingIds = getPendingQueueItemIds(queueLike);
  const selectedSet = new Set(Array.isArray(selectionIds) ? selectionIds : []);
  let insertionIndex = 0;
  for (const itemId of pendingIds) {
    if (selectedSet.has(itemId)) {
      break;
    }
    insertionIndex += 1;
  }
  return insertionIndex;
}

function getQueueMoveableSelectionIds(queueLike = null) {
  const queue = queueLike && typeof queueLike === 'object' ? queueLike : loadQueue();
  if (queueBatchMode) {
    return getOrderedQueueBatchSelectedIds(queue);
  }
  return queueFocusedItemId ? [queueFocusedItemId] : [];
}

function moveQueueSelectionToIndex(targetIndex) {
  if (!queueMoveMode) return;
  const currentQueue = loadQueue();
  const pendingIds = getPendingQueueItemIds(currentQueue);
  const selectionIds = Array.isArray(queueMoveSelectionIds) ? queueMoveSelectionIds : [];
  if (!selectionIds.length) return;

  const unselectedCount = Math.max(0, pendingIds.length - selectionIds.length);
  const boundedIndex = Math.max(0, Math.min(unselectedCount, Number(targetIndex) || 0));
  if (boundedIndex === queueMoveInsertionIndex) return;

  queueMoveInsertionIndex = boundedIndex;
  const reorderedPendingIds = buildReorderedPendingIds(pendingIds, selectionIds, boundedIndex);
  queueState = cloneQueueState({
    ...currentQueue,
    items: applyPendingOrderToQueueItems(currentQueue.items, reorderedPendingIds),
  });
  queueStateLoaded = true;
  renderQueue();
}

function exitQueueBatchMode() {
  if (!queueBatchMode) return false;
  queueBatchMode = false;
  clearQueueBatchSelection();
  renderQueue();
  return true;
}

function enterQueueBatchMode() {
  if (queueRunning || queueMoveMode || queueBatchMode) return false;
  const queue = getQueueForUi();
  if (!queue.items.length) return false;

  const seedItemId = queueFocusedItemId;
  queueBatchMode = true;
  clearQueueBatchSelection();
  clearQueueFocus();
  if (seedItemId && getQueueItemById(seedItemId, queue)) {
    queueBatchSelectedItemIds = new Set([seedItemId]);
    queueBatchAnchorItemId = seedItemId;
  }
  renderQueue();
  return true;
}

function toggleQueueBatchMode() {
  if (queueBatchMode) {
    exitQueueBatchMode();
    return;
  }
  enterQueueBatchMode();
}

function applyQueueBatchRangeSelection(targetItemId, queueLike = null) {
  const queue = queueLike && typeof queueLike === 'object' ? queueLike : loadQueue();
  const normalizedTargetId = String(targetItemId || '').trim();
  const normalizedAnchorId = String(queueBatchAnchorItemId || '').trim();
  if (!normalizedTargetId || !normalizedAnchorId) return false;

  const ids = (Array.isArray(queue.items) ? queue.items : [])
    .map((item) => String(item?.id || '').trim())
    .filter(Boolean);
  const anchorIndex = ids.indexOf(normalizedAnchorId);
  const targetIndex = ids.indexOf(normalizedTargetId);
  if (anchorIndex < 0 || targetIndex < 0) return false;

  const [start, end] = anchorIndex <= targetIndex
    ? [anchorIndex, targetIndex]
    : [targetIndex, anchorIndex];
  queueBatchSelectedItemIds = new Set(ids.slice(start, end + 1));
  return true;
}

function handleQueueBatchSelection(itemId, event, queueLike = null) {
  if (queueMoveMode || !queueBatchMode) return;
  const queue = queueLike && typeof queueLike === 'object' ? queueLike : loadQueue();
  const normalizedId = String(itemId || '').trim();
  if (!normalizedId || !getQueueItemById(normalizedId, queue)) return;

  if (event.shiftKey) {
    if (!applyQueueBatchRangeSelection(normalizedId, queue)) {
      queueBatchSelectedItemIds = new Set([normalizedId]);
    }
    queueBatchAnchorItemId = normalizedId;
    renderQueue();
    return;
  }

  if (event.ctrlKey || event.metaKey) {
    const next = new Set(Array.from(queueBatchSelectedItemIds));
    if (next.has(normalizedId)) next.delete(normalizedId);
    else next.add(normalizedId);
    queueBatchSelectedItemIds = next;
    queueBatchAnchorItemId = normalizedId;
    renderQueue();
    return;
  }

  queueBatchSelectedItemIds = new Set([normalizedId]);
  queueBatchAnchorItemId = normalizedId;
  renderQueue();
}

async function saveQueueRuntimeState(active) {
  const normalizedQueue = loadQueue();
  normalizedQueue.runtime = {
    active: Boolean(active),
    updatedAt: Date.now()
  };
  queueState = cloneQueueState(normalizedQueue);
  queueStateLoaded = true;

  if (!normalizedQueue.items.length) {
    return;
  }

  try {
    await persistQueueState();
  } catch (error) {
    console.warn('Failed to persist queue runtime state', error);
  }
}

function attachQueueUi() {
  if (queueUiLoaded) return false;
  queueUiLoaded = true;
  renderQueue();
  updateRunButtonState();
  return true;
}

async function attachQueueUiIfNeeded() {
  if (queueUiLoaded) return false;
  await ensureQueueStateLoaded();
  const queue = loadQueue();
  if (!queue.items.length) return false;
  return attachQueueUi();
}

async function loadQueueUi() {
  const attached = await attachQueueUiIfNeeded();
  if (!attached) {
    renderQueue();
    updateRunButtonState();
  }
  return attached;
}

async function saveQueue(queue) {
  const normalized = normalizeQueueState(queue);
  queueState = cloneQueueState(normalized);
  queueStateLoaded = true;

  if (!normalized.items.length) {
    await clearQueueStateFromServer();
    return;
  }

  await persistQueueState();
}

function isQueueRunning() {
  return queueRunning;
}

function requestQueueStopAfterCurrent() {
  if (!queueRunning) return false;
  if (queueStopRequested) return true;
  queueStopRequested = true;
  updateRunButtonState();
  return true;
}

function updateQueueSecondaryRow() {
  const row = document.getElementById('queueSecondaryRow');
  const autoCreateLabel = document.getElementById('queueAutoCreateSetLabel');
  const moveHint = document.getElementById('queueMoveHint');
  if (!row) return;
  const showAutoCreate = Boolean(autoCreateLabel && !autoCreateLabel.hidden);
  const showMoveHint = Boolean(moveHint && !moveHint.hidden);
  row.hidden = !showAutoCreate && !showMoveHint;
}

function getQueueAutoCreateSetUiState() {
  const selectedPaths = typeof getSelectedCsvPaths === 'function' ? getSelectedCsvPaths() : [];
  const wfToggle = document.getElementById('enableWF');
  const wfEnabled = Boolean(wfToggle && wfToggle.checked && !wfToggle.disabled);
  const selectedCount = selectedPaths.length;
  return {
    visible: wfEnabled,
    disabled: wfEnabled && selectedCount <= 1,
    eligible: wfEnabled && selectedCount > 1,
  };
}

function readQueueAutoCreateSetStoredPreference(input) {
  const value = String(input?.dataset?.preference || '').trim();
  if (value === '1') return true;
  if (value === '0') return false;
  return null;
}

function writeQueueAutoCreateSetStoredPreference(input, value) {
  if (!input) return;
  if (typeof value !== 'boolean') {
    delete input.dataset.preference;
    return;
  }
  input.dataset.preference = value ? '1' : '0';
}

function syncQueueAutoCreateSetUi(options = {}) {
  const autoCreateLabel = document.getElementById('queueAutoCreateSetLabel');
  const autoCreateInput = document.getElementById('queueAutoCreateSet');
  if (!autoCreateLabel || !autoCreateInput) return;

  if (options.resetPreference) {
    writeQueueAutoCreateSetStoredPreference(autoCreateInput, null);
  }
  if (typeof options.forceValue === 'boolean') {
    writeQueueAutoCreateSetStoredPreference(autoCreateInput, options.forceValue);
  }

  const uiState = getQueueAutoCreateSetUiState();
  autoCreateLabel.hidden = !uiState.visible;
  autoCreateLabel.classList.toggle('is-disabled', uiState.disabled);
  autoCreateInput.disabled = uiState.disabled;

  if (!uiState.visible) {
    autoCreateInput.checked = false;
    updateQueueSecondaryRow();
    return;
  }

  if (uiState.disabled) {
    autoCreateInput.checked = false;
    updateQueueSecondaryRow();
    return;
  }

  let storedPreference = readQueueAutoCreateSetStoredPreference(autoCreateInput);
  if (storedPreference === null) {
    storedPreference = true;
    writeQueueAutoCreateSetStoredPreference(autoCreateInput, true);
  }
  autoCreateInput.checked = storedPreference;
  updateQueueSecondaryRow();
}

function getQueueAutoCreateSetPreference() {
  const autoCreateLabel = document.getElementById('queueAutoCreateSetLabel');
  const autoCreateInput = document.getElementById('queueAutoCreateSet');
  if (!autoCreateLabel || autoCreateLabel.hidden || !autoCreateInput || autoCreateInput.disabled) return false;
  return Boolean(autoCreateInput.checked);
}

function updateQueueToolbarState() {
  const queue = getQueueForUi();
  const items = Array.isArray(queue.items) ? queue.items : [];
  const hasItems = items.length > 0;
  const hasStoredItems = hasPersistedQueueItems();
  const addBtn = document.getElementById('addToQueueBtn');
  const loadBtn = document.getElementById('loadQueueBtn');
  const clearBtn = document.getElementById('clearQueueBtn');
  const batchBtn = document.getElementById('batchQueueBtn');
  const moveBtn = document.getElementById('moveQueueBtn');
  const moveHint = document.getElementById('queueMoveHint');

  const batchSelectedIds = getOrderedQueueBatchSelectedIds(queue);
  const moveTargetIds = getQueueMoveableSelectionIds(queue);
  const canMove = !queueRunning
    && !queueMoveMode
    && moveTargetIds.length > 0
    && moveTargetIds.every((itemId) => isQueueItemPending(getQueueItemById(itemId, queue)));

  if (addBtn) {
    addBtn.disabled = queueRunning;
  }
  if (loadBtn) {
    loadBtn.style.display = queueRunning ? 'none' : 'inline-block';
    loadBtn.disabled = queueRunning || !hasStoredItems || queueUiLoaded;
  }
  if (clearBtn) {
    clearBtn.style.display = hasItems ? 'inline-block' : 'none';
    clearBtn.textContent = queueBatchMode ? 'Delete' : 'Clear';
    clearBtn.disabled = queueRunning || queueMoveMode || (
      queueBatchMode ? batchSelectedIds.length === 0 : !hasItems
    );
  }
  if (batchBtn) {
    batchBtn.style.display = hasItems ? 'inline-block' : 'none';
    batchBtn.disabled = queueRunning || queueMoveMode || !hasItems;
    batchBtn.classList.toggle('active', queueBatchMode);
  }
  if (moveBtn) {
    moveBtn.style.display = hasItems ? 'inline-block' : 'none';
    moveBtn.disabled = !canMove;
    moveBtn.classList.toggle('active', queueMoveMode);
  }
  if (moveHint) {
    moveHint.hidden = !queueMoveMode;
  }

  const removeButtons = document.querySelectorAll('.queue-item-remove');
  removeButtons.forEach((btn) => {
    const isLocked = btn.dataset.locked === '1';
    btn.disabled = queueRunning || isLocked;
    btn.style.visibility = btn.disabled ? 'hidden' : 'visible';
  });

  updateQueueSecondaryRow();
}

function setQueueControlsDisabled(disabled) {
  void disabled;
  updateQueueToolbarState();
}

function requestServerCancelBestEffort(runId = '') {
  if (typeof cancelOptimizationRequest !== 'function') return Promise.resolve();
  return cancelOptimizationRequest(runId).catch((error) => {
    console.warn('Queue cancel: failed to notify server cancel endpoint', error);
  });
}

function startQueueMoveMode() {
  if (queueRunning || queueMoveMode) return false;
  const currentQueue = loadQueue();
  const selectionIds = getQueueMoveableSelectionIds(currentQueue);
  if (!selectionIds.length) return false;

  const normalizedSelectionIds = selectionIds
    .map((itemId) => String(itemId || '').trim())
    .filter(Boolean);
  const allPending = normalizedSelectionIds.every((itemId) => {
    return isQueueItemPending(getQueueItemById(itemId, currentQueue));
  });
  if (!allPending) {
    alert('Move works only for pending queue items.');
    return false;
  }

  queueMoveMode = true;
  queueMoveSelectionIds = getPendingQueueItemIds(currentQueue)
    .filter((itemId) => normalizedSelectionIds.includes(itemId));
  queueMoveOriginalPendingOrder = getPendingQueueItemIds(currentQueue);
  queueMoveInsertionIndex = getInitialQueueMoveInsertionIndex(currentQueue, queueMoveSelectionIds);
  renderQueue();
  return true;
}

async function confirmQueueMoveMode() {
  if (!queueMoveMode) return false;
  const currentQueue = loadQueue();
  const currentPendingOrder = getPendingQueueItemIds(currentQueue);
  const originalOrder = Array.isArray(queueMoveOriginalPendingOrder)
    ? queueMoveOriginalPendingOrder.slice()
    : [];
  const changed = originalOrder.length === currentPendingOrder.length
    ? originalOrder.some((itemId, index) => itemId !== currentPendingOrder[index])
    : true;

  clearQueueMoveState();
  if (!changed) {
    renderQueue();
    return true;
  }

  try {
    await saveQueue(currentQueue);
    renderQueue();
    updateRunButtonState();
    return true;
  } catch (error) {
    alert(error?.message || 'Failed to save new queue order.');
    return false;
  }
}

function cancelQueueMoveMode() {
  if (!queueMoveMode) return false;
  const currentQueue = loadQueue();
  const originalPendingOrder = Array.isArray(queueMoveOriginalPendingOrder)
    ? queueMoveOriginalPendingOrder.slice()
    : [];
  if (originalPendingOrder.length) {
    queueState = cloneQueueState({
      ...currentQueue,
      items: applyPendingOrderToQueueItems(currentQueue.items, originalPendingOrder),
    });
    queueStateLoaded = true;
  }
  clearQueueMoveState();
  renderQueue();
  return true;
}

async function deleteSelectedQueueItems() {
  if (queueRunning || !queueBatchMode) return false;
  const queue = loadQueue();
  const selectedIds = new Set(getOrderedQueueBatchSelectedIds(queue));
  if (!selectedIds.size) return false;

  queue.items = (Array.isArray(queue.items) ? queue.items : [])
    .filter((item) => !selectedIds.has(String(item?.id || '').trim()));
  await saveQueue(queue);
  syncQueueUiEphemeralState(loadQueue());
  renderQueue();
  updateRunButtonState();
  return true;
}

async function handleQueueClearAction() {
  if (queueRunning) return false;
  await ensureQueueStateLoaded();
  const queue = loadQueue();

  if (queueBatchMode) {
    const selectedIds = getOrderedQueueBatchSelectedIds(queue);
    if (!selectedIds.length) return false;
    const confirmed = window.confirm(
      selectedIds.length > 1
        ? `Delete ${selectedIds.length} selected queue items?`
        : 'Delete selected queue item?'
    );
    if (!confirmed) return false;
    return deleteSelectedQueueItems();
  }

  if (!queue.items.length) return false;
  const confirmed = window.confirm('Clear all items from the queue?');
  if (!confirmed) return false;
  await clearQueue();
  return true;
}

function bindQueueUiEventsOnce() {
  if (queueUiEventsBound) return;

  const batchBtn = document.getElementById('batchQueueBtn');
  if (batchBtn) {
    batchBtn.addEventListener('click', () => {
      toggleQueueBatchMode();
    });
  }

  const moveBtn = document.getElementById('moveQueueBtn');
  if (moveBtn) {
    moveBtn.addEventListener('click', () => {
      startQueueMoveMode();
    });
  }

  const autoCreateInput = document.getElementById('queueAutoCreateSet');
  if (autoCreateInput) {
    autoCreateInput.addEventListener('change', () => {
      if (!autoCreateInput.disabled) {
        writeQueueAutoCreateSetStoredPreference(autoCreateInput, autoCreateInput.checked);
      }
      updateQueueSecondaryRow();
    });
  }

  document.addEventListener('keydown', (event) => {
    if (event.defaultPrevented) return;

    if (event.key === 'Escape') {
      if (queueMoveMode) {
        event.preventDefault();
        cancelQueueMoveMode();
        return;
      }
      if (queueBatchMode) {
        event.preventDefault();
        exitQueueBatchMode();
      }
      return;
    }

    if (!queueMoveMode || isTypingElement(document.activeElement)) return;

    if (event.key === 'Enter') {
      event.preventDefault();
      void confirmQueueMoveMode();
      return;
    }
    if (event.key === 'ArrowUp') {
      event.preventDefault();
      moveQueueSelectionToIndex(queueMoveInsertionIndex - 1);
      return;
    }
    if (event.key === 'ArrowDown') {
      event.preventDefault();
      moveQueueSelectionToIndex(queueMoveInsertionIndex + 1);
    }
  });

  queueUiEventsBound = true;
}

function collectQueueSources() {
  const paths = typeof getSelectedCsvPaths === 'function'
    ? getSelectedCsvPaths()
    : (window.selectedCsvPath ? [window.selectedCsvPath] : []);
  const sources = [];
  const seen = new Set();

  paths.forEach((path) => {
    const value = String(path || '').trim();
    if (!value) return;
    if (!isAbsoluteFilesystemPath(value)) {
      showQueueError(
        'Queue requires absolute CSV paths.\n'
        + 'Set CSV Directory and choose files from the browser before adding to queue.'
      );
      return;
    }
    const identity = 'path:' + value.toLowerCase();
    if (seen.has(identity)) return;
    seen.add(identity);
    sources.push({ type: 'path', path: value });
  });

  return sources;
}

function collectQueueUiSnapshot() {
  const snapshot = {};
  const controls = document.querySelectorAll(
    '#optimizerForm input[id], #optimizerForm select[id], #optimizerForm textarea[id]'
  );
  controls.forEach((control) => {
    const controlId = String(control.id || '').trim();
    if (!controlId) return;
    const controlType = String(control.type || '').toLowerCase();
    if (controlType === 'file') return;
    if (controlType === 'checkbox' || controlType === 'radio') {
      snapshot[controlId] = { checked: Boolean(control.checked) };
      return;
    }
    snapshot[controlId] = { value: control.value == null ? '' : String(control.value) };
  });

  const dbTarget = document.getElementById('dbTarget');
  if (dbTarget) {
    snapshot.dbTarget = { value: String(dbTarget.value || '') };
  }

  return {
    version: 1,
    controls: snapshot
  };
}

function applyQueueUiSnapshot(snapshot) {
  if (!snapshot || typeof snapshot !== 'object') return false;
  const controls = snapshot.controls && typeof snapshot.controls === 'object'
    ? snapshot.controls
    : null;
  if (!controls) return false;

  let appliedAny = false;
  Object.entries(controls).forEach(([controlId, state]) => {
    if (!state || typeof state !== 'object') return;
    const control = document.getElementById(controlId);
    if (!control) return;

    const controlType = String(control.type || '').toLowerCase();
    if (Object.prototype.hasOwnProperty.call(state, 'checked')
      && (controlType === 'checkbox' || controlType === 'radio')) {
      control.checked = Boolean(state.checked);
      appliedAny = true;
      return;
    }
    if (Object.prototype.hasOwnProperty.call(state, 'value') && controlType !== 'file') {
      control.value = state.value == null ? '' : String(state.value);
      appliedAny = true;
    }
  });

  return appliedAny;
}

function triggerControlEvent(controlId, eventName = 'change') {
  const control = document.getElementById(controlId);
  if (!control) return;
  control.dispatchEvent(new Event(eventName, { bubbles: true }));
}

function setQueueBudgetMode(mode) {
  const targetMode = typeof mode === 'string' && mode.trim() ? mode.trim() : 'trials';
  const radio = document.querySelector(`input[name="budgetMode"][value="${targetMode}"]`);
  if (radio) {
    radio.checked = true;
  }
}

function applyQueueObjectives(config) {
  const selectedObjectives = Array.isArray(config?.objectives) ? config.objectives : [];
  const objectiveCheckboxes = document.querySelectorAll('.objective-checkbox');
  if (!objectiveCheckboxes.length) return;

  const selectedSet = new Set(selectedObjectives.map((value) => String(value)));
  objectiveCheckboxes.forEach((checkbox) => {
    const objective = String(checkbox.dataset.objective || '');
    checkbox.checked = selectedSet.has(objective);
  });
  if (!selectedObjectives.length) {
    objectiveCheckboxes[0].checked = true;
  }

  if (window.OptunaUI && typeof window.OptunaUI.updateObjectiveSelection === 'function') {
    window.OptunaUI.updateObjectiveSelection();
  }

  const primaryObjective = String(config.primary_objective || '').trim();
  const primarySelect = document.getElementById('primaryObjective');
  if (primarySelect && primaryObjective) {
    const hasOption = Array.from(primarySelect.options).some((option) => option.value === primaryObjective);
    if (hasOption) {
      primarySelect.value = primaryObjective;
    }
  }
}

function applyQueueConstraints(config) {
  const constraints = Array.isArray(config?.constraints) ? config.constraints : [];
  const constraintsByMetric = new Map();
  constraints.forEach((constraint) => {
    const metric = String(constraint?.metric || '').trim();
    if (!metric) return;
    constraintsByMetric.set(metric, constraint);
  });

  const rows = document.querySelectorAll('.constraint-row');
  rows.forEach((row) => {
    const checkbox = row.querySelector('.constraint-checkbox');
    const input = row.querySelector('.constraint-input');
    const metric = String(checkbox?.dataset?.constraintMetric || '').trim();
    if (!metric) return;

    const constraint = constraintsByMetric.get(metric);
    checkbox.checked = Boolean(constraint && constraint.enabled);
    if (constraint && constraint.threshold != null && Number.isFinite(Number(constraint.threshold))) {
      input.value = Number(constraint.threshold);
    }
  });
}

function applyQueueParamSelection(config) {
  const enabledParams = config && typeof config.enabled_params === 'object'
    ? config.enabled_params
    : {};
  const paramRanges = config && typeof config.param_ranges === 'object'
    ? config.param_ranges
    : {};

  const optimizerParams = typeof getOptimizerParamElements === 'function'
    ? getOptimizerParamElements()
    : [];

  optimizerParams.forEach(({ name, checkbox, fromInput, toInput, stepInput, def }) => {
    if (!checkbox) return;
    const enabled = Boolean(enabledParams[name]);
    checkbox.checked = enabled;

    const paramType = String(def?.type || '').toLowerCase();
    const range = paramRanges[name];
    if (!enabled || range == null) return;

    if ((paramType === 'select' || paramType === 'options') && range && typeof range === 'object') {
      const selectedValues = new Set(
        Array.isArray(range.values)
          ? range.values.map((value) => String(value))
          : []
      );
      const optionCheckboxes = document.querySelectorAll(
        `input.select-option-checkbox[data-param-name="${name}"]`
      );
      optionCheckboxes.forEach((optionCheckbox) => {
        const optionValue = String(optionCheckbox.dataset.optionValue || '');
        if (optionValue === '__ALL__') return;
        optionCheckbox.checked = selectedValues.has(optionValue);
      });
      const allCheckbox = document.querySelector(
        `input.select-option-checkbox[data-param-name="${name}"][data-option-value="__ALL__"]`
      );
      if (allCheckbox) {
        const individual = document.querySelectorAll(
          `input.select-option-checkbox[data-param-name="${name}"]:not([data-option-value="__ALL__"])`
        );
        allCheckbox.checked = individual.length > 0 && Array.from(individual).every((entry) => entry.checked);
      }
      return;
    }

    if (Array.isArray(range) && range.length >= 3) {
      if (fromInput) fromInput.value = range[0];
      if (toInput) toInput.value = range[1];
      if (stepInput) stepInput.value = range[2];
    }
  });

  if (typeof bindOptimizerInputs === 'function') {
    bindOptimizerInputs();
  }
}

function applyQueueConfigFallback(item) {
  const config = item && typeof item.config === 'object' ? item.config : {};
  const fixedParams = config && typeof config.fixed_params === 'object'
    ? clonePreset(config.fixed_params)
    : {};

  setCheckboxValue('dateFilter', Boolean(fixedParams.dateFilter));

  const { date: startDate, time: startTime } = parseISOTimestamp(fixedParams.start || '');
  const { date: endDate, time: endTime } = parseISOTimestamp(fixedParams.end || '');
  setInputValue('startDate', startDate);
  setInputValue('startTime', startTime || '00:00');
  setInputValue('endDate', endDate);
  setInputValue('endTime', endTime || '00:00');

  delete fixedParams.dateFilter;
  delete fixedParams.start;
  delete fixedParams.end;
  if (typeof applyDynamicBacktestParams === 'function') {
    applyDynamicBacktestParams(fixedParams);
  }

  if (Object.prototype.hasOwnProperty.call(config, 'worker_processes')) {
    setInputValue('workerProcesses', config.worker_processes);
  }
  if (Object.prototype.hasOwnProperty.call(config, 'detailed_log')) {
    setCheckboxValue('detailedLog', Boolean(config.detailed_log));
  }
  if (Object.prototype.hasOwnProperty.call(config, 'trials_log')) {
    setCheckboxValue('trialsLog', Boolean(config.trials_log));
  }

  if (Object.prototype.hasOwnProperty.call(config, 'filter_min_profit')) {
    setCheckboxValue('minProfitFilter', Boolean(config.filter_min_profit));
  }
  if (Object.prototype.hasOwnProperty.call(config, 'min_profit_threshold')) {
    setInputValue('minProfitThreshold', config.min_profit_threshold);
  }
  if (typeof syncMinProfitFilterUI === 'function') {
    syncMinProfitFilterUI();
  }

  if (config.score_config && typeof applyScoreSettings === 'function') {
    applyScoreSettings({
      scoreFilterEnabled: Boolean(config.score_config.filter_enabled),
      scoreThreshold: config.score_config.min_score_threshold,
      scoreWeights: clonePreset(config.score_config.weights || {}),
      scoreEnabledMetrics: clonePreset(config.score_config.enabled_metrics || {}),
      scoreInvertMetrics: clonePreset(config.score_config.invert_metrics || {}),
      scoreMetricBounds: clonePreset(config.score_config.metric_bounds || {})
    });
  }

  setQueueBudgetMode(config.optuna_budget_mode);
  if (Object.prototype.hasOwnProperty.call(config, 'optuna_n_trials')) {
    setInputValue('optunaTrials', config.optuna_n_trials);
  }
  if (Object.prototype.hasOwnProperty.call(config, 'optuna_time_limit')) {
    const minutes = Math.max(1, Math.round(Number(config.optuna_time_limit) / 60));
    setInputValue('optunaTimeLimit', Number.isFinite(minutes) ? minutes : 60);
  }
  if (Object.prototype.hasOwnProperty.call(config, 'optuna_convergence')) {
    setInputValue('optunaConvergence', config.optuna_convergence);
  }
  if (Object.prototype.hasOwnProperty.call(config, 'optuna_enable_pruning')) {
    setCheckboxValue('optunaPruning', Boolean(config.optuna_enable_pruning));
  }
  if (Object.prototype.hasOwnProperty.call(config, 'sampler')) {
    setInputValue('optunaSampler', config.sampler);
  }
  if (Object.prototype.hasOwnProperty.call(config, 'optuna_pruner')) {
    setInputValue('optunaPruner', config.optuna_pruner);
  }
  if (Object.prototype.hasOwnProperty.call(config, 'n_startup_trials')) {
    setInputValue('optunaWarmupTrials', config.n_startup_trials);
  }
  if (Object.prototype.hasOwnProperty.call(config, 'coverage_mode')) {
    setCheckboxValue('optunaCoverageMode', Boolean(config.coverage_mode));
  }
  if (Object.prototype.hasOwnProperty.call(config, 'dispatcher_batch_result_processing')) {
    setCheckboxValue('dispatcherBatchResultProcessing', Boolean(config.dispatcher_batch_result_processing));
  }
  if (Object.prototype.hasOwnProperty.call(config, 'dispatcher_soft_duplicate_cycle_limit_enabled')) {
    setCheckboxValue('softDuplicateCycleLimitEnabled', Boolean(config.dispatcher_soft_duplicate_cycle_limit_enabled));
  }
  if (Object.prototype.hasOwnProperty.call(config, 'dispatcher_duplicate_cycle_limit')) {
    setInputValue('dispatcherDuplicateCycleLimit', config.dispatcher_duplicate_cycle_limit);
  }
  if (Object.prototype.hasOwnProperty.call(config, 'population_size')) {
    setInputValue('nsgaPopulationSize', config.population_size);
  }
  if (Object.prototype.hasOwnProperty.call(config, 'crossover_prob')) {
    setInputValue('nsgaCrossoverProb', config.crossover_prob);
  }
  if (Object.prototype.hasOwnProperty.call(config, 'mutation_prob')) {
    setInputValue('nsgaMutationProb', config.mutation_prob);
  }
  if (Object.prototype.hasOwnProperty.call(config, 'swapping_prob')) {
    setInputValue('nsgaSwappingProb', config.swapping_prob);
  }
  if (Object.prototype.hasOwnProperty.call(config, 'sanitize_enabled')) {
    setCheckboxValue('optuna_sanitize_enabled', Boolean(config.sanitize_enabled));
  }
  if (Object.prototype.hasOwnProperty.call(config, 'sanitize_trades_threshold')) {
    setInputValue('optuna_sanitize_trades_threshold', config.sanitize_trades_threshold);
  }

  applyQueueObjectives(config);
  applyQueueConstraints(config);
  applyQueueParamSelection(config);

  const isWfaMode = item.mode === 'wfa';
  setCheckboxValue('enableWF', isWfaMode);
  if (isWfaMode && item.wfa && typeof item.wfa === 'object') {
    setInputValue('wfIsPeriodDays', item.wfa.isPeriodDays);
    setInputValue('wfOosPeriodDays', item.wfa.oosPeriodDays);
    setInputValue('wfStoreTopNTrials', item.wfa.storeTopNTrials);
    setCheckboxValue('enableAdaptiveWF', Boolean(item.wfa.adaptiveMode));
    setInputValue('wfMaxOosPeriodDays', item.wfa.maxOosPeriodDays);
    setInputValue('wfMinOosTrades', item.wfa.minOosTrades);
    setInputValue('wfCheckIntervalTrades', item.wfa.checkIntervalTrades);
    setInputValue('wfCusumThreshold', item.wfa.cusumThreshold);
    setInputValue('wfDdThresholdMultiplier', item.wfa.ddThresholdMultiplier);
    setInputValue('wfInactivityMultiplier', item.wfa.inactivityMultiplier);
  } else {
    setCheckboxValue('enableAdaptiveWF', false);
  }

  const postProcess = config && typeof config.postProcess === 'object' ? config.postProcess : {};
  setCheckboxValue('enablePostProcess', Boolean(postProcess.enabled));
  if (Object.prototype.hasOwnProperty.call(postProcess, 'ftPeriodDays')) {
    setInputValue('ftPeriodDays', postProcess.ftPeriodDays);
  }
  if (Object.prototype.hasOwnProperty.call(postProcess, 'topK')) {
    setInputValue('ftTopK', postProcess.topK);
  }
  if (Object.prototype.hasOwnProperty.call(postProcess, 'sortMetric')) {
    setInputValue('ftSortMetric', postProcess.sortMetric);
  }
  setCheckboxValue('enableDSR', Boolean(postProcess.dsrEnabled));
  if (Object.prototype.hasOwnProperty.call(postProcess, 'dsrTopK')) {
    setInputValue('dsrTopK', postProcess.dsrTopK);
  }
  const stress = postProcess && typeof postProcess.stressTest === 'object' ? postProcess.stressTest : {};
  setCheckboxValue('enableStressTest', Boolean(stress.enabled));
  if (Object.prototype.hasOwnProperty.call(stress, 'topK')) {
    setInputValue('stTopK', stress.topK);
  }
  if (Object.prototype.hasOwnProperty.call(stress, 'failureThreshold')) {
    const failurePct = Number(stress.failureThreshold);
    const normalized = Number.isFinite(failurePct)
      ? (failurePct <= 1 ? failurePct * 100 : failurePct)
      : 70;
    setInputValue('stFailureThreshold', normalized);
  }
  if (Object.prototype.hasOwnProperty.call(stress, 'sortMetric')) {
    setInputValue('stSortMetric', stress.sortMetric);
  }

  const oosTest = config && typeof config.oosTest === 'object' ? config.oosTest : {};
  const oosEnabled = !isWfaMode && Boolean(oosTest.enabled);
  setCheckboxValue('enableOosTest', oosEnabled);
  if (Object.prototype.hasOwnProperty.call(oosTest, 'periodDays')) {
    setInputValue('oosPeriodDays', oosTest.periodDays);
  }
  if (Object.prototype.hasOwnProperty.call(oosTest, 'topK')) {
    setInputValue('oosTopK', oosTest.topK);
  }

  if (Object.prototype.hasOwnProperty.call(item, 'warmupBars')) {
    setInputValue('warmupBars', item.warmupBars);
  }
}

function refreshQueueFormUiAfterApply() {
  if (typeof syncBudgetInputs === 'function') {
    syncBudgetInputs();
  }
  if (typeof toggleWFSettings === 'function') {
    toggleWFSettings();
  }
  if (typeof toggleAdaptiveWFSettings === 'function') {
    toggleAdaptiveWFSettings();
  }
  triggerControlEvent('enableWF');
  triggerControlEvent('enableAdaptiveWF');
  triggerControlEvent('enableOosTest');
  triggerControlEvent('enablePostProcess');
  triggerControlEvent('enableDSR');
  triggerControlEvent('enableStressTest');
  triggerControlEvent('optuna_sanitize_enabled');

  if (window.OptunaUI && typeof window.OptunaUI.updateObjectiveSelection === 'function') {
    window.OptunaUI.updateObjectiveSelection();
  }
  if (window.OptunaUI && typeof window.OptunaUI.toggleNsgaSettings === 'function') {
    window.OptunaUI.toggleNsgaSettings();
  }
  if (window.OptunaUI && typeof window.OptunaUI.updateCoverageInfo === 'function') {
    window.OptunaUI.updateCoverageInfo();
  }
  if (window.OptunaUI && typeof window.OptunaUI.syncDispatcherControls === 'function') {
    window.OptunaUI.syncDispatcherControls();
  }
  if (typeof syncMinProfitFilterUI === 'function') {
    syncMinProfitFilterUI();
  }
  if (typeof syncScoreFilterUI === 'function') {
    syncScoreFilterUI();
  }
  if (typeof updateScoreFormulaPreview === 'function') {
    updateScoreFormulaPreview();
  }
  if (typeof window.updateDatasetPreview === 'function') {
    window.updateDatasetPreview();
  }
}

function getPathParentDirectory(path) {
  const value = String(path || '').trim();
  if (!value) return '';
  const slash = Math.max(value.lastIndexOf('/'), value.lastIndexOf('\\'));
  if (slash < 0) return '';
  const parent = value.slice(0, slash);
  if (/^[A-Za-z]:$/.test(parent)) {
    return parent + '\\';
  }
  return parent;
}

async function applyQueueDatabaseTarget(dbTargetRaw) {
  const select = document.getElementById('dbTarget');
  if (!select) return;

  const target = String(dbTargetRaw || '').trim();
  if (!target) return;

  const tryApply = () => {
    const hasOption = Array.from(select.options).some((option) => option.value === target);
    if (!hasOption) return false;
    select.value = target;
    if (typeof toggleDbLabelVisibility === 'function') {
      toggleDbLabelVisibility();
    }
    return true;
  };

  if (tryApply()) return;
  if (typeof loadDatabasesList === 'function') {
    try {
      await loadDatabasesList({ preserveSelection: true });
      tryApply();
    } catch (_error) {
      return;
    }
  }
}

async function ensureQueueItemStrategyLoaded(item) {
  const strategyId = String(item?.strategyId || '').trim();
  if (!strategyId) {
    throw new Error('Queue item has no strategy id.');
  }
  if (typeof loadStrategyConfig !== 'function') {
    throw new Error('Strategy loader is unavailable.');
  }

  const select = document.getElementById('strategySelect');
  if (select) {
    const hasOption = Array.from(select.options).some((option) => option.value === strategyId);
    if (!hasOption) {
      throw new Error('Strategy from queue item is not available in current UI.');
    }
    select.value = strategyId;
  }
  window.currentStrategyId = strategyId;
  await loadStrategyConfig(strategyId);
}

async function loadQueueItemIntoForm(itemId, options = {}) {
  const requestId = Number(options.requestId) || 0;
  const itemSnapshot = options.itemSnapshot && typeof options.itemSnapshot === 'object'
    ? clonePreset(options.itemSnapshot)
    : null;
  const suppressProgressOutput = options.suppressProgressOutput !== false;
  const isCurrentRequest = () => requestId === 0 || requestId === queueItemLoadRequestId;

  await ensureQueueStateLoaded();
  if (!isCurrentRequest()) return false;
  const queue = loadQueue();
  const item = queue.items.find((entry) => entry.id === itemId) || itemSnapshot;
  if (!item) {
    throw new Error('Queue item not found.');
  }

  await ensureQueueItemStrategyLoaded(item);
  if (!isCurrentRequest()) return false;

  if (Object.prototype.hasOwnProperty.call(item, 'warmupBars')) {
    setInputValue('warmupBars', item.warmupBars);
  }

  const sourcePaths = getQueueSources(item).map((source) => String(source.path || '').trim()).filter(Boolean);
  if (typeof setSelectedCsvPaths === 'function') {
    setSelectedCsvPaths(sourcePaths);
  } else {
    window.selectedCsvPaths = sourcePaths;
    window.selectedCsvPath = sourcePaths[0] || '';
    renderSelectedFiles([]);
  }

  const firstSourcePath = sourcePaths[0] || '';
  const csvDirectory = document.getElementById('csvDirectory');
  if (csvDirectory && firstSourcePath) {
    const parent = getPathParentDirectory(firstSourcePath);
    if (parent) {
      csvDirectory.value = parent;
    }
  }

  applyQueueConfigFallback(item);
  applyQueueUiSnapshot(item.uiSnapshot);

  await applyQueueDatabaseTarget(item.dbTarget);
  if (!isCurrentRequest()) return false;
  const studySetState = buildQueueStudySetState(item);
  if (studySetState.configured) {
    syncQueueAutoCreateSetUi({ forceValue: Boolean(studySetState.autoCreate) });
  } else {
    syncQueueAutoCreateSetUi({ resetPreference: true });
  }
  refreshQueueFormUiAfterApply();

  const optimizerResultsEl = document.getElementById('optimizerResults');
  if (optimizerResultsEl && !(suppressProgressOutput && queueRunning)) {
    optimizerResultsEl.textContent = 'Loaded run settings from queue item #' + (item.index || '?') + '.';
    optimizerResultsEl.classList.remove('ready', 'loading');
    optimizerResultsEl.style.display = 'block';
  }

  return true;
}

function extractQueueStrategyShortLabel(item) {
  const strategyId = String(item?.strategyId || '').trim();
  const match = strategyId.match(/^s(\d+)_/i);
  if (match) {
    return `S${String(match[1]).padStart(2, '0')}`;
  }
  const strategyName = String(item?.strategyConfig?.name || '').trim();
  return strategyName ? strategyName.split(/\s+/)[0] : 'Study';
}

function normalizeQueueTimeframeToken(rawToken) {
  const token = String(rawToken || '').trim();
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
  const withMinutes = lower.match(/^(\d+)m$/);
  if (withMinutes) {
    const minutes = Number(withMinutes[1]);
    if (minutes >= 1440 && minutes % 1440 === 0) return `${minutes / 1440}D`;
    if (minutes >= 60 && minutes % 60 === 0) return `${minutes / 60}h`;
    return `${minutes}m`;
  }
  if (/^\d+h$/i.test(token)) return lower;
  if (/^\d+d$/i.test(token)) return `${token.slice(0, -1)}D`;
  if (/^\d+w$/i.test(token)) return lower;
  return token;
}

function extractQueueTimeframeLabel(item) {
  const firstSource = getQueueSources(item)[0];
  const fileName = getPathFileName(firstSource?.path || '');
  if (!fileName) return 'TF';

  const match = fileName.match(/,\s*([^,\s]+)\s+\d{4}[.\-]/);
  if (match) {
    return normalizeQueueTimeframeToken(match[1]) || 'TF';
  }

  const fallback = fileName.split(',')[1] || '';
  const token = fallback.trim().split(/\s+/)[0] || '';
  return normalizeQueueTimeframeToken(token) || 'TF';
}

function formatQueueSamplerLabel(rawSampler) {
  const sampler = String(rawSampler || '').trim().toLowerCase();
  if (sampler === 'nsga2') return 'NSGA-2';
  if (sampler === 'nsga3') return 'NSGA-3';
  if (sampler === 'tpe') return 'TPE';
  if (sampler === 'random') return 'Random';
  return sampler ? sampler.toUpperCase() : 'TPE';
}

function formatQueueCompactTrialCount(rawCount) {
  const count = Number(rawCount);
  if (!Number.isFinite(count) || count <= 0) return '0';
  if (count >= 1000) {
    const compact = count / 1000;
    return Number.isInteger(compact) ? `${compact}k` : `${compact.toFixed(1).replace(/\.0$/, '')}k`;
  }
  return String(Math.round(count));
}

function buildQueueAutoSetBudgetLabel(item) {
  const budgetMode = String(item?.config?.optuna_budget_mode || 'trials').trim().toLowerCase();
  if (budgetMode === 'time') {
    const minutes = Math.max(1, Math.round(Number(item?.config?.optuna_time_limit || 3600) / 60));
    return `${minutes}m`;
  }
  if (budgetMode === 'convergence') {
    const convergence = Math.max(1, Math.round(Number(item?.config?.optuna_convergence || 50)));
    return `conv${convergence}`;
  }
  return formatQueueCompactTrialCount(item?.config?.optuna_n_trials || 0);
}

function buildQueueAutoSetModeLabel(item) {
  const isPeriod = Math.max(1, Math.round(Number(item?.wfa?.isPeriodDays || 0))) || '?';
  const oosPeriod = Math.max(1, Math.round(Number(item?.wfa?.oosPeriodDays || 0))) || '?';
  return `${item?.wfa?.adaptiveMode ? 'WFA-A' : 'WFA-F'} ${isPeriod}/${oosPeriod}`;
}

function buildQueueStudySetState(item, patch = {}) {
  return normalizeQueueStudySet({
    ...(item?.studySet || {}),
    ...(patch || {}),
  }, item);
}

function shouldQueueItemAutoCreateSet(item) {
  return Boolean(
    item
    && item.mode === 'wfa'
    && getQueueSources(item).length > 1
    && item.studySet
    && item.studySet.autoCreate
  );
}

function buildQueueStudySetNameLegacy(item) {
  const itemIndex = Number(item?.index);
  const queueLabel = Number.isFinite(itemIndex) && itemIndex > 0 ? `#${Math.round(itemIndex)}` : '#?';
  const strategyLabel = extractQueueStrategyShortLabel(item);
  const timeframeLabel = extractQueueTimeframeLabel(item);
  const samplerLabel = formatQueueSamplerLabel(item?.config?.sampler);
  const initialTrials = Math.max(0, Math.round(Number(item?.config?.n_startup_trials || 0)));
  const budgetLabel = buildQueueAutoSetBudgetLabel(item);
  const modeLabel = buildQueueAutoSetModeLabel(item);
  return [
    queueLabel,
    strategyLabel,
    timeframeLabel,
    `${samplerLabel} (${initialTrials})`,
    budgetLabel,
    modeLabel,
  ].join(' · ');
}

function buildQueueStudySetName(item) {
  const itemIndex = Number(item?.index);
  const queueLabel = Number.isFinite(itemIndex) && itemIndex > 0 ? `#${Math.round(itemIndex)}` : '#?';
  const strategyLabel = extractQueueStrategyShortLabel(item);
  const timeframeLabel = extractQueueTimeframeLabel(item);
  const samplerLabel = formatQueueSamplerLabel(item?.config?.sampler);
  const initialTrials = Math.max(0, Math.round(Number(item?.config?.n_startup_trials || 0)));
  const budgetLabel = buildQueueAutoSetBudgetLabel(item);
  const modeLabel = buildQueueAutoSetModeLabel(item);
  return [
    queueLabel,
    strategyLabel,
    timeframeLabel,
    `${samplerLabel} (${initialTrials})`,
    budgetLabel,
    modeLabel,
  ].join(' \u00B7 ');
}

async function finalizeQueueStudySetIfNeeded(item, studySetState) {
  const baseState = buildQueueStudySetState(item, studySetState);
  if (!shouldQueueItemAutoCreateSet({ ...item, studySet: baseState })) {
    return { studySet: baseState, created: false, warning: false };
  }
  if (baseState.createdSetId) {
    return { studySet: baseState, created: false, warning: false };
  }

  const completedStudyIds = uniqueStringValues(baseState.completedStudyIds);
  const lastUpdatedAt = new Date().toISOString();
  if (completedStudyIds.length < 2) {
    return {
      studySet: buildQueueStudySetState(item, {
        ...baseState,
        configured: true,
        status: 'skipped',
        error: '',
        lastUpdatedAt,
      }),
      created: false,
      warning: false,
    };
  }

  const requestedName = buildQueueStudySetName(item);
  try {
    const createdSet = await createAnalyticsSetRequest(requestedName, completedStudyIds, {
      colorToken: 'lavender',
    });
    return {
      studySet: buildQueueStudySetState(item, {
        ...baseState,
        configured: true,
        createdSetId: Number(createdSet?.id) || null,
        createdSetName: String(createdSet?.name || requestedName).trim(),
        status: 'created',
        error: '',
        lastUpdatedAt,
      }),
      created: true,
      warning: false,
    };
  } catch (error) {
    const message = String(error?.message || 'Failed to create study set.').trim();
    console.warn('Queue study set auto-create failed for item #' + (item?.index || '?'), error);
    return {
      studySet: buildQueueStudySetState(item, {
        ...baseState,
        configured: true,
        status: 'error',
        error: message,
        lastUpdatedAt,
      }),
      created: false,
      warning: true,
      warningMessage: message,
    };
  }
}

function collectQueueItem() {
  const itemId = 'q_' + Date.now() + '_' + Math.random().toString(36).slice(2, 6);
  const sources = collectQueueSources();
  if (sources === null) {
    return null;
  }

  if (!sources.length) {
    showQueueError(
      'Please select at least one CSV file before adding to queue.\n\n'
      + 'Set CSV Directory and use Choose Files to add data sources.'
    );
    return null;
  }

  if (!window.currentStrategyId) {
    showQueueError('Please select a strategy before adding to queue.');
    return null;
  }

  const validationErrors = validateOptimizerForm(window.currentStrategyConfig);
  if (validationErrors.length) {
    showQueueError('Validation errors:\n' + validationErrors.join('\n'));
    return null;
  }

  const state = gatherFormState();
  if (!state.start || !state.end) {
    showQueueError('Please specify both start and end dates.');
    return null;
  }

  const dbTargetError = getDatabaseTargetValidationError();
  if (dbTargetError) {
    showQueueError(dbTargetError);
    return null;
  }

  const config = buildOptunaConfig(state);
  const hasEnabledParams = Object.values(config.enabled_params || {}).some(Boolean);
  if (!hasEnabledParams) {
    showQueueError('Please enable at least one parameter to optimize.');
    return null;
  }

  const wfToggle = document.getElementById('enableWF');
  const wfEnabled = Boolean(wfToggle && wfToggle.checked && !wfToggle.disabled);
  const mode = wfEnabled ? 'wfa' : 'optuna';

  const queue = loadQueue();
  const itemIndex = queue.nextIndex || 1;

  const item = {
    id: itemId,
    index: itemIndex,
    addedAt: new Date().toISOString(),
    mode,
    strategyId: window.currentStrategyId,
    strategyConfig: clonePreset(window.currentStrategyConfig || {}),
    sources,
    warmupBars: Number(document.getElementById('warmupBars')?.value) || 1000,
    config,
    dbTarget: document.getElementById('dbTarget')?.value || '',
    uiSnapshot: collectQueueUiSnapshot(),
    sourceCursor: 0,
    successCount: 0,
    failureCount: 0
  };

  item.studySet = buildQueueStudySetState(item, {
    configured: true,
    autoCreate: getQueueAutoCreateSetPreference(),
    completedStudyIds: [],
    createdSetId: null,
    createdSetName: '',
    status: '',
    error: '',
    lastUpdatedAt: '',
  });

  if (mode === 'wfa') {
    const adaptiveMode = Boolean(document.getElementById('enableAdaptiveWF')?.checked);
    item.wfa = {
      isPeriodDays: Number(document.getElementById('wfIsPeriodDays')?.value) || 90,
      oosPeriodDays: Number(document.getElementById('wfOosPeriodDays')?.value) || 30,
      storeTopNTrials: Number(document.getElementById('wfStoreTopNTrials')?.value) || 50,
      adaptiveMode,
      maxOosPeriodDays: Number(document.getElementById('wfMaxOosPeriodDays')?.value) || 90,
      minOosTrades: Number(document.getElementById('wfMinOosTrades')?.value) || 5,
      checkIntervalTrades: Number(document.getElementById('wfCheckIntervalTrades')?.value) || 3,
      cusumThreshold: Number(document.getElementById('wfCusumThreshold')?.value) || 5.0,
      ddThresholdMultiplier: Number(document.getElementById('wfDdThresholdMultiplier')?.value) || 1.5,
      inactivityMultiplier: Number(document.getElementById('wfInactivityMultiplier')?.value) || 5.0
    };
  }

  item.label = generateQueueLabel(item);
  return item;
}

function generateQueueLabel(item) {
  const index = item.index || '?';

  const strategyName = item.strategyConfig?.name || item.strategyId || '???';
  const strategyVersion = item.strategyConfig?.version || '';
  const strategyLabel = strategyVersion ? (strategyName + ' v' + strategyVersion) : strategyName;

  const csvCount = getQueueSources(item).length;
  const csvLabel = csvCount === 1 ? '1 CSV' : (csvCount + ' CSVs');

  const startRaw = item.config?.fixed_params?.start || '';
  const endRaw = item.config?.fixed_params?.end || '';
  const dateFilter = Boolean(item.config?.fixed_params?.dateFilter);
  let dateLabel = 'no filter';
  if (dateFilter && startRaw && endRaw) {
    const fmtDate = (isoValue) => String(isoValue).slice(0, 10).replace(/-/g, '.');
    dateLabel = fmtDate(startRaw) + '-' + fmtDate(endRaw);
  }

  let modeLabel = 'OPT';
  if (item.mode === 'wfa') {
    const isPeriod = item.wfa?.isPeriodDays || '?';
    const oosPeriod = item.wfa?.oosPeriodDays || '?';
    modeLabel = (item.wfa?.adaptiveMode ? 'WFA-A' : 'WFA-F') + ' ' + isPeriod + '/' + oosPeriod;
  }

  let budgetLabel;
  const budgetMode = item.config?.optuna_budget_mode || 'trials';
  if (budgetMode === 'trials') {
    budgetLabel = String(item.config?.optuna_n_trials || 500) + 't';
  } else if (budgetMode === 'time') {
    const minutes = Math.round((item.config?.optuna_time_limit || 3600) / 60);
    budgetLabel = String(minutes) + 'min';
  } else {
    budgetLabel = 'conv ' + String(item.config?.optuna_convergence || 50);
  }

  return '#' + index + ' \u00B7 ' + strategyLabel + ' \u00B7 ' + csvLabel + ' \u00B7 '
    + dateLabel + ' \u00B7 ' + modeLabel + ' \u00B7 ' + budgetLabel;
}

async function addToQueue(item) {
  if (!item || typeof item !== 'object') return false;

  await ensureQueueStateLoaded();
  await attachQueueUiIfNeeded();
  if (!queueUiLoaded) {
    queueUiLoaded = true;
  }
  const queue = loadQueue();
  if (!item.index || item.index <= 0) {
    item.index = queue.nextIndex || 1;
  }
  if (!item.label) {
    item.label = generateQueueLabel(item);
  }

  queue.items.push(item);
  queue.nextIndex = Math.max(queue.nextIndex || 1, item.index + 1);
  await saveQueue(queue);
  if (!queueBatchMode) {
    setQueueFocusedItemId(item.id);
  }
  renderQueue();
  syncQueueAutoCreateSetUi();
  updateRunButtonState();
  return true;
}

async function removeFromQueue(itemId) {
  if (queueRunning) return;
  await ensureQueueStateLoaded();
  const queue = loadQueue();
  queue.items = queue.items.filter((item) => {
    if (item.id === itemId) {
      return false;
    }
    return true;
  });
  await saveQueue(queue);
  syncQueueUiEphemeralState(loadQueue());
  renderQueue();
  updateRunButtonState();
}

async function clearQueue() {
  if (queueRunning) return;
  await ensureQueueStateLoaded();
  const queue = loadQueue();
  queue.items = [];
  // Empty queue resets label numbering to keep UX predictable.
  await saveQueue(queue);
  syncQueueUiEphemeralState(loadQueue());
  renderQueue();
  updateRunButtonState();
}

function buildQueueTooltip(item) {
  const lines = [];
  const finalState = normalizeQueueFinalState(item?.finalState);
  if (finalState === 'completed') {
    lines.push('Status: Completed');
  } else if (finalState === 'failed') {
    lines.push('Status: Failed');
  } else {
    lines.push('Status: Pending');
  }

  const strategyName = item.strategyConfig?.name || item.strategyId || '(unknown)';
  const strategyVersion = item.strategyConfig?.version || '';
  lines.push('Strategy: ' + strategyName + (strategyVersion ? (' v' + strategyVersion) : ''));

  const sources = getQueueSources(item);
  lines.push('CSV Sources: ' + sources.length + ' path(s)');
  const maxShow = 5;
  sources.slice(0, maxShow).forEach((source, index) => {
    const fileName = buildSourceDisplayLabel(source, index);
    lines.push('  - [' + buildSourceModeLabel(source) + '] ' + fileName);
  });
  if (sources.length > maxShow) {
    lines.push('  ... and ' + (sources.length - maxShow) + ' more');
  }

  if (item.mode === 'wfa') {
    const typeLabel = item.wfa?.adaptiveMode ? 'Adaptive' : 'Fixed';
    lines.push('Mode: WFA ' + typeLabel + ' (IS: ' + item.wfa?.isPeriodDays + 'd, OOS: ' + item.wfa?.oosPeriodDays + 'd)');
  } else {
    lines.push('Mode: Optuna Optimization');
  }

  const budgetMode = item.config?.optuna_budget_mode || 'trials';
  const sampler = (item.config?.sampler || 'tpe').toUpperCase();
  if (budgetMode === 'trials') {
    lines.push('Budget: ' + item.config?.optuna_n_trials + ' trials (' + sampler + ' sampler)');
  } else if (budgetMode === 'time') {
    const minutes = Math.round((item.config?.optuna_time_limit || 3600) / 60);
    lines.push('Budget: ' + minutes + ' min (' + sampler + ' sampler)');
  } else {
    lines.push('Budget: convergence ' + item.config?.optuna_convergence + ' (' + sampler + ' sampler)');
  }

  const objectives = item.config?.objectives || [];
  if (objectives.length) {
    const objectiveNames = objectives.map((objective) => objective.replace(/_/g, ' ').replace(/pct/g, '%'));
    lines.push('Objectives: ' + objectiveNames.join(', '));
  }

  if (shouldQueueItemAutoCreateSet(item)) {
    const studySet = buildQueueStudySetState(item);
    lines.push('Auto-create Set: On');
    lines.push('Set Name: ' + (studySet.createdSetName || buildQueueStudySetName(item)));
    if (studySet.createdSetName) {
      lines.push(`Set Created: ${studySet.createdSetName}`);
    } else if (studySet.status === 'error' && studySet.error) {
      lines.push(`Set Error: ${studySet.error}`);
    } else if (studySet.status === 'skipped') {
      lines.push('Set Status: Skipped');
    }
    if (studySet.completedStudyIds.length) {
      lines.push(`Successful Studies: ${studySet.completedStudyIds.length}`);
    }
  }

  const constraints = Array.isArray(item.config?.constraints) ? item.config.constraints : [];
  const enabledConstraints = constraints.filter((constraint) => constraint && constraint.enabled && constraint.threshold != null);
  if (enabledConstraints.length) {
    const labels = enabledConstraints.map((constraint) => {
      const operator = CONSTRAINT_LE_METRICS.includes(constraint.metric) ? '<=' : '>=';
      return constraint.metric + ' ' + operator + ' ' + constraint.threshold;
    });
    lines.push('Constraints: ' + labels.join(', '));
  }

  const postProcess = item.config?.postProcess;
  if (postProcess?.enabled) {
    lines.push('Forward Test: ' + postProcess.ftPeriodDays + 'd (top ' + postProcess.topK + ')');
  }
  if (item.config?.oosTest?.enabled) {
    lines.push('OOS Test: ' + item.config.oosTest.periodDays + 'd (top ' + item.config.oosTest.topK + ')');
  }

  const dateFilter = item.config?.fixed_params?.dateFilter;
  const start = item.config?.fixed_params?.start || '';
  const end = item.config?.fixed_params?.end || '';
  if (dateFilter && start && end) {
    lines.push('Date Filter: ' + start.slice(0, 16).replace('T', ' ') + ' -> ' + end.slice(0, 16).replace('T', ' '));
  }

  lines.push('Warmup: ' + item.warmupBars + ' bars');
  lines.push('DB Target: ' + (item.dbTarget || '(none)'));

  const enabledCount = Object.values(item.config?.enabled_params || {}).filter(Boolean).length;
  const totalCount = Object.keys(item.config?.enabled_params || {}).length;
  lines.push('Enabled Params: ' + enabledCount + ' of ' + totalCount);

  if (item.sourceCursor && sources.length) {
    lines.push('Progress: source ' + item.sourceCursor + ' of ' + sources.length + ' already processed');
  }

  return lines.join('\n');
}

function renderQueue() {
  const emptyState = document.getElementById('queueEmptyState');
  const itemsList = document.getElementById('queueItemsList');
  const loadBtn = document.getElementById('loadQueueBtn');
  const clearBtn = document.getElementById('clearQueueBtn');
  const batchBtn = document.getElementById('batchQueueBtn');
  const moveBtn = document.getElementById('moveQueueBtn');
  const queue = getQueueForUi();
  syncQueueUiEphemeralState(queue);
  const hasStoredItems = hasPersistedQueueItems();

  if (loadBtn) {
    loadBtn.style.display = queueRunning ? 'none' : 'inline-block';
    loadBtn.disabled = queueRunning || !hasStoredItems || queueUiLoaded;
  }

  if (!queue.items.length) {
    if (emptyState) {
      if (!queueUiLoaded && hasStoredItems) {
        emptyState.textContent = 'Saved queue is available. Click "Load Queue" to attach it.';
      } else {
        emptyState.textContent = 'Configure settings and click "Add to Queue" to schedule runs.';
      }
      emptyState.style.display = 'block';
    }
    if (itemsList) {
      itemsList.style.display = 'none';
      itemsList.innerHTML = '';
    }
    if (clearBtn) clearBtn.style.display = 'none';
    if (batchBtn) batchBtn.style.display = 'none';
    if (moveBtn) moveBtn.style.display = 'none';
    setQueueControlsDisabled(queueRunning);
    return;
  }

  if (emptyState) emptyState.style.display = 'none';
  if (clearBtn) clearBtn.style.display = 'inline-block';
  if (batchBtn) batchBtn.style.display = 'inline-block';
  if (moveBtn) moveBtn.style.display = 'inline-block';
  if (!itemsList) return;

  itemsList.innerHTML = '';
  itemsList.style.display = 'flex';

  const fragment = document.createDocumentFragment();
  const moveSelectionSet = getQueueMoveSelectionSet();
  queue.items.forEach((item) => {
    const itemId = String(item.id || '').trim();
    const row = document.createElement('div');
    row.className = 'queue-item';
    row.classList.add('queue-item-clickable');
    const finalState = normalizeQueueFinalState(item.finalState);
    if (finalState) {
      row.classList.add(finalState);
    }
    if (!queueBatchMode && itemId && itemId === queueFocusedItemId) {
      row.classList.add('queue-item-focused');
    }
    if (queueBatchMode && queueBatchSelectedItemIds.has(itemId)) {
      row.classList.add('queue-item-batch-selected');
    }
    if (queueMoveMode && moveSelectionSet.has(itemId)) {
      row.classList.add('queue-item-moving');
    }
    row.tabIndex = 0;
    row.setAttribute('role', 'button');
    row.setAttribute('aria-label', 'Load settings from queue item #' + (item.index || '?'));
    row.dataset.queueId = item.id;
    row.title = buildQueueTooltip(item);

    const handleQueueRowClick = (event) => {
      if (queueMoveMode) return;
      if (queueBatchMode) {
        handleQueueBatchSelection(itemId, event, queue);
        return;
      }

      setQueueFocusedItemId(itemId);
      const runningNow = queueRunning;
      const requestId = ++queueItemLoadRequestId;
      void loadQueueItemIntoForm(item.id, {
        requestId,
        itemSnapshot: item,
        suppressProgressOutput: true
      }).catch((error) => {
        if (requestId !== queueItemLoadRequestId) return;
        if (runningNow) {
          console.warn('Failed to load queue item settings during queue execution', error);
          return;
        }
        showQueueError(error?.message || 'Failed to load queue item settings.');
      }).finally(() => {
        renderQueue();
      });
    };
    row.addEventListener('click', handleQueueRowClick);
    row.addEventListener('mousedown', (event) => {
      if (queueBatchMode || queueMoveMode) {
        event.preventDefault();
      }
    });
    row.addEventListener('keydown', (event) => {
      if (event.key !== 'Enter' && event.key !== ' ') return;
      event.preventDefault();
      handleQueueRowClick(event);
    });

    const label = document.createElement('span');
    label.className = 'queue-item-label';
    label.textContent = item.label;

    const removeBtn = document.createElement('button');
    removeBtn.type = 'button';
    removeBtn.className = 'queue-item-remove';
    removeBtn.setAttribute('aria-label', 'Remove from queue');
    removeBtn.innerHTML = '&times;';
    if (queueMoveMode || finalState === 'running') {
      removeBtn.disabled = true;
      removeBtn.dataset.locked = '1';
      removeBtn.style.visibility = 'hidden';
    } else {
      removeBtn.dataset.locked = '0';
    }
    removeBtn.addEventListener('click', (event) => {
      event.stopPropagation();
      void removeFromQueue(item.id).catch((error) => {
        showQueueError(error?.message || 'Failed to remove queue item.');
      });
    });

    row.appendChild(label);
    row.appendChild(removeBtn);
    fragment.appendChild(row);
  });

  itemsList.appendChild(fragment);
  setQueueControlsDisabled(queueRunning);
}

function updateRunButtonState() {
  const btn = document.getElementById('runOptimizationBtn');
  if (!btn) return;

  if (queueRunning) {
    btn.textContent = queueStopRequested ? 'Stopping...' : 'Cancel Queue';
    btn.classList.remove('queue-active');
    btn.classList.add('queue-cancel');
    btn.disabled = queueStopRequested;
    return;
  }

  const queue = getQueueForUi();
  const count = countQueuePendingItems(queue);
  btn.classList.remove('queue-cancel');
  btn.disabled = false;

  if (count > 0) {
    btn.textContent = 'Run Queue (' + count + ')';
    btn.classList.add('queue-active');
  } else {
    btn.textContent = 'Run Optimization';
    btn.classList.remove('queue-active');
  }
}

function showQueueError(message) {
  const optimizerResultsEl = document.getElementById('optimizerResults');
  if (!optimizerResultsEl) return;
  optimizerResultsEl.textContent = message;
  optimizerResultsEl.classList.remove('ready', 'loading');
  optimizerResultsEl.style.display = 'block';
}

function setQueueItemState(itemId, state) {
  const row = document.querySelector('.queue-item[data-queue-id="' + itemId + '"]');
  if (!row) return;
  row.classList.remove('running', 'completed', 'failed', 'skipped');
  row.classList.add(state);

  const removeBtn = row.querySelector('.queue-item-remove');
  if (!removeBtn) return;

  if (state === 'running') {
    removeBtn.disabled = true;
    removeBtn.dataset.locked = '1';
    removeBtn.style.visibility = 'hidden';
    return;
  }

  removeBtn.dataset.locked = '0';
  removeBtn.disabled = queueRunning;
  removeBtn.style.visibility = queueRunning ? 'hidden' : 'visible';
}

function buildStrategySummary(item) {
  return {
    id: item.strategyId || '',
    name: item.strategyConfig?.name || '',
    version: item.strategyConfig?.version || '',
    description: item.strategyConfig?.description || ''
  };
}

function buildDatasetLabel(path) {
  if (path && typeof path === 'object') {
    return buildSourceDisplayLabel(path);
  }
  return getPathFileName(path);
}

function buildStateForItem(item, status) {
  const firstSource = getQueueSources(item)[0];
  const state = {
    status,
    mode: item.mode || 'optuna',
    strategy: buildStrategySummary(item),
    strategyId: item.strategyId || '',
    dataset: { label: buildDatasetLabel(firstSource) },
    warmupBars: item.warmupBars,
    dateFilter: item.config?.fixed_params?.dateFilter,
    start: item.config?.fixed_params?.start,
    end: item.config?.fixed_params?.end,
    optuna: {
      objectives: item.config?.objectives,
      primaryObjective: item.config?.primary_objective,
      budgetMode: item.config?.optuna_budget_mode,
      nTrials: item.config?.optuna_n_trials,
      timeLimit: item.config?.optuna_time_limit,
      convergence: item.config?.optuna_convergence,
      sampler: item.config?.sampler,
      pruner: item.config?.optuna_pruner,
      workers: item.config?.worker_processes,
      sanitizeEnabled: item.config?.sanitize_enabled,
      sanitizeTradesThreshold: item.config?.sanitize_trades_threshold
    },
    fixedParams: clonePreset(item.config?.fixed_params || {}),
    strategyConfig: clonePreset(item.strategyConfig || {})
  };

  if (item.mode === 'wfa') {
    state.wfa = clonePreset(item.wfa || {});
  } else {
    state.wfa = {};
  }

  return state;
}

async function persistItemProgress(itemId, patch) {
  const queue = loadQueue();
  const item = queue.items.find((entry) => entry.id === itemId);
  if (!item) return;

  const normalizedPatch = patch && typeof patch === 'object' ? { ...patch } : {};
  if (Object.prototype.hasOwnProperty.call(normalizedPatch, 'studySet')) {
    item.studySet = buildQueueStudySetState(item, normalizedPatch.studySet);
    delete normalizedPatch.studySet;
  }
  Object.assign(item, normalizedPatch);
  await saveQueue(queue);
}

async function finalizeQueueItem(itemId, finalState, patch = {}) {
  const normalizedFinalState = normalizeQueueFinalState(finalState);
  if (!normalizedFinalState) return;
  await persistItemProgress(itemId, {
    ...patch,
    finalState: normalizedFinalState
  });
}

async function runQueue() {
  if (queueRunning) return;
  await ensureQueueStateLoaded();

  const initialQueue = getQueueForUi();
  const totalItems = countQueuePendingItems(initialQueue);
  if (totalItems === 0) {
    const optimizerResultsEl = document.getElementById('optimizerResults');
    if (optimizerResultsEl) {
      optimizerResultsEl.textContent = 'Queue has no pending items. Completed items are kept until you click Clear.';
      optimizerResultsEl.classList.remove('ready', 'loading');
      optimizerResultsEl.style.display = 'block';
    }
    updateRunButtonState();
    return;
  }

  queueRunning = true;
  queueStopRequested = false;
  queueUiLoaded = true;
  await saveQueueRuntimeState(true);
  setQueueControlsDisabled(true);
  updateRunButtonState();

  const optimizerResultsEl = document.getElementById('optimizerResults');
  const progressContainer = document.getElementById('optimizerProgress');
  const optunaProgress = document.getElementById('optunaProgress');
  const optunaProgressFill = document.getElementById('optunaProgressFill');
  const optunaProgressText = document.getElementById('optunaProgressText');
  const optunaBestTrial = document.getElementById('optunaBestTrial');

  const controller = new AbortController();
  window.optimizationAbortController = controller;
  const signal = controller.signal;

  const firstItem = findNextPendingQueueItem(initialQueue) || initialQueue.items[0];
  saveOptimizationState(buildStateForItem(firstItem, 'running'));
  setActiveOptimizationRunId('');
  openResultsPage();

  if (optimizerResultsEl) {
    optimizerResultsEl.textContent = '';
    optimizerResultsEl.classList.add('loading');
    optimizerResultsEl.classList.remove('ready');
    optimizerResultsEl.style.display = 'block';
  }
  if (progressContainer) progressContainer.style.display = 'block';
  if (optunaProgress) optunaProgress.style.display = 'block';

  let fullySucceededItems = 0;
  let partiallySucceededItems = 0;
  let failedItems = 0;
  let createdStudySets = 0;
  let studySetWarnings = 0;
  let aborted = false;
  let lastStudyId = '';
  let lastSummary = null;
  let lastDataPath = '';
  let lastMode = firstItem.mode || 'optuna';
  let lastStrategyId = firstItem.strategyId || '';
  let cancelNotified = false;
  let inFlightRunId = '';
  let stopAfterCurrent = false;

  try {
    while (true) {
      const currentQueue = loadQueue();
      const item = findNextPendingQueueItem(currentQueue);
      if (!item) break;

      if (signal.aborted) {
        aborted = true;
        break;
      }

      lastMode = item.mode || 'optuna';
      lastStrategyId = item.strategyId || '';
      updateOptimizationState(buildStateForItem(item, 'running'));
      setQueueItemState(item.id, 'running');

      const sources = getQueueSources(item);
      const totalSources = sources.length;
      let itemSuccess = Number.isFinite(Number(item.successCount)) ? Math.max(0, Math.floor(Number(item.successCount))) : 0;
      let itemFailure = Number.isFinite(Number(item.failureCount)) ? Math.max(0, Math.floor(Number(item.failureCount))) : 0;
      let studySetState = buildQueueStudySetState(item);

      if (totalSources === 0) {
        setQueueItemState(item.id, 'failed');
        failedItems += 1;
        itemFailure += 1;
        await finalizeQueueItem(item.id, 'failed', {
          sourceCursor: 0,
          successCount: itemSuccess,
          failureCount: itemFailure
        });
        continue;
      }

      const startCursorRaw = Number(item.sourceCursor);
      const startCursor = Number.isFinite(startCursorRaw)
        ? Math.max(0, Math.min(totalSources, Math.floor(startCursorRaw)))
        : 0;
      let processedCursor = startCursor;

      for (let sourceIndex = startCursor; sourceIndex < totalSources; sourceIndex += 1) {
        if (signal.aborted) {
          aborted = true;
          break;
        }
        if (queueStopRequested) {
          stopAfterCurrent = true;
          break;
        }

        const source = sources[sourceIndex];
        const sourceName = buildSourceDisplayLabel(source, sourceIndex);
        const sourceMode = buildSourceModeLabel(source);
        if (optimizerResultsEl) {
          optimizerResultsEl.textContent = (
            'Queue item: ' + item.label + '\n'
            + 'Source ' + (sourceIndex + 1) + '/' + totalSources + ': [' + sourceMode + '] ' + sourceName + ' - processing...'
          );
        }

        if (optunaProgressFill) optunaProgressFill.style.width = '0%';
        if (optunaProgressText) {
          const budgetMode = item.config?.optuna_budget_mode;
          if (budgetMode === 'trials') {
            const trials = item.config?.optuna_n_trials || 500;
            optunaProgressText.textContent = 'Trial: 0 / ' + trials.toLocaleString('en-US') + ' (0%)';
          } else if (budgetMode === 'time') {
            const minutes = Math.round((item.config?.optuna_time_limit || 3600) / 60);
            optunaProgressText.textContent = 'Time budget: ' + minutes + ' min';
          } else {
            optunaProgressText.textContent = 'Running...';
          }
        }
        if (optunaBestTrial) {
          optunaBestTrial.textContent = 'Waiting for first trial...';
        }

        const formData = new FormData();
        formData.append('strategy', item.strategyId);
        formData.append('warmupBars', String(item.warmupBars));
        formData.append('config', JSON.stringify(item.config));
        if (item.dbTarget) {
          formData.append('dbTarget', item.dbTarget);
        }

        const csvPath = String(source.path || '').trim();
        if (!isAbsoluteFilesystemPath(csvPath)) {
          itemFailure += 1;
          processedCursor = sourceIndex + 1;
          await persistItemProgress(item.id, {
            sourceCursor: processedCursor,
            successCount: itemSuccess,
            failureCount: itemFailure
          });
          continue;
        }
        formData.append('csvPath', csvPath);
        inFlightRunId = generateOptimizationRunId('queue');
        formData.append('runId', inFlightRunId);
        setActiveOptimizationRunId(inFlightRunId);

        try {
          let data;
          if (item.mode === 'wfa' && item.wfa) {
            formData.append('wf_is_period_days', String(item.wfa.isPeriodDays));
            formData.append('wf_oos_period_days', String(item.wfa.oosPeriodDays));
            formData.append('wf_store_top_n_trials', String(item.wfa.storeTopNTrials));
            formData.append('wf_adaptive_mode', item.wfa.adaptiveMode ? 'true' : 'false');
            formData.append('wf_max_oos_period_days', String(item.wfa.maxOosPeriodDays));
            formData.append('wf_min_oos_trades', String(item.wfa.minOosTrades));
            formData.append('wf_check_interval_trades', String(item.wfa.checkIntervalTrades));
            formData.append('wf_cusum_threshold', String(item.wfa.cusumThreshold));
            formData.append('wf_dd_threshold_multiplier', String(item.wfa.ddThresholdMultiplier));
            formData.append('wf_inactivity_multiplier', String(item.wfa.inactivityMultiplier));
            data = await runWalkForwardRequest(formData, signal);
          } else {
            data = await runOptimizationRequest(formData, signal);
          }

          itemSuccess += 1;
          const currentStudyId = String(data?.study_id || '').trim();
          if (currentStudyId) {
            studySetState = buildQueueStudySetState(item, {
              ...studySetState,
              configured: true,
              completedStudyIds: uniqueStringValues([...studySetState.completedStudyIds, currentStudyId]),
              lastUpdatedAt: new Date().toISOString(),
            });
            lastStudyId = currentStudyId;
          }
          lastSummary = data && data.summary ? data.summary : lastSummary;
          lastDataPath = data && data.data_path ? data.data_path : (sourceName || lastDataPath);
          inFlightRunId = '';
          setActiveOptimizationRunId('');
        } catch (error) {
          if (error && error.name === 'AbortError') {
            aborted = true;
            break;
          }
          itemFailure += 1;
          console.error('Queue source failed: ' + sourceName, error);
          inFlightRunId = '';
          setActiveOptimizationRunId('');
        }

        processedCursor = sourceIndex + 1;
        const progressPatch = {
          sourceCursor: processedCursor,
          successCount: itemSuccess,
          failureCount: itemFailure
        };
        if (studySetState.completedStudyIds.length || studySetState.status || studySetState.error) {
          progressPatch.studySet = studySetState;
        }
        await persistItemProgress(item.id, progressPatch);

        if (queueStopRequested) {
          stopAfterCurrent = true;
          break;
        }
      }

      if (aborted) {
        setQueueItemState(item.id, 'skipped');
        if (!cancelNotified && inFlightRunId) {
          cancelNotified = true;
          await requestServerCancelBestEffort(inFlightRunId);
        }
        inFlightRunId = '';
        setActiveOptimizationRunId('');
        break;
      }

      if (stopAfterCurrent) {
        const itemFinished = processedCursor >= totalSources;
        if (itemFinished) {
          let finalState = 'failed';
          if (itemSuccess === totalSources) {
            setQueueItemState(item.id, 'completed');
            fullySucceededItems += 1;
            finalState = 'completed';
          } else if (itemSuccess > 0) {
            setQueueItemState(item.id, 'completed');
            partiallySucceededItems += 1;
            finalState = 'completed';
          } else {
            setQueueItemState(item.id, 'failed');
            failedItems += 1;
          }

          if (finalState === 'completed') {
            const studySetResult = await finalizeQueueStudySetIfNeeded(item, studySetState);
            studySetState = studySetResult.studySet;
            if (studySetResult.created) createdStudySets += 1;
            if (studySetResult.warning) studySetWarnings += 1;
          }

          await finalizeQueueItem(item.id, finalState, {
            sourceCursor: processedCursor,
            successCount: itemSuccess,
            failureCount: itemFailure,
            studySet: studySetState
          });
        }
        break;
      }

      let finalState = 'failed';
      if (itemSuccess === totalSources) {
        setQueueItemState(item.id, 'completed');
        fullySucceededItems += 1;
        finalState = 'completed';
      } else if (itemSuccess > 0) {
        setQueueItemState(item.id, 'completed');
        partiallySucceededItems += 1;
        finalState = 'completed';
      } else {
        setQueueItemState(item.id, 'failed');
        failedItems += 1;
      }

      if (finalState === 'completed') {
        const studySetResult = await finalizeQueueStudySetIfNeeded(item, studySetState);
        studySetState = studySetResult.studySet;
        if (studySetResult.created) createdStudySets += 1;
        if (studySetResult.warning) studySetWarnings += 1;
      }

      if (lastStudyId) {
        updateOptimizationState({
          status: 'running',
          mode: lastMode,
          study_id: lastStudyId,
          summary: lastSummary || {},
          dataPath: lastDataPath,
          strategyId: lastStrategyId
        });
      }

      await finalizeQueueItem(item.id, finalState, {
        sourceCursor: processedCursor,
        successCount: itemSuccess,
        failureCount: itemFailure,
        studySet: studySetState
      });
    }

    if (optimizerResultsEl) {
      optimizerResultsEl.classList.remove('loading');
      const processedItems = fullySucceededItems + partiallySucceededItems + failedItems;

      if (aborted || stopAfterCurrent) {
        optimizerResultsEl.textContent = (
          'Queue cancelled. Processed ' + processedItems + ' of ' + totalItems + ' item(s).\n'
          + 'Successful: ' + fullySucceededItems
          + ', Partial: ' + partiallySucceededItems
          + ', Failed: ' + failedItems
          + '. Remaining items stay queued.'
        );
      } else if (failedItems === 0 && partiallySucceededItems === 0) {
        optimizerResultsEl.textContent = 'Queue complete! All ' + totalItems + ' item(s) processed successfully.';
        optimizerResultsEl.classList.add('ready');
      } else {
        optimizerResultsEl.textContent = (
          'Queue finished.\n'
          + 'Successful: ' + fullySucceededItems
          + ', Partial: ' + partiallySucceededItems
          + ', Failed: ' + failedItems + '.'
        );
      }

      const studySetSummary = [];
      if (createdStudySets > 0) {
        studySetSummary.push('Study Sets created: ' + createdStudySets);
      }
      if (studySetWarnings > 0) {
        studySetSummary.push('Set warnings: ' + studySetWarnings);
      }
      if (studySetSummary.length) {
        optimizerResultsEl.textContent += '\n' + studySetSummary.join(', ') + '.';
      }
    }

    if (aborted || stopAfterCurrent) {
      updateOptimizationState({
        status: 'cancelled',
        mode: lastMode,
        run_id: '',
        study_id: lastStudyId,
        summary: lastSummary || {},
        dataPath: lastDataPath,
        strategyId: lastStrategyId
      });
    } else {
      const succeededItems = fullySucceededItems + partiallySucceededItems;
      if (succeededItems > 0) {
        updateOptimizationState({
          status: 'completed',
          mode: lastMode,
          run_id: '',
          study_id: lastStudyId,
          summary: lastSummary || {},
          dataPath: lastDataPath,
          strategyId: lastStrategyId
        });
      } else {
        updateOptimizationState({
          status: 'error',
          mode: lastMode,
          run_id: '',
          study_id: lastStudyId,
          summary: lastSummary || {},
          dataPath: lastDataPath,
          strategyId: lastStrategyId,
          error: 'Queue finished with no successful items.'
        });
      }
    }
  } catch (error) {
    console.error('Queue execution failed unexpectedly', error);
    showQueueError('Queue execution failed: ' + (error?.message || 'Unknown error'));
    updateOptimizationState({
      status: 'error',
      mode: lastMode,
      run_id: '',
      error: error?.message || 'Queue execution failed.'
    });
  } finally {
    queueRunning = false;
    queueStopRequested = false;
    await saveQueueRuntimeState(false);
    window.optimizationAbortController = null;
    setActiveOptimizationRunId('');
    renderQueue();
    setQueueControlsDisabled(false);
    updateRunButtonState();
  }
}

async function initQueue() {
  queueUiLoaded = false;
  bindQueueUiEventsOnce();
  await ensureQueueStateLoaded();
  const runtimeState = loadQueueRuntimeState();
  if (runtimeState.active && hasPersistedQueueItems()) {
    queueUiLoaded = true;
  }
  syncQueueAutoCreateSetUi();
  renderQueue();
  updateRunButtonState();
}


