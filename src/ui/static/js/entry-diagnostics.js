/* Saved selected-execution statistics; no replay or population aggregation. */
(function (root) {
  function valid(value) {
    return value?.version === 1 && typeof value.scope === 'string'
      && Object.prototype.hasOwnProperty.call(value, 'maxEntryLeverageUsed')
      && Number.isSafeInteger(value.leverageCapSkippedEntries) && value.leverageCapSkippedEntries >= 0
      && (value.maxEntryLeverageUsed === null
        || (typeof value.maxEntryLeverageUsed === 'number' && Number.isFinite(value.maxEntryLeverageUsed)
          && value.maxEntryLeverageUsed > 0));
  }
  function format(value) {
    if (!valid(value)) return 'N/A';
    const maximum = value.maxEntryLeverageUsed === null ? '—' : `${value.maxEntryLeverageUsed.toFixed(2)}×`;
    return `${maximum} / ${value.leverageCapSkippedEntries}`;
  }
  function card(value) {
    return `<div class="summary-card entry-diagnostics-card"><div class="value">${format(value)}</div><div class="label">Max Entry Lev / Skipped</div></div>`;
  }
  function aggregate(studies) {
    const unique = new Map(studies.map((study) => [study.study_id, study.entry_diagnostics]));
    if (!unique.size || [...unique.values()].some((value) => !valid(value))) return null;
    const values = [...unique.values()];
    const maxima = values.map((value) => value.maxEntryLeverageUsed).filter((value) => value !== null);
    return { version: 1, scope: 'oos', maxEntryLeverageUsed: maxima.length ? Math.max(...maxima) : null,
      leverageCapSkippedEntries: values.reduce((sum, value) => sum + value.leverageCapSkippedEntries, 0) };
  }
  function show(value, only = false) {
    const container = document.querySelector('.summary-row');
    if (!container) return;
    if (only) container.innerHTML = card(value);
    else {
      container.querySelector('.entry-diagnostics-card')?.remove();
      container.insertAdjacentHTML('beforeend', card(value));
    }
    container.style.display = 'grid';
  }
  root.EntryDiagnostics = { valid, format, card, aggregate, show };
}(typeof window === 'undefined' ? globalThis : window));
