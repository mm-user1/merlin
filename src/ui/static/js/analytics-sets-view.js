(function () {
  const SET_COLOR_OPTIONS = [
    { token: null, label: 'Default' },
    { token: 'blue', label: 'Blue' },
    { token: 'teal', label: 'Teal' },
    { token: 'mint', label: 'Mint' },
    { token: 'olive', label: 'Olive' },
    { token: 'sand', label: 'Sand' },
    { token: 'amber', label: 'Amber' },
    { token: 'rose', label: 'Rose' },
    { token: 'lavender', label: 'Lavender' },
  ];
  const COLOR_TOKEN_SET = new Set(
    SET_COLOR_OPTIONS.map((option) => option.token).filter((token) => token !== null)
  );
  const TOKEN_BOUNDARY_CLASS = 'A-Za-z0-9_-';
  const SORT_META = {
    set_name: { label: 'Set Name', bestDirection: 'asc' },
    ann_profit_pct: { label: 'Ann.P%', bestDirection: 'desc' },
    profit_pct: { label: 'Profit%', bestDirection: 'desc' },
    max_drawdown_pct: { label: 'MaxDD%', bestDirection: 'asc' },
    profitable_pct: { label: 'Profitable', bestDirection: 'desc' },
    wfe_pct: { label: 'WFE%', bestDirection: 'desc' },
    oos_wins_pct: { label: 'OOS Wins', bestDirection: 'desc' },
    consistency: { label: 'Consist', bestDirection: 'desc' },
  };

  const state = {
    sets: [],
    availableColorTokens: [],
    colorFilter: null,
    query: '',
    draftQuery: '',
    queryMatcher: createQueryMatcher(''),
    draftError: null,
    sortState: {
      sortColumn: null,
      sortDirection: null,
      sortClickCount: 0,
    },
    controlsDisabled: false,
    onChange: null,
  };

  function escapeHtml(value) {
    return String(value ?? '')
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#39;');
  }

  function normalizeSetId(raw) {
    const parsed = Number(raw);
    if (!Number.isInteger(parsed) || parsed <= 0) return null;
    return parsed;
  }

  function normalizeColorToken(raw) {
    const token = String(raw || '').trim().toLowerCase();
    if (!token) return null;
    return COLOR_TOKEN_SET.has(token) ? token : null;
  }

  function cloneColorFilter(filter) {
    return filter instanceof Set ? new Set(filter) : null;
  }

  function cloneSortState(sortState) {
    return {
      sortColumn: sortState?.sortColumn || null,
      sortDirection: sortState?.sortDirection || null,
      sortClickCount: Number(sortState?.sortClickCount || 0),
    };
  }

  function colorFiltersEqual(left, right) {
    const leftSet = left instanceof Set ? left : null;
    const rightSet = right instanceof Set ? right : null;
    if (!leftSet && !rightSet) return true;
    if (!leftSet || !rightSet) return false;
    if (leftSet.size !== rightSet.size) return false;
    for (const token of leftSet) {
      if (!rightSet.has(token)) return false;
    }
    return true;
  }

  function cloneSetList(rawSets) {
    if (!Array.isArray(rawSets)) return [];
    return rawSets
      .map((setItem) => {
        const id = normalizeSetId(setItem?.id);
        if (id === null) return null;
        return {
          id,
          name: String(setItem?.name || '').trim(),
          color_token: normalizeColorToken(setItem?.color_token),
        };
      })
      .filter(Boolean);
  }

  function escapeRegExp(value) {
    return String(value || '').replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  }

  function tokenizeQuery(rawQuery) {
    const query = String(rawQuery || '');
    const tokens = [];
    let buffer = '';
    let literalParenBalance = 0;
    let structuralDepth = 0;

    const flushBuffer = () => {
      const term = buffer.trim();
      buffer = '';
      literalParenBalance = 0;
      if (!term) return;
      tokens.push({ type: 'term', value: term });
    };

    for (let index = 0; index < query.length;) {
      const operatorMatch = query.slice(index).match(/^\s+([+\-|])\s+/);
      if (operatorMatch && literalParenBalance === 0) {
        flushBuffer();
        const symbol = operatorMatch[1];
        tokens.push({
          type: symbol === '+' ? 'and' : symbol === '-' ? 'minus' : 'or',
          value: symbol,
        });
        index += operatorMatch[0].length;
        continue;
      }

      const char = query[index];
      if (char === '(') {
        if (!buffer.trim().length) {
          flushBuffer();
          tokens.push({ type: 'lparen', value: char });
          structuralDepth += 1;
        } else {
          buffer += char;
          literalParenBalance += 1;
        }
        index += 1;
        continue;
      }

      if (char === ')') {
        if (literalParenBalance > 0) {
          buffer += char;
          literalParenBalance -= 1;
          index += 1;
          continue;
        }
        if (structuralDepth > 0) {
          flushBuffer();
          tokens.push({ type: 'rparen', value: char });
          structuralDepth -= 1;
          index += 1;
          continue;
        }
      }

      buffer += char;
      index += 1;
    }

    flushBuffer();
    if (structuralDepth > 0) {
      throw new Error('Expected ")" to close group.');
    }
    return tokens;
  }

  function buildUnexpectedTokenError(token) {
    if (!token) return 'Unexpected end of expression.';
    if (token.type === 'term') {
      return `Unexpected term "${token.value}".`;
    }
    if (token.type === 'lparen') return 'Unexpected "(".';
    if (token.type === 'rparen') return 'Unexpected ")".';
    if (token.type === 'and') return 'Expected a term before or after "+".';
    if (token.type === 'minus') return 'Expected a term before or after "-".';
    if (token.type === 'or') return 'Expected a term before or after "|".';
    return 'Invalid filter expression.';
  }

  function createTermMatcher(rawTerm) {
    const term = String(rawTerm || '').trim();
    if (!term) {
      return () => true;
    }

    if (term.includes('*')) {
      const pattern = term
        .split('*')
        .map((segment) => escapeRegExp(segment))
        .join('.*');
      const regex = new RegExp(pattern, 'i');
      return (value) => regex.test(String(value || ''));
    }

    if (/\s/.test(term)) {
      const needle = term.toLowerCase();
      return (value) => String(value || '').toLowerCase().includes(needle);
    }

    const regex = new RegExp(
      `(^|[^${TOKEN_BOUNDARY_CLASS}])${escapeRegExp(term)}(?=$|[^${TOKEN_BOUNDARY_CLASS}])`,
      'i'
    );
    return (value) => regex.test(String(value || ''));
  }

  function parseQueryAst(rawQuery) {
    const query = String(rawQuery || '').trim();
    if (!query) return null;

    const tokens = tokenizeQuery(query);
    if (!tokens.length) return null;
    let position = 0;

    const peek = () => tokens[position] || null;
    const consume = () => {
      const token = tokens[position] || null;
      position += 1;
      return token;
    };

    const parsePrimary = () => {
      const token = peek();
      if (!token) {
        throw new Error('Unexpected end of expression.');
      }
      if (token.type === 'term') {
        consume();
        return { type: 'term', value: token.value };
      }
      if (token.type === 'lparen') {
        consume();
        if (peek()?.type === 'rparen') {
          throw new Error('Empty parentheses are not allowed.');
        }
        const inner = parseOrExpression();
        if (peek()?.type !== 'rparen') {
          throw new Error('Expected ")" to close group.');
        }
        consume();
        return inner;
      }
      throw new Error(buildUnexpectedTokenError(token));
    };

    const parseUnary = () => {
      let negate = false;
      while (peek() && (peek().type === 'and' || peek().type === 'minus')) {
        const token = consume();
        if (token.type === 'minus') negate = !negate;
      }
      let node = parsePrimary();
      if (negate) {
        node = { type: 'not', operand: node };
      }
      return node;
    };

    const parseAndExpression = () => {
      let node = parseUnary();
      while (peek() && (peek().type === 'and' || peek().type === 'minus')) {
        const operator = consume();
        const right = parseUnary();
        node = operator.type === 'and'
          ? { type: 'and', left: node, right }
          : { type: 'and', left: node, right: { type: 'not', operand: right } };
      }
      return node;
    };

    const parseOrExpression = () => {
      let node = parseAndExpression();
      while (peek() && peek().type === 'or') {
        consume();
        const right = parseAndExpression();
        node = { type: 'or', left: node, right };
      }
      return node;
    };

    const ast = parseOrExpression();
    if (position < tokens.length) {
      throw new Error(buildUnexpectedTokenError(peek()));
    }
    return ast;
  }

  function buildAstMatcher(node) {
    if (!node) return () => true;
    if (node.type === 'term') {
      return createTermMatcher(node.value);
    }
    if (node.type === 'not') {
      const operandMatcher = buildAstMatcher(node.operand);
      return (value) => !operandMatcher(value);
    }
    if (node.type === 'and') {
      const leftMatcher = buildAstMatcher(node.left);
      const rightMatcher = buildAstMatcher(node.right);
      return (value) => leftMatcher(value) && rightMatcher(value);
    }
    if (node.type === 'or') {
      const leftMatcher = buildAstMatcher(node.left);
      const rightMatcher = buildAstMatcher(node.right);
      return (value) => leftMatcher(value) || rightMatcher(value);
    }
    return () => true;
  }

  function createQueryMatcher(rawQuery) {
    const ast = parseQueryAst(rawQuery);
    if (!ast) return () => true;
    const matcher = buildAstMatcher(ast);

    return (value) => {
      const target = String(value || '');
      return matcher(target);
    };
  }

  function buildAvailableColorTokens() {
    const used = new Set();
    state.sets.forEach((setItem) => {
      used.add(normalizeColorToken(setItem?.color_token));
    });
    return SET_COLOR_OPTIONS
      .map((option) => option.token)
      .filter((token) => used.has(token));
  }

  function reconcileColorFilter() {
    if (!(state.colorFilter instanceof Set) || !state.availableColorTokens.length) {
      state.colorFilter = null;
      return;
    }

    const selected = Array.from(state.colorFilter)
      .filter((token) => state.availableColorTokens.includes(token));
    if (!selected.length || selected.length === state.availableColorTokens.length) {
      state.colorFilter = null;
      return;
    }
    state.colorFilter = new Set(selected);
  }

  function isColorSelected(token) {
    if (!(state.colorFilter instanceof Set)) return true;
    return state.colorFilter.has(token);
  }

  function matchesColorFilter(setItem) {
    if (!(state.colorFilter instanceof Set)) return true;
    return state.colorFilter.has(normalizeColorToken(setItem?.color_token));
  }

  function matchesTextFilter(setItem) {
    return state.queryMatcher(String(setItem?.name || ''));
  }

  function buildVisibleSetIds() {
    return state.sets
      .filter((setItem) => matchesColorFilter(setItem) && matchesTextFilter(setItem))
      .map((setItem) => setItem.id);
  }

  function hasActiveSort() {
    return Boolean(state.sortState.sortColumn && state.sortState.sortDirection);
  }

  function hasActiveFilters() {
    return (state.colorFilter instanceof Set) || state.query.length > 0;
  }

  function hasClearableState() {
    return (state.colorFilter instanceof Set)
      || state.query.length > 0
      || state.draftQuery.length > 0;
  }

  function hasPendingQueryChanges() {
    return state.draftQuery !== state.query;
  }

  function hasActiveControls() {
    return hasActiveFilters() || hasActiveSort();
  }

  function notifyChange() {
    if (typeof state.onChange !== 'function') return;
    state.onChange({
      visibleSetIds: getVisibleSetIds(),
      hasActiveFilters: hasActiveFilters(),
      hasActiveSort: hasActiveSort(),
      sortState: cloneSortState(state.sortState),
    });
  }

  function cycleSortForColumn(sortKey) {
    if (!SORT_META[sortKey]) return;
    const current = state.sortState;
    if (current.sortColumn !== sortKey || !current.sortColumn) {
      const bestDirection = SORT_META[sortKey].bestDirection || 'desc';
      state.sortState = {
        sortColumn: sortKey,
        sortDirection: bestDirection,
        sortClickCount: 1,
      };
      notifyChange();
      return;
    }

    if (current.sortClickCount === 1) {
      state.sortState = {
        sortColumn: sortKey,
        sortDirection: current.sortDirection === 'asc' ? 'desc' : 'asc',
        sortClickCount: 2,
      };
      notifyChange();
      return;
    }

    state.sortState = {
      sortColumn: null,
      sortDirection: null,
      sortClickCount: 0,
    };
    notifyChange();
  }

  function stopHeaderToggle(event) {
    event.stopPropagation();
  }

  function syncDraftControlsState(input, clearBtn) {
    if (input instanceof HTMLElement) {
      input.classList.toggle('is-dirty', hasPendingQueryChanges());
      input.classList.toggle('is-invalid', Boolean(state.draftError));
      input.setAttribute('aria-invalid', state.draftError ? 'true' : 'false');
    }
    if (clearBtn instanceof HTMLButtonElement) {
      clearBtn.disabled = state.controlsDisabled || !state.sets.length || !hasClearableState();
    }
  }

  function setDraftQuery(nextQueryRaw) {
    const nextQuery = String(nextQueryRaw ?? '');
    if (nextQuery === state.draftQuery) return;
    state.draftQuery = nextQuery;
    state.draftError = null;
  }

  function applyQuery(nextQueryRaw = state.draftQuery) {
    const nextQuery = String(nextQueryRaw ?? '');
    let nextMatcher;
    try {
      nextMatcher = createQueryMatcher(nextQuery);
    } catch (error) {
      state.draftQuery = nextQuery;
      state.draftError = error?.message || 'Invalid filter expression.';
      renderControls();
      return;
    }

    const changed = nextQuery !== state.query;
    state.query = nextQuery;
    state.draftQuery = nextQuery;
    state.queryMatcher = nextMatcher;
    state.draftError = null;
    renderControls();
    if (!changed) return;
    notifyChange();
  }

  function clearAllFilters() {
    const shouldNotify = hasActiveFilters();
    if (!shouldNotify && !state.draftQuery.length) return;
    state.colorFilter = null;
    state.query = '';
    state.draftQuery = '';
    state.queryMatcher = createQueryMatcher('');
    state.draftError = null;
    renderControls();
    if (shouldNotify) {
      notifyChange();
    }
  }

  function toggleColorToken(tokenRaw, onlyThis) {
    if (!state.availableColorTokens.length) return;
    const token = normalizeColorToken(tokenRaw);
    const selected = state.colorFilter instanceof Set
      ? new Set(state.colorFilter)
      : new Set(state.availableColorTokens);

    if (onlyThis) {
      if (selected.size === 1 && selected.has(token)) {
        state.colorFilter = null;
        renderControls();
        notifyChange();
        return;
      }
      selected.clear();
      selected.add(token);
    } else if (selected.has(token)) {
      selected.delete(token);
    } else {
      selected.add(token);
    }

    if (!selected.size || selected.size === state.availableColorTokens.length) {
      state.colorFilter = null;
    } else {
      state.colorFilter = selected;
    }
    renderControls();
    notifyChange();
  }

  function renderColorFilters(disabled) {
    if (!state.availableColorTokens.length) return '';

    const itemsHtml = state.availableColorTokens
      .map((token) => {
        const option = SET_COLOR_OPTIONS.find((entry) => entry.token === token) || { label: 'Default' };
        const isSelected = isColorSelected(token);
        const tokenAttr = token === null ? '' : escapeHtml(token);
        return `
          <button
            type="button"
            class="analytics-sets-filter-swatch${isSelected ? ' is-active' : ' is-inactive'}"
            data-set-filter-color="${tokenAttr}"
            title="${escapeHtml(`${option.label} (${isSelected ? 'included' : 'excluded'}). Click to toggle. Ctrl+Click to show only this color.`)}"
            aria-pressed="${isSelected ? 'true' : 'false'}"
            ${disabled ? 'disabled' : ''}>
            <span class="analytics-set-color-swatch" ${token === null ? '' : `data-color-token="${tokenAttr}"`}></span>
          </button>
        `;
      })
      .join('');

    return `
      <div class="analytics-sets-color-filters" role="group" aria-label="Filter study sets by color">
        ${itemsHtml}
      </div>
    `;
  }

  function renderControls(options = {}) {
    if (Object.prototype.hasOwnProperty.call(options, 'disabled')) {
      state.controlsDisabled = Boolean(options.disabled);
    }

    const container = document.getElementById('analyticsSetsControls');
    if (!container) return;
    const previousInput = document.getElementById('analyticsSetsFilterInput');
    const shouldRestoreFocus = document.activeElement === previousInput;
    const selectionStart = shouldRestoreFocus ? previousInput.selectionStart : null;
    const selectionEnd = shouldRestoreFocus ? previousInput.selectionEnd : null;

    const hasSets = state.sets.length > 0;
    const disabled = state.controlsDisabled || !hasSets;
    const syntaxTitle = [
      'Set name filter syntax:',
      'Plain text: exact token or phrase search',
      '"+" with spaces: include both parts',
      '"-" with spaces: exclude the next part',
      '"|" with spaces: match either branch',
      'Parentheses group sub-expressions',
      '"*" inside term: wildcard',
      'Press Enter to apply the text filter',
      'Examples: Uni_20; Uni_20 + NSGA-2; Uni_20 | Uni_20-Alt; (Uni_20 | Uni_20-Alt) + SQN; Uni_20*; *Uni_20',
      state.draftError ? `Draft error: ${state.draftError}` : '',
    ].join('\n');

    container.innerHTML = `
      <div class="analytics-sets-controls-inner">
        <div class="analytics-sets-filter-search">
          <input
            class="analytics-sets-filter-input"
            id="analyticsSetsFilterInput"
            type="text"
            value="${escapeHtml(state.draftQuery)}"
            placeholder="Filter sets"
            title="${escapeHtml(syntaxTitle)}"
            spellcheck="false"
            aria-invalid="${state.draftError ? 'true' : 'false'}"
            ${disabled ? 'disabled' : ''} />
          <button
            type="button"
            class="sel-btn analytics-sets-filter-clear"
            id="analyticsSetsFilterClear"
            title="Clear set filters"
            ${(disabled || !hasActiveFilters()) ? 'disabled' : ''}>
            Clear
          </button>
        </div>
        ${renderColorFilters(disabled)}
      </div>
    `;

    container.onmousedown = stopHeaderToggle;
    container.onclick = stopHeaderToggle;
    container.onpointerdown = stopHeaderToggle;

    const input = document.getElementById('analyticsSetsFilterInput');
    const clearBtn = document.getElementById('analyticsSetsFilterClear');
    if (input) {
      input.addEventListener('click', stopHeaderToggle);
      input.addEventListener('keydown', (event) => {
        if (event.key === 'Enter') {
          stopHeaderToggle(event);
          event.preventDefault();
          applyQuery(input.value);
          return;
        }
        if (event.key === 'Escape') {
          stopHeaderToggle(event);
          event.preventDefault();
          if (hasPendingQueryChanges()) {
            state.draftQuery = state.query;
            state.draftError = null;
            renderControls();
            return;
          }
          if (state.query.length > 0 || (state.colorFilter instanceof Set)) {
            clearAllFilters();
          }
        }
      });
      input.addEventListener('input', () => {
        setDraftQuery(input.value);
        if (input.classList.contains('is-invalid')) {
          input.classList.remove('is-invalid');
          input.setAttribute('aria-invalid', 'false');
        }
      });
      if (shouldRestoreFocus && !disabled) {
        input.focus({ preventScroll: true });
        if (typeof input.setSelectionRange === 'function' && selectionStart !== null && selectionEnd !== null) {
          input.setSelectionRange(selectionStart, selectionEnd);
        }
      }
    }

    if (clearBtn) {
      clearBtn.addEventListener('click', (event) => {
        event.preventDefault();
        stopHeaderToggle(event);
        clearAllFilters();
      });
    }
    syncDraftControlsState(input, clearBtn);

    container.querySelectorAll('[data-set-filter-color]').forEach((button) => {
      button.addEventListener('click', (event) => {
        event.preventDefault();
        stopHeaderToggle(event);
        toggleColorToken(button.getAttribute('data-set-filter-color'), Boolean(event.ctrlKey));
      });
    });
  }

  function init(options = {}) {
    state.onChange = typeof options.onChange === 'function' ? options.onChange : null;
    state.draftQuery = state.query;
    state.queryMatcher = createQueryMatcher(state.query);
    state.draftError = null;
  }

  function updateSets(sets, options = {}) {
    const previousColorFilter = cloneColorFilter(state.colorFilter);
    state.sets = cloneSetList(sets);
    state.availableColorTokens = buildAvailableColorTokens();
    reconcileColorFilter();
    renderControls();

    if (options.emitChange !== false && !colorFiltersEqual(previousColorFilter, state.colorFilter)) {
      notifyChange();
    }
  }

  function getVisibleSetIds() {
    return buildVisibleSetIds().slice();
  }

  window.AnalyticsSetsView = {
    init,
    updateSets,
    renderControls,
    getVisibleSetIds,
    hasActiveControls,
    hasActiveFilters,
    hasActiveSort,
    getSortState: () => cloneSortState(state.sortState),
    cycleSortForColumn,
  };
})();
