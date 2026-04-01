/**
 * Strategy configuration and form generation.
 * Dependencies: utils.js, api.js
 */

window.currentStrategyId = null;
window.currentStrategyConfig = null;

async function loadStrategiesList() {
  try {
    const data = await fetchStrategies();

    const select = document.getElementById('strategySelect');
    if (!select) {
      return;
    }

    select.innerHTML = '';

    if (!data.strategies || data.strategies.length === 0) {
      select.innerHTML = '<option value="">No strategies found</option>';
      console.error('No strategies discovered');
      return;
    }

    data.strategies.forEach((strategy) => {
      const option = document.createElement('option');
      option.value = strategy.id;
      option.textContent = `${strategy.name} ${strategy.version}`;
      select.appendChild(option);
    });

    if (data.strategies.length > 0) {
      window.currentStrategyId = data.strategies[0].id;
      select.value = window.currentStrategyId;
      await loadStrategyConfig(window.currentStrategyId);
    }
  } catch (error) {
    console.error('Failed to load strategies:', error);
    alert('Error loading strategies. Check console for details.');
  }
}

async function handleStrategyChange() {
  const select = document.getElementById('strategySelect');
  window.currentStrategyId = select?.value || null;

  if (!window.currentStrategyId) {
    return;
  }

  await loadStrategyConfig(window.currentStrategyId);
}

async function loadStrategyConfig(strategyId) {
  try {
    const config = await fetchStrategyConfig(strategyId);
    window.currentStrategyConfig = config;

    try {
      updateStrategyInfo(config);
    } catch (err) {
      console.warn('Failed to update strategy info:', err);
    }

    try {
      generateBacktestForm(config);
    } catch (err) {
      console.error('Failed to generate backtest form:', err);
      alert('Error generating backtest form. Please refresh the page.');
      return;
    }

    try {
      generateOptimizerForm(config);
    } catch (err) {
      console.error('Failed to generate optimizer form:', err);
      alert('Error generating optimizer form. Please refresh the page.');
      return;
    }

    console.log(`Loaded strategy: ${config.name}`);
  } catch (error) {
    console.error('Failed to load strategy config:', error);
    if (!window.currentStrategyConfig || !window.currentStrategyConfig.parameters) {
      alert(`Error loading strategy configuration: ${error.message}\n\nPlease check browser console for details.`);
    } else {
      console.warn('Non-critical error during strategy load, but forms populated successfully');
    }
  }
}

function updateStrategyInfo(config) {
  const info = document.getElementById('strategyInfo');
  if (!info) {
    return;
  }

  document.getElementById('strategyName').textContent = config.name || '';
  document.getElementById('strategyVersion').textContent = config.version || '';
  document.getElementById('strategyDescription').textContent = config.description || 'N/A';
  document.getElementById('strategyParamCount').textContent = Object.keys(config.parameters || {}).length;
  info.style.display = 'block';
}

function generateBacktestForm(config) {
  const container = document.getElementById('backtestParamsContent');
  if (!container) {
    return;
  }

  container.innerHTML = '';

  const params = config.parameters || {};
  const groups = {};

  for (const [paramName, paramDef] of Object.entries(params)) {
    const group = paramDef.group || 'Other';
    if (!groups[group]) {
      groups[group] = [];
    }
    groups[group].push({ name: paramName, def: paramDef });
  }

  for (const [groupName, groupParams] of Object.entries(groups)) {
    const groupDiv = document.createElement('div');
    groupDiv.className = 'param-group';
    groupDiv.style.marginBottom = '25px';
    groupDiv.style.flexDirection = 'column';
    groupDiv.style.alignItems = 'flex-start';

    const groupTitle = document.createElement('h4');
    groupTitle.textContent = groupName;
    groupTitle.style.color = '#4a90e2';
    groupTitle.style.marginBottom = '15px';
    groupDiv.appendChild(groupTitle);

    groupParams.forEach(({ name, def }) => {
      const formGroup = createFormField(name, def, 'backtest');
      groupDiv.appendChild(formGroup);
    });

    container.appendChild(groupDiv);
  }
}

function generateOptimizerForm(config) {
  const container = document.getElementById('optimizerParamsContainer');
  if (!container) {
    console.error('Optimizer container not found (#optimizerParamsContainer)');
    return;
  }

  container.innerHTML = '';

  const params = config.parameters || {};
  const groups = {};

  for (const [paramName, paramDef] of Object.entries(params)) {
    if (paramDef.optimize && paramDef.optimize.enabled) {
      const group = paramDef.group || 'Other';
      if (!groups[group]) {
        groups[group] = [];
      }
      groups[group].push({ name: paramName, def: paramDef });
    }
  }

  const totalParams = Object.values(groups).reduce((sum, g) => sum + g.length, 0);
  if (totalParams === 0) {
    container.innerHTML = '<p class="warning">No optimizable parameters defined for this strategy.</p>';
    if (typeof window.bindOptimizerInputs === 'function') {
      window.bindOptimizerInputs();
    }
    return;
  }

  for (const [groupName, groupParams] of Object.entries(groups)) {
    const groupDiv = document.createElement('div');
    groupDiv.className = 'opt-section';

    const groupTitle = document.createElement('div');
    groupTitle.className = 'opt-section-title';
    groupTitle.textContent = groupName;
    groupDiv.appendChild(groupTitle);

    groupParams.forEach(({ name, def }) => {
      const row = createOptimizerRow(name, def);
      groupDiv.appendChild(row);
    });

    container.appendChild(groupDiv);
  }

  if (typeof window.bindOptimizerInputs === 'function') {
    window.bindOptimizerInputs();
  }

  console.log(`Generated optimizer form with ${totalParams} parameters`);
}

function createOptimizerRow(paramName, paramDef) {
  const row = document.createElement('div');
  row.className = 'opt-row';

  const checkbox = document.createElement('input');
  checkbox.type = 'checkbox';
  checkbox.id = `opt-${paramName}`;
  checkbox.checked = Boolean(paramDef.optimize && paramDef.optimize.enabled);
  checkbox.dataset.paramName = paramName;
  checkbox.classList.add('opt-param-toggle');

  const label = document.createElement('label');
  label.className = 'opt-label';
  label.htmlFor = `opt-${paramName}`;
  label.textContent = paramDef.label || paramName;

  const controlsDiv = document.createElement('div');
  controlsDiv.className = 'opt-controls';

  const paramType = paramDef.type || 'float';

  if (paramType === 'select' || paramType === 'options') {
    const optionsContainer = createSelectOptions(paramName, paramDef);
    controlsDiv.appendChild(optionsContainer);
  } else if (paramType === 'bool' || paramType === 'boolean') {
    const optionsContainer = createSelectOptions(paramName, {
      ...paramDef,
      options: [true, false],
      selectAllByDefault: true
    });
    controlsDiv.appendChild(optionsContainer);
  } else {
    const isInt = paramType === 'int' || paramType === 'integer';
    const defaultStep = isInt ? 1 : 0.1;
    const minStep = isInt ? 1 : 0.01;

    const fromLabel = document.createElement('label');
    fromLabel.textContent = 'From:';
    const fromInput = document.createElement('input');
    fromInput.className = 'tiny-input';
    fromInput.id = `opt-${paramName}-from`;
    fromInput.type = 'number';
    fromInput.value = paramDef.optimize?.min ?? paramDef.min ?? 0;
    fromInput.step = paramDef.optimize?.step || paramDef.step || defaultStep;
    fromInput.dataset.paramName = paramName;

    const toLabel = document.createElement('label');
    toLabel.textContent = 'To:';
    const toInput = document.createElement('input');
    toInput.className = 'tiny-input';
    toInput.id = `opt-${paramName}-to`;
    toInput.type = 'number';
    toInput.value = paramDef.optimize?.max ?? paramDef.max ?? 100;
    toInput.step = paramDef.optimize?.step || paramDef.step || defaultStep;
    toInput.dataset.paramName = paramName;

    const stepLabel = document.createElement('label');
    stepLabel.textContent = 'Step:';
    const stepInput = document.createElement('input');
    stepInput.className = 'tiny-input';
    stepInput.id = `opt-${paramName}-step`;
    stepInput.type = 'number';
    stepInput.value = paramDef.optimize?.step || paramDef.step || defaultStep;
    stepInput.step = minStep;
    stepInput.min = minStep;
    stepInput.dataset.paramName = paramName;

    controlsDiv.appendChild(fromLabel);
    controlsDiv.appendChild(fromInput);
    controlsDiv.appendChild(toLabel);
    controlsDiv.appendChild(toInput);
    controlsDiv.appendChild(stepLabel);
    controlsDiv.appendChild(stepInput);
  }

  row.appendChild(checkbox);
  row.appendChild(label);
  row.appendChild(controlsDiv);

  return row;
}

function createSelectOptions(paramName, paramDef) {
  const container = document.createElement('div');
  container.className = 'select-options-container';
  container.dataset.paramName = paramName;
  const allByDefaultParams = new Set(['maType', 'trailMaType', 'maType3']);
  const selectAllByDefault = allByDefaultParams.has(paramName) || paramDef.selectAllByDefault === true;

  const options = paramDef.options || [];

  if (options.length === 0) {
    const warning = document.createElement('span');
    warning.className = 'warning-text';
    warning.textContent = 'No options defined for this parameter';
    container.appendChild(warning);
    return container;
  }

  const allCheckboxWrapper = document.createElement('label');
  allCheckboxWrapper.className = 'select-option-label all-option';
  allCheckboxWrapper.style.fontWeight = 'bold';

  const allCheckbox = document.createElement('input');
  allCheckbox.type = 'checkbox';
  allCheckbox.className = 'select-option-checkbox';
  allCheckbox.dataset.paramName = paramName;
  allCheckbox.dataset.optionValue = '__ALL__';
  allCheckbox.id = `opt-${paramName}-all`;

  const allLabel = document.createElement('span');
  allLabel.textContent = 'All';

  allCheckboxWrapper.appendChild(allCheckbox);
  allCheckboxWrapper.appendChild(allLabel);
  container.appendChild(allCheckboxWrapper);

  options.forEach((optionValue) => {
    const optionWrapper = document.createElement('label');
    optionWrapper.className = 'select-option-label';

    const optionCheckbox = document.createElement('input');
    optionCheckbox.type = 'checkbox';
    optionCheckbox.className = 'select-option-checkbox';
    optionCheckbox.dataset.paramName = paramName;
    optionCheckbox.dataset.optionValue = optionValue;
    optionCheckbox.id = `opt-${paramName}-${optionValue}`;

    if (selectAllByDefault || optionValue === paramDef.default) {
      optionCheckbox.checked = true;
    }

    const optionLabel = document.createElement('span');
    if (typeof optionValue === 'boolean') {
      optionLabel.textContent = optionValue ? 'True' : 'False';
    } else {
      optionLabel.textContent = optionValue;
    }

    optionWrapper.appendChild(optionCheckbox);
    optionWrapper.appendChild(optionLabel);
    container.appendChild(optionWrapper);
  });

  allCheckbox.addEventListener('change', () => {
    const individualCheckboxes = container.querySelectorAll(
      `input.select-option-checkbox[data-param-name="${paramName}"]:not([data-option-value="__ALL__"])`
    );
    individualCheckboxes.forEach((cb) => {
      cb.checked = allCheckbox.checked;
    });
  });

  const individualCheckboxes = container.querySelectorAll(
    `input.select-option-checkbox[data-param-name="${paramName}"]:not([data-option-value="__ALL__"])`
  );
  const areAllSelectedByDefault = Array.from(individualCheckboxes).every((checkbox) => checkbox.checked);
  allCheckbox.checked = areAllSelectedByDefault;
  individualCheckboxes.forEach((cb) => {
    cb.addEventListener('change', () => {
      const allChecked = Array.from(individualCheckboxes).every((checkbox) => checkbox.checked);
      allCheckbox.checked = allChecked;
    });
  });

  return container;
}

function getOptimizerParamElements() {
  const params = window.currentStrategyConfig?.parameters || {};
  const checkboxes = document.querySelectorAll('.opt-param-toggle');

  return Array.from(checkboxes).map((checkbox) => {
    const paramName = checkbox.dataset.paramName || checkbox.id.replace(/^opt-/, '');
    return {
      name: paramName,
      checkbox,
      fromInput: document.getElementById(`opt-${paramName}-from`),
      toInput: document.getElementById(`opt-${paramName}-to`),
      stepInput: document.getElementById(`opt-${paramName}-step`),
      def: params[paramName] || {}
    };
  });
}

function createFormField(paramName, paramDef, prefix) {
  const formGroup = document.createElement('div');
  formGroup.className = 'form-group';
  formGroup.style.marginBottom = '15px';

  const label = document.createElement('label');
  label.textContent = paramDef.label || paramName;
  label.style.display = 'inline-block';
  label.style.width = '200px';
  formGroup.appendChild(label);

  let input;

  if (paramDef.type === 'select') {
    input = document.createElement('select');
    input.id = `${prefix}_${paramName}`;
    input.name = paramName;
    input.style.padding = '5px';
    input.style.minWidth = '150px';

    (paramDef.options || []).forEach((option) => {
      const opt = document.createElement('option');
      opt.value = option;
      opt.textContent = option;
      if (option === paramDef.default) {
        opt.selected = true;
      }
      input.appendChild(opt);
    });
  } else if (paramDef.type === 'int' || paramDef.type === 'float') {
    input = document.createElement('input');
    input.type = 'number';
    input.id = `${prefix}_${paramName}`;
    input.name = paramName;
    input.value = paramDef.default ?? 0;
    input.min = paramDef.min !== undefined ? paramDef.min : '';
    input.max = paramDef.max !== undefined ? paramDef.max : '';
    input.step = paramDef.step || (paramDef.type === 'int' ? 1 : 0.1);
    input.style.padding = '5px';
    input.style.width = '120px';
  } else if (paramDef.type === 'bool') {
    input = document.createElement('input');
    input.type = 'checkbox';
    input.id = `${prefix}_${paramName}`;
    input.name = paramName;
    input.checked = paramDef.default || false;
  }

  if (input) {
    formGroup.appendChild(input);
  }

  return formGroup;
}
