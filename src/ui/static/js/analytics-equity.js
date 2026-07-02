(function () {
  const CHART_VIEWBOX_WIDTH = 800;
  const CHART_VIEWBOX_HEIGHT = 260;
  const RETURN_PROFILE_BOX_X = 10;
  const RETURN_PROFILE_BOX_Y = 10;
  const RETURN_PROFILE_BOX_HEIGHT = 76;
  const RETURN_PROFILE_MAX_STEMS = 60;
  const RETURN_PROFILE_STEM_WIDTH = 3;
  const RETURN_PROFILE_STEM_GAP = 4;
  const RETURN_PROFILE_LOSS_CAP = 100;
  const DEFAULT_FOCUSED_STROKE_WIDTH = 2;
  const Y_AXIS_LABEL_TARGET_COUNT = 5;
  const Y_AXIS_LABEL_MIN_COUNT = 4;
  const Y_AXIS_LABEL_MAX_COUNT = 6;
  const Y_AXIS_LABEL_RIGHT_PADDING = 4;
  const Y_AXIS_LABEL_FONT_SIZE = 10;
  const Y_AXIS_LABEL_COLOR = '#888';
  const PERCENT_TICK_MULTIPLIERS = [1, 2, 2.5, 5, 10];

  function toFiniteNumber(value) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }

  function parseTimestamp(value) {
    const parsed = Date.parse(value);
    return Number.isFinite(parsed) ? parsed : null;
  }

  function roundToDecimals(value, decimals) {
    const precision = Math.max(0, Math.min(6, Math.round(Number(decimals) || 0)));
    const factor = 10 ** precision;
    return Math.round(value * factor) / factor;
  }

  function getNiceTickStep(rawStep) {
    const normalizedRawStep = toFiniteNumber(rawStep);
    if (normalizedRawStep === null || normalizedRawStep <= 0) return 1;

    const exponent = Math.floor(Math.log10(normalizedRawStep));
    const magnitude = 10 ** exponent;
    const normalized = normalizedRawStep / magnitude;

    for (const multiplier of PERCENT_TICK_MULTIPLIERS) {
      if (normalized <= multiplier) {
        return multiplier * magnitude;
      }
    }

    return 10 * magnitude;
  }

  function getTickStepDecimals(step) {
    const normalizedStep = toFiniteNumber(step);
    if (normalizedStep === null || normalizedStep <= 0) return 0;

    const exponent = Math.floor(Math.log10(normalizedStep));
    const magnitude = 10 ** exponent;
    const normalized = normalizedStep / magnitude;
    const hasFractionalMultiplier = Math.abs(normalized - Math.round(normalized)) > 1e-9;
    return Math.max(0, -exponent + (hasFractionalMultiplier ? 1 : 0));
  }

  function buildTickValuesWithinRange(minValue, maxValue, step) {
    const low = toFiniteNumber(minValue);
    const high = toFiniteNumber(maxValue);
    const normalizedStep = toFiniteNumber(step);
    if (low === null || high === null || normalizedStep === null || normalizedStep <= 0) {
      return [];
    }

    const min = Math.min(low, high);
    const max = Math.max(low, high);
    const epsilon = normalizedStep * 1e-6;
    const decimals = getTickStepDecimals(normalizedStep);
    const start = roundToDecimals(
      Math.ceil((min - epsilon) / normalizedStep) * normalizedStep,
      decimals + 2
    );
    const end = roundToDecimals(
      Math.floor((max + epsilon) / normalizedStep) * normalizedStep,
      decimals + 2
    );

    if (start > end + epsilon) {
      return [];
    }

    const rawCount = ((end - start) / normalizedStep) + epsilon;
    const tickCount = Math.max(1, Math.floor(rawCount) + 1);
    return Array.from({ length: tickCount }, (_, index) => (
      roundToDecimals(start + (normalizedStep * index), decimals + 2)
    )).filter((value, index, values) => (
      value >= min - epsilon
      && value <= max + epsilon
      && (index === 0 || Math.abs(value - values[index - 1]) > epsilon)
    ));
  }

  function buildPercentTicks(minPercent, maxPercent) {
    const low = toFiniteNumber(minPercent);
    const high = toFiniteNumber(maxPercent);
    if (low === null || high === null) {
      return { step: 1, decimals: 0, values: [] };
    }

    const min = Math.min(low, high);
    const max = Math.max(low, high);
    const range = max - min;
    if (range <= 1e-9) {
      return {
        step: 1,
        decimals: 0,
        values: [roundToDecimals(min, 2)],
      };
    }

    const rawTargetStep = range / Math.max(1, Y_AXIS_LABEL_TARGET_COUNT - 1);
    const baselineStep = getNiceTickStep(rawTargetStep);
    const candidateSteps = new Set();

    for (let exponentOffset = -2; exponentOffset <= 2; exponentOffset += 1) {
      const step = baselineStep * (10 ** exponentOffset);
      if (step <= 0) continue;
      candidateSteps.add(step);
      candidateSteps.add(step / 2);
      candidateSteps.add(step * 2);
    }

    let bestCandidate = null;
    Array.from(candidateSteps)
      .filter((step) => Number.isFinite(step) && step > 0)
      .sort((left, right) => left - right)
      .forEach((step) => {
        const values = buildTickValuesWithinRange(min, max, step);
        if (!values.length) return;

        const tickCount = values.length;
        const isPreferredCount = tickCount >= Y_AXIS_LABEL_MIN_COUNT && tickCount <= Y_AXIS_LABEL_MAX_COUNT;
        const score = (isPreferredCount ? 0 : 100)
          + (Math.abs(tickCount - Y_AXIS_LABEL_TARGET_COUNT) * 10)
          + Math.abs(Math.log(step / rawTargetStep));

        if (!bestCandidate || score < bestCandidate.score) {
          bestCandidate = {
            step,
            score,
            values,
          };
        }
      });

    const step = bestCandidate?.step || baselineStep;
    return {
      step,
      decimals: getTickStepDecimals(step),
      values: bestCandidate?.values || buildTickValuesWithinRange(min, max, step),
    };
  }

  function formatPercentTickLabel(value, decimals) {
    const normalizedValue = Math.abs(Number(value) || 0) < 1e-9 ? 0 : Number(value);
    const safeDecimals = Math.max(0, Math.min(3, Math.round(Number(decimals) || 0)));
    return `${normalizedValue.toFixed(safeDecimals)}%`;
  }

  function renderPercentAxisLabels(svg, toY, minValue, maxValue) {
    if (!svg || typeof toY !== 'function') return;

    const minPercent = minValue - 100;
    const maxPercent = maxValue - 100;
    const ticks = buildPercentTicks(minPercent, maxPercent);
    if (!Array.isArray(ticks.values) || !ticks.values.length) return;

    ticks.values.forEach((percentValue) => {
      const value = 100 + percentValue;
      const y = toY(value);

      const label = document.createElementNS('http://www.w3.org/2000/svg', 'text');
      label.setAttribute('x', String(CHART_VIEWBOX_WIDTH - Y_AXIS_LABEL_RIGHT_PADDING));
      label.setAttribute('y', String(y));
      label.setAttribute('font-size', String(Y_AXIS_LABEL_FONT_SIZE));
      label.setAttribute('fill', Y_AXIS_LABEL_COLOR);
      label.setAttribute('text-anchor', 'end');
      label.setAttribute('dominant-baseline', 'middle');
      label.textContent = formatPercentTickLabel(percentValue, ticks.decimals);
      svg.appendChild(label);
    });
  }

  function normalizeReturnProfile(profile) {
    if (!profile || typeof profile !== 'object') return null;
    const stems = Array.isArray(profile.stems)
      ? profile.stems
        .map((value) => toFiniteNumber(value))
        .filter((value) => value !== null)
      : [];
    if (stems.length < 2) return null;
    return {
      stems,
      sourceCount: Math.max(0, Math.round(toFiniteNumber(profile.source_count) || stems.length)),
      displayCount: Math.max(0, Math.round(toFiniteNumber(profile.display_count) || stems.length)),
      isBinned: Boolean(profile.is_binned),
    };
  }

  function getRenderedChartScale(svg) {
    const bounds = typeof svg?.getBoundingClientRect === 'function'
      ? svg.getBoundingClientRect()
      : null;
    const width = Number(bounds?.width);
    const height = Number(bounds?.height);
    return {
      x: Number.isFinite(width) && width > 0 ? width / CHART_VIEWBOX_WIDTH : 1,
      y: Number.isFinite(height) && height > 0 ? height / CHART_VIEWBOX_HEIGHT : 1,
    };
  }

  function alignStrokeCenterPx(valuePx, strokeWidthPx) {
    if (strokeWidthPx % 2 === 0) {
      return Math.round(valuePx);
    }
    return Math.round(valuePx - 0.5) + 0.5;
  }

  function alignStrokeCenterView(value, scale, strokeWidthPx) {
    return alignStrokeCenterPx(value * scale, strokeWidthPx) / scale;
  }

  function buildReturnProfileStemLayout(svg, stemCount) {
    if (!Number.isInteger(stemCount) || stemCount <= 0) return null;

    const stemWidthPx = RETURN_PROFILE_STEM_WIDTH;
    const scale = getRenderedChartScale(svg);
    const boxLeftPx = Math.round(RETURN_PROFILE_BOX_X * scale.x);
    const innerPaddingPx = Math.max(1, Math.round(scale.x));
    if (stemCount === 1) {
      const centerPx = alignStrokeCenterPx(boxLeftPx + innerPaddingPx + (stemWidthPx / 2), stemWidthPx);
      const firstStemLeftPx = centerPx - (stemWidthPx / 2);
      const lastStemRightPx = centerPx + (stemWidthPx / 2);
      const boxWidthPx = stemWidthPx + (innerPaddingPx * 2);
      return {
        positions: [centerPx / scale.x],
        stemWidthPx,
        boxX: boxLeftPx / scale.x,
        boxWidth: boxWidthPx / scale.x,
        firstStemLeftX: firstStemLeftPx / scale.x,
        lastStemRightX: lastStemRightPx / scale.x,
        zeroY: alignStrokeCenterView(RETURN_PROFILE_BOX_Y + (RETURN_PROFILE_BOX_HEIGHT / 2), scale.y, 1),
      };
    }

    const gapPx = RETURN_PROFILE_STEM_GAP;
    const stepPx = gapPx + stemWidthPx;
    const contentWidthPx = (stepPx * Math.max(stemCount - 1, 0)) + stemWidthPx;
    const startLeftPx = boxLeftPx + innerPaddingPx;
    const firstCenterPx = alignStrokeCenterPx(startLeftPx + (stemWidthPx / 2), stemWidthPx);
    const firstStemLeftPx = firstCenterPx - (stemWidthPx / 2);
    const positions = Array.from({ length: stemCount }, (_, index) => (firstCenterPx + (stepPx * index)) / scale.x);
    const dynamicBoxWidthPx = contentWidthPx + (innerPaddingPx * 2);
    return {
      positions,
      stemWidthPx,
      boxX: boxLeftPx / scale.x,
      boxWidth: dynamicBoxWidthPx / scale.x,
      firstStemLeftX: firstStemLeftPx / scale.x,
      lastStemRightX: (firstStemLeftPx + contentWidthPx) / scale.x,
      zeroY: alignStrokeCenterView(RETURN_PROFILE_BOX_Y + (RETURN_PROFILE_BOX_HEIGHT / 2), scale.y, 1),
    };
  }

  function renderReturnProfile(svg, profile) {
    if (!svg) return;
    const normalized = normalizeReturnProfile(profile);
    if (!normalized) return;

    const stems = normalized.stems.slice().reverse();
    const layout = buildReturnProfileStemLayout(svg, stems.length);
    if (!layout) return;

    const {
      positions,
      stemWidthPx,
      boxX,
      boxWidth,
      firstStemLeftX,
      lastStemRightX,
      zeroY,
    } = layout;
    const positiveMax = Math.max(0, ...stems.filter((value) => value > 0));
    const topEdgeY = RETURN_PROFILE_BOX_Y;
    const bottomEdgeY = RETURN_PROFILE_BOX_Y + RETURN_PROFILE_BOX_HEIGHT;
    const positiveExtent = zeroY - topEdgeY;
    const negativeExtent = bottomEdgeY - zeroY;

    const group = document.createElementNS('http://www.w3.org/2000/svg', 'g');
    group.setAttribute('data-return-profile', 'true');

    const area = document.createElementNS('http://www.w3.org/2000/svg', 'rect');
    area.setAttribute('x', String(boxX));
    area.setAttribute('y', String(RETURN_PROFILE_BOX_Y));
    area.setAttribute('width', String(boxWidth));
    area.setAttribute('height', String(RETURN_PROFILE_BOX_HEIGHT));
    area.setAttribute('fill', 'rgba(255, 255, 255, 0.32)');
    area.setAttribute('stroke', 'rgba(120, 120, 120, 0.14)');
    area.setAttribute('stroke-width', '1');
    group.appendChild(area);

    const zeroLine = document.createElementNS('http://www.w3.org/2000/svg', 'line');
    zeroLine.setAttribute('x1', String(firstStemLeftX));
    zeroLine.setAttribute('y1', String(zeroY));
    zeroLine.setAttribute('x2', String(lastStemRightX));
    zeroLine.setAttribute('y2', String(zeroY));
    zeroLine.setAttribute('stroke', 'rgba(92, 92, 92, 0.34)');
    zeroLine.setAttribute('stroke-width', '1');
    zeroLine.setAttribute('vector-effect', 'non-scaling-stroke');
    zeroLine.setAttribute('stroke-linecap', 'butt');
    zeroLine.setAttribute('shape-rendering', 'crispEdges');
    group.appendChild(zeroLine);

    stems.forEach((value, index) => {
      let scaled = 0;
      if (value > 0 && positiveMax > 0) {
        scaled = value / positiveMax;
      } else if (value < 0) {
        scaled = -Math.min(Math.abs(value), RETURN_PROFILE_LOSS_CAP) / RETURN_PROFILE_LOSS_CAP;
      }
      if (scaled === 0) return;

      const x = positions[index];
      const y2 = scaled > 0
        ? zeroY - (positiveExtent * scaled)
        : zeroY + (negativeExtent * Math.abs(scaled));
      const stem = document.createElementNS('http://www.w3.org/2000/svg', 'line');
      stem.setAttribute('x1', String(x));
      stem.setAttribute('y1', String(zeroY));
      stem.setAttribute('x2', String(x));
      stem.setAttribute('y2', String(y2));
      stem.setAttribute('stroke', 'rgba(0, 0, 0, 0.2)');
      stem.setAttribute('stroke-width', String(stemWidthPx));
      stem.setAttribute('vector-effect', 'non-scaling-stroke');
      stem.setAttribute('stroke-linecap', 'butt');
      stem.setAttribute('shape-rendering', 'crispEdges');
      group.appendChild(stem);
    });

    svg.appendChild(group);
  }

  function renderEmpty(message) {
    const svg = document.getElementById('analyticsChartSvg');
    const axis = document.getElementById('analyticsEquityAxis');
    if (!svg) return;

    svg.innerHTML = '';
    const bg = document.createElementNS('http://www.w3.org/2000/svg', 'rect');
    bg.setAttribute('width', '100%');
    bg.setAttribute('height', '100%');
    bg.setAttribute('fill', '#fafafa');
    svg.appendChild(bg);

    const text = document.createElementNS('http://www.w3.org/2000/svg', 'text');
    text.setAttribute('x', '400');
    text.setAttribute('y', '130');
    text.setAttribute('text-anchor', 'middle');
    text.setAttribute('fill', '#999');
    text.setAttribute('font-size', '14');
    text.textContent = message || 'No data to display';
    svg.appendChild(text);

    if (axis) {
      axis.innerHTML = '';
    }
  }

  function normalizeSeriesList(seriesList) {
    if (!Array.isArray(seriesList)) return [];

    return seriesList
      .map((series) => {
        const curve = Array.isArray(series?.curve)
          ? series.curve.map((value) => Number(value)).filter((value) => Number.isFinite(value))
          : [];
        if (!curve.length) return null;

        const timestamps = Array.isArray(series?.timestamps) ? series.timestamps.slice() : [];
        const hasMatchingTimestamps = timestamps.length === curve.length;
        const parsedTimestamps = hasMatchingTimestamps
          ? timestamps.map((value) => parseTimestamp(value))
          : [];

        const color = String(series?.color || '').trim() || '#4a90e2';
        const strokeWidth = Number(series?.strokeWidth);
        return {
          curve,
          timestamps: hasMatchingTimestamps ? timestamps : [],
          parsedTimestamps: hasMatchingTimestamps ? parsedTimestamps : [],
          hasTimestamps: hasMatchingTimestamps,
          color,
          strokeWidth: Number.isFinite(strokeWidth) && strokeWidth > 0 ? strokeWidth : 1.5,
        };
      })
      .filter(Boolean);
  }

  function renderSeriesChart(seriesList, options = null) {
    const svg = document.getElementById('analyticsChartSvg');
    const axis = document.getElementById('analyticsEquityAxis');
    if (!svg) return;

    const normalizedSeries = normalizeSeriesList(seriesList);
    if (!normalizedSeries.length) {
      renderEmpty('No data to display');
      return;
    }

    const primarySeries = normalizedSeries[normalizedSeries.length - 1];
    const width = CHART_VIEWBOX_WIDTH;
    const height = CHART_VIEWBOX_HEIGHT;
    const padding = 20;
    const fallbackLength = normalizedSeries.reduce(
      (maxLength, series) => Math.max(maxLength, series.curve.length),
      0
    );

    let tStart = null;
    let tEnd = null;
    normalizedSeries.forEach((series) => {
      if (!series.hasTimestamps) return;
      const firstValid = series.parsedTimestamps.find((value) => value !== null);
      const lastValid = series.parsedTimestamps.length
        ? series.parsedTimestamps[series.parsedTimestamps.length - 1]
        : null;
      if (firstValid === null || lastValid === null || lastValid <= firstValid) return;
      tStart = tStart === null ? firstValid : Math.min(tStart, firstValid);
      tEnd = tEnd === null ? lastValid : Math.max(tEnd, lastValid);
    });

    const useTimeScale = tStart !== null && tEnd !== null && tEnd > tStart;

    const toXRatioByIndex = (index, length) => {
      const seriesLength = Math.max(1, Number(length) || fallbackLength || 1);
      const denom = Math.max(1, seriesLength - 1);
      return index / denom;
    };

    const toSeriesXRatio = (series, index) => {
      if (!useTimeScale || !series?.hasTimestamps) {
        return toXRatioByIndex(index, series?.curve?.length || fallbackLength);
      }

      const ts = series.parsedTimestamps[index];
      if (ts === null) {
        return toXRatioByIndex(index, series.curve.length);
      }
      const ratio = (ts - tStart) / (tEnd - tStart);
      return Math.min(1, Math.max(0, ratio));
    };

    const toXRatioByBoundary = (boundary) => {
      if (!boundary || typeof boundary !== 'object') return null;
      if (useTimeScale) {
        const boundaryTime = parseTimestamp(boundary.time || boundary.timestamp || boundary.date);
        if (boundaryTime !== null) {
          const ratio = (boundaryTime - tStart) / (tEnd - tStart);
          return Math.min(1, Math.max(0, ratio));
        }
      }
      const boundaryIndex = Number(boundary.index);
      if (!Number.isFinite(boundaryIndex)) return null;
      const normalizedIndex = Math.max(
        0,
        Math.min((primarySeries?.curve?.length || fallbackLength || 1) - 1, Math.round(boundaryIndex))
      );
      return toXRatioByIndex(normalizedIndex, primarySeries?.curve?.length || fallbackLength);
    };

    const baseValue = 100.0;
    const allValues = normalizedSeries.flatMap((series) => series.curve);
    const minValue = Math.min(baseValue, ...allValues);
    const maxValue = Math.max(baseValue, ...allValues);
    const valueRange = maxValue - minValue || 1;
    const toY = (value) => {
      return height - padding - ((value - minValue) / valueRange) * (height - padding * 2);
    };

    svg.innerHTML = '';
    if (axis) axis.innerHTML = '';

    const bg = document.createElementNS('http://www.w3.org/2000/svg', 'rect');
    bg.setAttribute('width', '100%');
    bg.setAttribute('height', '100%');
    bg.setAttribute('fill', '#fafafa');
    svg.appendChild(bg);

    const baseY = toY(baseValue);
    const baseLine = document.createElementNS('http://www.w3.org/2000/svg', 'line');
    baseLine.setAttribute('x1', '0');
    baseLine.setAttribute('y1', String(baseY));
    baseLine.setAttribute('x2', String(width));
    baseLine.setAttribute('y2', String(baseY));
    baseLine.setAttribute('stroke', '#c8c8c8');
    baseLine.setAttribute('stroke-width', '1');
    baseLine.setAttribute('stroke-dasharray', '3 4');
    svg.appendChild(baseLine);

    const windowBoundaries = Array.isArray(options?.windowBoundaries) ? options.windowBoundaries : [];
    if (windowBoundaries.length) {
      const boundariesWithX = windowBoundaries
        .map((boundary, index) => {
          const ratio = toXRatioByBoundary(boundary);
          if (ratio === null) return null;
          const x = ratio * width;
          const fallbackLabel = Number.isFinite(Number(boundary?.window_number))
            ? `W${Math.max(1, Math.round(Number(boundary.window_number)))}`
            : `W${index + 1}`;
          const label = String(boundary?.label || '').trim() || fallbackLabel;
          return { x, label };
        })
        .filter(Boolean);

      boundariesWithX.forEach((item, index) => {
        const line = document.createElementNS('http://www.w3.org/2000/svg', 'line');
        line.setAttribute('x1', String(item.x));
        line.setAttribute('y1', '0');
        line.setAttribute('x2', String(item.x));
        line.setAttribute('y2', String(height));
        line.setAttribute('stroke', '#a9c9ff');
        line.setAttribute('stroke-width', '2');
        line.setAttribute('stroke-dasharray', '6 4');
        svg.appendChild(line);

        const nextX = boundariesWithX[index + 1]?.x;
        const labelX = Number.isFinite(nextX) ? (item.x + nextX) / 2 : (item.x + 40);
        const clampedLabelX = Math.max(12, Math.min(width - 12, labelX));

        const text = document.createElementNS('http://www.w3.org/2000/svg', 'text');
        text.setAttribute('x', String(clampedLabelX));
        text.setAttribute('y', '20');
        text.setAttribute('font-size', '10');
        text.setAttribute('fill', '#999');
        text.setAttribute('text-anchor', 'middle');
        text.textContent = item.label;
        svg.appendChild(text);
      });
    }

    if ((useTimeScale || primarySeries.hasTimestamps) && axis) {
      const tickCount = Math.min(5, Math.max(primarySeries.curve.length, 2));
      for (let i = 0; i < tickCount; i += 1) {
        const ratio = tickCount === 1 ? 0 : i / (tickCount - 1);
        const x = ratio * width;
        const xPct = ratio * 100;

        const grid = document.createElementNS('http://www.w3.org/2000/svg', 'line');
        grid.setAttribute('x1', String(x));
        grid.setAttribute('y1', '0');
        grid.setAttribute('x2', String(x));
        grid.setAttribute('y2', String(height));
        grid.setAttribute('stroke', '#e3e3e3');
        grid.setAttribute('stroke-width', '1');
        svg.appendChild(grid);

        let labelDate = null;
        if (useTimeScale) {
          labelDate = new Date(tStart + ratio * (tEnd - tStart));
        } else {
          const index = Math.round(ratio * (primarySeries.timestamps.length - 1));
          labelDate = new Date(primarySeries.timestamps[index]);
        }
        if (Number.isNaN(labelDate.getTime())) continue;

        const month = String(labelDate.getUTCMonth() + 1).padStart(2, '0');
        const day = String(labelDate.getUTCDate()).padStart(2, '0');

        const label = document.createElement('div');
        label.className = 'chart-axis-label';
        if (i === 0) label.className += ' start';
        if (i === tickCount - 1) label.className += ' end';
        label.style.left = `${xPct}%`;
        label.textContent = `${month}.${day}`;
        axis.appendChild(label);
      }
    }

    normalizedSeries.forEach((series) => {
      const points = series.curve
        .map((value, index) => {
          const x = toSeriesXRatio(series, index) * width;
          const y = toY(value);
          return `${x},${y}`;
        })
        .join(' ');

      const line = document.createElementNS('http://www.w3.org/2000/svg', 'polyline');
      line.setAttribute('points', points);
      line.setAttribute('fill', 'none');
      line.setAttribute('stroke', series.color);
      line.setAttribute('stroke-width', String(series.strokeWidth));
      line.setAttribute('vector-effect', 'non-scaling-stroke');
      line.setAttribute('stroke-linejoin', 'round');
      line.setAttribute('stroke-linecap', 'round');
      svg.appendChild(line);
    });

    renderPercentAxisLabels(svg, toY, minValue, maxValue);
    renderReturnProfile(svg, options?.returnProfile || null);
  }

  function renderChart(equityCurve, timestamps, options = null) {
    renderSeriesChart(
      [
        {
          curve: equityCurve,
          timestamps,
          color: '#4a90e2',
          strokeWidth: DEFAULT_FOCUSED_STROKE_WIDTH,
        },
      ],
      options
    );
  }

  function renderMultiChart(seriesList, options = null) {
    renderSeriesChart(seriesList, options);
  }

  window.AnalyticsEquity = {
    renderChart,
    renderMultiChart,
    renderEmpty,
  };
})();
