const APP_VERSION = "20260401-seasonal1";

const probChart = document.getElementById("probChart");
const minChart = document.getElementById("minChart");
const maxChart = document.getElementById("maxChart");
const yyChart = document.getElementById("yyChart");
const surfaceMap = document.getElementById("surfaceMap");
const surfaceTooltip = document.getElementById("surfaceTooltip");

const shipTab = document.getElementById("shipTab");
const aggregateTab = document.getElementById("aggregateTab");
const shipSelectCard = document.getElementById("shipSelectCard");
const shipSelect = document.getElementById("shipSelect");
const speciesSelect = document.getElementById("speciesSelect");
const observedSort = document.getElementById("observedSort");
const rankingPanel = document.getElementById("rankingPanel");
const rankingList = document.getElementById("rankingList");
const evaluationPanel = document.getElementById("evaluationPanel");

const simulatorNodes = {
  airTemp: {
    input: document.getElementById("airSlider"),
    value: document.getElementById("airValue"),
  },
  seaTemp: {
    input: document.getElementById("seaSlider"),
    value: document.getElementById("seaValue"),
  },
  moonAge: {
    input: document.getElementById("moonSlider"),
    value: document.getElementById("moonValue"),
  },
};

const outputNodes = {
  probability: document.getElementById("simProbability"),
  min: document.getElementById("simMin"),
  max: document.getElementById("simMax"),
};

const chartState = new Map();
const payloadCache = new Map();

let catalogState = null;
let payloadState = null;
let currentView = "ship";
let simulatorListenersBound = false;
let observedSortBound = false;
let selectorBound = false;
let tabsBound = false;
let surfaceMapBound = false;
let evaluationPanelBound = false;
let surfaceState = null;

function setText(id, value) {
  const node = document.getElementById(id);
  if (node) {
    node.textContent = value;
  }
}

function formatRange(range, fallback = "-") {
  return range && range.from && range.to ? `${range.from} - ${range.to}` : fallback;
}

function shipName(payload) {
  return payload && payload.ship && payload.ship.name ? payload.ship.name : "船宿";
}

function speciesLabel(payload) {
  return payload && payload.species && payload.species.label ? payload.species.label : "魚種";
}

function speciesUnit(payload) {
  return payload && payload.species && payload.species.unit ? payload.species.unit : "";
}

function clamp(value, lower, upper) {
  return Math.max(lower, Math.min(upper, value));
}

function normalCdf(value) {
  const sign = value < 0 ? -1 : 1;
  const x = Math.abs(value) / Math.SQRT2;
  const t = 1 / (1 + 0.3275911 * x);
  const a1 = 0.254829592;
  const a2 = -0.284496736;
  const a3 = 1.421413741;
  const a4 = -1.453152027;
  const a5 = 1.061405429;
  const erf = 1 - (((((a5 * t + a4) * t + a3) * t + a2) * t + a1) * t * Math.exp(-x * x));
  return 0.5 * (1 + sign * erf);
}

function percent(value) {
  return `${(value * 100).toFixed(1)}%`;
}

function amountText(value, unit) {
  return `${value.toFixed(1)}${unit}`;
}

function formatControlValue(key, value) {
  if (key === "moonAge") {
    return `${value.toFixed(1)}日`;
  }
  return `${value.toFixed(1)}℃`;
}

function formatDate(iso) {
  const date = new Date(`${iso}T00:00:00+09:00`);
  return new Intl.DateTimeFormat("ja-JP", {
    month: "short",
    day: "numeric",
  }).format(date);
}

function monthlyTicks(points) {
  return points.flatMap((point, index) => {
    if (!point.date.endsWith("-01")) {
      return [];
    }
    const month = Number(point.date.slice(5, 7));
    return [{ index, label: `${month}月` }];
  });
}

function featureSourceLabel(source) {
  return {
    archive: "実測気象",
    forecast: "予報気象",
    climatology: "平年気候",
  }[source] || source;
}

function currentShip() {
  if (!catalogState) {
    return null;
  }
  return catalogState.ships.find((ship) => ship.id === shipSelect.value) || null;
}

function currentShipSpecies() {
  const ship = currentShip();
  if (!ship) {
    return null;
  }
  return ship.species.find((species) => species.id === speciesSelect.value) || null;
}

function currentAggregateSpecies() {
  if (!catalogState) {
    return null;
  }
  return catalogState.aggregateSpecies.find((species) => species.id === speciesSelect.value) || null;
}

function currentSpeciesSelection() {
  return currentView === "aggregate" ? currentAggregateSpecies() : currentShipSpecies();
}

function setView(view) {
  if (!shipTab || !aggregateTab || !shipSelectCard) {
    currentView = view === "aggregate" ? "aggregate" : "ship";
    return;
  }
  currentView = view === "aggregate" ? "aggregate" : "ship";
  const aggregateMode = currentView === "aggregate";
  shipTab.classList.toggle("is-active", !aggregateMode);
  aggregateTab.classList.toggle("is-active", aggregateMode);
  shipTab.setAttribute("aria-selected", String(!aggregateMode));
  aggregateTab.setAttribute("aria-selected", String(aggregateMode));
  shipSelectCard.hidden = aggregateMode;
}

function prepareCanvas(canvas) {
  if (!canvas) {
    return null;
  }
  const ctx = canvas.getContext("2d");
  if (!ctx) {
    return null;
  }
  const dpr = window.devicePixelRatio || 1;
  const cssWidth = canvas.clientWidth;
  const cssHeight = canvas.clientHeight;

  canvas.width = Math.floor(cssWidth * dpr);
  canvas.height = Math.floor(cssHeight * dpr);
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  ctx.clearRect(0, 0, cssWidth, cssHeight);
  return { ctx, cssWidth, cssHeight };
}

function drawGrid(ctx, geometry, maxValue, formatter) {
  const { margin, width, height, floorY } = geometry;
  ctx.strokeStyle = "rgba(255,255,255,0.08)";
  ctx.lineWidth = 1;
  ctx.font = "12px Segoe UI";
  ctx.fillStyle = "rgba(200,219,234,0.8)";

  [0, 0.25, 0.5, 0.75, 1].forEach((ratio) => {
    const y = margin.top + height * ratio;
    ctx.beginPath();
    ctx.moveTo(margin.left, y);
    ctx.lineTo(margin.left + width, y);
    ctx.stroke();
    ctx.fillText(formatter((1 - ratio) * maxValue), 8, y + 4);
  });

  ctx.strokeStyle = "rgba(255,255,255,0.18)";
  ctx.beginPath();
  ctx.moveTo(margin.left, floorY);
  ctx.lineTo(margin.left + width, floorY);
  ctx.stroke();
}

function drawBottomTicks(ctx, geometry, points) {
  const { margin, floorY, slotWidth } = geometry;
  ctx.fillStyle = "rgba(200,219,234,0.8)";
  ctx.font = "12px Segoe UI";

  monthlyTicks(points).forEach((tick) => {
    const x = margin.left + slotWidth * (tick.index + 0.5);
    ctx.fillText(tick.label, x - 8, floorY + 22);
  });
}

function drawProbabilityChart(payload) {
  const prepared = prepareCanvas(probChart);
  if (!prepared) {
    return;
  }
  const { ctx, cssWidth, cssHeight } = prepared;
  const margin = { top: 28, right: 26, bottom: 48, left: 48 };
  const width = cssWidth - margin.left - margin.right;
  const height = cssHeight - margin.top - margin.bottom;
  const floorY = margin.top + height;
  const slotWidth = width / payload.predictions.length;
  const maxValue = Math.max(Math.max(...payload.predictions.map((item) => item.probability)) * 1.16, 0.06);
  const geometry = { margin, width, height, floorY, slotWidth, maxValue };

  const bg = ctx.createLinearGradient(0, margin.top, 0, floorY);
  bg.addColorStop(0, "rgba(104, 209, 255, 0.06)");
  bg.addColorStop(1, "rgba(104, 209, 255, 0)");
  ctx.fillStyle = bg;
  ctx.fillRect(margin.left, margin.top, width, height);

  drawGrid(ctx, geometry, maxValue, (value) => `${Math.round(value * 100)}%`);

  const gradient = ctx.createLinearGradient(0, margin.top, 0, floorY);
  gradient.addColorStop(0, "#76dbff");
  gradient.addColorStop(0.55, "#39bcff");
  gradient.addColorStop(1, "#0f6cff");

  payload.predictions.forEach((point, index) => {
    const valueHeight = (point.probability / maxValue) * height;
    const x = margin.left + index * slotWidth;
    const y = floorY - valueHeight;
    ctx.fillStyle = gradient;
    ctx.fillRect(x + 0.35, y, Math.max(slotWidth - 0.7, 1), valueHeight);
  });

  drawBottomTicks(ctx, geometry, payload.predictions);
  chartState.set(probChart.id, geometry);
}

function drawAmountChart(canvas, payload, field, observedField, color, fillColor) {
  const prepared = prepareCanvas(canvas);
  if (!prepared) {
    return;
  }
  const { ctx, cssWidth, cssHeight } = prepared;
  const margin = { top: 22, right: 26, bottom: 42, left: 48 };
  const width = cssWidth - margin.left - margin.right;
  const height = cssHeight - margin.top - margin.bottom;
  const floorY = margin.top + height;
  const slotWidth = width / payload.predictions.length;
  const values = payload.predictions.map((item) => item[field]);
  const observedValues = payload.predictions.map((item) => item[observedField]).filter((value) => value !== null);
  const maxValue = Math.max(2, ...values, ...observedValues) * 1.16;
  const geometry = { margin, width, height, floorY, slotWidth, maxValue };

  const bg = ctx.createLinearGradient(0, margin.top, 0, floorY);
  bg.addColorStop(0, fillColor);
  bg.addColorStop(1, "rgba(255,255,255,0)");
  ctx.fillStyle = bg;
  ctx.fillRect(margin.left, margin.top, width, height);

  drawGrid(ctx, geometry, maxValue, (value) => `${value.toFixed(1)}`);

  const points = payload.predictions.map((point, index) => {
    const x = margin.left + slotWidth * (index + 0.5);
    const y = floorY - (point[field] / maxValue) * height;
    return { x, y };
  });

  ctx.beginPath();
  ctx.moveTo(points[0].x, floorY);
  points.forEach((point) => ctx.lineTo(point.x, point.y));
  ctx.lineTo(points[points.length - 1].x, floorY);
  ctx.closePath();
  ctx.fillStyle = fillColor;
  ctx.fill();

  ctx.strokeStyle = color;
  ctx.lineWidth = 2.5;
  ctx.beginPath();
  points.forEach((point, index) => {
    if (index === 0) {
      ctx.moveTo(point.x, point.y);
    } else {
      ctx.lineTo(point.x, point.y);
    }
  });
  ctx.stroke();

  payload.predictions.forEach((point, index) => {
    if (point[observedField] === null) {
      return;
    }
    const x = margin.left + slotWidth * (index + 0.5);
    const y = floorY - (point[observedField] / maxValue) * height;
    ctx.fillStyle = "#eef7ff";
    ctx.beginPath();
    ctx.arc(x, y, 3.2, 0, Math.PI * 2);
    ctx.fill();
    ctx.strokeStyle = color;
    ctx.lineWidth = 1.2;
    ctx.stroke();
  });

  drawBottomTicks(ctx, geometry, payload.predictions);
  chartState.set(canvas.id, geometry);
}

function buildFeatureMap(rawFeatures) {
  const angle = (rawFeatures.moonAge / 29.53058867) * Math.PI * 2;
  const dayOfYear = rawFeatures.dayOfYear || 1;
  const yearAngle = ((dayOfYear - 1) / 365.2425) * Math.PI * 2;
  const airSeaGap = rawFeatures.airTemp - rawFeatures.seaTemp;
  return {
    airTemp: rawFeatures.airTemp,
    seaTemp: rawFeatures.seaTemp,
    moonAge: rawFeatures.moonAge,
    dayOfYear,
    moonSin: Math.sin(angle),
    moonCos: Math.cos(angle),
    moonSin2: Math.sin(angle * 2),
    moonCos2: Math.cos(angle * 2),
    moonSin3: Math.sin(angle * 3),
    moonCos3: Math.cos(angle * 3),
    yearSin: Math.sin(yearAngle),
    yearCos: Math.cos(yearAngle),
    yearSin2: Math.sin(yearAngle * 2),
    yearCos2: Math.cos(yearAngle * 2),
    airSeaGap,
    airSeaMean: (rawFeatures.airTemp + rawFeatures.seaTemp) * 0.5,
    airSeaAbsGap: Math.abs(airSeaGap),
    moonFullness: 0.5 * (1 - Math.cos(angle)),
  };
}

function buildScaledFeatures(rawFeatures, regression) {
  const featureMap = buildFeatureMap(rawFeatures);
  const featureKeys = regression.featureSpec ? regression.featureSpec.featureKeys : [];
  const scaled = {};
  featureKeys.forEach((key) => {
    scaled[key] = (featureMap[key] - regression.stats.means[key]) / regression.stats.scales[key];
  });
  return { featureMap, scaled };
}

function evaluateBasisTerm(term, scaled) {
  if (term === "intercept") {
    return 1;
  }
  if (term.endsWith("^2")) {
    const key = term.slice(0, -2);
    return scaled[key] * scaled[key];
  }
  if (term.includes("*")) {
    const [left, right] = term.split("*");
    return scaled[left] * scaled[right];
  }
  return scaled[term];
}

function buildBasis(rawFeatures, regression) {
  const { scaled } = buildScaledFeatures(rawFeatures, regression);
  const basisTerms = regression.featureSpec ? regression.featureSpec.basisTerms : [];
  return {
    scaled,
    basis: basisTerms.map((term) => evaluateBasisTerm(term, scaled)),
  };
}

function dot(weights, vector) {
  return weights.reduce((sum, weight, index) => sum + weight * vector[index], 0);
}

function weightedAverage(pairs) {
  const totalWeight = pairs.reduce((sum, [weight]) => sum + weight, 0);
  if (totalWeight <= 0) {
    return 0;
  }
  return pairs.reduce((sum, [weight, value]) => sum + weight * value, 0) / totalWeight;
}

function estimateNeighborResiduals(scaled, featureKeys, neighbor) {
  if (!neighbor || !Array.isArray(neighbor.support) || !neighbor.support.length) {
    return { minResidual: 0, maxResidual: 0 };
  }

  const vector = featureKeys.map((key) => scaled[key]);
  const ranked = neighbor.support
    .map((item) => {
      const distanceSq = item.vector.reduce((sum, value, index) => sum + (vector[index] - value) ** 2, 0);
      const weight = Math.exp(-distanceSq / Math.max(2 * neighbor.bandwidth * neighbor.bandwidth, 1e-9));
      return { distanceSq, weight, item };
    })
    .sort((left, right) => left.distanceSq - right.distanceSq)
    .slice(0, neighbor.neighborCount);

  const minPairs = ranked.map(({ weight, item }) => [weight, item.minResidual]);
  const maxPairs = ranked.map(({ weight, item }) => [weight, item.maxResidual]);
  const totalWeight = minPairs.reduce((sum, [weight]) => sum + weight, 0);
  const priorWeight = neighbor.priorWeight || 1;
  const shrink = totalWeight > 0 ? totalWeight / (totalWeight + priorWeight) : 0;
  return {
    minResidual: weightedAverage(minPairs) * shrink,
    maxResidual: weightedAverage(maxPairs) * shrink,
  };
}

function buildModelFeatureMap(rawFeatures, contextFeatures = {}) {
  return { ...buildFeatureMap(rawFeatures), ...contextFeatures };
}

function predictTree(tree, featureRow) {
  let node = tree;
  while (node && node.v === undefined) {
    node = featureRow[node.f] <= node.t ? node.l : node.r;
  }
  return node && node.v !== undefined ? node.v : 0;
}

function predictForest(rawFeatures, regression, contextFeatures = {}) {
  const featureMap = buildModelFeatureMap(rawFeatures, contextFeatures);
  const featureKeys = regression.forest ? regression.forest.featureKeys : [];
  const featureRow = featureKeys.map((key) => (featureMap[key] !== undefined ? featureMap[key] : 0));
  const minTrees = regression.forest ? regression.forest.catchMin : [];
  const maxTrees = regression.forest ? regression.forest.catchMax : [];
  const minScore = minTrees.reduce((sum, tree) => sum + predictTree(tree, featureRow), 0) / Math.max(minTrees.length, 1);
  const maxScore = maxTrees.reduce((sum, tree) => sum + predictTree(tree, featureRow), 0) / Math.max(maxTrees.length, 1);
  return { minScore, maxScore };
}

function buildNeuralInput(rawFeatures, model, contextFeatures = {}) {
  const featureMap = buildModelFeatureMap(rawFeatures, contextFeatures);
  const featureKeys = model.input ? model.input.featureKeys : [];
  return featureKeys.map((key) => (((featureMap[key] !== undefined ? featureMap[key] : 0) - model.input.means[key]) / model.input.scales[key]));
}

function predictDenseLayers(inputVector, layers) {
  return layers.reduce((values, layer, layerIndex) => {
    const outputs = layer.weights.map((weights, outputIndex) => {
      const total = weights.reduce((sum, weight, inputIndex) => sum + weight * values[inputIndex], layer.biases[outputIndex]);
      return layerIndex < layers.length - 1 ? Math.tanh(total) : total;
    });
    return outputs;
  }, inputVector);
}

function predictNeuralWithContext(rawFeatures, model, contextFeatures = {}) {
  const [predictedMinScore, predictedGapScore] = predictDenseLayers(buildNeuralInput(rawFeatures, model, contextFeatures), model.network.layers);
  const predictedMin = clamp(Math.expm1(predictedMinScore), 0, model.countCeiling);
  const predictedGap = clamp(Math.expm1(predictedGapScore), 0, model.countCeiling);
  const predictedMax = clamp(predictedMin + predictedGap, predictedMin, model.countCeiling);
  return { predictedMin, predictedMax };
}

function simulate(rawFeatures, payload) {
  const resolvedRawFeatures = {
    ...rawFeatures,
    dayOfYear: rawFeatures.dayOfYear || (payload.simulatorContext ? payload.simulatorContext.dayOfYear : 1),
  };
  const model = payload.model || payload.regression;
  let predictedMin;
  let predictedMax;

  if (model.type === "neural_network") {
    const aggregateContexts = payload.aggregate && Array.isArray(payload.aggregate.modelContexts) ? payload.aggregate.modelContexts : [];
    if (payload.scope && payload.scope.mode === "aggregate" && aggregateContexts.length) {
      const predictions = aggregateContexts.map((context) => predictNeuralWithContext(resolvedRawFeatures, model, context.contextFeatures || {}));
      predictedMin = predictions.reduce((sum, item) => sum + item.predictedMin, 0) / Math.max(predictions.length, 1);
      predictedMax = predictions.reduce((sum, item) => sum + item.predictedMax, 0) / Math.max(predictions.length, 1);
    } else {
      const prediction = predictNeuralWithContext(resolvedRawFeatures, model, payload.contextFeatures || {});
      predictedMin = prediction.predictedMin;
      predictedMax = prediction.predictedMax;
    }
  } else if (model.type === "random_forest") {
    const aggregateContexts = payload.aggregate && Array.isArray(payload.aggregate.modelContexts) ? payload.aggregate.modelContexts : [];
    if (payload.scope && payload.scope.mode === "aggregate" && aggregateContexts.length) {
      const predictions = aggregateContexts.map((context) => {
        const forestPrediction = predictForest(resolvedRawFeatures, model, context.contextFeatures || {});
        const contextMin = clamp(Math.expm1(forestPrediction.minScore), 0, model.countCeiling);
        const contextMax = Math.max(contextMin, clamp(Math.expm1(forestPrediction.maxScore), 0, model.countCeiling));
        return { predictedMin: contextMin, predictedMax: contextMax };
      });
      predictedMin = predictions.reduce((sum, item) => sum + item.predictedMin, 0) / Math.max(predictions.length, 1);
      predictedMax = predictions.reduce((sum, item) => sum + item.predictedMax, 0) / Math.max(predictions.length, 1);
    } else {
      const forestPrediction = predictForest(resolvedRawFeatures, model, payload.contextFeatures || {});
      predictedMin = clamp(Math.expm1(forestPrediction.minScore), 0, model.countCeiling);
      predictedMax = Math.max(predictedMin, clamp(Math.expm1(forestPrediction.maxScore), 0, model.countCeiling));
    }
  } else {
    const { scaled, basis } = buildBasis(resolvedRawFeatures, model);
    let minScore = dot(model.baseline.catchMin.weights, basis);
    let maxScore = dot(model.baseline.catchMax.weights, basis);
    const featureKeys = model.featureSpec ? model.featureSpec.featureKeys : [];
    const neighborResiduals = estimateNeighborResiduals(scaled, featureKeys, model.neighbor);
    minScore += neighborResiduals.minResidual;
    maxScore += neighborResiduals.maxResidual;
    predictedMin = clamp(Math.expm1(minScore), 0, model.countCeiling);
    predictedMax = Math.max(predictedMin, clamp(Math.expm1(maxScore), 0, model.countCeiling));
  }
  const xDayModel = payload.xDayModel || {};
  const xDayPeak = payload.xDayPeak || {};
  const sigma = Math.max(xDayModel.maxSigma || 0, 0.01);
  const maximaSamples = Array.isArray(xDayModel.maximaSamples) ? xDayModel.maximaSamples : [];
  const probability = maximaSamples.length
    ? clamp(
        maximaSamples.reduce((sum, sample) => sum + (1 - normalCdf((sample - predictedMax) / sigma)), 0) / maximaSamples.length,
        0,
        0.995,
      )
    : clamp(predictedMax / Math.max(xDayPeak.predictedMax || 1, 1), 0, 0.995);
  return { probability, predictedMin, predictedMax };
}

function getSimulatorRawFeatures() {
  return {
    airTemp: Number(simulatorNodes.airTemp.input.value),
    seaTemp: Number(simulatorNodes.seaTemp.input.value),
    moonAge: Number(simulatorNodes.moonAge.input.value),
    dayOfYear: payloadState && payloadState.simulatorContext ? payloadState.simulatorContext.dayOfYear : 1,
  };
}

function populateTopDays(payload) {
  const host = document.getElementById("topDays");
  if (!host) {
    return;
  }
  const aggregateMode = payload.scope && payload.scope.mode === "aggregate";
  const maxLabel = aggregateMode ? "平均上限" : "上限";
  const minLabel = aggregateMode ? "平均下限" : "下限";
  host.innerHTML = "";
  payload.topDays.forEach((item) => {
    const chip = document.createElement("article");
    chip.className = "day-chip";
    chip.innerHTML = `
      <span class="date">${formatDate(item.date)}</span>
      <strong>${maxLabel} ${amountText(item.predictedMax, payload.species.unit)}</strong>
      <span class="detail">Xデー確率 ${percent(item.probability)} / ${minLabel} ${amountText(item.predictedMin, payload.species.unit)}</span>
      <span class="subdetail">気温 ${item.airTemp.toFixed(1)}℃ / 水温 ${item.seaTemp.toFixed(1)}℃ / 月齢 ${item.moonAge.toFixed(1)}日</span>
    `;
    host.appendChild(chip);
  });
}

function buildObservedRecords(payload) {
  const uniqueRecords = new Map();

  payload.predictions.forEach((point) => {
    if (!point.observedDate || uniqueRecords.has(point.observedDate)) {
      return;
    }

    uniqueRecords.set(point.observedDate, {
      observedDate: point.observedDate,
      observedMin: point.observedMin,
      observedMax: point.observedMax,
      observedText: point.observedText,
      observedShipCount: point.observedShipCount || 0,
      forecastDate: point.date,
    });
  });

  return Array.from(uniqueRecords.values());
}

function sortObservedRecords(records, sortKey) {
  const items = [...records];

  if (sortKey === "catch") {
    items.sort((left, right) => {
      if (right.observedMax !== left.observedMax) {
        return right.observedMax - left.observedMax;
      }
      if (right.observedMin !== left.observedMin) {
        return right.observedMin - left.observedMin;
      }
      return left.observedDate.localeCompare(right.observedDate);
    });
    return items;
  }

  items.sort((left, right) => left.observedDate.localeCompare(right.observedDate));
  return items;
}

function populateObservedList(payload) {
  const host = document.getElementById("observedList");
  if (!host) {
    return;
  }
  const aggregateMode = payload.scope && payload.scope.mode === "aggregate";
  const records = sortObservedRecords(buildObservedRecords(payload), observedSort ? observedSort.value : "date");

  setText("observedLabel", aggregateMode ? "前年釣果実績平均" : "前年釣果実績");
  setText("observedMeta", `${records.length}件`);
  host.innerHTML = "";

  if (!records.length) {
    const empty = document.createElement("div");
    empty.className = "observed-empty";
    empty.textContent = aggregateMode ? "前年実績平均がある日だけここに表示します。" : "前年実績がある日だけここに表示します。";
    host.appendChild(empty);
    return;
  }

  records.forEach((item) => {
    const chip = document.createElement("article");
    chip.className = "day-chip observed-chip";
    const fallbackText = `${amountText(item.observedMin, payload.species.unit)}〜${amountText(item.observedMax, payload.species.unit)}`;
    const modeDetail = aggregateMode && item.observedShipCount ? ` / ${item.observedShipCount}船平均` : "";
    chip.innerHTML = `
      <span class="date">${formatDate(item.observedDate)}</span>
      <strong>${item.observedText || fallbackText}</strong>
      <span class="detail">下限 ${amountText(item.observedMin, payload.species.unit)} / 上限 ${amountText(item.observedMax, payload.species.unit)}${modeDetail}</span>
      <span class="subdetail">対応予測日 ${formatDate(item.forecastDate)}</span>
    `;
    host.appendChild(chip);
  });
}

function populateRanking(payload) {
  const aggregateMode = payload.scope && payload.scope.mode === "aggregate";
  if (rankingPanel) {
    rankingPanel.hidden = !aggregateMode;
  }
  if (!rankingList) {
    return;
  }
  rankingList.innerHTML = "";

  if (!aggregateMode) {
    return;
  }

  const ranking = (payload.aggregate && payload.aggregate.ranking) || [];
  setText("rankingLabel", `${speciesLabel(payload)} 船宿ランキング`);
  setText("rankingMeta", `${ranking.length}船 / 平均上限順`);

  if (!ranking.length) {
    const empty = document.createElement("div");
    empty.className = "observed-empty";
    empty.textContent = "ランキング対象の船宿がありません。";
    rankingList.appendChild(empty);
    return;
  }

  ranking.forEach((item, index) => {
    const chip = document.createElement("article");
    chip.className = "day-chip observed-chip";
    chip.innerHTML = `
      <span class="date">${index + 1}位</span>
      <strong>${item.shipName}</strong>
      <span class="detail">平均上限 ${amountText(item.averageMax, item.unit)} / 平均下限 ${amountText(item.averageMin, item.unit)}</span>
      <span class="subdetail">平均中央 ${amountText(item.averageCenter, item.unit)} / 記録日 ${item.tripDays}</span>
    `;
    rankingList.appendChild(chip);
  });
}

function drawYYChart(payload) {
  const points = payload.evaluation ? payload.evaluation.yyPoints : [];
  const prepared = prepareCanvas(yyChart);
  if (!prepared) {
    return;
  }
  const { ctx, cssWidth, cssHeight } = prepared;
  const margin = { top: 28, right: 24, bottom: 46, left: 58 };
  const width = cssWidth - margin.left - margin.right;
  const height = cssHeight - margin.top - margin.bottom;

  ctx.fillStyle = "rgba(8, 20, 31, 0.96)";
  ctx.fillRect(0, 0, cssWidth, cssHeight);

  if (!points.length) {
    ctx.fillStyle = "rgba(200,219,234,0.82)";
    ctx.font = "15px Segoe UI";
    ctx.fillText("検証点が少ないため yy プロットを表示できません。", margin.left, margin.top + 22);
    return;
  }

  const maxValue = Math.max(
    2,
    ...points.map((point) => point.actualMax),
    ...points.map((point) => point.predictedMax),
  ) * 1.12;
  const plotX = (value) => margin.left + (value / Math.max(maxValue, 1e-6)) * width;
  const plotY = (value) => margin.top + height - (value / Math.max(maxValue, 1e-6)) * height;

  ctx.strokeStyle = "rgba(255,255,255,0.08)";
  ctx.lineWidth = 1;
  ctx.font = "12px Segoe UI";
  ctx.fillStyle = "rgba(200,219,234,0.82)";
  [0, 0.25, 0.5, 0.75, 1].forEach((ratio) => {
    const axisValue = maxValue * ratio;
    const x = plotX(axisValue);
    const y = plotY(axisValue);
    ctx.beginPath();
    ctx.moveTo(margin.left, y);
    ctx.lineTo(margin.left + width, y);
    ctx.moveTo(x, margin.top);
    ctx.lineTo(x, margin.top + height);
    ctx.stroke();
    ctx.fillText(axisValue.toFixed(1), x - 10, margin.top + height + 22);
    if (ratio < 1) {
      ctx.fillText(axisValue.toFixed(1), 10, y + 4);
    }
  });

  ctx.strokeStyle = "rgba(255,255,255,0.9)";
  ctx.lineWidth = 1.4;
  ctx.beginPath();
  ctx.moveTo(margin.left, margin.top + height);
  ctx.lineTo(margin.left + width, margin.top);
  ctx.stroke();

  points.forEach((point) => {
    const x = plotX(point.actualMax);
    const y = plotY(point.predictedMax);
    const error = Math.abs(point.predictedMax - point.actualMax);
    const alpha = clamp(0.36 + error / Math.max(maxValue, 1), 0.36, 0.96);
    ctx.fillStyle = `rgba(255, 209, 107, ${alpha})`;
    ctx.beginPath();
    ctx.arc(x, y, 4.2, 0, Math.PI * 2);
    ctx.fill();
    ctx.strokeStyle = "rgba(8,20,31,0.72)";
    ctx.lineWidth = 1;
    ctx.stroke();
  });

  ctx.fillStyle = "rgba(200,219,234,0.82)";
  ctx.fillText("実測上限", margin.left + width - 40, margin.top + height + 22);
  ctx.save();
  ctx.translate(16, margin.top + 44);
  ctx.rotate(-Math.PI / 2);
  ctx.fillText("予測上限", 0, 0);
  ctx.restore();
}

function populateEvaluation(payload) {
  const evaluation = payload.evaluation;
  if (!evaluationPanel) {
    return;
  }
  evaluationPanel.hidden = !evaluation;

  if (!evaluation) {
    return;
  }

  setText("evaluationLabel", `${speciesLabel(payload)} 上限 yyプロット`);
  setText("evaluationMeta", `タップして表示 / 検証 ${evaluation.validationRows}件 / 上限MAE ${evaluation.maxMae.toFixed(2)}`);
}

function surfaceColor(ratio) {
  const hue = 220 - ratio * 180;
  const saturation = 84;
  const lightness = 20 + ratio * 46;
  return `hsl(${hue} ${saturation}% ${lightness}%)`;
}

function drawSurfaceMap(payload) {
  if (!payload || !surfaceMap) {
    return;
  }

  const prepared = prepareCanvas(surfaceMap);
  if (!prepared) {
    return;
  }
  const { ctx, cssWidth, cssHeight } = prepared;
  const margin = { top: 28, right: 34, bottom: 56, left: 66 };
  const width = cssWidth - margin.left - margin.right;
  const height = cssHeight - margin.top - margin.bottom;
  const moonConfig = payload.featureRanges.moonAge;
  const seaConfig = payload.featureRanges.seaTemp;
  const airTemp = Number(simulatorNodes.airTemp.input.value);
  const currentSea = Number(simulatorNodes.seaTemp.input.value);
  const currentMoon = Number(simulatorNodes.moonAge.input.value);
  const columns = clamp(Math.floor(width / 9), 48, 110);
  const rows = clamp(Math.floor(height / 9), 32, 72);
  const cellWidth = width / columns;
  const cellHeight = height / rows;
  const samples = [];
  let minValue = Number.POSITIVE_INFINITY;
  let maxValue = Number.NEGATIVE_INFINITY;

  for (let rowIndex = 0; rowIndex < rows; rowIndex += 1) {
    const seaTemp = seaConfig.max - (rowIndex / Math.max(rows - 1, 1)) * (seaConfig.max - seaConfig.min);
    const line = [];
    for (let columnIndex = 0; columnIndex < columns; columnIndex += 1) {
      const moonAge = moonConfig.min + (columnIndex / Math.max(columns - 1, 1)) * (moonConfig.max - moonConfig.min);
      const value = simulate({ airTemp, seaTemp, moonAge }, payload).predictedMax;
      line.push(value);
      minValue = Math.min(minValue, value);
      maxValue = Math.max(maxValue, value);
    }
    samples.push(line);
  }

  const span = Math.max(maxValue - minValue, 1e-6);
  ctx.fillStyle = "rgba(8, 20, 31, 0.96)";
  ctx.fillRect(0, 0, cssWidth, cssHeight);

  for (let rowIndex = 0; rowIndex < rows; rowIndex += 1) {
    for (let columnIndex = 0; columnIndex < columns; columnIndex += 1) {
      const ratio = (samples[rowIndex][columnIndex] - minValue) / span;
      ctx.fillStyle = surfaceColor(ratio);
      ctx.fillRect(
        margin.left + columnIndex * cellWidth,
        margin.top + rowIndex * cellHeight,
        Math.ceil(cellWidth + 1),
        Math.ceil(cellHeight + 1),
      );
    }
  }

  ctx.strokeStyle = "rgba(255,255,255,0.16)";
  ctx.lineWidth = 1;
  ctx.strokeRect(margin.left, margin.top, width, height);

  ctx.font = "12px Segoe UI";
  ctx.fillStyle = "rgba(200,219,234,0.86)";
  for (let tick = 0; tick <= 4; tick += 1) {
    const ratio = tick / 4;
    const y = margin.top + height * ratio;
    const seaTemp = seaConfig.max - ratio * (seaConfig.max - seaConfig.min);
    ctx.strokeStyle = "rgba(255,255,255,0.08)";
    ctx.beginPath();
    ctx.moveTo(margin.left, y);
    ctx.lineTo(margin.left + width, y);
    ctx.stroke();
    ctx.fillText(`${seaTemp.toFixed(1)}℃`, 10, y + 4);
  }

  for (let tick = 0; tick <= 5; tick += 1) {
    const ratio = tick / 5;
    const x = margin.left + width * ratio;
    const moonAge = moonConfig.min + ratio * (moonConfig.max - moonConfig.min);
    ctx.strokeStyle = "rgba(255,255,255,0.08)";
    ctx.beginPath();
    ctx.moveTo(x, margin.top);
    ctx.lineTo(x, margin.top + height);
    ctx.stroke();
    ctx.fillText(`${moonAge.toFixed(1)}日`, x - 12, margin.top + height + 24);
  }

  const currentX = margin.left + ((currentMoon - moonConfig.min) / Math.max(moonConfig.max - moonConfig.min, 1e-6)) * width;
  const currentY = margin.top + ((seaConfig.max - currentSea) / Math.max(seaConfig.max - seaConfig.min, 1e-6)) * height;
  ctx.strokeStyle = "rgba(255,255,255,0.92)";
  ctx.lineWidth = 1.4;
  ctx.beginPath();
  ctx.moveTo(currentX, margin.top);
  ctx.lineTo(currentX, margin.top + height);
  ctx.moveTo(margin.left, currentY);
  ctx.lineTo(margin.left + width, currentY);
  ctx.stroke();
  ctx.fillStyle = "#ffffff";
  ctx.beginPath();
  ctx.arc(currentX, currentY, 4.2, 0, Math.PI * 2);
  ctx.fill();

  const legendWidth = Math.min(220, width * 0.34);
  const legendX = margin.left + width - legendWidth;
  const legendY = 10;
  const legend = ctx.createLinearGradient(legendX, 0, legendX + legendWidth, 0);
  legend.addColorStop(0, surfaceColor(0));
  legend.addColorStop(0.5, surfaceColor(0.5));
  legend.addColorStop(1, surfaceColor(1));
  ctx.fillStyle = legend;
  ctx.fillRect(legendX, legendY, legendWidth, 10);
  ctx.fillStyle = "rgba(200,219,234,0.86)";
  ctx.fillText(`予測上限 ${amountText(minValue, payload.species.unit)}`, legendX, legendY + 24);
  ctx.fillText(amountText(maxValue, payload.species.unit), legendX + legendWidth - 42, legendY + 24);
  ctx.fillText("海水温", 10, margin.top - 8);
  ctx.fillText("月齢", margin.left + width - 24, margin.top + height + 44);
  const referenceDate = payload.simulatorContext && payload.simulatorContext.referenceDate ? formatDate(payload.simulatorContext.referenceDate) : null;
  setText(
    "surfaceMeta",
    referenceDate
      ? `気温 ${airTemp.toFixed(1)}℃ で固定 / 年内位相 ${referenceDate} 相当 / 色は予測上限`
      : `気温 ${airTemp.toFixed(1)}℃ で固定 / 色は予測上限`,
  );
  surfaceState = {
    margin,
    width,
    height,
    moonConfig,
    seaConfig,
    airTemp,
    minValue,
    maxValue,
  };
}

function updateSimulator() {
  if (!payloadState) {
    return;
  }

  hideSurfaceTooltip();
  const rawFeatures = getSimulatorRawFeatures();
  Object.entries(simulatorNodes).forEach(([key, node]) => {
    if (!node.input || !node.value) {
      return;
    }
    node.value.textContent = formatControlValue(key, Number(node.input.value));
  });

  const result = simulate(rawFeatures, payloadState);
  if (outputNodes.probability) {
    outputNodes.probability.textContent = percent(result.probability);
  }
  if (outputNodes.min) {
    outputNodes.min.textContent = amountText(result.predictedMin, speciesUnit(payloadState));
  }
  if (outputNodes.max) {
    outputNodes.max.textContent = amountText(result.predictedMax, speciesUnit(payloadState));
  }
  drawSurfaceMap(payloadState);
}

function configureSimulator(payload, options = {}) {
  const { preserveValues = false } = options;
  Object.entries(simulatorNodes).forEach(([key, node]) => {
    if (!node.input) {
      return;
    }
    const config = payload.featureRanges[key];
    node.input.min = config.min;
    node.input.max = config.max;
    node.input.step = config.step;
    const currentValue = Number(node.input.value);
    const nextValue = preserveValues && Number.isFinite(currentValue) ? clamp(currentValue, config.min, config.max) : config.default;
    node.input.value = nextValue;
  });

  if (!simulatorListenersBound) {
    Object.values(simulatorNodes).forEach((node) => {
      if (node.input) {
        node.input.addEventListener("input", updateSimulator);
      }
    });
    simulatorListenersBound = true;
  }

  updateSimulator();
}

function hideSurfaceTooltip() {
  if (surfaceTooltip) {
    surfaceTooltip.hidden = true;
  }
}

function syncSimulatorToSurfacePoint(seaTemp, moonAge) {
  const seaConfig = payloadState && payloadState.featureRanges ? payloadState.featureRanges.seaTemp : null;
  const moonConfig = payloadState && payloadState.featureRanges ? payloadState.featureRanges.moonAge : null;
  if (!seaConfig || !moonConfig) {
    return;
  }

  if (!simulatorNodes.seaTemp.input || !simulatorNodes.moonAge.input) {
    return;
  }
  simulatorNodes.seaTemp.input.value = clamp(seaTemp, seaConfig.min, seaConfig.max).toFixed(1);
  simulatorNodes.moonAge.input.value = clamp(moonAge, moonConfig.min, moonConfig.max).toFixed(1);
  updateSimulator();
}

function showSurfaceTooltip(clientX, clientY, options = {}) {
  if (!payloadState || !surfaceState || !surfaceMap || !surfaceTooltip) {
    return;
  }

  const { commitSelection = false } = options;
  const rect = surfaceMap.getBoundingClientRect();
  const x = clientX - rect.left;
  const y = clientY - rect.top;
  const { margin, width, height, moonConfig, seaConfig, airTemp } = surfaceState;
  const withinX = x >= margin.left && x <= margin.left + width;
  const withinY = y >= margin.top && y <= margin.top + height;

  if (!withinX || !withinY) {
    hideSurfaceTooltip();
    return;
  }

  const moonRatio = clamp((x - margin.left) / Math.max(width, 1e-6), 0, 1);
  const seaRatio = clamp((y - margin.top) / Math.max(height, 1e-6), 0, 1);
  const moonAge = moonConfig.min + moonRatio * (moonConfig.max - moonConfig.min);
  const seaTemp = seaConfig.max - seaRatio * (seaConfig.max - seaConfig.min);
  if (commitSelection) {
    syncSimulatorToSurfacePoint(seaTemp, moonAge);
  }
  const result = simulate({ airTemp, seaTemp, moonAge }, payloadState);
  const aggregateMode = payloadState.scope && payloadState.scope.mode === "aggregate";
  const maxLabel = aggregateMode ? "平均予測上限" : "予測上限";
  const minLabel = aggregateMode ? "平均予測下限" : "予測下限";

  surfaceTooltip.innerHTML = `
    <strong>${maxLabel} ${amountText(result.predictedMax, payloadState.species.unit)}</strong>
    <span>${minLabel} ${amountText(result.predictedMin, payloadState.species.unit)}</span>
    <span>海水温 ${seaTemp.toFixed(1)}℃ / 月齢 ${moonAge.toFixed(1)}日</span>
    <span>気温 ${airTemp.toFixed(1)}℃ で固定</span>
  `;
  surfaceTooltip.hidden = false;

  const wrapRect = surfaceMap.parentElement.getBoundingClientRect();
  const tooltipWidth = surfaceTooltip.offsetWidth;
  const tooltipHeight = surfaceTooltip.offsetHeight;
  const localX = clientX - wrapRect.left;
  const localY = clientY - wrapRect.top;
  const padding = 12;
  const preferredRight = localX + 18;
  const fallbackLeft = localX - tooltipWidth - 18;
  const maxLeft = wrapRect.width - tooltipWidth - padding;
  const left = preferredRight <= maxLeft ? preferredRight : Math.max(padding, fallbackLeft);
  const top = clamp(localY - tooltipHeight * 0.5, padding, Math.max(padding, wrapRect.height - tooltipHeight - padding));
  surfaceTooltip.style.left = `${left}px`;
  surfaceTooltip.style.top = `${top}px`;
}

function bindSurfaceMap() {
  if (surfaceMapBound || !surfaceMap) {
    return;
  }

  const handlePreview = (clientX, clientY) => showSurfaceTooltip(clientX, clientY);
  const handleCommit = (clientX, clientY) => showSurfaceTooltip(clientX, clientY, { commitSelection: true });
  surfaceMap.addEventListener("click", (event) => handleCommit(event.clientX, event.clientY));
  surfaceMap.addEventListener("mousemove", (event) => handlePreview(event.clientX, event.clientY));
  surfaceMap.addEventListener(
    "touchstart",
    (event) => {
      const touch = event.touches[0];
      if (touch) {
        handleCommit(touch.clientX, touch.clientY);
      }
    },
    { passive: true },
  );
  surfaceMap.addEventListener(
    "touchmove",
    (event) => {
      const touch = event.touches[0];
      if (touch) {
        handleCommit(touch.clientX, touch.clientY);
      }
    },
    { passive: true },
  );
  surfaceMap.addEventListener("mouseleave", hideSurfaceTooltip);
  surfaceMap.addEventListener("touchcancel", hideSurfaceTooltip);
  surfaceMapBound = true;
}

function bindEvaluationPanel() {
  if (evaluationPanelBound || !evaluationPanel) {
    return;
  }

  evaluationPanel.addEventListener("toggle", () => {
    if (evaluationPanel.open && payloadState && payloadState.evaluation) {
      window.requestAnimationFrame(() => drawYYChart(payloadState));
    }
  });
  evaluationPanelBound = true;
}

function showTooltip(canvas, tooltip, scroller, clientX, clientY) {
  if (!payloadState) {
    return;
  }

  const geometry = chartState.get(canvas.id);
  if (!geometry) {
    return;
  }

  const canvasRect = canvas.getBoundingClientRect();
  const scrollerRect = scroller.getBoundingClientRect();
  const x = clientX - canvasRect.left;
  const index = clamp(
    Math.floor((x - geometry.margin.left) / geometry.slotWidth),
    0,
    payloadState.predictions.length - 1,
  );
  const point = payloadState.predictions[index];
  if (!point) {
    tooltip.hidden = true;
    return;
  }

  const aggregateMode = payloadState.scope && payloadState.scope.mode === "aggregate";
  const observedLine = point.observedDate
    ? aggregateMode
      ? `<span>前年平均 ${formatDate(point.observedDate)} ${point.observedText || `${amountText(point.observedMin, payloadState.species.unit)}〜${amountText(point.observedMax, payloadState.species.unit)}`}</span>`
      : `<span>前年実測 ${formatDate(point.observedDate)} 下限 ${amountText(point.observedMin, payloadState.species.unit)} / 上限 ${amountText(point.observedMax, payloadState.species.unit)}</span>`
    : "";
  const shipLine = aggregateMode && point.shipCount ? `<span>対象 ${point.shipCount}船</span>` : "";

  tooltip.innerHTML = `
    <strong>${formatDate(point.date)}</strong>
    <span>Xデー確率 ${percent(point.probability)}</span>
    <span>下限 ${amountText(point.predictedMin, payloadState.species.unit)} / 上限 ${amountText(point.predictedMax, payloadState.species.unit)}</span>
    ${observedLine}
    ${shipLine}
    <span>気温 ${point.airTemp.toFixed(1)}℃ / 水温 ${point.seaTemp.toFixed(1)}℃ / 月齢 ${point.moonAge.toFixed(1)}日</span>
    <span>${featureSourceLabel(point.featureSource)}</span>
  `;
  tooltip.hidden = false;

  const tooltipWidth = tooltip.offsetWidth;
  const tooltipHeight = tooltip.offsetHeight;
  const viewportX = clientX - scrollerRect.left;
  const viewportY = clientY - scrollerRect.top;
  const contentX = scroller.scrollLeft + viewportX;
  const padding = 12;
  const preferredRight = contentX + 18;
  const maxLeft = scroller.scrollLeft + scrollerRect.width - tooltipWidth - padding;
  const minLeft = scroller.scrollLeft + padding;
  const fallbackLeft = contentX - tooltipWidth - 18;
  const left = preferredRight <= maxLeft ? preferredRight : Math.max(minLeft, fallbackLeft);
  const top = clamp(
    viewportY - tooltipHeight * 0.5,
    padding,
    Math.max(padding, scrollerRect.height - tooltipHeight - padding),
  );

  tooltip.style.left = `${left}px`;
  tooltip.style.top = `${top}px`;
}

function bindTooltip(canvasId, scrollerId, tooltipId) {
  const canvas = document.getElementById(canvasId);
  const scroller = document.getElementById(scrollerId);
  const tooltip = document.getElementById(tooltipId);
  if (!canvas || !scroller || !tooltip) {
    return;
  }

  const handleMove = (clientX, clientY) => showTooltip(canvas, tooltip, scroller, clientX, clientY);
  canvas.addEventListener("click", (event) => handleMove(event.clientX, event.clientY));
  canvas.addEventListener("mousemove", (event) => handleMove(event.clientX, event.clientY));
  canvas.addEventListener(
    "touchstart",
    (event) => {
      const touch = event.touches[0];
      if (touch) {
        handleMove(touch.clientX, touch.clientY);
      }
    },
    { passive: true },
  );
  canvas.addEventListener(
    "touchmove",
    (event) => {
      const touch = event.touches[0];
      if (touch) {
        handleMove(touch.clientX, touch.clientY);
      }
    },
    { passive: true },
  );
  canvas.addEventListener("mouseleave", () => {
    tooltip.hidden = true;
  });
  canvas.addEventListener("touchcancel", () => {
    tooltip.hidden = true;
  });
  scroller.addEventListener("scroll", () => {
    tooltip.hidden = true;
  });
}

function updateUrl(view, shipId, speciesId) {
  const url = new URL(window.location.href);
  url.searchParams.set("view", view);
  if (view === "aggregate") {
    url.searchParams.delete("ship");
    url.searchParams.set("species", speciesId);
  } else {
    url.searchParams.set("ship", shipId);
    url.searchParams.set("species", speciesId);
  }
  window.history.replaceState({}, "", url);
}

function populateShipSelect() {
  if (!shipSelect) {
    return;
  }
  shipSelect.innerHTML = "";
  catalogState.ships.forEach((ship) => {
    const option = document.createElement("option");
    option.value = ship.id;
    option.textContent = ship.name;
    shipSelect.appendChild(option);
  });
}

function populateSpeciesSelect(ship, preferredSpeciesId = null) {
  if (!speciesSelect) {
    return;
  }
  speciesSelect.innerHTML = "";
  ship.species.forEach((species) => {
    const option = document.createElement("option");
    option.value = species.id;
    option.textContent = `${species.label} (${species.unit})`;
    speciesSelect.appendChild(option);
  });
  speciesSelect.value = ship.species.some((species) => species.id === preferredSpeciesId)
    ? preferredSpeciesId
    : ship.species[0].id;
}

function populateAggregateSpeciesSelect(preferredSpeciesId = null) {
  if (!speciesSelect || !catalogState.aggregateSpecies.length) {
    return;
  }
  speciesSelect.innerHTML = "";
  catalogState.aggregateSpecies.forEach((species) => {
    const option = document.createElement("option");
    option.value = species.id;
    option.textContent = `${species.label} (${species.unit})`;
    speciesSelect.appendChild(option);
  });
  speciesSelect.value = catalogState.aggregateSpecies.some((species) => species.id === preferredSpeciesId)
    ? preferredSpeciesId
    : catalogState.aggregateSpecies[0].id;
}

async function fetchPayload(file) {
  if (!payloadCache.has(file)) {
    payloadCache.set(
      file,
      fetch(`./${file}?v=${APP_VERSION}`).then((response) => {
        if (!response.ok) {
          throw new Error(`データ取得失敗: ${file}`);
        }
        return response.json();
      }),
    );
  }
  return payloadCache.get(file);
}

function render(payload, options = {}) {
  const { preserveSimulator = false, preserveEvaluationOpen = false } = options;
  payloadState = payload;
  hideSurfaceTooltip();
  const aggregateMode = payload.scope && payload.scope.mode === "aggregate";
  setView(aggregateMode ? "aggregate" : "ship");

  document.title = aggregateMode ? `${speciesLabel(payload)} 魚種統合 Xデー予測` : `${shipName(payload)} ${speciesLabel(payload)} Xデー予測`;
  setText("title", aggregateMode ? `${speciesLabel(payload)} 魚種統合 Xデー予測` : `${shipName(payload)} ${speciesLabel(payload)} Xデー予測`);
  setText("generatedAt", payload.generatedAt ? `更新 ${payload.generatedAt}` : "更新時刻 不明");
  setText(
    "summaryMeta",
    aggregateMode
      ? `統合 ${payload.aggregate && payload.aggregate.shipCount ? payload.aggregate.shipCount : "-"}船 / 学習 ${formatRange(payload.trainingRange, "期間不明")} / 記録日 ${payload.tripDays || "-"} / Xデー ${payload.xDayRule || "-"}`
      : `学習 ${formatRange(payload.trainingRange, "期間不明")} / 記録日 ${payload.tripDays || "-"} / Xデー ${payload.xDayRule || "-"}`,
  );
  setText("rangeLabel", formatRange(payload.forecastRange, "予測期間 不明"));
  setText("todayLabel", payload.today ? `基準日 ${payload.today}` : "基準日 不明");
  setText("minMetricLabel", `${aggregateMode ? "平均予測下限" : "予測下限"}${speciesUnit(payload)}`);
  setText("maxMetricLabel", `${aggregateMode ? "平均予測上限" : "予測上限"}${speciesUnit(payload)}`);
  setText("minChartLabel", `${aggregateMode ? "平均予測下限" : "予測下限"}${speciesUnit(payload)}`);
  setText("maxChartLabel", `${aggregateMode ? "平均予測上限" : "予測上限"}${speciesUnit(payload)}`);
  setText("unitLabelMin", `${speciesUnit(payload)} / 日`);
  setText("unitLabelMax", `${speciesUnit(payload)} / 日`);

  populateTopDays(payload);
  populateRanking(payload);
  populateObservedList(payload);
  if (evaluationPanel) {
    evaluationPanel.open = preserveEvaluationOpen && !!payload.evaluation;
  }
  populateEvaluation(payload);
  configureSimulator(payload, { preserveValues: preserveSimulator });
  drawProbabilityChart(payload);
  drawAmountChart(minChart, payload, "predictedMin", "observedMin", "#4ff0c6", "rgba(79, 240, 198, 0.22)");
  drawAmountChart(maxChart, payload, "predictedMax", "observedMax", "#ffd16b", "rgba(255, 209, 107, 0.20)");
  if (evaluationPanel && evaluationPanel.open && payload.evaluation) {
    drawYYChart(payload);
  }
}

async function loadSelection(view, shipId, speciesId) {
  if (view === "aggregate" && catalogState.aggregateSpecies.length) {
    setView("aggregate");
    populateAggregateSpeciesSelect(speciesId);
    const species = currentAggregateSpecies() || catalogState.aggregateSpecies[0];
    updateUrl("aggregate", null, species.id);
    const payload = await fetchPayload(species.file);
    render(payload);
    return;
  }

  setView("ship");
  const ship = catalogState.ships.find((item) => item.id === shipId) || catalogState.ships[0];
  shipSelect.value = ship.id;
  populateSpeciesSelect(ship, speciesId);
  const species = currentShipSpecies() || ship.species[0];
  updateUrl("ship", ship.id, species.id);
  const payload = await fetchPayload(species.file);
  render(payload);
}

function bindSelectors() {
  if (selectorBound || !shipSelect || !speciesSelect) {
    return;
  }

  shipSelect.addEventListener("change", async () => {
    if (currentView !== "ship") {
      return;
    }
    const ship = currentShip();
    populateSpeciesSelect(ship);
    const species = currentShipSpecies();
    const payload = await fetchPayload(species.file);
    updateUrl("ship", ship.id, species.id);
    render(payload);
  });

  speciesSelect.addEventListener("change", async () => {
    const species = currentSpeciesSelection();
    const payload = await fetchPayload(species.file);
    if (currentView === "aggregate") {
      updateUrl("aggregate", null, species.id);
    } else {
      const ship = currentShip();
      updateUrl("ship", ship.id, species.id);
    }
    render(payload);
  });

  selectorBound = true;
}

function bindTabs() {
  if (tabsBound || !shipTab || !aggregateTab) {
    return;
  }

  shipTab.addEventListener("click", async () => {
    const ship = currentShip() || catalogState.ships[0];
    const species = ship.species[0];
    await loadSelection("ship", ship.id, species.id);
  });

  aggregateTab.addEventListener("click", async () => {
    if (!catalogState.aggregateSpecies.length) {
      return;
    }
    const species = currentAggregateSpecies() || catalogState.aggregateSpecies[0];
    await loadSelection("aggregate", null, species.id);
  });

  tabsBound = true;
}

bindTooltip("probChart", "probChartScroller", "probTooltip");
bindTooltip("minChart", "minChartScroller", "minTooltip");
bindTooltip("maxChart", "maxChartScroller", "maxTooltip");
bindSurfaceMap();
bindEvaluationPanel();

if (!observedSortBound) {
  if (observedSort) {
    observedSort.addEventListener("change", () => {
      if (payloadState) {
        populateObservedList(payloadState);
      }
    });
    observedSortBound = true;
  }
}

window.addEventListener("resize", () => {
  if (payloadState) {
    render(payloadState, { preserveSimulator: true, preserveEvaluationOpen: evaluationPanel ? evaluationPanel.open : false });
  }
});

async function main() {
  const response = await fetch(`./data/catalog.json?v=${APP_VERSION}`);
  if (!response.ok) {
    throw new Error("カタログを読み込めませんでした");
  }
  catalogState = await response.json();
  catalogState.aggregateSpecies = Array.isArray(catalogState.aggregateSpecies) ? catalogState.aggregateSpecies : [];
  populateShipSelect();
  bindSelectors();
  bindTabs();

  const params = new URLSearchParams(window.location.search);
  const requestedView = params.get("view") === "aggregate" ? "aggregate" : "ship";
  const requestedShipId = params.get("ship");
  const requestedSpeciesId = params.get("species");
  const initialView = requestedView === "aggregate" && catalogState.aggregateSpecies.length ? "aggregate" : "ship";
  await loadSelection(initialView, requestedShipId, requestedSpeciesId);
}

main().catch((error) => {
  console.error(error);
  document.getElementById("title").textContent = "読込エラー";
  document.getElementById("generatedAt").textContent = error.message;
});
