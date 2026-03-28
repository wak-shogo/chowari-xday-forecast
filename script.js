const probChart = document.getElementById("probChart");
const minChart = document.getElementById("minChart");
const maxChart = document.getElementById("maxChart");

const shipSelect = document.getElementById("shipSelect");
const speciesSelect = document.getElementById("speciesSelect");

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
let simulatorBound = false;

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

function currentSpecies() {
  const ship = currentShip();
  if (!ship) {
    return null;
  }
  return ship.species.find((species) => species.id === speciesSelect.value) || null;
}

function prepareCanvas(canvas) {
  const ctx = canvas.getContext("2d");
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
  const { ctx, cssWidth, cssHeight } = prepareCanvas(probChart);
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
  const { ctx, cssWidth, cssHeight } = prepareCanvas(canvas);
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

function buildBasis(rawFeatures, regression) {
  const stats = regression.stats;
  const air = (rawFeatures.airTemp - stats.means.airTemp) / stats.scales.airTemp;
  const sea = (rawFeatures.seaTemp - stats.means.seaTemp) / stats.scales.seaTemp;
  const angle = (rawFeatures.moonAge / 29.53058867) * Math.PI * 2;
  const moonSin = (Math.sin(angle) - stats.means.moonSin) / stats.scales.moonSin;
  const moonCos = (Math.cos(angle) - stats.means.moonCos) / stats.scales.moonCos;
  return [1, air, sea, moonSin, moonCos, air * sea, air * moonSin, air * moonCos, sea * moonSin, sea * moonCos, air * air, sea * sea];
}

function dot(weights, vector) {
  return weights.reduce((sum, weight, index) => sum + weight * vector[index], 0);
}

function simulate(rawFeatures, payload) {
  const regression = payload.regression;
  const basis = buildBasis(rawFeatures, regression);
  const predictedMin = clamp(Math.expm1(dot(regression.models.catchMin.weights, basis)), 0, regression.countCeiling);
  const predictedMaxRaw = clamp(Math.expm1(dot(regression.models.catchMax.weights, basis)), 0, regression.countCeiling);
  const predictedMax = Math.max(predictedMin, predictedMaxRaw);
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

function populateTopDays(payload) {
  const host = document.getElementById("topDays");
  host.innerHTML = "";
  payload.topDays.forEach((item) => {
    const chip = document.createElement("article");
    chip.className = "day-chip";
    chip.innerHTML = `
      <span class="date">${formatDate(item.date)}</span>
      <strong>上限 ${amountText(item.predictedMax, payload.species.unit)}</strong>
      <span class="detail">Xデー確率 ${percent(item.probability)} / 下限 ${amountText(item.predictedMin, payload.species.unit)}</span>
      <span class="subdetail">気温 ${item.airTemp.toFixed(1)}℃ / 水温 ${item.seaTemp.toFixed(1)}℃ / 月齢 ${item.moonAge.toFixed(1)}日</span>
    `;
    host.appendChild(chip);
  });
}

function configureSimulator(payload) {
  const update = () => {
    const rawFeatures = {
      airTemp: Number(simulatorNodes.airTemp.input.value),
      seaTemp: Number(simulatorNodes.seaTemp.input.value),
      moonAge: Number(simulatorNodes.moonAge.input.value),
    };

    Object.entries(simulatorNodes).forEach(([key, node]) => {
      node.value.textContent = formatControlValue(key, Number(node.input.value));
    });

    const result = simulate(rawFeatures, payload);
    outputNodes.probability.textContent = percent(result.probability);
    outputNodes.min.textContent = amountText(result.predictedMin, payload.species.unit);
    outputNodes.max.textContent = amountText(result.predictedMax, payload.species.unit);
  };

  Object.entries(simulatorNodes).forEach(([key, node]) => {
    const config = payload.featureRanges[key];
    node.input.min = config.min;
    node.input.max = config.max;
    node.input.step = config.step;
    node.input.value = config.default;
    if (!simulatorBound) {
      node.input.addEventListener("input", update);
    }
  });

  simulatorBound = true;
  update();
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

  tooltip.innerHTML = `
    <strong>${formatDate(point.date)}</strong>
    <span>Xデー確率 ${percent(point.probability)}</span>
    <span>下限 ${amountText(point.predictedMin, payloadState.species.unit)} / 上限 ${amountText(point.predictedMax, payloadState.species.unit)}</span>
    ${
      point.observedDate
        ? `<span>前年実測 ${formatDate(point.observedDate)} 下限 ${amountText(point.observedMin, payloadState.species.unit)} / 上限 ${amountText(point.observedMax, payloadState.species.unit)}</span>`
        : ""
    }
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

function updateUrl(shipId, speciesId) {
  const url = new URL(window.location.href);
  url.searchParams.set("ship", shipId);
  url.searchParams.set("species", speciesId);
  window.history.replaceState({}, "", url);
}

function populateShipSelect() {
  shipSelect.innerHTML = "";
  catalogState.ships.forEach((ship) => {
    const option = document.createElement("option");
    option.value = ship.id;
    option.textContent = ship.name;
    shipSelect.appendChild(option);
  });
}

function populateSpeciesSelect(ship, preferredSpeciesId = null) {
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

async function fetchPayload(file) {
  if (!payloadCache.has(file)) {
    payloadCache.set(
      file,
      fetch(`./${file}`).then((response) => {
        if (!response.ok) {
          throw new Error(`データ取得失敗: ${file}`);
        }
        return response.json();
      }),
    );
  }
  return payloadCache.get(file);
}

function render(payload) {
  payloadState = payload;
  document.title = `${payload.ship.name} ${payload.species.label} Xデー予測`;
  document.getElementById("title").textContent = `${payload.ship.name} ${payload.species.label} Xデー予測`;
  document.getElementById("generatedAt").textContent = `更新 ${payload.generatedAt}`;
  document.getElementById("summaryMeta").textContent = `学習 ${payload.trainingRange.from} - ${payload.trainingRange.to} / 記録日 ${payload.tripDays} / Xデー ${payload.xDayRule}`;
  document.getElementById("rangeLabel").textContent = `${payload.forecastRange.from} - ${payload.forecastRange.to}`;
  document.getElementById("todayLabel").textContent = `基準日 ${payload.today}`;
  document.getElementById("minMetricLabel").textContent = `予測下限${payload.species.unit}`;
  document.getElementById("maxMetricLabel").textContent = `予測上限${payload.species.unit}`;
  document.getElementById("minChartLabel").textContent = `予測下限${payload.species.unit}`;
  document.getElementById("maxChartLabel").textContent = `予測上限${payload.species.unit}`;
  document.getElementById("unitLabelMin").textContent = `${payload.species.unit} / 日`;
  document.getElementById("unitLabelMax").textContent = `${payload.species.unit} / 日`;

  populateTopDays(payload);
  configureSimulator(payload);
  drawProbabilityChart(payload);
  drawAmountChart(minChart, payload, "predictedMin", "observedMin", "#4ff0c6", "rgba(79, 240, 198, 0.22)");
  drawAmountChart(maxChart, payload, "predictedMax", "observedMax", "#ffd16b", "rgba(255, 209, 107, 0.20)");
}

async function loadSelection(shipId, speciesId) {
  const ship = catalogState.ships.find((item) => item.id === shipId) || catalogState.ships[0];
  shipSelect.value = ship.id;
  populateSpeciesSelect(ship, speciesId);
  const species = ship.species.find((item) => item.id === speciesSelect.value) || ship.species[0];
  updateUrl(ship.id, species.id);
  const payload = await fetchPayload(species.file);
  render(payload);
}

function bindSelectors() {
  shipSelect.addEventListener("change", async () => {
    const ship = currentShip();
    populateSpeciesSelect(ship);
    const species = currentSpecies();
    const payload = await fetchPayload(species.file);
    updateUrl(ship.id, species.id);
    render(payload);
  });

  speciesSelect.addEventListener("change", async () => {
    const ship = currentShip();
    const species = currentSpecies();
    const payload = await fetchPayload(species.file);
    updateUrl(ship.id, species.id);
    render(payload);
  });
}

bindTooltip("probChart", "probChartScroller", "probTooltip");
bindTooltip("minChart", "minChartScroller", "minTooltip");
bindTooltip("maxChart", "maxChartScroller", "maxTooltip");

window.addEventListener("resize", () => {
  if (payloadState) {
    render(payloadState);
  }
});

async function main() {
  const response = await fetch("./data/catalog.json");
  if (!response.ok) {
    throw new Error("カタログを読み込めませんでした");
  }
  catalogState = await response.json();
  populateShipSelect();
  bindSelectors();

  const params = new URLSearchParams(window.location.search);
  const requestedShipId = params.get("ship");
  const requestedSpeciesId = params.get("species");
  await loadSelection(requestedShipId, requestedSpeciesId);
}

main().catch((error) => {
  console.error(error);
  document.getElementById("title").textContent = "読込エラー";
  document.getElementById("generatedAt").textContent = error.message;
});
