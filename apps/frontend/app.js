const pageHost = window.location.hostname || "localhost";
const pageProtocol = window.location.protocol === "https:" ? "https:" : "http:";
const defaultApiBase = `${pageProtocol}//${pageHost}:8000`;
const API_BASE = window.API_BASE || defaultApiBase;
const WINDOW_MAPBOX_TOKEN = window.MAPBOX_ACCESS_TOKEN || "";

const form = document.getElementById("search-form");
const input = document.getElementById("kadastrs-input");
const statusEl = document.getElementById("status");
const detailsEl = document.getElementById("details-list");
const regionSelect = document.getElementById("region-select");
const loadAllButton = document.getElementById("load-all");
const copyCommandsButton = document.getElementById("copy-commands");
const copyCadasterButton = document.getElementById("copy-cadaster");
const layerListEl = document.getElementById("layer-list");
const enableAllLayersButton = document.getElementById("enable-all-layers");
const disableAllLayersButton = document.getElementById("disable-all-layers");
const commandsText = [
  "cd /Users/JanisMac_mini/Atverto-datu/geo_ingest",
  "source .venv/bin/activate",
  "python3 ingest.py ingest",
].join("\n");
const testCadaster = "50720060539";

console.info("API base:", API_BASE);

let map = null;
function initMap(token) {
  if (!token) {
    setStatus("Mapbox token missing. Set MAPBOX_ACCESS_TOKEN in backend env.", true);
    console.warn("Mapbox token missing.");
    return false;
  }
  mapboxgl.accessToken = token;
  map = new mapboxgl.Map({
    container: "map",
    style: "mapbox://styles/mapbox/satellite-streets-v12",
    center: [24.6032, 56.8796],
    zoom: 7,
  });
  return true;
}

async function fetchFrontendConfig() {
  const response = await fetch(`${API_BASE}/frontend-config`);
  if (!response.ok) {
    throw new Error(`Frontend config failed (${response.status})`);
  }
  return response.json();
}

async function fetchLayers() {
  const response = await fetch(`${API_BASE}/layers`);
  if (!response.ok) {
    throw new Error(`Layers request failed (${response.status})`);
  }
  return response.json();
}

const sourceId = "cadastre";
const fillLayerId = "cadastre-fill";
const lineLayerId = "cadastre-line";
const labelLayerId = "cadastre-label";
const outlineLayerId = "cadastre-outline";

const overlayCache = new Map();
const overlayLayers = new Map();

function clearLayers() {
  if (!map) return;
  if (map.getLayer(fillLayerId)) {
    map.removeLayer(fillLayerId);
  }
  if (map.getLayer(lineLayerId)) {
    map.removeLayer(lineLayerId);
  }
  if (map.getLayer(outlineLayerId)) {
    map.removeLayer(outlineLayerId);
  }
  if (map.getLayer(labelLayerId)) {
    map.removeLayer(labelLayerId);
  }
  if (map.getSource(sourceId)) {
    map.removeSource(sourceId);
  }
}

function getOverlayIds(layerId) {
  return {
    sourceId: `layer-${layerId}`,
    fillId: `layer-${layerId}-fill`,
    lineId: `layer-${layerId}-line`,
    circleId: `layer-${layerId}-circle`,
  };
}

function ensureMapReady(callback) {
  if (!map) return;
  if (map.isStyleLoaded()) {
    callback();
  } else {
    map.once("load", callback);
  }
}

function removeOverlayLayer(layerId) {
  if (!map) return;
  const { sourceId, fillId, lineId, circleId } = getOverlayIds(layerId);
  if (map.getLayer(fillId)) {
    map.removeLayer(fillId);
  }
  if (map.getLayer(lineId)) {
    map.removeLayer(lineId);
  }
  if (map.getLayer(circleId)) {
    map.removeLayer(circleId);
  }
  if (map.getSource(sourceId)) {
    map.removeSource(sourceId);
  }
}

function detectGeometryTypes(data) {
  const types = new Set();
  if (!data) {
    return types;
  }
  const features = data.type === "FeatureCollection" ? data.features || [] : [data];
  for (const feature of features) {
    const geomType = feature && feature.geometry && feature.geometry.type;
    if (geomType) {
      types.add(geomType);
    }
  }
  return types;
}

function addOverlayLayer(layerDef, data) {
  if (!map) return;
  const { sourceId, fillId, lineId, circleId } = getOverlayIds(layerDef.id);
  const display = layerDef.display || {};
  const fillColor = display.fill_color || "#60a5fa";
  const rawFillOpacity = Number(display.fill_opacity);
  const fillOpacity = Number.isFinite(rawFillOpacity) ? rawFillOpacity : 0.2;
  const lineColor = display.line_color || fillColor;
  const rawLineWidth = Number(display.line_width);
  const lineWidth = Number.isFinite(rawLineWidth) ? rawLineWidth : 1.5;
  const circleColor = display.circle_color || lineColor;
  const rawCircleRadius = Number(display.circle_radius);
  const circleRadius = Number.isFinite(rawCircleRadius) ? rawCircleRadius : 4;
  const geometryTypes = detectGeometryTypes(data);
  const hasPolygon =
    geometryTypes.has("Polygon") || geometryTypes.has("MultiPolygon");
  const hasPoint = geometryTypes.has("Point") || geometryTypes.has("MultiPoint");
  ensureMapReady(() => {
    removeOverlayLayer(layerDef.id);
    map.addSource(sourceId, {
      type: "geojson",
      data,
    });
    if (hasPolygon) {
      map.addLayer({
        id: fillId,
        type: "fill",
        source: sourceId,
        filter: ["any", ["==", ["geometry-type"], "Polygon"], ["==", ["geometry-type"], "MultiPolygon"]],
        paint: {
          "fill-color": fillColor,
          "fill-opacity": fillOpacity,
        },
      });
      map.addLayer({
        id: lineId,
        type: "line",
        source: sourceId,
        filter: ["any", ["==", ["geometry-type"], "Polygon"], ["==", ["geometry-type"], "MultiPolygon"]],
        paint: {
          "line-color": lineColor,
          "line-width": lineWidth,
        },
      });
    }

    if (hasPoint) {
      map.addLayer({
        id: circleId,
        type: "circle",
        source: sourceId,
        filter: ["any", ["==", ["geometry-type"], "Point"], ["==", ["geometry-type"], "MultiPoint"]],
        paint: {
          "circle-color": circleColor,
          "circle-radius": circleRadius,
          "circle-opacity": 0.85,
          "circle-stroke-color": "#ffffff",
          "circle-stroke-width": 0.6,
        },
      });
    }

    const clickLayerId = hasPolygon ? fillId : circleId;
    if (!clickLayerId) {
      return;
    }
    map.off("click", clickLayerId);
    map.on("click", clickLayerId, (event) => {
      const props = (event.features && event.features[0] && event.features[0].properties) || {};
      const popupContent = buildPopupContent(props);
      new mapboxgl.Popup({ closeButton: true, maxWidth: "360px" })
        .setLngLat(event.lngLat)
        .setHTML(popupContent)
        .addTo(map);
    });
  });
}

async function handleLayerToggle(layer, checked, checkbox) {
  if (!map) {
    setStatus("Map unavailable: missing Mapbox token.", true);
    if (checkbox) {
      checkbox.checked = false;
    }
    return;
  }
  if (!checked) {
    removeOverlayLayer(layer.id);
    return;
  }
  setStatus(`Loading layer ${layer.id}...`);
  try {
    const cached = overlayCache.get(layer.id);
    const data = cached || (await fetchLayerFeatures(layer.id));
    overlayCache.set(layer.id, data);
    addOverlayLayer(layer, data);
    setStatus(`Layer ${layer.id} loaded.`);
  } catch (error) {
    console.error("Layer load failed:", error);
    setStatus(`Layer ${layer.id} failed to load.`, true);
    if (checkbox) {
      checkbox.checked = false;
    }
  }
}

function setAllLayers(enabled) {
  overlayLayers.forEach((layer) => {
    const checkbox = layerListEl
      ? layerListEl.querySelector(`input[data-layer-id="${layer.id}"]`)
      : null;
    if (!checkbox || checkbox.checked === enabled) {
      return;
    }
    checkbox.checked = enabled;
    handleLayerToggle(layer, enabled, checkbox);
  });
}

function flattenCoordinates(coords, out = []) {
  if (!Array.isArray(coords) || coords.length === 0) {
    return out;
  }
  if (typeof coords[0] === "number") {
    out.push(coords);
    return out;
  }
  coords.forEach((item) => flattenCoordinates(item, out));
  return out;
}

function getBbox(feature) {
  if (!feature) {
    return null;
  }
  const features = feature.type === "FeatureCollection" ? feature.features || [] : [feature];
  if (features.length === 0) {
    return null;
  }
  const coords = [];
  features.forEach((item) => {
    if (item && item.geometry && item.geometry.coordinates) {
      flattenCoordinates(item.geometry.coordinates, coords);
    }
  });
  let minX = Infinity;
  let minY = Infinity;
  let maxX = -Infinity;
  let maxY = -Infinity;
  coords.forEach(([x, y]) => {
    minX = Math.min(minX, x);
    minY = Math.min(minY, y);
    maxX = Math.max(maxX, x);
    maxY = Math.max(maxY, y);
  });
  if (!Number.isFinite(minX)) {
    return null;
  }
  return [minX, minY, maxX, maxY];
}

function setStatus(message, isError = false) {
  statusEl.textContent = message;
  statusEl.classList.toggle("error", isError);
}

async function fetchProperty(kadastrs) {
  const url = `${API_BASE}/properties?kadastrs=${encodeURIComponent(kadastrs)}`;
  console.info("Requesting", url);
  const response = await fetch(url);
  if (!response.ok) {
    if (response.status === 404) {
      throw new Error("Property not found");
    }
    throw new Error(`Request failed (${response.status})`);
  }
  return response.json();
}

async function fetchAll(region) {
  const url = `${API_BASE}/properties/all?region=${encodeURIComponent(region)}`;
  console.info("Requesting", url);
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`Request failed (${response.status})`);
  }
  return response.json();
}

function renderFeature(feature) {
  if (!map) {
    setStatus("Map unavailable: missing Mapbox token.", true);
    return;
  }
  clearLayers();
  if (!feature || (!feature.geometry && feature.type !== "FeatureCollection")) {
    console.error("Feature is missing geometry:", feature);
    setStatus("Geometry missing in response.", true);
    return;
  }
  map.addSource(sourceId, {
    type: "geojson",
    data: feature,
  });
  map.addLayer({
    id: fillLayerId,
    type: "fill",
    source: sourceId,
    filter: ["!=", ["get", "is_outline"], true],
    paint: {
      "fill-color": "#f5c16c",
      "fill-opacity": 0.15,
    },
  });
  map.addLayer({
    id: lineLayerId,
    type: "line",
    source: sourceId,
    filter: ["!=", ["get", "is_outline"], true],
    paint: {
      "line-color": "#f59e0b",
      "line-width": 1.5,
    },
  });
  map.addLayer({
    id: outlineLayerId,
    type: "line",
    source: sourceId,
    filter: ["==", ["get", "is_outline"], true],
    paint: {
      "line-color": "#ffffff",
      "line-width": 5,
      "line-opacity": 0.85,
    },
  });

  map.addLayer({
    id: labelLayerId,
    type: "symbol",
    source: sourceId,
    filter: ["!=", ["get", "is_outline"], true],
    layout: {
      "text-field": ["coalesce", ["get", "nog"], ["get", "kvart"], ["get", "kadastrs"]],
      "text-size": 12,
      "text-allow-overlap": true,
      "text-ignore-placement": true,
    },
    paint: {
      "text-color": "#111827",
      "text-halo-color": "#ffffff",
      "text-halo-width": 1.2,
    },
  });

  const bbox = getBbox(feature);
  if (bbox && bbox.length === 4) {
    map.fitBounds(
      [
        [bbox[0], bbox[1]],
        [bbox[2], bbox[3]],
      ],
      { padding: 24, maxZoom: 16 },
    );
  }
  const features = feature.type === "FeatureCollection" ? feature.features || [] : [feature];
  renderDetails(features);

  map.off("click", fillLayerId);
  map.on("click", fillLayerId, (event) => {
    const props = (event.features && event.features[0] && event.features[0].properties) || {};
    const popupContent = buildPopupContent(props);
    new mapboxgl.Popup({ closeButton: true, maxWidth: "360px" })
      .setLngLat(event.lngLat)
      .setHTML(popupContent)
      .addTo(map);
  });
}

function renderDetails(features) {
  const visible = (features || []).filter(
    (feature) => !(feature.properties && feature.properties.is_outline),
  );
  if (visible.length === 0) {
    detailsEl.textContent = "No data loaded.";
    return;
  }
  if (visible.length > 300) {
    detailsEl.innerHTML = `<div class="details-summary">Loaded ${visible.length} nogabali. Zoom in and click on the map to view details.</div>`;
    return;
  }
  const sortedFeatures = [...visible].sort((a, b) => {
    const aNog = Number(a.properties && a.properties.nog);
    const bNog = Number(b.properties && b.properties.nog);
    if (Number.isFinite(aNog) && Number.isFinite(bNog)) {
      return aNog - bNog;
    }
    return 0;
  });

  const blocks = sortedFeatures.map((feature, index) => {
    const props = feature.properties || {};
    const titleParts = [];
    if (props.kadastrs) {
      titleParts.push(`Kadastrs ${props.kadastrs}`);
    }
    if (props.kvart) {
      titleParts.push(`Kvart ${props.kvart}`);
    }
    if (props.nog) {
      titleParts.push(`Nog ${props.nog}`);
    }
    const title = titleParts.length > 0 ? titleParts.join(" · ") : `Nogabals ${index + 1}`;
    const entries = Object.entries(props).sort(([a], [b]) => a.localeCompare(b));
    const rows = entries
      .map(([key, value]) => {
        const safeKey = escapeHtml(key);
        const safeValue = escapeHtml(formatValue(value));
        return `<div class="details-row"><span>${safeKey}</span><span>${safeValue}</span></div>`;
      })
      .join("");
    return `
      <details class="details-block" ${index === 0 ? "open" : ""}>
        <summary>${escapeHtml(title)}</summary>
        <div class="details-grid">${rows}</div>
      </details>
    `;
  });

  const header = `<div class="details-summary">Nogabali: ${visible.length}</div>`;
  detailsEl.innerHTML = header + blocks.join("");
}

function formatValue(value) {
  if (value === null || value === undefined) {
    return "";
  }
  if (typeof value === "number") {
    return Number.isInteger(value) ? value.toString() : value.toFixed(4);
  }
  return String(value);
}

function escapeHtml(value) {
  return String(value)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/\"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function buildPopupContent(properties) {
  const entries = Object.entries(properties || {}).sort(([a], [b]) => a.localeCompare(b));
  const rows = entries
    .map(([key, value]) => {
      return `<tr><th>${escapeHtml(key)}</th><td>${escapeHtml(formatValue(value))}</td></tr>`;
    })
    .join("");
  return `<div class="popup"><table>${rows}</table></div>`;
}

async function fetchLayerFeatures(layerId) {
  const url = `${API_BASE}/layers/${encodeURIComponent(layerId)}/features`;
  console.info("Requesting", url);
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`Layer request failed (${response.status})`);
  }
  return response.json();
}

async function loadLayerControls() {
  if (!layerListEl) return;
  try {
    const payload = await fetchLayers();
    const layers = (payload.layers || []).filter((layer) => layer.id !== "cadastre");
    if (layers.length === 0) {
      layerListEl.textContent = "No extra layers.";
      return;
    }
    layerListEl.innerHTML = "";
    layers.forEach((layer) => {
      overlayLayers.set(layer.id, layer);
      const item = document.createElement("label");
      item.className = "layer-item";
      const display = layer.display || {};
      const swatchColor =
        display.fill_color || display.circle_color || display.line_color || "#94a3b8";
      const swatch = document.createElement("span");
      swatch.className = "layer-swatch";
      swatch.style.backgroundColor = swatchColor;
      const checkbox = document.createElement("input");
      checkbox.type = "checkbox";
      checkbox.dataset.layerId = layer.id;
      const label = document.createElement("span");
      label.textContent = layer.label || layer.id;
      item.appendChild(swatch);
      item.appendChild(checkbox);
      item.appendChild(label);
      layerListEl.appendChild(item);

      checkbox.addEventListener("change", (event) => {
        handleLayerToggle(layer, event.target.checked, checkbox);
      });
    });
  } catch (error) {
    console.error("Layer list failed:", error);
    layerListEl.textContent = "Layer list unavailable.";
  }
}

async function checkHealth() {
  const url = `${API_BASE}/health`;
  try {
    const response = await fetch(url);
    if (!response.ok) {
      setStatus(`API not healthy (${response.status}).`, true);
      return;
    }
    const payload = await response.json();
    console.info("API health:", payload);
    setStatus("API ready.");
  } catch (error) {
    console.error("Health check failed:", error);
    setStatus(`API unreachable at ${API_BASE}`, true);
  }
}

async function bootstrap() {
  await checkHealth();
  if (WINDOW_MAPBOX_TOKEN) {
    const ready = initMap(WINDOW_MAPBOX_TOKEN);
    if (ready) {
      await loadLayerControls();
    }
    return;
  }
  try {
    const cfg = await fetchFrontendConfig();
    const ready = initMap(cfg.mapboxAccessToken || "");
    if (ready) {
      await loadLayerControls();
    }
  } catch (error) {
    console.error("Frontend config failed:", error);
    setStatus("Map config unavailable. Check API and env.", true);
  }
}

bootstrap();

if (enableAllLayersButton) {
  enableAllLayersButton.addEventListener("click", () => {
    setAllLayers(true);
  });
}

if (disableAllLayersButton) {
  disableAllLayersButton.addEventListener("click", () => {
    setAllLayers(false);
  });
}

async function copyText(text, successMessage) {
  try {
    await navigator.clipboard.writeText(text);
    setStatus(successMessage || "Copied.");
  } catch (error) {
    console.error("Copy failed:", error);
    setStatus("Copy failed. See console.", true);
  }
}

if (copyCommandsButton) {
  copyCommandsButton.addEventListener("click", () => {
    copyText(commandsText, "Commands copied.");
  });
}

if (copyCadasterButton) {
  copyCadasterButton.addEventListener("click", () => {
    copyText(testCadaster, "Cadaster copied.");
    input.value = testCadaster;
  });
}

form.addEventListener("submit", async (event) => {
  event.preventDefault();
  const kadastrs = input.value.trim();
  if (!kadastrs) {
    setStatus("Enter a kadastrs number.", true);
    return;
  }
  if (!map) {
    setStatus("Map unavailable: missing Mapbox token.", true);
    return;
  }
  setStatus("Loading...");
  try {
    const feature = await fetchProperty(kadastrs);
    if (!map.isStyleLoaded()) {
      map.once("load", () => {
        renderFeature(feature);
      });
    } else {
      renderFeature(feature);
    }
    setStatus("Loaded.");
  } catch (error) {
    console.error("Lookup failed:", error);
    setStatus(`${error.message} (API: ${API_BASE})`, true);
    detailsEl.textContent = "No data loaded.";
  }
});

if (loadAllButton) {
  loadAllButton.addEventListener("click", async () => {
    const region = regionSelect ? regionSelect.value : "";
    if (!region) {
      setStatus("Select a region to load.", true);
      return;
    }
    if (!map) {
      setStatus("Map unavailable: missing Mapbox token.", true);
      return;
    }
    setStatus("Loading all properties...");
    try {
      const collection = await fetchAll(region);
      if (!map.isStyleLoaded()) {
        map.once("load", () => {
          renderFeature(collection);
        });
      } else {
        renderFeature(collection);
      }
      setStatus("Loaded.");
    } catch (error) {
      console.error("Load all failed:", error);
      setStatus(`${error.message} (API: ${API_BASE})`, true);
      detailsEl.textContent = "No data loaded.";
    }
  });
}
