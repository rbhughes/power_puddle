import maplibregl from "maplibre-gl";
import "maplibre-gl/dist/maplibre-gl.css";

// Charts are hand-rolled SVG; data comes from the committed artifacts
// built by src/scripts/build_site_data.py.
const fmt = (n) => Number(n).toLocaleString("en-US", { maximumFractionDigits: 0 });

const FUEL_COLORS = {
  "Natural Gas": "#eb6834",
  Coal: "#52514e",
  Nuclear: "#4a3aa7",
  Wind: "#2a78d6",
  Solar: "#eda100",
  Hydro: "#1baf7a",
  Oil: "#e34948",
  Biomass: "#008300",
  Other: "#c3c2b7",
  Geothermal: "#e87ba4",
  Unknown: "#c3c2b7",
};
const STACK_ORDER = ["Natural Gas", "Coal", "Nuclear", "Wind", "Solar",
                     "Hydro", "Oil", "Biomass", "Other", "Unknown"];

// ---------- fan chart: annual actuals vs forecast vintages ----------
function drawFan(fc) {
  const svg = document.getElementById("fan");
  const W = 960, H = 420, m = { t: 18, r: 96, b: 30, l: 56 };
  const years = [];
  for (let y = 2016; y <= 2031; y++) years.push(y);

  const annual = (series) => {
    const out = {};
    for (const [k, v] of Object.entries(series)) {
      const y = +k.slice(0, 4);
      (out[y] ??= []).push(v);
    }
    return Object.fromEntries(Object.entries(out)
      .filter(([y, v]) => v.length === 12
        && +y >= years[0] && +y <= years.at(-1))
      .map(([y, v]) => [y, v.reduce((a, b) => a + b, 0)]));
  };
  const actual = annual(fc.actuals);
  const vints = Object.fromEntries(Object.entries(fc.vintages)
    .map(([p, s]) => [p, annual(s)]));

  let lo = Infinity, hi = -Infinity;
  const scan = (o) => Object.values(o).forEach((v) => {
    lo = Math.min(lo, v); hi = Math.max(hi, v);
  });
  scan(actual); Object.values(vints).forEach(scan);
  lo = Math.floor(lo / 10000) * 10000; hi = Math.ceil(hi / 10000) * 10000;

  const x = (y) => m.l + ((y - years[0]) / (years.at(-1) - years[0])) * (W - m.l - m.r);
  const y = (v) => m.t + (1 - (v - lo) / (hi - lo)) * (H - m.t - m.b);
  const path = (o) => Object.keys(o).sort()
    .map((yr, i) => `${i ? "L" : "M"}${x(+yr).toFixed(1)},${y(o[yr]).toFixed(1)}`)
    .join("");

  let s = "";
  for (let v = lo; v <= hi; v += 10000)
    s += `<line x1="${m.l}" x2="${W - m.r}" y1="${y(v)}" y2="${y(v)}"
      stroke="#e1e0d9"/><text x="${m.l - 6}" y="${y(v) + 3}"
      text-anchor="end" font-size="10" fill="#898781">${v / 1000}k</text>`;
  for (const yr of years.filter((v) => v % 3 === 1))
    s += `<text x="${x(yr)}" y="${H - 8}" text-anchor="middle"
      font-size="10" fill="#898781">${yr}</text>`;

  const HILITE = { 2025: "#eb6834", 2026: "#4a3aa7" };
  for (const [pub, o] of Object.entries(vints)) {
    const c = HILITE[pub] ?? "#c3c2b7";
    const wd = HILITE[pub] ? 2.4 : 1.1;
    s += `<path d="${path(o)}" fill="none" stroke="${c}"
      stroke-width="${wd}" opacity="${HILITE[pub] ? 1 : 0.8}"/>`;
    const last = Object.keys(o).sort().at(-1);
    if (HILITE[pub])
      s += `<text x="${x(+last) + 5}" y="${y(o[last]) + 4}" font-size="11"
        font-weight="700" fill="${c}">v${pub}</text>`;
  }
  s += `<path d="${path(actual)}" fill="none" stroke="#2a78d6"
    stroke-width="3.2"/>`;
  const la = Object.keys(actual).sort().at(-1);
  s += `<text x="${x(+la) + 5}" y="${y(actual[la]) + 4}" font-size="11"
    font-weight="700" fill="#2a78d6">actual</text>`;
  s += `<text x="${W - m.r}" y="${m.t - 4}" text-anchor="end"
    font-size="10" fill="#898781">gray = 2016–2024 vintages</text>`;
  svg.innerHTML = s;
}

// ---------- MASE bars ----------
function drawMase(mase) {
  const svg = document.getElementById("masebars");
  const W = 700, H = 220, m = { t: 16, r: 10, b: 40, l: 10 };
  const pubs = Object.keys(mase).sort();
  const bw = (W - m.l - m.r) / pubs.length;
  const hi = Math.max(...pubs.map((p) => mase[p].mase), 2);
  const y = (v) => m.t + (1 - v / hi) * (H - m.t - m.b);
  let s = `<line x1="${m.l}" x2="${W - m.r}" y1="${y(1)}" y2="${y(1)}"
    stroke="#d95f00" stroke-dasharray="4 3"/>
    <text x="${W - m.r}" y="${y(1) - 4}" text-anchor="end" font-size="10"
    fill="#d95f00">MASE = 1 (naïve)</text>`;
  pubs.forEach((p, i) => {
    const v = mase[p].mase;
    const xx = m.l + i * bw;
    const small = mase[p].months < 24;
    s += `<rect x="${xx + 6}" y="${y(v)}" width="${bw - 12}"
      height="${y(0) - y(v)}" rx="3" fill="#2a78d6"
      opacity="${small ? 0.45 : 0.9}"><title>${p} vintage: MASE ${v}
      over ${mase[p].months} months</title></rect>
      <text x="${xx + bw / 2}" y="${y(v) - 4}" text-anchor="middle"
      font-size="11" font-weight="600" fill="#0b0b0b">${v}</text>
      <text x="${xx + bw / 2}" y="${H - 22}" text-anchor="middle"
      font-size="10" fill="#898781">${p}</text>`;
    if (small)
      s += `<text x="${xx + bw / 2}" y="${H - 10}" text-anchor="middle"
        font-size="8" fill="#898781">(${mase[p].months} mo)</text>`;
  });
  svg.innerHTML = s;
}

// ---------- stacked area mix charts ----------
function drawMix(id, series) {
  const svg = document.getElementById(id);
  const W = 470, H = 260, m = { t: 10, r: 8, b: 24, l: 34 };
  const months = [...new Set(Object.values(series)
    .flatMap((s) => Object.keys(s)))].sort();
  const cats = STACK_ORDER.filter((c) => series[c]);
  const totals = months.map((mo) =>
    cats.reduce((a, c) => a + (series[c][mo] ?? 0), 0));
  const hi = Math.ceil(Math.max(...totals) / 50) * 50;
  const x = (i) => m.l + (i / (months.length - 1)) * (W - m.l - m.r);
  const y = (v) => m.t + (1 - v / hi) * (H - m.t - m.b);

  let s = "";
  for (let v = 0; v <= hi; v += hi / 4)
    s += `<text x="${m.l - 4}" y="${y(v) + 3}" text-anchor="end"
      font-size="9" fill="#898781">${Math.round(v)}</text>`;
  for (let i = 0; i < months.length; i += 48)
    s += `<text x="${x(i)}" y="${H - 8}" text-anchor="middle"
      font-size="9" fill="#898781">${months[i].slice(0, 4)}</text>`;

  const cum = months.map(() => 0);
  for (const c of cats) {
    const tops = months.map((mo, i) => cum[i] + (series[c][mo] ?? 0));
    let d = `M${x(0)},${y(cum[0])}`;
    tops.forEach((v, i) => (d += `L${x(i)},${y(v)}`));
    for (let i = months.length - 1; i >= 0; i--)
      d += `L${x(i)},${y(cum[i])}`;
    d += "Z";
    s += `<path d="${d}" fill="${FUEL_COLORS[c]}" opacity="0.9">
      <title>${c}</title></path>`;
    tops.forEach((v, i) => (cum[i] = v));
  }
  svg.innerHTML = s;
}

// ---------- map ----------
function drawMap(data) {
  const map = new maplibregl.Map({
    container: "map",
    style: "https://tiles.openfreemap.org/styles/positron",
    bounds: [
      [-91.7, 36.9],
      [-87.0, 42.6],
    ],
    fitBoundsOptions: { padding: 16 },
    attributionControl: { compact: true },
    cooperativeGestures: true,
  });
  map.addControl(new maplibregl.NavigationControl({ showCompass: false }));
  map.on("error", (e) => console.error("[map]", e.error ?? e));
  window._map = map;

  map.on("load", () => {
    map.addSource("plants", {
      type: "geojson",
      data: {
        type: "FeatureCollection",
        features: data.plants.map((p) => ({
          type: "Feature",
          geometry: { type: "Point", coordinates: [p.lon, p.lat] },
          properties: { ...p, color: FUEL_COLORS[p.fuel] ?? "#c3c2b7" },
        })),
      },
    });
    map.addLayer({
      id: "plants",
      type: "circle",
      source: "plants",
      paint: {
        "circle-color": ["get", "color"],
        "circle-radius": [
          "interpolate", ["linear"], ["sqrt", ["get", "mwh"]],
          0, 2.5, 5000, 5, 32000, 16,
        ],
        "circle-opacity": 0.82,
        "circle-stroke-color": "#fcfcfb",
        "circle-stroke-width": 0.8,
      },
    });
    map.addSource("dcs", {
      type: "geojson",
      data: {
        type: "FeatureCollection",
        features: data.data_centers.map((d) => ({
          type: "Feature",
          geometry: { type: "Point", coordinates: [d.lon, d.lat] },
          properties: { n: d.n },
        })),
      },
    });
    map.addLayer({
      id: "dcs",
      type: "circle",
      source: "dcs",
      paint: {
        "circle-color": "#eda100",
        "circle-radius": 3.2,
        "circle-stroke-color": "#0b0b0b",
        "circle-stroke-width": 0.8,
      },
    });

    const popup = new maplibregl.Popup({ closeButton: false, closeOnClick: false });
    map.on("mousemove", "plants", (e) => {
      map.getCanvas().style.cursor = "pointer";
      const p = e.features[0].properties;
      popup.setLngLat(e.features[0].geometry.coordinates).setHTML(
        `<div class="pop-name">${p.n}</div>
         <div class="pop-sub">${p.fuel} · ${fmt(p.mwh / 1000)} GWh cumulative</div>`
      ).addTo(map);
    });
    map.on("mousemove", "dcs", (e) => {
      map.getCanvas().style.cursor = "pointer";
      popup.setLngLat(e.features[0].geometry.coordinates).setHTML(
        `<div class="pop-name">${e.features[0].properties.n}</div>
         <div class="pop-sub">data center (July 2025 survey)</div>`
      ).addTo(map);
    });
    for (const l of ["plants", "dcs"])
      map.on("mouseleave", l, () => {
        map.getCanvas().style.cursor = "";
        popup.remove();
      });
  });
}

// ---------- boot (independent of map load) ----------
Promise.all([
  fetch("/data/forecast.json").then((r) => r.json()),
  fetch("/data/us_generation.json").then((r) => r.json()),
  fetch("/data/il_generation.json").then((r) => r.json()),
  fetch("/data/map.json").then((r) => r.json()),
]).then(([fc, us, il, mapData]) => {
  drawFan(fc);
  drawMase(fc.mase);
  drawMix("mix-us", us);
  drawMix("mix-il", il);
  document.getElementById("fuel-legend").innerHTML = STACK_ORDER
    .filter((c) => us[c])
    .map((c) => `<span class="item"><i style="background:${
      FUEL_COLORS[c]}"></i>${c}</span>`)
    .join("");
  document.getElementById("fan-caption").textContent +=
    ` Actuals through ${fc.actuals_through}; recent months provisional.`;
  drawMap(mapData);
});
