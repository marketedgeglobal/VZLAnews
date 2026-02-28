import fs from "fs";
import path from "path";

const BASE = "https://www.imf.org/external/datamapper/api/v1";
const OUT = path.join(process.cwd(), "docs", "data", "imf_ven.json");

const METRICS = [
  { code: "NGDP_RPCH", label: "Real GDP growth" },
  { code: "NGDPD", label: "GDP, current prices" },
  { code: "NGDPDPC", label: "GDP per capita" },
  { code: "PCPIPCH", label: "Inflation (avg)" },
  { code: "LUR", label: "Unemployment rate" },
  { code: "BCA_NGDPD", label: "Current account (% GDP)" },
  { code: "GGXCNL_NGDP", label: "Fiscal balance (% GDP)" },
  { code: "GGXWDG_NGDP", label: "Government debt (% GDP)" }
];

function buildPeriods() {
  const y = new Date().getUTCFullYear();
  const years = [];
  for (let i = y - 15; i <= y + 5; i++) years.push(String(i));
  return years.join(",");
}

async function fetchJSON(url) {
  const res = await fetch(url, {
    headers: { "user-agent": "marketedge-vzlanews/1.0" }
  });
  if (!res.ok) throw new Error(`Fetch failed: ${url}`);
  return res.json();
}

function normalizeSeries(obj) {
  return Object.entries(obj || {})
    .map(([y, v]) => ({ year: Number(y), value: Number(v) }))
    .filter((d) => Number.isFinite(d.year) && Number.isFinite(d.value))
    .sort((a, b) => a.year - b.year);
}

function computeDelta(series) {
  if (series.length < 2) return { latest: null, prior: null, delta: null };
  const latest = series[series.length - 1];
  const prior = series[series.length - 2];
  return { latest, prior, delta: latest.value - prior.value };
}

async function main() {
  const catalog = await fetchJSON(`${BASE}/indicators`);
  const periods = buildPeriods();
  const metrics = [];

  for (const m of METRICS) {
    const meta = (catalog && catalog.indicators && catalog.indicators[m.code]) || {};
    const data = await fetchJSON(`${BASE}/${m.code}/VEN?periods=${periods}`);
    const raw = data && data.values && data.values[m.code] && data.values[m.code].VEN ? data.values[m.code].VEN : {};
    const series = normalizeSeries(raw);
    const { latest, prior, delta } = computeDelta(series);

    metrics.push({
      code: m.code,
      label: meta.label || m.label,
      unit: meta.unit || "",
      dataset: meta.dataset || "",
      series,
      latest,
      prior,
      delta
    });
  }

  const output = {
    source: "IMF DataMapper API",
    country: "VEN",
    asOf: new Date().toISOString(),
    metrics
  };

  fs.mkdirSync(path.dirname(OUT), { recursive: true });
  fs.writeFileSync(OUT, JSON.stringify(output, null, 2));
  console.log("IMF data updated.");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
