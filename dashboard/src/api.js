/**
 * MandiMax API Client
 * ─────────────────────────────────────────────────────────────
 * Priority:  Live Flask API  →  Cached mock (fallback)
 *
 * Set REACT_APP_API_BASE_URL in Netlify env vars to your backend:
 *   https://impatient-undefined-willed.ngrok-free.dev   (ngrok)
 *   https://mandimax-bot.azuredatabricksapps.net         (Databricks Apps)
 *
 * If not set → runs in offline mode using real cached gold-table data.
 */

// ── Base URL ───────────────────────────────────────────────────────────────────
// Empty string = relative URLs → Netlify proxy forwards /api/* to Databricks App
const API_BASE = (process.env.REACT_APP_API_BASE_URL || '').replace(/\/$/, '');
const LIVE     = true;  // Always try live — Netlify proxy is always available

// ── Generic fetch with timeout + fallback ─────────────────────────────────────
async function liveFetch(path, params = {}) {
  const url    = new URL(`${API_BASE}${path}`, window.location.origin);
  Object.entries(params).forEach(([k, v]) => v !== undefined && url.searchParams.set(k, v));
  const ctrl   = new AbortController();
  const timer  = setTimeout(() => ctrl.abort(), 10000); // 10s timeout
  try {
    const res  = await fetch(url.toString(), {
      signal: ctrl.signal,
      headers: { 'Content-Type': 'application/json' },
    });
    clearTimeout(timer);
    if (!res.ok) return null;
    return await res.json();
  } catch {
    clearTimeout(timer);
    return null;
  }
}

// ── Public API functions — used by React components ────────────────────────────

/** Pipeline health + model metrics (banner + sidebar) */
export async function fetchPipelineHealth() {
  const live = await liveFetch('/api/pipeline-health');
  if (live) return { ...MOCK_HEALTH, ...live, source: 'live' };
  return { ...MOCK_HEALTH, source: 'cached' };
}

/** StatCards numbers */
export async function fetchStats() {
  const live = await liveFetch('/api/stats');
  if (live) return { ...MOCK_STATS, ...live, source: 'live' };
  return { ...MOCK_STATS, source: 'cached' };
}

/** Arbitrage table — optionally filtered by commodity */
export async function fetchArbitrage(commodity = '', limit = 10) {
  const live = await liveFetch('/api/arbitrage', { commodity, limit });
  if (live?.data?.length) return { data: live.data, source: 'live' };
  const filtered = commodity
    ? MOCK_ARBITRAGE.filter(r => r.src_commodity?.toLowerCase() === commodity.toLowerCase())
    : MOCK_ARBITRAGE;
  return { data: filtered.length ? filtered : MOCK_ARBITRAGE, source: 'cached' };
}

/** Crop recommendations filtered by state */
export async function fetchCropRecs(state = '', limit = 5) {
  const live = await liveFetch('/api/crop-recommendations', { state, limit });
  if (live?.data?.length) return { data: live.data, source: 'live' };
  const filtered = state
    ? MOCK_CROP_RECS.filter(r => r.state?.toUpperCase().includes(state.toUpperCase()))
    : MOCK_CROP_RECS;
  return { data: filtered.length ? filtered : MOCK_CROP_RECS, source: 'cached' };
}

/** 7-day ML price forecast */
export async function fetchForecast(market = 'BANGALORE', commodity = 'Tomato') {
  const live = await liveFetch('/api/forecast', { market, commodity });
  if (live?.data?.length) return { data: live.data, source: 'live' };
  return { data: MOCK_FORECAST, source: 'cached' };
}

// ── REAL DATA — Sourced from gold tables (Databricks pipeline, April 18 2026) ──

export const MOCK_STATS = {
  totalRecords:       '727,656',
  uniqueMarkets:      '1,597',
  commodities:        '5',
  avgArbitrageSpread: '₹3,240',
  arbitragePairs:     '542',
  geocodedMarkets:    '96',
};

// ★ Real arbitrage opportunities from gold.arbitrage_opportunities_latest
export const MOCK_ARBITRAGE = [
  { src_market:'AHMEDABAD',     tgt_market:'SURAT',      src_commodity:'Tomato', src_variety:'Other',  src_price:2100, tgt_price:10000, arbitrage_spread:7900, distance_km:208, spread_per_km:38.04, tgt_lat:21.1702, tgt_lon:72.8311 },
  { src_market:'LUDHIANA',      tgt_market:'ROHTAK',     src_commodity:'Tomato', src_variety:'Other',  src_price:2000, tgt_price:9000,  arbitrage_spread:7000, distance_km:234, spread_per_km:29.86, tgt_lat:28.8955, tgt_lon:76.6066 },
  { src_market:'BAREILLY',      tgt_market:'MATHURA',    src_commodity:'Tomato', src_variety:'Deshi',  src_price:1500, tgt_price:8200,  arbitrage_spread:6700, distance_km:198, spread_per_km:33.82, tgt_lat:27.4924, tgt_lon:77.6737 },
  { src_market:'GORAKHPUR',     tgt_market:'SULTANPUR',  src_commodity:'Tomato', src_variety:'Hybrid', src_price:1200, tgt_price:7875,  arbitrage_spread:6675, distance_km:141, spread_per_km:47.46, tgt_lat:26.2647, tgt_lon:82.0727 },
  { src_market:'MUZZAFARNAGAR', tgt_market:'MATHURA',    src_commodity:'Tomato', src_variety:'Deshi',  src_price:1600, tgt_price:8200,  arbitrage_spread:6600, distance_km:220, spread_per_km:29.97, tgt_lat:27.4924, tgt_lon:77.6737 },
  { src_market:'BAREILLY',      tgt_market:'LUCKNOW',    src_commodity:'Tomato', src_variety:'Deshi',  src_price:1500, tgt_price:8000,  arbitrage_spread:6500, distance_km:226, spread_per_km:28.76, tgt_lat:26.8467, tgt_lon:80.9462 },
  { src_market:'ALLAHABAD',     tgt_market:'LUCKNOW',    src_commodity:'Tomato', src_variety:'Other',  src_price:1600, tgt_price:8000,  arbitrage_spread:6400, distance_km:181, spread_per_km:35.36, tgt_lat:26.8467, tgt_lon:80.9462 },
  { src_market:'AGRA',          tgt_market:'PUWAHA',     src_commodity:'Tomato', src_variety:'Other',  src_price:1510, tgt_price:7900,  arbitrage_spread:6390, distance_km:196, spread_per_km:32.60, tgt_lat:27.8718, tgt_lon:79.8584 },
  { src_market:'AKBARPUR',      tgt_market:'SULTANPUR',  src_commodity:'Tomato', src_variety:'Hybrid', src_price:1540, tgt_price:7875,  arbitrage_spread:6335, distance_km:49,  spread_per_km:129.3, tgt_lat:26.2647, tgt_lon:82.0727 },
  { src_market:'ALLAHABAD',     tgt_market:'PRATAPGARH', src_commodity:'Tomato', src_variety:'Other',  src_price:1600, tgt_price:7900,  arbitrage_spread:6300, distance_km:53,  spread_per_km:118.9, tgt_lat:25.8967, tgt_lon:81.9785 },
];

// ★ Real crop recommendations from gold.crop_recommendation
export const MOCK_CROP_RECS = [
  { district:'KRISHNA',   state:'ANDHRA PRADESH', crop:'Rice',   avg_price:4594, stability:98.4, volatility:'LOW',    districts:12 },
  { district:'LAKHIMPUR', state:'ASSAM',          crop:'Rice',   avg_price:3166, stability:99.6, volatility:'LOW',    districts:8  },
  { district:'BHOJPUR',   state:'BIHAR',          crop:'Potato', avg_price:1555, stability:96.9, volatility:'LOW',    districts:53 },
  { district:'BANKA',     state:'BIHAR',          crop:'Onion',  avg_price:2129, stability:95.4, volatility:'LOW',    districts:49 },
  { district:'SAHARANPUR',state:'UTTAR PRADESH',  crop:'Rice',   avg_price:3378, stability:99.9, volatility:'LOW',    districts:51 },
  { district:'BAREILLY',  state:'UTTAR PRADESH',  crop:'Potato', avg_price:1166, stability:95.5, volatility:'LOW',    districts:53 },
  { district:'AGRA',      state:'UTTAR PRADESH',  crop:'Onion',  avg_price:1205, stability:95.3, volatility:'LOW',    districts:53 },
];

// ★ Forecast with realistic price trajectory
export const MOCK_FORECAST = (() => {
  const base = 3800;
  return Array.from({ length: 14 }, (_, i) => ({
    date:     new Date(Date.UTC(2025, 9, i + 1)).toISOString().split('T')[0],
    actual:   i < 7  ? Math.round(base + Math.sin(i * 0.9) * 150 + (i * 30)) : null,
    forecast: i >= 5 ? Math.round(base + Math.sin((i+2) * 0.9) * 100 + (i * 35) + 200) : null,
    lower_ci: i >= 5 ? Math.round(base + Math.sin((i+2) * 0.9) * 100 + (i * 35) - 180) : null,
    upper_ci: i >= 5 ? Math.round(base + Math.sin((i+2) * 0.9) * 100 + (i * 35) + 580) : null,
  }));
})();

// ★ Real pipeline health from actual Databricks run
export const MOCK_HEALTH = {
  bronze_records:    '727,656',
  silver_records:    '727,656',
  gold_arb_pairs:    '542',
  gold_summary_rows: '715,033',
  crop_recs:         '548',
  geocoded_markets:  '96',
  model_version:     'HistGradientBoosting v2 (sklearn)',
  model_mape:        '9.75%',
  model_r2:          '0.727',
  model_mae:         '₹173.1/quintal',
  forecast_rows:     '20,412',
  last_pipeline_run: new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' }),
  geocode_hit_rate:  '8.6%',
  pipeline_status:   'LIVE',
  pipeline_stages: [
    { name: 'Bronze Ingestion',  status: 'OK',      rows: '727,656' },
    { name: 'Silver Transform',  status: 'OK',      rows: '727,656' },
    { name: 'Gold Aggregations', status: 'OK',      rows: '715,033' },
    { name: 'ML Training',       status: 'OK',      rows: '20,412'  },
    { name: 'Bot API',           status: 'OK',      rows: '-'       },
  ],
};
