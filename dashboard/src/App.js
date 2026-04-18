import React, { useState, useEffect } from 'react';
import './index.css';
import PipelineHealthBanner from './components/PipelineHealthBanner';
import StatCards from './components/StatCards';
import HeatmapPanel from './components/HeatmapPanel';
import ForecastChart from './components/ForecastChart';
import ArbitrageTable from './components/ArbitrageTable';
import CropRecommendationsGrid from './components/CropRecommendationsGrid';
import { MOCK_HEALTH, MOCK_STATS, MOCK_ARBITRAGE, MOCK_FORECAST, MOCK_CROP_RECS } from './api';
import ChatBot from './components/ChatBot';

const NAV_ITEMS = [
  { id: 'overview',   icon: '📊', label: 'Overview' },
  { id: 'heatmap',    icon: '🗺️',  label: 'Price Heatmap' },
  { id: 'arbitrage',  icon: '⚖️',  label: 'Arbitrage' },
  { id: 'forecast',   icon: '📈', label: '7-Day Forecast' },
  { id: 'crops',      icon: '🌾', label: 'Crop Advisory' },
];

const COMMODITIES = ['Tomato', 'Onion', 'Wheat', 'Potato', 'Rice'];
const STATES      = ['Uttar Pradesh', 'Gujarat', 'Punjab', 'Haryana', 'Bihar', 'Andhra Pradesh', 'Assam'];

function App() {
  const [activeNav,   setActiveNav]   = useState('overview');
  const [commodity,   setCommodity]   = useState('Tomato');       // Most arbitrage data
  const [state,       setState]       = useState('Uttar Pradesh'); // Most APMC coverage
  const [health,      setHealth]      = useState(MOCK_HEALTH);
  const [lastUpdated, setLastUpdated] = useState(new Date().toLocaleTimeString('en-IN'));

  // Simulate live data refresh every 60s
  useEffect(() => {
    const timer = setInterval(() => {
      setLastUpdated(new Date().toLocaleTimeString('en-IN'));
    }, 60000);
    return () => clearInterval(timer);
  }, []);

  return (
    <div className="app-shell">
      {/* ── Tricolor Accent ─────────────────────────────────── */}
      <div className="tricolor-bar" />

      {/* ── Header ──────────────────────────────────────────── */}
      <header className="header">
        <div className="header-logo">
          <div className="header-logo-icon">🌾</div>
          <div className="header-logo-text">
            <span className="header-logo-name">Mandi-Max</span>
            <span className="header-logo-tagline">FPO Intelligence Dashboard</span>
          </div>
        </div>

        <div className="header-meta">
          <div className="data-freshness-badge">
            <div className="pulse-dot" />
            <span>Live · Updated {lastUpdated}</span>
          </div>

          {/* Commodity Filter (global) */}
          <select
            id="global-commodity-filter"
            className="filter-select"
            value={commodity}
            onChange={e => setCommodity(e.target.value)}
          >
            {COMMODITIES.map(c => (
              <option key={c} value={c}>{c}</option>
            ))}
          </select>

          {/* State Filter */}
          <select
            id="global-state-filter"
            className="filter-select"
            value={state}
            onChange={e => setState(e.target.value)}
          >
            {STATES.map(s => (
              <option key={s} value={s}>{s}</option>
            ))}
          </select>
        </div>
      </header>

      <div className="app-body">
        {/* ── Sidebar ──────────────────────────────────────── */}
        <nav className="sidebar">
          <span className="sidebar-section-label">Navigation</span>
          {NAV_ITEMS.map(item => (
            <button
              key={item.id}
              id={`nav-${item.id}`}
              className={`sidebar-nav-item ${activeNav === item.id ? 'active' : ''}`}
              onClick={() => setActiveNav(item.id)}
            >
              <span className="sidebar-nav-icon">{item.icon}</span>
              {item.label}
            </button>
          ))}

          <span className="sidebar-section-label">Model</span>
          <div style={{ padding: '10px 12px' }}>
            <div style={{ fontSize: '11px', color: 'var(--text-muted)', marginBottom: '4px' }}>
              MLflow Model
            </div>
            <div style={{ fontSize: '13px', fontWeight: 600, color: 'var(--color-info)', fontFamily: 'var(--font-mono)' }}>
              {health.model_version}
            </div>
            <div style={{ fontSize: '11px', color: 'var(--text-muted)', marginTop: '8px', marginBottom: '4px' }}>
              MAPE
            </div>
            <div style={{ fontSize: '13px', fontWeight: 600, color: 'var(--color-success)', fontFamily: 'var(--font-mono)' }}>
              {health.model_mape}
            </div>
          </div>
        </nav>

        {/* ── Main Content ──────────────────────────────────── */}
        <main className="main-content">
          {/* Pipeline Health Banner — always visible */}
          <PipelineHealthBanner health={health} />

          {/* Overview Page */}
          {activeNav === 'overview' && (
            <>
              <div className="page-header">
                <h1 className="page-title">Overview</h1>
                <p className="page-subtitle">
                  Regional price intelligence for {commodity} across {state}
                </p>
              </div>
              <StatCards stats={MOCK_STATS} commodity={commodity} />
              <div className="dashboard-grid three-col">
                <ForecastChart data={MOCK_FORECAST} commodity={commodity} />
                <ArbitrageTable data={MOCK_ARBITRAGE.filter(d => d.src_commodity === commodity)} compact />
              </div>
              <HeatmapPanel commodity={commodity} state={state} compact />
            </>
          )}

          {/* Heatmap Page */}
          {activeNav === 'heatmap' && (
            <>
              <div className="page-header">
                <h1 className="page-title">Price Heatmap</h1>
                <p className="page-subtitle">
                  Modal price distribution — {commodity} — {state}
                </p>
              </div>
              <HeatmapPanel commodity={commodity} state={state} fullHeight />
            </>
          )}

          {/* Arbitrage Page */}
          {activeNav === 'arbitrage' && (
            <>
              <div className="page-header">
                <h1 className="page-title">Arbitrage Opportunities</h1>
                <p className="page-subtitle">
                  Real-time spatial spread analysis — {commodity}
                </p>
              </div>
              <ArbitrageTable data={MOCK_ARBITRAGE} fullView />
            </>
          )}

          {/* Forecast Page */}
          {activeNav === 'forecast' && (
            <>
              <div className="page-header">
                <h1 className="page-title">7-Day Price Forecast</h1>
                <p className="page-subtitle">
                  GBT Regressor predictions — {commodity} — MLflow Model {health.model_version}
                </p>
              </div>
              <ForecastChart data={MOCK_FORECAST} commodity={commodity} fullView modelHealth={health} />
            </>
          )}

          {/* Crop Advisory Page */}
          {activeNav === 'crops' && (
            <>
              <div className="page-header">
                <h1 className="page-title">Seasonal Crop Advisory</h1>
                <p className="page-subtitle">
                  Ranked by price stability and expected returns — {state}
                </p>
              </div>
              <CropRecommendationsGrid data={MOCK_CROP_RECS} state={state} />
            </>
          )}
        </main>
      </div>

      {/* ── WhatsApp-style Bot (floats over all pages) ─── */}
      <ChatBot />
    </div>
  );
}

export default App;
