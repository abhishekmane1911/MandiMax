import React, { useMemo } from 'react';

const VOLATILITY_CONFIG = {
  LOW:    { color: 'var(--color-success)', bg: 'rgba(34,197,94,0.12)',  border: 'rgba(34,197,94,0.25)'   },
  MEDIUM: { color: 'var(--color-warning)', bg: 'rgba(245,158,11,0.12)', border: 'rgba(245,158,11,0.25)'  },
  HIGH:   { color: 'var(--color-danger)',  bg: 'rgba(239,68,68,0.12)',   border: 'rgba(239,68,68,0.25)'   },
};

function VolatilityBadge({ level }) {
  const cfg = VOLATILITY_CONFIG[level] ?? VOLATILITY_CONFIG.MEDIUM;
  return (
    <span style={{
      background: cfg.bg,
      color: cfg.color,
      border: `1px solid ${cfg.border}`,
      borderRadius: 6,
      padding: '2px 8px',
      fontSize: 11,
      fontWeight: 600,
    }}>
      {level}
    </span>
  );
}

function StabilityBar({ score }) {
  const pct = Math.max(0, Math.min(100, score));
  const color = pct > 75 ? 'var(--color-success)' : pct > 50 ? 'var(--color-warning)' : 'var(--color-danger)';
  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
      <div style={{
        flex: 1, height: 6, background: 'var(--bg-elevated)', borderRadius: 3, overflow: 'hidden'
      }}>
        <div style={{
          width: `${pct}%`, height: '100%', background: color,
          borderRadius: 3, transition: 'width 0.5s ease'
        }} />
      </div>
      <span style={{ fontSize: 11, color: 'var(--text-muted)', minWidth: 28, fontFamily: 'var(--font-mono)' }}>
        {pct}
      </span>
    </div>
  );
}

export default function CropRecommendationsGrid({ data = [], state }) {
  // Group by district, show top 3 crops per district
  const grouped = useMemo(() => {
    const map = {};
    data.forEach(row => {
      if (!map[row.district]) map[row.district] = [];
      if (row.crop_rank <= 3) map[row.district].push(row);
    });
    // Sort each district's crops by rank
    Object.keys(map).forEach(d => map[d].sort((a, b) => a.crop_rank - b.crop_rank));
    return map;
  }, [data]);

  const rankEmoji = r => r === 1 ? '🥇' : r === 2 ? '🥈' : '🥉';

  return (
    <div id="crop-recommendations-section">
      <div className="panel" style={{ marginBottom: 0 }}>
        <div className="panel-header">
          <div className="panel-title">
            <span className="panel-title-icon">🌾</span>
            Seasonal Crop Advisory — {state}
          </div>
          <span className="panel-badge ml">Price-Stability Ranked</span>
        </div>
        <div className="panel-body" style={{ padding: 0 }}>
          <table className="arb-table" role="table" aria-label="Crop recommendations">
            <thead>
              <tr>
                <th>Rank</th>
                <th>District</th>
                <th>Recommended Crop</th>
                <th>Avg Price (30d)</th>
                <th>Volatility</th>
                <th>Stability Score</th>
              </tr>
            </thead>
            <tbody>
              {data.map((row, i) => (
                <tr key={i} id={`crop-rec-${row.district}-${row.crop_rank}`}>
                  <td>{rankEmoji(row.crop_rank)} #{row.crop_rank}</td>
                  <td style={{ fontWeight: 600, color: 'var(--text-primary)' }}>{row.district}</td>
                  <td style={{ color: 'var(--color-saffron)', fontWeight: 600 }}>{row.commodity}</td>
                  <td className="text-mono">₹{Number(row.avg_price).toLocaleString('en-IN')}/q</td>
                  <td><VolatilityBadge level={row.volatility} /></td>
                  <td style={{ minWidth: 140 }}><StabilityBar score={row.stability} /></td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* Grouped District Cards */}
      <div style={{
        display: 'grid',
        gridTemplateColumns: 'repeat(auto-fill, minmax(280px, 1fr))',
        gap: 16,
        marginTop: 20
      }}>
        {Object.entries(grouped).map(([district, crops]) => (
          <div
            className="panel"
            key={district}
            id={`district-card-${district.toLowerCase()}`}
          >
            <div className="panel-header" style={{ borderBottom: '1px solid var(--border-subtle)' }}>
              <div className="panel-title" style={{ fontSize: 13 }}>
                📍 {district}
              </div>
            </div>
            <div className="panel-body">
              {crops.map((crop, i) => (
                <div key={i} style={{
                  display: 'flex', alignItems: 'center', justifyContent: 'space-between',
                  padding: '8px 0',
                  borderBottom: i < crops.length - 1 ? '1px solid var(--border-subtle)' : 'none'
                }}>
                  <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                    <span>{rankEmoji(crop.crop_rank)}</span>
                    <span style={{ fontWeight: 600, fontSize: 13 }}>{crop.commodity}</span>
                  </div>
                  <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                    <span style={{ fontFamily: 'var(--font-mono)', fontSize: 13, color: 'var(--color-saffron)' }}>
                      ₹{Number(crop.avg_price).toLocaleString('en-IN')}
                    </span>
                    <VolatilityBadge level={crop.volatility} />
                  </div>
                </div>
              ))}
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}
