import React from 'react';

const CARD_CONFIG = [
  { key: 'totalRecords',        label: 'Total Records',       icon: '🗄️',  delta: '+12K today',      dir: 'up'   },
  { key: 'uniqueMarkets',       label: 'Active Markets',      icon: '📍',  delta: '+3 this week',    dir: 'up'   },
  { key: 'commodities',         label: 'Commodities Tracked', icon: '🌾',  delta: 'Stable',          dir: null   },
  { key: 'avgArbitrageSpread',  label: 'Avg Arbitrage Spread',icon: '⚖️',  delta: '+₹28 vs last week',dir: 'up'  },
];

export default function StatCards({ stats, commodity }) {
  return (
    <div className="stat-grid" role="region" aria-label="Key statistics">
      {CARD_CONFIG.map(card => (
        <div className="stat-card" key={card.key} id={`stat-${card.key}`}>
          <div className="stat-card-label">
            {card.icon} {card.label}
          </div>
          <div className="stat-card-value">
            {stats?.[card.key] ?? '—'}
          </div>
          {card.delta && (
            <div className={`stat-card-delta ${card.dir ?? ''}`}>
              {card.dir === 'up' ? '▲' : card.dir === 'down' ? '▼' : ''}
              {card.delta}
            </div>
          )}
        </div>
      ))}
    </div>
  );
}
