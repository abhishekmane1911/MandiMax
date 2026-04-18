import React, { useState } from 'react';

export default function ArbitrageTable({ data = [], compact = false, fullView = false }) {
  const [sortCol, setSortCol] = useState('arbitrage_spread');
  const [sortDir, setSortDir] = useState('desc');

  const handleSort = col => {
    if (sortCol === col) setSortDir(d => d === 'asc' ? 'desc' : 'asc');
    else { setSortCol(col); setSortDir('desc'); }
  };

  const sortedData = [...data].sort((a, b) => {
    const mult = sortDir === 'asc' ? 1 : -1;
    return (a[sortCol] - b[sortCol]) * mult;
  });

  const display = compact ? sortedData.slice(0, 5) : sortedData;

  const SortIcon = ({ col }) => sortCol === col
    ? (sortDir === 'asc' ? ' ▲' : ' ▼') : ' ·';

  return (
    <div className="panel" id="arbitrage-table-panel">
      <div className="panel-header">
        <div className="panel-title">
          <span className="panel-title-icon">⚖️</span>
          Arbitrage Opportunities
        </div>
        <span className="panel-badge live">Live</span>
      </div>
      <div className="panel-body" style={{ padding: 0 }}>
        {display.length === 0 ? (
          <div style={{ padding: '24px', textAlign: 'center', color: 'var(--text-muted)' }}>
            No arbitrage data for selected commodity.
          </div>
        ) : (
          <table className="arb-table" role="table" aria-label="Arbitrage opportunities">
            <thead>
              <tr>
                <th onClick={() => handleSort('src_market')} style={{ cursor: 'pointer' }}>
                  From Mandi<SortIcon col="src_market" />
                </th>
                <th onClick={() => handleSort('tgt_market')} style={{ cursor: 'pointer' }}>
                  To Mandi<SortIcon col="tgt_market" />
                </th>
                {!compact && <th>Crop</th>}
                <th onClick={() => handleSort('src_price')} style={{ cursor: 'pointer' }}>
                  From ₹<SortIcon col="src_price" />
                </th>
                <th onClick={() => handleSort('tgt_price')} style={{ cursor: 'pointer' }}>
                  To ₹<SortIcon col="tgt_price" />
                </th>
                <th onClick={() => handleSort('arbitrage_spread')} style={{ cursor: 'pointer' }}>
                  Spread<SortIcon col="arbitrage_spread" />
                </th>
                <th onClick={() => handleSort('distance_km')} style={{ cursor: 'pointer' }}>
                  Dist (km)<SortIcon col="distance_km" />
                </th>
              </tr>
            </thead>
            <tbody>
              {display.map((row, i) => (
                <tr key={i} id={`arb-row-${i}`}>
                  <td style={{ fontWeight: 600, color: 'var(--text-primary)' }}>
                    {row.src_market}
                  </td>
                  <td style={{ color: 'var(--color-india-green-light)', fontWeight: 600 }}>
                    {row.tgt_market}
                  </td>
                  {!compact && <td>{row.src_commodity}</td>}
                  <td className="text-mono">₹{Number(row.src_price).toLocaleString('en-IN')}</td>
                  <td className="text-mono" style={{ color: 'var(--color-success)' }}>
                    ₹{Number(row.tgt_price).toLocaleString('en-IN')}
                  </td>
                  <td>
                    <span className="spread-badge">
                      +₹{Number(row.arbitrage_spread).toLocaleString('en-IN')}
                    </span>
                  </td>
                  <td className="text-mono text-muted">
                    {Number(row.distance_km).toFixed(0)} km
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </div>
  );
}
