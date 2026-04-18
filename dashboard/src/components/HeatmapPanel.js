import React, { useMemo } from 'react';
import { MapContainer, TileLayer, CircleMarker, Tooltip, Polyline } from 'react-leaflet';
import { MOCK_ARBITRAGE } from '../api';

// Center of India
const INDIA_CENTER = [20.5937, 78.9629];

// Market coordinates for demo (hardcoded subset — real data comes from gold.market_geocodes_lookup)
const DEMO_COORDS = {
  'UJJAIN':    [23.1828, 75.7772],
  'INDORE':    [22.7196, 75.8577],
  'BHOPAL':    [23.2599, 77.4126],
  'DEWAS':     [22.9676, 76.0534],
  'RATLAM':    [23.3325, 75.0372],
  'MANDSAUR':  [24.0760, 75.0712],
  'SEHORE':    [23.2017, 77.0855],
  'NASHIK':    [19.9975, 73.7898],
  'PUNE':      [18.5204, 73.8567],
  'AURANGABAD':[19.8762, 75.3433],
};

function getColor(spread) {
  if (spread > 350) return '#22C55E';
  if (spread > 250) return '#F59E0B';
  return '#9BA8C0';
}

export default function HeatmapPanel({ commodity, state, compact = false, fullHeight = false }) {
  const filteredArb = useMemo(() =>
    MOCK_ARBITRAGE.filter(d => d.src_commodity === commodity),
    [commodity]
  );

  const mapHeight = fullHeight ? 560 : compact ? 320 : 400;

  return (
    <div className="panel" id="heatmap-panel">
      <div className="panel-header">
        <div className="panel-title">
          <span className="panel-title-icon">🗺️</span>
          Price Heatmap &amp; Arbitrage Arcs — {commodity}
        </div>
        <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
          <span style={{ fontSize: 11, color: 'var(--text-muted)' }}>
            Arc color: <span style={{ color: '#22C55E' }}>●</span> High spread &nbsp;
            <span style={{ color: '#F59E0B' }}>●</span> Mid &nbsp;
            <span style={{ color: '#9BA8C0' }}>●</span> Low
          </span>
          <span className="panel-badge live">Live</span>
        </div>
      </div>
      <div className="panel-body" style={{ padding: 0 }}>
        <div style={{ height: mapHeight }}>
          <MapContainer
            center={INDIA_CENTER}
            zoom={6}
            style={{ height: '100%', width: '100%', background: '#0d1117' }}
            id="india-price-map"
          >
            <TileLayer
              url="https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png"
              attribution='&copy; <a href="https://carto.com/">CARTO</a>'
            />

            {/* Draw arbitrage arcs (Polylines) between mandis */}
            {filteredArb.map((arb, i) => {
              const srcCoords = DEMO_COORDS[arb.src_market];
              const tgtCoords = DEMO_COORDS[arb.tgt_market];
              if (!srcCoords || !tgtCoords) return null;
              return (
                <Polyline
                  key={`arc-${i}`}
                  positions={[srcCoords, tgtCoords]}
                  pathOptions={{
                    color: getColor(arb.arbitrage_spread),
                    weight: 2,
                    opacity: 0.75,
                    dashArray: '6 4',
                  }}
                />
              );
            })}

            {/* Source market markers */}
            {filteredArb.map((arb, i) => {
              const coords = DEMO_COORDS[arb.src_market];
              if (!coords) return null;
              return (
                <CircleMarker
                  key={`src-${i}`}
                  center={coords}
                  radius={8}
                  pathOptions={{ color: '#EF4444', fillColor: '#EF4444', fillOpacity: 0.85 }}
                  id={`market-src-${arb.src_market}`}
                >
                  <Tooltip permanent={false} direction="top">
                    <div style={{ fontFamily: 'Inter, sans-serif', fontSize: 12 }}>
                      <strong>{arb.src_market}</strong><br />
                      From: ₹{arb.src_price}/q
                    </div>
                  </Tooltip>
                </CircleMarker>
              );
            })}

            {/* Target market markers */}
            {filteredArb.map((arb, i) => {
              const coords = DEMO_COORDS[arb.tgt_market];
              if (!coords) return null;
              return (
                <CircleMarker
                  key={`tgt-${i}`}
                  center={coords}
                  radius={10}
                  pathOptions={{ color: '#22C55E', fillColor: '#22C55E', fillOpacity: 0.85 }}
                  id={`market-tgt-${arb.tgt_market}`}
                >
                  <Tooltip permanent={false} direction="top">
                    <div style={{ fontFamily: 'Inter, sans-serif', fontSize: 12 }}>
                      <strong>{arb.tgt_market}</strong><br />
                      To: ₹{arb.tgt_price}/q<br />
                      Spread: <strong>+₹{arb.arbitrage_spread}</strong><br />
                      Distance: {arb.distance_km} km
                    </div>
                  </Tooltip>
                </CircleMarker>
              );
            })}
          </MapContainer>
        </div>

        {/* Legend */}
        <div style={{ padding: '12px 16px', borderTop: '1px solid var(--border-subtle)', display: 'flex', gap: 16 }}>
          <div style={{ display: 'flex', alignItems: 'center', gap: 6, fontSize: 12, color: 'var(--text-muted)' }}>
            <div style={{ width: 10, height: 10, borderRadius: '50%', background: '#EF4444' }} />
            Source Mandi (lower price)
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: 6, fontSize: 12, color: 'var(--text-muted)' }}>
            <div style={{ width: 10, height: 10, borderRadius: '50%', background: '#22C55E' }} />
            Target Mandi (higher price)
          </div>
          <div style={{ display: 'flex', alignItems: 'center', gap: 6, fontSize: 12, color: 'var(--text-muted)' }}>
            <div style={{ width: 20, height: 2, background: '#22C55E', borderRadius: 1 }} />
            Arbitrage Arc (dashed = profitable route)
          </div>
        </div>
      </div>
    </div>
  );
}
