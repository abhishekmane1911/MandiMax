import React, { useState, useEffect } from 'react';
import {
  ComposedChart, Line, Area, XAxis, YAxis, CartesianGrid,
  Tooltip, Legend, ResponsiveContainer, ReferenceLine
} from 'recharts';
import { format, parseISO } from 'date-fns';

const CustomTooltip = ({ active, payload, label }) => {
  if (!active || !payload?.length) return null;
  return (
    <div className="custom-tooltip">
      <div className="custom-tooltip-label">{label}</div>
      {payload.map((p, i) => (
        p.value != null && (
          <div key={i} style={{ color: p.color, fontSize: 13, fontWeight: 600 }}>
            {p.name}: ₹{Number(p.value).toFixed(0)}/q
          </div>
        )
      ))}
    </div>
  );
};

export default function ForecastChart({ data = [], commodity, fullView = false, modelHealth }) {
  const [market, setMarket] = useState('UJJAIN');

  const formattedData = data.map(d => ({
    ...d,
    dateLabel: d.date ? format(parseISO(d.date), 'dd MMM') : d.date,
  }));

  // Find where actual ends and forecast begins
  const splitDate = formattedData.find(d => d.actual == null && d.forecast != null)?.dateLabel;

  return (
    <div className="panel" id="forecast-chart-panel">
      <div className="panel-header">
        <div className="panel-title">
          <span className="panel-title-icon">📈</span>
          7-Day Price Forecast — {commodity}
        </div>
        <span className="panel-badge ml">GBT · MLflow</span>
      </div>
      <div className="panel-body">
        {/* Model quality pills */}
        {modelHealth && (
          <div className="model-quality-row">
            {[
              { label: 'MAPE',  value: modelHealth.model_mape },
              { label: 'R²',    value: modelHealth.model_r2   },
              { label: 'Model', value: modelHealth.model_version?.split('→')[0]?.trim() ?? '—' },
            ].map(m => (
              <div className="metric-pill" key={m.label}>
                <span className="metric-pill-value">{m.value}</span>
                <span className="metric-pill-label">{m.label}</span>
              </div>
            ))}
          </div>
        )}

        <div className="forecast-chart-container" style={{ height: fullView ? 400 : 280 }}>
          <ResponsiveContainer width="100%" height="100%">
            <ComposedChart data={formattedData} margin={{ top: 8, right: 8, bottom: 0, left: 8 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.05)" />
              <XAxis
                dataKey="dateLabel"
                tick={{ fill: '#9BA8C0', fontSize: 11 }}
                axisLine={{ stroke: 'rgba(255,255,255,0.08)' }}
                tickLine={false}
              />
              <YAxis
                tick={{ fill: '#9BA8C0', fontSize: 11 }}
                axisLine={false}
                tickLine={false}
                tickFormatter={v => `₹${(v/1000).toFixed(1)}K`}
                width={52}
              />
              <Tooltip content={<CustomTooltip />} />
              <Legend
                wrapperStyle={{ fontSize: 12, color: '#9BA8C0', paddingTop: 8 }}
              />

              {/* Confidence interval band */}
              <Area
                type="monotone"
                dataKey="upper_ci"
                stroke="none"
                fill="rgba(255,153,51,0.08)"
                name="Upper CI"
                legendType="none"
              />
              <Area
                type="monotone"
                dataKey="lower_ci"
                stroke="none"
                fill="var(--bg-card)"
                name="Lower CI"
                legendType="none"
              />

              {/* Actual price line */}
              <Line
                type="monotone"
                dataKey="actual"
                stroke="#3B82F6"
                strokeWidth={2.5}
                dot={{ r: 3, fill: '#3B82F6' }}
                name="Actual Price"
                connectNulls={false}
              />

              {/* Forecast line */}
              <Line
                type="monotone"
                dataKey="forecast"
                stroke="#FF9933"
                strokeWidth={2.5}
                strokeDasharray="6 3"
                dot={{ r: 3, fill: '#FF9933', strokeDasharray: '' }}
                name="7-Day Forecast"
                connectNulls={false}
              />

              {/* Today marker */}
              {splitDate && (
                <ReferenceLine
                  x={splitDate}
                  stroke="rgba(255,255,255,0.25)"
                  strokeDasharray="4 4"
                  label={{ value: 'Today', fill: '#9BA8C0', fontSize: 11, position: 'insideTopLeft' }}
                />
              )}
            </ComposedChart>
          </ResponsiveContainer>
        </div>
      </div>
    </div>
  );
}
