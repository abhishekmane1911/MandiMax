import React from 'react';

export default function PipelineHealthBanner({ health }) {
  const isOk = health?.pipeline_status === 'OK';

  return (
    <div className="health-banner" role="status" aria-label="Pipeline health status">
      <div className="health-item">
        <span className="health-item-label">Pipeline Status</span>
        <span className={`health-item-value ${isOk ? 'ok' : 'warn'}`}>
          {isOk ? '✅ Healthy' : '⚠️ Degraded'}
        </span>
      </div>
      <div className="health-item">
        <span className="health-item-label">Last Pipeline Run</span>
        <span className="health-item-value">{health?.last_pipeline_run ?? '—'}</span>
      </div>
      <div className="health-item">
        <span className="health-item-label">Bronze Records</span>
        <span className="health-item-value">{health?.bronze_records ?? '—'}</span>
      </div>
      <div className="health-item">
        <span className="health-item-label">Silver Records</span>
        <span className="health-item-value">{health?.silver_records ?? '—'}</span>
      </div>
      <div className="health-item">
        <span className="health-item-label">Arb Pairs (Today)</span>
        <span className="health-item-value">{health?.gold_arb_pairs ?? '—'}</span>
      </div>
      <div className="health-item">
        <span className="health-item-label">Geocode Hit Rate</span>
        <span className="health-item-value ok">{health?.geocode_hit_rate ?? '—'}</span>
      </div>
      <div className="health-item">
        <span className="health-item-label">Model (MLflow)</span>
        <span className="health-item-value" style={{ color: 'var(--color-info)' }}>
          {health?.model_version ?? '—'}
        </span>
      </div>
      <div className="health-item">
        <span className="health-item-label">Model MAPE</span>
        <span className="health-item-value ok">{health?.model_mape ?? '—'}</span>
      </div>
      <div className="health-item">
        <span className="health-item-label">Model R²</span>
        <span className="health-item-value ok">{health?.model_r2 ?? '—'}</span>
      </div>
    </div>
  );
}
