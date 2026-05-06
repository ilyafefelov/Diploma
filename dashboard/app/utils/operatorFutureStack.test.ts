import { describe, expect, it } from 'vitest'

import {
  buildPolicyForecastContextPoints,
  buildStrategyReadinessItems,
  buildStrategySelectItems,
  formatForecastQualityLabel,
  formatForecastWindowLabel,
  formatOperatorPolicyForecastContextLabel,
  formatPolicyForecastContextLabel,
  formatRuntimeAccelerationLabel,
  sortFutureForecastSeries
} from './operatorFutureStack'

import type { FutureForecastSeriesResponse, OperatorStrategyOptionResponse } from '~/types/control-plane'

const emptySeries = (modelName: string, sourceStatus: string): FutureForecastSeriesResponse => ({
  model_name: modelName,
  model_family: modelName.includes('tft') ? 'TFT' : 'NBEATSx',
  source_status: sourceStatus,
  uncertainty_kind: modelName.includes('tft') ? 'quantile' : 'point',
  mean_regret_uah: null,
  win_rate: null,
  out_of_dam_cap_rows: 0,
  quality_boundary: sourceStatus === 'official'
    ? 'smoke_values_inside_dam_cap_not_value_claim'
    : 'inside_dam_cap_not_value_claim',
  points: []
})

const strategy = (
  strategyId: string,
  label: string,
  enabled: boolean,
  reason?: string
): OperatorStrategyOptionResponse => ({
  strategy_id: strategyId,
  label,
  enabled,
  reason: reason ?? (enabled ? 'materialized' : 'missing'),
  mean_regret_uah: null,
  win_rate: null
})

describe('operator future stack display helpers', () => {
  it('formats exact forecast prediction windows for operator headers', () => {
    expect(formatForecastWindowLabel(
      '2026-05-04T18:00:00Z',
      '2026-05-05T17:00:00Z'
    )).toBe('04 May, 21:00 -> 05 May, 20:00')
  })

  it('prioritizes official forecast backend rows over compact fallback rows', () => {
    const ordered = sortFutureForecastSeries([
      emptySeries('tft_silver_v0', 'compact'),
      emptySeries('nbeatsx_official_v0', 'official'),
      emptySeries('nbeatsx_silver_v0', 'compact'),
      emptySeries('tft_official_v0', 'official')
    ])

    expect(ordered.map(series => series.model_name)).toEqual([
      'nbeatsx_official_v0',
      'tft_official_v0',
      'nbeatsx_silver_v0',
      'tft_silver_v0'
    ])
  })

  it('builds manual strategy switch items from backend availability', () => {
    expect(buildStrategySelectItems([
      strategy('strict_similar_day', 'Strict similar-day control', true),
      strategy('nbeatsx_official_v0', 'Official NBEATSx', true),
      strategy('decision_transformer', 'Decision Transformer', false)
    ])).toEqual([
      { label: 'Strict similar-day control', value: 'strict_similar_day', disabled: false },
      { label: 'Official NBEATSx', value: 'nbeatsx_official_v0', disabled: false },
      { label: 'Decision Transformer - missing', value: 'decision_transformer', disabled: true }
    ])
  })

  it('builds visible readiness chips for official forecasts and DT policy', () => {
    expect(buildStrategyReadinessItems([
      strategy('strict_similar_day', 'Strict similar-day control', true),
      strategy(
        'nbeatsx_official_v0',
        'Official NBEATSx',
        false,
        'official forecast rows need calibration: 1 out-of-cap rows'
      ),
      strategy('tft_official_v0', 'Official TFT', true, 'materialized forecast-store rows; values inside DAM caps'),
      strategy('decision_transformer', 'Decision Transformer', false)
    ])).toEqual([
      {
        strategyId: 'nbeatsx_official_v0',
        label: 'Official NBEATSx',
        status: 'blocked',
        reason: 'official forecast rows need calibration: 1 out-of-cap rows'
      },
      {
        strategyId: 'tft_official_v0',
        label: 'Official TFT',
        status: 'ready',
        reason: 'materialized forecast-store rows; values inside DAM caps'
      },
      {
        strategyId: 'decision_transformer',
        label: 'Decision Transformer',
        status: 'blocked',
        reason: 'missing'
      }
    ])
  })

  it('formats runtime acceleration for SOTA and DT status cards', () => {
    expect(formatRuntimeAccelerationLabel({
      backend: 'torch 2.11.0+cpu',
      device_type: 'cpu',
      device_name: 'CPU only',
      gpu_available: false,
      cuda_version: null,
      recommended_scope: 'keep official NBEATSx/TFT and DT runs small'
    })).toBe('CPU only / torch 2.11.0+cpu')

    expect(formatRuntimeAccelerationLabel({
      backend: 'torch 2.11.0',
      device_type: 'cuda',
      device_name: 'NVIDIA RTX',
      gpu_available: true,
      cuda_version: '12.6',
      recommended_scope: 'use GPU for official forecasts'
    })).toBe('CUDA / NVIDIA RTX')
  })

  it('formats DT forecast-context coverage for operator status cards', () => {
    expect(formatPolicyForecastContextLabel({
      forecast_context_coverage_ratio: 0.875,
      forecast_context_row_count: 21,
      row_count: 24
    })).toBe('88% forecast-conditioned (21/24 rows)')

    expect(formatPolicyForecastContextLabel(null)).toBe('forecast context pending')
    expect(formatOperatorPolicyForecastContextLabel({
      policy_forecast_context_coverage_ratio: 0.5,
      policy_forecast_context_row_count: 12
    })).toBe('50% forecast-conditioned (12 rows)')
  })

  it('summarizes forecast quality boundaries without hiding cap violations', () => {
    expect(formatForecastQualityLabel(emptySeries('tft_official_v0', 'official'))).toBe(
      'inside DAM cap / smoke only'
    )

    expect(formatForecastQualityLabel({
      ...emptySeries('nbeatsx_official_v0', 'official'),
      out_of_dam_cap_rows: 2,
      quality_boundary: 'needs_calibration_before_value_claim'
    })).toBe('2 out-of-cap rows')
  })

  it('extracts NBEATSx and TFT forecast context from DT policy rows', () => {
    expect(buildPolicyForecastContextPoints([
      {
        interval_start: '2026-05-05T18:00:00Z',
        state_market_price_uah_mwh: 4200,
        state_nbeatsx_forecast_uah_mwh: 4100,
        state_tft_forecast_uah_mwh: 4350,
        state_forecast_uncertainty_uah_mwh: 360,
        state_forecast_spread_uah_mwh: 250
      },
      {
        interval_start: '2026-05-05T19:00:00Z',
        state_market_price_uah_mwh: 3900,
        state_nbeatsx_forecast_uah_mwh: null,
        state_tft_forecast_uah_mwh: 4000,
        state_forecast_uncertainty_uah_mwh: null,
        state_forecast_spread_uah_mwh: null
      }
    ])).toEqual([
      {
        label: '05 May, 21:00',
        nbeatsxForecastUahMwh: 4100,
        tftForecastUahMwh: 4350,
        forecastUncertaintyUahMwh: 360,
        forecastSpreadUahMwh: 250
      },
      {
        label: '05 May, 22:00',
        nbeatsxForecastUahMwh: 3900,
        tftForecastUahMwh: 4000,
        forecastUncertaintyUahMwh: 100,
        forecastSpreadUahMwh: 100
      }
    ])
  })
})
