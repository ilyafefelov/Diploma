import { describe, expect, it } from 'vitest'

import {
  buildRecommendationInputSignalRows,
  buildRecommendationStrategySelectItems,
  filterOfficialPolicyValueSeries,
  buildPolicyForecastContextPoints,
  buildStrategyReadinessItems,
  buildStrategySelectItems,
  selectOperatorForecastChartSource,
  formatForecastQualityLabel,
  formatForecastWindowLabel,
  formatOperatorPolicyForecastContextLabel,
  formatPolicyForecastContextLabel,
  formatRuntimeAccelerationLabel,
  isChartSafeForecastSeries,
  sortFutureForecastSeries
} from './operatorFutureStack'

import type {
  FutureForecastSeriesResponse,
  OperatorStrategyOptionResponse
} from '~/types/control-plane'

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
  reason?: string,
  meanRegretUah: number | null = null
): OperatorStrategyOptionResponse => ({
  strategy_id: strategyId,
  label,
  enabled,
  reason: reason ?? (enabled ? 'materialized' : 'missing'),
  mean_regret_uah: meanRegretUah,
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

  it('selects delivery-day recommendation forecast rows before short future-stack context rows for the operator chart', () => {
    const deliverySeries = {
      ...emptySeries('nbeatsx_silver_v0', 'compact_fallback_from_lp_preview'),
      points: [
        {
          step_index: 0,
          interval_start: '2026-05-26T00:00:00',
          forecast_price_uah_mwh: 1828,
          actual_price_uah_mwh: null,
          p10_price_uah_mwh: null,
          p50_price_uah_mwh: 1828,
          p90_price_uah_mwh: null,
          net_power_mw: null,
          value_gap_uah: null,
          price_cap_status: 'inside_dam_cap'
        },
        {
          step_index: 23,
          interval_start: '2026-05-26T23:00:00',
          forecast_price_uah_mwh: 2642,
          actual_price_uah_mwh: null,
          p10_price_uah_mwh: null,
          p50_price_uah_mwh: 2642,
          p90_price_uah_mwh: null,
          net_power_mw: null,
          value_gap_uah: null,
          price_cap_status: 'inside_dam_cap'
        }
      ]
    }
    const futureStackSeries = {
      ...emptySeries('nbeatsx_official_v0', 'official'),
      points: [
        {
          step_index: 0,
          interval_start: '2026-05-25T19:00:00',
          forecast_price_uah_mwh: 4100,
          actual_price_uah_mwh: null,
          p10_price_uah_mwh: null,
          p50_price_uah_mwh: 4100,
          p90_price_uah_mwh: null,
          net_power_mw: null,
          value_gap_uah: null,
          price_cap_status: 'inside_dam_cap'
        }
      ]
    }

    const source = selectOperatorForecastChartSource({
      futureStack: {
        forecast_window_start: '2026-05-25T19:00:00',
        forecast_window_end: '2026-05-26T00:00:00',
        forecast_series: [futureStackSeries]
      },
      operatorRecommendation: {
        target_delivery_window_start: '2026-05-26T00:00:00',
        target_delivery_window_end: '2026-05-27T00:00:00',
        forecast_model_series: [deliverySeries]
      }
    })

    expect(source.kind).toBe('operator_delivery_day')
    expect(source.windowStart).toBe('2026-05-26T00:00:00')
    expect(source.windowEnd).toBe('2026-05-27T00:00:00')
    expect(source.series[0]?.points.map(point => point.interval_start)).toEqual([
      '2026-05-26T00:00:00',
      '2026-05-26T23:00:00'
    ])
  })

  it('builds recommendation input signal rows from the selected delivery-day schedule instead of duplicate model lines', () => {
    expect(buildRecommendationInputSignalRows(
      [
        {
          step_index: 0,
          interval_start: '2026-05-26T00:00:00',
          forecast_price_uah_mwh: 1542.84,
          recommended_net_power_mw: -0.234,
          projected_soc_before_fraction: 0.5,
          projected_soc_after_fraction: 0.95,
          throughput_mwh: 0.234,
          degradation_penalty_uah: 197.5,
          gross_market_value_uah: -361.9,
          net_value_uah: -559.4
        },
        {
          step_index: 1,
          interval_start: '2026-05-26T01:00:00',
          forecast_price_uah_mwh: 4908.36,
          recommended_net_power_mw: 0.25,
          projected_soc_before_fraction: 0.95,
          projected_soc_after_fraction: 0.47,
          throughput_mwh: 0.25,
          degradation_penalty_uah: 0,
          gross_market_value_uah: 1227,
          net_value_uah: 1227
        }
      ],
      [
        {
          timestamp: '2026-05-26T00:00:00',
          physical_soc: null,
          estimated_soc: 0.5,
          planning_soc: 0.95,
          soc_source: 'tenant_default',
          confidence: 'low'
        },
        {
          timestamp: '2026-05-26T01:00:00',
          physical_soc: null,
          estimated_soc: 0.5,
          planning_soc: 0.47,
          soc_source: 'tenant_default',
          confidence: 'low'
        }
      ],
      [
        {
          timestamp: '2026-05-26T00:00:00Z',
          load_mw: 0.31,
          pv_estimate_mw: 0.02,
          net_load_mw: 0.29,
          btm_battery_power_mw: 0.03,
          source_kind: 'configured',
          weather_source_kind: 'schedule_estimate',
          reason_code: 'off_hours'
        }
      ]
    )).toEqual([
      {
        label: '26 May, 00:00',
        forecastPriceUahMwh: 1543,
        selectedNetPowerMw: -0.234,
        projectedSocPercent: 95,
        siteNetLoadMw: 0.29
      },
      {
        label: '26 May, 01:00',
        forecastPriceUahMwh: 4908,
        selectedNetPowerMw: 0.25,
        projectedSocPercent: 47,
        siteNetLoadMw: null
      }
    ])
  })

  it('keeps only official rows for the policy-value official-row mode', () => {
    const officialNbeatsx = {
      ...emptySeries('nbeatsx_official_v0', 'official'),
      points: [
        {
          step_index: 0,
          interval_start: '2026-05-06T14:00:00Z',
          forecast_price_uah_mwh: 4200,
          actual_price_uah_mwh: null,
          p10_price_uah_mwh: null,
          p50_price_uah_mwh: 4200,
          p90_price_uah_mwh: null,
          net_power_mw: null,
          value_gap_uah: null,
          price_cap_status: 'inside_dam_cap'
        }
      ]
    }
    const officialTft = {
      ...emptySeries('tft_official_v0', 'official'),
      points: [
        {
          step_index: 0,
          interval_start: '2026-05-06T14:00:00Z',
          forecast_price_uah_mwh: 4100,
          actual_price_uah_mwh: null,
          p10_price_uah_mwh: 3900,
          p50_price_uah_mwh: 4100,
          p90_price_uah_mwh: 4300,
          net_power_mw: null,
          value_gap_uah: null,
          price_cap_status: 'inside_dam_cap'
        }
      ]
    }

    const officialSeries = filterOfficialPolicyValueSeries([
      emptySeries('tft_silver_v0', 'compact'),
      officialTft,
      officialNbeatsx
    ])

    expect(officialSeries.map(series => series.model_name)).toEqual([
      'nbeatsx_official_v0',
      'tft_official_v0'
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

  it('builds recommendation strategy switch items only from real schedule strategies', () => {
    expect(buildRecommendationStrategySelectItems([
      strategy('strict_similar_day', 'Strict similar-day control', true, 'fallback/control', 310.58),
      strategy('schedule_value_learner_v2_plus', 'Offline V2+ schedule/value learner', true, 'offline comparator', 174.77),
      strategy('nbeatsx_silver_v0', 'Compact NBEATSx', true, 'schedule rows materialized', 481.2),
      strategy('nbeatsx_official_v0', 'Official NBEATSx', true, 'forecast rows only', null),
      strategy('decision_transformer', 'Decision Transformer', false)
    ])).toEqual([
      { label: 'Offline V2+ schedule/value learner · 175 UAH', value: 'schedule_value_learner_v2_plus', disabled: false },
      { label: 'Strict similar-day control · 311 UAH', value: 'strict_similar_day', disabled: false },
      { label: 'Compact NBEATSx · 481 UAH', value: 'nbeatsx_silver_v0', disabled: false }
    ])
  })

  it('builds visible readiness chips for schedule strategies, not blocked raw official forecasts', () => {
    expect(buildStrategyReadinessItems([
      strategy('strict_similar_day', 'Strict similar-day control', true, 'fallback/control', 310.58),
      strategy('schedule_value_learner_v2_plus', 'Offline V2+ schedule/value learner', true, 'offline comparator', 174.77),
      strategy('nbeatsx_silver_v0', 'Compact NBEATSx', true, 'schedule rows materialized', 481.2),
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
        strategyId: 'strict_similar_day',
        label: 'Strict similar-day control',
        status: 'ready',
        reason: '311 UAH mean regret'
      },
      {
        strategyId: 'schedule_value_learner_v2_plus',
        label: 'Offline V2+ schedule/value learner',
        status: 'ready',
        reason: '175 UAH mean regret'
      },
      {
        strategyId: 'nbeatsx_silver_v0',
        label: 'Compact NBEATSx',
        status: 'ready',
        reason: '481 UAH mean regret'
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
    expect(formatOperatorPolicyForecastContextLabel({
      policy_forecast_context_coverage_ratio: 0,
      policy_forecast_context_row_count: 0
    })).toBe('forecast context not applicable')
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

  it('filters unsafe raw forecast rows out of operator charts', () => {
    expect(isChartSafeForecastSeries({
      ...emptySeries('nbeatsx_official_v0', 'official'),
      points: [
        {
          step_index: 0,
          interval_start: '2026-05-06T14:00:00Z',
          forecast_price_uah_mwh: 115_000_000,
          actual_price_uah_mwh: null,
          p10_price_uah_mwh: null,
          p50_price_uah_mwh: 115_000_000,
          p90_price_uah_mwh: null,
          net_power_mw: null,
          value_gap_uah: null,
          price_cap_status: 'above_dam_cap'
        }
      ],
      out_of_dam_cap_rows: 1,
      quality_boundary: 'needs_calibration_before_value_claim'
    })).toBe(false)

    expect(isChartSafeForecastSeries({
      ...emptySeries('tft_silver_v0', 'compact'),
      points: [
        {
          step_index: 0,
          interval_start: '2026-05-06T14:00:00Z',
          forecast_price_uah_mwh: 4200,
          actual_price_uah_mwh: null,
          p10_price_uah_mwh: 3900,
          p50_price_uah_mwh: 4200,
          p90_price_uah_mwh: 4500,
          net_power_mw: null,
          value_gap_uah: null,
          price_cap_status: 'inside_dam_cap'
        }
      ]
    })).toBe(true)
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
