import { describe, expect, it } from 'vitest'

import { buildMarketSignalHeroChartOption, buildSelectedStrategyDispatchChartOption } from './dashboardChartTheme'
import type { OperatorRecommendationResponse, SignalPreview } from '../types/control-plane'

describe('dashboard chart theme', () => {
  it('uses explicit signal timestamps for operator market chart periods', () => {
    const signalPreview: SignalPreview = {
      tenant_id: 'client_003_dnipro_factory',
      labels: ['18:00', '21:00'],
      label_timestamps: ['2026-05-04T15:00:00Z', '2026-05-04T18:00:00Z'],
      timezone: 'Europe/Kyiv',
      market_price: [4200, 3100],
      weather_bias: [50, -20],
      weather_sources: ['OPEN_METEO', 'OPEN_METEO'],
      charge_intent: [0.2, -0.1],
      regret: [500, 300],
      resolved_location: {
        latitude: 48.46,
        longitude: 35.04,
        timezone: 'Europe/Kyiv'
      }
    }

    const option = buildMarketSignalHeroChartOption(signalPreview) as {
      xAxis: { data: string[] }
    }

    expect(option.xAxis.data).toEqual(['04 May\n18:00', '04 May\n21:00'])
  })

  it('builds selected-strategy dispatch charts from operator recommendations', () => {
    const recommendation: OperatorRecommendationResponse = {
      tenant_id: 'client_003_dnipro_factory',
      market_scope: 'dam_hourly_planning_preview',
      market_venue: 'DAM',
      interval_minutes: 60,
      anchor_timestamp: '2026-05-19T14:00:00Z',
      forecast_generated_at: null,
      target_delivery_window_start: '2026-05-19T15:00:00Z',
      target_delivery_window_end: '2026-05-19T19:00:00Z',
      market_execution_enabled: false,
      read_model_boundary: 'operator_preview_no_market_submission',
      market_gate_status: 'not_evaluated_preview_only',
      bid_eligibility_status: 'not_applicable_no_proposed_bid',
      proposed_bid_status: 'not_emitted_operator_preview',
      selected_strategy_id: 'schedule_value_learner_v2_plus',
      selection_reason: 'manual strategy: Offline V2+ schedule/value learner',
      forecast_source: 'read-model preview adapter',
      soc_source: 'live telemetry',
      review_required: true,
      readiness_warnings: [],
      policy_mode: 'offline_strategy_promotion_preview',
      selected_policy_id: 'schedule_value_learner_v2_plus',
      policy_explanation: 'Read-model preview only',
      policy_readiness: 'preview_only',
      policy_forecast_context_source: 'read-model adapter',
      policy_forecast_context_row_count: 6,
      policy_forecast_context_coverage_ratio: 1,
      policy_forecast_context_warning: null,
      available_strategies: [
        {
          strategy_id: 'schedule_value_learner_v2_plus',
          label: 'Offline V2+ schedule/value learner',
          enabled: true,
          reason: 'headline evidence',
          mean_regret_uah: 174.77,
          win_rate: null
        }
      ],
      forecast_model_series: [],
      value_gap_series: [
        {
          step_index: 0,
          interval_start: '2026-05-19T15:00:00Z',
          chosen_value_uah: 90,
          best_visible_value_uah: 130,
          value_gap_uah: 40,
          metric_source: 'read_model'
        },
        {
          step_index: 1,
          interval_start: '2026-05-19T18:00:00Z',
          chosen_value_uah: 120,
          best_visible_value_uah: 125,
          value_gap_uah: 5,
          metric_source: 'read_model'
        }
      ],
      load_forecast: [],
      soc_projection: [],
      recommendation_schedule: [
        {
          step_index: 0,
          interval_start: '2026-05-19T15:00:00Z',
          forecast_price_uah_mwh: 4200,
          recommended_net_power_mw: 0.2,
          projected_soc_before_fraction: 0.5,
          projected_soc_after_fraction: 0.45,
          throughput_mwh: 0.2,
          degradation_penalty_uah: 5,
          gross_market_value_uah: 140,
          net_value_uah: 135
        },
        {
          step_index: 1,
          interval_start: '2026-05-19T18:00:00Z',
          forecast_price_uah_mwh: 3100,
          recommended_net_power_mw: -0.2,
          projected_soc_before_fraction: 0.45,
          projected_soc_after_fraction: 0.5,
          throughput_mwh: 0.2,
          degradation_penalty_uah: 4,
          gross_market_value_uah: -70,
          net_value_uah: -74
        }
      ],
      daily_value_uah: 61,
      hold_baseline_value_uah: 0,
      value_vs_hold_uah: 61,
      economics: {
        total_gross_market_value_uah: 70,
        total_degradation_penalty_uah: 9,
        total_net_value_uah: 61,
        total_throughput_mwh: 0.4
      }
    }

    const option = buildSelectedStrategyDispatchChartOption(recommendation, null) as {
      xAxis: { data: string[] }
      series: Array<{ name: string, data: number[] }>
    }

    expect(option.xAxis.data).toEqual(['19 May\n18:00', '19 May\n21:00'])
    expect(option.series.map(series => series.name)).toEqual([
      'Selected net power',
      'Selected net value',
      'Visible value gap'
    ])
    expect(option.series[0]?.data).toEqual([0.2, -0.2])
    expect(option.series[2]?.data).toEqual([40, 5])
  })
})
