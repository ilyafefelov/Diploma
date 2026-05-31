import { describe, expect, it } from 'vitest'

import {
  buildBackendStatusFacts,
  buildDecisionSourceFacts,
  buildFutureStatusCards,
  buildPreviewSourceSelectItems,
  buildSelectedRecommendationChartRows,
  buildShadowModelStoryItems,
  formatForecastSeriesLabel,
  formatInputSignalTooltipValue,
  formatPolicyTooltipValue,
  formatPreviewSourceOptionLabel,
  formatStrategyAxisLabel,
  shortStrategyLabel
} from './operatorFutureStackPresentation'
import type {
  DecisionPolicyPreviewResponse,
  FutureStackPreviewResponse,
  OperatorRecommendationResponse,
  ShadowRecommendationPreviewResponse
} from '../types/control-plane'

describe('operator future stack presentation helpers', () => {
  it('builds selected recommendation chart rows with rounded schedule and value-gap fields', () => {
    expect(buildSelectedRecommendationChartRows(
      [
        {
          step_index: 0,
          interval_start: '2026-05-26T00:00:00',
          forecast_price_uah_mwh: 1542.84,
          recommended_net_power_mw: -0.2344,
          projected_soc_before_fraction: 0.5,
          projected_soc_after_fraction: 0.95,
          throughput_mwh: 0.234,
          degradation_penalty_uah: 197.5,
          gross_market_value_uah: -361.9,
          net_value_uah: -559.4
        }
      ],
      [
        {
          step_index: 0,
          interval_start: '2026-05-26T00:00:00',
          chosen_value_uah: 420,
          best_visible_value_uah: 999.6,
          value_gap_uah: 579.6,
          metric_source: 'strict_reference'
        }
      ]
    )).toEqual([
      {
        label: '26 May, 00:00',
        netPowerMw: -0.234,
        forecastPriceUahMwh: 1543,
        valueGapUah: 580
      }
    ])
  })

  it('formats chart tooltip values with domain units', () => {
    expect(formatInputSignalTooltipValue('Projected SOC (%)', 94.6)).toBe('95%')
    expect(formatInputSignalTooltipValue('Selected battery net power (MW)', -0.2344)).toBe('-0.234 MW')
    expect(formatInputSignalTooltipValue('Recommendation price context (UAH/MWh)', 1542.84)).toBe('1,543 UAH/MWh')
    expect(formatPolicyTooltipValue('Projected action', 0.25)).toBe('0.250 MW')
    expect(formatPolicyTooltipValue('Official TFT p50', 4100.3, true)).toBe('4,100 UAH/MWh')
    expect(formatPolicyTooltipValue('Policy value gap', 4100.3)).toBe('4,100 UAH')
  })

  it('keeps source and strategy labels explicit about non-promoted preview state', () => {
    expect(formatPreviewSourceOptionLabel(
      'dt_v2_plus_safe_switch_selector_shadow',
      'safe_switch_evidence_not_promoted',
      'DT V2+ safe-switch selector'
    )).toBe('DT V2+ safe-switch selector (not promoted)')

    expect(shortStrategyLabel({
      sourceId: 'regret_aware_v2_plus_selector_shadow',
      label: 'Regret-aware V2+ selector',
      scheduleRows: 24,
      totalChargeMwh: 1,
      totalDischargeMwh: 1,
      meanRegretVsStrictUah: 174.77,
      meanRegretVsV2Uah: 0,
      totalValueUah: 1200,
      marketExecutionEnabled: false,
      isDefault: false,
      isPromoted: false,
      status: 'research-shadow',
      isBlocked: false
    })).toBe('RA V2+')

    expect(formatStrategyAxisLabel('DT V2+ safe-switch')).toBe('DT V2+\nsafe-switch')
  })

  it('builds default source select items without promoting blocked previews', () => {
    const items = buildPreviewSourceSelectItems(null)

    expect(items.map(item => item.value)).toContain('best_valid')
    expect(items.find(item => item.value === 'best_valid')?.label)
      .toBe('Best valid schedule (V2+ comparator/fallback)')
    expect(items.find(item => item.value === 'dt_v2_plus_safe_switch_selector_shadow')?.label)
      .toBe('DT V2+ safe-switch selector (not promoted)')
    expect(items.find(item => item.value === 'v13_dt_lava_promoted_training')?.label)
      .toBe('V13/DT/LAVA blocked (no schedule)')
  })

  it('builds future-stack panel copy from pure presentation helpers', () => {
    const operatorRecommendation = {
      selection_reason: 'manual strategy: Offline V2+ schedule/value learner',
      policy_mode: 'offline_strategy_promotion_preview',
      selected_policy_id: 'schedule_value_learner_v2_plus',
      policy_explanation: 'V2+ remains confirmed offline schedule-value comparator.'
    } as OperatorRecommendationResponse
    const futureStack = {
      backend_status: { api: 'healthy' },
      runtime_acceleration: {
        device_type: 'cpu',
        device_name: 'CPU',
        backend: 'torch',
        recommended_scope: 'local preview'
      }
    } as unknown as FutureStackPreviewResponse
    const decisionPolicy = {
      market_execution_enabled: false,
      policy_value_interpretation: 'Counterfactual only.'
    } as DecisionPolicyPreviewResponse
    const storyPreview = {
      preview_source_id: 'dt_v2_plus_safe_switch_selector_shadow',
      comparison_metrics: {
        selector_mean_regret_uah: 168.156,
        non_v2_plus_switch_count: 4,
        abstention_count: 86,
        recovered_safe_switch_opportunity_count: 3
      }
    } as unknown as ShadowRecommendationPreviewResponse

    expect(buildFutureStatusCards({
      selectedStrategyLabel: 'Offline V2+ schedule/value learner',
      operatorRecommendation,
      futureStack,
      decisionPolicy
    })).toContainEqual({
      label: 'Execution boundary',
      value: 'preview only',
      meta: 'read-model evidence; no live dispatch'
    })
    expect(buildDecisionSourceFacts({
      selectedStrategyLabel: 'Offline V2+ schedule/value learner',
      policyForecastContextLabel: 'forecast-conditioned',
      operatorRecommendation,
      decisionPolicy
    })).toContain('Boundary: review candidate only, no live IDM bid or market submission.')
    expect(buildBackendStatusFacts(futureStack)).toEqual([
      'api: healthy',
      'Runtime: CPU / torch.'
    ])
    expect(buildShadowModelStoryItems([storyPreview])[2]).toEqual({
      label: 'DT safe-switch shadow',
      value: '168.16 UAH mean regret',
      meta: '4 switches / 86 V2+ abstentions / 3 recovered wins'
    })
  })

  it('formats forecast model display labels without leaking raw ids', () => {
    expect(formatForecastSeriesLabel('nbeatsx_silver_v0')).toBe('Compact NBEATSx')
    expect(formatForecastSeriesLabel('tft_official_v0')).toBe('Official TFT p50')
    expect(formatForecastSeriesLabel('custom_model')).toBe('custom_model')
  })
})
