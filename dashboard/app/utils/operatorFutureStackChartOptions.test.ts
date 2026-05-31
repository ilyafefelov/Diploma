import { describe, expect, it } from 'vitest'

import { dashboardChartTokens } from '../lib/charts/dashboardChartCore'
import {
  buildDecisionPolicyChartSeries,
  buildForecastOption,
  buildRecommendationInputChartSeries,
  buildPolicyOption,
  buildSelectedRecommendationChartSeries,
  buildStrategyComparisonOption
} from './operatorFutureStackChartOptions'

import type {
  FutureForecastSeriesResponse,
  DecisionPolicyPreviewPointResponse
} from '~/types/control-plane'
import type {
  PolicyForecastContextPoint,
  RecommendationInputSignalPoint
} from './operatorFutureStack'

const forecastSeries = (modelName: string, modelFamily = 'NBEATSx'): FutureForecastSeriesResponse => ({
  model_name: modelName,
  model_family: modelFamily,
  source_status: 'official',
  uncertainty_kind: modelFamily === 'TFT' ? 'quantile' : 'point',
  mean_regret_uah: null,
  win_rate: null,
  out_of_dam_cap_rows: 0,
  quality_boundary: 'inside_dam_cap_not_value_claim',
  points: [
    {
      step_index: 0,
      interval_start: '2026-05-26T00:00:00',
      forecast_price_uah_mwh: 1542.84,
      actual_price_uah_mwh: null,
      p10_price_uah_mwh: modelFamily === 'TFT' ? 1400 : null,
      p50_price_uah_mwh: 1542.84,
      p90_price_uah_mwh: modelFamily === 'TFT' ? 1700 : null,
      net_power_mw: null,
      value_gap_uah: null,
      price_cap_status: 'inside_dam_cap'
    }
  ]
})

describe('operator future stack chart options', () => {
  it('aligns future-stack chart colors with shared dashboard chart tokens', () => {
    const recommendationSeries = buildRecommendationInputChartSeries([
      {
        label: '26 May, 00:00',
        forecastPriceUahMwh: 1543,
        selectedNetPowerMw: -0.234,
        projectedSocPercent: 95,
        siteNetLoadMw: 0.29
      }
    ])
    const forecastOption = buildForecastOption({
      hasRecommendationInputSignalRows: false,
      recommendationInputSignalRows: [],
      recommendationInputChartSeries: [],
      forecastLabels: ['26 May, 00:00'],
      forecastChartSeries: [
        forecastSeries('tft_official_v0', 'TFT'),
        forecastSeries('nbeatsx_official_v0')
      ]
    })
    const strategyOption = buildStrategyComparisonOption({
      strategyComparisonLabels: ['strict_lp'],
      strategyComparisonRows: [
        {
          sourceId: 'best_valid',
          label: 'Strict LP',
          scheduleRows: 24,
          totalChargeMwh: 1.2,
          totalDischargeMwh: 1.1,
          meanRegretVsStrictUah: 0,
          meanRegretVsV2Uah: 25,
          totalValueUah: 2400,
          marketExecutionEnabled: false,
          isDefault: true,
          isPromoted: false,
          status: 'preview',
          isBlocked: false
        }
      ]
    })

    expect(recommendationSeries[0]?.lineStyle).toMatchObject({ color: dashboardChartTokens.highlightOnDark })
    expect(recommendationSeries[1]?.itemStyle).toMatchObject({ color: dashboardChartTokens.secondarySoftOnDark })
    expect(recommendationSeries[2]?.lineStyle).toMatchObject({ color: dashboardChartTokens.rose })
    expect(recommendationSeries[3]?.lineStyle).toMatchObject({ color: dashboardChartTokens.warning })
    expect(forecastOption.tooltip).toMatchObject({
      backgroundColor: dashboardChartTokens.tooltipBackgroundDark,
      borderColor: dashboardChartTokens.tooltipBorderOnDark,
      textStyle: { color: dashboardChartTokens.tooltipTextOnDark }
    })
    expect(forecastOption.series[0]?.lineStyle).toMatchObject({ color: dashboardChartTokens.rose })
    expect(forecastOption.series[1]?.lineStyle).toMatchObject({ color: dashboardChartTokens.highlightOnDark })
    expect(strategyOption.series[0]?.itemStyle).toMatchObject({ color: dashboardChartTokens.warningStrongOnDark })
    expect(strategyOption.series[1]?.itemStyle).toMatchObject({ color: dashboardChartTokens.successStrongOnDark })
  })

  it('builds recommendation-input forecast options with price, power, SOC, and load axes', () => {
    const rows: RecommendationInputSignalPoint[] = [
      {
        label: '26 May, 00:00',
        forecastPriceUahMwh: 1543,
        selectedNetPowerMw: -0.234,
        projectedSocPercent: 95,
        siteNetLoadMw: 0.29
      }
    ]
    const series = buildRecommendationInputChartSeries(rows)
    const option = buildForecastOption({
      hasRecommendationInputSignalRows: true,
      recommendationInputSignalRows: rows,
      recommendationInputChartSeries: series,
      forecastLabels: [],
      forecastChartSeries: []
    })

    expect(series.map(item => item.name)).toEqual([
      'Recommendation price context (UAH/MWh)',
      'Selected battery net power (MW)',
      'Projected SOC (%)',
      'Site net load estimate (MW)'
    ])
    expect(Array.isArray(option.yAxis)).toBe(true)
    expect(option.series).toBe(series)
    expect(option.legend).toMatchObject({ show: false })
    expect(option.tooltip.formatter([
      {
        axisValue: '26 May, 00:00',
        marker: '',
        seriesName: 'Recommendation price context (UAH/MWh)',
        value: 1542.84
      }
    ])).toContain('1,543 UAH/MWh')
  })

  it('builds fallback forecast options from official model rows', () => {
    const option = buildForecastOption({
      hasRecommendationInputSignalRows: false,
      recommendationInputSignalRows: [],
      recommendationInputChartSeries: [],
      forecastLabels: ['26 May, 00:00'],
      forecastChartSeries: [forecastSeries('nbeatsx_official_v0')]
    })

    expect(option.yAxis).toMatchObject({
      name: 'UAH/MWh',
      type: 'value'
    })
    expect(option.series[0]?.name).toBe('Official NBEATSx')
    expect(option.legend).toMatchObject({ show: false })
  })

  it('builds selected recommendation and decision-policy series without Vue state', () => {
    expect(buildSelectedRecommendationChartSeries(
      [
        {
          label: '26 May, 00:00',
          netPowerMw: 0.25,
          forecastPriceUahMwh: 4908,
          valueGapUah: 175
        }
      ],
      {
        netPower: 'Selected DAM/IDM net power (MW)',
        valueGap: 'Value shortfall vs strict (UAH)',
        priceContext: 'Official/scenario price context (UAH/MWh)'
      }
    ).map(item => item.name)).toEqual([
      'Selected DAM/IDM net power (MW)',
      'Value shortfall vs strict (UAH)',
      'Official/scenario price context (UAH/MWh)'
    ])

    const policyRows: DecisionPolicyPreviewPointResponse[] = [
      {
        policy_run_id: 'policy-run',
        created_at: '2026-05-26T00:00:00Z',
        episode_id: 'episode-1',
        market_venue: 'DAM',
        scenario_index: 0,
        step_index: 0,
        interval_start: '2026-05-26T00:00:00',
        state_market_price_uah_mwh: 1543,
        state_nbeatsx_forecast_uah_mwh: 1543,
        state_tft_forecast_uah_mwh: 1490,
        state_forecast_uncertainty_uah_mwh: 100,
        state_forecast_spread_uah_mwh: 53,
        projected_soc_before: 0.5,
        projected_soc_after: 0.35,
        raw_charge_mw: 0,
        raw_discharge_mw: 0.25,
        projected_charge_mw: 0,
        projected_discharge_mw: 0.25,
        projected_net_power_mw: 0.25,
        projected_action_label: 'SELL',
        projection_status: 'accepted_without_projection',
        projection_adjustment_mw: 0,
        expected_policy_value_uah: 420,
        hold_value_uah: 0,
        value_vs_hold_uah: 420,
        oracle_value_uah: 595,
        value_gap_uah: 175,
        value_gap_ratio: 0.12,
        constraint_violation: false,
        gatekeeper_status: 'accepted',
        inference_latency_ms: 12,
        policy_mode: 'preview',
        readiness_status: 'research_only',
        model_name: 'decision_transformer',
        academic_scope: 'offline_preview'
      }
    ]
    const contextRows: PolicyForecastContextPoint[] = [
      {
        label: '26 May, 00:00',
        nbeatsxForecastUahMwh: 1543,
        tftForecastUahMwh: 1490,
        forecastUncertaintyUahMwh: 100,
        forecastSpreadUahMwh: 53
      }
    ]

    expect(buildDecisionPolicyChartSeries(policyRows, contextRows).map(item => item.name)).toEqual([
      'Policy value gap',
      'Projected action',
      'NBEATSx state forecast',
      'TFT state forecast'
    ])

    const policyOption = buildPolicyOption({
      isOfficialPolicyMode: false,
      usesDecisionPolicyPreview: false,
      policyLabels: ['26 May, 00:00'],
      officialPolicyChartSeries: [],
      decisionPolicyChartSeries: [],
      selectedRecommendationChartSeries: buildSelectedRecommendationChartSeries(
        [
          {
            label: '26 May, 00:00',
            netPowerMw: 0.25,
            forecastPriceUahMwh: 4908,
            valueGapUah: 175
          }
        ],
        {
          netPower: 'Selected DAM/IDM net power (MW)',
          valueGap: 'Value shortfall vs strict (UAH)',
          priceContext: 'Official/scenario price context (UAH/MWh)'
        }
      )
    })

    expect(policyOption.legend).toMatchObject({ show: false })
  })

  it('builds strategy comparison options with preview-only tooltip copy', () => {
    const option = buildStrategyComparisonOption({
      strategyComparisonLabels: ['DT V2+ safe-switch'],
      strategyComparisonRows: [
        {
          sourceId: 'dt_v2_plus_safe_switch_selector_shadow',
          label: 'DT V2+ safe-switch selector',
          scheduleRows: 24,
          totalChargeMwh: 1.2,
          totalDischargeMwh: 1.1,
          meanRegretVsStrictUah: 174.77,
          meanRegretVsV2Uah: 0,
          totalValueUah: 2400,
          marketExecutionEnabled: false,
          isDefault: false,
          isPromoted: false,
          status: 'research-shadow',
          isBlocked: false
        }
      ]
    })

    expect(option.xAxis.axisLabel.formatter?.('DT V2+ safe-switch')).toBe('DT V2+\nsafe-switch')
    expect(option.tooltip.formatter([
      {
        axisValue: 'DT V2+ safe-switch',
        marker: '',
        seriesName: 'Mean regret vs strict',
        value: 174.77
      }
    ])).toContain('market execution remains false')
  })
})
