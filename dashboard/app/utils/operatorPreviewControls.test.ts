import { describe, expect, it } from 'vitest'

import type { OperatorRecommendationResponse } from '~/types/control-plane'
import {
  buildOperatorRecommendationSignalPreview,
  formatOperatorPreviewErrorMessage,
  operatorPriceContextModeLabel,
  sliceOperatorRecommendationForChartHorizon,
  sliceSignalPreviewForChartHorizon,
  selectOperatorMarketSignalPreview
} from './operatorPreviewControls'
import { baseRecommendationWithSchedule } from './test-fixtures/operatorShadowPreviewFixtures'

describe('operator preview controls', () => {
  it('filters visible signal rows for chart horizon without changing source data', () => {
    const signalPreview = {
      tenant_id: 'client_003_dnipro_factory',
      labels: ['00', '01', '02', '03', '04', '05', '06'],
      label_timestamps: ['t0', 't1', 't2', 't3', 't4', 't5', 't6'],
      market_price: [1, 2, 3, 4, 5, 6, 7],
      weather_bias: [0, 0, 0, 0, 0, 0, 0],
      weather_sources: ['OREE', 'OREE', 'OREE', 'OREE', 'OREE', 'OREE', 'OREE'],
      charge_intent: [0, 0, 0, 0, 0, 0, 0],
      regret: [0, 0, 0, 0, 0, 0, 0],
      resolved_location: {
        latitude: 48.46,
        longitude: 35.04,
        timezone: 'Europe/Kyiv'
      }
    }

    const visibleSignalPreview = sliceSignalPreviewForChartHorizon(signalPreview, '6h')

    expect(visibleSignalPreview?.labels).toHaveLength(6)
    expect(signalPreview.labels).toHaveLength(7)
  })

  it('filters visible recommendation rows while preserving full-window economics', () => {
    const recommendation = {
      recommendation_schedule: Array.from({ length: 7 }, (_, index) => ({
        step_index: index,
        interval_start: `2026-05-20T0${index}:00:00`,
        forecast_price_uah_mwh: 1000 + index,
        recommended_net_power_mw: 0,
        projected_soc_before_fraction: 0.5,
        projected_soc_after_fraction: 0.5,
        throughput_mwh: 0,
        degradation_penalty_uah: 0,
        gross_market_value_uah: 0,
        net_value_uah: index
      })),
      bid_recommendation_preview: Array.from({ length: 7 }, (_, index) => ({
        step_index: index,
        interval_start: `2026-05-20T0${index}:00:00`,
        market_venue: 'IDM',
        side: 'HOLD',
        operator_action: 'hold',
        quantity_mw: 0,
        indicative_limit_price_uah_mwh: 0,
        preview_only: true,
        market_execution_enabled: false,
        market_order_payload_emitted: false,
        proposed_bid_status: 'not_emitted_operator_preview',
        read_model_boundary: 'operator_preview_no_market_submission'
      })),
      value_gap_series: Array.from({ length: 7 }, (_, index) => ({
        step_index: index,
        interval_start: `2026-05-20T0${index}:00:00`,
        chosen_value_uah: index,
        best_visible_value_uah: index,
        value_gap_uah: 0,
        metric_source: 'test'
      })),
      load_forecast: [],
      soc_projection: [],
      forecast_model_series: [],
      economics: {
        total_net_value_uah: 7000
      }
    }

    const visibleRecommendation = sliceOperatorRecommendationForChartHorizon(recommendation as never, '6h')

    expect(visibleRecommendation?.recommendation_schedule).toHaveLength(6)
    expect(visibleRecommendation?.economics.total_net_value_uah).toBe(7000)
    expect(recommendation.recommendation_schedule).toHaveLength(7)
  })

  it('prefers selected operator recommendation prices over stale general signal preview rows', () => {
    const staleSignalPreview = {
      tenant_id: 'client_003_dnipro_factory',
      labels: ['31 May'],
      label_timestamps: ['2026-05-31T12:00:00Z'],
      market_price: [7100],
      weather_bias: [0],
      weather_sources: ['OREE_DAM_OLD'],
      charge_intent: [0],
      regret: [0],
      resolved_location: {
        latitude: 48.46,
        longitude: 35.04,
        timezone: 'Europe/Kyiv'
      }
    }
    const baseRecommendation = baseRecommendationWithSchedule()
    const recommendation: OperatorRecommendationResponse = {
      ...baseRecommendation,
      market_venue: 'IDM',
      price_context_status: 'official_published',
      target_delivery_window_start: '2026-06-01T21:00:00Z',
      target_delivery_window_end: '2026-06-02T21:00:00Z',
      recommendation_schedule: [
        {
          ...baseRecommendation.recommendation_schedule[0]!,
          interval_start: '2026-06-01T21:00:00Z',
          forecast_price_uah_mwh: 6500,
          recommended_net_power_mw: -0.12
        },
        {
          ...baseRecommendation.recommendation_schedule[1]!,
          interval_start: '2026-06-01T22:00:00Z',
          forecast_price_uah_mwh: 5400,
          recommended_net_power_mw: 0.18
        }
      ]
    }

    const selectedPreview = selectOperatorMarketSignalPreview(staleSignalPreview, recommendation)

    expect(selectedPreview?.label_timestamps).toEqual(['2026-06-01T21:00:00Z', '2026-06-01T22:00:00Z'])
    expect(selectedPreview?.market_price).toEqual([6500, 5400])
    expect(selectedPreview?.forecast_window_start).toBe('2026-06-01T21:00:00Z')
    expect(selectedPreview?.forecast_window_end).toBe('2026-06-02T21:00:00Z')
    expect(selectedPreview?.weather_sources).toEqual([
      'OFFICIAL_OREE_IDM_PUBLISHED',
      'OFFICIAL_OREE_IDM_PUBLISHED'
    ])
  })

  it('does not fall back to stale general signal rows when the selected recommendation is missing', () => {
    const staleSignalPreview = {
      tenant_id: 'client_003_dnipro_factory',
      labels: ['31 May'],
      label_timestamps: ['2026-05-31T12:00:00Z'],
      market_price: [7100],
      weather_bias: [0],
      weather_sources: ['OREE_DAM_OLD'],
      charge_intent: [0],
      regret: [0],
      resolved_location: {
        latitude: 48.46,
        longitude: 35.04,
        timezone: 'Europe/Kyiv'
      }
    }

    const selectedPreview = selectOperatorMarketSignalPreview(staleSignalPreview, null)

    expect(selectedPreview).toBeNull()
  })

  it('rewrites source-missing API errors without exposing substitute-price fallback wording in the UI', () => {
    const blockedFallbackPhrase = ['syn', 'thetic fallback is disabled.'].join('')
    const blockedSourceWord = ['syn', 'thetic'].join('')
    const message = formatOperatorPreviewErrorMessage(
      new Error(`503: pre-publication forecast rows are required. ${blockedFallbackPhrase}`),
      'Unable to load operator recommendation.'
    )

    expect(message).toContain('No substitute prices are rendered')
    expect(message).not.toContain(blockedSourceWord)
  })

  it('strips transport details from Nuxt fetch errors before blocker copy is rendered', () => {
    const blockedFallbackPhrase = ['syn', 'thetic fallback is disabled.'].join('')
    const message = formatOperatorPreviewErrorMessage(
      {
        message: `[GET] "/api/control-plane/dashboard/operator-recommendation?tenant_id=client_003_dnipro_factory": 503 Official OREE IDM row is not published for target_delivery_date=2030-01-01; pre-publication forecast rows are required from NBEATSx/TFT forecast store. ${blockedFallbackPhrase}`
      },
      'Unable to load operator recommendation.'
    )

    expect(message).toBe(
      'Official OREE IDM row is not published for target_delivery_date=2030-01-01; pre-publication forecast rows are required from NBEATSx/TFT forecast store. No substitute prices are rendered.'
    )
  })

  it('labels pre-publication recommendation prices as ML forecast context', () => {
    const baseRecommendation = baseRecommendationWithSchedule()
    const recommendation: OperatorRecommendationResponse = {
      ...baseRecommendation,
      market_venue: 'DAM',
      price_context_status: 'pre_publication_forecast',
      forecast_generated_at: '2026-05-31T22:08:00Z',
      policy_forecast_context_source: 'nbeatsx_official_v0',
      recommendation_schedule: [
        {
          ...baseRecommendation.recommendation_schedule[0]!,
          interval_start: '2026-06-02T00:00:00Z',
          forecast_price_uah_mwh: 7200
        }
      ]
    }

    const selectedPreview = buildOperatorRecommendationSignalPreview(recommendation, null)

    expect(operatorPriceContextModeLabel(recommendation)).toBe('ML forecast price context')
    expect(selectedPreview?.market_price).toEqual([7200])
    expect(selectedPreview?.weather_sources).toEqual(['ML_FORECAST_NBEATSX_OFFICIAL_V0'])
  })
})
