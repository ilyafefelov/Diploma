import { describe, expect, it } from 'vitest'

import { normalizeForecastComparison } from './benchmarkDataset'
import type { ForecastStrategyComparisonResponse } from '../types/control-plane'

describe('benchmark dataset normalization', () => {
  it('normalizes a forecast comparison response into a one-anchor dataset', () => {
    const response: ForecastStrategyComparisonResponse = {
      tenant_id: 'client_003_dnipro_factory',
      market_venue: 'DAM',
      evaluation_id: 'eval-001',
      anchor_timestamp: '2026-05-04T20:00:00Z',
      generated_at: '2026-05-04T20:30:00Z',
      horizon_hours: 24,
      starting_soc_fraction: 0.5,
      starting_soc_source: 'tenant_default',
      comparisons: [
        {
          forecast_model_name: 'strict_similar_day',
          strategy_kind: 'forecast_driven_lp',
          decision_value_uah: 110,
          forecast_objective_value_uah: 105,
          oracle_value_uah: 130,
          regret_uah: 20,
          regret_ratio: 0.1538,
          total_degradation_penalty_uah: 9,
          total_throughput_mwh: 0.2,
          committed_action: 'HOLD',
          committed_power_mw: 0,
          rank_by_regret: 3,
          evaluation_payload: { data_quality_tier: 'demo_grade' }
        }
      ]
    }

    const dataset = normalizeForecastComparison(response)

    expect(dataset.kind).toBe('forecast_comparison')
    expect(dataset.title).toBe('Latest forecast comparison')
    expect(dataset.tenantIds).toEqual(['client_003_dnipro_factory'])
    expect(dataset.anchorCount).toBe(1)
    expect(dataset.rows).toHaveLength(1)
    expect(dataset.rows[0]).toMatchObject({
      tenantId: 'client_003_dnipro_factory',
      evaluationId: 'eval-001',
      anchorTimestamp: '2026-05-04T20:00:00Z',
      forecastModelName: 'strict_similar_day',
      strategyKind: 'forecast_driven_lp',
      decisionValueUah: 110,
      oracleValueUah: 130,
      regretUah: 20,
      rankByRegret: 3
    })
  })
})
