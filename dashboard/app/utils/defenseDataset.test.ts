import { describe, expect, it } from 'vitest'

import {
  buildDefenseModelRows,
  buildResearchReadinessRows,
  summarizeDefenseBenchmark
} from './defenseDataset'
import type {
  DecisionPolicyPreviewResponse,
  DecisionTransformerTrajectoryResponse,
  DflRelaxedPilotResponse,
  RealDataBenchmarkResponse,
  SimulatedLiveTradingResponse
} from '../types/control-plane'

const benchmarkResponse: RealDataBenchmarkResponse = {
  tenant_id: 'client_003_dnipro_factory',
  market_venue: 'DAM',
  generated_at: '2026-05-05T12:00:00Z',
  data_quality_tier: 'thesis_grade',
  anchor_count: 2,
  model_count: 2,
  best_model_name: 'strict_similar_day',
  mean_regret_uah: 18,
  median_regret_uah: 15,
  rows: [
    {
      evaluation_id: 'eval-001',
      anchor_timestamp: '2026-05-03T20:00:00Z',
      forecast_model_name: 'strict_similar_day',
      decision_value_uah: 120,
      oracle_value_uah: 130,
      regret_uah: 10,
      regret_ratio: 0.0769,
      total_degradation_penalty_uah: 4,
      total_throughput_mwh: 0.2,
      committed_action: 'HOLD',
      committed_power_mw: 0,
      rank_by_regret: 1,
      evaluation_payload: { data_quality_tier: 'thesis_grade' }
    },
    {
      evaluation_id: 'eval-002',
      anchor_timestamp: '2026-05-04T20:00:00Z',
      forecast_model_name: 'strict_similar_day',
      decision_value_uah: 118,
      oracle_value_uah: 140,
      regret_uah: 22,
      regret_ratio: 0.1571,
      total_degradation_penalty_uah: 5,
      total_throughput_mwh: 0.25,
      committed_action: 'DISCHARGE',
      committed_power_mw: 0.1,
      rank_by_regret: 2,
      evaluation_payload: { data_quality_tier: 'thesis_grade' }
    },
    {
      evaluation_id: 'eval-003',
      anchor_timestamp: '2026-05-03T20:00:00Z',
      forecast_model_name: 'tft_silver_v0',
      decision_value_uah: 100,
      oracle_value_uah: 130,
      regret_uah: 30,
      regret_ratio: 0.2308,
      total_degradation_penalty_uah: 4,
      total_throughput_mwh: 0.2,
      committed_action: 'HOLD',
      committed_power_mw: 0,
      rank_by_regret: 2,
      evaluation_payload: { data_quality_tier: 'thesis_grade' }
    },
    {
      evaluation_id: 'eval-004',
      anchor_timestamp: '2026-05-04T20:00:00Z',
      forecast_model_name: 'tft_silver_v0',
      decision_value_uah: 126,
      oracle_value_uah: 140,
      regret_uah: 14,
      regret_ratio: 0.1,
      total_degradation_penalty_uah: 5,
      total_throughput_mwh: 0.25,
      committed_action: 'DISCHARGE',
      committed_power_mw: 0.1,
      rank_by_regret: 1,
      evaluation_payload: { data_quality_tier: 'thesis_grade' }
    }
  ]
}

describe('defense dataset summaries', () => {
  it('summarizes benchmark evidence without inventing fallback data', () => {
    const summary = summarizeDefenseBenchmark(benchmarkResponse)

    expect(summary).toMatchObject({
      tenantId: 'client_003_dnipro_factory',
      marketVenue: 'DAM',
      dataQualityTier: 'thesis_grade',
      anchorCount: 2,
      modelCount: 2,
      bestModelName: 'strict_similar_day',
      meanRegretUah: 18,
      medianRegretUah: 15,
      sourceMode: 'fastapi_live'
    })
  })

  it('groups model rows and keeps strict similar-day as control first', () => {
    const rows = buildDefenseModelRows(benchmarkResponse)

    expect(rows.map(row => row.modelName)).toEqual(['strict_similar_day', 'tft_silver_v0'])
    expect(rows[0]).toMatchObject({
      modelName: 'strict_similar_day',
      role: 'control',
      anchorCount: 2,
      winRate: 0.5,
      meanRegretUah: 16,
      medianRegretUah: 16
    })
    expect(rows.at(1)?.meanRegretUah).toBe(22)
  })

  it('marks DFL and DT as research primitives, not live trading claims', () => {
    const dfl: DflRelaxedPilotResponse = {
      tenant_id: 'client_003_dnipro_factory',
      row_count: 12,
      mean_relaxed_regret_uah: 42,
      academic_scope: 'differentiable_relaxed_lp_pilot_not_final_dfl',
      rows: []
    }
    const dt: DecisionTransformerTrajectoryResponse = {
      tenant_id: 'client_003_dnipro_factory',
      row_count: 96,
      episode_count: 4,
      academic_scope: 'offline_dt_training_trajectory_not_live_policy',
      rows: []
    }
    const live: SimulatedLiveTradingResponse = {
      tenant_id: 'client_003_dnipro_factory',
      row_count: 24,
      simulated_only: true,
      rows: []
    }
    const dtPolicy: DecisionPolicyPreviewResponse = {
      tenant_id: 'client_003_dnipro_factory',
      row_count: 24,
      policy_run_id: 'dt-preview-1',
      created_at: '2026-05-05T12:00:00Z',
      policy_readiness: 'ready_for_operator_preview',
      live_policy_claim: false,
      market_execution_enabled: false,
      constraint_violation_count: 0,
      mean_value_gap_uah: 17,
      total_value_vs_hold_uah: 114,
      forecast_context_source: 'nbeatsx_tft_forecast_context',
      forecast_context_row_count: 24,
      forecast_context_coverage_ratio: 1,
      forecast_context_warning: null,
      policy_state_features: ['SOC', 'SOH', 'market price'],
      policy_value_interpretation: 'value_gap = oracle - expected',
      operator_boundary: 'preview_only_requires_gatekeeper_and_operator_review',
      academic_scope: 'offline_dt_policy_preview_not_market_execution',
      rows: []
    }

    const rows = buildResearchReadinessRows({ dfl, dt, dtPolicy, live })

    expect(rows).toEqual([
      {
        label: 'DFL',
        status: 'pilot',
        metric: '42 UAH relaxed regret',
        boundary: 'not full DFL'
      },
      {
        label: 'Decision Transformer',
        status: 'trajectory data',
        metric: '4 episodes / 96 rows',
        boundary: 'not live policy'
      },
      {
        label: 'DT policy preview',
        status: 'ready_for_operator_preview',
        metric: '17 UAH mean value gap / 100% forecast-conditioned',
        boundary: 'preview only'
      },
      {
        label: 'Paper trading',
        status: 'simulated only',
        metric: '24 rows',
        boundary: 'not market execution'
      }
    ])
  })
})
