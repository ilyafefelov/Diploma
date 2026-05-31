import { describe, expect, it } from 'vitest'

import {
  buildControlRegretTimeline,
  buildOperatorDecisionReadinessItems,
  buildOperatorDecisionStateCards,
  buildOperatorForecastScenarioCandidateRows,
  buildOperatorStrategyEvidenceRows,
  buildSensitivityEvidenceRows
} from './operatorDecisionEvidence'
import type {
  BaselineLpPreview,
  DashboardBatteryStateResponse,
  DashboardExogenousSignalsResponse,
  ForecastDispatchSensitivityResponse,
  OperatorRecommendationResponse,
  RealDataBenchmarkResponse
} from '../types/control-plane'
import type { DefenseModelRow } from './defenseDataset'

describe('operator decision evidence', () => {
  it('orders strategy evidence by mean regret and compares every row to strict control', () => {
    const rows: DefenseModelRow[] = [
      {
        modelName: 'strict_similar_day',
        role: 'control',
        anchorCount: 90,
        meanRegretUah: 390,
        medianRegretUah: 274,
        meanDecisionValueUah: 2100,
        meanOracleValueUah: 2490,
        winRate: 0.49,
        meanThroughputMwh: 0.379
      },
      {
        modelName: 'nbeatsx_silver_v0',
        role: 'forecast_candidate',
        anchorCount: 90,
        meanRegretUah: 481,
        medianRegretUah: 330,
        meanDecisionValueUah: 2009,
        meanOracleValueUah: 2490,
        winRate: 0.26,
        meanThroughputMwh: 0.377
      },
      {
        modelName: 'risk_adjusted_value_gate_v0',
        role: 'ensemble_gate',
        anchorCount: 90,
        meanRegretUah: 454,
        medianRegretUah: 312,
        meanDecisionValueUah: 2036,
        meanOracleValueUah: 2490,
        winRate: 1,
        meanThroughputMwh: 0.409
      }
    ]

    expect(buildOperatorStrategyEvidenceRows(rows)).toEqual([
      {
        modelName: 'strict_similar_day',
        role: 'control',
        meanRegretUah: 390,
        winRate: 0.49,
        regretDeltaVsControlUah: 0,
        controlComparisonLabel: 'control'
      },
      {
        modelName: 'risk_adjusted_value_gate_v0',
        role: 'ensemble_gate',
        meanRegretUah: 454,
        winRate: 1,
        regretDeltaVsControlUah: 64,
        controlComparisonLabel: '+64 UAH vs control'
      },
      {
        modelName: 'nbeatsx_silver_v0',
        role: 'forecast_candidate',
        meanRegretUah: 481,
        winRate: 0.26,
        regretDeltaVsControlUah: 91,
        controlComparisonLabel: '+91 UAH vs control'
      }
    ])
  })

  it('falls back to frozen thesis evidence when live benchmark rows are unavailable', () => {
    const recommendation = {
      selected_strategy_id: 'schedule_value_learner_v2_plus',
      available_strategies: [
        {
          strategy_id: 'schedule_value_learner_v2_plus',
          label: 'Offline V2+ schedule/value learner',
          enabled: true,
          reason: 'frozen comparator',
          mean_regret_uah: 174.77,
          win_rate: 1
        }
      ],
      recommendation_schedule: [
        {
          interval_start: '2026-05-24T00:00:00',
          throughput_mwh: 0.2
        }
      ]
    } as unknown as OperatorRecommendationResponse

    const rows = buildOperatorStrategyEvidenceRows([], recommendation)

    expect(rows.length).toBeGreaterThan(1)
    expect(rows[0]).toEqual(expect.objectContaining({
      modelName: 'Calibrated V2+',
      meanRegretUah: 174.77,
      winRate: 1
    }))
    expect(rows.some(row => row.modelName === 'strict_similar_day')).toBe(true)
  })

  it('builds chronological strict-control regret timeline from benchmark rows', () => {
    const benchmark = {
      rows: [
        {
          anchor_timestamp: '2026-01-03T00:00:00Z',
          forecast_model_name: 'tft_silver_v0',
          regret_uah: 999,
          decision_value_uah: 10,
          oracle_value_uah: 100,
          total_throughput_mwh: 0.2
        },
        {
          anchor_timestamp: '2026-01-02T00:00:00Z',
          forecast_model_name: 'strict_similar_day',
          regret_uah: 200,
          decision_value_uah: 800,
          oracle_value_uah: 1000,
          total_throughput_mwh: 0.3
        },
        {
          anchor_timestamp: '2026-01-01T00:00:00Z',
          forecast_model_name: 'strict_similar_day',
          regret_uah: 100,
          decision_value_uah: 700,
          oracle_value_uah: 800,
          total_throughput_mwh: 0.4
        }
      ]
    } as RealDataBenchmarkResponse

    expect(buildControlRegretTimeline(benchmark, 2)).toEqual([
      {
        anchorLabel: '01 Jan',
        regretUah: 100,
        decisionValueUah: 700,
        oracleValueUah: 800,
        throughputMwh: 0.4
      },
      {
        anchorLabel: '02 Jan',
        regretUah: 200,
        decisionValueUah: 800,
        oracleValueUah: 1000,
        throughputMwh: 0.3
      }
    ])
  })

  it('keeps a non-empty regret timeline when the live benchmark endpoint is missing', () => {
    const recommendation = {
      economics: {
        total_throughput_mwh: 0.66
      }
    } as unknown as OperatorRecommendationResponse

    const rows = buildControlRegretTimeline(null, 24, recommendation)

    expect(rows.length).toBeGreaterThan(1)
    expect(rows[0]).toEqual(expect.objectContaining({
      anchorLabel: 'strict_similar_day',
      regretUah: 310.58
    }))
    expect(rows.some(row => row.throughputMwh > 0)).toBe(true)
  })

  it('converts sensitivity buckets into chart-ready rows', () => {
    const sensitivity = {
      bucket_summary: [
        {
          diagnostic_bucket: 'forecast_error',
          rows: 10,
          mean_regret_uah: 500,
          mean_forecast_mae_uah_mwh: 1200,
          mean_dispatch_spread_error_uah_mwh: 230
        }
      ]
    } as ForecastDispatchSensitivityResponse

    expect(buildSensitivityEvidenceRows(sensitivity)).toEqual([
      {
        bucket: 'forecast_error',
        rows: 10,
        meanRegretUah: 500,
        meanForecastMaeUahMwh: 1200,
        meanDispatchSpreadErrorUahMwh: 230
      }
    ])
  })

  it('keeps diagnosis rows visible when forecast sensitivity is not materialized', () => {
    const recommendation = {
      recommendation_schedule: [
        { interval_start: '2026-05-24T00:00:00' },
        { interval_start: '2026-05-24T18:00:00' }
      ],
      forecast_model_series: [
        {
          model_name: 'nbeatsx_silver_v0',
          points: [{}, {}, {}]
        }
      ]
    } as unknown as OperatorRecommendationResponse

    const rows = buildSensitivityEvidenceRows(null, recommendation)

    expect(rows).toEqual([
      expect.objectContaining({
        bucket: 'strict control',
        rows: 90,
        meanRegretUah: 310.58
      }),
      expect.objectContaining({
        bucket: 'selected V2+',
        rows: 2,
        meanRegretUah: 174.77
      }),
      expect.objectContaining({
        bucket: 'forecast context',
        rows: 3
      })
    ])
  })

  it('formats pre-publication forecast scenario candidates in rank order', () => {
    const recommendation = {
      decision_advisor: {
        forecast_scenario_candidates: [
          {
            candidate_id: 'forecast_scenario:DAM:2026-05-20:tft_official_v0',
            model_name: 'tft_official_v0',
            rank: 2,
            advisor_decision: 'ranked_abstain_preview_only',
            decision_value_uah: 1234.4,
            regret_to_best_uah: 55.8,
            total_throughput_mwh: 0.742,
            gatekeeper_status: 'passed_lp_physical_constraints_preview_only',
            selected_for_operator_preview: false
          },
          {
            candidate_id: 'forecast_scenario:DAM:2026-05-20:nbeatsx_official_v0',
            model_name: 'nbeatsx_official_v0',
            rank: 1,
            advisor_decision: 'ranked_abstain_preview_only',
            decision_value_uah: 1290.2,
            regret_to_best_uah: 0,
            total_throughput_mwh: 0.68,
            gatekeeper_status: 'passed_lp_physical_constraints_preview_only',
            selected_for_operator_preview: true
          }
        ]
      }
    } as unknown as OperatorRecommendationResponse

    expect(buildOperatorForecastScenarioCandidateRows(recommendation)).toEqual([
      {
        candidateId: 'forecast_scenario:DAM:2026-05-20:nbeatsx_official_v0',
        modelName: 'Nbeatsx Official V0',
        rankLabel: '#1',
        decisionValueLabel: '1,290 UAH',
        regretLabel: 'best',
        throughputLabel: '0.68 MWh',
        statusLabel: 'selected preview',
        selectedForPreview: true
      },
      {
        candidateId: 'forecast_scenario:DAM:2026-05-20:tft_official_v0',
        modelName: 'TFT Official V0',
        rankLabel: '#2',
        decisionValueLabel: '1,234 UAH',
        regretLabel: '56 UAH regret',
        throughputLabel: '0.74 MWh',
        statusLabel: 'ranked abstain preview only',
        selectedForPreview: false
      }
    ])
  })

  it('summarizes operator decision readiness from physical state, planning state, and live context', () => {
    const batteryState = {
      latest_telemetry: {
        current_soc: 0.54,
        observed_at: '2026-05-05T12:00:00Z'
      },
      hourly_snapshot: null,
      fallback_reason: null
    } as DashboardBatteryStateResponse
    const baselinePreview = {
      starting_soc_fraction: 0.58,
      starting_soc_source: 'telemetry_hourly'
    } as BaselineLpPreview
    const exogenousSignals = {
      tenant_region_affected: true,
      outage_flag: false,
      saving_request_flag: true,
      event_source_freshness_hours: 2,
      latest_weather: { freshness_hours: 1.5 }
    } as DashboardExogenousSignalsResponse

    expect(buildOperatorDecisionReadinessItems({
      batteryState,
      baselinePreview,
      operatorRecommendation: {
        selected_strategy_id: 'schedule_value_learner_v2_plus',
        selection_reason: 'manual strategy: Offline V2+ schedule/value learner',
        soc_source: 'telemetry_live',
        soc_projection: [
          {
            timestamp: '2026-05-05T12:00:00Z',
            physical_soc: 0.54,
            estimated_soc: 0.58,
            planning_soc: 0.58,
            soc_source: 'telemetry_live',
            confidence: 'observed'
          }
        ],
        review_required: false,
        readiness_warnings: []
      } as unknown as OperatorRecommendationResponse,
      exogenousSignals
    })).toEqual([
      {
        label: 'Physical SOC',
        status: 'live',
        tone: 'green',
        detail: '58% via telemetry_live'
      },
      {
        label: 'Selected strategy',
        status: 'Offline V2+',
        tone: 'green',
        detail: 'manual strategy: Offline V2+ schedule/value learner'
      },
      {
        label: 'Grid context',
        status: 'review',
        tone: 'orange',
        detail: 'tenant region affected; saving request active'
      },
      {
        label: 'Readiness',
        status: 'fresh',
        tone: 'green',
        detail: 'weather 1.5h / grid 2.0h'
      }
    ])
  })

  it('builds operator state cards with hover explanation metadata', () => {
    const batteryState = {
      latest_telemetry: null,
      hourly_snapshot: { soc_close: 0.54, telemetry_freshness: 'hourly_snapshot_stale' },
      fallback_reason: 'hourly_snapshot_stale'
    } as DashboardBatteryStateResponse
    const baselinePreview = {
      starting_soc_fraction: 0.5,
      starting_soc_source: 'tenant_default'
    } as BaselineLpPreview
    const exogenousSignals = {
      national_grid_risk_score: 1,
      tenant_region_affected: true
    } as DashboardExogenousSignalsResponse
    const modelRows: DefenseModelRow[] = [
      {
        modelName: 'risk_adjusted_value_gate_v0',
        role: 'ensemble_gate',
        anchorCount: 90,
        meanRegretUah: 1566,
        medianRegretUah: 1000,
        meanDecisionValueUah: 2000,
        meanOracleValueUah: 3566,
        winRate: 0.5,
        meanThroughputMwh: 0.3
      }
    ]

    expect(buildOperatorDecisionStateCards({
      batteryState,
      baselinePreview,
      exogenousSignals,
      modelRows
    })).toEqual([
      expect.objectContaining({
        label: 'Physical SOC',
        value: '54%',
        meta: 'hourly_snapshot_stale',
        tooltipTitle: 'Physical SOC'
      }),
      expect.objectContaining({
        label: 'Planning SOC',
        value: '50%',
        meta: 'tenant_default',
        tooltipFormula: 'planning_soc = feasible_schedule[0].projected_soc_after_fraction'
      }),
      expect.objectContaining({
        label: 'V2+ comparator',
        value: '175 UAH',
        meta: '4/4 rolling; strict remains fallback'
      }),
      expect.objectContaining({
        label: 'Grid context',
        value: '100%',
        meta: 'tenant region affected'
      })
    ])
  })
})
