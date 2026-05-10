import { describe, expect, it } from 'vitest'

import {
  buildControlRegretTimeline,
  buildOperatorDecisionReadinessItems,
  buildOperatorDecisionStateCards,
  buildOperatorStrategyEvidenceRows,
  buildSensitivityEvidenceRows
} from './operatorDecisionEvidence'
import type {
  BaselineLpPreview,
  DashboardBatteryStateResponse,
  DashboardExogenousSignalsResponse,
  ForecastDispatchSensitivityResponse,
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
      exogenousSignals
    })).toEqual([
      {
        label: 'Physical SOC',
        status: 'live',
        tone: 'green',
        detail: '54% from latest telemetry'
      },
      {
        label: 'Planning SOC',
        status: 'aligned',
        tone: 'green',
        detail: '58% start; 4% gap vs physical'
      },
      {
        label: 'Grid context',
        status: 'review',
        tone: 'orange',
        detail: 'tenant region affected; saving request active'
      },
      {
        label: 'Source freshness',
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
        tooltipFormula: 'starting_soc_source = request_override | telemetry_hourly | tenant_default'
      }),
      expect.objectContaining({
        label: 'Best comparator',
        value: 'risk_adjusted_value_gate_v0',
        meta: 'lowest mean regret in read model'
      }),
      expect.objectContaining({
        label: 'Grid context',
        value: '100%',
        meta: 'tenant region affected'
      })
    ])
  })
})
