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
      v13_readiness: {
        gate_status: 'data_acquisition_needed',
        v13_candidate_generation_ready: false,
        dt_lava_ready: false,
        ready_rows: 0,
        readiness_rows: 5,
        missing_safe_switch_examples: 77,
        missing_required_inputs: [
          'oree_dam_publication_receipts_csv_path',
          'ua_context_safe_switch_examples_csv_path'
        ],
        top_priority_blocker: 'explicit_dam_publication_receipts',
        receipt_source_audit_probe_count: 5,
        receipt_source_audit_months_probed: [
          '01.2026',
          '02.2026',
          '03.2026',
          '04.2026',
          '05.2026'
        ],
        receipt_source_audit_candidate_found: false,
        receipt_source_audit_csv_generated: false,
        receipt_source_audit_all_probes_insufficient: true,
        source_governance_status: 'receipt_gated_for_market_submission',
        source_governance_label: 'receipt-gated for market submission',
        market_submission_receipt_gate_status: 'blocked_external_access',
        scmo_credentials_required_for_diploma_mvp: false,
        scmo_credentials_required_for_market_submission_grade_receipts: true,
        safe_switch_target_tenant_source_count: 5,
        safe_switch_max_new_examples_required: 18,
        safe_switch_acquisition_targets: [
          {
            acquisition_priority_rank: 1,
            tenant_id: 'client_004_kharkiv_hospital',
            source_model_name: 'nbeatsx_official_global_panel_horizon_calibrated_v1',
            current_prior_material_safe_switch_examples: 2,
            required_prior_material_safe_switch_examples: 20,
            target_new_prior_material_safe_switch_examples: 18,
            required_evidence_kind: 'train_prior_non_tail_risk_material_safe_switch_rows',
            recommended_next_step: 'acquire_ukrainian_context_and_backfill_safe_labels',
            target_is_precondition_only: true,
            market_execution_enabled: false
          }
        ],
        market_execution_enabled: false,
        boundary_doc: 'docs/technical/CURRENT_GOAL_BOUNDARY_V13.md',
        source_packet_path: 'data/research_runs/week3_dfl_ua_context_acquisition_v13/dfl_ua_context_v13_acquisition_summary.json'
      },
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
      bid_recommendation_preview: [
        {
          step_index: 0,
          interval_start: '2026-05-19T15:00:00Z',
          market_venue: 'DAM',
          side: 'SELL',
          operator_action: 'discharge',
          quantity_mw: 0.2,
          indicative_limit_price_uah_mwh: 4200,
          preview_only: true,
          market_execution_enabled: false,
          market_order_payload_emitted: false,
          proposed_bid_status: 'not_emitted_operator_preview',
          read_model_boundary: 'operator_preview_no_market_submission'
        },
        {
          step_index: 1,
          interval_start: '2026-05-19T18:00:00Z',
          market_venue: 'DAM',
          side: 'BUY',
          operator_action: 'charge',
          quantity_mw: 0.2,
          indicative_limit_price_uah_mwh: 3100,
          preview_only: true,
          market_execution_enabled: false,
          market_order_payload_emitted: false,
          proposed_bid_status: 'not_emitted_operator_preview',
          read_model_boundary: 'operator_preview_no_market_submission'
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
