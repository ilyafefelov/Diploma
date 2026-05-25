import { describe, expect, it } from 'vitest'

import {
  buildAcademicMvpDtShadowComparisonRows,
  buildAcademicMvpGatePassportItems,
  buildRecommendationInputSignalRows,
  buildRecommendationStrategySelectItems,
  filterOfficialPolicyValueSeries,
  buildPolicyForecastContextPoints,
  buildV13ReadinessItems,
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
  AcademicMvpReadinessResponse,
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

  it('builds V13 source-readiness chips without implying DT or execution readiness', () => {
    expect(buildV13ReadinessItems({
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
    })).toEqual([
      {
        label: 'V13 gate',
        value: 'data acquisition needed',
        status: 'blocked',
        reason: '0/5 source families ready; top blocker explicit_dam_publication_receipts'
      },
      {
        label: 'DAM receipts',
        value: 'receipt-gated for market submission',
        status: 'blocked',
        reason: 'receipt-gated for market submission; missing oree_dam_publication_receipts_csv_path; 5 months probed through 05.2026; no receipt CSV generated'
      },
      {
        label: 'Safe-switch evidence',
        value: '77 missing',
        status: 'blocked',
        reason: '20 prior/train non-tail-risk material examples per tenant/source required; top target client_004_kharkiv_hospital needs 18'
      },
      {
        label: 'Execution boundary',
        value: 'preview only',
        status: 'ready',
        reason: 'market_execution_enabled=false; DT/LAVA blocked'
      }
    ])
  })

  it('builds credentialless academic MVP gate chips from the gate passport', () => {
    expect(buildAcademicMvpGatePassportItems({
      claim_scope: 'credentialless_academic_mvp_readiness_not_market_execution',
      generated_at: '2026-05-25T02:36:18+00:00',
      academic_mvp_gate_passed: true,
      operator_preview_gate: {},
      source_governance: {},
      dt_lava_prototype_gate: {},
      dt_lava_teacher_contract_gate: {},
      offline_challenger_gate: {},
      dt_research_shadow_gate: {
        passed_for_academic_mvp: true,
        status: 'passed_research_shadow_not_promotable',
        research_shadow_training_rows: 3741,
        promotable_v13_permitted_training_rows: 0,
        forecast_context_coverage_status: 'partial_missing_tft',
        forecast_context_present_families: ['nbeatsx'],
        forecast_context_missing_families: ['tft'],
        publication_receipt_verified: false,
        market_availability_claim: false,
        research_shadow_not_promotable: true,
        market_execution_enabled: false
      },
      prototype_contract: {},
      prototype_evidence_scorecard: {
        scorecard_passed_for_academic_mvp: true,
        operator_bid_preview_rows: 1,
        teacher_train_selection_rows: 1,
        validation_tenant_anchor_count: 1,
        market_execution_enabled: false
      },
      prototype_phase_readiness: {
        phase_0_v13_source_readiness: {
          status: 'blocked_market_submission_receipts',
          ready_for_training: false,
          market_execution_enabled: false
        },
        phase_1_lava_npz_smoke: {
          status: 'passed_ci_smoke_not_promotion',
          gate_passed: true,
          market_execution_enabled: false
        },
        phase_2_v13_gated_teacher_contract: {
          status: 'passed_contract_training_rows_gated',
          permitted_model_training_rows: 0,
          market_execution_enabled: false
        },
        phase_3_offline_challenger: {
          status: 'passed_non_promotion_evidence',
          promotion_gate_passed: false,
          market_execution_enabled: false
        },
        phase_4_full_schedule_dfl: {
          status: 'future_work_not_started',
          gate_passed: false,
          market_execution_enabled: false
        },
        market_execution_enabled: false
      },
      gate_passport: {
        dam_bid_recommendation_preview_gate: {
          passed: true,
          status: 'passed',
          claim_scope: 'non_submittable_dam_buy_sell_hold_preview',
          bid_recommendation_preview_rows: 24
        },
        dt_lava_prototype_ci_smoke_gate: {
          passed: true,
          status: 'passed',
          claim_scope: 'lava_npz_ci_smoke_validation_not_promotion'
        },
        lava_npz_smoke_packet_validation_gate: {
          passed: true,
          status: 'passed',
          claim_scope: 'lava_npz_margin_smoke_packet_validation_not_market_execution',
          artifact_hashes_valid: true,
          metrics_valid: true,
          aggregate_valid: true,
          npz_contract_valid: true,
          baseline_comparison_valid: true,
          permits_model_training: false,
          promotion_gate_passed: false,
          market_execution_enabled: false
        },
        v13_gated_teacher_contract_gate: {
          passed: true,
          status: 'passed',
          claim_scope: 'candidate_index_or_schedule_family_teacher_contract',
          permitted_model_training_rows: 0,
          permits_model_training: false
        },
        offline_challenger_non_promotion_gate: {
          passed: true,
          status: 'passed',
          claim_scope: 'offline_challenger_packet_explains_non_promotion',
          promotion_gate_passed: false
        },
        dt_research_shadow_smoke_gate: {
          passed: true,
          status: 'passed_research_shadow_not_promotable',
          claim_scope: 'dt_research_shadow_not_promotable_not_market_execution',
          research_shadow_training_rows: 3741,
          promotable_v13_permitted_training_rows: 0,
          promotion_gate_passed: false,
          market_execution_enabled: false
        },
        prototype_evidence_scorecard_gate: {
          passed: true,
          status: 'passed',
          claim_scope: 'credentialless_dfl_dt_prototype_evidence_scorecard_not_market_execution',
          operator_bid_preview_rows: 24,
          teacher_train_selection_rows: 3741,
          validation_tenant_anchor_count: 90,
          permits_model_training: false,
          promotion_gate_passed: false,
          market_execution_enabled: false
        },
        dt_lava_training_promotion_gate: {
          passed: false,
          status: 'blocked_until_v13_source_readiness',
          claim_scope: 'future_dt_lava_strict_lp_oracle_promotion',
          required_for_academic_mvp: false
        },
        market_submission_receipt_gate: {
          passed: false,
          status: 'blocked_external_access',
          claim_scope: 'market_submission_grade_receipt_readiness',
          required_for_academic_mvp: false
        },
        market_execution_safety_gate: {
          passed: true,
          status: 'passed',
          claim_scope: 'prove_no_market_execution_enabled_true'
        }
      },
      market_submission_ready: false,
      market_execution_gate_passed: false,
      promotion_gate_passed: false,
      permits_model_training: false,
      market_execution_enabled: false,
      no_market_execution_safety_gate_passed: true,
      next_gate: 'credentialless_academic_mvp_ready_for_thesis_demo',
      artifact_validation: {
        passed: true,
        failures: [],
        gate_results: {
          dfl_dt_prototype_contract_gate: {
            passed: true,
            market_execution_enabled: false
          },
          market_execution_gate: {
            passed: true,
            market_execution_enabled: false
          }
        },
        market_execution_enabled: false
      },
      source_packet_path: 'data/research_runs/week3_credentialless_academic_mvp_current/credentialless_academic_mvp_readiness_summary.json',
      artifact_validation_packet_path: 'data/research_runs/week3_credentialless_academic_mvp_current/credentialless_academic_mvp_readiness_validation.json'
    })).toEqual([
      {
        label: 'Academic MVP',
        value: 'passed',
        status: 'ready',
        reason: 'credentialless_academic_mvp_ready_for_thesis_demo'
      },
      {
        label: 'Packet validation',
        value: 'passed',
        status: 'ready',
        reason: 'standalone validator artifact; data/research_runs/week3_credentialless_academic_mvp_current/credentialless_academic_mvp_readiness_validation.json'
      },
      {
        label: 'Prototype roadmap',
        value: 'credentialless prototype',
        status: 'ready',
        reason: 'Phase 0 blocked market submission receipts; Phase 1/2/3 credentialless evidence passed; Phase 4 future work'
      },
      {
        label: 'Evidence scorecard',
        value: 'passed',
        status: 'ready',
        reason: '24 bid-preview rows; 3,741 teacher rows; 90 challenger anchors'
      },
      {
        label: 'DAM bid preview',
        value: 'passed',
        status: 'ready',
        reason: '24 non-submittable DAM bid-preview rows'
      },
      {
        label: 'DT/LAVA smoke',
        value: 'passed',
        status: 'ready',
        reason: 'lava_npz_ci_smoke_validation_not_promotion'
      },
      {
        label: 'LAVA validation',
        value: 'passed',
        status: 'ready',
        reason: 'lava_npz_margin_smoke_packet_validation_not_market_execution'
      },
      {
        label: 'Teacher contract',
        value: 'passed',
        status: 'ready',
        reason: '0 training rows; candidate_index_or_schedule_family_teacher_contract'
      },
      {
        label: 'Offline challenger',
        value: 'passed',
        status: 'ready',
        reason: 'non-promotion evidence; offline_challenger_packet_explains_non_promotion'
      },
      {
        label: 'DT shadow',
        value: 'research smoke',
        status: 'ready',
        reason: '3,741 research rows; 0 promotable rows; forecast partial missing tft; receipt unverified'
      },
      {
        label: 'Future training',
        value: 'not required',
        status: 'ready',
        reason: 'blocked until V13 source readiness; not required for academic MVP'
      },
      {
        label: 'SCMO receipts',
        value: 'not required',
        status: 'ready',
        reason: 'not required for academic MVP'
      },
      {
        label: 'Execution safety',
        value: 'passed',
        status: 'ready',
        reason: 'market_execution_enabled=false'
      }
    ])
  })

  it('builds DT shadow regret/value comparison rows for the defense dashboard', () => {
    expect(buildAcademicMvpDtShadowComparisonRows({
      dt_research_shadow_gate: {
        evaluation_metrics: {
          dt_selected_mean_regret_uah: 507.898,
          dt_selected_mean_value_uah: 3403.586,
          v2_plus_mean_regret_uah: 510.818,
          v2_plus_mean_value_uah: 3400.667,
          strict_mean_regret_uah: 431.703,
          strict_mean_value_uah: 3479.781,
          behavior_cloning_mean_regret_uah: 510.818,
          behavior_cloning_mean_value_uah: 3400.667
        }
      }
    } as unknown as AcademicMvpReadinessResponse)).toEqual([
      {
        label: 'DT shadow',
        meanRegretUah: 507.898,
        meanValueUah: 3403.586,
        regretBarWidthPercent: 99,
        status: 'research-shadow',
        note: 'HF/local transformer candidate-index policy'
      },
      {
        label: 'V2+ fallback',
        meanRegretUah: 510.818,
        meanValueUah: 3400.667,
        regretBarWidthPercent: 100,
        status: 'fallback',
        note: 'teacher / comparator / fallback'
      },
      {
        label: 'Strict reference',
        meanRegretUah: 431.703,
        meanValueUah: 3479.781,
        regretBarWidthPercent: 85,
        status: 'reference',
        note: 'strict LP/oracle reference'
      },
      {
        label: 'Behavior cloning',
        meanRegretUah: 510.818,
        meanValueUah: 3400.667,
        regretBarWidthPercent: 100,
        status: 'control',
        note: 'imitation control, accuracy secondary'
      }
    ])

    expect(buildAcademicMvpDtShadowComparisonRows(null)).toEqual([])
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
