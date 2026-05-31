import { describe, expect, it } from 'vitest'

import {
  buildAcademicMvpDtShadowComparisonRows,
  buildAcademicMvpGatePassportItems,
  buildV13ReadinessItems
} from './operatorFutureStack'

import type { AcademicMvpReadinessResponse } from '~/types/control-plane'

describe('operator future stack readiness helpers', () => {
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
        label: 'OREE source evidence',
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
        label: 'DAM/IDM preview',
        value: 'passed',
        status: 'ready',
        reason: '24 non-submittable DAM/IDM hourly preview rows'
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
})
