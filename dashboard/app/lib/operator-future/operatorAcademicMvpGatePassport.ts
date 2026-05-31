import type { AcademicMvpReadinessResponse } from '~/types/control-plane'
import type { V13ReadinessItem } from './operatorFutureStackReadiness'

export type AcademicMvpGatePassportItem = V13ReadinessItem

export const buildAcademicMvpGatePassportItems = (
  readiness: AcademicMvpReadinessResponse | null | undefined
): AcademicMvpGatePassportItem[] => {
  if (!readiness) {
    return [
      {
        label: 'Academic MVP',
        value: 'packet pending',
        status: 'blocked',
        reason: 'credentialless academic MVP readiness packet not loaded'
      }
    ]
  }

  const artifactValidation = asRecord(readiness.artifact_validation)
  const artifactValidationPassed = artifactValidation.passed === true
    && artifactValidation.market_execution_enabled !== true
  const artifactValidationFailures = Array.isArray(artifactValidation.failures)
    ? artifactValidation.failures.filter(failure => typeof failure === 'string' && failure.length > 0)
    : []
  const phaseReadiness = asRecord(readiness.prototype_phase_readiness)
  const phase0 = asRecord(phaseReadiness.phase_0_v13_source_readiness)
  const phase1 = asRecord(phaseReadiness.phase_1_lava_npz_smoke)
  const phase2 = asRecord(phaseReadiness.phase_2_v13_gated_teacher_contract)
  const phase3 = asRecord(phaseReadiness.phase_3_offline_challenger)
  const phase4 = asRecord(phaseReadiness.phase_4_full_schedule_dfl)
  const prototypeRoadmapReady = phase0.status === 'blocked_market_submission_receipts'
    && phase1.status === 'passed_ci_smoke_not_promotion'
    && phase2.status === 'passed_contract_training_rows_gated'
    && phase3.status === 'passed_non_promotion_evidence'
    && phase4.status === 'future_work_not_started'
    && phaseReadiness.market_execution_enabled !== true
  const gatePassport = asRecord(readiness.gate_passport)
  const evidenceScorecard = asRecord(readiness.prototype_evidence_scorecard)
  const scorecardGate = asRecord(gatePassport.prototype_evidence_scorecard_gate)
  const scorecardPassed = gatePassed(scorecardGate)
    && evidenceScorecard.scorecard_passed_for_academic_mvp === true
    && scorecardGate.market_execution_enabled !== true
    && evidenceScorecard.market_execution_enabled !== true
  const damBidPreviewGate = asRecord(gatePassport.dam_bid_recommendation_preview_gate)
  const dtLavaSmokeGate = asRecord(gatePassport.dt_lava_prototype_ci_smoke_gate)
  const lavaValidationGate = asRecord(gatePassport.lava_npz_smoke_packet_validation_gate)
  const teacherContractGate = asRecord(gatePassport.v13_gated_teacher_contract_gate)
  const offlineChallengerGate = asRecord(gatePassport.offline_challenger_non_promotion_gate)
  const dtResearchShadowGate = asRecord(readiness.dt_research_shadow_gate)
  const dtResearchShadowSmokeGate = asRecord(gatePassport.dt_research_shadow_smoke_gate)
  const dtLavaTrainingGate = asRecord(gatePassport.dt_lava_training_promotion_gate)
  const marketSubmissionReceiptGate = asRecord(gatePassport.market_submission_receipt_gate)
  const marketExecutionSafetyGate = asRecord(gatePassport.market_execution_safety_gate)
  const bidPreviewRows = damBidPreviewGate.bid_recommendation_preview_rows
  const permittedTrainingRows = teacherContractGate.permitted_model_training_rows
  const dtResearchShadowReady = gatePassed(dtResearchShadowSmokeGate)
    && dtResearchShadowGate.research_shadow_not_promotable === true
    && dtResearchShadowGate.publication_receipt_verified === false
    && dtResearchShadowGate.market_availability_claim === false
    && dtResearchShadowGate.market_execution_enabled !== true

  return [
    {
      label: 'Academic MVP',
      value: readiness.academic_mvp_gate_passed ? 'passed' : 'blocked',
      status: readiness.academic_mvp_gate_passed ? 'ready' : 'blocked',
      reason: readiness.next_gate || readiness.claim_scope
    },
    {
      label: 'Packet validation',
      value: artifactValidationPassed ? 'passed' : 'blocked',
      status: artifactValidationPassed ? 'ready' : 'blocked',
      reason: artifactValidationFailures.length > 0
        ? artifactValidationFailures.join('; ')
        : `standalone validator artifact; ${readiness.artifact_validation_packet_path}`
    },
    {
      label: 'Prototype roadmap',
      value: prototypeRoadmapReady ? 'credentialless prototype' : 'blocked',
      status: prototypeRoadmapReady ? 'ready' : 'blocked',
      reason: prototypeRoadmapReady
        ? 'Phase 0 blocked market submission receipts; Phase 1/2/3 credentialless evidence passed; Phase 4 future work'
        : [
            `Phase 0 ${formatUnknownStatus(phase0.status)}`,
            `Phase 1 ${formatUnknownStatus(phase1.status)}`,
            `Phase 2 ${formatUnknownStatus(phase2.status)}`,
            `Phase 3 ${formatUnknownStatus(phase3.status)}`,
            `Phase 4 ${formatUnknownStatus(phase4.status)}`
          ].join('; ')
    },
    {
      label: 'Evidence scorecard',
      value: scorecardPassed ? 'passed' : 'blocked',
      status: scorecardPassed ? 'ready' : 'blocked',
      reason: scorecardPassed
        ? [
            `${formatCount(scorecardGate.operator_bid_preview_rows)} bid-preview rows`,
            `${formatCount(scorecardGate.teacher_train_selection_rows)} teacher rows`,
            `${formatCount(scorecardGate.validation_tenant_anchor_count)} challenger anchors`
          ].join('; ')
        : 'prototype evidence scorecard pending'
    },
    {
      label: 'DAM/IDM preview',
      value: formatGateStatus(damBidPreviewGate),
      status: gatePassed(damBidPreviewGate) ? 'ready' : 'blocked',
      reason: typeof bidPreviewRows === 'number'
        ? `${bidPreviewRows.toLocaleString('en-GB')} non-submittable DAM/IDM hourly preview rows`
        : gateClaimScope(damBidPreviewGate, 'non-submittable DAM/IDM hourly preview rows pending')
    },
    {
      label: 'DT/LAVA smoke',
      value: formatGateStatus(dtLavaSmokeGate),
      status: gatePassed(dtLavaSmokeGate) ? 'ready' : 'blocked',
      reason: gateClaimScope(dtLavaSmokeGate, 'LAVA NPZ CI smoke validation pending')
    },
    {
      label: 'LAVA validation',
      value: formatGateStatus(lavaValidationGate),
      status: gatePassed(lavaValidationGate) ? 'ready' : 'blocked',
      reason: gateClaimScope(lavaValidationGate, 'LAVA NPZ packet validation pending')
    },
    {
      label: 'Teacher contract',
      value: formatGateStatus(teacherContractGate),
      status: gatePassed(teacherContractGate) ? 'ready' : 'blocked',
      reason: [
        typeof permittedTrainingRows === 'number'
          ? `${permittedTrainingRows.toLocaleString('en-GB')} training rows`
          : 'training rows gated',
        gateClaimScope(teacherContractGate, 'candidate-index teacher contract pending')
      ].join('; ')
    },
    {
      label: 'Offline challenger',
      value: formatGateStatus(offlineChallengerGate),
      status: gatePassed(offlineChallengerGate) ? 'ready' : 'blocked',
      reason: `non-promotion evidence; ${gateClaimScope(offlineChallengerGate, 'offline challenger packet pending')}`
    },
    {
      label: 'DT shadow',
      value: dtResearchShadowReady ? 'research smoke' : formatGateStatus(dtResearchShadowSmokeGate),
      status: dtResearchShadowReady ? 'ready' : 'blocked',
      reason: dtResearchShadowReady
        ? [
            `${formatCount(dtResearchShadowGate.research_shadow_training_rows)} research rows`,
            `${formatCount(dtResearchShadowGate.promotable_v13_permitted_training_rows)} promotable rows`,
            `forecast ${formatUnknownStatus(dtResearchShadowGate.forecast_context_coverage_status)}`,
            'receipt unverified'
          ].join('; ')
        : gateClaimScope(dtResearchShadowSmokeGate, 'chronological DT research-shadow packet pending')
    },
    {
      label: 'Future training',
      value: gateRequiredForAcademicMvp(dtLavaTrainingGate) ? formatGateStatus(dtLavaTrainingGate) : 'not required',
      status: gatePassed(dtLavaTrainingGate) || !gateRequiredForAcademicMvp(dtLavaTrainingGate) ? 'ready' : 'blocked',
      reason: gateRequiredForAcademicMvp(dtLavaTrainingGate)
        ? gateClaimScope(dtLavaTrainingGate, 'future DT/LAVA training remains gated')
        : `${formatGateStatus(dtLavaTrainingGate)}; not required for academic MVP`
    },
    {
      label: 'SCMO receipts',
      value: gateRequiredForAcademicMvp(marketSubmissionReceiptGate) ? formatGateStatus(marketSubmissionReceiptGate) : 'not required',
      status: gatePassed(marketSubmissionReceiptGate) || !gateRequiredForAcademicMvp(marketSubmissionReceiptGate) ? 'ready' : 'blocked',
      reason: marketSubmissionReceiptGate.required_for_academic_mvp === false
        ? 'not required for academic MVP'
        : gateClaimScope(marketSubmissionReceiptGate, 'market-submission receipt readiness pending')
    },
    {
      label: 'Execution safety',
      value: formatGateStatus(marketExecutionSafetyGate),
      status: gatePassed(marketExecutionSafetyGate) && !readiness.market_execution_enabled ? 'ready' : 'blocked',
      reason: readiness.market_execution_enabled ? 'market_execution_enabled=true' : 'market_execution_enabled=false'
    }
  ]
}

const asRecord = (value: unknown): Record<string, unknown> => {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) {
    return {}
  }
  return value as Record<string, unknown>
}

const gatePassed = (gate: Record<string, unknown>): boolean => gate.passed === true

const gateRequiredForAcademicMvp = (gate: Record<string, unknown>): boolean => gate.required_for_academic_mvp !== false

const formatBoundaryValue = (value: string): string => value.replaceAll('_', ' ').replaceAll('v13', 'V13')

const formatGateStatus = (gate: Record<string, unknown>): string => {
  if (typeof gate.status === 'string') {
    return formatBoundaryValue(gate.status)
  }

  return gatePassed(gate) ? 'passed' : 'blocked'
}

const gateClaimScope = (gate: Record<string, unknown>, fallback: string): string => (
  typeof gate.claim_scope === 'string' ? gate.claim_scope : fallback
)

const formatUnknownStatus = (value: unknown): string => (
  typeof value === 'string' ? formatBoundaryValue(value) : 'missing'
)

const formatCount = (value: unknown): string => (
  typeof value === 'number' ? value.toLocaleString('en-GB') : '0'
)
