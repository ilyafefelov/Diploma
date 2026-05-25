import type {
  AcademicMvpReadinessResponse,
  BaselineRecommendationPoint,
  DecisionPolicyPreviewPointResponse,
  DecisionPolicyPreviewResponse,
  FutureStackPreviewResponse,
  FutureForecastSeriesResponse,
  OperatorLoadForecastPointResponse,
  OperatorRecommendationResponse,
  OperatorSocProjectionPointResponse,
  RuntimeAccelerationResponse,
  OperatorStrategyOptionResponse,
  OperatorV13ReadinessResponse
} from '~/types/control-plane'

const SOURCE_PRIORITY: Record<string, number> = {
  official: 0,
  calibrated: 1,
  compact: 2
}

const MODEL_PRIORITY: Record<string, number> = {
  nbeatsx: 0,
  tft: 1
}

const EXCLUDED_RECOMMENDATION_STRATEGIES = new Set([
  'decision_transformer',
  'nbeatsx_official_v0',
  'tft_official_v0'
])

const OFFLINE_V2_PLUS_STRATEGY_ID = 'schedule_value_learner_v2_plus'

export interface StrategyReadinessItem {
  strategyId: string
  label: string
  status: 'ready' | 'blocked'
  reason: string
}

export interface V13ReadinessItem {
  label: string
  value: string
  status: 'ready' | 'blocked'
  reason: string
}

export type AcademicMvpGatePassportItem = V13ReadinessItem

export interface AcademicMvpDtShadowComparisonRow {
  label: string
  meanRegretUah: number
  meanValueUah: number
  regretBarWidthPercent: number
  status: 'research-shadow' | 'fallback' | 'reference' | 'control'
  note: string
}

export interface PolicyForecastContextPoint {
  label: string
  nbeatsxForecastUahMwh: number
  tftForecastUahMwh: number
  forecastUncertaintyUahMwh: number
  forecastSpreadUahMwh: number
}

export interface OperatorForecastChartSource {
  kind: 'operator_delivery_day' | 'future_stack_context' | 'empty'
  series: FutureForecastSeriesResponse[]
  windowStart: string | null | undefined
  windowEnd: string | null | undefined
}

export interface RecommendationInputSignalPoint {
  label: string
  forecastPriceUahMwh: number
  selectedNetPowerMw: number
  projectedSocPercent: number | null
  siteNetLoadMw: number | null
}

type PolicyForecastContextRow = Pick<
  DecisionPolicyPreviewPointResponse,
  | 'interval_start'
  | 'state_market_price_uah_mwh'
  | 'state_nbeatsx_forecast_uah_mwh'
  | 'state_tft_forecast_uah_mwh'
  | 'state_forecast_uncertainty_uah_mwh'
  | 'state_forecast_spread_uah_mwh'
>

type OperatorForecastChartSourceInput = {
  futureStack: Pick<
    FutureStackPreviewResponse,
    'forecast_series' | 'forecast_window_start' | 'forecast_window_end'
  > | null | undefined
  operatorRecommendation: Pick<
    OperatorRecommendationResponse,
    'forecast_model_series' | 'target_delivery_window_start' | 'target_delivery_window_end'
  > | null | undefined
}

export const formatForecastWindowLabel = (
  forecastWindowStart: string | null | undefined,
  forecastWindowEnd: string | null | undefined
): string => {
  if (!forecastWindowStart || !forecastWindowEnd) {
    return 'forecast window pending'
  }

  return `${formatWindowTimestamp(forecastWindowStart)} -> ${formatWindowTimestamp(forecastWindowEnd)}`
}

export const selectOperatorForecastChartSource = (
  input: OperatorForecastChartSourceInput
): OperatorForecastChartSource => {
  const deliverySeries = input.operatorRecommendation?.forecast_model_series ?? []
  if (hasForecastRows(deliverySeries)) {
    return {
      kind: 'operator_delivery_day',
      series: deliverySeries,
      windowStart: input.operatorRecommendation?.target_delivery_window_start,
      windowEnd: input.operatorRecommendation?.target_delivery_window_end
    }
  }

  const futureStackSeries = input.futureStack?.forecast_series ?? []
  if (hasForecastRows(futureStackSeries)) {
    return {
      kind: 'future_stack_context',
      series: futureStackSeries,
      windowStart: input.futureStack?.forecast_window_start,
      windowEnd: input.futureStack?.forecast_window_end
    }
  }

  return {
    kind: 'empty',
    series: [],
    windowStart: null,
    windowEnd: null
  }
}

export const buildRecommendationInputSignalRows = (
  scheduleRows: BaselineRecommendationPoint[],
  socProjectionRows: OperatorSocProjectionPointResponse[] = [],
  loadForecastRows: OperatorLoadForecastPointResponse[] = []
): RecommendationInputSignalPoint[] => scheduleRows.map((row, index) => {
  const socProjection = socProjectionRows[index]
  const loadForecast = loadForecastRows[index]
  return {
    label: formatWindowTimestamp(row.interval_start),
    forecastPriceUahMwh: Math.round(row.forecast_price_uah_mwh),
    selectedNetPowerMw: Number(row.recommended_net_power_mw.toFixed(3)),
    projectedSocPercent: roundSocPercent(
      socProjection?.planning_soc ?? row.projected_soc_after_fraction
    ),
    siteNetLoadMw: typeof loadForecast?.net_load_mw === 'number'
      ? Number(loadForecast.net_load_mw.toFixed(3))
      : null
  }
})

export const sortFutureForecastSeries = (
  series: FutureForecastSeriesResponse[]
): FutureForecastSeriesResponse[] => [...series].sort((left, right) => {
  const sourceDelta = sourcePriority(left.source_status) - sourcePriority(right.source_status)
  if (sourceDelta !== 0) {
    return sourceDelta
  }

  const modelDelta = modelPriority(left.model_name) - modelPriority(right.model_name)
  if (modelDelta !== 0) {
    return modelDelta
  }

  return left.model_name.localeCompare(right.model_name)
})

export const filterOfficialPolicyValueSeries = (
  series: FutureForecastSeriesResponse[]
): FutureForecastSeriesResponse[] => sortFutureForecastSeries(series)
  .filter(candidate => candidate.source_status.toLowerCase().includes('official') && candidate.points.length > 0)

export const buildStrategySelectItems = (
  strategies: OperatorStrategyOptionResponse[]
): Array<{ label: string, value: string, disabled: boolean }> => strategies.map(strategy => ({
  label: strategy.enabled ? strategy.label : `${strategy.label} - ${strategy.reason}`,
  value: strategy.strategy_id,
  disabled: !strategy.enabled
}))

export const buildRecommendationStrategySelectItems = (
  strategies: OperatorStrategyOptionResponse[]
): Array<{ label: string, value: string, disabled: boolean }> => strategies
  .filter((strategy) => {
    if (!strategy.enabled) {
      return false
    }

    if (EXCLUDED_RECOMMENDATION_STRATEGIES.has(strategy.strategy_id)) {
      return false
    }

    return strategy.strategy_id === 'strict_similar_day' || typeof strategy.mean_regret_uah === 'number'
  })
  .sort((left, right) => {
    if (left.strategy_id === OFFLINE_V2_PLUS_STRATEGY_ID) {
      return -1
    }

    if (right.strategy_id === OFFLINE_V2_PLUS_STRATEGY_ID) {
      return 1
    }

    if (left.strategy_id === 'strict_similar_day') {
      return -1
    }

    if (right.strategy_id === 'strict_similar_day') {
      return 1
    }

    return (left.mean_regret_uah ?? Number.POSITIVE_INFINITY) - (right.mean_regret_uah ?? Number.POSITIVE_INFINITY)
  })
  .map(strategy => ({
    label: formatRecommendationStrategyLabel(strategy),
    value: strategy.strategy_id,
    disabled: false
  }))

export const buildStrategyReadinessItems = (
  strategies: OperatorStrategyOptionResponse[]
): StrategyReadinessItem[] => strategies
  .filter(strategy => strategy.enabled)
  .filter(strategy => !EXCLUDED_RECOMMENDATION_STRATEGIES.has(strategy.strategy_id))
  .filter(strategy => strategy.strategy_id === 'strict_similar_day' || typeof strategy.mean_regret_uah === 'number')
  .map(strategy => ({
    strategyId: strategy.strategy_id,
    label: strategy.label,
    status: 'ready',
    reason: typeof strategy.mean_regret_uah === 'number'
      ? `${Math.round(strategy.mean_regret_uah).toLocaleString('en-GB')} UAH mean regret`
      : strategy.reason
  }))

export const buildV13ReadinessItems = (
  readiness: OperatorV13ReadinessResponse | null | undefined
): V13ReadinessItem[] => {
  if (!readiness) {
    return [
      {
        label: 'V13 gate',
        value: 'packet pending',
        status: 'blocked',
        reason: 'operator recommendation has not loaded V13 source-readiness'
      }
    ]
  }

  const receiptInputMissing = readiness.missing_required_inputs.includes(
    'oree_dam_publication_receipts_csv_path'
  )
  const receiptProbeMonths = readiness.receipt_source_audit_months_probed
  const latestReceiptProbeMonth = receiptProbeMonths[receiptProbeMonths.length - 1]
  const receiptAuditSummary = readiness.receipt_source_audit_probe_count > 0
    ? [
        `${readiness.receipt_source_audit_probe_count.toLocaleString('en-GB')} months probed`
        + `${latestReceiptProbeMonth ? ` through ${latestReceiptProbeMonth}` : ''}`,
        readiness.receipt_source_audit_csv_generated
          ? 'receipt CSV generated'
          : 'no receipt CSV generated'
      ].join('; ')
    : 'no receipt source audit attached'
  const receiptGateLabel = readiness.source_governance_label || (
    receiptInputMissing ? 'blocked' : 'ready'
  )
  const receiptGatePrefix = readiness.source_governance_label
    ? `${readiness.source_governance_label}; `
    : ''
  const topSafeSwitchTarget = readiness.safe_switch_acquisition_targets[0]
  const safeSwitchTargetSummary = topSafeSwitchTarget
    ? `; top target ${topSafeSwitchTarget.tenant_id} needs ${topSafeSwitchTarget.target_new_prior_material_safe_switch_examples.toLocaleString('en-GB')}`
    : ''
  const sourceFamilyCount = `${readiness.ready_rows}/${readiness.readiness_rows}`
  return [
    {
      label: 'V13 gate',
      value: formatBoundaryValue(readiness.gate_status),
      status: readiness.v13_candidate_generation_ready ? 'ready' : 'blocked',
      reason: `${sourceFamilyCount} source families ready; top blocker ${readiness.top_priority_blocker}`
    },
    {
      label: 'DAM receipts',
      value: receiptGateLabel,
      status: receiptInputMissing ? 'blocked' : 'ready',
      reason: receiptInputMissing
        ? `${receiptGatePrefix}missing oree_dam_publication_receipts_csv_path; ${receiptAuditSummary}`
        : `${receiptGatePrefix}explicit source publication receipts attached; ${receiptAuditSummary}`
    },
    {
      label: 'Safe-switch evidence',
      value: readiness.missing_safe_switch_examples > 0
        ? `${readiness.missing_safe_switch_examples.toLocaleString('en-GB')} missing`
        : 'ready',
      status: readiness.missing_safe_switch_examples > 0 ? 'blocked' : 'ready',
      reason: `20 prior/train non-tail-risk material examples per tenant/source required${safeSwitchTargetSummary}`
    },
    {
      label: 'Execution boundary',
      value: readiness.market_execution_enabled ? 'market enabled' : 'preview only',
      status: readiness.market_execution_enabled ? 'blocked' : 'ready',
      reason: readiness.market_execution_enabled
        ? 'V13 unexpectedly reports market_execution_enabled=true'
        : `market_execution_enabled=false; DT/LAVA ${readiness.dt_lava_ready ? 'ready' : 'blocked'}`
    }
  ]
}

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
      label: 'DAM bid preview',
      value: formatGateStatus(damBidPreviewGate),
      status: gatePassed(damBidPreviewGate) ? 'ready' : 'blocked',
      reason: typeof bidPreviewRows === 'number'
        ? `${bidPreviewRows.toLocaleString('en-GB')} non-submittable DAM bid-preview rows`
        : gateClaimScope(damBidPreviewGate, 'non-submittable DAM bid preview rows pending')
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

export const buildAcademicMvpDtShadowComparisonRows = (
  readiness: AcademicMvpReadinessResponse | null | undefined
): AcademicMvpDtShadowComparisonRow[] => {
  const gate = asRecord(readiness?.dt_research_shadow_gate)
  const metrics = asRecord(gate.evaluation_metrics)
  const candidateRows: AcademicMvpDtShadowComparisonRow[] = [
    {
      label: 'DT shadow',
      meanRegretUah: numericMetric(metrics.dt_selected_mean_regret_uah),
      meanValueUah: numericMetric(metrics.dt_selected_mean_value_uah),
      regretBarWidthPercent: 0,
      status: 'research-shadow',
      note: 'HF/local transformer candidate-index policy'
    },
    {
      label: 'V2+ fallback',
      meanRegretUah: numericMetric(metrics.v2_plus_mean_regret_uah),
      meanValueUah: numericMetric(metrics.v2_plus_mean_value_uah),
      regretBarWidthPercent: 0,
      status: 'fallback',
      note: 'teacher / comparator / fallback'
    },
    {
      label: 'Strict reference',
      meanRegretUah: numericMetric(metrics.strict_mean_regret_uah),
      meanValueUah: numericMetric(metrics.strict_mean_value_uah),
      regretBarWidthPercent: 0,
      status: 'reference',
      note: 'strict LP/oracle reference'
    },
    {
      label: 'Behavior cloning',
      meanRegretUah: numericMetric(metrics.behavior_cloning_mean_regret_uah),
      meanValueUah: numericMetric(metrics.behavior_cloning_mean_value_uah),
      regretBarWidthPercent: 0,
      status: 'control',
      note: 'imitation control, accuracy secondary'
    }
  ]
  const rows = candidateRows.filter(row => Number.isFinite(row.meanRegretUah) && Number.isFinite(row.meanValueUah))
  const maxRegret = Math.max(...rows.map(row => row.meanRegretUah), 0)

  if (maxRegret <= 0) {
    return []
  }

  return rows.map(row => ({
    ...row,
    regretBarWidthPercent: Math.max(6, Math.round((row.meanRegretUah / maxRegret) * 100))
  }))
}

export const isChartSafeForecastSeries = (series: FutureForecastSeriesResponse): boolean => {
  const normalizedBoundary = series.quality_boundary.toLowerCase()

  return series.points.length > 0
    && series.out_of_dam_cap_rows === 0
    && !normalizedBoundary.includes('needs_calibration')
}

export const buildPolicyForecastContextPoints = (
  policyRows: PolicyForecastContextRow[]
): PolicyForecastContextPoint[] => policyRows.map((row) => {
  const nbeatsxForecast = row.state_nbeatsx_forecast_uah_mwh ?? row.state_market_price_uah_mwh
  const tftForecast = row.state_tft_forecast_uah_mwh ?? nbeatsxForecast
  const forecastSpread = row.state_forecast_spread_uah_mwh ?? tftForecast - nbeatsxForecast
  return {
    label: formatWindowTimestamp(row.interval_start),
    nbeatsxForecastUahMwh: nbeatsxForecast,
    tftForecastUahMwh: tftForecast,
    forecastUncertaintyUahMwh: row.state_forecast_uncertainty_uah_mwh ?? Math.abs(forecastSpread),
    forecastSpreadUahMwh: forecastSpread
  }
})

export const formatPolicyForecastContextLabel = (
  decisionPolicy: Pick<
    DecisionPolicyPreviewResponse,
    'forecast_context_coverage_ratio' | 'forecast_context_row_count' | 'row_count'
  > | null | undefined
): string => {
  if (!decisionPolicy) {
    return 'forecast context pending'
  }

  const percentage = Math.round(decisionPolicy.forecast_context_coverage_ratio * 100)
  return `${percentage}% forecast-conditioned (${decisionPolicy.forecast_context_row_count}/${decisionPolicy.row_count} rows)`
}

export const formatOperatorPolicyForecastContextLabel = (
  operatorRecommendation: Pick<
    OperatorRecommendationResponse,
    'policy_forecast_context_coverage_ratio' | 'policy_forecast_context_row_count'
  > | null | undefined
): string => {
  if (!operatorRecommendation) {
    return 'forecast context pending'
  }
  if (operatorRecommendation.policy_forecast_context_row_count === 0) {
    return 'forecast context not applicable'
  }

  const percentage = Math.round(operatorRecommendation.policy_forecast_context_coverage_ratio * 100)
  return `${percentage}% forecast-conditioned (${operatorRecommendation.policy_forecast_context_row_count} rows)`
}

export const formatRuntimeAccelerationLabel = (
  runtime: RuntimeAccelerationResponse | null | undefined
): string => {
  if (!runtime) {
    return 'runtime pending'
  }
  if (runtime.device_type === 'cuda') {
    return `CUDA / ${runtime.device_name}`
  }
  if (runtime.device_type === 'mps') {
    return `MPS / ${runtime.device_name}`
  }
  return `${runtime.device_name} / ${runtime.backend}`
}

export const formatForecastQualityLabel = (series: FutureForecastSeriesResponse): string => {
  if (series.out_of_dam_cap_rows > 0) {
    return `${series.out_of_dam_cap_rows} out-of-cap row${series.out_of_dam_cap_rows === 1 ? '' : 's'}`
  }

  if (series.quality_boundary === 'smoke_values_inside_dam_cap_not_value_claim') {
    return 'inside DAM cap / smoke only'
  }

  return 'inside DAM cap'
}

const sourcePriority = (sourceStatus: string): number => {
  const normalized = sourceStatus.toLowerCase()
  for (const [needle, priority] of Object.entries(SOURCE_PRIORITY)) {
    if (normalized.includes(needle)) {
      return priority
    }
  }
  return 99
}

const modelPriority = (modelName: string): number => {
  const normalized = modelName.toLowerCase()
  for (const [needle, priority] of Object.entries(MODEL_PRIORITY)) {
    if (normalized.includes(needle)) {
      return priority
    }
  }
  return 99
}

const hasForecastRows = (series: FutureForecastSeriesResponse[]): boolean => (
  series.some(candidate => candidate.points.length > 0)
)

const formatRecommendationStrategyLabel = (strategy: OperatorStrategyOptionResponse): string => {
  if (typeof strategy.mean_regret_uah !== 'number') {
    return strategy.label
  }

  return `${strategy.label} · ${Math.round(strategy.mean_regret_uah).toLocaleString('en-GB')} UAH`
}

const formatBoundaryValue = (value: string): string => value.replaceAll('_', ' ').replaceAll('v13', 'V13')

const asRecord = (value: unknown): Record<string, unknown> => {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) {
    return {}
  }
  return value as Record<string, unknown>
}

const gatePassed = (gate: Record<string, unknown>): boolean => gate.passed === true

const gateRequiredForAcademicMvp = (gate: Record<string, unknown>): boolean => gate.required_for_academic_mvp !== false

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

const numericMetric = (value: unknown): number => (
  typeof value === 'number' && Number.isFinite(value) ? value : Number.NaN
)

const roundSocPercent = (value: number | null | undefined): number | null => {
  if (typeof value !== 'number' || !Number.isFinite(value)) {
    return null
  }

  return Math.round(value * 100)
}

const formatWindowTimestamp = (timestamp: string): string => new Date(timestamp).toLocaleString('en-GB', {
  day: '2-digit',
  month: 'short',
  hour: '2-digit',
  minute: '2-digit',
  timeZone: 'Europe/Kyiv'
})
