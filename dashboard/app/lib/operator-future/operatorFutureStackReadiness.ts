import type { AcademicMvpReadinessResponse, OperatorV13ReadinessResponse } from '~/types/control-plane'

export interface V13ReadinessItem {
  label: string
  value: string
  status: 'ready' | 'blocked'
  reason: string
}

export interface AcademicMvpDtShadowComparisonRow {
  label: string
  meanRegretUah: number
  meanValueUah: number
  regretBarWidthPercent: number
  status: 'research-shadow' | 'fallback' | 'reference' | 'control'
  note: string
}

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

  const missingRequiredInputs = Array.isArray(readiness.missing_required_inputs)
    ? readiness.missing_required_inputs
    : ['oree_dam_publication_receipts_csv_path']
  const receiptInputMissing = missingRequiredInputs.includes(
    'oree_dam_publication_receipts_csv_path'
  )
  const receiptProbeMonths = Array.isArray(readiness.receipt_source_audit_months_probed)
    ? readiness.receipt_source_audit_months_probed
    : []
  const latestReceiptProbeMonth = receiptProbeMonths[receiptProbeMonths.length - 1]
  const receiptSourceAuditProbeCount = safeNumber(readiness.receipt_source_audit_probe_count, 0)
  const receiptAuditSummary = receiptSourceAuditProbeCount > 0
    ? [
        `${receiptSourceAuditProbeCount.toLocaleString('en-GB')} months probed`
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
  const safeSwitchAcquisitionTargets = Array.isArray(readiness.safe_switch_acquisition_targets)
    ? readiness.safe_switch_acquisition_targets
    : []
  const topSafeSwitchTarget = safeSwitchAcquisitionTargets[0]
  const safeSwitchTargetSummary = topSafeSwitchTarget
    ? `; top target ${topSafeSwitchTarget.tenant_id} needs ${topSafeSwitchTarget.target_new_prior_material_safe_switch_examples.toLocaleString('en-GB')}`
    : ''
  const sourceFamilyCount = `${safeNumber(readiness.ready_rows, 0)}/${safeNumber(readiness.readiness_rows, 0)}`
  const missingSafeSwitchExamples = safeNumber(readiness.missing_safe_switch_examples, 0)
  const gateStatus = readiness.gate_status || 'source_readiness_pending'
  const topPriorityBlocker = readiness.top_priority_blocker || 'source_readiness_pending'
  const marketExecutionEnabled = Boolean(readiness.market_execution_enabled)
  const dtLavaReady = Boolean(readiness.dt_lava_ready)
  return [
    {
      label: 'V13 gate',
      value: formatBoundaryValue(gateStatus),
      status: readiness.v13_candidate_generation_ready ? 'ready' : 'blocked',
      reason: `${sourceFamilyCount} source families ready; top blocker ${topPriorityBlocker}`
    },
    {
      label: 'OREE source evidence',
      value: receiptGateLabel,
      status: receiptInputMissing ? 'blocked' : 'ready',
      reason: receiptInputMissing
        ? `${receiptGatePrefix}missing oree_dam_publication_receipts_csv_path; ${receiptAuditSummary}`
        : `${receiptGatePrefix}explicit OREE DAM/IDM source/publication evidence attached; ${receiptAuditSummary}`
    },
    {
      label: 'Safe-switch evidence',
      value: missingSafeSwitchExamples > 0
        ? `${missingSafeSwitchExamples.toLocaleString('en-GB')} missing`
        : 'ready',
      status: missingSafeSwitchExamples > 0 ? 'blocked' : 'ready',
      reason: `20 prior/train non-tail-risk material examples per tenant/source required${safeSwitchTargetSummary}`
    },
    {
      label: 'Execution boundary',
      value: marketExecutionEnabled ? 'market enabled' : 'preview only',
      status: marketExecutionEnabled ? 'blocked' : 'ready',
      reason: marketExecutionEnabled
        ? 'V13 unexpectedly reports market_execution_enabled=true'
        : `market_execution_enabled=false; DT/LAVA ${dtLavaReady ? 'ready' : 'blocked'}`
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

const formatBoundaryValue = (value: string): string => value.replaceAll('_', ' ').replaceAll('v13', 'V13')

const safeNumber = (value: unknown, fallback: number): number => (
  typeof value === 'number' && Number.isFinite(value) ? value : fallback
)

const asRecord = (value: unknown): Record<string, unknown> => {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) {
    return {}
  }
  return value as Record<string, unknown>
}

const numericMetric = (value: unknown): number => (
  typeof value === 'number' && Number.isFinite(value) ? value : Number.NaN
)
