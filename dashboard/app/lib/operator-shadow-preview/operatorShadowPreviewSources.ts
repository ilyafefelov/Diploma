import type { ShadowRecommendationPreviewResponse } from '../../types/control-plane'

export type OperatorPreviewSourceId
  = | 'best_valid'
    | 'dt_shadow'
    | 'dt_direct_candidate_shadow'
    | 'dt_v2_plus_apples_to_apples_shadow'
    | 'dt_v2_plus_distillation_shadow'
    | 'dt_decision_aware_shadow'
    | 'regret_aware_v2_plus_selector_shadow'
    | 'dt_v2_plus_safe_switch_selector_shadow'
    | 'hf_live_safe_switch_shadow'
    | 'hf_live_safe_switch_value_aligned_shadow'
    | 'poland_tft_shadow'
    | 'dfl_diagnostics'
    | 'v13_dt_lava_promoted_training'

export const SHADOW_PREVIEW_SOURCE_IDS: OperatorPreviewSourceId[] = [
  'dt_shadow',
  'dt_direct_candidate_shadow',
  'dt_v2_plus_apples_to_apples_shadow',
  'dt_v2_plus_distillation_shadow',
  'dt_decision_aware_shadow',
  'regret_aware_v2_plus_selector_shadow',
  'dt_v2_plus_safe_switch_selector_shadow',
  'hf_live_safe_switch_shadow',
  'hf_live_safe_switch_value_aligned_shadow',
  'poland_tft_shadow',
  'dfl_diagnostics',
  'v13_dt_lava_promoted_training'
]

export const SHADOW_COMPARISON_PREVIEW_SOURCE_IDS: OperatorPreviewSourceId[] = SHADOW_PREVIEW_SOURCE_IDS
  .filter(sourceId => sourceId !== 'hf_live_safe_switch_shadow'
    && sourceId !== 'hf_live_safe_switch_value_aligned_shadow')

export const HF_LIVE_SHADOW_COMPARISON_PREVIEW_SOURCE_IDS: OperatorPreviewSourceId[] = [
  'dt_v2_plus_distillation_shadow',
  'dt_v2_plus_safe_switch_selector_shadow',
  'hf_live_safe_switch_shadow'
]

export const HF_VALUE_ALIGNED_SHADOW_COMPARISON_PREVIEW_SOURCE_IDS: OperatorPreviewSourceId[] = [
  'dt_v2_plus_distillation_shadow',
  'dt_v2_plus_safe_switch_selector_shadow',
  'hf_live_safe_switch_value_aligned_shadow'
]

export const VALUE_ALIGNED_HF_SHADOW_PROOF_SWITCH_DELIVERY_DATE = '2026-05-02'

export interface ShadowHourlyRecommendationRow {
  timestamp: string
  action: string
  quantityLabel: string
  socPathLabel: string
  candidateLabel: string
  scheduleFamily: string
  expectedValueLabel: string
  regretVsV2Label: string
  regretVsStrictLabel: string
  gateStatus: string
}

export interface StrategyComparisonRow {
  sourceId: OperatorPreviewSourceId
  label: string
  status: string
  scheduleRows: number
  totalChargeMwh: number
  totalDischargeMwh: number
  meanRegretVsV2Uah: number | null
  meanRegretVsStrictUah: number | null
  totalValueUah: number | null
  marketExecutionEnabled: boolean
  isDefault: boolean
  isPromoted: boolean
  isBlocked: boolean
}

export const shouldLoadShadowPreview = (previewSourceId: OperatorPreviewSourceId): boolean => {
  return previewSourceId !== 'best_valid'
}

export const isLiveHfSafeSwitchPreviewSource = (
  previewSourceId: OperatorPreviewSourceId
): boolean => {
  return previewSourceId === 'hf_live_safe_switch_shadow'
    || previewSourceId === 'hf_live_safe_switch_value_aligned_shadow'
}

export const comparisonPreviewSourceIdsFor = (
  selectedPreviewSourceId: OperatorPreviewSourceId
): OperatorPreviewSourceId[] => {
  if (selectedPreviewSourceId === 'hf_live_safe_switch_value_aligned_shadow') {
    return HF_VALUE_ALIGNED_SHADOW_COMPARISON_PREVIEW_SOURCE_IDS
  }
  if (selectedPreviewSourceId === 'hf_live_safe_switch_shadow') {
    return HF_LIVE_SHADOW_COMPARISON_PREVIEW_SOURCE_IDS
  }
  return SHADOW_COMPARISON_PREVIEW_SOURCE_IDS
}

export const resolveShadowPreviewTargetDeliveryDate = (
  previewSourceId: OperatorPreviewSourceId,
  selectedTargetDeliveryDate: string | null | undefined
): string | null => {
  void previewSourceId
  const explicitTargetDate = selectedTargetDeliveryDate?.trim()
  if (explicitTargetDate) {
    return explicitTargetDate
  }
  return null
}

export const previewModeLabel = (
  previewSourceId: OperatorPreviewSourceId,
  shadowPreview: ShadowRecommendationPreviewResponse | null
): string => {
  if (previewSourceId === 'best_valid') {
    return 'Best valid schedule (V2+ comparator/fallback)'
  }
  return previewSourceDisplayLabel(previewSourceId, shadowPreview?.preview_source_label)
}

export const previewSourceDisplayLabel = (
  previewSourceId: string,
  fallbackLabel?: string | null
): string => {
  if (previewSourceId === 'v13_dt_lava_promoted_training') {
    return 'V13/DT/LAVA blocked'
  }
  if (previewSourceId === 'regret_aware_v2_plus_selector_shadow') {
    return fallbackLabel || 'Regret-aware V2+ selector'
  }
  if (previewSourceId === 'dt_v2_plus_safe_switch_selector_shadow') {
    return fallbackLabel || 'DT V2+ safe-switch selector'
  }
  if (previewSourceId === 'hf_live_safe_switch_shadow') {
    return fallbackLabel || 'HF live safe-switch shadow'
  }
  if (previewSourceId === 'hf_live_safe_switch_value_aligned_shadow') {
    return fallbackLabel || 'HF live safe-switch value-aligned shadow'
  }
  if (previewSourceId === 'dt_v2_plus_distillation_shadow') {
    return fallbackLabel || 'DT V2+ distillation shadow'
  }

  return fallbackLabel || previewSourceId
}
