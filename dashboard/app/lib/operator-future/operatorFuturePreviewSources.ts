import type {
  ShadowPreviewSourceOptionResponse,
  ShadowRecommendationPreviewResponse
} from '~/types/control-plane'
import { previewSourceDisplayLabel } from '../../utils/operatorShadowPreview'

export interface PreviewSourceSelectItem {
  label: string
  value: string
}

export function formatPreviewSourceOptionLabel(
  previewSourceId: string,
  status: string,
  fallbackLabel: string
): string {
  if (previewSourceId === 'best_valid') {
    return 'Best valid schedule (V2+ comparator/fallback)'
  }
  if (previewSourceId === 'dt_shadow') {
    return 'DT Shadow preview (not promoted)'
  }
  if (previewSourceId === 'dt_direct_candidate_shadow') {
    return 'Direct DT shadow (not promoted)'
  }
  if (previewSourceId === 'dt_v2_plus_apples_to_apples_shadow') {
    return 'DT vs real V2+ shadow (not promoted)'
  }
  if (previewSourceId === 'regret_aware_v2_plus_selector_shadow') {
    return 'Regret-aware V2+ selector (abstains)'
  }
  if (previewSourceId === 'dt_v2_plus_safe_switch_selector_shadow') {
    return 'DT V2+ safe-switch selector (not promoted)'
  }
  if (previewSourceId === 'poland_tft_shadow') {
    return 'Poland/TFT shadow (positive, not promoted)'
  }
  if (previewSourceId === 'dfl_diagnostics') {
    return 'DFL diagnostics (not production)'
  }
  if (previewSourceId === 'v13_dt_lava_promoted_training') {
    return 'V13/DT/LAVA blocked (no schedule)'
  }
  return `${previewSourceDisplayLabel(previewSourceId, fallbackLabel)} / ${status}`
}

export function buildPreviewSourceSelectItems(
  shadowPreview: ShadowRecommendationPreviewResponse | null | undefined
): PreviewSourceSelectItem[] {
  const options = shadowPreview?.available_preview_sources.length
    ? shadowPreview.available_preview_sources
    : DEFAULT_PREVIEW_SOURCE_OPTIONS

  return options.map(option => ({
    label: formatPreviewSourceOptionLabel(option.preview_source_id, option.status, option.label),
    value: option.preview_source_id
  }))
}

const DEFAULT_PREVIEW_SOURCE_OPTIONS: ShadowPreviewSourceOptionResponse[] = [
  {
    preview_source_id: 'best_valid',
    label: 'Best valid recommendation',
    status: 'default_v2_plus_fallback',
    reason: 'V2+ remains confirmed offline schedule-value comparator.',
    is_default_strategy: true,
    is_promoted_strategy: true,
    market_execution_enabled: false
  },
  {
    preview_source_id: 'dt_shadow',
    label: 'DT Shadow',
    status: 'research_shadow_not_promoted',
    reason: 'Preview only.',
    is_default_strategy: false,
    is_promoted_strategy: false,
    market_execution_enabled: false
  },
  {
    preview_source_id: 'dt_direct_candidate_shadow',
    label: 'Direct DT Shadow',
    status: 'direct_candidate_shadow_not_promoted',
    reason: 'Direct candidate-index/schedule-family DT preview only.',
    is_default_strategy: false,
    is_promoted_strategy: false,
    market_execution_enabled: false
  },
  {
    preview_source_id: 'dt_v2_plus_apples_to_apples_shadow',
    label: 'DT vs real V2+ Shadow',
    status: 'apples_to_apples_not_promoted',
    reason: 'Comparator-aligned DT preview only.',
    is_default_strategy: false,
    is_promoted_strategy: false,
    market_execution_enabled: false
  },
  {
    preview_source_id: 'regret_aware_v2_plus_selector_shadow',
    label: 'Regret-aware V2+ selector',
    status: 'regret_aware_abstention_not_promoted',
    reason: 'Regret-aware selector with explicit V2+ abstention.',
    is_default_strategy: false,
    is_promoted_strategy: false,
    market_execution_enabled: false
  },
  {
    preview_source_id: 'dt_v2_plus_safe_switch_selector_shadow',
    label: 'DT V2+ safe-switch selector',
    status: 'safe_switch_evidence_not_promoted',
    reason: 'Corrected residual DT/V2+ shadow with safe-switch evidence and V2+ fallback.',
    is_default_strategy: false,
    is_promoted_strategy: false,
    market_execution_enabled: false
  },
  {
    preview_source_id: 'poland_tft_shadow',
    label: 'Poland-TFT Shadow',
    status: 'positive_not_promoted',
    reason: 'Shadow challenger.',
    is_default_strategy: false,
    is_promoted_strategy: false,
    market_execution_enabled: false
  },
  {
    preview_source_id: 'dfl_diagnostics',
    label: 'DFL diagnostics',
    status: 'diagnostic_only',
    reason: 'Diagnostic only.',
    is_default_strategy: false,
    is_promoted_strategy: false,
    market_execution_enabled: false
  },
  {
    preview_source_id: 'v13_dt_lava_promoted_training',
    label: 'V13/DT/LAVA blocked',
    status: 'blocked_source_readiness_roadmap',
    reason: 'Blocked roadmap.',
    is_default_strategy: false,
    is_promoted_strategy: false,
    market_execution_enabled: false
  }
]
