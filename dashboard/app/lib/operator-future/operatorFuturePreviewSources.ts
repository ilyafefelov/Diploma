import type {
  ShadowPreviewSourceOptionResponse,
  ShadowRecommendationPreviewResponse
} from '~/types/control-plane'
import type { OperatorMarketVenue } from '~/types/operator-dashboard'
import {
  previewSourceDisplayLabel,
  VALUE_ALIGNED_HF_SHADOW_PROOF_SWITCH_DELIVERY_DATE
} from '../../utils/operatorShadowPreview'

export interface PreviewSourceSelectItem {
  label: string
  value: string
}

export type ValueAlignedHfShadowDemoScenarioId
  = | 'official_dam_proof'
    | 'forecast_dam_action'
    | 'forecast_dam_abstention'
    | 'forecast_idm_abstention'

export interface ValueAlignedHfShadowDemoScenario {
  id: ValueAlignedHfShadowDemoScenarioId
  label: string
  marketVenue: OperatorMarketVenue
  targetDeliveryDate: string
  boundaryCopy: string
}

const OFFICIAL_DAM_PROOF_SCENARIO: ValueAlignedHfShadowDemoScenario = {
  id: 'official_dam_proof',
  label: 'Official DAM proof',
  marketVenue: 'DAM',
  targetDeliveryDate: VALUE_ALIGNED_HF_SHADOW_PROOF_SWITCH_DELIVERY_DATE,
  boundaryCopy: 'Official DAM proof day; guarded non-fallback shadow preview; no market execution.'
}

export const VALUE_ALIGNED_HF_SHADOW_DEMO_SCENARIOS: readonly ValueAlignedHfShadowDemoScenario[] = [
  OFFICIAL_DAM_PROOF_SCENARIO,
  {
    id: 'forecast_dam_action',
    label: 'Forecast DAM action',
    marketVenue: 'DAM',
    targetDeliveryDate: '2026-06-02',
    boundaryCopy: 'Forecast guarded action; source-backed NBEATSx/TFT context; no market execution.'
  },
  {
    id: 'forecast_dam_abstention',
    label: 'Forecast DAM abstention',
    marketVenue: 'DAM',
    targetDeliveryDate: '2026-06-03',
    boundaryCopy: 'Forecast guarded abstention; HOLD is selected because non-fallback gates did not pass.'
  },
  {
    id: 'forecast_idm_abstention',
    label: 'IDM abstention',
    marketVenue: 'IDM',
    targetDeliveryDate: '2026-06-02',
    boundaryCopy: 'IDM guarded abstention; wired preview evidence only, not promoted.'
  }
]

export function resolveValueAlignedHfShadowDemoScenario(
  scenarioId: ValueAlignedHfShadowDemoScenarioId
): ValueAlignedHfShadowDemoScenario {
  return VALUE_ALIGNED_HF_SHADOW_DEMO_SCENARIOS.find(scenario => scenario.id === scenarioId)
    ?? OFFICIAL_DAM_PROOF_SCENARIO
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
  if (previewSourceId === 'hf_live_safe_switch_shadow') {
    return 'HF live safe-switch shadow (manual preview)'
  }
  if (previewSourceId === 'hf_live_safe_switch_value_aligned_shadow') {
    return 'HF live safe-switch value-aligned shadow (manual preview)'
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
    preview_source_id: 'hf_live_safe_switch_shadow',
    label: 'HF live safe-switch shadow',
    status: 'live_shadow_not_promoted',
    reason: 'Live OREE/forecast candidate-ranking preview; not promoted and no market execution.',
    is_default_strategy: false,
    is_promoted_strategy: false,
    market_execution_enabled: false
  },
  {
    preview_source_id: 'hf_live_safe_switch_value_aligned_shadow',
    label: 'HF live safe-switch value-aligned shadow',
    status: 'value_aligned_shadow_not_promoted',
    reason: 'Value-aligned HF live candidate-ranking preview; manual review only and no market execution.',
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
