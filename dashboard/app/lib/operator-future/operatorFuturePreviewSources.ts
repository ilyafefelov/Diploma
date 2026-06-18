import type {
  ShadowRecommendationPreviewResponse
} from '~/types/control-plane'
import type { OperatorMarketVenue } from '~/types/operator-dashboard'
import {
  previewSourceDisplayLabel,
  VALUE_ALIGNED_HF_SHADOW_PROOF_SWITCH_DELIVERY_DATE
} from '../../utils/operatorShadowPreview'
import { DEFAULT_PREVIEW_SOURCE_OPTIONS } from './operatorFuturePreviewSourceDefaults'

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
  if (previewSourceId === 'hfdt_live_shadow_preview') {
    return 'HFDT live shadow preview (forecast candidate)'
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
