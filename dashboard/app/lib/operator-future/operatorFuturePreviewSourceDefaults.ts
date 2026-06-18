import type { ShadowPreviewSourceOptionResponse } from '~/types/control-plane'

export const DEFAULT_PREVIEW_SOURCE_OPTIONS: ShadowPreviewSourceOptionResponse[] = [
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
    preview_source_id: 'hfdt_live_shadow_preview',
    label: 'HFDT live shadow preview',
    status: 'forecast_candidate_library_shadow_not_promoted',
    reason: 'Ranks source-backed forecast candidate rows with V2+ forecast fallback; manual preview only.',
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
