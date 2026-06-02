import type {
  BaselineRecommendationPoint,
  DecisionPolicyPreviewResponse,
  FutureStackPreviewResponse,
  OperatorRecommendationResponse,
  OperatorValueGapPointResponse,
  ShadowRecommendationPreviewResponse
} from '~/types/control-plane'
import { formatRuntimeAccelerationLabel } from './operatorFutureStack'
import type { StrategyComparisonRow } from './operatorShadowPreview'

export {
  buildPreviewSourceSelectItems,
  formatPreviewSourceOptionLabel,
  resolveValueAlignedHfShadowDemoScenario,
  VALUE_ALIGNED_HF_SHADOW_DEMO_SCENARIOS,
  type ValueAlignedHfShadowDemoScenarioId,
  type PreviewSourceSelectItem
} from '../lib/operator-future/operatorFuturePreviewSources'

export type PolicyValueMode = 'selected' | 'official'

export interface FutureStackSummaryItem {
  label: string
  value: string
  meta: string
}

export interface FutureStatusCardArgs {
  selectedStrategyLabel: string
  operatorRecommendation: OperatorRecommendationResponse | null
  futureStack: FutureStackPreviewResponse | null
  decisionPolicy: DecisionPolicyPreviewResponse | null
}

export interface DecisionSourceFactArgs {
  selectedStrategyLabel: string
  policyForecastContextLabel: string
  operatorRecommendation: OperatorRecommendationResponse | null
  decisionPolicy: DecisionPolicyPreviewResponse | null
}

export interface SelectedRecommendationChartRow {
  label: string
  netPowerMw: number
  forecastPriceUahMwh: number
  valueGapUah: number
}

export function buildSelectedRecommendationChartRows(
  scheduleRows: BaselineRecommendationPoint[],
  gapRows: OperatorValueGapPointResponse[]
): SelectedRecommendationChartRow[] {
  return scheduleRows.map((row, index) => {
    const gapRow = gapRows[index]
    return {
      label: formatHour(row.interval_start),
      netPowerMw: Number(row.recommended_net_power_mw.toFixed(3)),
      forecastPriceUahMwh: Math.round(row.forecast_price_uah_mwh),
      valueGapUah: Math.round(gapRow?.value_gap_uah ?? 0)
    }
  })
}

export function buildFutureStatusCards({
  selectedStrategyLabel,
  operatorRecommendation,
  futureStack,
  decisionPolicy
}: FutureStatusCardArgs): FutureStackSummaryItem[] {
  return [
    {
      label: 'Headline result',
      value: 'V2+ offline',
      meta: '174.77 UAH mean regret / 4 of 4 rolling windows'
    },
    {
      label: 'Shown schedule',
      value: selectedStrategyLabel,
      meta: operatorRecommendation?.selection_reason || 'strict fallback/control'
    },
    {
      label: 'Policy mode',
      value: operatorRecommendation?.policy_mode || 'waiting',
      meta: operatorRecommendation?.selected_policy_id || 'no selected policy'
    },
    {
      label: 'Runtime',
      value: formatRuntimeAccelerationLabel(futureStack?.runtime_acceleration),
      meta: futureStack?.runtime_acceleration?.recommended_scope || 'training runtime not reported'
    },
    {
      label: 'Execution boundary',
      value: decisionPolicy?.market_execution_enabled ? 'market enabled' : 'preview only',
      meta: 'read-model evidence; no live dispatch'
    }
  ]
}

export function buildDecisionSourceFacts({
  selectedStrategyLabel,
  policyForecastContextLabel,
  operatorRecommendation,
  decisionPolicy
}: DecisionSourceFactArgs): string[] {
  const facts = [
    `Strategy: ${selectedStrategyLabel}`,
    `Forecast context: ${policyForecastContextLabel}`,
    'Boundary: review candidate only, no live IDM bid or market submission.'
  ]

  if (operatorRecommendation?.selection_reason) {
    facts.unshift(`Selection reason: ${operatorRecommendation.selection_reason}`)
  }

  if (operatorRecommendation?.policy_explanation) {
    facts.push(`Policy note: ${operatorRecommendation.policy_explanation}`)
  }

  if (decisionPolicy?.policy_value_interpretation) {
    facts.push(`Value note: ${decisionPolicy.policy_value_interpretation}`)
  }

  return facts
}

export function buildBackendStatusFacts(futureStack: FutureStackPreviewResponse | null): string[] {
  const items = Object.entries(futureStack?.backend_status ?? {}).length > 0
    ? Object.entries(futureStack?.backend_status ?? {}).map(([name, status]) => `${name}: ${status}`)
    : ['Official backend status not loaded yet.']

  items.push(`Runtime: ${formatRuntimeAccelerationLabel(futureStack?.runtime_acceleration)}.`)
  return items
}

export function buildShadowModelStoryItems(
  shadowComparisonPreviews: ShadowRecommendationPreviewResponse[]
): FutureStackSummaryItem[] {
  const shadowMetricsBySource = new Map(
    shadowComparisonPreviews.map(preview => [preview.preview_source_id, preview.comparison_metrics])
  )
  const applesMetrics = shadowMetricsBySource.get('dt_v2_plus_apples_to_apples_shadow') ?? {}
  const regretAwareMetrics = shadowMetricsBySource.get('regret_aware_v2_plus_selector_shadow') ?? {}
  const safeSwitchMetrics = shadowMetricsBySource.get('dt_v2_plus_safe_switch_selector_shadow') ?? {}
  const hfValueAlignedMetrics = shadowMetricsBySource.get('hf_live_safe_switch_value_aligned_shadow') ?? {}
  const v2Regret = numericMetric(regretAwareMetrics.v2_plus_mean_regret_uah)
    ?? numericMetric(safeSwitchMetrics.v2_plus_mean_regret_uah)
    ?? numericMetric(hfValueAlignedMetrics.v2_plus_baseline_mean_regret_uah)
    ?? numericMetric(applesMetrics.v2_plus_mean_regret_uah)
    ?? 174.77
  const dtRegret = numericMetric(applesMetrics.dt_selected_mean_regret_uah) ?? 460.30
  const selectorRegret = numericMetric(safeSwitchMetrics.selector_mean_regret_uah)
    ?? numericMetric(safeSwitchMetrics.dt_selected_mean_regret_uah)
    ?? numericMetric(regretAwareMetrics.selector_mean_regret_uah)
    ?? numericMetric(regretAwareMetrics.dt_selected_mean_regret_uah)
    ?? 174.77
  const switchCount = numericMetric(safeSwitchMetrics.non_v2_plus_switch_count)
    ?? numericMetric(regretAwareMetrics.non_v2_plus_switch_count)
    ?? 0
  const abstentionCount = numericMetric(safeSwitchMetrics.abstention_count)
    ?? numericMetric(regretAwareMetrics.abstention_count)
    ?? 90
  const recoveredSwitches = numericMetric(safeSwitchMetrics.recovered_safe_switch_opportunity_count) ?? 0
  const storyItems: FutureStackSummaryItem[] = [
    {
      label: 'V2+ evidence',
      value: `${formatRegretMean(v2Regret)} mean regret`,
      meta: 'headline comparator/fallback'
    },
    {
      label: 'Apples-to-apples DT',
      value: `${formatRegretMean(dtRegret)} mean regret`,
      meta: 'not promoted'
    },
    {
      label: 'DT safe-switch shadow',
      value: `${formatRegretMean(selectorRegret)} mean regret`,
      meta: `${Math.round(switchCount).toLocaleString('en-GB')} switches / ${Math.round(abstentionCount).toLocaleString('en-GB')} V2+ abstentions / ${Math.round(recoveredSwitches).toLocaleString('en-GB')} recovered wins`
    }
  ]

  if (shadowMetricsBySource.has('dt_v2_plus_safe_switch_selector_shadow')) {
    storyItems.push({
      label: 'Research gate',
      value: `${formatResearchEvidenceLevel(selectorRegret)} evidence`,
      meta: `${formatRegretMean(selectorRegret)} vs V2+ ${formatRegretMean(v2Regret)}; promotion=false / execution=false`
    })
  }

  if (shadowMetricsBySource.has('hf_live_safe_switch_value_aligned_shadow')) {
    const hfRegret = numericMetric(hfValueAlignedMetrics.hf_mean_regret_uah) ?? 158.7121
    const canonicalSafeSwitchRegret = numericMetric(hfValueAlignedMetrics.canonical_safe_switch_mean_regret_uah)
      ?? selectorRegret
    const gatePassed = numericMetric(hfValueAlignedMetrics.shadow_promotion_gate_passed) === 1
    storyItems.push({
      label: 'HF value-aligned shadow',
      value: `${formatRegretMean(hfRegret)} mean regret`,
      meta: `vs safe-switch ${formatRegretMean(canonicalSafeSwitchRegret)} / V2+ ${formatRegretMean(v2Regret)}; shadow gate ${gatePassed ? 'passed' : 'not passed'}; execution=false`
    })
  }

  return storyItems
}

export function formatInputSignalTooltipValue(
  seriesName: string | undefined,
  value: number | null | undefined
): string {
  if (value == null) {
    return 'n/a'
  }

  const normalizedSeriesName = seriesName?.toLowerCase() ?? ''
  if (normalizedSeriesName.includes('soc')) {
    return `${Math.round(value).toLocaleString('en-GB')}%`
  }
  if (normalizedSeriesName.includes('price') || normalizedSeriesName.includes('uah/mwh')) {
    return `${Math.round(value).toLocaleString('en-GB')} UAH/MWh`
  }
  if (normalizedSeriesName.includes('mw')) {
    return `${Number(value).toFixed(3)} MW`
  }
  return Math.round(value).toLocaleString('en-GB')
}

export function formatPolicyTooltipValue(
  seriesName: string | undefined,
  value: number | null | undefined,
  forcePriceUnit = false
): string {
  if (value == null) {
    return 'n/a'
  }
  const normalizedSeriesName = seriesName?.toLowerCase() ?? ''
  if (normalizedSeriesName.includes('mw') || normalizedSeriesName.includes('action')) {
    return `${Number(value).toFixed(3)} MW`
  }
  if (forcePriceUnit || normalizedSeriesName.includes('price') || normalizedSeriesName.includes('forecast')) {
    return `${Math.round(value).toLocaleString('en-GB')} UAH/MWh`
  }
  return `${Math.round(value).toLocaleString('en-GB')} UAH`
}

export function shortStrategyLabel(row: StrategyComparisonRow): string {
  if (row.sourceId === 'best_valid') {
    return 'Best valid'
  }
  if (row.sourceId === 'dt_shadow') {
    return 'DT Shadow'
  }
  if (row.sourceId === 'dt_direct_candidate_shadow') {
    return 'Direct DT'
  }
  if (row.sourceId === 'dt_v2_plus_apples_to_apples_shadow') {
    return 'DT/V2+'
  }
  if (row.sourceId === 'dt_v2_plus_distillation_shadow') {
    return 'DT distill'
  }
  if (row.sourceId === 'dt_decision_aware_shadow') {
    return 'Decision DT'
  }
  if (row.sourceId === 'regret_aware_v2_plus_selector_shadow') {
    return 'RA V2+'
  }
  if (row.sourceId === 'dt_v2_plus_safe_switch_selector_shadow') {
    return 'DT V2+ safe-switch'
  }
  if (row.sourceId === 'hf_live_safe_switch_shadow') {
    return 'HF live safe-switch'
  }
  if (row.sourceId === 'hf_live_safe_switch_value_aligned_shadow') {
    return 'HF value-aligned'
  }
  if (row.sourceId === 'poland_tft_shadow') {
    return 'Poland/TFT'
  }
  if (row.sourceId === 'dfl_diagnostics') {
    return 'DFL diag'
  }
  return 'V13 blocked'
}

export function formatStrategyAxisLabel(value: string): string {
  const wrappedLabels: Record<string, string> = {
    'Best valid': 'Best\nvalid',
    'DT Shadow': 'DT\nShadow',
    'Direct DT': 'Direct\nDT',
    'DT/V2+': 'DT/V2+',
    'DT distill': 'DT\ndistill',
    'Decision DT': 'Decision\nDT',
    'RA V2+': 'RA\nV2+',
    'DT V2+ safe-switch': 'DT V2+\nsafe-switch',
    'HF live safe-switch': 'HF live\nsafe-switch',
    'HF value-aligned': 'HF value\naligned',
    'Poland/TFT': 'Poland\nTFT',
    'DFL diag': 'DFL\ndiag',
    'V13 blocked': 'V13\nblocked'
  }

  return wrappedLabels[value] ?? value
}

export function formatEnergy(value: number): string {
  return `${value.toFixed(2)} MWh`
}

export function formatOptionalUah(value: number | null): string {
  if (value == null) {
    return 'n/a'
  }
  return `${Math.round(value).toLocaleString('en-GB')} UAH`
}

export function numericMetric(value: number | null | undefined): number | null {
  return typeof value === 'number' && Number.isFinite(value) ? value : null
}

export function formatRegretMean(value: number): string {
  return `${value.toLocaleString('en-GB', { minimumFractionDigits: 2, maximumFractionDigits: 2 })} UAH`
}

export function formatResearchEvidenceLevel(meanRegretUah: number): 'secondary' | 'fail' {
  return meanRegretUah <= 178.26 ? 'secondary' : 'fail'
}

export function formatForecastSeriesLabel(modelName: string): string {
  if (modelName === 'nbeatsx_silver_v0') {
    return 'Compact NBEATSx'
  }
  if (modelName === 'tft_silver_v0') {
    return 'Compact TFT p50'
  }
  if (modelName === 'nbeatsx_official_v0') {
    return 'Official NBEATSx'
  }
  if (modelName === 'tft_official_v0') {
    return 'Official TFT p50'
  }
  return modelName
}

export const roundOptionalPrice = (value: number | null): number | null => value === null ? null : Math.round(value)

export const formatHour = (timestamp: string): string => new Date(timestamp).toLocaleString('en-GB', {
  day: '2-digit',
  month: 'short',
  hour: '2-digit',
  minute: '2-digit'
})
