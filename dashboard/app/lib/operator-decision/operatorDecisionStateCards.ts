import type {
  BaselineLpPreview,
  DashboardBatteryStateResponse,
  DashboardExogenousSignalsResponse,
  OperatorRecommendationResponse
} from '../../types/control-plane'
import { CURRENT_OFFLINE_STRATEGY_PROMOTION_HEADLINE } from '../../utils/defenseDataset'
import type { DefenseModelRow } from '../../utils/defenseDataset'
import {
  formatFraction,
  formatSelectedStrategyStatus
} from './operatorDecisionEvidenceFormatters'

export interface OperatorDecisionStateCard {
  label: string
  value: string
  meta: string
  tooltipTitle: string
  tooltipBody: string
  tooltipFormula: string
}

export const buildOperatorDecisionStateCards = (input: {
  operatorRecommendation?: OperatorRecommendationResponse | null
  batteryState: DashboardBatteryStateResponse | null
  baselinePreview: BaselineLpPreview | null
  exogenousSignals: DashboardExogenousSignalsResponse | null
  modelRows: DefenseModelRow[]
}): OperatorDecisionStateCard[] => {
  const latestTelemetrySoc = input.batteryState?.latest_telemetry?.current_soc ?? null
  const hourlySoc = input.batteryState?.hourly_snapshot?.soc_close ?? null
  const firstSocProjection = input.operatorRecommendation?.soc_projection?.[0] ?? null
  const physicalSoc = firstSocProjection?.physical_soc ?? latestTelemetrySoc ?? hourlySoc
  const planningSoc = firstSocProjection?.planning_soc ?? input.baselinePreview?.starting_soc_fraction ?? null
  const gridRisk = input.exogenousSignals?.national_grid_risk_score ?? null

  return [
    {
      label: 'Physical SOC',
      value: physicalSoc === null ? 'waiting' : formatFraction(physicalSoc),
      meta: input.operatorRecommendation?.soc_source
        || input.batteryState?.fallback_reason
        || input.batteryState?.hourly_snapshot?.telemetry_freshness
        || (latestTelemetrySoc === null ? 'latest snapshot' : 'latest telemetry'),
      tooltipTitle: 'Physical SOC',
      tooltipBody: 'Latest battery state from live telemetry when available. If telemetry is stale, the operator recommendation read model projects from hourly SOC plus tenant load/PV schedule.',
      tooltipFormula: 'physical_soc = live_telemetry ?? hourly_snapshot; projected_soc uses tenant net load'
    },
    {
      label: 'Planning SOC',
      value: planningSoc === null ? 'waiting' : formatFraction(planningSoc),
      meta: input.operatorRecommendation
        ? formatSelectedStrategyStatus(input.operatorRecommendation.selected_strategy_id)
        : input.baselinePreview?.starting_soc_source || 'baseline preview',
      tooltipTitle: 'Planning SOC',
      tooltipBody: 'SOC after the first feasible planning step from the current selected operator strategy read model.',
      tooltipFormula: 'planning_soc = feasible_schedule[0].projected_soc_after_fraction'
    },
    {
      label: 'V2+ comparator',
      value: `${Math.round(CURRENT_OFFLINE_STRATEGY_PROMOTION_HEADLINE.meanRegretUah)} UAH`,
      meta: `${CURRENT_OFFLINE_STRATEGY_PROMOTION_HEADLINE.rollingPassCount}/${CURRENT_OFFLINE_STRATEGY_PROMOTION_HEADLINE.rollingWindowCount} rolling; strict remains fallback`,
      tooltipTitle: 'Frozen V2+ comparator',
      tooltipBody: 'Current thesis headline: Ukrainian-only Schedule/Value Learner V2+. Dashboard strategy previews should be read against this frozen offline comparator.',
      tooltipFormula: 'headline = strict LP/oracle regret gate; market_execution_enabled=false'
    },
    {
      label: 'Grid context',
      value: gridRisk === null ? 'waiting' : formatFraction(gridRisk),
      meta: input.exogenousSignals?.tenant_region_affected ? 'tenant region affected' : 'tenant region clear or unknown',
      tooltipTitle: 'Grid context',
      tooltipBody: 'Rule-based grid event signal from public Ukrenergo text, mapped to tenant region where possible. Treat as operational context.',
      tooltipFormula: 'grid_risk = weighted(event_count, outage_flag, saving_request_flag, tenant_region_affected)'
    }
  ]
}
