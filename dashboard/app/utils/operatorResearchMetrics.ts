import type {
  DashboardBatteryStateResponse,
  DashboardExogenousSignalsResponse
} from '../types/control-plane'
import type { DefenseModelRow, ResearchReadinessRow } from './defenseDataset'
import { formatPercent, formatUah } from './defenseDataset'

export interface OperatorResearchMetric {
  label: string
  value: string
  meta: string
  tone: 'blue' | 'green' | 'orange' | 'mint' | 'lime'
  tooltipTitle: string
  tooltipBody: string
  tooltipFormula: string
}

export const buildOperatorResearchMetrics = (input: {
  modelRows: DefenseModelRow[]
  readinessRows: ResearchReadinessRow[]
  exogenousSignals: DashboardExogenousSignalsResponse | null
  batteryState: DashboardBatteryStateResponse | null
}): OperatorResearchMetric[] => {
  const controlRow = input.modelRows.find(row => row.modelName === 'strict_similar_day') ?? null
  const bestRow = [...input.modelRows].sort((left, right) => left.meanRegretUah - right.meanRegretUah)[0] ?? null
  const gridRisk = input.exogenousSignals?.national_grid_risk_score ?? null
  const soc = input.batteryState?.latest_telemetry?.current_soc
    ?? input.batteryState?.hourly_snapshot?.soc_close
    ?? null
  const firstBoundary = input.readinessRows[0] ?? null

  return [
    {
      label: 'Control regret',
      value: controlRow ? formatUah(controlRow.meanRegretUah) : 'unavailable',
      meta: controlRow ? `strict similar-day / ${formatPercent(controlRow.winRate)} win rate` : 'benchmark not loaded',
      tone: 'blue',
      tooltipTitle: 'Control regret',
      tooltipBody: 'Mean lost value versus the perfect-foresight oracle when using strict similar-day forecasts. This stays visible as the default comparator.',
      tooltipFormula: 'regret = oracle_value_uah - decision_value_uah'
    },
    {
      label: 'Best comparator',
      value: bestRow?.modelName ?? 'unavailable',
      meta: bestRow ? `${formatUah(bestRow.meanRegretUah)} mean regret` : 'benchmark not loaded',
      tone: 'green',
      tooltipTitle: 'Best comparator',
      tooltipBody: 'Lowest mean regret among live benchmark candidates for this tenant, including ensemble gates where materialized.',
      tooltipFormula: 'best = argmin(mean_regret_uah)'
    },
    {
      label: 'Grid risk',
      value: typeof gridRisk === 'number' ? formatPercent(gridRisk) : 'unavailable',
      meta: input.exogenousSignals?.tenant_region_affected ? 'tenant region affected' : 'tenant region clear or unknown',
      tone: input.exogenousSignals?.outage_flag ? 'orange' : 'mint',
      tooltipTitle: 'Grid event risk',
      tooltipBody: 'Rule-based Ukrenergo signal from recent public grid-event text. It is context, not proven causal price prediction.',
      tooltipFormula: 'risk = weighted(event_count_24h, outage_flag, saving_request_flag, tenant_region_affected)'
    },
    {
      label: 'Telemetry SOC',
      value: typeof soc === 'number' ? formatPercent(soc) : 'unavailable',
      meta: input.batteryState?.fallback_reason || 'live telemetry',
      tone: 'mint',
      tooltipTitle: 'Physical battery state',
      tooltipBody: 'Latest telemetry SOC when available, otherwise latest hourly snapshot. This is physical state, separate from projected planning SOC.',
      tooltipFormula: 'display_soc = latest_telemetry.current_soc ?? hourly_snapshot.soc_close'
    },
    {
      label: 'Research boundary',
      value: firstBoundary?.status ?? 'not loaded',
      meta: firstBoundary ? `${firstBoundary.label}: ${firstBoundary.boundary}` : 'research rows unavailable',
      tone: 'lime',
      tooltipTitle: 'Research claim boundary',
      tooltipBody: 'Shows whether DFL/DT/live replay outputs are materialized, and prevents research primitives from being read as production decisions.',
      tooltipFormula: 'boundary = academic_scope / provenance flags from backend read models'
    }
  ]
}
