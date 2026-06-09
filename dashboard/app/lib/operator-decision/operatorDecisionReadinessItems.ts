import type {
  BaselineLpPreview,
  DashboardBatteryStateResponse,
  DashboardExogenousSignalsResponse,
  OperatorRecommendationResponse
} from '../../types/control-plane'
import {
  formatFraction,
  formatSelectedStrategyStatus
} from './operatorDecisionEvidenceFormatters'

export interface OperatorDecisionReadinessItem {
  label: string
  status: string
  tone: 'green' | 'orange' | 'red' | 'blue'
  detail: string
}

export const buildOperatorDecisionReadinessItems = (input: {
  operatorRecommendation?: OperatorRecommendationResponse | null
  batteryState: DashboardBatteryStateResponse | null
  baselinePreview: BaselineLpPreview | null
  exogenousSignals: DashboardExogenousSignalsResponse | null
}): OperatorDecisionReadinessItem[] => {
  const latestTelemetrySoc = input.batteryState?.latest_telemetry?.current_soc ?? null
  const hourlySoc = input.batteryState?.hourly_snapshot?.soc_close ?? null
  const firstSocProjection = input.operatorRecommendation?.soc_projection?.[0] ?? null
  const physicalSoc = firstSocProjection?.physical_soc ?? latestTelemetrySoc ?? hourlySoc
  const planningSoc = firstSocProjection?.planning_soc ?? input.baselinePreview?.starting_soc_fraction ?? null
  const gridFlags = collectGridFlags(input.exogenousSignals)
  const sourceFreshness = summarizeSourceFreshness(input.exogenousSignals)
  const operatorWarnings = input.operatorRecommendation?.readiness_warnings ?? []

  return [
    {
      label: 'Physical SOC',
      status: input.operatorRecommendation?.soc_source === 'telemetry_projected'
        ? 'projected'
        : latestTelemetrySoc !== null ? 'live' : hourlySoc !== null ? 'snapshot' : 'missing',
      tone: input.operatorRecommendation?.soc_source === 'telemetry_projected'
        ? 'orange'
        : latestTelemetrySoc !== null ? 'green' : hourlySoc !== null ? 'orange' : 'red',
      detail: input.operatorRecommendation
        ? `${formatFraction(input.operatorRecommendation.soc_projection?.[0]?.estimated_soc ?? planningSoc ?? 0)} via ${input.operatorRecommendation.soc_source}`
        : latestTelemetrySoc !== null
          ? `${formatFraction(latestTelemetrySoc)} from latest telemetry`
          : hourlySoc !== null
            ? `${formatFraction(hourlySoc)} from hourly snapshot`
            : input.batteryState?.fallback_reason || 'no telemetry or hourly snapshot'
    },
    {
      label: 'Selected strategy',
      status: input.operatorRecommendation
        ? formatSelectedStrategyStatus(input.operatorRecommendation.selected_strategy_id)
        : 'pending',
      tone: input.operatorRecommendation?.review_required ? 'orange' : input.operatorRecommendation ? 'green' : 'blue',
      detail: input.operatorRecommendation?.selection_reason || planningReadinessDetail(physicalSoc, planningSoc)
    },
    {
      label: 'Grid context',
      status: gridFlags.length > 0 ? 'review' : input.exogenousSignals ? 'clear' : 'missing',
      tone: gridFlags.length > 0 ? 'orange' : input.exogenousSignals ? 'green' : 'red',
      detail: gridFlags.length > 0 ? gridFlags.join('; ') : input.exogenousSignals ? 'no active tenant-region flag' : 'no grid signal loaded'
    },
    {
      label: 'Readiness',
      status: operatorWarnings.length > 0 ? 'review' : sourceFreshness.status,
      tone: operatorWarnings.length > 0 ? 'orange' : sourceFreshness.tone,
      detail: operatorWarnings[0] || sourceFreshness.detail
    }
  ]
}

const planningReadinessDetail = (
  physicalSoc: number | null,
  planningSoc: number | null
): string => {
  if (planningSoc === null) {
    return 'no baseline LP preview loaded'
  }

  if (physicalSoc === null) {
    return `${formatFraction(planningSoc)} start; physical SOC missing`
  }

  return `${formatFraction(planningSoc)} start; ${formatFraction(Math.abs(planningSoc - physicalSoc))} gap vs physical`
}

const collectGridFlags = (
  exogenousSignals: DashboardExogenousSignalsResponse | null
): string[] => {
  if (!exogenousSignals) {
    return []
  }

  return [
    exogenousSignals.tenant_region_affected ? 'tenant region affected' : null,
    exogenousSignals.outage_flag ? 'outage flag active' : null,
    exogenousSignals.saving_request_flag ? 'saving request active' : null,
    exogenousSignals.solar_shift_hint ? 'solar shift hint active' : null
  ].filter((item): item is string => item !== null)
}

const summarizeSourceFreshness = (
  exogenousSignals: DashboardExogenousSignalsResponse | null
): Pick<OperatorDecisionReadinessItem, 'status' | 'tone' | 'detail'> => {
  if (!exogenousSignals) {
    return {
      status: 'missing',
      tone: 'red',
      detail: 'weather missing / grid missing'
    }
  }

  const weatherHours = exogenousSignals.latest_weather?.freshness_hours ?? null
  const gridHours = exogenousSignals.event_source_freshness_hours ?? null
  const knownHours = [weatherHours, gridHours].filter((value): value is number => typeof value === 'number')
  const maxKnownHours = knownHours.length > 0 ? Math.max(...knownHours) : null

  return {
    status: maxKnownHours === null ? 'missing' : maxKnownHours <= 6 ? 'fresh' : maxKnownHours <= 24 ? 'aging' : 'stale',
    tone: maxKnownHours === null ? 'red' : maxKnownHours <= 6 ? 'green' : 'orange',
    detail: `weather ${formatFreshnessHours(weatherHours)} / grid ${formatFreshnessHours(gridHours)}`
  }
}

const formatFreshnessHours = (hours: number | null): string => {
  if (hours === null) {
    return 'missing'
  }

  return `${hours.toFixed(1)}h`
}
