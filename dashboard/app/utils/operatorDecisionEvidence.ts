import type {
  BaselineLpPreview,
  DashboardBatteryStateResponse,
  DashboardExogenousSignalsResponse,
  ForecastDispatchSensitivityResponse,
  OperatorRecommendationResponse,
  RealDataBenchmarkResponse
} from '../types/control-plane'
import type { DefenseModelRow } from './defenseDataset'

export interface OperatorStrategyEvidenceRow {
  modelName: string
  role: DefenseModelRow['role']
  meanRegretUah: number
  winRate: number
  regretDeltaVsControlUah: number
  controlComparisonLabel: string
}

export interface ControlRegretTimelinePoint {
  anchorLabel: string
  regretUah: number
  decisionValueUah: number
  oracleValueUah: number
  throughputMwh: number
}

export interface SensitivityEvidenceRow {
  bucket: string
  rows: number
  meanRegretUah: number
  meanForecastMaeUahMwh: number
  meanDispatchSpreadErrorUahMwh: number
}

export interface OperatorDecisionReadinessItem {
  label: string
  status: string
  tone: 'green' | 'orange' | 'red' | 'blue'
  detail: string
}

export interface OperatorDecisionStateCard {
  label: string
  value: string
  meta: string
  tooltipTitle: string
  tooltipBody: string
  tooltipFormula: string
}

export const buildOperatorStrategyEvidenceRows = (
  modelRows: DefenseModelRow[]
): OperatorStrategyEvidenceRow[] => {
  const controlRegret = modelRows.find(row => row.modelName === 'strict_similar_day')?.meanRegretUah ?? null

  return modelRows
    .map((row) => {
      const regretDelta = controlRegret === null ? 0 : row.meanRegretUah - controlRegret

      return {
        modelName: row.modelName,
        role: row.role,
        meanRegretUah: row.meanRegretUah,
        winRate: row.winRate,
        regretDeltaVsControlUah: regretDelta,
        controlComparisonLabel: row.modelName === 'strict_similar_day'
          ? 'control'
          : `${regretDelta >= 0 ? '+' : ''}${Math.round(regretDelta).toLocaleString('en-GB')} UAH vs control`
      }
    })
    .sort((left, right) => left.meanRegretUah - right.meanRegretUah)
}

export const buildControlRegretTimeline = (
  benchmark: RealDataBenchmarkResponse | null,
  limit = 24
): ControlRegretTimelinePoint[] => {
  const rows = benchmark?.rows
    .filter(row => row.forecast_model_name === 'strict_similar_day')
    .sort((left, right) => left.anchor_timestamp.localeCompare(right.anchor_timestamp))
    .slice(-limit) ?? []

  return rows.map(row => ({
    anchorLabel: formatAnchorLabel(row.anchor_timestamp),
    regretUah: row.regret_uah,
    decisionValueUah: row.decision_value_uah,
    oracleValueUah: row.oracle_value_uah,
    throughputMwh: row.total_throughput_mwh
  }))
}

export const buildSensitivityEvidenceRows = (
  sensitivity: ForecastDispatchSensitivityResponse | null
): SensitivityEvidenceRow[] => {
  return sensitivity?.bucket_summary.map(bucket => ({
    bucket: bucket.diagnostic_bucket,
    rows: bucket.rows,
    meanRegretUah: bucket.mean_regret_uah,
    meanForecastMaeUahMwh: bucket.mean_forecast_mae_uah_mwh,
    meanDispatchSpreadErrorUahMwh: bucket.mean_dispatch_spread_error_uah_mwh
  })) ?? []
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
  const firstSocProjection = input.operatorRecommendation?.soc_projection[0] ?? null
  const physicalSoc = firstSocProjection?.physical_soc ?? latestTelemetrySoc ?? hourlySoc
  const planningSoc = firstSocProjection?.planning_soc ?? input.baselinePreview?.starting_soc_fraction ?? null
  const gridRisk = input.exogenousSignals?.national_grid_risk_score ?? null
  const bestOperatorStrategy = input.operatorRecommendation?.available_strategies
    .filter(strategy => strategy.enabled && typeof strategy.mean_regret_uah === 'number')
    .sort((left, right) => (left.mean_regret_uah ?? Number.POSITIVE_INFINITY) - (right.mean_regret_uah ?? Number.POSITIVE_INFINITY))[0] ?? null
  const bestRow = [...input.modelRows].sort((left, right) => left.meanRegretUah - right.meanRegretUah)[0] ?? null

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
      meta: input.operatorRecommendation?.selected_strategy_id || input.baselinePreview?.starting_soc_source || 'baseline preview',
      tooltipTitle: 'Planning SOC',
      tooltipBody: 'SOC after the first feasible planning step from the current selected operator strategy read model.',
      tooltipFormula: 'planning_soc = feasible_schedule[0].projected_soc_after_fraction'
    },
    {
      label: 'Best comparator',
      value: bestOperatorStrategy?.strategy_id ?? bestRow?.modelName ?? 'waiting',
      meta: bestOperatorStrategy ? 'lowest mean regret in live read model' : bestRow ? 'lowest mean regret in defense model' : 'benchmark not loaded',
      tooltipTitle: 'Best comparator',
      tooltipBody: 'Lowest mean regret strategy in returned benchmark rows. It is a comparison signal, not automatic production model promotion.',
      tooltipFormula: 'best = argmin(mean_regret_uah) across benchmark model rows'
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

export const buildOperatorDecisionReadinessItems = (input: {
  operatorRecommendation?: OperatorRecommendationResponse | null
  batteryState: DashboardBatteryStateResponse | null
  baselinePreview: BaselineLpPreview | null
  exogenousSignals: DashboardExogenousSignalsResponse | null
}): OperatorDecisionReadinessItem[] => {
  const latestTelemetrySoc = input.batteryState?.latest_telemetry?.current_soc ?? null
  const hourlySoc = input.batteryState?.hourly_snapshot?.soc_close ?? null
  const firstSocProjection = input.operatorRecommendation?.soc_projection[0] ?? null
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
        ? `${formatFraction(input.operatorRecommendation.soc_projection[0]?.estimated_soc ?? planningSoc ?? 0)} via ${input.operatorRecommendation.soc_source}`
        : latestTelemetrySoc !== null
          ? `${formatFraction(latestTelemetrySoc)} from latest telemetry`
          : hourlySoc !== null
            ? `${formatFraction(hourlySoc)} from hourly snapshot`
            : input.batteryState?.fallback_reason || 'no telemetry or hourly snapshot'
    },
    {
      label: 'Selected strategy',
      status: input.operatorRecommendation?.selected_strategy_id ?? 'pending',
      tone: input.operatorRecommendation?.review_required ? 'orange' : input.operatorRecommendation ? 'green' : 'blue',
      detail: input.operatorRecommendation?.selection_reason || planningReadinessDetail(physicalSoc, planningSoc, input.baselinePreview?.starting_soc_source)
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

const formatAnchorLabel = (timestamp: string): string => new Date(timestamp).toLocaleDateString('en-GB', {
  day: '2-digit',
  month: 'short'
})

const formatFraction = (value: number): string => `${Math.round(value * 100)}%`

const planningReadinessDetail = (
  physicalSoc: number | null,
  planningSoc: number | null,
  source: string | undefined
): string => {
  if (planningSoc === null) {
    return 'no baseline LP preview loaded'
  }

  if (physicalSoc === null) {
    return `${formatFraction(planningSoc)} start; physical SOC missing`
  }

  return `${formatFraction(planningSoc)} start; ${formatFraction(Math.abs(planningSoc - physicalSoc))} gap vs physical${source ? '' : ''}`
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
