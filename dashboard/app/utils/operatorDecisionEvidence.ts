import type {
  BaselineLpPreview,
  DashboardBatteryStateResponse,
  DashboardExogenousSignalsResponse,
  ForecastDispatchSensitivityResponse,
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
  batteryState: DashboardBatteryStateResponse | null
  baselinePreview: BaselineLpPreview | null
  exogenousSignals: DashboardExogenousSignalsResponse | null
  modelRows: DefenseModelRow[]
}): OperatorDecisionStateCard[] => {
  const latestTelemetrySoc = input.batteryState?.latest_telemetry?.current_soc ?? null
  const hourlySoc = input.batteryState?.hourly_snapshot?.soc_close ?? null
  const physicalSoc = latestTelemetrySoc ?? hourlySoc
  const planningSoc = input.baselinePreview?.starting_soc_fraction ?? null
  const gridRisk = input.exogenousSignals?.national_grid_risk_score ?? null
  const bestRow = [...input.modelRows].sort((left, right) => left.meanRegretUah - right.meanRegretUah)[0] ?? null

  return [
    {
      label: 'Physical SOC',
      value: physicalSoc === null ? 'waiting' : formatFraction(physicalSoc),
      meta: input.batteryState?.fallback_reason
        || input.batteryState?.hourly_snapshot?.telemetry_freshness
        || (latestTelemetrySoc === null ? 'latest snapshot' : 'latest telemetry'),
      tooltipTitle: 'Physical SOC',
      tooltipBody: 'Latest battery state from telemetry when available, otherwise latest hourly Silver snapshot. This is physical truth, not a planned future state.',
      tooltipFormula: 'physical_soc = latest_telemetry.current_soc ?? hourly_snapshot.soc_close'
    },
    {
      label: 'Planning SOC',
      value: planningSoc === null ? 'waiting' : formatFraction(planningSoc),
      meta: input.baselinePreview?.starting_soc_source || 'baseline preview',
      tooltipTitle: 'Planning SOC',
      tooltipBody: 'Starting SOC used by the baseline LP preview. If live telemetry is fresh, backend can start from telemetry; otherwise it falls back to tenant defaults.',
      tooltipFormula: 'starting_soc_source = request_override | telemetry_hourly | tenant_default'
    },
    {
      label: 'Best comparator',
      value: bestRow?.modelName ?? 'waiting',
      meta: bestRow ? 'lowest mean regret in read model' : 'benchmark not loaded',
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
  batteryState: DashboardBatteryStateResponse | null
  baselinePreview: BaselineLpPreview | null
  exogenousSignals: DashboardExogenousSignalsResponse | null
}): OperatorDecisionReadinessItem[] => {
  const latestTelemetrySoc = input.batteryState?.latest_telemetry?.current_soc ?? null
  const hourlySoc = input.batteryState?.hourly_snapshot?.soc_close ?? null
  const physicalSoc = latestTelemetrySoc ?? hourlySoc
  const planningSoc = input.baselinePreview?.starting_soc_fraction ?? null
  const gridFlags = collectGridFlags(input.exogenousSignals)
  const sourceFreshness = summarizeSourceFreshness(input.exogenousSignals)

  return [
    {
      label: 'Physical SOC',
      status: latestTelemetrySoc !== null ? 'live' : hourlySoc !== null ? 'snapshot' : 'missing',
      tone: latestTelemetrySoc !== null ? 'green' : hourlySoc !== null ? 'orange' : 'red',
      detail: latestTelemetrySoc !== null
        ? `${formatFraction(latestTelemetrySoc)} from latest telemetry`
        : hourlySoc !== null
          ? `${formatFraction(hourlySoc)} from hourly snapshot`
          : input.batteryState?.fallback_reason || 'no telemetry or hourly snapshot'
    },
    {
      label: 'Planning SOC',
      status: planningReadinessStatus(physicalSoc, planningSoc),
      tone: planningReadinessTone(physicalSoc, planningSoc),
      detail: planningReadinessDetail(physicalSoc, planningSoc, input.baselinePreview?.starting_soc_source)
    },
    {
      label: 'Grid context',
      status: gridFlags.length > 0 ? 'review' : input.exogenousSignals ? 'clear' : 'missing',
      tone: gridFlags.length > 0 ? 'orange' : input.exogenousSignals ? 'green' : 'red',
      detail: gridFlags.length > 0 ? gridFlags.join('; ') : input.exogenousSignals ? 'no active tenant-region flag' : 'no grid signal loaded'
    },
    {
      label: 'Source freshness',
      status: sourceFreshness.status,
      tone: sourceFreshness.tone,
      detail: sourceFreshness.detail
    }
  ]
}

const formatAnchorLabel = (timestamp: string): string => new Date(timestamp).toLocaleDateString('en-GB', {
  day: '2-digit',
  month: 'short'
})

const formatFraction = (value: number): string => `${Math.round(value * 100)}%`

const planningReadinessStatus = (physicalSoc: number | null, planningSoc: number | null): string => {
  if (physicalSoc === null || planningSoc === null) {
    return 'missing'
  }

  const gap = Math.abs(physicalSoc - planningSoc)
  if (gap <= 0.05) {
    return 'aligned'
  }

  return gap <= 0.15 ? 'review' : 'reset'
}

const planningReadinessTone = (
  physicalSoc: number | null,
  planningSoc: number | null
): OperatorDecisionReadinessItem['tone'] => {
  const status = planningReadinessStatus(physicalSoc, planningSoc)
  if (status === 'aligned') {
    return 'green'
  }

  return status === 'review' ? 'orange' : 'red'
}

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
