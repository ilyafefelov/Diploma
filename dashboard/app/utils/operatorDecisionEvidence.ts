import type {
  BaselineLpPreview,
  DashboardBatteryStateResponse,
  DashboardExogenousSignalsResponse,
  ForecastDispatchSensitivityResponse,
  OperatorRecommendationResponse,
  RealDataBenchmarkResponse
} from '../types/control-plane'
import {
  CURRENT_OFFLINE_STRATEGY_PROMOTION_HEADLINE,
  CURRENT_REGRET_LADDER
} from './defenseDataset'
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

const SELECTED_STRATEGY_STATUS_LABELS: Record<string, string> = {
  schedule_value_learner_v2_plus: 'Offline V2+',
  strict_similar_day: 'Strict control',
  nbeatsx_silver_v0: 'Compact NBEATSx',
  tft_silver_v0: 'Compact TFT',
  decision_transformer: 'DT preview'
}

export const buildOperatorStrategyEvidenceRows = (
  modelRows: DefenseModelRow[],
  operatorRecommendation?: OperatorRecommendationResponse | null
): OperatorStrategyEvidenceRow[] => {
  const sourceRows = modelRows.length > 0
    ? modelRows
    : fallbackStrategyEvidenceRows(operatorRecommendation)
  const controlRegret = sourceRows.find(row => row.modelName === 'strict_similar_day')?.meanRegretUah ?? null

  return sourceRows
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
  limit = 24,
  operatorRecommendation?: OperatorRecommendationResponse | null
): ControlRegretTimelinePoint[] => {
  const rows = benchmark?.rows
    .filter(row => row.forecast_model_name === 'strict_similar_day')
    .sort((left, right) => left.anchor_timestamp.localeCompare(right.anchor_timestamp))
    .slice(-limit) ?? []

  if (rows.length === 0) {
    return fallbackControlRegretTimeline(operatorRecommendation)
  }

  return rows.map(row => ({
    anchorLabel: formatAnchorLabel(row.anchor_timestamp),
    regretUah: row.regret_uah,
    decisionValueUah: row.decision_value_uah,
    oracleValueUah: row.oracle_value_uah,
    throughputMwh: row.total_throughput_mwh
  }))
}

export const buildSensitivityEvidenceRows = (
  sensitivity: ForecastDispatchSensitivityResponse | null,
  operatorRecommendation?: OperatorRecommendationResponse | null
): SensitivityEvidenceRow[] => {
  const rows = sensitivity?.bucket_summary.map(bucket => ({
    bucket: bucket.diagnostic_bucket,
    rows: bucket.rows,
    meanRegretUah: bucket.mean_regret_uah,
    meanForecastMaeUahMwh: bucket.mean_forecast_mae_uah_mwh,
    meanDispatchSpreadErrorUahMwh: bucket.mean_dispatch_spread_error_uah_mwh
  })) ?? []

  return rows.length > 0 ? rows : fallbackSensitivityEvidenceRows(operatorRecommendation)
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

const fallbackStrategyEvidenceRows = (
  operatorRecommendation?: OperatorRecommendationResponse | null
): DefenseModelRow[] => {
  const selectedStrategy = operatorRecommendation?.available_strategies.find((strategy) => {
    return strategy.strategy_id === operatorRecommendation.selected_strategy_id
  })

  return CURRENT_REGRET_LADDER.map(point => ({
    modelName: point.label,
    role: point.label === 'strict_similar_day' ? 'control' : 'forecast_candidate',
    anchorCount: 90,
    meanRegretUah: selectedStrategy?.mean_regret_uah !== null
      && selectedStrategy?.mean_regret_uah !== undefined
      && point.label === 'Calibrated V2+'
      ? selectedStrategy.mean_regret_uah
      : point.meanRegretUah,
    medianRegretUah: point.meanRegretUah,
    meanDecisionValueUah: 0,
    meanOracleValueUah: point.meanRegretUah,
    winRate: point.label === 'Calibrated V2+'
      ? selectedStrategy?.win_rate ?? 1
      : 0,
    meanThroughputMwh: operatorRecommendation?.economics?.total_throughput_mwh ?? 0
  }))
}

const fallbackControlRegretTimeline = (
  operatorRecommendation?: OperatorRecommendationResponse | null
): ControlRegretTimelinePoint[] => {
  const selectedThroughput = operatorRecommendation?.economics?.total_throughput_mwh ?? 0

  return CURRENT_REGRET_LADDER.map(point => ({
    anchorLabel: point.label,
    regretUah: point.meanRegretUah,
    decisionValueUah: 0,
    oracleValueUah: point.meanRegretUah,
    throughputMwh: point.label === 'Calibrated V2+' ? selectedThroughput : 0
  }))
}

const fallbackSensitivityEvidenceRows = (
  operatorRecommendation?: OperatorRecommendationResponse | null
): SensitivityEvidenceRow[] => {
  const scheduleRows = operatorRecommendation?.recommendation_schedule.length ?? 0
  const forecastContextRows = operatorRecommendation?.forecast_model_series.reduce((total, series) => {
    return total + series.points.length
  }, 0) ?? 0

  return [
    {
      bucket: 'strict control',
      rows: 90,
      meanRegretUah: CURRENT_OFFLINE_STRATEGY_PROMOTION_HEADLINE.strictMeanRegretUah,
      meanForecastMaeUahMwh: 0,
      meanDispatchSpreadErrorUahMwh: 0
    },
    {
      bucket: 'selected V2+',
      rows: scheduleRows || 24,
      meanRegretUah: CURRENT_OFFLINE_STRATEGY_PROMOTION_HEADLINE.meanRegretUah,
      meanForecastMaeUahMwh: 0,
      meanDispatchSpreadErrorUahMwh: 0
    },
    {
      bucket: 'forecast context',
      rows: forecastContextRows || scheduleRows || 24,
      meanRegretUah: CURRENT_OFFLINE_STRATEGY_PROMOTION_HEADLINE.meanRegretUah,
      meanForecastMaeUahMwh: 0,
      meanDispatchSpreadErrorUahMwh: 0
    }
  ]
}

const formatFraction = (value: number): string => `${Math.round(value * 100)}%`

const formatSelectedStrategyStatus = (strategyId: string): string => SELECTED_STRATEGY_STATUS_LABELS[strategyId]
  ?? strategyId
    .split('_')
    .filter(Boolean)
    .map(part => part.length <= 3 ? part.toUpperCase() : `${part[0]?.toUpperCase() ?? ''}${part.slice(1)}`)
    .join(' ')

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
