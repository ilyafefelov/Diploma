import type {
  FutureForecastSeriesResponse,
  RuntimeAccelerationResponse,
  OperatorStrategyOptionResponse
} from '~/types/control-plane'

const SOURCE_PRIORITY: Record<string, number> = {
  official: 0,
  calibrated: 1,
  compact: 2
}

const MODEL_PRIORITY: Record<string, number> = {
  nbeatsx: 0,
  tft: 1
}

export const formatForecastWindowLabel = (
  forecastWindowStart: string | null | undefined,
  forecastWindowEnd: string | null | undefined
): string => {
  if (!forecastWindowStart || !forecastWindowEnd) {
    return 'forecast window pending'
  }

  return `${formatWindowTimestamp(forecastWindowStart)} -> ${formatWindowTimestamp(forecastWindowEnd)}`
}

export const sortFutureForecastSeries = (
  series: FutureForecastSeriesResponse[]
): FutureForecastSeriesResponse[] => [...series].sort((left, right) => {
  const sourceDelta = sourcePriority(left.source_status) - sourcePriority(right.source_status)
  if (sourceDelta !== 0) {
    return sourceDelta
  }

  const modelDelta = modelPriority(left.model_name) - modelPriority(right.model_name)
  if (modelDelta !== 0) {
    return modelDelta
  }

  return left.model_name.localeCompare(right.model_name)
})

export const buildStrategySelectItems = (
  strategies: OperatorStrategyOptionResponse[]
): Array<{ label: string, value: string, disabled: boolean }> => strategies.map(strategy => ({
  label: strategy.enabled ? strategy.label : `${strategy.label} - ${strategy.reason}`,
  value: strategy.strategy_id,
  disabled: !strategy.enabled
}))

export const formatRuntimeAccelerationLabel = (
  runtime: RuntimeAccelerationResponse | null | undefined
): string => {
  if (!runtime) {
    return 'runtime pending'
  }
  if (runtime.device_type === 'cuda') {
    return `CUDA / ${runtime.device_name}`
  }
  if (runtime.device_type === 'mps') {
    return `MPS / ${runtime.device_name}`
  }
  return `${runtime.device_name} / ${runtime.backend}`
}

const sourcePriority = (sourceStatus: string): number => {
  const normalized = sourceStatus.toLowerCase()
  for (const [needle, priority] of Object.entries(SOURCE_PRIORITY)) {
    if (normalized.includes(needle)) {
      return priority
    }
  }
  return 99
}

const modelPriority = (modelName: string): number => {
  const normalized = modelName.toLowerCase()
  for (const [needle, priority] of Object.entries(MODEL_PRIORITY)) {
    if (normalized.includes(needle)) {
      return priority
    }
  }
  return 99
}

const formatWindowTimestamp = (timestamp: string): string => new Date(timestamp).toLocaleString('en-GB', {
  day: '2-digit',
  month: 'short',
  hour: '2-digit',
  minute: '2-digit',
  timeZone: 'Europe/Kyiv'
})
