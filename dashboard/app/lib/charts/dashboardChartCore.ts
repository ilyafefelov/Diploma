import type { TooltipComponentFormatterCallbackParams } from 'echarts'

import type { SignalPreview } from '~/types/control-plane'

export type TooltipPoint = {
  axisValueLabel: string
  dataIndex: number
  seriesName: string
  value: number
  data: unknown
}

export type SignalTimelineInput
  = Pick<SignalPreview, 'labels'>
    & Partial<Pick<SignalPreview, 'label_timestamps' | 'resolved_location' | 'timezone'>>

export const dashboardChartTokens = {
  grid: 'rgba(0, 91, 149, 0.14)',
  axis: '#315c83',
  primary: '#0079c1',
  secondary: '#53b2ea',
  secondarySoftOnDark: 'rgba(83, 178, 234, 0.78)',
  secondaryStrongOnDark: 'rgba(83, 178, 234, 0.8)',
  highlight: '#7ed321',
  highlightOnDark: '#b8ff32',
  highlightTranslucentOnDark: 'rgba(184, 255, 50, 0.58)',
  warning: '#f5a623',
  warningStrongOnDark: 'rgba(245, 166, 35, 0.82)',
  rose: '#ff6fae',
  roseMutedOnDark: 'rgba(255, 111, 174, 0.45)',
  successStrongOnDark: 'rgba(83, 234, 141, 0.82)',
  tooltipBackground: 'rgba(0, 91, 157, 0.96)',
  tooltipBackgroundDark: 'rgba(0, 50, 104, 0.98)',
  tooltipBorder: 'rgba(255, 255, 255, 0.96)',
  tooltipBorderOnDark: 'rgba(202, 249, 255, 0.9)',
  tooltipBorderStrongOnDark: 'rgba(202, 249, 255, 0.92)',
  tooltipText: '#1b3551',
  tooltipTextOnDark: '#f0fbff',
  legendTextOnDark: 'rgba(236, 250, 255, 0.88)',
  axisTextOnDark: 'rgba(219, 245, 255, 0.9)',
  axisLineOnDark: 'rgba(152, 224, 255, 0.32)',
  gridOnDark: 'rgba(152, 224, 255, 0.13)',
  gridMutedOnDark: 'rgba(152, 224, 255, 0.11)',
  primaryArea: 'rgba(0, 121, 193, 0.12)',
  highlightBar: 'rgba(126, 211, 33, 0.68)',
  heroCyan: '#50f0ff',
  heroCyanBorder: '#e6fbff',
  heroCyanArea: 'rgba(80, 240, 255, 0.1)',
  heroAccentOnDark: '#d7ff4f',
  heroHighlightBorder: '#f4ffd0',
  heroHighlightArea: 'rgba(126, 211, 33, 0.1)',
  pointBorderOnDark: '#ffffff',
  shadow: 'rgba(0, 121, 193, 0.16)'
} as const

export const formatSignedMw = (value: number): string => `${value > 0 ? '+' : ''}${value.toFixed(2)} MW`

export const formatCurrency = (value: number): string => `${Math.round(value).toLocaleString('en-GB')} UAH`

export const formatSignedCurrencyPerMwh = (value: number): string => `${value > 0 ? '+' : ''}${Math.round(value).toLocaleString('en-GB')} UAH/MWh`

export const formatSignalTimestampLabel = (
  timestamp: string | undefined,
  fallbackLabel: string,
  timeZone: string | null | undefined
): string => {
  if (!timestamp) {
    return fallbackLabel
  }

  const parsedTimestamp = new Date(timestamp)
  if (Number.isNaN(parsedTimestamp.getTime())) {
    return fallbackLabel
  }

  try {
    return new Intl.DateTimeFormat('en-GB', {
      day: '2-digit',
      month: 'short',
      hour: '2-digit',
      minute: '2-digit',
      hour12: false,
      timeZone: timeZone || 'Europe/Kyiv'
    }).format(parsedTimestamp).replace(', ', '\n')
  } catch {
    return fallbackLabel
  }
}

export const buildSignalTimelineLabels = (signal: SignalTimelineInput): string[] => {
  const timeZone = signal.timezone || signal.resolved_location?.timezone

  return signal.labels.map((label, index) =>
    formatSignalTimestampLabel(signal.label_timestamps?.[index], label, timeZone)
  )
}

export const formatTooltipAxisPeriod = (axisValueLabel: string): string => axisValueLabel.replace('\n', ' ')

export const asRecord = (value: unknown): Record<string, unknown> | null => {
  if (typeof value !== 'object' || value === null) {
    return null
  }

  return value as Record<string, unknown>
}

export const numberFromValue = (value: unknown): number => {
  if (typeof value === 'number') {
    return value
  }

  if (typeof value === 'string') {
    const numericValue = Number(value)
    return Number.isFinite(numericValue) ? numericValue : 0
  }

  if (Array.isArray(value)) {
    const numericValue = value.find((entry): entry is number => typeof entry === 'number')
    return numericValue ?? 0
  }

  return 0
}

export const normalizeTooltipItems = (params: TooltipComponentFormatterCallbackParams): TooltipPoint[] => {
  const rawItems: unknown[] = Array.isArray(params) ? params : [params]

  return rawItems.map((rawItem) => {
    const item = asRecord(rawItem)

    return {
      axisValueLabel: typeof item?.axisValueLabel === 'string' ? item.axisValueLabel : '',
      dataIndex: typeof item?.dataIndex === 'number' ? item.dataIndex : 0,
      seriesName: typeof item?.seriesName === 'string' ? item.seriesName : '',
      value: numberFromValue(item?.value),
      data: item?.data
    }
  })
}

export const formatWeatherSourceLabel = (source: string): string => {
  if (source === 'OPEN_METEO') {
    return 'Open-Meteo live'
  }

  return source.replaceAll('_', ' ').toLowerCase().replace(/(^|\s)\w/g, letter => letter.toUpperCase())
}
