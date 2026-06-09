export interface FutureChartQualityItem {
  modelName: string
  label: string
  needsCalibration: boolean
}

export interface FutureChartHiddenForecastItem {
  modelName: string
  label: string
}

export interface FutureChartSummaryItem {
  label: string
  value: string
  meta: string
}

export interface FutureChartGuideItem {
  label: string
  detail: string
}

export interface FutureChartShadowStoryItem {
  label: string
  value: string
  meta: string
}
