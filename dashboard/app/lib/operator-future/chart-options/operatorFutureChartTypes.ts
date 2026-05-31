import { dashboardChartTokens } from '../../charts/dashboardChartCore'

export interface AxisTooltipItem {
  marker?: string
  seriesName?: string
  value?: number | null
  axisValue?: string
}

export interface ChartTooltip {
  trigger: string
  backgroundColor: string
  borderColor: string
  borderWidth: number
  textStyle: Record<string, string>
  formatter: (params: AxisTooltipItem[]) => string
}

interface ChartAxisLabel {
  color: string
  fontWeight: number
  fontSize?: number
  interval?: number
  formatter?: (value: string) => string
}

interface ChartCategoryAxis {
  type: 'category'
  data: string[]
  axisLabel: ChartAxisLabel
}

interface ChartValueAxis {
  type: 'value'
  name: string
  offset?: number
  min?: number
  max?: number
  axisLabel: ChartAxisLabel
}

export interface ChartSeries {
  type: string
  name: string
  data: Array<number | null>
  smooth?: boolean
  yAxisIndex?: number
  symbol?: string
  symbolSize?: number
  lineStyle?: Record<string, unknown>
  itemStyle?: Record<string, unknown>
}

export interface ChartOption {
  [key: string]: unknown
  animationDuration: number
  backgroundColor: string
  tooltip: ChartTooltip
  legend: Record<string, unknown>
  grid: Record<string, unknown>
  xAxis: ChartCategoryAxis
  yAxis: ChartValueAxis | ChartValueAxis[]
  series: ChartSeries[]
}

export const chartTextStyle = { color: dashboardChartTokens.legendTextOnDark, fontWeight: 800 }
export const axisLabelStyle = { color: dashboardChartTokens.axisTextOnDark, fontWeight: 800 }

export const baseTooltip = (formatter: ChartTooltip['formatter']): ChartTooltip => ({
  trigger: 'axis',
  backgroundColor: dashboardChartTokens.tooltipBackgroundDark,
  borderColor: dashboardChartTokens.tooltipBorderOnDark,
  borderWidth: 2,
  textStyle: { color: dashboardChartTokens.tooltipTextOnDark },
  formatter
})

export function valueAxis(name: string): ChartValueAxis {
  return {
    type: 'value',
    name,
    axisLabel: axisLabelStyle
  }
}
