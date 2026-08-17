import type { EChartsOption, TooltipComponentFormatterCallbackParams } from 'echarts'

import type {
  ControlRegretTimelinePoint,
  OperatorStrategyEvidenceRow,
  SensitivityEvidenceRow
} from '~/utils/operatorDecisionEvidence'
import { dashboardChartTokens, normalizeTooltipItems } from './dashboardChartCore'

const roundedUah = (value: number): number => Math.round(value)

const darkTooltip = (
  formatter?: (params: TooltipComponentFormatterCallbackParams) => string
): EChartsOption['tooltip'] => ({
  trigger: 'axis',
  backgroundColor: dashboardChartTokens.tooltipBackgroundDark,
  borderColor: dashboardChartTokens.tooltipBorderOnDark,
  borderWidth: 2,
  textStyle: { color: dashboardChartTokens.tooltipTextOnDark },
  formatter
})

const darkLegend = (): EChartsOption['legend'] => ({
  top: 0,
  textStyle: {
    color: dashboardChartTokens.legendTextOnDark,
    fontWeight: 800
  }
})

const darkCategoryAxis = (
  data: string[],
  rotate = 0,
  formatter?: (value: string) => string
): NonNullable<EChartsOption['xAxis']> => ({
  type: 'category',
  data,
  axisLabel: {
    color: dashboardChartTokens.axisTextOnDark,
    fontSize: 10,
    fontWeight: 800,
    hideOverlap: formatter ? false : true,
    interval: 0,
    rotate,
    formatter
  }
})

const sparseTimelineCategoryAxis = (data: string[]): NonNullable<EChartsOption['xAxis']> => {
  const labelStep = Math.max(1, Math.ceil(data.length / 6))

  return {
    type: 'category',
    data,
    boundaryGap: false,
    axisTick: { show: false },
    axisLabel: {
      color: dashboardChartTokens.axisTextOnDark,
      fontSize: 10,
      fontWeight: 800,
      hideOverlap: true,
      interval: (index: number) => index === 0 || index === data.length - 1 || index % labelStep === 0,
      rotate: 0
    }
  }
}

const darkValueAxis = (name: string, overrides: Record<string, unknown> = {}): Record<string, unknown> => ({
  type: 'value',
  name,
  axisLabel: {
    color: dashboardChartTokens.axisTextOnDark,
    fontWeight: 800
  },
  ...overrides
})

export const buildDecisionStrategyEvidenceOption = (
  rows: OperatorStrategyEvidenceRow[]
): EChartsOption => ({
  animationDuration: 650,
  backgroundColor: 'transparent',
  tooltip: darkTooltip((params) => {
    const items = normalizeTooltipItems(params)
    const modelName = items[0]?.axisValueLabel || 'model'
    const itemRows = items.map((item) => {
      const suffix = item.seriesName === 'Win rate' ? '%' : ' UAH'
      return `${item.seriesName}: ${item.value}${suffix}`
    })

    return [
      `<strong>${modelName}</strong>`,
      ...itemRows,
      'Mean regret = lost value vs oracle. Win rate = share of anchors ranked best.'
    ].join('<br/>')
  }),
  legend: darkLegend(),
  grid: { left: 54, right: 46, top: 42, bottom: 58, containLabel: true },
  xAxis: darkCategoryAxis(rows.map(row => row.modelName), 28, formatStrategyAxisLabel),
  yAxis: [
    darkValueAxis('UAH'),
    darkValueAxis('win %', { min: 0, max: 100 })
  ],
  series: [
    {
      type: 'bar',
      name: 'Mean regret',
      data: rows.map(row => roundedUah(row.meanRegretUah)),
      itemStyle: { color: dashboardChartTokens.secondary, borderRadius: [8, 8, 0, 0] }
    },
    {
      type: 'line',
      name: 'Win rate',
      yAxisIndex: 1,
      data: rows.map(row => Math.round(row.winRate * 100)),
      symbol: 'diamond',
      symbolSize: 8,
      lineStyle: { width: 3, color: dashboardChartTokens.highlightOnDark },
      itemStyle: { color: dashboardChartTokens.highlightOnDark }
    }
  ]
})

const formatStrategyAxisLabel = (value: string): string => {
  const knownLabels: Record<string, string> = {
    strict_similar_day: 'Strict',
    risk_adjusted_value_gate_v0: 'Risk',
    bra_schedule_aware_ensemble_v0: 'BRA',
    schedule_value_learner_v2_plus: 'V2+',
    nbeatsx_silver_v0: 'NBEATSx',
    tft_silver_v0: 'TFT',
    nbeatsx_official_v0: 'NBEATSx',
    tft_official_v0: 'TFT',
    nbeatsx_official_idm_v0: 'NBEATSx IDM',
    tft_official_idm_v0: 'TFT IDM'
  }

  const knownLabel = knownLabels[value]
  if (knownLabel) {
    return knownLabel
  }

  const tokens = value.split('_').filter(Boolean)
  if (tokens.length <= 2) {
    return tokens.join(' ')
  }

  return tokens.slice(0, 2).join(' ')
}

export const buildDecisionControlRegretTimelineOption = (
  points: ControlRegretTimelinePoint[]
): EChartsOption => ({
  animationDuration: 650,
  backgroundColor: 'transparent',
  tooltip: darkTooltip(),
  legend: {
    top: 4,
    right: 8,
    itemGap: 14,
    textStyle: {
      color: dashboardChartTokens.legendTextOnDark,
      fontSize: 11,
      fontWeight: 800
    }
  },
  grid: { left: 58, right: 54, top: 60, bottom: 32, containLabel: true },
  xAxis: sparseTimelineCategoryAxis(points.map(point => point.anchorLabel)),
  yAxis: [
    darkValueAxis('UAH', { min: 0, splitNumber: 4 }),
    darkValueAxis('MWh', { min: 0, splitNumber: 4 })
  ],
  series: [
    {
      type: 'line',
      name: 'Control regret',
      smooth: true,
      data: points.map(point => roundedUah(point.regretUah)),
      showSymbol: points.length <= 8,
      symbol: 'circle',
      symbolSize: 6,
      lineStyle: { width: 3, color: dashboardChartTokens.rose },
      itemStyle: { color: dashboardChartTokens.rose }
    },
    {
      type: 'line',
      name: 'Throughput',
      yAxisIndex: 1,
      smooth: true,
      data: points.map(point => Number(point.throughputMwh.toFixed(3))),
      showSymbol: false,
      lineStyle: {
        width: 2,
        color: dashboardChartTokens.highlightOnDark,
        opacity: 0.82
      },
      itemStyle: {
        color: dashboardChartTokens.highlightOnDark
      },
      areaStyle: {
        color: dashboardChartTokens.highlightTranslucentOnDark,
        opacity: 0.36
      }
    }
  ]
})

export const buildDecisionSensitivityOption = (
  rows: SensitivityEvidenceRow[]
): EChartsOption => ({
  animationDuration: 650,
  backgroundColor: 'transparent',
  tooltip: darkTooltip(),
  legend: darkLegend(),
  grid: { left: 54, right: 44, top: 42, bottom: 44, containLabel: true },
  xAxis: darkCategoryAxis(rows.map(row => row.bucket), 12),
  yAxis: [
    darkValueAxis('UAH'),
    darkValueAxis('rows')
  ],
  series: [
    {
      type: 'bar',
      name: 'Mean regret (UAH)',
      data: rows.map(row => roundedUah(row.meanRegretUah)),
      itemStyle: { color: dashboardChartTokens.warning, borderRadius: [8, 8, 0, 0] }
    },
    {
      type: 'line',
      name: 'Evidence rows',
      yAxisIndex: 1,
      data: rows.map(row => row.rows),
      symbol: 'diamond',
      symbolSize: 8,
      lineStyle: { width: 3, color: dashboardChartTokens.highlightOnDark },
      itemStyle: { color: dashboardChartTokens.highlightOnDark }
    }
  ]
})
