import { dashboardChartTokens } from '../../charts/dashboardChartCore'
import type { StrategyComparisonRow } from '../../../utils/operatorShadowPreview'
import { formatStrategyAxisLabel } from '../../../utils/operatorFutureStackPresentation'
import {
  axisLabelStyle,
  baseTooltip,
  chartTextStyle,
  valueAxis,
  type ChartOption
} from './operatorFutureChartTypes'

export function buildStrategyComparisonOption(input: {
  strategyComparisonLabels: string[]
  strategyComparisonRows: StrategyComparisonRow[]
}): ChartOption {
  return {
    animationDuration: 500,
    backgroundColor: 'transparent',
    tooltip: baseTooltip((params) => {
      const lines = params.map((item) => {
        const value = item.value == null
          ? 'n/a'
          : item.seriesName?.includes('regret')
            ? `${Math.round(item.value).toLocaleString('en-GB')} UAH`
            : `${Number(item.value).toFixed(2)} MWh`
        return `${item.marker || ''}${item.seriesName}: ${value}`
      })
      return [`<strong>${params[0]?.axisValue || 'strategy'}</strong>`, ...lines, 'All entries are preview-only; market execution remains false.'].join('<br/>')
    }),
    legend: {
      top: 0,
      textStyle: chartTextStyle
    },
    grid: { left: 58, right: 54, top: 48, bottom: 72, containLabel: true },
    xAxis: {
      type: 'category',
      data: input.strategyComparisonLabels,
      axisLabel: {
        ...axisLabelStyle,
        fontSize: 11,
        interval: 0,
        formatter: formatStrategyAxisLabel
      }
    },
    yAxis: [
      valueAxis('MWh'),
      valueAxis('UAH regret')
    ],
    series: [
      {
        type: 'bar',
        name: 'Charge MWh',
        data: input.strategyComparisonRows.map(row => row.totalChargeMwh),
        itemStyle: { color: dashboardChartTokens.warningStrongOnDark, borderRadius: [7, 7, 0, 0] }
      },
      {
        type: 'bar',
        name: 'Discharge MWh',
        data: input.strategyComparisonRows.map(row => row.totalDischargeMwh),
        itemStyle: { color: dashboardChartTokens.successStrongOnDark, borderRadius: [7, 7, 0, 0] }
      },
      {
        type: 'line',
        name: 'Mean regret vs strict',
        yAxisIndex: 1,
        smooth: true,
        data: input.strategyComparisonRows.map(row => row.meanRegretVsStrictUah),
        lineStyle: { width: 4, color: dashboardChartTokens.rose },
        itemStyle: { color: dashboardChartTokens.rose }
      },
      {
        type: 'line',
        name: 'Mean regret vs V2+',
        yAxisIndex: 1,
        smooth: true,
        data: input.strategyComparisonRows.map(row => row.meanRegretVsV2Uah),
        lineStyle: { width: 3, color: dashboardChartTokens.highlightOnDark, type: 'dashed' },
        itemStyle: { color: dashboardChartTokens.highlightOnDark }
      }
    ]
  }
}
