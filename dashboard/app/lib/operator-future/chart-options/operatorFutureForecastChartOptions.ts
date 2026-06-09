import type { FutureForecastSeriesResponse } from '~/types/control-plane'
import type { RecommendationInputSignalPoint } from '~/utils/operatorFutureStack'
import { dashboardChartTokens } from '../../charts/dashboardChartCore'
import {
  formatForecastSeriesLabel,
  formatInputSignalTooltipValue
} from '../../../utils/operatorFutureStackPresentation'
import {
  axisLabelStyle,
  baseTooltip,
  valueAxis,
  type ChartOption,
  type ChartSeries
} from './operatorFutureChartTypes'

export function buildRecommendationInputChartSeries(
  rows: RecommendationInputSignalPoint[]
): ChartSeries[] {
  const series: ChartSeries[] = [
    {
      type: 'line',
      name: 'Recommendation price context (UAH/MWh)',
      smooth: true,
      data: rows.map(row => row.forecastPriceUahMwh),
      lineStyle: { width: 3, color: dashboardChartTokens.highlightOnDark },
      itemStyle: { color: dashboardChartTokens.highlightOnDark }
    },
    {
      type: 'bar',
      name: 'Selected battery net power (MW)',
      yAxisIndex: 1,
      data: rows.map(row => row.selectedNetPowerMw),
      itemStyle: { color: dashboardChartTokens.secondarySoftOnDark, borderRadius: [7, 7, 0, 0] }
    },
    {
      type: 'line',
      name: 'Projected SOC (%)',
      yAxisIndex: 2,
      smooth: true,
      data: rows.map(row => row.projectedSocPercent),
      lineStyle: { width: 3, color: dashboardChartTokens.rose },
      itemStyle: { color: dashboardChartTokens.rose }
    }
  ]

  if (rows.some(row => row.siteNetLoadMw != null)) {
    series.push({
      type: 'line',
      name: 'Site net load estimate (MW)',
      yAxisIndex: 1,
      smooth: true,
      data: rows.map(row => row.siteNetLoadMw),
      lineStyle: { width: 2.5, color: dashboardChartTokens.warning, type: 'dashed' },
      itemStyle: { color: dashboardChartTokens.warning }
    })
  }

  return series
}

export function buildForecastOption(input: {
  hasRecommendationInputSignalRows: boolean
  recommendationInputSignalRows: RecommendationInputSignalPoint[]
  recommendationInputChartSeries: ChartSeries[]
  forecastLabels: string[]
  forecastChartSeries: FutureForecastSeriesResponse[]
}): ChartOption {
  return {
    animationDuration: 500,
    backgroundColor: 'transparent',
    tooltip: baseTooltip((params) => {
      const lines = params.map((item) => {
        const value = input.hasRecommendationInputSignalRows
          ? formatInputSignalTooltipValue(item.seriesName, item.value)
          : `${Math.round(item.value ?? 0).toLocaleString('en-GB')} UAH/MWh`
        return `${item.marker || ''}${item.seriesName}: ${value}`
      })
      const footer = input.hasRecommendationInputSignalRows
        ? 'These are the selected recommendation inputs: price context, battery action, SOC path, and site-load estimate where available.'
        : 'Forecast context only; selected strategy is shown in the schedule chart.'
      return [`<strong>${params[0]?.axisValue || 'hour'}</strong>`, ...lines, footer].join('<br/>')
    }),
    legend: { show: false },
    grid: {
      left: 58,
      right: input.hasRecommendationInputSignalRows ? 78 : 36,
      top: 44,
      bottom: 42,
      containLabel: true
    },
    xAxis: {
      type: 'category',
      data: input.hasRecommendationInputSignalRows
        ? input.recommendationInputSignalRows.map(row => row.label)
        : input.forecastLabels,
      axisLabel: axisLabelStyle
    },
    yAxis: input.hasRecommendationInputSignalRows
      ? [
          valueAxis('UAH/MWh'),
          valueAxis('MW'),
          {
            ...valueAxis('SOC %'),
            offset: 46,
            min: 0,
            max: 100
          }
        ]
      : valueAxis('UAH/MWh'),
    series: input.hasRecommendationInputSignalRows
      ? input.recommendationInputChartSeries
      : input.forecastChartSeries.map(series => ({
          type: 'line',
          name: formatForecastSeriesLabel(series.model_name),
          smooth: true,
          symbol: series.model_family === 'TFT' ? 'diamond' : 'circle',
          symbolSize: 7,
          lineStyle: {
            width: 3,
            color: series.model_family === 'TFT' ? dashboardChartTokens.rose : dashboardChartTokens.highlightOnDark
          },
          itemStyle: { color: series.model_family === 'TFT' ? dashboardChartTokens.rose : dashboardChartTokens.highlightOnDark },
          data: series.points.map(point => Math.round(point.p50_price_uah_mwh ?? point.forecast_price_uah_mwh))
        }))
  }
}
