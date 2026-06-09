import type { EChartsOption, LineSeriesOption, TooltipComponentFormatterCallbackParams } from 'echarts'

import type { SignalPreview } from '~/types/control-plane'
import type { OperatorMarketVenue } from '~/types/operator-dashboard'
import {
  buildSignalTimelineLabels,
  dashboardChartTokens,
  formatTooltipAxisPeriod,
  formatWeatherSourceLabel,
  normalizeTooltipItems
} from './dashboardChartCore'

export interface MarketSignalHeroChartContext {
  priceContextStatus?: string | null
  priceContextSourceLabel?: string | null
}

export const buildMarketSignalHeroChartOption = (
  signalPreview: SignalPreview | null,
  marketVenue: OperatorMarketVenue | string = 'DAM',
  context: MarketSignalHeroChartContext = {}
): EChartsOption => {
  const signal = signalPreview || {
    labels: [],
    market_price: [],
    weather_bias: [],
    weather_sources: []
  }
  const adjustedMarketPrice = signal.market_price.map((price, index) => Number((price + (signal.weather_bias[index] || 0)).toFixed(2)))
  const timelineLabels = buildSignalTimelineLabels(signal)
  const venueLabel = marketVenue === 'IDM' ? 'IDM' : 'DAM'
  const hasWeatherBias = signal.weather_bias.some(value => Math.abs(value) > 0.001)
  const primarySeriesName = context.priceContextStatus === 'pre_publication_forecast'
    ? `${venueLabel} ML forecast price`
    : `${venueLabel} official/source price`
  const sourceLabel = context.priceContextSourceLabel || `${venueLabel} price context`
  const statusLabel = context.priceContextStatus
    ? context.priceContextStatus.replaceAll('_', ' ')
    : 'source backed'
  const series: LineSeriesOption[] = [
    {
      type: 'line',
      name: primarySeriesName,
      smooth: true,
      data: signal.market_price,
      symbol: 'circle',
      symbolSize: 8,
      lineStyle: {
        width: 4,
        color: dashboardChartTokens.heroCyan
      },
      itemStyle: {
        color: dashboardChartTokens.heroCyan,
        borderColor: dashboardChartTokens.heroCyanBorder,
        borderWidth: 2
      },
      areaStyle: {
        color: dashboardChartTokens.heroCyanArea
      }
    }
  ]

  if (hasWeatherBias) {
    series.push({
      type: 'line',
      name: 'Weather-adjusted context',
      smooth: true,
      data: adjustedMarketPrice,
      symbol: 'diamond',
      symbolSize: 8,
      lineStyle: {
        width: 4,
        color: dashboardChartTokens.highlightOnDark
      },
      itemStyle: {
        color: dashboardChartTokens.highlightOnDark,
        borderColor: dashboardChartTokens.heroHighlightBorder,
        borderWidth: 2
      },
      areaStyle: {
        color: dashboardChartTokens.heroHighlightArea
      }
    })
  }

  return {
    animationDuration: 950,
    animationEasing: 'cubicOut',
    backgroundColor: 'transparent',
    color: [
      dashboardChartTokens.heroCyan,
      dashboardChartTokens.highlightOnDark,
      dashboardChartTokens.heroCyan,
      dashboardChartTokens.heroAccentOnDark
    ],
    legend: {
      top: 2,
      left: 12,
      right: 12,
      itemGap: 10,
      textStyle: {
        color: dashboardChartTokens.legendTextOnDark,
        fontSize: 11,
        fontWeight: 800
      }
    },
    tooltip: {
      trigger: 'axis',
      backgroundColor: dashboardChartTokens.tooltipBackgroundDark,
      borderWidth: 2,
      borderColor: dashboardChartTokens.tooltipBorderStrongOnDark,
      padding: [12, 14],
      textStyle: {
        color: dashboardChartTokens.tooltipTextOnDark
      },
      formatter: (params: TooltipComponentFormatterCallbackParams) => {
        const tooltipItems = normalizeTooltipItems(params)
        const dataIndex = tooltipItems[0]?.dataIndex ?? 0
        const weatherSource = formatWeatherSourceLabel(signal.weather_sources[dataIndex] || sourceLabel)
        const tooltipRows = [
          `<strong>${formatTooltipAxisPeriod(tooltipItems[0]?.axisValueLabel || '')}</strong>`,
          `${primarySeriesName}: ${Math.round(tooltipItems.find(item => item.seriesName === primarySeriesName)?.value ?? 0)} UAH/MWh`,
          `Source: ${sourceLabel}`,
          `Status: ${statusLabel}`,
          `Row evidence: ${weatherSource}`,
          'MVP process: DAM/IDM hourly recommendation preview. Price rows inform review only, not live IDM bidding.'
        ]

        if (hasWeatherBias) {
          tooltipRows.splice(
            2,
            0,
            `Weather-adjusted context: ${Math.round(tooltipItems.find(item => item.seriesName === 'Weather-adjusted context')?.value ?? 0)} UAH/MWh`
          )
        }

        return tooltipRows.join('<br/>')
      }
    },
    grid: {
      left: 54,
      right: 18,
      top: 74,
      bottom: 38,
      containLabel: true
    },
    xAxis: {
      type: 'category',
      boundaryGap: false,
      data: timelineLabels,
      axisLabel: {
        color: dashboardChartTokens.axisTextOnDark,
        fontWeight: 800
      },
      axisLine: {
        lineStyle: {
          color: dashboardChartTokens.axisLineOnDark
        }
      },
      splitLine: {
        show: true,
        lineStyle: {
          color: dashboardChartTokens.gridMutedOnDark
        }
      }
    },
    yAxis: {
      type: 'value',
      name: 'UAH/MWh',
      nameLocation: 'middle',
      nameGap: 42,
      axisLabel: {
        color: dashboardChartTokens.axisTextOnDark,
        fontWeight: 800,
        formatter: (value: number) => `${Math.round(value)}`
      },
      nameTextStyle: {
        color: dashboardChartTokens.heroCyan,
        fontWeight: 900
      },
      splitLine: {
        lineStyle: {
          color: dashboardChartTokens.gridOnDark
        }
      }
    },
    series
  }
}
