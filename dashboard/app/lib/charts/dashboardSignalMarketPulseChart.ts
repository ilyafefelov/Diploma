import type { EChartsOption, TooltipComponentFormatterCallbackParams } from 'echarts'

import type { SignalPreview } from '~/types/control-plane'
import type { OperatorMarketVenue } from '~/types/operator-dashboard'
import {
  buildSignalTimelineLabels,
  dashboardChartTokens,
  formatSignedCurrencyPerMwh,
  formatTooltipAxisPeriod,
  formatWeatherSourceLabel,
  normalizeTooltipItems
} from './dashboardChartCore'

export const buildMarketPulseChartOption = (
  signalPreview: SignalPreview | null,
  marketVenue: OperatorMarketVenue | string = 'DAM'
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

  return {
    animationDuration: 850,
    animationEasing: 'cubicOut',
    backgroundColor: 'transparent',
    legend: {
      top: 0,
      right: 0,
      textStyle: {
        color: dashboardChartTokens.axis,
        fontWeight: 700
      }
    },
    tooltip: {
      trigger: 'axis',
      backgroundColor: dashboardChartTokens.tooltipBackground,
      borderWidth: 2,
      borderColor: dashboardChartTokens.tooltipBorder,
      textStyle: {
        color: dashboardChartTokens.tooltipTextOnDark
      },
      formatter: (params: TooltipComponentFormatterCallbackParams) => {
        const tooltipItems = normalizeTooltipItems(params)
        const price = tooltipItems.find(item => item.seriesName === 'Expected market price')?.value ?? 0
        const uplift = tooltipItems.find(item => item.seriesName === 'Weather effect')?.value ?? 0
        const adjusted = tooltipItems.find(item => item.seriesName === 'Price after weather')?.value ?? 0
        const dataIndex = tooltipItems[0]?.dataIndex ?? 0
        const weatherSource = formatWeatherSourceLabel(signal.weather_sources[dataIndex] || 'SOURCE_UNAVAILABLE')

        return [
          formatTooltipAxisPeriod(tooltipItems[0]?.axisValueLabel || ''),
          `Expected market price: ${Math.round(price)} UAH/MWh`,
          `Calibrated weather effect: ${formatSignedCurrencyPerMwh(uplift)}`,
          `Price after weather: ${Math.round(adjusted)} UAH/MWh`,
          `Weather source: ${weatherSource}`,
          'Formula: price_after_weather = market_price + weather_bias',
          'Weather inputs: cloud cover, precipitation, humidity excess, temperature gap, effective solar, wind speed',
          `Current MVP source mix: official/source-backed ${venueLabel} rows plus governed weather context`
        ].join('<br/>')
      }
    },
    grid: {
      left: 48,
      right: 18,
      top: 40,
      bottom: 32,
      containLabel: true
    },
    xAxis: {
      type: 'category',
      data: timelineLabels,
      axisLabel: {
        color: dashboardChartTokens.axis,
        fontWeight: 700
      },
      axisLine: {
        lineStyle: {
          color: dashboardChartTokens.grid
        }
      }
    },
    yAxis: {
      type: 'value',
      name: 'UAH/MWh',
      nameLocation: 'middle',
      nameGap: 42,
      axisLabel: {
        color: dashboardChartTokens.axis,
        fontWeight: 700,
        formatter: (value: number) => `${Math.round(value)}`
      },
      nameTextStyle: {
        color: dashboardChartTokens.primary,
        fontWeight: 800
      },
      splitLine: {
        lineStyle: {
          color: dashboardChartTokens.grid
        }
      }
    },
    series: [
      {
        type: 'line',
        name: 'Expected market price',
        smooth: true,
        data: signal.market_price,
        symbol: 'circle',
        symbolSize: 8,
        lineStyle: {
          width: 4,
          color: dashboardChartTokens.primary
        },
        itemStyle: {
          color: dashboardChartTokens.primary
        },
        areaStyle: {
          color: dashboardChartTokens.primaryArea
        }
      },
      {
        type: 'bar',
        name: 'Weather effect',
        data: signal.weather_bias,
        barWidth: 14,
        itemStyle: {
          color: dashboardChartTokens.highlightBar,
          borderRadius: [10, 10, 0, 0]
        }
      },
      {
        type: 'line',
        name: 'Price after weather',
        smooth: true,
        data: adjustedMarketPrice,
        symbol: 'diamond',
        symbolSize: 7,
        lineStyle: {
          width: 3,
          type: 'dashed',
          color: dashboardChartTokens.highlight
        },
        itemStyle: {
          color: dashboardChartTokens.highlight
        }
      }
    ]
  }
}
