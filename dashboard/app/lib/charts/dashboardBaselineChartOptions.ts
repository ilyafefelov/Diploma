import type { EChartsOption, TooltipComponentFormatterCallbackParams } from 'echarts'

import type { BaselineLpPreview } from '~/types/control-plane'
import { dashboardChartTokens, formatCurrency, formatSignedMw, normalizeTooltipItems } from './dashboardChartCore'

export const buildBaselineForecastChartOption = (
  baselinePreview: BaselineLpPreview | null
): EChartsOption => {
  const forecastPoints = baselinePreview?.forecast || []
  const labels = forecastPoints.map(point => point.forecast_timestamp.slice(11, 16))
  const prices = forecastPoints.map(point => point.predicted_price_uah_mwh)

  return {
    animationDuration: 850,
    animationEasing: 'cubicOut',
    backgroundColor: 'transparent',
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
        const dataIndex = tooltipItems[0]?.dataIndex ?? 0
        const point = forecastPoints[dataIndex]
        const price = tooltipItems[0]?.value ?? 0

        return [
          tooltipItems[0]?.axisValueLabel || '',
          `Baseline forecast: ${Math.round(price)} UAH/MWh`,
          point ? `Source timestamp: ${point.source_timestamp.slice(11, 16)}` : 'Source timestamp: not available',
          'Field: forecast[].predicted_price_uah_mwh',
          'Current MVP path: deterministic LP over official/source-backed market rows'
        ].join('<br/>')
      }
    },
    grid: {
      left: 48,
      right: 18,
      top: 28,
      bottom: 32,
      containLabel: true
    },
    xAxis: {
      type: 'category',
      data: labels,
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
        formatter: (value: number) => `${Math.round(value)} UAH/MWh`
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
        name: 'Baseline forecast',
        smooth: true,
        data: prices,
        symbol: 'circle',
        symbolSize: 7,
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
      }
    ]
  }
}

export const buildBaselineScheduleChartOption = (
  baselinePreview: BaselineLpPreview | null
): EChartsOption => {
  const schedulePoints = baselinePreview?.recommendation_schedule || []
  const tracePoints = baselinePreview?.projected_state.trace || []
  const labels = schedulePoints.map(point => point.interval_start.slice(11, 16))
  const netPower = schedulePoints.map(point => point.recommended_net_power_mw)
  const soc = tracePoints.map(point => Number((point.soc_after_fraction * 100).toFixed(1)))

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
        const dataIndex = tooltipItems[0]?.dataIndex ?? 0
        const schedulePoint = schedulePoints[dataIndex]
        const tracePoint = tracePoints[dataIndex]
        const signedRecommendation = tooltipItems.find(item => item.seriesName === 'Signed recommendation')?.value ?? 0
        const projectedSoc = tooltipItems.find(item => item.seriesName === 'Projected SOC')?.value ?? 0

        return [
          tooltipItems[0]?.axisValueLabel || '',
          `Signed recommendation: ${formatSignedMw(signedRecommendation)}`,
          `Projected SOC: ${Math.round(projectedSoc)}%`,
          schedulePoint ? `Throughput: ${schedulePoint.throughput_mwh.toFixed(2)} MWh` : 'Throughput: not available',
          tracePoint ? `Degradation penalty: ${formatCurrency(tracePoint.degradation_penalty_uah)}` : 'Degradation penalty: not available',
          'Fields: recommendation_schedule[].recommended_net_power_mw and projected_state.trace[].soc_after_fraction',
          'Current MVP path: baseline LP schedule followed by projected battery-state simulation'
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
      data: labels,
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
    yAxis: [
      {
        type: 'value',
        name: 'MW',
        nameLocation: 'middle',
        nameGap: 40,
        axisLabel: {
          color: dashboardChartTokens.axis,
          fontWeight: 700,
          formatter: (value: number) => `${value.toFixed(1)}`
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
      {
        type: 'value',
        min: 0,
        max: 100,
        name: 'SOC %',
        nameLocation: 'middle',
        nameGap: 44,
        axisLabel: {
          color: dashboardChartTokens.axis,
          fontWeight: 700,
          formatter: (value: number) => `${value}%`
        },
        nameTextStyle: {
          color: dashboardChartTokens.rose,
          fontWeight: 800
        },
        splitLine: {
          show: false
        }
      }
    ],
    series: [
      {
        type: 'bar',
        name: 'Signed recommendation',
        data: netPower,
        barWidth: 16,
        itemStyle: {
          color: dashboardChartTokens.secondary,
          borderRadius: [10, 10, 10, 10]
        }
      },
      {
        type: 'line',
        name: 'Projected SOC',
        yAxisIndex: 1,
        smooth: true,
        data: soc,
        symbol: 'diamond',
        symbolSize: 7,
        lineStyle: {
          width: 3,
          color: dashboardChartTokens.rose
        },
        itemStyle: {
          color: dashboardChartTokens.rose
        }
      }
    ]
  }
}
