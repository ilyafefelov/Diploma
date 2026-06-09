import type { EChartsOption, TooltipComponentFormatterCallbackParams } from 'echarts'

import type { OperatorRecommendationResponse, SignalPreview } from '~/types/control-plane'
import {
  buildSignalTimelineLabels,
  dashboardChartTokens,
  formatCurrency,
  formatSignedMw,
  formatTooltipAxisPeriod,
  normalizeTooltipItems
} from './dashboardChartCore'

export const buildDispatchBalanceChartOption = (
  signalPreview: SignalPreview | null
): EChartsOption => {
  const signal = signalPreview || {
    labels: ['06:00', '09:00', '12:00', '15:00', '18:00', '21:00'],
    charge_intent: [0, 0, 0, 0, 0, 0],
    regret: [0, 0, 0, 0, 0, 0]
  }
  const timelineLabels = buildSignalTimelineLabels(signal)

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
        const batteryAction = tooltipItems.find(item => item.seriesName === 'Battery action')?.value ?? 0
        const missedValue = tooltipItems.find(item => item.seriesName === 'Missed value')?.value ?? 0

        return [
          formatTooltipAxisPeriod(tooltipItems[0]?.axisValueLabel || ''),
          `Battery action: ${formatSignedMw(batteryAction)}`,
          `Missed value: ${formatCurrency(missedValue)}`,
          'Battery action formula: clamp(((adjusted_price - avg_adjusted_price) / max_deviation) * max_power_mw)',
          'Sign meaning: positive MW = discharge bias, negative MW = charge bias',
          'Missed value formula: max(80, weather_bias * 2.4 + |adjusted_price - avg_adjusted_price| * 0.45)',
          'Missed value is a current MVP opportunity score, not settlement revenue and not the final promoted policy metric'
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
          color: dashboardChartTokens.secondary,
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
        name: 'UAH',
        nameLocation: 'middle',
        nameGap: 42,
        axisLabel: {
          color: dashboardChartTokens.axis,
          fontWeight: 700,
          formatter: (value: number) => `${Math.round(value)}`
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
        name: 'Battery action',
        data: signal.charge_intent,
        barWidth: 18,
        itemStyle: {
          color: dashboardChartTokens.secondary,
          borderRadius: [10, 10, 0, 0]
        }
      },
      {
        type: 'line',
        name: 'Missed value',
        yAxisIndex: 1,
        smooth: true,
        data: signal.regret,
        symbol: 'circle',
        symbolSize: 8,
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

export const buildSelectedStrategyDispatchChartOption = (
  operatorRecommendation: OperatorRecommendationResponse | null,
  fallbackSignalPreview: SignalPreview | null
): EChartsOption => {
  const schedulePoints = operatorRecommendation?.recommendation_schedule || []

  if (schedulePoints.length === 0) {
    return buildDispatchBalanceChartOption(fallbackSignalPreview)
  }

  const labels = buildSignalTimelineLabels({
    labels: schedulePoints.map(point => point.interval_start.slice(11, 16)),
    label_timestamps: schedulePoints.map(point => point.interval_start),
    timezone: 'Europe/Kyiv'
  })
  const netPower = schedulePoints.map(point => Number(point.recommended_net_power_mw.toFixed(3)))
  const netValue = schedulePoints.map(point => Math.round(point.net_value_uah))
  const valueGap = schedulePoints.map((point) => {
    const matchingGap = operatorRecommendation?.value_gap_series.find(gap => gap.step_index === point.step_index)
    return Math.round(matchingGap?.value_gap_uah ?? 0)
  })

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
        const signedRecommendation = tooltipItems.find(item => item.seriesName === 'Selected net power')?.value ?? 0
        const selectedValue = tooltipItems.find(item => item.seriesName === 'Selected net value')?.value ?? 0
        const visibleGap = tooltipItems.find(item => item.seriesName === 'Visible value gap')?.value ?? 0

        return [
          formatTooltipAxisPeriod(tooltipItems[0]?.axisValueLabel || ''),
          `Selected action: ${formatSignedMw(signedRecommendation)}`,
          `Selected net value: ${formatCurrency(selectedValue)}`,
          `Visible value gap: ${formatCurrency(visibleGap)}`,
          schedulePoint ? `Forecast price: ${Math.round(schedulePoint.forecast_price_uah_mwh)} UAH/MWh` : 'Forecast price: not available',
          schedulePoint ? `Projected SOC: ${Math.round(schedulePoint.projected_soc_after_fraction * 100)}%` : 'Projected SOC: not available',
          'Source: selected operator recommendation read model',
          'Boundary: preview schedule only; not a market dispatch command'
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
          color: dashboardChartTokens.secondary,
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
        name: 'UAH',
        nameLocation: 'middle',
        nameGap: 42,
        axisLabel: {
          color: dashboardChartTokens.axis,
          fontWeight: 700,
          formatter: (value: number) => `${Math.round(value)}`
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
        name: 'Selected net power',
        data: netPower,
        barWidth: 18,
        itemStyle: {
          color: dashboardChartTokens.secondary,
          borderRadius: [10, 10, 10, 10]
        }
      },
      {
        type: 'line',
        name: 'Selected net value',
        yAxisIndex: 1,
        smooth: true,
        data: netValue,
        symbol: 'circle',
        symbolSize: 8,
        lineStyle: {
          width: 3,
          color: dashboardChartTokens.highlight
        },
        itemStyle: {
          color: dashboardChartTokens.highlight
        }
      },
      {
        type: 'line',
        name: 'Visible value gap',
        yAxisIndex: 1,
        smooth: true,
        data: valueGap,
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
