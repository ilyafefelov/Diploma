import type { EChartsOption } from 'echarts'

import type { BaselineLpPreview, SignalPreview, TenantSummary } from '~/types/control-plane'

type TenantScatterPoint = {
  value: [number, number]
  name: string
  tenant: TenantSummary
  itemStyle: {
    color: string
    borderColor: string
    borderWidth: number
    shadowBlur: number
    shadowColor: string
  }
  symbolSize: number
  symbol: 'diamond' | 'circle'
}

export const dashboardChartTokens = {
  grid: 'rgba(0, 121, 193, 0.12)',
  axis: '#6c7c92',
  primary: '#0079c1',
  secondary: '#53b2ea',
  highlight: '#7ed321',
  warning: '#f5a623',
  rose: '#ff6fae',
  tooltipBackground: 'rgba(255, 255, 255, 0.96)',
  tooltipText: '#1b3551',
  shadow: 'rgba(0, 121, 193, 0.16)'
} as const

const formatSignedMw = (value: number): string => `${value > 0 ? '+' : ''}${value.toFixed(2)} MW`

const formatCurrency = (value: number): string => `${Math.round(value).toLocaleString('en-GB')} UAH`

const createTenantPoint = (tenant: TenantSummary, selectedTenantId: string): TenantScatterPoint => {
  const isSelected = tenant.tenant_id === selectedTenantId

  return {
    value: [tenant.longitude, tenant.latitude],
    name: tenant.name || tenant.tenant_id,
    tenant,
    itemStyle: {
      color: isSelected ? dashboardChartTokens.highlight : dashboardChartTokens.primary,
      borderColor: '#ffffff',
      borderWidth: isSelected ? 3 : 2,
      shadowBlur: isSelected ? 22 : 12,
      shadowColor: dashboardChartTokens.shadow
    },
    symbolSize: isSelected ? 30 : 18,
    symbol: isSelected ? 'diamond' : 'circle'
  }
}

export const buildTenantRegistryChartOption = (
  tenants: TenantSummary[],
  selectedTenantId: string
): EChartsOption => {
  const points = tenants.map((tenant) => createTenantPoint(tenant, selectedTenantId))
  const latitudes = tenants.map((tenant) => tenant.latitude)
  const longitudes = tenants.map((tenant) => tenant.longitude)
  const latitudeMin = latitudes.length > 0 ? Math.min(...latitudes) - 0.7 : 44
  const latitudeMax = latitudes.length > 0 ? Math.max(...latitudes) + 0.7 : 52
  const longitudeMin = longitudes.length > 0 ? Math.min(...longitudes) - 0.7 : 22
  const longitudeMax = longitudes.length > 0 ? Math.max(...longitudes) + 0.7 : 32

  return {
    animationDuration: 1400,
    animationEasing: 'elasticOut',
    backgroundColor: 'transparent',
    grid: {
      left: 64,
      right: 28,
      top: 28,
      bottom: 56,
      containLabel: true
    },
    tooltip: {
      trigger: 'item',
      backgroundColor: dashboardChartTokens.tooltipBackground,
      borderWidth: 2,
      borderColor: 'rgba(255, 255, 255, 0.96)',
      padding: [12, 14],
      textStyle: {
        color: dashboardChartTokens.tooltipText
      },
      formatter: (params: { data?: TenantScatterPoint }) => {
        const tenant = params.data?.tenant

        if (!tenant) {
          return 'Tenant data unavailable'
        }

        return [
          `<strong>${tenant.name || tenant.tenant_id}</strong>`,
          tenant.type || 'unspecified',
          `Lat ${tenant.latitude.toFixed(2)} / Lon ${tenant.longitude.toFixed(2)}`,
          tenant.timezone
        ].join('<br/>')
      }
    },
    xAxis: {
      type: 'value',
      min: longitudeMin,
      max: longitudeMax,
      name: 'Longitude',
      nameLocation: 'middle',
      nameGap: 36,
      axisLabel: {
        color: dashboardChartTokens.axis,
        fontWeight: 700
      },
      nameTextStyle: {
        color: dashboardChartTokens.primary,
        fontWeight: 800
      },
      axisLine: {
        lineStyle: {
          color: dashboardChartTokens.grid
        }
      },
      splitLine: {
        lineStyle: {
          color: dashboardChartTokens.grid
        }
      }
    },
    yAxis: {
      type: 'value',
      min: latitudeMin,
      max: latitudeMax,
      name: 'Latitude',
      nameLocation: 'middle',
      nameGap: 42,
      axisLabel: {
        color: dashboardChartTokens.axis,
        fontWeight: 700
      },
      nameTextStyle: {
        color: dashboardChartTokens.primary,
        fontWeight: 800
      },
      axisLine: {
        lineStyle: {
          color: dashboardChartTokens.grid
        }
      },
      splitLine: {
        lineStyle: {
          color: dashboardChartTokens.grid
        }
      }
    },
    series: [
      {
        type: 'scatter',
        data: points,
        symbolKeepAspect: true,
        emphasis: {
          scale: true,
          focus: 'series'
        },
        itemStyle: {
          opacity: 0.95
        }
      }
    ]
  }
}

export const buildMarketPulseChartOption = (
  signalPreview: SignalPreview | null
): EChartsOption => {
  const signal = signalPreview || {
    labels: ['06:00', '09:00', '12:00', '15:00', '18:00', '21:00'],
    market_price: [0, 0, 0, 0, 0, 0],
    weather_bias: [0, 0, 0, 0, 0, 0]
  }

  return {
    animationDuration: 1100,
    animationEasing: 'elasticOut',
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
      borderColor: 'rgba(255, 255, 255, 0.96)',
      textStyle: {
        color: dashboardChartTokens.tooltipText
      },
      valueFormatter: (value: number | string) => `${Math.round(Number(value))} UAH/MWh`
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
      data: signal.labels,
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
        name: 'DAM preview',
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
          color: 'rgba(0, 121, 193, 0.12)'
        }
      },
      {
        type: 'line',
        name: 'Weather bias',
        smooth: true,
        data: signal.weather_bias,
        symbol: 'diamond',
        symbolSize: 7,
        lineStyle: {
          width: 3,
          color: dashboardChartTokens.highlight
        },
        itemStyle: {
          color: dashboardChartTokens.highlight
        }
      }
    ]
  }
}

export const buildDispatchBalanceChartOption = (
  signalPreview: SignalPreview | null
): EChartsOption => {
  const signal = signalPreview || {
    labels: ['06:00', '09:00', '12:00', '15:00', '18:00', '21:00'],
    charge_intent: [0, 0, 0, 0, 0, 0],
    regret: [0, 0, 0, 0, 0, 0]
  }

  return {
    animationDuration: 1100,
    animationEasing: 'elasticOut',
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
      borderColor: 'rgba(255, 255, 255, 0.96)',
      textStyle: {
        color: dashboardChartTokens.tooltipText
      },
      formatter: (params: Array<{ axisValueLabel: string; seriesName: string; value: number }>) => {
        const lines = params.map((item) => item.seriesName === 'Charge intent'
          ? `${item.seriesName}: ${formatSignedMw(item.value)}`
          : `${item.seriesName}: ${formatCurrency(item.value)}`)

        return [params[0]?.axisValueLabel || '', ...lines].join('<br/>')
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
      data: signal.labels,
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
          color: dashboardChartTokens.warning,
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
        name: 'Charge intent',
        data: signal.charge_intent,
        barWidth: 18,
        itemStyle: {
          color: dashboardChartTokens.secondary,
          borderRadius: [10, 10, 0, 0]
        }
      },
      {
        type: 'line',
        name: 'Regret',
        yAxisIndex: 1,
        smooth: true,
        data: signal.regret,
        symbol: 'circle',
        symbolSize: 8,
        lineStyle: {
          width: 3,
          color: dashboardChartTokens.warning
        },
        itemStyle: {
          color: dashboardChartTokens.warning
        }
      }
    ]
  }
}

export const buildBaselineForecastChartOption = (
  baselinePreview: BaselineLpPreview | null
): EChartsOption => {
  const labels = baselinePreview?.forecast.map((point) => point.forecast_timestamp.slice(11, 16)) || []
  const prices = baselinePreview?.forecast.map((point) => point.predicted_price_uah_mwh) || []

  return {
    animationDuration: 1200,
    animationEasing: 'elasticOut',
    backgroundColor: 'transparent',
    tooltip: {
      trigger: 'axis',
      backgroundColor: dashboardChartTokens.tooltipBackground,
      borderWidth: 2,
      borderColor: 'rgba(255, 255, 255, 0.96)',
      textStyle: {
        color: dashboardChartTokens.tooltipText
      },
      valueFormatter: (value: number | string) => `${Math.round(Number(value))} UAH/MWh`
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
      axisLabel: {
        color: dashboardChartTokens.axis,
        fontWeight: 700,
        formatter: (value: number) => `${Math.round(value)} UAH`
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
          color: 'rgba(0, 121, 193, 0.12)'
        }
      }
    ]
  }
}

export const buildBaselineScheduleChartOption = (
  baselinePreview: BaselineLpPreview | null
): EChartsOption => {
  const labels = baselinePreview?.recommendation_schedule.map((point) => point.interval_start.slice(11, 16)) || []
  const netPower = baselinePreview?.recommendation_schedule.map((point) => point.recommended_net_power_mw) || []
  const soc = baselinePreview?.projected_state.trace.map((point) => Number((point.soc_after_fraction * 100).toFixed(1))) || []

  return {
    animationDuration: 1200,
    animationEasing: 'elasticOut',
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
      borderColor: 'rgba(255, 255, 255, 0.96)',
      textStyle: {
        color: dashboardChartTokens.tooltipText
      },
      formatter: (params: Array<{ axisValueLabel: string; seriesName: string; value: number }>) => {
        const lines = params.map((item) => item.seriesName === 'Projected SOC'
          ? `${item.seriesName}: ${Math.round(item.value)}%`
          : `${item.seriesName}: ${formatSignedMw(item.value)}`)

        return [params[0]?.axisValueLabel || '', ...lines].join('<br/>')
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