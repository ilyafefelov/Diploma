import type { EChartsOption } from 'echarts'

import type { TenantSummary } from '~/types/control-plane'

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

type DemoSignalSeries = {
  labels: string[]
  market: number[]
  weatherBias: number[]
  dispatch: number[]
  regret: number[]
}

export const dashboardChartTokens = {
  grid: 'rgba(0, 121, 193, 0.12)',
  axis: '#6c7c92',
  primary: '#0079c1',
  secondary: '#53b2ea',
  highlight: '#7ed321',
  warning: '#f5a623',
  tooltipBackground: 'rgba(255, 255, 255, 0.96)',
  tooltipText: '#1b3551',
  shadow: 'rgba(0, 121, 193, 0.16)'
} as const

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

const resolveReferenceTenant = (tenants: TenantSummary[], selectedTenantId: string): TenantSummary | null => {
  return tenants.find((tenant) => tenant.tenant_id === selectedTenantId) || tenants[0] || null
}

const buildDemoSignalSeries = (tenants: TenantSummary[], selectedTenantId: string): DemoSignalSeries => {
  const referenceTenant = resolveReferenceTenant(tenants, selectedTenantId)
  const labels = ['06:00', '09:00', '12:00', '15:00', '18:00', '21:00']

  if (!referenceTenant) {
    return {
      labels,
      market: [92, 101, 118, 126, 114, 97],
      weatherBias: [4, 6, 9, 7, 5, 3],
      dispatch: [32, 44, 58, 52, 40, 28],
      regret: [9, 8, 7, 6, 7, 8]
    }
  }

  const latitudeBias = Math.round((referenceTenant.latitude - 45) * 2)
  const longitudeBias = Math.round((referenceTenant.longitude - 22) * 1.5)
  const tenantLoadBias = tenants.length * 3

  return {
    labels,
    market: [84, 96, 113, 124, 117, 101].map((value, index) => value + latitudeBias + index),
    weatherBias: [3, 5, 8, 7, 5, 4].map((value, index) => value + Math.max(0, longitudeBias - index)),
    dispatch: [28, 42, 57, 54, 39, 26].map((value, index) => value + Math.round(tenantLoadBias / 4) - index),
    regret: [10, 9, 8, 7, 8, 9].map((value, index) => Math.max(3, value + Math.round(longitudeBias / 2) - index))
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
  tenants: TenantSummary[],
  selectedTenantId: string
): EChartsOption => {
  const signal = buildDemoSignalSeries(tenants, selectedTenantId)

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
    yAxis: {
      type: 'value',
      axisLabel: {
        color: dashboardChartTokens.axis,
        fontWeight: 700
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
        data: signal.market,
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
        data: signal.weatherBias,
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
  tenants: TenantSummary[],
  selectedTenantId: string
): EChartsOption => {
  const signal = buildDemoSignalSeries(tenants, selectedTenantId)

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
        axisLabel: {
          color: dashboardChartTokens.axis,
          fontWeight: 700
        },
        splitLine: {
          lineStyle: {
            color: dashboardChartTokens.grid
          }
        }
      },
      {
        type: 'value',
        axisLabel: {
          color: dashboardChartTokens.axis,
          fontWeight: 700
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
        data: signal.dispatch,
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