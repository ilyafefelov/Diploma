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