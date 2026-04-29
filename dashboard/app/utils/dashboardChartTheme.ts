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
  grid: 'rgba(21, 36, 61, 0.12)',
  axis: '#5f6a7d',
  primary: '#1c6872',
  highlight: '#d67e2c',
  tooltipBackground: 'rgba(20, 28, 51, 0.94)',
  tooltipText: '#f8fafc',
  shadow: 'rgba(20, 28, 51, 0.18)'
} as const

const createTenantPoint = (tenant: TenantSummary, selectedTenantId: string): TenantScatterPoint => {
  const isSelected = tenant.tenant_id === selectedTenantId

  return {
    value: [tenant.longitude, tenant.latitude],
    name: tenant.name || tenant.tenant_id,
    tenant,
    itemStyle: {
      color: isSelected ? dashboardChartTokens.highlight : dashboardChartTokens.primary,
      borderColor: '#fffdf9',
      borderWidth: isSelected ? 2 : 1,
      shadowBlur: isSelected ? 18 : 10,
      shadowColor: dashboardChartTokens.shadow
    },
    symbolSize: isSelected ? 24 : 16
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
    animationDuration: 500,
    grid: {
      left: 56,
      right: 24,
      top: 36,
      bottom: 56,
      containLabel: true
    },
    tooltip: {
      trigger: 'item',
      backgroundColor: dashboardChartTokens.tooltipBackground,
      borderWidth: 0,
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
      nameGap: 34,
      axisLabel: {
        color: dashboardChartTokens.axis
      },
      nameTextStyle: {
        color: dashboardChartTokens.axis,
        fontWeight: 600
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
        color: dashboardChartTokens.axis
      },
      nameTextStyle: {
        color: dashboardChartTokens.axis,
        fontWeight: 600
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
        emphasis: {
          scale: true,
          focus: 'series'
        }
      }
    ]
  }
}