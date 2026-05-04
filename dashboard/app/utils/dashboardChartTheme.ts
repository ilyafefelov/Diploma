import type { EChartsOption, TooltipComponentFormatterCallbackParams } from 'echarts'

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

type TooltipPoint = {
  axisValueLabel: string
  dataIndex: number
  seriesName: string
  value: number
  data: unknown
}

export const dashboardChartTokens = {
  grid: 'rgba(0, 91, 149, 0.14)',
  axis: '#315c83',
  primary: '#0079c1',
  secondary: '#53b2ea',
  highlight: '#7ed321',
  warning: '#f5a623',
  rose: '#ff6fae',
  tooltipBackground: 'rgba(0, 91, 157, 0.96)',
  tooltipText: '#1b3551',
  shadow: 'rgba(0, 121, 193, 0.16)'
} as const

const formatSignedMw = (value: number): string => `${value > 0 ? '+' : ''}${value.toFixed(2)} MW`

const formatCurrency = (value: number): string => `${Math.round(value).toLocaleString('en-GB')} UAH`

const formatSignedCurrencyPerMwh = (value: number): string => `${value > 0 ? '+' : ''}${Math.round(value).toLocaleString('en-GB')} UAH/MWh`

const asRecord = (value: unknown): Record<string, unknown> | null => {
  if (typeof value !== 'object' || value === null) {
    return null
  }

  return value as Record<string, unknown>
}

const numberFromValue = (value: unknown): number => {
  if (typeof value === 'number') {
    return value
  }

  if (typeof value === 'string') {
    const numericValue = Number(value)
    return Number.isFinite(numericValue) ? numericValue : 0
  }

  if (Array.isArray(value)) {
    const numericValue = value.find((entry): entry is number => typeof entry === 'number')
    return numericValue ?? 0
  }

  return 0
}

const normalizeTooltipItems = (params: TooltipComponentFormatterCallbackParams): TooltipPoint[] => {
  const rawItems: unknown[] = Array.isArray(params) ? params : [params]

  return rawItems.map((rawItem) => {
    const item = asRecord(rawItem)

    return {
      axisValueLabel: typeof item?.axisValueLabel === 'string' ? item.axisValueLabel : '',
      dataIndex: typeof item?.dataIndex === 'number' ? item.dataIndex : 0,
      seriesName: typeof item?.seriesName === 'string' ? item.seriesName : '',
      value: numberFromValue(item?.value),
      data: item?.data
    }
  })
}

const isTenantScatterPoint = (value: unknown): value is TenantScatterPoint => {
  const point = asRecord(value)
  return point !== null && asRecord(point.tenant) !== null
}

export const formatWeatherSourceLabel = (source: string): string => {
  if (source === 'OPEN_METEO') {
    return 'Open-Meteo live'
  }

  if (source === 'SYNTHETIC') {
    return 'Synthetic fallback'
  }

  return source.replaceAll('_', ' ').toLowerCase().replace(/(^|\s)\w/g, letter => letter.toUpperCase())
}

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
  const points = tenants.map(tenant => createTenantPoint(tenant, selectedTenantId))
  const latitudes = tenants.map(tenant => tenant.latitude)
  const longitudes = tenants.map(tenant => tenant.longitude)
  const latitudeMin = latitudes.length > 0 ? Math.min(...latitudes) - 0.7 : 44
  const latitudeMax = latitudes.length > 0 ? Math.max(...latitudes) + 0.7 : 52
  const longitudeMin = longitudes.length > 0 ? Math.min(...longitudes) - 0.7 : 22
  const longitudeMax = longitudes.length > 0 ? Math.max(...longitudes) + 0.7 : 32

  return {
    animationDuration: 900,
    animationEasing: 'cubicOut',
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
        color: '#f0fbff'
      },
      formatter: (params: TooltipComponentFormatterCallbackParams) => {
        const tooltipItems = normalizeTooltipItems(params)
        const tooltipData = tooltipItems[0]?.data
        const tenant = isTenantScatterPoint(tooltipData) ? tooltipData.tenant : null

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
    weather_bias: [0, 0, 0, 0, 0, 0],
    weather_sources: ['SYNTHETIC', 'SYNTHETIC', 'SYNTHETIC', 'SYNTHETIC', 'SYNTHETIC', 'SYNTHETIC']
  }
  const adjustedMarketPrice = signal.market_price.map((price, index) => Number((price + (signal.weather_bias[index] || 0)).toFixed(2)))

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
      borderColor: 'rgba(255, 255, 255, 0.96)',
      textStyle: {
        color: '#f0fbff'
      },
      formatter: (params: TooltipComponentFormatterCallbackParams) => {
        const tooltipItems = normalizeTooltipItems(params)
        const price = tooltipItems.find(item => item.seriesName === 'Expected market price')?.value ?? 0
        const uplift = tooltipItems.find(item => item.seriesName === 'Weather effect')?.value ?? 0
        const adjusted = tooltipItems.find(item => item.seriesName === 'Price after weather')?.value ?? 0
        const dataIndex = tooltipItems[0]?.dataIndex ?? 0
        const weatherSource = formatWeatherSourceLabel(signal.weather_sources[dataIndex] || 'SYNTHETIC')

        return [
          tooltipItems[0]?.axisValueLabel || '',
          `Expected market price: ${Math.round(price)} UAH/MWh`,
          `Calibrated weather effect: ${formatSignedCurrencyPerMwh(uplift)}`,
          `Price after weather: ${Math.round(adjusted)} UAH/MWh`,
          `Weather source: ${weatherSource}`,
          'Formula: price_after_weather = market_price + weather_bias',
          'Weather inputs: cloud cover, precipitation, humidity excess, temperature gap, effective solar, wind speed',
          'Current MVP source mix: tenant-aware synthetic DAM history + live or synthetic weather'
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
          color: 'rgba(0, 121, 193, 0.12)'
        }
      },
      {
        type: 'bar',
        name: 'Weather effect',
        data: signal.weather_bias,
        barWidth: 14,
        itemStyle: {
          color: 'rgba(126, 211, 33, 0.68)',
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

export const buildDispatchBalanceChartOption = (
  signalPreview: SignalPreview | null
): EChartsOption => {
  const signal = signalPreview || {
    labels: ['06:00', '09:00', '12:00', '15:00', '18:00', '21:00'],
    charge_intent: [0, 0, 0, 0, 0, 0],
    regret: [0, 0, 0, 0, 0, 0]
  }

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
      borderColor: 'rgba(255, 255, 255, 0.96)',
      textStyle: {
        color: '#f0fbff'
      },
      formatter: (params: TooltipComponentFormatterCallbackParams) => {
        const tooltipItems = normalizeTooltipItems(params)
        const batteryAction = tooltipItems.find(item => item.seriesName === 'Battery action')?.value ?? 0
        const missedValue = tooltipItems.find(item => item.seriesName === 'Missed value')?.value ?? 0

        return [
          tooltipItems[0]?.axisValueLabel || '',
          `Battery action: ${formatSignedMw(batteryAction)}`,
          `Missed value: ${formatCurrency(missedValue)}`,
          'Battery action formula: clamp(((adjusted_price - avg_adjusted_price) / max_deviation) * max_power_mw)',
          'Sign meaning: positive MW = discharge bias, negative MW = charge bias',
          'Missed value formula: max(80, weather_bias * 2.4 + |adjusted_price - avg_adjusted_price| * 0.45)',
          'Missed value is a current MVP opportunity score, not settlement revenue and not the final DT/M3DT policy metric'
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
      borderColor: 'rgba(255, 255, 255, 0.96)',
      textStyle: {
        color: '#f0fbff'
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
          'Current MVP path: HourlyDamBaselineSolver over tenant-aware DAM history'
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
          color: 'rgba(0, 121, 193, 0.12)'
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
      borderColor: 'rgba(255, 255, 255, 0.96)',
      textStyle: {
        color: '#f0fbff'
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

export const buildMarketSignalHeroChartOption = (
  signalPreview: SignalPreview | null
): EChartsOption => {
  const signal = signalPreview || {
    labels: ['12:00', '16:00', '20:00', '00:00', '04:00', '08:00'],
    market_price: [3600, 4200, 3900, 2100, 2300, 3650],
    weather_bias: [120, 180, 150, 90, 110, 160],
    weather_sources: ['SYNTHETIC', 'SYNTHETIC', 'SYNTHETIC', 'SYNTHETIC', 'SYNTHETIC', 'SYNTHETIC']
  }
  const adjustedMarketPrice = signal.market_price.map((price, index) => Number((price + (signal.weather_bias[index] || 0)).toFixed(2)))
  const splitIndex = Math.max(1, Math.floor(signal.labels.length / 2))
  const damForecast = signal.market_price.map((price, index) => index < splitIndex ? null : Number((price * 0.94 + 210 + index * 24).toFixed(2)))
  const idmForecast = adjustedMarketPrice.map((price, index) => index < splitIndex ? null : Number((price * 0.95 + 165 + index * 18).toFixed(2)))

  return {
    animationDuration: 950,
    animationEasing: 'cubicOut',
    backgroundColor: 'transparent',
    color: ['#50f0ff', '#b8ff32', '#50f0ff', '#d7ff4f'],
    legend: {
      top: 2,
      left: 12,
      itemGap: 22,
      textStyle: {
        color: 'rgba(236, 250, 255, 0.88)',
        fontWeight: 800
      }
    },
    tooltip: {
      trigger: 'axis',
      backgroundColor: 'rgba(0, 50, 104, 0.98)',
      borderWidth: 2,
      borderColor: 'rgba(202, 249, 255, 0.92)',
      padding: [12, 14],
      textStyle: {
        color: '#f0fbff'
      },
      formatter: (params: TooltipComponentFormatterCallbackParams) => {
        const tooltipItems = normalizeTooltipItems(params)
        const dataIndex = tooltipItems[0]?.dataIndex ?? 0
        const weatherSource = formatWeatherSourceLabel(signal.weather_sources[dataIndex] || 'SYNTHETIC')

        return [
          `<strong>${tooltipItems[0]?.axisValueLabel || ''}</strong>`,
          `DAM LMP: ${Math.round(tooltipItems.find(item => item.seriesName === 'DAM LMP')?.value ?? 0)} UAH/MWh`,
          `IDM price: ${Math.round(tooltipItems.find(item => item.seriesName === 'IDM Price')?.value ?? 0)} UAH/MWh`,
          `Forecast DAM: ${Math.round(tooltipItems.find(item => item.seriesName === 'Forecast (DAM)')?.value ?? 0)} UAH/MWh`,
          `Forecast IDM: ${Math.round(tooltipItems.find(item => item.seriesName === 'Forecast (IDM)')?.value ?? 0)} UAH/MWh`,
          `Weather source: ${weatherSource}`,
          'MVP process: baseline DAM forecast + calibrated weather uplift, then displayed as DAM/IDM operator signal bands.'
        ].join('<br/>')
      }
    },
    grid: {
      left: 54,
      right: 18,
      top: 48,
      bottom: 38,
      containLabel: true
    },
    xAxis: {
      type: 'category',
      boundaryGap: false,
      data: signal.labels,
      axisLabel: {
        color: 'rgba(219, 245, 255, 0.9)',
        fontWeight: 800
      },
      axisLine: {
        lineStyle: {
          color: 'rgba(152, 224, 255, 0.32)'
        }
      },
      splitLine: {
        show: true,
        lineStyle: {
          color: 'rgba(152, 224, 255, 0.11)'
        }
      }
    },
    yAxis: {
      type: 'value',
      name: 'UAH/MWh',
      nameLocation: 'middle',
      nameGap: 42,
      axisLabel: {
        color: 'rgba(219, 245, 255, 0.9)',
        fontWeight: 800,
        formatter: (value: number) => `${Math.round(value)}`
      },
      nameTextStyle: {
        color: '#50f0ff',
        fontWeight: 900
      },
      splitLine: {
        lineStyle: {
          color: 'rgba(152, 224, 255, 0.13)'
        }
      }
    },
    series: [
      {
        type: 'line',
        name: 'DAM LMP',
        smooth: true,
        data: signal.market_price,
        symbol: 'circle',
        symbolSize: 8,
        lineStyle: {
          width: 4,
          color: '#50f0ff'
        },
        itemStyle: {
          color: '#50f0ff',
          borderColor: '#e6fbff',
          borderWidth: 2
        },
        areaStyle: {
          color: 'rgba(80, 240, 255, 0.1)'
        }
      },
      {
        type: 'line',
        name: 'IDM Price',
        smooth: true,
        data: adjustedMarketPrice,
        symbol: 'diamond',
        symbolSize: 8,
        lineStyle: {
          width: 4,
          color: '#b8ff32'
        },
        itemStyle: {
          color: '#b8ff32',
          borderColor: '#f4ffd0',
          borderWidth: 2
        },
        areaStyle: {
          color: 'rgba(126, 211, 33, 0.1)'
        }
      },
      {
        type: 'line',
        name: 'Forecast (DAM)',
        smooth: true,
        data: damForecast,
        symbol: 'none',
        connectNulls: false,
        lineStyle: {
          width: 3,
          type: 'dashed',
          color: '#50f0ff'
        }
      },
      {
        type: 'line',
        name: 'Forecast (IDM)',
        smooth: true,
        data: idmForecast,
        symbol: 'none',
        connectNulls: false,
        lineStyle: {
          width: 3,
          type: 'dashed',
          color: '#d7ff4f'
        }
      }
    ]
  }
}
