import { computed, type Ref } from 'vue'

import type { BaselineLpPreview, OperatorStatus, SignalPreview, TenantSummary } from '~/types/control-plane'
import type {
  OperatorGatekeeperAction,
  OperatorGatekeeperActionLabel,
  OperatorHeadlineMetric,
  OperatorMarketRegimeChip,
  OperatorMoodChip,
  OperatorMotiveItem,
  OperatorNavItem,
  OperatorTimelineSegment,
  OperatorWeatherMaterializeResult,
  OperatorWeatherRunConfig
} from '~/types/operator-dashboard'

interface OperatorDashboardViewModelInput {
  tenants: Readonly<Ref<TenantSummary[]>>
  selectedTenant: Readonly<Ref<TenantSummary | null>>
  signalPreview: Readonly<Ref<SignalPreview | null>>
  baselinePreview: Readonly<Ref<BaselineLpPreview | null>>
  runConfig: Readonly<Ref<OperatorWeatherRunConfig | null>>
  materializeResult: Readonly<Ref<OperatorWeatherMaterializeResult | null>>
  operatorStatus: Readonly<Ref<OperatorStatus | null>>
  registryError: Readonly<Ref<string>>
  weatherError: Readonly<Ref<string>>
  signalPreviewError: Readonly<Ref<string>>
  baselinePreviewError: Readonly<Ref<string>>
  signalPreviewLastLoadedLabel: Readonly<Ref<string>>
  registryLastLoadedAt: Readonly<Ref<number | null>>
  isMaterializing: Readonly<Ref<boolean>>
}

export const useOperatorDashboardViewModel = (input: OperatorDashboardViewModelInput) => {
  const operatorNavItems = computed<OperatorNavItem[]>(() => [
    { label: 'Overview', icon: 'i-lucide-house', active: true },
    { label: 'Market', icon: 'i-lucide-chart-no-axes-combined', active: false },
    { label: 'Battery', icon: 'i-lucide-battery-charging', active: false },
    { label: 'Gatekeeper', icon: 'i-lucide-shield-check', active: false },
    { label: 'Baseline', icon: 'i-lucide-chart-column-big', active: false },
    { label: 'Reports', icon: 'i-lucide-file-text', active: false }
  ])

  const selectedTenantName = computed(() => {
    if (!input.selectedTenant.value) {
      return 'No tenant selected'
    }

    return input.selectedTenant.value.name || input.selectedTenant.value.tenant_id
  })

  const selectedTenantBadge = computed(() => {
    if (!input.selectedTenant.value) {
      return 'No active lot'
    }

    return `${input.selectedTenant.value.type || 'unspecified'} lot`
  })

  const criticalTenantCount = computed(() => {
    return input.tenants.value.filter(tenant => tenant.type === 'critical').length
  })

  const registryEnvelope = computed(() => {
    if (input.tenants.value.length === 0) {
      return 'Registry envelope unavailable'
    }

    const latitudes = input.tenants.value.map(tenant => tenant.latitude)
    const longitudes = input.tenants.value.map(tenant => tenant.longitude)
    const latitudeSpan = Math.max(...latitudes) - Math.min(...latitudes)
    const longitudeSpan = Math.max(...longitudes) - Math.min(...longitudes)

    return `${latitudeSpan.toFixed(2)} lat / ${longitudeSpan.toFixed(2)} lon span`
  })

  const activeRegistrySummary = computed(() => {
    if (input.tenants.value.length === 0) {
      return 'Registry offline'
    }

    return `${input.tenants.value.length} live tenants / ${criticalTenantCount.value} critical`
  })

  const activeAlertCount = computed(() => {
    return [
      input.registryError.value,
      input.weatherError.value,
      input.signalPreviewError.value,
      input.baselinePreviewError.value
    ].filter(Boolean).length
  })

  const weatherBiasAverage = computed(() => {
    const values = input.signalPreview.value?.weather_bias || []
    if (values.length === 0) {
      return 0
    }

    return values.reduce((total, value) => total + value, 0) / values.length
  })

  const latestRecommendedPowerMw = computed(() => {
    const baselinePoint = input.baselinePreview.value?.recommendation_schedule?.[0]
    if (baselinePoint) {
      return baselinePoint.recommended_net_power_mw
    }

    return input.signalPreview.value?.charge_intent?.[0] ?? 0
  })

  const batterySocPercent = computed(() => {
    const baselineSoc = input.baselinePreview.value?.starting_soc_fraction
    if (typeof baselineSoc === 'number') {
      return Math.round(baselineSoc * 100)
    }

    return 62
  })

  const batterySohProxyPercent = computed(() => {
    const throughput = input.baselinePreview.value?.economics.total_throughput_mwh ?? 0
    return Math.max(91, Number((96.2 - throughput * 0.12).toFixed(1)))
  })

  const availabilityPercent = computed(() => {
    if (activeAlertCount.value > 0) {
      return 92.4
    }

    if (input.operatorStatus.value?.status === 'completed') {
      return 99.1
    }

    return input.tenants.value.length > 0 ? 98.7 : 0
  })

  const equivalentCyclePreview = computed(() => {
    const metrics = input.baselinePreview.value?.battery_metrics
    const throughput = input.baselinePreview.value?.economics.total_throughput_mwh

    if (!metrics || typeof throughput !== 'number' || metrics.capacity_mwh === 0) {
      return 'Waiting'
    }

    return `${(throughput / (metrics.capacity_mwh * 2)).toFixed(2)} EFC`
  })

  const headlineMetrics = computed<OperatorHeadlineMetric[]>(() => [
    {
      label: 'Net plan value',
      value: input.baselinePreview.value ? formatUah(input.baselinePreview.value.economics.total_net_value_uah) : 'Waiting',
      meta: 'Baseline LP preview',
      icon: 'i-lucide-wallet-cards',
      tone: 'green',
      tooltipTitle: 'Net plan value',
      tooltipBody: 'Operator-facing value after the baseline LP preview subtracts battery degradation from gross market revenue.',
      tooltipFormula: 'net_value = gross_market_value - degradation_penalty'
    },
    {
      label: 'Energy arbitrage',
      value: input.baselinePreview.value ? formatUah(input.baselinePreview.value.economics.total_gross_market_value_uah) : 'Waiting',
      meta: 'Gross market value',
      icon: 'i-lucide-zap',
      tone: 'blue',
      tooltipTitle: 'Energy arbitrage',
      tooltipBody: 'Projected gross value from moving battery energy through the visible price spread before degradation cost is applied.',
      tooltipFormula: 'sum(hourly_dispatch_value) across the LP horizon'
    },
    {
      label: 'Weather uplift',
      value: `${weatherBiasAverage.value > 0 ? '+' : ''}${weatherBiasAverage.value.toFixed(1)} UAH/MWh`,
      meta: input.signalPreviewLastLoadedLabel.value,
      icon: 'i-lucide-cloud-sun',
      tone: 'mint',
      tooltipTitle: 'Weather uplift',
      tooltipBody: 'Average calibrated weather effect applied to the MVP market forecast for the selected location.',
      tooltipFormula: 'weather_bias = f(clouds, rain, humidity, temperature, solar, wind)'
    },
    {
      label: 'Cycle preview',
      value: equivalentCyclePreview.value,
      meta: 'Throughput-aware',
      icon: 'i-lucide-refresh-cw',
      tone: 'lime',
      tooltipTitle: 'Equivalent full cycles',
      tooltipBody: 'A quick wear proxy showing how much of a full charge-discharge cycle the preview schedule consumes.',
      tooltipFormula: 'EFC = throughput_mwh / (capacity_mwh * 2)'
    },
    {
      label: 'Availability',
      value: `${availabilityPercent.value.toFixed(1)}%`,
      meta: activeAlertCount.value === 0 ? 'System normal' : `${activeAlertCount.value} alert(s)`,
      icon: 'i-lucide-radio-tower',
      tone: activeAlertCount.value === 0 ? 'green' : 'orange',
      tooltipTitle: 'Operator availability',
      tooltipBody: 'A display health signal that combines registry state, materialization status, and visible alert count.',
      tooltipFormula: 'availability = dashboard_health - active_alert_penalty'
    }
  ])

  const moodChips = computed<OperatorMoodChip[]>(() => [
    {
      label: 'Prices',
      value: input.signalPreview.value ? 'Favorable' : 'Waiting',
      tone: input.signalPreview.value ? 'green' : 'blue'
    },
    {
      label: 'Spread',
      value: input.baselinePreview.value && input.baselinePreview.value.economics.total_net_value_uah > 0 ? 'Strong' : 'Learning',
      tone: 'green'
    },
    {
      label: 'Volatility',
      value: Math.abs(weatherBiasAverage.value) > 15 ? 'High' : 'Moderate',
      tone: Math.abs(weatherBiasAverage.value) > 15 ? 'orange' : 'blue'
    },
    {
      label: 'Demand',
      value: criticalTenantCount.value > 0 ? 'Healthy' : 'Quiet',
      tone: 'green'
    },
    {
      label: 'Weather',
      value: input.runConfig.value || input.materializeResult.value ? 'Good' : 'Staging',
      tone: 'mint'
    }
  ])

  const marketRegimeChips = computed<OperatorMarketRegimeChip[]>(() => [
    {
      label: 'Normal',
      icon: 'i-lucide-sun',
      active: activeAlertCount.value === 0,
      tooltipTitle: 'Normal regime',
      tooltipBody: 'No visible operator errors are active, so the preview can be read as a normal market-watch state.'
    },
    {
      label: 'Low vol',
      icon: 'i-lucide-cloud',
      active: Math.abs(weatherBiasAverage.value) < 8,
      tooltipTitle: 'Low volatility',
      tooltipBody: 'Weather uplift is small enough that the current preview treats the price path as calmer.'
    },
    {
      label: 'High vol',
      icon: 'i-lucide-activity',
      active: Math.abs(weatherBiasAverage.value) >= 8,
      tooltipTitle: 'High volatility',
      tooltipBody: 'Weather uplift is large enough to mark the current window as more sensitive for operator review.'
    },
    {
      label: 'Recovery',
      icon: 'i-lucide-trending-up',
      active: input.baselinePreview.value?.economics.total_net_value_uah
        ? input.baselinePreview.value.economics.total_net_value_uah > 0
        : false,
      tooltipTitle: 'Recovery window',
      tooltipBody: 'The LP preview is net-positive after degradation cost, so the screen flags this as a useful arbitrage recovery surface.'
    }
  ])

  const preferredGatekeeperAction = computed<OperatorGatekeeperActionLabel>(() => {
    if (latestRecommendedPowerMw.value > 1) {
      return 'SELL'
    }

    if (latestRecommendedPowerMw.value < -1) {
      return 'BUY'
    }

    return 'HOLD'
  })

  const gatekeeperActions = computed<OperatorGatekeeperAction[]>(() => [
    {
      label: 'BUY',
      score: preferredGatekeeperAction.value === 'BUY' ? 87 : 32,
      icon: 'i-lucide-download',
      active: preferredGatekeeperAction.value === 'BUY',
      tooltipTitle: 'Buy score',
      tooltipBody: 'Higher BUY means the preview sees more value in charging now and reserving energy for a later price window.',
      tooltipFormula: 'score = 50 + charge_bias * 35 - guardrail_penalty; charge_bias comes from negative recommended_net_power_mw'
    },
    {
      label: 'SELL',
      score: preferredGatekeeperAction.value === 'SELL' ? 87 : 38,
      icon: 'i-lucide-upload',
      active: preferredGatekeeperAction.value === 'SELL',
      tooltipTitle: 'Sell score',
      tooltipBody: 'Higher SELL means the preview expects discharge value now, then the gatekeeper still checks SOC and power limits.',
      tooltipFormula: 'score = 50 + discharge_bias * 35 - guardrail_penalty; discharge_bias comes from positive recommended_net_power_mw'
    },
    {
      label: 'HOLD',
      score: preferredGatekeeperAction.value === 'HOLD' ? 82 : 41,
      icon: 'i-lucide-pause',
      active: preferredGatekeeperAction.value === 'HOLD',
      tooltipTitle: 'Hold score',
      tooltipBody: 'Higher HOLD means the current spread is weak or the safer operator action is to wait for a cleaner interval.',
      tooltipFormula: 'score = 50 + idle_bias * 32 + uncertainty_penalty; idle_bias rises when recommended_net_power_mw is near zero'
    }
  ])

  const timelineSegments = computed<OperatorTimelineSegment[]>(() => {
    const schedule = input.baselinePreview.value?.recommendation_schedule?.slice(0, 5) || []

    if (schedule.length === 0) {
      return [
        {
          time: '00:00',
          label: 'Charge',
          value: '-60 MW',
          tone: 'green',
          tooltipTitle: 'Charge window',
          tooltipBody: 'Fallback preview slot for importing energy while the market is expected to be cheaper.'
        },
        {
          time: '06:00',
          label: 'Hold',
          value: '0 MW',
          tone: 'blue',
          tooltipTitle: 'Hold window',
          tooltipBody: 'Fallback preview slot where the battery waits because the value spread is not strong enough.'
        },
        {
          time: '12:00',
          label: 'Discharge',
          value: '+80 MW',
          tone: 'green',
          tooltipTitle: 'Discharge window',
          tooltipBody: 'Fallback preview slot for exporting energy when the expected price window is stronger.'
        },
        {
          time: '18:00',
          label: 'Hold',
          value: '0 MW',
          tone: 'blue',
          tooltipTitle: 'Hold window',
          tooltipBody: 'Fallback preview slot that protects battery wear while the system waits for a better spread.'
        }
      ]
    }

    return schedule.map((point) => {
      const label = powerToTimelineLabel(point.recommended_net_power_mw)

      return {
        time: point.interval_start.slice(11, 16),
        label,
        value: formatSignedMw(point.recommended_net_power_mw),
        tone: label === 'Hold' ? 'blue' : 'green',
        tooltipTitle: `${label} at ${point.interval_start.slice(11, 16)}`,
        tooltipBody: timelineTooltipBody(label, point.recommended_net_power_mw)
      }
    })
  })

  const operatorClockLabel = computed(() => {
    const timestamp = input.operatorStatus.value?.updated_at || input.registryLastLoadedAt.value
    if (!timestamp) {
      return 'Clock syncing'
    }

    return new Date(timestamp).toLocaleString('en-GB', {
      month: 'short',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit'
    })
  })

  const dispatchModeLabel = computed(() => input.isMaterializing.value ? 'Running' : 'Auto')

  const batteryStatusLabel = computed(() => latestRecommendedPowerMw.value > 1
    ? 'Discharging'
    : latestRecommendedPowerMw.value < -1
      ? 'Charging'
      : 'Holding'
  )

  const latestRecommendedPowerLabel = computed(() => formatSignedMw(latestRecommendedPowerMw.value))

  const motiveItems = computed<OperatorMotiveItem[]>(() => {
    const tenantCount = input.tenants.value.length
    const coverage = Math.min(100, 46 + tenantCount * 9)
    const readiness = Math.min(100, 52 + criticalTenantCount.value * 7 + (input.operatorStatus.value?.status === 'prepared' ? 12 : 0))
    const pressure = Math.min(100, 34 + tenantCount * 5 + (input.operatorStatus.value?.status === 'completed' ? 10 : 0))

    return [
      {
        label: 'Registry health',
        value: coverage,
        tone: 'blue',
        hint: `${tenantCount || 0} lots mapped into the operator shell.`
      },
      {
        label: 'Weather readiness',
        value: readiness,
        tone: 'green',
        hint: input.runConfig.value
          ? `Run config staged for ${input.runConfig.value.tenant_id}.`
          : 'Prepare a run config to stage the weather slice.'
      },
      {
        label: 'Grid pressure',
        value: pressure,
        tone: 'orange',
        hint: input.materializeResult.value?.success
          ? `Assets fired: ${input.materializeResult.value.selected_assets.join(', ')}.`
          : 'Preview signal only until materialization succeeds.'
      }
    ]
  })

  const selectedRunConfigSnippet = computed(() => {
    if (!input.runConfig.value) {
      return 'Run config not prepared yet.'
    }

    return JSON.stringify(input.runConfig.value.run_config, null, 2)
  })

  const weatherLocationLabel = computed(() => {
    const location = input.materializeResult.value?.resolved_location || input.runConfig.value?.resolved_location

    if (!location) {
      return 'No location prepared'
    }

    return `${location.latitude.toFixed(2)} / ${location.longitude.toFixed(2)} / ${location.timezone}`
  })

  return {
    activeAlertCount,
    activeRegistrySummary,
    batterySocPercent,
    batterySohProxyPercent,
    batteryStatusLabel,
    dispatchModeLabel,
    gatekeeperActions,
    headlineMetrics,
    latestRecommendedPowerLabel,
    latestRecommendedPowerMw,
    marketRegimeChips,
    moodChips,
    motiveItems,
    operatorClockLabel,
    operatorNavItems,
    registryEnvelope,
    selectedRunConfigSnippet,
    selectedTenantBadge,
    selectedTenantName,
    timelineSegments,
    weatherLocationLabel
  }
}

const formatUah = (value: number): string => `${Math.round(value).toLocaleString('en-GB')} UAH`

const formatSignedMw = (value: number): string => `${value > 0 ? '+' : ''}${value.toFixed(1)} MW`

const powerToTimelineLabel = (powerMw: number): OperatorTimelineSegment['label'] => {
  if (powerMw > 1) {
    return 'Discharge'
  }

  if (powerMw < -1) {
    return 'Charge'
  }

  return 'Hold'
}

const timelineTooltipBody = (label: OperatorTimelineSegment['label'], powerMw: number): string => {
  if (label === 'Charge') {
    return `Recommended net power is ${formatSignedMw(powerMw)}, so the baseline preview is filling the battery for a later market window.`
  }

  if (label === 'Discharge') {
    return `Recommended net power is ${formatSignedMw(powerMw)}, so the baseline preview is selling stored energy into this interval.`
  }

  return `Recommended net power is ${formatSignedMw(powerMw)}, so the preview keeps the battery idle and avoids unnecessary cycling.`
}
