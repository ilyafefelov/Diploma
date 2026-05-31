import { computed, type Ref } from 'vue'

import type {
  BaselineLpPreview,
  DashboardBatteryStateResponse,
  OperatorRecommendationResponse,
  OperatorStatus,
  SignalPreview,
  TenantSummary
} from '~/types/control-plane'
import type {
  OperatorNavItem,
  OperatorMarketVenue,
  OperatorWeatherMaterializeResult,
  OperatorWeatherRunConfig
} from '~/types/operator-dashboard'
import {
  buildOperatorHeadlineMetrics,
  buildOperatorMarketRegimeChips,
  buildOperatorMoodChips,
  buildOperatorMotiveItems
} from '../lib/operator-dashboard/operatorDashboardSignalModel'
import { useOperatorTimelineModel } from '../lib/operator-dashboard/useOperatorTimelineModel'
import { buildOperatorBatteryDisplay } from '../utils/operatorBatteryDisplay'

interface OperatorDashboardViewModelInput {
  tenants: Readonly<Ref<TenantSummary[]>>
  selectedTenant: Readonly<Ref<TenantSummary | null>>
  signalPreview: Readonly<Ref<SignalPreview | null>>
  baselinePreview: Readonly<Ref<BaselineLpPreview | null>>
  operatorRecommendation?: Readonly<Ref<OperatorRecommendationResponse | null>>
  selectedMarketVenue?: Readonly<Ref<OperatorMarketVenue>>
  batteryState?: Readonly<Ref<DashboardBatteryStateResponse | null>>
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
  readModelErrorCount?: Readonly<Ref<number>>
}

export const useOperatorDashboardViewModel = (input: OperatorDashboardViewModelInput) => {
  const operatorNavItems = computed<OperatorNavItem[]>(() => [
    { label: 'Overview', icon: 'i-lucide-house', active: true, targetId: 'operator-overview' },
    { label: 'Market', icon: 'i-lucide-chart-no-axes-combined', active: false, targetId: 'operator-market' },
    { label: 'Battery', icon: 'i-lucide-battery-charging', active: false, targetId: 'operator-battery' },
    { label: 'Gatekeeper', icon: 'i-lucide-shield-check', active: false, targetId: 'operator-gatekeeper' },
    { label: 'Baseline', icon: 'i-lucide-chart-column-big', active: false, targetId: 'operator-baseline' },
    { label: 'Evidence', icon: 'i-lucide-file-text', active: false, targetId: 'operator-research' }
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

  const surfaceAlertCount = computed(() => {
    return [
      input.registryError.value,
      input.weatherError.value,
      input.signalPreviewError.value,
      input.baselinePreviewError.value
    ].filter(Boolean).length
  })

  const readModelErrorCount = computed(() => Math.max(0, input.readModelErrorCount?.value ?? 0))
  const activeAlertCount = computed(() => surfaceAlertCount.value + readModelErrorCount.value)

  const weatherBiasAverage = computed(() => {
    const values = input.signalPreview.value?.weather_bias || []
    if (values.length === 0) {
      return 0
    }

    return values.reduce((total, value) => total + value, 0) / values.length
  })

  const {
    activeEconomics,
    batteryStatusLabel,
    deliveryWindowLabel,
    gatekeeperActions,
    latestRecommendedPowerLabel,
    latestRecommendedPowerMw,
    timelineSegments
  } = useOperatorTimelineModel({
    signalPreview: input.signalPreview,
    baselinePreview: input.baselinePreview,
    operatorRecommendation: input.operatorRecommendation,
    selectedMarketVenue: input.selectedMarketVenue
  })

  const operatorBatteryDisplay = computed(() => buildOperatorBatteryDisplay({
    batteryState: input.batteryState?.value ?? null,
    baselinePreview: input.baselinePreview.value
  }))

  const batterySocPercent = computed(() => operatorBatteryDisplay.value.socPercent)
  const batterySohProxyPercent = computed(() => operatorBatteryDisplay.value.sohPercent)
  const batterySocSourceLabel = computed(() => operatorBatteryDisplay.value.socSourceLabel)
  const batterySohSourceLabel = computed(() => operatorBatteryDisplay.value.sohSourceLabel)
  const batterySocFormula = computed(() => operatorBatteryDisplay.value.socFormula)
  const batterySohFormula = computed(() => operatorBatteryDisplay.value.sohFormula)
  const batteryTelemetryIngestLabel = computed(() => operatorBatteryDisplay.value.telemetryIngestLabel)
  const batteryTelemetryIngestTooltip = computed(() => operatorBatteryDisplay.value.telemetryIngestTooltip)

  const availabilityPercent = computed(() => {
    if (activeAlertCount.value > 0) {
      return 92.4
    }

    if (input.operatorStatus.value?.status === 'completed') {
      return 99.1
    }

    return input.tenants.value.length > 0 ? 98.7 : 0
  })

  const readModelHealthMeta = computed(() => {
    if (readModelErrorCount.value > 0) {
      return `${readModelErrorCount.value} read-model gap(s)`
    }

    if (surfaceAlertCount.value > 0) {
      return `${surfaceAlertCount.value} surface alert(s)`
    }

    return 'Preview sources loaded'
  })

  const equivalentCyclePreview = computed(() => {
    const metrics = input.baselinePreview.value?.battery_metrics
    const throughput = activeEconomics.value?.total_throughput_mwh

    if (!metrics || typeof throughput !== 'number' || metrics.capacity_mwh === 0) {
      return 'Waiting'
    }

    return `${(throughput / (metrics.capacity_mwh * 2)).toFixed(2)} EFC`
  })

  const batteryAssetLabel = computed(() => {
    const metrics = input.baselinePreview.value?.battery_metrics
    if (!metrics) {
      return 'Battery size loading'
    }

    return `${formatBatteryMetric(metrics.capacity_mwh)} MWh / ${formatBatteryMetric(metrics.max_power_mw)} MW max`
  })

  const batteryCapacityContextLabel = computed(() => {
    const metrics = input.baselinePreview.value?.battery_metrics
    if (!metrics) {
      return 'Capacity pending'
    }

    return `Battery: ${formatBatteryMetric(metrics.capacity_mwh)} MWh usable preview / ${formatBatteryMetric(metrics.max_power_mw)} MW max. For 1h DAM rows, MW is approximately MWh.`
  })

  const operatorHeadlineMetrics = computed(() => buildOperatorHeadlineMetrics({
    activeEconomics: activeEconomics.value,
    selectedStrategyId: input.operatorRecommendation?.value?.selected_strategy_id ?? null,
    weatherBiasAverage: weatherBiasAverage.value,
    signalPreviewLastLoadedLabel: input.signalPreviewLastLoadedLabel.value,
    equivalentCyclePreview: equivalentCyclePreview.value,
    availabilityPercent: availabilityPercent.value,
    readModelHealthMeta: readModelHealthMeta.value,
    activeAlertCount: activeAlertCount.value
  }))

  const operatorMoodChips = computed(() => buildOperatorMoodChips({
    activeAlertCount: activeAlertCount.value,
    activeEconomics: activeEconomics.value,
    weatherBiasAverage: weatherBiasAverage.value,
    criticalTenantCount: criticalTenantCount.value,
    hasPreparedWeatherData: Boolean(input.runConfig.value || input.materializeResult.value)
  }))

  const operatorMarketRegimeChips = computed(() => buildOperatorMarketRegimeChips({
    activeAlertCount: activeAlertCount.value,
    weatherBiasAverage: weatherBiasAverage.value,
    netValueUah: activeEconomics.value?.total_net_value_uah ?? null
  }))

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

  const dispatchModeLabel = computed(() => input.isMaterializing.value ? 'Refreshing preview' : 'Preview only')

  const operatorMotiveItems = computed(() => buildOperatorMotiveItems({
    tenantCount: input.tenants.value.length,
    criticalTenantCount: criticalTenantCount.value,
    operatorStatus: input.operatorStatus.value,
    runConfig: input.runConfig.value,
    materializeResult: input.materializeResult.value
  }))

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
    batteryAssetLabel,
    batteryCapacityContextLabel,
    batterySocFormula,
    batterySocPercent,
    batterySocSourceLabel,
    batterySohFormula,
    batterySohProxyPercent,
    batterySohSourceLabel,
    batteryStatusLabel,
    batteryTelemetryIngestLabel,
    batteryTelemetryIngestTooltip,
    dispatchModeLabel,
    deliveryWindowLabel,
    gatekeeperActions,
    headlineMetrics: operatorHeadlineMetrics,
    latestRecommendedPowerLabel,
    latestRecommendedPowerMw,
    marketRegimeChips: operatorMarketRegimeChips,
    moodChips: operatorMoodChips,
    motiveItems: operatorMotiveItems,
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

const formatBatteryMetric = (value: number): string => value.toLocaleString('en-GB', {
  maximumFractionDigits: 2,
  minimumFractionDigits: value < 10 ? 2 : 0
})
