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
import { buildOperatorBatteryDisplay } from '../utils/operatorBatteryDisplay'
import {
  DAM_REVIEW_ACTION_THRESHOLD_MW,
  formatDamDeliveryLabel,
  formatSignedMw,
  powerToTimelineLabel,
  timelineTooltipBody
} from '../utils/operatorTimeline'

interface OperatorDashboardViewModelInput {
  tenants: Readonly<Ref<TenantSummary[]>>
  selectedTenant: Readonly<Ref<TenantSummary | null>>
  signalPreview: Readonly<Ref<SignalPreview | null>>
  baselinePreview: Readonly<Ref<BaselineLpPreview | null>>
  operatorRecommendation?: Readonly<Ref<OperatorRecommendationResponse | null>>
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

const TIMELINE_SEGMENT_LIMIT = 5

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

  const activeRecommendationSchedule = computed(() => {
    const selectedSchedule = input.operatorRecommendation?.value?.recommendation_schedule ?? []
    if (selectedSchedule.length > 0) {
      return selectedSchedule
    }

    return input.baselinePreview.value?.recommendation_schedule ?? []
  })

  const activeEconomics = computed(() => input.operatorRecommendation?.value?.economics
    ?? input.baselinePreview.value?.economics
    ?? null)

  const selectedTimelineSchedulePoints = computed(() => selectTimelineSchedulePoints(activeRecommendationSchedule.value))

  const latestRecommendedPowerMw = computed(() => {
    const selectedPoint = selectedTimelineSchedulePoints.value[0]
    if (selectedPoint) {
      return selectedPoint.recommended_net_power_mw
    }

    return input.signalPreview.value?.charge_intent?.[0] ?? 0
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

  const headlineMetrics = computed<OperatorHeadlineMetric[]>(() => [
    {
      label: 'Net plan value',
      value: activeEconomics.value ? formatUah(activeEconomics.value.total_net_value_uah) : 'Waiting',
      meta: input.operatorRecommendation?.value?.selected_strategy_id || 'Baseline LP preview',
      icon: 'i-lucide-wallet-cards',
      tone: 'green',
      tooltipTitle: 'Net plan value',
      tooltipBody: 'Operator-facing value after the selected preview schedule subtracts battery degradation from gross market revenue.',
      tooltipFormula: 'net_value = gross_market_value - degradation_penalty'
    },
    {
      label: 'Energy arbitrage',
      value: activeEconomics.value ? formatUah(activeEconomics.value.total_gross_market_value_uah) : 'Waiting',
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
      label: 'Read-model health',
      value: `${availabilityPercent.value.toFixed(1)}%`,
      meta: readModelHealthMeta.value,
      icon: 'i-lucide-radio-tower',
      tone: activeAlertCount.value === 0 ? 'green' : 'orange',
      tooltipTitle: 'Read-model health',
      tooltipBody: 'A display health signal for required FastAPI read models and local operator surfaces. Gaps mean review-only evidence may be incomplete.',
      tooltipFormula: 'health = preview_sources_loaded - read_model_gap_penalty'
    }
  ])

  const moodChips = computed<OperatorMoodChip[]>(() => [
    {
      label: 'Read model',
      value: activeAlertCount.value > 0 ? 'Gaps' : 'Loaded',
      tone: activeAlertCount.value > 0 ? 'orange' : 'green'
    },
    {
      label: 'Value spread',
      value: activeEconomics.value && activeEconomics.value.total_net_value_uah > 0 ? 'Positive' : 'Learning',
      tone: 'green'
    },
    {
      label: 'DAM volatility',
      value: Math.abs(weatherBiasAverage.value) > 15 ? 'High' : 'Moderate',
      tone: Math.abs(weatherBiasAverage.value) > 15 ? 'orange' : 'blue'
    },
    {
      label: 'Tenant data',
      value: criticalTenantCount.value > 0 ? 'Critical lot' : 'Quiet',
      tone: 'green'
    },
    {
      label: 'Weather data',
      value: input.runConfig.value || input.materializeResult.value ? 'Prepared' : 'Staging',
      tone: 'mint'
    }
  ])

  const marketRegimeChips = computed<OperatorMarketRegimeChip[]>(() => [
    {
      label: 'Normal',
      icon: 'i-lucide-sun',
      active: activeAlertCount.value === 0,
      tooltipTitle: 'Normal regime',
      tooltipBody: 'No visible operator errors are active, so the DAM delivery-day preview can be reviewed as a normal market-watch state.'
    },
    {
      label: 'Low vol',
      icon: 'i-lucide-cloud',
      active: Math.abs(weatherBiasAverage.value) < 8,
      tooltipTitle: 'Low volatility',
      tooltipBody: 'Weather uplift is small enough that the selected DAM window is treated as calmer.'
    },
    {
      label: 'High vol',
      icon: 'i-lucide-activity',
      active: Math.abs(weatherBiasAverage.value) >= 8,
      tooltipTitle: 'High volatility',
      tooltipBody: 'Weather uplift is large enough to mark the selected DAM window as more sensitive for operator review.'
    },
    {
      label: 'Recovery',
      icon: 'i-lucide-trending-up',
      active: activeEconomics.value?.total_net_value_uah
        ? activeEconomics.value.total_net_value_uah > 0
        : false,
      tooltipTitle: 'Recovery window',
      tooltipBody: 'The LP preview is net-positive after degradation cost, so the screen flags this as a useful arbitrage recovery surface.'
    }
  ])

  const preferredGatekeeperAction = computed<OperatorGatekeeperActionLabel>(() => {
    const previewAction = powerToTimelineLabel(latestRecommendedPowerMw.value)

    if (previewAction === 'Discharge') {
      return 'SELL'
    }

    if (previewAction === 'Charge') {
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
      tooltipTitle: 'Charge preview score',
      tooltipBody: 'Higher BUY means the selected DAM delivery hour is a charging preview, reserving energy for a later price window.',
      tooltipFormula: 'score = 50 + charge_bias * 35 - guardrail_penalty; charge_bias comes from negative recommended_net_power_mw'
    },
    {
      label: 'SELL',
      score: preferredGatekeeperAction.value === 'SELL' ? 87 : 38,
      icon: 'i-lucide-upload',
      active: preferredGatekeeperAction.value === 'SELL',
      tooltipTitle: 'Discharge preview score',
      tooltipBody: 'Higher SELL means the selected DAM delivery hour is a discharge preview; future bid validation still checks SOC and power limits.',
      tooltipFormula: 'score = 50 + discharge_bias * 35 - guardrail_penalty; discharge_bias comes from positive recommended_net_power_mw'
    },
    {
      label: 'HOLD',
      score: preferredGatekeeperAction.value === 'HOLD' ? 82 : 41,
      icon: 'i-lucide-pause',
      active: preferredGatekeeperAction.value === 'HOLD',
      tooltipTitle: 'Hold preview score',
      tooltipBody: 'Higher HOLD means the selected DAM delivery-hour spread is weak or the safer review choice is to wait for a cleaner interval.',
      tooltipFormula: 'score = 50 + idle_bias * 32 + uncertainty_penalty; idle_bias rises when recommended_net_power_mw is near zero'
    }
  ])

  const timelineSegments = computed<OperatorTimelineSegment[]>(() => {
    const schedule = selectedTimelineSchedulePoints.value

    if (schedule.length === 0) {
      return [
        {
          time: 'DAM delivery',
          label: 'Preview pending',
          value: 'No schedule loaded',
          tone: 'blue',
          tooltipTitle: 'DAM delivery schedule pending',
          tooltipBody: 'No DAM delivery-hour schedule has loaded yet, so this dock is not showing a bid, ProposedBid, or market instruction.'
        }
      ]
    }

    return schedule.map((point) => {
      const label = powerToTimelineLabel(point.recommended_net_power_mw)

      return {
        time: formatDamDeliveryLabel(point.interval_start),
        label,
        value: formatSignedMw(point.recommended_net_power_mw),
        tone: label === 'Hold' ? 'blue' : 'green',
        tooltipTitle: `${label} for ${formatDamDeliveryLabel(point.interval_start)}`,
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

  const dispatchModeLabel = computed(() => input.isMaterializing.value ? 'Refreshing preview' : 'Preview only')

  const batteryStatusLabel = computed(() => {
    const action = powerToTimelineLabel(latestRecommendedPowerMw.value)

    if (action === 'Discharge') {
      return 'DAM discharge preview'
    }

    if (action === 'Charge') {
      return 'DAM charge preview'
    }

    return 'DAM hold preview'
  })

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

const selectTimelineSchedulePoints = <T extends { recommended_net_power_mw: number }>(schedule: T[]): T[] => {
  const actionPoints = schedule.filter(point => Math.abs(point.recommended_net_power_mw) >= DAM_REVIEW_ACTION_THRESHOLD_MW)
  if (actionPoints.length > 0) {
    return actionPoints.slice(0, TIMELINE_SEGMENT_LIMIT)
  }

  return schedule.slice(0, TIMELINE_SEGMENT_LIMIT)
}
