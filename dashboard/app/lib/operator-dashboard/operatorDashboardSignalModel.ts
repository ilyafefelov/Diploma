import type {
  BaselinePreviewEconomics,
  OperatorStatus
} from '~/types/control-plane'
import type {
  OperatorHeadlineMetric,
  OperatorMarketRegimeChip,
  OperatorMoodChip,
  OperatorMotiveItem,
  OperatorWeatherMaterializeResult,
  OperatorWeatherRunConfig
} from '~/types/operator-dashboard'

interface OperatorHeadlineMetricsInput {
  activeEconomics: BaselinePreviewEconomics | null
  selectedStrategyId: string | null
  weatherBiasAverage: number
  signalPreviewLastLoadedLabel: string
  equivalentCyclePreview: string
  availabilityPercent: number
  readModelHealthMeta: string
  activeAlertCount: number
}

interface OperatorMoodChipInput {
  activeAlertCount: number
  activeEconomics: BaselinePreviewEconomics | null
  weatherBiasAverage: number
  criticalTenantCount: number
  hasPreparedWeatherData: boolean
}

interface OperatorMarketRegimeInput {
  activeAlertCount: number
  weatherBiasAverage: number
  netValueUah: number | null
}

interface OperatorMotiveItemsInput {
  tenantCount: number
  criticalTenantCount: number
  operatorStatus: OperatorStatus | null
  runConfig: OperatorWeatherRunConfig | null
  materializeResult: OperatorWeatherMaterializeResult | null
}

export const buildOperatorHeadlineMetrics = (
  input: OperatorHeadlineMetricsInput
): OperatorHeadlineMetric[] => [
  {
    label: 'Net plan value',
    value: input.activeEconomics ? formatUah(input.activeEconomics.total_net_value_uah) : 'Waiting',
    meta: input.selectedStrategyId || 'Baseline LP preview',
    icon: 'i-lucide-wallet-cards',
    tone: 'green',
    tooltipTitle: 'Net plan value',
    tooltipBody: 'Operator-facing value after the selected preview schedule subtracts battery degradation from gross market revenue.',
    tooltipFormula: 'net_value = gross_market_value - degradation_penalty'
  },
  {
    label: 'Energy arbitrage',
    value: input.activeEconomics ? formatUah(input.activeEconomics.total_gross_market_value_uah) : 'Waiting',
    meta: 'Gross market value',
    icon: 'i-lucide-zap',
    tone: 'blue',
    tooltipTitle: 'Energy arbitrage',
    tooltipBody: 'Projected gross value from moving battery energy through the visible price spread before degradation cost is applied.',
    tooltipFormula: 'sum(hourly_dispatch_value) across the LP horizon'
  },
  {
    label: 'Weather uplift',
    value: `${input.weatherBiasAverage > 0 ? '+' : ''}${input.weatherBiasAverage.toFixed(1)} UAH/MWh`,
    meta: input.signalPreviewLastLoadedLabel,
    icon: 'i-lucide-cloud-sun',
    tone: 'mint',
    tooltipTitle: 'Weather uplift',
    tooltipBody: 'Average calibrated weather effect applied to the MVP market forecast for the selected location.',
    tooltipFormula: 'weather_bias = f(clouds, rain, humidity, temperature, solar, wind)'
  },
  {
    label: 'Cycle preview',
    value: input.equivalentCyclePreview,
    meta: 'Throughput-aware',
    icon: 'i-lucide-refresh-cw',
    tone: 'lime',
    tooltipTitle: 'Equivalent full cycles',
    tooltipBody: 'A quick wear proxy showing how much of a full charge-discharge cycle the preview schedule consumes.',
    tooltipFormula: 'EFC = throughput_mwh / (capacity_mwh * 2)'
  },
  {
    label: 'Read-model health',
    value: `${input.availabilityPercent.toFixed(1)}%`,
    meta: input.readModelHealthMeta,
    icon: 'i-lucide-radio-tower',
    tone: input.activeAlertCount === 0 ? 'green' : 'orange',
    tooltipTitle: 'Read-model health',
    tooltipBody: 'A display health signal for required FastAPI read models and local operator surfaces. Gaps mean review-only evidence may be incomplete.',
    tooltipFormula: 'health = preview_sources_loaded - read_model_gap_penalty'
  }
]

export const buildOperatorMoodChips = (input: OperatorMoodChipInput): OperatorMoodChip[] => [
  {
    label: 'Read model',
    value: input.activeAlertCount > 0 ? 'Gaps' : 'Loaded',
    tone: input.activeAlertCount > 0 ? 'orange' : 'green'
  },
  {
    label: 'Value spread',
    value: input.activeEconomics && input.activeEconomics.total_net_value_uah > 0 ? 'Positive' : 'Learning',
    tone: 'green'
  },
  {
    label: 'DAM volatility',
    value: Math.abs(input.weatherBiasAverage) > 15 ? 'High' : 'Moderate',
    tone: Math.abs(input.weatherBiasAverage) > 15 ? 'orange' : 'blue'
  },
  {
    label: 'Tenant data',
    value: input.criticalTenantCount > 0 ? 'Critical lot' : 'Quiet',
    tone: 'green'
  },
  {
    label: 'Weather data',
    value: input.hasPreparedWeatherData ? 'Prepared' : 'Staging',
    tone: 'mint'
  }
]

export const buildOperatorMarketRegimeChips = (
  input: OperatorMarketRegimeInput
): OperatorMarketRegimeChip[] => [
  {
    label: 'Normal',
    icon: 'i-lucide-sun',
    active: input.activeAlertCount === 0,
    tooltipTitle: 'Normal regime',
    tooltipBody: 'No visible operator errors are active, so the DAM/IDM hourly preview can be reviewed as a normal market-watch state.'
  },
  {
    label: 'Low vol',
    icon: 'i-lucide-cloud',
    active: Math.abs(input.weatherBiasAverage) < 8,
    tooltipTitle: 'Low volatility',
    tooltipBody: 'Weather uplift is small enough that the selected DAM window is treated as calmer.'
  },
  {
    label: 'High vol',
    icon: 'i-lucide-activity',
    active: Math.abs(input.weatherBiasAverage) >= 8,
    tooltipTitle: 'High volatility',
    tooltipBody: 'Weather uplift is large enough to mark the selected DAM window as more sensitive for operator review.'
  },
  {
    label: 'Recovery',
    icon: 'i-lucide-trending-up',
    active: typeof input.netValueUah === 'number' && input.netValueUah > 0,
    tooltipTitle: 'Recovery window',
    tooltipBody: 'The LP preview is net-positive after degradation cost, so the screen flags this as a useful arbitrage recovery surface.'
  }
]

export const buildOperatorMotiveItems = (input: OperatorMotiveItemsInput): OperatorMotiveItem[] => {
  const coverage = Math.min(100, 46 + input.tenantCount * 9)
  const readiness = Math.min(
    100,
    52 + input.criticalTenantCount * 7 + (input.operatorStatus?.status === 'prepared' ? 12 : 0)
  )
  const pressure = Math.min(
    100,
    34 + input.tenantCount * 5 + (input.operatorStatus?.status === 'completed' ? 10 : 0)
  )

  return [
    {
      label: 'Registry health',
      value: coverage,
      tone: 'blue',
      hint: `${input.tenantCount || 0} lots mapped into the operator shell.`
    },
    {
      label: 'Weather readiness',
      value: readiness,
      tone: 'green',
      hint: input.runConfig
        ? `Run config staged for ${input.runConfig.tenant_id}.`
        : 'Prepare a run config to stage the weather slice.'
    },
    {
      label: 'Grid pressure',
      value: pressure,
      tone: 'orange',
      hint: input.materializeResult?.success
        ? `Assets fired: ${input.materializeResult.selected_assets.join(', ')}.`
        : 'Preview signal only until materialization succeeds.'
    }
  ]
}

const formatUah = (value: number): string => `${Math.round(value).toLocaleString('en-GB')} UAH`
