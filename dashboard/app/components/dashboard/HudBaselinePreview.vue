<script setup lang="ts">
import { computed, ref } from 'vue'

import HudBaselineChartGrid from '~/components/dashboard/baseline/HudBaselineChartGrid.vue'
import HudBaselineExplainerGrid from '~/components/dashboard/baseline/HudBaselineExplainerGrid.vue'
import HudBaselineMetricStrips from '~/components/dashboard/baseline/HudBaselineMetricStrips.vue'
import HudBaselinePlanningBoundary from '~/components/dashboard/baseline/HudBaselinePlanningBoundary.vue'
import HudBaselinePreviewHeader from '~/components/dashboard/baseline/HudBaselinePreviewHeader.vue'
import type { BaselineLpPreview, OperatorRecommendationResponse } from '~/types/control-plane'
import type { OperatorChartHorizon } from '~/types/operator-dashboard'
import type {
  BaselineBoundaryItem,
  BaselineCompactItem,
  BaselineFeasiblePlanItem,
  BaselineMetricTooltipItem
} from '~/types/hud-baseline-preview'
import { buildBaselineForecastChartOption, buildBaselineScheduleChartOption } from '~/utils/dashboardChartTheme'
import { sliceBaselinePreviewForChartHorizon } from '~/utils/operatorPreviewControls'

const props = defineProps<{
  baselinePreview: BaselineLpPreview | null
  operatorRecommendation: OperatorRecommendationResponse | null
  selectedStrategyId: string
  selectedChartHorizon: OperatorChartHorizon
  isLoading: boolean
  lastLoadedLabel: string
  explanationMode: 'mvp' | 'future'
}>()

const visibleBaselinePreview = computed(() => sliceBaselinePreviewForChartHorizon(
  props.baselinePreview,
  props.selectedChartHorizon
))
const forecastOption = computed(() => buildBaselineForecastChartOption(visibleBaselinePreview.value))
const scheduleOption = computed(() => buildBaselineScheduleChartOption(visibleBaselinePreview.value))
const startingSocSourceLabel = computed(() => props.baselinePreview?.starting_soc_source || 'not reported')
const telemetryFreshnessLabel = computed(() => formatTelemetryFreshness(props.baselinePreview?.telemetry_freshness))
const isExpanded = ref(false)

const baselineBoundaryItems = computed<BaselineBoundaryItem[]>(() => {
  const preview = props.baselinePreview

  return [
    {
      label: `${preview?.market_venue === 'IDM' ? 'IDM' : 'DAM'} delivery`,
      value: preview?.target_delivery_window_start && preview.target_delivery_window_end
        ? `${formatBoundaryTimestamp(preview.target_delivery_window_start)} -> ${formatBoundaryTimestamp(preview.target_delivery_window_end)}`
        : 'loading'
    },
    {
      label: 'Read-model anchor',
      value: formatBoundaryTimestamp(preview?.anchor_timestamp)
    },
    {
      label: 'Execution',
      value: preview?.market_execution_enabled ? 'Market execution enabled' : 'No market execution'
    },
    {
      label: 'Bid status',
      value: formatBoundaryStatus(preview?.proposed_bid_status ?? 'not_emitted_operator_preview')
    }
  ]
})

const selectedStrategyLabel = computed(() => {
  if (!props.operatorRecommendation) {
    return formatStrategyId(props.selectedStrategyId)
  }

  const selectedOption = props.operatorRecommendation.available_strategies.find(strategy =>
    strategy.strategy_id === props.operatorRecommendation?.selected_strategy_id
  )

  return selectedOption?.label || props.operatorRecommendation.selected_strategy_id
})

const selectedStrategyValueLabel = computed(() => {
  if (!props.operatorRecommendation) {
    return 'value loading'
  }

  return `${Math.round(props.operatorRecommendation.economics.total_net_value_uah).toLocaleString('en-GB')} UAH`
})

const compactPreviewItems = computed<BaselineCompactItem[]>(() =>
  [economicsItems.value[0], economicsItems.value[2], economicsItems.value[3]]
    .filter((item): item is BaselineMetricTooltipItem => Boolean(item))
    .map(item => ({
      label: item.label,
      value: item.value
    }))
)

const economicsItems = computed<BaselineMetricTooltipItem[]>(() => {
  if (!props.baselinePreview) {
    return [
      {
        label: 'Gross value',
        value: 'Waiting',
        tooltipTitle: 'Gross market value',
        tooltipBody: 'Projected market revenue before degradation cost is applied.',
        tooltipFormula: 'Calculated by summing hourly market value across the recommendation schedule.'
      },
      {
        label: 'Degradation',
        value: 'Waiting',
        tooltipTitle: 'Degradation penalty',
        tooltipBody: 'Estimated battery wear cost from moving energy through the pack.',
        tooltipFormula: 'Calculated from simulated throughput and the configured cost per full cycle.'
      },
      {
        label: 'Net value',
        value: 'Waiting',
        tooltipTitle: 'Net plan value',
        tooltipBody: 'Projected economic outcome after subtracting battery wear from gross market value.',
        tooltipFormula: 'Gross value minus degradation penalty.'
      },
      {
        label: 'Throughput',
        value: 'Waiting',
        tooltipTitle: 'Battery throughput',
        tooltipBody: 'Total energy expected to pass through the battery during the feasible plan.',
        tooltipFormula: 'Sum of hourly charge and discharge energy handled by the projected state model.'
      }
    ]
  }

  const economics = props.baselinePreview.economics

  return [
    {
      label: 'Gross value',
      value: `${Math.round(economics.total_gross_market_value_uah).toLocaleString('en-GB')} UAH`,
      tooltipTitle: 'Gross market value',
      tooltipBody: 'This is the projected market-facing revenue from the baseline LP schedule before battery wear is charged against it.',
      tooltipFormula: 'Built by summing the hourly gross market value of every scheduled recommendation point.'
    },
    {
      label: 'Degradation',
      value: `${Math.round(economics.total_degradation_penalty_uah).toLocaleString('en-GB')} UAH`,
      tooltipTitle: 'Degradation penalty',
      tooltipBody: 'This is the expected battery wear cost caused by executing the feasible plan through the projected battery model.',
      tooltipFormula: 'Built from total simulated throughput and the configured degradation cost per equivalent full cycle.'
    },
    {
      label: 'Net value',
      value: `${Math.round(economics.total_net_value_uah).toLocaleString('en-GB')} UAH`,
      tooltipTitle: 'Net plan value',
      tooltipBody: 'This is the operator-facing value left after the battery wear penalty is deducted from gross market value.',
      tooltipFormula: 'Built as gross value minus degradation penalty across the full recommendation horizon.'
    },
    {
      label: 'Throughput',
      value: `${economics.total_throughput_mwh.toFixed(2)} MWh`,
      tooltipTitle: 'Battery throughput',
      tooltipBody: 'This is the total energy volume that the battery is expected to process while following the feasible plan.',
      tooltipFormula: 'Built by summing hourly charge and discharge energy from the projected state trace.'
    }
  ]
})

const feasiblePlanItems = computed<BaselineFeasiblePlanItem[]>(() => {
  if (!props.baselinePreview) {
    return [
      {
        label: 'Power corridor',
        value: 'Waiting',
        note: 'Signed dispatch envelope in MW.',
        tooltipTitle: 'Signed dispatch limits',
        tooltipBody: 'Fallback clip band used to keep every simulated step within the inverter and safety envelope.',
        tooltipFormula: 'power_cmd_clipped = clamp(recommended_net_power_mw, -Pmax, +Pmax)'
      },
      {
        label: 'SOC guardrails',
        value: 'Waiting',
        note: 'Projected battery band in %.',
        tooltipTitle: 'SOC guardrails',
        tooltipBody: 'Preview SOC must remain inside feasible charge limits for every timestep.',
        tooltipFormula: 'SOC_next = SOC_prev + (charge_eff x positive_power - discharge_power / discharge_eff) x Dt / E_cap'
      },
      {
        label: 'Planning grain',
        value: 'Waiting',
        note: 'Hourly review step for the preview.',
        tooltipTitle: 'Dispatch grain',
        tooltipBody: 'Controls how often the recommendation point is evaluated and executed.',
        tooltipFormula: 'control_points = total_horizon_minutes / interval_minutes'
      }
    ]
  }

  const metrics = props.baselinePreview.battery_metrics

  return [
    {
      label: 'Power corridor',
      value: `-${metrics.max_power_mw.toFixed(1)} to +${metrics.max_power_mw.toFixed(1)} MW`,
      note: 'Negative values mean charging, positive values mean discharge.',
      tooltipTitle: 'Signed dispatch limits',
      tooltipBody: 'The feasible model clamps every action to this battery-inverter corridor so no step violates nominal capability.',
      tooltipFormula: 'power_command = clamp(raw_command, -Pmax, +Pmax)'
    },
    {
      label: 'SOC guardrails',
      value: `${Math.round(metrics.soc_min_fraction * 100)}% to ${Math.round(metrics.soc_max_fraction * 100)}%`,
      note: 'Projected state must stay inside the feasible battery window.',
      tooltipTitle: 'SOC guardrails',
      tooltipBody: 'Battery SOC is constrained to this admissible band for reliability and longevity.',
      tooltipFormula: 'soc_min_fraction <= SOC_t <= soc_max_fraction'
    },
    {
      label: 'Planning grain',
      value: `${props.baselinePreview.interval_minutes} min`,
      note: 'Every recommendation point is one operator review bucket.',
      tooltipTitle: 'Dispatch grain',
      tooltipBody: 'The interval length sets both smoothing of policy signal and schedule granularity.',
      tooltipFormula: 'Dt = interval_minutes / 60'
    }
  ]
})

const formatTelemetryFreshness = (freshness: Record<string, unknown> | null | undefined): string => {
  if (!freshness) {
    return 'not reported'
  }

  const freshnessLabel = freshness.telemetry_freshness
    ?? freshness.freshness
    ?? freshness.status

  return typeof freshnessLabel === 'string' ? freshnessLabel : 'metadata available'
}

const formatBoundaryTimestamp = (value: string | null | undefined): string => {
  if (!value) {
    return 'not available'
  }

  return new Date(value).toLocaleString('en-GB', {
    month: 'short',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit'
  })
}

const formatBoundaryStatus = (value: string | null | undefined): string => {
  if (!value) {
    return 'not available'
  }

  return value
    .split('_')
    .filter(Boolean)
    .map(part => part.length <= 3 ? part.toUpperCase() : `${part[0]?.toUpperCase() ?? ''}${part.slice(1)}`)
    .join(' ')
}

const formatStrategyId = (strategyId: string): string => strategyId
  .split('_')
  .filter(Boolean)
  .map(part => part.length <= 3 ? part.toUpperCase() : `${part[0]?.toUpperCase() ?? ''}${part.slice(1)}`)
  .join(' ')
</script>

<template>
  <section
    id="operator-baseline"
    class="baseline-slab"
  >
    <HudBaselinePreviewHeader
      :is-expanded="isExpanded"
      :last-loaded-label="lastLoadedLabel"
      :selected-strategy-label="selectedStrategyLabel"
      :selected-strategy-value-label="selectedStrategyValueLabel"
      :baseline-boundary-items="baselineBoundaryItems"
      :compact-preview-items="compactPreviewItems"
      @toggle="isExpanded = !isExpanded"
    />

    <div
      v-if="isExpanded"
      class="baseline-slab__expanded"
    >
      <HudBaselineMetricStrips
        :economics-items="economicsItems"
        :feasible-plan-items="feasiblePlanItems"
      />

      <HudBaselineChartGrid
        :is-loading="isLoading"
        :forecast-option="forecastOption"
        :schedule-option="scheduleOption"
      />

      <HudBaselineExplainerGrid
        :explanation-mode="explanationMode"
        :starting-soc-source-label="startingSocSourceLabel"
        :telemetry-freshness-label="telemetryFreshnessLabel"
        :selected-strategy-label="selectedStrategyLabel"
      />

      <HudBaselinePlanningBoundary :explanation-mode="explanationMode" />
    </div>
  </section>
</template>

<style scoped>
.baseline-slab {
  position: relative;
  display: grid;
  gap: 0.85rem;
  padding: 0.95rem;
  overflow: visible;
  border: 1px solid color-mix(in oklab, var(--panel-strong) 70%, transparent);
  border-radius: 0.95rem;
  background:
    linear-gradient(180deg, color-mix(in oklab, var(--panel-strong) 12%, transparent), transparent 42%),
    radial-gradient(circle at top left, var(--operator-accent-glow), transparent 24%),
    linear-gradient(180deg, var(--operator-topbar-gradient-top), var(--operator-topbar-gradient-bottom));
  box-shadow:
    0 18px 38px color-mix(in oklab, var(--operator-surface) 30%, transparent),
    inset 0 1px 0 var(--operator-card-border);
}

.baseline-slab__expanded {
  display: grid;
  gap: 0.9rem;
}
</style>
