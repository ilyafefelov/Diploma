<script setup lang="ts">
import { computed } from 'vue'
import HudSignalCharts from '~/components/dashboard/HudSignalCharts.vue'
import OperatorMarketSignalHero from '~/components/dashboard/operator/OperatorMarketSignalHero.vue'
import type { OperatorRecommendationResponse, SignalPreview, TenantSummary } from '~/types/control-plane'
import type {
  OperatorChartHorizon,
  OperatorExplanationMode,
  OperatorMarketRegimeChip,
  OperatorMarketVenue
} from '~/types/operator-dashboard'
import {
  operatorChartHorizonOptions,
  operatorMarketScopeLabel,
  operatorMarketVenueLabel,
  operatorMarketVenueOptions
} from '~/utils/operatorPreviewControls'

const props = defineProps<{
  tenants: TenantSummary[]
  selectedTenantId: string
  registryEnvelope: string
  explanationMode: OperatorExplanationMode
  explanationModeLabel: string
  marketRegimeChips: OperatorMarketRegimeChip[]
  signalPreview: SignalPreview | null
  operatorRecommendation: OperatorRecommendationResponse | null
  marketPreviewError: string
  selectedMarketVenue: OperatorMarketVenue
  selectedTargetDeliveryDate: string | null
  selectedChartHorizon: OperatorChartHorizon
  isRegistryLoading: boolean
  isSignalPreviewLoading: boolean
  signalPreviewLastLoadedLabel: string
}>()

const emit = defineEmits<{
  'update:explanationMode': [value: OperatorExplanationMode]
  'update:selectedMarketVenue': [value: OperatorMarketVenue]
  'update:selectedTargetDeliveryDate': [value: string | null]
  'update:selectedChartHorizon': [value: OperatorChartHorizon]
}>()

const marketVenueOptions = operatorMarketVenueOptions
const chartHorizonOptions = operatorChartHorizonOptions

const formatDateInputValue = (date: Date): string => {
  const localDate = new Date(date.getTime() - date.getTimezoneOffset() * 60_000)
  return localDate.toISOString().slice(0, 10)
}

const addDays = (days: number): string => {
  const date = new Date()
  date.setDate(date.getDate() + days)
  return formatDateInputValue(date)
}

const targetDateShortcuts = computed(() => [
  {
    label: 'Latest official',
    value: null,
    detail: 'Official/source row first'
  },
  {
    label: 'Today',
    value: addDays(0),
    detail: 'current delivery date'
  },
  {
    label: 'Tomorrow',
    value: addDays(1),
    detail: 'next delivery date'
  },
  {
    label: 'Day +2',
    value: addDays(2),
    detail: 'pre-publication planning'
  }
])

const updateTargetDateFromInput = (event: Event): void => {
  const target = event.target
  if (!(target instanceof HTMLInputElement)) {
    return
  }

  emit('update:selectedTargetDeliveryDate', target.value || null)
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

const hasMarketPreviewError = computed(() => props.marketPreviewError.trim().length > 0)

const marketBoundaryItems = computed(() => {
  const recommendation = hasMarketPreviewError.value ? null : props.operatorRecommendation
  const responseVenue = operatorMarketVenueLabel(recommendation?.market_venue ?? props.selectedMarketVenue)

  return [
    {
      label: 'Scope',
      value: operatorMarketScopeLabel(responseVenue)
    },
    {
      label: 'Target',
      value: recommendation?.target_delivery_date
        ?? props.selectedTargetDeliveryDate
        ?? 'latest official/source row'
    },
    {
      label: 'Price',
      value: formatBoundaryStatus(recommendation?.price_context_status ?? 'source_pending')
    },
    {
      label: 'Delivery',
      value: recommendation?.target_delivery_window_start && recommendation.target_delivery_window_end
        ? `${formatBoundaryTimestamp(recommendation.target_delivery_window_start)} -> ${formatBoundaryTimestamp(recommendation.target_delivery_window_end)}`
        : 'loading'
    },
    {
      label: 'Gate',
      value: formatBoundaryStatus(recommendation?.market_gate_status ?? 'not_evaluated_preview_only')
    },
    {
      label: 'Bid',
      value: formatBoundaryStatus(recommendation?.proposed_bid_status ?? 'not_emitted_operator_preview')
    }
  ]
})
</script>

<template>
  <section
    id="operator-market"
    class="surface-panel market-console"
  >
    <div class="console-heading">
      <div>
        <p class="eyebrow">
          Market scope
        </p>
        <h2 class="section-title">
          DAM/IDM hourly recommendation preview
        </h2>
        <p class="console-subcopy">
          Official/source row first. NBEATSx/TFT scenario context only appears for unpublished horizons. No ProposedBid,
          no market submission, no live IDM bid, no settlement.
        </p>
        <div
          class="console-boundary-strip"
          aria-label="Operator preview boundary"
        >
          <span
            v-for="item in marketBoundaryItems"
            :key="item.label"
            class="console-boundary-pill"
          >
            <strong>{{ item.label }}</strong>
            {{ item.value }}
          </span>
        </div>
      </div>

      <div class="console-controls">
        <div
          class="console-control-group"
          aria-label="Market venue"
        >
          <span class="console-control-label">Venue</span>
          <div
            class="segmented-control"
            role="tablist"
            aria-label="Market venue"
          >
            <button
              v-for="option in marketVenueOptions"
              :key="option.value"
              type="button"
              :aria-selected="selectedMarketVenue === option.value"
              :title="option.description"
              :class="{ 'segmented-control__button-active': selectedMarketVenue === option.value }"
              @click="emit('update:selectedMarketVenue', option.value)"
            >
              {{ option.label }}
            </button>
          </div>
        </div>

        <div
          class="console-control-group console-control-group-wide"
          aria-label="Target delivery date"
        >
          <span class="console-control-label">Target date</span>
          <div class="console-date-row">
            <button
              v-for="shortcut in targetDateShortcuts"
              :key="shortcut.label"
              type="button"
              class="console-date-chip"
              :class="{ 'console-date-chip-active': selectedTargetDeliveryDate === shortcut.value }"
              :title="shortcut.detail"
              @click="emit('update:selectedTargetDeliveryDate', shortcut.value)"
            >
              {{ shortcut.label }}
            </button>
            <input
              class="console-date-input"
              type="date"
              :value="selectedTargetDeliveryDate ?? ''"
              aria-label="Custom target delivery date"
              @input="updateTargetDateFromInput"
            >
          </div>
        </div>

        <div
          class="console-control-group"
          aria-label="Visible chart horizon"
        >
          <span class="console-control-label">Charts</span>
          <div
            class="segmented-control"
            role="tablist"
            aria-label="Visible chart horizon"
          >
            <button
              v-for="option in chartHorizonOptions"
              :key="option.value"
              type="button"
              :aria-selected="selectedChartHorizon === option.value"
              :class="{ 'segmented-control__button-active': selectedChartHorizon === option.value }"
              @click="emit('update:selectedChartHorizon', option.value)"
            >
              {{ option.label }}
            </button>
          </div>
        </div>

        <div
          class="segmented-control"
          role="tablist"
          aria-label="Explanation mode"
        >
          <UButton
            label="MVP"
            color="info"
            variant="ghost"
            :class="{ 'segmented-control__button-active': explanationMode === 'mvp' }"
            @click="emit('update:explanationMode', 'mvp')"
          />
          <UButton
            label="Future"
            color="info"
            variant="ghost"
            :class="{ 'segmented-control__button-active': explanationMode === 'future' }"
            @click="emit('update:explanationMode', 'future')"
          />
        </div>

        <UBadge
          class="console-badge"
          :label="explanationModeLabel"
          color="success"
          variant="soft"
        />
      </div>
    </div>

    <div class="market-signal-layout">
      <div class="market-signal-panel">
        <OperatorMarketSignalHero
          :signal-preview="signalPreview"
          :operator-recommendation="operatorRecommendation"
          :selected-market-venue="selectedMarketVenue"
          :selected-target-delivery-date="selectedTargetDeliveryDate"
          :selected-chart-horizon="selectedChartHorizon"
          :market-preview-error="marketPreviewError"
          :is-loading="isSignalPreviewLoading"
          :last-loaded-label="signalPreviewLastLoadedLabel"
        />
      </div>

      <div class="market-regime">
        <div class="market-regime__heading">
          <p class="market-regime__label">
            Market regime
          </p>
        </div>
        <div class="market-regime__chips">
          <span
            v-for="chip in marketRegimeChips"
            :key="chip.label"
            class="market-chip"
            :class="{ 'market-chip-active': chip.active }"
            role="group"
            :aria-label="`${chip.label} market regime: ${chip.tooltipTitle}`"
            tabindex="0"
          >
            <UIcon :name="chip.icon" />
            <span>{{ chip.label }}</span>
            <span
              class="market-chip-tooltip"
              role="tooltip"
            >
              <strong>{{ chip.tooltipTitle }}</strong>
              <span>{{ chip.tooltipBody }}</span>
            </span>
          </span>
        </div>
      </div>
    </div>

    <ClientOnly>
      <HudSignalCharts
        :signal-preview="signalPreview"
        :operator-recommendation="operatorRecommendation"
        :selected-market-venue="selectedMarketVenue"
        :selected-chart-horizon="selectedChartHorizon"
        :market-preview-error="marketPreviewError"
        :is-loading="isSignalPreviewLoading"
        :last-loaded-label="signalPreviewLastLoadedLabel"
        :explanation-mode="explanationMode"
      />

      <template #fallback>
        <div class="chart-fallback chart-fallback-compact">
          Preparing signal charts...
        </div>
      </template>
    </ClientOnly>
  </section>
</template>
