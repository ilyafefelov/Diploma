<script setup lang="ts">
import { computed } from 'vue'

import ClientVChart from '~/components/dashboard/ClientVChart.vue'
import type { OperatorRecommendationResponse, SignalPreview } from '~/types/control-plane'
import type { OperatorChartHorizon, OperatorMarketVenue } from '~/types/operator-dashboard'
import { buildMarketSignalHeroChartOption } from '~/utils/dashboardChartTheme'
import {
  operatorPriceContextModeLabel,
  operatorPriceContextSourceLabel,
  operatorMarketVenueLabel,
  selectOperatorMarketSignalPreview,
  sliceSignalPreviewForChartHorizon
} from '~/utils/operatorPreviewControls'

const props = defineProps<{
  signalPreview: SignalPreview | null
  operatorRecommendation: OperatorRecommendationResponse | null
  selectedMarketVenue: OperatorMarketVenue
  selectedTargetDeliveryDate: string | null
  selectedChartHorizon: OperatorChartHorizon
  marketPreviewError: string
  isLoading: boolean
  lastLoadedLabel: string
}>()

const activeOperatorRecommendation = computed(() => props.isLoading ? null : props.operatorRecommendation)
const selectedMarketSignalPreview = computed(() => selectOperatorMarketSignalPreview(
  props.signalPreview,
  activeOperatorRecommendation.value
))
const visibleSignalPreview = computed(() => sliceSignalPreviewForChartHorizon(
  selectedMarketSignalPreview.value,
  props.selectedChartHorizon
))
const marketVenueLabel = computed(() => operatorMarketVenueLabel(props.selectedMarketVenue))
const chartOption = computed(() => buildMarketSignalHeroChartOption(
  visibleSignalPreview.value,
  props.selectedMarketVenue,
  {
    priceContextStatus: activeOperatorRecommendation.value?.price_context_status,
    priceContextSourceLabel: operatorPriceContextSourceLabel(activeOperatorRecommendation.value)
  }
))
const priceContextModeLabel = computed(() => operatorPriceContextModeLabel(activeOperatorRecommendation.value))
const priceContextSourceLabel = computed(() => operatorPriceContextSourceLabel(activeOperatorRecommendation.value))
const priceContextMetricLabel = computed(() => {
  if (activeOperatorRecommendation.value?.price_context_status === 'pre_publication_forecast') {
    return `${marketVenueLabel.value} ML forecast price`
  }

  return `${marketVenueLabel.value} official/source price`
})

const hasSignalData = computed(() => {
  return !!visibleSignalPreview.value && visibleSignalPreview.value.market_price.length > 0
})
const hasMarketPreviewBlocker = computed(() => props.marketPreviewError.trim().length > 0)
const isPreparingSelectedPreview = computed(() => {
  return !hasMarketPreviewBlocker.value && (props.isLoading || !hasSignalData.value)
})

const formatNumber = (value: number): string => `${value.toFixed(2)}`
const formatPowerLabel = (value: number): string => `${value >= 0 ? '+' : ''}${value.toFixed(1)} UAH/MWh`
const formatPeriodDateTime = (value: string | null | undefined): string => {
  if (!value) {
    return 'date pending'
  }

  const parsedDate = new Date(value)

  if (Number.isNaN(parsedDate.getTime())) {
    return 'date pending'
  }

  return new Intl.DateTimeFormat('en-GB', {
    day: '2-digit',
    month: 'short',
    hour: '2-digit',
    minute: '2-digit',
    timeZone: visibleSignalPreview.value?.timezone || visibleSignalPreview.value?.resolved_location.timezone || 'Europe/Kyiv'
  }).format(parsedDate)
}

const latestPricePeriodLabel = computed(() => {
  return `Hour: ${formatPeriodDateTime(visibleSignalPreview.value?.latest_price_timestamp || visibleSignalPreview.value?.forecast_window_end)}`
})

const selectedTargetWindowStart = computed(() => {
  if (!props.selectedTargetDeliveryDate) {
    return null
  }

  return `${props.selectedTargetDeliveryDate}T00:00:00`
})

const selectedTargetWindowEnd = computed(() => {
  if (!props.selectedTargetDeliveryDate) {
    return null
  }

  const targetDate = new Date(`${props.selectedTargetDeliveryDate}T00:00:00`)
  if (Number.isNaN(targetDate.getTime())) {
    return null
  }

  targetDate.setDate(targetDate.getDate() + 1)
  return targetDate.toISOString().slice(0, 19)
})

const forecastWindowPeriodLabel = computed(() => {
  const startLabel = formatPeriodDateTime(
    activeOperatorRecommendation.value?.target_delivery_window_start
      || visibleSignalPreview.value?.forecast_window_start
      || selectedTargetWindowStart.value
  )
  const endLabel = formatPeriodDateTime(
    activeOperatorRecommendation.value?.target_delivery_window_end
      || visibleSignalPreview.value?.forecast_window_end
      || selectedTargetWindowEnd.value
  )

  if (startLabel === 'date pending' || endLabel === 'date pending') {
    return 'Selected period pending'
  }

  return `${startLabel} → ${endLabel}`
})

const latestMarketPrice = computed(() => {
  if (!visibleSignalPreview.value?.market_price.length) {
    return null
  }

  return visibleSignalPreview.value.market_price.at(-1) ?? null
})

const maxMarketPrice = computed(() => {
  if (!visibleSignalPreview.value?.market_price.length) {
    return null
  }

  return Math.max(...visibleSignalPreview.value.market_price)
})

const minMarketPrice = computed(() => {
  if (!visibleSignalPreview.value?.market_price.length) {
    return null
  }

  return Math.min(...visibleSignalPreview.value.market_price)
})

const forecastSpread = computed(() => {
  if (maxMarketPrice.value == null || minMarketPrice.value == null) {
    return null
  }

  return maxMarketPrice.value - minMarketPrice.value
})

const contextRowCount = computed(() => visibleSignalPreview.value?.market_price.length ?? 0)
const sourceContextRowCount = computed(() => {
  if (!activeOperatorRecommendation.value) {
    return 0
  }

  if (activeOperatorRecommendation.value.price_context_status === 'pre_publication_forecast') {
    return activeOperatorRecommendation.value.policy_forecast_context_row_count
  }

  return activeOperatorRecommendation.value.recommendation_schedule.length
})
const sourceContextCoverageLabel = computed(() => {
  if (!activeOperatorRecommendation.value) {
    return 'coverage pending'
  }

  if (activeOperatorRecommendation.value.price_context_status === 'pre_publication_forecast') {
    const coveragePercent = Math.round(activeOperatorRecommendation.value.policy_forecast_context_coverage_ratio * 100)
    return `${sourceContextRowCount.value}/24 rows · ${coveragePercent}%`
  }

  return `${sourceContextRowCount.value}/24 source rows`
})
const contextRowStatusLabel = computed(() => {
  if (!activeOperatorRecommendation.value) {
    return 'preview pending'
  }

  if (activeOperatorRecommendation.value.price_context_status === 'pre_publication_forecast') {
    return activeOperatorRecommendation.value.policy_forecast_context_source || 'forecast store'
  }

  if (activeOperatorRecommendation.value.price_context_status === 'official_published') {
    return 'official/source-backed'
  }

  return activeOperatorRecommendation.value.price_context_status || 'source status pending'
})

const activeForecastSeries = computed(() => {
  const source = activeOperatorRecommendation.value?.policy_forecast_context_source
  if (!source) {
    return null
  }

  return activeOperatorRecommendation.value?.forecast_model_series.find(series => series.model_name === source) ?? null
})

const formatGeneratedAtLabel = (value: string | null | undefined): string => {
  if (!value) {
    return 'generated time pending'
  }

  return `generated ${formatPeriodDateTime(value)}`
}

const forecastQualityValueLabel = computed(() => {
  if (!activeOperatorRecommendation.value) {
    return 'Pending'
  }

  if (activeOperatorRecommendation.value.price_context_status === 'official_published') {
    return 'Official row'
  }

  return activeOperatorRecommendation.value.policy_forecast_context_source || 'Forecast store'
})

const forecastQualityMetaLabel = computed(() => {
  if (!activeOperatorRecommendation.value) {
    return 'selected preview not loaded'
  }

  if (activeOperatorRecommendation.value.price_context_status === 'official_published') {
    return `${sourceContextCoverageLabel.value} · no ML price override`
  }

  const uncertaintyKind = activeForecastSeries.value?.uncertainty_kind || 'point'
  const qualityBoundary = activeForecastSeries.value?.quality_boundary || 'quality evidence pending'
  return `${sourceContextCoverageLabel.value} · ${uncertaintyKind} · ${formatGeneratedAtLabel(activeOperatorRecommendation.value.forecast_generated_at)} · ${qualityBoundary}`
})
</script>

<template>
  <div class="market-signal-hero">
    <div class="market-signal-hero__toolbar">
      <div class="market-signal-hero__tabs">
        <span
          class="market-signal-hero__tab"
          :class="{ 'market-signal-hero__tab-active': selectedMarketVenue === 'DAM', 'market-signal-hero__tab-muted': selectedMarketVenue !== 'DAM' }"
        >DAM hourly</span>
        <span
          class="market-signal-hero__tab"
          :class="{ 'market-signal-hero__tab-active': selectedMarketVenue === 'IDM', 'market-signal-hero__tab-muted': selectedMarketVenue !== 'IDM' }"
        >IDM hourly preview</span>
      </div>

      <div class="market-signal-hero__range">
        <span>{{ selectedChartHorizon.toUpperCase() }}</span>
        <span>{{ activeOperatorRecommendation?.target_delivery_date ?? selectedTargetDeliveryDate ?? 'latest' }}</span>
      </div>

      <UBadge
        class="market-signal-hero__updated"
        :label="lastLoadedLabel"
        color="success"
        variant="soft"
      />
    </div>

    <div
      v-if="hasMarketPreviewBlocker"
      class="market-signal-hero__blocker"
      role="status"
    >
      <p>Source-backed preview blocker</p>
      <strong>{{ marketVenueLabel }} hourly preview cannot be charted as official/source-backed context.</strong>
      <span>{{ marketPreviewError }}</span>
    </div>

    <div
      v-else
      class="market-signal-hero__context-strip"
      aria-label="Selected market price context"
    >
      <span>
        <strong>Selected period</strong>
        {{ forecastWindowPeriodLabel }}
      </span>
      <span>
        <strong>Price source</strong>
        {{ priceContextSourceLabel }}
      </span>
      <span>
        <strong>Mode</strong>
        {{ priceContextModeLabel }}
      </span>
    </div>

    <div
      v-if="!hasMarketPreviewBlocker && !isPreparingSelectedPreview"
      class="market-signal-hero__metrics"
    >
      <article
        class="hud-mini-stat"
        role="group"
        :aria-label="`${priceContextMetricLabel}: ${latestMarketPrice == null ? 'pending' : formatPowerLabel(latestMarketPrice)}. ${latestPricePeriodLabel}`"
        tabindex="0"
      >
        <p class="hud-mini-stat__label">
          {{ priceContextMetricLabel }}
        </p>
        <strong>{{ latestMarketPrice == null ? '—' : formatPowerLabel(latestMarketPrice) }}</strong>
        <p class="hud-mini-stat__meta">
          {{ latestPricePeriodLabel }}
        </p>
        <span
          class="hud-mini-stat__tooltip"
          role="tooltip"
        >
          <span class="hud-mini-stat__tooltip-title">{{ priceContextMetricLabel }}</span>
          <span>Formula: P_t = market_price[t]</span>
          <span>Period: {{ latestPricePeriodLabel }}.</span>
          <span>Definition: published windows use official/source-backed rows; unpublished windows use ML forecast evidence; not a market bid.</span>
        </span>
      </article>
      <article
        class="hud-mini-stat"
        role="group"
        :aria-label="`Window spread: ${forecastSpread == null ? 'pending' : `${formatNumber(forecastSpread)} UAH/MWh`}. ${forecastWindowPeriodLabel}`"
        tabindex="0"
      >
        <p class="hud-mini-stat__label">
          Window spread
        </p>
        <strong>{{ forecastSpread == null ? '—' : `${formatNumber(forecastSpread)} UAH/MWh` }}</strong>
        <p class="hud-mini-stat__meta">
          {{ forecastWindowPeriodLabel }}
        </p>
        <span
          class="hud-mini-stat__tooltip"
          role="tooltip"
        >
          <span class="hud-mini-stat__tooltip-title">Forecast band</span>
          <span>Formula: spread = max(price_i) − min(price_i)</span>
          <span>Period: {{ forecastWindowPeriodLabel }}.</span>
          <span>Interpretation: higher spread usually gives higher arbitrage opportunity.</span>
        </span>
      </article>
      <article
        class="hud-mini-stat"
        role="group"
        :aria-label="`Context rows: ${sourceContextRowCount}. ${contextRowStatusLabel}. ${sourceContextCoverageLabel}. ${forecastWindowPeriodLabel}`"
        tabindex="0"
      >
        <p class="hud-mini-stat__label">
          Context rows
        </p>
        <strong>{{ sourceContextRowCount }}</strong>
        <p class="hud-mini-stat__meta">
          {{ sourceContextCoverageLabel }}
        </p>
        <span
          class="hud-mini-stat__tooltip"
          role="tooltip"
        >
          <span class="hud-mini-stat__tooltip-title">Selected preview rows</span>
          <span>Formula: row_count = len(selected_price_context)</span>
          <span>Period: {{ forecastWindowPeriodLabel }}.</span>
          <span>Source: {{ contextRowStatusLabel }}; {{ sourceContextCoverageLabel }}.</span>
        </span>
      </article>
      <article
        class="hud-mini-stat"
        role="group"
        :aria-label="`Forecast quality: ${forecastQualityValueLabel}. ${forecastQualityMetaLabel}. ${forecastWindowPeriodLabel}`"
        tabindex="0"
      >
        <p class="hud-mini-stat__label">
          Forecast QA
        </p>
        <strong>{{ forecastQualityValueLabel }}</strong>
        <p class="hud-mini-stat__meta">
          {{ forecastQualityMetaLabel }}
        </p>
        <span
          class="hud-mini-stat__tooltip"
          role="tooltip"
        >
          <span class="hud-mini-stat__tooltip-title">Forecast quality boundary</span>
          <span>Formula: quality = coverage + source/model + generated_at + calibration boundary</span>
          <span>Period: {{ forecastWindowPeriodLabel }}.</span>
          <span>Interpretation: official rows do not use ML price override; unpublished windows show forecast context and quality boundary only.</span>
        </span>
      </article>
    </div>

    <ClientOnly>
      <div
        v-if="hasMarketPreviewBlocker"
        class="chart-fallback"
      >
        Waiting for source-backed {{ marketVenueLabel }} rows before rendering this venue as official context.
      </div>
      <div
        v-else-if="isLoading"
        class="chart-fallback"
      >
        Preparing selected preview...
      </div>
      <div
        v-else-if="!hasSignalData"
        class="chart-fallback"
      >
        Preparing selected preview...
      </div>
      <ClientVChart
        v-else
        class="market-signal-hero__chart"
        :option="chartOption"
        autoresize
      />

      <template #fallback>
        <div class="chart-fallback">
          Preparing market signals...
        </div>
      </template>
    </ClientOnly>
  </div>
</template>
