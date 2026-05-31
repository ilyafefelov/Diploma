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

const selectedMarketSignalPreview = computed(() => selectOperatorMarketSignalPreview(
  props.signalPreview,
  props.operatorRecommendation
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
    priceContextStatus: props.operatorRecommendation?.price_context_status,
    priceContextSourceLabel: operatorPriceContextSourceLabel(props.operatorRecommendation)
  }
))
const priceContextModeLabel = computed(() => operatorPriceContextModeLabel(props.operatorRecommendation))
const priceContextSourceLabel = computed(() => operatorPriceContextSourceLabel(props.operatorRecommendation))
const priceContextMetricLabel = computed(() => {
  if (props.operatorRecommendation?.price_context_status === 'pre_publication_forecast') {
    return `${marketVenueLabel.value} ML forecast price`
  }

  return `${marketVenueLabel.value} official/source price`
})

const hasSignalData = computed(() => {
  return !!visibleSignalPreview.value && visibleSignalPreview.value.market_price.length > 0
})
const hasMarketPreviewBlocker = computed(() => props.marketPreviewError.trim().length > 0)

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

const forecastWindowPeriodLabel = computed(() => {
  const startLabel = formatPeriodDateTime(
    props.operatorRecommendation?.target_delivery_window_start
      || visibleSignalPreview.value?.forecast_window_start
  )
  const endLabel = formatPeriodDateTime(
    props.operatorRecommendation?.target_delivery_window_end
      || visibleSignalPreview.value?.forecast_window_end
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

const avgBias = computed(() => {
  if (!visibleSignalPreview.value?.weather_bias.length) {
    return null
  }

  const avg = visibleSignalPreview.value.weather_bias.reduce((acc, value) => acc + value, 0) / visibleSignalPreview.value.weather_bias.length
  return avg
})

const forecastSpread = computed(() => {
  if (maxMarketPrice.value == null || minMarketPrice.value == null) {
    return null
  }

  return maxMarketPrice.value - minMarketPrice.value
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
        <span>{{ operatorRecommendation?.target_delivery_date ?? selectedTargetDeliveryDate ?? 'latest' }}</span>
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
      v-else-if="!isLoading"
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
      v-if="!hasMarketPreviewBlocker && !isLoading"
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
        :aria-label="`Weather uplift: ${avgBias == null ? 'pending' : `${avgBias >= 0 ? '+' : ''}${formatNumber(avgBias)} UAH/MWh`}. ${forecastWindowPeriodLabel}`"
        tabindex="0"
      >
        <p class="hud-mini-stat__label">
          Weather uplift
        </p>
        <strong>{{ avgBias == null ? '—' : `${avgBias >= 0 ? '+' : ''}${formatNumber(avgBias)} UAH/MWh` }}</strong>
        <p class="hud-mini-stat__meta">
          {{ forecastWindowPeriodLabel }}
        </p>
        <span
          class="hud-mini-stat__tooltip"
          role="tooltip"
        >
          <span class="hud-mini-stat__tooltip-title">Weather term</span>
          <span>Formula: price_adj = market_price + weather_bias</span>
          <span>Period: {{ forecastWindowPeriodLabel }}.</span>
          <span>Source: calibrated uplift from cloud, precipitation, humidity, solar, and wind.</span>
        </span>
      </article>
      <article
        class="hud-mini-stat"
        role="group"
        :aria-label="`Signal count: ${hasSignalData ? visibleSignalPreview?.labels.length : 'pending'}. ${forecastWindowPeriodLabel}`"
        tabindex="0"
      >
        <p class="hud-mini-stat__label">
          Signal count
        </p>
        <strong>{{ hasSignalData ? visibleSignalPreview?.labels.length : '—' }}</strong>
        <p class="hud-mini-stat__meta">
          {{ forecastWindowPeriodLabel }}
        </p>
        <span
          class="hud-mini-stat__tooltip"
          role="tooltip"
        >
          <span class="hud-mini-stat__tooltip-title">Signal density</span>
          <span>Formula: point_count = len(labels)</span>
          <span>Period: {{ forecastWindowPeriodLabel }}.</span>
          <span>Interpretation: longer horizon gives smoother visual trend and confidence for schedule review.</span>
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
        Preparing market signals...
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
